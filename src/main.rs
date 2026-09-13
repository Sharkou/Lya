use std::{
    env,
    io::{self, IsTerminal, Write},
    path::Path,
    process::ExitCode,
};

use tokio::io::{AsyncBufReadExt, BufReader};

use lya::{
    agent::Agent,
    llm::ollama::OllamaClient,
    orchestrator::{
        control::{ControlCommand, ControlReceiver, parse_control_command},
        doctor::DoctorReport,
        events::{
            CompositeEventSink, HumanEventSink, HumanRenderMode, JsonEventSink, JsonlEventSink,
        },
        executor::{ClaudeCliExecutor, Executor, ExecutorRequest, ExecutorSession},
        home::LyaHome,
        job::{AutonomousOrchestrator, NewJob, OrchestrationError, RunResult, new_job_id},
        lock::JobLock,
        publisher::{GitPublishConfig, GitPublisher, Publisher},
        resume::{ResumeRejection, resumable_jobs, select_job},
        state::{JobState, JobStatus, StateStore},
        supervisor::{
            CodexCliSupervisor, Project, Supervisor, SupervisorRequest,
            load_required_private_context,
        },
    },
    process::SystemProcessRunner,
    runtime::Runtime,
};

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "doctor")
    {
        if arguments.len() != 1 {
            eprintln!("Usage: lya doctor");
            return ExitCode::FAILURE;
        }
        return run_doctor();
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "supervisor")
    {
        return run_supervisor(&arguments[1..]).await;
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "executor")
    {
        return run_executor(&arguments[1..]).await;
    }
    if arguments.first().is_some_and(|argument| argument == "run") {
        return run_autonomous_job(&arguments[1..]).await;
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "resume")
    {
        return resume_autonomous_job(&arguments[1..]).await;
    }

    let model = match env::var("OLLAMA_MODEL") {
        Ok(model) if !model.trim().is_empty() => model,
        _ => {
            eprintln!("OLLAMA_MODEL must name the model to query.");
            return ExitCode::FAILURE;
        }
    };

    let base_url =
        env::var("OLLAMA_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_owned());
    let client = OllamaClient::new(base_url);
    let prompt = arguments.join(" ");

    if prompt.is_empty() {
        eprintln!("Usage: cargo run -- <prompt>");
        return ExitCode::FAILURE;
    }

    let runtime = match Runtime::from_environment() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Could not configure runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let tools = runtime.tools();
    let agent = Agent::new(&client, tools)
        .with_max_iterations(Agent::<OllamaClient>::DEFAULT_MAX_ITERATIONS);

    match agent.run(model, prompt).await {
        Ok(answer) => {
            println!("{answer}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Agent request failed: {error}");
            ExitCode::FAILURE
        }
    }
}

type JobOrchestrator<P> = AutonomousOrchestrator<
    CodexCliSupervisor<SystemProcessRunner>,
    ClaudeCliExecutor<SystemProcessRunner>,
    SystemProcessRunner,
    P,
    CompositeEventSink,
>;

enum JobAction {
    Start(Box<NewJob>),
    Resume(Box<JobState>, String),
}

async fn execute_job_action<P: Publisher>(
    orchestrator: JobOrchestrator<P>,
    action: JobAction,
) -> Result<RunResult, OrchestrationError> {
    match action {
        JobAction::Start(request) => orchestrator.run_sequential(*request).await,
        JobAction::Resume(job, private_context) => {
            orchestrator.resume_sequential(*job, private_context).await
        }
    }
}

fn event_sink(home: &LyaHome, job_id: &str, output: &RunOutput) -> CompositeEventSink {
    match output {
        RunOutput::Json => CompositeEventSink::new(vec![
            Box::new(JsonlEventSink::for_job(home.path(), job_id)),
            Box::new(JsonEventSink::new(io::stdout())),
        ]),
        RunOutput::Human(mode) => CompositeEventSink::new(vec![
            Box::new(JsonlEventSink::for_job(home.path(), job_id)),
            Box::new(HumanEventSink::stdout(*mode)),
        ]),
    }
}

/// Moves any legacy `LYA_HOME/state.json` into the per-job layout before jobs are read or written.
fn prepare_home(home: &LyaHome) -> Result<StateStore, String> {
    let store = StateStore::new(home);
    let report = store
        .migrate_legacy()
        .map_err(|error| format!("could not migrate existing job state: {error}"))?;
    if !report.is_empty() {
        eprintln!(
            "Migrated {} job(s) from state.json into per-job state; {} already existed.",
            report.migrated.len(),
            report.kept_existing.len()
        );
    }
    Ok(store)
}

#[allow(clippy::too_many_arguments)]
async fn drive_job(
    home: &LyaHome,
    store: &StateStore,
    job_id: &str,
    output: RunOutput,
    publish: Option<GitPublishConfig>,
    browser: bool,
    max_iterations: u32,
    max_jobs: u32,
    action: JobAction,
) -> ExitCode {
    let _lock = match JobLock::acquire(store, job_id) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let control = start_control(&output);
    let sink = event_sink(home, job_id, &output);
    let base = AutonomousOrchestrator::new(
        CodexCliSupervisor::new_for_job(home.path(), job_id),
        ClaudeCliExecutor::new(),
        SystemProcessRunner,
        StateStore::new(home),
    )
    .with_max_iterations(max_iterations)
    .with_max_jobs(max_jobs)
    .with_browser(browser)
    .with_event_sink(sink)
    .with_control_receiver(control);

    let result = match publish {
        Some(configuration) => {
            execute_job_action(
                base.with_publisher(GitPublisher::new(configuration.clone()))
                    .with_publish_configuration(configuration),
                action,
            )
            .await
        }
        None => execute_job_action(base, action).await,
    };

    match result {
        Ok(run) => {
            let job = run
                .jobs
                .last()
                .expect("a run always contains its first job");
            match job.status {
                JobStatus::Accepted
                | JobStatus::Published
                | JobStatus::Paused
                | JobStatus::WaitingHuman
                | JobStatus::WaitingClaudeQuota
                | JobStatus::WaitingOpenAiQuota
                | JobStatus::Stopped => ExitCode::SUCCESS,
                _ => ExitCode::FAILURE,
            }
        }
        Err(error) => {
            eprintln!("Job {job_id} failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_autonomous_job(arguments: &[String]) -> ExitCode {
    let options = match parse_run_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!(
                "{error}\nUsage: lya run [--project <path>] [--browser] [--max-iterations <count>] [--max-jobs <count>] [--publish] [--verbose | --json] <task>"
            );
            return ExitCode::FAILURE;
        }
    };
    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!("Could not resolve Lya home: {error}");
            return ExitCode::FAILURE;
        }
    };
    let store = match prepare_home(&home) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let private_context = match load_required_private_context(&home) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("Could not prepare autonomous job: {error}");
            return ExitCode::FAILURE;
        }
    };
    let publish = if options.publish {
        match GitPublishConfig::from_environment() {
            Ok(configuration) => Some(configuration),
            Err(error) => {
                eprintln!("Could not configure Git publication: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    let job_id = new_job_id();
    let request = NewJob {
        job_id: job_id.clone(),
        project: Project {
            name: project_name(&options.project_path),
            path: options.project_path,
        },
        task: options.task,
        private_context,
    };

    drive_job(
        &home,
        &store,
        &job_id,
        options.output,
        publish,
        options.browser,
        options.max_iterations,
        options.max_jobs,
        JobAction::Start(Box::new(request)),
    )
    .await
}

async fn resume_autonomous_job(arguments: &[String]) -> ExitCode {
    let options = match parse_resume_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\nUsage: lya resume [--job <job-id>] [--verbose | --json]");
            return ExitCode::FAILURE;
        }
    };
    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!("Could not resolve Lya home: {error}");
            return ExitCode::FAILURE;
        }
    };
    let store = match prepare_home(&home) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let candidates = match resumable_jobs(&store) {
        Ok(jobs) => jobs,
        Err(error) => {
            eprintln!("Could not read persisted job state: {error}");
            return ExitCode::FAILURE;
        }
    };
    let job = match select_job(candidates, options.job_id.as_deref()) {
        Ok(job) => job,
        Err(error) => {
            eprintln!("{}", resume_error_message(&store, error));
            return ExitCode::FAILURE;
        }
    };
    let private_context = match load_required_private_context(&home) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("Could not prepare the resumed job: {error}");
            return ExitCode::FAILURE;
        }
    };
    // Publication continues with the configuration persisted for the job, not with whatever the
    // restarted shell happens to export.
    let publish = if job.run.publish {
        match job.run.git.clone() {
            Some(configuration) => Some(configuration),
            None => {
                eprintln!(
                    "Job {} needs to publish but no Git identity, remote and branch were persisted for it. Resolve it manually.",
                    job.job_id
                );
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    let job_id = job.job_id.clone();
    let browser = job.run.browser;
    let max_iterations = job.run.max_iterations;
    let max_jobs = job.run.max_jobs;

    drive_job(
        &home,
        &store,
        &job_id,
        options.output,
        publish,
        browser,
        max_iterations,
        max_jobs,
        JobAction::Resume(Box::new(job), private_context),
    )
    .await
}

/// Explains why a named job cannot be resumed, and otherwise lists what is available.
fn resume_error_message(store: &StateStore, error: ResumeRejection) -> String {
    let ResumeRejection::UnknownJob(job_id) = &error else {
        return error.to_string();
    };
    // A job that exists but is terminal deserves its real reason, not "unknown job".
    if let Ok(Some(job)) = store.load_job(job_id) {
        return ResumeRejection::NotResumable {
            job_id: job.job_id,
            status: job.status.label().to_owned(),
        }
        .to_string();
    }
    match resumable_jobs(store) {
        Ok(jobs) if !jobs.is_empty() => format!(
            "unknown job: {job_id}\nResumable jobs:\n  {}",
            jobs.iter()
                .map(lya::orchestrator::resume::describe)
                .collect::<Vec<_>>()
                .join("\n  ")
        ),
        _ => error.to_string(),
    }
}

async fn run_supervisor(arguments: &[String]) -> ExitCode {
    let task = arguments.join(" ");
    if task.trim().is_empty() {
        eprintln!("Usage: lya supervisor <task>");
        return ExitCode::FAILURE;
    }

    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!("Could not resolve Lya home: {error}");
            return ExitCode::FAILURE;
        }
    };
    let private_context = match load_required_private_context(&home) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("Could not prepare supervisor request: {error}");
            return ExitCode::FAILURE;
        }
    };
    let project_path = match env::current_dir().and_then(|path| path.canonicalize()) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("Could not determine current project directory: {error}");
            return ExitCode::FAILURE;
        }
    };
    let request = SupervisorRequest {
        private_context,
        project: Project {
            name: project_name(&project_path),
            path: project_path,
        },
        task,
        phase: None,
        iteration: 0,
        executor_report: None,
        repository_state: None,
        user_instructions: Vec::new(),
        cancellation: None,
    };
    let supervisor = CodexCliSupervisor::new(home.path());

    match supervisor.decide(request).await {
        Ok(decision) => match serde_json::to_string_pretty(&decision) {
            Ok(output) => {
                println!("{output}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("Could not render supervisor decision: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("Supervisor failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_executor(arguments: &[String]) -> ExitCode {
    let (request, max_turns) = match parse_executor_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!(
                "{error}\nUsage: lya executor [--project <path>] [--resume <session>] [--browser] [--timeout-seconds <seconds>] [--max-turns <count>] <prompt>"
            );
            return ExitCode::FAILURE;
        }
    };
    let mut executor = ClaudeCliExecutor::new();
    if let Some(max_turns) = max_turns {
        executor = executor.with_max_turns(max_turns);
    }

    match executor.execute(request).await {
        Ok(result) => match serde_json::to_string_pretty(&result) {
            Ok(output) => {
                println!("{output}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("Could not render executor result: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("Executor failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse_executor_arguments(
    arguments: &[String],
) -> Result<(ExecutorRequest, Option<u32>), String> {
    let mut project_path = env::current_dir().map_err(|error| error.to_string())?;
    let mut session = ExecutorSession::New;
    let mut browser = false;
    let mut timeout = None;
    let mut max_turns = None;
    let mut prompt = Vec::new();
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--project" => {
                position += 1;
                project_path = arguments
                    .get(position)
                    .map(std::path::PathBuf::from)
                    .ok_or_else(|| "--project requires a path".to_owned())?;
            }
            "--resume" => {
                position += 1;
                session = ExecutorSession::Resume(
                    arguments
                        .get(position)
                        .cloned()
                        .ok_or_else(|| "--resume requires a session ID".to_owned())?,
                );
            }
            "--browser" => browser = true,
            "--timeout-seconds" => {
                position += 1;
                let seconds = arguments
                    .get(position)
                    .ok_or_else(|| "--timeout-seconds requires a number".to_owned())?
                    .parse::<u64>()
                    .map_err(|_| "--timeout-seconds must be an unsigned integer".to_owned())?;
                timeout = Some(std::time::Duration::from_secs(seconds));
            }
            "--max-turns" => {
                position += 1;
                max_turns = Some(
                    arguments
                        .get(position)
                        .ok_or_else(|| "--max-turns requires a number".to_owned())?
                        .parse::<u32>()
                        .map_err(|_| "--max-turns must be an unsigned integer".to_owned())?,
                );
            }
            argument if argument.starts_with("--") => {
                return Err(format!("unknown executor option: {argument}"));
            }
            argument => prompt.push(argument.to_owned()),
        }
        position += 1;
    }

    let project_path = project_path
        .canonicalize()
        .map_err(|error| format!("could not resolve project path: {error}"))?;
    Ok((
        ExecutorRequest {
            project_name: project_name(&project_path),
            project_path,
            prompt: prompt.join(" "),
            session,
            browser,
            timeout,
            user_instructions: Vec::new(),
            cancellation: None,
        },
        max_turns,
    ))
}

/// Starts the control channel for a job.
///
/// The graceful termination signal is always handled, in every output mode and whether or not a
/// terminal is attached. Only the line-oriented command reader is restricted to an interactive
/// human-mode terminal, so `--json` stdout stays valid `JobEvent` JSONL.
fn start_control(output: &RunOutput) -> ControlReceiver {
    let (sender, receiver) = ControlReceiver::new();
    if interactive_enabled(
        output,
        io::stdin().is_terminal(),
        io::stdout().is_terminal(),
    ) {
        spawn_command_reader(sender.clone());
    }
    spawn_interrupt_handler(sender);
    receiver
}

fn spawn_command_reader(command_sender: lya::orchestrator::control::ControlSender) {
    tokio::spawn(async move {
        println!("/help for commands | Ctrl+C to stop");
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        loop {
            print!("> ");
            let _ = io::stdout().flush();
            let Ok(Some(line)) = lines.next_line().await else {
                break;
            };
            if line.trim() == "/help" {
                println!("/help  /status  /diff  /pause  /resume  /stop  /send <instruction>");
                continue;
            }
            match parse_control_command(&line) {
                Ok(Some(command)) => {
                    let acknowledgement = match &command {
                        ControlCommand::Pause => {
                            "Pause requested; Lya will pause at a safe boundary."
                        }
                        ControlCommand::Resume => "Resume requested.",
                        ControlCommand::Stop => {
                            "Stop requested. Finishing the current safe shutdown..."
                        }
                        ControlCommand::Status => "Status requested.",
                        ControlCommand::Diff => "Diff requested.",
                        ControlCommand::Send(_) => "Instruction queued for the next agent turn.",
                    };
                    if command_sender.send(command).is_err() {
                        break;
                    }
                    println!("LYA\n  {acknowledgement}");
                }
                Ok(None) => {}
                Err(error) => eprintln!("{error}"),
            }
        }
    });
}

fn spawn_interrupt_handler(sender: lya::orchestrator::control::ControlSender) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!(
                "Stop requested. Finishing the current safe shutdown...\nPress Ctrl+C again to force termination."
            );
            request_graceful_stop(&sender);
            if tokio::signal::ctrl_c().await.is_ok() {
                match interrupt_action(2) {
                    InterruptAction::ForceTerminate => std::process::exit(130),
                    InterruptAction::GracefulStop => unreachable!(),
                }
            }
        }
    });
}

fn interactive_enabled(
    output: &RunOutput,
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> bool {
    matches!(output, RunOutput::Human(_)) && stdin_is_terminal && stdout_is_terminal
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InterruptAction {
    GracefulStop,
    ForceTerminate,
}

fn interrupt_action(press_count: u8) -> InterruptAction {
    if press_count <= 1 {
        InterruptAction::GracefulStop
    } else {
        InterruptAction::ForceTerminate
    }
}

fn request_graceful_stop(sender: &lya::orchestrator::control::ControlSender) {
    sender.request_stop();
}

#[derive(Debug)]
struct RunOptions {
    project_path: std::path::PathBuf,
    browser: bool,
    max_iterations: u32,
    max_jobs: u32,
    publish: bool,
    output: RunOutput,
    task: String,
}

#[derive(Debug)]
enum RunOutput {
    Human(HumanRenderMode),
    Json,
}

#[derive(Debug)]
struct ResumeOptions {
    job_id: Option<String>,
    output: RunOutput,
}

fn parse_resume_arguments(arguments: &[String]) -> Result<ResumeOptions, String> {
    let mut job_id = None;
    let mut verbose = false;
    let mut json = false;
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--job" => {
                position += 1;
                job_id = Some(
                    arguments
                        .get(position)
                        .cloned()
                        .ok_or_else(|| "--job requires a job ID".to_owned())?,
                );
            }
            "--verbose" => verbose = true,
            "--json" => json = true,
            argument => return Err(format!("unknown resume option: {argument}")),
        }
        position += 1;
    }
    if verbose && json {
        return Err("--verbose cannot be combined with --json".to_owned());
    }
    Ok(ResumeOptions {
        job_id,
        output: if json {
            RunOutput::Json
        } else if verbose {
            RunOutput::Human(HumanRenderMode::Verbose)
        } else {
            RunOutput::Human(HumanRenderMode::Normal)
        },
    })
}

fn parse_run_arguments(arguments: &[String]) -> Result<RunOptions, String> {
    let mut project_path = env::current_dir().map_err(|error| error.to_string())?;
    let mut browser = false;
    let mut max_iterations = lya::orchestrator::job::DEFAULT_MAX_ITERATIONS;
    let mut max_jobs = lya::orchestrator::job::DEFAULT_MAX_JOBS;
    let mut publish = false;
    let mut verbose = false;
    let mut json = false;
    let mut task = Vec::new();
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--project" => {
                position += 1;
                project_path = arguments
                    .get(position)
                    .map(std::path::PathBuf::from)
                    .ok_or_else(|| "--project requires a path".to_owned())?;
            }
            "--browser" => browser = true,
            "--publish" => publish = true,
            "--verbose" => verbose = true,
            "--json" => json = true,
            "--max-iterations" => {
                position += 1;
                max_iterations = arguments
                    .get(position)
                    .ok_or_else(|| "--max-iterations requires a number".to_owned())?
                    .parse::<u32>()
                    .map_err(|_| "--max-iterations must be an unsigned integer".to_owned())?;
                if max_iterations == 0 {
                    return Err("--max-iterations must be greater than zero".to_owned());
                }
            }
            "--max-jobs" => {
                position += 1;
                max_jobs = arguments
                    .get(position)
                    .ok_or_else(|| "--max-jobs requires a number".to_owned())?
                    .parse::<u32>()
                    .map_err(|_| "--max-jobs must be an unsigned integer".to_owned())?;
                if max_jobs == 0 {
                    return Err("--max-jobs must be greater than zero".to_owned());
                }
            }
            argument if argument.starts_with("--") => {
                return Err(format!("unknown run option: {argument}"));
            }
            argument => task.push(argument.to_owned()),
        }
        position += 1;
    }

    if task.is_empty() {
        return Err("a task is required".to_owned());
    }
    if verbose && json {
        return Err("--verbose cannot be combined with --json".to_owned());
    }
    let project_path = project_path
        .canonicalize()
        .map_err(|error| format!("could not resolve project path: {error}"))?;
    Ok(RunOptions {
        project_path,
        browser,
        max_iterations,
        max_jobs,
        publish,
        output: if json {
            RunOutput::Json
        } else if verbose {
            RunOutput::Human(HumanRenderMode::Verbose)
        } else {
            RunOutput::Human(HumanRenderMode::Normal)
        },
        task: task.join(" "),
    })
}

fn project_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("current-project")
        .to_owned()
}

fn run_doctor() -> ExitCode {
    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!(
                "Lya doctor\n\nLYA_HOME       ERROR  {error}\n\nNot ready for orchestration."
            );
            return ExitCode::FAILURE;
        }
    };
    let report = DoctorReport::inspect(&home);
    println!("{}", report.render());

    if report.is_ready() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutorSession, InterruptAction, RunOutput, interactive_enabled, interrupt_action,
        parse_executor_arguments, parse_resume_arguments, parse_run_arguments,
        request_graceful_stop, start_control,
    };
    use lya::orchestrator::control::{ControlCommand, ControlReceiver};

    #[test]
    fn parses_executor_options_and_preserves_prompt_words() {
        let arguments = vec![
            "--resume".to_owned(),
            "session-name".to_owned(),
            "--browser".to_owned(),
            "--timeout-seconds".to_owned(),
            "900".to_owned(),
            "--max-turns".to_owned(),
            "4".to_owned(),
            "inspect".to_owned(),
            "files with spaces".to_owned(),
        ];

        let (request, max_turns) = parse_executor_arguments(&arguments)
            .expect("arguments should parse from the current project");

        assert_eq!(request.prompt, "inspect files with spaces");
        assert_eq!(
            request.session,
            ExecutorSession::Resume("session-name".to_owned())
        );
        assert!(request.browser);
        assert_eq!(request.timeout.map(|timeout| timeout.as_secs()), Some(900));
        assert_eq!(max_turns, Some(4));
    }

    #[test]
    fn parses_run_options_and_preserves_task_words() {
        let arguments = vec![
            "--browser".to_owned(),
            "--max-iterations".to_owned(),
            "5".to_owned(),
            "Fix".to_owned(),
            "the regression".to_owned(),
        ];

        let options = parse_run_arguments(&arguments)
            .expect("arguments should parse from the current project");

        assert!(options.browser);
        assert_eq!(options.max_iterations, 5);
        assert_eq!(options.max_jobs, lya::orchestrator::job::DEFAULT_MAX_JOBS);
        assert!(!options.publish);
        assert_eq!(options.task, "Fix the regression");
        assert!(matches!(options.output, RunOutput::Human(_)));
    }

    #[test]
    fn parses_explicit_publish_and_max_jobs_options() {
        let arguments = vec![
            "--publish".to_owned(),
            "--max-jobs".to_owned(),
            "3".to_owned(),
            "Publish".to_owned(),
            "this".to_owned(),
        ];

        let options = parse_run_arguments(&arguments)
            .expect("arguments should parse from the current project");

        assert!(!options.browser);
        assert_eq!(
            options.max_iterations,
            lya::orchestrator::job::DEFAULT_MAX_ITERATIONS
        );
        assert_eq!(options.max_jobs, 3);
        assert!(options.publish);
        assert_eq!(options.task, "Publish this");
    }

    #[test]
    fn parses_json_mode_and_rejects_conflicting_verbose_mode() {
        let options = parse_run_arguments(&["--json".to_owned(), "Inspect".to_owned()])
            .expect("JSON mode should parse");
        assert!(matches!(options.output, RunOutput::Json));
        let error = parse_run_arguments(&[
            "--verbose".to_owned(),
            "--json".to_owned(),
            "Inspect".to_owned(),
        ])
        .expect_err("conflicting modes should be rejected");
        assert!(error.contains("cannot be combined"));
    }

    #[test]
    fn json_mode_never_enables_the_interactive_stdin_reader() {
        assert!(!interactive_enabled(&RunOutput::Json, true, true));
        assert!(!interactive_enabled(
            &RunOutput::Human(lya::orchestrator::events::HumanRenderMode::Normal),
            false,
            true
        ));
        assert!(interactive_enabled(
            &RunOutput::Human(lya::orchestrator::events::HumanRenderMode::Normal),
            true,
            true
        ));
    }

    #[test]
    fn parses_resume_options() {
        let options = parse_resume_arguments(&["--job".to_owned(), "job-7".to_owned()])
            .expect("resume options should parse");
        assert_eq!(options.job_id.as_deref(), Some("job-7"));
        assert!(matches!(options.output, RunOutput::Human(_)));

        let options =
            parse_resume_arguments(&["--json".to_owned()]).expect("JSON resume should parse");
        assert_eq!(options.job_id, None);
        assert!(matches!(options.output, RunOutput::Json));

        assert!(parse_resume_arguments(&["--job".to_owned()]).is_err());
        assert!(parse_resume_arguments(&["--nope".to_owned()]).is_err());
        assert!(parse_resume_arguments(&["--verbose".to_owned(), "--json".to_owned()]).is_err());
    }

    #[tokio::test]
    async fn graceful_interrupt_handling_is_armed_even_without_an_interactive_terminal() {
        for output in [
            RunOutput::Json,
            RunOutput::Human(lya::orchestrator::events::HumanRenderMode::Normal),
        ] {
            let receiver = start_control(&output);

            // The interrupt handler owns a live sender in every mode. A closed channel would end
            // a paused job immediately instead of waiting for the graceful stop.
            let closed =
                tokio::time::timeout(std::time::Duration::from_millis(50), receiver.next()).await;

            assert!(
                closed.is_err(),
                "the control channel must stay open so Ctrl+C can request a graceful stop"
            );
        }
    }

    #[tokio::test]
    async fn first_interrupt_maps_to_graceful_stop_through_control_channel() {
        let (sender, receiver) = ControlReceiver::new();
        request_graceful_stop(&sender);

        assert_eq!(interrupt_action(1), InterruptAction::GracefulStop);
        assert_eq!(receiver.drain().await, vec![ControlCommand::Stop]);
        assert_eq!(interrupt_action(2), InterruptAction::ForceTerminate);
    }
}
