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
        job::{AutonomousOrchestrator, NewJob, new_job_id},
        publisher::{GitPublishConfig, GitPublisher},
        state::JobStatus,
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
    let private_context = match load_required_private_context(&home) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("Could not prepare autonomous job: {error}");
            return ExitCode::FAILURE;
        }
    };
    let job_id = new_job_id();
    let project = Project {
        name: project_name(&options.project_path),
        path: options.project_path,
    };

    let request = NewJob {
        job_id: job_id.clone(),
        project,
        task: options.task,
        private_context,
    };
    let control = start_interactive_control(&options.output);
    let event_sink = match options.output {
        RunOutput::Json => CompositeEventSink::new(vec![
            Box::new(JsonlEventSink::for_job(home.path(), &job_id)),
            Box::new(JsonEventSink::new(io::stdout())),
        ]),
        RunOutput::Human(mode) => CompositeEventSink::new(vec![
            Box::new(JsonlEventSink::for_job(home.path(), &job_id)),
            Box::new(HumanEventSink::stdout(mode)),
        ]),
    };
    let result = if options.publish {
        let configuration = match GitPublishConfig::from_environment() {
            Ok(configuration) => configuration,
            Err(error) => {
                eprintln!("Could not configure Git publication: {error}");
                return ExitCode::FAILURE;
            }
        };
        let orchestrator = AutonomousOrchestrator::new(
            CodexCliSupervisor::new_for_job(home.path(), &job_id),
            ClaudeCliExecutor::new(),
            SystemProcessRunner,
            lya::orchestrator::state::StateStore::new(&home),
        )
        .with_max_iterations(options.max_iterations)
        .with_max_jobs(options.max_jobs)
        .with_browser(options.browser)
        .with_publisher(GitPublisher::new(configuration))
        .with_event_sink(event_sink);
        match control {
            Some(control) => {
                orchestrator
                    .with_control_receiver(control)
                    .run_sequential(request)
                    .await
            }
            None => orchestrator.run_sequential(request).await,
        }
    } else {
        let orchestrator = AutonomousOrchestrator::new(
            CodexCliSupervisor::new_for_job(home.path(), &job_id),
            ClaudeCliExecutor::new(),
            SystemProcessRunner,
            lya::orchestrator::state::StateStore::new(&home),
        )
        .with_max_iterations(options.max_iterations)
        .with_max_jobs(options.max_jobs)
        .with_browser(options.browser)
        .with_event_sink(event_sink);
        match control {
            Some(control) => {
                orchestrator
                    .with_control_receiver(control)
                    .run_sequential(request)
                    .await
            }
            None => orchestrator.run_sequential(request).await,
        }
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

fn start_interactive_control(output: &RunOutput) -> Option<ControlReceiver> {
    if !interactive_enabled(
        output,
        io::stdin().is_terminal(),
        io::stdout().is_terminal(),
    ) {
        return None;
    }
    let (sender, receiver) = ControlReceiver::new();
    let command_sender = sender.clone();
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
    Some(receiver)
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
        parse_executor_arguments, parse_run_arguments, request_graceful_stop,
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

    #[tokio::test]
    async fn first_interrupt_maps_to_graceful_stop_through_control_channel() {
        let (sender, receiver) = ControlReceiver::new();
        request_graceful_stop(&sender);

        assert_eq!(interrupt_action(1), InterruptAction::GracefulStop);
        assert_eq!(receiver.drain().await, vec![ControlCommand::Stop]);
        assert_eq!(interrupt_action(2), InterruptAction::ForceTerminate);
    }
}
