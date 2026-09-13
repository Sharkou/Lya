use std::{
    env,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    pin::Pin,
    process::ExitCode,
    sync::Arc,
};

use tokio::io::{AsyncBufReadExt, BufReader};

use lya::{
    agent::Agent,
    daemon::{
        attach::JobEventBroadcaster,
        client::{
            ClientError, DaemonClient, describe, identify, wait_until_ready, wait_until_released,
        },
        events::{
            CompositeDaemonSink, DaemonEventSink, DaemonLogSink, DurableDaemonSink,
            HumanDaemonSink, JsonDaemonSink,
        },
        protocol::{
            ControlRequest, DaemonRequest, DaemonResponse, DaemonStatus, GitOptions, JobSummary,
            RunOptions as SubmitRunOptions, SubmitJob, SubmitOutcome,
        },
        runner::DaemonJobDriver,
        server::{Daemon, DaemonConfig},
        transport::DaemonEndpoint,
    },
    llm::ollama::OllamaClient,
    orchestrator::{
        batch::parse_job_file,
        control::{ControlCommand, ControlReceiver, parse_control_command},
        doctor::DoctorReport,
        events::{
            CompositeEventSink, EventSink, HumanEventSink, HumanRenderMode, JsonEventSink,
            JsonlEventSink,
        },
        executor::{ClaudeCliExecutor, Executor, ExecutorRequest, ExecutorSession},
        home::LyaHome,
        inventory::JobInventory,
        job::{AutonomousOrchestrator, NewJob, OrchestrationError, RunResult, new_job_id},
        lock::JobLock,
        publisher::{GitPublishConfig, GitPublisher, Publisher},
        repository_lock::{RepositoryIdentity, RepositoryLock},
        resume::{ResumeRejection, resumable_jobs, select_job},
        scheduler::{
            DEFAULT_MAX_CONCURRENT, HumanSchedulerSink, JobAssignment, JobDriver, JobOutcome,
            JsonSchedulerSink, ScheduledRequest, Scheduler, SchedulerEventSink, queued_jobs,
        },
        state::{JobState, JobStatus, RunConfiguration, StateStore, current_unix_seconds},
        supervisor::{
            CodexCliSupervisor, Project, Supervisor, SupervisorRequest,
            load_required_private_context,
        },
    },
    process::SystemProcessRunner,
    runtime::Runtime,
};

/// The version `lya --version` reports.
///
/// Taken from the package metadata at compile time, so it cannot drift from `Cargo.toml` and a
/// release tag check against `Cargo.toml` also checks this.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// One screen of orientation, deliberately not a manual.
///
/// Each command already prints its own usage line when it is given something invalid, and the real
/// documentation lives in `docs/`. Repeating either here would give three places to keep in sync.
const HELP: &str = "Lya — local-first AI agent and autonomous development orchestrator

Usage:
  lya <command> [options]
  lya <prompt>                    one-shot local agent run (needs OLLAMA_MODEL)

Commands:
  doctor                          check LYA_HOME, context.md, git, codex, claude
  run <task>                      drive one autonomous job in this terminal
  resume                          continue a persisted job
  jobs                            list persisted jobs (read-only)
  scheduler [<job-file>]          drive several repositories at once
  daemon start|run|status|stop    manage the local background daemon
  submit <task>                   hand work to a running daemon
  attach <job-id>                 watch one daemon-owned job live
  control <job-id> <command>      pause/resume/stop/status/diff/send, per job
  supervisor <task>               one Supervisor decision, for diagnostics
  executor <prompt>               one Executor invocation, for diagnostics

Options:
  -h, --help                      print this help
  -V, --version                   print the version

Common job options (run, scheduler, submit):
  --project <path>                the Git working tree to drive (default: .)
  --max-iterations <count>        Supervisor reviews per job (default: 10)
  --max-jobs <count>              jobs per sequential chain (default: 10)
  --browser                       pass the browser capability to the Executor
  --publish                       enable guarded Git commit and push
  --verbose | --json              human or machine-readable output

Environment:
  LYA_HOME                        private state directory (default: ~/.lya)

Pass an invalid option to any command to see that command's own usage line.
Documentation: https://github.com/Sharkou/Lya/blob/main/docs/README.md";

/// Whether the invocation asks for the help text.
///
/// Only the first argument is inspected, exactly like command dispatch: `lya run --help` stays a
/// `lya run` invocation, and `run`'s own parser reports the unknown option with `run`'s usage.
fn help_requested(arguments: &[String]) -> bool {
    arguments
        .first()
        .is_some_and(|argument| matches!(argument.as_str(), "-h" | "--help"))
}

/// Whether the invocation asks for the version, under the same first-argument rule as help.
fn version_requested(arguments: &[String]) -> bool {
    arguments
        .first()
        .is_some_and(|argument| matches!(argument.as_str(), "-V" | "--version"))
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    // Before dispatch, so neither can be shadowed by a command name, and before anything reads the
    // environment: `--help` and `--version` must answer on a machine that is not configured yet.
    if help_requested(&arguments) {
        println!("{HELP}");
        return ExitCode::SUCCESS;
    }
    if version_requested(&arguments) {
        println!("lya {VERSION}");
        return ExitCode::SUCCESS;
    }
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
    if arguments.first().is_some_and(|argument| argument == "jobs") {
        return list_jobs(&arguments[1..]);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "scheduler")
    {
        return run_scheduler(&arguments[1..]).await;
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "daemon")
    {
        return run_daemon_command(&arguments[1..]).await;
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "submit")
    {
        return submit_to_daemon(&arguments[1..]).await;
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "attach")
    {
        return attach_to_job(&arguments[1..]).await;
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "control")
    {
        return control_job(&arguments[1..]).await;
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

/// Whether a terminal job status counts as a successful outcome for the process exit code.
///
/// One definition, owned by [`JobStatus`], so `lya run`, `lya scheduler` and the daemon can never
/// disagree about the same status.
fn job_status_succeeded(status: &JobStatus) -> bool {
    status.is_successful_outcome()
}

#[allow(clippy::too_many_arguments)]
async fn drive_job(
    home: &LyaHome,
    store: &StateStore,
    job_id: &str,
    project_path: &Path,
    output: RunOutput,
    publish: Option<GitPublishConfig>,
    browser: bool,
    max_iterations: u32,
    max_jobs: u32,
    action: JobAction,
) -> ExitCode {
    // Repository first, job second — the same claim order the scheduler uses, so the two layers can
    // never deadlock against each other. A single `lya run` takes the repository claim too: without
    // it a scheduled job and a manual run could drive one working tree at the same time.
    let _repository_lock = match RepositoryIdentity::resolve(project_path)
        .and_then(|identity| RepositoryLock::acquire(store, &identity))
    {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
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
            if job_status_succeeded(&job.status) {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
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
    let project_path = options.project_path.clone();
    let request = NewJob::new(
        job_id.clone(),
        Project {
            name: project_name(&options.project_path),
            path: options.project_path,
        },
        options.task,
        private_context,
    );

    drive_job(
        &home,
        &store,
        &job_id,
        &project_path,
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
    let project_path = job.project_path.clone();
    let browser = job.run.browser;
    let max_iterations = job.run.max_iterations;
    let max_jobs = job.run.max_jobs;

    drive_job(
        &home,
        &store,
        &job_id,
        &project_path,
        options.output,
        publish,
        browser,
        max_iterations,
        max_jobs,
        JobAction::Resume(Box::new(job), private_context),
    )
    .await
}

/// Drives one scheduled job with the normal autonomous orchestrator.
///
/// The scheduler already holds this job's repository claim and job lock, so nothing here claims
/// them again; sequential children still take their own job locks inside the orchestrator.
struct SchedulerJobDriver {
    home: LyaHome,
    private_context: String,
    stdout: SharedJobSink,
}

/// One stdout writer shared by every concurrently driven job, so interleaved output stays
/// well-formed. Each job keeps its own `events.jsonl` in addition.
#[derive(Clone)]
enum SharedJobSink {
    Human(Arc<HumanEventSink<io::Stdout>>),
    Json(Arc<JsonEventSink<io::Stdout>>),
}

impl SharedJobSink {
    fn boxed(&self) -> Box<dyn EventSink> {
        match self {
            Self::Human(sink) => Box::new(Arc::clone(sink)),
            Self::Json(sink) => Box::new(Arc::clone(sink)),
        }
    }
}

impl JobDriver for SchedulerJobDriver {
    fn drive<'a>(
        &'a self,
        assignment: JobAssignment,
    ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
        Box::pin(async move {
            let sink = CompositeEventSink::new(vec![
                Box::new(JsonlEventSink::for_job(
                    self.home.path(),
                    &assignment.job_id,
                )),
                self.stdout.boxed(),
            ]);
            // The limits come from the job itself, as the scheduler persisted them, so one source
            // of truth survives a crash and a restart.
            let base = AutonomousOrchestrator::new(
                CodexCliSupervisor::new_for_job(self.home.path(), &assignment.job_id),
                ClaudeCliExecutor::new(),
                SystemProcessRunner,
                StateStore::new(&self.home),
            )
            .with_max_iterations(assignment.run.max_iterations)
            .with_max_jobs(assignment.run.max_jobs)
            .with_browser(assignment.run.browser)
            .with_event_sink(sink)
            .with_control_receiver(assignment.control);
            let request = NewJob::new(
                assignment.job_id,
                assignment.project,
                assignment.task,
                self.private_context.clone(),
            );

            let result = match assignment
                .run
                .publish
                .then_some(assignment.run.git)
                .flatten()
            {
                Some(configuration) => {
                    base.with_publisher(GitPublisher::new(configuration.clone()))
                        .with_publish_configuration(configuration)
                        .run_sequential(request)
                        .await
                }
                None => base.run_sequential(request).await,
            };

            match result {
                Ok(run) => {
                    let job = run
                        .jobs
                        .last()
                        .expect("a run always contains its first job");
                    JobOutcome::Finished {
                        status: job.status.label().to_owned(),
                        jobs: run.jobs.len(),
                        succeeded: job_status_succeeded(&job.status),
                    }
                }
                Err(error) => JobOutcome::Failed {
                    error: error.to_string(),
                },
            }
        })
    }
}

/// Runs several project/task requests through the bounded scheduler.
///
/// This mode is deliberately non-interactive: `/pause`, `/send` and the other line commands act on
/// one unambiguous job, and multiplexing them across concurrent jobs needs an interaction model
/// that M9 does not invent. `lya run` keeps the full interactive control it has today. Graceful
/// termination still works here: the first Ctrl+C stops the scheduler and every active job.
async fn run_scheduler(arguments: &[String]) -> ExitCode {
    let options = match parse_scheduler_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!(
                "{error}\nUsage: lya scheduler [<job-file>] [--resume-queued] [--max-concurrent <n>] [--browser] [--max-iterations <count>] [--max-jobs <count>] [--publish] [--verbose | --json]"
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
            eprintln!("Could not prepare scheduled jobs: {error}");
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

    let mut requests = Vec::new();
    if options.resume_queued {
        match queued_jobs(&store) {
            // Work a previous scheduler accepted and never started keeps its own job identity; it
            // adopts the options of the invocation that picks it up.
            Ok(jobs) => requests.extend(jobs.iter().map(ScheduledRequest::for_persisted_job)),
            Err(error) => {
                eprintln!("Could not read queued jobs: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    if let Some(path) = &options.job_file {
        let content = match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) => {
                eprintln!("Could not read the job file {}: {error}", path.display());
                return ExitCode::FAILURE;
            }
        };
        let base = path.parent().unwrap_or(Path::new(".")).to_owned();
        match parse_job_file(&content, &base) {
            Ok(parsed) => requests.extend(parsed),
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        }
    }
    if requests.is_empty() {
        eprintln!("Nothing to schedule.");
        return ExitCode::SUCCESS;
    }

    let (stdout, scheduler_sink): (SharedJobSink, Arc<dyn SchedulerEventSink>) =
        match options.output {
            RunOutput::Json => (
                SharedJobSink::Json(Arc::new(JsonEventSink::new(io::stdout()))),
                Arc::new(JsonSchedulerSink::new(io::stdout())),
            ),
            RunOutput::Human(mode) => (
                SharedJobSink::Human(Arc::new(
                    HumanEventSink::stdout(mode).with_job_context(true),
                )),
                Arc::new(HumanSchedulerSink::stdout()),
            ),
        };
    let driver = SchedulerJobDriver {
        home: LyaHome::from_path(home.path()),
        private_context,
        stdout,
    };
    let scheduler = Scheduler::new(driver, StateStore::new(&home))
        .with_max_concurrent(options.max_concurrent)
        .with_event_sink(scheduler_sink)
        .with_run_configuration(RunConfiguration {
            max_iterations: options.max_iterations,
            max_jobs: options.max_jobs,
            browser: options.browser,
            publish: publish.is_some(),
            git: publish,
        });
    let control = scheduler.control();
    spawn_interrupt_handler_with(move || control.request_stop());

    match scheduler.run(requests).await {
        Ok(report) => {
            // JSON mode already carries the summary as a `SCHEDULER_FINISHED` event, so stdout
            // stays free of prose.
            if matches!(options.output, RunOutput::Human(_)) {
                println!("{}", report.render());
            }
            if report.is_success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("Scheduler failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Lists persisted jobs.
///
/// Strictly read-only: no legacy migration, no job lock, no provider call and no Git command. It
/// deliberately does not go through [`prepare_home`], which writes.
fn list_jobs(arguments: &[String]) -> ExitCode {
    let options = match parse_jobs_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\nUsage: lya jobs [--resumable] [--json]");
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
    let store = StateStore::new(&home);
    let inventory = match JobInventory::collect(&store) {
        Ok(inventory) => inventory,
        Err(error) => {
            eprintln!("Could not read persisted job state: {error}");
            return ExitCode::FAILURE;
        }
    };
    let inventory = if options.resumable_only {
        inventory.only_resumable()
    } else {
        inventory
    };

    if options.json {
        match serde_json::to_string_pretty(&inventory.to_json()) {
            Ok(rendered) => println!("{rendered}"),
            Err(error) => {
                eprintln!("Could not render the job listing: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        println!("{}", inventory.render(current_unix_seconds()));
    }

    if inventory.unreadable.is_empty() {
        ExitCode::SUCCESS
    } else {
        // Corruption is never reported only in passing: it also fails the command.
        eprintln!(
            "{} persisted job(s) could not be read; they were left untouched.",
            inventory.unreadable.len()
        );
        ExitCode::FAILURE
    }
}

fn parse_jobs_arguments(arguments: &[String]) -> Result<JobsOptions, String> {
    let mut options = JobsOptions::default();
    for argument in arguments {
        match argument.as_str() {
            "--resumable" => options.resumable_only = true,
            "--json" => options.json = true,
            argument => return Err(format!("unknown jobs option: {argument}")),
        }
    }
    Ok(options)
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
    spawn_interrupt_handler_with(move || request_graceful_stop(&sender));
}

/// Arms graceful termination for whatever owns the active work.
///
/// A single `lya run` routes it into that job's control channel; the scheduler routes it into every
/// active job at once and stops launching new ones. The second interrupt keeps its existing
/// force-exit meaning in both cases.
fn spawn_interrupt_handler_with(request_stop: impl Fn() + Send + 'static) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!(
                "Stop requested. Finishing the current safe shutdown...\nPress Ctrl+C again to force termination."
            );
            request_stop();
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

#[derive(Debug, Default)]
struct JobsOptions {
    resumable_only: bool,
    json: bool,
}

#[derive(Debug)]
struct SchedulerOptions {
    job_file: Option<PathBuf>,
    resume_queued: bool,
    max_concurrent: usize,
    browser: bool,
    max_iterations: u32,
    max_jobs: u32,
    publish: bool,
    output: RunOutput,
}

fn parse_scheduler_arguments(arguments: &[String]) -> Result<SchedulerOptions, String> {
    let mut job_file = None;
    let mut resume_queued = false;
    let mut max_concurrent = DEFAULT_MAX_CONCURRENT;
    let mut browser = false;
    let mut max_iterations = lya::orchestrator::job::DEFAULT_MAX_ITERATIONS;
    let mut max_jobs = lya::orchestrator::job::DEFAULT_MAX_JOBS;
    let mut publish = false;
    let mut verbose = false;
    let mut json = false;
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--resume-queued" => resume_queued = true,
            "--browser" => browser = true,
            "--publish" => publish = true,
            "--verbose" => verbose = true,
            "--json" => json = true,
            "--max-concurrent" => {
                position += 1;
                max_concurrent = arguments
                    .get(position)
                    .ok_or_else(|| "--max-concurrent requires a number".to_owned())?
                    .parse::<usize>()
                    .map_err(|_| "--max-concurrent must be an unsigned integer".to_owned())?;
                if max_concurrent == 0 {
                    return Err("--max-concurrent must be greater than zero".to_owned());
                }
            }
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
                return Err(format!("unknown scheduler option: {argument}"));
            }
            argument if job_file.is_none() => job_file = Some(PathBuf::from(argument)),
            argument => return Err(format!("only one job file is accepted: {argument}")),
        }
        position += 1;
    }

    if verbose && json {
        return Err("--verbose cannot be combined with --json".to_owned());
    }
    if job_file.is_none() && !resume_queued {
        return Err("a job file or --resume-queued is required".to_owned());
    }
    Ok(SchedulerOptions {
        job_file,
        resume_queued,
        max_concurrent,
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
    })
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

// ------------------------------------------------------------------------------------------------
// Daemon mode
//
// Everything below is a *client* of the daemon, except `lya daemon run`, which is the daemon itself.
// Clients hold no orchestration logic: they encode a request, render what comes back and choose an
// exit code. Rendering never leaks into the protocol and the protocol never leaks into rendering.
//
// Existing commands are untouched. `lya run` and `lya scheduler` still run in the foreground and are
// never silently redirected to a daemon: a foreground run is a documented, deliberately different
// thing — it is attached to the terminal and it dies with it. `lya submit` is how work reaches the
// daemon, and it accepts the same job files `lya scheduler` does.
// ------------------------------------------------------------------------------------------------

async fn run_daemon_command(arguments: &[String]) -> ExitCode {
    match arguments.first().map(String::as_str) {
        Some("run") => run_daemon_foreground(&arguments[1..]).await,
        Some("start") => start_daemon(&arguments[1..]).await,
        Some("stop") => stop_daemon(&arguments[1..]).await,
        Some("status") => report_daemon_status(&arguments[1..]).await,
        Some(unknown) => {
            eprintln!("unknown daemon command: {unknown}\n{DAEMON_USAGE}");
            ExitCode::FAILURE
        }
        None => {
            eprintln!("{DAEMON_USAGE}");
            ExitCode::FAILURE
        }
    }
}

const DAEMON_USAGE: &str = "Usage:\n  lya daemon start [--max-concurrent <n>] [--resume-interrupted] [--no-recover-queued]\n  lya daemon run   [--max-concurrent <n>] [--resume-interrupted] [--no-recover-queued] [--verbose | --json]\n  lya daemon status [--json]\n  lya daemon stop";

/// Run the daemon in this process.
///
/// The foreground mode. `lya daemon start` uses it too: it starts exactly this, detached.
async fn run_daemon_foreground(arguments: &[String]) -> ExitCode {
    let options = match parse_daemon_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\n{DAEMON_USAGE}");
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
    // The daemon owns this home from now on, so the one-off migration happens here, once, before
    // any job is read or written.
    if let Err(error) = prepare_home(&home) {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }
    let private_context = match load_required_private_context(&home) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("Could not prepare the daemon: {error}");
            return ExitCode::FAILURE;
        }
    };

    // A foreground daemon narrates: daemon lines to standard error, and the job events every
    // driven job emits to standard output, prefixed with the job they belong to. A detached daemon
    // gets the same lines in its log.
    let (daemon_sink, scheduler_sink, job_events): (
        Box<dyn DaemonEventSink>,
        Arc<dyn SchedulerEventSink>,
        Arc<dyn EventSink>,
    ) = match options.output {
        RunOutput::Json => (
            Box::new(JsonDaemonSink::new(io::stderr())),
            Arc::new(JsonSchedulerSink::new(io::stdout())),
            Arc::new(JsonEventSink::new(io::stdout())),
        ),
        RunOutput::Human(mode) => (
            Box::new(HumanDaemonSink::stderr()),
            Arc::new(HumanSchedulerSink::stdout()),
            Arc::new(HumanEventSink::stdout(mode).with_job_context(true)),
        ),
    };
    // A daemon narrating to a terminal shows everything, including which clients came and went. The
    // same narration captured into a file — which is what a detached daemon's is — keeps only what
    // is worth keeping, so a daemon that runs for weeks does not fill its log with `lya daemon
    // status` traffic.
    let narration = if io::stderr().is_terminal() {
        daemon_sink
    } else {
        Box::new(DurableDaemonSink::new(daemon_sink))
    };
    // Durable daemon history always exists; the narration is in addition to it.
    let events = Arc::new(CompositeDaemonSink::new(vec![
        Box::new(DaemonLogSink::for_home(home.path())),
        narration,
    ]));

    let broadcaster = JobEventBroadcaster::new();
    let driver = DaemonJobDriver::new(
        LyaHome::from_path(home.path()),
        private_context,
        broadcaster.clone(),
    )
    .with_terminal_sink(job_events);
    let daemon = Daemon::new(LyaHome::from_path(home.path()), driver)
        .with_config(options.config)
        .with_event_sink(events)
        .with_scheduler_event_sink(scheduler_sink)
        .with_broadcaster(broadcaster);

    // A foreground daemon answers Ctrl+C the way every other Lya command does: the first interrupt
    // is a graceful shutdown, the second forces termination.
    let shutdown = daemon.shutdown_signal();
    spawn_interrupt_handler_with(move || shutdown.request("an interrupt was received"));

    match daemon.serve().await {
        Ok(outcome) => {
            if matches!(options.output, RunOutput::Human(_)) {
                println!("{}", outcome.report.render());
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Daemon failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Start a detached daemon and return the shell's prompt.
///
/// Starting twice cannot produce two daemons: the second one is refused by the claim on `LYA_HOME`
/// before it binds anything. This command checks first anyway, so the common case reports the daemon
/// that is already running instead of a failure from a child process nobody can see.
async fn start_daemon(arguments: &[String]) -> ExitCode {
    let options = match parse_daemon_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\n{DAEMON_USAGE}");
            return ExitCode::FAILURE;
        }
    };
    if options.explicit_output {
        eprintln!("--verbose and --json apply to lya daemon run, not to lya daemon start.");
        return ExitCode::FAILURE;
    }
    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!("Could not resolve Lya home: {error}");
            return ExitCode::FAILURE;
        }
    };
    match identify(&home).await {
        Ok(Some(identity)) => {
            println!(
                "A Lya daemon is already running for {} as process {} on {}.",
                home.path().display(),
                identity.process_id,
                identity.endpoint
            );
            return ExitCode::SUCCESS;
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!("Could not check for a running daemon: {error}");
            return ExitCode::FAILURE;
        }
    }

    let log_path = home.path().join("daemon").join("daemon.log");
    if let Err(error) = std::fs::create_dir_all(home.path().join("daemon")) {
        eprintln!("Could not prepare the daemon log directory: {error}");
        return ExitCode::FAILURE;
    }
    let log = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(file) => file,
        Err(error) => {
            eprintln!("Could not open {}: {error}", log_path.display());
            return ExitCode::FAILURE;
        }
    };
    let executable = match env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("Could not locate the Lya executable: {error}");
            return ExitCode::FAILURE;
        }
    };

    let mut command = std::process::Command::new(executable);
    command.arg("daemon").arg("run");
    command.args(options.forwarded());
    command.stdin(std::process::Stdio::null());
    let Ok(errors) = log.try_clone() else {
        eprintln!("Could not duplicate the daemon log handle.");
        return ExitCode::FAILURE;
    };
    command.stdout(log).stderr(errors);
    // The home the child must own is the one this command resolved, so a child started from a
    // different working directory or environment can never adopt a different home.
    command.env("LYA_HOME", home.path());
    detach(&mut command);

    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            eprintln!("Could not start the daemon: {error}");
            return ExitCode::FAILURE;
        }
    };

    match wait_until_ready(&home).await {
        Ok(Some(identity)) => {
            println!(
                "Lya daemon started as process {} on {}.\nLog: {}",
                identity.process_id,
                identity.endpoint,
                log_path.display()
            );
            ExitCode::SUCCESS
        }
        Ok(None) => {
            eprintln!(
                "The daemon did not become reachable (started as process {}). See {}.",
                child.id(),
                log_path.display()
            );
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!(
                "Could not reach the started daemon: {error}\nSee {}.",
                log_path.display()
            );
            ExitCode::FAILURE
        }
    }
}

/// Detach a child from this terminal, so closing the shell cannot take the daemon with it.
#[cfg(windows)]
fn detach(command: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;

    /// `DETACHED_PROCESS`: no console is inherited, so a closing console cannot signal the daemon.
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    /// `CREATE_NEW_PROCESS_GROUP`: a Ctrl+C in the starting console is not delivered to the daemon.
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

/// Detach a child from this terminal, so closing the shell cannot take the daemon with it.
#[cfg(unix)]
fn detach(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        // A new session means no controlling terminal: the daemon receives neither the shell's
        // interrupts nor its hangup.
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

/// Ask the daemon to shut down gracefully, and wait until it really has.
///
/// Reporting before the daemon is gone would be a lie a script could act on, and the endpoint is
/// not what says it is gone: a daemon stops serving the moment a shutdown begins and then keeps
/// running until every active job has shut down safely. The claim on `LYA_HOME` is polled instead,
/// because that is the thing the next `lya daemon start` has to be able to take.
async fn stop_daemon(arguments: &[String]) -> ExitCode {
    if !arguments.is_empty() {
        eprintln!("Usage: lya daemon stop");
        return ExitCode::FAILURE;
    }
    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!("Could not resolve Lya home: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut client = match DaemonClient::connect(&home).await {
        Ok(client) => client,
        Err(error) if error.is_not_running() => {
            println!("No Lya daemon is running for {}.", home.path().display());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match client.request(DaemonRequest::Shutdown).await {
        Ok(DaemonResponse::ShuttingDown { active_jobs }) => {
            println!(
                "Shutdown requested. {active_jobs} active job(s) are shutting down safely; no new work is accepted."
            );
        }
        Ok(other) => {
            eprintln!(
                "The daemon answered with an unexpected {}.",
                describe(&other)
            );
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    }
    client.close().await;

    match wait_until_released(&home).await {
        Ok(true) => {
            println!("Lya daemon stopped.");
            ExitCode::SUCCESS
        }
        Ok(false) => {
            eprintln!(
                "The daemon is still shutting down; active jobs are being stopped at a safe boundary."
            );
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!(
                "Could not confirm the daemon released {}: {error}",
                home.path().display()
            );
            ExitCode::FAILURE
        }
    }
}

/// Report whether a daemon is running, and what it is doing.
///
/// Exits with a failure when no daemon is running, so a script can test for one.
async fn report_daemon_status(arguments: &[String]) -> ExitCode {
    let json = match arguments {
        [] => false,
        [flag] if flag == "--json" => true,
        _ => {
            eprintln!("Usage: lya daemon status [--json]");
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
    let endpoint = DaemonEndpoint::for_home(&home);
    let mut client = match DaemonClient::connect(&home).await {
        Ok(client) => client,
        Err(error) if error.is_not_running() => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "running": false,
                        "lya_home": home.path().display().to_string(),
                        "endpoint": endpoint.address(),
                    })
                );
            } else {
                println!(
                    "Lya daemon\n\nSTATE          not running\nLYA_HOME       {}\nENDPOINT       {}\n\nStart one with: lya daemon start",
                    home.path().display(),
                    endpoint.address()
                );
            }
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let status = match client.request(DaemonRequest::Status).await {
        Ok(DaemonResponse::Status { status }) => *status,
        Ok(other) => {
            eprintln!(
                "The daemon answered with an unexpected {}.",
                describe(&other)
            );
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    client.close().await;

    if json {
        match serde_json::to_string_pretty(&status) {
            Ok(rendered) => println!("{rendered}"),
            Err(error) => {
                eprintln!("Could not render the daemon status: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        println!("{}", render_daemon_status(&home, &status));
    }
    ExitCode::SUCCESS
}

fn render_daemon_status(home: &LyaHome, status: &DaemonStatus) -> String {
    let mut lines = vec![
        "Lya daemon".to_owned(),
        String::new(),
        format!(
            "STATE          {}",
            if status.accepting_work {
                "running"
            } else {
                "shutting down"
            }
        ),
        format!("PROCESS        {}", status.identity.process_id),
        format!("LYA_HOME       {}", home.path().display()),
        format!("ENDPOINT       {}", status.identity.endpoint),
        format!("PROTOCOL       {}", status.identity.protocol_version),
        format!(
            "CONCURRENCY    at most {} repositories",
            status.max_concurrent
        ),
        format!(
            "CLIENTS        {} connected, {} attached",
            status.connected_clients, status.attached_clients
        ),
        format!(
            "RESUME         interrupted jobs are {}",
            if status.resume_interrupted {
                "resumed automatically"
            } else {
                "left parked"
            }
        ),
    ];
    let section = |title: &str, jobs: &[JobSummary], hint: Option<&str>| {
        let mut lines = vec![String::new()];
        if jobs.is_empty() {
            lines.push(format!("{title} (0)"));
            return lines;
        }
        lines.push(format!("{title} ({})", jobs.len()));
        for job in jobs {
            lines.push(format!(
                "  {}  {}  {}  iteration {}/{}  {}",
                job.job_id,
                job.project_name,
                job.status,
                job.iteration,
                job.max_iterations,
                job.task
            ));
        }
        if let Some(hint) = hint {
            lines.push(format!("  {hint}"));
        }
        lines
    };
    lines.extend(section(
        "ACTIVE",
        &status.active,
        Some("watch one with: lya attach <job-id>"),
    ));
    lines.extend(section("QUEUED", &status.queued, None));
    lines.extend(section(
        "RESUMABLE",
        &status.resumable,
        Some("continue one with: lya resume --job <job-id>"),
    ));
    lines.join("\n")
}

#[derive(Debug)]
struct DaemonOptions {
    config: DaemonConfig,
    output: RunOutput,
    /// Whether an output mode was asked for explicitly. `lya daemon start` refuses one, because the
    /// output it would describe goes to a log file nobody is watching.
    explicit_output: bool,
}

impl DaemonOptions {
    /// The arguments a detached `lya daemon run` must be started with to mean the same thing.
    fn forwarded(&self) -> Vec<String> {
        let mut arguments = vec![
            "--max-concurrent".to_owned(),
            self.config.max_concurrent.to_string(),
        ];
        if self.config.resume_interrupted {
            arguments.push("--resume-interrupted".to_owned());
        }
        if !self.config.recover_queued {
            arguments.push("--no-recover-queued".to_owned());
        }
        // A detached daemon writes to a log, so it writes machine-readable lines.
        arguments.push("--json".to_owned());
        arguments
    }
}

fn parse_daemon_arguments(arguments: &[String]) -> Result<DaemonOptions, String> {
    let mut config = DaemonConfig::default();
    let mut verbose = false;
    let mut json = false;
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--resume-interrupted" => config.resume_interrupted = true,
            "--no-recover-queued" => config.recover_queued = false,
            "--verbose" => verbose = true,
            "--json" => json = true,
            "--max-concurrent" => {
                position += 1;
                config.max_concurrent = arguments
                    .get(position)
                    .ok_or_else(|| "--max-concurrent requires a number".to_owned())?
                    .parse::<usize>()
                    .map_err(|_| "--max-concurrent must be an unsigned integer".to_owned())?;
                if config.max_concurrent == 0 {
                    return Err("--max-concurrent must be greater than zero".to_owned());
                }
            }
            argument => return Err(format!("unknown daemon option: {argument}")),
        }
        position += 1;
    }
    if verbose && json {
        return Err("--verbose cannot be combined with --json".to_owned());
    }
    Ok(DaemonOptions {
        config,
        output: if json {
            RunOutput::Json
        } else if verbose {
            RunOutput::Human(HumanRenderMode::Verbose)
        } else {
            RunOutput::Human(HumanRenderMode::Normal)
        },
        explicit_output: verbose || json,
    })
}

const SUBMIT_USAGE: &str = "Usage: lya submit [--project <path>] [--max-iterations <count>] [--max-jobs <count>] [--browser] [--publish] [--json] <task>\n   or: lya submit --file <job-file> [options]";

/// Hand work to the daemon.
///
/// The work becomes ordinary persisted Lya jobs and flows through the same scheduler `lya scheduler`
/// uses. This command owns no queue and no state of its own: it validates what it can validate
/// locally, hands the rest over and prints the job ids the daemon created.
async fn submit_to_daemon(arguments: &[String]) -> ExitCode {
    let options = match parse_submit_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\n{SUBMIT_USAGE}");
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
    // Publication configuration is read and validated here, in the shell that has the environment,
    // and travels with the job. The daemon never guesses at its own environment.
    let git = if options.run.publish {
        match GitPublishConfig::from_environment() {
            Ok(configuration) => Some(GitOptions::from(&configuration)),
            Err(error) => {
                eprintln!("Could not configure Git publication: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    let run = SubmitRunOptions { git, ..options.run };

    let jobs = match options.work {
        SubmitWork::Task { project_path, task } => vec![SubmitJob {
            project_path: project_path.display().to_string(),
            project_name: Some(project_name(&project_path)),
            task,
            run,
        }],
        // The job file is parsed by the same parser `lya scheduler` uses, so one grammar has one
        // implementation.
        SubmitWork::File(path) => {
            let content = match std::fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) => {
                    eprintln!("Could not read the job file {}: {error}", path.display());
                    return ExitCode::FAILURE;
                }
            };
            let base = path.parent().unwrap_or(Path::new(".")).to_owned();
            match parse_job_file(&content, &base) {
                Ok(requests) => requests
                    .into_iter()
                    .map(|request| SubmitJob {
                        project_path: request.project.path.display().to_string(),
                        project_name: Some(request.project.name),
                        task: request.task,
                        run: run.clone(),
                    })
                    .collect(),
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };

    let mut client = match DaemonClient::connect(&home).await {
        Ok(client) => client,
        Err(error) if error.is_not_running() => {
            eprintln!("{error}\nStart one with: lya daemon start");
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let outcomes = match client.request(DaemonRequest::Submit { jobs }).await {
        Ok(DaemonResponse::Submitted { jobs }) => jobs,
        Ok(other) => {
            eprintln!(
                "The daemon answered with an unexpected {}.",
                describe(&other)
            );
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    client.close().await;

    if options.json {
        match serde_json::to_string_pretty(&outcomes) {
            Ok(rendered) => println!("{rendered}"),
            Err(error) => {
                eprintln!("Could not render the submission: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        for outcome in &outcomes {
            match outcome {
                SubmitOutcome::Queued { job_id, repository } => {
                    println!("{job_id}  QUEUED  {repository}");
                }
                SubmitOutcome::Rejected { job_id, reason } => {
                    let job_id = if job_id.is_empty() { "-" } else { job_id };
                    eprintln!("{job_id}  NOT QUEUED  {reason}");
                }
            }
        }
        if outcomes
            .iter()
            .any(|outcome| matches!(outcome, SubmitOutcome::Queued { .. }))
        {
            println!("Watch one with: lya attach <job-id>");
        }
    }

    if outcomes
        .iter()
        .all(|outcome| matches!(outcome, SubmitOutcome::Queued { .. }))
    {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[derive(Debug)]
enum SubmitWork {
    Task { project_path: PathBuf, task: String },
    File(PathBuf),
}

#[derive(Debug)]
struct SubmitOptions {
    work: SubmitWork,
    run: SubmitRunOptions,
    json: bool,
}

fn parse_submit_arguments(arguments: &[String]) -> Result<SubmitOptions, String> {
    let mut project_path = None;
    let mut file = None;
    let mut run = SubmitRunOptions::default();
    let mut json = false;
    let mut task = Vec::new();
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--project" => {
                position += 1;
                project_path = Some(PathBuf::from(
                    arguments
                        .get(position)
                        .ok_or_else(|| "--project requires a path".to_owned())?,
                ));
            }
            "--file" => {
                position += 1;
                file = Some(PathBuf::from(
                    arguments
                        .get(position)
                        .ok_or_else(|| "--file requires a path".to_owned())?,
                ));
            }
            "--browser" => run.browser = true,
            "--publish" => run.publish = true,
            "--json" => json = true,
            "--max-iterations" => {
                position += 1;
                run.max_iterations = arguments
                    .get(position)
                    .ok_or_else(|| "--max-iterations requires a number".to_owned())?
                    .parse::<u32>()
                    .map_err(|_| "--max-iterations must be an unsigned integer".to_owned())?;
                if run.max_iterations == 0 {
                    return Err("--max-iterations must be greater than zero".to_owned());
                }
            }
            "--max-jobs" => {
                position += 1;
                run.max_jobs = arguments
                    .get(position)
                    .ok_or_else(|| "--max-jobs requires a number".to_owned())?
                    .parse::<u32>()
                    .map_err(|_| "--max-jobs must be an unsigned integer".to_owned())?;
                if run.max_jobs == 0 {
                    return Err("--max-jobs must be greater than zero".to_owned());
                }
            }
            argument if argument.starts_with("--") => {
                return Err(format!("unknown submit option: {argument}"));
            }
            argument => task.push(argument.to_owned()),
        }
        position += 1;
    }

    let work = match (file, task.is_empty()) {
        (Some(_), false) => {
            return Err("a job file and a task cannot be submitted in one invocation".to_owned());
        }
        (Some(file), true) => {
            if project_path.is_some() {
                return Err(
                    "--project does not apply to a job file, which names its own projects"
                        .to_owned(),
                );
            }
            SubmitWork::File(file)
        }
        (None, true) => return Err("a task or --file is required".to_owned()),
        (None, false) => {
            // Resolved in the shell that typed it: a relative path must never be interpreted
            // against the daemon's working directory.
            let path =
                project_path.unwrap_or(env::current_dir().map_err(|error| error.to_string())?);
            SubmitWork::Task {
                project_path: path
                    .canonicalize()
                    .map_err(|error| format!("could not resolve project path: {error}"))?,
                task: task.join(" "),
            }
        }
    };
    Ok(SubmitOptions { work, run, json })
}

/// Watch one daemon-owned job's events live.
///
/// Observational, and only observational. Ctrl+C detaches this viewer: the connection closes, the
/// daemon forgets it and the job carries on exactly as it was. Nothing here can pause, stop or steer
/// a job — that is what `lya control` is for.
async fn attach_to_job(arguments: &[String]) -> ExitCode {
    let options = match parse_attach_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\nUsage: lya attach <job-id> [--replay] [--verbose | --json]");
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
    let mut client = match DaemonClient::connect(&home).await {
        Ok(client) => client,
        Err(error) if error.is_not_running() => {
            eprintln!("{error}\nStart one with: lya daemon start");
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    // Whether the daemon opened a live stream or a replay of a finished job. It only decides what
    // this command *says*; the events are rendered identically either way.
    let live_stream = match client
        .request(DaemonRequest::Attach {
            job_id: options.job_id.clone(),
            replay: options.replay,
        })
        .await
    {
        Ok(DaemonResponse::Attached { job_id, live }) => {
            if matches!(options.output, RunOutput::Human(_)) {
                if live {
                    println!("Attached to {job_id}. Ctrl+C detaches; the job keeps running.");
                } else {
                    // A finished job is replayed rather than followed, so promising that it keeps
                    // running would be wrong in both halves of the sentence.
                    println!(
                        "Replaying {job_id}. It has finished, so no further events will arrive."
                    );
                }
            }
            live
        }
        Ok(other) => {
            eprintln!(
                "The daemon answered with an unexpected {}.",
                describe(&other)
            );
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    let sink: Box<dyn EventSink> = match options.output {
        RunOutput::Json => Box::new(JsonEventSink::new(io::stdout())),
        RunOutput::Human(mode) => Box::new(HumanEventSink::stdout(mode)),
    };
    let interrupted = loop {
        tokio::select! {
            // The interrupt is handled here rather than by the shared handler: detaching is a
            // client-side act and must not be confused with stopping a job.
            _ = tokio::signal::ctrl_c() => break true,
            message = client.next_message() => match message {
                Ok(Some(DaemonResponse::Event { event })) => {
                    if let Err(error) = sink.emit(&event) {
                        eprintln!("Could not render a job event: {error}");
                        return ExitCode::FAILURE;
                    }
                }
                Ok(Some(DaemonResponse::Detached { job_id, reason })) => {
                    if matches!(options.output, RunOutput::Human(_)) {
                        if live_stream {
                            println!("Detached from {job_id}: {reason}");
                        } else {
                            // The replay simply finished; nothing was detached from.
                            println!("{reason}");
                        }
                    }
                    break false;
                }
                Ok(Some(other)) => {
                    eprintln!(
                        "The daemon sent an unexpected {} on the event stream.",
                        describe(&other)
                    );
                    return ExitCode::FAILURE;
                }
                Ok(None) => break false,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::FAILURE;
                }
            },
        }
    };
    client.close().await;
    if interrupted && matches!(options.output, RunOutput::Human(_)) {
        if live_stream {
            println!(
                "Detached from {}. The job continues under the daemon.",
                options.job_id
            );
        } else {
            println!("Stopped replaying {}.", options.job_id);
        }
    }
    ExitCode::SUCCESS
}

#[derive(Debug)]
struct AttachOptions {
    job_id: String,
    replay: bool,
    output: RunOutput,
}

fn parse_attach_arguments(arguments: &[String]) -> Result<AttachOptions, String> {
    let mut job_id = None;
    let mut replay = false;
    let mut verbose = false;
    let mut json = false;

    for argument in arguments {
        match argument.as_str() {
            "--replay" => replay = true,
            "--verbose" => verbose = true,
            "--json" => json = true,
            argument if argument.starts_with("--") => {
                return Err(format!("unknown attach option: {argument}"));
            }
            argument if job_id.is_none() => job_id = Some(argument.to_owned()),
            argument => return Err(format!("only one job can be attached: {argument}")),
        }
    }
    if verbose && json {
        return Err("--verbose cannot be combined with --json".to_owned());
    }
    Ok(AttachOptions {
        job_id: job_id.ok_or_else(|| "a job ID is required".to_owned())?,
        replay,
        output: if json {
            RunOutput::Json
        } else if verbose {
            RunOutput::Human(HumanRenderMode::Verbose)
        } else {
            RunOutput::Human(HumanRenderMode::Normal)
        },
    })
}

const CONTROL_USAGE: &str =
    "Usage: lya control <job-id> <pause | resume | stop | status | diff | send <instruction>>";

/// Send one control command to one daemon-owned job.
///
/// These are the same commands an interactive `lya run` accepts, delivered through the same control
/// channel. Naming the job explicitly is what makes them unambiguous while several jobs run at once.
async fn control_job(arguments: &[String]) -> ExitCode {
    let (job_id, request) = match parse_control_arguments(arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("{error}\n{CONTROL_USAGE}");
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
    let mut client = match DaemonClient::connect(&home).await {
        Ok(client) => client,
        Err(error) if error.is_not_running() => {
            eprintln!("{error}\nStart one with: lya daemon start");
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let response = client
        .request(DaemonRequest::Control {
            job_id: job_id.clone(),
            command: request.clone(),
        })
        .await;
    client.close().await;

    match response {
        Ok(DaemonResponse::Controlled { job_id, command }) => {
            println!("{}", control_acknowledgement(&job_id, &command));
            ExitCode::SUCCESS
        }
        Ok(other) => {
            eprintln!(
                "The daemon answered with an unexpected {}.",
                describe(&other)
            );
            ExitCode::FAILURE
        }
        Err(ClientError::Daemon(error)) => {
            eprintln!("{}", error.message);
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// What a delivered control command means for the job, in one line.
///
/// `status` and `diff` answer into the job's own event stream rather than into this command's
/// output, because that is where the job reports: one job, one authoritative narration.
fn control_acknowledgement(job_id: &str, command: &str) -> String {
    match command {
        "PAUSE" => format!("Pause requested; {job_id} will pause at a safe boundary."),
        "RESUME" => format!("Resume requested for {job_id}."),
        "STOP" => format!("Stop requested; {job_id} is finishing a safe shutdown."),
        "STATUS" => {
            format!("Status requested; {job_id} reports it in its events (lya attach {job_id}).")
        }
        "DIFF" => {
            format!("Diff requested; {job_id} reports it in its events (lya attach {job_id}).")
        }
        "SEND" => format!("Instruction queued for {job_id}'s next agent turn."),
        other => format!("{other} delivered to {job_id}."),
    }
}

fn parse_control_arguments(arguments: &[String]) -> Result<(String, ControlRequest), String> {
    let job_id = arguments
        .first()
        .cloned()
        .ok_or_else(|| "a job ID is required".to_owned())?;
    if job_id.starts_with("--") {
        return Err(format!("a job ID is required, not {job_id}"));
    }
    let command = arguments
        .get(1)
        .ok_or_else(|| "a control command is required".to_owned())?;
    let rest = &arguments[2..];
    let request = match command.as_str() {
        "send" => {
            let instruction = rest.join(" ");
            if instruction.trim().is_empty() {
                return Err("send requires an instruction".to_owned());
            }
            ControlRequest::Send { instruction }
        }
        "pause" | "resume" | "stop" | "status" | "diff" if !rest.is_empty() => {
            return Err(format!("{command} does not accept an argument"));
        }
        "pause" => ControlRequest::Pause,
        "resume" => ControlRequest::Resume,
        "stop" => ControlRequest::Stop,
        "status" => ControlRequest::Status,
        "diff" => ControlRequest::Diff,
        other => return Err(format!("unknown control command: {other}")),
    };
    Ok((job_id, request))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        ControlRequest, ExecutorSession, HELP, InterruptAction, RunOutput, SubmitWork, VERSION,
        control_acknowledgement, help_requested, interactive_enabled, interrupt_action,
        job_status_succeeded, parse_attach_arguments, parse_control_arguments,
        parse_daemon_arguments, parse_executor_arguments, parse_jobs_arguments,
        parse_resume_arguments, parse_run_arguments, parse_scheduler_arguments,
        parse_submit_arguments, request_graceful_stop, start_control, version_requested,
    };
    use lya::orchestrator::{
        control::{ControlCommand, ControlReceiver},
        state::JobStatus,
    };

    fn owned(arguments: &[&str]) -> Vec<String> {
        arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect()
    }

    #[test]
    fn help_and_version_are_recognised_in_both_spellings() {
        for flag in ["-h", "--help"] {
            assert!(
                help_requested(&owned(&[flag])),
                "{flag} should ask for help"
            );
            assert!(!version_requested(&owned(&[flag])));
        }
        for flag in ["-V", "--version"] {
            assert!(
                version_requested(&owned(&[flag])),
                "{flag} should ask for the version"
            );
            assert!(!help_requested(&owned(&[flag])));
        }
    }

    #[test]
    fn help_and_version_are_only_recognised_as_the_first_argument() {
        // `lya run --help` stays a `lya run` invocation, so `run`'s own parser reports the unknown
        // option together with `run`'s usage line.
        let arguments = owned(&["run", "--help"]);
        assert!(!help_requested(&arguments));
        assert!(parse_run_arguments(&arguments[1..]).is_err());

        assert!(!version_requested(&owned(&["jobs", "--version"])));
        assert!(!help_requested(&[]));
        assert!(!version_requested(&[]));
        // Lowercase `-v` is not a version flag anywhere in Lya, and must not be treated as one.
        assert!(!version_requested(&owned(&["-v"])));
        assert!(!help_requested(&owned(&["-v"])));
    }

    #[test]
    fn version_comes_from_package_metadata() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            format!("lya {VERSION}"),
            format!("lya {}", env!("CARGO_PKG_VERSION"))
        );
        assert!(
            VERSION.split('.').count() >= 3,
            "the package version should be semantic: {VERSION}"
        );
    }

    #[test]
    fn help_names_every_dispatched_command_and_stays_one_screen() {
        for command in [
            "doctor",
            "supervisor",
            "executor",
            "run",
            "resume",
            "jobs",
            "scheduler",
            "daemon",
            "submit",
            "attach",
            "control",
        ] {
            assert!(
                HELP.contains(command),
                "help should name the {command} command"
            );
        }
        assert!(HELP.contains("--help"));
        assert!(HELP.contains("--version"));
        // Orientation, not a manual: it has to stay readable on one terminal screen.
        assert!(
            HELP.lines().count() <= 48,
            "help grew to {} lines; move detail into docs/ instead",
            HELP.lines().count()
        );
        assert!(
            HELP.lines().all(|line| line.len() <= 100),
            "every help line should fit a narrow terminal"
        );
    }

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

    #[test]
    fn parses_jobs_options_and_composes_the_filters() {
        let options = parse_jobs_arguments(&[]).expect("a bare listing should parse");
        assert!(!options.resumable_only);
        assert!(!options.json);

        let options = parse_jobs_arguments(&["--resumable".to_owned(), "--json".to_owned()])
            .expect("filters should compose");
        assert!(options.resumable_only);
        assert!(options.json);

        assert!(parse_jobs_arguments(&["--delete".to_owned()]).is_err());
        assert!(parse_jobs_arguments(&["job-1".to_owned()]).is_err());
    }

    #[test]
    fn parses_scheduler_options_with_a_job_file_and_a_bounded_concurrency() {
        let options = parse_scheduler_arguments(&[
            "jobs.jsonl".to_owned(),
            "--max-concurrent".to_owned(),
            "3".to_owned(),
            "--publish".to_owned(),
            "--max-jobs".to_owned(),
            "2".to_owned(),
        ])
        .expect("scheduler options should parse");

        assert_eq!(
            options.job_file.as_deref(),
            Some(std::path::Path::new("jobs.jsonl"))
        );
        assert_eq!(options.max_concurrent, 3);
        assert_eq!(options.max_jobs, 2);
        assert!(options.publish);
        assert!(!options.resume_queued);
        assert!(matches!(options.output, RunOutput::Human(_)));
    }

    #[test]
    fn scheduler_concurrency_defaults_to_two_and_rejects_zero() {
        let options = parse_scheduler_arguments(&["jobs.jsonl".to_owned()])
            .expect("a bare job file should parse");
        assert_eq!(
            options.max_concurrent,
            lya::orchestrator::scheduler::DEFAULT_MAX_CONCURRENT
        );
        assert_eq!(options.max_concurrent, 2);

        let error = parse_scheduler_arguments(&[
            "jobs.jsonl".to_owned(),
            "--max-concurrent".to_owned(),
            "0".to_owned(),
        ])
        .expect_err("zero concurrency should be refused");
        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn scheduler_requires_work_and_refuses_ambiguous_or_unknown_input() {
        assert!(
            parse_scheduler_arguments(&[]).is_err(),
            "a scheduler run needs a job file or queued work"
        );
        assert!(
            parse_scheduler_arguments(&["--resume-queued".to_owned()])
                .expect("queued work alone is enough")
                .job_file
                .is_none()
        );
        assert!(
            parse_scheduler_arguments(&["one.jsonl".to_owned(), "two.jsonl".to_owned()]).is_err()
        );
        assert!(parse_scheduler_arguments(&["--nope".to_owned()]).is_err());
        assert!(
            parse_scheduler_arguments(&[
                "jobs.jsonl".to_owned(),
                "--verbose".to_owned(),
                "--json".to_owned()
            ])
            .is_err()
        );
        assert!(parse_scheduler_arguments(&["--max-concurrent".to_owned()]).is_err());
    }

    #[test]
    fn scheduler_json_mode_parses_and_stays_machine_readable() {
        let options = parse_scheduler_arguments(&["jobs.jsonl".to_owned(), "--json".to_owned()])
            .expect("JSON mode should parse");

        assert!(matches!(options.output, RunOutput::Json));
    }

    /// The scheduler reuses the exit-code semantics of a single `lya run`, so a queued job never
    /// looks successful and a stopped one never looks failed.
    #[test]
    fn terminal_statuses_map_to_the_same_success_verdict_everywhere() {
        for status in [
            JobStatus::Accepted,
            JobStatus::Published,
            JobStatus::Paused,
            JobStatus::WaitingHuman,
            JobStatus::WaitingClaudeQuota,
            JobStatus::WaitingOpenAiQuota,
            JobStatus::Stopped,
        ] {
            assert!(job_status_succeeded(&status), "{status:?} should succeed");
        }
        for status in [JobStatus::Failed, JobStatus::Running, JobStatus::Queued] {
            assert!(!job_status_succeeded(&status), "{status:?} should not");
        }
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

    #[test]
    fn parses_daemon_options_and_keeps_the_safe_defaults() {
        let options = parse_daemon_arguments(&[]).expect("a bare daemon should parse");

        assert_eq!(
            options.config.max_concurrent,
            lya::orchestrator::scheduler::DEFAULT_MAX_CONCURRENT
        );
        assert!(
            options.config.recover_queued,
            "queued work is picked up by default: it was accepted and never started"
        );
        assert!(
            !options.config.resume_interrupted,
            "interrupted work is never continued unless it was asked for"
        );
        assert!(!options.explicit_output);

        let options = parse_daemon_arguments(&[
            "--max-concurrent".to_owned(),
            "4".to_owned(),
            "--resume-interrupted".to_owned(),
            "--no-recover-queued".to_owned(),
            "--json".to_owned(),
        ])
        .expect("every daemon option should parse");
        assert_eq!(options.config.max_concurrent, 4);
        assert!(options.config.resume_interrupted);
        assert!(!options.config.recover_queued);
        assert!(matches!(options.output, RunOutput::Json));
        assert!(options.explicit_output);
    }

    #[test]
    fn rejects_invalid_daemon_options() {
        assert!(parse_daemon_arguments(&["--max-concurrent".to_owned()]).is_err());
        assert!(
            parse_daemon_arguments(&["--max-concurrent".to_owned(), "0".to_owned()])
                .expect_err("zero concurrency should be refused")
                .contains("greater than zero")
        );
        assert!(parse_daemon_arguments(&["--nope".to_owned()]).is_err());
        assert!(parse_daemon_arguments(&["--verbose".to_owned(), "--json".to_owned()]).is_err());
    }

    /// A detached daemon must be started with arguments that mean exactly what the user asked for.
    #[test]
    fn a_detached_daemon_is_started_with_the_options_it_was_asked_for() {
        let options = parse_daemon_arguments(&[
            "--max-concurrent".to_owned(),
            "3".to_owned(),
            "--resume-interrupted".to_owned(),
        ])
        .expect("daemon options should parse");

        let forwarded = options.forwarded();

        assert_eq!(
            forwarded,
            vec![
                "--max-concurrent".to_owned(),
                "3".to_owned(),
                "--resume-interrupted".to_owned(),
                "--json".to_owned(),
            ]
        );
        // Round-trip: the forwarded arguments reproduce the configuration.
        let reparsed = parse_daemon_arguments(&forwarded).expect("forwarded options should parse");
        assert_eq!(reparsed.config, options.config);
    }

    #[test]
    fn a_detached_daemon_keeps_a_recovery_refusal_it_was_given() {
        let options = parse_daemon_arguments(&["--no-recover-queued".to_owned()])
            .expect("daemon options should parse");

        let reparsed =
            parse_daemon_arguments(&options.forwarded()).expect("forwarded options should parse");

        assert!(!reparsed.config.recover_queued);
        assert!(!reparsed.config.resume_interrupted);
    }

    #[test]
    fn parses_a_submitted_task_with_its_run_options() {
        let options = parse_submit_arguments(&[
            "--browser".to_owned(),
            "--max-iterations".to_owned(),
            "4".to_owned(),
            "--max-jobs".to_owned(),
            "2".to_owned(),
            "Fix".to_owned(),
            "the flaky test".to_owned(),
        ])
        .expect("a task should parse from the current project");

        assert!(options.run.browser);
        assert_eq!(options.run.max_iterations, 4);
        assert_eq!(options.run.max_jobs, 2);
        assert!(!options.run.publish);
        assert!(!options.json);
        match options.work {
            SubmitWork::Task { project_path, task } => {
                assert_eq!(task, "Fix the flaky test");
                assert!(
                    project_path.is_absolute(),
                    "the project is resolved by the client, never by the daemon: {}",
                    project_path.display()
                );
            }
            other => panic!("expected a task, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_submitted_job_file() {
        let options = parse_submit_arguments(&[
            "--file".to_owned(),
            "jobs.jsonl".to_owned(),
            "--publish".to_owned(),
            "--json".to_owned(),
        ])
        .expect("a job file should parse");

        assert!(options.run.publish);
        assert!(options.json);
        assert!(matches!(options.work, SubmitWork::File(path) if path == Path::new("jobs.jsonl")));
    }

    #[test]
    fn refuses_ambiguous_or_incomplete_submissions() {
        assert!(
            parse_submit_arguments(&[])
                .expect_err("a submission needs work")
                .contains("a task or --file is required")
        );
        assert!(
            parse_submit_arguments(&[
                "--file".to_owned(),
                "jobs.jsonl".to_owned(),
                "and".to_owned(),
                "a task".to_owned(),
            ])
            .is_err(),
            "a job file and a task cannot both be submitted"
        );
        assert!(
            parse_submit_arguments(&[
                "--project".to_owned(),
                ".".to_owned(),
                "--file".to_owned(),
                "jobs.jsonl".to_owned(),
            ])
            .is_err(),
            "a job file names its own projects"
        );
        assert!(
            parse_submit_arguments(&["--max-jobs".to_owned(), "0".to_owned(), "x".to_owned()])
                .is_err()
        );
        assert!(
            parse_submit_arguments(&[
                "--max-iterations".to_owned(),
                "0".to_owned(),
                "x".to_owned()
            ])
            .is_err()
        );
        assert!(parse_submit_arguments(&["--nope".to_owned()]).is_err());
    }

    #[test]
    fn parses_attach_options() {
        let options = parse_attach_arguments(&["job-7".to_owned(), "--replay".to_owned()])
            .expect("attach options should parse");

        assert_eq!(options.job_id, "job-7");
        assert!(options.replay);
        assert!(matches!(options.output, RunOutput::Human(_)));

        let options = parse_attach_arguments(&["job-7".to_owned(), "--json".to_owned()])
            .expect("JSON attach should parse");
        assert!(!options.replay);
        assert!(matches!(options.output, RunOutput::Json));

        assert!(parse_attach_arguments(&[]).is_err());
        assert!(parse_attach_arguments(&["a".to_owned(), "b".to_owned()]).is_err());
        assert!(parse_attach_arguments(&["job-7".to_owned(), "--nope".to_owned()]).is_err());
        assert!(
            parse_attach_arguments(&[
                "job-7".to_owned(),
                "--verbose".to_owned(),
                "--json".to_owned()
            ])
            .is_err()
        );
    }

    /// Every control command Lya already has is addressable by job, and means the same thing.
    #[test]
    fn parses_every_control_command_for_a_named_job() {
        for (typed, expected) in [
            ("pause", ControlRequest::Pause),
            ("resume", ControlRequest::Resume),
            ("stop", ControlRequest::Stop),
            ("status", ControlRequest::Status),
            ("diff", ControlRequest::Diff),
        ] {
            let (job_id, request) =
                parse_control_arguments(&["job-7".to_owned(), typed.to_owned()])
                    .expect("a control command should parse");

            assert_eq!(job_id, "job-7");
            assert_eq!(request, expected);
        }

        let (job_id, request) = parse_control_arguments(&[
            "job-7".to_owned(),
            "send".to_owned(),
            "Also update".to_owned(),
            "the changelog".to_owned(),
        ])
        .expect("an instruction should parse");

        assert_eq!(job_id, "job-7");
        assert_eq!(
            request,
            ControlRequest::Send {
                instruction: "Also update the changelog".to_owned()
            }
        );
    }

    #[test]
    fn refuses_invalid_control_requests() {
        assert!(parse_control_arguments(&[]).is_err());
        assert!(parse_control_arguments(&["job-7".to_owned()]).is_err());
        assert!(parse_control_arguments(&["job-7".to_owned(), "send".to_owned()]).is_err());
        assert!(parse_control_arguments(&["job-7".to_owned(), "explode".to_owned()]).is_err());
        assert!(
            parse_control_arguments(&["job-7".to_owned(), "pause".to_owned(), "now".to_owned()])
                .is_err(),
            "pause takes no argument"
        );
        assert!(
            parse_control_arguments(&["--json".to_owned(), "pause".to_owned()]).is_err(),
            "the first argument is the job, not an option"
        );
    }

    /// A command whose answer appears in the job's events must say so, or the user waits for output
    /// that is never coming.
    #[test]
    fn control_acknowledgements_say_where_the_answer_appears() {
        assert!(control_acknowledgement("job-7", "PAUSE").contains("safe boundary"));
        assert!(control_acknowledgement("job-7", "RESUME").contains("job-7"));
        assert!(control_acknowledgement("job-7", "STOP").contains("safe shutdown"));
        for command in ["STATUS", "DIFF"] {
            let acknowledgement = control_acknowledgement("job-7", command);
            assert!(
                acknowledgement.contains("lya attach job-7"),
                "{command}: {acknowledgement}"
            );
        }
        assert!(control_acknowledgement("job-7", "SEND").contains("next agent turn"));
    }
}
