use std::{
    error::Error,
    fmt,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::process::{ProcessRunner, SystemProcessRunner};

use super::{
    executor::{Executor, ExecutorError, ExecutorRequest, ExecutorSession},
    repository::{RepositoryError, RepositoryState},
    state::{JobPhase, JobState, JobStatus, StateError, StateStore},
    supervisor::{Project, Supervisor, SupervisorDecision, SupervisorError, SupervisorRequest},
};

pub const DEFAULT_MAX_ITERATIONS: u32 = 10;

static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    pub job_id: String,
    pub project: Project,
    pub task: String,
    pub private_context: String,
}

pub struct AutonomousOrchestrator<S, E, R = SystemProcessRunner> {
    supervisor: S,
    executor: E,
    repository_runner: R,
    state_store: StateStore,
    max_iterations: u32,
    browser: bool,
}

impl<S, E, R> AutonomousOrchestrator<S, E, R> {
    pub fn new(supervisor: S, executor: E, repository_runner: R, state_store: StateStore) -> Self {
        Self {
            supervisor,
            executor,
            repository_runner,
            state_store,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            browser: false,
        }
    }

    pub fn with_max_iterations(mut self, max_iterations: u32) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    pub fn with_browser(mut self, browser: bool) -> Self {
        self.browser = browser;
        self
    }
}

impl<S: Supervisor, E: Executor, R: ProcessRunner> AutonomousOrchestrator<S, E, R> {
    pub async fn run(&self, request: NewJob) -> Result<JobState, OrchestrationError> {
        if request.task.trim().is_empty() {
            return Err(OrchestrationError::InvalidTransition(
                "a job task cannot be empty".to_owned(),
            ));
        }
        if request.job_id.trim().is_empty() {
            return Err(OrchestrationError::InvalidTransition(
                "a job ID cannot be empty".to_owned(),
            ));
        }
        if request.private_context.trim().is_empty() {
            return Err(OrchestrationError::InvalidTransition(
                "private context cannot be empty".to_owned(),
            ));
        }
        if self.max_iterations == 0 {
            return Err(OrchestrationError::InvalidTransition(
                "max iterations must be greater than zero".to_owned(),
            ));
        }

        let initial_repository_state =
            RepositoryState::collect(&self.repository_runner, &request.project.path)
                .await
                .map_err(OrchestrationError::Repository)?;
        if !initial_repository_state.is_clean() {
            return Err(OrchestrationError::RepositoryDirty(
                request.project.path.clone(),
            ));
        }

        let mut job = JobState::new(
            request.job_id,
            request.project.name.clone(),
            request.project.path.clone(),
            request.task.clone(),
        );
        self.persist(&job)?;
        let mut repository_state = initial_repository_state;

        loop {
            if job.iteration >= self.max_iterations {
                self.fail(&mut job)?;
                return Err(OrchestrationError::IterationLimit {
                    limit: self.max_iterations,
                });
            }

            job.phase = JobPhase::Supervisor;
            job.iteration += 1;
            let supervisor_request = SupervisorRequest {
                private_context: request.private_context.clone(),
                project: request.project.clone(),
                task: request.task.clone(),
                phase: Some("supervisor review".to_owned()),
                iteration: job.iteration,
                executor_report: job.last_executor_report.clone(),
                repository_state: Some(repository_state.render_for_supervisor()),
            };
            let decision = match self.supervisor.decide(supervisor_request).await {
                Ok(decision) => decision,
                Err(error) => {
                    self.fail(&mut job)?;
                    return Err(OrchestrationError::Supervisor(error));
                }
            };
            job.last_supervisor_decision = Some(decision.clone());

            match decision {
                SupervisorDecision::Claude { prompt, .. } => {
                    job.phase = JobPhase::Executor;
                    job.touch();
                    self.persist(&job)?;

                    let session = match (&job.last_executor_report, &job.claude_session_id) {
                        (Some(_), Some(session_id)) if !session_id.trim().is_empty() => {
                            ExecutorSession::Resume(session_id.clone())
                        }
                        (Some(_), _) => {
                            self.fail(&mut job)?;
                            return Err(OrchestrationError::InvalidTransition(
                                "supervisor requested another Claude pass, but the previous Claude result has no resumable session ID".to_owned(),
                            ));
                        }
                        (None, _) => ExecutorSession::New,
                    };
                    let executor_request = ExecutorRequest {
                        project_name: job.project_name.clone(),
                        project_path: job.project_path.clone(),
                        prompt,
                        session,
                        browser: self.browser,
                        timeout: None,
                    };
                    let result = match self.executor.execute(executor_request).await {
                        Ok(result) => result,
                        Err(error) => {
                            self.fail(&mut job)?;
                            return Err(OrchestrationError::Executor(error));
                        }
                    };
                    if let Some(session_id) =
                        result.session_id.filter(|value| !value.trim().is_empty())
                    {
                        job.claude_session_id = Some(session_id);
                    }
                    job.last_executor_report = Some(result.final_response);
                    job.phase = JobPhase::Supervisor;
                    job.touch();
                    self.persist(&job)?;

                    repository_state = match RepositoryState::collect(
                        &self.repository_runner,
                        &request.project.path,
                    )
                    .await
                    {
                        Ok(state) => state,
                        Err(error) => {
                            self.fail(&mut job)?;
                            return Err(OrchestrationError::Repository(error));
                        }
                    };
                }
                SupervisorDecision::Accept { .. } => {
                    job.status = JobStatus::Accepted;
                    job.touch();
                    self.persist(&job)?;
                    return Ok(job);
                }
                SupervisorDecision::Human { .. } => {
                    job.status = JobStatus::WaitingHuman;
                    job.touch();
                    self.persist(&job)?;
                    return Ok(job);
                }
                SupervisorDecision::Stop { .. } => {
                    job.status = JobStatus::Stopped;
                    job.touch();
                    self.persist(&job)?;
                    return Ok(job);
                }
            }
        }
    }

    fn persist(&self, job: &JobState) -> Result<(), OrchestrationError> {
        let mut state = self.state_store.load().map_err(OrchestrationError::State)?;
        state.upsert(job.clone());
        self.state_store
            .save(&state)
            .map_err(OrchestrationError::State)
    }

    fn fail(&self, job: &mut JobState) -> Result<(), OrchestrationError> {
        job.status = JobStatus::Failed;
        job.touch();
        self.persist(job)
    }
}

pub fn new_job_id() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let sequence = NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed);
    format!("job-{seconds}-{sequence}")
}

#[derive(Debug)]
pub enum OrchestrationError {
    Supervisor(SupervisorError),
    Executor(ExecutorError),
    Repository(RepositoryError),
    RepositoryDirty(PathBuf),
    State(StateError),
    IterationLimit { limit: u32 },
    InvalidTransition(String),
}

impl fmt::Display for OrchestrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supervisor(error) => write!(formatter, "supervisor error: {error}"),
            Self::Executor(error) => write!(formatter, "executor error: {error}"),
            Self::Repository(error) => write!(formatter, "repository error: {error}"),
            Self::RepositoryDirty(path) => write!(
                formatter,
                "refusing to start job because the working tree is not clean: {}",
                path.display()
            ),
            Self::State(error) => write!(formatter, "state error: {error}"),
            Self::IterationLimit { limit } => write!(
                formatter,
                "autonomous job reached its iteration limit of {limit}"
            ),
            Self::InvalidTransition(error) => write!(formatter, "invalid job transition: {error}"),
        }
    }
}

impl Error for OrchestrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Supervisor(error) => Some(error),
            Self::Executor(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::State(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        fs,
        future::Future,
        path::PathBuf,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use super::{AutonomousOrchestrator, NewJob, OrchestrationError};
    use crate::{
        orchestrator::{
            executor::{Executor, ExecutorError, ExecutorRequest, ExecutorResult, ExecutorSession},
            home::LyaHome,
            state::{JobStatus, StateStore},
            supervisor::{
                Project, Supervisor, SupervisorDecision, SupervisorError, SupervisorRequest,
            },
        },
        process::{ProcessError, ProcessOutput, ProcessRunner, ProcessSpec},
    };

    #[derive(Clone)]
    struct FakeSupervisor {
        decisions: Arc<Mutex<VecDeque<Result<SupervisorDecision, SupervisorError>>>>,
        requests: Arc<Mutex<Vec<SupervisorRequest>>>,
    }

    impl FakeSupervisor {
        fn new(decisions: Vec<Result<SupervisorDecision, SupervisorError>>) -> Self {
            Self {
                decisions: Arc::new(Mutex::new(decisions.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Supervisor for FakeSupervisor {
        fn decide(
            &self,
            request: SupervisorRequest,
        ) -> Pin<Box<dyn Future<Output = Result<SupervisorDecision, SupervisorError>> + Send + '_>>
        {
            self.requests.lock().expect("request lock").push(request);
            let decision = self
                .decisions
                .lock()
                .expect("decision lock")
                .pop_front()
                .expect("a supervisor decision should be queued");
            Box::pin(async move { decision })
        }
    }

    #[derive(Clone)]
    struct FakeExecutor {
        results: Arc<Mutex<VecDeque<Result<ExecutorResult, ExecutorError>>>>,
        requests: Arc<Mutex<Vec<ExecutorRequest>>>,
    }

    impl FakeExecutor {
        fn new(results: Vec<Result<ExecutorResult, ExecutorError>>) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Executor for FakeExecutor {
        fn execute(
            &self,
            request: ExecutorRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ExecutorResult, ExecutorError>> + Send + '_>>
        {
            self.requests.lock().expect("request lock").push(request);
            let result = self
                .results
                .lock()
                .expect("result lock")
                .pop_front()
                .expect("an executor result should be queued");
            Box::pin(async move { result })
        }
    }

    #[derive(Clone)]
    struct FakeGitRunner {
        dirty: bool,
    }

    impl ProcessRunner for FakeGitRunner {
        fn run(
            &self,
            spec: ProcessSpec,
        ) -> Pin<Box<dyn Future<Output = Result<ProcessOutput, ProcessError>> + Send + '_>>
        {
            let stdout = match spec.args.as_slice() {
                [command, argument]
                    if command == "rev-parse" && argument == "--is-inside-work-tree" =>
                {
                    "true\n"
                }
                [command, argument] if command == "rev-parse" && argument == "HEAD" => "abc123\n",
                [command, argument] if command == "status" && argument == "--short" => {
                    if self.dirty { " M hello.txt\n" } else { "" }
                }
                [command, argument] if command == "diff" && argument == "--stat" => {
                    " hello.txt | 1 +\n"
                }
                [command, argument] if command == "diff" && argument == "--name-only" => {
                    "hello.txt\n"
                }
                [command, argument] if command == "diff" && argument == "--no-ext-diff" => {
                    "diff --git a/hello.txt b/hello.txt\n"
                }
                _ => panic!("unexpected Git invocation: {:?}", spec.args),
            };
            Box::pin(async move {
                Ok(ProcessOutput {
                    exit_code: Some(0),
                    stdout: stdout.to_owned(),
                    stderr: String::new(),
                })
            })
        }
    }

    fn claude_result(session_id: Option<&str>, report: &str) -> ExecutorResult {
        ExecutorResult {
            final_response: report.to_owned(),
            session_id: session_id.map(str::to_owned),
            exit_code: Some(0),
            duration_ms: None,
            turns: None,
            total_cost_usd: None,
            usage: None,
        }
    }

    fn job_request() -> NewJob {
        NewJob {
            job_id: "test-job".to_owned(),
            project: Project {
                name: "test-project".to_owned(),
                path: std::env::temp_dir(),
            },
            task: "Update hello.txt".to_owned(),
            private_context: "Private project context.".to_owned(),
        }
    }

    fn home(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("lya-job-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("test home should be created");
        path
    }

    fn orchestrator(
        supervisor: FakeSupervisor,
        executor: FakeExecutor,
        home: &PathBuf,
    ) -> AutonomousOrchestrator<FakeSupervisor, FakeExecutor, FakeGitRunner> {
        AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(home)),
        )
    }

    #[tokio::test]
    async fn happy_path_persists_acceptance_and_claude_session() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Update hello".to_owned(),
                next_prompt: Some("Continue later".to_owned()),
                reason: None,
            }),
        ]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session-1"), "Completed."))]);
        let executor_requests = executor.requests.clone();
        let directory = home("happy");
        let result = orchestrator(supervisor, executor, &directory)
            .with_browser(true)
            .run(job_request())
            .await
            .expect("job should finish");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(result.claude_session_id.as_deref(), Some("session-1"));
        assert!(
            matches!(result.last_supervisor_decision, Some(SupervisorDecision::Accept { ref commit_title, .. }) if commit_title == "Update hello")
        );
        assert_eq!(
            executor_requests
                .lock()
                .expect("executor request lock")
                .len(),
            1
        );
        assert_eq!(
            executor_requests.lock().expect("executor request lock")[0].session,
            ExecutorSession::New
        );
        assert!(executor_requests.lock().expect("executor request lock")[0].browser);
        assert_eq!(
            supervisor_requests
                .lock()
                .expect("supervisor request lock")
                .len(),
            2
        );
        let stored = StateStore::new(&LyaHome::from_path(&directory))
            .load()
            .expect("state should load");
        assert_eq!(stored.jobs.get(&result.job_id), Some(&result));
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn correction_resumes_exact_previous_claude_session() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Claude {
                prompt: "Correct it.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Fix hello".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let executor = FakeExecutor::new(vec![
            Ok(claude_result(Some("session-1"), "First pass.")),
            Ok(claude_result(Some("session-1"), "Correction complete.")),
        ]);
        let requests = executor.requests.clone();
        let directory = home("resume");
        let result = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect("job should finish");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(
            requests.lock().expect("executor request lock")[1].session,
            ExecutorSession::Resume("session-1".to_owned())
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn correction_without_resumable_session_marks_job_failed() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Claude {
                prompt: "Correct it.".to_owned(),
                reason: None,
            }),
        ]);
        let executor = FakeExecutor::new(vec![Ok(claude_result(None, "First pass."))]);
        let directory = home("missing-session");
        let error = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect_err("missing session should fail");

        assert!(matches!(error, OrchestrationError::InvalidTransition(_)));
        let stored = StateStore::new(&LyaHome::from_path(&directory))
            .load()
            .expect("state should load");
        assert_eq!(
            stored
                .jobs
                .values()
                .next()
                .expect("job should persist")
                .status,
            JobStatus::Failed
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn human_decision_does_not_call_executor() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Human {
            reason: "Choose an API.".to_owned(),
        })]);
        let executor = FakeExecutor::new(vec![]);
        let requests = executor.requests.clone();
        let directory = home("human");
        let result = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect("job should wait");

        assert_eq!(result.status, JobStatus::WaitingHuman);
        assert!(requests.lock().expect("executor request lock").is_empty());
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn stop_decision_does_not_call_executor() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Stop {
            reason: "Task is obsolete.".to_owned(),
        })]);
        let executor = FakeExecutor::new(vec![]);
        let requests = executor.requests.clone();
        let directory = home("stop");
        let result = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect("job should stop");

        assert_eq!(result.status, JobStatus::Stopped);
        assert!(requests.lock().expect("executor request lock").is_empty());
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn iteration_limit_marks_job_failed() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Claude {
            prompt: "Continue.".to_owned(),
            reason: None,
        })]);
        let executor =
            FakeExecutor::new(vec![Ok(claude_result(Some("session-1"), "Still working."))]);
        let directory = home("limit");
        let error = orchestrator(supervisor, executor, &directory)
            .with_max_iterations(1)
            .run(job_request())
            .await
            .expect_err("limit should fail");

        assert!(matches!(
            error,
            OrchestrationError::IterationLimit { limit: 1 }
        ));
        let stored = StateStore::new(&LyaHome::from_path(&directory))
            .load()
            .expect("state should load");
        assert_eq!(
            stored
                .jobs
                .values()
                .next()
                .expect("job should persist")
                .status,
            JobStatus::Failed
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn executor_failure_marks_job_failed() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Claude {
            prompt: "Implement.".to_owned(),
            reason: None,
        })]);
        let executor = FakeExecutor::new(vec![Err(ExecutorError::Process(
            "broken executor".to_owned(),
        ))]);
        let directory = home("executor-failure");
        let error = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect_err("executor should fail");

        assert!(matches!(error, OrchestrationError::Executor(_)));
        let stored = StateStore::new(&LyaHome::from_path(&directory))
            .load()
            .expect("state should load");
        assert_eq!(
            stored
                .jobs
                .values()
                .next()
                .expect("job should persist")
                .status,
            JobStatus::Failed
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn supervisor_failure_marks_job_failed() {
        let supervisor = FakeSupervisor::new(vec![Err(SupervisorError::Process(
            "broken supervisor".to_owned(),
        ))]);
        let executor = FakeExecutor::new(vec![]);
        let directory = home("supervisor-failure");
        let error = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect_err("supervisor should fail");

        assert!(matches!(error, OrchestrationError::Supervisor(_)));
        let stored = StateStore::new(&LyaHome::from_path(&directory))
            .load()
            .expect("state should load");
        assert_eq!(
            stored
                .jobs
                .values()
                .next()
                .expect("job should persist")
                .status,
            JobStatus::Failed
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn dirty_working_tree_is_refused_before_supervisor() {
        let supervisor = FakeSupervisor::new(vec![]);
        let requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![]);
        let directory = home("dirty");
        let orchestrator = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: true },
            StateStore::new(&LyaHome::from_path(&directory)),
        );
        let error = orchestrator
            .run(job_request())
            .await
            .expect_err("dirty repository should be refused");

        assert!(matches!(error, OrchestrationError::RepositoryDirty(_)));
        assert!(requests.lock().expect("supervisor request lock").is_empty());
        fs::remove_dir_all(directory).expect("test home should be removed");
    }
}
