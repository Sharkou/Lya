use std::{
    error::Error,
    fmt,
    path::PathBuf,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::process::{ProcessRunner, SystemProcessRunner};

use super::{
    executor::{Executor, ExecutorError, ExecutorRequest, ExecutorSession},
    publisher::{PublishError, PublishProgress, PublishRequest, PublishStage, Publisher},
    repository::{RepositoryError, RepositoryState},
    state::{JobPhase, JobState, JobStatus, StateError, StateStore},
    supervisor::{Project, Supervisor, SupervisorDecision, SupervisorError, SupervisorRequest},
};

pub const DEFAULT_MAX_ITERATIONS: u32 = 10;
pub const DEFAULT_MAX_JOBS: u32 = 10;

static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    pub job_id: String,
    pub project: Project,
    pub task: String,
    pub private_context: String,
}

pub struct AutonomousOrchestrator<S, E, R = SystemProcessRunner, P = ()> {
    supervisor: S,
    executor: E,
    repository_runner: R,
    state_store: StateStore,
    publisher: P,
    max_iterations: u32,
    max_jobs: u32,
    browser: bool,
}

impl<S, E, R> AutonomousOrchestrator<S, E, R, ()> {
    pub fn new(supervisor: S, executor: E, repository_runner: R, state_store: StateStore) -> Self {
        Self {
            supervisor,
            executor,
            repository_runner,
            state_store,
            publisher: (),
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_jobs: DEFAULT_MAX_JOBS,
            browser: false,
        }
    }
}

impl<S, E, R, P> AutonomousOrchestrator<S, E, R, P> {
    pub fn with_max_iterations(mut self, max_iterations: u32) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    pub fn with_max_jobs(mut self, max_jobs: u32) -> Self {
        self.max_jobs = max_jobs;
        self
    }

    pub fn with_browser(mut self, browser: bool) -> Self {
        self.browser = browser;
        self
    }

    pub fn with_publisher<Q>(self, publisher: Q) -> AutonomousOrchestrator<S, E, R, Q> {
        AutonomousOrchestrator {
            supervisor: self.supervisor,
            executor: self.executor,
            repository_runner: self.repository_runner,
            state_store: self.state_store,
            publisher,
            max_iterations: self.max_iterations,
            max_jobs: self.max_jobs,
            browser: self.browser,
        }
    }
}

impl<S: Supervisor, E: Executor, R: ProcessRunner, P: Publisher>
    AutonomousOrchestrator<S, E, R, P>
{
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
        if self.max_jobs == 0 {
            return Err(OrchestrationError::InvalidTransition(
                "max jobs must be greater than zero".to_owned(),
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
                SupervisorDecision::Accept { commit_title, .. } => {
                    job.status = JobStatus::Accepted;
                    job.accepted_repository_state = Some(repository_state.clone());
                    job.touch();
                    self.persist(&job)?;
                    if !self.publisher.is_enabled() {
                        return Ok(job);
                    }

                    job.status = JobStatus::Publishing;
                    job.phase = JobPhase::Publisher;
                    job.touch();
                    self.persist(&job)?;
                    let publish = {
                        let mut progress = JobPublicationProgress {
                            job: &mut job,
                            state_store: &self.state_store,
                        };
                        self.publisher
                            .publish(
                                PublishRequest {
                                    project_path: progress.job.project_path.clone(),
                                    accepted_repository_state: repository_state,
                                    commit_title,
                                },
                                &mut progress,
                            )
                            .await
                    };
                    match publish {
                        Ok(result) => {
                            job.publish_result = Some(result);
                            let post_commit_state = match RepositoryState::collect(
                                &self.repository_runner,
                                &job.project_path,
                            )
                            .await
                            {
                                Ok(state) => state,
                                Err(error) => {
                                    self.fail(&mut job)?;
                                    return Err(OrchestrationError::Repository(error));
                                }
                            };
                            if !post_commit_state.is_clean() {
                                self.fail(&mut job)?;
                                return Err(OrchestrationError::PostCommitWorkingTreeDirty(
                                    job.project_path.clone(),
                                ));
                            }
                            job.status = JobStatus::Published;
                            job.touch();
                            self.persist(&job)?;
                            return Ok(job);
                        }
                        Err(error) => {
                            if let PublishError::PushRejected(result) = &error {
                                job.publish_result = Some(result.clone());
                            }
                            self.fail(&mut job)?;
                            return Err(OrchestrationError::Publish(error));
                        }
                    }
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

    pub async fn run_sequential(&self, request: NewJob) -> Result<RunResult, OrchestrationError> {
        if self.max_jobs == 0 {
            return Err(OrchestrationError::InvalidTransition(
                "max jobs must be greater than zero".to_owned(),
            ));
        }
        let mut jobs = Vec::new();
        let mut current_request = request;
        loop {
            if jobs.len() as u32 >= self.max_jobs {
                return Ok(RunResult {
                    jobs,
                    max_jobs_reached: true,
                });
            }
            let job = self.run(current_request.clone()).await?;
            let next_prompt = match &job.last_supervisor_decision {
                Some(SupervisorDecision::Accept {
                    next_prompt: Some(next_prompt),
                    ..
                }) if job.status == JobStatus::Published => Some(next_prompt.clone()),
                _ => None,
            };
            jobs.push(job);
            let Some(task) = next_prompt else {
                return Ok(RunResult {
                    jobs,
                    max_jobs_reached: false,
                });
            };
            current_request = NewJob {
                job_id: new_job_id(),
                project: current_request.project,
                task,
                private_context: current_request.private_context,
            };
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub jobs: Vec<JobState>,
    pub max_jobs_reached: bool,
}

impl Publisher for () {
    fn is_enabled(&self) -> bool {
        false
    }

    fn publish<'a>(
        &'a self,
        _request: PublishRequest,
        _progress: &'a mut dyn PublishProgress,
    ) -> Pin<
        Box<dyn Future<Output = Result<super::publisher::PublishResult, PublishError>> + Send + 'a>,
    > {
        Box::pin(async { Err(PublishError::PublishingDisabled) })
    }
}

struct JobPublicationProgress<'a> {
    job: &'a mut JobState,
    state_store: &'a StateStore,
}

impl PublishProgress for JobPublicationProgress<'_> {
    fn record(&mut self, stage: PublishStage) -> Result<(), PublishError> {
        self.job.publish_stage = Some(stage);
        self.job.touch();
        let mut state = self
            .state_store
            .load()
            .map_err(|error| PublishError::ProgressPersistence(error.to_string()))?;
        state.upsert(self.job.clone());
        self.state_store
            .save(&state)
            .map_err(|error| PublishError::ProgressPersistence(error.to_string()))
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
    PostCommitWorkingTreeDirty(PathBuf),
    Publish(PublishError),
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
            Self::PostCommitWorkingTreeDirty(path) => write!(
                formatter,
                "working tree is not clean after publishing: {}",
                path.display()
            ),
            Self::Publish(error) => write!(formatter, "publication error: {error}"),
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
            Self::Publish(error) => Some(error),
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
        path::{Path, PathBuf},
        pin::Pin,
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
        sync::{Arc, Mutex},
    };

    use super::{AutonomousOrchestrator, NewJob, OrchestrationError};
    use crate::{
        orchestrator::{
            executor::{Executor, ExecutorError, ExecutorRequest, ExecutorResult, ExecutorSession},
            home::LyaHome,
            publisher::{
                GitPublishConfig, GitPublisher, PublishError, PublishProgress, PublishRequest,
                PublishResult, PublishStage, Publisher, PushStatus,
            },
            repository::RepositoryState,
            state::{JobStatus, StateStore},
            supervisor::{
                Project, Supervisor, SupervisorDecision, SupervisorError, SupervisorRequest,
            },
        },
        process::{ProcessError, ProcessOutput, ProcessRunner, ProcessSpec, SystemProcessRunner},
    };

    static NEXT_GIT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

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
                [command, argument] if command == "rev-parse" && argument == "--show-toplevel" => {
                    return Box::pin(async move {
                        Ok(ProcessOutput {
                            exit_code: Some(0),
                            stdout: format!(
                                "{}\n",
                                spec.cwd.expect("Git cwd should be set").display()
                            ),
                            stderr: String::new(),
                        })
                    });
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
                [command, first, second, third]
                    if command == "ls-files"
                        && first == "--others"
                        && second == "--exclude-standard"
                        && third == "-z" =>
                {
                    ""
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

    #[derive(Clone)]
    struct FakePublisher {
        results: Arc<Mutex<VecDeque<Result<PublishResult, PublishError>>>>,
        requests: Arc<Mutex<Vec<PublishRequest>>>,
    }

    impl FakePublisher {
        fn new(results: Vec<Result<PublishResult, PublishError>>) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Publisher for FakePublisher {
        fn publish<'a>(
            &'a self,
            request: PublishRequest,
            _progress: &'a mut dyn PublishProgress,
        ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>>
        {
            self.requests
                .lock()
                .expect("publisher request lock")
                .push(request);
            let result = self
                .results
                .lock()
                .expect("publisher result lock")
                .pop_front()
                .expect("a publisher result should be queued");
            Box::pin(async move { result })
        }
    }

    fn published_result(title: &str) -> PublishResult {
        PublishResult {
            commit_sha: format!("sha-{title}"),
            commit_title: title.to_owned(),
            remote: "origin".to_owned(),
            branch: "main".to_owned(),
            push_status: PushStatus::Pushed,
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
        assert!(result.accepted_repository_state.is_some());
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

    #[tokio::test]
    async fn sequential_run_starts_next_job_only_after_successful_publication() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement task one.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Complete task one".to_owned(),
                next_prompt: Some("task two".to_owned()),
                reason: None,
            }),
            Ok(SupervisorDecision::Claude {
                prompt: "Implement task two.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Complete task two".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![
            Ok(claude_result(Some("session-1"), "Task one complete.")),
            Ok(claude_result(Some("session-2"), "Task two complete.")),
        ]);
        let publisher = FakePublisher::new(vec![
            Ok(published_result("Complete task one")),
            Ok(published_result("Complete task two")),
        ]);
        let publish_requests = publisher.requests.clone();
        let directory = home("sequential");

        let result = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(&directory)),
        )
        .with_publisher(publisher)
        .run_sequential(job_request())
        .await
        .expect("sequential run should finish");

        assert_eq!(result.jobs.len(), 2);
        assert!(!result.max_jobs_reached);
        assert!(
            result
                .jobs
                .iter()
                .all(|job| job.status == JobStatus::Published)
        );
        assert_eq!(
            publish_requests
                .lock()
                .expect("publisher request lock")
                .len(),
            2
        );
        assert_eq!(
            supervisor_requests.lock().expect("supervisor request lock")[2].task,
            "task two"
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn publication_failure_never_creates_next_job() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "First".to_owned(),
                next_prompt: Some("must not start".to_owned()),
                reason: None,
            }),
        ]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session-1"), "Complete."))]);
        let publisher = FakePublisher::new(vec![Err(PublishError::RepositoryChangedAfterReview)]);
        let directory = home("publish-failure");

        let error = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(&directory)),
        )
        .with_publisher(publisher)
        .run_sequential(job_request())
        .await
        .expect_err("publish failure should stop the run");

        assert!(matches!(error, OrchestrationError::Publish(_)));
        assert_eq!(
            supervisor_requests
                .lock()
                .expect("supervisor request lock")
                .len(),
            2
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn sequential_run_stops_cleanly_at_max_jobs() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "First".to_owned(),
                next_prompt: Some("next task".to_owned()),
                reason: None,
            }),
        ]);
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session-1"), "Complete."))]);
        let publisher = FakePublisher::new(vec![Ok(published_result("First"))]);
        let directory = home("max-jobs");

        let result = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(&directory)),
        )
        .with_publisher(publisher)
        .with_max_jobs(1)
        .run_sequential(job_request())
        .await
        .expect("max jobs is a normal run stop");

        assert_eq!(result.jobs.len(), 1);
        assert!(result.max_jobs_reached);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[derive(Clone)]
    struct WritingExecutor {
        project_path: PathBuf,
    }

    impl Executor for WritingExecutor {
        fn execute(
            &self,
            _request: ExecutorRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ExecutorResult, ExecutorError>> + Send + '_>>
        {
            let project_path = self.project_path.clone();
            Box::pin(async move {
                fs::write(project_path.join("hello.txt"), "hello\nLYA_PUBLISH_OK\n")
                    .expect("executor test change should be written");
                Ok(claude_result(
                    Some("test-session"),
                    "Added and verified LYA_PUBLISH_OK.",
                ))
            })
        }
    }

    fn git(path: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(path)
            .output()
            .expect("Git should start");
        assert!(
            output.status.success(),
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("Git stdout should be UTF-8")
    }

    fn local_publish_repository() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "lya-job-publish-test-{}-{}",
            std::process::id(),
            NEXT_GIT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let work = root.join("work");
        let remote = root.join("remote.git");
        fs::create_dir_all(&work).expect("work directory should be created");
        git(&work, &["init", "--initial-branch", "main"]);
        fs::write(work.join("hello.txt"), "hello\n").expect("initial file should be written");
        git(&work, &["add", "hello.txt"]);
        git(
            &work,
            &[
                "-c",
                "user.name=Initial",
                "-c",
                "user.email=initial@example.com",
                "commit",
                "-m",
                "Initial",
            ],
        );
        git(
            &root,
            &[
                "init",
                "--bare",
                "--initial-branch",
                "main",
                remote.to_str().expect("remote path should be UTF-8"),
            ],
        );
        git(
            &work,
            &[
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path should be UTF-8"),
            ],
        );
        git(&work, &["push", "origin", "main"]);
        (work, remote)
    }

    #[tokio::test]
    async fn local_end_to_end_review_execution_commit_and_push_succeeds() {
        let (work, remote) = local_publish_repository();
        let state_home = home("local-end-to-end");
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Add one line containing LYA_PUBLISH_OK to hello.txt and verify it."
                    .to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Add LYA publish marker".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let publisher = GitPublisher::new(
            GitPublishConfig::new("Test Publisher", "publisher@example.com", "origin", "main")
                .expect("test publisher configuration should be valid"),
        );
        let result = AutonomousOrchestrator::new(
            supervisor,
            WritingExecutor {
                project_path: work.clone(),
            },
            SystemProcessRunner,
            StateStore::new(&LyaHome::from_path(&state_home)),
        )
        .with_publisher(publisher)
        .run_sequential(NewJob {
            job_id: "local-end-to-end".to_owned(),
            project: Project {
                name: "local-end-to-end".to_owned(),
                path: work.clone(),
            },
            task: "Add one line containing LYA_PUBLISH_OK to hello.txt and verify it.".to_owned(),
            private_context: "Test context.".to_owned(),
        })
        .await
        .expect("local end-to-end run should succeed");

        let job = result.jobs.last().expect("one job should have run");
        assert_eq!(job.status, JobStatus::Published);
        assert_eq!(
            fs::read_to_string(work.join("hello.txt")).expect("file should be readable"),
            "hello\nLYA_PUBLISH_OK\n"
        );
        let publication = job
            .publish_result
            .as_ref()
            .expect("publication should persist");
        assert_eq!(job.publish_stage, Some(PublishStage::Pushed));
        assert_eq!(publication.commit_title, "Add LYA publish marker");
        assert_eq!(publication.push_status, PushStatus::Pushed);
        assert_eq!(
            git(&work, &["show", "-s", "--format=%B", "HEAD"]),
            "Add LYA publish marker\n\n"
        );
        assert_eq!(
            git(
                remote.parent().expect("remote parent should exist"),
                &[
                    "--git-dir",
                    remote.to_str().expect("remote path should be UTF-8"),
                    "rev-parse",
                    "main"
                ],
            )
            .trim(),
            publication.commit_sha
        );
        assert!(
            RepositoryState::collect(&SystemProcessRunner, &work)
                .await
                .expect("final state should collect")
                .is_clean()
        );
        fs::remove_dir_all(work.parent().expect("work should have parent"))
            .expect("Git test directory should be removed");
        fs::remove_dir_all(state_home).expect("state home should be removed");
    }
}
