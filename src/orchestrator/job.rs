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
    events::{
        EventSink, EventSinkError, JobEvent, JobEventKind, NoopEventSink, RepositorySummary,
        executor_event_kind, supervisor_event_fields,
    },
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

pub struct AutonomousOrchestrator<S, E, R = SystemProcessRunner, P = (), N = NoopEventSink> {
    supervisor: S,
    executor: E,
    repository_runner: R,
    state_store: StateStore,
    publisher: P,
    event_sink: N,
    max_iterations: u32,
    max_jobs: u32,
    browser: bool,
}

impl<S, E, R> AutonomousOrchestrator<S, E, R, (), NoopEventSink> {
    pub fn new(supervisor: S, executor: E, repository_runner: R, state_store: StateStore) -> Self {
        Self {
            supervisor,
            executor,
            repository_runner,
            state_store,
            publisher: (),
            event_sink: NoopEventSink,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_jobs: DEFAULT_MAX_JOBS,
            browser: false,
        }
    }
}

impl<S, E, R, P, N> AutonomousOrchestrator<S, E, R, P, N> {
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

    pub fn with_publisher<Q>(self, publisher: Q) -> AutonomousOrchestrator<S, E, R, Q, N> {
        AutonomousOrchestrator {
            supervisor: self.supervisor,
            executor: self.executor,
            repository_runner: self.repository_runner,
            state_store: self.state_store,
            publisher,
            event_sink: self.event_sink,
            max_iterations: self.max_iterations,
            max_jobs: self.max_jobs,
            browser: self.browser,
        }
    }

    pub fn with_event_sink<Q>(self, event_sink: Q) -> AutonomousOrchestrator<S, E, R, P, Q> {
        AutonomousOrchestrator {
            supervisor: self.supervisor,
            executor: self.executor,
            repository_runner: self.repository_runner,
            state_store: self.state_store,
            publisher: self.publisher,
            event_sink,
            max_iterations: self.max_iterations,
            max_jobs: self.max_jobs,
            browser: self.browser,
        }
    }
}

impl<S: Supervisor, E: Executor, R: ProcessRunner, P: Publisher, N: EventSink>
    AutonomousOrchestrator<S, E, R, P, N>
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
            match RepositoryState::collect(&self.repository_runner, &request.project.path).await {
                Ok(state) => state,
                Err(error) => {
                    self.emit_request(
                        &request,
                        None,
                        JobEventKind::Failed {
                            error: error.to_string(),
                        },
                    )?;
                    return Err(OrchestrationError::Repository(error));
                }
            };
        if !initial_repository_state.is_clean() {
            self.emit_request(
                &request,
                None,
                JobEventKind::Failed {
                    error: format!(
                        "refusing to start job because the working tree is not clean: {}",
                        request.project.path.display()
                    ),
                },
            )?;
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
        self.emit(
            &job,
            JobEventKind::JobStarted {
                task: request.task.clone(),
            },
        )?;
        self.emit(
            &job,
            JobEventKind::RepositoryCaptured {
                summary: RepositorySummary::from(&initial_repository_state),
            },
        )?;
        let mut repository_state = initial_repository_state;

        loop {
            if job.iteration >= self.max_iterations {
                self.fail(
                    &mut job,
                    format!(
                        "autonomous job reached its iteration limit of {}",
                        self.max_iterations
                    ),
                )?;
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
            self.emit(
                &job,
                JobEventKind::SupervisorStarted {
                    prompt: observable_supervisor_prompt(&supervisor_request),
                },
            )?;
            let decision = match self.supervisor.decide(supervisor_request).await {
                Ok(decision) => decision,
                Err(error) => {
                    if is_quota_error(&error.to_string()) {
                        self.wait_for_quota(&mut job, "OpenAI", error.to_string())?;
                        return Ok(job);
                    }
                    self.fail(&mut job, error.to_string())?;
                    return Err(OrchestrationError::Supervisor(error));
                }
            };
            job.last_supervisor_decision = Some(decision.clone());
            let (action, reason, prompt, commit_title, next_prompt) =
                supervisor_event_fields(&decision);
            self.emit(
                &job,
                JobEventKind::SupervisorFinished {
                    action,
                    reason,
                    prompt,
                    commit_title,
                    next_prompt,
                },
            )?;

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
                            let error = "supervisor requested another Claude pass, but the previous Claude result has no resumable session ID".to_owned();
                            self.fail(&mut job, error.clone())?;
                            return Err(OrchestrationError::InvalidTransition(error));
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
                    self.emit(
                        &job,
                        JobEventKind::ExecutorStarted {
                            prompt: executor_request.prompt.clone(),
                            session_id: match &executor_request.session {
                                ExecutorSession::New => None,
                                ExecutorSession::Resume(session_id) => Some(session_id.clone()),
                            },
                        },
                    )?;
                    let result = match self.executor.execute(executor_request).await {
                        Ok(result) => result,
                        Err(error) => {
                            if is_quota_error(&error.to_string()) {
                                self.wait_for_quota(&mut job, "Claude", error.to_string())?;
                                return Ok(job);
                            }
                            self.fail(&mut job, error.to_string())?;
                            return Err(OrchestrationError::Executor(error));
                        }
                    };
                    self.emit(&job, executor_event_kind(result.clone()))?;
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
                            self.fail(&mut job, error.to_string())?;
                            return Err(OrchestrationError::Repository(error));
                        }
                    };
                    self.emit(
                        &job,
                        JobEventKind::RepositoryCaptured {
                            summary: RepositorySummary::from(&repository_state),
                        },
                    )?;
                }
                SupervisorDecision::Accept { commit_title, .. } => {
                    job.status = JobStatus::Accepted;
                    job.accepted_repository_state = Some(repository_state.clone());
                    job.touch();
                    self.persist(&job)?;
                    if !self.publisher.is_enabled() {
                        self.emit(
                            &job,
                            JobEventKind::JobFinished {
                                status: "ACCEPTED".to_owned(),
                            },
                        )?;
                        return Ok(job);
                    }

                    job.status = JobStatus::Publishing;
                    job.phase = JobPhase::Publisher;
                    job.touch();
                    self.persist(&job)?;
                    self.emit(
                        &job,
                        JobEventKind::PublishStarted {
                            commit_title: commit_title.clone(),
                        },
                    )?;
                    let publish = {
                        let mut progress = JobPublicationProgress {
                            job: &mut job,
                            state_store: &self.state_store,
                            event_sink: &self.event_sink,
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
                                    self.fail(&mut job, error.to_string())?;
                                    return Err(OrchestrationError::Repository(error));
                                }
                            };
                            if !post_commit_state.is_clean() {
                                let error = format!(
                                    "working tree is not clean after publishing: {}",
                                    job.project_path.display()
                                );
                                self.fail(&mut job, error)?;
                                return Err(OrchestrationError::PostCommitWorkingTreeDirty(
                                    job.project_path.clone(),
                                ));
                            }
                            job.status = JobStatus::Published;
                            job.touch();
                            self.persist(&job)?;
                            self.emit(
                                &job,
                                JobEventKind::RepositoryCaptured {
                                    summary: RepositorySummary::from(&post_commit_state),
                                },
                            )?;
                            self.emit(
                                &job,
                                JobEventKind::Published {
                                    result: job
                                        .publish_result
                                        .clone()
                                        .expect("publish result was stored"),
                                },
                            )?;
                            self.emit(
                                &job,
                                JobEventKind::JobFinished {
                                    status: "PUBLISHED".to_owned(),
                                },
                            )?;
                            return Ok(job);
                        }
                        Err(error) => {
                            if let PublishError::PushRejected(result) = &error {
                                job.publish_result = Some(result.clone());
                            }
                            self.fail(&mut job, error.to_string())?;
                            return Err(OrchestrationError::Publish(error));
                        }
                    }
                }
                SupervisorDecision::Human { reason } => {
                    job.status = JobStatus::WaitingHuman;
                    job.touch();
                    self.persist(&job)?;
                    self.emit(&job, JobEventKind::WaitingForHuman { reason })?;
                    self.emit(
                        &job,
                        JobEventKind::JobFinished {
                            status: "WAITING_HUMAN".to_owned(),
                        },
                    )?;
                    return Ok(job);
                }
                SupervisorDecision::Stop { reason } => {
                    job.status = JobStatus::Stopped;
                    job.touch();
                    self.persist(&job)?;
                    self.emit(&job, JobEventKind::Stopped { reason })?;
                    self.emit(
                        &job,
                        JobEventKind::JobFinished {
                            status: "STOPPED".to_owned(),
                        },
                    )?;
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

    fn emit(&self, job: &JobState, kind: JobEventKind) -> Result<(), OrchestrationError> {
        let project = Project {
            name: job.project_name.clone(),
            path: job.project_path.clone(),
        };
        match self.event_sink.emit(&JobEvent::new(
            &job.job_id,
            &project,
            Some(job.iteration),
            kind,
        )) {
            Ok(()) => Ok(()),
            Err(error) => {
                let mut failed = job.clone();
                failed.status = JobStatus::Failed;
                failed.touch();
                self.persist(&failed)?;
                Err(OrchestrationError::Event(error))
            }
        }
    }

    fn emit_request(
        &self,
        request: &NewJob,
        iteration: Option<u32>,
        kind: JobEventKind,
    ) -> Result<(), OrchestrationError> {
        self.event_sink
            .emit(&JobEvent::new(
                &request.job_id,
                &request.project,
                iteration,
                kind,
            ))
            .map_err(OrchestrationError::Event)
    }

    fn fail(&self, job: &mut JobState, error: String) -> Result<(), OrchestrationError> {
        job.status = JobStatus::Failed;
        job.touch();
        self.persist(job)?;
        self.emit(job, JobEventKind::Failed { error })
    }

    fn wait_for_quota(
        &self,
        job: &mut JobState,
        provider: &str,
        reason: String,
    ) -> Result<(), OrchestrationError> {
        job.status = if provider == "Claude" {
            JobStatus::WaitingClaudeQuota
        } else {
            JobStatus::WaitingOpenAiQuota
        };
        job.touch();
        self.persist(job)?;
        self.emit(
            job,
            JobEventKind::WaitingForQuota {
                provider: provider.to_owned(),
                reason,
            },
        )?;
        self.emit(
            job,
            JobEventKind::JobFinished {
                status: match job.status {
                    JobStatus::WaitingClaudeQuota => "WAITING_CLAUDE_QUOTA",
                    JobStatus::WaitingOpenAiQuota => "WAITING_OPENAI_QUOTA",
                    _ => unreachable!(),
                }
                .to_owned(),
            },
        )
    }
}

fn observable_supervisor_prompt(request: &SupervisorRequest) -> String {
    format!(
        "Review the current task and choose CLAUDE, ACCEPT, HUMAN, or STOP.\n\nProject: {}\nPath: {}\nPhase: {}\nIteration: {}\n\nTask:\n{}\n\nExecutor report:\n{}\n\nRepository state:\n{}\n\nPrivate context is intentionally omitted from the event log.",
        request.project.name,
        request.project.path.display(),
        request.phase.as_deref().unwrap_or("not provided"),
        request.iteration,
        request.task,
        request.executor_report.as_deref().unwrap_or("not provided"),
        request
            .repository_state
            .as_deref()
            .unwrap_or("not provided"),
    )
}

fn is_quota_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("quota") || error.contains("rate limit") || error.contains("rate_limit")
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
    event_sink: &'a dyn EventSink,
}

impl PublishProgress for JobPublicationProgress<'_> {
    fn record(&mut self, stage: PublishStage) -> Result<(), PublishError> {
        self.job.publish_stage = Some(stage.clone());
        self.job.touch();
        let mut state = self
            .state_store
            .load()
            .map_err(|error| PublishError::ProgressPersistence(error.to_string()))?;
        state.upsert(self.job.clone());
        self.state_store
            .save(&state)
            .map_err(|error| PublishError::ProgressPersistence(error.to_string()))?;
        let project = Project {
            name: self.job.project_name.clone(),
            path: self.job.project_path.clone(),
        };
        self.event_sink
            .emit(&JobEvent::new(
                &self.job.job_id,
                &project,
                Some(self.job.iteration),
                JobEventKind::PublishStageChanged { stage },
            ))
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
    Event(EventSinkError),
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
            Self::Event(error) => write!(formatter, "job event error: {error}"),
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
            Self::Event(error) => Some(error),
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
            events::{EventSink, JobEvent, JobEventKind, JsonlEventSink},
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

    #[derive(Clone, Default)]
    struct RecordingEventSink {
        events: Arc<Mutex<Vec<JobEvent>>>,
    }

    impl EventSink for RecordingEventSink {
        fn emit(&self, event: &JobEvent) -> Result<(), super::EventSinkError> {
            self.events.lock().expect("event lock").push(event.clone());
            Ok(())
        }
    }

    struct FailingEventSink;

    impl EventSink for FailingEventSink {
        fn emit(&self, _event: &JobEvent) -> Result<(), super::EventSinkError> {
            Err(super::EventSinkError::Write(
                "event storage unavailable".to_owned(),
            ))
        }
    }

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
            progress: &'a mut dyn PublishProgress,
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
            let progress_result = if result.is_ok() {
                [
                    PublishStage::Verifying,
                    PublishStage::Staging,
                    PublishStage::Staged,
                    PublishStage::Committing,
                    PublishStage::Committed,
                    PublishStage::Pushing,
                    PublishStage::Pushed,
                ]
                .into_iter()
                .try_for_each(|stage| progress.record(stage))
            } else {
                Ok(())
            };
            Box::pin(async move {
                progress_result?;
                result
            })
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
    async fn job_loop_emits_supervisor_executor_and_repository_events() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Update the explicit README wording.".to_owned(),
                reason: Some("The repository needs a small correction.".to_owned()),
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Update README wording".to_owned(),
                next_prompt: None,
                reason: Some("The change is ready.".to_owned()),
            }),
        ]);
        let executor = FakeExecutor::new(vec![Ok(claude_result(
            Some("session-visible"),
            "Updated README and verified the change.",
        ))]);
        let directory = home("event-lifecycle");
        let sink = RecordingEventSink::default();
        let recorded = sink.events.clone();

        orchestrator(supervisor, executor, &directory)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("job should finish");

        let events = recorded.lock().expect("event lock");
        assert!(matches!(events[0].kind, JobEventKind::JobStarted { .. }));
        assert!(matches!(
            events[1].kind,
            JobEventKind::RepositoryCaptured { .. }
        ));
        assert!(matches!(
            events[2].kind,
            JobEventKind::SupervisorStarted { .. }
        ));
        assert!(
            matches!(events[3].kind, JobEventKind::SupervisorFinished { ref action, ref prompt, .. } if action == "CLAUDE" && prompt.as_deref() == Some("Update the explicit README wording."))
        );
        assert!(
            matches!(events[4].kind, JobEventKind::ExecutorStarted { ref prompt, .. } if prompt == "Update the explicit README wording.")
        );
        assert!(
            matches!(events[5].kind, JobEventKind::ExecutorFinished { ref session_id, ref final_response, .. } if session_id.as_deref() == Some("session-visible") && final_response.contains("verified"))
        );
        assert!(matches!(
            events[6].kind,
            JobEventKind::RepositoryCaptured { .. }
        ));
        assert!(matches!(
            events[7].kind,
            JobEventKind::SupervisorStarted { .. }
        ));
        assert!(
            matches!(events[8].kind, JobEventKind::SupervisorFinished { ref action, ref commit_title, .. } if action == "ACCEPT" && commit_title.as_deref() == Some("Update README wording"))
        );
        assert!(
            matches!(events[9].kind, JobEventKind::JobFinished { ref status } if status == "ACCEPTED")
        );
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
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let result = orchestrator(supervisor, executor, &directory)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("job should wait");

        assert_eq!(result.status, JobStatus::WaitingHuman);
        assert!(requests.lock().expect("executor request lock").is_empty());
        assert!(
            events
                .lock()
                .expect("event lock")
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::WaitingForHuman { .. }))
        );
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
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let result = orchestrator(supervisor, executor, &directory)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("job should stop");

        assert_eq!(result.status, JobStatus::Stopped);
        assert!(requests.lock().expect("executor request lock").is_empty());
        assert!(
            events
                .lock()
                .expect("event lock")
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::Stopped { .. }))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn provider_quota_failure_becomes_an_observable_waiting_state() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Claude {
            prompt: "Implement.".to_owned(),
            reason: None,
        })]);
        let executor = FakeExecutor::new(vec![Err(ExecutorError::Process(
            "Claude rate limit reached".to_owned(),
        ))]);
        let directory = home("claude-quota");
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let result = orchestrator(supervisor, executor, &directory)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("quota is a waiting state");

        assert_eq!(result.status, JobStatus::WaitingClaudeQuota);
        assert!(events.lock().expect("event lock").iter().any(|event| matches!(event.kind, JobEventKind::WaitingForQuota { ref provider, .. } if provider == "Claude")));
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn failed_job_emits_failed_event() {
        let supervisor = FakeSupervisor::new(vec![Err(SupervisorError::Process(
            "supervisor unavailable".to_owned(),
        ))]);
        let executor = FakeExecutor::new(vec![]);
        let directory = home("event-failure");
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let error = orchestrator(supervisor, executor, &directory)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect_err("supervisor failure should fail the job");

        assert!(matches!(error, OrchestrationError::Supervisor(_)));
        assert!(events.lock().expect("event lock").iter().any(|event| matches!(event.kind, JobEventKind::Failed { ref error } if error.contains("unavailable"))));
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn event_sink_failure_stops_before_supervisor_and_persists_failed_state() {
        let supervisor = FakeSupervisor::new(vec![]);
        let requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![]);
        let directory = home("event-sink-failure");

        let error = orchestrator(supervisor, executor, &directory)
            .with_event_sink(FailingEventSink)
            .run(job_request())
            .await
            .expect_err("event failure should stop the job");

        assert!(matches!(error, OrchestrationError::Event(_)));
        assert!(requests.lock().expect("supervisor request lock").is_empty());
        let state = StateStore::new(&LyaHome::from_path(&directory))
            .load()
            .expect("state should load");
        assert_eq!(
            state
                .jobs
                .get("test-job")
                .expect("job should persist")
                .status,
            JobStatus::Failed
        );
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
    async fn publication_events_follow_guarded_publish_stages_in_order() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Publish event test".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session-1"), "Done."))]);
        let publisher = FakePublisher::new(vec![Ok(published_result("Publish event test"))]);
        let directory = home("publish-events");
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let result = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(&directory)),
        )
        .with_publisher(publisher)
        .with_event_sink(sink)
        .run(job_request())
        .await
        .expect("publication should finish");

        assert_eq!(result.status, JobStatus::Published);
        let stages = events
            .lock()
            .expect("event lock")
            .iter()
            .filter_map(|event| match &event.kind {
                JobEventKind::PublishStageChanged { stage } => Some(stage.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            stages,
            vec![
                PublishStage::Verifying,
                PublishStage::Staging,
                PublishStage::Staged,
                PublishStage::Committing,
                PublishStage::Committed,
                PublishStage::Pushing,
                PublishStage::Pushed,
            ]
        );
        assert!(
            events
                .lock()
                .expect("event lock")
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::Published { .. }))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn event_sink_never_causes_publication_before_acceptance() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Human {
            reason: "Need approval.".to_owned(),
        })]);
        let executor = FakeExecutor::new(vec![]);
        let publisher = FakePublisher::new(vec![Ok(published_result("must not publish"))]);
        let requests = publisher.requests.clone();
        let directory = home("event-no-publish-before-accept");

        let result = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(&directory)),
        )
        .with_publisher(publisher)
        .with_event_sink(RecordingEventSink::default())
        .run(job_request())
        .await
        .expect("human waiting is normal");

        assert_eq!(result.status, JobStatus::WaitingHuman);
        assert!(requests.lock().expect("publisher request lock").is_empty());
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
        .with_event_sink(JsonlEventSink::for_job(&state_home, "local-end-to-end"))
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
        let event_log = fs::read_to_string(
            state_home
                .join("jobs")
                .join("local-end-to-end")
                .join("events.jsonl"),
        )
        .expect("event log should be readable");
        let events = event_log
            .lines()
            .map(|line| serde_json::from_str::<JobEvent>(line).expect("event line should be JSON"))
            .collect::<Vec<_>>();
        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::Published { .. }))
        );
        fs::remove_dir_all(work.parent().expect("work should have parent"))
            .expect("Git test directory should be removed");
        fs::remove_dir_all(state_home).expect("state home should be removed");
    }
}
