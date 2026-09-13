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
    control::{ControlCommand, ControlReceiver},
    events::{
        EventSink, EventSinkError, JobEvent, JobEventKind, NoopEventSink, RepositorySummary,
        executor_event_kind, supervisor_event_fields,
    },
    executor::{Executor, ExecutorError, ExecutorRequest, ExecutorSession},
    lock::{JobLock, LockError},
    publisher::{
        GitPublishConfig, PublishError, PublishProgress, PublishRecoveryRequest, PublishRequest,
        PublishResult, PublishStage, Publisher,
    },
    repository::{RepositoryError, RepositoryState},
    resume::{ResumeContinuation, ResumePlan, ResumeRejection},
    state::{
        JobPhase, JobState, JobStatus, MAX_ACTIVE_USER_INSTRUCTION_BYTES,
        MAX_ACTIVE_USER_INSTRUCTIONS, PendingOperation, QuotaSource, QuotaWait, RunConfiguration,
        StateError, StateStore, current_unix_seconds,
    },
    supervisor::{Project, Supervisor, SupervisorDecision, SupervisorError, SupervisorRequest},
};

pub use super::state::{DEFAULT_MAX_ITERATIONS, DEFAULT_MAX_JOBS};

static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    pub job_id: String,
    pub project: Project,
    pub task: String,
    pub private_context: String,
    /// Position of this job in its sequential chain. It is part of the request so the very first
    /// authoritative write of a child job already carries it: `max_jobs` accounting must survive a
    /// crash between that write and the end of the child's run.
    pub sequential_index: u32,
}

impl NewJob {
    /// The first job of a chain.
    pub fn new(
        job_id: impl Into<String>,
        project: Project,
        task: impl Into<String>,
        private_context: impl Into<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            project,
            task: task.into(),
            private_context: private_context.into(),
            sequential_index: 0,
        }
    }
}

/// The next thing the loop should do. Derived either from a fresh start or from authoritative
/// persisted state at resume; never reconstructed from the event log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// `fresh` distinguishes a new iteration from retrying a review that was already counted.
    Supervise {
        fresh: bool,
    },
    Execute,
    Publish,
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
    publish_configuration: Option<GitPublishConfig>,
    control: ControlReceiver,
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
            publish_configuration: None,
            control: ControlReceiver::disabled(),
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

    /// Records the Git configuration a later process needs to continue publication. It carries no
    /// secrets: push authentication stays with the machine's own Git setup.
    pub fn with_publish_configuration(mut self, configuration: GitPublishConfig) -> Self {
        self.publish_configuration = Some(configuration);
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
            publish_configuration: self.publish_configuration,
            control: self.control,
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
            publish_configuration: self.publish_configuration,
            control: self.control,
        }
    }

    pub fn with_control_receiver(mut self, control: ControlReceiver) -> Self {
        self.control = control;
        self
    }
}

impl<S: Supervisor, E: Executor, R: ProcessRunner, P: Publisher, N: EventSink>
    AutonomousOrchestrator<S, E, R, P, N>
{
    fn run_configuration(&self) -> RunConfiguration {
        RunConfiguration {
            max_iterations: self.max_iterations,
            max_jobs: self.max_jobs,
            browser: self.browser,
            publish: self.publisher.is_enabled(),
            git: self.publish_configuration.clone(),
        }
    }

    fn validate_request(&self, request: &NewJob) -> Result<(), OrchestrationError> {
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
        Ok(())
    }

    pub async fn run(&self, request: NewJob) -> Result<JobState, OrchestrationError> {
        self.validate_request(&request)?;

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
            request.job_id.clone(),
            request.project.name.clone(),
            request.project.path.clone(),
            request.task.clone(),
        );
        job.run = self.run_configuration();
        job.sequential_index = request.sequential_index;
        job.last_repository_state = Some(initial_repository_state.clone());
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

        self.drive(
            job,
            &request.project,
            &request.private_context,
            initial_repository_state,
            Stage::Supervise { fresh: true },
        )
        .await
    }

    /// Continue a job persisted by an earlier process.
    ///
    /// Reality is compared against authoritative job state before any provider call or Git write.
    /// Anything that cannot be proven parks the job in `WAITING_HUMAN` with a precise reason.
    pub async fn resume(
        &self,
        job: JobState,
        private_context: String,
    ) -> Result<JobState, OrchestrationError> {
        if private_context.trim().is_empty() {
            return Err(OrchestrationError::InvalidTransition(
                "private context cannot be empty".to_owned(),
            ));
        }
        let project = Project {
            name: job.project_name.clone(),
            path: job.project_path.clone(),
        };
        let mut job = job;
        self.emit(
            &job,
            JobEventKind::ResumeStarted {
                status: job.status.label().to_owned(),
                pending_operation: job
                    .pending_operation
                    .map(|operation| operation.label().to_owned()),
            },
        )?;

        let plan = match ResumePlan::for_job(&job) {
            Ok(plan) => plan,
            Err(rejection) => {
                // A terminal status keeps its own semantics: it is already a decided outcome and
                // is never rewritten. Anything else is persisted state that no continuation can be
                // proven from, so the job is parked for a human instead of being left advertised
                // as resumable forever.
                if matches!(rejection, ResumeRejection::NotResumable { .. }) {
                    self.emit(
                        &job,
                        JobEventKind::ResumeRejected {
                            reason: rejection.to_string(),
                        },
                    )?;
                } else {
                    self.wait_for_human(&mut job, rejection.to_string()).await?;
                }
                return Err(OrchestrationError::Resume(rejection));
            }
        };

        let repository_state =
            match RepositoryState::collect(&self.repository_runner, &job.project_path).await {
                Ok(state) => state,
                Err(error) => {
                    self.emit(
                        &job,
                        JobEventKind::ResumeRejected {
                            reason: error.to_string(),
                        },
                    )?;
                    return Err(OrchestrationError::Repository(error));
                }
            };

        // Publication recovery proves its own invariants against the accepted snapshot, including
        // a staged index or an existing commit, so it is not required to match the last capture.
        if plan.continuation != ResumeContinuation::Publication {
            match &job.last_repository_state {
                Some(persisted) if persisted == &repository_state => {}
                Some(_) => {
                    let reason = format!(
                        "the repository at {} changed while Lya was not running; resume cannot continue safely",
                        job.project_path.display()
                    );
                    return self.wait_for_human(&mut job, reason).await;
                }
                None => {
                    return self
                        .wait_for_human(
                            &mut job,
                            "no repository snapshot was persisted for this job, so an unchanged repository cannot be proven".to_owned(),
                        )
                        .await;
                }
            }
        }

        let stage = match plan.continuation {
            ResumeContinuation::Fresh => Stage::Supervise { fresh: true },
            ResumeContinuation::Supervisor => Stage::Supervise { fresh: false },
            ResumeContinuation::Executor => Stage::Execute,
            ResumeContinuation::Publication => Stage::Publish,
        };
        self.emit(
            &job,
            JobEventKind::ResumeValidated {
                continuation: plan.continuation.label().to_owned(),
                head: repository_state.head.clone(),
            },
        )?;
        if let Some(quota) = &job.quota_wait {
            self.emit(
                &job,
                JobEventKind::QuotaRetryStarted {
                    provider: quota.provider.clone(),
                    operation: quota.operation.label().to_owned(),
                },
            )?;
        }
        job.quota_wait = None;
        job.status = JobStatus::Running;
        job.touch();
        self.persist(&job)?;

        self.drive(job, &project, &private_context, repository_state, stage)
            .await
    }

    async fn drive(
        &self,
        mut job: JobState,
        project: &Project,
        private_context: &str,
        mut repository_state: RepositoryState,
        mut stage: Stage,
    ) -> Result<JobState, OrchestrationError> {
        loop {
            if !self.safe_point(&mut job).await? {
                return Ok(job);
            }
            match stage {
                Stage::Supervise { fresh } => {
                    if fresh {
                        let limit = job.run.max_iterations;
                        if job.iteration >= limit {
                            self.fail(
                                &mut job,
                                format!("autonomous job reached its iteration limit of {limit}"),
                            )?;
                            return Err(OrchestrationError::IterationLimit { limit });
                        }
                        job.iteration += 1;
                    }
                    job.status = JobStatus::Running;
                    job.phase = JobPhase::Supervisor;
                    job.pending_operation = Some(PendingOperation::SupervisorReview);
                    job.touch();
                    self.persist(&job)?;

                    let supervisor_request = SupervisorRequest {
                        private_context: private_context.to_owned(),
                        project: project.clone(),
                        task: job.task.clone(),
                        phase: Some("supervisor review".to_owned()),
                        iteration: job.iteration,
                        executor_report: job.last_executor_report.clone(),
                        repository_state: Some(repository_state.render_for_supervisor()),
                        user_instructions: job.applied_user_instructions.clone(),
                        cancellation: Some(self.control.cancellation()),
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
                            if !self.safe_point(&mut job).await? {
                                return Ok(job);
                            }
                            if let Some((source, detail)) = supervisor_quota(&error) {
                                self.wait_for_quota(
                                    &mut job,
                                    "OpenAI",
                                    PendingOperation::SupervisorReview,
                                    source,
                                    detail,
                                )?;
                                return Ok(job);
                            }
                            self.fail(&mut job, error.to_string())?;
                            return Err(OrchestrationError::Supervisor(error));
                        }
                    };
                    if !self.safe_point(&mut job).await? {
                        return Ok(job);
                    }
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
                        SupervisorDecision::Claude { .. } => {
                            job.phase = JobPhase::Executor;
                            job.pending_operation = Some(PendingOperation::ExecutorRun);
                            job.touch();
                            self.persist(&job)?;
                            stage = Stage::Execute;
                        }
                        SupervisorDecision::Accept { .. } => {
                            job.accepted_repository_state = Some(repository_state.clone());
                            if !self.publisher.is_enabled() {
                                job.status = JobStatus::Accepted;
                                job.pending_operation = None;
                                job.touch();
                                self.persist(&job)?;
                                self.emit(
                                    &job,
                                    JobEventKind::JobFinished {
                                        status: "ACCEPTED".to_owned(),
                                    },
                                )?;
                                return Ok(job);
                            }
                            // Nothing is written here on purpose. `publish` performs the single
                            // write that records an owed publication, so `ACCEPTED` together with
                            // a pending publication — a state no resume can continue — is never
                            // persistable. A crash before that write simply leaves the job at its
                            // already persisted supervisor review, which resumes safely.
                            stage = Stage::Publish;
                        }
                        SupervisorDecision::Human { reason } => {
                            job.status = JobStatus::WaitingHuman;
                            job.pending_operation = None;
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
                            job.pending_operation = None;
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
                Stage::Execute => {
                    let Some(SupervisorDecision::Claude { prompt, .. }) =
                        job.last_supervisor_decision.clone()
                    else {
                        let error =
                            "executor work was requested without a recorded Claude decision"
                                .to_owned();
                        self.fail(&mut job, error.clone())?;
                        return Err(OrchestrationError::InvalidTransition(error));
                    };
                    let session = match (&job.last_executor_report, &job.claude_session_id) {
                        (Some(_), Some(session_id)) if !session_id.trim().is_empty() => {
                            ExecutorSession::Resume(session_id.clone())
                        }
                        (Some(_), _) => {
                            let error = "supervisor requested another Claude pass, but the previous Claude result has no resumable session ID".to_owned();
                            self.fail(&mut job, error.clone())?;
                            return Err(OrchestrationError::InvalidTransition(error));
                        }
                        (None, _) => match &job.claude_session_id {
                            // A resumed job with a known session continues that exact session.
                            Some(session_id) if !session_id.trim().is_empty() => {
                                ExecutorSession::Resume(session_id.clone())
                            }
                            _ => ExecutorSession::New,
                        },
                    };
                    let executor_request = ExecutorRequest {
                        project_name: job.project_name.clone(),
                        project_path: job.project_path.clone(),
                        prompt,
                        session,
                        browser: job.run.browser,
                        timeout: None,
                        user_instructions: job.applied_user_instructions.clone(),
                        cancellation: Some(self.control.cancellation()),
                    };
                    if !self.safe_point(&mut job).await? {
                        return Ok(job);
                    }
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
                            if !self.safe_point(&mut job).await? {
                                return Ok(job);
                            }
                            if let Some((source, detail)) = executor_quota(&error) {
                                self.wait_for_quota(
                                    &mut job,
                                    "Claude",
                                    PendingOperation::ExecutorRun,
                                    source,
                                    detail,
                                )?;
                                return Ok(job);
                            }
                            self.fail(&mut job, error.to_string())?;
                            return Err(OrchestrationError::Executor(error));
                        }
                    };
                    // The completed run is made durable before any interruptible boundary. A
                    // pause, an input EOF or a crash arriving the instant Claude returned must not
                    // discard the session and report, and must never make the next process replay
                    // an execution that already finished.
                    if let Some(session_id) = result
                        .session_id
                        .clone()
                        .filter(|value| !value.trim().is_empty())
                    {
                        job.claude_session_id = Some(session_id);
                    }
                    job.last_executor_report = Some(result.final_response.clone());
                    job.phase = JobPhase::Supervisor;
                    job.pending_operation = None;
                    job.touch();
                    self.persist(&job)?;
                    self.emit(&job, executor_event_kind(result))?;

                    repository_state =
                        match RepositoryState::collect(&self.repository_runner, &job.project_path)
                            .await
                        {
                            Ok(state) => state,
                            Err(error) => {
                                self.fail(&mut job, error.to_string())?;
                                return Err(OrchestrationError::Repository(error));
                            }
                        };
                    job.last_repository_state = Some(repository_state.clone());
                    job.touch();
                    self.persist(&job)?;
                    self.emit(
                        &job,
                        JobEventKind::RepositoryCaptured {
                            summary: RepositorySummary::from(&repository_state),
                        },
                    )?;
                    if !self.safe_point(&mut job).await? {
                        return Ok(job);
                    }
                    stage = Stage::Supervise { fresh: true };
                }
                Stage::Publish => return self.publish(job).await,
            }
        }
    }

    async fn publish(&self, mut job: JobState) -> Result<JobState, OrchestrationError> {
        let Some(accepted_repository_state) = job.accepted_repository_state.clone() else {
            return self
                .wait_for_human(
                    &mut job,
                    "publication was requested without a persisted accepted repository snapshot"
                        .to_owned(),
                )
                .await;
        };
        let Some(SupervisorDecision::Accept { commit_title, .. }) =
            job.last_supervisor_decision.clone()
        else {
            return self
                .wait_for_human(
                    &mut job,
                    "publication was requested without a recorded ACCEPT decision".to_owned(),
                )
                .await;
        };
        // The single authoritative write that records an owed publication. Status, phase, pending
        // operation and the accepted snapshot enter persisted state together, so every state a
        // crash can leave behind here is one `ResumePlan` can continue.
        job.status = JobStatus::Publishing;
        job.phase = JobPhase::Publisher;
        job.pending_operation = Some(PendingOperation::Publication);
        job.touch();
        self.persist(&job)?;
        self.emit(
            &job,
            JobEventKind::PublishStarted {
                commit_title: commit_title.clone(),
            },
        )?;

        // A recorded stage or commit means a previous process was already inside the guarded
        // sequence, so recovery must prove what exists before anything else happens.
        let recovering = job.publish_stage.is_some() || job.publish_result.is_some();
        let publish = {
            let mut progress = JobPublicationProgress {
                job: &mut job,
                state_store: &self.state_store,
                event_sink: &self.event_sink,
                control: &self.control,
            };
            if recovering {
                let request = PublishRecoveryRequest {
                    project_path: progress.job.project_path.clone(),
                    accepted_repository_state,
                    commit_title,
                    recorded_stage: progress.job.publish_stage.clone(),
                    recorded_result: progress.job.publish_result.clone(),
                };
                self.publisher.recover(request, &mut progress).await
            } else {
                let request = PublishRequest {
                    project_path: progress.job.project_path.clone(),
                    accepted_repository_state,
                    commit_title,
                };
                self.publisher.publish(request, &mut progress).await
            }
        };

        match publish {
            Ok(result) => {
                job.publish_result = Some(result);
                if !self.safe_point(&mut job).await? {
                    return Ok(job);
                }
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
                job.pending_operation = None;
                job.last_repository_state = Some(post_commit_state.clone());
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
                Ok(job)
            }
            Err(error) => {
                if !self.safe_point(&mut job).await? {
                    return Ok(job);
                }
                if let PublishError::RecoveryAmbiguous(_) = &error {
                    return self.wait_for_human(&mut job, error.to_string()).await;
                }
                if let PublishError::PushRejected(result) = &error {
                    job.publish_result = Some(result.clone());
                }
                self.fail(&mut job, error.to_string())?;
                Err(OrchestrationError::Publish(error))
            }
        }
    }

    pub async fn run_sequential(&self, request: NewJob) -> Result<RunResult, OrchestrationError> {
        if self.max_jobs == 0 {
            return Err(OrchestrationError::InvalidTransition(
                "max jobs must be greater than zero".to_owned(),
            ));
        }
        let job = self.run(request.clone()).await?;
        self.continue_sequentially(request, job).await
    }

    /// Resume one persisted job and then continue its sequential chain.
    pub async fn resume_sequential(
        &self,
        job: JobState,
        private_context: String,
    ) -> Result<RunResult, OrchestrationError> {
        let request = NewJob {
            job_id: job.job_id.clone(),
            project: Project {
                name: job.project_name.clone(),
                path: job.project_path.clone(),
            },
            task: job.task.clone(),
            private_context: private_context.clone(),
            // The resumed job keeps the chain position it was persisted with, so a job resumed
            // after a crash still counts against the original `max_jobs` budget.
            sequential_index: job.sequential_index,
        };
        let job = self.resume(job, private_context).await?;
        self.continue_sequentially(request, job).await
    }

    async fn continue_sequentially(
        &self,
        request: NewJob,
        first: JobState,
    ) -> Result<RunResult, OrchestrationError> {
        let mut current_request = request;
        let mut jobs = Vec::new();
        let mut job = first;
        loop {
            let sequential_index = job.sequential_index;
            let next_prompt = match &job.last_supervisor_decision {
                Some(SupervisorDecision::Accept {
                    next_prompt: Some(next_prompt),
                    ..
                }) if job.status == JobStatus::Published => Some(next_prompt.clone()),
                _ => None,
            };
            let max_jobs = job.run.max_jobs;
            jobs.push(job);
            if self.control.stop_requested() {
                return Ok(RunResult {
                    jobs,
                    max_jobs_reached: false,
                });
            }
            let Some(task) = next_prompt else {
                return Ok(RunResult {
                    jobs,
                    max_jobs_reached: false,
                });
            };
            if sequential_index + 1 >= max_jobs {
                return Ok(RunResult {
                    jobs,
                    max_jobs_reached: true,
                });
            }
            current_request = NewJob {
                job_id: new_job_id(),
                project: current_request.project,
                task,
                private_context: current_request.private_context,
                sequential_index: sequential_index + 1,
            };
            // A sequential job is a job in its own right, so it gets its own exclusive claim
            // before anything is persisted for it. The caller's lock covers only the job it
            // named; without this a second process could drive or resume this child while this
            // process is still working on it. The claim is released at the end of this iteration,
            // once the child has finished and is no longer being driven.
            let _child_lock = JobLock::acquire(&self.state_store, &current_request.job_id)
                .map_err(OrchestrationError::Lock)?;
            // `/send` instructions constrain the job they were given to; a new sequential job
            // starts from the persisted task alone.
            job = self.run(current_request.clone()).await?;
        }
    }

    fn persist(&self, job: &JobState) -> Result<(), OrchestrationError> {
        self.state_store
            .save_job(job)
            .map_err(OrchestrationError::State)
    }

    async fn safe_point(&self, job: &mut JobState) -> Result<bool, OrchestrationError> {
        for command in self.control.drain().await {
            self.handle_control_command(job, command).await?;
        }
        if self.control.stop_requested() {
            self.stop(job).await?;
            return Ok(false);
        }
        if !self.control.pause_requested() {
            return Ok(true);
        }

        job.status = JobStatus::Paused;
        job.touch();
        self.persist(job)?;
        self.emit(
            job,
            JobEventKind::Paused {
                reason: "safe boundary reached".to_owned(),
            },
        )?;
        loop {
            let Some(command) = self.control.next().await else {
                self.leave_paused_for_a_later_process(job)?;
                return Ok(false);
            };
            self.handle_control_command(job, command).await?;
            if self.control.stop_requested() {
                self.stop(job).await?;
                return Ok(false);
            }
            if !self.control.pause_requested() {
                job.status = JobStatus::Running;
                job.touch();
                self.persist(job)?;
                self.emit(job, JobEventKind::Resumed)?;
                return Ok(true);
            }
        }
    }

    async fn handle_control_command(
        &self,
        job: &mut JobState,
        command: ControlCommand,
    ) -> Result<(), OrchestrationError> {
        match command {
            ControlCommand::Pause => {
                if job.status != JobStatus::Paused {
                    self.emit(job, JobEventKind::PauseRequested)?;
                }
            }
            ControlCommand::Resume => {}
            ControlCommand::Stop => self.emit(job, JobEventKind::StopRequested)?,
            ControlCommand::Send(instruction) => {
                if let Some(reason) = instruction_budget_refusal(job, &instruction) {
                    self.emit(
                        job,
                        JobEventKind::UserInstructionRejected {
                            instruction,
                            reason,
                        },
                    )?;
                    return Ok(());
                }
                job.pending_user_instructions.push(instruction.clone());
                job.touch();
                self.persist(job)?;
                self.emit(job, JobEventKind::UserInstructionQueued { instruction })?;
            }
            ControlCommand::Status => self.emit(
                job,
                JobEventKind::StatusReported {
                    status: job.status.label().to_owned(),
                    phase: job.phase.label().to_owned(),
                    claude_session_id: job.claude_session_id.clone(),
                    publish_stage: job.publish_stage.clone(),
                    pause_requested: self.control.pause_requested(),
                    stop_requested: self.control.stop_requested(),
                },
            )?,
            ControlCommand::Diff => {
                match RepositoryState::collect(&self.repository_runner, &job.project_path).await {
                    Ok(state) => self.emit(
                        job,
                        JobEventKind::DiffReported {
                            summary: RepositorySummary::from(&state),
                        },
                    )?,
                    Err(error) => self.emit(
                        job,
                        JobEventKind::ControlMessage {
                            message: format!("could not collect diff: {error}"),
                        },
                    )?,
                }
            }
        }
        self.apply_queued_instructions(job)
    }

    fn apply_queued_instructions(&self, job: &mut JobState) -> Result<(), OrchestrationError> {
        while let Some(instruction) = job.pending_user_instructions.first().cloned() {
            job.pending_user_instructions.remove(0);
            job.applied_user_instructions.push(instruction.clone());
            job.touch();
            self.persist(job)?;
            self.emit(job, JobEventKind::UserInstructionApplied { instruction })?;
        }
        Ok(())
    }

    async fn stop(&self, job: &mut JobState) -> Result<(), OrchestrationError> {
        if job.status == JobStatus::Stopped {
            return Ok(());
        }
        if let Ok(state) =
            RepositoryState::collect(&self.repository_runner, &job.project_path).await
        {
            job.last_repository_state = Some(state.clone());
            self.emit(
                job,
                JobEventKind::RepositoryCaptured {
                    summary: RepositorySummary::from(&state),
                },
            )?;
        }
        job.status = JobStatus::Stopped;
        job.touch();
        self.persist(job)?;
        self.emit(
            job,
            JobEventKind::Stopped {
                reason: match &job.publish_result {
                    Some(result) => format!(
                        "stopped by user with local commit {} recorded as {}; repository changes were preserved",
                        result.commit_sha,
                        push_status_label(&result.push_status)
                    ),
                    None => "stopped by user; repository changes were preserved".to_owned(),
                },
            },
        )?;
        self.emit(
            job,
            JobEventKind::JobFinished {
                status: "STOPPED".to_owned(),
            },
        )
    }

    /// Input ended while the job was paused. Nobody asked to stop, so the job stays `PAUSED` and
    /// remains resumable by a later process; only an explicit `/stop` or Ctrl+C is terminal.
    fn leave_paused_for_a_later_process(
        &self,
        job: &mut JobState,
    ) -> Result<(), OrchestrationError> {
        job.status = JobStatus::Paused;
        job.touch();
        self.persist(job)?;
        self.emit(
            job,
            JobEventKind::ControlMessage {
                message: format!(
                    "input ended while paused; the job stays resumable with: lya resume --job {}",
                    job.job_id
                ),
            },
        )?;
        self.emit(
            job,
            JobEventKind::JobFinished {
                status: "PAUSED".to_owned(),
            },
        )
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

    async fn wait_for_human(
        &self,
        job: &mut JobState,
        reason: String,
    ) -> Result<JobState, OrchestrationError> {
        job.status = JobStatus::WaitingHuman;
        job.touch();
        self.persist(job)?;
        self.emit(
            job,
            JobEventKind::ResumeRejected {
                reason: reason.clone(),
            },
        )?;
        self.emit(job, JobEventKind::WaitingForHuman { reason })?;
        self.emit(
            job,
            JobEventKind::JobFinished {
                status: "WAITING_HUMAN".to_owned(),
            },
        )?;
        Ok(job.clone())
    }

    fn wait_for_quota(
        &self,
        job: &mut JobState,
        provider: &str,
        operation: PendingOperation,
        source: QuotaSource,
        reason: String,
    ) -> Result<(), OrchestrationError> {
        job.status = if provider == "Claude" {
            JobStatus::WaitingClaudeQuota
        } else {
            JobStatus::WaitingOpenAiQuota
        };
        job.pending_operation = Some(operation);
        job.quota_wait = Some(QuotaWait {
            provider: provider.to_owned(),
            operation,
            source,
            reason: reason.clone(),
            detected_unix_seconds: current_unix_seconds(),
        });
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
                status: job.status.label().to_owned(),
            },
        )
    }
}

fn instruction_budget_refusal(job: &JobState, instruction: &str) -> Option<String> {
    if job.active_instruction_count() >= MAX_ACTIVE_USER_INSTRUCTIONS {
        return Some(format!(
            "this job already holds the maximum of {MAX_ACTIVE_USER_INSTRUCTIONS} active instructions"
        ));
    }
    let total = job.active_instruction_bytes() + instruction.len();
    if total > MAX_ACTIVE_USER_INSTRUCTION_BYTES {
        return Some(format!(
            "active instructions would reach {total} bytes, above the {MAX_ACTIVE_USER_INSTRUCTION_BYTES} byte limit for one job"
        ));
    }
    None
}

fn push_status_label(status: &super::publisher::PushStatus) -> &'static str {
    match status {
        super::publisher::PushStatus::Pending => "committed but not pushed",
        super::publisher::PushStatus::Pushed => "pushed",
        super::publisher::PushStatus::Rejected => "rejected by the remote",
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

/// A quota condition is recognised at the provider boundary, where Lya still knows which text came
/// from the provider's own diagnostics and which text is task or model content.
///
/// Nothing is re-classified here from a rendered error: that string carries the task, the prompt
/// and Claude's own answer, so a normal failure about rate limiting would otherwise be mistaken for
/// an exhausted quota. A provider failure that is not classified as a quota stays a normal failure.
fn supervisor_quota(error: &SupervisorError) -> Option<(QuotaSource, String)> {
    match error {
        SupervisorError::QuotaExceeded { source, detail } => Some((*source, detail.clone())),
        _ => None,
    }
}

fn executor_quota(error: &ExecutorError) -> Option<(QuotaSource, String)> {
    match error {
        ExecutorError::QuotaExceeded { source, detail } => Some((*source, detail.clone())),
        _ => None,
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
    ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>> {
        Box::pin(async { Err(PublishError::PublishingDisabled) })
    }

    fn recover<'a>(
        &'a self,
        _request: PublishRecoveryRequest,
        _progress: &'a mut dyn PublishProgress,
    ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>> {
        Box::pin(async { Err(PublishError::PublishingDisabled) })
    }
}

struct JobPublicationProgress<'a> {
    job: &'a mut JobState,
    state_store: &'a StateStore,
    event_sink: &'a dyn EventSink,
    control: &'a ControlReceiver,
}

impl JobPublicationProgress<'_> {
    fn persist(&self) -> Result<(), PublishError> {
        self.state_store
            .save_job(self.job)
            .map_err(|error| PublishError::ProgressPersistence(error.to_string()))
    }

    fn emit(&self, kind: JobEventKind) -> Result<(), PublishError> {
        let project = Project {
            name: self.job.project_name.clone(),
            path: self.job.project_path.clone(),
        };
        self.event_sink
            .emit(&JobEvent::new(
                &self.job.job_id,
                &project,
                Some(self.job.iteration),
                kind,
            ))
            .map_err(|error| PublishError::ProgressPersistence(error.to_string()))
    }
}

impl PublishProgress for JobPublicationProgress<'_> {
    fn record(&mut self, stage: PublishStage) -> Result<(), PublishError> {
        self.job.publish_stage = Some(stage.clone());
        self.job.touch();
        self.persist()?;
        self.emit(JobEventKind::PublishStageChanged { stage })
    }

    fn record_commit(&mut self, result: &PublishResult) -> Result<(), PublishError> {
        self.job.publish_result = Some(result.clone());
        self.job.touch();
        self.persist()
    }

    fn stop_requested(&self) -> bool {
        self.control.stop_requested()
    }
}

pub fn new_job_id() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let sequence = NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed);
    format!("job-{seconds}-{}-{sequence}", std::process::id())
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
    Resume(ResumeRejection),
    State(StateError),
    Lock(LockError),
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
            Self::Resume(error) => write!(formatter, "cannot resume job: {error}"),
            Self::State(error) => write!(formatter, "state error: {error}"),
            Self::Lock(error) => write!(formatter, "job lock error: {error}"),
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
            Self::Resume(error) => Some(error),
            Self::State(error) => Some(error),
            Self::Lock(error) => Some(error),
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
            control::{ControlCommand, ControlReceiver, ControlSender},
            events::{EventSink, JobEvent, JobEventKind, JsonlEventSink},
            executor::{Executor, ExecutorError, ExecutorRequest, ExecutorResult, ExecutorSession},
            home::LyaHome,
            lock::JobLock,
            publisher::{
                GitPublishConfig, GitPublisher, PublishError, PublishProgress,
                PublishRecoveryRequest, PublishRequest, PublishResult, PublishStage, Publisher,
                PushStatus,
            },
            repository::RepositoryState,
            state::{
                DEFAULT_MAX_JOBS, JobState, JobStatus, MAX_ACTIVE_USER_INSTRUCTIONS,
                PendingOperation, QuotaSource, QuotaWait, StateStore,
            },
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

    struct PauseSignalSink {
        events: Arc<Mutex<Vec<JobEvent>>>,
        paused: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl EventSink for PauseSignalSink {
        fn emit(&self, event: &JobEvent) -> Result<(), super::EventSinkError> {
            self.events.lock().expect("event lock").push(event.clone());
            if matches!(event.kind, JobEventKind::Paused { .. })
                && let Some(sender) = self.paused.lock().expect("paused lock").take()
            {
                let _ = sender.send(());
            }
            Ok(())
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

    struct BlockingExecutor {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<ExecutorResult>>>,
        requests: Arc<Mutex<Vec<ExecutorRequest>>>,
    }

    impl Executor for BlockingExecutor {
        fn execute(
            &self,
            request: ExecutorRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ExecutorResult, ExecutorError>> + Send + '_>>
        {
            self.requests.lock().expect("request lock").push(request);
            let started = self.started.lock().expect("started lock").take();
            let release = self.release.lock().expect("release lock").take();
            Box::pin(async move {
                if let Some(sender) = started {
                    let _ = sender.send(());
                }
                Ok(release
                    .expect("release should exist")
                    .await
                    .expect("result should be released"))
            })
        }
    }

    struct StopOnAcceptSink {
        events: Arc<Mutex<Vec<JobEvent>>>,
        sender: ControlSender,
    }

    impl EventSink for StopOnAcceptSink {
        fn emit(&self, event: &JobEvent) -> Result<(), super::EventSinkError> {
            self.events.lock().expect("event lock").push(event.clone());
            if matches!(event.kind, JobEventKind::SupervisorFinished { ref action, .. } if action == "ACCEPT")
            {
                self.sender.request_stop();
            }
            Ok(())
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
        recoveries: Arc<Mutex<Vec<PublishRecoveryRequest>>>,
    }

    impl FakePublisher {
        fn new(results: Vec<Result<PublishResult, PublishError>>) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
                recoveries: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Publisher for FakePublisher {
        fn recover<'a>(
            &'a self,
            request: PublishRecoveryRequest,
            progress: &'a mut dyn PublishProgress,
        ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>>
        {
            self.recoveries
                .lock()
                .expect("publisher recovery lock")
                .push(request.clone());
            let publish_request = PublishRequest {
                project_path: request.project_path,
                accepted_repository_state: request.accepted_repository_state,
                commit_title: request.commit_title,
            };
            self.publish(publish_request, progress)
        }

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
        NewJob::new(
            "test-job",
            Project {
                name: "test-project".to_owned(),
                path: std::env::temp_dir(),
            },
            "Update hello.txt",
            "Private project context.",
        )
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
            .load_job(&result.job_id)
            .expect("state should load");
        assert_eq!(stored, Some(result.clone()));
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
            .load_all()
            .expect("state should load");
        assert_eq!(
            stored.first().expect("job should persist").status,
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
        // The classification itself belongs to the provider boundary; the loop reacts only to a
        // failure the executor already classified as a quota.
        let executor = FakeExecutor::new(vec![Err(ExecutorError::QuotaExceeded {
            source: QuotaSource::ProviderMessageHeuristic,
            detail: "Claude rate limit reached".to_owned(),
        })]);
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
            .load_job("test-job")
            .expect("state should load")
            .expect("job should persist");
        assert_eq!(state.status, JobStatus::Failed);
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
            .load_all()
            .expect("state should load");
        assert_eq!(
            stored.first().expect("job should persist").status,
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
            .load_all()
            .expect("state should load");
        assert_eq!(
            stored.first().expect("job should persist").status,
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
            .load_all()
            .expect("state should load");
        assert_eq!(
            stored.first().expect("job should persist").status,
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
        .run_sequential(NewJob::new(
            "local-end-to-end",
            Project {
                name: "local-end-to-end".to_owned(),
                path: work.clone(),
            },
            "Add one line containing LYA_PUBLISH_OK to hello.txt and verify it.",
            "Test context.",
        ))
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

    #[tokio::test]
    async fn queued_instructions_preserve_order_reach_prompts_and_emit_events() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement the requested update.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Update documentation".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session"), "Done."))]);
        let executor_requests = executor.requests.clone();
        let directory = home("instructions");
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let (sender, receiver) = ControlReceiver::new();
        sender
            .send(ControlCommand::Send(
                "Keep the public format stable.".to_owned(),
            ))
            .expect("instruction should queue");
        sender
            .send(ControlCommand::Send("Add tests for edge cases.".to_owned()))
            .expect("instruction should queue");

        orchestrator(supervisor, executor, &directory)
            .with_control_receiver(receiver)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("job should complete");

        let expected = vec![
            "Keep the public format stable.".to_owned(),
            "Add tests for edge cases.".to_owned(),
        ];
        assert_eq!(
            supervisor_requests.lock().expect("requests")[0].user_instructions,
            expected
        );
        assert_eq!(
            executor_requests.lock().expect("requests")[0].user_instructions,
            expected
        );
        let events = events.lock().expect("events");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.kind, JobEventKind::UserInstructionQueued { .. }))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.kind, JobEventKind::UserInstructionApplied { .. }))
                .count(),
            2
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn pause_before_supervisor_waits_for_resume_without_duplicate_work() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "No changes".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![]);
        let directory = home("pause-before-supervisor");
        let events = Arc::new(Mutex::new(Vec::new()));
        let (paused_sender, paused_receiver) = tokio::sync::oneshot::channel();
        let sink = PauseSignalSink {
            events: events.clone(),
            paused: Mutex::new(Some(paused_sender)),
        };
        let (sender, receiver) = ControlReceiver::new();
        sender
            .send(ControlCommand::Pause)
            .expect("pause should queue");
        let job = job_request();
        let run = tokio::spawn(async move {
            orchestrator(supervisor, executor, &directory)
                .with_control_receiver(receiver)
                .with_event_sink(sink)
                .run(job)
                .await
        });

        paused_receiver
            .await
            .expect("job should reach paused state");
        assert!(supervisor_requests.lock().expect("requests").is_empty());
        sender
            .send(ControlCommand::Resume)
            .expect("resume should queue");
        let result = run
            .await
            .expect("job task should join")
            .expect("job should complete");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(supervisor_requests.lock().expect("requests").len(), 1);
        assert!(
            events
                .lock()
                .expect("events")
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::PauseRequested))
        );
        fs::remove_dir_all(home("pause-before-supervisor")).ok();
    }

    #[tokio::test]
    async fn stop_before_work_prevents_provider_and_preserves_stopped_state() {
        let supervisor = FakeSupervisor::new(vec![]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![]);
        let directory = home("stop-before-work");
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let (sender, receiver) = ControlReceiver::new();
        sender
            .send(ControlCommand::Stop)
            .expect("stop should queue");

        let result = orchestrator(supervisor, executor, &directory)
            .with_control_receiver(receiver)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("stopping is a normal terminal state");

        assert_eq!(result.status, JobStatus::Stopped);
        assert!(supervisor_requests.lock().expect("requests").is_empty());
        assert!(
            events
                .lock()
                .expect("events")
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::StopRequested))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn status_and_diff_commands_use_authoritative_job_and_repository_state() {
        let supervisor = FakeSupervisor::new(vec![]);
        let executor = FakeExecutor::new(vec![]);
        let directory = home("status-and-diff");
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let (sender, receiver) = ControlReceiver::new();
        sender
            .send(ControlCommand::Status)
            .expect("status should queue");
        sender
            .send(ControlCommand::Diff)
            .expect("diff should queue");
        sender
            .send(ControlCommand::Stop)
            .expect("stop should queue");

        let result = orchestrator(supervisor, executor, &directory)
            .with_control_receiver(receiver)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("stopping is normal");

        assert_eq!(result.status, JobStatus::Stopped);
        let events = events.lock().expect("events");
        assert!(events.iter().any(|event| matches!(event.kind, JobEventKind::StatusReported { ref status, ref phase, .. } if status == "RUNNING" && phase == "SUPERVISOR")));
        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::DiffReported { .. }))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn pause_during_executor_waits_for_its_safe_boundary_then_resumes_once() {
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Implement.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Finish".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        let executor_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = BlockingExecutor {
            started: Mutex::new(Some(started_sender)),
            release: Mutex::new(Some(release_receiver)),
            requests: executor_requests.clone(),
        };
        let directory = home("pause-during-executor");
        let run_directory = directory.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (paused_sender, paused_receiver) = tokio::sync::oneshot::channel();
        let (sender, receiver) = ControlReceiver::new();
        let run = tokio::spawn(async move {
            AutonomousOrchestrator::new(
                supervisor,
                executor,
                FakeGitRunner { dirty: false },
                StateStore::new(&LyaHome::from_path(&run_directory)),
            )
            .with_control_receiver(receiver)
            .with_event_sink(PauseSignalSink {
                events,
                paused: Mutex::new(Some(paused_sender)),
            })
            .run(job_request())
            .await
        });

        started_receiver.await.expect("executor should start");
        sender
            .send(ControlCommand::Pause)
            .expect("pause should queue");
        release_sender
            .send(claude_result(Some("session"), "Completed."))
            .expect("executor should receive result");
        paused_receiver
            .await
            .expect("pause should happen after executor");
        assert_eq!(executor_requests.lock().expect("requests").len(), 1);
        sender
            .send(ControlCommand::Resume)
            .expect("resume should queue");
        let result = run
            .await
            .expect("job should join")
            .expect("job should finish");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(executor_requests.lock().expect("requests").len(), 1);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn stop_after_accept_before_publication_prevents_all_git_writes() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Do not publish".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let executor = FakeExecutor::new(vec![]);
        let publisher = FakePublisher::new(vec![Ok(published_result("Do not publish"))]);
        let publish_requests = publisher.requests.clone();
        let directory = home("stop-before-publish");
        let (sender, receiver) = ControlReceiver::new();
        let events = Arc::new(Mutex::new(Vec::new()));

        let result = AutonomousOrchestrator::new(
            supervisor,
            executor,
            FakeGitRunner { dirty: false },
            StateStore::new(&LyaHome::from_path(&directory)),
        )
        .with_publisher(publisher)
        .with_control_receiver(receiver)
        .with_event_sink(StopOnAcceptSink { events, sender })
        .run(job_request())
        .await
        .expect("stopping is terminal but not an error");

        assert_eq!(result.status, JobStatus::Stopped);
        assert!(
            publish_requests
                .lock()
                .expect("publish requests")
                .is_empty()
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    // --- Milestone 8: crash-safe resume -------------------------------------------------------

    async fn fake_repository_state() -> RepositoryState {
        RepositoryState::collect(&FakeGitRunner { dirty: false }, &std::env::temp_dir())
            .await
            .expect("fake repository state should collect")
    }

    /// A job as a previous process would have persisted it just before the process disappeared.
    async fn persisted_job(status: JobStatus, operation: Option<PendingOperation>) -> JobState {
        let mut job = JobState::new(
            "test-job",
            "test-project",
            std::env::temp_dir(),
            "Update hello.txt",
        );
        job.status = status;
        job.pending_operation = operation;
        job.iteration = 1;
        job.last_repository_state = Some(fake_repository_state().await);
        job
    }

    fn store(directory: &PathBuf) -> StateStore {
        StateStore::new(&LyaHome::from_path(directory))
    }

    /// Persists the job, then reloads it exactly the way a new process would.
    fn round_trip(directory: &PathBuf, job: &JobState) -> JobState {
        let store = store(directory);
        store.save_job(job).expect("job should persist");
        store
            .load_job(&job.job_id)
            .expect("job should reload")
            .expect("job should exist")
    }

    /// Persisted status, pending operation and iteration as seen from inside a provider call.
    type ObservedState = (JobStatus, Option<PendingOperation>, u32);

    struct StateProbeSupervisor {
        store: StateStore,
        observed: Arc<Mutex<Vec<ObservedState>>>,
        decisions: Arc<Mutex<VecDeque<Result<SupervisorDecision, SupervisorError>>>>,
        requests: Arc<Mutex<Vec<SupervisorRequest>>>,
    }

    impl Supervisor for StateProbeSupervisor {
        fn decide(
            &self,
            request: SupervisorRequest,
        ) -> Pin<Box<dyn Future<Output = Result<SupervisorDecision, SupervisorError>> + Send + '_>>
        {
            let persisted = self
                .store
                .load_job("test-job")
                .expect("state should load")
                .expect("state should exist");
            self.observed.lock().expect("observed lock").push((
                persisted.status.clone(),
                persisted.pending_operation,
                persisted.iteration,
            ));
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

    #[tokio::test]
    async fn run_configuration_and_pending_operation_are_persisted_before_each_provider_call() {
        let directory = home("persisted-run-configuration");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let supervisor = StateProbeSupervisor {
            store: store(&directory),
            observed: observed.clone(),
            decisions: Arc::new(Mutex::new(
                vec![Ok(SupervisorDecision::Accept {
                    commit_title: "Persisted".to_owned(),
                    next_prompt: None,
                    reason: None,
                })]
                .into(),
            )),
            requests: Arc::new(Mutex::new(Vec::new())),
        };

        let result = AutonomousOrchestrator::new(
            supervisor,
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_max_iterations(7)
        .with_browser(true)
        .run(job_request())
        .await
        .expect("job should finish");

        assert_eq!(
            observed.lock().expect("observed lock").as_slice(),
            [(
                JobStatus::Running,
                Some(PendingOperation::SupervisorReview),
                1
            )]
        );
        let stored = store(&directory)
            .load_job("test-job")
            .expect("state should load")
            .expect("job should persist");
        assert_eq!(stored.run.max_iterations, 7);
        assert_eq!(stored.run.max_jobs, DEFAULT_MAX_JOBS);
        assert!(stored.run.browser);
        assert!(!stored.run.publish);
        assert_eq!(stored.run.git, None);
        assert_eq!(stored.pending_operation, None);
        assert_eq!(result.status, JobStatus::Accepted);
        assert!(stored.last_repository_state.is_some());
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn input_ending_while_paused_keeps_the_job_resumable() {
        let directory = home("paused-on-input-end");
        let supervisor = FakeSupervisor::new(vec![]);
        let requests = supervisor.requests.clone();
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let (sender, receiver) = ControlReceiver::new();
        sender
            .send(ControlCommand::Pause)
            .expect("pause should queue");
        // Dropping the sender is what happens when Lya's input ends without an explicit stop.
        drop(sender);

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .with_control_receiver(receiver)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("a paused job is a normal outcome");

        assert_eq!(result.status, JobStatus::Paused);
        assert!(requests.lock().expect("request lock").is_empty());
        assert_eq!(
            store(&directory)
                .load_job("test-job")
                .expect("state should load")
                .expect("job should persist")
                .status,
            JobStatus::Paused
        );
        assert!(
            events
                .lock()
                .expect("event lock")
                .iter()
                .any(|event| matches!(
                    event.kind,
                    JobEventKind::JobFinished { ref status } if status == "PAUSED"
                ))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_continues_a_paused_job_without_repeating_a_counted_iteration() {
        let directory = home("resume-paused");
        let job = round_trip(
            &directory,
            &persisted_job(JobStatus::Paused, Some(PendingOperation::SupervisorReview)).await,
        );
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Finish the paused job".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let requests = supervisor.requests.clone();
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .with_event_sink(sink)
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("a paused job should resume");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(result.iteration, 1);
        let requests = requests.lock().expect("request lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].iteration, 1);
        let events = events.lock().expect("event lock");
        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, JobEventKind::ResumeStarted { .. }))
        );
        assert!(events.iter().any(|event| matches!(
            event.kind,
            JobEventKind::ResumeValidated { ref continuation, .. } if continuation == "SUPERVISOR_REVIEW"
        )));
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_retries_the_executor_after_a_claude_quota_wait_and_reuses_the_session() {
        let directory = home("resume-claude-quota");
        let mut job = persisted_job(
            JobStatus::WaitingClaudeQuota,
            Some(PendingOperation::ExecutorRun),
        )
        .await;
        job.claude_session_id = Some("session-1".to_owned());
        job.last_supervisor_decision = Some(SupervisorDecision::Claude {
            prompt: "Implement the change.".to_owned(),
            reason: None,
        });
        job.quota_wait = Some(QuotaWait {
            provider: "Claude".to_owned(),
            operation: PendingOperation::ExecutorRun,
            source: QuotaSource::ProviderMessageHeuristic,
            reason: "Claude rate limit reached".to_owned(),
            detected_unix_seconds: 100,
        });
        let job = round_trip(&directory, &job);
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Finish after the quota wait".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session-1"), "Retried."))]);
        let executor_requests = executor.requests.clone();
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let result = orchestrator(supervisor, executor, &directory)
            .with_event_sink(sink)
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("a quota wait should resume");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(result.quota_wait, None);
        let executor_requests = executor_requests.lock().expect("request lock");
        assert_eq!(executor_requests.len(), 1);
        assert_eq!(
            executor_requests[0].session,
            ExecutorSession::Resume("session-1".to_owned())
        );
        assert_eq!(executor_requests[0].prompt, "Implement the change.");
        // The interrupted review is not repeated; the next review is the one after the retry.
        let supervisor_requests = supervisor_requests.lock().expect("request lock");
        assert_eq!(supervisor_requests.len(), 1);
        assert_eq!(supervisor_requests[0].iteration, 2);
        assert!(
            events
                .lock()
                .expect("event lock")
                .iter()
                .any(|event| matches!(
                    event.kind,
                    JobEventKind::QuotaRetryStarted { ref provider, ref operation }
                        if provider == "Claude" && operation == "EXECUTOR_RUN"
                ))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_retries_the_supervisor_after_an_openai_quota_wait() {
        let directory = home("resume-openai-quota");
        let mut job = persisted_job(
            JobStatus::WaitingOpenAiQuota,
            Some(PendingOperation::SupervisorReview),
        )
        .await;
        job.iteration = 2;
        job.quota_wait = Some(QuotaWait {
            provider: "OpenAI".to_owned(),
            operation: PendingOperation::SupervisorReview,
            source: QuotaSource::ProviderMessageHeuristic,
            reason: "Codex quota exhausted".to_owned(),
            detected_unix_seconds: 100,
        });
        let job = round_trip(&directory, &job);
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Finish after the OpenAI wait".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let requests = supervisor.requests.clone();

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("a quota wait should resume");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(result.iteration, 2);
        let requests = requests.lock().expect("request lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].iteration, 2);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_refuses_a_repository_that_changed_while_lya_was_not_running() {
        let directory = home("resume-diverged");
        let mut job =
            persisted_job(JobStatus::Paused, Some(PendingOperation::SupervisorReview)).await;
        let mut stale = fake_repository_state().await;
        stale.head = "0000000".to_owned();
        job.last_repository_state = Some(stale);
        let job = round_trip(&directory, &job);
        let supervisor = FakeSupervisor::new(vec![]);
        let requests = supervisor.requests.clone();
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .with_event_sink(sink)
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("a diverged repository is a waiting state, not an error");

        assert_eq!(result.status, JobStatus::WaitingHuman);
        assert!(requests.lock().expect("request lock").is_empty());
        assert!(events.lock().expect("event lock").iter().any(|event| matches!(
            event.kind,
            JobEventKind::ResumeRejected { ref reason } if reason.contains("changed while Lya was not running")
        )));
        assert_eq!(
            store(&directory)
                .load_job("test-job")
                .expect("state should load")
                .expect("job should persist")
                .status,
            JobStatus::WaitingHuman
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_refuses_a_job_without_a_persisted_repository_snapshot() {
        let directory = home("resume-no-snapshot");
        let mut job =
            persisted_job(JobStatus::Paused, Some(PendingOperation::SupervisorReview)).await;
        job.last_repository_state = None;
        let job = round_trip(&directory, &job);
        let supervisor = FakeSupervisor::new(vec![]);
        let requests = supervisor.requests.clone();

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("an unprovable repository is a waiting state");

        assert_eq!(result.status, JobStatus::WaitingHuman);
        assert!(requests.lock().expect("request lock").is_empty());
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn instructions_and_the_accepted_snapshot_survive_a_restart() {
        let directory = home("resume-instructions");
        let mut job =
            persisted_job(JobStatus::Paused, Some(PendingOperation::SupervisorReview)).await;
        job.applied_user_instructions = vec![
            "Keep the public format stable.".to_owned(),
            "Add tests for edge cases.".to_owned(),
        ];
        job.accepted_repository_state = Some(fake_repository_state().await);
        let job = round_trip(&directory, &job);
        assert_eq!(
            job.applied_user_instructions,
            [
                "Keep the public format stable.",
                "Add tests for edge cases."
            ]
        );
        assert!(job.accepted_repository_state.is_some());
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Claude {
                prompt: "Continue the work.".to_owned(),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Finish the resumed job".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let supervisor_requests = supervisor.requests.clone();
        let executor = FakeExecutor::new(vec![Ok(claude_result(Some("session-2"), "Done."))]);
        let executor_requests = executor.requests.clone();

        let result = orchestrator(supervisor, executor, &directory)
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("the resumed job should finish");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(
            supervisor_requests.lock().expect("request lock")[0].user_instructions,
            [
                "Keep the public format stable.",
                "Add tests for edge cases."
            ]
        );
        assert_eq!(
            executor_requests.lock().expect("request lock")[0].user_instructions,
            [
                "Keep the public format stable.",
                "Add tests for edge cases."
            ]
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_appends_to_the_same_event_log_and_keeps_it_valid_json() {
        let directory = home("resume-events");
        let sink = JsonlEventSink::for_job(&directory, "test-job");
        let log_path = sink.path().to_owned();
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Human {
            reason: "Decide the API first.".to_owned(),
        })]);
        orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("first process should finish");
        let first_lines = fs::read_to_string(&log_path)
            .expect("event log should exist")
            .lines()
            .count();

        let job = round_trip(
            &directory,
            &persisted_job(JobStatus::Paused, Some(PendingOperation::SupervisorReview)).await,
        );
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Finish".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .with_event_sink(JsonlEventSink::for_job(&directory, "test-job"))
            .resume(job, "Private project context.".to_owned())
            .await
            .expect("second process should finish");

        let content = fs::read_to_string(&log_path).expect("event log should exist");
        let lines = content.lines().collect::<Vec<_>>();
        assert!(
            lines.len() > first_lines,
            "resume must append to the existing log"
        );
        for line in &lines {
            let event: serde_json::Value =
                serde_json::from_str(line).expect("every line stays valid JSON");
            assert_eq!(event["job_id"], "test-job");
            assert!(event["event"].is_string());
        }
        assert!(
            lines[first_lines..]
                .iter()
                .any(|line| line.contains("RESUME_STARTED"))
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_refuses_terminal_jobs() {
        let directory = home("resume-terminal");
        for status in [JobStatus::Published, JobStatus::Stopped, JobStatus::Failed] {
            let job = round_trip(&directory, &persisted_job(status.clone(), None).await);
            let supervisor = FakeSupervisor::new(vec![]);
            let requests = supervisor.requests.clone();

            let error = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
                .resume(job, "Private project context.".to_owned())
                .await
                .expect_err("a terminal job must not resume");

            assert!(matches!(error, OrchestrationError::Resume(_)));
            assert!(requests.lock().expect("request lock").is_empty());
        }
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    async fn interrupted_publication_job(
        directory: &PathBuf,
        stage: Option<PublishStage>,
    ) -> JobState {
        let mut job =
            persisted_job(JobStatus::Publishing, Some(PendingOperation::Publication)).await;
        job.run.publish = true;
        job.run.git = Some(
            GitPublishConfig::new("Test Bot", "bot@example.com", "origin", "main")
                .expect("configuration should be valid"),
        );
        job.accepted_repository_state = Some(fake_repository_state().await);
        job.last_supervisor_decision = Some(SupervisorDecision::Accept {
            commit_title: "Finish publishing".to_owned(),
            next_prompt: None,
            reason: None,
        });
        job.publish_stage = stage;
        round_trip(directory, &job)
    }

    #[tokio::test]
    async fn resume_sends_an_interrupted_publication_through_the_recovery_path() {
        let directory = home("resume-publication");
        let job = interrupted_publication_job(&directory, Some(PublishStage::Committing)).await;
        let publisher = FakePublisher::new(vec![Ok(published_result("Finish publishing"))]);
        let recoveries = publisher.recoveries.clone();
        let requests = publisher.requests.clone();

        let result = AutonomousOrchestrator::new(
            FakeSupervisor::new(vec![]),
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(publisher)
        .resume(job, "Private project context.".to_owned())
        .await
        .expect("an interrupted publication should recover");

        assert_eq!(result.status, JobStatus::Published);
        let recoveries = recoveries.lock().expect("recovery lock");
        assert_eq!(recoveries.len(), 1);
        assert_eq!(recoveries[0].recorded_stage, Some(PublishStage::Committing));
        assert_eq!(recoveries[0].commit_title, "Finish publishing");
        // The recovery entry point is used instead of a fresh publication.
        assert_eq!(requests.lock().expect("request lock").len(), 1);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn resume_hands_a_recorded_commit_to_the_publisher_for_proof() {
        let directory = home("resume-recorded-commit");
        let mut job = interrupted_publication_job(&directory, Some(PublishStage::Committed)).await;
        job.publish_result = Some(PublishResult {
            commit_sha: "abcdef1".to_owned(),
            commit_title: "Finish publishing".to_owned(),
            remote: "origin".to_owned(),
            branch: "main".to_owned(),
            push_status: PushStatus::Pending,
        });
        let job = round_trip(&directory, &job);
        let publisher = FakePublisher::new(vec![Ok(published_result("Finish publishing"))]);
        let recoveries = publisher.recoveries.clone();

        let result = AutonomousOrchestrator::new(
            FakeSupervisor::new(vec![]),
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(publisher)
        .resume(job, "Private project context.".to_owned())
        .await
        .expect("a recorded commit should resume at push");

        assert_eq!(result.status, JobStatus::Published);
        let recoveries = recoveries.lock().expect("recovery lock");
        assert_eq!(
            recoveries[0]
                .recorded_result
                .as_ref()
                .expect("the recorded commit must reach the publisher")
                .commit_sha,
            "abcdef1"
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn ambiguous_publication_recovery_parks_the_job_for_a_human() {
        let directory = home("resume-ambiguous-publication");
        let job = interrupted_publication_job(&directory, Some(PublishStage::Committing)).await;
        let publisher = FakePublisher::new(vec![Err(PublishError::RecoveryAmbiguous(
            "HEAD is not the recorded commit".to_owned(),
        ))]);
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let result = AutonomousOrchestrator::new(
            FakeSupervisor::new(vec![]),
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(publisher)
        .with_event_sink(sink)
        .resume(job, "Private project context.".to_owned())
        .await
        .expect("an ambiguous recovery is a waiting state, not an error");

        assert_eq!(result.status, JobStatus::WaitingHuman);
        assert_eq!(
            store(&directory)
                .load_job("test-job")
                .expect("state should load")
                .expect("job should persist")
                .status,
            JobStatus::WaitingHuman
        );
        assert!(events.lock().expect("event lock").iter().any(|event| matches!(
            event.kind,
            JobEventKind::WaitingForHuman { ref reason } if reason.contains("cannot be recovered safely")
        )));
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn user_instructions_are_bounded_and_refusals_are_explicit() {
        let directory = home("instruction-budget");
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Bounded".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let supervisor_requests = supervisor.requests.clone();
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();
        let (sender, receiver) = ControlReceiver::new();
        for index in 0..(MAX_ACTIVE_USER_INSTRUCTIONS + 2) {
            sender
                .send(ControlCommand::Send(format!("instruction {index}")))
                .expect("instruction should queue");
        }

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .with_control_receiver(receiver)
            .with_event_sink(sink)
            .run(job_request())
            .await
            .expect("job should finish");

        assert_eq!(
            result.applied_user_instructions.len(),
            MAX_ACTIVE_USER_INSTRUCTIONS
        );
        assert_eq!(
            supervisor_requests.lock().expect("request lock")[0]
                .user_instructions
                .len(),
            MAX_ACTIVE_USER_INSTRUCTIONS
        );
        let events = events.lock().expect("event lock");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.kind, JobEventKind::UserInstructionRejected { .. }))
                .count(),
            2
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn a_sequential_next_job_never_inherits_the_previous_job_instructions() {
        let directory = home("instructions-not-inherited");
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Accept {
                commit_title: "First".to_owned(),
                next_prompt: Some("second task".to_owned()),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Second".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let requests = supervisor.requests.clone();
        let publisher = FakePublisher::new(vec![
            Ok(published_result("First")),
            Ok(published_result("Second")),
        ]);
        let (sender, receiver) = ControlReceiver::new();
        sender
            .send(ControlCommand::Send("Only for the first job.".to_owned()))
            .expect("instruction should queue");

        let result = AutonomousOrchestrator::new(
            supervisor,
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(publisher)
        .with_control_receiver(receiver)
        .run_sequential(job_request())
        .await
        .expect("both jobs should run");

        assert_eq!(result.jobs.len(), 2);
        let requests = requests.lock().expect("request lock");
        assert_eq!(requests[0].user_instructions, ["Only for the first job."]);
        assert!(requests[1].user_instructions.is_empty());
        assert!(result.jobs[1].applied_user_instructions.is_empty());
        assert_eq!(result.jobs[1].sequential_index, 1);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    /// Tries to claim the job that emitted each event, exactly the way an independent Lya process
    /// would. Every attempt happens while the orchestrator is still driving that job.
    struct LockProbeEventSink {
        home: PathBuf,
        attempts: Arc<Mutex<Vec<(String, bool)>>>,
    }

    impl EventSink for LockProbeEventSink {
        fn emit(&self, event: &JobEvent) -> Result<(), super::EventSinkError> {
            let claimed = JobLock::acquire(&StateStore::at(&self.home), &event.job_id).is_ok();
            self.attempts
                .lock()
                .expect("attempt lock")
                .push((event.job_id.clone(), claimed));
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_sequential_child_job_cannot_be_claimed_by_a_second_process() {
        let directory = home("sequential-child-lock");
        let caller_store = store(&directory);
        // Exactly the claim `lya run` holds for the job it was asked to start. It covers that job
        // only, so the sequential child has to be claimed by the orchestrator itself.
        let root_lock =
            JobLock::acquire(&caller_store, "test-job").expect("root job should be claimed");
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let supervisor = FakeSupervisor::new(vec![
            Ok(SupervisorDecision::Accept {
                commit_title: "First".to_owned(),
                next_prompt: Some("second task".to_owned()),
                reason: None,
            }),
            Ok(SupervisorDecision::Accept {
                commit_title: "Second".to_owned(),
                next_prompt: None,
                reason: None,
            }),
        ]);
        let publisher = FakePublisher::new(vec![
            Ok(published_result("First")),
            Ok(published_result("Second")),
        ]);

        let result = AutonomousOrchestrator::new(
            supervisor,
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(publisher)
        .with_event_sink(LockProbeEventSink {
            home: directory.clone(),
            attempts: attempts.clone(),
        })
        .run_sequential(job_request())
        .await
        .expect("both jobs should run");

        assert_eq!(result.jobs.len(), 2);
        let child_id = result.jobs[1].job_id.clone();
        assert_ne!(child_id, "test-job");

        let recorded = attempts.lock().expect("attempt lock").clone();
        assert!(
            recorded.iter().any(|(job_id, _)| job_id == &child_id),
            "the probe must have run while the sequential child was being driven"
        );
        assert!(
            recorded.iter().all(|(_, claimed)| !claimed),
            "a job being driven must never be claimable by a second owner: {recorded:?}"
        );

        // The child's claim is released once it stops being driven, and releasing it never
        // released the root claim either.
        assert!(
            JobLock::acquire(&caller_store, "test-job").is_err(),
            "the caller's claim on the root job must outlive the sequential chain"
        );
        drop(root_lock);
        let reclaimed_root = JobLock::acquire(&caller_store, "test-job")
            .expect("the root job should be claimable again");
        let reclaimed_child = JobLock::acquire(&caller_store, &child_id)
            .expect("the sequential child should be claimable once it is no longer driven");
        drop(reclaimed_root);
        drop(reclaimed_child);

        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    /// Reads persisted state the moment the publisher is asked to publish, which is the first
    /// moment a crash could leave a job owing a publication.
    struct StateProbePublisher {
        store: StateStore,
        observed: Arc<Mutex<Vec<JobState>>>,
        results: Arc<Mutex<VecDeque<Result<PublishResult, PublishError>>>>,
    }

    impl Publisher for StateProbePublisher {
        fn recover<'a>(
            &'a self,
            _request: PublishRecoveryRequest,
            _progress: &'a mut dyn PublishProgress,
        ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>>
        {
            unreachable!("a fresh publication never takes the recovery path")
        }

        fn publish<'a>(
            &'a self,
            _request: PublishRequest,
            _progress: &'a mut dyn PublishProgress,
        ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>>
        {
            self.observed.lock().expect("observed lock").push(
                self.store
                    .load_job("test-job")
                    .expect("state should load")
                    .expect("state should exist"),
            );
            let result = self
                .results
                .lock()
                .expect("publisher result lock")
                .pop_front()
                .expect("a publisher result should be queued");
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn an_owed_publication_is_never_persisted_as_a_state_resume_cannot_continue() {
        let directory = home("publication-owed-state");
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Publish it".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let publisher = StateProbePublisher {
            store: store(&directory),
            observed: observed.clone(),
            results: Arc::new(Mutex::new(vec![Ok(published_result("Publish it"))].into())),
        };

        let result = AutonomousOrchestrator::new(
            supervisor,
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(publisher)
        .with_publish_configuration(
            GitPublishConfig::new("Bot", "bot@example.com", "origin", "main")
                .expect("configuration"),
        )
        .run(job_request())
        .await
        .expect("the job should publish");

        assert_eq!(result.status, JobStatus::Published);
        let observed = observed.lock().expect("observed lock");
        let owed = observed.first().expect("the publisher should have run");
        assert_eq!(owed.status, JobStatus::Publishing);
        assert_eq!(owed.pending_operation, Some(PendingOperation::Publication));
        assert!(owed.accepted_repository_state.is_some());
        // The window that mattered: this exact persisted state must be continuable.
        assert!(
            super::ResumePlan::for_job(owed).is_ok(),
            "a persisted owed publication must be resumable"
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn an_accepted_job_without_publication_owes_nothing() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Accept only".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let directory = home("accepted-owes-nothing");

        let result = orchestrator(supervisor, FakeExecutor::new(vec![]), &directory)
            .run(job_request())
            .await
            .expect("the job should be accepted");

        assert_eq!(result.status, JobStatus::Accepted);
        assert_eq!(result.pending_operation, None);
        let persisted = store(&directory)
            .load_job("test-job")
            .expect("state should load")
            .expect("state should exist");
        assert_eq!(persisted.status, JobStatus::Accepted);
        assert_eq!(persisted.pending_operation, None);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    /// Reads the persisted child job the first time the supervisor is asked about it, which is
    /// right after the child's first authoritative write and before it has done any work.
    struct ChildStateProbeSupervisor {
        store: StateStore,
        observed: Arc<Mutex<Vec<JobState>>>,
        decisions: Arc<Mutex<VecDeque<Result<SupervisorDecision, SupervisorError>>>>,
    }

    impl Supervisor for ChildStateProbeSupervisor {
        fn decide(
            &self,
            request: SupervisorRequest,
        ) -> Pin<Box<dyn Future<Output = Result<SupervisorDecision, SupervisorError>> + Send + '_>>
        {
            if request.task == "second task" {
                let child = self
                    .store
                    .load_all()
                    .expect("state should load")
                    .into_iter()
                    .find(|job| job.job_id != "test-job")
                    .expect("the child job should be persisted before its first review");
                self.observed.lock().expect("observed lock").push(child);
            }
            let decision = self
                .decisions
                .lock()
                .expect("decision lock")
                .pop_front()
                .expect("a supervisor decision should be queued");
            Box::pin(async move { decision })
        }
    }

    #[tokio::test]
    async fn a_sequential_child_carries_its_chain_position_in_its_first_persisted_write() {
        let directory = home("sequential-index-first-write");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let supervisor = ChildStateProbeSupervisor {
            store: store(&directory),
            observed: observed.clone(),
            decisions: Arc::new(Mutex::new(
                vec![
                    Ok(SupervisorDecision::Accept {
                        commit_title: "First".to_owned(),
                        next_prompt: Some("second task".to_owned()),
                        reason: None,
                    }),
                    // The child stops before finishing, standing in for a process that
                    // disappeared: nothing after its first write may fix its chain position.
                    Ok(SupervisorDecision::Stop {
                        reason: "child interrupted".to_owned(),
                    }),
                ]
                .into(),
            )),
        };

        let result = AutonomousOrchestrator::new(
            supervisor,
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(FakePublisher::new(vec![Ok(published_result("First"))]))
        .with_max_jobs(2)
        .run_sequential(job_request())
        .await
        .expect("the chain should run");

        assert_eq!(result.jobs.len(), 2);
        let observed = observed.lock().expect("observed lock");
        assert_eq!(
            observed
                .first()
                .expect("the child should have been observed")
                .sequential_index,
            1,
            "the first authoritative write of a child must already carry its chain position"
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn a_child_resumed_after_a_crash_still_counts_against_max_jobs() {
        let directory = home("sequential-index-crash-resume");
        // The child exactly as its own first write persisted it, before it did any work.
        let mut child = persisted_job(JobStatus::Running, None).await;
        child.job_id = "child-job".to_owned();
        child.task = "second task".to_owned();
        child.iteration = 0;
        child.sequential_index = 1;
        child.run.max_jobs = 2;
        child.run.publish = true;
        child.run.git = Some(
            GitPublishConfig::new("Bot", "bot@example.com", "origin", "main")
                .expect("configuration"),
        );
        let child = round_trip(&directory, &child);
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Second".to_owned(),
            // The resumed child asks for a third job it is no longer allowed to start.
            next_prompt: Some("third task".to_owned()),
            reason: None,
        })]);

        let result = AutonomousOrchestrator::new(
            supervisor,
            FakeExecutor::new(vec![]),
            FakeGitRunner { dirty: false },
            store(&directory),
        )
        .with_publisher(FakePublisher::new(vec![Ok(published_result("Second"))]))
        .with_max_jobs(2)
        .resume_sequential(child, "Private project context.".to_owned())
        .await
        .expect("the child should resume");

        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.jobs[0].sequential_index, 1);
        assert!(
            result.max_jobs_reached,
            "a resumed child must keep the chain position it was persisted with"
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn a_pause_the_instant_claude_returns_never_discards_its_completed_run() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Claude {
            prompt: "Implement.".to_owned(),
            reason: None,
        })]);
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        let executor_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = BlockingExecutor {
            started: Mutex::new(Some(started_sender)),
            release: Mutex::new(Some(release_receiver)),
            requests: executor_requests.clone(),
        };
        let directory = home("pause-right-after-executor");
        let run_directory = directory.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (paused_sender, paused_receiver) = tokio::sync::oneshot::channel();
        let (sender, receiver) = ControlReceiver::new();
        let run = tokio::spawn(async move {
            AutonomousOrchestrator::new(
                supervisor,
                executor,
                FakeGitRunner { dirty: false },
                StateStore::new(&LyaHome::from_path(&run_directory)),
            )
            .with_control_receiver(receiver)
            .with_event_sink(PauseSignalSink {
                events,
                paused: Mutex::new(Some(paused_sender)),
            })
            .run(job_request())
            .await
        });

        started_receiver.await.expect("executor should start");
        sender
            .send(ControlCommand::Pause)
            .expect("pause should queue");
        release_sender
            .send(claude_result(Some("session-7"), "Completed the work."))
            .expect("executor should receive result");
        paused_receiver
            .await
            .expect("the pause should happen after the executor returned");
        // Input ends while paused, exactly as a closed stdin or a vanished shell would.
        drop(sender);
        let paused = run
            .await
            .expect("job should join")
            .expect("an input end while paused is not a failure");

        assert_eq!(paused.status, JobStatus::Paused);
        assert_eq!(paused.claude_session_id.as_deref(), Some("session-7"));
        assert_eq!(
            paused.last_executor_report.as_deref(),
            Some("Completed the work.")
        );
        assert_eq!(paused.pending_operation, None);
        let persisted = store(&directory)
            .load_job("test-job")
            .expect("state should load")
            .expect("state should exist");
        assert_eq!(persisted.claude_session_id.as_deref(), Some("session-7"));
        assert_eq!(
            persisted.last_executor_report.as_deref(),
            Some("Completed the work.")
        );
        assert_eq!(persisted.pending_operation, None);
        assert_eq!(executor_requests.lock().expect("requests").len(), 1);

        // A later process continues with a new review instead of replaying Claude.
        let resume_supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Accept {
            commit_title: "Finish".to_owned(),
            next_prompt: None,
            reason: None,
        })]);
        let resume_supervisor_requests = resume_supervisor.requests.clone();
        let resume_executor = FakeExecutor::new(vec![]);
        let resume_executor_requests = resume_executor.requests.clone();
        let resumed = orchestrator(resume_supervisor, resume_executor, &directory)
            .resume(persisted, "Private project context.".to_owned())
            .await
            .expect("the paused job should resume");

        assert_eq!(resumed.status, JobStatus::Accepted);
        assert!(
            resume_executor_requests
                .lock()
                .expect("requests")
                .is_empty(),
            "a completed execution must never be replayed"
        );
        let reviews = resume_supervisor_requests.lock().expect("request lock");
        assert_eq!(
            reviews[0].executor_report.as_deref(),
            Some("Completed the work.")
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn an_inconsistent_job_is_parked_for_a_human_instead_of_staying_resumable() {
        let directory = home("resume-invalid-state");
        let mut job = persisted_job(
            JobStatus::WaitingClaudeQuota,
            Some(PendingOperation::SupervisorReview),
        )
        .await;
        // A quota wait naming a different operation than the one actually owed: no continuation
        // can be proven from it without risking a repeated provider call.
        job.quota_wait = Some(QuotaWait {
            provider: "Claude".to_owned(),
            operation: PendingOperation::ExecutorRun,
            source: QuotaSource::ProviderMessageHeuristic,
            reason: "Claude rate limit reached".to_owned(),
            detected_unix_seconds: 100,
        });
        let job = round_trip(&directory, &job);
        let sink = RecordingEventSink::default();
        let events = sink.events.clone();

        let error = orchestrator(
            FakeSupervisor::new(vec![]),
            FakeExecutor::new(vec![]),
            &directory,
        )
        .with_event_sink(sink)
        .resume(job, "Private project context.".to_owned())
        .await
        .expect_err("an inconsistent job cannot be resumed");

        assert!(matches!(error, OrchestrationError::Resume(_)));
        let persisted = store(&directory)
            .load_job("test-job")
            .expect("state should load")
            .expect("state should exist");
        assert_eq!(persisted.status, JobStatus::WaitingHuman);
        let events = events.lock().expect("event lock");
        assert!(events.iter().any(|event| matches!(
            event.kind,
            JobEventKind::WaitingForHuman { ref reason }
                if reason.contains("does not match the pending operation")
        )));
        // The job is no longer advertised to the next `lya resume`, however often it runs.
        assert!(
            crate::orchestrator::resume::resumable_jobs(&store(&directory))
                .expect("candidates should load")
                .is_empty()
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn a_terminal_job_keeps_its_own_status_when_a_resume_is_refused() {
        let directory = home("resume-terminal-untouched");
        let job = round_trip(&directory, &persisted_job(JobStatus::Failed, None).await);

        let error = orchestrator(
            FakeSupervisor::new(vec![]),
            FakeExecutor::new(vec![]),
            &directory,
        )
        .resume(job, "Private project context.".to_owned())
        .await
        .expect_err("a terminal job cannot be resumed");

        assert!(matches!(error, OrchestrationError::Resume(_)));
        assert_eq!(
            store(&directory)
                .load_job("test-job")
                .expect("state should load")
                .expect("state should exist")
                .status,
            JobStatus::Failed
        );
        fs::remove_dir_all(directory).expect("test home should be removed");
    }

    #[tokio::test]
    async fn a_failure_about_rate_limits_is_not_mistaken_for_an_exhausted_quota() {
        let supervisor = FakeSupervisor::new(vec![Ok(SupervisorDecision::Claude {
            prompt: "Implement the rate limit middleware.".to_owned(),
            reason: None,
        })]);
        // A normal failure whose output happens to discuss the task's own subject.
        let executor = FakeExecutor::new(vec![Err(ExecutorError::ProcessFailed {
            exit_code: Some(1),
            stdout: "The rate limit tests do not compile yet.".to_owned(),
            stderr: "error[E0433]: failed to resolve RateLimiter".to_owned(),
        })]);
        let directory = home("quota-false-positive");

        let error = orchestrator(supervisor, executor, &directory)
            .run(job_request())
            .await
            .expect_err("a normal failure should fail the job");

        assert!(matches!(error, OrchestrationError::Executor(_)));
        let persisted = store(&directory)
            .load_job("test-job")
            .expect("state should load")
            .expect("state should exist");
        assert_eq!(persisted.status, JobStatus::Failed);
        assert_eq!(persisted.quota_wait, None);
        fs::remove_dir_all(directory).expect("test home should be removed");
    }
}
