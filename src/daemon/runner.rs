//! The driver the daemon runs real jobs with.
//!
//! It is the same orchestrator every other Lya surface uses, wired for a process with no terminal:
//!
//! * events go to the job's own `events.jsonl` and to the live fan-out attached clients read, never
//!   to a standard output nobody is holding;
//! * limits come from the job itself — the run configuration the scheduler persisted on it — so a
//!   job keeps the limits it was accepted with, even across a daemon restart;
//! * a job is started or continued according to the mode the scheduler assigned, and continuing one
//!   goes through the ordinary resume path with its full validation.
//!
//! There is no daemon-specific job logic here, and there must never be: the daemon is a boundary.

use std::{pin::Pin, sync::Arc};

use crate::{
    orchestrator::{
        events::{CompositeEventSink, EventSink, JsonlEventSink},
        executor::ClaudeCliExecutor,
        home::LyaHome,
        job::{AutonomousOrchestrator, NewJob, OrchestrationError, RunResult},
        publisher::{GitPublisher, Publisher},
        scheduler::{JobAssignment, JobDriver, JobOutcome, ScheduleMode},
        state::{JobState, StateStore},
        supervisor::CodexCliSupervisor,
    },
    process::SystemProcessRunner,
};

use super::attach::JobEventBroadcaster;

type JobOrchestrator<P> = AutonomousOrchestrator<
    CodexCliSupervisor<SystemProcessRunner>,
    ClaudeCliExecutor<SystemProcessRunner>,
    SystemProcessRunner,
    P,
    CompositeEventSink,
>;

pub struct DaemonJobDriver {
    home: LyaHome,
    private_context: String,
    broadcaster: JobEventBroadcaster,
    /// Where a foreground daemon also renders the events of every job it drives.
    ///
    /// One writer shared by every concurrently driven job, so interleaved output stays well-formed.
    /// A detached daemon has one too — its standard output is its log file.
    terminal: Option<Arc<dyn EventSink>>,
}

impl DaemonJobDriver {
    pub fn new(
        home: LyaHome,
        private_context: impl Into<String>,
        broadcaster: JobEventBroadcaster,
    ) -> Self {
        Self {
            home,
            private_context: private_context.into(),
            broadcaster,
            terminal: None,
        }
    }

    /// Also render every driven job's events to this sink.
    pub fn with_terminal_sink(mut self, terminal: Arc<dyn EventSink>) -> Self {
        self.terminal = Some(terminal);
        self
    }
}

impl JobDriver for DaemonJobDriver {
    fn drive<'a>(
        &'a self,
        assignment: JobAssignment,
    ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
        Box::pin(async move {
            let job_id = assignment.job_id.clone();
            let outcome = self.run(assignment).await;
            // The live stream belongs to the job: once the job is over there is nothing further to
            // watch, and attached clients are told so rather than waiting for ever.
            self.broadcaster.release(&job_id);
            outcome
        })
    }
}

impl DaemonJobDriver {
    async fn run(&self, assignment: JobAssignment) -> JobOutcome {
        let action = match assignment.mode {
            ScheduleMode::Start => Action::Start(Box::new(NewJob::new(
                assignment.job_id.clone(),
                assignment.project.clone(),
                assignment.task.clone(),
                self.private_context.clone(),
            ))),
            ScheduleMode::Resume => {
                match StateStore::new(&self.home).load_job(&assignment.job_id) {
                    Ok(Some(job)) => Action::Resume(Box::new(job)),
                    Ok(None) => {
                        return JobOutcome::Failed {
                            error: format!("job {} is no longer persisted", assignment.job_id),
                        };
                    }
                    Err(error) => {
                        return JobOutcome::Failed {
                            error: error.to_string(),
                        };
                    }
                }
            }
        };

        let mut sinks: Vec<Box<dyn EventSink>> = vec![
            Box::new(JsonlEventSink::for_job(
                self.home.path(),
                &assignment.job_id,
            )),
            Box::new(self.broadcaster.sink(&assignment.job_id)),
        ];
        if let Some(terminal) = &self.terminal {
            sinks.push(Box::new(Arc::clone(terminal)));
        }
        let sink = CompositeEventSink::new(sinks);
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

        // Publication continues with the configuration persisted for the job, never with whatever
        // the daemon's own environment happens to hold.
        let result = match assignment
            .run
            .publish
            .then_some(assignment.run.git)
            .flatten()
        {
            Some(configuration) => {
                self.execute(
                    base.with_publisher(GitPublisher::new(configuration.clone()))
                        .with_publish_configuration(configuration),
                    action,
                )
                .await
            }
            None => self.execute(base, action).await,
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
                    succeeded: job.status.is_successful_outcome(),
                }
            }
            Err(error) => JobOutcome::Failed {
                error: error.to_string(),
            },
        }
    }

    async fn execute<P: Publisher>(
        &self,
        orchestrator: JobOrchestrator<P>,
        action: Action,
    ) -> Result<RunResult, OrchestrationError> {
        match action {
            Action::Start(request) => orchestrator.run_sequential(*request).await,
            Action::Resume(job) => {
                orchestrator
                    .resume_sequential(*job, self.private_context.clone())
                    .await
            }
        }
    }
}

enum Action {
    Start(Box<NewJob>),
    Resume(Box<JobState>),
}
