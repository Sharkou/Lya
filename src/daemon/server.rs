//! The daemon: the process that owns autonomous work.
//!
//! ```text
//!                 lya submit / attach / control / daemon status
//!                                  |
//!                      local IPC (named pipe / Unix socket)
//!                                  |
//!                        +---------v---------+
//!                        |      Daemon       |  claim, endpoint, connections
//!                        +---------+---------+
//!                                  |
//!                        +---------v---------+
//!                        |     Scheduler     |  repository claims, slots, queue
//!                        +---------+---------+
//!                                  |
//!                   AutonomousOrchestrator per job (unchanged)
//! ```
//!
//! The daemon is a boundary, exactly as the scheduler is. It owns the *process-level* concerns —
//! the claim on one `LYA_HOME`, the local endpoint, connected clients, the live event fan-out and
//! graceful shutdown — and coordinates the components that already exist. It contains no scheduling
//! policy, no queue of its own, no job semantics and no control state machine:
//!
//! * which work may start, when, and how much at once is [`Scheduler`]'s answer;
//! * what a job does is [`AutonomousOrchestrator`](crate::orchestrator::job::AutonomousOrchestrator)'s;
//! * what a control command means is [`ControlCommand`](crate::orchestrator::control::ControlCommand)'s;
//! * whether an interrupted job may continue is [`ResumePlan`](crate::orchestrator::resume::ResumePlan)'s.
//!
//! Ownership is proven the same way it is everywhere in Lya — by the operating system. The daemon
//! holds [`DaemonLock`] for as long as it runs, and every job it drives holds the same
//! [`JobLock`](crate::orchestrator::lock::JobLock) and
//! [`RepositoryLock`](crate::orchestrator::repository_lock::RepositoryLock) a foreground `lya run`
//! takes. A foreground process therefore cannot touch a repository or a job the daemon is driving,
//! and the daemon cannot touch one a foreground process is driving. Both directions fail closed.
//!
//! One client can never hurt another. Every connection is its own task with its own buffer; a
//! malformed frame is answered with an error, a client that disappears mid-stream is forgotten, and
//! neither outcome reaches the scheduler, the jobs or any other client.

use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;

use crate::orchestrator::{
    batch::MAX_REQUESTS,
    home::LyaHome,
    lock::JobLock,
    resume::resumable_jobs,
    scheduler::{
        AdmissionOutcome, DEFAULT_MAX_CONCURRENT, JobDriver, NoopSchedulerSink, ScheduledOutcome,
        ScheduledRequest, Scheduler, SchedulerAdmission, SchedulerControl, SchedulerError,
        SchedulerEventSink, SchedulerReport,
    },
    state::{JobState, JobStatus, StateError, StateStore},
    supervisor::Project,
};

use super::{
    attach::{JobEventBroadcaster, ReplayGuard, recorded_events},
    events::{DaemonEvent, DaemonEventKind, DaemonEventSink, NoopDaemonSink},
    lock::{DaemonLock, DaemonLockError},
    protocol::{
        ControlRequest, DaemonErrorCode, DaemonErrorResponse, DaemonIdentity, DaemonRequest,
        DaemonResponse, DaemonStatus, JobSummary, PROTOCOL_VERSION, SubmitJob, SubmitOutcome,
        decode_request, encode_response,
    },
    transport::{
        DaemonConnection, DaemonEndpoint, DaemonListener, DaemonReader, DaemonWriter,
        TransportError,
    },
};

/// How a daemon is configured for one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    /// How many repositories may be driven at once.
    pub max_concurrent: usize,
    /// Whether work a previous daemon accepted and never started is picked up again.
    pub recover_queued: bool,
    /// Whether interrupted jobs are continued automatically.
    ///
    /// Off by default, and deliberately so. Continuing an interrupted job is safe only where
    /// [`ResumePlan`](crate::orchestrator::resume::ResumePlan) can prove it, and a daemon that
    /// starts on boot should not make that decision for a human who has not asked. With it off,
    /// interrupted jobs are found, reported and left exactly as they are.
    pub resume_interrupted: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            recover_queued: true,
            resume_interrupted: false,
        }
    }
}

/// How a daemon run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonOutcome {
    pub reason: String,
    pub report: SchedulerReport,
}

/// Requests a graceful shutdown, and lets every part of the daemon wait for one.
///
/// The reason is recorded once, by whoever asked first, so the observation says what actually
/// happened rather than what the last caller happened to say.
#[derive(Clone)]
pub struct ShutdownSignal {
    sender: Arc<watch::Sender<Option<String>>>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self {
            sender: Arc::new(watch::channel(None).0),
        }
    }

    pub fn request(&self, reason: impl Into<String>) {
        let reason = reason.into();
        self.sender.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(reason);
            true
        });
    }

    pub fn is_requested(&self) -> bool {
        self.sender.borrow().is_some()
    }

    pub fn reason(&self) -> Option<String> {
        self.sender.borrow().clone()
    }

    /// Resolve as soon as a shutdown has been requested, now or later.
    pub async fn wait(&self) -> String {
        let mut receiver = self.sender.subscribe();
        loop {
            if let Some(reason) = receiver.borrow_and_update().clone() {
                return reason;
            }
            if receiver.changed().await.is_err() {
                return "the daemon shut down".to_owned();
            }
        }
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// The daemon, before it starts serving.
pub struct Daemon<D: JobDriver> {
    home: LyaHome,
    driver: D,
    config: DaemonConfig,
    events: Arc<dyn DaemonEventSink>,
    scheduler_events: Arc<dyn SchedulerEventSink>,
    broadcaster: JobEventBroadcaster,
    shutdown: ShutdownSignal,
}

impl<D: JobDriver> Daemon<D> {
    pub fn new(home: LyaHome, driver: D) -> Self {
        Self {
            home,
            driver,
            config: DaemonConfig::default(),
            events: Arc::new(NoopDaemonSink),
            scheduler_events: Arc::new(NoopSchedulerSink),
            broadcaster: JobEventBroadcaster::new(),
            shutdown: ShutdownSignal::new(),
        }
    }

    pub fn with_config(mut self, config: DaemonConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_event_sink(mut self, events: Arc<dyn DaemonEventSink>) -> Self {
        self.events = events;
        self
    }

    pub fn with_scheduler_event_sink(mut self, events: Arc<dyn SchedulerEventSink>) -> Self {
        self.scheduler_events = events;
        self
    }

    /// The fan-out the driven jobs emit into. Must be the same one the driver was built with.
    pub fn with_broadcaster(mut self, broadcaster: JobEventBroadcaster) -> Self {
        self.broadcaster = broadcaster;
        self
    }

    /// The handle that asks this daemon to stop. Take it before [`Daemon::serve`] consumes the
    /// daemon — the interrupt handler needs it.
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.shutdown.clone()
    }

    /// Claim the home, start listening and serve until a graceful shutdown completes.
    ///
    /// The claim is taken first, so a second daemon for the same home is refused before it can bind
    /// an endpoint, write metadata or touch any job.
    pub async fn serve(self) -> Result<DaemonOutcome, DaemonError> {
        let lock = DaemonLock::acquire(&self.home).map_err(DaemonError::Claim)?;
        let endpoint = DaemonEndpoint::for_home(&self.home);
        let mut listener = DaemonListener::bind(&endpoint)
            .await
            .map_err(DaemonError::Transport)?;
        let metadata = lock
            .publish_metadata(endpoint.address())
            .map_err(DaemonError::Claim)?;

        let store = StateStore::new(&self.home);
        let scheduler = Scheduler::new(self.driver, StateStore::new(&self.home))
            .with_max_concurrent(self.config.max_concurrent)
            .with_event_sink(Arc::clone(&self.scheduler_events));
        let state = Arc::new(DaemonState {
            home: LyaHome::from_path(self.home.path()),
            store,
            admission: scheduler.admission(),
            control: scheduler.control(),
            broadcaster: self.broadcaster.clone(),
            identity: DaemonIdentity {
                protocol_version: PROTOCOL_VERSION,
                process_id: metadata.process_id,
                started_unix_seconds: metadata.started_unix_seconds,
                endpoint: endpoint.address().to_owned(),
            },
            config: self.config.clone(),
            events: Arc::clone(&self.events),
            shutdown: self.shutdown.clone(),
            connected_clients: AtomicUsize::new(0),
            next_client_id: AtomicU64::new(1),
        });

        state.emit(DaemonEventKind::DaemonStarted {
            process_id: state.identity.process_id,
            endpoint: state.identity.endpoint.clone(),
            protocol_version: PROTOCOL_VERSION,
            max_concurrent: self.config.max_concurrent,
        });
        state.recover()?;

        let scheduler_task = tokio::spawn(scheduler.serve());
        state.emit(DaemonEventKind::SchedulerStarted {
            max_concurrent: self.config.max_concurrent,
        });

        // Accept until a shutdown is requested. A connection that fails to be accepted is reported
        // and the loop continues: one client's bad luck never ends the daemon.
        let reason = loop {
            tokio::select! {
                reason = state.shutdown.wait() => break reason,
                accepted = listener.accept() => match accepted {
                    Ok(connection) => {
                        let state = Arc::clone(&state);
                        tokio::spawn(async move { serve_client(state, connection).await });
                    }
                    Err(error) => {
                        state.emit(DaemonEventKind::ClientRejected {
                            client_id: 0,
                            code: "INTERNAL".to_owned(),
                            reason: error.to_string(),
                        });
                    }
                },
            }
        };

        // Graceful shutdown, in the only order that is safe: stop accepting work, then ask every
        // active job for the same graceful stop a foreground Ctrl+C would request, then wait for
        // the scheduler to report that they have all shut down.
        state.emit(DaemonEventKind::DaemonStopping {
            reason: reason.clone(),
            active_jobs: state.control.active_job_ids().len(),
        });
        // The stop flag is set *before* admission closes, and the order is the whole point: closing
        // admission wakes every idle worker, and a worker that wakes before the flag is visible can
        // pull a queued entry and start it — turning work that was only ever accepted into a job
        // that is immediately stopped, and therefore terminal, and therefore never recovered.
        // Setting the flag first wakes nobody; closing admission then wakes workers that can only
        // observe a stop. A worker already past its own check still gives its entry back, which is
        // the second half of the same guarantee.
        state.control.request_stop();
        state.admission.close();
        drop(listener);

        let report = scheduler_task
            .await
            .map_err(|error| DaemonError::Worker(error.to_string()))?
            .map_err(DaemonError::Scheduler)?;
        state.emit(DaemonEventKind::SchedulerStopped {
            completed: count(&report, |outcome| {
                matches!(outcome, ScheduledOutcome::Finished { .. })
            }),
            failed: count(&report, |outcome| {
                matches!(outcome, ScheduledOutcome::Failed { .. })
            }),
            rejected: count(&report, |outcome| {
                matches!(outcome, ScheduledOutcome::Rejected { .. })
            }),
            queued_remaining: report.queued_remaining.len(),
        });
        state.emit(DaemonEventKind::DaemonStopped);
        drop(lock);

        Ok(DaemonOutcome { reason, report })
    }
}

fn count(report: &SchedulerReport, matching: impl Fn(&ScheduledOutcome) -> bool) -> usize {
    report
        .jobs
        .iter()
        .filter(|job| matching(&job.outcome))
        .count()
}

/// Everything a connection handler is allowed to reach.
///
/// Shared immutably: a client handler can submit, look up state and deliver control commands, and
/// can change nothing about the daemon except by asking it to stop.
struct DaemonState {
    home: LyaHome,
    store: StateStore,
    admission: SchedulerAdmission,
    control: SchedulerControl,
    broadcaster: JobEventBroadcaster,
    identity: DaemonIdentity,
    config: DaemonConfig,
    events: Arc<dyn DaemonEventSink>,
    shutdown: ShutdownSignal,
    connected_clients: AtomicUsize,
    next_client_id: AtomicU64,
}

impl DaemonState {
    fn emit(&self, kind: DaemonEventKind) {
        // Daemon observation must never be able to end a daemon. A sink that cannot write is a
        // problem with the sink.
        let _ = self.events.emit(&DaemonEvent::new(kind));
    }

    /// Pick up what a previous daemon left behind.
    ///
    /// Two separate questions, answered separately:
    ///
    /// * queued work was accepted and never started, so there is nothing to reconstruct — it is
    ///   submitted again with the limits it was accepted with;
    /// * interrupted work was in the middle of something, so it is only continued when this daemon
    ///   was explicitly configured to and [`ResumePlan`](crate::orchestrator::resume::ResumePlan)
    ///   accepts it. Otherwise it stays exactly as it is and is reported, never guessed at.
    fn recover(&self) -> Result<(), DaemonError> {
        let mut requests = Vec::new();
        if self.config.recover_queued {
            let queued = crate::orchestrator::scheduler::queued_jobs(&self.store)
                .map_err(DaemonError::State)?;
            if !queued.is_empty() {
                self.emit(DaemonEventKind::QueuedWorkRecovered {
                    job_ids: queued.iter().map(|job| job.job_id.clone()).collect(),
                });
            }
            requests.extend(
                queued
                    .iter()
                    // The limits a job was accepted with are persisted on the job, so recovery
                    // never silently changes them.
                    .map(|job| ScheduledRequest::for_persisted_job(job).with_run(job.run.clone())),
            );
        }

        let interrupted = resumable_jobs(&self.store).map_err(DaemonError::State)?;
        // A job whose lock is held elsewhere is being driven by another Lya process right now. It is
        // not this daemon's to recover, and claiming otherwise would be the one way a daemon could
        // steal live foreground work.
        let (unowned, live): (Vec<JobState>, Vec<JobState>) = interrupted
            .into_iter()
            .partition(|job| JobLock::acquire(&self.store, &job.job_id).is_ok());
        if !live.is_empty() {
            self.emit(DaemonEventKind::InterruptedWorkParked {
                job_ids: live.iter().map(|job| job.job_id.clone()).collect(),
            });
        }
        if !unowned.is_empty() {
            let job_ids = unowned.iter().map(|job| job.job_id.clone()).collect();
            if self.config.resume_interrupted {
                self.emit(DaemonEventKind::InterruptedWorkResumed { job_ids });
                requests.extend(unowned.iter().map(ScheduledRequest::to_resume));
            } else {
                self.emit(DaemonEventKind::InterruptedWorkParked { job_ids });
            }
        }

        if requests.is_empty() {
            return Ok(());
        }
        self.admission
            .submit(requests)
            .map_err(DaemonError::Scheduler)?;
        Ok(())
    }

    fn status(&self) -> Result<DaemonStatus, StateError> {
        let active_ids = self.control.active_job_ids();
        let mut active = Vec::new();
        for job_id in &active_ids {
            if let Some(job) = self.store.load_job(job_id)? {
                active.push(JobSummary::from(&job));
            }
        }
        let queued = self
            .store
            .load_all()?
            .iter()
            .filter(|job| job.status == JobStatus::Queued)
            .map(JobSummary::from)
            .collect();
        let resumable = resumable_jobs(&self.store)?
            .iter()
            // A job this daemon is driving is not "resumable work someone should look at".
            .filter(|job| !active_ids.contains(&job.job_id))
            .map(JobSummary::from)
            .collect();
        Ok(DaemonStatus {
            identity: self.identity.clone(),
            accepting_work: self.admission.is_open() && !self.shutdown.is_requested(),
            max_concurrent: self.config.max_concurrent,
            resume_interrupted: self.config.resume_interrupted,
            connected_clients: self.connected_clients.load(Ordering::Relaxed),
            attached_clients: self.broadcaster.attached_clients(),
            active,
            queued,
            resumable,
        })
    }

    /// Hand submitted work to the scheduler.
    ///
    /// Per-job validation failures reject that job alone: a batch of ten with one unusable path
    /// queues nine and says exactly which one it refused.
    fn submit(&self, jobs: Vec<SubmitJob>) -> Result<Vec<SubmitOutcome>, DaemonErrorResponse> {
        if self.shutdown.is_requested() || !self.admission.is_open() {
            return Err(DaemonErrorResponse::new(
                DaemonErrorCode::ShuttingDown,
                "the daemon is shutting down and is not accepting new work",
            ));
        }
        if jobs.is_empty() {
            return Err(DaemonErrorResponse::new(
                DaemonErrorCode::InvalidRequest,
                "a submission must contain at least one job",
            ));
        }
        if jobs.len() > MAX_REQUESTS {
            return Err(DaemonErrorResponse::new(
                DaemonErrorCode::InvalidRequest,
                format!("a submission may not contain more than {MAX_REQUESTS} jobs"),
            ));
        }

        let mut requests = Vec::new();
        let mut rejected = Vec::new();
        for job in jobs {
            if job.task.trim().is_empty() {
                rejected.push(SubmitOutcome::Rejected {
                    job_id: String::new(),
                    reason: "a task cannot be empty".to_owned(),
                });
                continue;
            }
            let run = match job.run.to_run_configuration() {
                Ok(run) => run,
                Err(error) => {
                    rejected.push(SubmitOutcome::Rejected {
                        job_id: String::new(),
                        reason: error,
                    });
                    continue;
                }
            };
            let path = std::path::PathBuf::from(&job.project_path);
            let name = job
                .project_name
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| project_name(&path));
            requests.push(
                ScheduledRequest::new(Project { name, path }, job.task.trim().to_owned())
                    .with_run(run),
            );
        }

        let mut outcomes = rejected;
        if !requests.is_empty() {
            let submitted = self.admission.submit(requests).map_err(|error| {
                let code = match error {
                    SchedulerError::AdmissionClosed => DaemonErrorCode::ShuttingDown,
                    _ => DaemonErrorCode::Internal,
                };
                DaemonErrorResponse::new(code, error.to_string())
            })?;
            outcomes.extend(submitted.into_iter().map(|outcome| match outcome {
                AdmissionOutcome::Queued {
                    job_id, repository, ..
                } => SubmitOutcome::Queued {
                    job_id,
                    repository: repository.display().to_string(),
                },
                AdmissionOutcome::Rejected { job_id, reason, .. } => {
                    SubmitOutcome::Rejected { job_id, reason }
                }
            }));
        }
        Ok(outcomes)
    }

    /// Deliver one control command to one job.
    ///
    /// The command travels through the job's own existing control channel, so `pause`, `resume`,
    /// `stop`, `status`, `diff` and `send` mean here exactly what they mean in an interactive
    /// `lya run`. Everything this function adds is the decision of whether there *is* a channel to
    /// deliver to, and every way there is not is a distinct, named refusal.
    fn control(
        &self,
        job_id: &str,
        request: &ControlRequest,
    ) -> Result<DaemonResponse, DaemonErrorResponse> {
        let command = request
            .to_command()
            .map_err(|error| DaemonErrorResponse::new(DaemonErrorCode::InvalidRequest, error))?;
        let job = self.store.load_job(job_id).map_err(|error| {
            DaemonErrorResponse::new(DaemonErrorCode::Internal, error.to_string())
        })?;

        match self.control.sender_for(job_id) {
            Some(sender) => {
                if sender.send(command).is_err() {
                    return Err(DaemonErrorResponse::new(
                        DaemonErrorCode::JobNotOwned,
                        format!("job {job_id} stopped accepting control commands"),
                    ));
                }
                Ok(DaemonResponse::Controlled {
                    job_id: job_id.to_owned(),
                    command: request.label().to_owned(),
                })
            }
            None => Err(match job {
                None => DaemonErrorResponse::new(
                    DaemonErrorCode::UnknownJob,
                    format!("no persisted job {job_id}"),
                ),
                Some(job) if is_terminal(&job.status) => DaemonErrorResponse::new(
                    DaemonErrorCode::JobTerminal,
                    format!(
                        "job {job_id} is {} and accepts no control commands",
                        job.status.label()
                    ),
                ),
                Some(job) if job.status == JobStatus::Queued => DaemonErrorResponse::new(
                    DaemonErrorCode::InvalidForState,
                    format!("job {job_id} is queued and has not started yet"),
                ),
                Some(_) => DaemonErrorResponse::new(
                    DaemonErrorCode::JobNotOwned,
                    format!("this daemon is not driving job {job_id}"),
                ),
            }),
        }
    }

    /// How a job may be observed, and why it may not be when it cannot.
    ///
    /// A job that exists is always observable. Only two answers are refusals: a job that was never
    /// persisted, and a store this daemon cannot read.
    fn may_attach(&self, job_id: &str) -> Result<AttachMode, DaemonErrorResponse> {
        if self.control.sender_for(job_id).is_some() {
            return Ok(AttachMode::Live);
        }
        match self.store.load_job(job_id) {
            Ok(None) => Err(DaemonErrorResponse::new(
                DaemonErrorCode::UnknownJob,
                format!("no persisted job {job_id}"),
            )),
            // A finished job used to be refused outright, which made the daemon's own
            // "watch one with: lya attach <job-id>" a broken instruction the moment the job
            // outran the person reading it. It has a recorded history and that is worth showing:
            // the history is replayed and the stream then ends, because a terminal job cannot
            // emit anything further and waiting for it would hang forever. Persisted state is
            // consulted only to establish that the job exists and is terminal; what gets shown
            // comes from the event log, which stays observational.
            Ok(Some(job)) if is_terminal(&job.status) => Ok(AttachMode::History {
                status: job.status.label().to_owned(),
            }),
            // Queued work and work this daemon has not started yet may be watched: the stream opens
            // now and carries events as soon as they exist.
            Ok(Some(_)) => Ok(AttachMode::Live),
            Err(error) => Err(DaemonErrorResponse::new(
                DaemonErrorCode::Internal,
                error.to_string(),
            )),
        }
    }
}

/// What an attach can actually deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AttachMode {
    /// The job can still emit events, so the stream stays open and follows them.
    Live,
    /// The job has finished. Its recorded history is replayed and the stream ends.
    History { status: String },
}

impl AttachMode {
    fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }
}

/// Statuses that accept nothing further. `PAUSED` and the quota waits are not among them: they are
/// exactly the states a control command or a resume is for.
fn is_terminal(status: &JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Accepted
            | JobStatus::Published
            | JobStatus::Failed
            | JobStatus::Stopped
            | JobStatus::WaitingHuman
    )
}

fn project_name(path: &std::path::Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("current-project")
        .to_owned()
}

/// What to do with a connection after one request.
enum Flow {
    /// Keep reading requests from this client.
    Continue,
    /// Close this connection.
    Close,
    /// Hand the connection over to an event stream.
    Attach {
        job_id: String,
        replay: bool,
        mode: AttachMode,
    },
}

/// How long a connected client has to send its first frame.
///
/// A connection that has arrived but said nothing costs the daemon a task and a pipe instance for
/// as long as it lasts, and nothing about arriving proves anything is coming. Generous enough that
/// no real client on a loaded machine can miss it, finite so an idle connection cannot be held open
/// for ever. It applies to the opening request only: once a client has asked for an event stream,
/// silence is exactly what a viewer is supposed to produce.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve one client.
///
/// Everything that can go wrong here ends this connection and nothing else: a frame that is not a
/// request, a frame that is too long, a client that vanishes, a client that stops reading, a client
/// that connects and never speaks. The daemon, the scheduler, the jobs and every other client are
/// unaffected.
async fn serve_client(state: Arc<DaemonState>, connection: DaemonConnection) {
    let client_id = state.next_client_id.fetch_add(1, Ordering::Relaxed);
    state.connected_clients.fetch_add(1, Ordering::Relaxed);
    state.emit(DaemonEventKind::ClientConnected { client_id });

    let (mut reader, mut writer) = connection.split();
    let mut flow = Flow::Continue;
    let mut reason = "the client closed the connection".to_owned();
    let mut opening = true;

    loop {
        let received = if opening {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, reader.read_frame()).await {
                Ok(received) => received,
                Err(_) => {
                    reason = format!(
                        "the client sent no request within {} seconds",
                        HANDSHAKE_TIMEOUT.as_secs()
                    );
                    state.emit(DaemonEventKind::ClientRejected {
                        client_id,
                        code: DaemonErrorCode::InvalidRequest.label().to_owned(),
                        reason: reason.clone(),
                    });
                    break;
                }
            }
        } else {
            reader.read_frame().await
        };
        opening = false;
        let line = match received {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                reason = error.to_string();
                if matches!(error, TransportError::FrameTooLong) {
                    state.emit(DaemonEventKind::ClientRejected {
                        client_id,
                        code: DaemonErrorCode::InvalidRequest.label().to_owned(),
                        reason: error.to_string(),
                    });
                }
                break;
            }
        };
        let response = match decode_request(&line) {
            Ok(request) => {
                let (response, next) = handle(&state, client_id, request).await;
                flow = next;
                response
            }
            Err(error) => {
                // A client that speaks nonsense is told so and may try again. It is never a reason
                // to end the daemon, and never a reason to touch another client.
                state.emit(DaemonEventKind::ClientRejected {
                    client_id,
                    code: error.code.label().to_owned(),
                    reason: error.message.clone(),
                });
                DaemonResponse::Error { error }
            }
        };
        if !write_response(&mut writer, response).await {
            reason = "the client stopped reading".to_owned();
            flow = Flow::Close;
            break;
        }
        match flow {
            Flow::Continue => continue,
            Flow::Close => break,
            Flow::Attach { .. } => break,
        }
    }

    if let Flow::Attach {
        job_id,
        replay,
        mode,
    } = flow
    {
        let detach_reason = stream_events(&state, &job_id, replay, &mode, reader, writer).await;
        state.emit(DaemonEventKind::JobDetached {
            client_id,
            job_id,
            reason: detach_reason.clone(),
        });
        reason = detach_reason;
    } else {
        writer.shutdown().await;
    }

    state.connected_clients.fetch_sub(1, Ordering::Relaxed);
    state.emit(DaemonEventKind::ClientDisconnected { client_id, reason });
}

async fn write_response(writer: &mut DaemonWriter, response: DaemonResponse) -> bool {
    let Ok(frame) = encode_response(response) else {
        return false;
    };
    writer.write_frame(&frame).await.is_ok()
}

async fn handle(
    state: &Arc<DaemonState>,
    client_id: u64,
    request: DaemonRequest,
) -> (DaemonResponse, Flow) {
    match request {
        DaemonRequest::Ping => (
            DaemonResponse::Pong {
                daemon: state.identity.clone(),
            },
            Flow::Continue,
        ),
        DaemonRequest::Status => match state.status() {
            Ok(status) => (
                DaemonResponse::Status {
                    status: Box::new(status),
                },
                Flow::Continue,
            ),
            Err(error) => (
                DaemonResponse::Error {
                    error: DaemonErrorResponse::new(DaemonErrorCode::Internal, error.to_string()),
                },
                Flow::Continue,
            ),
        },
        DaemonRequest::Submit { jobs } => match state.submit(jobs) {
            Ok(outcomes) => {
                let job_ids = outcomes
                    .iter()
                    .filter_map(|outcome| match outcome {
                        SubmitOutcome::Queued { job_id, .. } => Some(job_id.clone()),
                        SubmitOutcome::Rejected { .. } => None,
                    })
                    .collect::<Vec<_>>();
                if !job_ids.is_empty() {
                    state.emit(DaemonEventKind::JobsSubmitted { client_id, job_ids });
                }
                (DaemonResponse::Submitted { jobs: outcomes }, Flow::Continue)
            }
            Err(error) => (DaemonResponse::Error { error }, Flow::Continue),
        },
        DaemonRequest::Control { job_id, command } => match state.control(&job_id, &command) {
            Ok(response) => {
                state.emit(DaemonEventKind::ControlDelivered {
                    client_id,
                    job_id,
                    command: command.label().to_owned(),
                });
                (response, Flow::Continue)
            }
            Err(error) => (DaemonResponse::Error { error }, Flow::Continue),
        },
        DaemonRequest::Attach { job_id, replay } => match state.may_attach(&job_id) {
            Ok(mode) => {
                state.emit(DaemonEventKind::JobAttached {
                    client_id,
                    job_id: job_id.clone(),
                });
                (
                    DaemonResponse::Attached {
                        job_id: job_id.clone(),
                        live: mode.is_live(),
                    },
                    Flow::Attach {
                        job_id,
                        replay,
                        mode,
                    },
                )
            }
            Err(error) => (DaemonResponse::Error { error }, Flow::Continue),
        },
        DaemonRequest::Shutdown => {
            let active_jobs = state.control.active_job_ids().len();
            state.shutdown.request("a client requested a shutdown");
            (DaemonResponse::ShuttingDown { active_jobs }, Flow::Close)
        }
    }
}

/// Stream one job's events to one client until either side ends it.
///
/// The subscription is opened before any history is read, so an event emitted while the replay is
/// being written is queued rather than lost, and [`ReplayGuard`] keeps it from being shown twice.
///
/// Both halves of the connection are watched. Writing carries the events; reading exists only to
/// notice the client going away, which is what detaching *is* — `Ctrl+C` in `lya attach` closes the
/// connection, and the daemon should forget the viewer then rather than at the next event. A client
/// on an event stream has nothing to say, so anything it does send is ignored.
///
/// A terminal job takes the short path instead: its history is replayed and the stream ends. No
/// subscription is opened for it, because a job that has finished has no publisher and a viewer
/// waiting on one would wait for as long as it was willing to.
///
/// Nothing here can affect the job: a client that leaves, falls behind or closes mid-frame ends this
/// stream alone.
async fn stream_events(
    state: &Arc<DaemonState>,
    job_id: &str,
    replay: bool,
    mode: &AttachMode,
    mut reader: DaemonReader,
    mut writer: DaemonWriter,
) -> String {
    use tokio::sync::broadcast::error::RecvError;

    if let AttachMode::History { status } = mode {
        return replay_terminal_job(state, job_id, status, writer).await;
    }

    let mut subscription = state.broadcaster.subscribe(job_id);
    let mut guard = ReplayGuard::new();
    if replay {
        for event in recorded_events(state.home.path(), job_id) {
            guard.record(&event);
            if !write_response(
                &mut writer,
                DaemonResponse::Event {
                    event: Box::new(event),
                },
            )
            .await
            {
                return "the client closed the stream".to_owned();
            }
        }
    }

    loop {
        tokio::select! {
            received = reader.read_frame() => match received {
                // Anything a viewer sends on its stream is ignored; the stream is observational.
                Ok(Some(_)) => continue,
                Ok(None) => return "the client detached".to_owned(),
                Err(error) => return error.to_string(),
            },
            reason = state.shutdown.wait() => {
                let _ = write_response(&mut writer, DaemonResponse::Detached {
                    job_id: job_id.to_owned(),
                    reason: reason.clone(),
                }).await;
                writer.shutdown().await;
                return reason;
            }
            received = subscription.recv() => match received {
                Ok(event) => {
                    if guard.already_shown(&event) {
                        continue;
                    }
                    if !write_response(
                        &mut writer,
                        DaemonResponse::Event {
                            event: Box::new(event.as_ref().clone()),
                        },
                    )
                    .await
                    {
                        return "the client closed the stream".to_owned();
                    }
                }
                Err(RecvError::Lagged(missed)) => {
                    let reason = format!("the client fell behind by {missed} event(s)");
                    let _ = write_response(&mut writer, DaemonResponse::Detached {
                        job_id: job_id.to_owned(),
                        reason: reason.clone(),
                    }).await;
                    writer.shutdown().await;
                    return reason;
                }
                Err(RecvError::Closed) => {
                    let reason = format!("job {job_id} is no longer emitting events");
                    let _ = write_response(&mut writer, DaemonResponse::Detached {
                        job_id: job_id.to_owned(),
                        reason: reason.clone(),
                    }).await;
                    writer.shutdown().await;
                    return reason;
                }
            },
        }
    }
}

/// Replay a finished job's recorded history, then end the stream.
///
/// No subscription and no read half: there is no publisher to wait for and nothing a viewer could
/// say. The reason the stream ends names the job's authoritative status and how much history was
/// found, so a job whose event log is missing, empty or unreadable still produces a useful answer
/// instead of silence. Unparseable lines are skipped by [`recorded_events`], which is deliberately
/// tolerant: a log truncated by a crash is a record, never an authority.
async fn replay_terminal_job(
    state: &Arc<DaemonState>,
    job_id: &str,
    status: &str,
    mut writer: DaemonWriter,
) -> String {
    let history = recorded_events(state.home.path(), job_id);
    let recorded = history.len();
    for event in history {
        if !write_response(
            &mut writer,
            DaemonResponse::Event {
                event: Box::new(event),
            },
        )
        .await
        {
            return "the client closed the stream".to_owned();
        }
    }
    let reason = if recorded == 0 {
        format!("job {job_id} is {status}; no recorded events were found")
    } else {
        format!("job {job_id} is {status}; replayed {recorded} recorded event(s)")
    };
    let _ = write_response(
        &mut writer,
        DaemonResponse::Detached {
            job_id: job_id.to_owned(),
            reason: reason.clone(),
        },
    )
    .await;
    writer.shutdown().await;
    reason
}

#[derive(Debug)]
pub enum DaemonError {
    Claim(DaemonLockError),
    Transport(TransportError),
    Scheduler(SchedulerError),
    State(StateError),
    Worker(String),
}

impl fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Claim(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Scheduler(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Worker(error) => write!(formatter, "a daemon task ended abnormally: {error}"),
        }
    }
}

impl Error for DaemonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Claim(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Scheduler(error) => Some(error),
            Self::State(error) => Some(error),
            Self::Worker(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
    };

    use super::{Daemon, DaemonConfig, DaemonError, DaemonOutcome};
    use crate::{
        daemon::{
            attach::JobEventBroadcaster,
            client::{ClientError, DaemonClient, wait_until_ready},
            events::{DaemonEvent, DaemonEventKind, DaemonEventSink},
            lock::{DaemonLock, DaemonMetadata},
            protocol::{
                ControlRequest, DaemonRequest, DaemonResponse, DaemonStatus, PROTOCOL_VERSION,
                RunOptions, SubmitJob, SubmitOutcome,
            },
            transport::{DaemonEndpoint, connect},
        },
        orchestrator::{
            control::ControlCommand,
            events::{EventSink, EventSinkError, JobEvent, JobEventKind},
            home::LyaHome,
            job::new_job_id,
            lock::JobLock,
            repository_lock::{RepositoryIdentity, RepositoryLock},
            scheduler::{JobAssignment, JobDriver, JobOutcome, ScheduleMode, ScheduledOutcome},
            state::{
                JobPhase, JobState, JobStatus, PendingOperation, RunConfiguration, StateStore,
            },
            supervisor::{Project, SupervisorDecision},
        },
    };

    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn unique(prefix: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lya-{prefix}-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("test directory should be created");
        path
    }

    fn home() -> LyaHome {
        LyaHome::from_path(unique("daemon-home"))
    }

    /// A disposable directory that looks like a Git working tree to the identity resolver.
    fn repository(name: &str) -> PathBuf {
        let path = unique(name);
        fs::create_dir_all(path.join(".git")).expect("repository marker should be created");
        path
    }

    fn job(project: &Path, task: &str) -> SubmitJob {
        SubmitJob {
            project_path: project.display().to_string(),
            project_name: None,
            task: task.to_owned(),
            run: RunOptions::default(),
        }
    }

    /// What a driven job told the test about itself.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Started {
        job_id: String,
        task: String,
        mode: ScheduleMode,
    }

    /// A driver that reports every job it is handed and finishes only when the test lets it.
    ///
    /// It stands in for the real orchestrator and does what the real one does at the boundary: it
    /// reads its control channel, emits events through the broadcaster and returns an outcome.
    /// Nothing here sleeps — every wait is a channel.
    struct TestDriver {
        /// Where the driven job's authoritative state lives. The real orchestrator's first act is to
        /// persist the job it was handed as `RUNNING`, and its last is to persist a terminal status;
        /// a stand-in that skipped that would make recovery tests lie.
        home: PathBuf,
        started: mpsc::UnboundedSender<Started>,
        broadcaster: JobEventBroadcaster,
        /// Release channels keyed by task, so two jobs can be released independently.
        gates: Mutex<HashMap<String, oneshot::Receiver<()>>>,
        /// Senders of gates nobody asked for, kept alive so those jobs simply keep running.
        retained: Mutex<Vec<oneshot::Sender<()>>>,
        control: Arc<Mutex<Vec<(String, ControlCommand)>>>,
        /// Jobs that observed a graceful stop on their own control channel.
        stopped: Arc<Mutex<Vec<String>>>,
        /// Jobs that finished because the test released them rather than because of a stop.
        released: Arc<Mutex<Vec<String>>>,
    }

    struct TestDriverHandles {
        started: mpsc::UnboundedReceiver<Started>,
        control: Arc<Mutex<Vec<(String, ControlCommand)>>>,
        stopped: Arc<Mutex<Vec<String>>>,
        released: Arc<Mutex<Vec<String>>>,
        gates: HashMap<String, oneshot::Sender<()>>,
    }

    impl TestDriver {
        /// Builds a driver whose named tasks can be released one by one.
        fn new(
            home: &LyaHome,
            broadcaster: JobEventBroadcaster,
            tasks: &[&str],
        ) -> (Self, TestDriverHandles) {
            let (started, started_receiver) = mpsc::unbounded_channel();
            let mut gates = HashMap::new();
            let mut senders = HashMap::new();
            for task in tasks {
                let (sender, receiver) = oneshot::channel();
                gates.insert((*task).to_owned(), receiver);
                senders.insert((*task).to_owned(), sender);
            }
            let control = Arc::new(Mutex::new(Vec::new()));
            let stopped = Arc::new(Mutex::new(Vec::new()));
            let released = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    home: home.path().to_owned(),
                    started,
                    broadcaster,
                    gates: Mutex::new(gates),
                    retained: Mutex::new(Vec::new()),
                    control: Arc::clone(&control),
                    stopped: Arc::clone(&stopped),
                    released: Arc::clone(&released),
                },
                TestDriverHandles {
                    started: started_receiver,
                    control,
                    stopped,
                    released,
                    gates: senders,
                },
            )
        }

        /// Take ownership of a job the way the real orchestrator does: by writing its status.
        fn record(&self, job_id: &str, status: JobStatus) {
            let store = StateStore::at(&self.home);
            if let Ok(Some(mut job)) = store.load_job(job_id) {
                job.status = status;
                job.touch();
                let _ = store.save_job(&job);
            }
        }

        fn gate(&self, task: &str) -> oneshot::Receiver<()> {
            if let Some(gate) = self.gates.lock().expect("gate lock").remove(task) {
                return gate;
            }
            // No gate was prepared for this task, so it runs until it is stopped.
            let (sender, receiver) = oneshot::channel();
            self.retained.lock().expect("gate lock").push(sender);
            receiver
        }
    }

    impl JobDriver for TestDriver {
        fn drive<'a>(
            &'a self,
            assignment: JobAssignment,
        ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
            Box::pin(async move {
                let job_id = assignment.job_id.clone();
                let sink = self.broadcaster.sink(&job_id);
                let mut gate = self.gate(&assignment.task);
                let _ = self.started.send(Started {
                    job_id: job_id.clone(),
                    task: assignment.task.clone(),
                    mode: assignment.mode,
                });
                let _ = sink.emit(&event(&job_id, "job started"));
                self.record(&job_id, JobStatus::Running);

                let mut stopped = false;
                let mut released = false;
                loop {
                    tokio::select! {
                        command = assignment.control.next() => match command {
                            Some(command) => {
                                self.control
                                    .lock()
                                    .expect("control lock")
                                    .push((job_id.clone(), command.clone()));
                                let _ = sink.emit(&event(&job_id, &format!("control {command:?}")));
                                if matches!(command, ControlCommand::Stop) {
                                    stopped = true;
                                    break;
                                }
                            }
                            None => break,
                        },
                        _ = &mut gate => {
                            released = true;
                            break;
                        }
                    }
                }
                if stopped {
                    self.stopped.lock().expect("stop lock").push(job_id.clone());
                }
                if released {
                    self.released
                        .lock()
                        .expect("release lock")
                        .push(job_id.clone());
                }
                let status = if stopped {
                    JobStatus::Stopped
                } else {
                    JobStatus::Accepted
                };
                self.record(&job_id, status.clone());
                self.broadcaster.release(&job_id);
                JobOutcome::Finished {
                    status: status.label().to_owned(),
                    jobs: 1,
                    succeeded: true,
                }
            })
        }
    }

    fn event(job_id: &str, message: &str) -> JobEvent {
        JobEvent::new(
            job_id,
            &Project {
                name: "project".to_owned(),
                path: PathBuf::from("/work/project"),
            },
            Some(1),
            JobEventKind::ControlMessage {
                message: message.to_owned(),
            },
        )
    }

    #[derive(Default)]
    struct RecordingDaemonSink {
        events: Mutex<Vec<DaemonEvent>>,
    }

    impl RecordingDaemonSink {
        fn kinds(&self) -> Vec<DaemonEventKind> {
            self.events
                .lock()
                .expect("event lock")
                .iter()
                .map(|event| event.kind.clone())
                .collect()
        }
    }

    impl DaemonEventSink for RecordingDaemonSink {
        fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
            self.events.lock().expect("event lock").push(event.clone());
            Ok(())
        }
    }

    /// One running daemon, owned by the test.
    struct RunningDaemon {
        home: LyaHome,
        task: JoinHandle<Result<DaemonOutcome, DaemonError>>,
        events: Arc<RecordingDaemonSink>,
    }

    impl RunningDaemon {
        async fn start(
            home: &LyaHome,
            driver: TestDriver,
            broadcaster: JobEventBroadcaster,
            config: DaemonConfig,
        ) -> Self {
            let events = Arc::new(RecordingDaemonSink::default());
            let daemon = Daemon::new(LyaHome::from_path(home.path()), driver)
                .with_config(config)
                .with_broadcaster(broadcaster)
                .with_event_sink(Arc::clone(&events) as Arc<dyn DaemonEventSink>);
            let task = tokio::spawn(daemon.serve());
            let identity = wait_until_ready(home)
                .await
                .expect("the endpoint should be reachable")
                .expect("the daemon should become ready");
            assert_eq!(identity.protocol_version, PROTOCOL_VERSION);
            Self {
                home: LyaHome::from_path(home.path()),
                task,
                events,
            }
        }

        async fn client(&self) -> DaemonClient {
            DaemonClient::connect(&self.home)
                .await
                .expect("a client should connect")
        }

        /// Ask for a graceful shutdown and wait for the daemon to finish.
        async fn stop(self) -> DaemonOutcome {
            let mut client = self.client().await;
            let response = client
                .request(DaemonRequest::Shutdown)
                .await
                .expect("a shutdown should be accepted");
            assert!(matches!(response, DaemonResponse::ShuttingDown { .. }));
            client.close().await;
            self.task
                .await
                .expect("the daemon task should finish")
                .expect("the daemon should shut down cleanly")
        }
    }

    async fn status(client: &mut DaemonClient) -> DaemonStatus {
        match client
            .request(DaemonRequest::Status)
            .await
            .expect("status should be served")
        {
            DaemonResponse::Status { status } => *status,
            other => panic!("expected a status, got {other:?}"),
        }
    }

    async fn submit(client: &mut DaemonClient, jobs: Vec<SubmitJob>) -> Vec<SubmitOutcome> {
        match client
            .request(DaemonRequest::Submit { jobs })
            .await
            .expect("a submission should be accepted")
        {
            DaemonResponse::Submitted { jobs } => jobs,
            other => panic!("expected a submission result, got {other:?}"),
        }
    }

    fn queued_id(outcomes: &[SubmitOutcome]) -> String {
        match outcomes.first().expect("one outcome") {
            SubmitOutcome::Queued { job_id, .. } => job_id.clone(),
            SubmitOutcome::Rejected { reason, .. } => panic!("the job was rejected: {reason}"),
        }
    }

    /// A persisted job that was interrupted mid-executor-run: resumable, and exactly the shape that
    /// must never be replayed carelessly.
    fn persist_interrupted_job(home: &LyaHome, project: &Path, task: &str) -> String {
        let store = StateStore::at(home.path());
        let job_id = new_job_id();
        let mut job = JobState::new(&job_id, "project", project.to_owned(), task);
        job.status = JobStatus::Running;
        job.phase = JobPhase::Executor;
        job.iteration = 1;
        job.pending_operation = Some(PendingOperation::ExecutorRun);
        job.last_supervisor_decision = Some(SupervisorDecision::Claude {
            prompt: "continue the work".to_owned(),
            reason: None,
        });
        job.run = RunConfiguration::default();
        store.save_job(&job).expect("the job should persist");
        job_id
    }

    // --------------------------------------------------------------------------------------------
    // Identity and exclusion
    // --------------------------------------------------------------------------------------------

    /// The central guarantee: one `LYA_HOME`, one daemon. The second one is refused by the claim
    /// before it can bind an endpoint or touch a job.
    #[tokio::test]
    async fn a_second_daemon_for_one_home_is_refused() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;

        let (second_driver, _second_handles) =
            TestDriver::new(&home, JobEventBroadcaster::new(), &[]);
        let error = Daemon::new(LyaHome::from_path(home.path()), second_driver)
            .serve()
            .await
            .expect_err("a second daemon must be refused");

        assert!(matches!(error, DaemonError::Claim(_)), "{error}");
        assert!(error.to_string().contains("already running"));
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    /// Discovery needs no registry: a client resolves the same endpoint from the same home and the
    /// daemon answers with its own identity.
    #[tokio::test]
    async fn a_client_discovers_and_identifies_the_daemon_of_its_home() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;

        let mut client = daemon.client().await;
        let response = client
            .request(DaemonRequest::Ping)
            .await
            .expect("a ping should be answered");

        match response {
            DaemonResponse::Pong { daemon: identity } => {
                assert_eq!(identity.process_id, std::process::id());
                assert_eq!(identity.protocol_version, PROTOCOL_VERSION);
                assert_eq!(
                    identity.endpoint,
                    DaemonEndpoint::for_home(&home).address(),
                    "the daemon reports the endpoint a client derives from the home"
                );
            }
            other => panic!("expected a pong, got {other:?}"),
        }
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    // --------------------------------------------------------------------------------------------
    // Robustness against clients
    // --------------------------------------------------------------------------------------------

    /// A client that speaks nonsense is told so, keeps its connection, and changes nothing for
    /// anyone else.
    #[tokio::test]
    async fn a_malformed_request_is_refused_without_ending_the_daemon() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let endpoint = DaemonEndpoint::for_home(&home);

        let mut raw = connect(&endpoint)
            .await
            .expect("a raw client should connect");
        for garbage in [
            "this is not json\n",
            "{}\n",
            "{\"request\":\"NOT_A_REQUEST\"}\n",
            "{\"protocol_version\":999,\"message\":{\"request\":\"PING\"}}\n",
            "\n",
        ] {
            raw.write_frame(garbage.as_bytes())
                .await
                .expect("the daemon should still be accepting frames");
            let reply = raw
                .read_frame()
                .await
                .expect("the daemon should answer")
                .expect("the daemon should not close the connection");
            let value: serde_json::Value =
                serde_json::from_str(&reply).expect("every answer is one JSON object");
            assert_eq!(
                value["message"]["response"], "ERROR",
                "{garbage} -> {reply}"
            );
            assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        }
        // The same connection still works for a real request.
        raw.write_frame(
            &crate::daemon::protocol::encode_request(DaemonRequest::Ping)
                .expect("a ping should encode"),
        )
        .await
        .expect("the connection should still be usable");
        let reply = raw
            .read_frame()
            .await
            .expect("the ping should be answered")
            .expect("the connection is still open");
        assert!(reply.contains("PONG"), "{reply}");

        // And so does a fresh one.
        let mut client = daemon.client().await;
        assert!(status(&mut client).await.accepting_work);
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    /// One client's failure is one client's failure. A connection dropped mid-request cannot reach
    /// another client or the daemon.
    #[tokio::test]
    async fn one_failing_client_does_not_affect_another() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let endpoint = DaemonEndpoint::for_home(&home);
        let mut survivor = daemon.client().await;

        // A client that writes half a frame and vanishes.
        let mut broken = connect(&endpoint)
            .await
            .expect("a raw client should connect");
        broken
            .write_frame(b"{\"protocol_version\":1,\"request\":\"PI")
            .await
            .expect("a partial frame should be writable");
        drop(broken);

        let status = status(&mut survivor).await;

        assert!(status.accepting_work);
        assert_eq!(status.identity.process_id, std::process::id());
        survivor.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    /// Concurrent clients share one daemon and one protocol. Every answer must still be exactly one
    /// well-formed frame.
    #[tokio::test]
    async fn framing_stays_valid_under_concurrent_clients() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let endpoint = DaemonEndpoint::for_home(&home);

        let mut clients = Vec::new();
        for _ in 0..8 {
            let endpoint = endpoint.clone();
            clients.push(tokio::spawn(async move {
                let mut connection = connect(&endpoint).await.expect("a client should connect");
                let mut replies = Vec::new();
                for request in [DaemonRequest::Ping, DaemonRequest::Status] {
                    connection
                        .write_frame(
                            &crate::daemon::protocol::encode_request(request)
                                .expect("a request should encode"),
                        )
                        .await
                        .expect("a request should be writable");
                    let reply = connection
                        .read_frame()
                        .await
                        .expect("a reply should arrive")
                        .expect("the daemon should not close the connection");
                    replies.push(reply);
                }
                replies
            }));
        }

        for client in clients {
            let replies = client.await.expect("a client task should finish");
            assert_eq!(replies.len(), 2);
            for reply in replies {
                assert!(
                    !reply.contains('\n'),
                    "a frame is exactly one line: {reply}"
                );
                let value: serde_json::Value =
                    serde_json::from_str(&reply).expect("every frame is one JSON object");
                assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
                assert!(
                    matches!(
                        value["message"]["response"].as_str(),
                        Some("PONG" | "STATUS")
                    ),
                    "{reply}"
                );
            }
        }
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    // --------------------------------------------------------------------------------------------
    // Submitting work
    // --------------------------------------------------------------------------------------------

    /// Submitted work becomes an ordinary persisted Lya job before the client is answered, and the
    /// scheduler picks it up.
    #[tokio::test]
    async fn submitted_work_is_persisted_and_consumed_by_the_scheduler() {
        let home = home();
        let project = repository("daemon-submit");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["do the work"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let outcomes = submit(&mut client, vec![job(&project, "do the work")]).await;

        let job_id = queued_id(&outcomes);
        let persisted = StateStore::at(home.path())
            .load_job(&job_id)
            .expect("state should be readable")
            .expect("submitted work exists as a persisted job");
        assert_eq!(persisted.task, "do the work");
        assert!(
            matches!(persisted.status, JobStatus::Queued | JobStatus::Running),
            "submitted work is durable before it starts: {:?}",
            persisted.status
        );

        let started = handles
            .started
            .recv()
            .await
            .expect("the scheduler should drive the submitted job");
        assert_eq!(started.job_id, job_id);
        assert_eq!(started.mode, ScheduleMode::Start);

        let mut after = status(&mut client).await;
        assert_eq!(after.active.len(), 1);
        assert_eq!(after.active.remove(0).job_id, job_id);

        handles
            .gates
            .remove("do the work")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        let outcome = daemon.stop().await;
        assert!(
            outcome.report.jobs.iter().any(|job| job.job_id == job_id
                && matches!(job.outcome, ScheduledOutcome::Finished { .. })),
            "{:?}",
            outcome.report.jobs
        );
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// A submission carries the run limits the job is accepted with, and they are persisted on the
    /// job rather than remembered by the client.
    #[tokio::test]
    async fn submitted_run_options_are_persisted_on_the_job() {
        let home = home();
        let project = repository("daemon-options");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["bounded"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let outcomes = submit(
            &mut client,
            vec![SubmitJob {
                project_path: project.display().to_string(),
                project_name: Some("named".to_owned()),
                task: "bounded".to_owned(),
                run: RunOptions {
                    max_iterations: 3,
                    max_jobs: 2,
                    browser: true,
                    publish: false,
                    git: None,
                },
            }],
        )
        .await;

        let job_id = queued_id(&outcomes);
        let persisted = StateStore::at(home.path())
            .load_job(&job_id)
            .expect("state should be readable")
            .expect("the job exists");
        assert_eq!(persisted.project_name, "named");
        assert_eq!(persisted.run.max_iterations, 3);
        assert_eq!(persisted.run.max_jobs, 2);
        assert!(persisted.run.browser);
        assert!(!persisted.run.publish);

        handles
            .started
            .recv()
            .await
            .expect("the job should be driven");
        handles
            .gates
            .remove("bounded")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// One unusable job in a submission rejects that job alone and says why.
    #[tokio::test]
    async fn an_unusable_job_is_rejected_without_affecting_the_rest() {
        let home = home();
        let project = repository("daemon-partial");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["good"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let outcomes = submit(
            &mut client,
            vec![
                job(Path::new("this-path-does-not-exist-anywhere"), "bad"),
                job(&project, "good"),
                SubmitJob {
                    project_path: project.display().to_string(),
                    project_name: None,
                    task: "   ".to_owned(),
                    run: RunOptions::default(),
                },
            ],
        )
        .await;

        assert_eq!(outcomes.len(), 3);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, SubmitOutcome::Queued { .. }))
                .count(),
            1,
            "{outcomes:?}"
        );
        assert!(
            outcomes.iter().any(|outcome| matches!(
                outcome,
                SubmitOutcome::Rejected { reason, .. } if reason.contains("task cannot be empty")
            )),
            "{outcomes:?}"
        );

        handles
            .started
            .recv()
            .await
            .expect("the usable job should still run");
        handles
            .gates
            .remove("good")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    // --------------------------------------------------------------------------------------------
    // Attach and detach
    // --------------------------------------------------------------------------------------------

    /// An attached client sees the events a job emits while it is running.
    #[tokio::test]
    async fn an_attached_client_sees_live_job_events() {
        let home = home();
        let project = repository("daemon-attach");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["watch me"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;
        let outcomes = submit(&mut client, vec![job(&project, "watch me")]).await;
        let job_id = queued_id(&outcomes);
        handles
            .started
            .recv()
            .await
            .expect("the job should be driven");

        let mut viewer = daemon.client().await;
        let attached = viewer
            .request(DaemonRequest::Attach {
                job_id: job_id.clone(),
                replay: false,
            })
            .await
            .expect("attaching should be accepted");
        assert!(matches!(attached, DaemonResponse::Attached { .. }));

        // A control command the job echoes as an event: the live stream carries what happens after
        // the client attached.
        client
            .request(DaemonRequest::Control {
                job_id: job_id.clone(),
                command: ControlRequest::Status,
            })
            .await
            .expect("the control command should be delivered");

        let event = loop {
            match viewer
                .next_message()
                .await
                .expect("the stream should stay healthy")
            {
                Some(DaemonResponse::Event { event }) => {
                    if matches!(&event.kind, JobEventKind::ControlMessage { message } if message.contains("Status"))
                    {
                        break event;
                    }
                }
                Some(other) => panic!("unexpected stream message: {other:?}"),
                None => panic!("the stream closed before the event arrived"),
            }
        };
        assert_eq!(event.job_id, job_id);

        let mut watching = status(&mut client).await;
        assert_eq!(watching.attached_clients, 1);
        assert_eq!(watching.active.remove(0).job_id, job_id);

        viewer.close().await;
        handles
            .gates
            .remove("watch me")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// Replay puts recorded history in front of the live stream, and shows each recorded event once.
    #[tokio::test]
    async fn a_replayed_attach_shows_recorded_history_before_live_events() {
        let home = home();
        let project = repository("daemon-replay");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["replay me"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;
        let outcomes = submit(&mut client, vec![job(&project, "replay me")]).await;
        let job_id = queued_id(&outcomes);
        handles
            .started
            .recv()
            .await
            .expect("the job should be driven");
        // Recorded history a real job would have written.
        let recorded = crate::orchestrator::events::JsonlEventSink::for_job(home.path(), &job_id);
        recorded
            .emit(&event(&job_id, "historical event"))
            .expect("history should be recorded");

        let mut viewer = daemon.client().await;
        viewer
            .request(DaemonRequest::Attach {
                job_id: job_id.clone(),
                replay: true,
            })
            .await
            .expect("attaching should be accepted");

        match viewer
            .next_message()
            .await
            .expect("the replay should be readable")
        {
            Some(DaemonResponse::Event { event }) => assert!(
                matches!(&event.kind, JobEventKind::ControlMessage { message } if message == "historical event"),
                "{:?}",
                event.kind
            ),
            other => panic!("expected the replayed event, got {other:?}"),
        }

        viewer.close().await;
        handles
            .gates
            .remove("replay me")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// The guarantee that makes attach safe to use: a viewer leaving is not a job ending. This is
    /// what Ctrl+C in `lya attach` does.
    #[tokio::test]
    async fn detaching_a_viewer_never_stops_the_job() {
        let home = home();
        let project = repository("daemon-detach");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["keep going"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;
        let outcomes = submit(&mut client, vec![job(&project, "keep going")]).await;
        let job_id = queued_id(&outcomes);
        handles
            .started
            .recv()
            .await
            .expect("the job should be driven");

        let mut viewer = daemon.client().await;
        viewer
            .request(DaemonRequest::Attach {
                job_id: job_id.clone(),
                replay: false,
            })
            .await
            .expect("attaching should be accepted");
        viewer.close().await;

        // The daemon notices the viewer leaving rather than waiting for the next event.
        loop {
            if status(&mut client).await.attached_clients == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }

        // The job is still being driven, and still controllable, after its viewer left.
        client
            .request(DaemonRequest::Control {
                job_id: job_id.clone(),
                command: ControlRequest::Status,
            })
            .await
            .expect("the job must still be driven after its viewer detached");
        let mut after = status(&mut client).await;
        assert_eq!(after.active.len(), 1, "{after:?}");
        assert_eq!(after.active.remove(0).job_id, job_id);
        assert!(
            handles.stopped.lock().expect("stop lock").is_empty(),
            "a detaching viewer must never stop a job"
        );

        handles
            .gates
            .remove("keep going")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        assert_eq!(
            handles.released.lock().expect("release lock").as_slice(),
            &[job_id],
            "the job finished on its own terms, not because a viewer left"
        );
        assert!(handles.stopped.lock().expect("stop lock").is_empty());
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// Attaching to a job that was never persisted still fails with a reason, never silently.
    ///
    /// This is the one attach refusal left: a job that does not exist. A job that *does* exist is
    /// always observable, whether it is still running or already finished.
    #[tokio::test]
    async fn attaching_to_an_unknown_job_fails_clearly() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let unknown = client
            .request(DaemonRequest::Attach {
                job_id: "no-such-job".to_owned(),
                replay: false,
            })
            .await
            .expect_err("an unknown job cannot be attached");

        assert!(
            matches!(&unknown, ClientError::Daemon(error)
                if error.code == crate::daemon::protocol::DaemonErrorCode::UnknownJob),
            "{unknown}"
        );
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    /// A finished job replays its recorded history and the stream ends.
    ///
    /// The daemon tells people to "watch one with: lya attach <job-id>", and a job that finished
    /// before they got there used to answer `JOB_TERMINAL`. It has a history worth showing, so it
    /// shows it. Every terminal status behaves the same way, and none of them waits: a job with no
    /// publisher left would otherwise hold a viewer open for as long as it was willing to wait.
    #[tokio::test]
    async fn a_terminal_job_replays_its_history_and_ends_the_stream() {
        for status in [
            JobStatus::Accepted,
            JobStatus::Published,
            JobStatus::Failed,
            JobStatus::Stopped,
            JobStatus::WaitingHuman,
        ] {
            let home = home();
            let project = repository("daemon-terminal-replay");
            let store = StateStore::at(home.path());
            let job_id = format!("finished-{}", status.label().to_lowercase());
            let mut finished = JobState::new(&job_id, "project", project.clone(), "done");
            finished.status = status.clone();
            store.save_job(&finished).expect("the job should persist");

            let recorded =
                crate::orchestrator::events::JsonlEventSink::for_job(home.path(), &job_id);
            for message in ["first recorded", "second recorded"] {
                recorded
                    .emit(&event(&job_id, message))
                    .expect("history should be recorded");
            }

            let broadcaster = JobEventBroadcaster::new();
            let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
            let daemon =
                RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
            let mut client = daemon.client().await;

            let attached = client
                .request(DaemonRequest::Attach {
                    job_id: job_id.clone(),
                    replay: false,
                })
                .await
                .unwrap_or_else(|error| {
                    panic!("a {} job should be observable: {error}", status.label())
                });
            assert!(
                matches!(&attached, DaemonResponse::Attached { live, .. } if !live),
                "a finished job is not a live stream: {attached:?}"
            );

            // The whole exchange has to complete on its own. A terminal attach that waited on a
            // subscription nobody publishes to would hang here rather than fail.
            let messages = tokio::time::timeout(Duration::from_secs(5), async {
                let mut seen = Vec::new();
                while let Ok(Some(message)) = client.next_message().await {
                    let done = matches!(message, DaemonResponse::Detached { .. });
                    seen.push(message);
                    if done {
                        break;
                    }
                }
                seen
            })
            .await
            .expect("a terminal replay must finish rather than wait for a live stream");

            let replayed = messages
                .iter()
                .filter_map(|message| match message {
                    DaemonResponse::Event { event } => match &event.kind {
                        JobEventKind::ControlMessage { message } => Some(message.clone()),
                        _ => None,
                    },
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                replayed,
                vec!["first recorded".to_owned(), "second recorded".to_owned()],
                "the recorded history should be replayed in order for {}",
                status.label()
            );

            let reason = messages
                .iter()
                .find_map(|message| match message {
                    DaemonResponse::Detached { reason, .. } => Some(reason.clone()),
                    _ => None,
                })
                .expect("the stream should end with a reason");
            assert!(
                reason.contains(status.label()) && reason.contains("replayed 2"),
                "the reason should name the authoritative status and the history size: {reason}"
            );

            client.close().await;
            daemon.stop().await;
            let _ = fs::remove_dir_all(home.path());
            let _ = fs::remove_dir_all(project);
        }
    }

    /// A finished job with no readable history is still answered from authoritative state.
    ///
    /// A missing, empty or crash-truncated event log must not make the job look as though it never
    /// existed, and must not hang. The persisted status is the answer in that case.
    #[tokio::test]
    async fn a_terminal_job_without_history_reports_its_persisted_status() {
        let home = home();
        let project = repository("daemon-terminal-no-history");
        let store = StateStore::at(home.path());
        let mut finished = JobState::new("no-history", "project", project.clone(), "done");
        finished.status = JobStatus::Failed;
        store.save_job(&finished).expect("the job should persist");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let attached = client
            .request(DaemonRequest::Attach {
                job_id: "no-history".to_owned(),
                replay: false,
            })
            .await
            .expect("a job with no history is still a job that exists");
        assert!(matches!(&attached, DaemonResponse::Attached { live, .. } if !live));

        let reason = tokio::time::timeout(Duration::from_secs(5), async {
            while let Ok(Some(message)) = client.next_message().await {
                if let DaemonResponse::Detached { reason, .. } = message {
                    return reason;
                }
            }
            String::new()
        })
        .await
        .expect("an empty replay must finish rather than wait");

        assert!(
            reason.contains("FAILED") && reason.contains("no recorded events"),
            "the reason should say what the job is and that nothing was recorded: {reason}"
        );
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// Attach became more permissive; control did not.
    ///
    /// Observing a finished job is harmless. Steering one is not, and a command that cannot be
    /// delivered must still say so rather than reporting a success nothing acted on.
    #[tokio::test]
    async fn control_still_refuses_a_terminal_job_that_attach_now_replays() {
        let home = home();
        let project = repository("daemon-terminal-control");
        let store = StateStore::at(home.path());
        let mut finished = JobState::new("done-job", "project", project.clone(), "done");
        finished.status = JobStatus::Accepted;
        store.save_job(&finished).expect("the job should persist");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let refused = client
            .request(DaemonRequest::Control {
                job_id: "done-job".to_owned(),
                command: ControlRequest::Pause,
            })
            .await
            .expect_err("a finished job cannot be controlled");

        assert!(
            matches!(&refused, ClientError::Daemon(error)
                if error.code == crate::daemon::protocol::DaemonErrorCode::JobTerminal),
            "{refused}"
        );
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    // --------------------------------------------------------------------------------------------
    // Control
    // --------------------------------------------------------------------------------------------

    /// Every control command reaches the named job's own control channel, and only that job's.
    #[tokio::test]
    async fn control_commands_reach_the_named_job_and_no_other() {
        let home = home();
        let first_project = repository("daemon-control-a");
        let second_project = repository("daemon-control-b");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) =
            TestDriver::new(&home, broadcaster.clone(), &["first", "second"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;
        let first = queued_id(&submit(&mut client, vec![job(&first_project, "first")]).await);
        let second = queued_id(&submit(&mut client, vec![job(&second_project, "second")]).await);
        let mut started = [
            handles.started.recv().await.expect("a job should start"),
            handles.started.recv().await.expect("a job should start"),
        ];
        started.sort_by(|left, right| left.task.cmp(&right.task));
        assert_eq!(started[0].task, "first");
        assert_eq!(started[1].task, "second");

        for command in [
            ControlRequest::Pause,
            ControlRequest::Resume,
            ControlRequest::Status,
            ControlRequest::Diff,
            ControlRequest::Send {
                instruction: "also update the changelog".to_owned(),
            },
        ] {
            let response = client
                .request(DaemonRequest::Control {
                    job_id: first.clone(),
                    command: command.clone(),
                })
                .await
                .expect("the command should be delivered");
            match response {
                DaemonResponse::Controlled {
                    job_id,
                    command: label,
                } => {
                    assert_eq!(job_id, first);
                    assert_eq!(label, command.label());
                }
                other => panic!("expected a delivery acknowledgement, got {other:?}"),
            }
        }
        // Delivered in order, to one job, through the one existing control channel.
        client
            .request(DaemonRequest::Control {
                job_id: first.clone(),
                command: ControlRequest::Stop,
            })
            .await
            .expect("stop should be delivered");

        // The stop ends the first job; the second one is untouched and still running.
        let stopped = loop {
            let stopped = handles.stopped.lock().expect("stop lock").clone();
            if !stopped.is_empty() {
                break stopped;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(stopped, vec![first.clone()]);

        let delivered = handles.control.lock().expect("control lock").clone();
        let to_first = delivered
            .iter()
            .filter(|(job_id, _)| job_id == &first)
            .map(|(_, command)| command.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            to_first,
            vec![
                ControlCommand::Pause,
                ControlCommand::Resume,
                ControlCommand::Status,
                ControlCommand::Diff,
                ControlCommand::Send("also update the changelog".to_owned()),
                ControlCommand::Stop,
            ]
        );
        assert!(
            !delivered.iter().any(|(job_id, _)| job_id == &second),
            "a named command must never reach another job: {delivered:?}"
        );

        handles
            .gates
            .remove("second")
            .expect("the gate exists")
            .send(())
            .expect("the second job should still be running");
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(first_project);
        let _ = fs::remove_dir_all(second_project);
    }

    /// Every way a control request can be wrong has its own named refusal, and none of them is a
    /// silent success.
    #[tokio::test]
    async fn a_control_request_fails_closed_with_a_reason() {
        use crate::daemon::protocol::DaemonErrorCode;

        let home = home();
        let project = repository("daemon-control-refusal");
        let store = StateStore::at(home.path());
        let mut queued = JobState::new("queued-job", "project", project.clone(), "later");
        queued.status = JobStatus::Queued;
        store.save_job(&queued).expect("the job should persist");
        let mut published = JobState::new("published-job", "project", project.clone(), "done");
        published.status = JobStatus::Published;
        store.save_job(&published).expect("the job should persist");
        let mut paused = JobState::new("paused-job", "project", project.clone(), "paused");
        paused.status = JobStatus::Paused;
        store.save_job(&paused).expect("the job should persist");

        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        // Queued recovery is off here so the persisted `QUEUED` job stays queued for the test.
        let daemon = RunningDaemon::start(
            &home,
            driver,
            broadcaster,
            DaemonConfig {
                recover_queued: false,
                ..DaemonConfig::default()
            },
        )
        .await;
        let mut client = daemon.client().await;

        for (job_id, command, expected) in [
            (
                "no-such-job",
                ControlRequest::Pause,
                DaemonErrorCode::UnknownJob,
            ),
            (
                "published-job",
                ControlRequest::Pause,
                DaemonErrorCode::JobTerminal,
            ),
            (
                "queued-job",
                ControlRequest::Pause,
                DaemonErrorCode::InvalidForState,
            ),
            (
                // Not terminal, not queued, and not driven by this daemon.
                "paused-job",
                ControlRequest::Resume,
                DaemonErrorCode::JobNotOwned,
            ),
            (
                "paused-job",
                ControlRequest::Send {
                    instruction: "   ".to_owned(),
                },
                DaemonErrorCode::InvalidRequest,
            ),
        ] {
            let error = client
                .request(DaemonRequest::Control {
                    job_id: job_id.to_owned(),
                    command,
                })
                .await
                .expect_err("the request should be refused");

            match error {
                ClientError::Daemon(error) => {
                    assert_eq!(error.code, expected, "{job_id}: {}", error.message);
                    assert!(!error.message.is_empty());
                }
                other => panic!("expected a daemon refusal for {job_id}, got {other}"),
            }
        }
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    // --------------------------------------------------------------------------------------------
    // Shutdown
    // --------------------------------------------------------------------------------------------

    /// A shutdown stops accepting work immediately, and says so with a code a client can act on.
    #[tokio::test]
    async fn a_shutting_down_daemon_accepts_no_new_work() {
        let home = home();
        let project = repository("daemon-shutdown-submit");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["running"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;
        let running = queued_id(&submit(&mut client, vec![job(&project, "running")]).await);
        handles
            .started
            .recv()
            .await
            .expect("the job should be driven");

        // Asked for on one connection, observed on another that was already open.
        let mut asker = daemon.client().await;
        asker
            .request(DaemonRequest::Shutdown)
            .await
            .expect("the shutdown should be accepted");
        asker.close().await;

        let error = client
            .request(DaemonRequest::Submit {
                jobs: vec![job(&project, "too late")],
            })
            .await
            .expect_err("a shutting-down daemon must refuse new work");
        assert!(
            matches!(&error, ClientError::Daemon(error)
                if error.code == crate::daemon::protocol::DaemonErrorCode::ShuttingDown),
            "{error}"
        );
        let status = status(&mut client).await;
        assert!(!status.accepting_work);
        client.close().await;

        // The active job was asked to stop gracefully, not killed.
        let outcome = daemon.task.await.expect("the daemon task should finish");
        let outcome = outcome.expect("the daemon should shut down cleanly");
        assert_eq!(
            handles.stopped.lock().expect("stop lock").as_slice(),
            std::slice::from_ref(&running)
        );
        assert!(
            outcome.report.jobs.iter().any(|job| job.job_id == running
                && matches!(&job.outcome, ScheduledOutcome::Finished { status, .. } if status == "STOPPED")),
            "{:?}",
            outcome.report.jobs
        );
        assert!(daemon.events.kinds().iter().any(|kind| matches!(
            kind,
            DaemonEventKind::DaemonStopping { active_jobs, .. } if *active_jobs == 1
        )));
        assert!(
            daemon
                .events
                .kinds()
                .iter()
                .any(|kind| matches!(kind, DaemonEventKind::DaemonStopped))
        );
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// Queued work that never started stays queued, and the next daemon picks it up with its own
    /// identity and its own limits.
    #[tokio::test]
    async fn queued_work_survives_a_daemon_restart() {
        let home = home();
        let first_project = repository("daemon-restart-a");
        let second_project = repository("daemon-restart-b");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) =
            TestDriver::new(&home, broadcaster.clone(), &["first", "waiting"]);
        // One slot, two repositories: the second job is accepted, persisted and never started.
        let daemon = RunningDaemon::start(
            &home,
            driver,
            broadcaster,
            DaemonConfig {
                max_concurrent: 1,
                ..DaemonConfig::default()
            },
        )
        .await;
        let mut client = daemon.client().await;
        let first = queued_id(&submit(&mut client, vec![job(&first_project, "first")]).await);
        let started = handles
            .started
            .recv()
            .await
            .expect("the first job should be driven");
        assert_eq!(started.job_id, first);
        let waiting = queued_id(
            &submit(
                &mut client,
                vec![SubmitJob {
                    project_path: second_project.display().to_string(),
                    project_name: None,
                    task: "waiting".to_owned(),
                    run: RunOptions {
                        max_iterations: 7,
                        ..RunOptions::default()
                    },
                }],
            )
            .await,
        );
        client.close().await;

        let outcome = daemon.stop().await;

        assert!(
            outcome.report.queued_remaining.contains(&waiting),
            "work that never started stays queued: {:?}",
            outcome.report.queued_remaining
        );
        let store = StateStore::at(home.path());
        let persisted = store
            .load_job(&waiting)
            .expect("state should be readable")
            .expect("the queued job survived");
        assert_eq!(persisted.status, JobStatus::Queued);
        assert_eq!(persisted.run.max_iterations, 7);

        // A new daemon, and the same job.
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut second_handles) =
            TestDriver::new(&home, broadcaster.clone(), &["waiting"]);
        let restarted =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;

        let recovered = second_handles
            .started
            .recv()
            .await
            .expect("the restarted daemon should pick the queued job up");

        assert_eq!(recovered.job_id, waiting, "the job keeps its identity");
        assert_eq!(recovered.mode, ScheduleMode::Start);
        assert!(restarted.events.kinds().iter().any(|kind| matches!(
            kind,
            DaemonEventKind::QueuedWorkRecovered { job_ids } if job_ids.contains(&waiting)
        )));
        second_handles
            .gates
            .remove("waiting")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        restarted.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(first_project);
        let _ = fs::remove_dir_all(second_project);
    }

    // --------------------------------------------------------------------------------------------
    // Restart recovery
    // --------------------------------------------------------------------------------------------

    /// Interrupted work is never replayed on a hunch. By default the daemon finds it, reports it and
    /// leaves it exactly as it is.
    #[tokio::test]
    async fn interrupted_work_is_parked_and_reported_rather_than_replayed() {
        let home = home();
        let project = repository("daemon-parked");
        let interrupted = persist_interrupted_job(&home, &project, "was interrupted");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["new work"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        // Something the daemon *will* start, so "the interrupted job was not started" is observed
        // rather than waited for.
        let fresh = queued_id(&submit(&mut client, vec![job(&project, "new work")]).await);
        let started = handles
            .started
            .recv()
            .await
            .expect("the submitted job should be driven");

        assert_eq!(
            started.job_id, fresh,
            "the first job driven is the submitted one, never the interrupted one"
        );
        assert!(
            handles.started.try_recv().is_err(),
            "an interrupted job must not be replayed without being asked for"
        );
        let status = status(&mut client).await;
        assert!(
            status.resumable.iter().any(|job| job.job_id == interrupted),
            "a parked job must be visible: {status:?}"
        );
        assert!(!status.resume_interrupted);
        assert!(daemon.events.kinds().iter().any(|kind| matches!(
            kind,
            DaemonEventKind::InterruptedWorkParked { job_ids } if job_ids.contains(&interrupted)
        )));
        let untouched = StateStore::at(home.path())
            .load_job(&interrupted)
            .expect("state should be readable")
            .expect("the job is still there");
        assert_eq!(untouched.status, JobStatus::Running);
        assert_eq!(
            untouched.pending_operation,
            Some(PendingOperation::ExecutorRun),
            "the pending operation must survive untouched"
        );

        handles
            .gates
            .remove("new work")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// Asked for explicitly, interrupted work is continued through the ordinary resume path — the
    /// scheduler hands it over as a resume, and its persisted state is never rewritten to start it.
    #[tokio::test]
    async fn interrupted_work_is_resumed_only_when_explicitly_configured() {
        let home = home();
        let project = repository("daemon-resumed");
        let interrupted = persist_interrupted_job(&home, &project, "was interrupted");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) =
            TestDriver::new(&home, broadcaster.clone(), &["was interrupted"]);
        let daemon = RunningDaemon::start(
            &home,
            driver,
            broadcaster,
            DaemonConfig {
                resume_interrupted: true,
                ..DaemonConfig::default()
            },
        )
        .await;

        let started = handles
            .started
            .recv()
            .await
            .expect("the interrupted job should be continued");

        assert_eq!(started.job_id, interrupted);
        assert_eq!(
            started.mode,
            ScheduleMode::Resume,
            "an interrupted job is continued, never restarted"
        );
        let persisted = StateStore::at(home.path())
            .load_job(&interrupted)
            .expect("state should be readable")
            .expect("the job is still there");
        assert_eq!(
            persisted.pending_operation,
            Some(PendingOperation::ExecutorRun),
            "accepting a resume must not rewrite authoritative state"
        );
        assert_eq!(persisted.iteration, 1);
        assert!(daemon.events.kinds().iter().any(|kind| matches!(
            kind,
            DaemonEventKind::InterruptedWorkResumed { job_ids } if job_ids.contains(&interrupted)
        )));

        handles
            .gates
            .remove("was interrupted")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// A daemon starting up must never take over work another Lya process is driving, even when it
    /// was told to resume interrupted jobs.
    #[tokio::test]
    async fn daemon_startup_never_steals_a_job_another_process_is_driving() {
        let home = home();
        let project = repository("daemon-no-steal");
        let other_project = repository("daemon-no-steal-other");
        let interrupted = persist_interrupted_job(&home, &project, "already being driven");
        // Exactly what a foreground `lya run` or `lya resume` holds while it drives this job.
        let foreground = JobLock::acquire(&StateStore::at(home.path()), &interrupted)
            .expect("the foreground claim should be taken");

        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["unrelated"]);
        let daemon = RunningDaemon::start(
            &home,
            driver,
            broadcaster,
            DaemonConfig {
                resume_interrupted: true,
                ..DaemonConfig::default()
            },
        )
        .await;
        let mut client = daemon.client().await;

        let unrelated =
            queued_id(&submit(&mut client, vec![job(&other_project, "unrelated")]).await);
        let started = handles
            .started
            .recv()
            .await
            .expect("unrelated work should still run");

        assert_eq!(
            started.job_id, unrelated,
            "the claimed job must not have been taken over"
        );
        assert!(
            handles.started.try_recv().is_err(),
            "a job another process is driving must never be resumed here"
        );
        assert!(daemon.events.kinds().iter().any(|kind| matches!(
            kind,
            DaemonEventKind::InterruptedWorkParked { job_ids } if job_ids.contains(&interrupted)
        )));

        handles
            .gates
            .remove("unrelated")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;
        drop(foreground);
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
        let _ = fs::remove_dir_all(other_project);
    }

    /// The other direction: while the daemon drives a job, a foreground process can claim neither
    /// its repository nor the job itself.
    ///
    /// Both claims are refused here inside one process, which is the stricter case: an advisory lock
    /// belongs to the open file description, so a second claim is refused whether it comes from this
    /// process or another one. The cross-process half of the guarantee is proven directly in
    /// [`crate::orchestrator::repository_lock`].
    #[tokio::test]
    async fn a_foreground_process_cannot_claim_a_repository_or_job_the_daemon_drives() {
        let home = home();
        let project = repository("daemon-exclusion");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["mine"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;
        let job_id = queued_id(&submit(&mut client, vec![job(&project, "mine")]).await);
        handles
            .started
            .recv()
            .await
            .expect("the job should be driven");

        let store = StateStore::at(home.path());
        let identity = RepositoryIdentity::resolve(&project).expect("identity should resolve");
        let repository_refusal = RepositoryLock::acquire(&store, &identity)
            .expect_err("the daemon holds this repository");
        let job_refusal = JobLock::acquire(&store, &job_id).expect_err("the daemon holds this job");

        assert!(
            repository_refusal
                .to_string()
                .contains("already being driven"),
            "{repository_refusal}"
        );
        assert!(
            job_refusal.to_string().contains("already being driven"),
            "{job_refusal}"
        );

        handles
            .gates
            .remove("mine")
            .expect("the gate exists")
            .send(())
            .expect("the job should still be running");
        client.close().await;
        daemon.stop().await;

        // Released with the job, so the next process can have them.
        assert!(RepositoryLock::acquire(&store, &identity).is_ok());
        assert!(JobLock::acquire(&store, &job_id).is_ok());
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(project);
    }

    /// A shutdown must shut work down, never consume it.
    ///
    /// One concurrency slot, one job running and one job queued behind it. The shutdown is
    /// requested while the first job is still gated, so the queued job has never been touched. It
    /// must not be handed to the driver, and it must still be `QUEUED` on disk afterwards: a job
    /// started only to be stopped ends `STOPPED`, which is terminal, which means no later daemon
    /// can recover it and the work is simply gone.
    #[tokio::test]
    async fn a_shutdown_leaves_untouched_queued_work_queued() {
        let home = home();
        let running_project = repository("daemon-shutdown-running");
        let waiting_project = repository("daemon-shutdown-waiting");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, mut handles) = TestDriver::new(&home, broadcaster.clone(), &["running"]);
        let daemon = RunningDaemon::start(
            &home,
            driver,
            broadcaster,
            DaemonConfig {
                max_concurrent: 1,
                ..DaemonConfig::default()
            },
        )
        .await;
        let mut client = daemon.client().await;

        let running = submit(&mut client, vec![job(&running_project, "running")]).await;
        let started = handles
            .started
            .recv()
            .await
            .expect("the first job should start");
        assert_eq!(started.task, "running");
        let waiting = submit(&mut client, vec![job(&waiting_project, "waiting")]).await;
        let waiting_id = queued_id(&waiting);
        let _ = queued_id(&running);
        client.close().await;

        // Requested while the only slot is held by the gated job, so nothing is mid-selection.
        let mut stopper = daemon.client().await;
        let response = stopper
            .request(DaemonRequest::Shutdown)
            .await
            .expect("a shutdown should be accepted");
        assert!(matches!(response, DaemonResponse::ShuttingDown { .. }));
        stopper.close().await;
        // The running job receives the same graceful stop a foreground Ctrl+C sends and parks
        // itself; the gate is never needed and is dropped with the handles.
        let outcome = daemon
            .task
            .await
            .expect("the daemon task should finish")
            .expect("the daemon should shut down cleanly");

        assert!(
            handles.started.try_recv().is_err(),
            "a shutdown must never start queued work"
        );
        assert!(
            outcome.report.queued_remaining.contains(&waiting_id),
            "work that never started stays queued: {:?}",
            outcome.report.queued_remaining
        );
        let waiting = StateStore::at(home.path())
            .load_job(&waiting_id)
            .expect("the job should be readable")
            .expect("a returned job id names a persisted job");
        assert_eq!(
            waiting.status,
            JobStatus::Queued,
            "queued work a shutdown never touched must stay recoverable"
        );
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(running_project);
        let _ = fs::remove_dir_all(waiting_project);
    }

    /// Probing liveness must not change anything.
    ///
    /// `is_held` used to answer by taking the claim and dropping it, and dropping a claim removes
    /// the metadata its owner is responsible for — so asking whether a daemon existed deleted that
    /// daemon's diagnostics. A probe that a stop command polls in a loop must be inert.
    #[tokio::test]
    async fn probing_the_claim_never_disturbs_the_daemon_that_holds_it() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;

        let before = DaemonMetadata::read(&home).expect("a running daemon records metadata");
        for _ in 0..5 {
            assert!(
                DaemonLock::is_held(&home).expect("the claim should be readable"),
                "a running daemon holds its claim"
            );
        }
        let after = DaemonMetadata::read(&home).expect("probing must not remove the metadata");

        assert_eq!(before, after);
        let mut client = daemon.client().await;
        assert_eq!(
            status(&mut client).await.identity.process_id,
            before.process_id,
            "the daemon is still serving after being probed"
        );
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }

    /// A batch keeps the identities it already committed to.
    ///
    /// The middle job names a path that is not a repository, which fails inside the per-job accept
    /// path exactly as an unwritable state store does. The daemon must still answer for all three,
    /// because a caller that is told only "the submission failed" cannot tell which jobs are now
    /// queued and running, and a retry would duplicate them.
    #[tokio::test]
    async fn a_job_that_cannot_be_accepted_never_hides_the_jobs_accepted_beside_it() {
        let home = home();
        let first_project = repository("daemon-partial-first");
        let last_project = repository("daemon-partial-last");
        let missing = unique("daemon-partial-missing");
        fs::remove_dir_all(&missing).expect("the missing path should not exist");
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &["first", "last"]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;
        let mut client = daemon.client().await;

        let outcomes = submit(
            &mut client,
            vec![
                job(&first_project, "first"),
                job(&missing, "unusable"),
                job(&last_project, "last"),
            ],
        )
        .await;

        assert_eq!(outcomes.len(), 3, "every job is answered: {outcomes:?}");
        let store = StateStore::at(home.path());
        for (index, outcome) in outcomes.iter().enumerate() {
            match (index, outcome) {
                (1, SubmitOutcome::Rejected { reason, .. }) => {
                    assert!(!reason.is_empty(), "a refusal says why");
                }
                (_, SubmitOutcome::Queued { job_id, .. }) => {
                    let job = store
                        .load_job(job_id)
                        .expect("the job should be readable")
                        .expect("a returned job id names durably accepted work");
                    assert!(
                        matches!(job.status, JobStatus::Queued | JobStatus::Running),
                        "a job id is only returned once the job exists: {:?}",
                        job.status
                    );
                }
                (index, outcome) => panic!("unexpected outcome at {index}: {outcome:?}"),
            }
        }
        client.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
        let _ = fs::remove_dir_all(first_project);
        let _ = fs::remove_dir_all(last_project);
    }

    /// A connection that arrives and says nothing must cost the daemon one connection, briefly.
    ///
    /// On Windows the pipe can be opened by anyone the access control list admits, and an opened
    /// pipe that never speaks holds a task and an instance for as long as it lasts. The handshake
    /// timeout bounds that, and bounds nothing else: the silent client is disconnected on its own
    /// while another client keeps being served normally.
    #[tokio::test]
    async fn a_client_that_never_sends_a_request_is_disconnected_alone() {
        let home = home();
        let broadcaster = JobEventBroadcaster::new();
        let (driver, _handles) = TestDriver::new(&home, broadcaster.clone(), &[]);
        let daemon =
            RunningDaemon::start(&home, driver, broadcaster, DaemonConfig::default()).await;

        // Connected, and deliberately mute.
        let mut silent = connect(&DaemonEndpoint::for_home(&home))
            .await
            .expect("a client should connect");
        let mut talking = daemon.client().await;
        assert_eq!(
            status(&mut talking).await.identity.protocol_version,
            PROTOCOL_VERSION
        );

        // Paused only now, so starting the daemon above ran on real time; the timeout is then
        // crossed without a test ever waiting for it.
        tokio::time::pause();
        tokio::time::advance(super::HANDSHAKE_TIMEOUT + Duration::from_secs(1)).await;
        tokio::time::resume();

        assert!(
            matches!(silent.read_frame().await, Ok(None) | Err(_)),
            "a client that never sent a request is disconnected"
        );
        // The daemon and every other client are untouched by that.
        assert_eq!(
            status(&mut talking).await.identity.protocol_version,
            PROTOCOL_VERSION,
            "another client keeps being served"
        );
        let rejected = daemon.events.kinds().into_iter().any(|kind| {
            matches!(kind, DaemonEventKind::ClientRejected { reason, .. } if reason.contains("no request within"))
        });
        assert!(rejected, "the disconnection is observed as a refusal");
        talking.close().await;
        daemon.stop().await;
        let _ = fs::remove_dir_all(home.path());
    }
}
