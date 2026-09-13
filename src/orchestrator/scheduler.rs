//! Bounded multi-project scheduling.
//!
//! The scheduler is a boundary, not an orchestrator. [`AutonomousOrchestrator`] keeps owning
//! exactly one autonomous job and its sequential chain; the scheduler only decides *which* work may
//! start, *when*, and *how much at once*:
//!
//! ```text
//! CLI / future daemon
//!         |
//!         v
//!     Scheduler ── repository claims, concurrency slots, queue bookkeeping
//!    /    |    \
//!   v     v     v
//! Job A  Job B  Job C          (one [`JobDriver`] call each)
//!   |      |      |
//!   +------|------+
//!          |
//!   AutonomousOrchestrator      (unchanged: one job/chain per call)
//! ```
//!
//! Supervisors and executors know nothing about scheduling, and nothing here can reach around Git
//! verification: the scheduler starts jobs, it never publishes.
//!
//! Two rules define it:
//!
//! * different repositories may run concurrently;
//! * the same repository is never driven concurrently — not by two jobs in one scheduler, and not
//!   by a second Lya process (see [`RepositoryLock`]).
//!
//! [`AutonomousOrchestrator`]: super::job::AutonomousOrchestrator

use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    fmt,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{
    control::{ControlReceiver, ControlSender},
    events::{EventSinkError, format_timestamp, style},
    job::new_job_id,
    lock::JobLock,
    repository_lock::{RepositoryIdentity, RepositoryLock},
    state::{JobState, JobStatus, RunConfiguration, StateError, StateStore},
    supervisor::Project,
};

/// How many repositories may be driven at once by default.
pub const DEFAULT_MAX_CONCURRENT: usize = 2;

/// One unit of work handed to the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledRequest {
    pub project: Project,
    pub task: String,
    /// `None` for new work, which the scheduler gives a fresh job ID. `Some` when re-queueing a job
    /// the scheduler already persisted, so an interrupted queue keeps its identities.
    pub job_id: Option<String>,
}

impl ScheduledRequest {
    pub fn new(project: Project, task: impl Into<String>) -> Self {
        Self {
            project,
            task: task.into(),
            job_id: None,
        }
    }

    /// Re-queue work a previous scheduler persisted as `QUEUED` and never started.
    pub fn for_persisted_job(job: &JobState) -> Self {
        Self {
            project: Project {
                name: job.project_name.clone(),
                path: job.project_path.clone(),
            },
            task: job.task.clone(),
            job_id: Some(job.job_id.clone()),
        }
    }
}

/// Everything a driver needs to run one scheduled job.
///
/// The repository claim and the job lock are already held by the scheduler when this is handed
/// over, and stay held until the driver's future resolves — including across every sequential child
/// the chain starts.
pub struct JobAssignment {
    pub job_id: String,
    pub project: Project,
    pub task: String,
    /// The canonical repository this job is claiming.
    pub repository: PathBuf,
    /// This job's own control channel. The scheduler routes a graceful stop into it.
    pub control: ControlReceiver,
}

/// How one scheduled job ended, as the driver saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    Finished {
        /// Terminal status label of the last job in the chain.
        status: String,
        /// How many jobs the sequential chain contained.
        jobs: usize,
        /// Whether that status counts as a successful outcome. The driver decides; the scheduler
        /// does not reinterpret job semantics.
        succeeded: bool,
    },
    Failed {
        error: String,
    },
}

/// Drives exactly one scheduled job to completion.
///
/// The production implementation builds an [`AutonomousOrchestrator`](super::job::AutonomousOrchestrator)
/// and calls `run_sequential`. Tests substitute a driver that can be blocked and released
/// deterministically.
pub trait JobDriver: Send + Sync + 'static {
    fn drive<'a>(
        &'a self,
        assignment: JobAssignment,
    ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>>;
}

/// Graceful-stop handle for scheduler-owned work.
///
/// The first interrupt requests a stop: no new job is started, and every active job receives the
/// same graceful stop a single `lya run` would receive, including provider process-tree
/// cancellation. The second interrupt keeps its existing force-exit meaning and is handled by the
/// caller, not here.
#[derive(Clone, Default)]
pub struct SchedulerControl {
    inner: Arc<SchedulerControlInner>,
}

#[derive(Default)]
struct SchedulerControlInner {
    stop_requested: AtomicBool,
    stop_announced: AtomicBool,
    active: Mutex<Vec<(String, ControlSender)>>,
}

impl SchedulerControl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop launching new work and ask every active job to stop at its next safe boundary.
    pub fn request_stop(&self) {
        self.inner.stop_requested.store(true, Ordering::Release);
        let active = self.inner.active.lock().expect("scheduler control lock");
        for (_, sender) in active.iter() {
            sender.request_stop();
        }
    }

    pub fn stop_requested(&self) -> bool {
        self.inner.stop_requested.load(Ordering::Acquire)
    }

    fn register(&self, job_id: &str, sender: ControlSender) {
        let mut active = self.inner.active.lock().expect("scheduler control lock");
        active.push((job_id.to_owned(), sender.clone()));
        drop(active);
        // A stop that arrived while this job was starting still reaches it.
        if self.stop_requested() {
            sender.request_stop();
        }
    }

    fn deregister(&self, job_id: &str) {
        let mut active = self.inner.active.lock().expect("scheduler control lock");
        active.retain(|(registered, _)| registered != job_id);
    }

    /// True exactly once, for the first caller that observes a stop.
    fn claim_stop_announcement(&self) -> bool {
        self.stop_requested()
            && self
                .inner
                .stop_announced
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
}

/// Scheduler-wide observation.
///
/// Job-level facts stay in each job's own `events.jsonl`; this covers only what belongs to the
/// scheduler itself. Like job events it is observation, never authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerEvent {
    pub timestamp_unix_millis: u128,
    #[serde(flatten)]
    pub kind: SchedulerEventKind,
}

impl SchedulerEvent {
    pub fn new(kind: SchedulerEventKind) -> Self {
        Self {
            timestamp_unix_millis: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scheduler_event", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchedulerEventKind {
    SchedulerStarted {
        max_concurrent: usize,
        queued: usize,
    },
    JobQueued {
        job_id: String,
        project_name: String,
        repository: PathBuf,
    },
    WaitingForRepository {
        job_id: String,
        project_name: String,
        repository: PathBuf,
    },
    JobStarted {
        job_id: String,
        project_name: String,
        repository: PathBuf,
    },
    JobCompleted {
        job_id: String,
        project_name: String,
        status: String,
        jobs: usize,
    },
    JobFailed {
        job_id: String,
        project_name: String,
        error: String,
    },
    JobRejected {
        job_id: String,
        project_name: String,
        reason: String,
    },
    SchedulerStopping {
        reason: String,
    },
    SchedulerFinished {
        completed: usize,
        failed: usize,
        rejected: usize,
        queued_remaining: usize,
    },
}

pub trait SchedulerEventSink: Send + Sync {
    fn emit(&self, event: &SchedulerEvent) -> Result<(), EventSinkError>;
}

#[derive(Debug, Default)]
pub struct NoopSchedulerSink;

impl SchedulerEventSink for NoopSchedulerSink {
    fn emit(&self, _event: &SchedulerEvent) -> Result<(), EventSinkError> {
        Ok(())
    }
}

/// One JSON object per line, so `--json` stdout stays machine-readable while jobs run
/// concurrently. Scheduler events carry a `scheduler_event` tag and job events an `event` tag, so
/// the two never have to be told apart by guessing.
pub struct JsonSchedulerSink<W: std::io::Write + Send> {
    writer: Mutex<W>,
}

impl<W: std::io::Write + Send> JsonSchedulerSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl<W: std::io::Write + Send> SchedulerEventSink for JsonSchedulerSink<W> {
    fn emit(&self, event: &SchedulerEvent) -> Result<(), EventSinkError> {
        let mut line = serde_json::to_vec(event)
            .map_err(|error| EventSinkError::Serialize(error.to_string()))?;
        line.push(b'\n');
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| EventSinkError::Write("JSON output lock was poisoned".to_owned()))?;
        writer
            .write_all(&line)
            .and_then(|_| writer.flush())
            .map_err(|error| EventSinkError::Write(error.to_string()))
    }
}

pub struct HumanSchedulerSink<W: std::io::Write + Send> {
    writer: Mutex<W>,
    color: bool,
}

impl<W: std::io::Write + Send> HumanSchedulerSink<W> {
    pub fn new(writer: W, color: bool) -> Self {
        Self {
            writer: Mutex::new(writer),
            color,
        }
    }
}

impl HumanSchedulerSink<std::io::Stdout> {
    pub fn stdout() -> Self {
        use std::io::IsTerminal;
        Self::new(std::io::stdout(), std::io::stdout().is_terminal())
    }
}

impl<W: std::io::Write + Send> SchedulerEventSink for HumanSchedulerSink<W> {
    fn emit(&self, event: &SchedulerEvent) -> Result<(), EventSinkError> {
        let rendered = render_scheduler_event(event, self.color);
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| EventSinkError::Write("terminal output lock was poisoned".to_owned()))?;
        writer
            .write_all(rendered.as_bytes())
            .and_then(|_| writer.flush())
            .map_err(|error| EventSinkError::Write(error.to_string()))
    }
}

pub fn render_scheduler_event(event: &SchedulerEvent, color: bool) -> String {
    let timestamp = format_timestamp(event.timestamp_unix_millis);
    let heading = style("SCHEDULER", "36", color);
    let line = |summary: &str| format!("{timestamp}  {heading}  {summary}\n");
    match &event.kind {
        SchedulerEventKind::SchedulerStarted {
            max_concurrent,
            queued,
        } => line(&format!(
            "{queued} job(s) queued; at most {max_concurrent} repositories at a time"
        )),
        SchedulerEventKind::JobQueued {
            job_id,
            project_name,
            repository,
        } => line(&format!(
            "queued {project_name} {job_id} ({})",
            repository.display()
        )),
        SchedulerEventKind::WaitingForRepository {
            job_id,
            project_name,
            repository,
        } => line(&format!(
            "{project_name} {job_id} is waiting for {}",
            repository.display()
        )),
        SchedulerEventKind::JobStarted {
            job_id,
            project_name,
            ..
        } => line(&format!(
            "{} {project_name} {job_id}",
            style("started", "32", color)
        )),
        SchedulerEventKind::JobCompleted {
            job_id,
            project_name,
            status,
            jobs,
        } => line(&format!(
            "{} {project_name} {job_id} {status} ({jobs} job(s) in the chain)",
            style("finished", "32", color)
        )),
        SchedulerEventKind::JobFailed {
            job_id,
            project_name,
            error,
        } => line(&format!(
            "{} {project_name} {job_id}: {error}",
            style("failed", "31", color)
        )),
        SchedulerEventKind::JobRejected {
            job_id,
            project_name,
            reason,
        } => line(&format!(
            "{} {project_name} {job_id}: {reason}",
            style("not started", "31", color)
        )),
        SchedulerEventKind::SchedulerStopping { reason } => line(&format!(
            "{}; no new job will be started ({reason})",
            style("stopping", "33", color)
        )),
        SchedulerEventKind::SchedulerFinished {
            completed,
            failed,
            rejected,
            queued_remaining,
        } => line(&format!(
            "{completed} finished, {failed} failed, {rejected} not started, {queued_remaining} still queued"
        )),
    }
}

/// What happened to one scheduled request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledOutcome {
    Finished {
        status: String,
        jobs: usize,
        succeeded: bool,
    },
    Failed {
        error: String,
    },
    /// Never started: the repository or the job could not be claimed, or its path is unusable. The
    /// persisted `QUEUED` state is left untouched so the work can be picked up later.
    Rejected {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledJobReport {
    /// Position in the submitted order, so a report reads the same on every run.
    pub position: usize,
    pub job_id: String,
    pub project_name: String,
    pub outcome: ScheduledOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerReport {
    /// In submitted order, not completion order.
    pub jobs: Vec<ScheduledJobReport>,
    /// Job IDs still persisted as `QUEUED` because they were never started.
    pub queued_remaining: Vec<String>,
    pub stopped: bool,
}

impl SchedulerReport {
    pub fn is_success(&self) -> bool {
        self.jobs.iter().all(|job| match &job.outcome {
            ScheduledOutcome::Finished { succeeded, .. } => *succeeded,
            _ => false,
        })
    }

    pub fn render(&self) -> String {
        let mut lines = Vec::new();
        for job in &self.jobs {
            let summary = match &job.outcome {
                ScheduledOutcome::Finished { status, jobs, .. } => {
                    format!("{status} ({jobs} job(s))")
                }
                ScheduledOutcome::Failed { error } => format!("FAILED: {error}"),
                ScheduledOutcome::Rejected { reason } => format!("NOT STARTED: {reason}"),
            };
            lines.push(format!("{}  {}  {summary}", job.project_name, job.job_id));
        }
        if !self.queued_remaining.is_empty() {
            lines.push(format!(
                "{} job(s) stay QUEUED and can be picked up with: lya scheduler --resume-queued",
                self.queued_remaining.len()
            ));
        }
        if lines.is_empty() {
            lines.push("No work was scheduled.".to_owned());
        }
        lines.join("\n")
    }
}

/// Every job a previous scheduler accepted and never started, oldest first.
pub fn queued_jobs(store: &StateStore) -> Result<Vec<JobState>, StateError> {
    let mut jobs = store
        .load_all()?
        .into_iter()
        .filter(|job| job.status == JobStatus::Queued)
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| {
        left.created_unix_seconds
            .cmp(&right.created_unix_seconds)
            .then_with(|| left.job_id.cmp(&right.job_id))
    });
    Ok(jobs)
}

#[derive(Debug)]
struct QueueEntry {
    position: usize,
    job_id: String,
    project: Project,
    task: String,
    repository: RepositoryIdentity,
    /// Whether a "waiting for repository" observation was already emitted for this entry, so a
    /// blocked entry is announced once instead of on every scan.
    wait_announced: bool,
}

#[derive(Debug, Default)]
struct QueueState {
    pending: VecDeque<QueueEntry>,
    active: HashSet<String>,
}

impl QueueState {
    /// The first queued entry whose repository is free, plus the entries this scan newly found
    /// blocked.
    ///
    /// Scanning past a blocked entry instead of stopping at it is what keeps a repository conflict
    /// from wasting an execution slot; taking the *first* runnable entry is what keeps the order
    /// FIFO and the outcome deterministic.
    fn take_runnable(&mut self) -> (Option<QueueEntry>, Vec<(String, String, PathBuf)>) {
        let mut announcements = Vec::new();
        let mut chosen = None;
        for (index, entry) in self.pending.iter_mut().enumerate() {
            if !self.active.contains(entry.repository.key()) {
                chosen = Some(index);
                break;
            }
            if !entry.wait_announced {
                entry.wait_announced = true;
                announcements.push((
                    entry.job_id.clone(),
                    entry.project.name.clone(),
                    entry.repository.root().to_owned(),
                ));
            }
        }
        let Some(index) = chosen else {
            return (None, announcements);
        };
        let entry = self
            .pending
            .remove(index)
            .expect("the scanned index exists in the queue");
        self.active.insert(entry.repository.key().to_owned());
        (Some(entry), announcements)
    }

    fn release(&mut self, key: &str) {
        self.active.remove(key);
    }
}

struct SchedulerShared {
    queue: Mutex<QueueState>,
    results: Mutex<Vec<ScheduledJobReport>>,
    progress: watch::Sender<u64>,
    event_failure: Mutex<Option<String>>,
}

impl SchedulerShared {
    fn emit(&self, sink: &dyn SchedulerEventSink, kind: SchedulerEventKind) {
        if let Err(error) = sink.emit(&SchedulerEvent::new(kind)) {
            let mut failure = self.event_failure.lock().expect("event failure lock");
            if failure.is_none() {
                *failure = Some(error.to_string());
            }
        }
    }

    fn announce_progress(&self) {
        self.progress.send_modify(|version| *version += 1);
    }
}

pub struct Scheduler<D: JobDriver> {
    driver: Arc<D>,
    store: Arc<StateStore>,
    events: Arc<dyn SchedulerEventSink>,
    control: SchedulerControl,
    max_concurrent: usize,
    run: RunConfiguration,
}

impl<D: JobDriver> Scheduler<D> {
    pub fn new(driver: D, store: StateStore) -> Self {
        Self {
            driver: Arc::new(driver),
            store: Arc::new(store),
            events: Arc::new(NoopSchedulerSink),
            control: SchedulerControl::new(),
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            run: RunConfiguration::default(),
        }
    }

    pub fn with_max_concurrent(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent = max_concurrent;
        self
    }

    pub fn with_event_sink(mut self, events: Arc<dyn SchedulerEventSink>) -> Self {
        self.events = events;
        self
    }

    pub fn with_control(mut self, control: SchedulerControl) -> Self {
        self.control = control;
        self
    }

    /// The run configuration recorded on every queued job, so an interrupted queue keeps the limits
    /// it was accepted with.
    pub fn with_run_configuration(mut self, run: RunConfiguration) -> Self {
        self.run = run;
        self
    }

    pub fn control(&self) -> SchedulerControl {
        self.control.clone()
    }

    pub async fn run(
        self,
        requests: Vec<ScheduledRequest>,
    ) -> Result<SchedulerReport, SchedulerError> {
        if self.max_concurrent == 0 {
            return Err(SchedulerError::InvalidConcurrency);
        }

        let (progress, progress_receiver) = watch::channel(0u64);
        let shared = Arc::new(SchedulerShared {
            queue: Mutex::new(QueueState::default()),
            results: Mutex::new(Vec::new()),
            progress,
            event_failure: Mutex::new(None),
        });

        let accepted = self.accept(&shared, requests)?;
        shared.emit(
            self.events.as_ref(),
            SchedulerEventKind::SchedulerStarted {
                max_concurrent: self.max_concurrent,
                queued: accepted,
            },
        );
        if self.control.claim_stop_announcement() {
            shared.emit(
                self.events.as_ref(),
                SchedulerEventKind::SchedulerStopping {
                    reason: "a stop was requested before any job started".to_owned(),
                },
            );
        }

        let workers = self.max_concurrent.min(accepted.max(1));
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let shared = Arc::clone(&shared);
            let store = Arc::clone(&self.store);
            let driver = Arc::clone(&self.driver);
            let events = Arc::clone(&self.events);
            let control = self.control.clone();
            let progress = progress_receiver.clone();
            handles.push(tokio::spawn(async move {
                worker(shared, store, driver, events, control, progress).await;
            }));
        }
        for handle in handles {
            handle
                .await
                .map_err(|error| SchedulerError::Worker(error.to_string()))?;
        }

        let mut jobs = shared
            .results
            .lock()
            .expect("scheduler results lock")
            .clone();
        jobs.sort_by_key(|job| job.position);
        let queued_remaining = shared
            .queue
            .lock()
            .expect("scheduler queue lock")
            .pending
            .iter()
            .map(|entry| entry.job_id.clone())
            .collect::<Vec<_>>();
        let report = SchedulerReport {
            jobs,
            queued_remaining,
            stopped: self.control.stop_requested(),
        };

        let completed = report
            .jobs
            .iter()
            .filter(|job| matches!(job.outcome, ScheduledOutcome::Finished { .. }))
            .count();
        let failed = report
            .jobs
            .iter()
            .filter(|job| matches!(job.outcome, ScheduledOutcome::Failed { .. }))
            .count();
        let rejected = report
            .jobs
            .iter()
            .filter(|job| matches!(job.outcome, ScheduledOutcome::Rejected { .. }))
            .count();
        shared.emit(
            self.events.as_ref(),
            SchedulerEventKind::SchedulerFinished {
                completed,
                failed,
                rejected,
                queued_remaining: report.queued_remaining.len(),
            },
        );

        if let Some(error) = shared
            .event_failure
            .lock()
            .expect("event failure lock")
            .take()
        {
            return Err(SchedulerError::Event(error));
        }
        Ok(report)
    }

    /// Give every request an identity, a repository claim target and a durable `QUEUED` record
    /// before anything starts.
    ///
    /// Persisting first is what makes a scheduler crash survivable: accepted work exists on disk as
    /// a real job, distinguishable from work that was already being driven.
    fn accept(
        &self,
        shared: &Arc<SchedulerShared>,
        requests: Vec<ScheduledRequest>,
    ) -> Result<usize, SchedulerError> {
        let mut accepted = 0;
        for (position, request) in requests.into_iter().enumerate() {
            let job_id = request.job_id.clone().unwrap_or_else(new_job_id);
            let repository = match RepositoryIdentity::resolve(&request.project.path) {
                Ok(identity) => identity,
                Err(error) => {
                    shared.emit(
                        self.events.as_ref(),
                        SchedulerEventKind::JobRejected {
                            job_id: job_id.clone(),
                            project_name: request.project.name.clone(),
                            reason: error.to_string(),
                        },
                    );
                    shared.results.lock().expect("scheduler results lock").push(
                        ScheduledJobReport {
                            position,
                            job_id,
                            project_name: request.project.name,
                            outcome: ScheduledOutcome::Rejected {
                                reason: error.to_string(),
                            },
                        },
                    );
                    continue;
                }
            };
            let mut job = JobState::new(
                job_id.clone(),
                request.project.name.clone(),
                request.project.path.clone(),
                request.task.clone(),
            );
            job.status = JobStatus::Queued;
            job.run = self.run.clone();
            self.store.save_job(&job).map_err(SchedulerError::State)?;
            shared.emit(
                self.events.as_ref(),
                SchedulerEventKind::JobQueued {
                    job_id: job_id.clone(),
                    project_name: request.project.name.clone(),
                    repository: repository.root().to_owned(),
                },
            );
            shared
                .queue
                .lock()
                .expect("scheduler queue lock")
                .pending
                .push_back(QueueEntry {
                    position,
                    job_id,
                    project: request.project,
                    task: request.task,
                    repository,
                    wait_announced: false,
                });
            accepted += 1;
        }
        Ok(accepted)
    }
}

/// One execution slot.
///
/// Exactly `max_concurrent` of these exist; no task is spawned per job, so concurrency is bounded
/// by construction rather than by gating provider calls after the fact.
async fn worker<D: JobDriver>(
    shared: Arc<SchedulerShared>,
    store: Arc<StateStore>,
    driver: Arc<D>,
    events: Arc<dyn SchedulerEventSink>,
    control: SchedulerControl,
    mut progress: watch::Receiver<u64>,
) {
    loop {
        if control.stop_requested() {
            if control.claim_stop_announcement() {
                shared.emit(
                    events.as_ref(),
                    SchedulerEventKind::SchedulerStopping {
                        reason: "a graceful stop was requested".to_owned(),
                    },
                );
            }
            return;
        }
        // Marking the current version seen *before* inspecting the queue is what makes the
        // check-then-wait below race-free: a release that happens after this point always makes
        // `changed()` return immediately.
        progress.mark_unchanged();
        let (entry, waiting) = {
            let mut queue = shared.queue.lock().expect("scheduler queue lock");
            queue.take_runnable()
        };
        for (job_id, project_name, repository) in waiting {
            shared.emit(
                events.as_ref(),
                SchedulerEventKind::WaitingForRepository {
                    job_id,
                    project_name,
                    repository,
                },
            );
        }
        let Some(entry) = entry else {
            let pending_empty = shared
                .queue
                .lock()
                .expect("scheduler queue lock")
                .pending
                .is_empty();
            if pending_empty {
                return;
            }
            // Only another worker releasing a repository can unblock the queue.
            if progress.changed().await.is_err() {
                return;
            }
            continue;
        };

        let key = entry.repository.key().to_owned();
        let report = execute(
            &shared,
            &store,
            driver.as_ref(),
            events.as_ref(),
            &control,
            entry,
        )
        .await;
        shared
            .queue
            .lock()
            .expect("scheduler queue lock")
            .release(&key);
        shared
            .results
            .lock()
            .expect("scheduler results lock")
            .push(report);
        shared.announce_progress();
    }
}

/// Claim, drive and release exactly one scheduled job.
///
/// Claim order is repository first, job second, everywhere in Lya, so the two layers can never
/// deadlock against each other.
async fn execute<D: JobDriver + ?Sized>(
    shared: &Arc<SchedulerShared>,
    store: &StateStore,
    driver: &D,
    events: &dyn SchedulerEventSink,
    control: &SchedulerControl,
    entry: QueueEntry,
) -> ScheduledJobReport {
    let rejected = |reason: String| ScheduledJobReport {
        position: entry.position,
        job_id: entry.job_id.clone(),
        project_name: entry.project.name.clone(),
        outcome: ScheduledOutcome::Rejected { reason },
    };

    // In-process bookkeeping already proved no other worker holds this repository; this proves no
    // other Lya process holds it either, and keeps holding it for the whole sequential chain.
    let repository_lock = match RepositoryLock::acquire(store, &entry.repository) {
        Ok(lock) => lock,
        Err(error) => {
            shared.emit(
                events,
                SchedulerEventKind::JobRejected {
                    job_id: entry.job_id.clone(),
                    project_name: entry.project.name.clone(),
                    reason: error.to_string(),
                },
            );
            return rejected(error.to_string());
        }
    };
    let job_lock = match JobLock::acquire(store, &entry.job_id) {
        Ok(lock) => lock,
        Err(error) => {
            shared.emit(
                events,
                SchedulerEventKind::JobRejected {
                    job_id: entry.job_id.clone(),
                    project_name: entry.project.name.clone(),
                    reason: error.to_string(),
                },
            );
            return rejected(error.to_string());
        }
    };

    let (sender, receiver) = ControlReceiver::new();
    control.register(&entry.job_id, sender);
    shared.emit(
        events,
        SchedulerEventKind::JobStarted {
            job_id: entry.job_id.clone(),
            project_name: entry.project.name.clone(),
            repository: entry.repository.root().to_owned(),
        },
    );

    let outcome = driver
        .drive(JobAssignment {
            job_id: entry.job_id.clone(),
            project: entry.project.clone(),
            task: entry.task.clone(),
            repository: entry.repository.root().to_owned(),
            control: receiver,
        })
        .await;

    control.deregister(&entry.job_id);
    drop(job_lock);
    drop(repository_lock);

    let outcome = match outcome {
        JobOutcome::Finished {
            status,
            jobs,
            succeeded,
        } => {
            shared.emit(
                events,
                SchedulerEventKind::JobCompleted {
                    job_id: entry.job_id.clone(),
                    project_name: entry.project.name.clone(),
                    status: status.clone(),
                    jobs,
                },
            );
            ScheduledOutcome::Finished {
                status,
                jobs,
                succeeded,
            }
        }
        // One job's failure is that job's failure. Unrelated repositories keep running.
        JobOutcome::Failed { error } => {
            settle_failed_queued_state(store, &entry.job_id);
            shared.emit(
                events,
                SchedulerEventKind::JobFailed {
                    job_id: entry.job_id.clone(),
                    project_name: entry.project.name.clone(),
                    error: error.clone(),
                },
            );
            ScheduledOutcome::Failed { error }
        }
    };

    ScheduledJobReport {
        position: entry.position,
        job_id: entry.job_id,
        project_name: entry.project.name,
        outcome,
    }
}

/// Close the queue record of a job that failed before its driver ever wrote state.
///
/// The orchestrator's first act is to persist the job it was handed, so normally there is nothing
/// to settle here. It can fail earlier than that — a dirty working tree, an unreadable repository —
/// and the durable record would then still say `QUEUED` for work that was attempted and refused. A
/// later `--resume-queued` would silently retry it, and the scheduler report would disagree with
/// the disk. Recording the failure on the job itself keeps the two in step.
///
/// Best effort on purpose: if this write cannot happen, the job stays `QUEUED`, which is the
/// recoverable direction rather than a lost job.
fn settle_failed_queued_state(store: &StateStore, job_id: &str) {
    let Ok(Some(mut job)) = store.load_job(job_id) else {
        return;
    };
    if job.status != JobStatus::Queued {
        return;
    }
    job.status = JobStatus::Failed;
    job.touch();
    let _ = store.save_job(&job);
}

#[derive(Debug)]
pub enum SchedulerError {
    InvalidConcurrency,
    State(StateError),
    Event(String),
    Worker(String),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConcurrency => {
                formatter.write_str("maximum concurrency must be greater than zero")
            }
            Self::State(error) => write!(formatter, "could not persist queued work: {error}"),
            Self::Event(error) => write!(formatter, "scheduler event error: {error}"),
            Self::Worker(error) => {
                write!(formatter, "a scheduler worker ended abnormally: {error}")
            }
        }
    }
}

impl Error for SchedulerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::State(error) => Some(error),
            _ => None,
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
    };

    use tokio::sync::{mpsc, oneshot};

    use super::{
        JobAssignment, JobDriver, JobOutcome, JsonSchedulerSink, ScheduledOutcome,
        ScheduledRequest, Scheduler, SchedulerControl, SchedulerError, SchedulerEvent,
        SchedulerEventKind, SchedulerEventSink, queued_jobs,
    };
    use crate::orchestrator::{
        events::EventSinkError,
        repository_lock::{RepositoryIdentity, RepositoryLock},
        state::{JobStatus, RunConfiguration, StateStore},
        supervisor::Project,
    };

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn unique(prefix: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lya-{prefix}-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("test directory should be created");
        path
    }

    /// A disposable directory that looks like a Git working tree.
    fn repository(name: &str) -> PathBuf {
        let path = unique(name);
        fs::create_dir_all(path.join(".git")).expect("repository marker should be created");
        path
    }

    fn project(path: &Path) -> Project {
        Project {
            name: path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("project")
                .to_owned(),
            path: path.to_owned(),
        }
    }

    #[derive(Default)]
    struct RecordingSchedulerSink {
        events: Mutex<Vec<SchedulerEvent>>,
    }

    impl RecordingSchedulerSink {
        fn kinds(&self) -> Vec<SchedulerEventKind> {
            self.events
                .lock()
                .expect("event lock")
                .iter()
                .map(|event| event.kind.clone())
                .collect()
        }
    }

    impl SchedulerEventSink for RecordingSchedulerSink {
        fn emit(&self, event: &SchedulerEvent) -> Result<(), EventSinkError> {
            self.events.lock().expect("event lock").push(event.clone());
            Ok(())
        }
    }

    /// A driver that reports every entry and exit and only finishes when the test releases it.
    ///
    /// Overlap is proven by the observed messages and the release channels, never by sleeping.
    /// Gates are keyed by task, so two jobs targeting the same repository can still be released
    /// independently.
    struct GatedDriver {
        started: mpsc::UnboundedSender<String>,
        gates: Mutex<HashMap<String, oneshot::Receiver<JobOutcome>>>,
        concurrency: Arc<Mutex<ConcurrencyWitness>>,
    }

    #[derive(Default)]
    struct ConcurrencyWitness {
        active: HashMap<String, usize>,
        peak_total: usize,
        peak_per_project: usize,
    }

    impl ConcurrencyWitness {
        fn enter(&mut self, project: &str) {
            *self.active.entry(project.to_owned()).or_insert(0) += 1;
            self.peak_total = self.peak_total.max(self.active.values().sum::<usize>());
            self.peak_per_project = self
                .peak_per_project
                .max(*self.active.get(project).unwrap_or(&0));
        }

        fn exit(&mut self, project: &str) {
            if let Some(count) = self.active.get_mut(project) {
                *count -= 1;
            }
        }
    }

    impl GatedDriver {
        fn new(
            gates: Vec<(String, oneshot::Receiver<JobOutcome>)>,
        ) -> (
            Self,
            mpsc::UnboundedReceiver<String>,
            Arc<Mutex<ConcurrencyWitness>>,
        ) {
            let (started, receiver) = mpsc::unbounded_channel();
            let concurrency = Arc::new(Mutex::new(ConcurrencyWitness::default()));
            (
                Self {
                    started,
                    gates: Mutex::new(gates.into_iter().collect()),
                    concurrency: Arc::clone(&concurrency),
                },
                receiver,
                concurrency,
            )
        }
    }

    impl JobDriver for GatedDriver {
        fn drive<'a>(
            &'a self,
            assignment: JobAssignment,
        ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
            let gate = self
                .gates
                .lock()
                .expect("gate lock")
                .remove(&assignment.task);
            let started = self.started.clone();
            let concurrency = Arc::clone(&self.concurrency);
            Box::pin(async move {
                concurrency
                    .lock()
                    .expect("concurrency lock")
                    .enter(&assignment.project.name);
                let _ = started.send(assignment.project.name.clone());
                let outcome = match gate {
                    Some(gate) => gate.await.unwrap_or(JobOutcome::Failed {
                        error: "gate dropped".to_owned(),
                    }),
                    None => JobOutcome::Finished {
                        status: "ACCEPTED".to_owned(),
                        jobs: 1,
                        succeeded: true,
                    },
                };
                concurrency
                    .lock()
                    .expect("concurrency lock")
                    .exit(&assignment.project.name);
                outcome
            })
        }
    }

    /// Take ownership of a queued job the way the real orchestrator does: its very first act is to
    /// write authoritative state for the job it was handed.
    fn record_driven(store_root: &Path, job_id: &str, status: JobStatus) {
        let store = StateStore::at(store_root);
        let mut job = store
            .load_job(job_id)
            .expect("the scheduler should have persisted the job")
            .expect("a driven job exists");
        job.status = status;
        job.touch();
        store.save_job(&job).expect("job state should save");
    }

    /// A driver that asks the scheduler to stop from inside the first job it runs.
    struct StopRequestingDriver {
        control: SchedulerControl,
        started: mpsc::UnboundedSender<String>,
        store_root: PathBuf,
    }

    impl JobDriver for StopRequestingDriver {
        fn drive<'a>(
            &'a self,
            assignment: JobAssignment,
        ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
            let control = self.control.clone();
            let started = self.started.clone();
            let store_root = self.store_root.clone();
            Box::pin(async move {
                record_driven(&store_root, &assignment.job_id, JobStatus::Running);
                let _ = started.send(assignment.project.name.clone());
                control.request_stop();
                // Exactly what a real job observes: its own control channel carries the stop.
                let stopped = assignment.control.stop_requested();
                record_driven(&store_root, &assignment.job_id, JobStatus::Stopped);
                JobOutcome::Finished {
                    status: if stopped { "STOPPED" } else { "ACCEPTED" }.to_owned(),
                    jobs: 1,
                    succeeded: true,
                }
            })
        }
    }

    /// A driver that blocks until its own control channel carries a graceful stop.
    struct StopAwaitingDriver {
        started: mpsc::UnboundedSender<String>,
    }

    impl JobDriver for StopAwaitingDriver {
        fn drive<'a>(
            &'a self,
            assignment: JobAssignment,
        ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
            let started = self.started.clone();
            Box::pin(async move {
                let _ = started.send(assignment.project.name.clone());
                while assignment.control.next().await.is_some() {
                    if assignment.control.stop_requested() {
                        break;
                    }
                }
                JobOutcome::Finished {
                    status: "STOPPED".to_owned(),
                    jobs: 1,
                    succeeded: true,
                }
            })
        }
    }

    /// A driver that checks the repository claim is held while it runs.
    struct ClaimInspectingDriver {
        store_root: PathBuf,
        observations: Arc<Mutex<Vec<bool>>>,
    }

    impl JobDriver for ClaimInspectingDriver {
        fn drive<'a>(
            &'a self,
            assignment: JobAssignment,
        ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
            let store = StateStore::at(&self.store_root);
            let observations = Arc::clone(&self.observations);
            Box::pin(async move {
                let identity = RepositoryIdentity::resolve(&assignment.project.path)
                    .expect("identity should resolve");
                let claimed = RepositoryLock::acquire(&store, &identity).is_err();
                observations.lock().expect("observation lock").push(claimed);
                JobOutcome::Finished {
                    status: "ACCEPTED".to_owned(),
                    jobs: 2,
                    succeeded: true,
                }
            })
        }
    }

    fn cleanup(paths: Vec<PathBuf>) {
        for path in paths {
            let _ = fs::remove_dir_all(path);
        }
    }

    #[tokio::test]
    async fn two_different_repositories_execute_concurrently() {
        let home = unique("scheduler-concurrent-home");
        let first = repository("scheduler-concurrent-a");
        let second = repository("scheduler-concurrent-b");
        let (first_gate, first_release) = oneshot::channel();
        let (second_gate, second_release) = oneshot::channel();
        let (driver, mut started, witness) = GatedDriver::new(vec![
            ("first task".to_owned(), first_release),
            ("second task".to_owned(), second_release),
        ]);
        let scheduler = Scheduler::new(driver, StateStore::at(&home)).with_max_concurrent(2);

        let handle = tokio::spawn(scheduler.run(vec![
            ScheduledRequest::new(project(&first), "first task"),
            ScheduledRequest::new(project(&second), "second task"),
        ]));

        // Both jobs are inside the driver at the same time: neither can finish until released.
        let one = started.recv().await.expect("the first job should start");
        let two = started.recv().await.expect("the second job should start");
        assert_ne!(one, two);
        assert_eq!(
            witness.lock().expect("concurrency lock").peak_total,
            2,
            "two different repositories must be driven at the same time"
        );
        first_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("first job should be released");
        second_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("second job should be released");

        let report = handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert_eq!(report.jobs.len(), 2);
        assert!(report.is_success());
        assert!(report.queued_remaining.is_empty());
        cleanup(vec![home, first, second]);
    }

    #[tokio::test]
    async fn max_concurrency_is_enforced() {
        let home = unique("scheduler-bound-home");
        let repositories = ["bound-a", "bound-b", "bound-c"].map(repository);
        let mut gates = Vec::new();
        let mut releases = Vec::new();
        for index in 0..repositories.len() {
            let (gate, release) = oneshot::channel();
            gates.push((format!("task {index}"), release));
            releases.push(gate);
        }
        let (driver, mut started, witness) = GatedDriver::new(gates);
        let scheduler = Scheduler::new(driver, StateStore::at(&home)).with_max_concurrent(2);
        let requests = repositories
            .iter()
            .enumerate()
            .map(|(index, path)| ScheduledRequest::new(project(path), format!("task {index}")))
            .collect::<Vec<_>>();

        let handle = tokio::spawn(scheduler.run(requests));

        started.recv().await.expect("the first job should start");
        started.recv().await.expect("the second job should start");
        assert!(
            started.try_recv().is_err(),
            "a third job must not start while both slots are taken"
        );
        assert_eq!(witness.lock().expect("concurrency lock").peak_total, 2);
        let mut releases = releases.into_iter();
        releases
            .next()
            .expect("release")
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("first job should be released");
        started
            .recv()
            .await
            .expect("the freed slot should start the third job");
        for release in releases {
            let _ = release.send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            });
        }

        let report = handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert_eq!(report.jobs.len(), 3);
        assert_eq!(
            witness.lock().expect("concurrency lock").peak_total,
            2,
            "the bound must never be exceeded"
        );
        cleanup(
            [home]
                .into_iter()
                .chain(repositories.into_iter())
                .collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    async fn concurrency_of_one_serializes_everything() {
        let home = unique("scheduler-serial-home");
        let first = repository("serial-a");
        let second = repository("serial-b");
        let (first_gate, first_release) = oneshot::channel();
        let (second_gate, second_release) = oneshot::channel();
        let (driver, mut started, witness) = GatedDriver::new(vec![
            ("first".to_owned(), first_release),
            ("second".to_owned(), second_release),
        ]);
        let scheduler = Scheduler::new(driver, StateStore::at(&home)).with_max_concurrent(1);

        let handle = tokio::spawn(scheduler.run(vec![
            ScheduledRequest::new(project(&first), "first"),
            ScheduledRequest::new(project(&second), "second"),
        ]));

        let first_started = started.recv().await.expect("the first job should start");
        assert_eq!(
            first_started,
            project(&first).name,
            "FIFO order decides which job takes the single slot"
        );
        assert!(
            started.try_recv().is_err(),
            "a single slot must never run two jobs at once"
        );
        first_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("first job should be released");
        assert_eq!(
            started.recv().await.expect("the second job should start"),
            project(&second).name
        );
        second_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("second job should be released");

        handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert_eq!(witness.lock().expect("concurrency lock").peak_total, 1);
        cleanup(vec![home, first, second]);
    }

    #[tokio::test]
    async fn two_jobs_for_the_same_repository_never_overlap() {
        let home = unique("scheduler-exclusive-home");
        let project_path = repository("exclusive");
        // Two spellings of one repository: the canonical path and a path through a subdirectory.
        let nested = project_path.join("crate").join("inner");
        fs::create_dir_all(&nested).expect("nested directory should be created");
        let (first_gate, first_release) = oneshot::channel();
        let (second_gate, second_release) = oneshot::channel();
        let (driver, mut started, witness) = GatedDriver::new(vec![
            ("first".to_owned(), first_release),
            ("second".to_owned(), second_release),
        ]);
        let scheduler = Scheduler::new(driver, StateStore::at(&home)).with_max_concurrent(2);
        let mut nested_project = project(&project_path);
        nested_project.path = nested;

        let handle = tokio::spawn(scheduler.run(vec![
            ScheduledRequest::new(project(&project_path), "first"),
            ScheduledRequest::new(nested_project, "second"),
        ]));

        started.recv().await.expect("the first job should start");
        assert!(
            started.try_recv().is_err(),
            "a second job must never enter the same repository while the first is inside it"
        );
        first_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("the first job should be released");
        started
            .recv()
            .await
            .expect("the second job should start once the repository is free");
        second_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("the second job should be released");

        let report = handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert_eq!(report.jobs.len(), 2);
        assert!(report.is_success());
        assert_eq!(
            witness.lock().expect("concurrency lock").peak_per_project,
            1,
            "one repository must never be driven by two jobs at once"
        );
        cleanup(vec![home, project_path]);
    }

    #[tokio::test]
    async fn a_repository_conflict_never_wastes_the_free_slot() {
        let home = unique("scheduler-skip-home");
        let shared_repository = repository("skip-shared");
        let other = repository("skip-other");
        let (blocking_gate, blocking_release) = oneshot::channel();
        let (driver, mut started, _) =
            GatedDriver::new(vec![("first".to_owned(), blocking_release)]);
        let sink = Arc::new(RecordingSchedulerSink::default());
        let scheduler = Scheduler::new(driver, StateStore::at(&home))
            .with_max_concurrent(2)
            .with_event_sink(Arc::clone(&sink) as Arc<dyn SchedulerEventSink>);

        let handle = tokio::spawn(scheduler.run(vec![
            ScheduledRequest::new(project(&shared_repository), "first"),
            ScheduledRequest::new(project(&shared_repository), "blocked by the first"),
            ScheduledRequest::new(project(&other), "unrelated"),
        ]));

        started.recv().await.expect("the first job should start");
        // The blocked second entry must not hold the free slot hostage.
        assert_eq!(
            started
                .recv()
                .await
                .expect("the unrelated repository should start"),
            project(&other).name
        );
        blocking_gate
            .send(JobOutcome::Finished {
                status: "ACCEPTED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("the blocking job should be released");

        let report = handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert_eq!(report.jobs.len(), 3);
        assert!(
            sink.kinds()
                .iter()
                .any(|kind| matches!(kind, SchedulerEventKind::WaitingForRepository { .. })),
            "a blocked entry should be observable"
        );
        cleanup(vec![home, shared_repository, other]);
    }

    #[tokio::test]
    async fn the_repository_claim_is_held_for_the_whole_driver_call_and_released_after_it() {
        let home = unique("scheduler-claim-home");
        let project_path = repository("claim-lifecycle");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let driver = ClaimInspectingDriver {
            store_root: home.clone(),
            observations: Arc::clone(&observations),
        };
        let scheduler = Scheduler::new(driver, StateStore::at(&home));

        let report = scheduler
            .run(vec![ScheduledRequest::new(
                project(&project_path),
                "sequential chain",
            )])
            .await
            .expect("the scheduler should finish");

        assert!(report.is_success());
        assert_eq!(
            observations.lock().expect("observation lock").as_slice(),
            [true],
            "the claim must be held for the entire chain, including its sequential children"
        );
        let store = StateStore::at(&home);
        let identity = RepositoryIdentity::resolve(&project_path).expect("identity");
        assert!(
            RepositoryLock::acquire(&store, &identity).is_ok(),
            "a normal completion must release the repository claim"
        );
        cleanup(vec![home, project_path]);
    }

    #[tokio::test]
    async fn one_job_failure_never_kills_an_unrelated_job() {
        let home = unique("scheduler-failure-home");
        let failing = repository("failure-a");
        let healthy = repository("failure-b");
        let (failing_gate, failing_release) = oneshot::channel();
        let (healthy_gate, healthy_release) = oneshot::channel();
        let (driver, mut started, _) = GatedDriver::new(vec![
            ("will fail".to_owned(), failing_release),
            ("must still finish".to_owned(), healthy_release),
        ]);
        let scheduler = Scheduler::new(driver, StateStore::at(&home)).with_max_concurrent(2);

        let handle = tokio::spawn(scheduler.run(vec![
            ScheduledRequest::new(project(&failing), "will fail"),
            ScheduledRequest::new(project(&healthy), "must still finish"),
        ]));

        started.recv().await.expect("the first job should start");
        started.recv().await.expect("the second job should start");
        failing_gate
            .send(JobOutcome::Failed {
                error: "supervisor error: provider unavailable".to_owned(),
            })
            .expect("the failing job should be released");
        healthy_gate
            .send(JobOutcome::Finished {
                status: "PUBLISHED".to_owned(),
                jobs: 1,
                succeeded: true,
            })
            .expect("the healthy job should be released");

        let report = handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert!(matches!(
            report.jobs[0].outcome,
            ScheduledOutcome::Failed { .. }
        ));
        assert!(matches!(
            report.jobs[1].outcome,
            ScheduledOutcome::Finished { ref status, .. } if status == "PUBLISHED"
        ));
        assert!(!report.is_success());
        assert!(!report.stopped);
        cleanup(vec![home, failing, healthy]);
    }

    #[tokio::test]
    async fn a_graceful_stop_prevents_new_jobs_and_leaves_the_rest_queued() {
        let home = unique("scheduler-stop-home");
        let first = repository("stop-a");
        let second = repository("stop-b");
        let control = SchedulerControl::new();
        let (started, mut observed) = mpsc::unbounded_channel();
        let sink = Arc::new(RecordingSchedulerSink::default());
        let scheduler = Scheduler::new(
            StopRequestingDriver {
                control: control.clone(),
                started,
                store_root: home.clone(),
            },
            StateStore::at(&home),
        )
        .with_max_concurrent(1)
        .with_control(control)
        .with_event_sink(Arc::clone(&sink) as Arc<dyn SchedulerEventSink>);

        let report = scheduler
            .run(vec![
                ScheduledRequest::new(project(&first), "runs"),
                ScheduledRequest::new(project(&second), "never starts"),
            ])
            .await
            .expect("the scheduler should finish");

        assert_eq!(
            observed.recv().await.as_deref(),
            Some(project(&first).name.as_str())
        );
        assert!(
            observed.try_recv().is_err(),
            "no new job may start after a graceful stop"
        );
        assert_eq!(report.jobs.len(), 1);
        assert!(report.stopped);
        assert_eq!(report.queued_remaining.len(), 1);
        assert!(
            matches!(report.jobs[0].outcome, ScheduledOutcome::Finished { ref status, .. } if status == "STOPPED"),
            "the active job must receive the graceful stop through its own control channel"
        );
        assert!(
            sink.kinds()
                .iter()
                .any(|kind| matches!(kind, SchedulerEventKind::SchedulerStopping { .. })),
            "stopping must be observable"
        );

        // Queued work survives the interruption, is distinguishable from driven work, and stays
        // outside `lya resume`.
        let store = StateStore::at(&home);
        let queued = queued_jobs(&store).expect("queued jobs should load");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].job_id, report.queued_remaining[0]);
        assert_eq!(queued[0].status, JobStatus::Queued);
        assert!(!queued[0].status.is_resumable());
        assert_eq!(queued[0].task, "never starts");
        cleanup(vec![home, first, second]);
    }

    #[tokio::test]
    async fn active_jobs_receive_the_graceful_stop() {
        let home = unique("scheduler-active-stop-home");
        let project_path = repository("active-stop");
        let control = SchedulerControl::new();
        let (started, mut observed) = mpsc::unbounded_channel();
        let scheduler = Scheduler::new(StopAwaitingDriver { started }, StateStore::at(&home))
            .with_control(control.clone());

        let handle = tokio::spawn(scheduler.run(vec![ScheduledRequest::new(
            project(&project_path),
            "waits for a stop",
        )]));
        observed.recv().await.expect("the job should start");
        control.request_stop();

        let report = handle
            .await
            .expect("the scheduler task should join")
            .expect("the scheduler should finish");
        assert!(
            matches!(report.jobs[0].outcome, ScheduledOutcome::Finished { ref status, .. } if status == "STOPPED")
        );
        cleanup(vec![home, project_path]);
    }

    #[tokio::test]
    async fn persisted_queued_work_can_be_scheduled_again_with_its_own_identity() {
        let home = unique("scheduler-requeue-home");
        let project_path = repository("requeue");
        let control = SchedulerControl::new();
        control.request_stop();
        let (driver, _started, _) = GatedDriver::new(Vec::new());
        let scheduler = Scheduler::new(driver, StateStore::at(&home)).with_control(control);
        let report = scheduler
            .run(vec![ScheduledRequest::new(project(&project_path), "later")])
            .await
            .expect("the scheduler should finish");
        assert_eq!(report.queued_remaining.len(), 1);
        let job_id = report.queued_remaining[0].clone();

        let store = StateStore::at(&home);
        let persisted = queued_jobs(&store).expect("queued jobs should load");
        let (driver, _started, _) = GatedDriver::new(Vec::new());
        let report = Scheduler::new(driver, StateStore::at(&home))
            .run(
                persisted
                    .iter()
                    .map(ScheduledRequest::for_persisted_job)
                    .collect(),
            )
            .await
            .expect("the scheduler should finish");

        assert_eq!(report.jobs.len(), 1);
        assert_eq!(
            report.jobs[0].job_id, job_id,
            "re-queued work keeps its original job identity"
        );
        assert!(report.queued_remaining.is_empty());
        cleanup(vec![home, project_path]);
    }

    #[tokio::test]
    async fn queued_work_records_the_run_configuration_it_was_accepted_with() {
        let home = unique("scheduler-configuration-home");
        let project_path = repository("configuration");
        let control = SchedulerControl::new();
        control.request_stop();
        let (driver, _started, _) = GatedDriver::new(Vec::new());
        let run = RunConfiguration {
            max_iterations: 4,
            max_jobs: 3,
            browser: true,
            publish: false,
            git: None,
        };

        Scheduler::new(driver, StateStore::at(&home))
            .with_control(control)
            .with_run_configuration(run.clone())
            .run(vec![ScheduledRequest::new(
                project(&project_path),
                "queued only",
            )])
            .await
            .expect("the scheduler should finish");

        let queued = queued_jobs(&StateStore::at(&home)).expect("queued jobs should load");
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].run, run,
            "max_jobs and the other limits must survive the queue"
        );
        assert_eq!(queued[0].sequential_index, 0);
        cleanup(vec![home, project_path]);
    }

    #[tokio::test]
    async fn a_repository_claimed_by_another_process_is_reported_instead_of_started() {
        let home = unique("scheduler-claimed-home");
        let project_path = repository("claimed-elsewhere");
        let store = StateStore::at(&home);
        let identity = RepositoryIdentity::resolve(&project_path).expect("identity");
        // Stands in for another Lya process already driving this repository.
        let elsewhere = RepositoryLock::acquire(&store, &identity).expect("claim should be taken");
        let (driver, mut started, _) = GatedDriver::new(Vec::new());

        let report = Scheduler::new(driver, StateStore::at(&home))
            .run(vec![ScheduledRequest::new(
                project(&project_path),
                "must not start",
            )])
            .await
            .expect("the scheduler should finish");

        assert!(
            started.try_recv().is_err(),
            "a repository driven elsewhere must never be driven here"
        );
        assert!(matches!(
            report.jobs[0].outcome,
            ScheduledOutcome::Rejected { ref reason } if reason.contains("another Lya process")
        ));
        assert!(!report.is_success());
        drop(elsewhere);
        cleanup(vec![home, project_path]);
    }

    /// A job that fails before its driver ever writes state must not stay advertised as queued: the
    /// durable record and the scheduler report have to agree, and `--resume-queued` must not
    /// silently retry attempted work.
    #[tokio::test]
    async fn a_job_that_fails_before_its_first_write_stops_being_queued() {
        struct FailingDriver;

        impl JobDriver for FailingDriver {
            fn drive<'a>(
                &'a self,
                _assignment: JobAssignment,
            ) -> Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>> {
                // Exactly what the orchestrator does when it refuses a dirty working tree: it
                // returns an error without ever persisting the job.
                Box::pin(async {
                    JobOutcome::Failed {
                        error: "refusing to start job because the working tree is not clean"
                            .to_owned(),
                    }
                })
            }
        }

        let home = unique("scheduler-settle-home");
        let project_path = repository("settle");

        let report = Scheduler::new(FailingDriver, StateStore::at(&home))
            .run(vec![ScheduledRequest::new(
                project(&project_path),
                "refused",
            )])
            .await
            .expect("the scheduler should finish");

        assert!(matches!(
            report.jobs[0].outcome,
            ScheduledOutcome::Failed { .. }
        ));
        assert!(report.queued_remaining.is_empty());
        let store = StateStore::at(&home);
        assert_eq!(
            store
                .load_job(&report.jobs[0].job_id)
                .expect("job should load")
                .expect("job should exist")
                .status,
            JobStatus::Failed
        );
        assert!(
            queued_jobs(&store)
                .expect("queued jobs should load")
                .is_empty(),
            "attempted work must not be picked up again by --resume-queued"
        );
        cleanup(vec![home, project_path]);
    }

    #[tokio::test]
    async fn json_scheduler_output_stays_one_valid_object_per_line() {
        let home = unique("scheduler-json-home");
        let first = repository("json-a");
        let second = repository("json-b");
        let (driver, _started, _) = GatedDriver::new(Vec::new());
        let buffer = SharedBuffer::default();
        let scheduler = Scheduler::new(driver, StateStore::at(&home))
            .with_max_concurrent(2)
            .with_event_sink(Arc::new(JsonSchedulerSink::new(buffer.clone())));

        scheduler
            .run(vec![
                ScheduledRequest::new(project(&first), "first"),
                ScheduledRequest::new(project(&second), "second"),
            ])
            .await
            .expect("the scheduler should finish");

        let rendered = buffer.contents();
        assert!(!rendered.is_empty());
        for line in rendered.lines() {
            let value: serde_json::Value =
                serde_json::from_str(line).expect("every stdout line must be one JSON object");
            assert!(
                value.get("scheduler_event").is_some(),
                "a scheduler event must identify itself: {line}"
            );
        }
        assert!(rendered.contains("SCHEDULER_STARTED"));
        assert!(rendered.contains("SCHEDULER_FINISHED"));
        cleanup(vec![home, first, second]);
    }

    #[tokio::test]
    async fn zero_concurrency_is_refused() {
        let home = unique("scheduler-zero-home");
        let (driver, _started, _) = GatedDriver::new(Vec::new());

        let error = Scheduler::new(driver, StateStore::at(&home))
            .with_max_concurrent(0)
            .run(Vec::new())
            .await
            .expect_err("zero concurrency should be refused");

        assert!(matches!(error, SchedulerError::InvalidConcurrency));
        assert!(error.to_string().contains("greater than zero"));
        cleanup(vec![home]);
    }

    #[tokio::test]
    async fn an_unusable_project_path_is_reported_without_stopping_the_rest() {
        let home = unique("scheduler-unusable-home");
        let healthy = repository("unusable-healthy");
        let (driver, _started, _) = GatedDriver::new(Vec::new());
        let missing = Project {
            name: "missing".to_owned(),
            path: PathBuf::from("this-path-does-not-exist-anywhere"),
        };

        let report = Scheduler::new(driver, StateStore::at(&home))
            .run(vec![
                ScheduledRequest::new(missing, "cannot resolve"),
                ScheduledRequest::new(project(&healthy), "still runs"),
            ])
            .await
            .expect("the scheduler should finish");

        assert!(matches!(
            report.jobs[0].outcome,
            ScheduledOutcome::Rejected { .. }
        ));
        assert!(matches!(
            report.jobs[1].outcome,
            ScheduledOutcome::Finished { .. }
        ));
        cleanup(vec![home, healthy]);
    }

    /// A writer the test can keep reading while the scheduler writes into it.
    #[derive(Clone, Default)]
    struct SharedBuffer {
        contents: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedBuffer {
        fn contents(&self) -> String {
            String::from_utf8(self.contents.lock().expect("buffer lock").clone())
                .expect("buffer should be UTF-8")
        }
    }

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.contents
                .lock()
                .expect("buffer lock")
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
