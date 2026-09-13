//! Daemon-level observation.
//!
//! This is a third, separate stream, and the separation is the point:
//!
//! * [`JobEvent`](crate::orchestrator::events::JobEvent) describes what happens *inside* one job and
//!   keeps living in that job's `events.jsonl`;
//! * [`SchedulerEvent`](crate::orchestrator::scheduler::SchedulerEvent) describes what the scheduler
//!   admits and starts;
//! * [`DaemonEvent`] describes the daemon itself — that it started, who connected, what was
//!   submitted, that it is stopping.
//!
//! Like both of the others it is observation, never authority: every decision is made from
//! authoritative persisted state and from the operating system's own claims, and nothing is ever
//! replayed from a log.
//!
//! Not everything worth showing is worth keeping. A client connecting and disconnecting is useful
//! while watching a foreground daemon and would be pure noise in a file that grows for months, so
//! [`DaemonEventKind::is_durable`] decides what reaches the permanent log and
//! [`DaemonLogSink`] writes only that. Transient events still reach a foreground terminal.

use std::{
    fs::{self, OpenOptions},
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};

use crate::orchestrator::events::{EventSinkError, current_unix_millis, format_timestamp, style};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEvent {
    pub timestamp_unix_millis: u64,
    #[serde(flatten)]
    pub kind: DaemonEventKind,
}

impl DaemonEvent {
    pub fn new(kind: DaemonEventKind) -> Self {
        Self {
            timestamp_unix_millis: current_unix_millis(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "daemon_event", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DaemonEventKind {
    DaemonStarted {
        process_id: u32,
        endpoint: String,
        protocol_version: u32,
        max_concurrent: usize,
    },
    /// Work a previous daemon accepted and never started, picked up again.
    QueuedWorkRecovered {
        job_ids: Vec<String>,
    },
    /// Interrupted jobs that were found but deliberately not continued.
    InterruptedWorkParked {
        job_ids: Vec<String>,
    },
    /// Interrupted jobs this daemon was explicitly configured to continue.
    InterruptedWorkResumed {
        job_ids: Vec<String>,
    },
    SchedulerStarted {
        max_concurrent: usize,
    },
    SchedulerStopped {
        completed: usize,
        failed: usize,
        rejected: usize,
        queued_remaining: usize,
    },
    ClientConnected {
        client_id: u64,
    },
    ClientDisconnected {
        client_id: u64,
        reason: String,
    },
    /// A client sent something this daemon could not use. Kept out of the permanent log: it says
    /// something about that client, not about the daemon's work.
    ClientRejected {
        client_id: u64,
        code: String,
        reason: String,
    },
    JobsSubmitted {
        client_id: u64,
        job_ids: Vec<String>,
    },
    JobAttached {
        client_id: u64,
        job_id: String,
    },
    JobDetached {
        client_id: u64,
        job_id: String,
        reason: String,
    },
    ControlDelivered {
        client_id: u64,
        job_id: String,
        command: String,
    },
    DaemonStopping {
        reason: String,
        active_jobs: usize,
    },
    DaemonStopped,
}

impl DaemonEventKind {
    /// Whether this event belongs in the permanent daemon log.
    ///
    /// Lifecycle and work keep their history. Per-connection traffic does not: a long-lived daemon
    /// would otherwise fill its log with the comings and goings of `lya daemon status`.
    pub fn is_durable(&self) -> bool {
        match self {
            Self::DaemonStarted { .. }
            | Self::QueuedWorkRecovered { .. }
            | Self::InterruptedWorkParked { .. }
            | Self::InterruptedWorkResumed { .. }
            | Self::SchedulerStarted { .. }
            | Self::SchedulerStopped { .. }
            | Self::JobsSubmitted { .. }
            | Self::ControlDelivered { .. }
            | Self::DaemonStopping { .. }
            | Self::DaemonStopped => true,
            Self::ClientConnected { .. }
            | Self::ClientDisconnected { .. }
            | Self::ClientRejected { .. }
            | Self::JobAttached { .. }
            | Self::JobDetached { .. } => false,
        }
    }
}

pub trait DaemonEventSink: Send + Sync {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError>;
}

impl<T: DaemonEventSink + ?Sized> DaemonEventSink for std::sync::Arc<T> {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
        (**self).emit(event)
    }
}

#[derive(Debug, Default)]
pub struct NoopDaemonSink;

impl DaemonEventSink for NoopDaemonSink {
    fn emit(&self, _event: &DaemonEvent) -> Result<(), EventSinkError> {
        Ok(())
    }
}

pub struct CompositeDaemonSink {
    sinks: Vec<Box<dyn DaemonEventSink>>,
}

impl CompositeDaemonSink {
    pub fn new(sinks: Vec<Box<dyn DaemonEventSink>>) -> Self {
        Self { sinks }
    }
}

impl DaemonEventSink for CompositeDaemonSink {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
        for sink in &self.sinks {
            sink.emit(event)?;
        }
        Ok(())
    }
}

/// Passes on only the events worth keeping, and drops the transient ones.
///
/// A daemon narrating to a terminal should show everything, including who connected. The same
/// narration captured into a file must not: a daemon that runs for weeks would fill it with the
/// comings and goings of every `lya daemon status`. Wrapping the sink is what lets one narration
/// serve both without either destination knowing about the other.
pub struct DurableDaemonSink {
    inner: Box<dyn DaemonEventSink>,
}

impl DurableDaemonSink {
    pub fn new(inner: Box<dyn DaemonEventSink>) -> Self {
        Self { inner }
    }
}

impl DaemonEventSink for DurableDaemonSink {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
        if !event.kind.is_durable() {
            return Ok(());
        }
        self.inner.emit(event)
    }
}

/// The permanent daemon log, `LYA_HOME/daemon/events.jsonl`.
///
/// Durable events only, one JSON object per line. It is a record of what the daemon did, never
/// something a decision is derived from.
pub struct DaemonLogSink {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl DaemonLogSink {
    pub fn for_home(home: &Path) -> Self {
        Self::new(home.join("daemon").join("events.jsonl"))
    }

    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DaemonEventSink for DaemonLogSink {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
        if !event.kind.is_durable() {
            return Ok(());
        }
        let _lock = self
            .write_lock
            .lock()
            .map_err(|_| EventSinkError::Write("daemon log lock was poisoned".to_owned()))?;
        let parent = self.path.parent().ok_or_else(|| {
            EventSinkError::Write("daemon log path has no parent directory".to_owned())
        })?;
        fs::create_dir_all(parent).map_err(|error| EventSinkError::Write(error.to_string()))?;
        let mut line = serde_json::to_vec(event)
            .map_err(|error| EventSinkError::Serialize(error.to_string()))?;
        line.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| EventSinkError::Write(error.to_string()))?;
        file.write_all(&line)
            .map_err(|error| EventSinkError::Write(error.to_string()))?;
        file.sync_data()
            .map_err(|error| EventSinkError::Write(error.to_string()))
    }
}

/// One JSON object per line, for `lya daemon run --json`.
pub struct JsonDaemonSink<W: Write + Send> {
    writer: Mutex<W>,
}

impl<W: Write + Send> JsonDaemonSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl<W: Write + Send> DaemonEventSink for JsonDaemonSink<W> {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
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

/// Human-readable daemon lines, for a foreground daemon.
pub struct HumanDaemonSink<W: Write + Send> {
    writer: Mutex<W>,
    color: bool,
}

impl<W: Write + Send> HumanDaemonSink<W> {
    pub fn new(writer: W, color: bool) -> Self {
        Self {
            writer: Mutex::new(writer),
            color,
        }
    }
}

impl HumanDaemonSink<io::Stderr> {
    /// Daemon lines go to standard error, so a foreground daemon's own narration never mixes into
    /// the job event stream on standard output.
    pub fn stderr() -> Self {
        Self::new(io::stderr(), io::stderr().is_terminal())
    }
}

impl<W: Write + Send> DaemonEventSink for HumanDaemonSink<W> {
    fn emit(&self, event: &DaemonEvent) -> Result<(), EventSinkError> {
        let rendered = render_daemon_event(event, self.color);
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

pub fn render_daemon_event(event: &DaemonEvent, color: bool) -> String {
    let timestamp = format_timestamp(event.timestamp_unix_millis);
    let heading = style("DAEMON", "35", color);
    let line = |summary: &str| format!("{timestamp}  {heading}  {summary}\n");
    let list = |job_ids: &[String]| job_ids.join(", ");
    match &event.kind {
        DaemonEventKind::DaemonStarted {
            process_id,
            endpoint,
            protocol_version,
            max_concurrent,
        } => line(&format!(
            "{} as process {process_id} on {endpoint} (protocol {protocol_version}, at most {max_concurrent} repositories at a time)",
            style("started", "32", color)
        )),
        DaemonEventKind::QueuedWorkRecovered { job_ids } => line(&format!(
            "recovered {} queued job(s): {}",
            job_ids.len(),
            list(job_ids)
        )),
        DaemonEventKind::InterruptedWorkParked { job_ids } => line(&format!(
            "{} {} interrupted job(s) left parked: {} (continue with lya resume --job <id>)",
            style("parked", "33", color),
            job_ids.len(),
            list(job_ids)
        )),
        DaemonEventKind::InterruptedWorkResumed { job_ids } => line(&format!(
            "resuming {} interrupted job(s): {}",
            job_ids.len(),
            list(job_ids)
        )),
        DaemonEventKind::SchedulerStarted { max_concurrent } => line(&format!(
            "scheduler ready for at most {max_concurrent} repositories at a time"
        )),
        DaemonEventKind::SchedulerStopped {
            completed,
            failed,
            rejected,
            queued_remaining,
        } => line(&format!(
            "scheduler stopped: {completed} finished, {failed} failed, {rejected} not started, {queued_remaining} still queued"
        )),
        DaemonEventKind::ClientConnected { client_id } => {
            line(&format!("client {client_id} connected"))
        }
        DaemonEventKind::ClientDisconnected { client_id, reason } => {
            line(&format!("client {client_id} disconnected ({reason})"))
        }
        DaemonEventKind::ClientRejected {
            client_id,
            code,
            reason,
        } => line(&format!(
            "{} client {client_id}: {code} {reason}",
            style("refused", "31", color)
        )),
        DaemonEventKind::JobsSubmitted { client_id, job_ids } => line(&format!(
            "client {client_id} submitted {} job(s): {}",
            job_ids.len(),
            list(job_ids)
        )),
        DaemonEventKind::JobAttached { client_id, job_id } => {
            line(&format!("client {client_id} attached to {job_id}"))
        }
        DaemonEventKind::JobDetached {
            client_id,
            job_id,
            reason,
        } => line(&format!(
            "client {client_id} detached from {job_id} ({reason})"
        )),
        DaemonEventKind::ControlDelivered {
            client_id,
            job_id,
            command,
        } => line(&format!("client {client_id} sent {command} to {job_id}")),
        DaemonEventKind::DaemonStopping {
            reason,
            active_jobs,
        } => line(&format!(
            "{}; {active_jobs} active job(s) will shut down safely ({reason})",
            style("stopping", "33", color)
        )),
        DaemonEventKind::DaemonStopped => line(&style("stopped", "32", color)),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        DaemonEvent, DaemonEventKind, DaemonEventSink, DaemonLogSink, DurableDaemonSink,
        JsonDaemonSink, render_daemon_event,
    };

    static NEXT_HOME: AtomicUsize = AtomicUsize::new(0);

    fn home() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lya-daemon-events-{}-{}",
            std::process::id(),
            NEXT_HOME.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("home should be created");
        path
    }

    #[test]
    fn daemon_events_are_tagged_so_they_never_have_to_be_told_apart_by_guessing() {
        let rendered = serde_json::to_value(DaemonEvent::new(DaemonEventKind::DaemonStopped))
            .expect("the event should encode");

        assert_eq!(rendered["daemon_event"], "DAEMON_STOPPED");
        assert!(rendered["timestamp_unix_millis"].is_number());
    }

    /// The permanent log keeps lifecycle and work, and refuses per-connection noise.
    #[test]
    fn the_permanent_log_keeps_work_and_drops_transient_connection_noise() {
        let home = home();
        let sink = DaemonLogSink::for_home(&home);

        for kind in [
            DaemonEventKind::DaemonStarted {
                process_id: 1,
                endpoint: "endpoint".to_owned(),
                protocol_version: 1,
                max_concurrent: 2,
            },
            DaemonEventKind::ClientConnected { client_id: 7 },
            DaemonEventKind::ClientDisconnected {
                client_id: 7,
                reason: "closed".to_owned(),
            },
            DaemonEventKind::JobAttached {
                client_id: 7,
                job_id: "job-1".to_owned(),
            },
            DaemonEventKind::JobsSubmitted {
                client_id: 7,
                job_ids: vec!["job-1".to_owned()],
            },
            DaemonEventKind::DaemonStopped,
        ] {
            sink.emit(&DaemonEvent::new(kind))
                .expect("emit should work");
        }

        let written = fs::read_to_string(sink.path()).expect("the log should exist");
        let lines = written.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3, "only durable events are kept: {written}");
        assert!(written.contains("DAEMON_STARTED"));
        assert!(written.contains("JOBS_SUBMITTED"));
        assert!(written.contains("DAEMON_STOPPED"));
        assert!(!written.contains("CLIENT_CONNECTED"));
        assert!(!written.contains("JOB_ATTACHED"));
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).expect("every line is one JSON object");
        }
        let _ = fs::remove_dir_all(home);
    }

    /// A narration captured into a file keeps the work and drops the connection traffic; the same
    /// narration on a terminal keeps everything.
    #[test]
    fn a_captured_narration_keeps_only_the_events_worth_keeping() {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = DurableDaemonSink::new(Box::new(JsonDaemonSink::new(Shared(
            std::sync::Arc::clone(&buffer),
        ))));

        sink.emit(&DaemonEvent::new(DaemonEventKind::ClientConnected {
            client_id: 1,
        }))
        .expect("emit should work");
        sink.emit(&DaemonEvent::new(DaemonEventKind::JobsSubmitted {
            client_id: 1,
            job_ids: vec!["job-1".to_owned()],
        }))
        .expect("emit should work");

        let written = String::from_utf8(buffer.lock().expect("buffer lock").clone())
            .expect("output should be UTF-8");
        assert_eq!(written.lines().count(), 1, "{written}");
        assert!(written.contains("JOBS_SUBMITTED"));
        assert!(!written.contains("CLIENT_CONNECTED"));
    }

    /// A writer the tests can read back.
    struct Shared(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Shared {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buffer lock").extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_json_sink_emits_every_event_as_one_line() {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = JsonDaemonSink::new(Shared(std::sync::Arc::clone(&buffer)));

        sink.emit(&DaemonEvent::new(DaemonEventKind::ClientConnected {
            client_id: 3,
        }))
        .expect("emit should work");
        sink.emit(&DaemonEvent::new(DaemonEventKind::DaemonStopped))
            .expect("emit should work");

        let written = String::from_utf8(buffer.lock().expect("buffer lock").clone())
            .expect("output should be UTF-8");
        assert_eq!(
            written.lines().count(),
            2,
            "a foreground daemon shows transient events too: {written}"
        );
    }

    #[test]
    fn every_event_renders_one_terminal_line() {
        for kind in [
            DaemonEventKind::DaemonStarted {
                process_id: 4,
                endpoint: "endpoint".to_owned(),
                protocol_version: 1,
                max_concurrent: 2,
            },
            DaemonEventKind::QueuedWorkRecovered {
                job_ids: vec!["job-1".to_owned()],
            },
            DaemonEventKind::InterruptedWorkParked {
                job_ids: vec!["job-2".to_owned()],
            },
            DaemonEventKind::InterruptedWorkResumed {
                job_ids: vec!["job-2".to_owned()],
            },
            DaemonEventKind::SchedulerStarted { max_concurrent: 2 },
            DaemonEventKind::SchedulerStopped {
                completed: 1,
                failed: 0,
                rejected: 0,
                queued_remaining: 0,
            },
            DaemonEventKind::ClientConnected { client_id: 1 },
            DaemonEventKind::ClientDisconnected {
                client_id: 1,
                reason: "closed".to_owned(),
            },
            DaemonEventKind::ClientRejected {
                client_id: 1,
                code: "INVALID_REQUEST".to_owned(),
                reason: "not JSON".to_owned(),
            },
            DaemonEventKind::JobsSubmitted {
                client_id: 1,
                job_ids: vec!["job-1".to_owned()],
            },
            DaemonEventKind::JobAttached {
                client_id: 1,
                job_id: "job-1".to_owned(),
            },
            DaemonEventKind::JobDetached {
                client_id: 1,
                job_id: "job-1".to_owned(),
                reason: "client closed".to_owned(),
            },
            DaemonEventKind::ControlDelivered {
                client_id: 1,
                job_id: "job-1".to_owned(),
                command: "PAUSE".to_owned(),
            },
            DaemonEventKind::DaemonStopping {
                reason: "a client asked".to_owned(),
                active_jobs: 1,
            },
            DaemonEventKind::DaemonStopped,
        ] {
            let rendered = render_daemon_event(&DaemonEvent::new(kind.clone()), false);

            assert!(rendered.ends_with('\n'), "{kind:?}");
            assert_eq!(rendered.lines().count(), 1, "{kind:?}");
            assert!(rendered.contains("DAEMON"), "{kind:?}");
        }
    }
}
