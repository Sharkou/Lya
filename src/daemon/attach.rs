//! Live observation of a daemon-owned job.
//!
//! Attaching is observational. A client subscribes to the events one job is emitting *now*; it does
//! not take the job over, and leaving does not touch it. The job belongs to the daemon before,
//! during and after any number of viewers.
//!
//! The stream is a broadcast, which is what makes that true mechanically:
//!
//! * emitting an event never blocks on a viewer, so a slow or vanished client cannot hold up a job;
//! * a viewer that cannot keep up is told it fell behind and is disconnected, alone;
//! * a job with no viewers emits into a channel nobody reads, at no cost.
//!
//! History is optional and never authoritative. `events.jsonl` can be replayed before the live
//! stream so an attaching client sees where a job has got to, and [`ReplayGuard`] keeps the join
//! seamless: the subscription is opened *before* the file is read, so nothing that happens during
//! the read is missed, and any event the replay already showed is suppressed once when it arrives
//! live. Decisions are never derived from either stream — the orchestrator reads persisted state.

use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use tokio::sync::broadcast;

use crate::orchestrator::events::{EventSink, EventSinkError, JobEvent};

/// How many events one attached client may fall behind before it is disconnected.
///
/// Generous enough that a terminal never loses events in practice, bounded so a client that stopped
/// reading cannot make the daemon buffer without limit.
pub const ATTACH_CHANNEL_CAPACITY: usize = 1024;

/// How many recorded events a replay shows at most, newest last.
///
/// A job that has been running for days has a long history and an attaching client wants its recent
/// shape, not a re-run of everything.
pub const MAX_REPLAYED_EVENTS: usize = 500;

/// How many replayed events are remembered for duplicate suppression.
pub const REPLAY_GUARD_CAPACITY: usize = 256;

/// Fans one job's events out to every attached client.
///
/// One channel per job, created when the job starts emitting and dropped when it is released, so a
/// daemon that has driven a thousand jobs holds channels only for the jobs it is driving.
#[derive(Clone, Default)]
pub struct JobEventBroadcaster {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<Arc<JobEvent>>>>>,
}

impl JobEventBroadcaster {
    pub fn new() -> Self {
        Self::default()
    }

    /// The sink a driven job emits into, in addition to its own `events.jsonl`.
    ///
    /// `job_id` is the job the driver was handed — the first of a sequential chain. Events from the
    /// children that chain starts reach both this stream and their own, so a client can watch the
    /// whole chain from where it started or one job of it by name.
    pub fn sink(&self, job_id: &str) -> BroadcastEventSink {
        BroadcastEventSink {
            broadcaster: self.clone(),
            chain: job_id.to_owned(),
        }
    }

    /// Subscribe to one job's live events.
    ///
    /// A subscription can be opened before the job emits anything, which is what lets an attaching
    /// client cover the gap while history is being replayed.
    pub fn subscribe(&self, job_id: &str) -> broadcast::Receiver<Arc<JobEvent>> {
        self.sender(job_id).subscribe()
    }

    /// Forget one job's channel. Attached clients see the stream end.
    pub fn release(&self, job_id: &str) {
        self.channels
            .lock()
            .expect("attach channel lock")
            .remove(job_id);
    }

    /// How many clients are attached to anything.
    pub fn attached_clients(&self) -> usize {
        self.channels
            .lock()
            .expect("attach channel lock")
            .values()
            .map(broadcast::Sender::receiver_count)
            .sum()
    }

    pub fn streaming_job_ids(&self) -> Vec<String> {
        self.channels
            .lock()
            .expect("attach channel lock")
            .keys()
            .cloned()
            .collect()
    }

    fn sender(&self, job_id: &str) -> broadcast::Sender<Arc<JobEvent>> {
        self.channels
            .lock()
            .expect("attach channel lock")
            .entry(job_id.to_owned())
            .or_insert_with(|| broadcast::channel(ATTACH_CHANNEL_CAPACITY).0)
            .clone()
    }
}

/// Emits a job's events to whoever is watching.
///
/// Having no viewers is not a failure, and neither is one that stopped reading: a job's progress can
/// never depend on somebody watching it.
pub struct BroadcastEventSink {
    broadcaster: JobEventBroadcaster,
    /// The job the driver was handed, which is the whole chain's stream.
    chain: String,
}

impl EventSink for BroadcastEventSink {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError> {
        let event = Arc::new(event.clone());
        let _ = self
            .broadcaster
            .sender(&self.chain)
            .send(Arc::clone(&event));
        // A sequential child also has its own name, and a client may have attached to that.
        if event.job_id != self.chain {
            let _ = self.broadcaster.sender(&event.job_id).send(event);
        }
        Ok(())
    }
}

/// Suppresses the events a replay already showed.
///
/// Bounded on purpose: only the tail of a replay can overlap the live stream, so remembering the
/// tail is enough and remembering everything would be a leak.
#[derive(Debug, Default)]
pub struct ReplayGuard {
    recent: VecDeque<JobEvent>,
}

impl ReplayGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, event: &JobEvent) {
        if self.recent.len() == REPLAY_GUARD_CAPACITY {
            self.recent.pop_front();
        }
        self.recent.push_back(event.clone());
    }

    /// Whether this live event is one the replay has already shown.
    pub fn already_shown(&mut self, event: &JobEvent) -> bool {
        if let Some(index) = self.recent.iter().position(|shown| shown == event) {
            // Removed once: a job that legitimately emits the same event twice is shown twice.
            self.recent.remove(index);
            return true;
        }
        false
    }
}

/// The recorded events of one job, oldest first, at most [`MAX_REPLAYED_EVENTS`].
///
/// A line that cannot be parsed is skipped rather than failing the attach: the event log is a
/// record, not a source of truth, and a truncated last line is exactly what a crash leaves behind.
pub fn recorded_events(home: &Path, job_id: &str) -> Vec<JobEvent> {
    let path = home.join("jobs").join(job_id).join("events.jsonl");
    let Ok(content) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut events = content
        .lines()
        .filter_map(|line| serde_json::from_str::<JobEvent>(line).ok())
        .collect::<Vec<_>>();
    if events.len() > MAX_REPLAYED_EVENTS {
        events.drain(..events.len() - MAX_REPLAYED_EVENTS);
    }
    events
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        ATTACH_CHANNEL_CAPACITY, JobEventBroadcaster, MAX_REPLAYED_EVENTS, ReplayGuard,
        recorded_events,
    };
    use crate::orchestrator::{
        events::{EventSink, JobEvent, JobEventKind, JsonlEventSink},
        supervisor::Project,
    };

    static NEXT_HOME: AtomicUsize = AtomicUsize::new(0);

    fn home() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lya-attach-{}-{}",
            std::process::id(),
            NEXT_HOME.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("home should be created");
        path
    }

    fn event(job_id: &str, message: &str) -> JobEvent {
        JobEvent::new(
            job_id,
            &Project {
                name: "project".to_owned(),
                path: std::path::PathBuf::from("/work/project"),
            },
            Some(1),
            JobEventKind::ControlMessage {
                message: message.to_owned(),
            },
        )
    }

    #[tokio::test]
    async fn an_attached_client_sees_the_events_of_its_own_job_only() {
        let broadcaster = JobEventBroadcaster::new();
        let mut watched = broadcaster.subscribe("job-1");
        let mut other = broadcaster.subscribe("job-2");

        broadcaster
            .sink("job-1")
            .emit(&event("job-1", "first"))
            .expect("emit should work");

        let received = watched.recv().await.expect("the event should arrive");
        assert_eq!(received.job_id, "job-1");
        assert!(
            other.try_recv().is_err(),
            "another job's stream must stay empty"
        );
    }

    /// The guarantee that makes attach safe: emitting does not depend on anyone listening.
    #[tokio::test]
    async fn emitting_succeeds_with_no_viewers_and_after_every_viewer_leaves() {
        let broadcaster = JobEventBroadcaster::new();
        let sink = broadcaster.sink("job-1");

        assert!(sink.emit(&event("job-1", "nobody watching")).is_ok());

        let viewer = broadcaster.subscribe("job-1");
        assert_eq!(broadcaster.attached_clients(), 1);
        drop(viewer);

        assert_eq!(broadcaster.attached_clients(), 0);
        assert!(
            sink.emit(&event("job-1", "viewer left")).is_ok(),
            "a detached viewer must never make a job fail"
        );
    }

    /// A client that stops reading is disconnected by itself; the job keeps emitting.
    #[tokio::test]
    async fn a_viewer_that_falls_behind_is_told_so_without_affecting_the_job() {
        let broadcaster = JobEventBroadcaster::new();
        let mut slow = broadcaster.subscribe("job-1");
        let sink = broadcaster.sink("job-1");

        for index in 0..ATTACH_CHANNEL_CAPACITY + 8 {
            sink.emit(&event("job-1", &format!("event {index}")))
                .expect("emitting must never fail because a viewer is slow");
        }

        let error = slow
            .recv()
            .await
            .expect_err("a viewer this far behind cannot be served");
        assert!(matches!(
            error,
            tokio::sync::broadcast::error::RecvError::Lagged(_)
        ));
    }

    /// A sequential chain is one piece of work with several job identities. It can be watched from
    /// where it started, and each job of it can also be watched by its own name.
    #[tokio::test]
    async fn a_sequential_child_reaches_both_its_chain_and_its_own_stream() {
        let broadcaster = JobEventBroadcaster::new();
        let mut chain = broadcaster.subscribe("job-parent");
        let mut child = broadcaster.subscribe("job-child");
        // The sink belongs to the job the driver was handed; the chain's children emit through it.
        let sink = broadcaster.sink("job-parent");

        sink.emit(&event("job-child", "child event"))
            .expect("emit should work");

        assert_eq!(
            chain.recv().await.expect("the chain sees its child").job_id,
            "job-child"
        );
        assert_eq!(
            child
                .recv()
                .await
                .expect("the child is watchable by its own name")
                .job_id,
            "job-child"
        );

        // The job the driver was handed is not delivered twice to its own stream.
        sink.emit(&event("job-parent", "parent event"))
            .expect("emit should work");
        assert_eq!(
            chain.recv().await.expect("the parent event arrives").job_id,
            "job-parent"
        );
        assert!(
            chain.try_recv().is_err(),
            "one event reaches one stream once"
        );
    }

    #[tokio::test]
    async fn releasing_a_job_ends_its_streams() {
        let broadcaster = JobEventBroadcaster::new();
        let mut viewer = broadcaster.subscribe("job-1");

        broadcaster.release("job-1");

        assert!(
            viewer.recv().await.is_err(),
            "a released job's stream is closed"
        );
        assert!(broadcaster.streaming_job_ids().is_empty());
    }

    #[test]
    fn a_replay_reads_recorded_history_and_survives_a_corrupt_tail() {
        let home = home();
        let sink = JsonlEventSink::for_job(&home, "job-1");
        sink.emit(&event("job-1", "first")).expect("emit");
        sink.emit(&event("job-1", "second")).expect("emit");
        // Exactly what a crash mid-write leaves behind.
        fs::OpenOptions::new()
            .append(true)
            .open(sink.path())
            .and_then(|mut file| std::io::Write::write_all(&mut file, b"{\"timestamp_un"))
            .expect("a truncated line should be appendable");

        let replayed = recorded_events(&home, "job-1");

        assert_eq!(replayed.len(), 2);
        assert!(matches!(
            &replayed[0].kind,
            crate::orchestrator::events::JobEventKind::ControlMessage { message } if message == "first"
        ));
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn a_replay_is_bounded_and_keeps_the_most_recent_events() {
        let home = home();
        let sink = JsonlEventSink::for_job(&home, "job-1");
        for index in 0..MAX_REPLAYED_EVENTS + 10 {
            sink.emit(&event("job-1", &format!("event {index}")))
                .expect("emit");
        }

        let replayed = recorded_events(&home, "job-1");

        assert_eq!(replayed.len(), MAX_REPLAYED_EVENTS);
        let last = replayed.last().expect("a replay has a last event");
        assert!(matches!(
            &last.kind,
            crate::orchestrator::events::JobEventKind::ControlMessage { message }
                if message == &format!("event {}", MAX_REPLAYED_EVENTS + 9)
        ));
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn a_replayed_event_is_shown_once_even_when_it_also_arrives_live() {
        let mut guard = ReplayGuard::new();
        let replayed = event("job-1", "already shown");
        guard.record(&replayed);

        assert!(
            guard.already_shown(&replayed),
            "an event the replay showed must not be repeated by the live stream"
        );
        assert!(
            !guard.already_shown(&replayed),
            "a genuinely repeated event is shown again"
        );
        assert!(!guard.already_shown(&event("job-1", "new")));
    }

    #[test]
    fn a_missing_event_log_replays_nothing_rather_than_failing() {
        let home = home();

        assert!(recorded_events(&home, "never-ran").is_empty());
        let _ = fs::remove_dir_all(home);
    }
}
