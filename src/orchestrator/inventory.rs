//! Read-only inventory of persisted jobs.
//!
//! This module answers a question; it never drives a job. It reads authoritative per-job state,
//! asks the resume logic itself whether a job could be continued, and reports jobs it could not
//! read. It never writes, locks, repairs, or migrates anything, and it never contacts a provider
//! or Git.

use std::path::PathBuf;

use serde_json::{Value, json};

use super::{
    resume::ResumePlan,
    state::{JobEntry, JobPhase, JobState, JobStatus, StateError, StateStore},
};

/// What `lya resume` would do with a persisted job.
///
/// The verdict comes from [`ResumePlan::for_job`], the same authority `lya resume` uses, so the
/// listing and an actual resume cannot silently disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resumability {
    Resumable { continuation: &'static str },
    Blocked { reason: String },
}

impl Resumability {
    pub fn of(job: &JobState) -> Self {
        match ResumePlan::for_job(job) {
            Ok(plan) => Self::Resumable {
                continuation: plan.continuation.label(),
            },
            Err(rejection) => Self::Blocked {
                reason: rejection.to_string(),
            },
        }
    }

    pub fn is_resumable(&self) -> bool {
        matches!(self, Self::Resumable { .. })
    }
}

/// The displayable facts of one persisted job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSummary {
    pub job_id: String,
    pub project_name: String,
    pub status: JobStatus,
    pub phase: JobPhase,
    pub iteration: u32,
    pub created_unix_seconds: u64,
    pub last_updated_unix_seconds: u64,
    pub resumability: Resumability,
}

impl JobSummary {
    fn from_state(job: &JobState) -> Self {
        Self {
            job_id: job.job_id.clone(),
            project_name: job.project_name.clone(),
            status: job.status.clone(),
            phase: job.phase.clone(),
            iteration: job.iteration,
            created_unix_seconds: job.created_unix_seconds,
            last_updated_unix_seconds: job.last_updated_unix_seconds,
            resumability: Resumability::of(job),
        }
    }

    fn to_json(&self) -> Value {
        let (resumable, continuation, blocked_reason) = match &self.resumability {
            Resumability::Resumable { continuation } => {
                (true, Value::from(*continuation), Value::Null)
            }
            Resumability::Blocked { reason } => (false, Value::Null, Value::from(reason.as_str())),
        };
        json!({
            "job_id": self.job_id,
            "project_name": self.project_name,
            "status": self.status.label(),
            "phase": self.phase.label(),
            "iteration": self.iteration,
            "created_unix_seconds": self.created_unix_seconds,
            "last_updated_unix_seconds": self.last_updated_unix_seconds,
            "resumable": resumable,
            "continuation": continuation,
            "blocked_reason": blocked_reason,
        })
    }
}

/// A persisted job directory whose state could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableJob {
    pub job_id: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInventory {
    /// Most recently updated first, then by job ID.
    pub jobs: Vec<JobSummary>,
    pub unreadable: Vec<UnreadableJob>,
    /// A legacy `LYA_HOME/state.json` still waiting for the migration `lya run`/`lya resume`
    /// performs. Listing jobs never migrates it.
    pub legacy_state_file: Option<PathBuf>,
}

impl JobInventory {
    pub fn collect(store: &StateStore) -> Result<Self, StateError> {
        let mut jobs = Vec::new();
        let mut unreadable = Vec::new();
        for entry in store.load_all_entries()? {
            match entry {
                JobEntry::Loaded(job) => jobs.push(JobSummary::from_state(&job)),
                JobEntry::Unreadable { job_id, error } => {
                    unreadable.push(UnreadableJob { job_id, error })
                }
            }
        }
        jobs.sort_by(|left, right| {
            right
                .last_updated_unix_seconds
                .cmp(&left.last_updated_unix_seconds)
                .then_with(|| left.job_id.cmp(&right.job_id))
        });
        unreadable.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        let legacy = store.legacy_state_path();

        Ok(Self {
            jobs,
            unreadable,
            legacy_state_file: legacy.is_file().then_some(legacy),
        })
    }

    /// Keep only the jobs the resume logic would actually continue. Unreadable entries are kept:
    /// a job that cannot be read cannot be declared irrelevant either.
    pub fn only_resumable(mut self) -> Self {
        self.jobs.retain(|job| job.resumability.is_resumable());
        self
    }

    pub fn to_json(&self) -> Value {
        json!({
            "jobs": self.jobs.iter().map(JobSummary::to_json).collect::<Vec<_>>(),
            "unreadable": self
                .unreadable
                .iter()
                .map(|job| json!({ "job_id": job.job_id, "error": job.error }))
                .collect::<Vec<_>>(),
            "legacy_state_file": self
                .legacy_state_file
                .as_ref()
                .map(|path| path.display().to_string()),
        })
    }

    /// Human-readable listing. `now` is the reference point for the relative update times.
    pub fn render(&self, now: u64) -> String {
        let mut lines = Vec::new();
        if self.jobs.is_empty() {
            lines.push("No persisted jobs.".to_owned());
        } else {
            let header = [
                "JOB".to_owned(),
                "PROJECT".to_owned(),
                "STATUS".to_owned(),
                "PHASE".to_owned(),
                "ITER".to_owned(),
                "UPDATED".to_owned(),
                "RESUMABLE".to_owned(),
            ];
            let rows = self
                .jobs
                .iter()
                .map(|job| {
                    [
                        job.job_id.clone(),
                        job.project_name.clone(),
                        job.status.label().to_owned(),
                        job.phase.label().to_owned(),
                        job.iteration.to_string(),
                        render_age(now, job.last_updated_unix_seconds),
                        if job.resumability.is_resumable() {
                            "yes".to_owned()
                        } else {
                            "no".to_owned()
                        },
                    ]
                })
                .collect::<Vec<_>>();
            let widths = column_widths(&header, &rows);
            lines.push(render_row(&header, &widths));
            lines.extend(rows.iter().map(|row| render_row(row, &widths)));
        }

        // A job whose status looks resumable but whose persisted state is inconsistent is the one
        // surprising case, so it gets its exact reason instead of a bare "no".
        let blocked = self
            .jobs
            .iter()
            .filter(|job| job.status.is_resumable() && !job.resumability.is_resumable())
            .collect::<Vec<_>>();
        if !blocked.is_empty() {
            lines.push(String::new());
            lines.push("Not resumable despite a resumable status:".to_owned());
            lines.extend(blocked.iter().map(|job| {
                let Resumability::Blocked { reason } = &job.resumability else {
                    unreachable!("a blocked job carries its reason");
                };
                format!("  {}  {reason}", job.job_id)
            }));
        }

        if !self.unreadable.is_empty() {
            lines.push(String::new());
            lines.push(format!(
                "{} job(s) could not be read:",
                self.unreadable.len()
            ));
            lines.extend(
                self.unreadable
                    .iter()
                    .map(|job| format!("  {}  ERROR  {}", job.job_id, job.error)),
            );
            lines.push("Lya did not modify them. Inspect or move them manually.".to_owned());
        }

        if let Some(path) = &self.legacy_state_file {
            lines.push(String::new());
            lines.push(format!(
                "A legacy {} still holds jobs; `lya run` or `lya resume` migrates it.",
                path.display()
            ));
        }
        lines.join("\n")
    }
}

fn column_widths<const N: usize>(header: &[String; N], rows: &[[String; N]]) -> [usize; N] {
    let mut widths: [usize; N] = std::array::from_fn(|index| header[index].chars().count());
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row.iter()) {
            *width = (*width).max(cell.chars().count());
        }
    }
    widths
}

fn render_row<const N: usize>(row: &[String; N], widths: &[usize; N]) -> String {
    row.iter()
        .zip(widths.iter())
        .map(|(cell, width)| format!("{cell:<width$}"))
        .collect::<Vec<_>>()
        .join("  ")
        .trim_end()
        .to_owned()
}

fn render_age(now: u64, timestamp: u64) -> String {
    let seconds = now.saturating_sub(timestamp);
    match seconds {
        0..60 => format!("{seconds}s ago"),
        60..3_600 => format!("{}m ago", seconds / 60),
        3_600..86_400 => format!("{}h ago", seconds / 3_600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{JobInventory, Resumability, render_age};
    use crate::orchestrator::{
        resume::resumable_jobs,
        state::{JobState, JobStatus, PendingOperation, StateStore},
    };

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn home() -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-inventory-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("home directory should be created");
        directory
    }

    fn job(job_id: &str, status: JobStatus, updated: u64) -> JobState {
        let mut job = JobState::new(
            job_id,
            "Sandbox",
            std::env::temp_dir(),
            "improve the sandbox",
        );
        job.status = status;
        job.iteration = 2;
        job.created_unix_seconds = 100;
        job.last_updated_unix_seconds = updated;
        job
    }

    fn write_corrupt_job(store: &StateStore, job_id: &str) {
        let path = store.job_state_path(job_id).expect("path");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory should be created");
        fs::write(&path, "{ not json").expect("corrupt state should be written");
    }

    /// Every file under `root`, with its exact bytes.
    fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();
        let mut pending = vec![root.to_owned()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).expect("directory should be readable") {
                let path = entry.expect("entry should be readable").path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    let content = fs::read(&path).expect("file should be readable");
                    files.insert(path, content);
                }
            }
        }
        files
    }

    #[test]
    fn lists_persisted_jobs_most_recently_updated_first() {
        let directory = home();
        let store = StateStore::at(&directory);
        store
            .save_job(&job("job-old", JobStatus::Paused, 100))
            .expect("save");
        store
            .save_job(&job("job-new", JobStatus::Running, 300))
            .expect("save");
        store
            .save_job(&job("job-middle", JobStatus::Published, 200))
            .expect("save");

        let inventory = JobInventory::collect(&store).expect("inventory should collect");

        assert_eq!(
            inventory
                .jobs
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-new", "job-middle", "job-old"]
        );
        assert_eq!(inventory.jobs[0].project_name, "Sandbox");
        assert_eq!(inventory.jobs[0].iteration, 2);
        assert_eq!(inventory.jobs[0].status, JobStatus::Running);
        assert!(inventory.unreadable.is_empty());
        assert_eq!(inventory.legacy_state_file, None);
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn equally_recent_jobs_keep_a_stable_order() {
        let directory = home();
        let store = StateStore::at(&directory);
        for job_id in ["job-c", "job-a", "job-b"] {
            store
                .save_job(&job(job_id, JobStatus::Paused, 500))
                .expect("save");
        }

        let first = JobInventory::collect(&store).expect("inventory should collect");
        let second = JobInventory::collect(&store).expect("inventory should collect again");

        assert_eq!(
            first
                .jobs
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-a", "job-b", "job-c"]
        );
        assert_eq!(first.jobs, second.jobs);
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn resumable_filtering_matches_the_resume_logic() {
        let directory = home();
        let store = StateStore::at(&directory);
        store
            .save_job(&job("job-paused", JobStatus::Paused, 400))
            .expect("save");
        store
            .save_job(&job("job-quota", JobStatus::WaitingClaudeQuota, 300))
            .expect("save");
        store
            .save_job(&job("job-published", JobStatus::Published, 200))
            .expect("save");

        let inventory = JobInventory::collect(&store)
            .expect("inventory should collect")
            .only_resumable();

        // A Claude quota wait must continue with an executor run; this fixture records none, so
        // the resume logic itself refuses it and the listing must agree.
        assert_eq!(
            inventory
                .jobs
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-paused"]
        );
        assert_eq!(
            inventory.jobs[0].resumability,
            Resumability::Resumable {
                continuation: "NEW_SUPERVISOR_ITERATION"
            }
        );
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn terminal_jobs_are_never_listed_as_resumable() {
        let directory = home();
        let store = StateStore::at(&directory);
        for (index, status) in [
            JobStatus::Published,
            JobStatus::Stopped,
            JobStatus::Failed,
            JobStatus::Accepted,
            JobStatus::WaitingHuman,
        ]
        .into_iter()
        .enumerate()
        {
            store
                .save_job(&job(&format!("job-{index}"), status, 100))
                .expect("save");
        }

        let inventory = JobInventory::collect(&store).expect("inventory should collect");

        assert_eq!(inventory.jobs.len(), 5);
        assert!(
            inventory
                .jobs
                .iter()
                .all(|job| !job.resumability.is_resumable())
        );
        assert!(inventory.only_resumable().jobs.is_empty());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn a_resumable_status_with_inconsistent_state_is_reported_with_its_reason() {
        let directory = home();
        let store = StateStore::at(&directory);
        let mut inconsistent = job("job-broken-state", JobStatus::Paused, 100);
        inconsistent.iteration = 0;
        inconsistent.pending_operation = Some(PendingOperation::SupervisorReview);
        store.save_job(&inconsistent).expect("save");

        let inventory = JobInventory::collect(&store).expect("inventory should collect");

        let Resumability::Blocked { reason } = &inventory.jobs[0].resumability else {
            panic!("an inconsistent job should not be resumable");
        };
        assert!(reason.contains("no iteration was counted"));
        assert!(
            inventory
                .render(500)
                .contains("Not resumable despite a resumable status:")
        );
        assert!(inventory.only_resumable().jobs.is_empty());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn a_corrupt_job_is_reported_and_healthy_jobs_still_list() {
        let directory = home();
        let store = StateStore::at(&directory);
        store
            .save_job(&job("job-healthy", JobStatus::Paused, 100))
            .expect("save");
        write_corrupt_job(&store, "job-corrupt");

        let inventory = JobInventory::collect(&store).expect("inventory should collect");

        assert_eq!(
            inventory
                .jobs
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-healthy"]
        );
        assert_eq!(inventory.unreadable.len(), 1);
        assert_eq!(inventory.unreadable[0].job_id, "job-corrupt");
        assert!(inventory.unreadable[0].error.contains("invalid JSON"));
        let rendered = inventory.render(100);
        assert!(rendered.contains("job-healthy"));
        assert!(rendered.contains("1 job(s) could not be read:"));

        // The resumable filter must not turn corruption into silence.
        let filtered = inventory.only_resumable();
        assert_eq!(filtered.unreadable.len(), 1);
        assert!(filtered.to_json()["unreadable"][0]["job_id"] == "job-corrupt");
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn json_output_is_valid_and_documents_each_job() {
        let directory = home();
        let store = StateStore::at(&directory);
        store
            .save_job(&job("job-paused", JobStatus::Paused, 400))
            .expect("save");
        store
            .save_job(&job("job-failed", JobStatus::Failed, 300))
            .expect("save");
        write_corrupt_job(&store, "job-corrupt");

        let inventory = JobInventory::collect(&store).expect("inventory should collect");
        let rendered =
            serde_json::to_string(&inventory.to_json()).expect("inventory should serialize");
        let parsed: serde_json::Value =
            serde_json::from_str(&rendered).expect("output should be valid JSON");

        assert_eq!(parsed["jobs"].as_array().expect("jobs array").len(), 2);
        assert_eq!(parsed["jobs"][0]["job_id"], "job-paused");
        assert_eq!(parsed["jobs"][0]["status"], "PAUSED");
        assert_eq!(parsed["jobs"][0]["phase"], "SUPERVISOR");
        assert_eq!(parsed["jobs"][0]["iteration"], 2);
        assert_eq!(parsed["jobs"][0]["last_updated_unix_seconds"], 400);
        assert_eq!(parsed["jobs"][0]["resumable"], true);
        assert_eq!(
            parsed["jobs"][0]["continuation"],
            "NEW_SUPERVISOR_ITERATION"
        );
        assert_eq!(parsed["jobs"][0]["blocked_reason"], serde_json::Value::Null);
        assert_eq!(parsed["jobs"][1]["resumable"], false);
        assert!(
            parsed["jobs"][1]["blocked_reason"]
                .as_str()
                .expect("a refused job explains itself")
                .contains("FAILED")
        );
        assert_eq!(parsed["unreadable"][0]["job_id"], "job-corrupt");
        assert_eq!(parsed["legacy_state_file"], serde_json::Value::Null);
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn collecting_the_inventory_never_changes_the_job_directory() {
        let directory = home();
        let store = StateStore::at(&directory);
        store
            .save_job(&job("job-healthy", JobStatus::Paused, 100))
            .expect("save");
        write_corrupt_job(&store, "job-corrupt");
        fs::write(store.legacy_state_path(), "{\"jobs\":{}}").expect("legacy state");
        let before = snapshot(&directory);

        let inventory = JobInventory::collect(&store).expect("inventory should collect");
        let _ = inventory.render(100);
        let _ = inventory.to_json();

        assert_eq!(snapshot(&directory), before);
        assert_eq!(inventory.legacy_state_file, Some(store.legacy_state_path()));
        assert!(inventory.render(100).contains("A legacy"));
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn the_listing_agrees_with_the_resume_candidate_set() {
        let directory = home();
        let store = StateStore::at(&directory);
        store
            .save_job(&job("job-paused", JobStatus::Paused, 400))
            .expect("save");
        store
            .save_job(&job("job-running", JobStatus::Running, 300))
            .expect("save");
        store
            .save_job(&job("job-stopped", JobStatus::Stopped, 200))
            .expect("save");

        let listed = JobInventory::collect(&store)
            .expect("inventory should collect")
            .only_resumable()
            .jobs
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>();
        let candidates = resumable_jobs(&store)
            .expect("resume candidates should load")
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>();

        assert_eq!(listed, candidates);
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn relative_update_times_stay_readable() {
        assert_eq!(render_age(100, 100), "0s ago");
        assert_eq!(render_age(159, 100), "59s ago");
        assert_eq!(render_age(400, 100), "5m ago");
        assert_eq!(render_age(7_500, 100), "2h ago");
        assert_eq!(render_age(200_000, 100), "2d ago");
        // A job written by a machine whose clock is ahead must not panic.
        assert_eq!(render_age(100, 900), "0s ago");
    }
}
