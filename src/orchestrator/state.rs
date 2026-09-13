use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use super::{
    home::LyaHome,
    publisher::{GitPublishConfig, PublishResult, PublishStage},
    repository::RepositoryState,
    supervisor::SupervisorDecision,
};

static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);

pub const DEFAULT_MAX_ITERATIONS: u32 = 10;
pub const DEFAULT_MAX_JOBS: u32 = 10;

/// Upper bounds for active `/send` instructions. Instructions stay active for the rest of the
/// current job, so they are bounded explicitly instead of growing without limit. Exceeding either
/// bound is reported to the user; an instruction is never dropped silently.
pub const MAX_ACTIVE_USER_INSTRUCTIONS: usize = 16;
pub const MAX_ACTIVE_USER_INSTRUCTION_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    /// Accepted by the scheduler and durably recorded, but never started.
    ///
    /// A queued job owns a real job ID, directory and `state.json` before any work begins, so a
    /// scheduler that dies never silently loses accepted work. It is deliberately *not* resumable:
    /// only the scheduler starts queued work, so `lya resume` cannot pick one up and bypass
    /// repository exclusion or the concurrency bound.
    #[serde(rename = "QUEUED")]
    Queued,
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "PAUSED")]
    Paused,
    #[serde(rename = "WAITING_CLAUDE_QUOTA")]
    WaitingClaudeQuota,
    #[serde(rename = "WAITING_OPENAI_QUOTA")]
    WaitingOpenAiQuota,
    #[serde(rename = "WAITING_HUMAN")]
    WaitingHuman,
    #[serde(rename = "ACCEPTED")]
    Accepted,
    #[serde(rename = "PUBLISHING")]
    Publishing,
    #[serde(rename = "PUBLISHED")]
    Published,
    #[serde(rename = "FAILED")]
    Failed,
    #[serde(rename = "STOPPED")]
    Stopped,
}

impl JobStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
            Self::WaitingClaudeQuota => "WAITING_CLAUDE_QUOTA",
            Self::WaitingOpenAiQuota => "WAITING_OPENAI_QUOTA",
            Self::WaitingHuman => "WAITING_HUMAN",
            Self::Accepted => "ACCEPTED",
            Self::Publishing => "PUBLISHING",
            Self::Published => "PUBLISHED",
            Self::Failed => "FAILED",
            Self::Stopped => "STOPPED",
        }
    }

    /// Whether this terminal status counts as a successful outcome.
    ///
    /// One definition for every surface: a single `lya run`, a scheduled job and a daemon-driven job
    /// must never disagree about whether the same status was a success. A job that is still
    /// `RUNNING`, or one that was accepted into a queue and never started, is not an outcome at all
    /// and is therefore not a successful one.
    pub fn is_successful_outcome(&self) -> bool {
        matches!(
            self,
            Self::Accepted
                | Self::Published
                | Self::Paused
                | Self::WaitingHuman
                | Self::WaitingClaudeQuota
                | Self::WaitingOpenAiQuota
                | Self::Stopped
        )
    }

    /// A job that a later Lya process may continue. `FAILED`, `STOPPED`, `PUBLISHED`, `ACCEPTED`
    /// and `WAITING_HUMAN` are deliberately excluded: they are terminal for automatic recovery and
    /// need an explicit human decision instead. `QUEUED` is excluded too: it was never started, so
    /// there is nothing to continue, and starting it belongs to the scheduler that owns the
    /// repository claim and the concurrency bound.
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            Self::Running
                | Self::Paused
                | Self::Publishing
                | Self::WaitingClaudeQuota
                | Self::WaitingOpenAiQuota
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPhase {
    #[serde(rename = "SUPERVISOR")]
    Supervisor,
    #[serde(rename = "EXECUTOR")]
    Executor,
    #[serde(rename = "PUBLISHER")]
    Publisher,
}

impl JobPhase {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Supervisor => "SUPERVISOR",
            Self::Executor => "EXECUTOR",
            Self::Publisher => "PUBLISHER",
        }
    }
}

/// The single external action a job still owes. It is persisted before the action starts and
/// cleared once the action is durably recorded, so a later process knows exactly where to continue
/// without replaying a completed provider call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingOperation {
    #[serde(rename = "SUPERVISOR_REVIEW")]
    SupervisorReview,
    #[serde(rename = "EXECUTOR_RUN")]
    ExecutorRun,
    #[serde(rename = "PUBLICATION")]
    Publication,
}

impl PendingOperation {
    pub fn label(&self) -> &'static str {
        match self {
            Self::SupervisorReview => "SUPERVISOR_REVIEW",
            Self::ExecutorRun => "EXECUTOR_RUN",
            Self::Publication => "PUBLICATION",
        }
    }
}

/// How a quota condition was recognised. Structured provider information is preferred; the message
/// heuristic is an explicit, recorded fallback rather than a silent guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaSource {
    #[serde(rename = "PROVIDER_STRUCTURED")]
    ProviderStructured,
    #[serde(rename = "PROVIDER_MESSAGE_HEURISTIC")]
    ProviderMessageHeuristic,
}

impl QuotaSource {
    pub fn label(&self) -> &'static str {
        match self {
            Self::ProviderStructured => "PROVIDER_STRUCTURED",
            Self::ProviderMessageHeuristic => "PROVIDER_MESSAGE_HEURISTIC",
        }
    }
}

/// Explicit fallback used only where a provider exposes no documented structured signal. It is
/// always recorded as [`QuotaSource::ProviderMessageHeuristic`] so a quota decision never looks
/// more precise than it really is.
pub fn message_indicates_quota(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("quota")
        || text.contains("rate limit")
        || text.contains("rate_limit")
        || text.contains("usage limit")
        || text.contains("too many requests")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaWait {
    pub provider: String,
    pub operation: PendingOperation,
    pub source: QuotaSource,
    pub reason: String,
    pub detected_unix_seconds: u64,
}

/// Everything needed to continue a run correctly in a new process. It never contains secrets: Git
/// push authentication stays with the machine's own Git configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunConfiguration {
    pub max_iterations: u32,
    pub max_jobs: u32,
    pub browser: bool,
    pub publish: bool,
    pub git: Option<GitPublishConfig>,
}

impl Default for RunConfiguration {
    fn default() -> Self {
        Self {
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_jobs: DEFAULT_MAX_JOBS,
            browser: false,
            publish: false,
            git: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobState {
    pub job_id: String,
    pub project_name: String,
    pub project_path: PathBuf,
    pub task: String,
    pub status: JobStatus,
    pub iteration: u32,
    pub phase: JobPhase,
    pub claude_session_id: Option<String>,
    pub last_executor_report: Option<String>,
    pub last_supervisor_decision: Option<SupervisorDecision>,
    pub accepted_repository_state: Option<RepositoryState>,
    pub publish_result: Option<PublishResult>,
    pub publish_stage: Option<PublishStage>,
    #[serde(default)]
    pub pending_user_instructions: Vec<String>,
    #[serde(default)]
    pub applied_user_instructions: Vec<String>,
    #[serde(default)]
    pub run: RunConfiguration,
    #[serde(default)]
    pub pending_operation: Option<PendingOperation>,
    #[serde(default)]
    pub quota_wait: Option<QuotaWait>,
    #[serde(default)]
    pub last_repository_state: Option<RepositoryState>,
    #[serde(default)]
    pub sequential_index: u32,
    pub created_unix_seconds: u64,
    pub last_updated_unix_seconds: u64,
}

impl JobState {
    pub fn new(
        job_id: impl Into<String>,
        project_name: impl Into<String>,
        project_path: PathBuf,
        task: impl Into<String>,
    ) -> Self {
        let now = current_unix_seconds();
        Self {
            job_id: job_id.into(),
            project_name: project_name.into(),
            project_path,
            task: task.into(),
            status: JobStatus::Running,
            iteration: 0,
            phase: JobPhase::Supervisor,
            claude_session_id: None,
            last_executor_report: None,
            last_supervisor_decision: None,
            accepted_repository_state: None,
            publish_result: None,
            publish_stage: None,
            pending_user_instructions: Vec::new(),
            applied_user_instructions: Vec::new(),
            run: RunConfiguration::default(),
            pending_operation: None,
            quota_wait: None,
            last_repository_state: None,
            sequential_index: 0,
            created_unix_seconds: now,
            last_updated_unix_seconds: now,
        }
    }

    pub fn touch(&mut self) {
        self.last_updated_unix_seconds = current_unix_seconds();
    }

    /// Total serialized size of the instructions that are active for the rest of this job.
    pub fn active_instruction_bytes(&self) -> usize {
        self.applied_user_instructions
            .iter()
            .chain(self.pending_user_instructions.iter())
            .map(String::len)
            .sum()
    }

    pub fn active_instruction_count(&self) -> usize {
        self.applied_user_instructions.len() + self.pending_user_instructions.len()
    }
}

/// Legacy single-file layout. Retained so existing `LYA_HOME/state.json` files can be migrated
/// into the per-job layout without losing anything.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrchestratorState {
    pub jobs: BTreeMap<String, JobState>,
}

impl OrchestratorState {
    pub fn upsert(&mut self, job: JobState) {
        self.jobs.insert(job.job_id.clone(), job);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyMigration {
    pub migrated: Vec<String>,
    pub kept_existing: Vec<String>,
    pub archived_legacy_path: Option<PathBuf>,
}

impl LegacyMigration {
    pub fn is_empty(&self) -> bool {
        self.migrated.is_empty() && self.kept_existing.is_empty()
    }
}

/// One entry of the persisted job directory.
///
/// A job whose `state.json` is corrupt or unreadable keeps its place in the listing as an explicit
/// error instead of disappearing from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEntry {
    Loaded(Box<JobState>),
    Unreadable { job_id: String, error: String },
}

/// Per-job persistent state under `LYA_HOME/jobs/<job-id>/state.json`.
///
/// Every job owns its own file so two Lya processes working on different jobs never rewrite each
/// other's state. Writes are atomic (unique temporary file, `sync_all`, rename).
pub struct StateStore {
    root: PathBuf,
}

impl StateStore {
    pub fn new(home: &LyaHome) -> Self {
        Self::at(home.path())
    }

    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn jobs_directory(&self) -> PathBuf {
        self.root.join("jobs")
    }

    /// Where repository-level claims live. One file per canonical repository, shared by every Lya
    /// process using this home.
    pub fn repositories_directory(&self) -> PathBuf {
        self.root.join("repositories")
    }

    pub fn job_directory(&self, job_id: &str) -> Result<PathBuf, StateError> {
        validate_job_id(job_id)?;
        Ok(self.jobs_directory().join(job_id))
    }

    pub fn job_state_path(&self, job_id: &str) -> Result<PathBuf, StateError> {
        Ok(self.job_directory(job_id)?.join("state.json"))
    }

    pub fn legacy_state_path(&self) -> PathBuf {
        self.root.join("state.json")
    }

    pub fn save_job(&self, job: &JobState) -> Result<(), StateError> {
        let path = self.job_state_path(&job.job_id)?;
        let parent = self
            .job_directory(&job.job_id)
            .expect("job ID was validated");
        fs::create_dir_all(&parent).map_err(|error| StateError::Write(error.to_string()))?;
        let content = serde_json::to_vec_pretty(job)
            .map_err(|error| StateError::Serialize(error.to_string()))?;
        write_atomically(&path, &content)
    }

    pub fn load_job(&self, job_id: &str) -> Result<Option<JobState>, StateError> {
        let path = self.job_state_path(job_id)?;
        read_job(&path)
    }

    /// Every persisted job, ordered by job ID. Unreadable directories are reported instead of
    /// being skipped silently.
    pub fn load_all(&self) -> Result<Vec<JobState>, StateError> {
        let directory = self.jobs_directory();
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(StateError::Read(error.to_string())),
        };
        let mut jobs = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|error| StateError::Read(error.to_string()))?;
            if !entry.path().is_dir() {
                continue;
            }
            let Some(job) = read_job(&entry.path().join("state.json"))? else {
                continue;
            };
            jobs.insert(job.job_id.clone(), job);
        }
        Ok(jobs.into_values().collect())
    }

    /// Every entry in the persisted job directory, ordered by job ID.
    ///
    /// Only a failure to enumerate the directory itself is an error here: a single job whose
    /// `state.json` cannot be read is reported as [`JobEntry::Unreadable`] instead of hiding every
    /// healthy job behind it. Nothing is written, moved or repaired.
    pub fn load_all_entries(&self) -> Result<Vec<JobEntry>, StateError> {
        let directory = self.jobs_directory();
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(StateError::Read(error.to_string())),
        };
        let mut found = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|error| StateError::Read(error.to_string()))?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let directory_name = entry.file_name().to_string_lossy().into_owned();
            match read_job(&path.join("state.json")) {
                Ok(Some(job)) => {
                    found.insert(job.job_id.clone(), JobEntry::Loaded(Box::new(job)));
                }
                Ok(None) => {}
                Err(error) => {
                    found.insert(
                        directory_name.clone(),
                        JobEntry::Unreadable {
                            job_id: directory_name,
                            error: error.to_string(),
                        },
                    );
                }
            }
        }
        Ok(found.into_values().collect())
    }

    /// Move an existing `LYA_HOME/state.json` into the per-job layout. A per-job file that already
    /// exists always wins; the legacy file is archived rather than deleted.
    pub fn migrate_legacy(&self) -> Result<LegacyMigration, StateError> {
        let legacy_path = self.legacy_state_path();
        let content = match fs::read_to_string(&legacy_path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(LegacyMigration::default());
            }
            Err(error) => return Err(StateError::Read(error.to_string())),
        };
        let legacy: OrchestratorState = serde_json::from_str(&content)
            .map_err(|error| StateError::InvalidJson(error.to_string()))?;
        let mut report = LegacyMigration::default();
        for (job_id, job) in legacy.jobs {
            if self.load_job(&job_id)?.is_some() {
                report.kept_existing.push(job_id);
                continue;
            }
            self.save_job(&job)?;
            report.migrated.push(job_id);
        }
        let archived =
            legacy_path.with_file_name(format!("state.json.migrated-{}", current_unix_seconds()));
        fs::rename(&legacy_path, &archived)
            .map_err(|error| StateError::Write(error.to_string()))?;
        report.archived_legacy_path = Some(archived);
        Ok(report)
    }
}

fn read_job(path: &Path) -> Result<Option<JobState>, StateError> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(StateError::Read(error.to_string())),
    };
    serde_json::from_str(&content)
        .map(Some)
        .map_err(|error| StateError::InvalidJson(error.to_string()))
}

fn write_atomically(path: &Path, content: &[u8]) -> Result<(), StateError> {
    let temporary = temporary_path(path);
    let write_result = (|| -> Result<(), StateError> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| StateError::Write(error.to_string()))?;
        file.write_all(content)
            .map_err(|error| StateError::Write(error.to_string()))?;
        file.sync_all()
            .map_err(|error| StateError::Write(error.to_string()))?;
        drop(file);
        fs::rename(&temporary, path).map_err(|error| StateError::Write(error.to_string()))
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

/// A job ID becomes a directory name, so it must stay a single, boring path segment.
pub fn validate_job_id(job_id: &str) -> Result<(), StateError> {
    if job_id.is_empty() || job_id.len() > 128 {
        return Err(StateError::InvalidJobId(job_id.to_owned()));
    }
    if job_id == "." || job_id == ".." {
        return Err(StateError::InvalidJobId(job_id.to_owned()));
    }
    if !job_id
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        return Err(StateError::InvalidJobId(job_id.to_owned()));
    }
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
    path.with_file_name(format!(".state-{}-{sequence}.tmp", std::process::id()))
}

pub fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug)]
pub enum StateError {
    Read(String),
    Serialize(String),
    InvalidJson(String),
    InvalidJobId(String),
    Write(String),
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "could not read state: {error}"),
            Self::Serialize(error) => write!(formatter, "could not serialize state: {error}"),
            Self::InvalidJson(error) => write!(formatter, "state contains invalid JSON: {error}"),
            Self::InvalidJobId(job_id) => write!(formatter, "unsafe job ID: {job_id}"),
            Self::Write(error) => write!(formatter, "could not write state: {error}"),
        }
    }
}

impl Error for StateError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        JobState, JobStatus, OrchestratorState, StateStore, current_unix_seconds, validate_job_id,
    };
    use crate::orchestrator::home::LyaHome;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn home() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-state-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("home directory should be created");
        directory
    }

    fn job(job_id: &str, status: JobStatus) -> JobState {
        let mut job = JobState::new(job_id, "Sandbox", std::env::temp_dir(), "validate change");
        job.status = status;
        job.iteration = 2;
        job.created_unix_seconds = 120;
        job.last_updated_unix_seconds = 123;
        job
    }

    #[test]
    fn missing_state_loads_as_empty_initial_state() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));

        assert_eq!(store.load_all().expect("missing state should load"), vec![]);
        assert_eq!(store.load_job("absent").expect("missing job"), None);
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn saves_and_loads_state_per_job() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let first = job("job-one", JobStatus::Running);
        let second = job("job-two", JobStatus::Paused);

        store.save_job(&first).expect("first job should save");
        store.save_job(&second).expect("second job should save");

        assert_eq!(
            store.load_job("job-one").expect("job should load"),
            Some(first.clone())
        );
        assert_eq!(
            store.load_all().expect("jobs should load"),
            vec![first, second]
        );
        assert!(store.job_state_path("job-one").expect("path").is_file());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn save_replaces_existing_job_without_touching_other_jobs() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let other = job("job-other", JobStatus::Running);
        store.save_job(&other).expect("other job should save");
        store
            .save_job(&job("job-one", JobStatus::Running))
            .expect("first state should save");
        let replacement = job("job-one", JobStatus::Stopped);

        store
            .save_job(&replacement)
            .expect("replacement should save");

        assert_eq!(
            store.load_job("job-one").expect("state should load"),
            Some(replacement)
        );
        assert_eq!(
            store.load_job("job-other").expect("state should load"),
            Some(other)
        );
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn reports_invalid_json() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let path = store.job_state_path("broken").expect("path");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory should be created");
        fs::write(&path, "not JSON").expect("state should be written");

        let error = store
            .load_job("broken")
            .expect_err("invalid JSON should fail");

        assert!(error.to_string().contains("invalid JSON"));
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn entry_listing_reports_a_corrupt_job_without_hiding_the_healthy_ones() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        store
            .save_job(&job("healthy", JobStatus::Paused))
            .expect("healthy job should save");
        let broken = store.job_state_path("broken").expect("path");
        fs::create_dir_all(broken.parent().expect("parent")).expect("directory should be created");
        fs::write(&broken, "not JSON").expect("corrupt state should be written");

        let entries = store.load_all_entries().expect("entries should load");

        assert_eq!(entries.len(), 2);
        let super::JobEntry::Unreadable { job_id, error } = &entries[0] else {
            panic!("the corrupt job should be reported as unreadable");
        };
        assert_eq!(job_id, "broken");
        assert!(error.contains("invalid JSON"));
        assert!(matches!(&entries[1], super::JobEntry::Loaded(job) if job.job_id == "healthy"));
        // The strict loader keeps its existing all-or-nothing contract.
        assert!(store.load_all().is_err());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn rejects_unsafe_job_identifiers() {
        assert!(validate_job_id("job-1").is_ok());
        assert!(validate_job_id("..").is_err());
        assert!(validate_job_id("a/b").is_err());
        assert!(validate_job_id("a\\b").is_err());
        assert!(validate_job_id("").is_err());
    }

    #[test]
    fn migrates_legacy_state_file_without_losing_jobs() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let mut legacy = OrchestratorState::default();
        legacy.upsert(job("legacy-one", JobStatus::Paused));
        legacy.upsert(job("legacy-two", JobStatus::WaitingClaudeQuota));
        fs::write(
            store.legacy_state_path(),
            serde_json::to_vec_pretty(&legacy).expect("legacy state should serialize"),
        )
        .expect("legacy state should be written");

        let report = store.migrate_legacy().expect("migration should succeed");

        assert_eq!(report.migrated, vec!["legacy-one", "legacy-two"]);
        assert!(report.kept_existing.is_empty());
        assert_eq!(
            store
                .load_job("legacy-one")
                .expect("job should load")
                .map(|job| job.status),
            Some(JobStatus::Paused)
        );
        assert!(!store.legacy_state_path().exists());
        assert!(
            report
                .archived_legacy_path
                .as_ref()
                .expect("legacy file should be archived")
                .is_file()
        );
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn migration_never_overwrites_an_existing_per_job_state() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let current = job("shared", JobStatus::Paused);
        store.save_job(&current).expect("current job should save");
        let mut legacy = OrchestratorState::default();
        legacy.upsert(job("shared", JobStatus::Failed));
        fs::write(
            store.legacy_state_path(),
            serde_json::to_vec_pretty(&legacy).expect("legacy state should serialize"),
        )
        .expect("legacy state should be written");

        let report = store.migrate_legacy().expect("migration should succeed");

        assert_eq!(report.kept_existing, vec!["shared"]);
        assert!(report.migrated.is_empty());
        assert_eq!(
            store.load_job("shared").expect("job should load"),
            Some(current)
        );
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn legacy_job_state_without_milestone_eight_fields_still_loads() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let path = store.job_state_path("old-job").expect("path");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory should be created");
        let legacy_json = serde_json::json!({
            "job_id": "old-job",
            "project_name": "Sandbox",
            "project_path": std::env::temp_dir(),
            "task": "older task",
            "status": "PAUSED",
            "iteration": 3,
            "phase": "SUPERVISOR",
            "claude_session_id": "session-9",
            "last_executor_report": null,
            "last_supervisor_decision": null,
            "accepted_repository_state": null,
            "publish_result": null,
            "publish_stage": null,
            "created_unix_seconds": 10,
            "last_updated_unix_seconds": 20
        });
        fs::write(
            &path,
            serde_json::to_vec_pretty(&legacy_json).expect("legacy job should serialize"),
        )
        .expect("legacy job should be written");

        let loaded = store
            .load_job("old-job")
            .expect("legacy job should load")
            .expect("legacy job should exist");

        assert_eq!(loaded.status, JobStatus::Paused);
        assert_eq!(loaded.run, super::RunConfiguration::default());
        assert_eq!(loaded.pending_operation, None);
        assert!(loaded.applied_user_instructions.is_empty());
        assert!(loaded.last_repository_state.is_none());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn tracks_active_instruction_budget() {
        let mut state = job("budget", JobStatus::Running);
        state.applied_user_instructions.push("abc".to_owned());
        state.pending_user_instructions.push("de".to_owned());

        assert_eq!(state.active_instruction_count(), 2);
        assert_eq!(state.active_instruction_bytes(), 5);
    }

    /// Queued work is durable, distinguishable and deliberately outside `lya resume`.
    #[test]
    fn queued_jobs_round_trip_and_are_never_resumable() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let queued = job("job-queued", JobStatus::Queued);

        store.save_job(&queued).expect("queued job should save");

        let loaded = store
            .load_job("job-queued")
            .expect("queued job should load")
            .expect("queued job should exist");
        assert_eq!(loaded.status, JobStatus::Queued);
        assert_eq!(loaded.status.label(), "QUEUED");
        assert!(
            !loaded.status.is_resumable(),
            "lya resume must never start scheduler-owned queued work"
        );
        let rendered = serde_json::to_string(&queued).expect("queued job should serialize");
        assert!(rendered.contains("\"QUEUED\""));
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn repository_claims_live_beside_jobs_in_the_home() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));

        assert_eq!(
            store.repositories_directory(),
            directory.join("repositories")
        );
        assert_ne!(store.repositories_directory(), store.jobs_directory());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn current_time_is_monotonic_enough_for_state_timestamps() {
        assert!(current_unix_seconds() > 1_600_000_000);
    }
}
