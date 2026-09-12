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

use super::{home::LyaHome, supervisor::SupervisorDecision};

static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "WAITING_CLAUDE_QUOTA")]
    WaitingClaudeQuota,
    #[serde(rename = "WAITING_OPENAI_QUOTA")]
    WaitingOpenAiQuota,
    #[serde(rename = "WAITING_HUMAN")]
    WaitingHuman,
    #[serde(rename = "ACCEPTED")]
    Accepted,
    #[serde(rename = "FAILED")]
    Failed,
    #[serde(rename = "STOPPED")]
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPhase {
    #[serde(rename = "SUPERVISOR")]
    Supervisor,
    #[serde(rename = "EXECUTOR")]
    Executor,
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
            created_unix_seconds: now,
            last_updated_unix_seconds: now,
        }
    }

    pub fn touch(&mut self) {
        self.last_updated_unix_seconds = current_unix_seconds();
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrchestratorState {
    pub jobs: BTreeMap<String, JobState>,
}

impl OrchestratorState {
    pub fn upsert(&mut self, job: JobState) {
        self.jobs.insert(job.job_id.clone(), job);
    }
}

pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    pub fn new(home: &LyaHome) -> Self {
        Self {
            path: home.state_path(),
        }
    }

    pub fn load(&self) -> Result<OrchestratorState, StateError> {
        let content = match fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(OrchestratorState::default());
            }
            Err(error) => return Err(StateError::Read(error.to_string())),
        };
        serde_json::from_str(&content).map_err(|error| StateError::InvalidJson(error.to_string()))
    }

    pub fn save(&self, state: &OrchestratorState) -> Result<(), StateError> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| StateError::Write("state path has no parent directory".to_owned()))?;
        fs::create_dir_all(parent).map_err(|error| StateError::Write(error.to_string()))?;
        let content = serde_json::to_vec_pretty(state)
            .map_err(|error| StateError::Serialize(error.to_string()))?;
        let temporary = temporary_path(&self.path);
        let write_result = (|| -> Result<(), StateError> {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| StateError::Write(error.to_string()))?;
            file.write_all(&content)
                .map_err(|error| StateError::Write(error.to_string()))?;
            file.sync_all()
                .map_err(|error| StateError::Write(error.to_string()))?;
            fs::rename(&temporary, &self.path).map_err(|error| StateError::Write(error.to_string()))
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        write_result
    }

    #[cfg(test)]
    fn path(&self) -> &Path {
        &self.path
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
    path.with_file_name(format!(".state-{}-{sequence}.tmp", std::process::id()))
}

fn current_unix_seconds() -> u64 {
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
    Write(String),
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "could not read state: {error}"),
            Self::Serialize(error) => write!(formatter, "could not serialize state: {error}"),
            Self::InvalidJson(error) => write!(formatter, "state contains invalid JSON: {error}"),
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

    use super::{JobState, JobStatus, OrchestratorState, StateStore};
    use crate::orchestrator::home::LyaHome;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn home() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-state-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("home directory should be created");
        directory
    }

    fn job(job_id: &str, status: JobStatus) -> JobState {
        let mut job = JobState::new(
            job_id,
            "Pixel Creator",
            std::env::temp_dir(),
            "validate change",
        );
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

        assert_eq!(
            store.load().expect("missing state should load"),
            OrchestratorState::default()
        );
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn saves_and_loads_state() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let mut state = OrchestratorState::default();
        state.upsert(job("pixel-creator", JobStatus::Running));

        store.save(&state).expect("state should save");

        assert_eq!(store.load().expect("state should load"), state);
        assert!(store.path().is_file());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn save_replaces_existing_state() {
        let directory = home();
        let store = StateStore::new(&LyaHome::from_path(&directory));
        let mut first = OrchestratorState::default();
        first.upsert(job("pixel-creator", JobStatus::Running));
        store.save(&first).expect("first state should save");
        let mut second = OrchestratorState::default();
        second.upsert(job("pixel-creator", JobStatus::Stopped));

        store.save(&second).expect("replacement state should save");

        assert_eq!(store.load().expect("state should load"), second);
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn reports_invalid_json() {
        let directory = home();
        fs::write(directory.join("state.json"), "not JSON").expect("state should be written");
        let store = StateStore::new(&LyaHome::from_path(&directory));

        let error = store.load().expect_err("invalid JSON should fail");

        assert!(error.to_string().contains("invalid JSON"));
        fs::remove_dir_all(directory).expect("home should be removed");
    }
}
