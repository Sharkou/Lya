//! The daemon claim, and the metadata that describes the daemon holding it.
//!
//! The two are deliberately separate files, because they answer different questions and only one of
//! them is allowed to be authoritative:
//!
//! * `LYA_HOME/daemon.lock` is the **claim**. It is the operating system's own advisory lock on an
//!   open handle — `LockFileEx` on Windows, `flock(2)` on Linux and macOS — exactly as
//!   [`JobLock`](crate::orchestrator::lock::JobLock) and
//!   [`RepositoryLock`](crate::orchestrator::repository_lock::RepositoryLock) are. The kernel
//!   releases it when the handle closes, including on a crash, a kill or a machine restart, so an
//!   abandoned claim recovers by itself and a stale file can never permanently block startup.
//! * `LYA_HOME/daemon.json` is **diagnostics**: process ID, start time, protocol version and
//!   endpoint. Nothing reads it to decide whether a daemon may start, so a recorded process ID that
//!   has been reused grants nothing. It is a separate file because Windows denies reads of a locked
//!   byte range: metadata inside the claim file would be unreadable exactly while it matters.
//!
//! The claim and the endpoint together are the ownership authority. A process ID never is.

use std::{
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::orchestrator::{home::LyaHome, state::current_unix_seconds};

use super::protocol::PROTOCOL_VERSION;

/// What a daemon records about itself for discovery and diagnostics.
///
/// Nothing here is a secret and nothing here is authority. It exists so `lya daemon status` can
/// describe a daemon, and so a human can tell which process to look at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonMetadata {
    pub process_id: u32,
    pub started_unix_seconds: u64,
    pub protocol_version: u32,
    pub endpoint: String,
}

impl DaemonMetadata {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            process_id: std::process::id(),
            started_unix_seconds: current_unix_seconds(),
            protocol_version: PROTOCOL_VERSION,
            endpoint: endpoint.into(),
        }
    }

    pub fn path(home: &LyaHome) -> PathBuf {
        home.path().join("daemon.json")
    }

    /// Read whatever a previous or current daemon recorded. Unreadable or invalid metadata is simply
    /// absent: it is diagnostics, so it never fails a command.
    pub fn read(home: &LyaHome) -> Option<Self> {
        let content = fs::read_to_string(Self::path(home)).ok()?;
        serde_json::from_str(&content).ok()
    }

    fn write(&self, home: &LyaHome) -> Result<(), DaemonLockError> {
        let path = Self::path(home);
        let content = serde_json::to_vec_pretty(self)
            .map_err(|error| DaemonLockError::Io(error.to_string()))?;
        fs::write(&path, content).map_err(|error| DaemonLockError::Io(error.to_string()))
    }

    fn remove(home: &LyaHome) {
        let _ = fs::remove_file(Self::path(home));
    }
}

/// An exclusive claim on the daemon role for one `LYA_HOME`.
///
/// Held for the daemon's whole life. While it is held, no second daemon can start against the same
/// home; when the holder dies for any reason, the next one starts without cleanup.
#[derive(Debug)]
pub struct DaemonLock {
    path: PathBuf,
    home: LyaHome,
    file: Option<fs::File>,
}

impl DaemonLock {
    /// Take the claim, or explain who holds it.
    pub fn acquire(home: &LyaHome) -> Result<Self, DaemonLockError> {
        fs::create_dir_all(home.path()).map_err(|error| DaemonLockError::Io(error.to_string()))?;
        let path = Self::path(home);

        // Opened without truncation: the previous owner's diagnostics must survive until this
        // process actually holds the claim.
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| DaemonLockError::Io(error.to_string()))?;

        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(DaemonLockError::Held {
                    detail: describe_holder(home),
                });
            }
            Err(fs::TryLockError::Error(error)) => {
                return Err(DaemonLockError::Io(error.to_string()));
            }
        }

        Ok(Self {
            path,
            home: LyaHome::from_path(home.path()),
            file: Some(file),
        })
    }

    pub fn path(home: &LyaHome) -> PathBuf {
        home.path().join("daemon.lock")
    }

    /// Whether a daemon currently holds the claim for one home.
    ///
    /// Answered by trying to take the claim and releasing it again, never by looking at a recorded
    /// process ID. A claim that cannot be taken for any other reason is reported as an error rather
    /// than guessed at.
    ///
    /// Read-only, and deliberately not implemented in terms of [`DaemonLock::acquire`]. Acquiring
    /// produces an owner, and dropping an owner removes the metadata an owner is responsible for —
    /// so a probe built that way would delete another daemon's diagnostics simply by asking whether
    /// that daemon exists. This takes and releases the kernel's lock on its own handle and touches
    /// nothing else, which is what makes it safe to poll.
    pub fn is_held(home: &LyaHome) -> Result<bool, DaemonLockError> {
        let path = Self::path(home);
        let file = match fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) => return Err(DaemonLockError::Io(error.to_string())),
        };
        match file.try_lock() {
            // Taken, so nobody held it. Released again by closing the handle, and nothing else on
            // disk was touched.
            Ok(()) => {
                let _ = file.unlock();
                Ok(false)
            }
            Err(fs::TryLockError::WouldBlock) => Ok(true),
            Err(fs::TryLockError::Error(error)) => Err(DaemonLockError::Io(error.to_string())),
        }
    }

    /// Record this daemon's diagnostics. Only meaningful once the claim is held.
    pub fn publish_metadata(&self, endpoint: &str) -> Result<DaemonMetadata, DaemonLockError> {
        let metadata = DaemonMetadata::new(endpoint);
        metadata.write(&self.home)?;
        Ok(metadata)
    }

    /// The claim file itself, for diagnostics.
    pub fn lock_path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        // The metadata describes a daemon that no longer exists, so it goes first: a graceful exit
        // should not leave a file suggesting a daemon is up. A crash leaves it behind, which is
        // harmless — nothing reads it to decide anything.
        DaemonMetadata::remove(&self.home);
        if let Some(file) = self.file.take() {
            // Closing the handle would release the claim anyway; unlocking makes it explicit. The
            // file is deliberately left in place: unlinking a locked path lets another process lock
            // a different inode under the same name and believe it owns the daemon role.
            let _ = file.unlock();
        }
    }
}

/// Diagnostics for a refusal message only; never part of the locking decision.
fn describe_holder(home: &LyaHome) -> String {
    match DaemonMetadata::read(home) {
        Some(metadata) => format!(
            "process {} since {}, protocol {}, endpoint {}",
            metadata.process_id,
            metadata.started_unix_seconds,
            metadata.protocol_version,
            metadata.endpoint
        ),
        None => format!("see {}", DaemonLock::path(home).display()),
    }
}

#[derive(Debug)]
pub enum DaemonLockError {
    Held { detail: String },
    Io(String),
}

impl fmt::Display for DaemonLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held { detail } => write!(
                formatter,
                "a Lya daemon is already running for this LYA_HOME ({detail})"
            ),
            Self::Io(error) => write!(formatter, "could not claim the daemon role: {error}"),
        }
    }
}

impl Error for DaemonLockError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{DaemonLock, DaemonLockError, DaemonMetadata};
    use crate::orchestrator::home::LyaHome;

    static NEXT_HOME: AtomicUsize = AtomicUsize::new(0);

    /// The full libtest path of the child-process helper below.
    const HELPER_TEST: &str = "daemon::lock::tests::daemon_claim_helper";

    fn home() -> LyaHome {
        let path = std::env::temp_dir().join(format!(
            "lya-daemon-lock-{}-{}",
            std::process::id(),
            NEXT_HOME.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("home should be created");
        LyaHome::from_path(path)
    }

    #[test]
    fn only_one_daemon_can_claim_one_home() {
        let home = home();
        let first = DaemonLock::acquire(&home).expect("the first claim should be taken");
        first
            .publish_metadata("test-endpoint")
            .expect("metadata should be recorded");

        let error = DaemonLock::acquire(&home).expect_err("a second daemon should be refused");

        assert!(matches!(error, DaemonLockError::Held { .. }));
        let message = error.to_string();
        assert!(message.contains("already running"), "{message}");
        assert!(
            message.contains("test-endpoint") || message.contains("daemon.lock"),
            "the refusal should identify the holder: {message}"
        );
        drop(first);
        let _ = fs::remove_dir_all(home.path());
    }

    #[test]
    fn two_homes_run_two_independent_daemons() {
        let first_home = home();
        let second_home = home();

        let first = DaemonLock::acquire(&first_home).expect("the first home should be claimable");
        let second = DaemonLock::acquire(&second_home)
            .expect("a different home must be claimable at the same time");

        assert_ne!(first.lock_path(), second.lock_path());
        drop(first);
        drop(second);
        let _ = fs::remove_dir_all(first_home.path());
        let _ = fs::remove_dir_all(second_home.path());
    }

    #[test]
    fn a_released_claim_can_be_retaken_and_keeps_its_file() {
        let home = home();
        let first = DaemonLock::acquire(&home).expect("the claim should be taken");
        let path = first.lock_path().to_owned();
        drop(first);

        let second = DaemonLock::acquire(&home).expect("the claim should be retaken");

        assert_eq!(second.lock_path(), path);
        drop(second);
        assert!(
            path.is_file(),
            "the claim file must survive release so the path keeps identifying one claim"
        );
        let _ = fs::remove_dir_all(home.path());
    }

    /// Exactly the state a crash or a machine restart leaves behind: a claim file and stale metadata
    /// naming a process that is gone. Neither may block the next daemon.
    #[test]
    fn a_stale_claim_and_stale_metadata_never_block_startup() {
        let home = home();
        fs::write(
            DaemonLock::path(&home),
            b"{\n  \"process_id\": 4294967294\n}\n",
        )
        .expect("a stale claim file should be written");
        fs::write(
            DaemonMetadata::path(&home),
            b"{\n  \"process_id\": 4294967294,\n  \"started_unix_seconds\": 1,\n  \"protocol_version\": 1,\n  \"endpoint\": \"gone\"\n}\n",
        )
        .expect("stale metadata should be written");

        let lock = DaemonLock::acquire(&home)
            .expect("an abandoned claim must never make a home permanently unusable");

        assert!(
            DaemonLock::is_held(&home).expect("the claim state should be readable"),
            "the new owner holds the claim"
        );
        drop(lock);
        assert!(
            !DaemonLock::is_held(&home).expect("the claim state should be readable"),
            "the claim is free again once the owner releases it"
        );
        assert!(
            DaemonMetadata::read(&home).is_none(),
            "a graceful exit removes the metadata it wrote"
        );
        let _ = fs::remove_dir_all(home.path());
    }

    #[test]
    fn metadata_describes_the_daemon_without_being_authority() {
        let home = home();
        let lock = DaemonLock::acquire(&home).expect("the claim should be taken");

        let metadata = lock
            .publish_metadata("local-endpoint")
            .expect("metadata should be recorded");

        assert_eq!(metadata.process_id, std::process::id());
        assert_eq!(metadata.endpoint, "local-endpoint");
        assert_eq!(metadata.protocol_version, super::PROTOCOL_VERSION);
        let read = DaemonMetadata::read(&home).expect("metadata should be readable");
        assert_eq!(read, metadata);

        // Metadata claiming a live daemon does not make one: the claim is what decides.
        drop(lock);
        read.write(&home).expect("metadata should be rewritable");
        assert!(
            !DaemonLock::is_held(&home).expect("the claim state should be readable"),
            "recorded metadata must never make a released claim look held"
        );
        assert!(DaemonLock::acquire(&home).is_ok());
        let _ = fs::remove_dir_all(home.path());
    }

    #[test]
    fn unreadable_metadata_is_absent_rather_than_fatal() {
        let home = home();
        fs::write(DaemonMetadata::path(&home), b"not json").expect("metadata should be written");

        assert!(DaemonMetadata::read(&home).is_none());
        assert!(DaemonLock::acquire(&home).is_ok());
        let _ = fs::remove_dir_all(home.path());
    }

    /// The claim has to exclude a second *process*, not only a second call inside one. The child is
    /// this test binary re-invoked on the ignored helper below, the closest available stand-in for
    /// another Lya process.
    #[test]
    fn a_second_process_cannot_claim_a_home_this_process_owns() {
        let home = home();

        let lock = DaemonLock::acquire(&home).expect("the claim should be taken");
        let refused = run_helper(home.path());
        drop(lock);
        let allowed = run_helper(home.path());

        assert!(
            refused.contains("REFUSED"),
            "a second process must not claim a home this process owns: {refused}"
        );
        assert!(
            allowed.contains("ACQUIRED"),
            "a released claim must be available to a second process: {allowed}"
        );
        let _ = fs::remove_dir_all(home.path());
    }

    /// A daemon that dies without unlocking releases the claim anyway, because the kernel owns the
    /// release. This is the crash-recovery guarantee, proven across a real process boundary.
    #[test]
    fn a_claim_held_by_a_dead_process_is_available_again() {
        let home = home();

        let held = run_helper_holding(home.path());
        assert!(
            held.contains("ACQUIRED"),
            "the child should have taken the claim: {held}"
        );

        let lock = DaemonLock::acquire(&home)
            .expect("the claim of a process that died must be available again");

        drop(lock);
        let _ = fs::remove_dir_all(home.path());
    }

    fn run_helper(home: &std::path::Path) -> String {
        helper_output(home, false)
    }

    /// Runs the helper in the mode where it takes the claim and then exits without unlocking.
    fn run_helper_holding(home: &std::path::Path) -> String {
        helper_output(home, true)
    }

    fn helper_output(home: &std::path::Path, abandon: bool) -> String {
        let executable = std::env::current_exe().expect("the test binary should be locatable");
        let output = Command::new(executable)
            .args(["--exact", HELPER_TEST, "--ignored", "--nocapture"])
            .env("LYA_TEST_DAEMON_HOME", home)
            .env("LYA_TEST_DAEMON_ABANDON", if abandon { "1" } else { "0" })
            .output()
            .expect("the helper process should start");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// Child half of the cross-process claim tests. Ignored by default and inert unless the parent
    /// names a home for it.
    #[test]
    #[ignore = "child process helper, driven by the cross-process daemon claim tests"]
    fn daemon_claim_helper() {
        let Ok(home) = std::env::var("LYA_TEST_DAEMON_HOME") else {
            return;
        };
        let abandon = std::env::var("LYA_TEST_DAEMON_ABANDON").as_deref() == Ok("1");
        let home = LyaHome::from_path(PathBuf::from(home));
        match DaemonLock::acquire(&home) {
            Ok(lock) => {
                println!("ACQUIRED");
                if abandon {
                    // Leak the claim and let the process end: exactly what a crash leaves behind.
                    std::mem::forget(lock);
                } else {
                    drop(lock);
                }
            }
            Err(_) => println!("REFUSED"),
        }
    }
}
