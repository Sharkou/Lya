use std::{
    error::Error,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
};

use super::state::{StateError, StateStore, current_unix_seconds, validate_job_id};

/// An exclusive claim on one job, held for as long as a Lya process is driving it.
///
/// The claim is the operating system's own advisory lock on `lock.json`
/// (`std::fs::File::try_lock`), never the presence of the file and never a recorded process ID:
///
/// * Windows takes a `LockFileEx` byte-range lock on the handle;
/// * Linux and macOS take a `flock(2)` lock on the open file description.
///
/// On every one of those platforms the kernel releases the lock when the handle closes, which
/// includes abnormal termination — a killed or crashed process therefore leaves a lock file with
/// no authority behind it, and the next Lya process acquires it immediately. A `flock` lock belongs
/// to the open file description rather than to the process, so a second independent open in the
/// same process is refused as well.
///
/// The file body records the owning job, process ID and acquisition time for diagnostics only.
/// Nothing reads it to decide whether the lock may be taken, so process-ID reuse cannot grant a
/// claim.
///
/// (Rust's standard library falls back to POSIX `fcntl` record locks on a few Unix targets that
/// lack `flock`, such as Solaris and illumos. Lya does not currently support those targets; they
/// would weaken the same-process guarantee, not the crash-recovery one.)
#[derive(Debug)]
pub struct JobLock {
    path: PathBuf,
    file: Option<fs::File>,
}

impl JobLock {
    pub fn acquire(store: &StateStore, job_id: &str) -> Result<Self, LockError> {
        validate_job_id(job_id).map_err(LockError::State)?;
        let directory = store.job_directory(job_id).map_err(LockError::State)?;
        fs::create_dir_all(&directory).map_err(|error| LockError::Io(error.to_string()))?;
        let path = directory.join("lock.json");

        // The file is opened without truncation: the previous owner's diagnostics must survive
        // until this process actually holds the lock.
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| LockError::Io(error.to_string()))?;

        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(LockError::Held {
                    job_id: job_id.to_owned(),
                    detail: read_owner(&path),
                });
            }
            Err(fs::TryLockError::Error(error)) => return Err(LockError::Io(error.to_string())),
        }

        // The lock is held from here on, so the diagnostics can safely be replaced.
        write_owner(&mut file, job_id)?;
        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for JobLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            // Closing the handle would release the lock on its own; unlocking first makes the
            // release explicit. The file itself is deliberately left in place: removing a locked
            // path lets another process lock a different inode under the same name and believe it
            // owns the job.
            let _ = file.unlock();
        }
    }
}

fn write_owner(file: &mut fs::File, job_id: &str) -> Result<(), LockError> {
    let body = format!(
        "{{\n  \"job_id\": \"{job_id}\",\n  \"process_id\": {},\n  \"acquired_unix_seconds\": {}\n}}\n",
        std::process::id(),
        current_unix_seconds()
    );
    file.set_len(0)
        .map_err(|error| LockError::Io(error.to_string()))?;
    file.write_all(body.as_bytes())
        .map_err(|error| LockError::Io(error.to_string()))?;
    file.flush()
        .map_err(|error| LockError::Io(error.to_string()))
}

/// Diagnostics for the refusal message only; never part of the locking decision.
///
/// Windows denies reads of a range another handle has locked, so the recorded owner is usually
/// unreadable exactly while the lock is held and the message falls back to naming the lock file.
/// Unix `flock` locks do not restrict reads, so the owner is normally included there.
fn read_owner(path: &Path) -> String {
    match fs::read_to_string(path) {
        Ok(content) if !content.trim().is_empty() => content.trim().replace('\n', " "),
        _ => format!("see {}", path.display()),
    }
}

#[derive(Debug)]
pub enum LockError {
    Held { job_id: String, detail: String },
    Io(String),
    State(StateError),
}

impl fmt::Display for LockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held { job_id, detail } => write!(
                formatter,
                "job {job_id} is already being driven by another Lya process ({detail})"
            ),
            Self::Io(error) => write!(formatter, "could not acquire the job lock: {error}"),
            Self::State(error) => write!(formatter, "could not acquire the job lock: {error}"),
        }
    }
}

impl Error for LockError {
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
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{JobLock, LockError};
    use crate::orchestrator::state::StateStore;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn store() -> (std::path::PathBuf, StateStore) {
        let directory = std::env::temp_dir().join(format!(
            "lya-lock-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("lock home should be created");
        let store = StateStore::at(&directory);
        (directory, store)
    }

    #[test]
    fn a_live_lock_refuses_a_second_owner() {
        let (directory, store) = store();
        let first = JobLock::acquire(&store, "locked-job").expect("first lock should be acquired");

        let error =
            JobLock::acquire(&store, "locked-job").expect_err("second lock should be refused");

        assert!(matches!(error, LockError::Held { ref job_id, .. } if job_id == "locked-job"));
        let message = error.to_string();
        assert!(message.contains("another Lya process"));
        // The detail names the recorded owner where the platform allows reading a locked file, and
        // the lock path otherwise; both must point the user at the right lock.
        assert!(
            message.contains("lock.json") || message.contains("\"process_id\""),
            "the refusal should identify the lock: {message}"
        );
        drop(first);
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    #[test]
    fn releasing_a_lock_allows_a_later_process_to_acquire_it() {
        let (directory, store) = store();
        let first = JobLock::acquire(&store, "reusable").expect("first lock should be acquired");
        let path = first.path().to_owned();
        assert!(path.is_file());
        drop(first);

        let second = JobLock::acquire(&store, "reusable").expect("lock should be reacquired");

        assert_eq!(second.path(), path);
        drop(second);
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    /// A released lock keeps its file on purpose: unlinking a locked path would let another
    /// process lock a fresh inode under the same name and believe it owns the same job.
    #[test]
    fn releasing_a_lock_leaves_the_file_in_place() {
        let (directory, store) = store();
        let lock = JobLock::acquire(&store, "kept-file").expect("lock should be acquired");
        let path = lock.path().to_owned();
        drop(lock);

        assert!(
            path.is_file(),
            "the lock file must survive release so the path keeps identifying one lock"
        );
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    /// Exactly the state a killed process leaves behind: the file and its recorded owner are still
    /// there, but the operating system already released the lock. This must never block recovery.
    #[test]
    fn a_lock_file_abandoned_by_a_dead_owner_never_blocks_recovery() {
        let (directory, store) = store();
        let path = store
            .job_directory("crashed-job")
            .expect("job directory should resolve")
            .join("lock.json");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory should be created");
        // A process ID that is not this process, recorded by an owner that never released it.
        fs::write(
            &path,
            b"{\n  \"job_id\": \"crashed-job\",\n  \"process_id\": 4294967294,\n  \"acquired_unix_seconds\": 1\n}\n",
        )
        .expect("stale lock file should be written");

        let lock = JobLock::acquire(&store, "crashed-job")
            .expect("an abandoned lock file must not make a job unresumable");
        let lock_path = lock.path().to_owned();
        // Windows denies reads while the lock is held, so the body is inspected after release.
        drop(lock);

        let recorded = fs::read_to_string(&lock_path).expect("lock body should be readable");
        assert!(
            recorded.contains(&format!("\"process_id\": {}", std::process::id())),
            "the new owner should replace the stale diagnostics: {recorded}"
        );
        assert!(recorded.contains("crashed-job"));
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    #[test]
    fn an_empty_abandoned_lock_file_is_also_recoverable() {
        let (directory, store) = store();
        let path = store
            .job_directory("empty-lock")
            .expect("job directory should resolve")
            .join("lock.json");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory should be created");
        fs::write(&path, b"").expect("empty lock file should be written");

        let lock =
            JobLock::acquire(&store, "empty-lock").expect("an empty lock file must not block");

        drop(lock);
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    #[test]
    fn different_jobs_never_block_each_other() {
        let (directory, store) = store();
        let first = JobLock::acquire(&store, "job-a").expect("first lock should be acquired");
        let second = JobLock::acquire(&store, "job-b").expect("second lock should be acquired");

        assert_ne!(first.path(), second.path());
        drop(first);
        drop(second);
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    #[test]
    fn a_released_job_lock_does_not_release_another_job() {
        let (directory, store) = store();
        let kept = JobLock::acquire(&store, "kept").expect("lock should be acquired");
        let released = JobLock::acquire(&store, "released").expect("lock should be acquired");
        drop(released);

        assert!(
            JobLock::acquire(&store, "kept").is_err(),
            "releasing one job must not release another"
        );
        assert!(JobLock::acquire(&store, "released").is_ok());
        drop(kept);
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }

    #[test]
    fn rejects_unsafe_job_identifiers() {
        let (directory, store) = store();

        let error = JobLock::acquire(&store, "../escape").expect_err("unsafe ID should be refused");

        assert!(matches!(error, LockError::State(_)));
        fs::remove_dir_all(directory).expect("lock home should be removed");
    }
}
