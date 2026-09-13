//! Repository-level exclusion.
//!
//! [`JobLock`](super::lock::JobLock) answers "is anyone else driving *this job*". It cannot answer
//! "is anyone else driving *this repository*", which is the question bounded multi-project
//! scheduling actually has to answer: two different jobs pointed at the same working tree would
//! interleave Git writes and destroy every snapshot guarantee Lya relies on.
//!
//! Repository locking is an additional layer, never a replacement for the per-job lock. A scheduled
//! job holds both.

use std::{
    error::Error,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
};

use super::state::{StateStore, current_unix_seconds};

/// The canonical identity of one repository.
///
/// Identity is derived from the repository's real location on disk, never from a project display
/// name and never from the spelling the user happened to type:
///
/// * the project path is canonicalized, so symlinks, `.`/`..` segments and (on Windows) letter
///   case resolve to one real path;
/// * the nearest ancestor holding a `.git` entry becomes the repository root, so a job started from
///   a subdirectory claims the same repository as a job started from its top level.
///
/// The claim file is named after a fingerprint of that root rather than after the path itself,
/// because a path is not a portable file name. A fingerprint collision could only make two
/// unrelated repositories take turns; it can never let one repository be claimed twice.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepositoryIdentity {
    root: PathBuf,
    key: String,
}

impl RepositoryIdentity {
    pub fn resolve(project_path: &Path) -> Result<Self, RepositoryLockError> {
        let canonical = project_path.canonicalize().map_err(|error| {
            RepositoryLockError::UnresolvablePath(project_path.to_owned(), error.to_string())
        })?;
        let root = repository_root(&canonical);
        let key = fingerprint(&comparable(&root));
        Ok(Self { root, key })
    }

    /// The canonical repository root this identity claims.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The stable file-name-safe fingerprint of [`RepositoryIdentity::root`].
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Display for RepositoryIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.root.display())
    }
}

/// An exclusive claim on one repository, held for as long as a Lya process drives work in it.
///
/// The claim uses exactly the philosophy [`JobLock`](super::lock::JobLock) uses: the operating
/// system's own advisory lock on an open handle (`LockFileEx` on Windows, `flock(2)` on Linux and
/// macOS), never the presence of the file and never a recorded process ID. The kernel releases the
/// lock when the handle closes, including on a crash or a kill, so an abandoned claim recovers by
/// itself. The file body is diagnostics only; nothing reads it to decide whether the claim may be
/// taken, so process-ID reuse grants nothing.
///
/// Because a `flock` lock belongs to the open file description, a second independent open inside
/// the same process is refused too — a second scheduler in one process cannot double-claim either.
#[derive(Debug)]
pub struct RepositoryLock {
    path: PathBuf,
    repository: PathBuf,
    file: Option<fs::File>,
}

impl RepositoryLock {
    pub fn acquire(
        store: &StateStore,
        identity: &RepositoryIdentity,
    ) -> Result<Self, RepositoryLockError> {
        let directory = store.repositories_directory();
        fs::create_dir_all(&directory)
            .map_err(|error| RepositoryLockError::Io(error.to_string()))?;
        let path = directory.join(format!("{}.lock", identity.key()));

        // Opened without truncation: the previous owner's diagnostics must survive until this
        // process actually holds the claim.
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| RepositoryLockError::Io(error.to_string()))?;

        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(RepositoryLockError::Held {
                    repository: identity.root.clone(),
                    detail: read_owner(&path),
                });
            }
            Err(fs::TryLockError::Error(error)) => {
                return Err(RepositoryLockError::Io(error.to_string()));
            }
        }

        write_owner(&mut file, identity)?;
        Ok(Self {
            path,
            repository: identity.root.clone(),
            file: Some(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn repository(&self) -> &Path {
        &self.repository
    }
}

impl Drop for RepositoryLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            // Closing the handle would release the claim anyway; unlocking makes it explicit. The
            // file is deliberately left in place: unlinking a locked path lets another process lock
            // a different inode under the same name and believe it owns the repository.
            let _ = file.unlock();
        }
    }
}

fn write_owner(
    file: &mut fs::File,
    identity: &RepositoryIdentity,
) -> Result<(), RepositoryLockError> {
    let body = format!(
        "{{\n  \"repository\": {},\n  \"process_id\": {},\n  \"acquired_unix_seconds\": {}\n}}\n",
        serde_json::Value::from(identity.root.display().to_string()),
        std::process::id(),
        current_unix_seconds()
    );
    file.set_len(0)
        .map_err(|error| RepositoryLockError::Io(error.to_string()))?;
    file.write_all(body.as_bytes())
        .map_err(|error| RepositoryLockError::Io(error.to_string()))?;
    file.flush()
        .map_err(|error| RepositoryLockError::Io(error.to_string()))
}

/// Diagnostics for the refusal message only; never part of the locking decision.
///
/// Windows denies reads of a range another handle has locked, so the recorded owner is usually
/// unreadable exactly while the claim is held and the message falls back to naming the claim file.
fn read_owner(path: &Path) -> String {
    match fs::read_to_string(path) {
        Ok(content) if !content.trim().is_empty() => content.trim().replace('\n', " "),
        _ => format!("see {}", path.display()),
    }
}

/// The nearest ancestor that holds a `.git` entry, or the path itself when none does.
///
/// `.git` is matched as an entry rather than as a directory so linked worktrees and submodules,
/// where `.git` is a file, resolve to their own repository instead of to their parent's.
fn repository_root(canonical: &Path) -> PathBuf {
    let mut current = Some(canonical);
    while let Some(directory) = current {
        if directory.join(".git").exists() {
            return directory.to_owned();
        }
        current = directory.parent();
    }
    canonical.to_owned()
}

/// Windows file systems compare paths case-insensitively, so the same repository reached through
/// different letter case must fingerprint identically there. Unix paths are case-sensitive and are
/// compared exactly.
pub(crate) fn comparable(root: &Path) -> String {
    let rendered = root.to_string_lossy().into_owned();
    if cfg!(windows) {
        rendered.to_lowercase()
    } else {
        rendered
    }
}

/// FNV-1a, 128 bit. Chosen because it is a handful of lines with no dependency and produces a
/// stable, platform-independent file name for the same input.
pub(crate) fn fingerprint(value: &str) -> String {
    const OFFSET_BASIS: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut hash = OFFSET_BASIS;
    for byte in value.as_bytes() {
        hash ^= u128::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:032x}")
}

#[derive(Debug)]
pub enum RepositoryLockError {
    Held { repository: PathBuf, detail: String },
    UnresolvablePath(PathBuf, String),
    Io(String),
}

impl fmt::Display for RepositoryLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held { repository, detail } => write!(
                formatter,
                "repository {} is already being driven by another Lya process ({detail})",
                repository.display()
            ),
            Self::UnresolvablePath(path, error) => write!(
                formatter,
                "could not resolve the repository at {}: {error}",
                path.display()
            ),
            Self::Io(error) => write!(formatter, "could not claim the repository: {error}"),
        }
    }
}

impl Error for RepositoryLockError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{RepositoryIdentity, RepositoryLock, RepositoryLockError};
    use crate::orchestrator::state::StateStore;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    /// The full libtest path of the child-process helper below.
    const HELPER_TEST: &str = "orchestrator::repository_lock::tests::repository_claim_helper";

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

    fn home() -> (PathBuf, StateStore) {
        let path = unique("repository-lock-home");
        let store = StateStore::at(&path);
        (path, store)
    }

    /// A disposable directory that looks like a Git working tree to the identity resolver.
    fn repository(name: &str) -> PathBuf {
        let path = unique(name);
        fs::create_dir_all(path.join(".git")).expect("repository marker should be created");
        path
    }

    #[test]
    fn a_live_repository_claim_refuses_a_second_owner() {
        let (directory, store) = home();
        let project = repository("repository-claim");
        let identity = RepositoryIdentity::resolve(&project).expect("identity should resolve");
        let first =
            RepositoryLock::acquire(&store, &identity).expect("first claim should be taken");

        let error = RepositoryLock::acquire(&store, &identity)
            .expect_err("a second claim on the same repository should be refused");

        assert!(matches!(error, RepositoryLockError::Held { .. }));
        assert!(error.to_string().contains("another Lya process"));
        drop(first);
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    #[test]
    fn different_repositories_never_block_each_other() {
        let (directory, store) = home();
        let first_project = repository("repository-a");
        let second_project = repository("repository-b");
        let first = RepositoryLock::acquire(
            &store,
            &RepositoryIdentity::resolve(&first_project).expect("identity"),
        )
        .expect("first repository should be claimed");

        let second = RepositoryLock::acquire(
            &store,
            &RepositoryIdentity::resolve(&second_project).expect("identity"),
        )
        .expect("a different repository should be claimable concurrently");

        assert_ne!(first.path(), second.path());
        drop(first);
        drop(second);
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(first_project).expect("repository should be removed");
        fs::remove_dir_all(second_project).expect("repository should be removed");
    }

    /// The point of canonical identity: the same repository reached through a different spelling is
    /// the same claim, so it can never be driven twice.
    #[test]
    fn equivalent_paths_resolve_to_one_repository_claim() {
        let (directory, store) = home();
        let project = repository("repository-equivalence");
        let nested = project.join("crates").join("inner");
        fs::create_dir_all(&nested).expect("nested directory should be created");

        let canonical = RepositoryIdentity::resolve(&project).expect("identity");
        let through_dot = RepositoryIdentity::resolve(&project.join(".")).expect("identity");
        let through_parent =
            RepositoryIdentity::resolve(&nested.join("..").join("..")).expect("identity");
        let from_subdirectory = RepositoryIdentity::resolve(&nested).expect("identity");

        assert_eq!(canonical.key(), through_dot.key());
        assert_eq!(canonical.key(), through_parent.key());
        assert_eq!(
            canonical.key(),
            from_subdirectory.key(),
            "a job started inside the repository claims the same repository"
        );
        assert_eq!(canonical.root(), from_subdirectory.root());

        let claim = RepositoryLock::acquire(&store, &canonical).expect("claim should be taken");
        assert!(
            RepositoryLock::acquire(&store, &from_subdirectory).is_err(),
            "an equivalent path must not be able to claim the same repository again"
        );
        drop(claim);
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    #[cfg(windows)]
    #[test]
    fn windows_letter_case_resolves_to_one_repository_claim() {
        let project = repository("repository-case");
        let shouted = PathBuf::from(project.to_string_lossy().to_uppercase());

        let canonical = RepositoryIdentity::resolve(&project).expect("identity");
        let upper = RepositoryIdentity::resolve(&shouted).expect("identity");

        assert_eq!(canonical.key(), upper.key());
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    #[test]
    fn releasing_a_claim_allows_a_later_owner_and_keeps_the_file() {
        let (directory, store) = home();
        let project = repository("repository-release");
        let identity = RepositoryIdentity::resolve(&project).expect("identity");
        let first = RepositoryLock::acquire(&store, &identity).expect("claim should be taken");
        let path = first.path().to_owned();
        drop(first);

        let second = RepositoryLock::acquire(&store, &identity).expect("claim should be retaken");

        assert_eq!(second.path(), path);
        drop(second);
        assert!(
            path.is_file(),
            "the claim file must survive release so the path keeps identifying one repository"
        );
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    /// Exactly the state a killed process leaves behind: the file and its recorded owner are still
    /// there, but the operating system already released the claim.
    #[test]
    fn an_abandoned_repository_claim_is_recoverable() {
        let (directory, store) = home();
        let project = repository("repository-abandoned");
        let identity = RepositoryIdentity::resolve(&project).expect("identity");
        let claim_path = store
            .repositories_directory()
            .join(format!("{}.lock", identity.key()));
        fs::create_dir_all(store.repositories_directory()).expect("directory should be created");
        fs::write(
            &claim_path,
            b"{\n  \"repository\": \"somewhere\",\n  \"process_id\": 4294967294,\n  \"acquired_unix_seconds\": 1\n}\n",
        )
        .expect("stale claim should be written");

        let claim = RepositoryLock::acquire(&store, &identity)
            .expect("an abandoned claim must never make a repository permanently unusable");

        // Windows denies reads while the claim is held, so the body is inspected after release.
        drop(claim);
        let recorded = fs::read_to_string(&claim_path).expect("claim body should be readable");
        assert!(
            recorded.contains(&format!("\"process_id\": {}", std::process::id())),
            "the new owner should replace the stale diagnostics: {recorded}"
        );
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    #[test]
    fn an_empty_abandoned_claim_file_is_also_recoverable() {
        let (directory, store) = home();
        let project = repository("repository-empty-claim");
        let identity = RepositoryIdentity::resolve(&project).expect("identity");
        fs::create_dir_all(store.repositories_directory()).expect("directory should be created");
        fs::write(
            store
                .repositories_directory()
                .join(format!("{}.lock", identity.key())),
            b"",
        )
        .expect("empty claim should be written");

        let claim = RepositoryLock::acquire(&store, &identity)
            .expect("an empty claim file must not block a repository");

        drop(claim);
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    #[test]
    fn a_missing_project_path_is_refused_explicitly() {
        let error = RepositoryIdentity::resolve(Path::new("this-path-does-not-exist-anywhere"))
            .expect_err("an unresolvable path should be refused");

        assert!(matches!(error, RepositoryLockError::UnresolvablePath(..)));
    }

    /// Coordination must survive a second Lya *process*, not only a second job inside one
    /// scheduler. The child here is this very test binary re-invoked on the ignored helper below,
    /// which is the closest available stand-in for another Lya process.
    #[test]
    fn a_second_process_cannot_claim_a_repository_already_driven_here() {
        let (directory, store) = home();
        let project = repository("repository-cross-process");
        let identity = RepositoryIdentity::resolve(&project).expect("identity");

        let claim = RepositoryLock::acquire(&store, &identity).expect("claim should be taken");
        let refused = run_helper(&directory, &project);
        drop(claim);
        let allowed = run_helper(&directory, &project);

        assert!(
            refused.contains("REFUSED"),
            "a second process must not claim a repository this process is driving: {refused}"
        );
        assert!(
            allowed.contains("ACQUIRED"),
            "a released claim must be available to a second process: {allowed}"
        );
        fs::remove_dir_all(directory).expect("home should be removed");
        fs::remove_dir_all(project).expect("repository should be removed");
    }

    fn run_helper(home: &Path, project: &Path) -> String {
        let executable = std::env::current_exe().expect("the test binary should be locatable");
        let output = Command::new(executable)
            .args(["--exact", HELPER_TEST, "--ignored", "--nocapture"])
            .env("LYA_TEST_REPOSITORY_HOME", home)
            .env("LYA_TEST_REPOSITORY_PROJECT", project)
            .output()
            .expect("the helper process should start");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// Child half of [`a_second_process_cannot_claim_a_repository_already_driven_here`]. It is
    /// ignored by default and does nothing unless the parent names a repository for it.
    #[test]
    #[ignore = "child process helper, driven by the cross-process repository claim test"]
    fn repository_claim_helper() {
        let (Ok(home), Ok(project)) = (
            std::env::var("LYA_TEST_REPOSITORY_HOME"),
            std::env::var("LYA_TEST_REPOSITORY_PROJECT"),
        ) else {
            return;
        };
        let store = StateStore::at(home);
        let identity =
            RepositoryIdentity::resolve(Path::new(&project)).expect("identity should resolve");
        match RepositoryLock::acquire(&store, &identity) {
            Ok(claim) => {
                println!("ACQUIRED");
                drop(claim);
            }
            Err(_) => println!("REFUSED"),
        }
    }
}
