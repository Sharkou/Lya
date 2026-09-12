use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
};

use serde::{Deserialize, Serialize};

use crate::process::{ProcessError, ProcessRunner, ProcessSpec, SystemProcessRunner};

use super::repository::{RepositoryError, RepositoryState};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitIdentity {
    pub name: String,
    pub email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPublishConfig {
    pub identity: GitIdentity,
    pub remote: String,
    pub branch: String,
}

impl GitPublishConfig {
    pub fn from_environment() -> Result<Self, PublishError> {
        Self::new(
            std::env::var("LYA_GIT_NAME").unwrap_or_default(),
            std::env::var("LYA_GIT_EMAIL").unwrap_or_default(),
            std::env::var("LYA_GIT_REMOTE").unwrap_or_else(|_| "origin".to_owned()),
            std::env::var("LYA_GIT_BRANCH").unwrap_or_default(),
        )
    }

    pub fn new(
        name: impl Into<String>,
        email: impl Into<String>,
        remote: impl Into<String>,
        branch: impl Into<String>,
    ) -> Result<Self, PublishError> {
        let identity = GitIdentity {
            name: name.into(),
            email: email.into(),
        };
        let remote = remote.into();
        let branch = branch.into();
        if identity.name.trim().is_empty() {
            return Err(PublishError::MissingIdentity("LYA_GIT_NAME"));
        }
        if identity.email.trim().is_empty() {
            return Err(PublishError::MissingIdentity("LYA_GIT_EMAIL"));
        }
        if remote.trim().is_empty() {
            return Err(PublishError::MissingRemoteConfiguration);
        }
        if branch.trim().is_empty() {
            return Err(PublishError::MissingBranchConfiguration);
        }
        Ok(Self {
            identity,
            remote,
            branch,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub project_path: PathBuf,
    pub accepted_repository_state: RepositoryState,
    pub commit_title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PushStatus {
    #[serde(rename = "PUSHED")]
    Pushed,
    #[serde(rename = "REJECTED")]
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishStage {
    #[serde(rename = "VERIFYING")]
    Verifying,
    #[serde(rename = "STAGING")]
    Staging,
    #[serde(rename = "STAGED")]
    Staged,
    #[serde(rename = "COMMITTING")]
    Committing,
    #[serde(rename = "COMMITTED")]
    Committed,
    #[serde(rename = "PUSHING")]
    Pushing,
    #[serde(rename = "PUSHED")]
    Pushed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishResult {
    pub commit_sha: String,
    pub commit_title: String,
    pub remote: String,
    pub branch: String,
    pub push_status: PushStatus,
}

pub trait Publisher: Send + Sync {
    fn is_enabled(&self) -> bool {
        true
    }

    fn publish<'a>(
        &'a self,
        request: PublishRequest,
        progress: &'a mut dyn PublishProgress,
    ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>>;
}

pub trait PublishProgress: Send {
    fn record(&mut self, stage: PublishStage) -> Result<(), PublishError>;
}

#[derive(Debug, Default)]
pub struct NoopPublishProgress;

impl PublishProgress for NoopPublishProgress {
    fn record(&mut self, _stage: PublishStage) -> Result<(), PublishError> {
        Ok(())
    }
}

pub struct GitPublisher<R = SystemProcessRunner> {
    runner: R,
    config: GitPublishConfig,
}

impl GitPublisher<SystemProcessRunner> {
    pub fn new(config: GitPublishConfig) -> Self {
        Self::with_runner(SystemProcessRunner, config)
    }
}

impl<R> GitPublisher<R> {
    pub fn with_runner(runner: R, config: GitPublishConfig) -> Self {
        Self { runner, config }
    }
}

impl<R: ProcessRunner> GitPublisher<R> {
    async fn run_git(
        &self,
        project_path: &Path,
        arguments: Vec<String>,
        operation: &'static str,
    ) -> Result<String, PublishError> {
        let mut spec = ProcessSpec::new("git");
        spec.args = arguments;
        spec.cwd = Some(project_path.to_owned());
        let output = self.runner.run(spec).await.map_err(PublishError::Process)?;
        if output.exit_code != Some(0) {
            return Err(PublishError::GitCommandFailed {
                operation,
                exit_code: output.exit_code,
            });
        }
        Ok(output.stdout)
    }

    async fn verify_before_staging(&self, request: &PublishRequest) -> Result<(), PublishError> {
        if request.commit_title.trim().is_empty() || request.commit_title.contains(['\n', '\r']) {
            return Err(PublishError::InvalidCommitTitle);
        }
        if !request
            .accepted_repository_state
            .is_complete_for_publication()
        {
            return Err(PublishError::RepositorySnapshotIncomplete);
        }
        let current_branch = self
            .run_git(
                &request.project_path,
                vec!["branch".to_owned(), "--show-current".to_owned()],
                "read current branch",
            )
            .await?;
        if current_branch.trim() != self.config.branch {
            return Err(PublishError::WrongBranch {
                expected: self.config.branch.clone(),
                actual: current_branch.trim().to_owned(),
            });
        }
        self.run_git(
            &request.project_path,
            vec![
                "remote".to_owned(),
                "get-url".to_owned(),
                self.config.remote.clone(),
            ],
            "read configured remote",
        )
        .await
        .map_err(|error| match error {
            PublishError::GitCommandFailed { .. } => {
                PublishError::MissingRemote(self.config.remote.clone())
            }
            error => error,
        })?;
        let current_state = RepositoryState::collect(&self.runner, &request.project_path)
            .await
            .map_err(PublishError::Repository)?;
        if current_state != request.accepted_repository_state {
            return Err(PublishError::RepositoryChangedAfterReview);
        }
        Ok(())
    }

    async fn verify_staged_state(&self, request: &PublishRequest) -> Result<(), PublishError> {
        let cached_stat = self
            .run_git(
                &request.project_path,
                vec![
                    "diff".to_owned(),
                    "--cached".to_owned(),
                    "--stat".to_owned(),
                ],
                "read staged diff stat",
            )
            .await?;
        let cached_diff = self
            .run_git(
                &request.project_path,
                vec![
                    "diff".to_owned(),
                    "--cached".to_owned(),
                    "--no-ext-diff".to_owned(),
                ],
                "read staged diff",
            )
            .await?;
        let cached_names = self
            .run_git(
                &request.project_path,
                vec![
                    "diff".to_owned(),
                    "--cached".to_owned(),
                    "--name-only".to_owned(),
                ],
                "read staged names",
            )
            .await?;
        let expected_names = request
            .accepted_repository_state
            .changed_files
            .iter()
            .chain(
                request
                    .accepted_repository_state
                    .untracked_files
                    .iter()
                    .map(|file| &file.path),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        let staged_names = cached_names
            .lines()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        if staged_names != expected_names
            || (!expected_names.is_empty()
                && (cached_stat.trim().is_empty() || cached_diff.trim().is_empty()))
        {
            return Err(PublishError::StagedStateMismatch);
        }

        if !request.accepted_repository_state.changed_files.is_empty() {
            let mut arguments = vec![
                "diff".to_owned(),
                "--cached".to_owned(),
                "--no-ext-diff".to_owned(),
                "--".to_owned(),
            ];
            arguments.extend(
                request
                    .accepted_repository_state
                    .changed_files
                    .iter()
                    .cloned(),
            );
            let staged_tracked_diff = self
                .run_git(&request.project_path, arguments, "read staged tracked diff")
                .await?;
            if staged_tracked_diff != request.accepted_repository_state.diff {
                return Err(PublishError::StagedStateMismatch);
            }
        }

        for file in &request.accepted_repository_state.untracked_files {
            let staged_blob = self
                .run_git(
                    &request.project_path,
                    vec!["rev-parse".to_owned(), format!(":{}", file.path)],
                    "read staged untracked file",
                )
                .await?;
            if staged_blob.trim() != file.blob_id {
                return Err(PublishError::StagedStateMismatch);
            }
        }
        Ok(())
    }
}

impl<R: ProcessRunner> Publisher for GitPublisher<R> {
    fn publish<'a>(
        &'a self,
        request: PublishRequest,
        progress: &'a mut dyn PublishProgress,
    ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>> {
        Box::pin(async move {
            progress.record(PublishStage::Verifying)?;
            self.verify_before_staging(&request).await?;
            progress.record(PublishStage::Staging)?;
            self.run_git(
                &request.project_path,
                vec!["add".to_owned(), "--all".to_owned()],
                "stage accepted changes",
            )
            .await?;
            self.verify_staged_state(&request).await?;
            progress.record(PublishStage::Staged)?;
            let current_head = self
                .run_git(
                    &request.project_path,
                    vec!["rev-parse".to_owned(), "HEAD".to_owned()],
                    "verify reviewed HEAD",
                )
                .await?;
            if current_head.trim() != request.accepted_repository_state.head {
                return Err(PublishError::RepositoryChangedAfterReview);
            }
            progress.record(PublishStage::Committing)?;
            self.run_git(
                &request.project_path,
                vec![
                    "-c".to_owned(),
                    format!("user.name={}", self.config.identity.name),
                    "-c".to_owned(),
                    format!("user.email={}", self.config.identity.email),
                    "commit".to_owned(),
                    "-m".to_owned(),
                    request.commit_title.clone(),
                ],
                "commit staged changes",
            )
            .await?;
            let commit_sha = self
                .run_git(
                    &request.project_path,
                    vec!["rev-parse".to_owned(), "HEAD".to_owned()],
                    "read committed SHA",
                )
                .await?
                .trim()
                .to_owned();
            progress.record(PublishStage::Committed)?;
            let result = PublishResult {
                commit_sha,
                commit_title: request.commit_title,
                remote: self.config.remote.clone(),
                branch: self.config.branch.clone(),
                push_status: PushStatus::Pushed,
            };
            progress.record(PublishStage::Pushing)?;
            match self
                .run_git(
                    &request.project_path,
                    vec![
                        "push".to_owned(),
                        result.remote.clone(),
                        result.branch.clone(),
                    ],
                    "push committed changes",
                )
                .await
            {
                Ok(_) => {
                    progress.record(PublishStage::Pushed)?;
                    Ok(result)
                }
                Err(PublishError::GitCommandFailed { .. }) => {
                    Err(PublishError::PushRejected(PublishResult {
                        push_status: PushStatus::Rejected,
                        ..result
                    }))
                }
                Err(error) => Err(error),
            }
        })
    }
}

#[derive(Debug)]
pub enum PublishError {
    PublishingDisabled,
    ProgressPersistence(String),
    MissingIdentity(&'static str),
    MissingRemoteConfiguration,
    MissingBranchConfiguration,
    InvalidCommitTitle,
    RepositorySnapshotIncomplete,
    RepositoryChangedAfterReview,
    WrongBranch {
        expected: String,
        actual: String,
    },
    MissingRemote(String),
    StagedStateMismatch,
    PushRejected(PublishResult),
    Repository(RepositoryError),
    Process(ProcessError),
    GitCommandFailed {
        operation: &'static str,
        exit_code: Option<i32>,
    },
}

impl fmt::Display for PublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PublishingDisabled => formatter.write_str("Git publication is disabled"),
            Self::ProgressPersistence(error) => {
                write!(formatter, "could not persist publication progress: {error}")
            }
            Self::MissingIdentity(variable) => write!(formatter, "{variable} must be configured"),
            Self::MissingRemoteConfiguration => formatter.write_str("a Git remote must be configured"),
            Self::MissingBranchConfiguration => formatter.write_str("LYA_GIT_BRANCH must be configured"),
            Self::InvalidCommitTitle => formatter.write_str("commit title must be one non-empty line"),
            Self::RepositorySnapshotIncomplete => formatter.write_str(
                "accepted repository snapshot contains truncated or binary content and cannot be published automatically",
            ),
            Self::RepositoryChangedAfterReview => {
                formatter.write_str("repository changed after Supervisor review")
            }
            Self::WrongBranch { expected, actual } => {
                write!(formatter, "current branch {actual:?} does not match configured branch {expected:?}")
            }
            Self::MissingRemote(remote) => write!(formatter, "configured Git remote does not exist: {remote}"),
            Self::StagedStateMismatch => formatter.write_str("staged state does not match the accepted repository snapshot"),
            Self::PushRejected(result) => write!(
                formatter,
                "push rejected after local commit {} to {}/{}",
                result.commit_sha, result.remote, result.branch
            ),
            Self::Repository(error) => write!(formatter, "could not inspect repository: {error}"),
            Self::Process(error) => write!(formatter, "could not run Git: {error}"),
            Self::GitCommandFailed { operation, exit_code } => write!(
                formatter,
                "Git operation {operation} failed with status {}",
                exit_code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
            ),
        }
    }
}

impl Error for PublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Process(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        future::Future,
        path::{Path, PathBuf},
        pin::Pin,
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        GitPublishConfig, GitPublisher, NoopPublishProgress, PublishError, PublishRequest,
        Publisher, PushStatus,
    };
    use crate::{
        orchestrator::repository::RepositoryState,
        process::{ProcessError, ProcessOutput, ProcessRunner, ProcessSpec, SystemProcessRunner},
    };

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn directories(name: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "lya-publisher-test-{}-{name}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let work = root.join("work");
        let remote = root.join("remote.git");
        fs::create_dir_all(&work).expect("working directory should be created");
        git(&work, &["init", "--initial-branch", "main"]);
        fs::write(work.join("hello.txt"), "hello\n").expect("initial file should be written");
        git(&work, &["add", "hello.txt"]);
        git(
            &work,
            &[
                "-c",
                "user.name=Initial",
                "-c",
                "user.email=initial@example.com",
                "commit",
                "-m",
                "Initial",
            ],
        );
        git(
            &root,
            &[
                "init",
                "--bare",
                "--initial-branch",
                "main",
                remote.to_str().expect("remote path should be UTF-8"),
            ],
        );
        git(
            &work,
            &[
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path should be UTF-8"),
            ],
        );
        git(&work, &["push", "origin", "main"]);
        (work, remote)
    }

    fn git(path: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(path)
            .output()
            .expect("Git should start");
        assert!(
            output.status.success(),
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("Git stdout should be UTF-8")
    }

    fn config() -> GitPublishConfig {
        GitPublishConfig::new("Test Bot", "bot@example.com", "origin", "main")
            .expect("test configuration should be valid")
    }

    async fn request(work: &Path, title: &str) -> PublishRequest {
        PublishRequest {
            project_path: work.to_owned(),
            accepted_repository_state: RepositoryState::collect(&SystemProcessRunner, work)
                .await
                .expect("state should collect"),
            commit_title: title.to_owned(),
        }
    }

    async fn publish<R: ProcessRunner>(
        publisher: &GitPublisher<R>,
        request: PublishRequest,
    ) -> Result<super::PublishResult, PublishError> {
        let mut progress = NoopPublishProgress;
        publisher.publish(request, &mut progress).await
    }

    fn clean_up(work: &Path) {
        let root = work.parent().expect("work should have a parent");
        fs::remove_dir_all(root).expect("test directory should be removed");
    }

    #[tokio::test]
    async fn commits_and_pushes_the_accepted_snapshot_with_configured_identity() {
        let (work, remote) = directories("happy");
        fs::write(work.join("hello.txt"), "hello\nLYA_PUBLISH_OK\n")
            .expect("change should be written");
        let publisher = GitPublisher::new(config());
        let result = publish(&publisher, request(&work, "Add publish marker").await)
            .await
            .expect("publication should succeed");

        assert_eq!(result.push_status, PushStatus::Pushed);
        assert_eq!(result.commit_title, "Add publish marker");
        assert_eq!(
            git(&work, &["show", "-s", "--format=%an|%ae|%cn|%ce", "HEAD"]).trim(),
            "Test Bot|bot@example.com|Test Bot|bot@example.com"
        );
        assert_eq!(
            git(&work, &["show", "-s", "--format=%B", "HEAD"]),
            "Add publish marker\n\n"
        );
        assert_eq!(
            git(
                remote.parent().expect("remote parent should exist"),
                &[
                    "--git-dir",
                    remote.to_str().expect("remote path should be UTF-8"),
                    "rev-parse",
                    "main"
                ],
            )
            .trim(),
            result.commit_sha
        );
        assert!(
            RepositoryState::collect(&SystemProcessRunner, &work)
                .await
                .expect("post-publish state should collect")
                .is_clean()
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn rejects_snapshot_changed_after_review_before_staging() {
        let (work, _) = directories("changed");
        fs::write(work.join("hello.txt"), "first change\n").expect("change should be written");
        let accepted = request(&work, "First").await;
        fs::write(work.join("hello.txt"), "different change\n")
            .expect("change should be rewritten");

        let publisher = GitPublisher::new(config());
        let error = publish(&publisher, accepted)
            .await
            .expect_err("changed snapshot must be rejected");

        assert!(matches!(error, PublishError::RepositoryChangedAfterReview));
        assert!(
            git(&work, &["diff", "--cached", "--name-only"])
                .trim()
                .is_empty()
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn commits_reviewed_untracked_file() {
        let (work, _) = directories("untracked");
        fs::write(work.join("new.txt"), "reviewed untracked content\n")
            .expect("new file should be written");
        let accepted = request(&work, "Add reviewed file").await;
        assert_eq!(accepted.accepted_repository_state.untracked_files.len(), 1);

        let publisher = GitPublisher::new(config());
        publish(&publisher, accepted)
            .await
            .expect("untracked file should publish");

        assert_eq!(
            git(&work, &["show", "HEAD:new.txt"]),
            "reviewed untracked content\n"
        );
        clean_up(&work);
    }

    #[derive(Clone)]
    struct MutatingRunner {
        work: PathBuf,
    }

    impl ProcessRunner for MutatingRunner {
        fn run(
            &self,
            spec: ProcessSpec,
        ) -> Pin<Box<dyn Future<Output = Result<ProcessOutput, ProcessError>> + Send + '_>>
        {
            let should_mutate = spec.args == ["add", "--all"];
            let work = self.work.clone();
            Box::pin(async move {
                if should_mutate {
                    fs::write(work.join("hello.txt"), "changed after review\n")
                        .expect("test mutation should succeed");
                }
                spec.run().await
            })
        }
    }

    #[tokio::test]
    async fn rejects_staged_state_that_differs_from_review() {
        let (work, _) = directories("staged-mismatch");
        fs::write(work.join("hello.txt"), "reviewed change\n").expect("change should be written");
        let accepted = request(&work, "Reviewed change").await;

        let publisher = GitPublisher::with_runner(MutatingRunner { work: work.clone() }, config());
        let error = publish(&publisher, accepted)
            .await
            .expect_err("staged mismatch must be rejected");

        assert!(matches!(error, PublishError::StagedStateMismatch));
        clean_up(&work);
    }

    #[tokio::test]
    async fn reports_hook_failure_without_pushing() {
        let (work, remote) = directories("hook");
        fs::write(work.join("hello.txt"), "hook change\n").expect("change should be written");
        fs::write(work.join(".git/hooks/pre-commit"), "#!/bin/sh\nexit 1\n")
            .expect("hook should be written");

        let publisher = GitPublisher::new(config());
        let error = publish(&publisher, request(&work, "Hook failure").await)
            .await
            .expect_err("hook should reject commit");

        assert!(matches!(
            error,
            PublishError::GitCommandFailed {
                operation: "commit staged changes",
                ..
            }
        ));
        assert_eq!(
            git(
                remote.parent().expect("remote parent should exist"),
                &[
                    "--git-dir",
                    remote.to_str().expect("remote path should be UTF-8"),
                    "log",
                    "--format=%s",
                    "main"
                ],
            )
            .trim(),
            "Initial"
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn keeps_local_commit_when_push_is_rejected() {
        let (work, remote) = directories("push-rejected");
        let other = work
            .parent()
            .expect("work should have parent")
            .join("other");
        git(
            work.parent().expect("work should have parent"),
            &[
                "clone",
                remote.to_str().expect("remote path should be UTF-8"),
                other.to_str().expect("other path should be UTF-8"),
            ],
        );
        fs::write(other.join("remote.txt"), "advance remote\n")
            .expect("remote change should be written");
        git(&other, &["add", "remote.txt"]);
        git(
            &other,
            &[
                "-c",
                "user.name=Other",
                "-c",
                "user.email=other@example.com",
                "commit",
                "-m",
                "Advance",
            ],
        );
        git(&other, &["push", "origin", "main"]);
        fs::write(work.join("hello.txt"), "local change\n")
            .expect("local change should be written");

        let publisher = GitPublisher::new(config());
        let error = publish(&publisher, request(&work, "Local change").await)
            .await
            .expect_err("outdated branch push should fail");

        let result = match error {
            PublishError::PushRejected(result) => result,
            error => panic!("expected push rejection, got {error}"),
        };
        assert_eq!(result.push_status, PushStatus::Rejected);
        assert_eq!(
            git(&work, &["show", "-s", "--format=%s", "HEAD"]).trim(),
            "Local change"
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn rejects_wrong_branch_and_missing_remote() {
        let (work, _) = directories("configuration");
        fs::write(work.join("hello.txt"), "configuration test\n")
            .expect("change should be written");
        let accepted = request(&work, "Configuration test").await;
        let wrong_branch = GitPublishConfig::new("Test", "test@example.com", "origin", "other")
            .expect("configuration should parse");
        let publisher = GitPublisher::new(wrong_branch);
        let error = publish(&publisher, accepted.clone())
            .await
            .expect_err("wrong branch must fail");
        assert!(matches!(error, PublishError::WrongBranch { .. }));
        let missing_remote = GitPublishConfig::new("Test", "test@example.com", "missing", "main")
            .expect("configuration should parse");
        let publisher = GitPublisher::new(missing_remote);
        let error = publish(&publisher, accepted)
            .await
            .expect_err("missing remote must fail");
        assert!(matches!(error, PublishError::MissingRemote(_)));
        assert!(matches!(
            GitPublishConfig::new("", "test@example.com", "origin", "main"),
            Err(PublishError::MissingIdentity("LYA_GIT_NAME"))
        ));
        assert!(matches!(
            GitPublishConfig::new("Test", "", "origin", "main"),
            Err(PublishError::MissingIdentity("LYA_GIT_EMAIL"))
        ));
        clean_up(&work);
    }
}
