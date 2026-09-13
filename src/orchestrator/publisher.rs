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

/// Everything a later process needs to decide whether an interrupted publication may continue.
/// `recorded_stage` and `recorded_result` come from authoritative job state, never from events.
#[derive(Debug, Clone)]
pub struct PublishRecoveryRequest {
    pub project_path: PathBuf,
    pub accepted_repository_state: RepositoryState,
    pub commit_title: String,
    pub recorded_stage: Option<PublishStage>,
    pub recorded_result: Option<PublishResult>,
}

impl PublishRecoveryRequest {
    fn into_publish_request(self) -> PublishRequest {
        PublishRequest {
            project_path: self.project_path,
            accepted_repository_state: self.accepted_repository_state,
            commit_title: self.commit_title,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PushStatus {
    /// The commit exists locally and has not been pushed yet.
    #[serde(rename = "PENDING")]
    Pending,
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

    /// Continue a publication that a previous process left unfinished.
    ///
    /// Every invariant is proven against the live repository before any Git write. Anything that
    /// cannot be proven is reported as [`PublishError::RecoveryAmbiguous`] so the caller can park
    /// the job for a human instead of guessing.
    fn recover<'a>(
        &'a self,
        request: PublishRecoveryRequest,
        progress: &'a mut dyn PublishProgress,
    ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>>;
}

pub trait PublishProgress: Send {
    fn record(&mut self, stage: PublishStage) -> Result<(), PublishError>;

    /// Durably record a commit that now exists locally.
    ///
    /// The publisher calls this immediately after `git commit` succeeds and before any further
    /// step, so a real commit can never exist without authoritative state describing its SHA,
    /// remote, branch and push state.
    fn record_commit(&mut self, result: &PublishResult) -> Result<(), PublishError>;

    fn stop_requested(&self) -> bool {
        false
    }
}

#[derive(Debug, Default)]
pub struct NoopPublishProgress;

impl PublishProgress for NoopPublishProgress {
    fn record(&mut self, _stage: PublishStage) -> Result<(), PublishError> {
        Ok(())
    }

    fn record_commit(&mut self, _result: &PublishResult) -> Result<(), PublishError> {
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
        self.verify_branch_and_remote(&request.project_path).await?;
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
        let expected_names = self.expected_published_paths(&request.accepted_repository_state);
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

    fn expected_published_paths(&self, state: &RepositoryState) -> BTreeSet<String> {
        state
            .changed_files
            .iter()
            .chain(state.untracked_files.iter().map(|file| &file.path))
            .cloned()
            .collect()
    }

    async fn verify_branch_and_remote(&self, project_path: &Path) -> Result<(), PublishError> {
        let current_branch = self
            .run_git(
                project_path,
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
            project_path,
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
        Ok(())
    }

    /// Create the commit and record it durably before doing anything else.
    async fn commit_accepted_state(
        &self,
        request: &PublishRequest,
        progress: &mut dyn PublishProgress,
    ) -> Result<PublishResult, PublishError> {
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
        let result = PublishResult {
            commit_sha,
            commit_title: request.commit_title.clone(),
            remote: self.config.remote.clone(),
            branch: self.config.branch.clone(),
            push_status: PushStatus::Pending,
        };
        progress.record_commit(&result)?;
        progress.record(PublishStage::Committed)?;
        Ok(result)
    }

    async fn push_commit(
        &self,
        project_path: &Path,
        result: PublishResult,
        progress: &mut dyn PublishProgress,
    ) -> Result<PublishResult, PublishError> {
        progress.record(PublishStage::Pushing)?;
        match self
            .run_git(
                project_path,
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
                let pushed = PublishResult {
                    push_status: PushStatus::Pushed,
                    ..result
                };
                progress.record_commit(&pushed)?;
                progress.record(PublishStage::Pushed)?;
                Ok(pushed)
            }
            Err(PublishError::GitCommandFailed { .. }) => {
                let rejected = PublishResult {
                    push_status: PushStatus::Rejected,
                    ..result
                };
                progress.record_commit(&rejected)?;
                Err(PublishError::PushRejected(rejected))
            }
            Err(error) => Err(error),
        }
    }

    /// Prove that the commit recorded in job state is exactly the commit Lya created from the
    /// accepted snapshot, without mutating Git in any way.
    async fn verify_recorded_commit(
        &self,
        request: &PublishRecoveryRequest,
        result: &PublishResult,
    ) -> Result<(), PublishError> {
        if result.remote != self.config.remote || result.branch != self.config.branch {
            return Err(PublishError::RecoveryAmbiguous(format!(
                "recorded publication targets {}/{} but the current configuration targets {}/{}",
                result.remote, result.branch, self.config.remote, self.config.branch
            )));
        }
        if result.commit_title != request.commit_title {
            return Err(PublishError::RecoveryAmbiguous(
                "recorded commit title does not match the accepted commit title".to_owned(),
            ));
        }
        self.verify_branch_and_remote(&request.project_path).await?;
        let head = self
            .run_git(
                &request.project_path,
                vec!["rev-parse".to_owned(), "HEAD".to_owned()],
                "read current HEAD",
            )
            .await?;
        if head.trim() != result.commit_sha {
            return Err(PublishError::RecoveryAmbiguous(format!(
                "HEAD is {} but the recorded commit is {}",
                head.trim(),
                result.commit_sha
            )));
        }
        let parents = self
            .run_git(
                &request.project_path,
                vec![
                    "rev-list".to_owned(),
                    "--parents".to_owned(),
                    "-n".to_owned(),
                    "1".to_owned(),
                    "HEAD".to_owned(),
                ],
                "read commit parents",
            )
            .await?;
        let mut parts = parents.split_whitespace();
        let _commit = parts.next();
        let parents = parts.collect::<Vec<_>>();
        if parents.len() != 1 || parents[0] != request.accepted_repository_state.head {
            return Err(PublishError::RecoveryAmbiguous(format!(
                "recorded commit does not have the accepted snapshot {} as its only parent",
                request.accepted_repository_state.head
            )));
        }
        let subject = self
            .run_git(
                &request.project_path,
                vec!["log".to_owned(), "-1".to_owned(), "--format=%s".to_owned()],
                "read commit subject",
            )
            .await?;
        if subject.trim() != request.commit_title.trim() {
            return Err(PublishError::RecoveryAmbiguous(
                "the commit at HEAD does not carry the accepted commit title".to_owned(),
            ));
        }
        let committed_names = self
            .run_git(
                &request.project_path,
                vec![
                    "diff".to_owned(),
                    "--name-only".to_owned(),
                    "HEAD~1".to_owned(),
                    "HEAD".to_owned(),
                ],
                "read committed paths",
            )
            .await?
            .lines()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let expected_names = self.expected_published_paths(&request.accepted_repository_state);
        if committed_names != expected_names {
            return Err(PublishError::RecoveryAmbiguous(
                "the commit at HEAD does not contain exactly the accepted paths".to_owned(),
            ));
        }
        let current_state = RepositoryState::collect(&self.runner, &request.project_path)
            .await
            .map_err(PublishError::Repository)?;
        if !current_state.is_clean() {
            return Err(PublishError::RecoveryAmbiguous(
                "the working tree is not clean, so the recorded commit cannot be confirmed as the only change".to_owned(),
            ));
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
            if progress.stop_requested() {
                return Err(PublishError::Stopped);
            }
            progress.record(PublishStage::Verifying)?;
            self.verify_before_staging(&request).await?;
            if progress.stop_requested() {
                return Err(PublishError::Stopped);
            }
            progress.record(PublishStage::Staging)?;
            self.run_git(
                &request.project_path,
                vec!["add".to_owned(), "--all".to_owned()],
                "stage accepted changes",
            )
            .await?;
            self.verify_staged_state(&request).await?;
            if progress.stop_requested() {
                return Err(PublishError::Stopped);
            }
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
            if progress.stop_requested() {
                return Err(PublishError::Stopped);
            }
            let result = self.commit_accepted_state(&request, progress).await?;
            if progress.stop_requested() {
                return Err(PublishError::Stopped);
            }
            self.push_commit(&request.project_path, result, progress)
                .await
        })
    }

    fn recover<'a>(
        &'a self,
        request: PublishRecoveryRequest,
        progress: &'a mut dyn PublishProgress,
    ) -> Pin<Box<dyn Future<Output = Result<PublishResult, PublishError>> + Send + 'a>> {
        Box::pin(async move {
            if progress.stop_requested() {
                return Err(PublishError::Stopped);
            }
            if !request
                .accepted_repository_state
                .is_complete_for_publication()
            {
                return Err(PublishError::RepositorySnapshotIncomplete);
            }
            if let Some(result) = request.recorded_result.clone() {
                if result.push_status == PushStatus::Pushed {
                    return Ok(result);
                }
                progress.record(PublishStage::Verifying)?;
                self.verify_recorded_commit(&request, &result).await?;
                if progress.stop_requested() {
                    return Err(PublishError::Stopped);
                }
                let project_path = request.project_path.clone();
                return self.push_commit(&project_path, result, progress).await;
            }

            // No commit was recorded, so no commit may exist. Prove HEAD never moved before
            // considering any continuation.
            self.verify_branch_and_remote(&request.project_path).await?;
            let head = self
                .run_git(
                    &request.project_path,
                    vec!["rev-parse".to_owned(), "HEAD".to_owned()],
                    "read current HEAD",
                )
                .await?;
            if head.trim() != request.accepted_repository_state.head {
                return Err(PublishError::RecoveryAmbiguous(format!(
                    "HEAD is {} but no commit was recorded for the accepted snapshot {}",
                    head.trim(),
                    request.accepted_repository_state.head
                )));
            }
            match request.recorded_stage {
                Some(PublishStage::Committed | PublishStage::Pushing | PublishStage::Pushed) => {
                    Err(PublishError::RecoveryAmbiguous(
                        "a commit stage was recorded without an authoritative commit".to_owned(),
                    ))
                }
                Some(PublishStage::Staging | PublishStage::Staged | PublishStage::Committing) => {
                    let publish_request = request.into_publish_request();
                    match self.verify_before_staging(&publish_request).await {
                        // Nothing was staged yet: the ordinary guarded sequence applies.
                        Ok(()) => {
                            self.publish_staged_sequence(publish_request, progress)
                                .await
                        }
                        Err(PublishError::RepositoryChangedAfterReview) => {
                            // The index may already hold exactly the accepted change.
                            self.verify_staged_state(&publish_request)
                                .await
                                .map_err(|_| {
                                    PublishError::RecoveryAmbiguous(
                                        "the working tree and the index both differ from the accepted snapshot".to_owned(),
                                    )
                                })?;
                            if progress.stop_requested() {
                                return Err(PublishError::Stopped);
                            }
                            progress.record(PublishStage::Staged)?;
                            let result = self
                                .commit_accepted_state(&publish_request, progress)
                                .await?;
                            if progress.stop_requested() {
                                return Err(PublishError::Stopped);
                            }
                            self.push_commit(&publish_request.project_path, result, progress)
                                .await
                        }
                        Err(error) => Err(error),
                    }
                }
                None | Some(PublishStage::Verifying) => {
                    self.publish(request.into_publish_request(), progress).await
                }
            }
        })
    }
}

impl<R: ProcessRunner> GitPublisher<R> {
    /// The guarded sequence from staging onwards, shared by a fresh publication and a recovery
    /// that proved the repository still matches the accepted snapshot.
    async fn publish_staged_sequence(
        &self,
        request: PublishRequest,
        progress: &mut dyn PublishProgress,
    ) -> Result<PublishResult, PublishError> {
        if progress.stop_requested() {
            return Err(PublishError::Stopped);
        }
        progress.record(PublishStage::Staging)?;
        self.run_git(
            &request.project_path,
            vec!["add".to_owned(), "--all".to_owned()],
            "stage accepted changes",
        )
        .await?;
        self.verify_staged_state(&request).await?;
        if progress.stop_requested() {
            return Err(PublishError::Stopped);
        }
        progress.record(PublishStage::Staged)?;
        let result = self.commit_accepted_state(&request, progress).await?;
        if progress.stop_requested() {
            return Err(PublishError::Stopped);
        }
        self.push_commit(&request.project_path, result, progress)
            .await
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
    Stopped,
    RecoveryAmbiguous(String),
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
            Self::Stopped => formatter.write_str("publication stopped by user request"),
            Self::RecoveryAmbiguous(reason) => write!(
                formatter,
                "interrupted publication cannot be recovered safely: {reason}"
            ),
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
        GitPublishConfig, GitPublisher, NoopPublishProgress, PublishError, PublishProgress,
        PublishRecoveryRequest, PublishRequest, PublishResult, PublishStage, Publisher, PushStatus,
    };
    use crate::{
        orchestrator::repository::RepositoryState,
        process::{ProcessError, ProcessOutput, ProcessRunner, ProcessSpec, SystemProcessRunner},
    };

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    /// Install a Git hook that always rejects.
    ///
    /// Two details decide whether Git runs a hook at all, and getting either wrong makes it skip
    /// the hook *silently* — which then looks like a publisher that ignores hooks rather than a
    /// fixture that never installed one:
    ///
    /// * **the executable bit.** Git on Linux and macOS runs a hook only when the file is
    ///   executable, and says nothing when it is not. Windows Git ignores the mode entirely, which
    ///   is why a fixture missing this passes there and fails on both Unix platforms.
    /// * **the line endings.** The `\n` escapes are written literally and never translated, because
    ///   `#!/bin/sh\r` names an interpreter no Unix kernel can execute.
    ///
    /// `#!/bin/sh` is the one interpreter both Unix platforms are guaranteed to have, and `exit 1`
    /// is the whole script, so nothing here depends on a particular shell's features.
    ///
    /// `.git/hooks` is created rather than assumed: `git init` does make it, but a template
    /// directory or a `core.hooksPath` setting can mean it is not the directory in use.
    fn write_rejecting_hook(work: &Path, name: &str) {
        let hooks = work.join(".git").join("hooks");
        fs::create_dir_all(&hooks).expect("hook directory should be created");
        let hook = hooks.join(name);
        fs::write(&hook, "#!/bin/sh\nexit 1\n").expect("hook should be written");
        arm_hook(&hook);
    }

    /// Make a hook runnable by Git on Unix, and prove it took.
    #[cfg(unix)]
    fn arm_hook(hook: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(hook)
            .expect("the hook should exist")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(hook, permissions).expect("the hook should become executable");

        let mode = fs::metadata(hook)
            .expect("the hook should exist")
            .permissions()
            .mode();
        assert!(
            mode & 0o100 != 0,
            "Git silently skips a hook the owner cannot execute, and the commit would succeed: {mode:o}"
        );
    }

    /// Windows Git runs a hook through its own bundled shell and ignores the file mode, so there is
    /// nothing to set — only the file's presence to confirm.
    #[cfg(not(unix))]
    fn arm_hook(hook: &Path) {
        assert!(hook.is_file(), "the hook should exist: {}", hook.display());
    }

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
        write_rejecting_hook(&work, "pre-commit");

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
        // No commit locally, so the failure really was the hook refusing this commit rather than
        // something later in the sequence.
        assert_eq!(
            git(&work, &["log", "--format=%s", "-1", "main"]).trim(),
            "Initial",
            "a rejected hook must leave no commit behind"
        );
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
            "Initial",
            "nothing may be pushed when the commit was rejected"
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

    #[derive(Debug, Default)]
    struct RecordingProgress {
        stages: Vec<PublishStage>,
        commits: Vec<PublishResult>,
        stop_after: Option<PublishStage>,
        stopped: bool,
    }

    impl RecordingProgress {
        fn stopping_after(stage: PublishStage) -> Self {
            Self {
                stop_after: Some(stage),
                ..Self::default()
            }
        }
    }

    impl PublishProgress for RecordingProgress {
        fn record(&mut self, stage: PublishStage) -> Result<(), PublishError> {
            if self.stop_after.as_ref() == Some(&stage) {
                self.stopped = true;
            }
            self.stages.push(stage);
            Ok(())
        }

        fn record_commit(&mut self, result: &PublishResult) -> Result<(), PublishError> {
            self.commits.push(result.clone());
            Ok(())
        }

        fn stop_requested(&self) -> bool {
            self.stopped
        }
    }

    fn remote_head(remote: &Path) -> String {
        git(
            remote.parent().expect("remote parent should exist"),
            &[
                "--git-dir",
                remote.to_str().expect("remote path should be UTF-8"),
                "rev-parse",
                "main",
            ],
        )
        .trim()
        .to_owned()
    }

    fn commit_count(work: &Path) -> usize {
        git(work, &["rev-list", "--count", "HEAD"])
            .trim()
            .parse()
            .expect("commit count should parse")
    }

    async fn recovery(
        work: &Path,
        accepted: &PublishRequest,
        stage: Option<PublishStage>,
        result: Option<PublishResult>,
    ) -> PublishRecoveryRequest {
        PublishRecoveryRequest {
            project_path: work.to_owned(),
            accepted_repository_state: accepted.accepted_repository_state.clone(),
            commit_title: accepted.commit_title.clone(),
            recorded_stage: stage,
            recorded_result: result,
        }
    }

    #[tokio::test]
    async fn a_commit_is_recorded_before_any_later_publication_step() {
        let (work, _) = directories("record-commit");
        fs::write(work.join("hello.txt"), "recorded commit\n").expect("change should be written");
        let publisher = GitPublisher::new(config());
        let mut progress = RecordingProgress::default();

        publisher
            .publish(request(&work, "Recorded commit").await, &mut progress)
            .await
            .expect("publication should succeed");

        let first_commit = progress
            .commits
            .first()
            .expect("a commit must be recorded")
            .clone();
        assert_eq!(first_commit.push_status, PushStatus::Pending);
        assert_eq!(
            first_commit.commit_sha,
            git(&work, &["rev-parse", "HEAD"]).trim()
        );
        assert_eq!(first_commit.remote, "origin");
        assert_eq!(first_commit.branch, "main");
        let committed_index = progress
            .stages
            .iter()
            .position(|stage| stage == &PublishStage::Committed)
            .expect("the committed stage should be recorded");
        assert!(
            progress.stages[..committed_index]
                .iter()
                .all(|stage| stage != &PublishStage::Pushing),
            "no push stage may precede the recorded commit"
        );
        assert_eq!(
            progress.commits.last().expect("final state").push_status,
            PushStatus::Pushed
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn a_stop_between_committing_and_pushing_still_records_the_existing_commit() {
        let (work, remote) = directories("stop-after-commit");
        let remote_before = remote_head(&remote);
        fs::write(work.join("hello.txt"), "committed not pushed\n")
            .expect("change should be written");
        let publisher = GitPublisher::new(config());
        let mut progress = RecordingProgress::stopping_after(PublishStage::Committed);

        let error = publisher
            .publish(
                request(&work, "Committed but not pushed").await,
                &mut progress,
            )
            .await
            .expect_err("a stop must interrupt publication");

        assert!(matches!(error, PublishError::Stopped));
        let recorded = progress
            .commits
            .last()
            .expect("the commit must be recorded even though the push never happened");
        assert_eq!(recorded.push_status, PushStatus::Pending);
        assert_eq!(
            recorded.commit_sha,
            git(&work, &["rev-parse", "HEAD"]).trim()
        );
        assert_eq!(recorded.remote, "origin");
        assert_eq!(recorded.branch, "main");
        assert_eq!(remote_head(&remote), remote_before);
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_pushes_a_recorded_commit_without_creating_a_second_one() {
        let (work, remote) = directories("recover-push");
        fs::write(work.join("hello.txt"), "recover me\n").expect("change should be written");
        let accepted = request(&work, "Recover this commit").await;
        let publisher = GitPublisher::new(config());
        let mut interrupted = RecordingProgress::stopping_after(PublishStage::Committed);
        publisher
            .publish(accepted.clone(), &mut interrupted)
            .await
            .expect_err("publication is interrupted on purpose");
        let recorded = interrupted
            .commits
            .last()
            .expect("commit should be recorded")
            .clone();
        let commits_before = commit_count(&work);

        let mut progress = RecordingProgress::default();
        let result = publisher
            .recover(
                recovery(
                    &work,
                    &accepted,
                    Some(PublishStage::Committed),
                    Some(recorded.clone()),
                )
                .await,
                &mut progress,
            )
            .await
            .expect("a proven commit should continue at push");

        assert_eq!(result.push_status, PushStatus::Pushed);
        assert_eq!(result.commit_sha, recorded.commit_sha);
        assert_eq!(commit_count(&work), commits_before);
        assert_eq!(remote_head(&remote), recorded.commit_sha);
        assert!(
            !progress.stages.contains(&PublishStage::Committing),
            "recovery must not commit again"
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_refuses_a_commit_that_does_not_match_the_recorded_state() {
        let (work, remote) = directories("recover-ambiguous");
        fs::write(work.join("hello.txt"), "ambiguous\n").expect("change should be written");
        let accepted = request(&work, "Ambiguous recovery").await;
        let publisher = GitPublisher::new(config());
        let mut interrupted = RecordingProgress::stopping_after(PublishStage::Committed);
        publisher
            .publish(accepted.clone(), &mut interrupted)
            .await
            .expect_err("publication is interrupted on purpose");
        let mut recorded = interrupted
            .commits
            .last()
            .expect("commit should be recorded")
            .clone();
        recorded.commit_sha = "0000000000000000000000000000000000000000".to_owned();
        let head_before = git(&work, &["rev-parse", "HEAD"]).trim().to_owned();
        let remote_before = remote_head(&remote);

        let mut progress = RecordingProgress::default();
        let error = publisher
            .recover(
                recovery(
                    &work,
                    &accepted,
                    Some(PublishStage::Committed),
                    Some(recorded),
                )
                .await,
                &mut progress,
            )
            .await
            .expect_err("an unproven commit must be refused");

        assert!(matches!(error, PublishError::RecoveryAmbiguous(_)));
        assert_eq!(git(&work, &["rev-parse", "HEAD"]).trim(), head_before);
        assert_eq!(remote_head(&remote), remote_before);
        assert!(
            !progress.stages.contains(&PublishStage::Pushing),
            "no Git write may happen before recovery is proven"
        );
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_refuses_an_unrecorded_commit_that_moved_head() {
        let (work, remote) = directories("recover-unrecorded");
        fs::write(work.join("hello.txt"), "unrecorded\n").expect("change should be written");
        let accepted = request(&work, "Unrecorded commit").await;
        git(&work, &["add", "--all"]);
        git(
            &work,
            &[
                "-c",
                "user.name=Someone",
                "-c",
                "user.email=someone@example.com",
                "commit",
                "-m",
                "Unrecorded commit",
            ],
        );
        let head_before = git(&work, &["rev-parse", "HEAD"]).trim().to_owned();
        let remote_before = remote_head(&remote);
        let publisher = GitPublisher::new(config());

        let mut progress = RecordingProgress::default();
        let error = publisher
            .recover(
                recovery(&work, &accepted, Some(PublishStage::Committing), None).await,
                &mut progress,
            )
            .await
            .expect_err("a commit without authoritative state must be refused");

        assert!(matches!(error, PublishError::RecoveryAmbiguous(_)));
        assert_eq!(git(&work, &["rev-parse", "HEAD"]).trim(), head_before);
        assert_eq!(remote_head(&remote), remote_before);
        assert!(progress.commits.is_empty());
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_before_any_commit_runs_the_whole_guarded_sequence() {
        let (work, remote) = directories("recover-verifying");
        fs::write(work.join("hello.txt"), "restart cleanly\n").expect("change should be written");
        let accepted = request(&work, "Restart cleanly").await;
        let publisher = GitPublisher::new(config());

        let mut progress = RecordingProgress::default();
        let result = publisher
            .recover(
                recovery(&work, &accepted, Some(PublishStage::Verifying), None).await,
                &mut progress,
            )
            .await
            .expect("an untouched repository restarts the guarded sequence");

        assert_eq!(result.push_status, PushStatus::Pushed);
        assert_eq!(remote_head(&remote), result.commit_sha);
        assert!(progress.stages.contains(&PublishStage::Staging));
        assert!(progress.stages.contains(&PublishStage::Committing));
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_continues_from_an_index_that_matches_the_accepted_snapshot() {
        let (work, remote) = directories("recover-staged");
        fs::write(work.join("hello.txt"), "already staged\n").expect("change should be written");
        let accepted = request(&work, "Already staged").await;
        git(&work, &["add", "--all"]);
        let publisher = GitPublisher::new(config());

        let mut progress = RecordingProgress::default();
        let result = publisher
            .recover(
                recovery(&work, &accepted, Some(PublishStage::Staging), None).await,
                &mut progress,
            )
            .await
            .expect("a matching index should continue at commit");

        assert_eq!(result.push_status, PushStatus::Pushed);
        assert_eq!(remote_head(&remote), result.commit_sha);
        assert_eq!(git(&work, &["show", "HEAD:hello.txt"]), "already staged\n");
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_refuses_an_index_that_no_longer_matches_the_accepted_snapshot() {
        let (work, remote) = directories("recover-staged-mismatch");
        fs::write(work.join("hello.txt"), "accepted content\n").expect("change should be written");
        let accepted = request(&work, "Accepted content").await;
        fs::write(work.join("hello.txt"), "something else entirely\n")
            .expect("change should be rewritten");
        git(&work, &["add", "--all"]);
        let head_before = git(&work, &["rev-parse", "HEAD"]).trim().to_owned();
        let remote_before = remote_head(&remote);
        let publisher = GitPublisher::new(config());

        let mut progress = RecordingProgress::default();
        let error = publisher
            .recover(
                recovery(&work, &accepted, Some(PublishStage::Staged), None).await,
                &mut progress,
            )
            .await
            .expect_err("a divergent index must be refused");

        assert!(matches!(error, PublishError::RecoveryAmbiguous(_)));
        assert_eq!(git(&work, &["rev-parse", "HEAD"]).trim(), head_before);
        assert_eq!(remote_head(&remote), remote_before);
        assert!(progress.commits.is_empty());
        clean_up(&work);
    }

    #[tokio::test]
    async fn recovery_of_an_already_pushed_commit_changes_nothing() {
        let (work, remote) = directories("recover-published");
        fs::write(work.join("hello.txt"), "already pushed\n").expect("change should be written");
        let accepted = request(&work, "Already pushed").await;
        let publisher = GitPublisher::new(config());
        let published = publish(&publisher, accepted.clone())
            .await
            .expect("publication should succeed");
        let commits_before = commit_count(&work);

        let mut progress = RecordingProgress::default();
        let result = publisher
            .recover(
                recovery(
                    &work,
                    &accepted,
                    Some(PublishStage::Pushed),
                    Some(published.clone()),
                )
                .await,
                &mut progress,
            )
            .await
            .expect("an already pushed commit is already complete");

        assert_eq!(result, published);
        assert_eq!(commit_count(&work), commits_before);
        assert_eq!(remote_head(&remote), published.commit_sha);
        assert!(progress.stages.is_empty());
        clean_up(&work);
    }
}
