use std::{
    error::Error,
    fmt, fs,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::process::{ProcessError, ProcessRunner, ProcessSpec};

pub const MAX_DIFF_BYTES: usize = 128 * 1024;
pub const MAX_UNTRACKED_BYTES: usize = MAX_DIFF_BYTES;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UntrackedFileState {
    pub path: String,
    pub size_bytes: usize,
    pub content: Option<String>,
    pub blob_id: String,
    pub is_binary: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryState {
    pub head: String,
    pub status_short: String,
    pub diff_stat: String,
    pub changed_files: Vec<String>,
    pub diff: String,
    pub diff_truncated: bool,
    pub diff_total_bytes: usize,
    pub untracked_files: Vec<UntrackedFileState>,
    pub untracked_total_bytes: usize,
    pub untracked_truncated: bool,
}

impl RepositoryState {
    pub async fn collect<R: ProcessRunner>(
        runner: &R,
        project_path: &Path,
    ) -> Result<Self, RepositoryError> {
        if !project_path.is_dir() {
            return Err(RepositoryError::InvalidProjectPath(project_path.to_owned()));
        }

        let repository_check = run_git(
            runner,
            project_path,
            &["rev-parse", "--is-inside-work-tree"],
        )
        .await?;
        if repository_check.trim() != "true" {
            return Err(RepositoryError::NotGitRepository(project_path.to_owned()));
        }
        let repository_root = PathBuf::from(
            run_git(runner, project_path, &["rev-parse", "--show-toplevel"])
                .await?
                .trim(),
        )
        .canonicalize()
        .map_err(|error| RepositoryError::ReadPath(project_path.to_owned(), error.to_string()))?;

        let head = run_git(runner, project_path, &["rev-parse", "HEAD"]).await?;
        let status_short = run_git(runner, project_path, &["status", "--short"]).await?;
        let diff_stat = run_git(runner, project_path, &["diff", "--stat"]).await?;
        let changed_files = run_git(runner, project_path, &["diff", "--name-only"])
            .await?
            .lines()
            .map(str::to_owned)
            .collect();
        let full_diff = run_git(runner, project_path, &["diff", "--no-ext-diff"]).await?;
        let diff_total_bytes = full_diff.len();
        let (diff, diff_truncated) = truncate_utf8(&full_diff, MAX_DIFF_BYTES);
        let untracked_paths = run_git(
            runner,
            &repository_root,
            &["ls-files", "--others", "--exclude-standard", "-z"],
        )
        .await?;
        let (untracked_files, untracked_total_bytes, untracked_truncated) =
            collect_untracked_files(runner, &repository_root, &untracked_paths).await?;

        Ok(Self {
            head: head.trim().to_owned(),
            status_short,
            diff_stat,
            changed_files,
            diff,
            diff_truncated,
            diff_total_bytes,
            untracked_files,
            untracked_total_bytes,
            untracked_truncated,
        })
    }

    pub fn is_clean(&self) -> bool {
        self.status_short.trim().is_empty()
    }

    pub fn is_complete_for_publication(&self) -> bool {
        !self.diff_truncated
            && !self.untracked_truncated
            && self.untracked_files.iter().all(|file| !file.is_binary)
    }

    pub fn render_for_supervisor(&self) -> String {
        let changed_files = if self.changed_files.is_empty() {
            "(none)".to_owned()
        } else {
            self.changed_files.join("\n")
        };
        let tracked_capture = if self.diff_truncated {
            format!(
                "TRUNCATED: captured {} of {} bytes; changed-file list and diff stat are complete.",
                self.diff.len(),
                self.diff_total_bytes
            )
        } else {
            "Complete diff captured.".to_owned()
        };
        let untracked_capture = if self.untracked_truncated {
            format!(
                "TRUNCATED: captured at most {} of {} bytes across untracked files.",
                MAX_UNTRACKED_BYTES, self.untracked_total_bytes
            )
        } else {
            "Complete untracked-file capture.".to_owned()
        };
        format!(
            "HEAD:\n{}\n\nGIT STATUS --SHORT:\n{}\n\nGIT DIFF --STAT:\n{}\n\nCHANGED FILES:\n{}\n\nTRACKED DIFF CAPTURE:\n{}\n\nTRACKED DIFF:\n{}\n\nUNTRACKED FILES CAPTURE:\n{}\n\nUNTRACKED FILES:\n{}",
            self.head,
            blank_as_none(&self.status_short),
            blank_as_none(&self.diff_stat),
            changed_files,
            tracked_capture,
            blank_as_none(&self.diff),
            untracked_capture,
            render_untracked_files(&self.untracked_files),
        )
    }
}

async fn collect_untracked_files<R: ProcessRunner>(
    runner: &R,
    repository_root: &Path,
    paths: &str,
) -> Result<(Vec<UntrackedFileState>, usize, bool), RepositoryError> {
    let mut files = Vec::new();
    let mut captured_bytes = 0;
    let mut total_bytes = 0;
    let mut any_truncated = false;

    for path in paths.split('\0').filter(|path| !path.is_empty()) {
        let resolved_path = safe_untracked_path(repository_root, path)?;
        let bytes = fs::read(&resolved_path)
            .map_err(|error| RepositoryError::ReadPath(resolved_path.clone(), error.to_string()))?;
        let size_bytes = bytes.len();
        total_bytes += size_bytes;
        let available_bytes = MAX_UNTRACKED_BYTES.saturating_sub(captured_bytes);
        let is_binary = std::str::from_utf8(&bytes).is_err();
        let (content, truncated) = if is_binary {
            (None, size_bytes > available_bytes)
        } else {
            let text = std::str::from_utf8(&bytes).expect("UTF-8 was checked");
            let (content, was_truncated) = truncate_utf8(text, available_bytes);
            (Some(content), was_truncated)
        };
        captured_bytes += size_bytes.min(available_bytes);
        any_truncated |= truncated;
        let blob_id = run_git(
            runner,
            repository_root,
            &["hash-object", "--no-filters", "--", path],
        )
        .await?
        .trim()
        .to_owned();
        files.push(UntrackedFileState {
            path: path.to_owned(),
            size_bytes,
            content,
            blob_id,
            is_binary,
            truncated,
        });
    }

    Ok((
        files,
        total_bytes,
        any_truncated || total_bytes > MAX_UNTRACKED_BYTES,
    ))
}

fn safe_untracked_path(repository_root: &Path, path: &str) -> Result<PathBuf, RepositoryError> {
    let relative = Path::new(path);
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(RepositoryError::UnsafeUntrackedPath(path.to_owned()));
    }
    let resolved = repository_root
        .join(relative)
        .canonicalize()
        .map_err(|error| {
            RepositoryError::ReadPath(repository_root.join(relative), error.to_string())
        })?;
    if !resolved.starts_with(repository_root) || !resolved.is_file() {
        return Err(RepositoryError::UnsafeUntrackedPath(path.to_owned()));
    }
    Ok(resolved)
}

fn render_untracked_files(files: &[UntrackedFileState]) -> String {
    if files.is_empty() {
        return "(none)".to_owned();
    }

    files
        .iter()
        .map(|file| {
            let capture = if file.is_binary {
                "BINARY / NON-UTF-8: content not rendered".to_owned()
            } else if file.truncated {
                format!(
                    "UTF-8 CONTENT (TRUNCATED):\n{}",
                    file.content.as_deref().unwrap_or("")
                )
            } else {
                format!("UTF-8 CONTENT:\n{}", file.content.as_deref().unwrap_or(""))
            };
            format!(
                "PATH: {}\nSIZE: {} bytes\n{}",
                file.path, file.size_bytes, capture
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

async fn run_git<R: ProcessRunner>(
    runner: &R,
    project_path: &Path,
    arguments: &[&str],
) -> Result<String, RepositoryError> {
    let mut spec = ProcessSpec::new("git");
    spec.args = arguments
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect();
    spec.cwd = Some(project_path.to_owned());
    let output = runner.run(spec).await.map_err(RepositoryError::Process)?;
    if output.exit_code != Some(0) {
        return Err(RepositoryError::CommandFailed {
            arguments: arguments
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
            exit_code: output.exit_code,
            stderr: output.stderr,
        });
    }
    Ok(output.stdout)
}

fn truncate_utf8(value: &str, limit: usize) -> (String, bool) {
    if value.len() <= limit {
        return (value.to_owned(), false);
    }
    let mut end = limit;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_owned(), true)
}

fn blank_as_none(value: &str) -> &str {
    if value.trim().is_empty() {
        "(none)"
    } else {
        value
    }
}

#[derive(Debug)]
pub enum RepositoryError {
    InvalidProjectPath(PathBuf),
    NotGitRepository(PathBuf),
    UnsafeUntrackedPath(String),
    ReadPath(PathBuf, String),
    Process(ProcessError),
    CommandFailed {
        arguments: Vec<String>,
        exit_code: Option<i32>,
        stderr: String,
    },
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProjectPath(path) => write!(
                formatter,
                "project path is not a directory: {}",
                path.display()
            ),
            Self::NotGitRepository(path) => write!(
                formatter,
                "project path is not a Git repository: {}",
                path.display()
            ),
            Self::UnsafeUntrackedPath(path) => {
                write!(
                    formatter,
                    "untracked path is outside the repository or unsafe: {path}"
                )
            }
            Self::ReadPath(path, error) => {
                write!(
                    formatter,
                    "could not read repository path {}: {error}",
                    path.display()
                )
            }
            Self::Process(error) => write!(formatter, "could not run Git: {error}"),
            Self::CommandFailed {
                arguments,
                exit_code,
                stderr,
            } => write!(
                formatter,
                "git {} exited with status {}: {}",
                arguments.join(" "),
                exit_code.map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
                stderr.trim()
            ),
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Process(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        MAX_DIFF_BYTES, RepositoryError, RepositoryState, safe_untracked_path, truncate_utf8,
    };
    use crate::process::SystemProcessRunner;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn repository(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lya-repository-test-{}-{name}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("repository directory should be created");
        git(&path, &["init", "--initial-branch", "main"]);
        fs::write(path.join("tracked.txt"), "initial\n").expect("tracked file should be written");
        git(&path, &["add", "tracked.txt"]);
        git(
            &path,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-m",
                "Initial",
            ],
        );
        path
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

    async fn collect(path: &Path) -> RepositoryState {
        RepositoryState::collect(&SystemProcessRunner, path)
            .await
            .expect("state should collect")
    }

    #[test]
    fn diff_truncation_preserves_utf8_boundaries() {
        let value = format!("{}é", "x".repeat(MAX_DIFF_BYTES - 1));
        let (truncated, was_truncated) = truncate_utf8(&value, MAX_DIFF_BYTES);

        assert!(was_truncated);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert_eq!(truncated.len(), MAX_DIFF_BYTES - 1);
    }

    #[tokio::test]
    async fn captures_new_utf8_text_file_for_supervisor_review() {
        let path = repository("text");
        fs::write(
            path.join("new-file.rs"),
            "pub const CAFE: &str = \"café\";\n",
        )
        .expect("new file should be written");

        let state = collect(&path).await;

        assert_eq!(state.untracked_files.len(), 1);
        assert_eq!(state.untracked_files[0].path, "new-file.rs");
        assert_eq!(
            state.untracked_files[0].content.as_deref(),
            Some("pub const CAFE: &str = \"café\";\n")
        );
        assert!(state.render_for_supervisor().contains("UNTRACKED FILES"));
        fs::remove_dir_all(path).expect("repository should be removed");
    }

    #[tokio::test]
    async fn captures_multiple_untracked_files() {
        let path = repository("multiple");
        fs::write(path.join("one.txt"), "one\n").expect("first file should be written");
        fs::write(path.join("two.txt"), "two\n").expect("second file should be written");

        let state = collect(&path).await;

        assert_eq!(state.untracked_files.len(), 2);
        assert_eq!(state.untracked_files[0].path, "one.txt");
        assert_eq!(state.untracked_files[1].path, "two.txt");
        fs::remove_dir_all(path).expect("repository should be removed");
    }

    #[tokio::test]
    async fn marks_binary_untracked_file_without_rendering_content() {
        let path = repository("binary");
        fs::write(path.join("image.bin"), [0, 159, 146, 150])
            .expect("binary file should be written");

        let state = collect(&path).await;

        assert!(state.untracked_files[0].is_binary);
        assert_eq!(state.untracked_files[0].content, None);
        assert!(!state.is_complete_for_publication());
        assert!(state.render_for_supervisor().contains("BINARY / NON-UTF-8"));
        fs::remove_dir_all(path).expect("repository should be removed");
    }

    #[tokio::test]
    async fn marks_large_untracked_file_as_truncated() {
        let path = repository("truncated");
        fs::write(path.join("large.txt"), "x".repeat(MAX_DIFF_BYTES + 1))
            .expect("large file should be written");

        let state = collect(&path).await;

        assert!(state.untracked_truncated);
        assert!(state.untracked_files[0].truncated);
        assert!(state.render_for_supervisor().contains("TRUNCATED"));
        assert!(!state.is_complete_for_publication());
        fs::remove_dir_all(path).expect("repository should be removed");
    }

    #[tokio::test]
    async fn excludes_ignored_files() {
        let path = repository("ignored");
        fs::write(path.join(".gitignore"), "ignored.txt\n").expect("ignore file should be written");
        git(&path, &["add", ".gitignore"]);
        git(
            &path,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-m",
                "Ignore",
            ],
        );
        fs::write(path.join("ignored.txt"), "secret\n").expect("ignored file should be written");

        let state = collect(&path).await;

        assert!(state.untracked_files.is_empty());
        fs::remove_dir_all(path).expect("repository should be removed");
    }

    #[test]
    fn rejects_untracked_path_traversal() {
        let root = std::env::temp_dir()
            .canonicalize()
            .expect("temp directory should resolve");

        let error = safe_untracked_path(&root, "../outside.txt")
            .expect_err("parent traversal must be rejected");

        assert!(matches!(error, RepositoryError::UnsafeUntrackedPath(_)));
    }
}
