use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::process::{ProcessError, ProcessRunner, ProcessSpec};

pub const MAX_DIFF_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryState {
    pub head: String,
    pub status_short: String,
    pub diff_stat: String,
    pub changed_files: Vec<String>,
    pub diff: String,
    pub diff_truncated: bool,
    pub diff_total_bytes: usize,
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

        Ok(Self {
            head: head.trim().to_owned(),
            status_short,
            diff_stat,
            changed_files,
            diff,
            diff_truncated,
            diff_total_bytes,
        })
    }

    pub fn is_clean(&self) -> bool {
        self.status_short.trim().is_empty()
    }

    pub fn render_for_supervisor(&self) -> String {
        let changed_files = if self.changed_files.is_empty() {
            "(none)".to_owned()
        } else {
            self.changed_files.join("\n")
        };
        let truncation = if self.diff_truncated {
            format!(
                "TRUNCATED: captured {} of {} bytes; changed-file list and diff stat are complete.",
                self.diff.len(),
                self.diff_total_bytes
            )
        } else {
            "Complete diff captured.".to_owned()
        };
        format!(
            "HEAD:\n{}\n\nGIT STATUS --SHORT:\n{}\n\nGIT DIFF --STAT:\n{}\n\nCHANGED FILES:\n{}\n\nDIFF CAPTURE:\n{}\n\nGIT DIFF:\n{}",
            self.head,
            blank_as_none(&self.status_short),
            blank_as_none(&self.diff_stat),
            changed_files,
            truncation,
            blank_as_none(&self.diff),
        )
    }
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
    use super::{MAX_DIFF_BYTES, RepositoryState, truncate_utf8};

    #[test]
    fn diff_truncation_preserves_utf8_boundaries() {
        let value = format!("{}é", "x".repeat(MAX_DIFF_BYTES - 1));
        let (truncated, was_truncated) = truncate_utf8(&value, MAX_DIFF_BYTES);

        assert!(was_truncated);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert_eq!(truncated.len(), MAX_DIFF_BYTES - 1);
    }

    #[test]
    fn rendered_state_exposes_explicit_truncation() {
        let state = RepositoryState {
            head: "abc123".to_owned(),
            status_short: " M hello.txt\n".to_owned(),
            diff_stat: " hello.txt | 1 +\n".to_owned(),
            changed_files: vec!["hello.txt".to_owned()],
            diff: "partial diff".to_owned(),
            diff_truncated: true,
            diff_total_bytes: MAX_DIFF_BYTES + 20,
        };

        let rendered = state.render_for_supervisor();

        assert!(rendered.contains("TRUNCATED"));
        assert!(rendered.contains("hello.txt"));
    }
}
