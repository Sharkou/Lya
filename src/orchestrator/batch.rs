//! The declarative job file `lya scheduler` consumes.
//!
//! Deliberately not a configuration language: one JSON object per line, two required fields, one
//! optional one. It exists so several project/task pairs can be given in a single invocation
//! without inventing shell quoting rules for multi-line tasks, and it stays small enough that the
//! whole grammar fits in this doc comment:
//!
//! ```text
//! # comments and blank lines are ignored
//! {"project": "/path/to/repository", "task": "Fix the flaky test"}
//! {"project": "../other", "task": "Update the changelog", "name": "Other"}
//! ```
//!
//! * `project` — path to the Git working tree. A relative path resolves against the directory of
//!   the job file itself, so a job file is portable with the paths it names.
//! * `task` — the task text, exactly as `lya run` would take it.
//! * `name` — optional display name; defaults to the directory name, like `lya run` does.
//!
//! Unknown fields are refused rather than ignored, so a typo is reported instead of silently
//! dropping an instruction.

use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use super::{scheduler::ScheduledRequest, supervisor::Project};

/// Bounded on purpose: a job file is a small batch, not a database.
pub const MAX_REQUESTS: usize = 256;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequest {
    project: String,
    task: String,
    #[serde(default)]
    name: Option<String>,
}

/// Parse a job file. `base` is the directory relative project paths resolve against.
pub fn parse_job_file(content: &str, base: &Path) -> Result<Vec<ScheduledRequest>, BatchError> {
    let mut requests = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let raw: RawRequest = serde_json::from_str(trimmed)
            .map_err(|error| BatchError::InvalidLine(number, error.to_string()))?;
        if raw.task.trim().is_empty() {
            return Err(BatchError::InvalidLine(
                number,
                "task cannot be empty".to_owned(),
            ));
        }
        if raw.project.trim().is_empty() {
            return Err(BatchError::InvalidLine(
                number,
                "project cannot be empty".to_owned(),
            ));
        }
        let requested = PathBuf::from(raw.project.trim());
        let resolved = if requested.is_absolute() {
            requested
        } else {
            base.join(requested)
        };
        // Resolved here so an unusable path is refused with its line number, before anything is
        // queued. The scheduler canonicalizes again when it derives repository identity.
        let path = resolved.canonicalize().map_err(|error| {
            BatchError::UnresolvableProject(number, resolved, error.to_string())
        })?;
        let name = raw
            .name
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| project_name(&path));
        requests.push(ScheduledRequest::new(Project { name, path }, raw.task));
        if requests.len() > MAX_REQUESTS {
            return Err(BatchError::TooManyRequests(MAX_REQUESTS));
        }
    }
    if requests.is_empty() {
        return Err(BatchError::Empty);
    }
    Ok(requests)
}

fn project_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("current-project")
        .to_owned()
}

#[derive(Debug)]
pub enum BatchError {
    Empty,
    InvalidLine(usize, String),
    UnresolvableProject(usize, PathBuf, String),
    TooManyRequests(usize),
}

impl fmt::Display for BatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("the job file contains no job"),
            Self::InvalidLine(line, error) => {
                write!(formatter, "job file line {line} is invalid: {error}")
            }
            Self::UnresolvableProject(line, path, error) => write!(
                formatter,
                "job file line {line} names a project that cannot be resolved ({}): {error}",
                path.display()
            ),
            Self::TooManyRequests(maximum) => write!(
                formatter,
                "a job file holds at most {maximum} jobs; split the batch"
            ),
        }
    }
}

impl Error for BatchError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{BatchError, parse_job_file};

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lya-batch-{name}-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("test directory should be created");
        path
    }

    #[test]
    fn parses_requests_and_ignores_comments_and_blank_lines() {
        let base = directory("parse");
        fs::create_dir_all(base.join("alpha")).expect("project should be created");
        fs::create_dir_all(base.join("beta")).expect("project should be created");
        let content = format!(
            "# a batch\n\n{{\"project\": \"alpha\", \"task\": \"Fix the flaky test\"}}\n{{\"project\": {}, \"task\": \"Update\", \"name\": \"Second\"}}\n",
            serde_json::Value::from(base.join("beta").display().to_string())
        );

        let requests = parse_job_file(&content, &base).expect("the job file should parse");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].project.name, "alpha");
        assert_eq!(requests[0].task, "Fix the flaky test");
        assert!(requests[0].job_id.is_none());
        assert_eq!(requests[1].project.name, "Second");
        assert_eq!(
            requests[1].project.path,
            base.join("beta")
                .canonicalize()
                .expect("project should canonicalize")
        );
        fs::remove_dir_all(base).expect("test directory should be removed");
    }

    #[test]
    fn relative_project_paths_resolve_against_the_job_file() {
        let base = directory("relative");
        let nested = base.join("workspace").join("inner");
        fs::create_dir_all(&nested).expect("project should be created");
        let job_file_directory = base.join("workspace");

        let requests = parse_job_file(
            "{\"project\": \"inner\", \"task\": \"Work\"}\n",
            &job_file_directory,
        )
        .expect("the job file should parse");

        assert_eq!(
            requests[0].project.path,
            nested.canonicalize().expect("project should canonicalize")
        );
        fs::remove_dir_all(base).expect("test directory should be removed");
    }

    #[test]
    fn reports_the_offending_line_for_every_refusal() {
        let base = directory("refusals");
        fs::create_dir_all(base.join("alpha")).expect("project should be created");

        let error = parse_job_file("{\"project\": \"alpha\"}\n", &base)
            .expect_err("a missing task should be refused");
        assert!(matches!(error, BatchError::InvalidLine(1, _)));

        let error = parse_job_file(
            "{\"project\": \"alpha\", \"task\": \"ok\"}\n{\"project\": \"alpha\", \"task\": \"\"}\n",
            &base,
        )
        .expect_err("an empty task should be refused");
        assert!(matches!(error, BatchError::InvalidLine(2, _)));

        let error = parse_job_file(
            "{\"project\": \"alpha\", \"task\": \"ok\", \"publish\": true}\n",
            &base,
        )
        .expect_err("an unknown field should be refused instead of ignored");
        assert!(error.to_string().contains("line 1"));

        let error = parse_job_file("{\"project\": \"absent\", \"task\": \"ok\"}\n", &base)
            .expect_err("an unresolvable project should be refused");
        assert!(matches!(error, BatchError::UnresolvableProject(1, ..)));

        let error =
            parse_job_file("# only comments\n\n", &base).expect_err("an empty batch is useless");
        assert!(matches!(error, BatchError::Empty));
        fs::remove_dir_all(base).expect("test directory should be removed");
    }
}
