use std::{
    collections::BTreeMap, error::Error, ffi::OsString, fmt, path::PathBuf, process::Stdio,
    time::Duration,
};

use tokio::{io::AsyncWriteExt, process::Command};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    pub program: OsString,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub env_remove: Vec<String>,
    pub stdin: Option<String>,
    pub timeout: Option<Duration>,
}

impl ProcessSpec {
    pub fn new(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            env_remove: Vec::new(),
            stdin: None,
            timeout: None,
        }
    }

    pub async fn run(&self) -> Result<ProcessOutput, ProcessError> {
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        command.envs(&self.env);
        for name in &self.env_remove {
            command.env_remove(name);
        }
        if self.stdin.is_some() {
            command.stdin(Stdio::piped());
        }

        let mut child = command
            .spawn()
            .map_err(|error| ProcessError::Start(error.to_string()))?;
        let stdin = child.stdin.take();
        let input = self.stdin.clone();
        let collect = async move {
            let write_stdin = async move {
                if let (Some(mut stdin), Some(input)) = (stdin, input) {
                    stdin
                        .write_all(input.as_bytes())
                        .await
                        .map_err(|error| ProcessError::Stdin(error.to_string()))?;
                    stdin
                        .shutdown()
                        .await
                        .map_err(|error| ProcessError::Stdin(error.to_string()))?;
                }
                Ok::<(), ProcessError>(())
            };
            let wait_for_output = async move {
                child
                    .wait_with_output()
                    .await
                    .map_err(|error| ProcessError::Wait(error.to_string()))
            };

            let (_, output) = tokio::try_join!(write_stdin, wait_for_output)?;
            Ok(ProcessOutput {
                exit_code: output.status.code(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        };

        match self.timeout {
            Some(timeout) => tokio::time::timeout(timeout, collect)
                .await
                .map_err(|_| ProcessError::Timeout(timeout))?,
            None => collect.await,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOutput {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug)]
pub enum ProcessError {
    Start(String),
    Stdin(String),
    Wait(String),
    Timeout(Duration),
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start(error) => write!(formatter, "could not start process: {error}"),
            Self::Stdin(error) => write!(formatter, "could not write process stdin: {error}"),
            Self::Wait(error) => write!(formatter, "could not wait for process: {error}"),
            Self::Timeout(timeout) => {
                write!(
                    formatter,
                    "process timed out after {} seconds",
                    timeout.as_secs_f64()
                )
            }
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, time::Duration};

    use super::{ProcessError, ProcessSpec};

    #[cfg(unix)]
    fn spec(program: &str, args: &[&str]) -> ProcessSpec {
        let mut spec = ProcessSpec::new(program);
        spec.args = args.iter().map(|argument| (*argument).to_owned()).collect();
        spec
    }

    #[cfg(windows)]
    fn spec(_program: &str, args: &[&str]) -> ProcessSpec {
        let mut spec = ProcessSpec::new("cmd.exe");
        spec.args = args.iter().map(|argument| (*argument).to_owned()).collect();
        spec
    }

    #[tokio::test]
    async fn preserves_arguments_with_spaces() {
        #[cfg(unix)]
        let spec = spec("printf", &["%s", "one value with spaces"]);
        #[cfg(windows)]
        let spec = spec("cmd.exe", &["/C", "echo", "one value with spaces"]);

        let output = spec.run().await.expect("process should succeed");

        assert_eq!(output.exit_code, Some(0));
        assert!(output.stdout.contains("one value with spaces"));
    }

    #[tokio::test]
    async fn uses_requested_working_directory() {
        let directory = env::temp_dir()
            .canonicalize()
            .expect("temporary directory should exist");
        #[cfg(unix)]
        let mut spec = spec("pwd", &[]);
        #[cfg(windows)]
        let mut spec = spec("cmd.exe", &["/C", "cd"]);
        spec.cwd = Some(directory.clone());

        let output = spec.run().await.expect("process should succeed");
        let expected_directory = directory
            .to_string_lossy()
            .trim_start_matches(r"\\?\")
            .to_owned();

        assert!(
            output.stdout.contains(&expected_directory),
            "process reported cwd: {}",
            output.stdout
        );
    }

    #[tokio::test]
    async fn captures_stdout_stderr_and_non_zero_exit_codes() {
        #[cfg(unix)]
        let spec = spec("sh", &["-c", "printf 'expected stderr' >&2; exit 7"]);
        #[cfg(windows)]
        let spec = spec("cmd.exe", &["/C", "echo expected stderr 1>&2 & exit /b 7"]);
        let output = spec.run().await.expect("process should run");

        assert_eq!(output.exit_code, Some(7));
        assert!(output.stderr.contains("expected stderr"));
    }

    #[tokio::test]
    async fn accepts_stdin_and_injected_environment() {
        #[cfg(unix)]
        let mut stdin = spec("cat", &[]);
        #[cfg(windows)]
        let mut stdin = spec("cmd.exe", &["/C", "findstr .*"]);
        stdin.stdin = Some("hello Lya\n".to_owned());
        let stdin_output = stdin.run().await.expect("process should succeed");
        assert!(stdin_output.stdout.contains("hello Lya"));

        #[cfg(unix)]
        let mut environment = spec("sh", &["-c", "printf '%s' \"$LYA_PROCESS_TEST_VALUE\""]);
        #[cfg(windows)]
        let mut environment = spec("cmd.exe", &["/C", "echo %LYA_PROCESS_TEST_VALUE%"]);
        environment.env = BTreeMap::from([(
            "LYA_PROCESS_TEST_VALUE".to_owned(),
            "injected value".to_owned(),
        )]);
        let environment_output = environment.run().await.expect("process should succeed");
        assert!(environment_output.stdout.contains("injected value"));
    }

    #[tokio::test]
    async fn removes_environment_variables_from_child() {
        #[cfg(unix)]
        let mut spec = spec(
            "sh",
            &[
                "-c",
                "if [ -z \"${LYA_PROCESS_TEST_VALUE+x}\" ]; then printf missing; else printf present; fi",
            ],
        );
        #[cfg(windows)]
        let mut spec = spec(
            "cmd.exe",
            &[
                "/C",
                "if defined LYA_PROCESS_TEST_VALUE (echo present) else (echo missing)",
            ],
        );
        spec.env_remove.push("LYA_PROCESS_TEST_VALUE".to_owned());
        spec.env.insert(
            "LYA_PROCESS_TEST_VALUE".to_owned(),
            "injected then removed".to_owned(),
        );

        let output = spec.run().await.expect("process should succeed");

        assert!(output.stdout.contains("missing"));
    }

    #[tokio::test]
    async fn reports_timeout() {
        #[cfg(unix)]
        let mut spec = spec("sleep", &["2"]);
        #[cfg(windows)]
        let mut spec = spec("timeout.exe", &["/T", "2", "/NOBREAK"]);
        spec.timeout = Some(Duration::from_millis(50));

        let error = spec.run().await.expect_err("process should time out");

        assert!(matches!(error, ProcessError::Timeout(_)));
    }
}
