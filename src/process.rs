use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    fmt,
    future::Future,
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Notify,
};

#[derive(Clone, Debug)]
pub struct ProcessCancellation {
    cancelled: Arc<AtomicBool>,
    notification: Arc<Notify>,
}

impl PartialEq for ProcessCancellation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cancelled, &other.cancelled)
    }
}

impl Eq for ProcessCancellation {}

impl ProcessCancellation {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notification: Arc::new(Notify::new()),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notification.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    async fn cancelled(&self) {
        let notification = self.notification.notified();
        if !self.is_cancelled() {
            notification.await;
        }
    }
}

impl Default for ProcessCancellation {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ProcessRunner: Send + Sync {
    fn run(
        &self,
        spec: ProcessSpec,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessOutput, ProcessError>> + Send + '_>>;
}

#[derive(Debug, Default)]
pub struct SystemProcessRunner;

impl ProcessRunner for SystemProcessRunner {
    fn run(
        &self,
        spec: ProcessSpec,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessOutput, ProcessError>> + Send + '_>> {
        Box::pin(async move { spec.run().await })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    pub program: OsString,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub env_remove: Vec<String>,
    pub stdin: Option<String>,
    pub timeout: Option<Duration>,
    pub cancellation: Option<ProcessCancellation>,
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
            cancellation: None,
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

        let mut child = command.spawn().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ProcessError::ExecutableMissing(error.to_string())
            } else {
                ProcessError::Start(error.to_string())
            }
        })?;
        let mut stdin_task = match (child.stdin.take(), self.stdin.clone()) {
            (Some(mut stdin), Some(input)) => Some(tokio::spawn(async move {
                stdin
                    .write_all(input.as_bytes())
                    .await
                    .map_err(|error| ProcessError::Stdin(error.to_string()))?;
                stdin
                    .shutdown()
                    .await
                    .map_err(|error| ProcessError::Stdin(error.to_string()))
            })),
            _ => None,
        };
        let mut stdout = child.stdout.take().expect("stdout was piped");
        let mut stderr = child.stderr.take().expect("stderr was piped");
        let stdout_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            stdout.read_to_end(&mut bytes).await.map(|_| bytes)
        });
        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).await.map(|_| bytes)
        });
        let cancellation = self.cancellation.clone();
        let status = match (self.timeout, cancellation) {
            (Some(timeout), Some(cancellation)) => tokio::select! {
                result = child.wait() => result.map_err(|error| ProcessError::Wait(error.to_string()))?,
                _ = cancellation.cancelled() => return cancelled_child(&mut child, &mut stdin_task, stdout_task, stderr_task).await,
                _ = tokio::time::sleep(timeout) => return timed_out_child(&mut child, &mut stdin_task, stdout_task, stderr_task, timeout).await,
            },
            (Some(timeout), None) => tokio::select! {
                result = child.wait() => result.map_err(|error| ProcessError::Wait(error.to_string()))?,
                _ = tokio::time::sleep(timeout) => return timed_out_child(&mut child, &mut stdin_task, stdout_task, stderr_task, timeout).await,
            },
            (None, Some(cancellation)) => tokio::select! {
                result = child.wait() => result.map_err(|error| ProcessError::Wait(error.to_string()))?,
                _ = cancellation.cancelled() => return cancelled_child(&mut child, &mut stdin_task, stdout_task, stderr_task).await,
            },
            (None, None) => child
                .wait()
                .await
                .map_err(|error| ProcessError::Wait(error.to_string()))?,
        };
        let stdin_result = collect_stdin(&mut stdin_task).await;
        let stdout = collect_pipe(stdout_task).await?;
        let stderr = collect_pipe(stderr_task).await?;
        stdin_result?;
        Ok(ProcessOutput {
            exit_code: status.code(),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }
}

async fn collect_pipe(
    task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
) -> Result<Vec<u8>, ProcessError> {
    task.await
        .map_err(|error| ProcessError::Wait(error.to_string()))?
        .map_err(|error| ProcessError::Wait(error.to_string()))
}

async fn collect_stdin(
    task: &mut Option<tokio::task::JoinHandle<Result<(), ProcessError>>>,
) -> Result<(), ProcessError> {
    match task.take() {
        Some(task) => task
            .await
            .map_err(|error| ProcessError::Stdin(error.to_string()))?,
        None => Ok(()),
    }
}

async fn cancelled_child(
    child: &mut tokio::process::Child,
    stdin_task: &mut Option<tokio::task::JoinHandle<Result<(), ProcessError>>>,
    stdout_task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    stderr_task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
) -> Result<ProcessOutput, ProcessError> {
    child
        .kill()
        .await
        .map_err(|error| ProcessError::Wait(error.to_string()))?;
    let _ = collect_stdin(stdin_task).await;
    let stdout = String::from_utf8_lossy(&collect_pipe(stdout_task).await?).into_owned();
    let stderr = String::from_utf8_lossy(&collect_pipe(stderr_task).await?).into_owned();
    Err(ProcessError::Cancelled { stdout, stderr })
}

async fn timed_out_child(
    child: &mut tokio::process::Child,
    stdin_task: &mut Option<tokio::task::JoinHandle<Result<(), ProcessError>>>,
    stdout_task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    stderr_task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    timeout: Duration,
) -> Result<ProcessOutput, ProcessError> {
    child
        .kill()
        .await
        .map_err(|error| ProcessError::Wait(error.to_string()))?;
    let _ = collect_stdin(stdin_task).await;
    let _ = collect_pipe(stdout_task).await?;
    let _ = collect_pipe(stderr_task).await?;
    Err(ProcessError::Timeout(timeout))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOutput {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub enum ProcessError {
    ExecutableMissing(String),
    Start(String),
    Stdin(String),
    Wait(String),
    Timeout(Duration),
    Cancelled { stdout: String, stderr: String },
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutableMissing(error) => {
                write!(
                    formatter,
                    "could not start process: executable was not found: {error}"
                )
            }
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
            Self::Cancelled { .. } => formatter.write_str("process was cancelled by the user"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, time::Duration};

    use super::{ProcessCancellation, ProcessError, ProcessSpec};

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

    #[tokio::test]
    async fn cancellation_kills_and_reaps_a_child_without_becoming_a_timeout() {
        #[cfg(unix)]
        let mut spec = spec("sleep", &["2"]);
        #[cfg(windows)]
        let mut spec = spec("cmd.exe", &["/C", "timeout /T 2 /NOBREAK > NUL"]);
        let cancellation = ProcessCancellation::new();
        cancellation.cancel();
        spec.cancellation = Some(cancellation);

        let error = spec
            .run()
            .await
            .expect_err("cancelled child should not succeed");

        assert!(matches!(error, ProcessError::Cancelled { .. }));
    }
}
