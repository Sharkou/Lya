use std::{
    error::Error, ffi::OsString, fmt, future::Future, path::PathBuf, pin::Pin, time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::process::{ProcessError, ProcessRunner, ProcessSpec, SystemProcessRunner};

const STDIN_TASK_INSTRUCTION: &str =
    "Complete the task provided through standard input. Follow it exactly.";

pub trait Executor {
    fn execute(
        &self,
        request: ExecutorRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutorResult, ExecutorError>> + Send + '_>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorRequest {
    pub project_name: String,
    pub project_path: PathBuf,
    pub prompt: String,
    pub session: ExecutorSession,
    pub browser: bool,
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub user_instructions: Vec<String>,
    #[serde(skip)]
    pub cancellation: Option<crate::process::ProcessCancellation>,
}

impl ExecutorRequest {
    pub fn validate(&self) -> Result<(), ExecutorError> {
        if self.project_name.trim().is_empty() {
            return Err(ExecutorError::InvalidRequest(
                "project name cannot be empty".to_owned(),
            ));
        }
        if !self.project_path.is_dir() {
            return Err(ExecutorError::InvalidRequest(format!(
                "project path is not a directory: {}",
                self.project_path.display()
            )));
        }
        if self.prompt.trim().is_empty() {
            return Err(ExecutorError::InvalidRequest(
                "prompt cannot be empty".to_owned(),
            ));
        }
        if let ExecutorSession::Resume(session_id) = &self.session
            && session_id.trim().is_empty()
        {
            return Err(ExecutorError::InvalidRequest(
                "session reference cannot be empty when resuming".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExecutorSession {
    #[default]
    New,
    Resume(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutorResult {
    pub final_response: String,
    pub session_id: Option<String>,
    pub exit_code: Option<i32>,
    pub duration_ms: Option<u64>,
    pub turns: Option<u32>,
    pub total_cost_usd: Option<f64>,
    pub usage: Option<serde_json::Value>,
}

pub struct ClaudeCliExecutor<R = SystemProcessRunner> {
    runner: R,
    program: OsString,
    max_turns: Option<u32>,
}

impl ClaudeCliExecutor<SystemProcessRunner> {
    pub fn new() -> Self {
        Self::with_runner(SystemProcessRunner)
    }
}

impl Default for ClaudeCliExecutor<SystemProcessRunner> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> ClaudeCliExecutor<R> {
    pub fn with_runner(runner: R) -> Self {
        Self {
            runner,
            program: claude_program_from_environment(|name| std::env::var_os(name)),
            max_turns: None,
        }
    }

    pub fn with_max_turns(mut self, max_turns: u32) -> Self {
        self.max_turns = Some(max_turns);
        self
    }

    pub fn build_process_spec(
        &self,
        request: &ExecutorRequest,
    ) -> Result<ProcessSpec, ExecutorError> {
        request.validate()?;

        let mut spec = ProcessSpec::new(&self.program);
        spec.args = vec![
            "--print".to_owned(),
            STDIN_TASK_INSTRUCTION.to_owned(),
            "--output-format".to_owned(),
            "json".to_owned(),
            "--permission-mode".to_owned(),
            "auto".to_owned(),
            "--permission-prompts".to_owned(),
            "none".to_owned(),
        ];
        if let ExecutorSession::Resume(session_id) = &request.session {
            spec.args.push("--resume".to_owned());
            spec.args.push(session_id.clone());
        }
        if let Some(max_turns) = self.max_turns {
            spec.args.push("--max-turns".to_owned());
            spec.args.push(max_turns.to_string());
        }
        if request.browser {
            spec.args.push("--chrome".to_owned());
        }
        spec.cwd = Some(request.project_path.clone());
        spec.env_remove.push("ANTHROPIC_API_KEY".to_owned());
        spec.stdin = Some(render_executor_prompt(request));
        spec.timeout = request.timeout;
        spec.cancellation = request.cancellation.clone();
        Ok(spec)
    }
}

fn render_executor_prompt(request: &ExecutorRequest) -> String {
    if request.user_instructions.is_empty() {
        return request.prompt.clone();
    }
    format!(
        "{}\n\nAdditional user instructions (follow each independently):\n{}",
        request.prompt,
        request.user_instructions.join("\n")
    )
}

fn claude_program_from_environment(lookup: impl Fn(&str) -> Option<OsString>) -> OsString {
    lookup("LYA_CLAUDE_BIN").unwrap_or_else(|| OsString::from("claude"))
}

impl<R: ProcessRunner> Executor for ClaudeCliExecutor<R> {
    fn execute(
        &self,
        request: ExecutorRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutorResult, ExecutorError>> + Send + '_>> {
        Box::pin(async move {
            let spec = self.build_process_spec(&request)?;
            let output = self
                .runner
                .run(spec)
                .await
                .map_err(ExecutorError::from_process)?;
            if output.exit_code != Some(0) {
                return Err(ExecutorError::ProcessFailed {
                    exit_code: output.exit_code,
                    stdout: output.stdout,
                    stderr: output.stderr,
                });
            }
            ExecutorResult::from_claude_json(&output.stdout, output.exit_code)
        })
    }
}

impl ExecutorResult {
    pub fn from_claude_json(output: &str, exit_code: Option<i32>) -> Result<Self, ExecutorError> {
        if output.trim().is_empty() {
            return Err(ExecutorError::InvalidOutput(
                ExecutorOutputError::EmptyOutput,
            ));
        }
        let output = serde_json::from_str::<ClaudeJsonOutput>(output).map_err(|error| {
            ExecutorError::InvalidOutput(ExecutorOutputError::InvalidJson(error.to_string()))
        })?;
        let final_response = output
            .result
            .filter(|result| !result.trim().is_empty())
            .ok_or(ExecutorError::InvalidOutput(
                ExecutorOutputError::MissingResult,
            ))?;
        Ok(Self {
            final_response,
            session_id: output.session_id,
            exit_code,
            duration_ms: output.duration_ms,
            turns: output.num_turns,
            total_cost_usd: output.total_cost_usd,
            usage: output.usage,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ClaudeJsonOutput {
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    num_turns: Option<u32>,
    #[serde(default)]
    total_cost_usd: Option<f64>,
    #[serde(default)]
    usage: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorOutputError {
    EmptyOutput,
    InvalidJson(String),
    MissingResult,
}

impl fmt::Display for ExecutorOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyOutput => formatter.write_str("Claude output is empty"),
            Self::InvalidJson(error) => {
                write!(formatter, "Claude output is not valid JSON: {error}")
            }
            Self::MissingResult => formatter.write_str("Claude JSON output has no result"),
        }
    }
}

impl Error for ExecutorOutputError {}

#[derive(Debug)]
pub enum ExecutorError {
    InvalidRequest(String),
    ExecutableMissing(String),
    ProcessFailed {
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    Timeout(Duration),
    InvalidOutput(ExecutorOutputError),
    AuthenticationRequired,
    QuotaExceeded,
    Process(String),
}

impl ExecutorError {
    fn from_process(error: ProcessError) -> Self {
        match error {
            ProcessError::ExecutableMissing(error) => Self::ExecutableMissing(error),
            ProcessError::Timeout(timeout) => Self::Timeout(timeout),
            error => Self::Process(error.to_string()),
        }
    }
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(error) => write!(formatter, "invalid executor request: {error}"),
            Self::ExecutableMissing(error) => {
                write!(formatter, "Claude executable is missing: {error}")
            }
            Self::ProcessFailed {
                exit_code,
                stdout,
                stderr,
            } => write!(
                formatter,
                "Claude exited with status {}: {}{}",
                exit_code.map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
                stderr.trim(),
                if stdout.trim().is_empty() {
                    String::new()
                } else {
                    format!("\n{}", stdout.trim())
                }
            ),
            Self::Timeout(timeout) => write!(
                formatter,
                "Claude timed out after {} seconds",
                timeout.as_secs_f64()
            ),
            Self::InvalidOutput(error) => write!(formatter, "invalid Claude output: {error}"),
            Self::AuthenticationRequired => {
                formatter.write_str("Claude authentication is required")
            }
            Self::QuotaExceeded => formatter.write_str("Claude subscription quota is exhausted"),
            Self::Process(error) => write!(formatter, "Claude process error: {error}"),
        }
    }
}

impl Error for ExecutorError {}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        future::Future,
        path::PathBuf,
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        ClaudeCliExecutor, Executor, ExecutorError, ExecutorOutputError, ExecutorRequest,
        ExecutorResult, ExecutorSession, STDIN_TASK_INSTRUCTION, claude_program_from_environment,
    };
    use crate::process::{
        ProcessError, ProcessOutput, ProcessRunner, ProcessSpec, SystemProcessRunner,
    };

    #[derive(Clone)]
    struct FakeRunner {
        result: Result<ProcessOutput, ProcessError>,
        specs: Arc<Mutex<Vec<ProcessSpec>>>,
    }

    impl ProcessRunner for FakeRunner {
        fn run(
            &self,
            spec: ProcessSpec,
        ) -> Pin<Box<dyn Future<Output = Result<ProcessOutput, ProcessError>> + Send + '_>>
        {
            self.specs.lock().expect("spec lock").push(spec);
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    fn request() -> ExecutorRequest {
        ExecutorRequest {
            project_name: "Lya development".to_owned(),
            project_path: std::env::temp_dir(),
            prompt: "Inspect files with spaces intact".to_owned(),
            session: ExecutorSession::New,
            browser: false,
            timeout: None,
            user_instructions: Vec::new(),
            cancellation: None,
        }
    }

    fn successful_runner(output: &str) -> FakeRunner {
        FakeRunner {
            result: Ok(ProcessOutput {
                exit_code: Some(0),
                stdout: output.to_owned(),
                stderr: String::new(),
            }),
            specs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn default_program_executor() -> ClaudeCliExecutor {
        ClaudeCliExecutor {
            runner: SystemProcessRunner,
            program: OsString::from("claude"),
            max_turns: None,
        }
    }

    #[test]
    fn request_validation_rejects_empty_prompt_and_invalid_project_path() {
        let mut invalid_prompt = request();
        invalid_prompt.prompt = "  ".to_owned();
        assert!(matches!(
            invalid_prompt.validate(),
            Err(ExecutorError::InvalidRequest(_))
        ));

        let mut invalid_path = request();
        invalid_path.project_path = PathBuf::from("this-path-does-not-exist");
        assert!(matches!(
            invalid_path.validate(),
            Err(ExecutorError::InvalidRequest(_))
        ));
    }

    #[test]
    fn builds_new_session_spec_without_browser_and_removes_api_key() {
        let executor = default_program_executor();
        let spec = executor
            .build_process_spec(&request())
            .expect("request should be valid");

        assert_eq!(spec.program, "claude");
        assert_eq!(spec.cwd, Some(std::env::temp_dir()));
        assert_eq!(spec.args[0], "--print");
        assert_eq!(spec.args[1], STDIN_TASK_INSTRUCTION);
        assert!(
            !spec
                .args
                .iter()
                .any(|arg| arg == "Inspect files with spaces intact")
        );
        assert_eq!(
            spec.stdin.as_deref(),
            Some("Inspect files with spaces intact")
        );
        assert!(
            spec.args
                .windows(2)
                .any(|args| args == ["--output-format", "json"])
        );
        assert!(
            spec.args
                .windows(2)
                .any(|args| args == ["--permission-mode", "auto"])
        );
        assert!(!spec.args.iter().any(|arg| arg == "--resume"));
        assert!(!spec.args.iter().any(|arg| arg == "--chrome"));
        assert!(
            spec.env_remove
                .iter()
                .any(|name| name == "ANTHROPIC_API_KEY")
        );
    }

    #[test]
    fn builds_resume_browser_and_max_turns_arguments() {
        let executor = default_program_executor().with_max_turns(12);
        let mut request = request();
        request.session = ExecutorSession::Resume("session with spaces".to_owned());
        request.browser = true;
        request.timeout = Some(Duration::from_secs(900));

        let spec = executor
            .build_process_spec(&request)
            .expect("request should be valid");

        assert!(
            spec.args
                .windows(2)
                .any(|args| args == ["--resume", "session with spaces"])
        );
        assert!(spec.args.iter().any(|arg| arg == "--chrome"));
        assert!(
            spec.args
                .windows(2)
                .any(|args| args == ["--max-turns", "12"])
        );
        assert_eq!(spec.timeout, Some(Duration::from_secs(900)));
    }

    #[test]
    fn resolves_claude_program_from_optional_environment_override() {
        assert_eq!(
            claude_program_from_environment(|_| Some(OsString::from("C:/tools/claude.exe"))),
            OsString::from("C:/tools/claude.exe")
        );
        assert_eq!(
            claude_program_from_environment(|_| None),
            OsString::from("claude")
        );
    }

    #[test]
    fn puts_large_prompt_only_in_stdin() {
        let mut request = request();
        request.prompt = "long task ".repeat(100_000);

        let spec = default_program_executor()
            .build_process_spec(&request)
            .expect("request should be valid");

        assert!(!spec.args.iter().any(|argument| argument == &request.prompt));
        assert_eq!(spec.stdin.as_deref(), Some(request.prompt.as_str()));
    }

    #[test]
    fn parses_structured_claude_result_and_metadata() {
        let result = ExecutorResult::from_claude_json(
            r#"{"result":"Completed the review.","session_id":"abc","duration_ms":42,"num_turns":3,"total_cost_usd":0.01,"usage":{"input_tokens":5}}"#,
            Some(0),
        )
        .expect("JSON should parse");

        assert_eq!(result.final_response, "Completed the review.");
        assert_eq!(result.session_id.as_deref(), Some("abc"));
        assert_eq!(result.duration_ms, Some(42));
        assert_eq!(result.turns, Some(3));
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.usage, Some(serde_json::json!({"input_tokens": 5})));
    }

    #[test]
    fn rejects_empty_invalid_and_incomplete_structured_output() {
        assert!(matches!(
            ExecutorResult::from_claude_json("", Some(0)),
            Err(ExecutorError::InvalidOutput(
                ExecutorOutputError::EmptyOutput
            ))
        ));
        assert!(matches!(
            ExecutorResult::from_claude_json("not JSON", Some(0)),
            Err(ExecutorError::InvalidOutput(
                ExecutorOutputError::InvalidJson(_)
            ))
        ));
        assert!(matches!(
            ExecutorResult::from_claude_json(r#"{"session_id":"abc"}"#, Some(0)),
            Err(ExecutorError::InvalidOutput(
                ExecutorOutputError::MissingResult
            ))
        ));
    }

    #[tokio::test]
    async fn executor_runs_structured_spec_and_returns_result() {
        let runner = successful_runner(r#"{"result":"Done.","session_id":"new-session"}"#);
        let specs = runner.specs.clone();
        let executor = ClaudeCliExecutor::with_runner(runner);

        let result = executor
            .execute(request())
            .await
            .expect("run should succeed");

        assert_eq!(result.final_response, "Done.");
        assert_eq!(result.session_id.as_deref(), Some("new-session"));
        assert_eq!(
            specs.lock().expect("spec lock").len(),
            1,
            "executor should delegate exactly one ProcessSpec"
        );
    }

    #[tokio::test]
    async fn executor_reports_process_failure_missing_executable_and_timeout() {
        let failed = FakeRunner {
            result: Ok(ProcessOutput {
                exit_code: Some(9),
                stdout: "ignored".to_owned(),
                stderr: "failed".to_owned(),
            }),
            specs: Arc::new(Mutex::new(Vec::new())),
        };
        assert!(matches!(
            ClaudeCliExecutor::with_runner(failed)
                .execute(request())
                .await,
            Err(ExecutorError::ProcessFailed { .. })
        ));

        let missing = FakeRunner {
            result: Err(ProcessError::ExecutableMissing("claude".to_owned())),
            specs: Arc::new(Mutex::new(Vec::new())),
        };
        assert!(matches!(
            ClaudeCliExecutor::with_runner(missing)
                .execute(request())
                .await,
            Err(ExecutorError::ExecutableMissing(_))
        ));

        let timeout = FakeRunner {
            result: Err(ProcessError::Timeout(Duration::from_secs(900))),
            specs: Arc::new(Mutex::new(Vec::new())),
        };
        assert!(matches!(
            ClaudeCliExecutor::with_runner(timeout)
                .execute(request())
                .await,
            Err(ExecutorError::Timeout(_))
        ));
    }
}
