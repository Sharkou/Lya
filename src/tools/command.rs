use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::{
    llm::ToolDefinition,
    tools::{Tool, ToolError},
};

pub struct RunCommand {
    current_dir: PathBuf,
    timeout: Duration,
}

impl RunCommand {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    pub fn new(current_dir: impl AsRef<Path>) -> Self {
        Self {
            current_dir: current_dir.as_ref().to_owned(),
            timeout: Self::DEFAULT_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_timeout(current_dir: impl AsRef<Path>, timeout: Duration) -> Self {
        Self {
            current_dir: current_dir.as_ref().to_owned(),
            timeout,
        }
    }

    fn run(&self, command: &str) -> Result<serde_json::Value, ToolError> {
        let mut parts = command.split_whitespace();
        let program = parts
            .next()
            .ok_or_else(|| ToolError::new("run_command requires a non-empty command"))?;
        let arguments: Vec<_> = parts.collect();
        let mut child = Command::new(program)
            .args(arguments)
            .current_dir(&self.current_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| ToolError::new(format!("could not start command: {error}")))?;
        let deadline = Instant::now() + self.timeout;

        loop {
            if child
                .try_wait()
                .map_err(|error| ToolError::new(format!("could not wait for command: {error}")))?
                .is_some()
            {
                let output = child.wait_with_output().map_err(|error| {
                    ToolError::new(format!("could not collect command output: {error}"))
                })?;
                return Ok(serde_json::json!({
                    "exit_code": output.status.code(),
                    "stdout": String::from_utf8_lossy(&output.stdout),
                    "stderr": String::from_utf8_lossy(&output.stderr)
                }));
            }

            if Instant::now() >= deadline {
                child.kill().map_err(|error| {
                    ToolError::new(format!("could not stop timed out command: {error}"))
                })?;
                child.wait().map_err(|error| {
                    ToolError::new(format!("could not wait for timed out command: {error}"))
                })?;
                return Err(ToolError::new(format!(
                    "command timed out after {} seconds",
                    self.timeout.as_secs_f64()
                )));
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Default for RunCommand {
    fn default() -> Self {
        Self::new(std::env::current_dir().expect("current directory should exist"))
    }
}

impl Tool for RunCommand {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "run_command",
            "Runs a command with simple whitespace-separated arguments in Lya's current directory.",
            serde_json::json!({
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"],
                "additionalProperties": false
            }),
        )
    }

    fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value, ToolError> {
        let command = arguments
            .as_object()
            .and_then(|arguments| {
                (arguments.len() == 1)
                    .then(|| arguments.get("command"))
                    .flatten()
                    .and_then(serde_json::Value::as_str)
            })
            .ok_or_else(|| ToolError::new("run_command requires only a string command argument"))?;

        self.run(command)
    }
}

#[cfg(test)]
mod tests {
    use std::{env, time::Duration};

    use super::RunCommand;
    use crate::tools::Tool;

    #[cfg(unix)]
    const SUCCESS_COMMAND: &str = "echo hello";
    #[cfg(windows)]
    const SUCCESS_COMMAND: &str = "whoami";

    #[cfg(unix)]
    const TIMEOUT_COMMAND: &str = "sleep 1";
    #[cfg(windows)]
    const TIMEOUT_COMMAND: &str = "timeout.exe /T 1 /NOBREAK";

    #[test]
    fn captures_successful_command_stdout() {
        let result = RunCommand::new(env::current_dir().expect("current directory should exist"))
            .execute(serde_json::json!({"command": SUCCESS_COMMAND}))
            .expect("command should succeed");

        assert_eq!(result["exit_code"], 0);
        assert!(
            !result["stdout"]
                .as_str()
                .expect("stdout should be text")
                .trim()
                .is_empty()
        );
        #[cfg(unix)]
        assert_eq!(result["stdout"], "hello\n");
    }

    #[test]
    fn captures_non_zero_exit_code_and_stderr() {
        let result = RunCommand::new(env::current_dir().expect("current directory should exist"))
            .execute(serde_json::json!({"command": "cargo invalid-subcommand"}))
            .expect("command should run");

        assert_ne!(result["exit_code"], 0);
        assert!(
            !result["stderr"]
                .as_str()
                .expect("stderr should be text")
                .is_empty()
        );
    }

    #[test]
    fn rejects_missing_command() {
        let error = RunCommand::new(env::current_dir().expect("current directory should exist"))
            .execute(serde_json::json!({"command": "lya-command-that-does-not-exist"}))
            .expect_err("missing command should fail");

        assert!(error.to_string().contains("could not start command"));
    }

    #[test]
    fn stops_timed_out_command() {
        let error = RunCommand::with_timeout(
            env::current_dir().expect("current directory should exist"),
            Duration::from_millis(50),
        )
        .execute(serde_json::json!({"command": TIMEOUT_COMMAND}))
        .expect_err("command should time out");

        assert!(error.to_string().contains("command timed out"));
    }
}
