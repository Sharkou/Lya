use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    llm::ToolDefinition,
    process::ProcessSpec,
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
        let mut spec = ProcessSpec::new(program);
        spec.args = parts.map(str::to_owned).collect();
        spec.cwd = Some(self.current_dir.clone());
        spec.timeout = Some(self.timeout);

        let output = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    ToolError::new(format!("could not start command runtime: {error}"))
                })?
                .block_on(spec.run())
                .map_err(|error| ToolError::new(error.to_string()))
        })
        .join()
        .map_err(|_| ToolError::new("command execution thread panicked"))??;

        Ok(serde_json::json!({
            "exit_code": output.exit_code,
            "stdout": output.stdout,
            "stderr": output.stderr
        }))
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
            "Runs a command with whitespace-separated arguments in Lya's current directory.",
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

        assert!(error.to_string().contains("could not start process"));
    }

    #[test]
    fn stops_timed_out_command() {
        let error = RunCommand::with_timeout(
            env::current_dir().expect("current directory should exist"),
            Duration::from_millis(50),
        )
        .execute(serde_json::json!({"command": TIMEOUT_COMMAND}))
        .expect_err("command should time out");

        assert!(error.to_string().contains("process timed out"));
    }
}
