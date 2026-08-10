use std::{
    env,
    path::{Path, PathBuf},
};

use crate::tools::{
    Tool, ToolError,
    command::RunCommand,
    filesystem::{GetCurrentDirectory, ReadFile, WriteFile},
};

pub struct Runtime {
    workspace: PathBuf,
}

impl Runtime {
    pub fn from_environment() -> Result<Self, ToolError> {
        let workspace = env::var_os("LYA_WORKSPACE")
            .map(PathBuf::from)
            .map_or_else(env::current_dir, Ok)
            .map_err(|error| ToolError::new(format!("could not determine workspace: {error}")))?;

        Self::new(workspace)
    }

    pub fn new(workspace: impl AsRef<Path>) -> Result<Self, ToolError> {
        let workspace = workspace
            .as_ref()
            .canonicalize()
            .map_err(|error| ToolError::new(format!("could not access workspace: {error}")))?;
        if !workspace.is_dir() {
            return Err(ToolError::new("workspace must be a directory"));
        }

        Ok(Self { workspace })
    }

    #[cfg(test)]
    fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn tools(&self) -> Vec<Box<dyn Tool>> {
        vec![
            Box::new(GetCurrentDirectory),
            Box::new(ReadFile::new(&self.workspace).expect("runtime workspace is valid")),
            Box::new(WriteFile::new(&self.workspace).expect("runtime workspace is valid")),
            Box::new(RunCommand::new(&self.workspace)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::{Mutex, OnceLock},
    };

    use super::Runtime;

    fn environment_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn workspace(name: &str) -> std::path::PathBuf {
        let directory =
            std::env::temp_dir().join(format!("lya-runtime-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir(&directory).expect("workspace should be created");
        directory
    }

    #[test]
    fn uses_workspace_from_environment() {
        let _lock = environment_lock()
            .lock()
            .expect("environment lock should not be poisoned");
        let workspace = workspace("environment");
        let previous = std::env::var_os("LYA_WORKSPACE");
        unsafe { std::env::set_var("LYA_WORKSPACE", &workspace) };

        let runtime = Runtime::from_environment().expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            workspace.canonicalize().expect("workspace should exist")
        );
        unsafe {
            match previous {
                Some(value) => std::env::set_var("LYA_WORKSPACE", value),
                None => std::env::remove_var("LYA_WORKSPACE"),
            }
        }
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn defaults_to_current_directory() {
        let _lock = environment_lock()
            .lock()
            .expect("environment lock should not be poisoned");
        let previous = std::env::var_os("LYA_WORKSPACE");
        unsafe { std::env::remove_var("LYA_WORKSPACE") };

        let runtime = Runtime::from_environment().expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            std::env::current_dir()
                .expect("current directory should exist")
                .canonicalize()
                .expect("current directory should resolve")
        );
        unsafe {
            if let Some(value) = previous {
                std::env::set_var("LYA_WORKSPACE", value);
            }
        }
    }

    #[test]
    fn reads_and_writes_files_in_workspace() {
        let workspace = workspace("filesystem");
        let runtime = Runtime::new(&workspace).expect("runtime should be created");
        let write_file = runtime
            .tools()
            .into_iter()
            .find(|tool| tool.definition().function.name == "write_file")
            .expect("write_file should be registered");

        write_file
            .execute(serde_json::json!({"path": "note.txt", "content": "hello Lya"}))
            .expect("file should be written");

        let read_file = runtime
            .tools()
            .into_iter()
            .find(|tool| tool.definition().function.name == "read_file")
            .expect("read_file should be registered");
        let result = read_file
            .execute(serde_json::json!({"path": "note.txt"}))
            .expect("file should be read");

        assert_eq!(result, serde_json::json!({"content": "hello Lya"}));
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn runs_commands_in_workspace() {
        let workspace = workspace("command");
        fs::write(
            workspace.join("Cargo.toml"),
            "[package]\nname = \"runtime-test\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        )
        .expect("manifest should be written");
        let runtime = Runtime::new(&workspace).expect("runtime should be created");
        let command = runtime
            .tools()
            .into_iter()
            .find(|tool| tool.definition().function.name == "run_command")
            .expect("run_command should be registered");

        let result = command
            .execute(serde_json::json!({
                "command": "cargo locate-project --message-format plain"
            }))
            .expect("command should run");

        assert!(
            result["stdout"]
                .as_str()
                .expect("stdout should be text")
                .contains(
                    Path::new(&workspace)
                        .canonicalize()
                        .expect("workspace should exist")
                        .join("Cargo.toml")
                        .to_string_lossy()
                        .as_ref()
                )
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }
}
