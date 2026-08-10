use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::tools::{
    Tool, ToolError,
    command::RunCommand,
    directory::CreateDirectory,
    filesystem::{GetCurrentDirectory, ReadFile, WriteFile},
};

pub struct Runtime {
    workspace: PathBuf,
}

impl Runtime {
    pub fn from_environment() -> Result<Self, ToolError> {
        if let Some(workspace) = env::var_os("LYA_WORKSPACE") {
            return Self::new(PathBuf::from(workspace));
        }

        Self::from_default_workspace(Path::new(env!("CARGO_MANIFEST_DIR")))
    }

    fn from_default_workspace(project_directory: &Path) -> Result<Self, ToolError> {
        let workspace = project_directory.join("agent").join("workspace");
        fs::create_dir_all(&workspace)
            .map_err(|error| ToolError::new(format!("could not create workspace: {error}")))?;

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
            Box::new(GetCurrentDirectory::new(&self.workspace)),
            Box::new(ReadFile::new(&self.workspace).expect("runtime workspace is valid")),
            Box::new(WriteFile::new(&self.workspace).expect("runtime workspace is valid")),
            Box::new(CreateDirectory::new(&self.workspace).expect("runtime workspace is valid")),
            Box::new(RunCommand::new(&self.workspace)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
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

    struct EnvironmentVariable {
        name: &'static str,
        previous: Option<OsString>,
    }

    impl EnvironmentVariable {
        fn set(name: &'static str, value: Option<&Path>) -> Self {
            let previous = std::env::var_os(name);
            unsafe {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
            Self { name, previous }
        }
    }

    impl Drop for EnvironmentVariable {
        fn drop(&mut self) {
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.name, value),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

    #[test]
    fn uses_workspace_from_environment() {
        let _lock = environment_lock()
            .lock()
            .expect("environment lock should not be poisoned");
        let workspace = workspace("environment");
        let _workspace = EnvironmentVariable::set("LYA_WORKSPACE", Some(&workspace));

        let runtime = Runtime::from_environment().expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            workspace.canonicalize().expect("workspace should exist")
        );
        drop(_workspace);
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn uses_project_workspace_without_environment_override() {
        let _lock = environment_lock()
            .lock()
            .expect("environment lock should not be poisoned");
        let _workspace = EnvironmentVariable::set("LYA_WORKSPACE", None);

        let runtime = Runtime::from_environment().expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("agent")
                .join("workspace")
                .canonicalize()
                .expect("project workspace should exist")
        );
    }

    #[test]
    fn creates_missing_default_workspace() {
        let project_directory = workspace("default-workspace");
        let default_workspace = project_directory.join("agent").join("workspace");

        let runtime =
            Runtime::from_default_workspace(&project_directory).expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            default_workspace
                .canonicalize()
                .expect("default workspace should be created")
        );
        fs::remove_dir_all(project_directory).expect("project directory should be removed");
    }

    #[test]
    fn all_workspace_tools_use_the_runtime_workspace() {
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
        let create_directory = runtime
            .tools()
            .into_iter()
            .find(|tool| tool.definition().function.name == "create_directory")
            .expect("create_directory should be registered");
        create_directory
            .execute(serde_json::json!({"path": "project/src"}))
            .expect("directory should be created");
        assert!(workspace.join("project/src").is_dir());
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
        let current_directory = runtime
            .tools()
            .into_iter()
            .find(|tool| tool.definition().function.name == "get_current_directory")
            .expect("get_current_directory should be registered")
            .execute(serde_json::json!({}))
            .expect("current directory should be returned");

        assert_eq!(
            current_directory["directory"],
            serde_json::json!(workspace.canonicalize().expect("workspace should exist"))
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }
}
