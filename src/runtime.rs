use std::{
    env, fs,
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
        if let Some(workspace) = env::var_os("LYA_WORKSPACE") {
            return Self::new(PathBuf::from(workspace));
        }

        let workspace = Self::default_workspace()?;
        fs::create_dir_all(&workspace)
            .map_err(|error| ToolError::new(format!("could not create workspace: {error}")))?;

        Self::new(workspace)
    }

    fn default_workspace() -> Result<PathBuf, ToolError> {
        let home = user_home_directory()?;
        let documents_workspace = home.join("Documents").join("Lya");

        Ok(if documents_workspace.is_dir() {
            documents_workspace
        } else {
            home.join("Lya")
        })
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

#[cfg(windows)]
fn user_home_directory() -> Result<PathBuf, ToolError> {
    env::var_os("USERPROFILE")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| ToolError::new("USERPROFILE must define the user home directory"))
}

#[cfg(not(windows))]
fn user_home_directory() -> Result<PathBuf, ToolError> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| ToolError::new("HOME must define the user home directory"))
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
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

    #[cfg(windows)]
    const HOME_VARIABLE: &str = "USERPROFILE";
    #[cfg(not(windows))]
    const HOME_VARIABLE: &str = "HOME";

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
    fn uses_documents_workspace_when_it_exists() {
        let _lock = environment_lock()
            .lock()
            .expect("environment lock should not be poisoned");
        let home = workspace("documents-workspace");
        let documents_workspace = home.join("Documents").join("Lya");
        fs::create_dir_all(&documents_workspace).expect("documents workspace should be created");
        let _workspace = EnvironmentVariable::set("LYA_WORKSPACE", None);
        let _home = EnvironmentVariable::set(HOME_VARIABLE, Some(&home));

        let runtime = Runtime::from_environment().expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            documents_workspace
                .canonicalize()
                .expect("documents workspace should exist")
        );
        drop(_home);
        drop(_workspace);
        fs::remove_dir_all(home).expect("home should be removed");
    }

    #[test]
    fn creates_missing_default_workspace() {
        let _lock = environment_lock()
            .lock()
            .expect("environment lock should not be poisoned");
        let home = workspace("default-workspace");
        let default_workspace = home.join("Lya");
        let _workspace = EnvironmentVariable::set("LYA_WORKSPACE", None);
        let _home = EnvironmentVariable::set(HOME_VARIABLE, Some(&home));

        let runtime = Runtime::from_environment().expect("runtime should be created");

        assert_eq!(
            runtime.workspace(),
            default_workspace
                .canonicalize()
                .expect("default workspace should be created")
        );
        drop(_home);
        drop(_workspace);
        fs::remove_dir_all(home).expect("home should be removed");
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
