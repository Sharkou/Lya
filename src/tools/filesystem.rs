use std::{
    env, fs,
    path::{Component, Path, PathBuf},
};

use crate::{
    llm::ToolDefinition,
    tools::{Tool, ToolError},
};

pub struct GetCurrentDirectory;

impl Tool for GetCurrentDirectory {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "get_current_directory",
            "Returns Lya's current working directory.",
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        )
    }

    fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value, ToolError> {
        match arguments {
            serde_json::Value::Object(properties) if properties.is_empty() => {}
            _ => return Err(ToolError::new("get_current_directory accepts no arguments")),
        }

        let directory = env::current_dir()
            .map_err(|error| ToolError::new(format!("could not get current directory: {error}")))?;

        Ok(serde_json::json!({"directory": directory}))
    }
}

pub struct ReadFile {
    workspace: PathBuf,
}

impl ReadFile {
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

    fn resolve_path(&self, path: &str) -> Result<PathBuf, ToolError> {
        let requested = Path::new(path);
        if requested.is_absolute() {
            return Err(ToolError::new("path must be relative to the workspace"));
        }
        if requested
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(ToolError::new("path must not contain '..'"));
        }

        let resolved = self
            .workspace
            .join(requested)
            .canonicalize()
            .map_err(|error| ToolError::new(format!("could not access requested file: {error}")))?;
        if !resolved.starts_with(&self.workspace) {
            return Err(ToolError::new("path resolves outside the workspace"));
        }

        Ok(resolved)
    }
}

impl Tool for ReadFile {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "read_file",
            "Reads a UTF-8 text file relative to Lya's workspace.",
            serde_json::json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": false
            }),
        )
    }

    fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value, ToolError> {
        let path = arguments
            .as_object()
            .and_then(|arguments| {
                (arguments.len() == 1)
                    .then(|| arguments.get("path"))
                    .flatten()
                    .and_then(serde_json::Value::as_str)
            })
            .ok_or_else(|| ToolError::new("read_file requires only a string path argument"))?;
        let path = self.resolve_path(path)?;

        if path.is_dir() {
            return Err(ToolError::new("path points to a directory"));
        }

        let bytes = fs::read(&path)
            .map_err(|error| ToolError::new(format!("could not read file: {error}")))?;
        let content = String::from_utf8(bytes)
            .map_err(|error| ToolError::new(format!("file is not valid UTF-8: {error}")))?;

        Ok(serde_json::json!({"content": content}))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{GetCurrentDirectory, ReadFile};
    use crate::tools::Tool;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn workspace() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-read-file-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("workspace should be created");
        directory
    }

    #[test]
    fn returns_current_directory() {
        let result = GetCurrentDirectory
            .execute(serde_json::json!({}))
            .expect("tool should execute");

        assert_eq!(
            result["directory"],
            serde_json::json!(std::env::current_dir().expect("current directory should exist"))
        );
    }

    #[test]
    fn reads_valid_utf8_file_in_workspace() {
        let workspace = workspace();
        fs::write(workspace.join("note.txt"), "hello Lya").expect("file should be written");
        let tool = ReadFile::new(&workspace).expect("workspace should be valid");

        let result = tool
            .execute(serde_json::json!({"path": "note.txt"}))
            .expect("file should be read");

        assert_eq!(result, serde_json::json!({"content": "hello Lya"}));
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_missing_file() {
        let workspace = workspace();
        let tool = ReadFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "missing.txt"}))
            .expect_err("missing file should fail");

        assert!(
            error
                .to_string()
                .contains("could not access requested file")
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_parent_directory_traversal() {
        let workspace = workspace();
        let tool = ReadFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "../outside.txt"}))
            .expect_err("traversal should fail");

        assert_eq!(error.to_string(), "path must not contain '..'");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_absolute_path() {
        let workspace = workspace();
        let tool = ReadFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "/tmp/outside.txt"}))
            .expect_err("absolute path should fail");

        assert_eq!(error.to_string(), "path must be relative to the workspace");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_directory() {
        let workspace = workspace();
        fs::create_dir(workspace.join("nested")).expect("directory should be created");
        let tool = ReadFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "nested"}))
            .expect_err("directory should fail");

        assert_eq!(error.to_string(), "path points to a directory");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_non_utf8_file() {
        let workspace = workspace();
        fs::write(workspace.join("binary.dat"), [0xff, 0xfe]).expect("file should be written");
        let tool = ReadFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "binary.dat"}))
            .expect_err("non-UTF-8 file should fail");

        assert!(error.to_string().contains("file is not valid UTF-8"));
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }
}
