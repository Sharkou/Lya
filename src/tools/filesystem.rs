use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

use crate::{
    llm::ToolDefinition,
    tools::{Tool, ToolError},
};

pub struct GetCurrentDirectory {
    workspace: PathBuf,
}

impl GetCurrentDirectory {
    pub fn new(workspace: impl AsRef<Path>) -> Self {
        Self {
            workspace: workspace.as_ref().to_owned(),
        }
    }
}

impl Tool for GetCurrentDirectory {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "get_current_directory",
            "Returns Lya's workspace directory.",
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

        Ok(serde_json::json!({"directory": self.workspace}))
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
        if requested.is_absolute() || requested.has_root() {
            return Err(ToolError::new("path must be relative to the workspace"));
        }
        if requested
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
        {
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

pub struct WriteFile {
    workspace: PathBuf,
}

impl WriteFile {
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
        if requested.is_absolute() || requested.has_root() {
            return Err(ToolError::new("path must be relative to the workspace"));
        }
        if requested
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
        {
            return Err(ToolError::new("path must be relative to the workspace"));
        }
        if requested
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(ToolError::new("path must not contain '..'"));
        }

        let file_name = requested
            .file_name()
            .ok_or_else(|| ToolError::new("path must name a file"))?;
        let parent = requested.parent().unwrap_or_else(|| Path::new(""));
        let parent = self
            .workspace
            .join(parent)
            .canonicalize()
            .map_err(|error| {
                ToolError::new(format!("could not access parent directory: {error}"))
            })?;
        if !parent.starts_with(&self.workspace) {
            return Err(ToolError::new("path resolves outside the workspace"));
        }

        let target = parent.join(file_name);
        match fs::symlink_metadata(&target) {
            Ok(_) => {
                let resolved = target.canonicalize().map_err(|error| {
                    ToolError::new(format!("could not access requested file: {error}"))
                })?;
                if !resolved.starts_with(&self.workspace) {
                    return Err(ToolError::new("path resolves outside the workspace"));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ToolError::new(format!(
                    "could not access requested file: {error}"
                )));
            }
        }

        Ok(target)
    }
}

impl Tool for WriteFile {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "write_file",
            "Writes UTF-8 text to a file relative to Lya's workspace. Parent directories must already exist.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"}
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
        )
    }

    fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value, ToolError> {
        let arguments = arguments
            .as_object()
            .filter(|arguments| arguments.len() == 2)
            .ok_or_else(|| {
                ToolError::new("write_file requires only string path and content arguments")
            })?;
        let path = arguments
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ToolError::new("write_file requires only string path and content arguments")
            })?;
        let content = arguments
            .get("content")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ToolError::new("write_file requires only string path and content arguments")
            })?;
        let path = self.resolve_path(path)?;

        if path.is_dir() {
            return Err(ToolError::new("path points to a directory"));
        }

        fs::write(&path, content)
            .map_err(|error| ToolError::new(format!("could not write file: {error}")))?;

        Ok(serde_json::json!({"path": path, "bytes_written": content.len()}))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{GetCurrentDirectory, ReadFile, WriteFile};
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
        let workspace = workspace();
        let workspace = workspace.canonicalize().expect("workspace should exist");
        let result = GetCurrentDirectory::new(&workspace)
            .execute(serde_json::json!({}))
            .expect("tool should execute");

        assert_eq!(result["directory"], serde_json::json!(workspace));
        fs::remove_dir_all(workspace).expect("workspace should be removed");
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

    #[test]
    fn writes_new_utf8_file_in_workspace() {
        let workspace = workspace();
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let result = tool
            .execute(serde_json::json!({"path": "note.txt", "content": "hello Lya"}))
            .expect("file should be written");

        assert_eq!(
            fs::read_to_string(workspace.join("note.txt")).expect("file should exist"),
            "hello Lya"
        );
        assert_eq!(result["bytes_written"], 9);
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn replaces_existing_file() {
        let workspace = workspace();
        let path = workspace.join("note.txt");
        fs::write(&path, "old content").expect("file should be written");
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        tool.execute(serde_json::json!({"path": "note.txt", "content": "new content"}))
            .expect("file should be replaced");

        assert_eq!(
            fs::read_to_string(path).expect("file should exist"),
            "new content"
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn write_rejects_absolute_path() {
        let workspace = workspace();
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "/tmp/outside.txt", "content": "content"}))
            .expect_err("absolute path should fail");

        assert_eq!(error.to_string(), "path must be relative to the workspace");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn write_rejects_parent_directory_traversal() {
        let workspace = workspace();
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "../outside.txt", "content": "content"}))
            .expect_err("traversal should fail");

        assert_eq!(error.to_string(), "path must not contain '..'");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn write_rejects_missing_parent_directory() {
        let workspace = workspace();
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "missing/note.txt", "content": "content"}))
            .expect_err("missing parent should fail");

        assert!(
            error
                .to_string()
                .contains("could not access parent directory")
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn write_rejects_symlink_to_outside_workspace() {
        use std::os::unix::fs::symlink;

        let workspace = workspace();
        let outside = workspace.with_extension("outside");
        fs::create_dir(&outside).expect("outside directory should be created");
        symlink(&outside, workspace.join("escape")).expect("symlink should be created");
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "escape/outside.txt", "content": "content"}))
            .expect_err("symlink escape should fail");

        assert_eq!(error.to_string(), "path resolves outside the workspace");
        assert!(!outside.join("outside.txt").exists());
        fs::remove_dir_all(workspace).expect("workspace should be removed");
        fs::remove_dir_all(outside).expect("outside directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn write_rejects_existing_file_symlink_to_outside_workspace() {
        use std::os::unix::fs::symlink;

        let workspace = workspace();
        let outside = workspace.with_extension("outside");
        fs::write(&outside, "original content").expect("outside file should be created");
        symlink(&outside, workspace.join("escape.txt")).expect("symlink should be created");
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "escape.txt", "content": "content"}))
            .expect_err("symlink escape should fail");

        assert_eq!(error.to_string(), "path resolves outside the workspace");
        assert_eq!(
            fs::read_to_string(&outside).expect("outside file should exist"),
            "original content"
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
        fs::remove_file(outside).expect("outside file should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn write_rejects_dangling_symlink() {
        use std::os::unix::fs::symlink;

        let workspace = workspace();
        let outside = workspace.with_extension("outside");
        symlink(&outside, workspace.join("escape.txt")).expect("symlink should be created");
        let tool = WriteFile::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "escape.txt", "content": "content"}))
            .expect_err("dangling symlink should fail");

        assert!(
            error
                .to_string()
                .contains("could not access requested file")
        );
        assert!(!outside.exists());
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }
}
