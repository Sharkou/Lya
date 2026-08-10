use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    llm::ToolDefinition,
    tools::{Tool, ToolError, filesystem::validate_relative_path},
};

pub struct ListDirectory {
    workspace: PathBuf,
}

impl ListDirectory {
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

    fn resolve_directory(&self, path: &str) -> Result<PathBuf, ToolError> {
        let requested = validate_relative_path(path)?;
        let directory = self
            .workspace
            .join(requested)
            .canonicalize()
            .map_err(|error| {
                ToolError::new(format!("could not access requested directory: {error}"))
            })?;
        if !directory.starts_with(&self.workspace) {
            return Err(ToolError::new("path resolves outside the workspace"));
        }
        if !directory.is_dir() {
            return Err(ToolError::new("path points to a file"));
        }

        Ok(directory)
    }

    fn list(&self, path: &str) -> Result<Vec<serde_json::Value>, ToolError> {
        let directory = self.resolve_directory(path)?;
        let mut entries = Vec::new();

        for entry in fs::read_dir(&directory)
            .map_err(|error| ToolError::new(format!("could not list directory: {error}")))?
        {
            let entry = entry.map_err(|error| {
                ToolError::new(format!("could not read directory entry: {error}"))
            })?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
                ToolError::new(format!("could not access directory entry: {error}"))
            })?;
            let file_type = metadata.file_type();
            let entry_type = if file_type.is_file() {
                "file"
            } else if file_type.is_dir() {
                "directory"
            } else {
                continue;
            };
            let name = entry.file_name().to_string_lossy().into_owned();

            entries.push(serde_json::json!({"name": name, "type": entry_type}));
        }

        entries.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
        Ok(entries)
    }
}

impl Tool for ListDirectory {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "list_directory",
            "Lists files and directories relative to Lya's workspace.",
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
            .ok_or_else(|| ToolError::new("list_directory requires only a string path argument"))?;

        Ok(serde_json::json!({"entries": self.list(path)?}))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::ListDirectory;
    use crate::tools::Tool;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn workspace() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-list-directory-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("workspace should be created");
        directory
    }

    #[test]
    fn lists_workspace_root_in_name_order() {
        let workspace = workspace();
        fs::write(workspace.join("hello.txt"), "hello").expect("file should be created");
        fs::create_dir(workspace.join("projects")).expect("directory should be created");
        fs::create_dir(workspace.join("test-project")).expect("directory should be created");
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let result = tool
            .execute(serde_json::json!({"path": "."}))
            .expect("workspace should be listed");

        assert_eq!(
            result,
            serde_json::json!({"entries": [
                {"name": "hello.txt", "type": "file"},
                {"name": "projects", "type": "directory"},
                {"name": "test-project", "type": "directory"}
            ]})
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn lists_subdirectory() {
        let workspace = workspace();
        fs::create_dir_all(workspace.join("projects/demo")).expect("directory should be created");
        fs::write(workspace.join("projects/demo/readme.md"), "hello")
            .expect("file should be created");
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let result = tool
            .execute(serde_json::json!({"path": "projects/demo"}))
            .expect("subdirectory should be listed");

        assert_eq!(
            result,
            serde_json::json!({"entries": [{"name": "readme.md", "type": "file"}]})
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn lists_empty_directory() {
        let workspace = workspace();
        fs::create_dir(workspace.join("empty")).expect("directory should be created");
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let result = tool
            .execute(serde_json::json!({"path": "empty"}))
            .expect("empty directory should be listed");

        assert_eq!(result, serde_json::json!({"entries": []}));
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_missing_directory() {
        let workspace = workspace();
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "missing"}))
            .expect_err("missing directory should fail");

        assert!(
            error
                .to_string()
                .contains("could not access requested directory")
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_file_path() {
        let workspace = workspace();
        fs::write(workspace.join("note.txt"), "hello").expect("file should be created");
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "note.txt"}))
            .expect_err("file path should fail");

        assert_eq!(error.to_string(), "path points to a file");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_absolute_path() {
        let workspace = workspace();
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "/tmp/outside"}))
            .expect_err("absolute path should fail");

        assert_eq!(error.to_string(), "path must be relative to the workspace");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_parent_directory_traversal() {
        let workspace = workspace();
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "../outside"}))
            .expect_err("parent directory traversal should fail");

        assert_eq!(error.to_string(), "path must not contain '..'");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_to_outside_workspace() {
        use std::os::unix::fs::symlink;

        let workspace = workspace();
        let outside = workspace.with_extension("outside");
        fs::create_dir(&outside).expect("outside directory should be created");
        symlink(&outside, workspace.join("escape")).expect("symlink should be created");
        let tool = ListDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "escape"}))
            .expect_err("symlink escape should fail");

        assert_eq!(error.to_string(), "path resolves outside the workspace");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
        fs::remove_dir_all(outside).expect("outside directory should be removed");
    }
}
