use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

use crate::{
    llm::ToolDefinition,
    tools::{Tool, ToolError, filesystem::validate_relative_path},
};

pub struct CreateDirectory {
    workspace: PathBuf,
}

impl CreateDirectory {
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

    fn create(&self, path: &str) -> Result<PathBuf, ToolError> {
        let requested = validate_relative_path(path)?;
        let mut existing = self.workspace.clone();

        for component in requested.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            let next = existing.join(component);
            match fs::symlink_metadata(&next) {
                Ok(_) => {
                    let resolved = next.canonicalize().map_err(|error| {
                        ToolError::new(format!("could not access requested directory: {error}"))
                    })?;
                    if !resolved.starts_with(&self.workspace) {
                        return Err(ToolError::new("path resolves outside the workspace"));
                    }
                    existing = resolved;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(ToolError::new(format!(
                        "could not access requested directory: {error}"
                    )));
                }
            }
        }

        let target = self.workspace.join(requested);
        fs::create_dir_all(&target)
            .map_err(|error| ToolError::new(format!("could not create directory: {error}")))?;
        let resolved = target.canonicalize().map_err(|error| {
            ToolError::new(format!("could not access created directory: {error}"))
        })?;
        if !resolved.starts_with(&self.workspace) {
            return Err(ToolError::new("path resolves outside the workspace"));
        }
        if !resolved.is_dir() {
            return Err(ToolError::new("path points to a file"));
        }

        Ok(resolved)
    }
}

impl Tool for CreateDirectory {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "create_directory",
            "Creates a directory and any missing parent directories relative to Lya's workspace.",
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
            .ok_or_else(|| {
                ToolError::new("create_directory requires only a string path argument")
            })?;
        let path = self.create(path)?;

        Ok(serde_json::json!({"path": path}))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::CreateDirectory;
    use crate::tools::Tool;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn workspace() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-create-directory-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("workspace should be created");
        directory
    }

    #[test]
    fn creates_directory() {
        let workspace = workspace();
        let tool = CreateDirectory::new(&workspace).expect("workspace should be valid");

        let result = tool
            .execute(serde_json::json!({"path": "project"}))
            .expect("directory should be created");

        assert!(workspace.join("project").is_dir());
        assert_eq!(
            result["path"],
            serde_json::json!(
                workspace
                    .join("project")
                    .canonicalize()
                    .expect("directory should exist")
            )
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn creates_nested_directories() {
        let workspace = workspace();
        let tool = CreateDirectory::new(&workspace).expect("workspace should be valid");

        tool.execute(serde_json::json!({"path": "projects/hello-world/src"}))
            .expect("directories should be created");

        assert!(workspace.join("projects/hello-world/src").is_dir());
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn accepts_existing_directory() {
        let workspace = workspace();
        fs::create_dir(workspace.join("project")).expect("directory should be created");
        let tool = CreateDirectory::new(&workspace).expect("workspace should be valid");

        tool.execute(serde_json::json!({"path": "project"}))
            .expect("existing directory should succeed");

        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_absolute_path() {
        let workspace = workspace();
        let tool = CreateDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "/tmp/outside"}))
            .expect_err("absolute path should fail");

        assert_eq!(error.to_string(), "path must be relative to the workspace");
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[test]
    fn rejects_parent_directory_traversal() {
        let workspace = workspace();
        let tool = CreateDirectory::new(&workspace).expect("workspace should be valid");

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
        let tool = CreateDirectory::new(&workspace).expect("workspace should be valid");

        let error = tool
            .execute(serde_json::json!({"path": "escape/nested"}))
            .expect_err("symlink escape should fail");

        assert_eq!(error.to_string(), "path resolves outside the workspace");
        assert!(!outside.join("nested").exists());
        fs::remove_dir_all(workspace).expect("workspace should be removed");
        fs::remove_dir_all(outside).expect("outside directory should be removed");
    }
}
