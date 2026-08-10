use std::env;

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

#[cfg(test)]
mod tests {
    use super::GetCurrentDirectory;
    use crate::tools::Tool;

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
}
