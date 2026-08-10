pub mod command;
pub mod directory;
pub mod filesystem;
pub mod listing;

use std::{error::Error, fmt};

use crate::llm::ToolDefinition;

pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;

    fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value, ToolError>;
}

#[derive(Debug)]
pub struct ToolError {
    message: String,
}

impl ToolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ToolError {}
