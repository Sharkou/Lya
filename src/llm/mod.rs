pub mod ollama;

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl ChatMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_owned(),
            content: Some(content.into()),
            tool_calls: None,
            tool_call_id: None,
        }
    }

    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: "tool".to_owned(),
            content: Some(content.into()),
            tool_calls: None,
            tool_call_id: Some(tool_call_id.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolDefinition>,
}

impl ChatRequest {
    pub fn new(
        model: impl Into<String>,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Self {
        Self {
            model: model.into(),
            messages,
            tools,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatResponse {
    pub message: ChatMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolDefinition {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ToolFunction,
}

impl ToolDefinition {
    pub fn function(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: serde_json::Value,
    ) -> Self {
        Self {
            kind: "function".to_owned(),
            function: ToolFunction {
                name: name.into(),
                description: description.into(),
                parameters,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolFunction {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallFunction {
    pub name: String,
    pub arguments: String,
}

pub trait LlmClient {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, LlmError>;
}

#[derive(Debug)]
pub enum LlmError {
    Network(reqwest::Error),
    Http {
        status: reqwest::StatusCode,
        body: String,
    },
    Json(serde_json::Error),
    NoChoices,
    MissingContent,
}

impl fmt::Display for LlmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network(error) => write!(formatter, "network error: {error}"),
            Self::Http { status, body } => write!(formatter, "HTTP {status}: {body}"),
            Self::Json(error) => write!(formatter, "invalid JSON response: {error}"),
            Self::NoChoices => write!(formatter, "response contains no completion choices"),
            Self::MissingContent => write!(formatter, "response choice has no message content"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChatMessage, ToolDefinition};

    #[test]
    fn serializes_tool_declaration() {
        let tool = ToolDefinition::function(
            "get_current_directory",
            "Returns Lya's current working directory.",
            serde_json::json!({"type": "object", "properties": {}, "additionalProperties": false}),
        );

        assert_eq!(
            serde_json::to_value(tool).expect("tool should serialize"),
            serde_json::json!({
                "type": "function",
                "function": {
                    "name": "get_current_directory",
                    "description": "Returns Lya's current working directory.",
                    "parameters": {"type": "object", "properties": {}, "additionalProperties": false}
                }
            })
        );
    }

    #[test]
    fn creates_tool_result_message() {
        let message = ChatMessage::tool_result("call_123", r#"{"directory":"/tmp"}"#);

        assert_eq!(message.role, "tool");
        assert_eq!(message.tool_call_id.as_deref(), Some("call_123"));
        assert_eq!(message.content.as_deref(), Some(r#"{"directory":"/tmp"}"#));
    }
}

impl Error for LlmError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Network(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Http { .. } | Self::NoChoices | Self::MissingContent => None,
        }
    }
}
