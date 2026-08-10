pub mod ollama;

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

impl ChatMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_owned(),
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
}

impl ChatRequest {
    pub fn new(model: impl Into<String>, messages: Vec<ChatMessage>) -> Self {
        Self {
            model: model.into(),
            messages,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatResponse {
    pub content: String,
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

impl Error for LlmError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Network(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Http { .. } | Self::NoChoices | Self::MissingContent => None,
        }
    }
}
