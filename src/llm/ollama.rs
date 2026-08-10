use serde::{Deserialize, Serialize};

use super::{ChatRequest, ChatResponse, LlmClient, LlmError};

pub struct OllamaClient {
    client: reqwest::Client,
    base_url: String,
}

impl OllamaClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
        }
    }
}

impl LlmClient for OllamaClient {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        let url = format!("{}/chat/completions", self.base_url);
        let debug = std::env::var_os("LYA_OLLAMA_DEBUG").is_some();
        let body = OpenAiChatRequest {
            model: &request.model,
            messages: &request.messages,
            tools: &request.tools,
        };
        if debug {
            let body = serde_json::to_string(&body).map_err(LlmError::Json)?;
            eprintln!("LYA_OLLAMA_DEBUG request: {body}");
        }
        let response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(LlmError::Network)?;
        let status = response.status();
        let body = response.text().await.map_err(LlmError::Network)?;

        if debug {
            eprintln!("LYA_OLLAMA_DEBUG response: {body}");
        }

        if !status.is_success() {
            return Err(LlmError::Http { status, body });
        }

        parse_response(&body)
    }
}

#[derive(Serialize)]
struct OpenAiChatRequest<'a> {
    model: &'a str,
    messages: &'a [super::ChatMessage],
    tools: &'a [super::ToolDefinition],
}

#[derive(Deserialize)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: super::ChatMessage,
}

fn parse_response(body: &str) -> Result<ChatResponse, LlmError> {
    let response: OpenAiChatResponse = serde_json::from_str(body).map_err(LlmError::Json)?;
    let choice = response
        .choices
        .into_iter()
        .next()
        .ok_or(LlmError::NoChoices)?;
    if choice.message.content.is_none() && choice.message.tool_calls.is_none() {
        return Err(LlmError::MissingContent);
    }

    Ok(ChatResponse {
        message: choice.message,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_response;
    use crate::llm::LlmError;

    #[test]
    fn parses_openai_compatible_response() {
        let response = parse_response(
            r#"{"choices":[{"message":{"role":"assistant","content":"Connected"}}]}"#,
        )
        .expect("response should deserialize");

        assert_eq!(response.message.content.as_deref(), Some("Connected"));
    }

    #[test]
    fn parses_response_with_tool_call() {
        let response = parse_response(
            r#"{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_123","type":"function","function":{"name":"get_current_directory","arguments":"{}"}}]}}]}"#,
        )
        .expect("response should deserialize");

        let tool_calls = response
            .message
            .tool_calls
            .expect("tool calls should be present");
        assert_eq!(tool_calls[0].function.name, "get_current_directory");
        assert_eq!(tool_calls[0].function.arguments, "{}");
    }

    #[test]
    fn rejects_response_without_choices() {
        let error = parse_response(r#"{"choices":[]}"#).expect_err("choices are required");

        assert!(matches!(error, LlmError::NoChoices));
    }

    #[test]
    fn rejects_invalid_json() {
        let error = parse_response("not json").expect_err("JSON is invalid");

        assert!(matches!(error, LlmError::Json(_)));
    }
}
