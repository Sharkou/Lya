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
        let body = OpenAiChatRequest {
            model: &request.model,
            messages: &request.messages,
        };
        let response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(LlmError::Network)?;
        let status = response.status();
        let body = response.text().await.map_err(LlmError::Network)?;

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
}

#[derive(Deserialize)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: OpenAiMessage,
}

#[derive(Deserialize)]
struct OpenAiMessage {
    content: Option<String>,
}

fn parse_response(body: &str) -> Result<ChatResponse, LlmError> {
    let response: OpenAiChatResponse = serde_json::from_str(body).map_err(LlmError::Json)?;
    let choice = response
        .choices
        .into_iter()
        .next()
        .ok_or(LlmError::NoChoices)?;
    let content = choice.message.content.ok_or(LlmError::MissingContent)?;

    Ok(ChatResponse { content })
}

#[cfg(test)]
mod tests {
    use super::parse_response;
    use crate::llm::LlmError;

    #[test]
    fn parses_openai_compatible_response() {
        let response = parse_response(r#"{"choices":[{"message":{"content":"Connected"}}]}"#)
            .expect("response should deserialize");

        assert_eq!(response.content, "Connected");
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
