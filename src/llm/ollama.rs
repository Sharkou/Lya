use serde::{Deserialize, Serialize};

use super::{
    ChatMessage, ChatRequest, ChatResponse, LlmClient, LlmError, ToolCall, ToolCallFunction,
};

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
            tools: &request.tools,
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
    tools: &'a [super::ToolDefinition],
}

#[derive(Deserialize)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: ChatMessage,
}

fn parse_response(body: &str) -> Result<ChatResponse, LlmError> {
    let response: OpenAiChatResponse = serde_json::from_str(body).map_err(LlmError::Json)?;
    let choice = response
        .choices
        .into_iter()
        .next()
        .ok_or(LlmError::NoChoices)?;
    let mut message = choice.message;
    normalize_qwen_tool_call(&mut message);

    if message.content.is_none() && message.tool_calls.is_none() {
        return Err(LlmError::MissingContent);
    }

    Ok(ChatResponse { message })
}

fn normalize_qwen_tool_call(message: &mut ChatMessage) {
    if message.tool_calls.is_some() {
        return;
    }
    let Some(content) = &message.content else {
        return;
    };
    let Some((tool_call, remaining_content)) = parse_qwen_tool_call(content) else {
        return;
    };

    message.tool_calls = Some(vec![tool_call]);
    message.content = (!remaining_content.is_empty()).then_some(remaining_content);
}

fn parse_qwen_tool_call(content: &str) -> Option<(ToolCall, String)> {
    let start = content.find("<function=")?;
    let name_start = start + "<function=".len();
    let name_end = content[name_start..].find('>')? + name_start;
    let name = content[name_start..name_end].trim();
    if name.is_empty() {
        return None;
    }

    let body_start = name_end + 1;
    let remaining = &content[body_start..];
    let (body_end, end_marker) = remaining
        .find("</tool_call>")
        .map(|index| (body_start + index, "</tool_call>"))
        .or_else(|| {
            remaining
                .find("<tool_call>")
                .map(|index| (body_start + index, "<tool_call>"))
        })?;
    let parameters = parse_qwen_parameters(&content[body_start..body_end])?;
    let arguments = serde_json::to_string(&parameters).ok()?;
    let before = content[..start].trim();
    let after = content[body_end + end_marker.len()..].trim();
    let remaining_content = match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (false, true) => before.to_owned(),
        (true, false) => after.to_owned(),
        (false, false) => format!("{before}\n{after}"),
    };

    Some((
        ToolCall {
            id: "qwen-tool-call-0".to_owned(),
            kind: "function".to_owned(),
            function: ToolCallFunction {
                name: name.to_owned(),
                arguments,
            },
        },
        remaining_content,
    ))
}

fn parse_qwen_parameters(body: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut parameters = serde_json::Map::new();
    let mut current_name = None;
    let mut current_value = String::new();

    for line in body.lines() {
        let line = line.trim();
        if line == "</parameter>" {
            if let Some(name) = current_name.take() {
                parameters.insert(
                    name,
                    serde_json::Value::String(current_value.trim().to_owned()),
                );
                current_value.clear();
            }
        } else if line == "</function>" {
            break;
        } else if let Some(name) = line
            .strip_prefix("<parameter=")
            .and_then(|value| value.strip_suffix('>'))
        {
            if let Some(name) = current_name.take() {
                parameters.insert(
                    name,
                    serde_json::Value::String(current_value.trim().to_owned()),
                );
                current_value.clear();
            }
            if name.is_empty() {
                return None;
            }
            current_name = Some(name.to_owned());
        } else if current_name.is_some() {
            if !current_value.is_empty() {
                current_value.push('\n');
            }
            current_value.push_str(line);
        }
    }

    if let Some(name) = current_name {
        parameters.insert(
            name,
            serde_json::Value::String(current_value.trim().to_owned()),
        );
    }

    (!parameters.is_empty()).then_some(parameters)
}

#[cfg(test)]
mod tests {
    use super::{normalize_qwen_tool_call, parse_response};
    use crate::llm::{ChatMessage, LlmError};

    const QWEN_WRITE_FILE_CALL: &str = "Je vais créer un fichier test.txt avec le contenu spécifié.\n\n<function=write_file>\n<parameter=path>\ntest.txt\n</parameter>\n<parameter=content>\nBonjour depuis Lya\n</parameter>\n</function>\n</tool_call>";

    fn assert_qwen_write_file_call(message: &ChatMessage) {
        let tool_calls = message
            .tool_calls
            .as_ref()
            .expect("tool call should be present");

        assert_eq!(tool_calls[0].id, "qwen-tool-call-0");
        assert_eq!(tool_calls[0].function.name, "write_file");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&tool_calls[0].function.arguments)
                .expect("arguments should be JSON"),
            serde_json::json!({"path": "test.txt", "content": "Bonjour depuis Lya"})
        );
        assert_eq!(
            message.content.as_deref(),
            Some("Je vais créer un fichier test.txt avec le contenu spécifié.")
        );
    }

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
    fn normalizes_qwen_textual_write_file_tool_call() {
        let response = parse_response(
            r#"{"choices":[{"message":{"role":"assistant","content":"<function=write_file>\n<parameter=path>\ntest.txt\n</parameter>\n<parameter=content>\nBonjour depuis Lya\n</parameter>\n</function>\n</tool_call>"}}]}"#,
        )
        .expect("response should normalize");

        let tool_calls = response
            .message
            .tool_calls
            .expect("tool call should be present");
        assert_eq!(tool_calls[0].id, "qwen-tool-call-0");
        assert_eq!(tool_calls[0].function.name, "write_file");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&tool_calls[0].function.arguments)
                .expect("arguments should be JSON"),
            serde_json::json!({"content": "Bonjour depuis Lya", "path": "test.txt"})
        );
        assert_eq!(response.message.content, None);
    }

    #[test]
    fn normalizes_exact_qwen_write_file_content() {
        let mut message = ChatMessage {
            role: "assistant".to_owned(),
            content: Some(QWEN_WRITE_FILE_CALL.to_owned()),
            tool_calls: None,
            tool_call_id: None,
        };

        normalize_qwen_tool_call(&mut message);

        assert_qwen_write_file_call(&message);
    }

    #[test]
    fn normalizes_qwen_call_after_deserializing_http_response() {
        let body = serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": QWEN_WRITE_FILE_CALL
                }
            }]
        })
        .to_string();

        let response = parse_response(&body).expect("response should normalize");

        assert_qwen_write_file_call(&response.message);
    }

    #[test]
    fn preserves_content_outside_qwen_tool_call() {
        let response = parse_response(
            r#"{"choices":[{"message":{"role":"assistant","content":"I will write the file.\n<function=write_file>\n<parameter=path>\ntest.txt\n<parameter=content>\nHello\n<tool_call>\nDone."}}]}"#,
        )
        .expect("response should normalize");

        assert_eq!(
            response.message.content.as_deref(),
            Some("I will write the file.\nDone.")
        );
        assert!(response.message.tool_calls.is_some());
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
