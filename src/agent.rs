use std::{error::Error, fmt};

use crate::{
    llm::{ChatMessage, ChatRequest, LlmClient, LlmError},
    tools::Tool,
};

pub struct Agent<'a, Client> {
    client: &'a Client,
    tools: Vec<Box<dyn Tool>>,
    max_tool_calls: usize,
}

impl<'a, Client: LlmClient> Agent<'a, Client> {
    pub const DEFAULT_MAX_TOOL_CALLS: usize = 16;

    pub fn new(client: &'a Client, tools: Vec<Box<dyn Tool>>) -> Self {
        Self {
            client,
            tools,
            max_tool_calls: Self::DEFAULT_MAX_TOOL_CALLS,
        }
    }

    pub fn with_max_tool_calls(mut self, max_tool_calls: usize) -> Self {
        self.max_tool_calls = max_tool_calls;
        self
    }

    pub async fn run(
        &self,
        model: impl Into<String>,
        prompt: impl Into<String>,
    ) -> Result<String, AgentError> {
        let model = model.into();
        let mut messages = vec![ChatMessage::user(prompt)];
        let definitions: Vec<_> = self.tools.iter().map(|tool| tool.definition()).collect();
        let mut tool_calls_used = 0;

        loop {
            let response = self
                .client
                .chat(ChatRequest::new(
                    model.clone(),
                    messages.clone(),
                    definitions.clone(),
                ))
                .await
                .map_err(AgentError::Llm)?;
            let message = response.message;

            if let Some(tool_calls) = &message.tool_calls {
                messages.push(message.clone());

                for tool_call in tool_calls {
                    if tool_calls_used == self.max_tool_calls {
                        return Err(AgentError::ToolCallLimitReached {
                            limit: self.max_tool_calls,
                        });
                    }
                    tool_calls_used += 1;
                    let content = self.execute_tool_call(tool_call)?;

                    messages.push(ChatMessage::tool_result(&tool_call.id, content));
                }
            } else if let Some(content) = message.content {
                return Ok(content);
            } else {
                return Err(AgentError::MissingFinalContent);
            }
        }
    }

    fn execute_tool_call(&self, tool_call: &crate::llm::ToolCall) -> Result<String, AgentError> {
        let name = &tool_call.function.name;
        let result = match serde_json::from_str(&tool_call.function.arguments) {
            Err(error) => tool_error_result(name, format!("invalid JSON arguments: {error}")),
            Ok(arguments) => match self
                .tools
                .iter()
                .find(|tool| tool.definition().function.name == *name)
            {
                None => tool_error_result(name, "unknown tool"),
                Some(tool) => match tool.execute(arguments) {
                    Ok(result) => result,
                    Err(error) => tool_error_result(name, error.to_string()),
                },
            },
        };

        serde_json::to_string(&result).map_err(AgentError::SerializeToolResult)
    }
}

fn tool_error_result(tool: &str, message: impl Into<String>) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "tool": tool,
            "message": message.into()
        }
    })
}

#[derive(Debug)]
pub enum AgentError {
    Llm(LlmError),
    SerializeToolResult(serde_json::Error),
    MissingFinalContent,
    ToolCallLimitReached { limit: usize },
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Llm(error) => write!(formatter, "LLM error: {error}"),
            Self::SerializeToolResult(error) => {
                write!(formatter, "could not serialize tool result: {error}")
            }
            Self::MissingFinalContent => write!(formatter, "LLM response has no final content"),
            Self::ToolCallLimitReached { limit } => {
                write!(formatter, "tool call limit of {limit} reached")
            }
        }
    }
}

impl Error for AgentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Llm(error) => Some(error),
            Self::SerializeToolResult(error) => Some(error),
            Self::MissingFinalContent | Self::ToolCallLimitReached { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{
        llm::{ChatMessage, ChatResponse, ToolCall, ToolCallFunction},
        tools::{Tool, ToolError, filesystem::GetCurrentDirectory},
    };

    use super::{Agent, AgentError};

    struct FailingTool;

    impl Tool for FailingTool {
        fn definition(&self) -> crate::llm::ToolDefinition {
            crate::llm::ToolDefinition::function(
                "failing_tool",
                "Always fails.",
                serde_json::json!({"type": "object", "properties": {}}),
            )
        }

        fn execute(&self, _arguments: serde_json::Value) -> Result<serde_json::Value, ToolError> {
            Err(ToolError::new("simulated execution failure"))
        }
    }

    struct FakeClient {
        responses: Mutex<Vec<ChatResponse>>,
        requests: Mutex<Vec<crate::llm::ChatRequest>>,
    }

    impl FakeClient {
        fn new(responses: Vec<ChatResponse>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().rev().collect()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<crate::llm::ChatRequest> {
            self.requests
                .lock()
                .expect("requests should not be poisoned")
                .clone()
        }
    }

    impl crate::llm::LlmClient for FakeClient {
        async fn chat(
            &self,
            request: crate::llm::ChatRequest,
        ) -> Result<ChatResponse, crate::llm::LlmError> {
            self.requests
                .lock()
                .expect("requests should not be poisoned")
                .push(request);
            Ok(self
                .responses
                .lock()
                .expect("responses should not be poisoned")
                .pop()
                .expect("a response should be available"))
        }
    }

    fn tool_call(name: &str, arguments: &str) -> ChatResponse {
        ChatResponse {
            message: ChatMessage {
                role: "assistant".to_owned(),
                content: None,
                tool_calls: Some(vec![ToolCall {
                    id: "call_123".to_owned(),
                    kind: "function".to_owned(),
                    function: ToolCallFunction {
                        name: name.to_owned(),
                        arguments: arguments.to_owned(),
                    },
                }]),
                tool_call_id: None,
            },
        }
    }

    fn final_response(content: &str) -> ChatResponse {
        ChatResponse {
            message: ChatMessage {
                role: "assistant".to_owned(),
                content: Some(content.to_owned()),
                tool_calls: None,
                tool_call_id: None,
            },
        }
    }

    #[tokio::test]
    async fn sends_tool_result_before_returning_final_answer() {
        let client = FakeClient::new(vec![
            tool_call("get_current_directory", "{}"),
            final_response("The current directory was returned."),
        ]);
        let agent = Agent::new(&client, vec![Box::new(GetCurrentDirectory)]);

        let answer = agent
            .run("test", "Where am I?")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "The current directory was returned.");
        let requests = client.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].tools.len(), 1);
        assert_eq!(requests[1].messages.len(), 3);
        assert_eq!(requests[1].messages[2].role, "tool");
        assert_eq!(
            requests[1].messages[2].tool_call_id.as_deref(),
            Some("call_123")
        );
        assert!(requests[1].messages[1].tool_calls.is_some());
    }

    #[tokio::test]
    async fn reports_unknown_tool_to_llm() {
        let client = FakeClient::new(vec![
            tool_call("unknown", "{}"),
            final_response("I cannot use that tool."),
        ]);
        let agent = Agent::new(&client, vec![Box::new(GetCurrentDirectory)]);

        let answer = agent
            .run("test", "prompt")
            .await
            .expect("LLM should receive the tool error");

        assert_eq!(answer, "I cannot use that tool.");
        let requests = client.requests();
        assert_eq!(requests[1].messages[1].role, "assistant");
        assert_eq!(requests[1].messages[2].role, "tool");
        assert_eq!(
            requests[1].messages[2].content.as_deref(),
            Some(r#"{"error":{"message":"unknown tool","tool":"unknown"}}"#)
        );
    }

    #[tokio::test]
    async fn reports_invalid_tool_arguments_to_llm() {
        let client = FakeClient::new(vec![
            tool_call("get_current_directory", "not json"),
            final_response("The tool arguments were invalid."),
        ]);
        let agent = Agent::new(&client, vec![Box::new(GetCurrentDirectory)]);

        let answer = agent
            .run("test", "prompt")
            .await
            .expect("LLM should receive the tool error");

        assert_eq!(answer, "The tool arguments were invalid.");
        let requests = client.requests();
        assert!(
            requests[1].messages[2]
                .content
                .as_deref()
                .expect("tool result should have content")
                .contains("invalid JSON arguments")
        );
    }

    #[tokio::test]
    async fn reports_tool_execution_error_to_llm() {
        let client = FakeClient::new(vec![
            tool_call("failing_tool", "{}"),
            final_response("The tool failed."),
        ]);
        let agent = Agent::new(&client, vec![Box::new(FailingTool)]);

        let answer = agent
            .run("test", "prompt")
            .await
            .expect("LLM should receive the execution error");

        assert_eq!(answer, "The tool failed.");
        let requests = client.requests();
        assert_eq!(
            requests[1].messages[2].content.as_deref(),
            Some(r#"{"error":{"message":"simulated execution failure","tool":"failing_tool"}}"#)
        );
    }

    #[tokio::test]
    async fn stops_after_configured_tool_call_limit() {
        let client = FakeClient::new(vec![
            tool_call("get_current_directory", "{}"),
            tool_call("get_current_directory", "{}"),
        ]);
        let agent = Agent::new(&client, vec![Box::new(GetCurrentDirectory)]).with_max_tool_calls(1);

        let error = agent
            .run("test", "prompt")
            .await
            .expect_err("second tool call should exceed the limit");

        assert!(matches!(
            error,
            AgentError::ToolCallLimitReached { limit: 1 }
        ));
        assert_eq!(client.requests().len(), 2);
    }
}
