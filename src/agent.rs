use std::{error::Error, fmt};

use crate::{
    llm::{ChatMessage, ChatRequest, LlmClient, LlmError},
    tools::{Tool, ToolError},
};

pub struct Agent<'a, Client> {
    client: &'a Client,
    tools: Vec<Box<dyn Tool>>,
}

impl<'a, Client: LlmClient> Agent<'a, Client> {
    pub fn new(client: &'a Client, tools: Vec<Box<dyn Tool>>) -> Self {
        Self { client, tools }
    }

    pub async fn run(
        &self,
        model: impl Into<String>,
        prompt: impl Into<String>,
    ) -> Result<String, AgentError> {
        let model = model.into();
        let mut messages = vec![ChatMessage::user(prompt)];
        let definitions: Vec<_> = self.tools.iter().map(|tool| tool.definition()).collect();

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
                    let arguments =
                        serde_json::from_str(&tool_call.function.arguments).map_err(|error| {
                            AgentError::InvalidToolArguments {
                                tool: tool_call.function.name.clone(),
                                error,
                            }
                        })?;
                    let tool = self
                        .tools
                        .iter()
                        .find(|tool| tool.definition().function.name == tool_call.function.name)
                        .ok_or_else(|| AgentError::UnknownTool(tool_call.function.name.clone()))?;
                    let result = tool.execute(arguments).map_err(|error| AgentError::Tool {
                        tool: tool_call.function.name.clone(),
                        error,
                    })?;
                    let content =
                        serde_json::to_string(&result).map_err(AgentError::SerializeToolResult)?;

                    messages.push(ChatMessage::tool_result(&tool_call.id, content));
                }
            } else if let Some(content) = message.content {
                return Ok(content);
            } else {
                return Err(AgentError::MissingFinalContent);
            }
        }
    }
}

#[derive(Debug)]
pub enum AgentError {
    Llm(LlmError),
    InvalidToolArguments {
        tool: String,
        error: serde_json::Error,
    },
    UnknownTool(String),
    Tool {
        tool: String,
        error: ToolError,
    },
    SerializeToolResult(serde_json::Error),
    MissingFinalContent,
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Llm(error) => write!(formatter, "LLM error: {error}"),
            Self::InvalidToolArguments { tool, error } => {
                write!(formatter, "invalid arguments for tool {tool}: {error}")
            }
            Self::UnknownTool(tool) => write!(formatter, "unknown tool requested: {tool}"),
            Self::Tool { tool, error } => write!(formatter, "tool {tool} failed: {error}"),
            Self::SerializeToolResult(error) => {
                write!(formatter, "could not serialize tool result: {error}")
            }
            Self::MissingFinalContent => write!(formatter, "LLM response has no final content"),
        }
    }
}

impl Error for AgentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Llm(error) => Some(error),
            Self::InvalidToolArguments { error, .. } | Self::SerializeToolResult(error) => {
                Some(error)
            }
            Self::Tool { error, .. } => Some(error),
            Self::UnknownTool(_) | Self::MissingFinalContent => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{
        llm::{ChatMessage, ChatResponse, ToolCall, ToolCallFunction},
        tools::filesystem::GetCurrentDirectory,
    };

    use super::{Agent, AgentError};

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
    }

    #[tokio::test]
    async fn rejects_unknown_tool() {
        let client = FakeClient::new(vec![tool_call("unknown", "{}")]);
        let agent = Agent::new(&client, vec![Box::new(GetCurrentDirectory)]);

        let error = agent
            .run("test", "prompt")
            .await
            .expect_err("tool is unknown");

        assert!(matches!(error, AgentError::UnknownTool(tool) if tool == "unknown"));
    }

    #[tokio::test]
    async fn rejects_invalid_tool_arguments() {
        let client = FakeClient::new(vec![tool_call("get_current_directory", "not json")]);
        let agent = Agent::new(&client, vec![Box::new(GetCurrentDirectory)]);

        let error = agent
            .run("test", "prompt")
            .await
            .expect_err("arguments are invalid");

        assert!(
            matches!(error, AgentError::InvalidToolArguments { tool, .. } if tool == "get_current_directory")
        );
    }
}
