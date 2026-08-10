use std::{error::Error, fmt};

use crate::{
    llm::{ChatMessage, ChatRequest, LlmClient, LlmError},
    tools::Tool,
};

pub struct Agent<'a, Client> {
    client: &'a Client,
    tools: Vec<Box<dyn Tool>>,
    max_iterations: usize,
}

impl<'a, Client: LlmClient> Agent<'a, Client> {
    pub const DEFAULT_MAX_ITERATIONS: usize = 20;

    pub fn new(client: &'a Client, tools: Vec<Box<dyn Tool>>) -> Self {
        Self {
            client,
            tools,
            max_iterations: Self::DEFAULT_MAX_ITERATIONS,
        }
    }

    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
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
        let mut iterations = 0;

        loop {
            if iterations == self.max_iterations {
                return Err(AgentError::IterationLimitReached {
                    limit: self.max_iterations,
                });
            }
            iterations += 1;
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
    IterationLimitReached { limit: usize },
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Llm(error) => write!(formatter, "LLM error: {error}"),
            Self::SerializeToolResult(error) => {
                write!(formatter, "could not serialize tool result: {error}")
            }
            Self::MissingFinalContent => write!(formatter, "LLM response has no final content"),
            Self::IterationLimitReached { limit } => {
                write!(formatter, "iteration limit of {limit} reached")
            }
        }
    }
}

impl Error for AgentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Llm(error) => Some(error),
            Self::SerializeToolResult(error) => Some(error),
            Self::MissingFinalContent | Self::IterationLimitReached { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::{
        llm::{ChatMessage, ChatResponse, ToolCall, ToolCallFunction},
        tools::{
            Tool, ToolError,
            directory::CreateDirectory,
            filesystem::{GetCurrentDirectory, ReadFile, WriteFile},
            listing::ListDirectory,
        },
    };

    use super::{Agent, AgentError};

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

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

    fn workspace() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-agent-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("workspace should be created");
        directory
    }

    #[tokio::test]
    async fn returns_immediate_final_answer_after_one_llm_call() {
        let client = FakeClient::new(vec![final_response("The task is complete.")]);
        let agent = Agent::new(&client, Vec::new());

        let answer = agent
            .run("test", "Do nothing")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "The task is complete.");
        assert_eq!(client.requests().len(), 1);
    }

    #[tokio::test]
    async fn reads_file_and_sends_content_to_llm() {
        let workspace = workspace();
        fs::write(workspace.join("answer.txt"), "workspace content")
            .expect("file should be written");
        let client = FakeClient::new(vec![
            tool_call("read_file", r#"{"path":"answer.txt"}"#),
            final_response("I read the file."),
        ]);
        let read_file = ReadFile::new(&workspace).expect("workspace should be valid");
        let agent = Agent::new(&client, vec![Box::new(read_file)]);

        let answer = agent
            .run("test", "Read answer.txt")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "I read the file.");
        let requests = client.requests();
        assert_eq!(requests[1].messages[1].role, "assistant");
        assert_eq!(
            requests[1].messages[2].content.as_deref(),
            Some(r#"{"content":"workspace content"}"#)
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[tokio::test]
    async fn continues_after_two_successive_tool_calls() {
        let workspace = workspace();
        fs::write(workspace.join("answer.txt"), "workspace content")
            .expect("file should be written");
        let client = FakeClient::new(vec![
            tool_call("list_directory", r#"{"path":"."}"#),
            tool_call("read_file", r#"{"path":"answer.txt"}"#),
            final_response("I inspected and read the file."),
        ]);
        let agent = Agent::new(
            &client,
            vec![
                Box::new(ListDirectory::new(&workspace).expect("workspace should be valid")),
                Box::new(ReadFile::new(&workspace).expect("workspace should be valid")),
            ],
        );

        let answer = agent
            .run("test", "Inspect answer.txt")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "I inspected and read the file.");
        assert_eq!(client.requests().len(), 3);
        assert_eq!(client.requests()[2].messages.len(), 5);
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[tokio::test]
    async fn writes_file_and_sends_result_to_llm() {
        let workspace = workspace();
        let client = FakeClient::new(vec![
            tool_call(
                "write_file",
                r#"{"path":"answer.txt","content":"workspace content"}"#,
            ),
            final_response("I wrote the file."),
        ]);
        let write_file = WriteFile::new(&workspace).expect("workspace should be valid");
        let agent = Agent::new(&client, vec![Box::new(write_file)]);

        let answer = agent
            .run("test", "Write answer.txt")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "I wrote the file.");
        assert_eq!(
            fs::read_to_string(workspace.join("answer.txt")).expect("file should exist"),
            "workspace content"
        );
        let requests = client.requests();
        assert_eq!(requests[1].messages[1].role, "assistant");
        assert_eq!(requests[1].messages[2].role, "tool");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                requests[1].messages[2]
                    .content
                    .as_deref()
                    .expect("tool result should have content")
            )
            .expect("tool result should be JSON")["bytes_written"],
            17
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[tokio::test]
    async fn creates_directory_and_sends_result_to_llm() {
        let workspace = workspace();
        let client = FakeClient::new(vec![
            tool_call("create_directory", r#"{"path":"projects/hello-world/src"}"#),
            final_response("I created the directory."),
        ]);
        let create_directory = CreateDirectory::new(&workspace).expect("workspace should be valid");
        let agent = Agent::new(&client, vec![Box::new(create_directory)]);

        let answer = agent
            .run("test", "Create a project directory")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "I created the directory.");
        assert!(workspace.join("projects/hello-world/src").is_dir());
        let requests = client.requests();
        let result: serde_json::Value = serde_json::from_str(
            requests[1].messages[2]
                .content
                .as_deref()
                .expect("tool result should have content"),
        )
        .expect("tool result should be JSON");
        assert_eq!(
            result["path"],
            serde_json::json!(
                workspace
                    .join("projects/hello-world/src")
                    .canonicalize()
                    .expect("directory should exist")
            )
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[tokio::test]
    async fn completes_multiple_workspace_operations() {
        let workspace = workspace();
        let client = FakeClient::new(vec![
            tool_call("create_directory", r#"{"path":"project/src"}"#),
            tool_call(
                "write_file",
                r#"{"path":"project/src/main.rs","content":"fn main() {}"}"#,
            ),
            tool_call("read_file", r#"{"path":"project/src/main.rs"}"#),
            final_response("The project file is ready."),
        ]);
        let agent = Agent::new(
            &client,
            vec![
                Box::new(CreateDirectory::new(&workspace).expect("workspace should be valid")),
                Box::new(WriteFile::new(&workspace).expect("workspace should be valid")),
                Box::new(ReadFile::new(&workspace).expect("workspace should be valid")),
            ],
        );

        let answer = agent
            .run("test", "Create and inspect a Rust project file")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "The project file is ready.");
        assert_eq!(
            fs::read_to_string(workspace.join("project/src/main.rs"))
                .expect("project file should exist"),
            "fn main() {}"
        );
        assert_eq!(client.requests().len(), 4);
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[tokio::test]
    async fn lists_directory_and_sends_result_to_llm() {
        let workspace = workspace();
        fs::write(workspace.join("answer.txt"), "workspace content")
            .expect("file should be written");
        let client = FakeClient::new(vec![
            tool_call("list_directory", r#"{"path":"."}"#),
            final_response("I listed the directory."),
        ]);
        let list_directory = ListDirectory::new(&workspace).expect("workspace should be valid");
        let agent = Agent::new(&client, vec![Box::new(list_directory)]);

        let answer = agent
            .run("test", "List the workspace")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "I listed the directory.");
        let requests = client.requests();
        assert_eq!(
            requests[1].messages[2].content.as_deref(),
            Some(r#"{"entries":[{"name":"answer.txt","type":"file"}]}"#)
        );
        fs::remove_dir_all(workspace).expect("workspace should be removed");
    }

    #[tokio::test]
    async fn runs_command_and_sends_result_to_llm() {
        let client = FakeClient::new(vec![
            tool_call("run_command", r#"{"command":"cargo --version"}"#),
            final_response("I ran the command."),
        ]);
        let agent = Agent::new(
            &client,
            vec![Box::new(crate::tools::command::RunCommand::new(
                std::env::current_dir().expect("current directory should exist"),
            ))],
        );

        let answer = agent
            .run("test", "Show the Cargo version")
            .await
            .expect("agent should finish");

        assert_eq!(answer, "I ran the command.");
        let requests = client.requests();
        let result: serde_json::Value = serde_json::from_str(
            requests[1].messages[2]
                .content
                .as_deref()
                .expect("tool result should have content"),
        )
        .expect("tool result should be JSON");
        assert_eq!(result["exit_code"], 0);
        assert!(
            result["stdout"]
                .as_str()
                .expect("stdout should be text")
                .contains("cargo")
        );
    }

    #[tokio::test]
    async fn sends_tool_result_before_returning_final_answer() {
        let client = FakeClient::new(vec![
            tool_call("get_current_directory", "{}"),
            final_response("The current directory was returned."),
        ]);
        let agent = Agent::new(
            &client,
            vec![Box::new(GetCurrentDirectory::new(
                std::env::current_dir().expect("current directory should exist"),
            ))],
        );

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
        let agent = Agent::new(
            &client,
            vec![Box::new(GetCurrentDirectory::new(
                std::env::current_dir().expect("current directory should exist"),
            ))],
        );

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
        let agent = Agent::new(
            &client,
            vec![Box::new(GetCurrentDirectory::new(
                std::env::current_dir().expect("current directory should exist"),
            ))],
        );

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
    async fn stops_after_configured_iteration_limit() {
        let client = FakeClient::new(vec![
            tool_call("get_current_directory", "{}"),
            tool_call("get_current_directory", "{}"),
        ]);
        let agent = Agent::new(
            &client,
            vec![Box::new(GetCurrentDirectory::new(
                std::env::current_dir().expect("current directory should exist"),
            ))],
        )
        .with_max_iterations(1);

        let error = agent
            .run("test", "prompt")
            .await
            .expect_err("second LLM iteration should exceed the limit");

        assert!(matches!(
            error,
            AgentError::IterationLimitReached { limit: 1 }
        ));
        assert_eq!(client.requests().len(), 1);
    }
}
