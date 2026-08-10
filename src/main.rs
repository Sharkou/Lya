mod llm;

use std::{env, process::ExitCode};

use llm::{ChatMessage, ChatRequest, LlmClient, ollama::OllamaClient};

#[tokio::main]
async fn main() -> ExitCode {
    let model = match env::var("OLLAMA_MODEL") {
        Ok(model) if !model.trim().is_empty() => model,
        _ => {
            eprintln!("OLLAMA_MODEL must name the model to query.");
            return ExitCode::FAILURE;
        }
    };

    let base_url =
        env::var("OLLAMA_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_owned());
    let client = OllamaClient::new(base_url);
    let request = ChatRequest::new(
        model,
        vec![ChatMessage::user(
            "Reply with a short confirmation that the Ollama connection works.",
        )],
    );

    match client.chat(request).await {
        Ok(response) => {
            println!("{}", response.content);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Ollama request failed: {error}");
            ExitCode::FAILURE
        }
    }
}
