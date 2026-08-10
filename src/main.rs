mod agent;
mod llm;
mod tools;

use std::{env, process::ExitCode};

use agent::Agent;
use llm::ollama::OllamaClient;
use tools::command::RunCommand;
use tools::filesystem::{GetCurrentDirectory, ReadFile, WriteFile};

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
    let prompt = env::args().skip(1).collect::<Vec<_>>().join(" ");

    if prompt.is_empty() {
        eprintln!("Usage: cargo run -- <prompt>");
        return ExitCode::FAILURE;
    }

    let workspace = match env::var_os("LYA_WORKSPACE") {
        Some(workspace) => workspace.into(),
        None => match env::current_dir() {
            Ok(workspace) => workspace,
            Err(error) => {
                eprintln!("Could not determine workspace: {error}");
                return ExitCode::FAILURE;
            }
        },
    };
    let read_file = match ReadFile::new(&workspace) {
        Ok(tool) => tool,
        Err(error) => {
            eprintln!("Could not configure read_file: {error}");
            return ExitCode::FAILURE;
        }
    };
    let write_file = match WriteFile::new(workspace) {
        Ok(tool) => tool,
        Err(error) => {
            eprintln!("Could not configure write_file: {error}");
            return ExitCode::FAILURE;
        }
    };
    let agent = Agent::new(
        &client,
        vec![
            Box::new(GetCurrentDirectory),
            Box::new(read_file),
            Box::new(write_file),
            Box::new(RunCommand::new()),
        ],
    )
    .with_max_tool_calls(Agent::<OllamaClient>::DEFAULT_MAX_TOOL_CALLS);

    match agent.run(model, prompt).await {
        Ok(answer) => {
            println!("{answer}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Agent request failed: {error}");
            ExitCode::FAILURE
        }
    }
}
