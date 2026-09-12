use std::{env, process::ExitCode};

use lya::{
    agent::Agent,
    llm::ollama::OllamaClient,
    orchestrator::{doctor::DoctorReport, home::LyaHome},
    runtime::Runtime,
};

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "doctor")
    {
        if arguments.len() != 1 {
            eprintln!("Usage: lya doctor");
            return ExitCode::FAILURE;
        }
        return run_doctor();
    }

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
    let prompt = arguments.join(" ");

    if prompt.is_empty() {
        eprintln!("Usage: cargo run -- <prompt>");
        return ExitCode::FAILURE;
    }

    let runtime = match Runtime::from_environment() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Could not configure runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let tools = runtime.tools();
    let agent = Agent::new(&client, tools)
        .with_max_iterations(Agent::<OllamaClient>::DEFAULT_MAX_ITERATIONS);

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

fn run_doctor() -> ExitCode {
    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!(
                "Lya doctor\n\nLYA_HOME       ERROR  {error}\n\nNot ready for orchestration."
            );
            return ExitCode::FAILURE;
        }
    };
    let report = DoctorReport::inspect(&home);
    println!("{}", report.render());

    if report.is_ready() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
