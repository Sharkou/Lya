use std::{env, path::Path, process::ExitCode};

use lya::{
    agent::Agent,
    llm::ollama::OllamaClient,
    orchestrator::{
        doctor::DoctorReport,
        home::LyaHome,
        supervisor::{
            CodexCliSupervisor, Project, Supervisor, SupervisorRequest,
            load_required_private_context,
        },
    },
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
    if arguments
        .first()
        .is_some_and(|argument| argument == "supervisor")
    {
        return run_supervisor(&arguments[1..]).await;
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

async fn run_supervisor(arguments: &[String]) -> ExitCode {
    let task = arguments.join(" ");
    if task.trim().is_empty() {
        eprintln!("Usage: lya supervisor <task>");
        return ExitCode::FAILURE;
    }

    let home = match LyaHome::resolve() {
        Ok(home) => home,
        Err(error) => {
            eprintln!("Could not resolve Lya home: {error}");
            return ExitCode::FAILURE;
        }
    };
    let private_context = match load_required_private_context(&home) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("Could not prepare supervisor request: {error}");
            return ExitCode::FAILURE;
        }
    };
    let project_path = match env::current_dir().and_then(|path| path.canonicalize()) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("Could not determine current project directory: {error}");
            return ExitCode::FAILURE;
        }
    };
    let request = SupervisorRequest {
        private_context,
        project: Project {
            name: project_name(&project_path),
            path: project_path,
        },
        task,
        phase: None,
        iteration: 0,
        executor_report: None,
        repository_state: None,
    };
    let supervisor = CodexCliSupervisor::new(home.path());

    match supervisor.decide(request).await {
        Ok(decision) => match serde_json::to_string_pretty(&decision) {
            Ok(output) => {
                println!("{output}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("Could not render supervisor decision: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("Supervisor failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn project_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("current-project")
        .to_owned()
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
