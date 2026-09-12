use std::{env, path::Path, process::ExitCode};

use lya::{
    agent::Agent,
    llm::ollama::OllamaClient,
    orchestrator::{
        doctor::DoctorReport,
        executor::{ClaudeCliExecutor, Executor, ExecutorRequest, ExecutorSession},
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
    if arguments
        .first()
        .is_some_and(|argument| argument == "executor")
    {
        return run_executor(&arguments[1..]).await;
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

async fn run_executor(arguments: &[String]) -> ExitCode {
    let (request, max_turns) = match parse_executor_arguments(arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!(
                "{error}\nUsage: lya executor [--project <path>] [--resume <session>] [--browser] [--timeout-seconds <seconds>] [--max-turns <count>] <prompt>"
            );
            return ExitCode::FAILURE;
        }
    };
    let mut executor = ClaudeCliExecutor::new();
    if let Some(max_turns) = max_turns {
        executor = executor.with_max_turns(max_turns);
    }

    match executor.execute(request).await {
        Ok(result) => match serde_json::to_string_pretty(&result) {
            Ok(output) => {
                println!("{output}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("Could not render executor result: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("Executor failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse_executor_arguments(
    arguments: &[String],
) -> Result<(ExecutorRequest, Option<u32>), String> {
    let mut project_path = env::current_dir().map_err(|error| error.to_string())?;
    let mut session = ExecutorSession::New;
    let mut browser = false;
    let mut timeout = None;
    let mut max_turns = None;
    let mut prompt = Vec::new();
    let mut position = 0;

    while position < arguments.len() {
        match arguments[position].as_str() {
            "--project" => {
                position += 1;
                project_path = arguments
                    .get(position)
                    .map(std::path::PathBuf::from)
                    .ok_or_else(|| "--project requires a path".to_owned())?;
            }
            "--resume" => {
                position += 1;
                session = ExecutorSession::Resume(
                    arguments
                        .get(position)
                        .cloned()
                        .ok_or_else(|| "--resume requires a session ID".to_owned())?,
                );
            }
            "--browser" => browser = true,
            "--timeout-seconds" => {
                position += 1;
                let seconds = arguments
                    .get(position)
                    .ok_or_else(|| "--timeout-seconds requires a number".to_owned())?
                    .parse::<u64>()
                    .map_err(|_| "--timeout-seconds must be an unsigned integer".to_owned())?;
                timeout = Some(std::time::Duration::from_secs(seconds));
            }
            "--max-turns" => {
                position += 1;
                max_turns = Some(
                    arguments
                        .get(position)
                        .ok_or_else(|| "--max-turns requires a number".to_owned())?
                        .parse::<u32>()
                        .map_err(|_| "--max-turns must be an unsigned integer".to_owned())?,
                );
            }
            argument if argument.starts_with("--") => {
                return Err(format!("unknown executor option: {argument}"));
            }
            argument => prompt.push(argument.to_owned()),
        }
        position += 1;
    }

    let project_path = project_path
        .canonicalize()
        .map_err(|error| format!("could not resolve project path: {error}"))?;
    Ok((
        ExecutorRequest {
            project_name: project_name(&project_path),
            project_path,
            prompt: prompt.join(" "),
            session,
            browser,
            timeout,
        },
        max_turns,
    ))
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

#[cfg(test)]
mod tests {
    use super::{ExecutorSession, parse_executor_arguments};

    #[test]
    fn parses_executor_options_and_preserves_prompt_words() {
        let arguments = vec![
            "--resume".to_owned(),
            "session-name".to_owned(),
            "--browser".to_owned(),
            "--timeout-seconds".to_owned(),
            "900".to_owned(),
            "--max-turns".to_owned(),
            "4".to_owned(),
            "inspect".to_owned(),
            "files with spaces".to_owned(),
        ];

        let (request, max_turns) = parse_executor_arguments(&arguments)
            .expect("arguments should parse from the current project");

        assert_eq!(request.prompt, "inspect files with spaces");
        assert_eq!(
            request.session,
            ExecutorSession::Resume("session-name".to_owned())
        );
        assert!(request.browser);
        assert_eq!(request.timeout.map(|timeout| timeout.as_secs()), Some(900));
        assert_eq!(max_turns, Some(4));
    }
}
