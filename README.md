# Lya

**Lya** is a lightweight, open-source AI agent written in Rust.

The goal of Lya is to provide a simple, extensible and local-first AI agent capable of interacting with tools and external systems while keeping the architecture lightweight and understandable.

Lya is designed to work with local LLMs through [Ollama](https://ollama.com/). It also provides local-only process, private-context, state, and environment-diagnostic foundations for future development orchestration.

## Goals

* 🦀 Built in Rust
* 🧠 Support local LLMs
* 🛠️ Tool and function calling
* 🔌 Extensible agent architecture
* 🏠 Local-first and privacy-friendly
* ⚡ Lightweight and fast
* 📖 Easy to understand and extend

## Architecture

Lya is built around a modular agent architecture.

```text
Lya
├── Agent
│   ├── LLM
│   ├── Tools
│   └── Memory
│
└── Providers
    └── Ollama
```

The architecture is intentionally kept simple during the early development stages so that new components can be added without unnecessary complexity.

## Requirements

* Rust
* Ollama
* A compatible local LLM

## Getting Started

Clone the repository:

```bash
git clone https://github.com/Sharkou/Lya.git
cd Lya
```

Install and run Ollama, then make sure a compatible model is available.

Build Lya:

```bash
cargo build
```

Run it:

```bash
OLLAMA_MODEL=<model> cargo run -- <prompt>
```

## Supervisor Development Command

Lya resolves its private local directory from `LYA_HOME`, falling back to `~/.lya`. This directory is outside the repository and contains the required private `context.md`. The Codex Supervisor reads that context, builds a structured request for the current directory, and returns a validated JSON decision. It does not invoke Claude, modify the project, commit, or push.

Check the local prerequisites without contacting an LLM or network service:

```bash
cargo run -- doctor
```

`doctor` reports the resolved `LYA_HOME`, whether `context.md` is readable, and whether `git`, `codex`, and `claude` are available on `PATH`.

With a Codex CLI installation authenticated through ChatGPT, test the Supervisor from a project directory:

```bash
cargo run -- supervisor "Determine the next development step"
```

The command requires `context.md`, runs `codex exec` with a strict JSON Schema, removes `OPENAI_API_KEY` from the Codex child process, and prints one of `CLAUDE`, `ACCEPT`, `HUMAN`, or `STOP` as JSON.

## Executor Development Command

The Claude Executor runs independently from the Supervisor. It invokes Claude Code in print mode with JSON output, sending the complete task through stdin so long Supervisor prompts do not depend on the Windows command-line limit. It then prints an `ExecutorResult` containing Claude's final response, session reference, exit code, and any available duration, turn, cost, and usage metadata. It does not connect a Codex decision to Claude, commit, push, or start a daemon.

From the project to work on, run:

```bash
cargo run -- executor --max-turns 1 "Summarize this repository without modifying any files"
```

The command accepts `--project <path>`, `--resume <session>`, `--browser`, `--timeout-seconds <seconds>`, and `--max-turns <count>`. `--resume` accepts the session reference returned in the preceding JSON result. By default there is no Lya-imposed timeout or turn limit. Set `LYA_CLAUDE_BIN` to use a Claude executable not available on `PATH`; otherwise Lya invokes `claude`.

Lya starts Claude Code with `--permission-mode auto --permission-prompts none`: Claude's safety classifier evaluates actions, while actions that would need an unanswered approval are denied. Lya never uses `--dangerously-skip-permissions`. It removes `ANTHROPIC_API_KEY` only from the Claude child process, so the CLI uses its normal Claude subscription authentication and cannot silently fall back to API-key billing.

The Claude execution loop, automated Git operations, automatic quota handling, and a daemon are not implemented yet.

> Lya is currently under active development. APIs, architecture and features may change significantly.

## Roadmap

* [x] Initial Rust project
* [ ] Ollama integration
* [ ] Tool calling
* [ ] Agent loop
* [ ] Conversation context
* [ ] Memory system
* [ ] More LLM providers
* [ ] Configuration system
* [ ] Documentation
* [ ] Stable API

## Contributing

Lya is an open-source project and contributions are welcome.

The project is still in an early stage, so architecture and APIs are expected to evolve.

## License

Lya is licensed under the MIT License.

This means you are free to use, copy, modify, merge, publish, distribute, sublicense, and sell copies of the software, subject to the terms of the license.

See the [`LICENSE`](LICENSE) file for the full license text.
