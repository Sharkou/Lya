# Lya development instructions

Lya is a general-purpose autonomous AI agent written in Rust.

## Architecture

- Keep the Agent Core small, explicit and extensible.
- Separate LLM communication, agent orchestration, tool registration and tool execution.
- The Agent must not depend directly on a specific LLM provider.
- Ollama is an LLM backend, not part of the Agent Core.
- Tools are executed by Lya, never directly by the LLM.
- Do not introduce abstractions before they are needed.
- Do not implement future features prematurely.

## Current version

Lya v0.1 is intentionally minimal.

Current goal:

User
→ Agent
→ LLM
→ tool_call
→ Tool execution
→ tool_result
→ LLM
→ final answer

Current tool:
- get_current_directory

Planned later:
- read_file
- write_file
- run_command
- git
- browser
- vision
- persistent memory

Do not implement future features unless explicitly requested.

## Development

- Inspect existing code before modifying it.
- Make the smallest appropriate change.
- Run cargo fmt, cargo check, cargo test and cargo clippy when appropriate.
- Never hide warnings or errors.
- Never claim something works without testing it.
- Preserve working code.