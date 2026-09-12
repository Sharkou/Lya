# Lya

**Lya** is a lightweight, open-source, local-first AI agent and development orchestrator written in Rust.

Lya started as a simple agent runtime for local language models and has evolved to also support autonomous software-development workflows. It can coordinate specialized AI tools, inspect real repository state, persist job progress, and safely publish reviewed changes through Git.

The project intentionally favors small, understandable components over a large agent framework.

> Lya is under active development. APIs, commands, and internal architecture may change significantly.

## Goals

* 🦀 **Rust-first** — small, fast, strongly typed runtime
* 🏠 **Local-first** — orchestration, state, configuration, and repository access stay on your machine
* 🧠 **Multiple AI backends** — local models and external AI tools can be integrated behind focused adapters
* 🛠️ **Tool and function calling** — agents can interact with files, commands, and external systems
* 🤖 **Autonomous development** — AI supervisors and executors can work together in controlled loops
* 🔒 **Guarded automation** — repository changes are reviewed and revalidated before publication
* 🔌 **Extensible architecture** — providers and tools remain replaceable
* 📖 **Understandable by design** — avoid unnecessary framework complexity

## Architecture

Lya currently has two complementary roles:

```text
Lya
├── Agent Runtime
│   ├── LLM
│   │   └── Ollama
│   └── Tools
│
└── Development Orchestration
    ├── Supervisor
    │   └── Codex CLI
    ├── Executor
    │   └── Claude Code
    ├── Repository State
    │   └── Git
    ├── Publisher
    │   └── Git
    └── Local State
        ├── context.md
        ├── state.json
        └── jobs/
            └── <job-id>/
                ├── events.jsonl
                └── Supervisor artifacts
```

The components are intentionally separated:

* the **Supervisor** decides what should happen next;
* the **Executor** performs development work;
* Lya independently inspects the repository instead of trusting executor reports;
* the **Publisher** can commit and push only changes that match the reviewed repository state;
* persistent state is stored locally so long-running orchestration can later support pause/resume workflows.

The current development workflow uses Codex as the Supervisor and Claude Code as the Executor, but the architecture is designed so implementations can be replaced without rewriting the orchestration core.

## Requirements

### Base

* Rust
* Git

### Local agent mode

* [Ollama](https://ollama.com/)
* a compatible local model

### Development orchestration

* Codex CLI
* Claude Code

The relevant CLIs must be authenticated independently according to their providers' normal setup.

Lya does not require API keys for its standard Codex/Claude CLI workflow.

## Getting Started

Clone and build Lya:

```bash
git clone https://github.com/Sharkou/Lya.git
cd Lya
cargo build
```

Run the test suite:

```bash
cargo test
```

### Local agent

Start Ollama, make sure a compatible model is installed, then run:

```bash
OLLAMA_MODEL=<model> cargo run -- <prompt>
```

## Local State

Lya stores private runtime data outside the repository.

The location is resolved from:

```text
LYA_HOME
```

and defaults to:

```text
~/.lya
```

The directory currently contains data such as:

```text
~/.lya/
├── context.md
├── state.json
└── jobs/
```

`context.md` provides private user/project context to the Supervisor.

**Important:** "private" means that this file is kept outside the project repository. Its relevant content is sent to the configured Supervisor when a request is made. Do not store credentials, API keys, passwords, or other secrets in it.

### Job Event History

Every autonomous job appends structured JSON Lines to:

```text
LYA_HOME/jobs/<job-id>/events.jsonl
```

Each line is an independently useful `JobEvent` with a timestamp, job and project identity, optional iteration, and event-specific data. Events cover job lifecycle, Supervisor decisions, Claude execution reports, repository summaries, waiting/failure states, and guarded publication stages. The file is appended and synced after each event; it is never rewritten as a whole.

Event persistence is required for an autonomous job. If Lya cannot write or sync an event, it stops the job before the next external Supervisor, Executor, or Git action and reports the error. Existing event logs are append-only and are not parsed to decide whether Git may write, so malformed older logs cannot weaken publication verification.

Event logs are local, but they may contain prompts, model final responses, file paths, repository metadata, commit titles, and other project details. Treat them as potentially sensitive. Lya never logs child-process environment variables or credentials. The private `context.md` body is intentionally omitted from the observable Supervisor-request event.

## Diagnostics

Check the local environment without contacting an LLM:

```bash
cargo run -- doctor
```

`doctor` reports:

* the resolved `LYA_HOME`;
* whether `context.md` is readable;
* whether Git is available;
* whether Codex is available;
* whether Claude Code is available.

Custom executable locations can be supplied with:

```text
LYA_CODEX_BIN
LYA_CLAUDE_BIN
```

Otherwise Lya resolves `codex` and `claude` through `PATH`.

## Supervisor

The Supervisor can be tested independently:

```bash
cargo run -- supervisor "Determine the next development step"
```

The Codex Supervisor:

1. loads the local private context;
2. builds a structured request describing the current task and repository state;
3. invokes `codex exec`;
4. requires a structured decision;
5. validates that decision locally.

A decision is one of:

```text
CLAUDE
ACCEPT
HUMAN
STOP
```

Their meanings are:

* `CLAUDE` — send additional work to the Executor;
* `ACCEPT` — the current work is accepted;
* `HUMAN` — human input is required;
* `STOP` — stop the current job intentionally.

Lya removes `OPENAI_API_KEY` from the Codex child-process environment so an environment-provided API key is not used accidentally.

## Executor

The Claude Executor can also be tested independently:

```bash
cargo run -- executor --max-turns 1 "Summarize this repository without modifying any files"
```

Useful options include:

```text
--project <path>
--resume <session>
--browser
--timeout-seconds <seconds>
--max-turns <count>
```

The full task is sent to Claude through stdin instead of being embedded in the command line, allowing large prompts to work reliably across platforms.

Claude sessions can be resumed using the session reference returned by the previous execution.

When browser support is enabled, Lya passes the corresponding browser capability to Claude Code. Browser automation itself remains Claude Code's responsibility.

Lya starts Claude Code using its non-interactive permission system and never enables `--dangerously-skip-permissions`.

`ANTHROPIC_API_KEY` is removed from the Claude child-process environment so an environment-provided API key is not used accidentally.

## Autonomous Development

The `run` command connects the Supervisor and Executor into a sequential autonomous-development loop.

For example:

```bash
cargo run -- run \
  --project /path/to/project \
  --browser \
  --max-iterations 5 \
  "Fix a small regression and verify the result."
```

A project must:

* exist;
* be a Git repository;
* have a clean working tree when the job starts.

Lya deliberately refuses to start autonomous work on an already dirty repository so pre-existing changes cannot be confused with agent-generated work.

The loop is conceptually:

```text
Task
 ↓
Supervisor
 ↓
CLAUDE
 ↓
Executor
 ↓
Repository inspection
 ↓
Supervisor
 ↓
CLAUDE / ACCEPT / HUMAN / STOP
 ↓
...
```

One iteration consists of one Supervisor review and the optional Executor invocation requested by that review.

The default maximum is 10 iterations.

Claude sessions are resumed across correction cycles so the Executor retains the context of its previous work.

### Live Output

`lya run` renders the job event stream live in a compact, human-readable form by default. It shows Supervisor decisions, the explicit prompts Lya sends to Claude, Claude's final report, concise repository summaries, waiting/failure states, and publication progress. ANSI color is used only when stdout is an interactive terminal; redirected output remains readable text.

```bash
cargo run -- run --project /path/to/project "Fix a small regression and verify the result."
```

Use `--verbose` to include the full safe Supervisor review request, explicit structured decision fields, complete Claude final response, detailed repository metadata, and publication details:

```bash
cargo run -- run --verbose --project /path/to/project "Fix a small regression and verify the result."
```

Use `--json` for machine-readable JSON Lines on stdout. In this mode stdout contains only `JobEvent` JSON objects, one per line; diagnostics are written to stderr. `--verbose` and `--json` cannot be combined.

```bash
cargo run -- run --json --project /path/to/project "Fix a small regression and verify the result."
```

This visibility records the exchange Lya is legitimately allowed to know: the safe review request it sends Codex, Codex's structured decision and explicit reason, the prompt Lya sends Claude, Claude's final response/report, and subsequent repository state. Hidden model chain-of-thought is not available and is never claimed or logged. The same event model is intentionally independent of terminal rendering so a later daemon or web UI can subscribe to it.

## Repository Review

Lya independently collects repository state before reviews.

This includes information such as:

```text
HEAD
git status --short
git diff --stat
changed files
tracked diff
untracked files
```

Untracked files are included explicitly rather than being represented only by `git status`.

For review safety, Lya records:

* paths;
* file sizes;
* UTF-8 content when appropriate;
* Git blobs;
* binary/non-UTF-8 markers;
* explicit truncation markers.

Large repository data is bounded before being sent to the Supervisor. Truncation is always reported explicitly rather than hidden.

Binary or insufficiently reviewed states cannot be published automatically.

## Guarded Git Publication

Git publication is opt-in.

Without:

```text
--publish
```

an `ACCEPT` decision records the proposed commit title but performs no Git write.

With publication enabled:

```bash
cargo run -- run \
  --publish \
  --project /path/to/project \
  "Implement and verify a small change."
```

Lya performs a guarded publication sequence:

```text
Supervisor ACCEPT
 ↓
store reviewed snapshot
 ↓
recollect repository state
 ↓
verify exact match
 ↓
git add --all
 ↓
verify staged state
 ↓
commit
 ↓
push
 ↓
verify clean working tree
```

If the repository changes between review and publication, Lya refuses to publish it.

The staged content is also checked against the reviewed state before the commit is created.

Lya does **not** automatically:

```text
checkout
switch
pull
merge
rebase
reset
force-push
```

Normal Git hooks are respected.

A rejected push stops publication instead of attempting to rewrite history or resolve the conflict automatically.

## Git Configuration

Publication requires an explicit Git identity and branch.

Configure:

```text
LYA_GIT_NAME
LYA_GIT_EMAIL
LYA_GIT_BRANCH
```

The remote can optionally be configured with:

```text
LYA_GIT_REMOTE
```

and defaults to:

```text
origin
```

For example:

```powershell
$env:LYA_GIT_NAME = "Automation Bot"
$env:LYA_GIT_EMAIL = "bot@example.com"
$env:LYA_GIT_BRANCH = "main"
$env:LYA_GIT_REMOTE = "origin"

cargo run -- run `
  --publish `
  --project C:\Projects\Example `
  "Implement and verify one small improvement."
```

The configured branch must already be the current local branch.

Lya applies the configured identity only to the commit it creates. It does not need or persist GitHub credentials.

Push authentication is delegated entirely to the machine's existing Git configuration, such as SSH or a credential manager.

## Sequential Jobs

An accepted job may contain a `next_prompt`.

When publication is enabled and succeeds, Lya can use that prompt to start another sequential job.

```text
Job 1
 ↓
PUBLISHED
 ↓
next_prompt
 ↓
Job 2
 ↓
PUBLISHED
 ↓
...
```

The number of sequential jobs is bounded by:

```text
--max-jobs <count>
```

with a default of 10.

A new job starts only after:

* the previous job was accepted;
* publication succeeded;
* the push succeeded;
* the working tree is clean.

Without `--publish`, `next_prompt` is retained and displayed but does not automatically start another job.

## Safety Model

Lya deliberately separates reasoning from repository publication.

The Supervisor does not commit.

The Executor does not own publication.

The Publisher does not use an LLM.

Before publication, Lya checks that what Git is about to commit is the same repository state the Supervisor reviewed.

Lya also avoids giving provider processes unnecessary billing credentials by removing environment-provided API keys from their child-process environments.

These protections reduce accidental autonomous changes, but Lya is experimental software. Run autonomous workflows only in repositories where you understand and accept the risks.

## Current Limitations

Lya currently runs jobs sequentially.

The following are not implemented yet:

* automatic resume after process restart;
* quota-aware pause and resume;
* persistent daemon/service mode;
* background scheduling;
* parallel jobs;
* remote administration UI;
* automatic conflict resolution;
* GitHub API integration.

## Roadmap

* [x] Rust agent core
* [x] Ollama provider
* [x] Tool and function calling
* [x] Agent execution loop
* [x] Structured local process runtime
* [x] Private local context
* [x] Persistent job state
* [x] Codex Supervisor
* [x] Claude Code Executor
* [x] Resumable Claude sessions
* [x] Autonomous Supervisor ↔ Executor loop
* [x] Independent Git repository review
* [x] Guarded Git commit and push
* [x] Sequential multi-job runs
* [x] Structured job events and live CLI output
* [ ] Persisted job/run resume
* [ ] Quota-aware pause and resume
* [ ] Daemon/service mode
* [ ] Remote administration interface
* [ ] Scheduling and parallel execution
* [ ] Additional Supervisor and Executor providers
* [ ] Stable public API

## Contributing

Lya is open source and contributions are welcome.

The project is still evolving quickly, so architecture and public APIs may change while the autonomous runtime is being stabilized.

When contributing, prefer small, focused changes that preserve Lya's lightweight and understandable architecture.

## License

Lya is licensed under the MIT License.

See [`LICENSE`](LICENSE) for details.
