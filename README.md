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
        └── jobs/
            └── <job-id>/
                ├── state.json
                ├── lock.json
                ├── events.jsonl
                └── Supervisor artifacts
```

The components are intentionally separated:

* the **Supervisor** decides what should happen next;
* the **Executor** performs development work;
* Lya independently inspects the repository instead of trusting executor reports;
* the **Publisher** can commit and push only changes that match the reviewed repository state;
* persistent state is stored locally per job, so an interrupted run can be continued by a later process.

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
└── jobs/
    └── <job-id>/
        ├── state.json
        ├── lock.json
        ├── events.jsonl
        └── supervisor-decision*.json
```

`context.md` provides private user/project context to the Supervisor.

Each job owns its own `state.json`, written atomically through a unique temporary file, a
synchronised write and a rename. Two Lya processes working on different jobs therefore never
rewrite each other's state.

### Migration From The Single State File

Earlier versions stored every job in one `LYA_HOME/state.json`. On the next `lya run` or
`lya resume`, Lya moves those jobs into the per-job layout and renames the old file to
`state.json.migrated-<unix-seconds>`. Nothing is deleted, and a per-job file that already exists is
never overwritten.

### Job Locks

While a Lya process drives a job it holds an exclusive operating-system lock on
`LYA_HOME/jobs/<job-id>/lock.json`. A second process that tries to run or resume the same job is
refused instead of corrupting shared state.

The claim is the OS lock itself, never the presence of the file and never a recorded process ID:
Windows uses a `LockFileEx` lock, Linux and macOS use `flock(2)`. On all three the kernel releases
the lock when the process exits, including a crash or a forced kill, so an abandoned lock file can
never make a job permanently unresumable. The file body records the owning job, process ID and
acquisition time for diagnostics only; because nothing reads it to decide ownership, process-ID
reuse cannot grant a claim.

A released lock keeps its file on purpose. Deleting a locked path would let another process lock a
fresh file under the same name and believe it owns the same job.

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

### Interactive Control

When `lya run` is attached to an interactive terminal, Lya accepts line-oriented commands while the job runs. The terminal remains a normal scrolling log; it does not enter a fullscreen TUI.

```text
/help
/status
/diff
/pause
/resume
/stop
/send <instruction>
```

`/status` reports the authoritative live job state, including the job and project, phase, iteration, known Claude session, publication progress, and pending pause/stop requests. `/diff` performs a read-only repository capture and prints tracked paths, untracked paths, and a concise diff stat. It never changes the repository.

`/send <instruction>` queues the complete instruction in order, acknowledges it immediately, persists it in job state, and applies it at the next safe model turn. Lya includes applied instructions in both the relevant Codex review and Claude execution prompts; it never attempts to inject text into a model process already generating. Queued and applied instructions are recorded in `events.jsonl`.

An instruction is a constraint for the rest of the current job. It is not one-turn-only, and it is
never carried into a sequential next job, which starts from its own task alone. To keep prompts
bounded, one job holds at most **16 active instructions** totalling at most **8 KiB**. An
instruction beyond either limit is refused explicitly, with the reason reported in the terminal and
recorded as a `USER_INSTRUCTION_REJECTED` event; it is never dropped silently.

`/pause` records a request immediately, then enters `PAUSED` only at a safe boundary. A running Codex/Claude invocation, repository capture, or Git operation is allowed to finish its current safe operation first. While paused, `/status`, `/diff`, `/send`, `/resume`, and `/stop` remain available. `/resume` continues from that exact boundary without repeating a completed provider invocation.

`/stop` prevents new Supervisor, Executor, and publication actions. If Codex or Claude is active, Lya cancels and reaps its child process through the shared process runner, then records a terminal `STOPPED` state after a best-effort repository capture. Lya does not reset working-tree changes made before the stop request. During publication, a stop is observed before each guarded stage; Lya does not begin a later stage after it has observed the request.

The first `Ctrl+C` follows the same graceful stop path and prints a second-press warning. A second `Ctrl+C` force-terminates the Lya process after the cancellation signal has already been sent to active child processes.

Graceful termination is always armed. Human mode, `--verbose`, `--json`, redirected output and
non-TTY runs all route the first termination signal through the same stop semantics. Only the
line-oriented command reader is restricted: `--json` never starts a stdin reader, so its stdout
remains valid `JobEvent` JSONL for scripts, and redirected non-TTY runs accept no typed commands.

If Lya's input simply ends while a job is paused, nobody asked to stop, so the job stays `PAUSED`
and remains resumable. Only an explicit `/stop` or `Ctrl+C` reaches the terminal `STOPPED` state.

This visibility records the exchange Lya is legitimately allowed to know: the safe review request it sends Codex, Codex's structured decision and explicit reason, the prompt Lya sends Claude, Claude's final response/report, and subsequent repository state. Hidden model chain-of-thought is not available and is never claimed or logged. The same event model is intentionally independent of terminal rendering so a later daemon or web UI can subscribe to it.

Lya currently does not log an actual Codex or Claude model identifier. Both CLIs support model selection, but the structured Codex decision and Claude JSON result contracts used by Lya do not reliably report which model executed a request. Lya will add model metadata only when it is available through a documented structured provider contract.

## Inspecting Jobs

Persisted jobs can be listed without resuming anything:

```bash
cargo run -- jobs
```

```text
JOB                     PROJECT       STATUS                PHASE       ITER  UPDATED  RESUMABLE
job-1789250000-4242-0   PixelCreator  WAITING_OPENAI_QUOTA  SUPERVISOR  3     4m ago   yes
job-1789240000-4242-0   Lya           ACCEPTED              PUBLISHER   1     2h ago   no
```

Jobs are listed most recently updated first, then by job ID.

`lya jobs` is strictly read-only. It reads authoritative per-job state and changes nothing: no job
state is written, no legacy `state.json` is migrated, no job lock is taken, no Supervisor or
Executor is invoked and no Git command is run.

Restrict the listing to the jobs `lya resume` would actually continue:

```bash
cargo run -- jobs --resumable
```

The verdict comes from the same resume logic `lya resume` uses, so the two cannot disagree. A job
whose status looks resumable but whose persisted state is inconsistent is listed as not resumable
together with the exact reason.

Machine-readable output prints one JSON object on stdout; diagnostics stay on stderr:

```bash
cargo run -- jobs --json
```

```json
{
  "jobs": [
    {
      "job_id": "job-1789250000-4242-0",
      "project_name": "PixelCreator",
      "status": "WAITING_OPENAI_QUOTA",
      "phase": "SUPERVISOR",
      "iteration": 3,
      "created_unix_seconds": 1789249000,
      "last_updated_unix_seconds": 1789250000,
      "resumable": true,
      "continuation": "SUPERVISOR_REVIEW",
      "blocked_reason": null
    }
  ],
  "unreadable": [],
  "legacy_state_file": null
}
```

`--json` and `--resumable` compose.

A job whose `state.json` is corrupt or unreadable never disappears from the listing. Healthy jobs
are still listed; the unreadable ones are reported individually with their error, in human and JSON
output alike, and `lya jobs` exits with a failure status. Lya does not modify, repair or archive
them.

## Resuming Jobs

A job that was paused, parked on a provider quota, or interrupted by a process exit can be
continued by a later Lya process:

```bash
cargo run -- resume
```

```bash
cargo run -- resume --job job-1789250000-4242-0
```

Without `--job`, Lya resumes only when exactly one job can be resumed. Otherwise it lists the
candidates and asks for an explicit choice. `lya jobs --resumable` shows the same candidates
without starting anything.

Resumable states are `RUNNING`, `PAUSED`, `PUBLISHING`, `WAITING_CLAUDE_QUOTA` and
`WAITING_OPENAI_QUOTA`. `FAILED`, `STOPPED`, `PUBLISHED`, `ACCEPTED` and `WAITING_HUMAN` are
terminal for automatic recovery and are refused.

Resume reads authoritative job state. The event log stays observability-only and is never parsed to
decide what may happen next.

Before any provider call or Git write, Lya:

1. loads the persisted job;
2. validates it structurally and semantically;
3. confirms the project path is still the expected Git repository;
4. captures the current repository state;
5. compares reality against the persisted snapshot;
6. determines the exact continuation point;
7. only then contacts a provider or touches Git.

Each job persists the run configuration it needs, so a restarted shell does not have to export the
original environment variables again:

```text
project identity and path
task
max iterations / max jobs / sequential position
browser flag
publication enabled
Git identity, remote and branch
Claude session ID
iteration, phase, status and pending operation
accepted repository snapshot
publication stage and recorded commit
active user instructions
```

No secret is ever persisted. Push authentication stays with the machine's own Git configuration.

Lya records exactly one pending operation per job: the single external action it still owes. A
resumed job continues at that operation and never replays a provider call that is already durably
recorded. A pending Claude correction reuses the persisted Claude session.

If the repository changed while Lya was not running, Lya does not guess: the job moves to
`WAITING_HUMAN` with a precise reason.

Resume emits `RESUME_STARTED`, `RESUME_VALIDATED`, `RESUME_REJECTED` and `QUOTA_RETRY_STARTED`
events into the same `events.jsonl` as the original process. Interactive control and `--json`
output work exactly as they do for a fresh run.

### Quota Waiting

A provider quota is a parked, resumable state rather than a dead end. Lya records which provider
was exhausted, the operation that still needs to run, how the condition was classified and the
reported reason.

Classification prefers documented structured provider information. Where a CLI documents no
machine-readable quota signal, Lya falls back to a message heuristic and records that explicitly as
`PROVIDER_MESSAGE_HEURISTIC`, so a quota decision never looks more precise than it really is.

`lya resume` retries only the operation that had not completed. The iteration counter is not
advanced again, and a model action that already finished is never repeated.

Lya never falls back to a paid API to work around a subscription quota.

### Recovering An Interrupted Publication

Publication is recovered only when the state can be proven safe.

If no commit was recorded, Lya proves `HEAD` never moved, then either restarts the guarded sequence
or, when the index already holds exactly the accepted change, continues at the commit step.

If a commit was recorded, Lya verifies that:

```text
the commit is recorded in authoritative job state
HEAD is exactly that commit
its only parent is the accepted snapshot HEAD
the commit carries the accepted commit title
the commit contains exactly the accepted paths
the working tree is clean
the configured branch and remote still match
```

Only then does publication continue at the push, without creating a second commit.

Anything ambiguous — including a commit that exists without authoritative state — moves the job to
`WAITING_HUMAN` and leaves Git untouched. Lya never resets or rewrites history to recover.

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

As soon as a commit exists it is recorded in authoritative job state with its SHA, publication
stage, remote, branch and push state, before any further step. A stop between the commit and the
push therefore leaves enough state for a later process to know exactly what exists and to continue
at the push.

The commit and its record are still two separate steps, so a crash in between can leave a commit
that no persisted state describes. That case is detected rather than assumed away: the next resume
finds that `HEAD` moved without an authoritative commit record, stops, and parks the job in
`WAITING_HUMAN` for a person to resolve.

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

A commit is recorded in authoritative job state as soon as it exists, but the commit and its record
are two separate steps: a crash between them can leave a commit that no persisted state describes.
Lya does not paper over that window. On the next resume it sees that `HEAD` moved without an
authoritative commit record, refuses to continue automatically, and parks the job in
`WAITING_HUMAN`. It never creates a second commit, never pushes, and never rewrites history to
recover. An interrupted publication is only continued when every invariant can be proven against
the live repository.

Lya also avoids giving provider processes unnecessary billing credentials by removing environment-provided API keys from their child-process environments.

These protections reduce accidental autonomous changes, but Lya is experimental software. Run autonomous workflows only in repositories where you understand and accept the risks.

## Current Limitations

Lya currently runs jobs sequentially.

Cancelling a provider kills and reaps the Codex or Claude process Lya started, but not the
processes that CLI started in turn. A provider's child processes can therefore outlive a stop.
Cancelling the whole provider process tree needs a Windows Job Object and a Unix process group and
is deliberately kept as a separate, immediately following change.

A second `Ctrl+C` force-terminates Lya immediately. That is intentional and safe for job locks,
which the operating system releases on process exit, but it skips Lya's own cleanup.

The following are not implemented yet:

* cancellation of a provider's whole process tree;
* persistent daemon/service mode;
* background scheduling;
* parallel jobs;
* multi-project scheduling;
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
* [x] Interactive job control
* [x] Persisted job/run resume
* [x] Quota-aware pause and resume
* [x] Read-only job listing
* [ ] Provider process-tree cancellation
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
