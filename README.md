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
    ├── Daemon
    │   ├── Local control endpoint (named pipe / Unix socket)
    │   └── Live job event streams
    ├── Scheduler
    │   └── Repository claims and concurrency slots
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
        ├── daemon.lock
        ├── daemon.json
        ├── daemon/
        │   └── events.jsonl
        ├── repositories/
        │   └── <repository-fingerprint>.lock
        └── jobs/
            └── <job-id>/
                ├── state.json
                ├── lock.json
                ├── events.jsonl
                └── Supervisor artifacts
```

The components are intentionally separated:

* the **Daemon** owns the process-level concerns — the claim on one `LYA_HOME`, the local control
  endpoint, connected clients and graceful shutdown — so autonomous work no longer needs a terminal;
* the **Scheduler** decides which jobs may start, how many run at once, and which repository each one owns;
* the **Supervisor** decides what should happen next;
* the **Executor** performs development work;
* Lya independently inspects the repository instead of trusting executor reports;
* the **Publisher** can commit and push only changes that match the reviewed repository state;
* persistent state is stored locally per job, so an interrupted run can be continued by a later process.

The daemon and the scheduler are both boundaries above the orchestrator, not replacements for it.
The orchestrator still owns exactly one autonomous job and its sequential chain:

```text
lya submit / attach / control        (clients)
        |
 local IPC, same machine only
        |
        v
     Daemon            claim on LYA_HOME, endpoint, clients, shutdown
        |
        v
    Scheduler          repository claims, concurrency slots, queue
   /    |    \
  v     v     v
Job A  Job B  Job C
  |      |      |
  +------|------+
         |
  AutonomousOrchestrator
```

Supervisor and Executor implementations know nothing about scheduling, and nothing in the daemon
knows how a job works. Each layer adds one decision and delegates the rest.

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

### Background daemon

Autonomous work can run without your terminal staying open:

```bash
lya daemon start
lya submit --project /path/to/project "Fix a small regression and verify the result."
lya daemon status
lya attach <job-id>
```

See [Daemon Mode](#daemon-mode).

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
├── daemon.lock
├── daemon.json
├── daemon.sock            (Unix only; the Windows endpoint is a named pipe)
├── daemon/
│   ├── events.jsonl
│   └── daemon.log
├── repositories/
│   └── <repository-fingerprint>.lock
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

### Repository Claims

A job lock answers "is anyone else driving *this job*". It cannot answer "is anyone else driving
*this repository*", which is what concurrent scheduling has to answer: two different jobs pointed at
one working tree would interleave Git writes and destroy every snapshot guarantee.

While Lya drives work in a repository it therefore also holds an exclusive claim on:

```text
LYA_HOME/repositories/<repository-fingerprint>.lock
```

Repository claims are an additional layer, never a replacement for job locks. A job holds both, and
claims are taken repository-first, job-second everywhere, so the two layers cannot deadlock.

Identity comes from the repository's real location, not from a project display name:

* the project path is canonicalized, so symlinks, `.`/`..` segments and Windows letter case resolve
  to one real path;
* the nearest ancestor holding a `.git` entry becomes the repository root, so a job started from a
  subdirectory claims the same repository as a job started from its top level.

The claim file is named after a fingerprint of that root because a path is not a portable file name.

The claim uses exactly the same operating-system locking philosophy as a job lock: the kernel's own
advisory lock on an open handle, never the presence of the file and never a recorded process ID.
The kernel releases it when the process exits, including a crash or a kill, so an abandoned claim
recovers by itself. The file body is diagnostics only, and a released claim keeps its file for the
same reason a job lock does.

`lya run`, `lya resume`, `lya scheduler` and the daemon all take the claim, so a manual run and a
daemon-driven job can never drive one working tree at the same time — not even across two Lya
processes.

**Important:** "private" means that this file is kept outside the project repository. Its relevant content is sent to the configured Supervisor when a request is made. Do not store credentials, API keys, passwords, or other secrets in it.

### The Daemon Claim

A daemon holds an exclusive claim on:

```text
LYA_HOME/daemon.lock
```

for as long as it runs, using exactly the locking philosophy job locks and repository claims use: the
operating system's own advisory lock on an open handle. Starting a second daemon against the same
`LYA_HOME` is refused before it can bind an endpoint or touch a job, and a daemon that dies for any
reason — crash, forced kill, machine restart — releases the claim automatically, so the next one
starts without cleanup.

Diagnostics live in a separate file, `LYA_HOME/daemon.json`:

```json
{
  "process_id": 4812,
  "started_unix_seconds": 1763040000,
  "protocol_version": 1,
  "endpoint": "\\\\.\\pipe\\lya-daemon-<home-fingerprint>"
}
```

They are separate on purpose. Nothing reads them to decide whether a daemon may start, so a recorded
process ID that has been reused grants nothing, and metadata left behind by a crash cannot block
startup. They are also unreadable through a held lock on Windows, which is precisely when a refusal
needs to name the holder — hence two files rather than one.

Ownership authority is the claim and the endpoint. A process ID never is.

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

`lya run` stays attached to the terminal that started it and ends with it. To hand the same work to a
background daemon instead, use [`lya submit`](#submitting-work).

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

`/stop` prevents new Supervisor, Executor, and publication actions. If Codex or Claude is active, Lya cancels its whole process tree and reaps its own child through the shared process runner, then records a terminal `STOPPED` state after a best-effort repository capture. Lya does not reset working-tree changes made before the stop request. During publication, a stop is observed before each guarded stage; Lya does not begin a later stage after it has observed the request.

### Provider Process-Tree Cancellation

Codex and Claude start helper processes of their own, so cancelling only the process Lya spawned
would leave those helpers running. Every provider process is therefore claimed by the operating
system when it starts, and a cancellation or a timeout terminates the claim as a unit:

```text
Windows   Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
Unix      dedicated process group, signalled with killpg
```

The direct child is still killed and reaped afterwards, so a cancelled provider never becomes a
zombie, and captured output finishes instead of waiting on a descendant that inherited the pipe. A
timeout and a user cancellation stay distinct outcomes; both clean up the same way.

On Windows the claim also covers Lya's own exit: because the job is closed when Lya's handle goes
away, a second `Ctrl+C` cannot leave a provider tree behind. Anything a provider manages to start
in the microseconds between the spawn and the assignment is outside the job; Windows offers no way
to assign a job to a process that is already running.

On Unix the provider no longer shares Lya's foreground process group, so a terminal `Ctrl+C`
reaches Lya alone and the first interrupt stays a graceful stop that Lya controls.

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

Jobs are listed most recently updated first, then by job ID. Work the scheduler accepted but never
started appears as `QUEUED` and is listed as not resumable, because only the scheduler starts it.

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

Resuming takes the repository claim as well as the job lock, and `QUEUED` work is never resumable,
so a resume can never bypass scheduler safety.

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

Persisted state that no continuation can be proven from — a quota wait naming a different operation
than the one actually owed, a pending operation without the state it needs — is treated the same
way. The job moves to `WAITING_HUMAN` with the exact reason before the resume returns, so it stops
being offered to every later `lya resume` as a job that can still be continued. A job that is
already terminal keeps its own status and is simply refused.

An interrupted publication is one state Lya can always continue: status, phase, pending operation
and the reviewed snapshot enter persisted state in a single write, so a crash between the `ACCEPT`
decision and the first Git command leaves either the reviewed iteration or a resumable
`PUBLISHING` job, never something in between.

A completed Claude run is made durable before Lya reaches its next interruptible boundary. Pausing,
closing Lya's input, or losing the process the moment Claude returns keeps the session reference
and the report, and the resumed job continues with a new review instead of re-running Claude.

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

A quota is recognised once, at the provider boundary, and only in text the provider itself produced
as a diagnostic: the CLI's standard error, and the structured envelope's own fields on a run the
CLI flagged as an error. Claude's answer to the task is never classified, and no decision is ever
re-derived from a rendered error message, which also carries the task, the prompt and that answer.
A job about rate limiting whose run fails for an unrelated reason therefore stays a normal failure
instead of parking as an exhausted quota.

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

Every job records its own position in the chain in its first authoritative write, before it does
any work, so a job that is resumed after a crash still counts against the original `--max-jobs`
budget instead of restarting the count.

A new job starts only after:

* the previous job was accepted;
* publication succeeded;
* the push succeeded;
* the working tree is clean.

Without `--publish`, `next_prompt` is retained and displayed but does not automatically start another job.

## Multi-Project Scheduling

`lya scheduler` drives several autonomous jobs at once, under two rules:

* different repositories may run concurrently;
* the same repository is never driven concurrently.

Work is described by a small job file, one JSON object per line:

```jsonl
# comments and blank lines are ignored
{"project": "/path/to/service", "task": "Fix the flaky integration test"}
{"project": "../website", "task": "Update the changelog", "name": "Website"}
```

| field | required | meaning |
| --- | --- | --- |
| `project` | yes | path to the Git working tree; a relative path resolves against the job file's own directory |
| `task` | yes | the task text, exactly as `lya run` would take it |
| `name` | no | display name; defaults to the directory name |

Unknown fields are refused rather than ignored, so a typo is reported instead of silently dropping
an instruction. A job file holds at most 256 jobs.

```bash
cargo run -- scheduler jobs.jsonl --max-concurrent 3
```

Each request becomes a normal Lya job with its own job ID, `state.json`, `lock.json` and
`events.jsonl`. `--browser`, `--max-iterations`, `--max-jobs`, `--publish`, `--verbose` and `--json`
mean exactly what they mean for `lya run` and apply to every scheduled job.

### Concurrency And Fairness

```text
--max-concurrent <n>
```

defaults to **2** and must be greater than zero.

Exactly `n` execution slots exist; Lya does not spawn a task per job and gate provider calls
afterwards. Ordering is FIFO and deterministic: a worker takes the first queued job whose repository
is free. A job whose repository is busy is skipped rather than allowed to hold a slot, so one
contended repository never stalls unrelated work.

One job failing never cancels an unrelated job. Every job keeps its own outcome.

### Queued Work Survives A Crash

Accepted work is durable before it starts. Every request is persisted as a real job with status:

```text
QUEUED
```

so four situations stay distinguishable after an interruption:

| situation | how it looks |
| --- | --- |
| never started | `QUEUED` |
| currently being driven | `RUNNING`/`PUBLISHING`/… with a live job lock |
| resumable interrupted work | the same statuses with no live lock |
| terminal | `ACCEPTED`, `PUBLISHED`, `FAILED`, `STOPPED`, `WAITING_HUMAN` |

`QUEUED` is deliberately **not** resumable. `lya resume` refuses it, so ordinary resume can never
bypass the repository claim or the concurrency bound. Only the scheduler starts queued work:

```bash
cargo run -- scheduler --resume-queued
```

Re-queued jobs keep their original job identity and adopt the options of the invocation that picks
them up. A job that fails before the orchestrator's first write is recorded as `FAILED` rather than
left advertised as queued, so the durable record and the scheduler report always agree.

### Sequential Jobs Under The Scheduler

A `next_prompt` chain stays owned by one scheduled root. The worker holds that repository's claim
for the whole chain instead of releasing and reacquiring it between chained jobs, so a chain never
loses its repository to another job halfway through, and a chain does not consume one global slot
per child.

Every sequential child still takes its own job lock, still counts against `--max-jobs`, and still
records its own `sequential_index`.

### Output

Scheduler-wide observation is a separate structured stream from job events, so job events stay
exactly what they were. Scheduler events cover queued, waiting for repository, started, completed,
failed, not started, stopping and finished.

Human output prefixes every job line with its project and job, so an interleaved terminal stream
stays readable:

```text
01:29:10  SCHEDULER  2 job(s) queued; at most 2 repositories at a time
01:29:10  SCHEDULER  started service job-1789262950-23120-0
[service job-1789262950-23120-0] 01:29:11  SUPERVISOR  CLAUDE
[website job-1789262950-23120-1] 01:29:11  CLAUDE
```

`--json` keeps stdout machine-readable: one JSON object per line and nothing else. Job events carry
an `event` field and scheduler events a `scheduler_event` field, so the two never have to be told
apart by guessing.

```json
{"timestamp_unix_millis":1789262921937,"scheduler_event":"SCHEDULER_STARTED","max_concurrent":2,"queued":2}
```

Event logs remain non-authoritative, and each job keeps its own `events.jsonl`.

### Stopping

The first `Ctrl+C` stops the scheduler launching new work and requests the same graceful stop every
active job would receive from a single `lya run`, including provider process-tree cancellation. The
scheduler then waits for those jobs to shut down in a controlled way. Jobs that never started stay
`QUEUED`. A second `Ctrl+C` keeps its existing force-exit meaning.

### Typed Commands Are Not Multiplexed

Scheduler mode is **non-interactive**. `/pause`, `/resume`, `/status`, `/diff`, `/send` and `/stop`
act on one unambiguous job, and multiplexing typed commands across several simultaneous jobs on one
terminal needs an interaction model Lya does not have. Rather than invent one casually,
`lya scheduler` accepts no typed commands.

Controlling one of several concurrent jobs is answered by naming it instead, in
[daemon mode](#controlling-a-job):

```bash
lya control <job-id> pause
```

`lya run` keeps the full interactive control it has always had. Graceful termination works in both.

## Daemon Mode

Autonomous work does not need your terminal to stay open.

A Lya daemon owns the scheduler, the running jobs, the queued jobs, the repository coordination and
the live control endpoint. It survives the shell that started it. Other `lya` commands become
clients of it.

```text
lya daemon start          start it detached and get your prompt back
lya daemon status         is one running, and what is it doing
lya daemon stop           stop accepting work, shut active jobs down safely, exit
lya daemon run            run it in the foreground (development and debugging)

lya submit ...            hand work over; get the job ids back
lya attach <job-id>       watch one job live; Ctrl+C detaches, the job keeps running
lya control <job-id> ...  pause / resume / stop / status / diff / send, per job
```

This is **local daemon mode only**. No port is opened, no address is advertised, no remote access
exists and there is nothing to authenticate over a network.

### Starting And Stopping

```bash
lya daemon start
```

starts Lya detached — a new session on Unix, a detached process group on Windows — and returns
immediately:

```text
Lya daemon started as process 4812 on \\.\pipe\lya-daemon-9f1c....
Log: ~/.lya/daemon/daemon.log
```

Starting twice cannot produce two daemons for one `LYA_HOME`. The second one is refused by the
[daemon claim](#the-daemon-claim) before it binds anything, and `lya daemon start` reports the daemon
that is already running instead of failing:

```text
A Lya daemon is already running for ~/.lya as process 4812 on \\.\pipe\lya-daemon-9f1c....
```

Options:

| Option | Meaning |
| --- | --- |
| `--max-concurrent <n>` | How many repositories may be driven at once (default 2). |
| `--resume-interrupted` | Continue interrupted jobs on startup. Off by default; see [Restart Recovery](#restart-recovery). |
| `--no-recover-queued` | Do not pick up work a previous daemon accepted and never started. |
| `--verbose` / `--json` | `lya daemon run` only: how the foreground daemon narrates. |

```bash
lya daemon status
```

```text
Lya daemon

STATE          running
PROCESS        4812
LYA_HOME       ~/.lya
ENDPOINT       \\.\pipe\lya-daemon-9f1c...
PROTOCOL       1
CONCURRENCY    at most 2 repositories
CLIENTS        1 connected, 0 attached
RESUME         interrupted jobs are left parked

ACTIVE (1)
  job-1763040000-4812-0  api  RUNNING  iteration 2/10  Fix the flaky test
  watch one with: lya attach <job-id>

QUEUED (1)
  job-1763040007-4812-1  web  QUEUED  iteration 0/10  Update the changelog

RESUMABLE (0)
```

`lya daemon status --json` prints the same information as one JSON object. The command exits with a
failure when no daemon is running, so a script can test for one.

```bash
lya daemon stop
```

requests a graceful shutdown and waits until it has actually happened:

1. no further work is accepted — a submission arriving now is refused with `SHUTTING_DOWN`;
2. every active job receives the same graceful stop a foreground `Ctrl+C` would request, including
   provider process-tree cancellation, and parks itself at a safe boundary under the existing job
   semantics;
3. the daemon waits for those jobs to shut down;
4. it releases its claim and exits.

The command does not print `Lya daemon stopped.` until step 4 has happened, and it decides that by
polling the claim on `LYA_HOME` — never by watching the endpoint. The endpoint stops answering at
step 1, while the daemon is still running and still shutting jobs down, so treating an unreachable
endpoint as "stopped" would report success on a home the next command cannot use. Because the claim
is the authority:

```bash
lya daemon stop && lya daemon start
```

works even when active jobs take minutes to drain.

Work that never started stays `QUEUED` and is picked up by the next daemon — a shutdown never starts
a queued job in order to stop it. Stopping when nothing is running succeeds and says so, so the
command is safe to repeat.

### Submitting Work

```bash
lya submit "Fix the flaky test in tests/api.rs"
lya submit --project ../web --max-iterations 6 --publish "Update the changelog"
lya submit --file jobs.jsonl
```

Submitted work becomes ordinary persisted Lya jobs and flows through the same scheduler
`lya scheduler` uses. The client prints the job ids the daemon created:

```text
job-1763040000-4812-0  QUEUED  /home/you/projects/api
Watch one with: lya attach <job-id>
```

Options are the ones `lya run` already has — `--project`, `--max-iterations`, `--max-jobs`,
`--browser`, `--publish` — plus `--file` and `--json`. `--file` takes exactly the
[job file](#multi-project-scheduling) `lya scheduler` takes, parsed by the same parser, so one
grammar has one implementation.

Two things are resolved by the client, in the shell that has the context for them, and travel with
the job:

* the project path, canonicalized, so a relative path is never interpreted against the daemon's
  working directory;
* the Git publication configuration, read from `LYA_GIT_*` and validated before the job is queued,
  so a job is never accepted in a shape that can only fail later.

Each job is validated and accepted on its own, and **every submitted job gets an answer**. A
submission of ten jobs with one unusable path queues nine and says exactly which one it refused:

```text
-                      NOT QUEUED  could not resolve the repository at ../gone
job-1763040000-4812-0  QUEUED  /home/you/projects/api
```

That holds for every way one job can fail, including one the daemon could not write down. A failure
partway through a batch refuses that job and nothing else; it never becomes an error for the whole
submission, because an error for the whole submission would throw away the ids of the jobs already
durably accepted before it — and a client that never learns those ids cannot tell a failed
submission from a partly succeeded one.

The client holds no queue and no state. It sends a request and prints the answer.

#### Delivery Semantics

A job id is returned only after that job exists on disk as `QUEUED`. What the daemon reports, it has
already committed to.

The reverse is not guaranteed, and the honest statement is: **submission is at-least-once.** The
daemon can durably accept jobs and then fail to deliver the response — the connection drops, the
client is interrupted, the machine loses power between the write and the read. The client then
reports a failure for work that is queued and will run.

There is no submission identity on the wire and no de-duplication, so **re-running an identical
`lya submit` after a lost response can create duplicate jobs**, each with its own id, each running
against the same repository. They will not run concurrently — the repository claim serialises them —
but the work does happen twice.

If a submission fails in a way that leaves it unclear, check before retrying:

```bash
lya daemon status
```

Anything durably accepted is listed under `QUEUED` or `ACTIVE` with its id.

### Attaching And Detaching

```bash
lya attach job-1763040000-4812-0
```

streams that job's live events — the same `JobEvent` objects `events.jsonl` and `lya run --json`
carry — rendered exactly as `lya run` renders them:

```text
Attached to job-1763040000-4812-0. Ctrl+C detaches; the job keeps running.
14:03:21  SUPERVISOR  CLAUDE  Run the failing test and fix the race
14:04:02  EXECUTOR    finished in 41s (3 turns)
```

Attach is **observational**. `Ctrl+C` detaches the viewer: the connection closes, the daemon forgets
it and the job continues untouched under the daemon. Nothing about attaching can pause, stop or steer
a job — that is what `lya control` is for. Any number of viewers may watch one job, and a job with no
viewers runs exactly the same.

`--replay` shows the job's recorded history before the live events. The subscription is opened before
the history is read, so nothing emitted during the replay is lost, and an event the replay already
showed is not repeated when it arrives live. The replay is bounded to the most recent events, and a
line the event log cannot parse — what a crash mid-write leaves behind — is skipped rather than
failing the attach. `events.jsonl` remains a record, never an authority.

A viewer that stops reading is disconnected on its own, with a reason, and the job is unaffected:

```text
Detached from job-1763040000-4812-0: the client fell behind by 128 event(s)
```

A [sequential chain](#sequential-jobs) is one piece of work with several job identities. Attaching to
the job you submitted follows the whole chain, and each job of it can also be watched by its own
name.

`--json` streams the raw events instead, one object per line.

### Controlling A Job

The interactive commands `lya run` accepts are addressable by job name:

```bash
lya control job-1763040000-4812-0 pause
lya control job-1763040000-4812-0 resume
lya control job-1763040000-4812-0 send "Also update the changelog"
lya control job-1763040000-4812-0 status
lya control job-1763040000-4812-0 diff
lya control job-1763040000-4812-0 stop
```

Each command is delivered into that job's own existing control channel, so it means exactly what it
means in an interactive `lya run`: `pause` takes effect at a safe boundary, `stop` performs a
controlled shutdown, and `send` queues an instruction for the next agent turn under the existing
per-job instruction limits and persistence. There is one control state machine, and the daemon does
not add a second one.

Naming the job is what makes this unambiguous while several jobs run at once — the multiplexing
problem `lya scheduler` deliberately does not solve.

`status` and `diff` answer into the job's own event stream rather than into the command's output,
because that is where a job reports. The acknowledgement says so:

```text
Status requested; job-1763040000-4812-0 reports it in its events (lya attach job-1763040000-4812-0).
```

A control request fails, with a distinct reason, when it cannot be delivered:

| Situation | Reported as |
| --- | --- |
| No such persisted job | `UNKNOWN_JOB` |
| The job has reached a terminal status | `JOB_TERMINAL` |
| The job is queued and has not started | `INVALID_FOR_STATE` |
| The job is live but this daemon is not driving it | `JOB_NOT_OWNED` |
| The command itself is not usable (an empty or oversized instruction) | `INVALID_REQUEST` |

It never reports success for a command that was not delivered.

### Restart Recovery

If a daemon crashes, or the machine restarts, no job state is lost. Authoritative state is each job's
own `state.json`, and every lock is released by the operating system on process exit.

On the next start the daemon separates two questions it must not confuse:

* **Queued work** was accepted and never started, so there is nothing to reconstruct. It is
  submitted again, keeping its job identity and the limits it was accepted with. `--no-recover-queued`
  turns this off.
* **Interrupted work** was in the middle of something. By default the daemon finds it, reports it and
  leaves it exactly as it is:

  ```text
  DAEMON  parked 1 interrupted job(s): job-1763039000-3140-0 (continue with lya resume --job <id>)
  ```

  Parked jobs appear under `RESUMABLE` in `lya daemon status`, so nothing is silently dropped, and
  their persisted state is not touched.

`--resume-interrupted` asks the daemon to continue them, through the ordinary resume path with its
full validation: the persisted job is never rewritten to start it, `ResumePlan` decides where it
continues, and anything ambiguous is parked in `WAITING_HUMAN` exactly as `lya resume` would park it.
The daemon invents no recovery semantics of its own — it only decides whether to ask.

A job another Lya process is currently driving is never taken over, even with
`--resume-interrupted`: its job lock is held, so the daemon parks it and reports it. Startup fails
closed.

### Ownership And Exclusion

Every job the daemon drives holds the same claims a foreground `lya run` takes — its
[job lock](#job-locks) and its [repository claim](#repository-claims) — for the whole sequential
chain. Consequently:

* a foreground `lya run`, `lya resume` or `lya scheduler` cannot drive a job or a repository the
  daemon owns; it is refused, not queued behind it;
* the daemon cannot take over a job or a repository a foreground process owns;
* scheduler concurrency, persisted resume rules and queued-job semantics are unchanged.

Both directions fail closed, and both are enforced by the operating system rather than by
bookkeeping.

### Existing Commands Are Unchanged

`doctor`, `supervisor`, `executor`, `run`, `resume`, `jobs` and `scheduler` behave exactly as before
and are **never silently redirected** to a daemon.

That is a deliberate compatibility rule, not an omission. `lya run` and `lya scheduler` are attached
to your terminal and end with it; daemon-owned work does not. Quietly changing which one you got
would change where your job lives, who can control it and what happens when you close the shell. Work
reaches the daemon when you ask it to, through `lya submit`.

`lya jobs` keeps listing every persisted job, whoever is driving it, and stays read-only.

### The Local Protocol

Clients and daemon speak a versioned, newline-delimited JSON protocol over a local transport:

| Platform | Endpoint |
| --- | --- |
| Windows | Named pipe, `\\.\pipe\lya-daemon-<home-fingerprint>` |
| Linux, macOS | Unix stream socket, `LYA_HOME/daemon.sock` |

The endpoint is derived from `LYA_HOME`, so two homes are two independent daemons and one home is
always the same endpoint. A client never has to be told where to look.

Each frame is one JSON object on one line: an envelope carrying the protocol version around one
tagged payload.

```text
{"protocol_version":1,"message":{"request":"ATTACH","job_id":"job-1763040000-4812-0","replay":true}}
{"protocol_version":1,"message":{"response":"ATTACHED","job_id":"job-1763040000-4812-0"}}
```

The payload is nested rather than merged into the envelope so that no payload field can ever collide
with the envelope's own. Requests and responses are explicit data transfer objects: a job in a status
listing is a projection of persisted state, not that state serialized, so the on-disk layout is free
to change and fields that have no business leaving the machine do not exist on the wire. Live job
events are the deliberate exception — they are already Lya's published observation format.

Every frame is bounded, and a malformed one is answered with an error rather than tolerated:

* a frame that is not JSON, or not a message this version knows, gets `INVALID_REQUEST`;
* a client speaking another protocol version gets `UNSUPPORTED_PROTOCOL`, naming both versions;
* a frame that exceeds the size bound ends that connection.

None of this can affect the daemon, the jobs or another client. Every connection is its own task: a
client that sends nonsense, stops reading, or disappears mid-frame is the only thing affected.

### Daemon Observability

The daemon keeps its own structured history, separate from job events:

```text
LYA_HOME/daemon/events.jsonl
```

One JSON object per line, tagged `daemon_event`, covering the daemon's own life: started, stopping,
stopped, scheduler started and stopped, work submitted, control delivered, queued work recovered,
interrupted work parked or resumed.

Transient per-connection traffic — clients connecting, disconnecting, attaching, detaching, being
refused — is shown while you watch a daemon and deliberately **not** written to the permanent log. A
daemon that runs for weeks would otherwise fill its history with the comings and goings of
`lya daemon status`.

Each job keeps its own `events.jsonl`, unchanged. Neither log is ever authority: every decision comes
from authoritative persisted state and from the operating system's own claims.

A detached daemon's narration is captured in `LYA_HOME/daemon/daemon.log`, which is also where a
startup failure explains itself.

### Security

* The endpoint is local-only. No TCP socket is opened and no port is bound.
* **Windows — the named pipe carries an explicit access control list.** Only the user running the
  daemon and `SYSTEM` are granted access. Windows' *default* named-pipe descriptor is not used,
  because it also grants read access to `Everyone` and to `ANONYMOUS LOGON`, which is enough for any
  local account to open the pipe and hold an instance. Remote clients are refused explicitly.
* **Windows — both ends verify the other's user.** The pipe name is a deterministic fingerprint of
  `LYA_HOME`, and any local account may create a name in the named-pipe namespace, so a squatter can
  own the name before the daemon starts. The access control list cannot prevent that, so it is not
  relied on alone: the daemon checks every accepted client's token user before serving it — by
  impersonating the client, falling back to the pipe's own client process only when impersonation is
  unavailable — and disconnects a stranger without reporting anything to it; a client checks the
  serving process's token user *before sending a single byte* and refuses anything that is not this
  user's daemon. Identity is a security identifier compared with `EqualSid`, never a process ID; a
  process ID is only ever a way to reach a token, and every failure on that path refuses.
* **Unix — the socket is `0600` inside a `0700` home.** File-system permissions are the access
  control. There is no name to squat: the socket is a path inside a directory only the owner can
  traverse. *Verified by code and API contract; the Windows behaviour above is verified on Windows.*
* A connected client that sends no request within ten seconds is disconnected. Only that client:
  the daemon, its jobs and every other client are untouched.
* No credential is persisted, sent or logged. Provider API keys continue to be removed from
  child-process environments, Git publication is described by identity, remote and branch only — push
  authentication stays with the machine's own Git configuration — and daemon metadata holds nothing
  but a process ID, a start time, a protocol version and an endpoint.

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

Scheduler concurrency changes none of this. The scheduler starts jobs; it never inspects a
repository, never commits and never pushes. Every scheduled job goes through the same Git
verification, the same snapshot comparison and the same guarded publication as a single `lya run`,
and repository claims mean two jobs can never reach one working tree at the same time.

Daemon mode changes none of it either. The daemon owns processes, connections and shutdown; it holds
no job semantics, no control state machine and no recovery rules of its own. A daemon-driven job is
the same job, under the same claims, with the same guarded publication, and a client can ask it only
for things a terminal could already ask for. Its endpoint is local, and nothing it stores, sends or
logs contains a credential.

Lya also avoids giving provider processes unnecessary billing credentials by removing environment-provided API keys from their child-process environments.

These protections reduce accidental autonomous changes, but Lya is experimental software. Run autonomous workflows only in repositories where you understand and accept the risks.

## Current Limitations

`lya run` drives one job at a time. Concurrent work goes through `lya scheduler` or the daemon.
Neither accepts typed commands on one terminal: a concurrent job is controlled by naming it, with
`lya control <job-id> ...`.

A second `Ctrl+C` force-terminates Lya immediately. That is intentional and safe for job locks,
repository claims and the daemon claim, which the operating system releases on process exit, but it
skips Lya's own cleanup.

Daemon mode is local only, by design: one daemon per `LYA_HOME`, reachable from the machine it runs
on and from nowhere else.

`lya attach` is observational. It streams events; it does not accept typed commands on the stream.

The daemon does not continue interrupted jobs unless it is started with `--resume-interrupted`.
Interrupted work is reported and left parked instead.

The following are not implemented yet:

* running the daemon as a system service (`systemd`, `launchd`, a Windows service);
* remote or multi-machine access of any kind;
* a web or browser administration interface;
* typed interactive commands multiplexed across concurrent jobs on one terminal;
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
* [x] Provider process-tree cancellation
* [x] Bounded multi-project scheduling
* [x] Repository-level coordination
* [x] Persistent local daemon mode
* [x] Background scheduling with attach/detach
* [x] Per-job remote control through the daemon
* [ ] Running the daemon as a system service
* [ ] Remote administration interface
* [ ] Typed interactive commands across concurrent jobs
* [ ] Additional Supervisor and Executor providers
* [ ] Stable public API

## Contributing

Lya is open source and contributions are welcome.

The project is still evolving quickly, so architecture and public APIs may change while the autonomous runtime is being stabilized.

When contributing, prefer small, focused changes that preserve Lya's lightweight and understandable architecture.

## License

Lya is licensed under the MIT License.

See [`LICENSE`](LICENSE) for details.
