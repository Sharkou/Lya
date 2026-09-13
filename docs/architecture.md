# Architecture

Lya has two complementary roles behind one executable: a small local agent runtime, and an
autonomous development orchestrator. They share the process and almost nothing else.

* [The orchestration path](#the-orchestration-path)
* [Layers](#layers)
* [Components](#components)
* [The local agent runtime](#the-local-agent-runtime)
* [Module map](#module-map)
* [Design rules](#design-rules)

## The orchestration path

```text
user
  ↓
Lya
  ├─ Codex Supervisor   decides what happens next
  └─ Claude Executor    performs the development work
```

Expanded, with everything that sits around that pair:

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
         |
    +----+----+---------------+---------------+
    v         v               v               v
Supervisor  Executor    Repository state   Publisher
(codex)     (claude)    (git, read-only)   (git, guarded)
         |
         v
  per-job persisted state + events.jsonl
```

The daemon and the scheduler are boundaries *above* the orchestrator, not replacements for it. The
orchestrator still owns exactly one autonomous job and its sequential chain. Each layer adds one
decision and delegates the rest: Supervisor and Executor implementations know nothing about
scheduling, and nothing in the daemon knows how a job works.

## Layers

| Layer | Owns | Does not own |
| --- | --- | --- |
| Daemon | the claim on one `LYA_HOME`, the local endpoint, connected clients, graceful shutdown | job semantics, control state, recovery rules |
| Scheduler | which jobs may start, how many run at once, which repository each owns | repository inspection, commits, pushes |
| Orchestrator | one job and its sequential chain, the iteration loop, persisted state, events | how a decision is produced, how work is performed |
| Supervisor | the next decision | committing anything |
| Executor | development work | publication |
| Publisher | commit and push, guarded | any model call |

## Components

### Supervisor (Codex CLI)

Invoked as `codex exec --sandbox read-only` with a JSON output schema. It:

1. loads the local private context (`LYA_HOME/context.md`);
2. builds a structured request describing the task, phase, iteration, the previous executor report
   and the repository state Lya collected;
3. invokes the CLI with the prompt on stdin;
4. requires a structured decision;
5. validates that decision locally against the schema.

A decision is exactly one of `CLAUDE`, `ACCEPT`, `HUMAN` or `STOP`. The invocation has a
120-second timeout, and `OPENAI_API_KEY` is removed from its environment.

### Executor (Claude Code)

Invoked as `claude --print --output-format json --permission-mode auto --permission-prompts none`,
with the working directory set to the project and the prompt on stdin. Sessions are resumed across
correction cycles with `--resume <session>`, so the Executor keeps the context of its own previous
work inside a job. `--chrome` is added when `--browser` is requested; browser automation itself
remains Claude Code's responsibility. `ANTHROPIC_API_KEY` is removed from its environment.

It has no timeout inside an autonomous job — a long run is allowed to finish.

### Repository state

Lya collects `HEAD`, `git status --short`, `git diff --stat`, changed files, the tracked diff and
untracked files itself, with explicit size bounds and explicit truncation markers. This is the state
the Supervisor reviews and the state publication is verified against; an executor report is never
the authority.

### Publisher

Commits and pushes, and uses no model. It re-collects and compares repository state after the
`ACCEPT` decision, verifies the staged content again, and records the commit in authoritative job
state as soon as it exists. See [Git publication](git-publication.md).

### Scheduler

Holds `n` execution slots (default 2), takes jobs FIFO, and skips a job whose repository is busy
rather than letting it hold a slot. Every accepted request is persisted as a `QUEUED` job before it
starts. See [Scheduler](scheduler.md).

### Daemon

Owns the process-level concerns so autonomous work does not need a terminal: one claim per
`LYA_HOME`, the local control endpoint, connected clients and graceful shutdown. It runs the same
scheduler, and clients (`submit`, `attach`, `control`, `daemon status`) hold no orchestration logic —
they encode a request, render what comes back and choose an exit code.

### Local IPC

A versioned, newline-delimited JSON protocol over a named pipe (Windows) or a Unix stream socket,
derived from `LYA_HOME`. Local only: no TCP, no port, no remote address. Transport knows about pipes
and sockets and nothing else; the protocol is the wire contract and renders nothing; rendering never
leaks into the protocol. See [Daemon — the local protocol](daemon.md#the-local-protocol) and
[Security](security.md#the-daemon-endpoint).

### Locks and claims

Three operating-system claims, all advisory locks on open handles rather than files-as-flags:
`JobLock` (one driver per job), `RepositoryLock` (one driver per repository) and `DaemonLock` (one
daemon per home). Taken repository-first, job-second, so the layers cannot deadlock; released by the
kernel on process exit, so a crash needs no cleanup. See
[Persistence and recovery — locks and claims](persistence-and-recovery.md#locks-and-claims).

### Persisted state and events

Authoritative per-job `state.json`, written by temporary file plus sync plus rename. Observability
is `events.jsonl` per job, plus the daemon's own `daemon/events.jsonl`. **Neither log is ever
authority** — no decision is re-derived from a rendered message or a recorded event.

## The local agent runtime

Independent of everything above. No `LYA_HOME`, no job state, no Codex, no Claude.

```text
user prompt
  ↓
Agent loop (bounded at 20 iterations)
  ↓
LLM  ──►  Ollama, an OpenAI-compatible /chat/completions endpoint
  ↓
tool_call
  ↓
tool executed by Lya, inside LYA_WORKSPACE
  ↓
tool_result  ──►  back to the LLM
  ↓
final answer
```

Tools: `get_current_directory`, `read_file`, `write_file`, `create_directory`, `list_directory`,
`run_command`. Tools are executed by Lya, never by the model directly, and are confined to
`LYA_WORKSPACE`. The agent does not depend on a specific provider; Ollama is one backend behind a
focused adapter.

## Module map

```text
src/
├── main.rs                  CLI: argument parsing, rendering, exit codes
├── lib.rs
├── agent.rs                 the local agent loop
├── runtime.rs               workspace resolution and tool registration
├── process.rs               child-process runner, timeouts, cancellation
├── process/tree.rs          platform process-tree claim (job object / process group)
├── llm/
│   ├── mod.rs               provider-agnostic chat contract
│   └── ollama.rs            Ollama adapter
├── tools/                   command, directory, filesystem, listing
├── orchestrator/
│   ├── job.rs               AutonomousOrchestrator: the job loop
│   ├── supervisor.rs        Codex Supervisor
│   ├── executor.rs          Claude Code Executor
│   ├── repository.rs        independent Git state collection
│   ├── publisher.rs         guarded commit and push
│   ├── scheduler.rs         slots, queue, repository fairness
│   ├── batch.rs             the job-file grammar
│   ├── state.rs             authoritative persisted state
│   ├── resume.rs            resume validation and continuation planning
│   ├── inventory.rs         read-only job listing
│   ├── lock.rs              JobLock
│   ├── repository_lock.rs   RepositoryLock and repository identity
│   ├── control.rs           typed control commands
│   ├── events.rs            JobEvent and its sinks
│   ├── home.rs              LYA_HOME resolution
│   ├── context.rs           private context loading
│   └── doctor.rs            environment diagnostics
└── daemon/
    ├── server.rs            coordinates scheduler and orchestrator; owns no job semantics
    ├── runner.rs            the production job driver
    ├── client.rs            what the CLI uses; formats nothing
    ├── protocol.rs          the versioned wire contract; renders nothing
    ├── transport.rs         named pipe / Unix socket, and framing
    ├── security.rs          Windows ACL and same-user verification (Windows only)
    ├── lock.rs              DaemonLock
    ├── attach.rs            live event fan-out
    └── events.rs            DaemonEvent and its sinks
```

## Design rules

These are the constraints the codebase is written against, and the reason it looks the way it does:

* **One authority per question.** Persisted state and operating-system claims decide what may
  happen; logs never do.
* **Each layer adds one decision.** A layer that starts knowing how the layer below works has grown
  a second responsibility.
* **Fail closed.** Anything ambiguous parks a job for a person rather than guessing.
* **No silent redirection.** A foreground command never becomes a daemon-driven one.
* **Replaceable providers.** The current Supervisor is Codex and the current Executor is Claude
  Code, but implementations sit behind focused traits so the orchestration core does not have to be
  rewritten to change them.
* **No abstraction before it is needed.** Small understandable components over a framework.
