# Lya

**A local-first AI agent and autonomous development orchestrator, written in Rust.**

Lya coordinates specialized AI command-line tools into controlled autonomous development loops. It
inspects real repository state itself, persists every job so an interrupted run can be continued,
and publishes changes through Git only after verifying that what is about to be committed is exactly
what was reviewed.

It started as a small agent runtime for local language models, and the original runtime is still
there. The project favours small, understandable components over a large agent framework.

[![CI](https://github.com/Sharkou/Lya/actions/workflows/ci.yml/badge.svg)](https://github.com/Sharkou/Lya/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Status

**Experimental — v0.1.1, under active development.** Commands, APIs and internal architecture may
change without a deprecation path while the autonomous runtime is being stabilized.

Lya drives AI tools that modify real repositories, and it is **not a sandbox** for them. Run
autonomous workflows only where you understand and accept the risk, and read
[Security — what is not a security boundary](docs/security.md#what-is-not-a-security-boundary)
before you point it at anything you care about.

## Capabilities

* 🤖 **Autonomous development loop** — a Codex Supervisor decides, a Claude Code Executor works, and
  Lya independently inspects the repository between turns.
* 🔒 **Guarded Git publication** — opt-in commit and push, verified twice against the reviewed
  snapshot; never `pull`, `merge`, `rebase`, `reset` or force-push.
* 💾 **Crash-safe, resumable jobs** — authoritative per-job state on disk, one pending operation per
  job, and a resume that validates against the live repository before contacting a provider.
* 🧭 **Bounded multi-project scheduling** — different repositories run concurrently, the same
  repository never does, enforced by operating-system claims.
* 🛰️ **Persistent local daemon** — work that outlives your terminal, with `submit`, `attach` and
  per-job `control` over a local-only endpoint.
* 🎛️ **Interactive control** — pause, resume, stop, inspect the diff, or inject an instruction while
  a job runs, applied at a safe boundary.
* 🧱 **Provider process-tree cancellation** — a cancelled provider takes its helper processes with
  it, via a Windows job object or a Unix process group.
* 🏠 **Local-first** — orchestration, state, configuration and repository access stay on your
  machine. No API key needed: the provider CLIs use their own authenticated sessions.
* 🦀 **Rust, no framework** — small, explicit, strongly typed, with replaceable provider adapters.

## Architecture

```text
user
  ↓
Lya
  ├─ Codex Supervisor   decides what happens next
  └─ Claude Executor    performs the development work
```

With the layers that sit around that pair:

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
        |
  AutonomousOrchestrator
        |
   +----+----+---------------+---------------+
   v         v               v               v
Supervisor  Executor   Repository state   Publisher
(codex)     (claude)   (git, read-only)   (git, guarded)
```

The daemon and the scheduler are boundaries *above* the orchestrator, not replacements for it. Each
layer adds one decision and delegates the rest: the Supervisor never commits, the Executor never owns
publication, the Publisher uses no model, and Lya trusts its own repository inspection over any
executor report.

Full detail in [Architecture](docs/architecture.md).

## Installation

Prebuilt archives for Windows x86_64, Linux x86_64, macOS arm64 and macOS x86_64 are attached to
every [release](https://github.com/Sharkou/Lya/releases). Binaries are unsigned — verify the
published SHA-256 checksums.

Or build from source with Rust 1.89 or newer:

```bash
git clone https://github.com/Sharkou/Lya.git
cd Lya
cargo build --release --locked
```

Full instructions, prerequisites and platform notes: **[Installation](docs/installation.md)**.

### Prerequisites, in short

| Needed for | Requirement |
| --- | --- |
| everything | Git on `PATH` |
| autonomous orchestration | [Codex CLI](https://developers.openai.com/codex/cli) and [Claude Code](https://docs.claude.com/en/docs/claude-code), each installed and signed in independently |
| local agent mode | [Ollama](https://ollama.com/) and a tool-calling model |
| Git publication | working push authentication, plus `LYA_GIT_*` |

Lya needs **no API key of its own** — it runs the provider CLIs as child processes and relies on
their existing authenticated sessions, and it removes `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` from
those child environments so an ambient key cannot be billed by accident.

## Quick start

```bash
# 1. private context the Supervisor requires
mkdir -p ~/.lya && printf 'Standing project context.\n' > ~/.lya/context.md

# 2. check the environment
lya doctor

# 3. one autonomous job, in a clean Git working tree, nothing published
cd /path/to/project
lya run --max-iterations 3 "Add a unit test for the existing input validation and verify it passes."
```

`lya --help` lists the commands and `lya --version` prints the version; `lya doctor` is the
environment check. [The CLI reference](docs/cli-reference.md) has every flag.

More, including the PowerShell equivalents: **[Quick start](docs/quick-start.md)**.

## Example workflow

Hand work to a background daemon, watch it, steer it, and let it publish:

```bash
export LYA_GIT_NAME="Automation Bot"
export LYA_GIT_EMAIL="bot@example.com"
export LYA_GIT_BRANCH="main"

lya daemon start
lya submit --project /path/to/project --publish "Fix the flaky integration test and verify it."
lya daemon status

lya attach job-1763040000-4812-0
lya control job-1763040000-4812-0 send "Also update the changelog."
lya control job-1763040000-4812-0 pause

lya daemon stop
```

`Ctrl+C` during `lya attach` detaches the viewer only — the job keeps running. `lya daemon stop`
refuses new work, stops active jobs at a safe boundary, waits for them, and only then reports
success.

Several repositories at once, without a daemon:

```bash
cat > jobs.jsonl <<'EOF'
{"project": "/path/to/service", "task": "Fix the flaky integration test"}
{"project": "../website", "task": "Update the changelog", "name": "Website"}
EOF

lya scheduler jobs.jsonl --max-concurrent 3
```

## Documentation

The [`docs/`](docs/README.md) directory is the source of truth.

| Document | What it answers |
| --- | --- |
| [Installation](docs/installation.md) | platforms, prebuilt binaries, source builds, prerequisites |
| [Quick start](docs/quick-start.md) | first job, first daemon |
| [CLI reference](docs/cli-reference.md) | every command, flag, default and exit code |
| [Configuration](docs/configuration.md) | every environment variable, `LYA_HOME`, `context.md` |
| [Autonomous jobs](docs/autonomous-jobs.md) | the loop, statuses, control, quotas, events |
| [Scheduler](docs/scheduler.md) | multi-project scheduling and the job file |
| [Daemon](docs/daemon.md) | background work, submit/attach/control, recovery, protocol |
| [Git publication](docs/git-publication.md) | the guarded commit-and-push sequence |
| [Persistence and recovery](docs/persistence-and-recovery.md) | on-disk layout, locks, resume, real guarantees |
| [Security](docs/security.md) | what is enforced, and what is not a boundary |
| [Architecture](docs/architecture.md) | layers, components, module map, design rules |
| [Troubleshooting](docs/troubleshooting.md) | concrete failures and what to do |
| [Development](docs/development.md) | validation flow, tests, CI |
| [Releases](docs/releases.md) | versioning, release workflow, checksums, signing status |

## Development

```bash
cargo fmt --check
cargo check --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release --locked
```

The test suite is provider-free — nothing in it calls Codex or Claude — and CI runs the same checks
natively on Linux, Windows and macOS, because a large part of Lya is platform-specific.

See **[Development](docs/development.md)**.

## Security

Lya separates reasoning from repository publication, verifies the reviewed snapshot twice before a
commit exists, keeps its daemon endpoint local-only with same-user verification, and persists no
credential.

It is also explicitly **not** a sandbox for the AI tools it drives, and `context.md` is sent to the
Supervisor — so it must never contain secrets.

Read [Security](docs/security.md) for both halves of that, and
[SECURITY.md](SECURITY.md) to report a vulnerability. Please do not open a public issue for one.

## Roadmap

Implemented: agent core and Ollama provider, tool calling, private local context, persistent job
state, Codex Supervisor, Claude Code Executor, resumable provider sessions, the autonomous loop,
independent Git review, guarded commit and push, sequential multi-job runs, structured job events and
live output, interactive control, persisted resume, quota-aware pause and resume, read-only job
listing, provider process-tree cancellation, bounded multi-project scheduling, repository-level
coordination, persistent local daemon mode, background scheduling with attach/detach, and per-job
remote control through the daemon.

Not implemented yet:

* running the daemon as a system service (`systemd`, `launchd`, a Windows service);
* remote or multi-machine access of any kind;
* a web or browser administration interface;
* typed interactive commands multiplexed across concurrent jobs on one terminal;
* automatic conflict resolution;
* GitHub API integration;
* additional Supervisor and Executor providers;
* a stable public API;
* code signing and notarization of release binaries.

## Contributing

Contributions are welcome. The project is still evolving quickly, so prefer small, focused changes
that preserve Lya's lightweight and understandable architecture.

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
