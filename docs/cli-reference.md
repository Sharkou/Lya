# CLI reference

Every command below is generated from the current argument parsers in `src/main.rs`. Flags not
listed here do not exist: an unknown `--flag` is refused with the offending name and that command's
usage line, and each command accepts only its own flags.

> `lya --help` (`-h`) prints one screen of orientation and `lya --version` (`-V`) prints the
> version; both exit `0`. They are recognised **only as the first argument**, because the first
> argument is what selects a subcommand — so `lya run --help` stays a `lya run` invocation and
> `run`'s own parser reports the unknown option together with `run`'s usage line. That is how you
> see any subcommand's usage: pass it something invalid.

## Command index

| Command | Purpose | Foreground |
| --- | --- | --- |
| [`lya <prompt>`](#lya-prompt) | local agent runtime against Ollama | yes |
| [`lya doctor`](#lya-doctor) | environment diagnostics | yes |
| [`lya supervisor`](#lya-supervisor) | one Supervisor decision, in isolation | yes |
| [`lya executor`](#lya-executor) | one Executor invocation, in isolation | yes |
| [`lya run`](#lya-run) | one autonomous job, attached to the terminal | yes |
| [`lya resume`](#lya-resume) | continue a persisted job | yes |
| [`lya jobs`](#lya-jobs) | read-only job listing | yes |
| [`lya scheduler`](#lya-scheduler) | several jobs at once, attached | yes |
| [`lya daemon start`](#lya-daemon-start) | start a detached daemon | no |
| [`lya daemon run`](#lya-daemon-run) | run the daemon in this process | yes |
| [`lya daemon status`](#lya-daemon-status) | what a daemon is doing | yes |
| [`lya daemon stop`](#lya-daemon-stop) | graceful daemon shutdown | yes |
| [`lya submit`](#lya-submit) | hand work to a daemon | yes |
| [`lya attach`](#lya-attach) | watch one daemon-owned job live | yes |
| [`lya control`](#lya-control) | steer one daemon-owned job | yes |

## Conventions

### Global flags

```text
lya --help     |  lya -h
lya --version  |  lya -V
```

`--help` prints one screen: the commands, the options shared by the job commands, `LYA_HOME`, and a
link to this documentation. It is orientation, not a manual — per-command detail lives in the
sections below.

`--version` prints `lya <version>`, taken from the crate version at compile time, so it cannot drift
from `Cargo.toml`:

```text
$ lya --version
lya 0.1.0
```

Both exit `0`, read no environment and contact nothing, so they work on a machine where nothing is
configured yet. Both are recognised only as the first argument — see the note at the top of this
page.

### Human, verbose and JSON output

Commands that narrate a job accept one output mode:

| Mode | Flag | Behaviour |
| --- | --- | --- |
| normal | *(default)* | compact human-readable event stream; ANSI colour only when stdout is an interactive terminal |
| verbose | `--verbose` | adds the full Supervisor review request, explicit decision fields, the complete Executor response, detailed repository metadata and publication details |
| JSON | `--json` | stdout carries nothing but one JSON object per line; diagnostics go to stderr |

`--verbose` and `--json` cannot be combined; doing so is an error.

In `--json` mode, job events carry an `event` field and scheduler events a `scheduler_event` field,
so the two never have to be told apart by guessing. See
[Autonomous jobs — event stream](autonomous-jobs.md#event-stream).

### Exit codes

Every command exits `0` on success and non-zero on failure. Specifically:

| Command | Non-zero means |
| --- | --- |
| `lya doctor` | some prerequisite is missing or unreadable |
| `lya run`, `lya resume` | the job ended in a status that is not a successful outcome, or the job could not start |
| `lya jobs` | at least one persisted job could not be read |
| `lya scheduler` | at least one scheduled job failed or was rejected |
| `lya daemon status` | no daemon is running for this `LYA_HOME` |
| `lya daemon stop` | the daemon was asked to stop but had not released its claim before the wait ended |
| `lya submit` | at least one submitted job was not queued |
| `lya control` | the command could not be delivered |

`lya daemon status` failing when nothing runs is deliberate, so a script can test for a daemon with
the command itself.

### Shared job options

These mean the same thing everywhere they appear:

| Option | Default | Meaning |
| --- | --- | --- |
| `--project <path>` | current directory | the Git working tree to drive; canonicalized by the command that reads it |
| `--browser` | off | pass the browser capability through to the Executor |
| `--max-iterations <count>` | `10` | Supervisor reviews allowed in one job; must be greater than zero |
| `--max-jobs <count>` | `10` | jobs allowed in one [sequential chain](autonomous-jobs.md#sequential-jobs); must be greater than zero |
| `--publish` | off | enable [guarded Git publication](git-publication.md) |

---

## `lya <prompt>`

The original local agent runtime: one prompt, a tool-calling loop against a local
OpenAI-compatible endpoint, one final answer on stdout.

```bash
lya "List the files in the workspace and summarise what they contain"
```

All arguments are joined with single spaces into the prompt, so quoting is optional for simple
prompts. An empty prompt is an error.

**Environment**

| Variable | Default | Required |
| --- | --- | --- |
| `OLLAMA_MODEL` | — | yes |
| `OLLAMA_BASE_URL` | `http://127.0.0.1:11434/v1` | no |
| `LYA_WORKSPACE` | a compile-time path inside the source checkout | with a prebuilt binary, yes |

The loop is bounded at 20 agent iterations. The tools exposed to the model are
`get_current_directory`, `read_file`, `write_file`, `create_directory`, `list_directory` and
`run_command`, all confined to `LYA_WORKSPACE`.

This mode is independent of the orchestrator: it uses no `LYA_HOME`, writes no job state and needs
neither Codex nor Claude. See [Configuration — local agent mode](configuration.md#local-agent-mode).

---

## `lya doctor`

```text
lya doctor
```

Takes no arguments; passing any is an error. Contacts no model, reads no repository and writes
nothing.

Reports the resolved `LYA_HOME`, whether `context.md` is readable, and whether `git`, `codex` and
`claude` resolve to an executable — honouring [`LYA_CODEX_BIN` and
`LYA_CLAUDE_BIN`](configuration.md#provider-executables), and `PATHEXT` on Windows.

```text
Lya doctor

LYA_HOME       OK  /home/you/.lya
context.md     OK
git            OK  /usr/bin/git
codex          OK  /usr/local/bin/codex
claude         MISSING

Not ready for orchestration.
```

Exits `0` only when every check passes.

---

## `lya supervisor`

```text
lya supervisor <task>
```

Runs exactly one Supervisor decision and prints it as pretty-printed JSON. All arguments are joined
into the task; an empty task is an error. The project is always the current directory — this
command has no `--project`.

```bash
lya supervisor "Determine the next development step"
```

Requires `context.md` in `LYA_HOME` and an authenticated `codex`. The Supervisor invocation has a
120-second timeout. A decision is one of `CLAUDE`, `ACCEPT`, `HUMAN` or `STOP`; see
[Architecture — Supervisor](architecture.md#supervisor-codex-cli).

Diagnostic only: it persists no job, takes no lock and touches no repository.

---

## `lya executor`

```text
lya executor [--project <path>] [--resume <session>] [--browser]
             [--timeout-seconds <seconds>] [--max-turns <count>] <prompt>
```

Runs exactly one Executor invocation and prints its structured result as pretty-printed JSON.

| Option | Default | Meaning |
| --- | --- | --- |
| `--project <path>` | current directory | working tree handed to Claude Code |
| `--resume <session>` | new session | continue the Claude session with this id |
| `--browser` | off | pass the browser capability through |
| `--timeout-seconds <seconds>` | no timeout | kill the provider process tree after this long |
| `--max-turns <count>` | provider default | cap Claude Code's turns |

```bash
lya executor --max-turns 1 "Summarise this repository without modifying any files"
```

The prompt is delivered on the child's stdin rather than on its command line, so large prompts work
on every platform. Resuming reuses the session reference the previous run returned.

Diagnostic only: no job state, no lock, no publication. Note that it *can* modify the repository —
that is Claude Code's job — so use `--max-turns` and a read-only prompt when exploring.

---

## `lya run`

```text
lya run [--project <path>] [--browser] [--max-iterations <count>] [--max-jobs <count>]
        [--publish] [--verbose | --json] <task>
```

Drives one autonomous job in the foreground: Supervisor decides, Executor works, Lya inspects the
repository itself, repeat. All non-flag arguments are joined into the task; a missing task is an
error.

```bash
lya run --project /path/to/project --max-iterations 5 "Fix a small regression and verify the result."
```

```powershell
lya run --project C:\Projects\Example --max-iterations 5 "Fix a small regression and verify the result."
```

Before the first provider call the project must exist, be a Git repository, and have a clean
working tree. It takes a [repository claim and a job lock](persistence-and-recovery.md#locks-and-claims),
repository first, and is refused if either is held elsewhere — including by a daemon.

`lya run` is attached to the terminal and ends with it. It is **never** silently redirected to a
daemon; [`lya submit`](#lya-submit) is how work reaches one.

**Interactive control.** On an interactive terminal it accepts `/help`, `/status`, `/diff`,
`/pause`, `/resume`, `/stop` and `/send <instruction>`. The typed-command reader is not started in
`--json` mode or when stdin is not a terminal, so `--json` stdout stays valid JSONL. Graceful
termination on the first `Ctrl+C` is always armed regardless of mode. See
[Autonomous jobs — interactive control](autonomous-jobs.md#interactive-control).

Exits `0` when the final job of the run reached a successful terminal status.

---

## `lya resume`

```text
lya resume [--job <job-id>] [--verbose | --json]
```

Continues a persisted job with a later process.

| Option | Meaning |
| --- | --- |
| `--job <job-id>` | which job to continue |
| `--verbose` / `--json` | output mode, exactly as `lya run` |

Without `--job`, resume proceeds only when exactly one job is resumable; otherwise it lists the
candidates and asks for an explicit choice. `lya jobs --resumable` shows the same list without
starting anything.

```bash
lya resume
lya resume --job job-1789250000-4242-0
```

Resumable statuses are `RUNNING`, `PAUSED`, `PUBLISHING`, `WAITING_CLAUDE_QUOTA` and
`WAITING_OPENAI_QUOTA`. `QUEUED` is deliberately not resumable — only the scheduler starts queued
work. `FAILED`, `STOPPED`, `PUBLISHED`, `ACCEPTED` and `WAITING_HUMAN` are terminal for automatic
recovery and are refused with the real reason.

Run options are **not** re-read from flags or the environment: each job persists its own project,
task, limits, browser flag and publication configuration, and resume uses those. See
[Persistence and recovery](persistence-and-recovery.md).

---

## `lya jobs`

```text
lya jobs [--resumable] [--json]
```

Lists persisted jobs, most recently updated first, then by job id. Strictly read-only: no state is
written, no legacy `state.json` is migrated, no lock is taken, no provider is invoked and no Git
command runs.

| Option | Meaning |
| --- | --- |
| `--resumable` | only the jobs `lya resume` would actually continue |
| `--json` | one JSON object on stdout; diagnostics on stderr |

The two flags compose.

```bash
lya jobs
lya jobs --resumable
lya jobs --json
```

```text
JOB                     PROJECT          STATUS                PHASE       ITER  UPDATED  RESUMABLE
job-1789250000-4242-0   example-service  WAITING_OPENAI_QUOTA  SUPERVISOR  3     4m ago   yes
job-1789240000-4242-0   example-website  ACCEPTED              PUBLISHER   1     2h ago   no
```

```json
{
  "jobs": [
    {
      "job_id": "job-1789250000-4242-0",
      "project_name": "example-service",
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

A job whose `state.json` is corrupt never disappears from the listing: healthy jobs are still
listed, unreadable ones are reported individually with their error in both output modes, and the
command exits non-zero. Nothing is repaired, archived or deleted.

---

## `lya scheduler`

```text
lya scheduler [<job-file>] [--resume-queued] [--max-concurrent <n>] [--browser]
              [--max-iterations <count>] [--max-jobs <count>] [--publish]
              [--verbose | --json]
```

Drives several autonomous jobs at once in the foreground. Different repositories may run
concurrently; the same repository never does.

| Option | Default | Meaning |
| --- | --- | --- |
| `<job-file>` | — | one [JSON-lines job file](scheduler.md#job-file); at most one may be given |
| `--resume-queued` | off | also start work a previous scheduler or daemon accepted but never began |
| `--max-concurrent <n>` | `2` | repositories driven at once; must be greater than zero |
| `--browser`, `--max-iterations`, `--max-jobs`, `--publish` | see [shared job options](#shared-job-options) | applied to every scheduled job |
| `--verbose` / `--json` | normal | output mode |

A job file or `--resume-queued` is required; with neither, the command reports what is missing. With
work described but nothing to do, it prints `Nothing to schedule.` and exits `0`.

```bash
lya scheduler jobs.jsonl --max-concurrent 3
lya scheduler --resume-queued
```

Scheduler mode is **non-interactive** — it accepts no typed commands, because multiplexing them
across concurrent jobs on one terminal needs an interaction model Lya does not have. Control one of
several concurrent jobs by naming it, with [`lya control`](#lya-control) under a daemon. The first
`Ctrl+C` stops new work being launched and requests the same graceful stop each active job would get
from a single `lya run`.

Full semantics in [Scheduler](scheduler.md).

---

## `lya daemon start`

```text
lya daemon start [--max-concurrent <n>] [--resume-interrupted] [--no-recover-queued]
```

Starts a daemon detached from this terminal — a new session on Unix, a detached process group on
Windows — and returns immediately.

| Option | Default | Meaning |
| --- | --- | --- |
| `--max-concurrent <n>` | `2` | repositories driven at once |
| `--resume-interrupted` | off | continue interrupted jobs on startup instead of parking them |
| `--no-recover-queued` | off | do not pick up work a previous daemon accepted and never started |

```text
Lya daemon started as process 4812 on \\.\pipe\lya-daemon-9f1c....
Log: /home/you/.lya/daemon/daemon.log
```

`--verbose` and `--json` are **refused** here: they describe how a foreground daemon narrates, and a
detached daemon narrates into a log file. Use [`lya daemon run`](#lya-daemon-run) for those.

Starting twice cannot produce two daemons for one `LYA_HOME`. This command checks first and reports
the daemon that is already running, exiting `0`:

```text
A Lya daemon is already running for /home/you/.lya as process 4812 on /home/you/.lya/daemon.sock.
```

The child is started with `LYA_HOME` pinned to the home this command resolved, so a daemon can never
adopt a different one. Its stdout and stderr go to `LYA_HOME/daemon/daemon.log`, which is also where
a startup failure explains itself.

---

## `lya daemon run`

```text
lya daemon run [--max-concurrent <n>] [--resume-interrupted] [--no-recover-queued]
               [--verbose | --json]
```

Runs the daemon in **this** process, attached to the terminal — the mode `lya daemon start` launches
detached. Intended for development and debugging.

Daemon-level lines go to stderr and the events of every driven job go to stdout, prefixed with the
job they belong to. The first `Ctrl+C` requests a graceful shutdown; a second forces termination.

When stderr is not a terminal, the narration is filtered to what is worth keeping in a long-lived
log — transient client traffic such as `lya daemon status` connections is dropped.

---

## `lya daemon status`

```text
lya daemon status [--json]
```

Reports whether a daemon is running for this `LYA_HOME` and what it is doing. `--json` is the only
accepted flag.

```text
Lya daemon

STATE          running
PROCESS        4812
LYA_HOME       /home/you/.lya
ENDPOINT       /home/you/.lya/daemon.sock
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

**Exits non-zero when no daemon is running**, printing the resolved home and the endpoint it would
use — in human or JSON form, on stdout. That is the intended way to test for a daemon.

---

## `lya daemon stop`

```text
lya daemon stop
```

Takes no arguments. Requests a graceful shutdown and waits until it has actually happened:

1. no further work is accepted — a submission arriving now is refused with `SHUTTING_DOWN`;
2. every active job receives the same graceful stop a foreground `Ctrl+C` requests, including
   provider process-tree cancellation, and parks itself at a safe boundary;
3. the daemon waits for those jobs;
4. it releases its claim on `LYA_HOME` and exits.

Success is reported only after step 4, and it is decided by polling the claim — never by watching
the endpoint, which stops answering at step 1 while the daemon is still draining jobs. That makes
`lya daemon stop && lya daemon start` correct even when active jobs take minutes to finish.

Stopping when nothing runs succeeds and says so, so the command is safe to repeat. Work that never
started stays `QUEUED` for the next daemon.

---

## `lya submit`

```text
lya submit [--project <path>] [--max-iterations <count>] [--max-jobs <count>]
           [--browser] [--publish] [--json] <task>
lya submit --file <job-file> [options]
```

Hands work to a running daemon. The work becomes ordinary persisted Lya jobs and flows through the
same scheduler `lya scheduler` uses.

| Option | Default | Meaning |
| --- | --- | --- |
| `--project <path>` | current directory | canonicalized **by the client**, so a relative path is never resolved against the daemon's working directory |
| `--file <job-file>` | — | the same [job file](scheduler.md#job-file) `lya scheduler` takes, parsed by the same parser |
| `--max-iterations`, `--max-jobs`, `--browser`, `--publish` | see [shared job options](#shared-job-options) | applied to every submitted job |
| `--json` | off | print the submission outcomes as JSON |

A task or `--file` is required, never both. `--project` does not apply to a job file, which names
its own projects.

```bash
lya submit "Fix the flaky test in tests/api.rs"
lya submit --project ../web --max-iterations 6 --publish "Update the changelog"
lya submit --file jobs.jsonl
```

```text
job-1763040000-4812-0  QUEUED  /home/you/projects/api
Watch one with: lya attach <job-id>
```

With `--publish`, the Git configuration is read from [`LYA_GIT_*`](configuration.md#git-publication)
and validated **in your shell** before anything is queued, then travels with the job. The daemon
never guesses at its own environment.

Every submitted job gets its own answer: a batch of ten with one unusable path queues nine and names
the one it refused. Rejections go to stderr, queued ids to stdout, and the command exits non-zero if
any job was not queued.

```text
-                      NOT QUEUED  could not resolve the repository at ../gone
job-1763040000-4812-0  QUEUED  /home/you/projects/api
```

Submission is **at-least-once**: a job id is returned only after that job exists on disk as
`QUEUED`, but a durably accepted batch whose response is lost looks like a failure. Re-running an
identical `lya submit` can therefore create duplicate jobs. Check with `lya daemon status` before
retrying — see [Daemon — delivery semantics](daemon.md#delivery-semantics).

---

## `lya attach`

```text
lya attach <job-id> [--replay] [--verbose | --json]
```

Streams one daemon-owned job's live events — the same objects `events.jsonl` and `lya run --json`
carry — rendered exactly as `lya run` renders them. Exactly one job id is required.

| Option | Meaning |
| --- | --- |
| `--replay` | show the job's recorded history before the live events |
| `--verbose` / `--json` | output mode, exactly as `lya run` |

```bash
lya attach job-1763040000-4812-0
lya attach job-1763040000-4812-0 --replay --json
```

Attach is **observational**. `Ctrl+C` detaches the viewer: the connection closes, the daemon forgets
it and the job continues untouched. Nothing about attaching can pause, stop or steer a job — that is
[`lya control`](#lya-control). Any number of viewers may watch one job, and a job with no viewers
runs exactly the same.

The replay is bounded to the most recent events, and an unparseable line — what a crash mid-write
leaves behind — is skipped rather than failing the attach.

---

## `lya control`

```text
lya control <job-id> <pause | resume | stop | status | diff | send <instruction>>
```

Sends one control command to one daemon-owned job, through that job's own existing control channel.
The command means exactly what the matching typed command means in an interactive `lya run`; there
is one control state machine and the daemon adds no second one.

```bash
lya control job-1763040000-4812-0 pause
lya control job-1763040000-4812-0 resume
lya control job-1763040000-4812-0 send "Also update the changelog"
lya control job-1763040000-4812-0 status
lya control job-1763040000-4812-0 diff
lya control job-1763040000-4812-0 stop
```

`pause`, `resume`, `stop`, `status` and `diff` accept no argument; `send` requires a non-empty
instruction, which is joined from the remaining arguments.

`status` and `diff` answer into the job's **event stream**, not into this command's output, because
that is where a job reports:

```text
Status requested; job-1763040000-4812-0 reports it in its events (lya attach job-1763040000-4812-0).
```

A request that cannot be delivered fails with a distinct reason, and never reports success:

| Situation | Reported as |
| --- | --- |
| No such persisted job | `UNKNOWN_JOB` |
| The job reached a terminal status | `JOB_TERMINAL` |
| The job is queued and has not started | `INVALID_FOR_STATE` |
| The job is live but this daemon is not driving it | `JOB_NOT_OWNED` |
| The command itself is unusable (empty or oversized instruction) | `INVALID_REQUEST` |

## Command interactions

* `lya run`, `lya resume` and `lya scheduler` are foreground and are **never** redirected to a
  daemon. Work reaches a daemon only through `lya submit`.
* Foreground and daemon-driven work exclude each other through the same operating-system claims, in
  both directions: neither can take a job or a repository the other holds. See
  [Persistence and recovery — locks and claims](persistence-and-recovery.md#locks-and-claims).
* `lya jobs` lists every persisted job whoever is driving it, and stays read-only.
* `lya submit`, `lya attach`, `lya control` and `lya daemon status`/`stop` are clients: they need a
  running daemon and report clearly when there is none.
* `lya resume` refuses `QUEUED` work, so resume can never bypass scheduler concurrency or a
  repository claim. Queued work is started by `lya scheduler --resume-queued` or by a daemon.
* `lya doctor`, `lya supervisor` and `lya executor` take no locks and persist nothing.
