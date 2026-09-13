# Troubleshooting

Start here:

```bash
lya doctor
lya jobs
lya daemon status
```

Those three are read-only and answer most questions: what the environment looks like, what state
exists, and whether something is already driving it.

* [Environment and prerequisites](#environment-and-prerequisites)
* [Starting a job](#starting-a-job)
* [Locks and claims](#locks-and-claims)
* [Jobs that stop or park](#jobs-that-stop-or-park)
* [Publication](#publication)
* [Daemon](#daemon)
* [Local agent mode](#local-agent-mode)
* [Output and scripting](#output-and-scripting)
* [Reading the logs](#reading-the-logs)
* [Reporting a problem](#reporting-a-problem)

## Environment and prerequisites

### `lya run --help` reports an unknown option instead of printing help

Expected. `--help`, `-h`, `--version` and `-V` are recognised only as the **first** argument,
because the first argument is what selects a subcommand. `lya run --help` therefore stays a
`lya run` invocation, and `run`'s parser reports the unknown option together with `run`'s own usage
line — which is the per-command help. `lya --help` on its own prints the command list.

### `context.md     MISSING`

Every orchestration command requires `LYA_HOME/context.md`. Create it:

```bash
mkdir -p ~/.lya && printf 'Standing project context for the Supervisor.\n' > ~/.lya/context.md
```

```powershell
New-Item -ItemType Directory -Force "$HOME\.lya" | Out-Null
Set-Content -Encoding utf8 "$HOME\.lya\context.md" "Standing project context for the Supervisor."
```

If the report says `context.md     ERROR  private context is not valid UTF-8`, rewrite the file as
UTF-8. PowerShell's `Set-Content` without `-Encoding utf8` can produce something else.

### `codex     MISSING` or `claude     MISSING`

The executable is not on `PATH` under that name. Either add it, or point Lya at it:

```bash
export LYA_CODEX_BIN=/opt/codex/bin/codex
export LYA_CLAUDE_BIN="$HOME/.local/bin/claude"
```

```powershell
$env:LYA_CODEX_BIN = "C:\Tools\codex\codex.exe"
$env:LYA_CLAUDE_BIN = "$env:LOCALAPPDATA\Programs\claude\claude.exe"
```

Re-run `lya doctor` — it prints the resolved path, so it confirms the override took effect.

### The provider fails immediately with an authentication error

Lya performs no sign-in. Authenticate each tool through its own flow and confirm it works standalone
before asking Lya to drive it:

```bash
codex exec --sandbox read-only "Say OK"
claude --print "Say OK"
```

Lya needs no `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`, and removes both from the provider child
environments. If you rely on a key rather than a signed-in session, that is why the provider does not
see it — see [Security — provider credentials](security.md#provider-credentials).

### `could not determine a home directory; set LYA_HOME explicitly`

Neither `USERPROFILE`/`HOMEDRIVE`+`HOMEPATH` (Windows) nor `HOME` (Unix) is usable. Set `LYA_HOME`
yourself.

## Starting a job

### `refusing to start job because the working tree is not clean`

Deliberate: pre-existing changes must not be confusable with agent-generated work. Commit or stash
first.

```bash
git status --short
git stash --include-untracked
```

### `project path is not a Git repository`

`--project` must point into a Git working tree. Lya walks up to the nearest ancestor holding a
`.git` entry, so a subdirectory is fine — an unrelated directory is not.

### `could not resolve project path`

The path does not exist, or is not reachable. It is canonicalized by the command that reads it, so
relative paths resolve against **your current directory** — for `lya submit` too, deliberately, so a
daemon never reinterprets them.

### `a task is required`

Every non-flag argument becomes part of the task, so this means only flags were given. Quote a task
containing shell metacharacters.

### `--verbose cannot be combined with --json`

Pick one output mode.

## Locks and claims

### `job <id> is already being driven by another Lya process`

Another `lya run`, `lya resume`, `lya scheduler` or daemon holds that job's lock. Find out which:

```bash
lya daemon status
lya jobs
```

The claim is the operating system's lock on an open handle, so it is released when that process
exits — including a crash or a kill. **Do not delete `lock.json`**: a released lock keeps its file on
purpose, and deleting a locked path lets a second process lock a fresh file under the same name and
believe it owns the job.

If a daemon owns it, stop the daemon or steer the job by name:

```bash
lya control <job-id> stop
lya daemon stop
```

### `repository ... is already being driven by another Lya process`

A different job holds that repository's claim. Only one driver per working tree is allowed, by
design. Wait for it, or stop it. Same rule about not deleting the claim file.

Two jobs in two *different* repositories never contend — check that both really resolve to different
repository roots, remembering that symlinks and `.`/`..` are canonicalized away.

## Jobs that stop or park

### `WAITING_CLAUDE_QUOTA` / `WAITING_OPENAI_QUOTA`

The provider reported an exhausted quota. The job is parked and resumable; only the operation that
had not completed is retried, and the iteration counter is not advanced again.

```bash
lya jobs --resumable
lya resume --job <job-id>
```

Lya never falls back to a paid API to work around a subscription quota.

If a job parks on a quota that you believe is not exhausted, look at the recorded classification: a
`PROVIDER_MESSAGE_HEURISTIC` classification came from message text rather than a structured provider
signal, because the CLI documents no machine-readable one.

### `WAITING_HUMAN`

Either the Supervisor decided a person must choose, or Lya refused to guess. It is **not**
automatically resumable — read the recorded reason first:

```bash
lya jobs --json
```

Common reasons:

* the repository changed while Lya was not running;
* a pending operation without the state it needs;
* a quota wait naming a different operation than the one actually owed;
* a commit exists that no authoritative state describes.

Resolve the repository situation yourself, then start new work. Lya will not reset or rewrite history
to recover.

### `lya resume` says the job is not resumable

`QUEUED` work is never resumable — only a scheduler or daemon starts it:

```bash
lya scheduler --resume-queued
```

`FAILED`, `STOPPED`, `PUBLISHED`, `ACCEPTED` and `WAITING_HUMAN` are terminal for automatic
recovery. The message names the real status rather than claiming the job is unknown.

### `lya resume` without `--job` refuses to choose

Resume proceeds automatically only when exactly one job is resumable. It lists the candidates
otherwise:

```bash
lya jobs --resumable
lya resume --job <job-id>
```

### A job reached its iteration limit

Raise the bound for the next run; it is per job, not global:

```bash
lya run --max-iterations 20 "..."
```

### `lya jobs` exits non-zero with "persisted job(s) could not be read"

At least one `state.json` is corrupt. Healthy jobs are still listed, and each unreadable one is
reported with its error. Lya never modifies, repairs or archives them — that is your call. The job
directory is `LYA_HOME/jobs/<job-id>/`.

### Typed commands do nothing

`/status`, `/pause` and friends are read only from an interactive human-mode terminal. They are not
available in `--json` mode, in redirected output, or under `lya scheduler`. Graceful termination on
the first `Ctrl+C` works in every mode. For concurrent jobs, name the job:
`lya control <job-id> pause`.

## Publication

### `LYA_GIT_NAME must be configured` (or `EMAIL` / `BRANCH`)

`--publish` requires an explicit identity and branch. See
[Configuration — Git publication](configuration.md#git-publication). The check runs **before** the
job starts, so nothing has happened yet.

### Publication refused because the repository changed

Expected. The reviewed snapshot is compared against reality after the decision, and the staged
content is verified again before the commit. If anything else wrote to the working tree in between,
Lya refuses rather than committing something nobody reviewed.

### The push was rejected

Publication stops. Lya does not pull, merge, rebase or force-push. Resolve the divergence yourself,
then start new work.

### The configured branch is not the current branch

Lya does not switch branches. Check out the branch named by `LYA_GIT_BRANCH` first.

### A commit exists but the job parked

The commit and its authoritative record are two separate writes, and a crash can land between them.
Lya detects that `HEAD` moved without a record and parks the job in `WAITING_HUMAN` rather than
creating a second commit. Inspect `git log -1` and decide; see
[Git publication — recording the commit](git-publication.md#recording-the-commit).

### Push authentication fails

Lya holds no credentials — push authentication is entirely your machine's Git configuration. Verify
it directly:

```bash
git push --dry-run <remote> <branch>
```

## Daemon

### `lya daemon status` exits non-zero

That is how "no daemon is running" is reported, so a script can test for one. The output still names
the resolved home and the endpoint it would use.

### `lya submit` / `attach` / `control` say no daemon is running

Start one, and confirm both commands resolve the **same** `LYA_HOME` — two homes are two independent
daemons:

```bash
lya daemon start
lya daemon status
```

### `lya daemon start` says a daemon is already running

One daemon per `LYA_HOME`, enforced by an operating-system claim. This exits `0` and names the
process and endpoint. Use a different `LYA_HOME` if you genuinely want a second one.

### The daemon did not become reachable

The child was started but never answered. Its startup failure is in the log:

```bash
tail -n 50 ~/.lya/daemon/daemon.log
```

```powershell
Get-Content "$HOME\.lya\daemon\daemon.log" -Tail 50
```

### `lya daemon stop` reports that jobs are still shutting down

Active jobs park at a safe boundary, which can take as long as a provider invocation. The command
polls the claim on `LYA_HOME`, not the endpoint, so once it prints `Lya daemon stopped.` the next
`lya daemon start` will work. Repeat it if it timed out; it is safe to repeat.

### `lya submit` failed but the job seems to be running

Submission is [at-least-once](daemon.md#delivery-semantics). A durably accepted batch whose response
was lost looks like a failure. **Check before retrying**, or you will duplicate the work:

```bash
lya daemon status
```

### `lya attach` detached me by itself

```text
Detached from <job-id>: the client fell behind by 128 event(s)
```

The viewer stopped reading fast enough and was disconnected on its own; the job was unaffected.
Re-attach, and avoid piping a live attach into something slow.

### A control command was refused

Each refusal has a distinct reason — `UNKNOWN_JOB`, `JOB_TERMINAL`, `INVALID_FOR_STATE` (queued, not
started), `JOB_NOT_OWNED` (live but driven by someone else), `INVALID_REQUEST` (empty or oversized
instruction). It never reports success for something it did not deliver.

### An instruction was rejected

One job holds at most 16 active instructions totalling 8 KiB. The refusal is explicit and recorded
as `USER_INSTRUCTION_REJECTED`; nothing is dropped silently.

## Local agent mode

### `OLLAMA_MODEL must name the model to query.`

Set it. Also check `OLLAMA_BASE_URL` if Ollama is not on `http://127.0.0.1:11434/v1`.

### `Could not configure runtime: could not create workspace`

You are running a prebuilt binary without `LYA_WORKSPACE`. The fallback workspace path is baked in at
compile time and points at the build machine. Set it to a directory that exists:

```bash
mkdir -p ~/lya-workspace && export LYA_WORKSPACE=~/lya-workspace
```

```powershell
New-Item -ItemType Directory -Force "$HOME\lya-workspace" | Out-Null
$env:LYA_WORKSPACE = "$HOME\lya-workspace"
```

### The model never calls a tool

The model must support tool calling. Try a model documented to do so.

## Output and scripting

* `--json` puts **only** job-event objects on stdout; diagnostics go to stderr. Do not parse stderr.
* Under `lya scheduler --json`, job events carry `event` and scheduler events carry
  `scheduler_event`. Branch on which field is present.
* Exit codes are meaningful for every command — see
  [CLI reference — exit codes](cli-reference.md#exit-codes).
* `lya jobs --json` is the read-only way to inspect state from a script; it takes no locks and runs
  no Git command.

## Reading the logs

```text
LYA_HOME/jobs/<job-id>/events.jsonl   one job's full history
LYA_HOME/daemon/events.jsonl          the daemon's own history
LYA_HOME/daemon/daemon.log            a detached daemon's narration and startup failures
```

Replay a daemon-owned job's history and then follow it live:

```bash
lya attach <job-id> --replay
```

Remember that logs are observability, never authority: Lya does not re-derive a decision from them,
and neither should you. Authoritative state is `LYA_HOME/jobs/<job-id>/state.json`.

Event logs can contain prompts, model responses, file paths and commit titles — treat them as
potentially sensitive, and redact before pasting them into an issue.

## Reporting a problem

Open an issue with:

* the exact command, and the exact error text;
* `lya doctor` output;
* `lya jobs --json` for the job involved, redacted as needed;
* the relevant slice of `events.jsonl`, redacted as needed;
* your operating system, and whether you used a release archive or a source build.

**Do not** paste `context.md`, credentials, private paths or private repository content. If the
problem is a security vulnerability, follow [SECURITY.md](../SECURITY.md) instead of opening a public
issue.
