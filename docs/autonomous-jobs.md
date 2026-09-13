# Autonomous jobs

An autonomous job is one task driven by a Supervisor/Executor loop, with Lya inspecting the
repository itself between turns. `lya run` drives one job in the foreground;
[`lya scheduler`](scheduler.md) and the [daemon](daemon.md) drive the same jobs at a higher level.

* [The loop](#the-loop)
* [Starting a job](#starting-a-job)
* [Iterations and limits](#iterations-and-limits)
* [Job statuses](#job-statuses)
* [Live output](#live-output)
* [Interactive control](#interactive-control)
* [User instructions](#user-instructions)
* [Provider process-tree cancellation](#provider-process-tree-cancellation)
* [Quota waiting](#quota-waiting)
* [Repository review](#repository-review)
* [Sequential jobs](#sequential-jobs)
* [Event stream](#event-stream)

## The loop

```text
Task
 ↓
Supervisor  ──►  CLAUDE / ACCEPT / HUMAN / STOP
 ↓ (CLAUDE)
Executor
 ↓
Repository inspection, by Lya
 ↓
Supervisor
 ↓
...
```

One **iteration** is one Supervisor review plus the optional Executor invocation that review asked
for. The Supervisor decides; it never commits. The Executor works; it never owns publication. Lya
collects repository state itself rather than trusting the Executor's report, and the Supervisor is
told to prefer that state over any claim in a report.

The four decisions:

| Decision | Meaning |
| --- | --- |
| `CLAUDE` | send more work to the Executor |
| `ACCEPT` | the work is accepted; a commit title is recorded, and published if `--publish` is on |
| `HUMAN` | a person has to decide; the job parks in `WAITING_HUMAN` |
| `STOP` | stop this job intentionally |

Claude sessions are resumed across correction cycles, so the Executor keeps the context of its own
previous work within a job.

## Starting a job

```bash
lya run --project /path/to/project --max-iterations 5 "Fix a small regression and verify the result."
```

```powershell
lya run --project C:\Projects\Example --max-iterations 5 "Fix a small regression and verify the result."
```

Before any provider call, the project must:

* exist;
* be a Git repository;
* have a clean working tree.

Lya refuses to start autonomous work on an already dirty repository, so pre-existing changes cannot
be confused with agent-generated work.

The job also takes a [repository claim and a job lock](persistence-and-recovery.md#locks-and-claims)
— repository first, job second — so a manual run and a daemon-driven job can never drive one working
tree at the same time.

`lya run` is attached to the terminal that started it and ends with it. To hand the same work to a
background daemon, use [`lya submit`](daemon.md#submitting-work).

## Iterations and limits

| Limit | Flag | Default |
| --- | --- | --- |
| Supervisor reviews per job | `--max-iterations <count>` | 10 |
| Jobs per sequential chain | `--max-jobs <count>` | 10 |
| Active user instructions per job | — | 16, totalling at most 8 KiB |

The Supervisor invocation has a 120-second timeout. The Executor invocation inside an autonomous job
has **no** timeout — a long Claude Code run is allowed to finish. `lya executor` can impose one with
`--timeout-seconds` for diagnostics.

## Job statuses

| Status | Meaning | Resumable |
| --- | --- | --- |
| `QUEUED` | accepted by a scheduler or daemon, never started | no — only a scheduler starts it |
| `RUNNING` | being driven, or interrupted mid-flight | yes |
| `PAUSED` | parked at a safe boundary on request | yes |
| `PUBLISHING` | inside the guarded publication sequence | yes |
| `WAITING_CLAUDE_QUOTA` | Executor quota exhausted | yes |
| `WAITING_OPENAI_QUOTA` | Supervisor quota exhausted | yes |
| `WAITING_HUMAN` | a person must decide, or state was ambiguous | no |
| `ACCEPTED` | accepted without publication | no |
| `PUBLISHED` | accepted, committed and pushed | no |
| `FAILED` | ended in an error | no |
| `STOPPED` | stopped on request | no |

`ACCEPTED` and `PUBLISHED` are successful outcomes for the process exit code; the other terminal
statuses are not. One definition is shared by `lya run`, `lya scheduler` and the daemon, so they
cannot disagree about the same status.

## Live output

`lya run` renders the job's event stream live in a compact human-readable form: Supervisor
decisions, the explicit prompts Lya sends the Executor, the Executor's final report, concise
repository summaries, waiting and failure states, and publication progress. ANSI colour is used only
when stdout is an interactive terminal, so redirected output stays readable text.

```bash
lya run --project /path/to/project "Fix a small regression and verify the result."
```

`--verbose` adds the full safe Supervisor review request, explicit structured decision fields, the
complete Executor response, detailed repository metadata and publication details:

```bash
lya run --verbose --project /path/to/project "Fix a small regression and verify the result."
```

`--json` makes stdout machine-readable: only job-event objects, one per line, with diagnostics on
stderr. `--verbose` and `--json` cannot be combined.

```bash
lya run --json --project /path/to/project "Fix a small regression and verify the result."
```

What the stream records is the exchange Lya is legitimately allowed to know: the safe review request
it sent, the Supervisor's structured decision and reason, the prompt it sent the Executor, the
Executor's final response, and the repository state it then collected. Hidden model reasoning is not
available to Lya and is never claimed or logged.

Lya does not log a model identifier. Both provider CLIs support model selection, but the structured
contracts Lya relies on do not reliably report which model executed a request, and Lya will not
invent one.

## Interactive control

When `lya run` is attached to an interactive terminal it accepts line-oriented commands while the
job runs. The terminal stays a normal scrolling log; there is no fullscreen interface.

```text
/help
/status
/diff
/pause
/resume
/stop
/send <instruction>
```

| Command | Effect |
| --- | --- |
| `/status` | reports authoritative live job state: job and project, phase, iteration, known Claude session, publication progress, pending pause/stop requests |
| `/diff` | read-only repository capture — tracked paths, untracked paths, a concise diff stat; changes nothing |
| `/pause` | records the request immediately, enters `PAUSED` only at a safe boundary |
| `/resume` | continues from that exact boundary, without repeating a completed provider invocation |
| `/stop` | prevents new Supervisor, Executor and publication actions, then shuts down |
| `/send` | queues an instruction for the next safe model turn |

`/pause` lets a running provider invocation, repository capture or Git operation finish its current
safe operation first. While paused, `/status`, `/diff`, `/send`, `/resume` and `/stop` all remain
available.

`/stop` cancels an active provider's whole process tree, reaps Lya's own child, and records a
terminal `STOPPED` state after a best-effort repository capture. It does **not** reset working-tree
changes made before the stop. During publication, a stop is observed before each guarded stage, and
no later stage is begun after the request is seen.

The typed-command reader is only started for an interactive human-mode terminal. `--json` never
starts one, so its stdout stays valid JSONL, and a redirected non-TTY run accepts no typed commands.
**Graceful termination is always armed** — every mode routes the first termination signal through
the same stop semantics.

If Lya's input simply ends while a job is paused, nobody asked to stop: the job stays `PAUSED` and
remains resumable. Only an explicit `/stop` or `Ctrl+C` reaches the terminal `STOPPED` state.

Scheduler and daemon-driven jobs accept no typed commands on a shared terminal. Name the job
instead, with [`lya control <job-id> ...`](daemon.md#controlling-a-job).

## User instructions

`/send <instruction>` (and `lya control <job-id> send ...`) queues the complete instruction in order,
acknowledges it immediately, persists it in job state and applies it at the next safe model turn.
Applied instructions are included in the relevant Supervisor review and Executor prompts. Lya never
tries to inject text into a model process that is already generating.

An instruction is a constraint **for the rest of the current job**. It is not one-turn-only, and it
is never carried into a sequential next job, which starts from its own task alone.

To keep prompts bounded, one job holds at most **16 active instructions** totalling at most
**8 KiB**. An instruction beyond either limit is refused explicitly, with the reason reported and
recorded as a `USER_INSTRUCTION_REJECTED` event. Nothing is dropped silently.

## Provider process-tree cancellation

Codex and Claude Code start helper processes of their own, so cancelling only the process Lya spawned
would leave those helpers running. Every provider process is claimed by the operating system when it
starts, and a cancellation or a timeout terminates the claim as a unit:

```text
Windows   Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
Unix      dedicated process group, signalled with killpg
```

The direct child is still killed and reaped afterwards, so a cancelled provider never becomes a
zombie, and captured output finishes instead of waiting on a descendant that inherited the pipe. A
timeout and a user cancellation stay distinct outcomes; both clean up the same way.

On Windows the claim also covers Lya's own exit: the job is closed when Lya's handle goes away, so a
second `Ctrl+C` cannot leave a provider tree behind. Anything a provider starts in the microseconds
between the spawn and the assignment is outside the job — Windows offers no way to assign a job to a
process that is already running.

On Unix the provider no longer shares Lya's foreground process group, so a terminal `Ctrl+C` reaches
Lya alone and the first interrupt stays a graceful stop Lya controls.

The first `Ctrl+C` follows the graceful stop path and prints a second-press warning. A second
`Ctrl+C` force-terminates Lya after the cancellation signal has already been sent to active
children.

## Quota waiting

A provider quota is a parked, resumable state rather than a dead end. Lya records which provider was
exhausted, the operation that still needs to run, how the condition was classified, and the reported
reason.

Classification prefers documented structured provider information. Where a CLI documents no
machine-readable quota signal, Lya falls back to a message heuristic and records that explicitly as
`PROVIDER_MESSAGE_HEURISTIC`, so a quota decision never looks more precise than it is.

A quota is recognised once, at the provider boundary, and only in text the provider produced as a
diagnostic: the CLI's standard error, and the structured envelope's own fields on a run the CLI
flagged as an error. The Executor's answer to the task is never classified, and no decision is
re-derived from a rendered error message — which also carries the task, the prompt and that answer.
A job *about* rate limiting whose run fails for an unrelated reason therefore stays a normal failure
instead of parking as an exhausted quota.

`lya resume` retries only the operation that had not completed. The iteration counter is not
advanced again, and a model action that already finished is never repeated.

**Lya never falls back to a paid API to work around a subscription quota.**

## Repository review

Lya collects repository state itself before every review:

```text
HEAD
git status --short
git diff --stat
changed files
tracked diff
untracked files
```

Untracked files are included explicitly rather than being represented only by `git status`. For
review safety Lya records paths, file sizes, UTF-8 content where appropriate, Git blobs,
binary/non-UTF-8 markers and explicit truncation markers.

Large repository data is bounded before it is sent to the Supervisor, and truncation is always
reported explicitly rather than hidden. Binary or insufficiently reviewed states cannot be published
automatically.

## Sequential jobs

An accepted job may carry a `next_prompt`. When publication is enabled and succeeds, Lya can use it
to start another job:

```text
Job 1  ──►  PUBLISHED  ──►  next_prompt  ──►  Job 2  ──►  PUBLISHED  ──►  ...
```

A new job starts only after the previous job was accepted, publication succeeded, the push
succeeded, and the working tree is clean. The chain is bounded by `--max-jobs` (default 10).

Every job records its own position in the chain in its first authoritative write, before it does any
work, so a job resumed after a crash still counts against the original `--max-jobs` budget instead
of restarting the count.

Without `--publish`, `next_prompt` is retained and displayed but starts nothing automatically.

Under the scheduler, a chain stays owned by one scheduled root: the worker holds that repository's
claim for the whole chain rather than releasing it between children, so a chain never loses its
repository halfway through and does not consume one global slot per child.

## Event stream

Every job appends structured JSON Lines to:

```text
LYA_HOME/jobs/<job-id>/events.jsonl
```

Each line is an independently useful event with a millisecond timestamp, job and project identity,
an optional iteration, and event-specific fields under a `event` discriminator:

```json
{"timestamp_unix_millis":1789262921937,"job_id":"job-1789262921-4242-0","project_name":"example-service","project_path":"/home/you/projects/example-service","iteration":1,"event":"SUPERVISOR_FINISHED","action":"CLAUDE","reason":"The test is still failing","prompt":"Run the failing test and fix the race","commit_title":null,"next_prompt":null}
```

The event kinds are:

```text
JOB_STARTED                 JOB_FINISHED
SUPERVISOR_STARTED          SUPERVISOR_FINISHED
EXECUTOR_STARTED            EXECUTOR_FINISHED
REPOSITORY_CAPTURED
PUBLISH_STARTED             PUBLISH_STAGE_CHANGED      PUBLISHED
WAITING_FOR_QUOTA           WAITING_FOR_HUMAN
PAUSE_REQUESTED             PAUSED                     RESUMED
STOP_REQUESTED              STOPPED
USER_INSTRUCTION_QUEUED     USER_INSTRUCTION_APPLIED   USER_INSTRUCTION_REJECTED
RESUME_STARTED              RESUME_VALIDATED           RESUME_REJECTED
QUOTA_RETRY_STARTED
STATUS_REPORTED             DIFF_REPORTED              CONTROL_MESSAGE
FAILED
```

The file is appended and synced after each event and is never rewritten as a whole. Event
persistence is **required**: if Lya cannot write or sync an event it stops the job before the next
Supervisor, Executor or Git action and reports the error.

#### `EXECUTOR_FINISHED` and `total_cost_usd`

`EXECUTOR_FINISHED` carries the fields Claude Code reports in its own JSON envelope, including
`total_cost_usd`. That number is **provider metadata, preserved verbatim**. Lya does not compute it,
does not aggregate it, and has no knowledge of how the provider CLI is authenticated or billed — it
[removes `ANTHROPIC_API_KEY` and `OPENAI_API_KEY`](security.md#provider-credentials) from provider
child environments, so an authenticated subscription session is the normal case.

So the field is **not evidence that anything was charged, and not evidence that nothing was**. Lya
will not claim either. `lya run --verbose` and `lya attach --verbose` say so where a person reads it:

```text
      Claude-reported cost metadata: $0.0794 (not proof of billing)
```

Authentication and billing belong to the provider CLI and your account with that provider. Check
them there, not here.

#### Encoding

`events.jsonl` is plain UTF-8 with no byte-order mark, which is what JSON requires and what every
JSON reader expects. Provider text is stored exactly as the provider produced it — non-ASCII
characters are neither escaped nor rewritten.

On Windows that matters when you read a log by hand. Windows PowerShell 5.1's `Get-Content` decodes
a file without a byte-order mark using the legacy ANSI codepage, so an em dash arrives as `â€”` and an
arrow as `â†’`. The file is fine; the reader needs telling:

```powershell
Get-Content -Encoding UTF8 "$HOME\.lya\jobs\<job-id>\events.jsonl"
```

PowerShell 7 and `lya attach` both decode it correctly without help.

The log is observability, never authority. It is not parsed to decide whether Git may write, so a
malformed older log cannot weaken publication verification. The same event model is independent of
terminal rendering, which is why `lya attach` can replay and stream it unchanged.

Event logs are local, but they can contain prompts, model responses, file paths, repository metadata
and commit titles. Treat them as potentially sensitive. Lya never logs child-process environment
variables or credentials, and the `context.md` body is deliberately omitted from the Supervisor
request event.
