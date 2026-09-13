# Scheduler

`lya scheduler` drives several autonomous jobs at once in the foreground, under two rules:

* different repositories may run concurrently;
* the same repository is never driven concurrently.

The scheduler is a boundary *above* the orchestrator, not a replacement for it. Each scheduled
request becomes a normal Lya job with its own job id, `state.json`, `lock.json` and `events.jsonl`,
and goes through exactly the same [autonomous loop](autonomous-jobs.md), the same repository
verification and the same [guarded publication](git-publication.md) as a single `lya run`.

* [Job file](#job-file)
* [Running it](#running-it)
* [Concurrency and fairness](#concurrency-and-fairness)
* [Queued work survives a crash](#queued-work-survives-a-crash)
* [Sequential chains](#sequential-chains)
* [Output](#output)
* [Stopping](#stopping)
* [Why typed commands are not multiplexed](#why-typed-commands-are-not-multiplexed)

## Job file

Work is described by a small JSON-lines file: one JSON object per line.

```jsonl
# comments and blank lines are ignored
{"project": "/path/to/service", "task": "Fix the flaky integration test"}
{"project": "../website", "task": "Update the changelog", "name": "Website"}
```

| Field | Required | Meaning |
| --- | --- | --- |
| `project` | yes | path to the Git working tree; a relative path resolves against the job file's **own** directory, so the file is portable with the paths it names |
| `task` | yes | the task text, exactly as `lya run` would take it |
| `name` | no | display name; defaults to the directory name |

Unknown fields are **refused** rather than ignored, so a typo is reported instead of silently
dropping an instruction. An empty `project` or `task` is refused with its line number, as is a path
that cannot be resolved. A job file holds at most **256** jobs.

It is deliberately not a configuration language: it exists so several project/task pairs can be
given in one invocation without inventing shell quoting rules for multi-line tasks.

The same file and the same parser are used by [`lya submit --file`](daemon.md#submitting-work), so
one grammar has one implementation.

## Running it

```bash
lya scheduler jobs.jsonl --max-concurrent 3
lya scheduler --resume-queued
lya scheduler jobs.jsonl --resume-queued --publish --json
```

A job file or `--resume-queued` is required. `--browser`, `--max-iterations`, `--max-jobs`,
`--publish`, `--verbose` and `--json` mean exactly what they mean for `lya run` and apply to every
scheduled job. Full syntax in the [CLI reference](cli-reference.md#lya-scheduler).

With work described but nothing to do, the scheduler prints `Nothing to schedule.` and exits `0`.

## Concurrency and fairness

```text
--max-concurrent <n>
```

defaults to **2** and must be greater than zero.

Exactly `n` execution slots exist; Lya does not spawn a task per job and then gate provider calls.
Ordering is FIFO and deterministic: a worker takes the first queued job whose repository is free. A
job whose repository is busy is skipped rather than allowed to hold a slot, so one contended
repository never stalls unrelated work.

One job failing never cancels an unrelated job — every job keeps its own outcome, and the scheduler
exits non-zero if any of them failed or was rejected.

Repository identity comes from the repository's real location, not a display name; see
[Persistence and recovery — repository claims](persistence-and-recovery.md#repository-claims).

## Queued work survives a crash

Accepted work is durable before it starts: every request is persisted as a real job with status
`QUEUED`. That keeps four situations distinguishable after an interruption:

| Situation | How it looks |
| --- | --- |
| never started | `QUEUED` |
| currently being driven | `RUNNING`/`PUBLISHING`/… with a live job lock |
| resumable interrupted work | the same statuses with no live lock |
| terminal | `ACCEPTED`, `PUBLISHED`, `FAILED`, `STOPPED`, `WAITING_HUMAN` |

`QUEUED` is deliberately **not** resumable. `lya resume` refuses it, so ordinary resume can never
bypass a repository claim or the concurrency bound. Only a scheduler or a daemon starts queued work:

```bash
lya scheduler --resume-queued
```

Re-queued jobs keep their original job identity and adopt the options of the invocation that picks
them up. A job that fails before the orchestrator's first write is recorded as `FAILED` rather than
left advertised as queued, so the durable record and the scheduler report always agree.

## Sequential chains

A [`next_prompt` chain](autonomous-jobs.md#sequential-jobs) stays owned by one scheduled root. The
worker holds that repository's claim for the whole chain instead of releasing and reacquiring it
between chained jobs, so a chain never loses its repository halfway through, and a chain does not
consume one global slot per child.

Every sequential child still takes its own job lock, still counts against `--max-jobs`, and still
records its own position in the chain.

## Output

Scheduler-wide observation is a separate structured stream from job events, so job events stay
exactly what they were. Scheduler events cover started, queued, waiting for repository, job started,
completed, failed, not started, stopping and finished.

Human output prefixes every job line with its project and job, so an interleaved terminal stream
stays readable:

```text
01:29:10  SCHEDULER  2 job(s) queued; at most 2 repositories at a time
01:29:10  SCHEDULER  started service job-1789262950-23120-0
[service job-1789262950-23120-0] 01:29:11  SUPERVISOR  CLAUDE
      The flaky test has no isolation between cases
      Claude: Serialise the shared fixture setup and re-run the suite
[website job-1789262950-23120-1] 01:29:11  CLAUDE
      starting requested execution
```

`--json` keeps stdout machine-readable: one JSON object per line and nothing else. Job events carry
an `event` field and scheduler events a `scheduler_event` field, so the two never have to be told
apart by guessing.

```json
{"timestamp_unix_millis":1789262921937,"scheduler_event":"SCHEDULER_STARTED","max_concurrent":2,"queued":2}
```

Each job still keeps its own `events.jsonl`, and neither stream is ever authority.

## Stopping

The first `Ctrl+C` stops the scheduler launching new work and requests the same graceful stop every
active job would receive from a single `lya run`, including
[provider process-tree cancellation](autonomous-jobs.md#provider-process-tree-cancellation). The
scheduler then waits for those jobs to shut down in a controlled way. Jobs that never started stay
`QUEUED`. A second `Ctrl+C` keeps its force-exit meaning.

## Why typed commands are not multiplexed

Scheduler mode is **non-interactive**. `/pause`, `/resume`, `/status`, `/diff`, `/send` and `/stop`
act on one unambiguous job, and multiplexing typed commands across several simultaneous jobs on one
terminal needs an interaction model Lya does not have. Rather than invent one casually,
`lya scheduler` accepts no typed commands.

Controlling one of several concurrent jobs is answered by naming it, in [daemon
mode](daemon.md#controlling-a-job):

```bash
lya control <job-id> pause
```

`lya run` keeps the full interactive control it has always had, and graceful termination works in
both.

## Scheduler or daemon?

| | `lya scheduler` | [daemon](daemon.md) |
| --- | --- | --- |
| Lifetime | attached to your terminal, ends with it | survives the shell that started it |
| Adding work while running | no — the job set is fixed at start | yes, `lya submit` |
| Watching one job | interleaved on one terminal | `lya attach <job-id>` |
| Steering one job | not possible | `lya control <job-id> ...` |
| Concurrency model | identical | identical |
| Job semantics | identical | identical |

The daemon runs the same scheduler. Choose the scheduler for a bounded batch you want to watch to
completion, and the daemon for work that should outlive the terminal.
