# Daemon

Autonomous work does not need your terminal to stay open. A Lya daemon owns the scheduler, the
running jobs, the queued jobs, the repository coordination and a live local control endpoint. It
survives the shell that started it, and other `lya` commands become clients of it.

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
exists and there is nothing to authenticate over a network. See [Security](security.md).

* [Starting and stopping](#starting-and-stopping)
* [Submitting work](#submitting-work)
* [Delivery semantics](#delivery-semantics)
* [Attaching and detaching](#attaching-and-detaching)
* [Controlling a job](#controlling-a-job)
* [Restart recovery](#restart-recovery)
* [Ownership and exclusion](#ownership-and-exclusion)
* [Existing commands are unchanged](#existing-commands-are-unchanged)
* [The local protocol](#the-local-protocol)
* [Observability](#observability)

## Starting and stopping

```bash
lya daemon start
```

starts Lya detached — a new session on Unix, a detached process group on Windows — and returns
immediately:

```text
Lya daemon started as process 4812 on \\.\pipe\lya-daemon-9f1c....
Log: /home/you/.lya/daemon/daemon.log
```

Starting twice cannot produce two daemons for one `LYA_HOME`. The second is refused by the
[daemon claim](persistence-and-recovery.md#the-daemon-claim) before it binds anything, and
`lya daemon start` checks first so the common case reports the daemon that is already running rather
than a failure from a child nobody can see:

```text
A Lya daemon is already running for /home/you/.lya as process 4812 on /home/you/.lya/daemon.sock.
```

| Option | Meaning |
| --- | --- |
| `--max-concurrent <n>` | how many repositories may be driven at once (default 2) |
| `--resume-interrupted` | continue interrupted jobs on startup; off by default, see [Restart recovery](#restart-recovery) |
| `--no-recover-queued` | do not pick up work a previous daemon accepted and never started |

`--verbose` and `--json` apply to `lya daemon run`, not to `lya daemon start`, and are refused there:
they describe how a foreground daemon narrates, and a detached daemon narrates into a log file.

```bash
lya daemon status
```

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

`lya daemon status --json` prints the same information as one JSON object. The command **exits with
a failure when no daemon is running**, so a script can test for one.

```bash
lya daemon stop
```

requests a graceful shutdown and waits until it has actually happened:

1. no further work is accepted — a submission arriving now is refused with `SHUTTING_DOWN`;
2. every active job receives the same graceful stop a foreground `Ctrl+C` would request, including
   [provider process-tree cancellation](autonomous-jobs.md#provider-process-tree-cancellation), and
   parks itself at a safe boundary under the existing job semantics;
3. the daemon waits for those jobs to shut down;
4. it releases its claim and exits.

`Lya daemon stopped.` is not printed until step 4, and that is decided by polling the claim on
`LYA_HOME` — never by watching the endpoint. The endpoint stops answering at step 1, while the
daemon is still running and still shutting jobs down, so treating an unreachable endpoint as
"stopped" would report success on a home the next command cannot use. Because the claim is the
authority:

```bash
lya daemon stop && lya daemon start
```

works even when active jobs take minutes to drain.

Work that never started stays `QUEUED` and is picked up by the next daemon — a shutdown never starts
a queued job in order to stop it. Stopping when nothing is running succeeds and says so, so the
command is safe to repeat.

## Submitting work

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
[job file](scheduler.md#job-file) `lya scheduler` takes, parsed by the same parser.

Two things are resolved by the client, in the shell that has the context for them, and travel with
the job:

* the **project path**, canonicalized, so a relative path is never interpreted against the daemon's
  working directory;
* the **Git publication configuration**, read from `LYA_GIT_*` and validated before the job is
  queued, so a job is never accepted in a shape that can only fail later.

Each job is validated and accepted on its own, and **every submitted job gets an answer**. A
submission of ten jobs with one unusable path queues nine and says exactly which one it refused:

```text
-                      NOT QUEUED  could not resolve the repository at ../gone
job-1763040000-4812-0  QUEUED  /home/you/projects/api
```

That holds for every way one job can fail, including one the daemon could not write down. A failure
partway through a batch refuses that job and nothing else; it never becomes an error for the whole
submission — which would throw away the ids of jobs already durably accepted, leaving a client
unable to tell a failed submission from a partly succeeded one.

The client holds no queue and no state. It sends a request and prints the answer.

## Delivery semantics

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

## Attaching and detaching

```bash
lya attach job-1763040000-4812-0
```

streams that job's live events — the same objects `events.jsonl` and `lya run --json` carry —
rendered exactly as `lya run` renders them:

```text
Attached to job-1763040000-4812-0. Ctrl+C detaches; the job keeps running.
14:03:21  SUPERVISOR  CLAUDE
      The integration test is still failing intermittently
      Claude: Run the failing test and fix the race
14:04:02  CLAUDE
      session 0b9d1f3a
      Serialised the shared fixture setup; the test passes 50 runs in a row.
```

Attach is **observational**. `Ctrl+C` detaches the viewer: the connection closes, the daemon forgets
it and the job continues untouched. Nothing about attaching can pause, stop or steer a job — that is
what [`lya control`](#controlling-a-job) is for. Any number of viewers may watch one job, and a job
with no viewers runs exactly the same.

`--replay` shows the job's recorded history before the live events. The subscription is opened before
the history is read, so nothing emitted during the replay is lost, and an event the replay already
showed is not repeated when it arrives live. The replay is bounded to the most recent events, and a
line the event log cannot parse — what a crash mid-write leaves behind — is skipped rather than
failing the attach. `events.jsonl` remains a record, never an authority.

A viewer that stops reading is disconnected on its own, with a reason, and the job is unaffected:

```text
Detached from job-1763040000-4812-0: the client fell behind by 128 event(s)
```

A [sequential chain](autonomous-jobs.md#sequential-jobs) is one piece of work with several job
identities. Attaching to the job you submitted follows the whole chain, and each job of it can also
be watched by its own name.

`--json` streams the raw events instead, one object per line.

## Controlling a job

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
[per-job instruction limits](autonomous-jobs.md#user-instructions) and persistence. There is one
control state machine, and the daemon does not add a second one.

Naming the job is what makes this unambiguous while several jobs run at once — the multiplexing
problem [`lya scheduler` deliberately does not
solve](scheduler.md#why-typed-commands-are-not-multiplexed).

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

## Restart recovery

If a daemon crashes, or the machine restarts, no job state is lost. Authoritative state is each job's
own `state.json`, and every lock is released by the operating system on process exit.

On the next start the daemon separates two questions it must not confuse:

* **Queued work** was accepted and never started, so there is nothing to reconstruct. It is
  submitted again, keeping its job identity and the limits it was accepted with.
  `--no-recover-queued` turns this off.
* **Interrupted work** was in the middle of something. By default the daemon finds it, reports it and
  leaves it exactly as it is:

  ```text
  DAEMON  parked 1 interrupted job(s): job-1763039000-3140-0 (continue with lya resume --job <id>)
  ```

  Parked jobs appear under `RESUMABLE` in `lya daemon status`, so nothing is silently dropped, and
  their persisted state is not touched.

`--resume-interrupted` asks the daemon to continue them, through the ordinary resume path with its
full validation: the persisted job is never rewritten to start it, the resume plan decides where it
continues, and anything ambiguous is parked in `WAITING_HUMAN` exactly as `lya resume` would park it.
The daemon invents no recovery semantics of its own — it only decides whether to ask.

A job another Lya process is currently driving is never taken over, even with `--resume-interrupted`:
its job lock is held, so the daemon parks it and reports it. Startup fails closed.

## Ownership and exclusion

Every job the daemon drives holds the same claims a foreground `lya run` takes — its
[job lock](persistence-and-recovery.md#job-locks) and its
[repository claim](persistence-and-recovery.md#repository-claims) — for the whole sequential chain.
Consequently:

* a foreground `lya run`, `lya resume` or `lya scheduler` cannot drive a job or a repository the
  daemon owns; it is refused, not queued behind it;
* the daemon cannot take over a job or a repository a foreground process owns;
* scheduler concurrency, persisted resume rules and queued-job semantics are unchanged.

Both directions fail closed, and both are enforced by the operating system rather than by
bookkeeping.

## Existing commands are unchanged

`doctor`, `supervisor`, `executor`, `run`, `resume`, `jobs` and `scheduler` behave exactly as they do
without a daemon and are **never silently redirected** to one.

That is a deliberate compatibility rule, not an omission. `lya run` and `lya scheduler` are attached
to your terminal and end with it; daemon-owned work does not. Quietly changing which one you got
would change where your job lives, who can control it and what happens when you close the shell.
Work reaches the daemon when you ask it to, through `lya submit`.

`lya jobs` keeps listing every persisted job, whoever is driving it, and stays read-only.

## The local protocol

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

The payload is nested rather than merged into the envelope so no payload field can collide with the
envelope's own. Requests and responses are explicit data transfer objects: a job in a status listing
is a projection of persisted state, not that state serialized, so the on-disk layout is free to
change and fields that have no business leaving the machine do not exist on the wire. Live job events
are the deliberate exception — they are already Lya's published observation format.

Every frame is bounded at 1 MiB, and a malformed one is answered rather than tolerated:

* a frame that is not JSON, or not a message this version knows, gets `INVALID_REQUEST`;
* a client speaking another protocol version gets `UNSUPPORTED_PROTOCOL`, naming both versions;
* a frame that exceeds the size bound ends that connection;
* a connection that sends no request within ten seconds is disconnected.

None of this can affect the daemon, the jobs or another client. Every connection is its own task: a
client that sends nonsense, stops reading, or disappears mid-frame is the only thing affected.

## Observability

The daemon keeps its own structured history, separate from job events:

```text
LYA_HOME/daemon/events.jsonl
```

One JSON object per line, tagged `daemon_event`, covering the daemon's own life: started, stopping,
stopped, scheduler started and stopped, work submitted, control delivered, queued work recovered,
interrupted work parked or resumed.

Transient per-connection traffic — clients connecting, disconnecting, attaching, detaching, being
refused — is shown while you watch a foreground daemon and deliberately **not** written to the
permanent log. A daemon that runs for weeks would otherwise fill its history with the comings and
goings of `lya daemon status`.

Each job keeps its own `events.jsonl`, unchanged. Neither log is ever authority: every decision comes
from authoritative persisted state and from the operating system's own claims.

A detached daemon's narration is captured in `LYA_HOME/daemon/daemon.log`, which is also where a
startup failure explains itself.

## Not implemented

* running the daemon as a system service (`systemd`, `launchd`, a Windows service);
* remote or multi-machine access of any kind;
* a web or browser administration interface;
* typed interactive commands on the `lya attach` stream.
