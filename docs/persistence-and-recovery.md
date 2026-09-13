# Persistence and recovery

Lya's authority is **per-job persisted state on disk plus the operating system's own locks**. Event
logs are observability and are never consulted to decide what may happen next.

* [On-disk layout](#on-disk-layout)
* [What a job persists](#what-a-job-persists)
* [Atomic writes](#atomic-writes)
* [Migration from the single state file](#migration-from-the-single-state-file)
* [Locks and claims](#locks-and-claims)
* [Resuming a job](#resuming-a-job)
* [Ambiguous state is parked, not guessed](#ambiguous-state-is-parked-not-guessed)
* [What is genuinely guaranteed](#what-is-genuinely-guaranteed)

## On-disk layout

Everything lives under [`LYA_HOME`](configuration.md#lya_home), which defaults to `~/.lya`:

```text
~/.lya/
├── context.md                          private Supervisor context (required)
├── daemon.lock                         the daemon's exclusive claim on this home
├── daemon.json                         daemon diagnostics (process id, endpoint, protocol)
├── daemon.sock                         Unix only; Windows uses a named pipe
├── daemon/
│   ├── events.jsonl                    the daemon's own structured history
│   └── daemon.log                      a detached daemon's captured narration
├── repositories/
│   └── <repository-fingerprint>.lock   one claim per repository being driven
└── jobs/
    └── <job-id>/
        ├── state.json                  authoritative job state
        ├── lock.json                   this job's exclusive claim
        ├── events.jsonl                this job's event history
        └── supervisor-decision*.json   Supervisor request/decision artifacts
```

An older layout may also leave a `state.json` at the top level; see
[migration](#migration-from-the-single-state-file).

## What a job persists

Each job records the run configuration it needs, so a restarted shell does not have to export the
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

**No secret is ever persisted.** Push authentication stays with the machine's own Git configuration,
and provider authentication stays with the provider CLIs.

Lya records exactly **one pending operation** per job: the single external action it still owes. A
resumed job continues at that operation and never replays a provider call that is already durably
recorded. A pending Executor correction reuses the persisted Claude session.

## Atomic writes

Each job's `state.json` is written through a unique temporary file, a synchronised write and a
rename. Two Lya processes working on different jobs therefore never rewrite each other's state.

`events.jsonl` is appended and synced after each event and never rewritten as a whole. Event
persistence is required: a job stops before its next Supervisor, Executor or Git action if an event
cannot be written or synced.

## Migration from the single state file

Earlier versions stored every job in one `LYA_HOME/state.json`. On the next writing command —
`lya run`, `lya resume`, `lya scheduler` or a daemon start — Lya moves those jobs into the per-job
layout and renames the old file to `state.json.migrated-<unix-seconds>`.

Nothing is deleted, and a per-job file that already exists is never overwritten. The migration
reports how many jobs it moved and how many already existed.

`lya jobs` is strictly read-only and deliberately does **not** migrate; it reports that a legacy
file is still present instead.

## Locks and claims

Three claims exist, all built on the same principle: the claim **is** the operating system's own
advisory lock on an open handle — never the presence of a file, and never a recorded process ID.

```text
Windows        LockFileEx
Linux, macOS   flock(2)
```

The kernel releases the lock when the process exits, including a crash or a forced kill, so an
abandoned lock file can never make a job permanently unresumable. Every lock file's body records the
owner, process ID and acquisition time **for diagnostics only**; because nothing reads it to decide
ownership, process-ID reuse cannot grant a claim.

A released lock keeps its file on purpose. Deleting a locked path would let another process lock a
fresh file under the same name and believe it owns the same thing.

### Job locks

```text
LYA_HOME/jobs/<job-id>/lock.json
```

Held while a process drives that job. A second process trying to run or resume the same job is
refused instead of corrupting shared state.

### Repository claims

```text
LYA_HOME/repositories/<repository-fingerprint>.lock
```

A job lock answers "is anyone else driving *this job*". It cannot answer "is anyone else driving
*this repository*", which is what concurrent scheduling has to answer: two different jobs pointed at
one working tree would interleave Git writes and destroy every snapshot guarantee.

Identity comes from the repository's real location, not from a project display name:

* the project path is canonicalized, so symlinks, `.`/`..` segments and Windows letter case resolve
  to one real path;
* the nearest ancestor holding a `.git` entry becomes the repository root, so a job started from a
  subdirectory claims the same repository as one started from the top level.

The claim file is named after a fingerprint of that root, because a path is not a portable file name.

`lya run`, `lya resume`, `lya scheduler` and the daemon all take the claim, so a manual run and a
daemon-driven job can never drive one working tree at the same time — not even across two Lya
processes. Claims are taken **repository first, job second** everywhere, so the two layers cannot
deadlock.

### The daemon claim

```text
LYA_HOME/daemon.lock
```

Held for as long as a daemon runs. Starting a second daemon against the same `LYA_HOME` is refused
before it can bind an endpoint or touch a job, and a daemon that dies for any reason releases the
claim automatically, so the next one starts without cleanup.

Diagnostics live in a **separate** file, `LYA_HOME/daemon.json`:

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
startup. They are also unreadable through a held lock on Windows — precisely when a refusal needs to
name the holder — hence two files rather than one.

Ownership authority is the claim and the endpoint. A process ID never is.

## Resuming a job

```bash
lya jobs --resumable
lya resume --job job-1789250000-4242-0
```

Resumable statuses are `RUNNING`, `PAUSED`, `PUBLISHING`, `WAITING_CLAUDE_QUOTA` and
`WAITING_OPENAI_QUOTA`. `QUEUED` is deliberately not resumable — only a scheduler or daemon starts
queued work, so resume can never bypass scheduler safety. `FAILED`, `STOPPED`, `PUBLISHED`,
`ACCEPTED` and `WAITING_HUMAN` are terminal for automatic recovery and are refused with their real
reason.

Resuming takes the repository claim as well as the job lock.

Before any provider call or Git write, Lya:

1. loads the persisted job;
2. validates it structurally and semantically;
3. confirms the project path is still the expected Git repository;
4. captures the current repository state;
5. compares reality against the persisted snapshot;
6. determines the exact continuation point;
7. only then contacts a provider or touches Git.

Resume emits `RESUME_STARTED`, `RESUME_VALIDATED`, `RESUME_REJECTED` and `QUOTA_RETRY_STARTED`
events into the same `events.jsonl` as the original process. Interactive control and `--json` output
work exactly as they do for a fresh run.

A completed Executor run is made durable before Lya reaches its next interruptible boundary. Pausing,
closing Lya's input, or losing the process the moment the Executor returns keeps the session
reference and the report, and the resumed job continues with a new review instead of re-running the
Executor.

## Ambiguous state is parked, not guessed

If the repository changed while Lya was not running, Lya does not guess: the job moves to
`WAITING_HUMAN` with a precise reason.

Persisted state that no continuation can be proven from is treated the same way — a quota wait naming
a different operation than the one actually owed, or a pending operation without the state it needs.
The job moves to `WAITING_HUMAN` with the exact reason **before the resume returns**, so it stops
being offered to every later `lya resume` as something that can still be continued. A job that is
already terminal keeps its own status and is simply refused.

An interrupted publication is the one state Lya can usually continue, and only when every invariant
can be proven against the live repository. See
[Git publication — recovering an interrupted publication](git-publication.md#recovering-an-interrupted-publication).

## What is genuinely guaranteed

Being precise about this matters more than sounding strong.

**Guaranteed:**

* every lock and claim is released by the operating system when its process exits, however it exits;
* `state.json` is replaced by rename, so a reader sees the old state or the new one, never a partial
  one;
* status, phase, pending operation and the reviewed snapshot enter persisted state in a single write;
* queued work is durable before it starts, so accepted-but-unstarted work is distinguishable from
  interrupted work;
* a commit that exists without an authoritative record is **detected** and parked, not papered over;
* no credential is persisted, sent or logged.

**Not guaranteed:**

* Lya does not promise that a power loss or a hard kernel crash leaves the file system in the state
  the last completed write described. It syncs its own writes, and beyond that it depends on the
  file system and the hardware.
* The commit and its record remain two steps. The window is small and its consequence is a parked
  job needing a person, not a lost or duplicated commit.
* Daemon submission is [at-least-once](daemon.md#delivery-semantics): a lost response can leave a
  job queued that the client reported as failed, and a blind retry can duplicate the work.
* A second `Ctrl+C` force-terminates Lya. That is safe for every lock and claim, which the operating
  system releases, but it skips Lya's own cleanup.
