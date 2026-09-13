# Quick start

This walks from a fresh install to one completed autonomous job, then shows the same work handed to
a background daemon. It assumes [Installation](installation.md) is done: `lya` is on your `PATH`,
Git is installed, and Codex CLI and Claude Code are installed and signed in.

* [1. Verify the environment](#1-verify-the-environment)
* [2. Pick a repository](#2-pick-a-repository)
* [3. Run your first autonomous job](#3-run-your-first-autonomous-job)
* [4. Watch and steer the job](#4-watch-and-steer-the-job)
* [5. Inspect and resume](#5-inspect-and-resume)
* [6. Same work, in the background](#6-same-work-in-the-background)
* [What to read next](#what-to-read-next)

## 1. Verify the environment

First confirm which binary you are running, then check the environment it needs:

```bash
lya --version
lya doctor
```

`lya --help` lists the commands at any point. `lya doctor` is the real gate: every line must read
`OK` before autonomous work can start. A `MISSING` line for `context.md` means
[`LYA_HOME`](configuration.md#lya_home) has no private context file yet; see
[Installation — set up LYA_HOME](installation.md#set-up-lya_home).

## 2. Pick a repository

A project Lya may drive must:

* exist;
* be a Git repository;
* have a **clean working tree** when the job starts.

Lya refuses to start on a dirty repository, so pre-existing edits can never be mistaken for
agent-generated work. Commit or stash first.

```bash
cd /path/to/project
git status --short
```

## 3. Run your first autonomous job

Start without `--publish`. Nothing is committed or pushed; an accepted result records the commit
title it proposed and stops there.

```bash
lya run --max-iterations 3 "Add a unit test for the existing input validation and verify it passes."
```

```powershell
lya run --max-iterations 3 "Add a unit test for the existing input validation and verify it passes."
```

Without `--project`, the current directory is the project. To drive another checkout:

```bash
lya run --project ../other-service --max-iterations 3 "Fix the flaky integration test."
```

```powershell
lya run --project ..\other-service --max-iterations 3 "Fix the flaky integration test."
```

Output is a live, human-readable event stream: each Supervisor decision, the prompt sent to the
Executor, the Executor's final report, and a repository summary Lya collected itself rather than
taking on trust.

```text
19:04:11  JOB  example-project
      Add a unit test for the existing input validation and verify it passes.
19:04:12  SUPERVISOR
      review started
19:04:38  SUPERVISOR  CLAUDE
      No test covers the validation path yet
      Claude: Add the test next to the existing validation tests and run them
19:04:39  CLAUDE
      starting requested execution
19:05:30  CLAUDE
      session 0b9d1f3a
      Added tests/validation.rs and confirmed the suite passes.
19:05:31  REPOSITORY
      HEAD 9f1c4ad (dirty)
      tracked: tests/validation.rs
      tracked diff: 1 file changed, 24 insertions(+)
19:06:12  SUPERVISOR  ACCEPT
      The test covers the validation path and passes
      commit: Add validation unit test
19:06:12  JOB  ACCEPTED
```

Each event is a heading line — `HH:MM:SS`, then the stage — followed by indented detail lines. The
Executor's heading is `CLAUDE`, because that is the tool performing the work.

`lya run` stays attached to this terminal and ends with it. It exits `0` when the job reaches a
successful terminal status and non-zero otherwise, so it composes in scripts.

## 4. Watch and steer the job

While a `lya run` is attached to an interactive terminal it accepts typed commands. The terminal
stays a normal scrolling log — there is no fullscreen interface.

```text
/status              what the job is really doing right now
/diff                read-only repository capture
/send <instruction>  add a constraint for the rest of this job
/pause               pause at the next safe boundary
/resume              continue from that boundary
/stop                controlled shutdown
/help
```

The first `Ctrl+C` is the same graceful stop as `/stop`; a second one force-exits. Details in
[Autonomous jobs — interactive control](autonomous-jobs.md#interactive-control).

## 5. Inspect and resume

Every job is persisted. List them without touching anything:

```bash
lya jobs
```

```text
JOB                     PROJECT          STATUS     PHASE       ITER  UPDATED  RESUMABLE
job-1789250000-4242-0   example-project  ACCEPTED   PUBLISHER   2     4m ago   no
```

A job that was paused, parked on a provider quota, or interrupted by a process exit can be
continued by a later process:

```bash
lya jobs --resumable
lya resume --job job-1789250000-4242-0
```

Resume validates the persisted job against the live repository before it contacts a provider or
touches Git. See [Persistence and recovery](persistence-and-recovery.md).

## 6. Same work, in the background

A daemon owns the scheduler, the running jobs and a local control endpoint, and survives the shell
that started it.

```bash
lya daemon start
lya submit --project /path/to/project "Fix the flaky integration test."
lya daemon status
```

```powershell
lya daemon start
lya submit --project C:\Projects\Example "Fix the flaky integration test."
lya daemon status
```

`lya submit` prints the job id the daemon created. Watch it live, steer it by name, then shut the
daemon down:

```bash
lya attach job-1789250000-4812-0
lya control job-1789250000-4812-0 send "Also update the changelog."
lya control job-1789250000-4812-0 pause
lya daemon stop
```

`Ctrl+C` during `lya attach` detaches the viewer only — the job keeps running. `lya daemon stop`
refuses new work, stops active jobs at a safe boundary, waits for them, and only then reports
success.

Daemon mode is **local only**: no port is opened and no remote access exists. See
[Daemon](daemon.md).

## What to read next

| If you want to | Read |
| --- | --- |
| know every command and flag | [CLI reference](cli-reference.md) |
| set environment variables correctly | [Configuration](configuration.md) |
| understand the Supervisor/Executor loop | [Autonomous jobs](autonomous-jobs.md) |
| drive several repositories at once | [Scheduler](scheduler.md) |
| let Lya commit and push | [Git publication](git-publication.md) |
| know what survives a crash | [Persistence and recovery](persistence-and-recovery.md) |
| know the real security boundaries | [Security](security.md) |
| fix a failure | [Troubleshooting](troubleshooting.md) |
