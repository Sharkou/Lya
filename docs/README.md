# Lya documentation

This directory is the **source of truth** for Lya's documentation. It is plain Markdown with
relative links, so it can later back a GitHub Wiki or GitHub Pages without rewriting content. The
GitHub Wiki is not authoritative.

Everything here describes the implementation as it currently exists. Where a behaviour is not
implemented, or a guarantee does not hold, that is stated rather than omitted.

## Start here

| Document | What it answers |
| --- | --- |
| [Installation](installation.md) | supported platforms, prebuilt binaries, building from source, prerequisites, verifying the install |
| [Quick start](quick-start.md) | from a fresh install to a first autonomous job, then the same work in the background |

## Using Lya

| Document | What it answers |
| --- | --- |
| [CLI reference](cli-reference.md) | every command, flag, default, exit code and example |
| [Configuration](configuration.md) | every environment variable, `LYA_HOME`, `context.md` |
| [Autonomous jobs](autonomous-jobs.md) | the Supervisor/Executor loop, statuses, live output, interactive control, quotas, sequential jobs, events |
| [Scheduler](scheduler.md) | driving several repositories at once, the job file, concurrency and fairness |
| [Daemon](daemon.md) | background work, submit/attach/control, restart recovery, the local protocol |
| [Git publication](git-publication.md) | the guarded commit-and-push sequence and what Lya refuses to do |

## Understanding Lya

| Document | What it answers |
| --- | --- |
| [Architecture](architecture.md) | the layers, the components, the module map, the design rules |
| [Persistence and recovery](persistence-and-recovery.md) | the on-disk layout, locks and claims, resume, what is genuinely guaranteed |
| [Security](security.md) | what is enforced, and what is explicitly **not** a security boundary |

## Operating and contributing

| Document | What it answers |
| --- | --- |
| [Troubleshooting](troubleshooting.md) | concrete failures, their real messages, and what to do |
| [Development](development.md) | the local validation flow, where the binary lands, test conventions, CI |
| [Releases](releases.md) | versioning, the tag-driven release workflow, checksums, signing status |

## Repository-level documents

* [README](../README.md) — the project landing page
* [CHANGELOG](../CHANGELOG.md)
* [CONTRIBUTING](../CONTRIBUTING.md)
* [SECURITY](../SECURITY.md) — how to report a vulnerability
* [LICENSE](../LICENSE) — MIT

## Read these before running autonomous work

Two sections matter more than the rest if Lya is going to modify real repositories:

* [Security — what is not a security boundary](security.md#what-is-not-a-security-boundary)
* [Persistence and recovery — what is genuinely guaranteed](persistence-and-recovery.md#what-is-genuinely-guaranteed)
