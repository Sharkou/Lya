# Changelog

All notable changes to Lya are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While Lya is at `0.y.z`, treat
every release as potentially breaking: the CLI and the public API are still moving.

## [Unreleased]

### Changed

* `lya attach <job-id>` now works on a job that has already finished: it replays the recorded
  history and exits instead of refusing with `JOB_TERMINAL`, which made the daemon's own
  "Watch one with: lya attach <job-id>" hint unusable for a job that outran the person reading it.
  A terminal job opens no live subscription, so the command cannot hang waiting for events that
  will never come, and a job whose event log is missing or truncated still reports its
  authoritative status. `--replay` is unchanged and still bridges history to live events for a
  running job. Attach only: `lya control` against a finished job still reports `JOB_TERMINAL`.
* The verbose Executor line now reads `Claude-reported cost metadata: $0.0794 (not proof of
  billing)`. `total_cost_usd` is provider metadata parsed from Claude Code's own JSON envelope and
  is preserved untouched on the event; Lya neither computes it nor knows how the provider CLI is
  authenticated, so it no longer reads as a confirmed charge.
* The `ATTACHED` response carries a `live` field saying whether events can still arrive. Additive
  and defaulted, so the protocol version is unchanged.

### Documentation

* Documented terminal-job attach semantics, `total_cost_usd` as provider metadata, and that
  `events.jsonl` is plain UTF-8 without a byte-order mark — including the Windows PowerShell 5.1
  `Get-Content -Encoding UTF8` needed to read it by hand, since 5.1 otherwise decodes it with the
  legacy ANSI codepage and shows mojibake for text Lya stored correctly.

## [0.1.1] — 2026-09-13

### Fixed

* Windows provider subprocesses launched by Lya are now kept headless with `CREATE_NO_WINDOW`,
  preventing visible console windows under the daemon and preventing provider processes from being
  exposed to unrelated console control events such as those that caused Codex to exit with
  `STATUS_CONTROL_C_EXIT` / `0xC000013A`.

## [0.1.0] — 2026-09-13

The first tagged release. It brought together the autonomous runtime that had accumulated in the
repository and the packaging, documentation and release infrastructure that made it installable; the
runtime entries below are listed for completeness so the initial release has a full record.

### Added

* Documentation set under [`docs/`](docs/README.md), the source of truth: installation, quick start,
  CLI reference, configuration, autonomous jobs, scheduler, daemon, Git publication, persistence and
  recovery, security, troubleshooting, architecture, development and releases.
* Project hygiene files: `LICENSE` (MIT), `CONTRIBUTING.md`, `SECURITY.md`, this changelog, a pull
  request template and issue templates.
* GitHub Actions CI on a native Linux, Windows and macOS matrix, running `cargo fmt --check`,
  `cargo check --all-targets`, `cargo test` and `cargo clippy --all-targets -- -D warnings`. No
  repository secrets and no provider calls.
* Tag-driven GitHub Release workflow building native archives for Windows x86_64, Linux x86_64,
  macOS arm64 and macOS x86_64, with a `SHA256SUMS.txt` asset, generated release notes and
  non-blocking build provenance attestation.
* Package metadata in `Cargo.toml`: description, repository, license, readme, keywords, categories
  and `rust-version = "1.89"`.
* `lya --help` / `-h` and `lya --version` / `-V`. Help is one screen naming every command; the
  version derives from the crate version at compile time. Both are recognised only as the first
  argument, so `lya run --help` still reports `run`'s own usage.
* **Agent runtime** — a bounded tool-calling loop against an OpenAI-compatible endpoint (Ollama),
  with `get_current_directory`, `read_file`, `write_file`, `create_directory`, `list_directory` and
  `run_command`, confined to `LYA_WORKSPACE`.
* **Codex Supervisor** — `codex exec` in a read-only sandbox, answering against a JSON output schema
  Lya validates locally. Decisions are `CLAUDE`, `ACCEPT`, `HUMAN` or `STOP`.
* **Claude Code Executor** — non-interactive invocation with resumable sessions, optional browser
  capability and prompt delivery over stdin.
* **Autonomous job loop** — Supervisor review, optional Executor invocation, independent repository
  inspection, repeat; bounded by `--max-iterations`.
* **Independent Git repository review** — `HEAD`, status, diff stat, changed files, tracked diff and
  untracked files collected by Lya itself, with explicit bounds and truncation markers.
* **Guarded Git publication** — opt-in `--publish`, with the reviewed snapshot verified after the
  decision and the staged content verified again before the commit; no automatic `checkout`,
  `switch`, `pull`, `merge`, `rebase`, `reset` or force-push.
* **Sequential multi-job runs** — `next_prompt` chains bounded by `--max-jobs`, advancing only after
  a successful publication.
* **Structured job events and live output** — per-job `events.jsonl`, plus normal, `--verbose` and
  `--json` rendering.
* **Interactive job control** — `/status`, `/diff`, `/pause`, `/resume`, `/stop` and `/send`, applied
  at safe boundaries, with bounded persisted user instructions.
* **Persisted job state and resume** — authoritative per-job `state.json` written atomically, one
  pending operation per job, and a resume that validates against the live repository before
  contacting a provider or touching Git.
* **Quota-aware pause and resume** — exhausted provider quotas park a resumable job and retry only
  the operation that had not completed; never a fallback to a paid API.
* **Read-only job listing** — `lya jobs`, with `--resumable` and `--json`, taking no locks and
  running no Git command.
* **Provider process-tree cancellation** — a Windows job object or a Unix process group, so a
  cancelled provider takes its helpers with it.
* **Bounded multi-project scheduling** — `lya scheduler` with a JSON-lines job file, FIFO ordering,
  `--max-concurrent` slots and durable `QUEUED` work.
* **Repository-level coordination** — an operating-system claim per repository root, so one working
  tree is never driven twice.
* **Persistent local daemon mode** — `lya daemon start/run/status/stop` with a local-only endpoint
  (Windows named pipe with an explicit ACL and same-user verification; Unix `0600` socket in a
  `0700` home), plus `lya submit`, `lya attach` and per-job `lya control`.
* **Environment diagnostics** — `lya doctor`.

### Changed

* The published executable is now named `lya` on every platform, via an explicit `[[bin]]` target.
  Cargo previously derived `Lya.exe` / `Lya` from the package name, which did not match the
  documented command. The package and library names are unchanged.
* `README.md` is now a landing page; its reference material moved into `docs/` rather than being
  deleted.
* The Supervisor system prompt escalates to "the project owner" instead of naming an individual, and
  test fixtures use generic project names.

[Unreleased]: https://github.com/Sharkou/Lya/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/Sharkou/Lya/releases/tag/v0.1.1
[0.1.0]: https://github.com/Sharkou/Lya/releases/tag/v0.1.0
