# Contributing to Lya

Contributions are welcome. Lya is still evolving quickly, so the most useful contributions are
small, focused changes that preserve its lightweight and understandable architecture.

## Before you start

* For a **bug**, open an issue first if the behaviour is unclear; a small fix with a regression test
  can go straight to a pull request.
* For a **feature or a refactor**, open an issue first. The project deliberately avoids abstractions
  before they are needed, and an agreed scope saves a rewrite.
* For a **security vulnerability**, do not open a public issue — follow [SECURITY.md](SECURITY.md).

## Development setup

You need Rust 1.89 or newer and Git. (Edition 2024 needs 1.85; the standard library file-locking
API behind the job, repository and daemon claims needs 1.89.) Codex CLI and Claude Code are
**not** needed to build or test: every provider interaction in the test suite goes through a stubbed
process runner.

```bash
git clone https://github.com/Sharkou/Lya.git
cd Lya
cargo build
```

See [docs/development.md](docs/development.md) for the full picture.

## Before opening a pull request

Run the same checks CI runs, in this order:

```bash
cargo fmt --check
cargo check --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
```

All four must pass. Clippy warnings are errors in CI, on all three platforms.

If you changed anything platform-specific — locks, the daemon transport, process-tree cancellation,
path handling — say so in the pull request, and say which platforms you tested on. CI covers Linux,
Windows and macOS natively, and that matters precisely because those code paths differ.

## Code conventions

* Keep the agent core small, explicit and extensible.
* Separate LLM communication, agent orchestration, tool registration and tool execution.
* The agent must not depend directly on a specific provider.
* Tools are executed by Lya, never by the model directly.
* Do not introduce abstractions before they are needed; do not implement future features
  prematurely.
* Inspect existing code before modifying it, and make the smallest appropriate change.
* Never hide warnings or errors. Never claim something works without testing it.
* Preserve working code.

Two rules run through the whole orchestrator and are worth stating separately, because breaking them
is not a style problem:

* **One authority per question.** Persisted job state and operating-system claims decide what may
  happen next. Event logs are observability and must never be parsed to make a decision.
* **Fail closed.** Anything ambiguous parks a job for a person rather than guessing. Do not add a
  heuristic that lets Git write on an unproven assumption.

Doc comments carry the *why*. Where a decision looks odd — two files for the daemon claim, a lock
file kept after release — the comment next to it explains the failure mode it prevents. Please keep
that intact when you change the code, and add the same kind of comment for a new one.

See [docs/architecture.md](docs/architecture.md#design-rules).

## Tests

* Unit tests live beside the code in `#[cfg(test)] mod tests` blocks.
* **Tests must stay provider-free.** Nothing may invoke `codex` or `claude`. Drive Supervisor and
  Executor behaviour through the stubbed process runner.
* Tests may use the real `git` executable against temporary repositories. Pass identity per command
  (`-c user.name=... -c user.email=...`); never depend on or modify global Git configuration.
* Tests must not require repository secrets, network access or a running Ollama.
* A bug fix should come with a test that fails before it and passes after.
* Child-process halves of cross-process lock tests are marked `#[ignore]` — they are helpers
  launched by their parent test. Keep that pattern rather than making them run standalone.

## Documentation

[`docs/`](docs/README.md) is the source of truth, in the repository. The GitHub Wiki is not.

* A change that alters a command, a flag, a default, an exit code or an environment variable must
  update [docs/cli-reference.md](docs/cli-reference.md) or
  [docs/configuration.md](docs/configuration.md) in the same pull request.
* Every documented command must correspond to the actual current implementation. Do not document
  intent.
* Do not overclaim. If a guarantee has a window, describe the window — see
  [docs/persistence-and-recovery.md](docs/persistence-and-recovery.md#what-is-genuinely-guaranteed).
* Keep the README a landing page. Detail belongs in `docs/`.
* Use generic examples. No private paths, personal account information, private project names or
  secrets anywhere in the repository, including test fixtures.
* Keep Windows PowerShell and Unix shell differences explicit where they matter.
* Internal Markdown links are relative, and must resolve.

## Commits

Use conventional-commit style, matching the existing history:

```text
feat: add bounded multi-project scheduling
fix: harden provider cancellation and persisted resume state
docs: document the guarded publication sequence
chore: release v0.1.0
```

One logical change per commit. Write the message for someone reading `git log` in a year.

## Pull requests

Fill in [the template](.github/PULL_REQUEST_TEMPLATE.md). It asks what changed, why, how you
validated it, and which platforms you tested — that last one matters here more than in most projects.

Keep pull requests reviewable. A large mechanical change and a behaviour change do not belong in one
pull request.

## Releases

Releases are cut by the maintainer, by pushing a `vX.Y.Z` tag that matches `Cargo.toml`'s version.
Do not bump the version in a contribution pull request unless you were asked to. See
[docs/releases.md](docs/releases.md).

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
