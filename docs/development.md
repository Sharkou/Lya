# Development

* [Requirements](#requirements)
* [Local validation flow](#local-validation-flow)
* [Where the binary lands](#where-the-binary-lands)
* [Testing the release binary](#testing-the-release-binary)
* [Running from a source checkout](#running-from-a-source-checkout)
* [Test suite conventions](#test-suite-conventions)
* [Continuous integration](#continuous-integration)
* [Code conventions](#code-conventions)
* [Repository layout](#repository-layout)

## Requirements

* **Rust 1.89 or newer** — edition 2024 needs 1.85, and the standard library file-locking API the
  job, repository and daemon claims are built on was stabilized in 1.89. `rust-version` in
  `Cargo.toml` records the real floor, and Clippy's `incompatible_msrv` lint keeps it honest.
* **Git** — not just for version control: the test suite creates real temporary repositories and
  drives the `git` executable against them.

Codex CLI and Claude Code are **not** needed to build or test. Every provider interaction in the
test suite goes through a stubbed process runner; no test performs a real provider call.

## Local validation flow

Run these in order. It is the same sequence CI runs, so a clean local pass means CI is unlikely to
disagree.

```bash
cargo fmt --check
```

```bash
cargo check --all-targets
```

```bash
cargo test
```

```bash
cargo clippy --all-targets -- -D warnings
```

```bash
cargo build --release --locked
```

`cargo clean` is **not** part of this flow and is not needed for an ordinary release build — Cargo
rebuilds what changed. Use it only when you are deliberately measuring a cold build or chasing a
suspected stale-artifact problem.

`--locked` makes the build fail rather than silently update `Cargo.lock`. Keep it for release builds
and commit the lockfile.

On Windows PowerShell, the same commands work unchanged; `&&` is not available in Windows PowerShell
5.1, so run them on separate lines.

## Where the binary lands

The package is named `Lya`, but the published command is `lya` on every platform. `Cargo.toml`
declares an explicit `[[bin]]` target so the executable file name is lowercase everywhere:

| Platform | Debug | Release |
| --- | --- | --- |
| Windows | `target\debug\lya.exe` | `target\release\lya.exe` |
| Linux, macOS | `target/debug/lya` | `target/release/lya` |

The library target is `lya` (`liblya.rlib`), so `use lya::...` works in tests and downstream code.

## Testing the release binary

Verify the artifact you would actually ship, not just the source:

```bash
./target/release/lya doctor
```

```powershell
.\target\release\lya.exe doctor
```

```bash
./target/release/lya --version
./target/release/lya --help
```

`--version` derives from `CARGO_PKG_VERSION` at compile time, so it is the fastest confirmation that
you are running the binary you just built rather than one earlier on your `PATH`. Then the read-only
checks:

```bash
./target/release/lya doctor
./target/release/lya jobs --json
./target/release/lya daemon status
```

`lya doctor` exits `0` only when every prerequisite resolves. `lya daemon status` exits non-zero when
no daemon runs, which is the intended behaviour, not a failure of the binary.

A fuller smoke test against a throwaway repository, with a throwaway `LYA_HOME`:

```bash
export LYA_HOME=$(mktemp -d)
printf 'Smoke test context.\n' > "$LYA_HOME/context.md"
./target/release/lya doctor
./target/release/lya daemon start
./target/release/lya daemon status
./target/release/lya daemon stop
```

```powershell
$env:LYA_HOME = Join-Path $env:TEMP ("lya-smoke-" + [guid]::NewGuid())
New-Item -ItemType Directory -Force $env:LYA_HOME | Out-Null
Set-Content -Encoding utf8 "$env:LYA_HOME\context.md" "Smoke test context."
.\target\release\lya.exe doctor
.\target\release\lya.exe daemon start
.\target\release\lya.exe daemon status
.\target\release\lya.exe daemon stop
```

That exercises `LYA_HOME` resolution, the daemon claim, the local endpoint and graceful shutdown
without contacting a provider.

## Running from a source checkout

```bash
cargo run -- doctor
cargo run -- jobs
cargo run -- run --max-iterations 3 "Fix a small regression and verify the result."
```

Note the `--` separator: everything after it goes to Lya rather than to Cargo.

Local agent mode finds its workspace automatically in a source checkout — the compile-time fallback
is `agent/workspace` under the crate directory, which is why `LYA_WORKSPACE` is only strictly
required for a prebuilt binary:

```bash
OLLAMA_MODEL=<model> cargo run -- "Summarise the workspace"
```

```powershell
$env:OLLAMA_MODEL = "<model>"
cargo run -- "Summarise the workspace"
```

## Test suite conventions

* **Provider-free.** Supervisor and Executor interactions are driven through a stubbed process
  runner. Nothing in the suite invokes `codex` or `claude`, and CI must stay that way.
* **Real Git.** Repository, publisher and orchestrator tests create temporary repositories and run
  the real `git` executable, passing identity per-command with `-c user.name=... -c user.email=...`
  so no global Git configuration is required or modified.
* **Real locks.** Lock and claim tests exercise the actual operating-system locks, including
  cross-process cases driven by child processes. Those child-side halves are marked
  `#[ignore]` — they are helpers launched by their parent test, not tests you run directly. A normal
  `cargo test` reports them as ignored, which is expected.
* **Unit tests live beside the code** in `#[cfg(test)] mod tests` blocks.
* Run one module's tests with a filter:

  ```bash
  cargo test orchestrator::publisher
  ```

## Continuous integration

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push to `main` and every
pull request, on a native matrix:

| Runner | Checks |
| --- | --- |
| `ubuntu-latest` | `cargo fmt --check`, `cargo check --all-targets`, `cargo test`, `cargo clippy --all-targets -- -D warnings` |
| `windows-latest` | `cargo check`, `cargo test`, `cargo clippy` |
| `macos-latest` | `cargo check`, `cargo test`, `cargo clippy` |

The matrix is native rather than cross-compiled on purpose: a large part of Lya is platform-specific
— named pipes and DACLs against Unix sockets, `LockFileEx` against `flock`, job objects against
process groups — and only a real runner on each platform exercises it.

`cargo fmt --check` runs once, on Linux, because formatting is platform-independent. Clippy runs
everywhere, because platform-gated code is only linted on the platform that compiles it.

CI requires **no repository secrets** and makes no provider calls.

## Code conventions

From [`.github/copilot-instructions.md`](../.github/copilot-instructions.md) and how the codebase is
actually written:

* keep the agent core small, explicit and extensible;
* separate LLM communication, agent orchestration, tool registration and tool execution;
* the agent must not depend directly on a specific provider;
* tools are executed by Lya, never by the model directly;
* do not introduce abstractions before they are needed, and do not implement future features
  prematurely;
* inspect existing code before modifying it, and make the smallest appropriate change;
* never hide warnings or errors, and never claim something works without testing it;
* one authority per question — persisted state and operating-system claims decide, logs never do;
* fail closed: anything ambiguous parks a job for a person rather than guessing.

Doc comments carry the *why*. Where a decision looks odd — two files for the daemon claim, a lock
file that is kept after release — the comment next to it explains the failure mode it prevents.
Preserve that when you change the code.

See [Architecture — design rules](architecture.md#design-rules).

## Repository layout

```text
.
├── Cargo.toml              package, lib and bin targets, metadata
├── Cargo.lock              committed; --locked builds depend on it
├── LICENSE                 MIT
├── README.md               project landing page
├── CHANGELOG.md
├── CONTRIBUTING.md
├── SECURITY.md
├── docs/                   the documentation source of truth
├── agent/
│   ├── INSTRUCTIONS.md     the operating instructions Lya itself is given
│   └── memory/             agent memory files
├── src/                    see the module map in docs/architecture.md
└── .github/
    ├── copilot-instructions.md
    ├── PULL_REQUEST_TEMPLATE.md
    ├── ISSUE_TEMPLATE/
    └── workflows/          ci.yml, release.yml
```

`docs/` is the source of truth for documentation, in the repository. It is plain Markdown with
relative links, so it can later back a GitHub Wiki or GitHub Pages without rewriting content. The
Wiki is not authoritative.

For cutting a release, see [Releases](releases.md).
