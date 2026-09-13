# Installation

Lya is a single executable named `lya` (`lya.exe` on Windows) plus a local state directory.
There is no installer, no service registration and no background component you have to provision
before the first run.

* [Supported platforms](#supported-platforms)
* [Option A — prebuilt release binary](#option-a--prebuilt-release-binary)
* [Option B — build from source](#option-b--build-from-source)
* [Prerequisites](#prerequisites)
* [Set up LYA_HOME](#set-up-lya_home)
* [Verify the installation](#verify-the-installation)
* [Upgrading](#upgrading)
* [Uninstalling](#uninstalling)

## Supported platforms

| Platform | Tested in CI | Prebuilt release archive |
| --- | --- | --- |
| Windows x86_64 | yes — `windows-latest` | `lya-vX.Y.Z-windows-x86_64.zip` |
| Linux x86_64 (glibc) | yes — `ubuntu-latest` | `lya-vX.Y.Z-linux-x86_64.tar.gz` |
| macOS arm64 (Apple silicon) | yes — `macos-latest` | `lya-vX.Y.Z-macos-arm64.tar.gz` |
| macOS x86_64 (Intel) | built, not separately tested | `lya-vX.Y.Z-macos-x86_64.tar.gz` |

Every archive is a real native build on that operating system and architecture — nothing is
cross-compiled and nothing is emulated. Anything not listed is unsupported: it may well build from
source, and it is not tested.

The one row worth reading twice is **macOS x86_64**: it is compiled natively on an Intel macOS
runner for each release, but the CI matrix runs the test suite on Apple silicon only. It shares
every line of source and every Unix code path with the arm64 build, and it does not get its own test
run. If you are on an Intel Mac and something behaves oddly, say so in an issue.

Linux archives are built against the glibc of the release runner and are **not** static. A distro
older than that runner's glibc, or a musl distro such as Alpine, needs
[a build from source](#option-b--build-from-source).

Release binaries are **not code-signed**, and macOS binaries are **not notarized**. See
[Releases — signing status](releases.md#signing-status) for what that means when you open them.

## Option A — prebuilt release binary

No Rust toolchain is needed for this path.

1. Open the [releases page](https://github.com/Sharkou/Lya/releases) and pick the archive for your
   platform from the table above.
2. Verify its SHA-256 against the `SHA256SUMS.txt` asset published with the same release.
3. Unpack it and put `lya` somewhere on your `PATH`.

### Windows (PowerShell)

```powershell
$version = "v0.1.1"
$archive = "lya-$version-windows-x86_64.zip"
Invoke-WebRequest -Uri "https://github.com/Sharkou/Lya/releases/download/$version/$archive" -OutFile $archive
Invoke-WebRequest -Uri "https://github.com/Sharkou/Lya/releases/download/$version/SHA256SUMS.txt" -OutFile SHA256SUMS.txt
```

Compare the archive hash against the matching line of `SHA256SUMS.txt`:

```powershell
(Get-FileHash -Algorithm SHA256 $archive).Hash.ToLower()
Select-String -Path SHA256SUMS.txt -Pattern $archive
```

Unpack it and put it on your `PATH`:

```powershell
Expand-Archive -Path $archive -DestinationPath "$env:LOCALAPPDATA\Programs\Lya" -Force
$env:PATH = "$env:LOCALAPPDATA\Programs\Lya;$env:PATH"
```

To keep it on `PATH` in future sessions:

```powershell
[Environment]::SetEnvironmentVariable("PATH", "$env:LOCALAPPDATA\Programs\Lya;" + [Environment]::GetEnvironmentVariable("PATH", "User"), "User")
```

Windows binaries are unsigned, so SmartScreen may warn the first time you run `lya.exe`.

### Linux and macOS (bash / zsh)

```bash
version=v0.1.1
archive=lya-$version-linux-x86_64.tar.gz
curl -LO "https://github.com/Sharkou/Lya/releases/download/$version/$archive"
curl -LO "https://github.com/Sharkou/Lya/releases/download/$version/SHA256SUMS.txt"
```

On macOS use `lya-$version-macos-arm64.tar.gz` or `lya-$version-macos-x86_64.tar.gz` instead.

Verify the checksum — `sha256sum` on Linux, `shasum -a 256` on macOS:

```bash
sha256sum --check --ignore-missing SHA256SUMS.txt
```

```bash
shasum -a 256 --check --ignore-missing SHA256SUMS.txt
```

Unpack it and put it on your `PATH`:

```bash
tar -xzf "$archive"
mkdir -p "$HOME/.local/bin"
install -m 0755 lya "$HOME/.local/bin/lya"
export PATH="$HOME/.local/bin:$PATH"
```

macOS binaries are unsigned and un-notarized, so Gatekeeper blocks the first run. Approve it in
**System Settings → Privacy & Security**, or clear the quarantine attribute yourself:

```bash
xattr -d com.apple.quarantine "$HOME/.local/bin/lya"
```

### Local agent mode needs LYA_WORKSPACE with a prebuilt binary

The [local agent runtime](cli-reference.md#lya-prompt) sandboxes its file tools to a workspace
directory. When no `LYA_WORKSPACE` is set it falls back to a path baked in **at compile time** —
the source checkout the binary was built from. For a release binary that is a directory on the
build runner, which does not exist on your machine, so the fallback fails.

Prebuilt binaries therefore need the workspace named explicitly:

```powershell
New-Item -ItemType Directory -Force "$HOME\lya-workspace" | Out-Null
$env:LYA_WORKSPACE = "$HOME\lya-workspace"
```

```bash
mkdir -p "$HOME/lya-workspace"
export LYA_WORKSPACE="$HOME/lya-workspace"
```

The directory must already exist. This affects only `lya <prompt>`; every orchestration command
(`run`, `resume`, `jobs`, `scheduler`, `daemon`, `submit`, `attach`, `control`, `doctor`) uses
[`LYA_HOME`](configuration.md#lya_home) and is unaffected.

## Option B — build from source

Requires a Rust toolchain of **1.89 or newer** and Git. The crate is edition 2024 (Rust 1.85+)
and uses the standard library's file-locking API, stabilized in 1.89; `rust-version` in
`Cargo.toml` records that floor.

```bash
git clone https://github.com/Sharkou/Lya.git
cd Lya
cargo build --release --locked
```

The executable lands in:

| Platform | Path |
| --- | --- |
| Windows | `target\release\lya.exe` |
| Linux, macOS | `target/release/lya` |

Copy it onto your `PATH`, or keep using `cargo run --` during development. See
[Development](development.md) for the full local validation flow.

## Prerequisites

Which prerequisites you need depends on which part of Lya you use. Nothing below requires an API
key from Lya itself.

### Always

* **Git** — Lya inspects repository state with the `git` executable, and needs it on `PATH`.

### Autonomous development orchestration

`lya run`, `lya resume`, `lya scheduler`, `lya submit` and the daemon coordinate two external
command-line tools:

| Role | Tool | Needed for |
| --- | --- | --- |
| Supervisor | [Codex CLI](https://developers.openai.com/codex/cli) (`codex`) | deciding what happens next |
| Executor | [Claude Code](https://docs.claude.com/en/docs/claude-code) (`claude`) | performing development work |

Both must be installed and **authenticated independently**, through their own normal sign-in flow.
Lya never performs, proxies or stores that authentication. It runs `codex exec` and `claude` as
child processes and relies on whatever credentials those tools already hold — typically an
authenticated subscription session on your machine.

Lya does **not** need `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`. It actively removes both from the
provider child-process environments so an ambient key cannot be billed by accident. See
[Security — provider credentials](security.md#provider-credentials).

By default the two tools are resolved through `PATH` as `codex` and `claude`. Override either with
[`LYA_CODEX_BIN` / `LYA_CLAUDE_BIN`](configuration.md#provider-executables).

### Local agent mode

`lya <prompt>` talks to an OpenAI-compatible chat-completions endpoint, which is expected to be a
local [Ollama](https://ollama.com/) instance:

* Ollama running locally;
* a model installed that supports tool calling;
* [`OLLAMA_MODEL`](configuration.md#local-agent-mode) set;
* [`LYA_WORKSPACE`](configuration.md#lya_workspace) set when you use a prebuilt binary.

### Git publication

[Guarded Git publication](git-publication.md) is opt-in and only needed if you pass `--publish`.
It requires:

* a Git identity and target branch, configured through [`LYA_GIT_*`](configuration.md#git-publication);
* working push authentication for the repository — SSH keys or a credential manager.

Push authentication stays entirely with your machine's existing Git configuration. Lya holds no
GitHub token, asks for no credential and stores none.

## Set up LYA_HOME

Lya keeps private runtime data outside your repositories, in `LYA_HOME`, which defaults to
`~/.lya`. Create it and add the private context file the orchestrator requires:

```powershell
New-Item -ItemType Directory -Force "$HOME\.lya" | Out-Null
Set-Content -Encoding utf8 "$HOME\.lya\context.md" "Standing project context for the Supervisor."
```

```bash
mkdir -p ~/.lya
printf 'Standing project context for the Supervisor.\n' > ~/.lya/context.md
```

`context.md` is **required** by every orchestration command; a missing one is a hard error, not a
warning. Its content is sent to the Supervisor as reference material.

> **Never put credentials, API keys, tokens or passwords in `context.md`.** "Private" here means
> "outside your repository", not "kept local" — its content leaves the machine with a Supervisor
> request.

See [Configuration](configuration.md) for everything `LYA_HOME` holds and
[Persistence and recovery](persistence-and-recovery.md) for the on-disk layout.

## Verify the installation

```bash
lya doctor
```

A healthy report looks like this:

```text
Lya doctor

LYA_HOME       OK  /home/you/.lya
context.md     OK
git            OK  /usr/bin/git
codex          OK  /usr/local/bin/codex
claude         OK  /home/you/.local/bin/claude

Ready for orchestration.
```

`lya doctor` exits `0` only when every line is `OK`, so it is usable as a scripted gate. It
contacts no model and reads no repository.

Two more checks that need nothing configured at all — useful for confirming the right binary is on
your `PATH`:

```bash
lya --version
lya --help
```

`lya --version` prints `lya <version>`; `lya --help` prints one screen listing the commands. Both
exit `0`, and neither reads `LYA_HOME` or contacts anything.

A read-only check that also proves persisted state can be discovered:

```bash
lya jobs --json
```

For per-command syntax, see the [CLI reference](cli-reference.md) — or pass any subcommand an
invalid option, which prints that subcommand's own usage line.

## Upgrading

Replace the executable. Nothing in `LYA_HOME` has to be migrated by hand: an older single
`state.json` layout is migrated automatically the first time a writing command runs. See
[Persistence and recovery — migration](persistence-and-recovery.md#migration-from-the-single-state-file).

Stop a running daemon before replacing the binary it was started from:

```bash
lya daemon stop
```

## Uninstalling

1. `lya daemon stop`, if a daemon is running.
2. Delete the executable.
3. Delete `LYA_HOME` (`~/.lya` by default) if you no longer want its job history, event logs and
   private context.

Lya writes nothing outside `LYA_HOME`, the repositories you point it at, and — in local agent
mode — `LYA_WORKSPACE`.
