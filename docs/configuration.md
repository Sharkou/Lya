# Configuration

Lya is configured entirely through environment variables and command-line flags. There is no
configuration file to write, and nothing in a repository configures Lya.

* [Complete variable reference](#complete-variable-reference)
* [LYA_HOME](#lya_home)
* [context.md](#contextmd)
* [Provider executables](#provider-executables)
* [Git publication](#git-publication)
* [Local agent mode](#local-agent-mode)
* [LYA_WORKSPACE](#lya_workspace)
* [Variables Lya removes](#variables-lya-removes)
* [Setting variables](#setting-variables)

## Complete variable reference

| Variable | Used by | Default | Required |
| --- | --- | --- | --- |
| `LYA_HOME` | every orchestration command | `~/.lya` | no |
| `LYA_CODEX_BIN` | Supervisor | `codex` on `PATH` | no |
| `LYA_CLAUDE_BIN` | Executor | `claude` on `PATH` | no |
| `LYA_GIT_NAME` | `--publish` | — | yes, with `--publish` |
| `LYA_GIT_EMAIL` | `--publish` | — | yes, with `--publish` |
| `LYA_GIT_BRANCH` | `--publish` | — | yes, with `--publish` |
| `LYA_GIT_REMOTE` | `--publish` | `origin` | no |
| `OLLAMA_MODEL` | `lya <prompt>` | — | yes, for local agent mode |
| `OLLAMA_BASE_URL` | `lya <prompt>` | `http://127.0.0.1:11434/v1` | no |
| `LYA_WORKSPACE` | `lya <prompt>` | a compile-time path in the source checkout | yes, with a prebuilt binary |

That is the whole list. Variables beginning `LYA_TEST_` exist only to coordinate child processes in
the test suite and have no effect on normal use.

## LYA_HOME

`LYA_HOME` is the private directory holding everything Lya keeps outside your repositories: the
private context, per-job state, event logs, locks and daemon metadata.

Resolution order:

1. `LYA_HOME`, if set and non-empty;
2. otherwise `<home>/.lya`, where `<home>` is `USERPROFILE` (or `HOMEDRIVE` + `HOMEPATH`) on
   Windows, and `HOME` elsewhere.

If no home directory can be determined at all, commands fail with a message telling you to set
`LYA_HOME` explicitly.

Two different `LYA_HOME` values are two fully independent installations: separate job state,
separate locks, and separate daemons with separate endpoints. That is the supported way to keep
unrelated work apart.

`lya daemon start` pins the home it resolved into the detached child's environment, so a daemon can
never adopt a different one from a different shell.

For the directory layout, see
[Persistence and recovery — on-disk layout](persistence-and-recovery.md#on-disk-layout).

## context.md

```text
LYA_HOME/context.md
```

A UTF-8 Markdown file holding standing private context for the Supervisor — conventions, priorities,
constraints that are not in the repository.

It is **required** by `lya supervisor`, `lya run`, `lya resume`, `lya scheduler` and the daemon. A
missing file is a hard error before any provider call, not a warning. Invalid UTF-8 is also refused
explicitly.

> **Never store credentials, API keys, tokens or passwords in `context.md`.** "Private" means
> "outside your repository", not "kept on this machine": the file's content is sent to the
> configured Supervisor with each review request. Nothing in Lya reads a secret out of it, and
> nothing redacts one either.

The body is deliberately omitted from the observable Supervisor-request event, so it is not copied
into `events.jsonl`.

## Provider executables

| Variable | Overrides |
| --- | --- |
| `LYA_CODEX_BIN` | the Supervisor's `codex` executable |
| `LYA_CLAUDE_BIN` | the Executor's `claude` executable |

Each takes a full path or a name resolvable on `PATH`. Unset, Lya uses `codex` and `claude` from
`PATH`. `lya doctor` reports which path it resolved, so it is the fastest way to confirm an override
took effect.

Both tools authenticate on their own. Lya performs no sign-in, proxies no token and stores no
credential.

## Git publication

Required only when a command is given `--publish`.

| Variable | Meaning |
| --- | --- |
| `LYA_GIT_NAME` | commit author/committer name |
| `LYA_GIT_EMAIL` | commit author/committer email |
| `LYA_GIT_BRANCH` | the branch publication targets; it must already be the current local branch |
| `LYA_GIT_REMOTE` | push remote; defaults to `origin` |

An empty or missing `LYA_GIT_NAME`, `LYA_GIT_EMAIL` or `LYA_GIT_BRANCH` is refused **before** the job
starts, naming the variable — publication is never attempted in a shape that can only fail later.

The identity is applied only to the commit Lya creates; it does not change the repository's own Git
configuration. Push authentication is delegated entirely to the machine's existing setup (SSH agent,
credential manager). Lya holds no GitHub token.

`lya run` and `lya scheduler` read these variables in the shell that starts them. `lya submit` also
reads and validates them client-side, then sends identity, remote and branch along with the job, so
the daemon never guesses at its own environment. A resumed job uses the configuration **persisted
with the job**, not whatever the new shell happens to export.

See [Git publication](git-publication.md) for the guarded sequence itself.

## Local agent mode

`lya <prompt>` talks to an OpenAI-compatible `/chat/completions` endpoint.

| Variable | Default | Notes |
| --- | --- | --- |
| `OLLAMA_MODEL` | — | required; an empty value is rejected |
| `OLLAMA_BASE_URL` | `http://127.0.0.1:11434/v1` | the base URL; Lya appends `/chat/completions` |

The model must support tool calling, because the loop exposes tools to it. The loop is bounded at 20
agent iterations.

This mode uses no `LYA_HOME`, writes no job state and needs neither Codex nor Claude.

## LYA_WORKSPACE

The directory the local agent's file tools are confined to. `get_current_directory`, `read_file`,
`write_file`, `create_directory`, `list_directory` and `run_command` all operate inside it.

When `LYA_WORKSPACE` is unset, Lya falls back to `agent/workspace` under the crate directory
recorded **at compile time**. That works for `cargo run` in a source checkout and does not work for
a prebuilt release binary, whose recorded path is a directory on the build runner. Set
`LYA_WORKSPACE` when you use a release binary; the directory must already exist.

It affects only `lya <prompt>`. Orchestration commands ignore it.

## Variables Lya removes

Lya removes these from the **child-process** environments it starts, so an ambient key cannot be
billed by accident:

| Variable | Removed from |
| --- | --- |
| `OPENAI_API_KEY` | the Codex Supervisor child process |
| `ANTHROPIC_API_KEY` | the Claude Code Executor child process |

Your own shell is untouched. Lya has no paid-API fallback: a provider quota is a parked, resumable
state, never a silent switch to metered billing. See
[Security — provider credentials](security.md#provider-credentials).

## Setting variables

### PowerShell (Windows)

Current session:

```powershell
$env:LYA_HOME = "$HOME\.lya"
$env:LYA_GIT_NAME = "Automation Bot"
$env:LYA_GIT_EMAIL = "bot@example.com"
$env:LYA_GIT_BRANCH = "main"
```

Persistently, for your user account:

```powershell
[Environment]::SetEnvironmentVariable("LYA_HOME", "$HOME\.lya", "User")
```

Note that `$env:VAR = ...` only affects the current session, and that a value set with
`SetEnvironmentVariable` is picked up by **new** shells, not the one you typed it in.

### bash / zsh (Linux, macOS)

Current session:

```bash
export LYA_HOME="$HOME/.lya"
export LYA_GIT_NAME="Automation Bot"
export LYA_GIT_EMAIL="bot@example.com"
export LYA_GIT_BRANCH="main"
```

Persistently, in `~/.bashrc` or `~/.zshrc`:

```bash
echo 'export LYA_HOME="$HOME/.lya"' >> ~/.zshrc
```

A one-off, for a single command:

```bash
OLLAMA_MODEL=qwen2.5-coder lya "Summarise the workspace"
```

PowerShell has no inline variable prefix — set `$env:OLLAMA_MODEL` on its own line first.
