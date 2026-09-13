# Security Policy

## Supported versions

Lya is experimental and pre-1.0. Only the **latest release** and the current `main` branch receive
security fixes. There are no long-term support branches.

| Version | Supported |
| --- | --- |
| latest release | yes |
| `main` | yes |
| anything older | no |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions or pull
requests.**

Report privately through GitHub's private vulnerability reporting:

1. go to <https://github.com/Sharkou/Lya/security/advisories/new>;
2. describe the issue and submit the draft advisory.

That channel is private between you and the maintainers, and it needs no email address on either
side. If private reporting is unavailable for you, open a public issue containing **only** a request
for a private contact channel — no technical detail, no reproduction steps.

### What to include

As much of this as you have:

* the type of issue, and which component — orchestrator, daemon endpoint, publisher, agent runtime,
  provider invocation;
* affected version or commit, and your operating system;
* step-by-step reproduction, and a proof of concept if you have one;
* the impact: what an attacker gains, and what access they need to start with;
* any workaround you have found.

Please redact credentials, private paths and private repository content from anything you attach.

### What to expect

* an acknowledgement of your report, as promptly as a small project can manage;
* an assessment of whether it is a vulnerability, with reasoning if we disagree;
* a fix in `main` and a release, for accepted reports;
* credit in the advisory and the changelog, unless you ask otherwise.

This is a small volunteer project with no paid security team and no bug bounty. Timelines are
best-effort, and we will tell you where a report stands rather than leaving it silent.

Please give us a reasonable opportunity to fix an issue before disclosing it publicly.

## Scope

### In scope

* Escaping the guarded Git publication sequence — anything that lets a commit or push happen that
  does not match the reviewed snapshot, or that makes Lya run a Git operation it documents as never
  automatic (`checkout`, `switch`, `pull`, `merge`, `rebase`, `reset`, force-push).
* Access to the daemon endpoint by a **different local user**, on either platform, or any bypass of
  the Windows same-user verification.
* A local denial of service against the daemon from one connection — a malformed frame, a stalled
  client or an oversized message affecting the daemon, its jobs or another client.
* Credential exposure: any path by which Lya persists, logs, transmits or leaks an API key, a Git
  credential, a provider token or the content of `context.md` beyond the documented Supervisor
  request.
* Defeating the locks such that two processes drive one job or one repository concurrently.
* A resume or recovery path that acts on unproven state instead of parking the job.
* Path escapes from `LYA_WORKSPACE` in the local agent runtime's file tools.

### Out of scope

These are documented behaviour, not vulnerabilities. See
[docs/security.md](docs/security.md#what-is-not-a-security-boundary).

* **Lya is not a sandbox for the Executor.** Claude Code is run non-interactively
  (`--permission-mode auto --permission-prompts none`) in the project directory you point it at, and
  is intended to modify files there. A report that "the Executor changed my repository" is not a
  vulnerability.
* **`run_command` in local agent mode executes commands.** That is the tool's purpose. Path
  confinement to `LYA_WORKSPACE` is in scope; the existence of the tool is not.
* **A user who can write to `LYA_HOME` can influence Lya.** Job state is protected by file-system
  permissions, not by Lya.
* **Vulnerabilities in Codex CLI, Claude Code, Ollama, Git or Rust dependencies.** Report those
  upstream. If Lya *uses* one of them in a way that makes an upstream issue materially worse, that
  part is in scope — tell us.
* **Prompt injection that changes a model's decision.** Lya instructs the Supervisor to treat
  supplied data as untrusted reference material, and structurally the Supervisor cannot commit. A
  report showing injection leading to a Git write that bypasses snapshot verification **is** in
  scope; a report showing a model being persuaded to make a poor decision is not.
* **Remote access.** There is none to attack: no TCP socket, no port, no remote address.
* **Unsigned release binaries.** Windows binaries are not code-signed and macOS binaries are not
  notarized. This is documented in [docs/releases.md](docs/releases.md#signing-status).
* Findings that require an attacker to already have the ability to run code as your user.

## Hardening you control

* Keep `LYA_HOME` readable only by you. On Unix, Lya sets the home to `0700` and the daemon socket
  to `0600` — do not loosen them, since on Unix those permissions *are* the access control.
* Never store secrets in `context.md`. Its content is sent to the Supervisor.
* Treat `events.jsonl` as potentially sensitive: it can contain prompts, model responses, file paths
  and commit titles.
* Use `--publish` only on branches and repositories where an automated commit is acceptable.
* Verify the SHA-256 checksum of any release archive you download.
