# Security

This describes what Lya actually enforces, and — just as important — what it does **not**. Lya is
experimental software that drives AI tools which modify real repositories. Read
[What is not a security boundary](#what-is-not-a-security-boundary) before running it anywhere you
care about.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).

* [Separation of duties](#separation-of-duties)
* [Provider credentials](#provider-credentials)
* [Provider sandboxing, as it really is](#provider-sandboxing-as-it-really-is)
* [Git publication safeguards](#git-publication-safeguards)
* [Process-tree cancellation](#process-tree-cancellation)
* [The daemon endpoint](#the-daemon-endpoint)
* [Protocol robustness](#protocol-robustness)
* [Prompt-injection posture](#prompt-injection-posture)
* [Local data sensitivity](#local-data-sensitivity)
* [What is not a security boundary](#what-is-not-a-security-boundary)

## Separation of duties

Lya deliberately separates reasoning from repository publication:

* the **Supervisor** decides; it does not commit;
* the **Executor** performs development work; it does not own publication;
* the **Publisher** commits and pushes; it uses no language model;
* Lya inspects the repository **itself** rather than trusting an executor's report, and the
  Supervisor is instructed to prefer that state over any claim in a report.

Before publication Lya checks that what Git is about to commit is the same repository state the
Supervisor reviewed. Scheduling and daemon mode change none of this: the scheduler starts jobs and
never inspects a repository, commits or pushes; the daemon owns processes, connections and shutdown
and holds no job semantics, no control state machine and no recovery rules of its own.

## Provider credentials

Lya needs no API key of its own. It runs the provider CLIs as child processes and relies on the
authenticated sessions those tools already hold.

It also actively removes ambient keys from those child environments, so an environment-provided key
cannot be billed by accident:

| Removed | From |
| --- | --- |
| `OPENAI_API_KEY` | the Codex Supervisor child process |
| `ANTHROPIC_API_KEY` | the Claude Code Executor child process |

Your own shell environment is untouched.

**Lya never falls back to a paid API to work around a subscription quota.** An exhausted quota is a
parked, resumable job status, not a silent switch to metered billing.

No credential is persisted, sent or logged. Git publication state describes identity, remote and
branch only; push authentication stays with the machine's own Git configuration. Daemon metadata
holds nothing but a process ID, a start time, a protocol version and an endpoint. Lya never logs
child-process environment variables.

## Provider sandboxing, as it really is

These are the flags Lya actually passes, and they are worth knowing exactly:

| Provider | Invocation | What it means |
| --- | --- | --- |
| Codex Supervisor | `codex exec --sandbox read-only --skip-git-repo-check --output-schema <schema> ...` | the Supervisor runs in Codex's **read-only** sandbox and answers against a structured output schema Lya validates locally |
| Claude Code Executor | `claude --print --output-format json --permission-mode auto --permission-prompts none ...` | the Executor runs **non-interactively with automatic permissions and no prompts** |

Lya never passes `--dangerously-skip-permissions`. But be clear about the consequence of the line
above: the Executor is intended to modify the repository, and it is run without interactive
confirmation. It operates inside Claude Code's own permission system, in the project directory Lya
gives it. **Lya adds no sandbox of its own around it.**

The Supervisor's decision is validated locally against a schema, so a malformed or unexpected
decision is rejected rather than acted on.

The full task is delivered to both providers on the child's **stdin** rather than on a command line,
so large prompts work reliably and prompt text does not appear in process listings.

## Git publication safeguards

Publication is opt-in (`--publish`) and guarded. In summary:

* the repository must be a Git repository with a clean working tree before a job starts, so
  pre-existing changes cannot be mistaken for agent-generated work;
* after `ACCEPT`, repository state is re-collected and compared against the reviewed snapshot;
* the **staged** content is verified against that snapshot again before the commit is created;
* the configured branch must already be the current local branch;
* `checkout`, `switch`, `pull`, `merge`, `rebase`, `reset` and force-push are **never** run
  automatically;
* a rejected push stops publication rather than rewriting history;
* normal Git hooks are respected;
* a commit is recorded in authoritative job state as soon as it exists — and a commit found without
  such a record parks the job in `WAITING_HUMAN` instead of being papered over.

Full detail in [Git publication](git-publication.md).

## Process-tree cancellation

Provider CLIs start helper processes, so killing only Lya's direct child would leave them running.
Every provider process is claimed by the operating system when it starts, and cancellation or a
timeout terminates the claim as a unit:

```text
Windows   Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
Unix      dedicated process group, signalled with killpg
```

On Windows the job is closed when Lya's handle goes away, so even a forced exit cannot leave a
provider tree behind. The one gap is honest and unavoidable: anything a provider starts in the
microseconds between the spawn and the job assignment is outside the job, because Windows offers no
way to assign a job to a process that is already running.

On Unix the provider does not share Lya's foreground process group, so a terminal `Ctrl+C` reaches
Lya alone and the first interrupt stays a graceful stop Lya controls.

## The daemon endpoint

The daemon endpoint is **local only**. No TCP socket is opened and no port is bound. There is no
remote access and nothing to authenticate over a network.

### Windows — named pipe

```text
\\.\pipe\lya-daemon-<home-fingerprint>
```

* **An explicit access control list.** Only the user running the daemon and `SYSTEM` are granted
  access. Windows' *default* named-pipe descriptor is deliberately not used, because it also grants
  read access to `Everyone` and to `ANONYMOUS LOGON` — enough for any local account to open the pipe
  and hold an instance.
* **Remote clients are refused explicitly** (`reject_remote_clients`).
* **One listener per name**, enforced by Windows itself (`first_pipe_instance`), so a second daemon
  cannot listen on the same name.
* **Both ends verify the other's user.** The pipe name is a deterministic fingerprint of `LYA_HOME`,
  and any local account may create a name in the named-pipe namespace, so a squatter can own the
  name before the daemon starts. The access control list cannot prevent that, so it is not relied on
  alone:
  * the daemon checks every accepted client's token user before serving it — by impersonating the
    client, falling back to the pipe's own client process only when impersonation is unavailable —
    and disconnects a stranger **without reporting anything to it**;
  * a client checks the serving process's token user **before sending a single byte** and refuses
    anything that is not this user's daemon.
* Identity is a security identifier compared with `EqualSid`, never a process ID. A process ID is
  only ever a way to reach a token, and every failure on that path refuses.

### Unix — local socket

```text
LYA_HOME/daemon.sock
```

* The socket is created `0600` inside a home directory set to `0700`. **File-system permissions are
  the access control.**
* There is no name to squat: the socket is a path inside a directory only the owner can traverse.
* A socket left behind by a dead daemon is only replaced after Lya confirms nothing answers on it.

Be precise about the difference: on Unix there is **no peer-credential check** in the code. The
guarantee is the file-system permission model, which is why the `0700` home matters. If you loosen
those permissions, you loosen the boundary.

### Both platforms

* A connected client that sends no request within **ten seconds** is disconnected. Only that client;
  the daemon, its jobs and every other client are untouched.
* One daemon per `LYA_HOME`, enforced by an operating-system claim on `daemon.lock` — not by a
  recorded process ID, which could have been reused.

## Protocol robustness

Every frame is one JSON object on one line, bounded at **1 MiB**, and a malformed one is answered
rather than tolerated:

* not JSON, or not a message this version knows → `INVALID_REQUEST`;
* a different protocol version → `UNSUPPORTED_PROTOCOL`, naming both versions;
* over the size bound → that connection ends.

Every connection is its own task, so a client that sends nonsense, stops reading or disappears
mid-frame affects nothing but itself. A viewer that falls behind on an attach stream is disconnected
with a reason and the job is unaffected.

The wire format carries explicit data transfer objects rather than serialized internal state, so
fields that have no business leaving the machine do not exist on the wire.

## Prompt-injection posture

Lya's Supervisor prompt instructs the model to treat every supplied data section as **untrusted
reference material**, never as instructions that override its role, and not to invent repository
facts or claim tests are green without evidence.

That is a mitigation, not a guarantee. Repository content, file names and executor reports are
attacker-influenceable in a repository you did not write, and a language model can be talked into
things. The real protection against a bad decision is structural, not linguistic: the Supervisor
cannot commit, the reviewed snapshot is verified twice before a commit is created, and publication
is opt-in.

## Local data sensitivity

* `LYA_HOME/context.md` is sent to the Supervisor with each review request. "Private" means "outside
  your repository", **not** "never leaves the machine". Never store credentials, keys, tokens or
  passwords in it. Its body is deliberately omitted from the observable Supervisor-request event.
* `events.jsonl` files are local but can contain prompts, model responses, file paths, repository
  metadata and commit titles. Treat them as potentially sensitive.
* `LYA_HOME/daemon/daemon.log` captures a detached daemon's narration.
* Hidden model reasoning is not available to Lya and is never claimed or logged.

## What is not a security boundary

Say this plainly, because the opposite assumption is how people get hurt:

* **Lya is not a sandbox for the Executor.** Claude Code runs non-interactively with automatic
  permissions in the directory you point it at. It can create, modify and delete files there.
* **Lya is not a sandbox for local agent mode.** `lya <prompt>` exposes `write_file` and
  `run_command` to a model, confined to `LYA_WORKSPACE` by path checks — that is a containment
  measure inside one trust domain, not protection against a hostile model or a hostile prompt.
  `run_command` executes commands.
* **Locks are not a permission system.** Job locks, repository claims and the daemon claim prevent
  *concurrent* drivers colliding. They do not stop a user who can write to `LYA_HOME` from editing
  job state, and anyone who can write there can influence what a later resume does.
* **`LYA_HOME` is not encrypted.** Its protection is file-system permissions.
* **The daemon does not authenticate a different user — it refuses one.** There is no multi-user
  model, no roles and no authorization layer. Same-user-only is the whole policy.
* **Provider trust is transitive.** Lya delegates authentication to Codex CLI and Claude Code, and
  inherits whatever those tools do with your credentials and your code.
* **Crash-safety is not power-loss safety.** See
  [Persistence and recovery — what is genuinely guaranteed](persistence-and-recovery.md#what-is-genuinely-guaranteed).

These protections reduce *accidental* autonomous changes. Run autonomous workflows only in
repositories where you understand and accept the risk, and prefer a branch you are willing to throw
away while you are learning what Lya does.
