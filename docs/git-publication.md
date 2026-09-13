# Guarded Git publication

Git publication is **opt-in**. Without `--publish`, an `ACCEPT` decision records the proposed commit
title and performs no Git write at all.

* [Enabling it](#enabling-it)
* [Configuration](#configuration)
* [The guarded sequence](#the-guarded-sequence)
* [What Lya never does](#what-lya-never-does)
* [Recording the commit](#recording-the-commit)
* [Recovering an interrupted publication](#recovering-an-interrupted-publication)
* [Push authentication](#push-authentication)
* [Publication and sequential jobs](#publication-and-sequential-jobs)

## Enabling it

```bash
lya run --publish --project /path/to/project "Implement and verify a small change."
```

```powershell
lya run --publish --project C:\Projects\Example "Implement and verify a small change."
```

`--publish` also exists on [`lya scheduler`](cli-reference.md#lya-scheduler) and
[`lya submit`](cli-reference.md#lya-submit), where it applies to every job of that invocation.

## Configuration

Publication requires an explicit Git identity and target branch:

| Variable | Required | Default |
| --- | --- | --- |
| `LYA_GIT_NAME` | yes | — |
| `LYA_GIT_EMAIL` | yes | — |
| `LYA_GIT_BRANCH` | yes | — |
| `LYA_GIT_REMOTE` | no | `origin` |

A missing or empty required variable is refused **before the job starts**, naming the variable.

```powershell
$env:LYA_GIT_NAME = "Automation Bot"
$env:LYA_GIT_EMAIL = "bot@example.com"
$env:LYA_GIT_BRANCH = "main"
$env:LYA_GIT_REMOTE = "origin"

lya run --publish --project C:\Projects\Example "Implement and verify one small improvement."
```

```bash
export LYA_GIT_NAME="Automation Bot"
export LYA_GIT_EMAIL="bot@example.com"
export LYA_GIT_BRANCH="main"
export LYA_GIT_REMOTE="origin"

lya run --publish --project /path/to/project "Implement and verify one small improvement."
```

The configured branch **must already be the current local branch** — Lya does not switch branches.

The identity is applied only to the commit Lya creates; the repository's own Git configuration is
not modified.

`lya submit --publish` reads and validates these variables in your shell, then sends identity,
remote and branch with the job. A **resumed** job publishes with the configuration persisted for it,
not with whatever the restarted shell exports. A job that needs to publish but has no persisted Git
configuration is refused with that exact reason rather than guessing.

## The guarded sequence

```text
Supervisor ACCEPT
 ↓
store reviewed snapshot
 ↓
recollect repository state
 ↓
verify exact match against the reviewed snapshot
 ↓
git add --all
 ↓
verify the staged state against the reviewed snapshot
 ↓
commit
 ↓
push
 ↓
verify clean working tree
```

Two verifications, not one: the repository is re-collected and compared after the decision, and the
**staged** content is checked again before the commit is created. If the repository changed between
review and publication, Lya refuses to publish it.

Normal Git hooks are respected. A rejected push stops publication instead of attempting to rewrite
history or resolve the conflict.

Binary or insufficiently reviewed states cannot be published automatically; see
[Autonomous jobs — repository review](autonomous-jobs.md#repository-review).

## What Lya never does

```text
checkout
switch
pull
merge
rebase
reset
force-push
```

None of these is ever run automatically. Lya does not reset or rewrite history to recover from
anything.

## Recording the commit

As soon as a commit exists it is recorded in authoritative job state with its SHA, publication
stage, remote, branch and push state, before any further step. A stop between the commit and the
push therefore leaves enough state for a later process to know exactly what exists and to continue
at the push.

The commit and its record are still **two separate steps**, so a crash in between can leave a commit
that no persisted state describes. That case is detected rather than assumed away: the next resume
finds that `HEAD` moved without an authoritative commit record, stops, and parks the job in
`WAITING_HUMAN` for a person to resolve.

Status, phase, pending operation and the reviewed snapshot enter persisted state in a **single**
write, so a crash between the `ACCEPT` decision and the first Git command leaves either the reviewed
iteration or a resumable `PUBLISHING` job — never something in between.

## Recovering an interrupted publication

Publication is recovered only when the state can be proven safe.

**If no commit was recorded**, Lya proves `HEAD` never moved, then either restarts the guarded
sequence or — when the index already holds exactly the accepted change — continues at the commit
step.

**If a commit was recorded**, Lya verifies all of:

```text
the commit is recorded in authoritative job state
HEAD is exactly that commit
its only parent is the accepted snapshot HEAD
the commit carries the accepted commit title
the commit contains exactly the accepted paths
the working tree is clean
the configured branch and remote still match
```

Only then does publication continue at the push, without creating a second commit.

Anything ambiguous — including a commit that exists without authoritative state — moves the job to
`WAITING_HUMAN` and leaves Git untouched.

## Push authentication

Push authentication is delegated entirely to the machine's existing Git configuration: an SSH agent,
a credential manager, whatever already works for `git push` in that repository.

Lya holds no GitHub token, asks for no credential, and persists none. Publication state describes
identity, remote and branch only.

## Publication and sequential jobs

A [sequential chain](autonomous-jobs.md#sequential-jobs) advances only after publication genuinely
succeeded:

* the previous job was accepted;
* publication succeeded;
* the push succeeded;
* the working tree is clean.

Without `--publish`, a `next_prompt` is retained and displayed but starts nothing automatically.
