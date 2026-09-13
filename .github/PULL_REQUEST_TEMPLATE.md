# Pull request

## What changed

<!-- One or two sentences. What does this do? -->

## Why

<!-- The problem or the motivation. Link the issue if there is one: Closes #123 -->

## Validation

All four must pass before review:

- [ ] `cargo fmt --check`
- [ ] `cargo check --all-targets`
- [ ] `cargo test`
- [ ] `cargo clippy --all-targets -- -D warnings`

<!-- Paste the relevant output, or describe what you ran, if anything is unusual. -->

## Platforms tested

A large part of Lya is platform-specific — locks, the daemon transport, process-tree cancellation,
path handling. Tick what you actually ran on, and say if this change does not touch platform code.

- [ ] Linux
- [ ] Windows
- [ ] macOS
- [ ] Not platform-specific

## Type of change

- [ ] Bug fix
- [ ] Feature
- [ ] Refactor with no behaviour change
- [ ] Documentation
- [ ] CI / packaging / release infrastructure

## Checklist

- [ ] The change is as small as it can be for what it does
- [ ] Tests cover the new behaviour, or the fixed bug fails without the fix
- [ ] Tests remain provider-free — nothing invokes `codex` or `claude`
- [ ] No new requirement for repository secrets, network access or a running Ollama
- [ ] Documentation in `docs/` is updated if a command, flag, default, exit code or environment
      variable changed
- [ ] No private paths, personal account information, private project names or secrets anywhere,
      including test fixtures
- [ ] `CHANGELOG.md` updated under `Unreleased` if this is user-visible
- [ ] The crate version is **not** bumped (releases are cut separately)

## Anything a reviewer should know

<!-- Trade-offs, things you deliberately left out, follow-up work. -->
