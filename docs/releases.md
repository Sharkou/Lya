# Releases

Releases are cut by pushing a Git tag. Everything after that is automated by
[`.github/workflows/release.yml`](../.github/workflows/release.yml).

```text
tag vX.Y.Z
    ↓
verify the tag matches the Cargo version
    ↓
native release builds, one per platform
    ↓
packaged archives
    ↓
SHA-256 checksums
    ↓
GitHub Release with generated notes and all assets
```

* [Versioning](#versioning)
* [Release targets](#release-targets)
* [Asset names](#asset-names)
* [Archive contents](#archive-contents)
* [Checksums](#checksums)
* [Build provenance](#build-provenance)
* [Signing status](#signing-status)
* [Cutting a release](#cutting-a-release)
* [If the workflow fails](#if-the-workflow-fails)
* [Verifying a downloaded archive](#verifying-a-downloaded-archive)

## Versioning

Lya follows semantic versioning, and the Git tag is the crate version with a `v` prefix:

```text
v<crate-version>          e.g. Cargo.toml version 0.1.0  →  tag v0.1.0
```

The release workflow **verifies this and fails loudly** if the tag and
`Cargo.toml`'s `[package] version` disagree, before anything is built or published. That check is the
whole reason the two can't drift.

The workflow **never bumps the version itself**. Bumping is a deliberate, reviewable commit:

1. edit `[package] version` in `Cargo.toml`;
2. run `cargo build --locked` so `Cargo.lock` records the new version;
3. update `CHANGELOG.md`;
4. commit;
5. tag.

While the crate is at `0.y.z`, treat every release as potentially breaking — the public API and the
CLI are still moving.

## Release targets

Each archive is built **natively** on that operating system and architecture. Nothing is
cross-compiled, and nothing is emulated.

| Platform | Runner | Rust target |
| --- | --- | --- |
| Windows x86_64 | `windows-latest` | `x86_64-pc-windows-msvc` |
| Linux x86_64 (glibc) | `ubuntu-22.04` | `x86_64-unknown-linux-gnu` |
| macOS arm64 | `macos-15` | `aarch64-apple-darwin` |
| macOS x86_64 | `macos-15-intel` | `x86_64-apple-darwin` |

Windows x86_64 is the minimum a release must contain. The other three are published because a real
runner exists for each; if a runner image is retired, that row is removed from the matrix rather than
faked.

The Intel macOS build runs on `macos-15-intel`, the current GitHub-hosted Intel runner. The older
`macos-13` image it replaces has been retired. The artifact is still compiled natively on Intel
hardware — it is never cross-compiled from the arm64 runner.

The release matrix is wider than the CI matrix. CI tests Linux x86_64, Windows x86_64 and macOS
arm64; **macOS x86_64 is compiled for each release but does not get its own test run**. The two
macOS builds share every line of source and every Unix code path. This is stated in
[Installation — supported platforms](installation.md#supported-platforms) too, so a user is not
misled about it.

Linux builds use `ubuntu-22.04` rather than `ubuntu-latest` so the archive links against an older
glibc and runs on more distributions. The binaries are **not** static: a distribution with an older
glibc, or a musl distribution such as Alpine, needs a source build.

No build in the matrix produces an artifact for a platform it did not compile on.

## Asset names

```text
lya-v0.1.0-windows-x86_64.zip
lya-v0.1.0-linux-x86_64.tar.gz
lya-v0.1.0-macos-arm64.tar.gz
lya-v0.1.0-macos-x86_64.tar.gz
SHA256SUMS.txt
```

## Archive contents

Each archive contains the executable plus the files that need to travel with it:

```text
lya  (or lya.exe)
LICENSE
README.md
```

`LICENSE` is included because the MIT licence requires the notice to accompany distributed copies.
`README.md` is included so a downloaded archive can point at the documentation without a network
round trip. Unix archives preserve the executable bit.

## Checksums

One `SHA256SUMS.txt` asset covers every archive in the release, in the standard `sha256sum` format:

```text
<hex digest>  lya-v0.1.0-windows-x86_64.zip
<hex digest>  lya-v0.1.0-linux-x86_64.tar.gz
```

It is generated in a single job on Linux, after every build artifact has been collected, so all
digests come from one implementation rather than one per platform.

## Build provenance

The workflow attests build provenance for the release archives with GitHub's official
`actions/attest@v4` — the action GitHub now points new implementations at, in place of the
`actions/attest-build-provenance` wrapper. Given no predicate input it produces a
[SLSA build provenance](https://slsa.dev/spec/v1.0/provenance) attestation: a signed, verifiable
record of which workflow, at which commit, produced which artifact.

The subject is `SHA256SUMS.txt` itself, passed as `subject-checksums`, so one call covers every
archive in the release and the digests being attested are the same digests you verify by hand.
`SHA256SUMS.txt` is generated independently of this step, before it, and is published whether or not
attestation succeeds.

No signing secret is involved: Sigstore signs with the workflow's own short-lived OIDC identity. The
job grants `id-token: write`, `attestations: write` and `artifact-metadata: write` — that third scope
is what lets the action create its artifact storage record, and omitting it makes the step fail.

This step is **non-blocking**. Provenance attestation requires a public repository (or GitHub
Advanced Security on a private one), so on a private repository the step fails and is allowed to fail
without failing the release. A release is never held up by it, and its absence is not a defect.

Where an attestation exists, verify it with the GitHub CLI:

```bash
gh attestation verify lya-v0.1.0-linux-x86_64.tar.gz --repo Sharkou/Lya
```

## Signing status

Be clear about this, because the alternative is users guessing:

* **Windows binaries are not code-signed.** There is no Authenticode signature. SmartScreen may warn
  on first run, and that warning is expected.
* **macOS binaries are not signed and not notarized.** Gatekeeper blocks them on first run. Approve
  the binary in **System Settings → Privacy & Security**, or remove the quarantine attribute:

  ```bash
  xattr -d com.apple.quarantine /path/to/lya
  ```

* **Linux binaries carry no distribution signature**, which is normal for a directly downloaded
  archive.

The integrity mechanisms that *do* exist are the SHA-256 checksums and, where available, the build
provenance attestation. Verify the checksum.

Code signing and notarization **can be added later** without changing the release model: both are
additional steps inside the existing per-platform build jobs, gated on credentials in repository
secrets. They are deliberately not implemented, because doing so requires an Authenticode
certificate and an Apple Developer identity that must be owned and paid for by the project owner.
**No signing key or certificate is stored in this repository**, and none should ever be committed.

## Cutting a release

Prerequisites: `main` is green in CI, and the local validation flow in
[Development](development.md#local-validation-flow) passes.

1. **Bump the version** in `Cargo.toml`.

2. **Refresh the lockfile** so it records the new version:

   ```bash
   cargo build --locked
   ```

   If `--locked` fails because the version changed, run `cargo build` once and commit the updated
   `Cargo.lock`.

3. **Update `CHANGELOG.md`** — move the entries under `Unreleased` into a new `vX.Y.Z` section with
   the date.

4. **Validate locally**:

   ```bash
   cargo fmt --check
   cargo check --all-targets
   cargo test
   cargo clippy --all-targets -- -D warnings
   cargo build --release --locked
   ```

5. **Commit**:

   ```bash
   git commit -am "chore: release v0.1.0"
   ```

6. **Tag and push**. The tag must be `v` plus the exact `Cargo.toml` version:

   ```bash
   git push origin main
   git tag -a v0.1.0 -m "Lya v0.1.0"
   git push origin v0.1.0
   ```

7. **Watch the workflow**:

   ```bash
   gh run watch
   ```

8. **Review the release**. The workflow publishes it with generated release notes; edit the body on
   GitHub if you want a hand-written summary above them.

The release is created only from a pushed tag. Nothing in the workflow bumps a version, commits, or
pushes to a branch.

## If the workflow fails

**The version check failed.** The tag and `Cargo.toml` disagree. Nothing was built or published.
Delete the tag, fix the version, and tag again:

```bash
git push --delete origin v0.1.0
git tag -d v0.1.0
```

**A build failed on one platform.** No partial release is published: the release job runs only after
every build job succeeds, so an archive is never uploaded for a platform that did not build. Fix the
cause, then re-tag — see below.

**Re-releasing the same version.** Delete the GitHub Release and the tag, then push the tag again.
Prefer a new patch version if the old release was public for any length of time; people may already
have downloaded assets whose checksums you are about to change.

**The attestation step failed.** Expected on a private repository, and non-blocking. The release is
complete.

## Verifying a downloaded archive

```bash
curl -LO https://github.com/Sharkou/Lya/releases/download/v0.1.0/SHA256SUMS.txt
sha256sum --check --ignore-missing SHA256SUMS.txt
```

```bash
shasum -a 256 --check --ignore-missing SHA256SUMS.txt
```

```powershell
Invoke-WebRequest -Uri "https://github.com/Sharkou/Lya/releases/download/v0.1.0/SHA256SUMS.txt" -OutFile SHA256SUMS.txt
(Get-FileHash -Algorithm SHA256 .\lya-v0.1.0-windows-x86_64.zip).Hash.ToLower()
Select-String -Path SHA256SUMS.txt -Pattern "windows-x86_64"
```

Compare the two hashes. Installation steps are in [Installation](installation.md).
