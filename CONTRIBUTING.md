# Contributing

## Setup

A fresh clone needs:

```bash
git config core.hooksPath .githooks
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

Nothing tells you if you skip them.

## Checks

CI runs:

```bash
cargo fmt --all -- --check
cargo test
cargo build --release --all-targets
```

The pre-commit hook rejects unformatted commits without fixing them; run `cargo fmt --all`.

## Tests

Tests live in `#[cfg(test)]` modules next to the code they exercise; there is no `tests/` directory.

Put a test next to the unit whose breakage should fail it. Most tests build a `SimpleDB` as a fixture, which is not a reason to put them in `src/server/simple_db.rs` — that file is for tests of the constructor itself.

## Lints

`cargo clippy --all-targets` reports 86 diagnostics and is not gated. Most are `arc_with_non_send_sync`, which needs a design decision rather than a mechanical fix.

## Commits

Reformat the tree in its own commit, and add that commit's full hash to `.git-blame-ignore-revs`.
