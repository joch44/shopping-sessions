#!/usr/bin/env bash
# Builds the pipeline at any git revision WITHOUT touching the working tree or the shared
# stash (other sessions use worktrees of this repo):  tools/build_rev.sh REV  -> prints binary path
set -euo pipefail
cd "$(dirname "$0")/.."
REV=$(git rev-parse --short "$1"); SRC=/tmp/claude-1000/rev-$REV; BIN=/tmp/claude-1000/pipeline-$REV
[ -x "$BIN" ] && { echo "$BIN"; exit 0; }
rm -rf "$SRC" && mkdir -p "$SRC" && git archive "$REV" pipeline | tar -x -C "$SRC"
( cd "$SRC/pipeline" && source ~/.cargo/env && CARGO_TARGET_DIR=/tmp/claude-1000/cargo-ab cargo build --release -q ) >&2
cp /tmp/claude-1000/cargo-ab/release/pipeline "$BIN" && echo "$BIN"
