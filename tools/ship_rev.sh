#!/usr/bin/env bash
# Builds the SHIPPED binary (static, cascadelake, PGO with that tree's own profile, exactly the
# Dockerfile's flags) for a git revision or an exported tree, without touching the working tree:
#   tools/ship_rev.sh REV|DIR NAME   -> prints /tmp/claude-1000/ship-NAME
set -euo pipefail
cd "$(dirname "$0")/.."
SRC=$1; NAME=$2; OUT=/tmp/claude-1000/ship-$NAME
if [ ! -d "$SRC" ]; then
  T=/tmp/claude-1000/shiptree-$NAME; rm -rf "$T" && mkdir -p "$T" && git archive "$SRC" pipeline | tar -x -C "$T"; SRC=$T
fi
( cd "$SRC/pipeline" && source ~/.cargo/env && \
  RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-use=$(realpath pgo/merged.profdata)" \
  CARGO_TARGET_DIR=/tmp/claude-1000/cargo-ship-$NAME cargo build --release --locked --target x86_64-unknown-linux-gnu -q ) >&2
cp /tmp/claude-1000/cargo-ship-$NAME/x86_64-unknown-linux-gnu/release/pipeline "$OUT" && echo "$OUT"
