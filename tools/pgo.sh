#!/usr/bin/env bash
# Regenerates pipeline/pgo/merged.profdata (profile-guided optimization) for the CURRENT
# code. Run before each submission: a stale profile still builds but loses the benefit.
# Instruments with exactly the Dockerfile's flags and target, trains on datasets that are
# never timed (practice-s201, medium-s101, dev-s1, tiny-s3), then merges with the toolchain's
# own llvm-profdata (same rustc 1.100.0 / LLVM as the Docker image).
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/.cargo/env
RAW=${PGO_RAW:-/tmp/claude-1000/pgo-raw}; TGT=${PGO_TGT:-/tmp/claude-1000/cargo-pgo-gen}
rm -rf "$RAW" && mkdir -p "$RAW"
tools/datasets.sh practice >/dev/null; tools/datasets.sh medium >/dev/null; tools/datasets.sh dev >/dev/null; tools/datasets.sh tiny >/dev/null
( cd pipeline && RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-generate=$RAW" \
    CARGO_TARGET_DIR=$TGT cargo build --release --locked --target x86_64-unknown-linux-gnu -q )
BIN=$TGT/x86_64-unknown-linux-gnu/release/pipeline
for d in data/practice-s201 data/medium-s101 data/dev-s1 data/tiny-s3-e40000; do
  out=$(mktemp -d); NCPU=8 DATA_DIR=$d OUTPUT_DIR=$out "$BIN"; rm -rf "$out"
done
PROFDATA=$(ls ~/.rustup/toolchains/*/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata | head -1)
mkdir -p pipeline/pgo
"$PROFDATA" merge -o pipeline/pgo/merged.profdata "$RAW"
ls -la pipeline/pgo/merged.profdata
