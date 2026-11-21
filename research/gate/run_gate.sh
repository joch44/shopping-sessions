#!/usr/bin/env bash
# Parse gate: live pipeline csv/key/ts fast paths vs the frozen v1 reference.
#   research/gate/run_gate.sh [PIPELINE_DIR] [DATASET_DIRS...]
# PIPELINE_DIR defaults to ./pipeline; datasets default to the quick-gate set
# (organizer fixtures, hand-made corpus, fuzz failures, 40 tiny seeds, dev).
# Exit 0 = identical; 1 = a difference (printed). Suggested Makefile target:
#   parsegate: ; @research/gate/run_gate.sh pipeline
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
PIPE=$(cd "${1:-$ROOT/pipeline}" && pwd); shift || true
DATA=${DATA_ROOT:-$ROOT/data}
if [ $# -eq 0 ]; then
  set -- $(ls -d "$ROOT"/vendor/starter-duckdb/starter-duckdb/fixtures/*/input 2>/dev/null || true) \
         $(ls -d "$ROOT"/tests/corpus/cases/*/input 2>/dev/null || true) \
         $(ls -d "$ROOT"/tests/fuzz-failures/*/ 2>/dev/null || true) \
         $(ls -d "$DATA"/tiny-* 2>/dev/null | sort -V | head -40) \
         $(ls -d "$DATA"/dev-s1 2>/dev/null || true)
fi
mkdir -p "$HERE/src/live"
cp "$PIPE/src/csv.rs" "$PIPE/src/key.rs" "$PIPE/src/ts.rs" "$HERE/src/live/"
export PATH="$HOME/.cargo/bin:$PATH"
( cd "$HERE" && RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release --quiet 2>&1 | grep -E '^error' -A8 ) && { echo "parse gate: build failed"; exit 1; } || true
"$HERE/target/release/parsegate" "$@"
