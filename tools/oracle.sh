#!/usr/bin/env bash
# Full-size oracle (milestone 6): the validated Python reference, on the analysis VM.
#   tools/oracle.sh SEED EVENTS CLICKS
# Generates the dataset if missing (byte-identical to the mirror VM's copy), runs the
# reference, and records the output checksums in tools/checksums.csv (once per dataset).
# Then a timed run anywhere is verified with:  tools/compare.py --hash OUT  vs that row.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"
SEED=$1; EVENTS=$2; CLICKS=$3
if grep -q "^$SEED,$EVENTS,$CLICKS," tools/checksums.csv; then grep "^$SEED,$EVENTS,$CLICKS," tools/checksums.csv; exit 0; fi
DATA=data/oracle-s$SEED-e$EVENTS-c$CLICKS; OUT=$DATA.out
[ -f "$DATA/manifest.json" ] || bin/generate --seed "$SEED" --events "$EVENTS" --clicks "$CLICKS" --output "$DATA" >/dev/null
rm -rf "$OUT" data/oracle-scratch-$SEED
DATA_DIR=$DATA OUTPUT_DIR=$OUT SCRATCH_DIR=data/oracle-scratch-$SEED NCPU=${NCPU:-16} .venv/bin/python reference/reference.py 2>/dev/null
sums=$(.venv/bin/python tools/compare.py --hash "$OUT")
echo "$sums" | grep -q '^PASS$' || { echo "reference output failed its own checks" >&2; echo "$sums" >&2; exit 1; }
u=$(echo "$sums" | awk '/^user_daily: checksum/{print $3}'); c=$(echo "$sums" | awk '/^campaign_daily: checksum/{print $3}')
echo "$SEED,$EVENTS,$CLICKS,$u,$c" | tee -a tools/checksums.csv
rm -rf "$OUT" data/oracle-scratch-$SEED
