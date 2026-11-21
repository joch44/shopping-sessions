#!/usr/bin/env bash
# On the analysis VM: verify.sh TAG  -- checks bench.sh's saved outputs against tools/checksums.csv
cd "$(dirname "$0")/../.."
work=$(mktemp -d -p data verify-XXXX); trap 'rm -rf "$work"' EXIT
for t in p20m-s11:11 p20m-s12:12 p20m-s13:13 f60m-s301:301; do
  ds=${t%%:*}; seed=${t##*:}; rm -rf "$work/out" && mkdir -p "$work/out"
  ssh nolimit-sim "cat /tmp/$1-$ds.tar" | tar -C "$work/out" -xf - || { echo "$ds: no output"; continue; }
  got=$(.venv/bin/python tools/compare.py --hash "$work/out/output" | awk '/checksum/{print $3}' | paste -sd,)
  want=$(grep "^$seed," tools/checksums.csv | cut -d, -f4,5)
  [ "$got" = "$want" ] && echo "$ds: MATCH" || echo "$ds: MISMATCH ($got vs $want)"
done
