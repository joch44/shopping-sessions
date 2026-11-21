#!/usr/bin/env bash
# On the analysis VM: verify.sh TAG  -- checks bench.sh's saved outputs against tools/checksums.csv
cd "$(dirname "$0")/../.."
for t in p20m-s11:11 p20m-s12:12 p20m-s13:13 f60m-s301:301; do
  ds=${t%%:*}; seed=${t##*:}; rm -rf data/sim-out && mkdir -p data/sim-out
  ssh nolimit-sim "cat /tmp/$1-$ds.tar" | tar -C data/sim-out -xf - || { echo "$ds: no output"; continue; }
  got=$(.venv/bin/python tools/compare.py --hash data/sim-out/output | awk '/checksum/{print $3}' | paste -sd,)
  want=$(grep "^$seed," tools/checksums.csv | cut -d, -f4,5)
  [ "$got" = "$want" ] && echo "$ds: MATCH" || echo "$ds: MISMATCH ($got vs $want)"
done
