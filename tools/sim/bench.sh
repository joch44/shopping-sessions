#!/usr/bin/env bash
# On nolimit-sim: bench.sh TAG CONTEXT_DIR
# Builds CONTEXT_DIR (an exported git tree) as shopping-rust:TAG, times it with nolimit-run
# (3 runs on 20M seeds 11-13, 3 runs on 60M seed 301), keeps each dataset's last output in
# /tmp/TAG-<dataset>.tar for checksum verification, then prints in-process stage timings.
set -uo pipefail
TAG=$1; CTX=$2; IMG=shopping-rust:$TAG
RUNS=0 nolimit-run "$CTX" "$IMG" 2>&1 | grep -E "build took|FAIL|error" || true
for ds in p20m-s11 p20m-s12 p20m-s13 f60m-s301; do
  echo "== $ds"; DATA=/srv/nolimit/datasets/$ds RUNS=3 NOBUILD=1 nolimit-run - "$IMG" 2>/dev/null
  sudo tar -C /srv/nolimit/run -cf /tmp/$TAG-$ds.tar output
done
id=$(sudo docker create "$IMG"); sudo docker cp "$id:/pipeline" /srv/nolimit/bin/pipeline-$TAG >/dev/null; sudo docker rm "$id" >/dev/null
exec 9>/srv/nolimit/run.lock; flock 9; exec 8>>/tmp/feas-bench.lock; flock 8; sync; sleep 1
for ds in p20m-s11 f60m-s301; do
  echo "-- stages $ds"; rm -rf /tmp/tout
  TIMING=1 NCPU=8 DATA_DIR=/srv/nolimit/datasets/$ds OUTPUT_DIR=/tmp/tout taskset -c 0-7 /srv/nolimit/bin/pipeline-$TAG 2>&1 | sed 's/\[timing\] //'
done
