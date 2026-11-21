#!/usr/bin/env bash
# On nolimit-sim: ab.sh TAG_A TAG_B DATASET ROUNDS
# Interleaved A/B of two built images (shopping-rust:TAG) on one dataset: A, B, A, B, ...
# one fresh container per run (nolimit-run), then medians. Interleaving cancels drift.
set -uo pipefail
A=$1; B=$2; DS=$3; N=${4:-5}; ta=(); tb=()
for i in $(seq 1 "$N"); do
  for t in "$A" "$B"; do
    w=$(DATA=/srv/nolimit/datasets/$DS RUNS=1 NOBUILD=1 nolimit-run - "shopping-rust:$t" 2>/dev/null | grep -oE 'wall_s=[0-9.]+' | cut -d= -f2)
    [ "$t" = "$A" ] && ta+=("$w") || tb+=("$w")
  done
done
med() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END {print (NR%2 ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2)}'; }
echo "$DS  $A: $(med "${ta[@]}") s  [${ta[*]}]"
echo "$DS  $B: $(med "${tb[@]}") s  [${tb[*]}]"
