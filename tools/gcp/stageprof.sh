#!/usr/bin/env bash
# Per-stage profile, run ON a timing VM:  stageprof.sh OUTROOT DATASET ROUNDS NAME=BIN...
# Each run executes the binary in its own cgroup v2 (cpu.stat / memory.stat deltas survive the
# exit, unlike a docker scope), NCPU=8, TIMING=1, output+scratch on OUTROOT (must be a dedicated
# fs), page cache warmed, binaries interleaved per round. Caller must hold the bench lock(s).
set -uo pipefail
OUT=$1; DS=$2; ROUNDS=$3; shift 3
CG=/sys/fs/cgroup/osuprof
sudo mkdir -p $CG && sudo chown -R "$USER" $CG
cat "$DS"/* "$DS"/*/* 2>/dev/null >/dev/null
sys0() { awk '/^cpu /{print $5+$6, $9}' /proc/stat; awk '$1~/^(compact_stall|thp_fault_fallback|thp_fault_alloc)$/{printf "%s ", $2}' /proc/vmstat; }
stat1() { awk -v k="$2" '$1==k{print $2}' "$CG/$1"; }
for r in $(seq 0 "$ROUNDS"); do
  for spec in "$@"; do
    name=${spec%%=*}; bin=${spec#*=}
    sudo rm -rf "$OUT/o" "$OUT/s"; sudo install -d -o "$USER" "$OUT/o" "$OUT/s"; sync
    u0=$(stat1 cpu.stat user_usec); s0=$(stat1 cpu.stat system_usec)
    f0=$(stat1 memory.stat pgfault); t0=$(stat1 memory.stat thp_fault_alloc); c0=$(stat1 memory.stat thp_collapse_alloc)
    read -r i0 st0 cs0 tfa0 tff0 <<<"$(sys0)"
    a=$(date +%s%N)
    log=$(bash -c "sudo sh -c \"echo \$\$ > $CG/cgroup.procs\"; exec env TIMING=1 NCPU=8 MEM_GB=24 DATA_DIR=$DS OUTPUT_DIR=$OUT/o SCRATCH_DIR=$OUT/s $bin" 2>&1); rc=$?
    b=$(date +%s%N)
    read -r i1 st1 cs1 tfa1 tff1 <<<"$(sys0)"
    u1=$(stat1 cpu.stat user_usec); s1=$(stat1 cpu.stat system_usec)
    f1=$(stat1 memory.stat pgfault); t1=$(stat1 memory.stat thp_fault_alloc); c1=$(stat1 memory.stat thp_collapse_alloc)
    [ "$r" = 0 ] && continue   # warm-up round
    stages=$(echo "$log" | awk '/s  \(total/{n=$0; sub(/^\[timing\] /,"",n); sub(/ +[0-9.]+s +\(total.*/,"",n); gsub(/ /,"_",n); for(i=1;i<=NF;i++) if($i ~ /^[0-9.]+s$/){v=$i; sub(/s$/,"",v); printf "%s=%s ", n, v; break}}')
    awk -v r="$r" -v n="$name" -v rc="$rc" -v w=$((b-a)) -v u=$((u1-u0)) -v s=$((s1-s0)) -v f=$((f1-f0)) -v t=$((t1-t0)) -v c=$((c1-c0)) -v st="$stages" -v idle=$((i1-i0)) -v steal=$((st1-st0)) -v cs=$((cs1-cs0)) -v tff=$((tff1-tff0)) \
      'BEGIN{printf "R%d %s rc=%d wall=%.3f user=%.2f sys=%.2f faults=%d thp_fault=%d thp_collapse=%d idle+iow=%.2f steal=%.2f compact_stall=%d thp_fallback=%d %s\n", r, n, rc, w/1e9, u/1e6, s/1e6, f, t, c, idle/100, steal/100, cs, tff, st}' 
  done
done
