#!/usr/bin/env bash
# Mutation gate (from the Master session): mut.sh BASE KIND RATE SEED [--late]
# Mutates dataset BASE with ~/osu/tools/mutate.py (valid under rules.md, but breaks the
# generator structure the fast path relies on), runs the image ${IMG:-kit/m1} through
# tools/docker-impl.sh and our v36 binary, and compares the outputs.
#   cd ~/osu && xargs -L1 -P4 ~/osu-el/tools/mutgate/mut.sh < ~/osu-el/tools/mutgate/jobs.txt
T=${MUT_TMP:-/home/azureuser/.claude/jobs/7ac2f85c/tmp/mut}; O=~/osu; mkdir -p "$T/m"
B=$1; K=$2; R=$3; S=$4; L=${5:-}
n=$(basename "$B")-$K-$R-$S${L:+-late}; D=$T/m/$n
python3 -I $O/tools/mutate.py "$B" "$D" "$K" "$R" "$S" $L
rm -rf "${D:?}.f" "${D:?}.r"; mkdir -p "$D.f" "$D.r"
fb=$(DATA_DIR=$D OUTPUT_DIR=$D.f NCPU=8 ~/osu-el/tools/docker-impl.sh 2>&1 | grep -o 'FALLBACK: .*' | head -1)
NCPU=8 $O/pipeline/target/release/pipeline "$D" "$D.r" >/dev/null 2>&1
res=$($O/.venv/bin/python $O/tools/compare.py "$D.f" "$D.r" >/dev/null 2>&1 && echo MATCH || echo MISMATCH)
echo "$n $res ${fb:-fast-path}"
rm -rf "${D:?}" "${D:?}.f" "${D:?}.r"
