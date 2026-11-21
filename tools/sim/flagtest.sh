#!/usr/bin/env bash
# Times the image under variants of the scorer's docker flags (one variant per line).
D=/srv/nolimit/datasets/p20m-s11; R=/srv/nolimit/run; IMG=shopping-rust:par1
BASE="--rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --user 1000:1000 --pids-limit 4096 --cap-drop ALL --security-opt no-new-privileges"
ENVS="-e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 -e TIMING=1"
run() {  # label, extra flags
  sudo rm -rf $R/output $R/scratch; sudo install -d -o 1000 -g 1000 $R/output $R/scratch
  t0=$(date +%s.%N)
  inner=$(sudo docker run $BASE $2 -v $D:/data:ro -v $R/output:/output -v $R/scratch:/scratch $ENVS $IMG 2>&1 | grep parquet | grep -o 'total *[0-9.]*s')
  t1=$(date +%s.%N); printf "%-44s wall %6.2f s   inside: %s\n" "$1" "$(echo "$t1 - $t0" | bc)" "$inner"
}
exec 9>/srv/nolimit/run.lock; flock 9
for i in 1 2; do
  run "scorer flags (cpuset + --cpus 8 + 24g)" "--cpuset-cpus 0-7 --cpus 8 --memory 24g --memory-swap 24g"
  run "no --cpus quota (cpuset + 24g)"          "--cpuset-cpus 0-7 --memory 24g --memory-swap 24g"
  run "no memory limit (cpuset + --cpus 8)"     "--cpuset-cpus 0-7 --cpus 8"
  run "no limits at all"                        ""
done
