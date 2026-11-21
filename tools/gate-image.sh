#!/usr/bin/env bash
# Full gate for a mission image, as run before every merge into `mission`.
#   tools/gate-image.sh IMG [BASE_IMG] [MUT_REPEATS]
# IMG       image under test (e.g. kit/m1x)
# BASE_IMG  image it must equal on every shape set (default kit/n1 = mission f970fa0)
# Checks: normal 20M/60M reference hashes (s11, s13, s302, s901); shape sets equal to
# BASE_IMG (user17, user70, eid14, eiddash, cats at 20M; user17 at 60M if present);
# organizer fixtures, hand-made corpus, dev answer, 40 tiny seeds, 60 fuzz; the mutation
# gate MUT_REPEATS times (default 5); /fallback, /fallback-v3, /fallback-v2 run directly
# on 20M s11. Prints PASS/FAIL per check and exits non-zero on any failure.
set -u
IMG=${1:?image}; BASE=${2:-kit/n1}; REP=${3:-5}
O=$HOME/osu; T=${GATE_TMP:-/home/azureuser/.claude/jobs/7ac2f85c/tmp/gate}; mkdir -p "$T"
cd "$O" || exit 2
PY=$O/.venv/bin/python; fail=0
run() { # image dataset outdir [entrypoint]
  rm -rf "${T:?}/$3"; mkdir -p "$T/$3"; chmod 777 "$T/$3"
  docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL \
    --security-opt no-new-privileges --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
    -e NCPU=8 -v "$O/data/$2":/data:ro -v "$T/$3":/output ${4:+--entrypoint $4} "$1" >/dev/null 2>&1
}
hashes() { $PY tools/compare.py --hash "$T/$1" | awk '/checksum/{print $3}' | tr '\n' ',' | sed 's/,$//'; }
ref() { grep "^$1," tools/checksums.csv | cut -d, -f4,5; }
check() { if [ "$2" = "$3" ]; then echo "PASS $1"; else echo "FAIL $1 (got $2, want $3)"; fail=1; fi; }

for d in "p20m-s11 11" "p20m-s13 13" "oracle-s302-e60000000-c15000000 302" "oracle-s901-e60000000-c15000000 901"; do
  set -- $d; run "$IMG" "$1" o; check "hash $1" "$(hashes o)" "$(ref "$2")"
done
for ep in /fallback /fallback-v3 /fallback-v2; do run "$IMG" p20m-s11 o "$ep"; check "hash $ep p20m-s11" "$(hashes o)" "$(ref 11)"; done
for d in shape/p20m-user17 shape/p20m-user70 shape/p20m-eid14 shape/eiddash shape/p20m-cats; do
  run "$BASE" "$d" a; run "$IMG" "$d" b
  r=$($PY tools/compare.py "$T/a" "$T/b" | tail -1); check "shape $d vs $BASE" "$r" MATCH
done
out=$(timeout 3000 make -k fixtures corpus dev tiny fuzz IMPL="$HOME/osu-el/tools/docker-impl.sh" IMG="$IMG" 2>&1)
echo "$out" | grep -q "Error" && { echo "FAIL make check"; echo "$out" | grep -E "Error|DIFFER|MISMATCH" | head; fail=1; } || echo "PASS make check (fixtures corpus dev tiny fuzz)"
m=$(for r in $(seq "$REP"); do IMG="$IMG" xargs -L1 -P4 "$HOME/osu-el/tools/mutgate/mut.sh" < "$HOME/osu-el/tools/mutgate/jobs.txt"; done 2>&1)
bad=$(echo "$m" | grep -vc " MATCH "); total=$(echo "$m" | grep -c .)
check "mutation gate ${REP}x ($total runs)" "$bad" 0
[ $fail -eq 0 ] && echo "GATE PASSED: $IMG" || echo "GATE FAILED: $IMG"
exit $fail
