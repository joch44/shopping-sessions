set -e
T=x86_64-unknown-linux-musl
mkdir -p /out/p /t
b() {
  ( cd "$1" && if [ "$4" = - ]; then env $6 cargo build -q --release --target $T --target-dir /t/$2; else env $6 cargo build -q --release --target $T --target-dir /t/$2 --features "$4"; fi && cp /t/$2/$T/release/$3 "$5" ) > /t/$2.log 2>&1 || touch /t/$2.fail
}
b /src probe probe - /out/probe &
b /src/gen gen generate - /out/generate &
b /src/kf kf kvmfacts - /out/kf &
( gcc -O2 -static -pthread -o /out/dsb /src/dsb/d.c > /t/dsb.log 2>&1 || touch /t/dsb.fail ) &
while read -r n k f rest; do
  [ -n "$n" ] && [ "$k" = b ] || continue
  be=""
  for t in $rest; do case "$t" in BUILD:*) be="$be ${t#BUILD:}" ;; esac; done
  cp -r /src/v/$n /t/src_$n
  b /t/src_$n v_$n pipeline "$f" /out/p/$n "$be" &
done < /src/v/list
wait
fail=0
for x in /t/*.fail; do [ -e "$x" ] || continue; echo "== FAILED $x"; cat "${x%.fail}.log"; fail=1; done
cp /src/v/list /out/p/list
ls -la /out /out/p
exit $fail
