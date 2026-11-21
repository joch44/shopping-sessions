p = "/usr/local/bin/nolimit-run"
s = open(p).read()
old = 'if ! flock -n 9; then echo ">> another nolimit-run is active; waiting for it" >&2; flock 9; fi\n'
new = old + '''# Also share the lock other sessions use for benchmarks on this VM, so timings never overlap.
exec 8>>/tmp/feas-bench.lock
if ! flock -n 8; then echo ">> another session is benchmarking (/tmp/feas-bench.lock); waiting" >&2; flock 8; fi
'''
if "feas-bench.lock" not in s:
    assert old in s
    s = s.replace(old, new, 1)
    open(p, "w").write(s)
print("patched" if "feas-bench.lock" in s else "NOT patched")
