p = "/usr/local/bin/nolimit-run"
s = open(p).read()
old_a = 'exec 9>/srv/nolimit/run.lock\nif ! flock -n 9; then echo ">> another nolimit-run is active; waiting for it" >&2; flock 9; fi\n'
old_b = 'exec 8>>/tmp/feas-bench.lock\nif ! flock -n 8; then echo ">> another session is benchmarking (/tmp/feas-bench.lock); waiting" >&2; flock 8; fi\n'
if "HOLDING_LOCKS" not in s:
    assert old_a in s and old_b in s
    s = s.replace(old_a, '# HOLDING_LOCKS=1: the caller already holds both locks for a whole session of runs.\nif [ "${HOLDING_LOCKS:-0}" != 1 ]; then\n' + old_a, 1)
    s = s.replace(old_b, old_b + 'fi\n', 1)
    open(p, "w").write(s)
print("patched" if "HOLDING_LOCKS" in s else "NOT patched")
