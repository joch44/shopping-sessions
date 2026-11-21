p = "/usr/local/bin/nolimit-run"
s = open(p).read()
old = 'echo ">> output left in $RUN/output; logs in $LOGS" >&2\n'
new = '''# KEEP=/path.tar saves the last run's output while the lock is still held (another
# session's run could otherwise replace it before the caller copies it).
[ -n "${KEEP:-}" ] && sudo tar -C "$RUN" -cf "$KEEP" output
''' + old
if "KEEP=/path.tar" not in s:
    assert old in s
    s = s.replace(old, new, 1)
    open(p, "w").write(s)
print("patched" if "KEEP=/path.tar" in s else "NOT patched")
