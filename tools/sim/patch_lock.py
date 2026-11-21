p = "/usr/local/bin/nolimit-run"
s = open(p).read()
old = 'RUN=/srv/nolimit/run; LOGS=$HOME/nolimit-logs/$(date +%Y%m%d-%H%M%S); mkdir -p "$LOGS"\n'
new = old + '''# One harness at a time: overlapping runs share /srv/nolimit/run, delete each
# other's output and spoil each other's timings.
exec 9>/srv/nolimit/run.lock
if ! flock -n 9; then echo ">> another nolimit-run is active; waiting for it" >&2; flock 9; fi
'''
if "flock" not in s:
    assert old in s
    s = s.replace(old, new, 1)
    open(p, "w").write(s)
print("patched" if "flock" in s else "NOT patched")
