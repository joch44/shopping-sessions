p = "/usr/local/bin/nolimit-run"
s = open(p).read()
old = '  [ "${COLD:-0}" = 1 ] && { sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; }\n'
new = '''  # Flush pending writes first (builds, earlier outputs): background writeback on this
  # VM's ~0.9 GB/s disk otherwise steals seconds from the timed run.
  sync; sleep 1
''' + old
if "background writeback" not in s:
    assert old in s
    s = s.replace(old, new, 1)
    open(p, "w").write(s)
print("patched" if "background writeback" in s else "NOT patched")
