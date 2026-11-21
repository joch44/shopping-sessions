#!/usr/bin/env python3
"""Runs a pipeline on every case folder and diffs it against expected/.

    run_cases.py CASES_DIR [CASES_DIR ...] -- CMD ...

Each case is CASES_DIR/<name>/{input,expected} (the organizers' fixtures and
tests/corpus/cases share this layout). CMD gets DATA_DIR, OUTPUT_DIR and
SCRATCH_DIR in its environment, and DATA_DIR OUTPUT_DIR as its last two arguments
(like the organizers' run_fixtures.py, so scripts/docker_run.sh works). Prints one line per case and the failures'
diffs; exits 1 if any case fails.
"""
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import compare  # noqa: E402


def run_case(case_dir, command, forbid=None):
    with tempfile.TemporaryDirectory() as output, tempfile.TemporaryDirectory() as scratch:
        env = dict(os.environ, DATA_DIR=os.path.join(case_dir, "input"), OUTPUT_DIR=output,
                   SCRATCH_DIR=scratch)
        process = subprocess.run(command + [env["DATA_DIR"], output], env=env, capture_output=True, text=True, timeout=600)
        if process.returncode != 0:
            return False, [f"exit {process.returncode}", process.stderr[-1500:]]
        if forbid and forbid in process.stderr:
            return False, [f"stderr contains forbidden text {forbid!r}:", process.stderr[-1500:]]
        problems = []
        try:
            actual = {t: compare.load(output, t, problems) for t in compare.TABLES}
            expected = {t: compare.load(os.path.join(case_dir, "expected"), t, []) for t in compare.TABLES}
        except (compare.Unreadable, OSError, ValueError) as error:
            return False, [f"unreadable output: {error}"]
        lines = [f"CHECK FAILED {p}" for p in problems]
        ok = not problems
        for table in compare.TABLES:
            same, diff_lines = compare.diff(compare.keyed(actual[table], table, "actual", []),
                                            compare.keyed(expected[table], table, "expected", []), table)
            if not same:
                ok = False
                lines += diff_lines
        return ok, lines


def main(argv):
    split = argv.index("--")
    roots, command = argv[:split], argv[split + 1:]
    forbid = None
    if roots and roots[0].startswith("--forbid-stderr="):
        forbid = roots.pop(0).split("=", 1)[1]  # e.g. a fallback that must not trigger
    cases = sorted(os.path.join(root, name) for root in roots for name in os.listdir(root)
                   if os.path.isdir(os.path.join(root, name, "input")))
    failed = []
    for case_dir in cases:
        ok, lines = run_case(case_dir, command, forbid)
        name = os.path.relpath(case_dir)
        print(f"{'ok  ' if ok else 'FAIL'} {name}")
        if not ok:
            failed.append(name)
            catches = os.path.join(case_dir, "CATCHES.txt")
            if os.path.isfile(catches):
                print("       catches:", open(catches).read().strip())
            for line in lines:
                print("      ", line)
    print(f"\n{len(cases) - len(failed)}/{len(cases)} cases passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
