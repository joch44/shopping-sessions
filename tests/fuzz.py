#!/usr/bin/env python3
"""Mutation fuzzer: takes a (small) generated dataset and rewrites it with dirt at
high rates, aimed at every rule boundary, including forms the generator never
writes (\\r, bad offsets, leading-zero F3, spaces, upper-case IDs).

    fuzz.py make SRC DST SEED        write a fuzzed copy of dataset SRC to DST
    fuzz.py run  SRC N -- CMD_A [-- CMD_B]
        N fuzzed datasets from SRC (seeds 1..N). Runs CMD_A on each (and CMD_B if
        given), checks invariants, and with two commands diffs A against B.
        Failing datasets are kept under tests/fuzz-failures/.

Only the INPUT is random; no expected answers are produced here. With one
command it finds crashes and invariant breaks; with two (fast vs reference)
it finds any disagreement.
"""
import datetime
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import compare  # noqa: E402

DAY = 86_400_000
HOUR = 3_600_000
MINUTE = 60_000


# ---------------------------------------------------------------- tiny helpers (own code, not the reference's)

def to_f1(ms_value):
    when = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=ms_value)
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_f2(ms_value, offset_minutes):
    when = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=ms_value + offset_minutes * MINUTE)
    sign = "-" if offset_minutes < 0 else "+"
    return when.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{abs(offset_minutes) // 60:02d}:{abs(offset_minutes) % 60:02d}"


def loose_ms(text):
    """Best-effort epoch ms of a clean F1 or F3 value; None otherwise."""
    if text.isdigit() and len(text) < 16:
        return int(text)
    try:
        when = datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None
    return int((when - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)


def unquote_simple(field):
    return field[1:-1] if len(field) >= 2 and field[0] == '"' and field[-1] == '"' else field


def read_lines(path):
    with open(path, "rb") as handle:
        data = handle.read().decode("utf-8")
    lines = data.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines[0] if lines else None, lines[1:]


# ---------------------------------------------------------------- mutations

class Fuzzer:
    def __init__(self, seed, window):
        self.r = random.Random(seed)
        self.window_start, self.window_end = window
        # Per-seed intensity, so some datasets are mostly clean and some are filthy.
        self.rate = self.r.choice([0.02, 0.05, 0.1, 0.2])

    def chance(self, scale=1.0):
        return self.r.random() < self.rate * scale

    def ts_variant(self, ms_value):
        """Re-encode a valid instant: same instant (valid) or a broken one."""
        r = self.r
        pick = r.randrange(12)
        if pick == 0:
            return str(ms_value)
        if pick == 1:
            return "0" * r.randint(1, 20) + str(ms_value)
        if pick == 2 and ms_value % 1000 == 0:
            return to_f2(ms_value, r.choice([-600, -59, 0, 1, 330, 345, 720, 1439, -1439]))
        if pick == 3 and ms_value % 1000 == 0:
            return to_f2(ms_value, 0).replace("+00:00", "-00:00")
        if pick == 4:                                            # bad offsets / forms
            return r.choice([to_f2(ms_value - ms_value % 1000, 0)[:-6] + bad for bad in
                             ("+24:00", "+05:60", "+0200", "+2:00", "Z ", "z", "")])
        if pick == 5:
            return str(ms_value) + r.choice([".0", "\r", " ", "e0"])
        if pick == 6:
            return r.choice(["", "-5", "1.79e12", str(ms_value)[:-1] + "x"])
        if pick == 7:                                            # an exact window boundary
            return str(r.choice([self.window_start, self.window_start - 1, self.window_end - 1,
                                 self.window_end, self.window_start - 7 * DAY,
                                 self.window_start - 7 * DAY - 1]))
        if pick == 8:
            return str(10 ** r.randint(19, 30) + ms_value)       # too long for 64 bits
        return to_f1(ms_value - ms_value % 1000) if ms_value % 1000 == 0 else str(ms_value)

    def id_variant(self, value):
        r = self.r
        return r.choice([value.upper(), " " + value, value + " ", value + "\r", value, value])

    def quote(self, fields):
        return [f'"{f}"' if '"' not in f and self.chance(3) else f for f in fields]

    def break_line(self, fields):
        """A malformed line (wrong count, stray quote) or a blank one."""
        r = self.r
        pick = r.randrange(6)
        if pick == 0:
            return ",".join(fields[:-1])
        if pick == 1:
            return ",".join(fields + ["x"])
        if pick == 2:
            at = r.randrange(len(fields))
            fields = list(fields)
            fields[at] = fields[at][:1] + '"' + fields[at][1:]
            return ",".join(fields)
        if pick == 3:
            return '"' + ",".join(fields)
        if pick == 4:
            return ""
        return ",".join(fields) + '"'

    # -- events
    def events(self, lines, user_times):
        r = self.r
        out = []
        for line in lines:
            fields = [unquote_simple(f) for f in line.split(",")]
            if len(fields) != 7:
                out.append(line)
                continue
            event_id, user, ts, ingest, kind, product, price = fields
            ts_ms = loose_ms(ts)
            if ts_ms is not None and user:
                user_times.setdefault(user, []).append(ts_ms)
            if ts_ms is not None and self.chance(2):
                ts = self.ts_variant(ts_ms)
            if ts_ms is not None and self.chance(0.5):          # ingest exactly at the 24 h edge
                edge = ts_ms - ts_ms % 1000 + r.choice([DAY, DAY + 1000, -1000, 0])
                ingest = to_f1(edge)
            if self.chance(0.5):
                user = self.id_variant(user)
            if self.chance(0.5):
                kind = r.choice([kind.capitalize(), kind + " ", "purchase", "cart", "view", ""])
            if kind == "purchase" and self.chance():
                price = r.choice(["0", "00", "1", "0250", "10000000", "10000001", "+5", "5\r", "", "٥", " 5"])
            if self.chance(0.3):
                product = r.choice([product.upper(), "", " " + product, "p99999"])
            fields = [event_id, user, ts, ingest, kind, product, price]
            emitted = [fields]
            if self.chance(2):                                  # duplicates moving the earliest copy
                for _ in range(r.randint(1, 3)):
                    copy = list(fields)
                    ingest_ms = loose_ms(fields[3])
                    if ingest_ms is not None:
                        copy[3] = to_f1(ingest_ms - ingest_ms % 1000 + r.choice([-2000, -1000, 0, 1000, 600000]))
                    if self.chance(2):
                        copy[1] = r.choice(list(user_times) or [user])
                    if self.chance(2) and ts_ms is not None:
                        copy[2] = self.ts_variant(ts_ms)
                    emitted.append(copy)
            for copy in emitted:
                text = ",".join(self.quote(copy))
                if self.chance(0.2):
                    text = self.break_line(copy)
                if self.chance(0.2):
                    text += "\r"
                out.append(text)
        return out

    # -- clicks, aimed at real session starts
    def clicks(self, lines, user_times):
        r = self.r
        out = []
        users = list(user_times)
        for line in lines:
            fields = [unquote_simple(f) for f in line.split(",")]
            if len(fields) != 4:
                out.append(line)
                continue
            click_id, user, campaign, ts = fields
            if users and self.chance(3):                        # aim at an event time exactly
                user = r.choice(users)
                target = r.choice(user_times[user])
                ts = str(target - r.choice([0, -1, 7 * DAY, 7 * DAY + 1, 1, 30 * MINUTE]))
            elif (ts_ms := loose_ms(ts)) is not None and self.chance():
                ts = self.ts_variant(ts_ms)
            if self.chance(0.3):
                campaign = r.choice([campaign.upper(), " " + campaign, "c_0", ""])
            if self.chance(0.5):
                user = self.id_variant(user)
            fields = [click_id, user, campaign, ts]
            copies = 1 + (r.randint(1, 2) if self.chance() else 0)
            if self.chance(0.5):                                # same-ms twin with another id
                out.append(",".join(self.quote([click_id + "a", user, campaign, ts])))
            for _ in range(copies):
                text = ",".join(self.quote(fields))
                if self.chance(0.2):
                    text = self.break_line(fields)
                out.append(text)
        return out

    # -- links: extra merges, chains, self-links, dirt
    def links(self, lines, user_times):
        r = self.r
        out = list(lines)
        users = list(user_times)
        for _ in range(int(len(users) * self.rate)):
            a, b = r.choice(users), r.choice(users)
            out.append(f"{a},{b}")
        if users and self.chance(5):                            # a long chain through new IDs
            chain = [r.choice(users)] + [f"zz{n}" for n in range(r.randint(10, 300))] + [r.choice(users)]
            pairs = [f"{chain[n]},{chain[n + 1]}" for n in range(len(chain) - 1)]
            r.shuffle(pairs)
            out += pairs
        for _ in range(int(len(users) * self.rate * 0.3)):
            u = r.choice(users) if users else "u"
            out.append(r.choice([f"{u},{u}", f"{u},", f",{u}", f"{u},{u.upper()}", f"{u},{u}\r",
                                 f'"{u}","A{u}"', "user_id_a,user_id_b"]))
        r.shuffle(out)
        return out


def write_split(r, folder, header, lines):
    """Spread lines over 1-8 files, some header-only, some zero-byte, maybe no final newline."""
    os.makedirs(folder, exist_ok=True)
    count = r.randint(1, 8)
    buckets = [[] for _ in range(count)]
    for line in lines:
        buckets[r.randrange(count)].append(line)
    for n, bucket in enumerate(buckets):
        path = os.path.join(folder, f"part-{n:05d}.csv")
        if not bucket and r.random() < 0.3:
            open(path, "wb").close()
            continue
        text = "\n".join([header] + bucket)
        if r.random() > 0.1:
            text += "\n"
        with open(path, "wb") as handle:
            handle.write(text.encode("utf-8"))


def make(src, dst, seed):
    with open(os.path.join(src, "manifest.json")) as handle:
        manifest = json.load(handle)
    window = (loose_ms(manifest["window_start"]), loose_ms(manifest["window_end"]))
    fuzzer = Fuzzer(seed, window)
    shutil.rmtree(dst, ignore_errors=True)
    os.makedirs(dst)
    for name in ("manifest.json", "products.csv", "campaigns.csv"):
        shutil.copy(os.path.join(src, name), os.path.join(dst, name))
    tables = {}
    for folder in ("events", "clicks", "links"):
        header, lines = None, []
        for name in sorted(os.listdir(os.path.join(src, folder))):
            file_header, file_lines = read_lines(os.path.join(src, folder, name))
            header = header or file_header
            lines += file_lines
        tables[folder] = (header, lines)
    user_times = {}
    events = fuzzer.events(tables["events"][1], user_times)
    clicks = fuzzer.clicks(tables["clicks"][1], user_times)
    links = fuzzer.links(tables["links"][1], user_times)
    for folder, lines in (("events", events), ("clicks", clicks), ("links", links)):
        fuzzer.r.shuffle(lines)
        write_split(fuzzer.r, os.path.join(dst, folder), tables[folder][0], lines)


# ---------------------------------------------------------------- driver

def run_one(command, data):
    output = tempfile.mkdtemp()
    scratch = tempfile.mkdtemp()
    env = dict(os.environ, DATA_DIR=data, OUTPUT_DIR=output, SCRATCH_DIR=scratch)
    process = subprocess.run(command, env=env, capture_output=True, text=True, timeout=900)
    shutil.rmtree(scratch, ignore_errors=True)
    return process, output


def check(output):
    problems = []
    tables = {t: compare.load(output, t, problems) for t in compare.TABLES}
    problems += compare.invariants(tables["user_daily"], tables["campaign_daily"], "output")
    for table in compare.TABLES:
        compare.keyed(tables[table], table, "output", problems)
    return tables, problems


def main(argv):
    if argv[:1] == ["make"]:
        make(argv[1], argv[2], int(argv[3]))
        return 0
    if argv[:1] != ["run"]:
        print(__doc__)
        return 2
    src, count = argv[1], int(argv[2])
    rest = argv[3:]
    commands = []
    while rest and rest[0] == "--":
        rest = rest[1:]
        end = rest.index("--") if "--" in rest else len(rest)
        commands.append(rest[:end])
        rest = rest[end:]
    failures = 0
    keep = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fuzz-failures")
    for seed in range(1, count + 1):
        data = tempfile.mkdtemp()
        make(src, data, seed)
        verdict, outputs = "ok", []
        for command in commands:
            process, output = run_one(command, data)
            outputs.append(output)
            if process.returncode != 0:
                verdict = f"CRASH ({' '.join(command[-1:])}): {process.stderr.strip()[-300:]}"
                break
            try:
                tables, problems = check(output)
            except compare.Unreadable as error:
                verdict = f"UNREADABLE: {error}"
                break
            if problems:
                verdict = "INVARIANT: " + "; ".join(problems[:3])
                break
        if verdict == "ok" and len(outputs) == 2:
            for table in compare.TABLES:
                a = compare.keyed(compare.load(outputs[0], table, []), table, "a", [])
                b = compare.keyed(compare.load(outputs[1], table, []), table, "b", [])
                same, lines = compare.diff(a, b, table)
                if not same:
                    verdict = "DIFFER: " + " | ".join(lines[:3])
                    break
        print(f"seed {seed:4d}: {verdict}", flush=True)
        if verdict != "ok":
            failures += 1
            os.makedirs(keep, exist_ok=True)
            shutil.copytree(data, os.path.join(keep, f"{os.path.basename(src)}-fuzz{seed}"), dirs_exist_ok=True)
        shutil.rmtree(data, ignore_errors=True)
        for output in outputs:
            shutil.rmtree(output, ignore_errors=True)
    print(f"\n{count - failures}/{count} fuzzed datasets ok")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
