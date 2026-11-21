#!/usr/bin/env python3
"""Profiles generated datasets for DATA_PROFILE.md.

    profile_data.py DATASET [DATASET ...]     prints one JSON object per dataset

Raw-file facts (bytes, files, ID shapes, timestamp forms, quoting, malformed
lines) are measured directly. Person, session and output facts come from the
reference's output, which is run on each dataset (correct by construction).
Facts are for design decisions only, never for hard-coding.
"""
import collections
import json
import os
import statistics
import subprocess
import sys
import tempfile
from multiprocessing import Pool

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "reference"))
import reference as ref  # noqa: E402  (parsing helpers only: the validated dialect)

FIELDS = {"events": 7, "clicks": 4, "links": 2}
ID_FIELDS = {"events": {0: "event_id", 1: "user_id", 5: "product_id"},
             "clicks": {0: "click_id", 1: "user_id", 2: "campaign_id"},
             "links": {0: "link_id", 1: "link_id"}}
TS_FIELDS = {"events": 2, "clicks": 3}


def ts_form(value):
    if value is None:
        return "empty"
    if value.isdigit():
        return "F3"
    if len(value) == 20 and value.endswith(b"Z"):
        return "F1" if ref.parse_timestamp(value) is not None else "bad"
    if len(value) == 25:
        return "F2" if ref.parse_timestamp(value) is not None else "bad"
    return "bad"


def scan_file(task):
    kind, path = task
    stats = collections.Counter()
    lengths = collections.defaultdict(collections.Counter)
    stats["bytes"] = os.path.getsize(path)
    with open(path, "rb") as handle:
        first = True
        for line in handle:
            if line.endswith(b"\n"):
                line = line[:-1]
            if first:
                first = False
                continue
            stats["lines"] += 1
            stats["quoted_fields"] += line.count(b'"') // 2
            values = ref.parse_line(line, FIELDS[kind])
            if values is None:
                stats["malformed"] += 1
                continue
            stats["rows"] += 1
            stats["fields"] += FIELDS[kind]
            for at, name in ID_FIELDS[kind].items():
                lengths[name][len(values[at] or b"")] += 1
            if kind in TS_FIELDS:
                stats["ts_" + ts_form(values[TS_FIELDS[kind]])] += 1
            if kind == "events":
                stats["type_" + (values[4] or b"").decode("utf-8", "replace")] += 1
    return kind, path, stats, {name: dict(counter) for name, counter in lengths.items()}


def percentile(sorted_values, p):
    if not sorted_values:
        return 0
    return sorted_values[min(len(sorted_values) - 1, int(p / 100 * len(sorted_values)))]


def profile(dataset, workers):
    tasks = [(kind, os.path.join(dataset, kind, name))
             for kind in FIELDS for name in sorted(os.listdir(os.path.join(dataset, kind)))]
    totals = {kind: collections.Counter() for kind in FIELDS}
    lengths = collections.defaultdict(collections.Counter)
    file_sizes = collections.defaultdict(list)
    with Pool(workers) as pool:
        for kind, path, stats, file_lengths in pool.imap_unordered(scan_file, tasks):
            totals[kind].update(stats)
            file_sizes[kind].append(stats["bytes"])
            for name, counter in file_lengths.items():
                lengths[name].update({int(k): v for k, v in counter.items()})

    result = {"dataset": os.path.basename(dataset.rstrip("/"))}
    with open(os.path.join(dataset, "manifest.json")) as handle:
        result["window"] = json.load(handle)
    for kind in FIELDS:
        t = totals[kind]
        sizes = sorted(file_sizes[kind])
        result[kind] = {
            "files": len(sizes), "header_only_files": sum(1 for s in sizes if s < 100),
            "bytes": t["bytes"], "largest_file_share": round(sizes[-1] / max(1, t["bytes"]), 3),
            "rows": t["rows"], "bytes_per_line": round(t["bytes"] / max(1, t["lines"]), 1),
            "malformed_share": round(t["malformed"] / max(1, t["lines"]), 5),
            "quoted_field_share": round(t["quoted_fields"] / max(1, t["fields"]), 4),
        }
        if kind in TS_FIELDS:
            result[kind]["ts_forms"] = {k[3:]: round(v / max(1, t["rows"]), 4)
                                        for k, v in sorted(t.items()) if k.startswith("ts_")}
        if kind == "events":
            result[kind]["types"] = {k[5:]: round(v / max(1, t["rows"]), 4)
                                     for k, v in t.most_common() if k.startswith("type_")}
    result["id_lengths"] = {name: {"min": min(c), "max": max(c), "mode": c.most_common(1)[0][0]}
                            for name, c in sorted(lengths.items()) if c}

    # Links: people and ID-group sizes (the validated union-find).
    people = ref.resolve_people(dataset)
    groups = collections.Counter(people.values())
    group_sizes = sorted(groups.values())
    result["links"]["linked_ids"] = len(people)
    result["links"]["linked_people"] = len(groups)
    result["links"]["ids_per_linked_person"] = {
        "median": percentile(group_sizes, 50), "p99": percentile(group_sizes, 99),
        "max": group_sizes[-1] if group_sizes else 0, "top5": group_sizes[-5:]}

    # Run the reference and profile its output.
    with tempfile.TemporaryDirectory() as out, tempfile.TemporaryDirectory() as scratch:
        env = dict(os.environ, DATA_DIR=dataset, OUTPUT_DIR=out, SCRATCH_DIR=scratch, NCPU=str(workers))
        log = subprocess.run([sys.executable, os.path.join(ROOT, "reference", "reference.py")],
                             env=env, capture_output=True, text=True, check=True).stderr
        result["reference_log"] = [line.split("] ", 1)[1] for line in log.splitlines() if "] " in line][1:-1]
        import pyarrow.parquet as pq
        users = pq.read_table(os.path.join(out, "user_daily")).to_pydict()
        campaigns = pq.read_table(os.path.join(out, "campaign_daily")).to_pydict()

    per_person_events = collections.Counter()
    per_person_sessions = collections.Counter()
    for person, sessions, events in zip(users["user_id"], users["sessions"], users["events"]):
        per_person_events[person] += events
        per_person_sessions[person] += sessions
    events_sorted = sorted(per_person_events.values())
    sessions_sorted = sorted(per_person_sessions.values())
    total_events = sum(events_sorted)
    result["people"] = {
        "people_with_sessions": len(events_sorted),
        "events_per_person": {"median": percentile(events_sorted, 50), "p99": percentile(events_sorted, 99),
                              "max": events_sorted[-1]},
        "sessions_per_person": {"median": percentile(sessions_sorted, 50),
                                "p99": percentile(sessions_sorted, 99), "max": sessions_sorted[-1]},
        "top5_people_event_share": [round(v / total_events, 4) for v in events_sorted[-5:]],
        "largest_people": [(p, per_person_events[p], per_person_sessions[p])
                           for p, _ in per_person_events.most_common(5)],
    }
    total_sessions = sum(campaigns["sessions"])
    organic = sum(s for c, s in zip(campaigns["campaign_id"], campaigns["sessions"]) if c == "organic")
    result["output"] = {
        "user_daily_rows": len(users["user_id"]), "campaign_daily_rows": len(campaigns["campaign_id"]),
        "user_daily_rows_per_event_row": round(len(users["user_id"]) / max(1, result["events"]["rows"]), 4),
        "sessions": total_sessions, "events_per_session": round(total_events / max(1, total_sessions), 2),
        "credited_share": round(1 - organic / max(1, total_sessions), 4),
        "days": len(set(users["day"])),
        "top_category_null_share": round(sum(1 for c in users["top_category"] if c is None)
                                         / max(1, len(users["top_category"])), 4),
    }
    return result


def main(argv):
    workers = int(os.environ.get("NCPU") or 16)
    for dataset in argv:
        print(json.dumps(profile(dataset, workers), default=str), flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
