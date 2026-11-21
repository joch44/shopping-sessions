#!/usr/bin/env python3
"""Shopping Sessions comparator.

    compare.py ACTUAL EXPECTED     schema + invariants on ACTUAL, then an exact keyed diff
    compare.py ACTUAL              schema + invariants only (no answer available, e.g. full size)
    compare.py --hash ACTUAL       schema + invariants, then a canonical checksum per table

ACTUAL is an output folder (user_daily/ and campaign_daily/ holding *.parquet).
EXPECTED is either an output folder like ACTUAL, or a folder holding
user_daily.csv and campaign_daily.csv (the fixtures' expected/ and dev-answer/).

Rules for reading output folders follow rules.md "Outputs": only *.parquet files
directly in the folder, skipping names starting with "." or "_" and symlinks.

Exit codes: 0 match / all checks pass, 1 mismatch or failed check, 2 unreadable input.
Needs pyarrow only.
"""
import csv
import datetime
import hashlib
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

TABLES = {
    "user_daily": {
        "key": ("user_id", "day"),
        "columns": {
            "user_id": "string", "day": "date", "sessions": "int", "events": "int",
            "revenue_cents": "int", "top_category": "string?",
        },
    },
    "campaign_daily": {
        "key": ("campaign_id", "day"),
        "columns": {
            "campaign_id": "string", "channel": "string", "day": "date", "sessions": "int",
            "purchasing_sessions": "int", "revenue_cents": "int", "assisted_sessions": "int",
        },
    },
}
SHOW = 10      # differences printed per kind


class Unreadable(Exception):
    pass


# ---------------------------------------------------------------- loading

def type_ok(arrow_type, kind):
    if pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type
    if kind.startswith("string"):
        return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) \
            or pa.types.is_string_view(arrow_type)
    if kind == "date":
        return pa.types.is_date32(arrow_type)      # Parquet DATE
    if kind == "int":
        return pa.types.is_integer(arrow_type)
    raise AssertionError(kind)


def load_parquet_table(folder, table, problems):
    spec = TABLES[table]["columns"]
    path = os.path.join(folder, table)
    if os.path.islink(path) or not os.path.isdir(path):
        raise Unreadable(f"{path}: not a directory")
    files = sorted(
        name for name in os.listdir(path)
        if name.endswith(".parquet") and not name.startswith((".", "_"))
        and os.path.isfile(os.path.join(path, name)) and not os.path.islink(os.path.join(path, name))
    )
    if not files:
        raise Unreadable(f"{path}: no parquet files")
    rows = []
    for name in files:
        file_path = os.path.join(path, name)
        try:
            data = pq.read_table(file_path)
        except Exception as error:     # noqa: BLE001 - report any reader failure
            raise Unreadable(f"{file_path}: {error}") from error
        names = data.schema.names
        if sorted(names) != sorted(spec) or len(set(names)) != len(names):
            problems.append(f"{table}/{name}: columns {names}, expected exactly {sorted(spec)}")
            continue
        for column, kind in spec.items():
            arrow_type = data.schema.field(column).type
            if not type_ok(arrow_type, kind):
                problems.append(f"{table}/{name}: column {column} has type {arrow_type}, expected {kind}")
            if not kind.endswith("?") and data.column(column).null_count:
                problems.append(f"{table}/{name}: column {column} has "
                                f"{data.column(column).null_count} nulls")
        columns = [data.column(column).to_pylist() for column in spec]
        rows.extend(zip(*columns))
    return rows


def load_csv_table(folder, table):
    spec = TABLES[table]["columns"]
    path = os.path.join(folder, f"{table}.csv")
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None or sorted(header) != sorted(spec):
            raise Unreadable(f"{path}: header {header}")
        order = [header.index(column) for column in spec]
        rows = []
        for record in reader:
            values = []
            for column, at in zip(spec, order):
                text = record[at]
                kind = spec[column]
                if kind == "date":
                    values.append(datetime.date.fromisoformat(text))
                elif kind == "int":
                    values.append(int(text))
                elif kind == "string?" and text == "":
                    values.append(None)
                else:
                    values.append(text)
            rows.append(tuple(values))
    return rows


def load(folder, table, problems):
    if os.path.isfile(os.path.join(folder, f"{table}.csv")):
        return load_csv_table(folder, table)
    return load_parquet_table(folder, table, problems)


# ---------------------------------------------------------------- checks

def keyed(rows, table, label, problems):
    columns = list(TABLES[table]["columns"])
    key_at = [columns.index(column) for column in TABLES[table]["key"]]
    by_key = {}
    duplicates = 0
    for row in rows:
        key = tuple(row[at] for at in key_at)
        if key in by_key:
            duplicates += 1
            if duplicates <= SHOW:
                problems.append(f"{label} {table}: duplicate key {key}")
        by_key[key] = row
    if duplicates > SHOW:
        problems.append(f"{label} {table}: {duplicates} duplicate keys in all")
    return by_key


def invariants(user_rows, campaign_rows, label):
    """rules.md "A quick self-check", plus organic never assisting."""
    problems = []
    u_sessions = sum(row[2] for row in user_rows)
    u_revenue = sum(row[4] for row in user_rows)
    c_sessions = sum(row[3] for row in campaign_rows)
    c_revenue = sum(row[5] for row in campaign_rows)
    if u_sessions != c_sessions:
        problems.append(f"{label}: sessions sum {u_sessions} (user_daily) != {c_sessions} (campaign_daily)")
    if u_revenue != c_revenue:
        problems.append(f"{label}: revenue sum {u_revenue} (user_daily) != {c_revenue} (campaign_daily)")
    assisted = sum(row[6] for row in campaign_rows)
    credited = sum(row[3] for row in campaign_rows if row[0] != "organic")
    if assisted < credited:
        problems.append(f"{label}: assisted_sessions sum {assisted} < credited non-organic sessions {credited}")
    for row in campaign_rows:
        if row[0] == "organic" and (row[6] != 0 or row[1] != "organic"):
            problems.append(f"{label}: organic row {row} must have channel organic and 0 assisted")
        if row[4] > row[3]:
            problems.append(f"{label}: purchasing_sessions > sessions in {row}")
        if min(row[3:]) < 0:
            problems.append(f"{label}: negative count in {row}")
    for row in user_rows:
        if row[2] < 1 or row[3] < row[2] or row[4] < 0:
            problems.append(f"{label}: impossible counts in {row}")
        if (row[5] is None) != (row[4] == 0):
            # revenue 0 <-> no purchases (every valid price is at least 1).
            problems.append(f"{label}: top_category {row[5]!r} with revenue {row[4]} in {row}")
    return problems[: SHOW * 3]


def diff(actual, expected, table):
    columns = list(TABLES[table]["columns"])
    lines = []
    missing = sorted((key for key in expected if key not in actual), key=sort_key)
    extra = sorted((key for key in actual if key not in expected), key=sort_key)
    changed = sorted((key for key in expected if key in actual and actual[key] != expected[key]),
                     key=sort_key)
    lines.append(f"{table}: {len(actual)} actual vs {len(expected)} expected rows; "
                 f"{len(missing)} missing, {len(extra)} extra, {len(changed)} differ")
    for key in missing[:SHOW]:
        lines.append(f"  missing {key}: expected {dict(zip(columns, expected[key]))}")
    for key in extra[:SHOW]:
        lines.append(f"  extra   {key}: actual   {dict(zip(columns, actual[key]))}")
    for key in changed[:SHOW]:
        fields = [f"{column} {got!r} != {want!r}" for column, got, want
                  in zip(columns, actual[key], expected[key]) if got != want]
        lines.append(f"  differ  {key}: " + "; ".join(fields))
    return not (missing or extra or changed), lines


def sort_key(key):
    return tuple("" if value is None else str(value) for value in key)


def checksum(rows):
    digest = hashlib.sha256()
    for row in sorted(rows, key=sort_key):
        digest.update(repr(tuple("\\N" if v is None else str(v) for v in row)).encode())
        digest.update(b"\n")
    return digest.hexdigest()[:16]


# ---------------------------------------------------------------- main

def main(argv):
    want_hash = "--hash" in argv
    paths = [arg for arg in argv if arg != "--hash"]
    if len(paths) not in (1, 2):
        print(__doc__, file=sys.stderr)
        return 2
    actual_dir = paths[0]
    expected_dir = paths[1] if len(paths) == 2 else None

    problems = []
    try:
        actual = {table: load(actual_dir, table, problems) for table in TABLES}
        expected = ({table: load(expected_dir, table, []) for table in TABLES}
                    if expected_dir else None)
    except (Unreadable, OSError, ValueError) as error:
        print(f"UNREADABLE: {error}")
        return 2

    problems += invariants(actual["user_daily"], actual["campaign_daily"], "actual")
    ok = not problems
    for problem in problems:
        print(f"CHECK FAILED {problem}")

    for table in TABLES:
        actual_keyed = keyed(actual[table], table, "actual", problems_dup := [])
        for problem in problems_dup:
            print(f"CHECK FAILED {problem}")
        ok &= not problems_dup
        if expected is not None:
            expected_keyed = keyed(expected[table], table, "expected", [])
            same, lines = diff(actual_keyed, expected_keyed, table)
            ok &= same
            print("\n".join(lines))
        else:
            print(f"{table}: {len(actual[table])} rows")
        if want_hash:
            print(f"{table}: checksum {checksum(actual[table])}")

    print("MATCH" if ok and expected is not None else "PASS" if ok else "DIFFER" if expected else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
