#!/usr/bin/env python3
"""Builds the hand-made adversarial corpus: tests/corpus/cases/<name>/{input,expected}.

Each case targets one rule. Expected rows are written BY HAND from rules.md in
the case definitions below; they are never computed by any implementation.
Layout matches the organizers' fixtures, so the same runners work on both.

Defaults (same as the fixtures): window 2026-10-01 to 2026-10-15, products
p1 (shoes) and p2 (books), campaigns c_1 (search) and c_2 (social); p9 is
not a product.

    python3 tests/corpus/build.py        # (re)writes tests/corpus/cases/
"""
import datetime
import json
import os
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "cases")
EVENT_HEADER = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CLICK_HEADER = b"click_id,user_id,campaign_id,ts"
LINK_HEADER = b"user_id_a,user_id_b"
USER_HEADER = "user_id,day,sessions,events,revenue_cents,top_category"
CAMPAIGN_HEADER = "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions"


def T(day, hms="10:00:00"):
    """F1 timestamp in October 2026: T(3) -> 2026-10-03T10:00:00Z."""
    return f"2026-10-{day:02d}T{hms}Z"


def ms(text):
    """Epoch ms of an F1 string (for writing F3 values and ingest defaults)."""
    when = datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    return int(when.timestamp()) * 1000


def plus(text, seconds):
    when = datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(seconds=seconds)
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def ev(event_id, user, ts, ingest=None, kind="view", product="p1", price=""):
    """One event line. ingest defaults to ts + 60 s (ts must then be F1)."""
    if ingest is None:
        ingest = plus(ts, 60)
    return f"{event_id},{user},{ts},{ingest},{kind},{product},{price}"


def ck(click_id, user, campaign, ts):
    return f"{click_id},{user},{campaign},{ts}"


CASES = []


def case(name, catches, events=(), clicks=(), links=(), users=(), campaigns=(),
         files=None, window=("2026-10-01T00:00:00Z", "2026-10-15T00:00:00Z"),
         products=(("p1", "shoes"), ("p2", "books")),
         campaign_list=(("c_1", "search"), ("c_2", "social"))):
    """events/clicks/links: lines (str or bytes) of one part file each.
    files: {relative path: raw bytes} written as-is, replacing the defaults.
    users/campaigns: expected rows as tuples, written by hand."""
    CASES.append(dict(name=name, catches=catches, events=events, clicks=clicks, links=links,
                      users=users, campaigns=campaigns, files=files or {}, window=window,
                      products=products, campaign_list=campaign_list))


def raw(lines, header):
    body = [header] + [line if isinstance(line, bytes) else line.encode() for line in lines]
    return b"\n".join(body) + b"\n"


ORG = lambda day, sessions, purchasing=0, revenue=0: ("organic", "organic", day, sessions, purchasing, revenue, 0)  # noqa: E731
D3 = "2026-10-03"
D4 = "2026-10-04"

# ============================================================== CSV dialect

case("csv_quoted_key_dedups_with_unquoted",
     "`\"e1\"` and `e1` are one event_id; the earliest copy (the quoted one) wins, user u2",
     events=[ev("e1", "u1", T(3), T(3, "10:05:00")),
             ev('"e1"', "u2", T(3), T(3, "10:01:00"))],
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_quote_inside_field_is_malformed",
     "`p\"1` (quote not around the whole field) drops the line",
     events=[ev("e1", "u1", T(3), product='p"1'), ev("e2", "u2", T(3))],
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_lone_and_triple_quotes",
     "A field `\"` or `\"\"\"` is malformed; `\"\"` is an empty (null) user, dropped",
     events=[ev("e1", '"', T(3)), ev("e2", '"""', T(3)), ev("e3", '""', T(3)), ev("e4", "u1", T(3))],
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_quotes_around_every_field",
     "Quotes on every field, including a quoted empty price on a view",
     events=['"e1","u1","2026-10-03T10:00:00Z","2026-10-03T10:01:00Z","view","p1",""',
             '"e2","u1","2026-10-03T10:05:00Z","2026-10-03T10:06:00Z","purchase","p2","700"'],
     users=[("u1", D3, 1, 2, 700, "books")], campaigns=[ORG(D3, 1, 1, 700)])

case("csv_no_trailing_newline",
     "The last line of a file has no \\n and still counts",
     files={"events/part-00000.csv": raw([ev("e1", "u1", T(3))], EVENT_HEADER)
            + ev("e2", "u1", T(3, "10:10:00")).encode()},
     users=[("u1", D3, 1, 2, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_zero_byte_and_header_only_files",
     "Zero-byte and header-only files anywhere are just empty",
     files={"events/part-00000.csv": b"",
            "events/part-00001.csv": EVENT_HEADER + b"\n",
            "events/part-00002.csv": raw([ev("e1", "u1", T(3))], EVENT_HEADER),
            "clicks/part-00000.csv": b"",
            "links/part-00000.csv": b"",
            "links/part-00001.csv": LINK_HEADER},
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_blank_lines_and_comma_only_lines",
     "Empty lines and `,,,,,,` (empty event_id) are dropped; `\\n\\n` at the end too",
     files={"events/part-00000.csv": raw(["", ev("e1", "u1", T(3)), ",,,,,,", ""], EVENT_HEADER) + b"\n"},
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_header_text_as_data_in_events",
     "Only the first line is a header; a repeated header line is a data row (here invalid)",
     events=["event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", ev("e1", "u1", T(3))],
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("csv_carriage_returns",
     "\\r is data: `view` with price `\\r` kept; purchase `1000\\r` dropped; `e3\\r` and `e3` are two "
     "events; `u1\\r` is its own person",
     events=[ev("e1", "u1", T(3)) + "\r",
             ev("e2", "u1", T(3, "10:05:00"), kind="purchase", price="1000\r"),
             ev("e3\r", "u1", T(3, "10:10:00")),
             ev("e3", "u1", T(3, "10:10:00")),
             ev("e4", "u1\r", T(3))],
     users=[("u1", D3, 1, 3, 0, ""), ("u1\r", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 2)])

case("csv_first_line_not_a_header_is_still_skipped",
     "The first line is skipped whatever it holds, even a valid-looking event",
     files={"events/part-00000.csv": (ev("e0", "u9", T(3)) + "\n" + ev("e1", "u1", T(3)) + "\n").encode()},
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

# ============================================================== timestamps

case("ts_feb29_leap_2028_valid",
     "Feb 29 exists in 2028; Feb 30 does not",
     window=("2028-02-20T00:00:00Z", "2028-03-05T00:00:00Z"),
     events=[ev("e1", "u1", "2028-02-29T12:00:00Z", "2028-02-29T12:01:00Z"),
             ev("e2", "u2", "2028-02-30T12:00:00Z", "2028-02-29T12:01:00Z")],
     users=[("u1", "2028-02-29", 1, 1, 0, "")], campaigns=[ORG("2028-02-29", 1)])

case("ts_feb29_non_leap_2026_invalid",
     "Feb 29 2026 is impossible; the next day is fine",
     window=("2026-02-20T00:00:00Z", "2026-03-06T00:00:00Z"),
     events=[ev("e1", "u1", "2026-02-29T10:00:00Z", "2026-02-28T10:01:00Z"),
             ev("e2", "u2", "2026-03-01T10:00:00Z", "2026-03-01T10:01:00Z")],
     users=[("u2", "2026-03-01", 1, 1, 0, "")], campaigns=[ORG("2026-03-01", 1)])

case("ts_offsets_that_change_the_date",
     "+12:00 and -10:00 offsets move the UTC date; the UTC date is the day",
     events=[ev("e1", "u1", "2026-10-04T08:00:00+12:00", T(3, "20:01:00")),    # Oct 3 20:00Z
             ev("e2", "u2", "2026-10-02T14:30:00-10:00", T(3, "00:31:00")),    # Oct 3 00:30Z
             ev("e3", "u3", "2026-10-03T23:30:00-01:00", T(4, "00:31:00"))],   # Oct 4 00:30Z
     users=[("u1", D3, 1, 1, 0, ""), ("u2", D3, 1, 1, 0, ""), ("u3", D4, 1, 1, 0, "")],
     campaigns=[ORG(D3, 2), ORG(D4, 1)])

case("ts_offset_sessions_merge_with_f1",
     "An F2 time and an F1 time 20 min apart in UTC share a session",
     events=[ev("e1", "u1", "2026-10-03T12:00:00+02:00", T(3, "10:01:00")),     # 10:00Z
             ev("e2", "u1", T(3, "10:20:00"))],
     users=[("u1", D3, 1, 2, 0, "")], campaigns=[ORG(D3, 1)])

case("ts_f3_leading_zeros_and_huge",
     "F3 with many leading zeros is valid; 26 digits is a number outside the window, dropped, no crash",
     events=[ev("e1", "u1", "0001791071890000", T(3, "23:59:00")),          # 2026-10-03T23:58:10Z
             ev("e2", "u2", "000000000000000001791071890000", T(3, "23:59:00")),
             ev("e3", "u3", "99999999999999999999999999", T(3, "23:59:00")),
             ev("e4", "u4", "18446744073709551616", T(3, "23:59:00"))],     # 2^64
     users=[("u1", D3, 1, 1, 0, ""), ("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 2)])

case("ts_bad_forms",
     "Every bad form is dropped; only u0 survives",
     events=[ev("e0", "u0", T(3))] + [
         ev(f"e{n}", f"u{n}", bad, T(3, "10:01:00")) for n, bad in enumerate([
             "2026-10-03T10:00:00.000Z", "2026-10-03T10:00:00z", "2026-10-03t10:00:00Z",
             "2026-10-03 10:00:00Z", "2026-10-03T10:00:00", "1791071890000.0", "1.79e12", "-5",
             "2026-13-03T10:00:00Z", "2026-10-03T24:00:00Z", "2026-10-03T10:00:60Z",
             "2026-09-31T10:00:00Z", "2026-10-3T10:00:00Z", "+1791071890000", " 2026-10-03T10:00:00Z",
             "2026-10-03T10:00:00Z ", "2026-10-03T10:00:00+02", "2026-10-03T10:00:00+2:00",
             "2026-10-03T10:00:00+02:00Z", "2026-10-03T10:00:00UTC", "２026-10-03T10:00:00Z",
             "2026-00-03T10:00:00Z", "2026-10-00T10:00:00Z", "2026-10-03T10:60:00Z",
             "2026-10-03T10:00:00-24:00", "2026-10-03T10:00:00+00:60", "1791071890000 ",
             "2026/10/03T10:00:00Z", "0x1A0B", "", "2026-10-03T10:00:00+02:00:00"], start=1)],
     users=[("u0", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("ts_millisecond_day_boundary",
     "A session starting at 23:59:59.999 is that day's; one at 00:00:00.000 is the next",
     events=[ev("e1", "u1", str(ms(T(3, "23:59:59")) + 999), T(4, "00:00:30")),
             ev("e2", "u2", str(ms(T(4, "00:00:00"))), T(4, "00:00:30"))],
     users=[("u1", D3, 1, 1, 0, ""), ("u2", D4, 1, 1, 0, "")], campaigns=[ORG(D3, 1), ORG(D4, 1)])

case("ts_event_window_edges_in_ms",
     "window_start - 1 ms and window_end are out; window_end - 1 ms is in (day Oct 14)",
     events=[ev("e1", "u1", str(ms("2026-10-01T00:00:00Z") - 1), "2026-10-01T00:01:00Z"),
             ev("e2", "u2", str(ms("2026-10-15T00:00:00Z")), "2026-10-15T00:01:00Z"),
             ev("e3", "u3", str(ms("2026-10-15T00:00:00Z") - 1), "2026-10-15T00:01:00Z")],
     users=[("u3", "2026-10-14", 1, 1, 0, "")], campaigns=[ORG("2026-10-14", 1)])

case("ts_window_not_at_midnight",
     "A window starting at 06:30: an event at 06:00 is out, one at 06:30 is in; the day is still the UTC date",
     window=("2026-10-01T06:30:00Z", "2026-10-15T06:30:00Z"),
     events=[ev("e1", "u1", "2026-10-01T06:00:00Z"), ev("e2", "u2", "2026-10-01T06:30:00Z"),
             ev("e3", "u3", "2026-10-15T06:00:00Z")],
     users=[("u2", "2026-10-01", 1, 1, 0, ""), ("u3", "2026-10-15", 1, 1, 0, "")],
     campaigns=[ORG("2026-10-01", 1), ORG("2026-10-15", 1)])

# ============================================================== events: dedup, validation, lateness

case("dedup_earliest_copy_invalid_type_kills_event",
     "Earliest copy has type `Purchase`; the later valid copy does not rescue it",
     events=[ev("e1", "u1", T(3), T(3, "10:01:00"), kind="Purchase", price="500"),
             ev("e1", "u1", T(3), T(3, "10:05:00"), kind="purchase", price="500"),
             ev("e2", "u2", T(3))],
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("dedup_earliest_copy_empty_user_kills_event",
     "Earliest copy has an empty user_id",
     events=[ev("e1", "", T(3), T(3, "10:01:00")), ev("e1", "u1", T(3), T(3, "10:05:00")),
             ev("e2", "u2", T(3))],
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("dedup_malformed_earliest_copy_is_ignored",
     "A malformed copy with the earliest ingest never counts, so the later valid copy is kept",
     events=[ev("e1", "u2", T(3), T(3, "10:01:00")) + ",x",
             ev("e1", "u1", T(3), T(3, "10:05:00"))],
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("dedup_middle_copy_earliest_names_other_user",
     "Three copies; the smallest ingest_ts is in the middle and names u2",
     events=[ev("e1", "u1", T(3), T(3, "10:09:00")), ev("e1", "u2", T(3), T(3, "10:01:00")),
             ev("e1", "u3", T(3), T(3, "10:05:00"))],
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("dedup_across_files_and_ingest_forms",
     "Copies in two files; ingest compared as instants, quoted or not",
     files={"events/part-00000.csv": raw([ev("e1", "u1", T(3), '"2026-10-03T10:07:00Z"')], EVENT_HEADER),
            "events/part-00001.csv": raw([ev("e1", "u2", T(3), "2026-10-03T10:02:00Z")], EVENT_HEADER)},
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("dedup_earliest_copy_is_late",
     "The earliest copy is late (ts 25 h before ingest); a later copy with a different, fine ts does not rescue it",
     events=[ev("e1", "u1", T(3, "09:00:00"), T(4, "10:00:00")),
             ev("e1", "u1", T(4, "10:00:00"), T(4, "10:05:00")),
             ev("e2", "u2", T(3))],
     users=[("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("late_boundaries",
     "Exactly 24 h kept; 24 h + 1 ms dropped (F3 ts); ingest before ts kept",
     events=[ev("e1", "u1", T(3), T(4)),
             ev("e2", "u2", str(ms(T(3)) - 1), T(4)),
             ev("e3", "u3", T(3), T(3, "09:00:00"))],
     users=[("u1", D3, 1, 1, 0, ""), ("u3", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 2)])

case("price_rules",
     "Valid: 1, 10000000, 0250. Invalid: 0, 00, 10000001, +5, -5, 5.0, 1e3, empty, ' 5'. View prices ignored",
     events=[ev("e1", "u1", T(3, "10:00:00"), kind="purchase", price="1"),
             ev("e2", "u1", T(3, "10:01:00"), kind="purchase", price="10000000"),
             ev("e3", "u1", T(3, "10:02:00"), kind="purchase", price="0250")] + [
         ev(f"x{n}", "u1", T(3, "10:03:00"), kind="purchase", price=bad)
         for n, bad in enumerate(["0", "00", "10000001", "+5", "-5", "5.0", "1e3", "", " 5", "5 ", "٥"])] + [
         ev("e4", "u1", T(3, "10:04:00"), kind="view", price="-5"),
         ev("e5", "u1", T(3, "10:05:00"), kind="cart", price="abc")],
     users=[("u1", D3, 1, 5, 10000251, "shoes")], campaigns=[ORG(D3, 1, 1, 10000251)])

case("product_unknown_and_empty",
     "Unknown product `p9` and case variant `P1` are category unknown; an empty product drops the event",
     events=[ev("e1", "u1", T(3, "10:00:00"), kind="purchase", product="p9", price="300"),
             ev("e2", "u1", T(3, "10:01:00"), kind="purchase", product="P1", price="300"),
             ev("e3", "u1", T(3, "10:02:00"), kind="purchase", product="p1", price="500"),
             ev("e4", "u1", T(3, "10:03:00"), kind="purchase", product="", price="900")],
     users=[("u1", D3, 1, 3, 1100, "unknown")], campaigns=[ORG(D3, 1, 1, 1100)])

case("event_type_strict",
     "Only view, cart, purchase exactly",
     events=[ev("e1", "u1", T(3), kind="view"), ev("e2", "u1", T(3), kind="cart"),
             ev("e3", "u1", T(3), kind="View"), ev("e4", "u1", T(3), kind="cart "),
             ev("e5", "u1", T(3), kind=" view"), ev("e6", "u1", T(3), kind="click"),
             ev("e7", "u1", T(3), kind="")],
     users=[("u1", D3, 1, 2, 0, "")], campaigns=[ORG(D3, 1)])

# ============================================================== sessions and days

case("session_gap_chain",
     "10:00, 10:30:00.000 (exactly 30 min: same), 11:00:00.001 (30 min + 1 ms: new)",
     events=[ev("e1", "u1", T(3)), ev("e2", "u1", T(3, "10:30:00")),
             ev("e3", "u1", str(ms(T(3, "11:00:00")) + 1), T(3, "11:01:00"))],
     users=[("u1", D3, 2, 3, 0, "")], campaigns=[ORG(D3, 2)])

case("session_equal_timestamps_unsorted",
     "Three events at 10:00, two at 10:30:00.001, read out of order: two sessions",
     events=[ev("e5", "u1", str(ms(T(3, "10:30:00")) + 1), T(3, "10:31:00")), ev("e1", "u1", T(3)),
             ev("e2", "u1", T(3)), ev("e4", "u1", str(ms(T(3, "10:30:00")) + 1), T(3, "10:31:00")),
             ev("e3", "u1", T(3))],
     users=[("u1", D3, 2, 5, 0, "")], campaigns=[ORG(D3, 2)])

case("session_long_across_midnight",
     "Events every 20 min from 22:00 Oct 3 to 02:00 Oct 4: one session on Oct 3, purchase after midnight counts there",
     events=[ev(f"e{n}", "u1", plus(T(3, "22:00:00"), n * 1200)) for n in range(13)] + [
         ev("p", "u1", T(4, "01:41:00"), kind="purchase", product="p2", price="500")],
     users=[("u1", D3, 1, 14, 500, "books")], campaigns=[ORG(D3, 1, 1, 500)])

case("session_linked_ids_interleave",
     "u1 at 10:00 and 10:40, linked u2 at 10:20: one session of 3; unlinked u3 at 10:20 is apart",
     events=[ev("e1", "u1", T(3)), ev("e2", "u1", T(3, "10:40:00")), ev("e3", "u2", T(3, "10:20:00")),
             ev("e4", "u3", T(3, "10:20:00"))],
     links=["u2,u1"],
     users=[("u1", D3, 1, 3, 0, ""), ("u3", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 2)])

case("top_category_ties_and_sums_over_the_day",
     "u1: two sessions on Oct 3, shoes 300 vs books 200+200: books. u2: unknown 500 ties books 500: books. "
     "u3: unknown 700 beats shoes 500",
     events=[ev("a1", "u1", T(3, "08:00:00"), kind="purchase", product="p1", price="300"),
             ev("a2", "u1", T(3, "12:00:00"), kind="purchase", product="p2", price="200"),
             ev("a3", "u1", T(3, "12:05:00"), kind="purchase", product="p2", price="200"),
             ev("b1", "u2", T(3), kind="purchase", product="p9", price="500"),
             ev("b2", "u2", T(3), kind="purchase", product="p2", price="500"),
             ev("c1", "u3", T(3), kind="purchase", product="p9", price="700"),
             ev("c2", "u3", T(3), kind="purchase", product="p1", price="500")],
     users=[("u1", D3, 2, 3, 700, "books"), ("u2", D3, 1, 2, 1000, "books"),
            ("u3", D3, 1, 2, 1200, "unknown")],
     campaigns=[ORG(D3, 4, 4, 2900)])

case("top_category_byte_order_uppercase_first",
     "Categories `Zebra` and `apple` tie: byte-wise `Zebra` (0x5A) < `apple` (0x61)",
     products=(("p1", "apple"), ("p2", "Zebra")),
     events=[ev("e1", "u1", T(3), kind="purchase", product="p1", price="400"),
             ev("e2", "u1", T(3), kind="purchase", product="p2", price="400")],
     users=[("u1", D3, 1, 2, 800, "Zebra")], campaigns=[ORG(D3, 1, 1, 800)])

# ============================================================== linked IDs

CHAIN = [f"x{n:03d}" for n in range(500)]
case("links_chain_500_shuffled",
     "A 500-ID chain written in scrambled order across two files; named by x000, which has no events",
     files={"links/part-00000.csv": raw([f"{CHAIN[n]},{CHAIN[n + 1]}" for n in range(0, 499, 2)][::-1],
                                        LINK_HEADER),
            "links/part-00001.csv": raw([f"{CHAIN[n + 1]},{CHAIN[n]}" for n in range(1, 499, 2)], LINK_HEADER)},
     events=[ev("e1", "x250", T(3)), ev("e2", "x499", T(3, "10:10:00"))],
     users=[("x000", D3, 1, 2, 0, "")], campaigns=[ORG(D3, 1)])

case("links_uppercase_and_space_names",
     "`Ua` (0x55) sorts before `ua` and names that person; ` u5` (leading space, 0x20) is a different ID from "
     "`u5` and names u4's person; plain `u5` stays a person of its own",
     links=["ua,Ua", "u4, u5"],
     events=[ev("e1", "ua", T(3)), ev("e2", "u4", T(3)), ev("e3", "u5", T(3))],
     users=[("Ua", D3, 1, 1, 0, ""), (" u5", D3, 1, 1, 0, ""), ("u5", D3, 1, 1, 0, "")],
     campaigns=[ORG(D3, 3)])

case("links_separate_people_not_merged",
     "u1-u2 and u3-u4 are two people; repeats, reverses and self-links change nothing",
     links=["u1,u2", "u2,u1", "u1,u2", "u3,u4", "u4,u4", "u3,u3"],
     events=[ev("e1", "u1", T(3)), ev("e2", "u2", T(3)), ev("e3", "u3", T(3)), ev("e4", "u4", T(3))],
     users=[("u1", D3, 1, 2, 0, ""), ("u3", D3, 1, 2, 0, "")], campaigns=[ORG(D3, 2)])

case("links_carriage_return_side",
     "`u1,u2\\r` links u1 to `u2\\r`, not to u2",
     links=["u1,u2\r"],
     events=[ev("e1", "u1", T(3)), ev("e2", "u2", T(3))],
     users=[("u1", D3, 1, 1, 0, ""), ("u2", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 2)])

case("links_star_through_unused_hub",
     "Hub `a` (no events) links five IDs: one person named `a`; their events interleave into one session",
     links=["a,u5", "u4,a", "a,u3", "u2,a", "a,u1"],
     events=[ev(f"e{n}", f"u{n}", T(3, f"10:{n * 5:02d}:00")) for n in range(1, 6)],
     users=[("a", D3, 1, 5, 0, "")], campaigns=[ORG(D3, 1)])

def links_across_files(links, files, seed, last_lines):
    """Spread link lines over `files - 1` files in a shuffled order (two of them header-only,
    every fifth one without a final newline), then a last file holding `last_lines`."""
    import random
    r = random.Random(seed)
    links = list(links)
    r.shuffle(links)
    buckets = [[] for _ in range(files - 1)]
    for line in links:
        buckets[r.randrange(files - 1)].append(line)
    for empty in (3, 7):  # header-only files
        buckets[0] += buckets[empty]
        buckets[empty] = []
    out = {}
    for n, bucket in enumerate(buckets):
        body = raw(bucket, LINK_HEADER)
        if n % 5 == 4 and bucket:
            body = body[:-1]  # no final newline
        out[f"links/part-{n:05d}.csv"] = body
    out[f"links/part-{files - 1:05d}.csv"] = raw(last_lines, LINK_HEADER)
    return out


CHAIN_A = ["a000"] + [f"x{n:03d}" for n in range(1, 500)]
CHAIN_A_LINKS = [f"{CHAIN_A[n]},{CHAIN_A[n + 1]}" for n in range(1, 499)]   # all but a000-x001

case("links_chain_across_20_files_smallest_last_b_side",
     "A 500-ID chain shuffled over 20 link files (2 header-only, some without a final newline); its byte-smallest "
     "ID a000 appears only in the last file, on the b side. Events and a click by far-apart IDs are one person, a000",
     files=links_across_files(CHAIN_A_LINKS, 20, 1, ["x001,a000"]),
     events=[ev("e1", "x250", T(3)), ev("e2", "x499", T(3, "10:10:00"))],
     clicks=[ck("k1", "x100", "c_1", T(3, "09:00:00"))],
     users=[("a000", D3, 1, 2, 0, "")], campaigns=[("c_1", "search", D3, 1, 0, 0, 1)])

LONG_ID = "A" * 22  # long (> 15 bytes) and upper-case: sorts before every other ID here
CHAIN_B = [f"y{n:03d}" for n in range(100)]
CHAIN_B_LINKS = [f"{CHAIN_B[n]},{CHAIN_B[n + 1]}" for n in range(99)] + [f"y099,{LONG_ID}", "x250,y050"]
LONG_CLICK = "K" * 21  # long upper-case click_id: byte-wise before "k1"

case("links_two_chains_joined_long_uppercase_name",
     "Chain A (smallest a000, only in the last file) joined to chain B by one middle link (x250,y050); chain B holds a "
     "22-byte upper-case ID, the byte-smallest of the merged person, so it names the person. Two clicks at the same ms "
     "by different IDs of that person: the long upper-case click_id sorts before k1, so c_2 is credited; both assist",
     files=links_across_files(CHAIN_A_LINKS + CHAIN_B_LINKS, 20, 2, ["x001,a000"]),
     events=[ev("e1", "y000", T(3)), ev("e2", "x499", T(3, "10:20:00"))],
     clicks=[ck("k1", "x001", "c_1", T(3, "09:00:00")), ck(LONG_CLICK, "y001", "c_2", T(3, "09:00:00"))],
     users=[(LONG_ID, D3, 1, 2, 0, "")],
     campaigns=[("c_1", "search", D3, 0, 0, 0, 1), ("c_2", "social", D3, 1, 0, 0, 1)])

case("links_self_repeat_reverse_empty_side_with_click_tie",
     "u1,u1 (self) and u1,u2 / u2,u1 (repeat reversed) make one person u1; 'u3,' and ',u4' are dropped so u3 and u4 "
     "stay apart; u5,u6 names u6's person u5. Clicks k2 (c_2, by u2) and k10 (c_1, by u1) at the same ms: byte-wise "
     "k10 < k2, so c_1 is credited; both assist",
     links=["u1,u1", "u1,u2", "u2,u1", "u3,", ",u4", "u5,u6"],
     events=[ev("e1", "u2", T(3)), ev("e2", "u1", T(3, "10:05:00")), ev("e3", "u3", T(3)), ev("e4", "u4", T(3)),
             ev("e5", "u6", T(3))],
     clicks=[ck("k2", "u2", "c_2", T(3, "09:00:00")), ck("k10", "u1", "c_1", T(3, "09:00:00"))],
     users=[("u1", D3, 1, 2, 0, ""), ("u3", D3, 1, 1, 0, ""), ("u4", D3, 1, 1, 0, ""), ("u5", D3, 1, 1, 0, "")],
     campaigns=[("c_1", "search", D3, 1, 0, 0, 1), ("c_2", "social", D3, 0, 0, 0, 1), ORG(D3, 3)])

# ============================================================== clicks and attribution

case("click_same_ms_tie_is_bytewise",
     "k10 (c_2) and k9 (c_1) at the same ms: byte-wise `k10` < `k9`, so c_2 is credited; both assist",
     events=[ev("e1", "u1", T(3))],
     clicks=[ck("k9", "u1", "c_1", T(3, "09:00:00")), ck("k10", "u1", "c_2", T(3, "09:00:00"))],
     campaigns=[("c_1", "search", D3, 0, 0, 0, 1), ("c_2", "social", D3, 1, 0, 0, 1)],
     users=[("u1", D3, 1, 1, 0, "")])

case("click_forms_and_duplicates",
     "Click ts in F2 and F3; a duplicate quoted differently; the F2 click (09:30Z) is the latest",
     events=[ev("e1", "u1", T(3))],
     clicks=[ck("k1", "u1", "c_1", str(ms(T(3, "09:00:00")))),
             ck("k2", "u1", "c_2", "2026-10-03T11:30:00+02:00"),
             ck('"k2"', '"u1"', '"c_2"', '"2026-10-03T11:30:00+02:00"')],
     campaigns=[("c_1", "search", D3, 0, 0, 0, 1), ("c_2", "social", D3, 1, 0, 0, 1)],
     users=[("u1", D3, 1, 1, 0, "")])

case("click_campaign_variants_dropped",
     "`C_1`, ` c_1` and `c_3` are not campaigns: the session is organic",
     events=[ev("e1", "u1", T(3))],
     clicks=[ck("k1", "u1", "C_1", T(3, "09:00:00")), ck("k2", "u1", " c_1", T(3, "09:00:00")),
             ck("k3", "u1", "c_3", T(3, "09:00:00")), ck("k4", "u1", "", T(3, "09:00:00"))],
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])

case("click_bad_ts_dropped",
     "Clicks with a bad ts are dropped even though a valid click for another campaign is earlier",
     events=[ev("e1", "u1", T(3))],
     clicks=[ck("k1", "u1", "c_1", T(3, "08:00:00")), ck("k2", "u1", "c_2", "2026-10-03T09:00:00z"),
             ck("k3", "u1", "c_2", "2026-10-03T09:00:00.000Z")],
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[("c_1", "search", D3, 1, 0, 0, 1)])

case("click_credit_and_purchasing_counts",
     "c_1 credits two sessions on Oct 3, one with purchases (500+250): sessions 2, purchasing 1, revenue 750",
     events=[ev("e1", "u1", T(3, "10:00:00"), kind="purchase", product="p1", price="500"),
             ev("e2", "u1", T(3, "10:05:00"), kind="purchase", product="p2", price="250"),
             ev("e3", "u1", T(3, "14:00:00"))],
     clicks=[ck("k1", "u1", "c_1", T(3, "09:00:00"))],
     users=[("u1", D3, 2, 3, 750, "shoes")], campaigns=[("c_1", "search", D3, 2, 1, 750, 2)])

case("click_assist_only_and_organic_same_day",
     "u1 credited to c_2 with c_1 assisting; u2 organic: c_1 has an assist-only row",
     events=[ev("e1", "u1", T(3)), ev("e2", "u2", T(3))],
     clicks=[ck("k1", "u1", "c_1", T(2)), ck("k2", "u1", "c_2", T(3, "09:00:00"))],
     users=[("u1", D3, 1, 1, 0, ""), ("u2", D3, 1, 1, 0, "")],
     campaigns=[("c_1", "search", D3, 0, 0, 0, 1), ("c_2", "social", D3, 1, 0, 0, 1), ORG(D3, 1)])

case("click_mid_session_and_reach_over_days",
     "A click at 10:20 does not credit the 10:00 session (started before it) but credits sessions within 7 days "
     "after, each on its own day; a session exactly 7 days later counts; one 7 days 2 h later is organic",
     events=[ev("e1", "u1", T(3)), ev("e2", "u1", T(3, "10:25:00")), ev("e3", "u1", T(5)),
             ev("e4", "u1", T(10, "10:20:00")),
             ev("e5", "u1", str(ms(T(10, "12:20:00")) + 1), T(10, "12:21:00"))],
     clicks=[ck("k1", "u1", "c_1", T(3, "10:20:00"))],
     users=[("u1", D3, 1, 2, 0, ""), ("u1", "2026-10-05", 1, 1, 0, ""),
            ("u1", "2026-10-10", 2, 2, 0, "")],
     campaigns=[ORG(D3, 1), ("c_1", "search", "2026-10-05", 1, 0, 0, 1),
                ("c_1", "search", "2026-10-10", 1, 0, 0, 1), ORG("2026-10-10", 1)])

case("click_by_link_only_id",
     "A click by an ID that appears only in links and clicks still credits the person",
     links=["u1,k_user"],
     events=[ev("e1", "u1", T(3))],
     clicks=[ck("k1", "k_user", "c_2", T(3, "09:00:00"))],
     users=[("k_user", D3, 1, 1, 0, "")], campaigns=[("c_2", "social", D3, 1, 0, 0, 1)])

# ============================================================== robustness

case("robust_no_event_files",
     "events/ holds no files at all: both tables empty, still valid Parquet",
     files={"events/.keep": None},
     clicks=[ck("k1", "u1", "c_1", T(3))])

case("robust_everything_header_only",
     "Every folder holds only header files")

case("robust_all_events_invalid",
     "Events exist but none survive: both tables empty",
     events=[ev("e1", "", T(3)), ev("e2", "u1", "bad", T(3)), ev("e3", "u1", T(3), kind="x")])

case("robust_missing_links_and_clicks_folders",
     "No links/ and no clicks/ folders at all",
     events=[ev("e1", "u1", T(3))],
     files={"links": None, "clicks": None},
     users=[("u1", D3, 1, 1, 0, "")], campaigns=[ORG(D3, 1)])


# ============================================================== writing

def write_case(spec):
    root = os.path.join(OUT, spec["name"])
    data = os.path.join(root, "input")
    for folder in ("events", "clicks", "links"):
        os.makedirs(os.path.join(data, folder), exist_ok=True)
    defaults = {
        "events/part-00000.csv": raw(spec["events"], EVENT_HEADER),
        "clicks/part-00000.csv": raw(spec["clicks"], CLICK_HEADER),
        "links/part-00000.csv": raw(spec["links"], LINK_HEADER),
    }
    files = dict(defaults)
    if any(path.startswith("events/") for path in spec["files"]):
        files.pop("events/part-00000.csv")
    if any(path.startswith("clicks") for path in spec["files"]):
        files.pop("clicks/part-00000.csv")
    if any(path.startswith("links") for path in spec["files"]):
        files.pop("links/part-00000.csv")
    for path, content in spec["files"].items():
        if content is None:                      # None: the folder exists but is empty, or is absent
            target = os.path.join(data, path)
            if os.path.basename(path) == ".keep":
                continue
            shutil.rmtree(target, ignore_errors=True)
            continue
        files[path] = content
    for path, content in files.items():
        with open(os.path.join(data, path), "wb") as handle:
            handle.write(content)
    with open(os.path.join(data, "products.csv"), "w") as handle:
        handle.write("product_id,category\n" + "".join(f"{p},{c}\n" for p, c in spec["products"]))
    with open(os.path.join(data, "campaigns.csv"), "w") as handle:
        handle.write("campaign_id,channel\n" + "".join(f"{c},{h}\n" for c, h in spec["campaign_list"]))
    with open(os.path.join(data, "manifest.json"), "w") as handle:
        json.dump({"window_start": spec["window"][0], "window_end": spec["window"][1]}, handle)

    expected = os.path.join(root, "expected")
    os.makedirs(expected, exist_ok=True)
    import csv
    for name, header, rows in (("user_daily", USER_HEADER, spec["users"]),
                               ("campaign_daily", CAMPAIGN_HEADER, spec["campaigns"])):
        with open(os.path.join(expected, f"{name}.csv"), "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(header.split(","))
            for row in rows:
                writer.writerow(row)
    with open(os.path.join(root, "CATCHES.txt"), "w") as handle:
        handle.write(spec["catches"] + "\n")


def main():
    shutil.rmtree(OUT, ignore_errors=True)
    names = [spec["name"] for spec in CASES]
    assert len(names) == len(set(names)), "duplicate case names"
    for spec in CASES:
        write_case(spec)
    print(f"wrote {len(CASES)} cases to {OUT}")


if __name__ == "__main__":
    main()
