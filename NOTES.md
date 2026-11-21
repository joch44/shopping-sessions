# NOTES

`rules.md` is the only source of truth. This file records facts about machines,
the contest, and the generator that design decisions depend on.

## Machines

| | Grader | Competition VM (`nolimit-sim`) | Analysis VM (`kalshi-vm`) |
|---|---|---|---|
| Where | GCP n2 | Azure `Standard_D8s_v5`, westus3, 57.154.24.116 | Azure `Standard_E32as_v7`, eastus |
| CPU | Intel Xeon, Cascade Lake or newer | Intel Xeon Platinum 8370C (Ice Lake) | AMD EPYC 9V45 (Zen 5) |
| vCPUs | 8 pinned | 8 (4 cores x 2 HT) | 32 (16 cores x 2 HT), 1 NUMA node |
| RAM | 24 GB, no swap | 32 GB (container capped at 24 GB, no swap) | 251 GB |
| AVX-512 | yes (Cascade Lake set) | yes (Ice Lake set, superset) | yes (Zen 5) |
| Use | scoring | **only place timings count** | everything else; never tune from its timings |

**Build target: `-C target-cpu=cascadelake`.** Never `native`: Ice Lake and Zen 5 both have
instructions Cascade Lake lacks (VBMI, VBMI2, IFMA, VPOPCNTDQ, BITALG, GFNI, VAES), so a
`native` build can die with SIGILL on the grader.

Analysis VM: THP `madvise`, kernel 7.0.0-1017-azure, 758 GB free disk.
Competition VM: 1 NUMA node, 48 MiB L3, THP `madvise`, 128 GB virtual disk (`sda`).

## Floors on the competition VM (2026-11-21, `tools/floor`, 8 threads, byte-split 8 MiB chunks)

| | 20M events (1.87 GB) | 60M events (5.62 GB) |
|---|---|---|
| Docker start+exit, python-slim image running `true` | 0.29 s | |
| Docker FROM-scratch image + warm read of all input | 0.24 s | |
| Cold read (page cache dropped) | 1.99 s (0.94 GB/s) | 6.54 s (0.86 GB/s) |
| Warm read (pread into buffers, no work) | 0.08 s (22 GB/s) | 0.21 s (26 GB/s) |
| Warm newline count, 8 threads | 0.13 s (14.4 GB/s) | 0.36 s (15.6 GB/s) |
| Warm newline count, 1 / 2 / 4 threads | 0.46 / 0.26 / 0.15 s | |

- A simple pass saturates memory bandwidth at ~14-15 GB/s from 4 threads up.
- The board leader's 1.3 s at ~20M cannot be a cold read (cold read alone is ~2 s here):
  the median-of-3 rule for runs under 20 s means the scored run is effectively warm.
- Container start/exit is ~0.15-0.3 s of any score: a FROM-scratch (or distroless) image with
  a static binary saves ~0.05-0.1 s over python-slim.

## Calibration (sim -> board): see `tools/calibration.csv`, `tools/to_board.py`

Reference: 123.7 s on nolimit-sim (mean of 4 runs, 20M, seeds 11-13) vs ~124 s on the board:
factor 1.00. Recalibrate with every submission.

## Contest (known)

- Submission: Docker image, `linux/amd64`, built **on the grader with network**, at most 10 min.
  The run has no network.
- Run limits: 8 vCPUs pinned, 24 GB, no swap; read-only rootfs except `/tmp` (64 MB tmpfs);
  uid 1000; at most 4,096 processes/threads; at most 50 MB stdout (log to stderr); image at most 8 GB;
  `/output` + `/scratch` at most 50 GB.
- Env: `DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24`.
- Scoring: exact match, then fastest.

## Contest (from the starter kits in `inputs/`, unpacked in `vendor/`)

- Submit a public GitHub repo (optionally a commit SHA). Grader builds the Dockerfile natively
  (linux/amd64). **Build time is never timed; only the run is.** `CMD` runs with no args; exit 0 = done.
- Scorer flags (starter `scripts/docker_run.sh`): `--network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --cpus 8 --memory 24g --memory-swap 24g
  --user 1000:1000`, `/data:ro`, `/output`, `/scratch`.
- 36 hand-written fixtures (A1-A12, E1-E20, L1-L4), identical in all three starters, expected
  rows "written by hand from the rules, not produced by the reference".
- `dev-answer`: expected tables for `--seed 1 --events 1000000 --clicks 250000`
  (117,038 user_daily rows, 27,400 campaign_daily rows).
- Official comparator `compare.py`: order-insensitive exact match; any integer width; string or
  categorical; `day` must be Date; exact column set; flags duplicate keys.
- **Fixture L3 trap:** only the FIRST line of a file is the header. A later `user_id_a,user_id_b`
  line is data and links IDs named `user_id_a`/`user_id_b`. IDs can contain a TAB (`A\tB`), which
  sorts before lowercase and becomes the person's name.
- Organizers confirm DuckDB/Polars CSV readers do not follow `rules.md`.

## Scoring (track page)

- Practice: hidden ~20M events; one timed run, or median of 3 if under 20 s; limit 2x baseline
  (343.8 s) and at most 5 min. Board leader ~1.3 s; our reference 124 s.
- Final: new ~60M dataset, 3 runs, median; each capped at 3 min. The 5+ fastest re-timed 5x on
  an idle machine; that median decides. Up to ~20% machine-to-machine spread on practice.
- Clock: container start to exit. Dev set check first (at most 3 min). 2 submissions/hour.
- Keep the repo public with every submitted commit until results are published.

## Contest (unknown; ask)

- Cold or warm page cache on the first run? (Practice median-of-3 makes it moot for fast entries.)
- Starter kit: generator README mentions "the starter's `make dev`" and code mentions
  "fixture E5". Do we have it (fixtures with expected outputs)?

## Generator (`vendor/generator`, built copy in `tools/generator`, binary `bin/generate`)

- Rust source, no external dependencies. `Cargo.lock` names a `shopping-sessions-reference`
  crate (used only by a `#[cfg(test)]` in `text.rs`) that is **not** included.
- Deterministic: byte-identical output for a seed regardless of `--threads`.
- Public dev set: `--seed 1 --events 1000000 --clicks 250000` (79 MB events, 0.09 s to generate).
- Full size guess (seed 1, 60M/15M): events 4.6 GB in 30 files (largest 1.25 GB = 27%),
  clicks 689 MB, links 53 MB. ~79 bytes/event row. Generates in 3.5 s, 8.4 GB RSS.

### Facts read from the source (for test design, never for hard-coding)

- `window_start` is always a UTC midnight between 2025-01-01 and 2028-12-17; window is 14 days.
- IDs: `u`/`e`/`k` + lowercase base36 of a 40-bit scrambled number (at most 9 chars). Products `p1..p50000`,
  campaigns `c_1..c_2000`. Case variants upper-case the whole ID (`E...`, `U...`, `C_12`, `P123`).
- Sessions: one per 2-hour slot, start at most 25 min into the slot, last at most 29:59, so one
  generated user's sessions never merge by accident. Whales: a session in all 168 slots.
  Linking (especially kiosks) merges generated users, so interleaved sessions DO merge.
- 3% of sessions have millisecond starts (F3 only). 2.5% cross midnight (22:00 slot).
  0.1% have one gap of exactly 1,800,000 or 1,800,001 ms.
- Dedup traps: 2% of duplicated events have a bad `ts` only on the earliest copy. In 1/64 of
  duplicated events (keyed draw), later copies name the NEXT generated user's first ID.
  Later copies are ingested 1 to 600 s after the original.
- Malformed (0.05% of emits, links too): drop the last field, append `,x`, prepend `"`,
  append `"`, delete `event_id`/`click_id` (leading comma), or a blank line (original still written).
- Ingest: 0.5% ingested 1 to 5 s BEFORE `ts` (kept). Late: some exactly +24 h, some +24 h 1 s.
- Bad prices: `19.99 -5 +500 0 "" 10000001 250000000 1e3 00`. Odd valid: `10000000 1 0250`.
  Non-purchase rows carry empty / real / bad prices (ignored).
- Bad types: `Purchase PURCHASE View "cart " click refund "" views`.
- Unknown products: empty (1/10), `P<n>` (1/10), `p50001..p99999`.
- Unknown campaigns: `C_<1..2000>` (1/10), `c_2001..c_9999`.
- Click deltas before the targeted session start: exactly 7 d, 7 d + 1 ms, 0, -1 ms (after start),
  same-millisecond pairs (1%), and clicks up to 3 d beyond 7 d.
- Kiosk IDs appear only in links (never in rows); can be the byte-smallest ID of a giant person.
- Bad timestamps produced: empty, `.mmmZ`, lowercase `z`/`t`, space separator, no zone,
  `N.0`, `N.NNe12`, `-N`, month 13, Feb 30, hour 24, second 60, Feb 29 in a non-leap year,
  and valid forms just outside the window.
- **Not produced by the generator** (hand-made tests needed): `\r` anywhere, bad offsets
  (`+24:00`, `+05:60`, `+0200`), `-00:00`, F3 longer than 19 digits, F3 with leading zeros,
  non-ASCII digits, spaces inside IDs, `""` as a whole field, quoted keys equal to unquoted
  keys only via random quoting (covered), zero-byte files, missing trailing newline.
