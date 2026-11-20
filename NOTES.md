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
Competition VM: TODO lscpu NUMA/L3, disk speed (fio), THP, I/O floors.

## Contest (known)

- Submission: Docker image, `linux/amd64`, built **on the grader with network**, at most 10 min.
  The run has no network.
- Run limits: 8 vCPUs pinned, 24 GB, no swap; read-only rootfs except `/tmp` (64 MB tmpfs);
  uid 1000; at most 4,096 processes/threads; at most 50 MB stdout (log to stderr); image at most 8 GB;
  `/output` + `/scratch` at most 50 GB.
- Env: `DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24`.
- Scoring: exact match, then fastest.

## Contest (unknown; ask)

- Graded `--events` / `--clicks`. Best guess: ~60M events + 15M clicks (generator comment:
  "a few hundred at the full 75M rows"; dev set ratio is 4:1).
- Cold or warm page cache? One seed or several? One run or several?
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
