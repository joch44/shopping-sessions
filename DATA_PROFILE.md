# DATA_PROFILE

Measured with `tools/profile_data.py` on 20 seeds at 2M events (`medium-s101..120`) and 3 seeds
at the practice size, 20M events (`practice-s201..203`); clicks = events / 4. Raw file facts are
measured directly; person/session/output facts come from the reference's (validated) output.
Raw data in `data/profile/profile.jsonl`.

**These facts guide data-structure and fast-path choices. None may be hard-coded: every
regularity below needs a check and a general fallback.**

## Bytes and files

| | 2M events | 20M events | 60M events (seed 1, 301) |
|---|---|---|---|
| events bytes | ~154 MB | ~1.54 GB | 4.6 GB |
| total input | ~182 MB | ~1.87 GB | 5.3-5.6 GB |
| bytes per line: events / clicks / links | 77.0 / 47.1 / 20.6 | same | same |
| event files | 24-159 | 27-111 | 30 |
| click files / link files | 11-48 / 4-23 | 13-48 / 4-19 | |
| largest event file share | 18-28% | 19-26% | 27% |

- **Split work by bytes, never by file**: one file can hold over a quarter of all rows.
- Quoted fields: 20.0% of fields, every seed. Malformed lines: ~0.04%.

## Field shapes

- ID lengths (bytes, after unquoting): `event_id`, `click_id`, `user_id`, link IDs **0-9**
  (`e`/`k`/`u` + up to 8 base36 chars); `product_id` 0-6 (`p1`..`p50000`, plus dirt);
  `campaign_id` 3-6 (`c_1`..`c_2000`, plus dirt). Hand-made/rule cases can be any length,
  contain spaces, tabs, `\r`, non-ASCII: fast path for short IDs packed into fixed-width
  integers (e.g. 16 bytes = u128), with a general fallback for longer ones.
- Event `ts` forms: **F1 56-58%, F2 9.4-9.7%, F3 32-34%**, bad 0.3-0.65%, empty ~0.04%.
  `ingest_ts`: always F1 in generated data.
- Event types: ~87% view, ~10% cart, ~3% purchase, 0.1% bad.

## People (after linking)

| | 2M events | 20M events |
|---|---|---|
| people with sessions | ~66.8k | ~667k |
| events per person: median / p99 / max | 13 / ~240 / 26k-39k | 12 / ~228 / **345k-385k** |
| sessions per person: median / p99 / max | 3 / ~51 / 182-202 | 3 / ~48 / 242-293 |
| largest person's share of events | 1.3-2.0% | 1.8-2.0% |
| IDs in the largest linked person | 196-382 | **1,232-1,665** |

- The largest people are the **kiosk merges** (thousands of generated users joined by a
  shared device), scaling with data size: ~385k events in one person at 20M, so roughly
  1.1M+ at 60M. Their sessions merge heavily (only ~240-290 sessions despite 385k events).
  Per-person work must not stall one thread on them: sorting ~1M events of one person is
  ~0.05-0.1 s single-threaded; acceptable, but schedule the heavy people first.
- Typical person: a dozen events, 3 sessions. Small-person fast path (insertion sort) matters.

## Output

| | 2M | 20M | 60M (seed 1) |
|---|---|---|---|
| `user_daily` rows | ~232k | ~2.24M | 6.54M |
| rows per input event row | 0.11 | 0.105 | 0.11 |
| `campaign_daily` rows | ~27,970 | 28,014 | 28,014 |
| events per session | 5.5 | 5.75 | |
| sessions credited to a campaign | 60-62% | 61-62% | 61.7% |
| `top_category` null | 82% | 82% | |

- `user_daily` at 60M is ~6.5M rows: worth a lean Parquet writer, but small next to the input.
- `campaign_daily` is ~2,000 campaigns x 14 days + organic: trivially small.

## Reference counts at 20M (seed 201)

26.4M rows with an id parsed; 19.99M distinct event ids, 19.66M survive; 5.0M distinct click
ids, 4.95M valid; 3.37M sessions.
