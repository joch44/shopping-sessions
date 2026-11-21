# EXPERIMENTS

One change at a time: hypothesis and expected gain, gate result, measured gain on the
mirror VM (`nolimit-sim`, harness `nolimit-run`, median of 3, 20M seeds 11-13 and 60M seed 301,
outputs checked against `tools/checksums.csv`). Analysis-VM timings are marked "indicative".

## v1 (commit 022a603): 3.62 s @20M, 10.68 s @60M (mirror VM)
Stages @20M (mirror VM, in-process): links 0.41, parse 1.76, dedup 0.49, sessions 0.49, parquet 0.09.
perf (analysis VM): parse+validate+scatter closure 23%, people HashMap<u128,u128>::get 15%,
dedup sort 10%, product/campaign HashMap get 5%, ts::parse+days_from_civil 5.6%, person sort 5%.

## v2: experiments 1-4 together (gate: quick gate x3 configs PASS)
1. Links on one thread overlapped with event parsing; events keep the user key, person
   resolved in dedup; clicks parsed after links. Expect: -0.4 s (links off the critical path).
2. Dedup by open-addressing table instead of sort. Expect: dedup -40%.
3. Date cache in ts::parse (thread-local last date). Expect: -3% CPU.
4. Exit right after outputs close (no teardown). Expect: -0.1 s.
Indicative (analysis VM, 20M): in-process 2.81 -> 1.97-2.21 s; checksum matches.
Mirror VM: 3.17 s @20M (3.62), 9.00 s @60M (10.68). All outputs match.

## v3: experiments 5-7 (gate: quick gate x4 configs PASS, incl. NCPU=40)
5. Dedup indexes the per-thread pieces in place (no 1.6 GB concatenation copy).
6. Flat person table (open addressing, two arrays) with software prefetch 16 lookups ahead.
7. Partition buffers pre-sized from input bytes (77 B/event line, 47 B/click line, +15%).
Expect: dedup -25%. Indicative A/B (analysis VM, 20M, 3 pairs): dedup 0.52-0.55 -> 0.39-0.41 s;
wall 1.98 -> 1.78 s (-10%); checksum matches.
Robustness: handles pack 31 pieces; more threads merge pieces (tested NCPU=40).
Mirror VM: 3.01 s @20M, 8.81 s @60M. All outputs match.

## Not adopted yet
- GLIBC_TUNABLES=glibc.malloc.hugetlb=1: -8% indicative, but only 88 MB of 4.4 GB got huge
  pages (growing Vecs). Proper THP needs explicit madvise on pre-sized big buffers.

## Rejected / measured elsewhere (from the feasibility session, nolimit-sim, 20M)
- Shared lock-free global dedup table: 1.7x slower than partitioning. Don't.
- Predicting heavy people from link-component size: wrong (heaviest are whales, 3-6 IDs).
- Measured wins to try: THP via madvise for big buffers (-26..-58% on lookup/dedup),
  24-byte pre-dedup records, write-combining scatter buffers (needs THP), ~1024 dedup
  partitions with L2-sized tables, software prefetch for person lookup.

## Mirror results v4-v15 (nolimit-sim, nolimit-run, medians; every output checked against tools/checksums.csv)

| ver | change | 20M | 60M | note |
|---|---|---|---|---|
| v4 | skip product lookup for views/carts (feasibility session) | 2.94 | 8.49 | |
| v5 | 1024 dedup partitions, explicit date cache, single-pass splitter, 64 B copies | 2.66 | 7.72 | dedup 0.49 -> 0.33 |
| v6 | hash-grouped sessions (counting sort) | 2.57 | 7.36 | |
| v7 | AVX-512 record splitter (masked loads) + SIMD u128 ID packing | 2.44 | 7.06 | ideas from the feasibility prototype |
| v8 | THP allocator, >= 1 MiB | 2.36 | 7.17 | 60M regressed: per-partition temps lost glibc recycling |
| v9 | sessions reuse scratch, pieces in place | 2.24 | 6.67 | **submitted: 1.97 s on the grader** |
| v10 | timestamp fast paths | | | profile: date cache missed 93% (shuffled rows) |
| v11 | month-keyed 16-slot date cache, branch-free digit check | ~2.20 | 6.65 | |
| v12 | THP threshold 16 MiB (research session) | ~2.21 | 6.40 | ~= v9 at 20M in interleaved A/B |
| v13 | tagged person table (research R5) | | | |
| v14 | per-thread scatter arenas (research R3, one region per partition) | 2.02 | 5.61 | interleaved vs v9: 2.16 / 6.56 |
| v15 | dedup table (hash32, index), sequential winner emit | 1.92 | 5.35 | interleaved vs v14: 1.96 / 5.53 |
| v17 | click person lookups batched (32) with prefetch | | | clicks 0.18 -> 0.13 s (mirror stage) |
| v18 | hand-written Parquet writer (research Topic E), crate fallback > 1 GiB | 1.93 | 5.10 | clean interleaved vs v15: 2.00 / 5.41 |
| v19 | dense campaign counters + 1024 person partitions (research, mirror-measured) | 1.78 | 4.97 | interleaved vs v18: 1.82 / 5.12 |
| v20 | 16-byte per-person session events, 32-byte clicks | 1.73 | 4.85 | interleaved vs v19: 1.76 / 4.89; **submitted: 1.60 s on the grader** |
| v21 | non-temporal stores for 64-byte EventCopy (sfence per chunk) | 1.67 | 4.46 | store breakdown (mirror, 20M): split 0.26, judge 0.30, store 0.17, link overlap 0.04; NT: parse events 0.77 -> 0.655 s; research full A/B 1.79 -> 1.66 / 4.83 -> 4.49 |
| v22 | SIMD timestamps (parser session) + ingest_ts fast path (research; relies on the rules' F1 guarantee) | 1.53 | 4.12 | parse events 0.63 -> 0.53 s; research A/B vs v21: 1.65 -> 1.52 / 4.56 -> 4.17 |
| v23 | links resolved in parallel, still overlapped with event parsing (research) | 1.54 | 4.16 | tie with v22 (research A/B 1.507/1.514); links ~0.08 s of parallel work at 20M |
| v24 | PGO (communicator session), profile from never-timed datasets via tools/pgo.sh | 1.46 | 3.99 | interleaved vs v23: 1.50 / 4.16 |
| v25 | PEXT splitter (parser session) + regenerated profile | 1.40 | 3.63 | parse events 0.38 s @20M / 1.11 s @60M; research A/B on v23: 1.565 -> 1.414 s |

Calibration: v9 mirror 2.24 s vs grader 1.97 s -> factor 0.878 (tools/to_board.py). Target <1.0 s grader
~= 1.1 s mirror. Container start/stop floor ~0.34 s (research): in-process budget ~0.75 s @20M.

Lessons: analysis-VM (Zen 5, 16 cores) gains roughly halve on the mirror (Ice Lake, 4 cores x 2 HT);
memory-layout changes must be A/B'd on the mirror, interleaved (run-to-run drift is ~2-4%).

Rejected (measured, mirror, interleaved vs v25): mimalloc as the only allocator (20M 1.377 vs 1.365 s,
60M 3.74 vs 3.70 s) and HugeAlloc >= 16 MiB + mimalloc for small (20M 1.380 vs 1.385 tie, 60M 3.62 vs
3.56 s); the analysis-VM gain (-5.8%, faster end-of-run frees) does not transfer to Ice Lake.
Rejected (measured, research, mirror): offsets-based judge (no gain once the PEXT splitter is in);
links-parallel as a separate stage before parsing (tie at 20M, +0.05 s at 60M; superseded by v23);
target-cpu=icelake-server (communicator: nothing; the grader is only guaranteed Cascade Lake anyway).
Rejected (measured, research, mirror): Click padded to 64 B for NT stores (0.131 -> 0.134 s, worse:
clicks are not RFO-bound); 32 B Click via source locations (<0.01 s upside); chunk size 16/4/1 MiB
(<5 ms; no barrier/overlap restructuring worth doing).
Rejected (measured): v16 one pool for clicks + dedup (no change); 2048/4096 dedup partitions
(dedup -0.01 s but parse +0.03..0.06 s: more scatter targets); research Topic A one-scatter +
retractions (correct, slower); radix sort for sessions (research: no gain over counting sort).
Rejected earlier: GLIBC_TUNABLES hugetlb (88 MB of 4.4 GB got huge pages); THP threshold 1 MiB (v8);
shared global dedup table (feasibility, 1.7x slower); software write-combining (no gain on Ice Lake, research).

## Target < 0.8 s on the board (~0.9 s mirror, ~0.55 s in-process @20M): no idle core

Known serial sections / barriers in v22 (sent to the research session, which instruments a per-thread
busy/idle timeline on the mirror and sends patches for every gap > ~5 ms):
1. startup on one thread: manifest + products/campaigns lookups + per-file mmap (~0.023 s)
2. links on ONE thread (~0.25 s CPU) inside the events pool -> research parallel-links patch
3. barrier + respawn after the events pool; clicks wait on links (patch: parse clicks without people)
4. dedup waits on people (person lookup in its emit loop)
5. serial person-partition weighting + sort; sessions skew from whale partitions (heaviest first)
6. serial merge of 8 dense campaign-counter arrays; output join; four thread spawns per run
7. exit teardown in the kernel (~0.11 s, at the container floor)
Ownership: links/clicks/people handoff and the timeline = research; parser = parser session;
this session ports, gates in 4 configs, mirror-verifies and commits.
