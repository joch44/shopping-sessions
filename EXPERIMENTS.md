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
Mirror VM: PENDING.

## Rejected / measured elsewhere (from the feasibility session, nolimit-sim, 20M)
- Shared lock-free global dedup table: 1.7x slower than partitioning. Don't.
- Predicting heavy people from link-component size: wrong (heaviest are whales, 3-6 IDs).
- Measured wins to try: THP via madvise for big buffers (-26..-58% on lookup/dedup),
  24-byte pre-dedup records, write-combining scatter buffers (needs THP), ~1024 dedup
  partitions with L2-sized tables, software prefetch for person lookup.
