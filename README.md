# Shopping Sessions: our pipeline

Builders Buildathon 2026, Shopping Sessions track. The image builds one Rust
binary from `pipeline/` (plus portable copies for CPUs without AVX-512) that reads
the CSV inputs from `/data` and writes the `user_daily` and `campaign_daily` Parquet
tables to `/output`, following `rules.md`.

## Open-source libraries used

Rust crates (`pipeline/Cargo.toml`; full list with versions in `pipeline/Cargo.lock`):

- `parquet` (Apache Arrow Rust, Apache-2.0): Parquet output
- `memchr` (MIT/Unlicense): byte search
- `memmap2` (MIT/Apache-2.0): memory-mapped input files
- `libc` (MIT/Apache-2.0): system calls
- `rustc-hash` (MIT/Apache-2.0): FxHash hash maps
- `xxhash-rust` (BSL-1.0), xxh3: hashing long IDs
- `serde_json` (MIT/Apache-2.0): reading `manifest.json`

and their transitive dependencies as locked in `Cargo.lock`. The build image is the
official `rust:1.100.0-slim-bookworm`.

## Other code in this repository

- `inputs/`, `vendor/`: the organizers' data generator, rules and starter kits, used
  for local testing only; not part of the built image.
- `reference/`, `tests/`, `tools/`: our own reference implementation, test suites and
  benchmarking scripts; not part of the built image.

## License

MIT, see `LICENSE`. It covers every commit in this repository that we wrote.
