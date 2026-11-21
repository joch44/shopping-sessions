# The gate. No optimization is kept unless `make check` passes.
#   make check IMPL="target/release/pipeline"      quick gate (minutes)
#   make deep  IMPL="target/release/pipeline"      deep gate (analysis VM, long)
# IMPL is any command that reads DATA_DIR and writes OUTPUT_DIR. Default: the reference.

PY       := $(CURDIR)/.venv/bin/python
REF      := $(PY) $(CURDIR)/reference/reference.py
IMPL     ?= $(REF)
FIXTURES := vendor/starter-duckdb/starter-duckdb/fixtures
CORPUS   := tests/corpus/cases
TINY_N   ?= 40
FUZZ_N   ?= 60

.PHONY: check deep corpus fixtures corpus-cases dev tiny fuzz medium parsegate pgo shipped

check: fixtures corpus dev tiny fuzz parsegate
	@echo "== QUICK GATE PASSED"

corpus-cases:
	@$(PY) tests/corpus/build.py >/dev/null

fixtures:
	@echo "== 36 organizer fixtures"; $(PY) tests/run_cases.py $(FIXTURES) -- $(IMPL) | tail -1 | grep -q '^36/36' \
	  || { $(PY) tests/run_cases.py $(FIXTURES) -- $(IMPL) | grep -v '^ok'; exit 1; }

corpus: corpus-cases
	@echo "== hand-made corpus"; out=$$($(PY) tests/run_cases.py $(CORPUS) -- $(IMPL)); \
	  echo "$$out" | tail -1; echo "$$out" | tail -1 | grep -qE '^([0-9]+)/\1 ' || { echo "$$out" | grep -v '^ok'; exit 1; }

dev:
	@echo "== public dev set vs official answer"; tools/datasets.sh dev >/dev/null; \
	  out=$$(mktemp -d); scratch=$$(mktemp -d); \
	  DATA_DIR=data/dev-s1 OUTPUT_DIR=$$out SCRATCH_DIR=$$scratch $(IMPL) 2>/dev/null \
	  && $(PY) tools/compare.py $$out vendor/dev-answer/dev-answer | tail -1 | grep -q MATCH; \
	  rc=$$?; rm -rf $$out $$scratch; [ $$rc -eq 0 ] && echo MATCH || { echo "DEV MISMATCH"; exit 1; }

# Tiny generator seeds: implementation vs reference (skipped when IMPL is the reference).
tiny:
	@echo "== $(TINY_N) tiny generator seeds vs reference"; tools/datasets.sh tiny >/dev/null; \
	  if [ "$(IMPL)" = "$(REF)" ]; then echo "(IMPL is the reference: skipped)"; exit 0; fi; \
	  fail=0; for d in $$(ls -d data/tiny-* | sort -V | head -$(TINY_N)); do \
	    a=$$(mktemp -d); b=$$(mktemp -d); s=$$(mktemp -d); \
	    DATA_DIR=$$d OUTPUT_DIR=$$a SCRATCH_DIR=$$s $(IMPL) 2>/dev/null; \
	    DATA_DIR=$$d OUTPUT_DIR=$$b SCRATCH_DIR=$$s NCPU=2 BUCKETS=4 $(REF) 2>/dev/null; \
	    $(PY) tools/compare.py $$a $$b >/dev/null || { echo "DIFFER on $$d"; fail=1; }; rm -rf $$a $$b $$s; \
	  done; [ $$fail -eq 0 ] && echo "all match"

fuzz:
	@echo "== $(FUZZ_N) fuzzed datasets"; tools/datasets.sh tiny >/dev/null; \
	  if [ "$(IMPL)" = "$(REF)" ]; then \
	    $(PY) tests/fuzz.py run $$(ls -d data/tiny-s5-e*) $(FUZZ_N) -- $(IMPL) | tail -1; \
	  else \
	    $(PY) tests/fuzz.py run $$(ls -d data/tiny-s5-e*) $(FUZZ_N) -- $(IMPL) -- env NCPU=2 BUCKETS=4 $(REF) | tail -1; \
	  fi | tee /dev/stderr | grep -qE '^([0-9]+)/\1 '

# Parse gate (from the research session): live csv/key/ts fast paths vs a frozen v1
# reference, field for field, on real datasets at many chunkings and synthetic edge files.
parsegate: corpus-cases
	@echo "== parse gate"; tools/datasets.sh tiny >/dev/null; research/gate/run_gate.sh pipeline | tail -1 | tee /dev/stderr | grep -q '^PARSE GATE OK'

# Deep gate: medium datasets vs reference, long fuzz. Run in the background on the analysis VM.
deep: check
	@echo "== medium seeds vs reference"; tools/datasets.sh medium >/dev/null; fail=0; \
	  for d in data/medium-s1*; do a=$$(mktemp -d); b=$$(mktemp -d); s=$$(mktemp -d); \
	    DATA_DIR=$$d OUTPUT_DIR=$$a SCRATCH_DIR=$$s $(IMPL) 2>/dev/null; \
	    DATA_DIR=$$d OUTPUT_DIR=$$b SCRATCH_DIR=$$s NCPU=16 $(REF) 2>/dev/null; \
	    $(PY) tools/compare.py $$a $$b >/dev/null && echo "ok $$d" || { echo "DIFFER $$d"; fail=1; }; \
	    rm -rf $$a $$b $$s; done; [ $$fail -eq 0 ]
	@$(MAKE) --no-print-directory fuzz FUZZ_N=1000
	@echo "== DEEP GATE PASSED"

# Profile-guided optimization: regenerate the profile for the current code (before each submission).
pgo:
	@tools/pgo.sh

# The binary exactly as the Dockerfile builds it (PGO profile, static, Cascade Lake): gate this one.
SHIPPED := /tmp/claude-1000/cargo-shipped/x86_64-unknown-linux-gnu/release/pipeline
shipped:
	@cd pipeline && . ~/.cargo/env && RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-use=$(CURDIR)/pipeline/pgo/merged.profdata" \
	  CARGO_TARGET_DIR=/tmp/claude-1000/cargo-shipped cargo build --release --locked --target x86_64-unknown-linux-gnu -q && echo $(SHIPPED)
