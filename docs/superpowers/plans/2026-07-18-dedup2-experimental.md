# dedup2 Experimental In-Memory Deduplicator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship standalone workspace `dedup2/` that loads Parquet via Arrow scan into memory and runs Name / URI / Metadata dedup per `docs/superpowers/specs/2026-07-18-dedup2-experimental-design.md`.

**Branch:** develop directly on `main` (no feature-branch / worktree isolation).

**Architecture:** Thin `core` (entities + engines + stats) + `cli` (clap, progress/ETA, CSV/JSON reports). Load path is Arrow/`parquet` column projection with online aggregation — no DuckDB staging table.

**Tech Stack:** Rust 2024 edition, clap, parquet+arrow, rapidfuzz 0.5, rayon, ahash, serde/csv, unicode-normalization, sha2, num-bigint.

## Global Constraints

- Workspace path: `dedup2/` (not under root Cargo workspace unless later wired)
- Directories: `crates/core`, `crates/cli`; packages: `dedup_core`, `dedup_cli`; binary: `dedup2`
- No TOML run config; CLI flags only
- No spill / external postings / stage resume / recall-audit gate
- Name + Metadata strategies follow `REWRITE_ARCHITECTURE.md`; URI exact
- Load: Arrow scan only (no DuckDB mem full-table)
- Progress: tty/json + EWMA ETA
- Do not commit unless the user explicitly asks

---

### Task 1: Workspace scaffold + CLI skeleton

**Files:**
- Create: `dedup2/Cargo.toml`
- Create: `dedup2/README.md`
- Create: `dedup2/crates/core/Cargo.toml`
- Create: `dedup2/crates/core/src/lib.rs`
- Create: `dedup2/crates/cli/Cargo.toml`
- Create: `dedup2/crates/cli/src/main.rs`

**Produces:** `cargo build -p cli` / binary `dedup2` that parses `all` and flags, exits 0 with “not implemented” message for engines until later tasks.

- [x] Create workspace members `dedup_core`, `dedup_cli` with shared deps
- [x] CLI: subcommands `all|run-name|run-uri|run-metadata`; flags per spec
- [x] `cargo build --manifest-path dedup2/Cargo.toml`

### Task 2: Progress reporter + ETA

**Files:**
- Create: `dedup2/crates/cli/src/progress.rs`
- Create: `dedup2/crates/core/src/progress.rs` (observer trait)

**Produces:** `ProgressObserver` in core; tty/json reporter in cli with EWMA ETA.

- [ ] Trait: `set_stage`, `set_phase`, `set_total`, `add_completed`, `finish`
- [ ] EWMA alpha 0.25; confident after ≥3 positive throughput samples
- [ ] Unit test EWMA ETA math in cli or core

### Task 3: Arrow Parquet load + entity build

**Files:**
- Create: `dedup2/crates/core/src/parquet.rs`
- Create: `dedup2/crates/core/src/entity.rs`
- Create: `dedup2/crates/core/src/error.rs`
- Test: `dedup2/crates/core/tests/load_entities.rs` (+ tiny fixture parquet)

**Produces:** `load_entities(inputs, progress) -> EntityStore`

- [ ] Validate required columns; project UTF-8 strings
- [ ] Apply trim/lower(chain)/coalesce empty strings; preserve file_ordinal + row
- [ ] Online build: contracts, URI postings, metadata maps, denominators
- [ ] Test with 2-chain fixture: contract name first-non-empty, URI posting counts

### Task 4: Scope counters + report writers

**Files:**
- Create: `dedup2/crates/core/src/scope.rs`
- Create: `dedup2/crates/core/src/stats.rs`
- Create: `dedup2/crates/cli/src/report.rs`

**Produces:** `SummaryAccumulator` → `summary.csv`, `chain_matrix.csv`, `run_manifest.json`

### Task 5: URI engine

**Files:**
- Create: `dedup2/crates/core/src/uri.rs`
- Test: unit tests in `uri.rs`

**Produces:** exact token/image URI scope hits wired into accumulator

### Task 6: Name engine (CandidateBounds + resident postings + JW)

**Files:**
- Create: `dedup2/crates/core/src/name/mod.rs`
- Create: `dedup2/crates/core/src/name/candidate_bounds.rs`
- Create: `dedup2/crates/core/src/name/postings.rs`
- Test: bounds cover exhaustive hits at 0.95

**Produces:** Name scope hits; progress by scored candidates

### Task 7: Metadata engine (anchors, template, LSH prefilter, BM25 verify)

**Files:**
- Create: `dedup2/crates/core/src/metadata/*.rs` per spec layout
- Tests: low-info skip, shared-token / Solana fallback, BM25 threshold

**Produces:** Metadata scope hits + prefilter stats for manifest

### Task 8: Pipeline wiring + README

**Files:**
- Modify: `dedup2/crates/cli/src/main.rs`
- Create: `dedup2/crates/cli/src/pipeline.rs`
- Modify: `dedup2/README.md`

**Produces:** `dedup2 all --input … --output-dir …` end-to-end on fixture

---

## Spec coverage

| Spec item | Task |
|---|---|
| Arrow load, no DuckDB staging | 3 |
| CLI flags, no config | 1 |
| Progress + ETA | 2 |
| Three scopes + CSV/manifest | 4 |
| URI exact | 5 |
| Name CandidateBounds + JW | 6 |
| Metadata digest+LSH+BM25 | 7 |
| `all` pipeline | 8 |
