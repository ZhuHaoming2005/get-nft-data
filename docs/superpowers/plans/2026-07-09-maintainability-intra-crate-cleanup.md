# Maintainability Intra-Crate Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify tokenizer/eligibility, collapse dual-track BM25, split `name_uri` metadata into a real submodule, and finish P2 cleanup — without a shared crate.

**Architecture:** Intra-crate only. `name_uri` switches `include!("metadata.rs")` → `mod metadata;` with `metadata/{mod,parse,bm25,load,index,sketch}.rs`. `top_contract` keeps string BM25 as `#[cfg(test)]` oracle. Cross-crate copies get `Keep in sync` comments.

**Tech Stack:** Rust, regex, rapidfuzz, DuckDB, rayon

**Spec:** `docs/superpowers/specs/2026-07-09-maintainability-intra-crate-cleanup-design.md`

---

### Task 1: P0 — name_uri tokenizer → `[\p{L}\p{N}_]+`

**Files:**
- Modify: `name_uri_analysis_rs/Cargo.toml` (add `regex`, `once_cell` if needed)
- Modify: `name_uri_analysis_rs/src/analysis/metadata.rs` (`metadata_bm25_tokens` ~3332)
- Modify: tests in same file / `metadata_tests`

- [ ] Add `regex = "1"` (and `once_cell` if not present) to name_uri dependencies
- [ ] Replace hand-rolled alphanumeric tokenizer with Lazy `TOKEN_RE = [\p{L}\p{N}_]+`, filter `len() >= 2`
- [ ] Add `// Keep in sync with top_contract_analysis_rs::analysis::scoring::TOKEN_RE`
- [ ] Add/adjust test that Unicode letter/number tokens match regex semantics
- [ ] `cargo test -p name_uri_analysis_rs metadata_bm25` (or full crate tests for metadata)

### Task 2: P0 — Unify eligibility (Rust + SQL)

**Files:**
- Modify: `name_uri_analysis_rs/src/analysis/duckdb_prep.rs` (`metadata_json_eligible_predicate`)
- Modify: `name_uri_analysis_rs/src/analysis/metadata.rs` (fallback SQL ~1066, `metadata_is_dedup_eligible`)
- Modify: `top_contract_analysis_rs/src/store/duckdb_store.rs` (`sql_metadata_json_eligible_predicate`) — verify already trim+LIKE; align comments
- Modify: `top_contract_analysis_rs/src/analysis/scoring.rs` (`metadata_is_dedup_eligible`)

- [ ] Rust both sides: trim, non-empty, len≤64KiB, first char `{`|`[`
- [ ] name_uri SQL: use `trim(coalesce(...))` + length + starts_with/LIKE on trimmed value (even if analysis_rows already trims)
- [ ] Sync comments on eligibility helpers
- [ ] Unit test: leading-whitespace JSON `" {...}"` is eligible in Rust

### Task 3: P0 — Alchemy ops note

**Files:**
- Modify: `top_contract_analysis_rs/README.md`

- [ ] At `--alchemy-api-max-concurrency 16`, note account must tolerate 16; override flag to reduce

### Task 4: P1 — top_contract string BM25 → test-only

**Files:**
- Modify: `top_contract_analysis_rs/src/analysis/scoring.rs`
- Grep: ensure no production callers of string BM25 APIs outside `#[cfg(test)]`

- [ ] Gate `MetadataBm25Corpus`, `MetadataBm25Query`, `MetadataBm25CorpusBuilder`, `bm25_score_terms*`, `score_metadata_indexed_pair_with_*` (string), `score_metadata_document_pair*` behind `#[cfg(test)]` **or** keep thin `pub` wrappers that delegate to compact if external API required
- [ ] Prefer: keep `pub fn score_metadata_document_pair` as thin wrappers using compact corpus if anything outside tests needs them; otherwise cfg(test)
- [ ] Existing compact-vs-string tests still compile
- [ ] `cargo test` in top_contract_analysis_rs

### Task 5: P1 — Split name_uri metadata into submodule

**Files:**
- Create: `name_uri_analysis_rs/src/analysis/metadata/mod.rs`
- Create: `name_uri_analysis_rs/src/analysis/metadata/parse.rs`
- Create: `name_uri_analysis_rs/src/analysis/metadata/bm25.rs`
- Create: `name_uri_analysis_rs/src/analysis/metadata/load.rs`
- Create: `name_uri_analysis_rs/src/analysis/metadata/index.rs`
- Create: `name_uri_analysis_rs/src/analysis/metadata/sketch.rs` (if clean)
- Modify: `name_uri_analysis_rs/src/analysis.rs` — replace `include!("analysis/metadata.rs")` with `mod metadata; use metadata::*;` or `pub(crate) use metadata::*`
- Delete: `name_uri_analysis_rs/src/analysis/metadata.rs` after move

**Visibility rules:**
- Parent still uses `include!` for other files; metadata becomes a real child module
- Export `run_metadata_analysis` and anything duckdb_prep needs (`MAX_METADATA_BYTES_FOR_DEDUP` if shared — currently duplicated const; keep in parse or re-export)
- Use `super::` for `AnalysisError`, `ProgressTracker`, Arrow helpers in duckdb_prep, etc.
- Extract shared Okapi term score into `bm25.rs`

- [ ] Mechanical move first (mod.rs includes all code), compile
- [ ] Then split into parse/bm25/load/index/sketch
- [ ] `cargo test` name_uri

### Task 6: P2 — Cleanup

**Files:**
- Modify: `name_uri_analysis_rs/Cargo.toml` (remove strsim)
- Modify: `name_uri_analysis_rs/src/analysis/name_scoring.rs` (oracle via PreparedNameQuery)
- Modify: `name_uri_analysis_rs/src/analysis/duckdb_prep.rs` (delete execute_duckdb_progress_batch; call execute_progress_batch)
- Sync comments on PreparedNameQuery both crates

- [ ] Remove strsim; fix `score_normalized_name_pair` test helper
- [ ] Delete progress-batch wrapper
- [ ] Keep-in-sync comments on PreparedNameQuery
- [ ] Optional: simplify test-only NameCandidateScratch::new if low risk

### Task 7: Full verification

- [ ] `cd name_uri_analysis_rs && cargo test`
- [ ] `cd top_contract_analysis_rs && cargo test`
- [ ] Confirm no shared crate / no inter-crate path dependency

---

## Self-review notes

- Spec coverage: P0/P1/P2 and non-goals mapped to tasks.
- `include!` → `mod` is the main structural risk; Task 5 does compile-first then split.
- String BM25 public API: grep showed only scoring.rs uses; safe to cfg(test) or thin-wrap.
