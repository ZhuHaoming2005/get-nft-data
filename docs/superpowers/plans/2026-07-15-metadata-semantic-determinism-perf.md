# Metadata Semantic Determinism + Perf Refactor Implementation Plan

> **For agentic workers:** Implement task-by-task in this session without subagents. Steps use checkbox (`- [ ]`) syntax for tracking. Do **not** require byte-identical features/blocking/IDs across thread counts.

**Goal:** Speed up Prepare/Encode/Match while keeping final normalized groups and summary rows as the only deterministic boundary.

**Architecture:** Drop global stable internal IDs; shard PayloadArena and term dictionaries by hash; stream fallback; build SoA/CSR directly; add proof-safe Match signatures; drop non-semantic edge sorts; bump encode/blocking/stage revisions with run-scoped staging.

**Tech Stack:** Rust, DuckDB, Rayon, Arrow, existing `metadata_engine` + `name_uri_analysis_rs`.

## Global Constraints

- Determinism boundary: normalized duplicate groups + summary field equality only.
- Internal IDs, processing order, forest edges, features/blocking bytes may change across threads/runs.
- No new performance profiling instrumentation; gate is end-to-end wall time only (must not regress).
- Preserve business tie-breaks: lowest token-ID representative; `(source_file, source_row_number)`; fallback candidate order; Solana address case rules.
- Keep three column-pruned Parquet scans; do not introduce wide `analysis_rows` staging.
- Payload CAS byte compare on digest collision remains mandatory (length + full bytes).
- Template vs content dictionaries stay separate.
- Do not remove rescue / shared-token / recall paths.
- Match continues to ignore Prepare DuckDB.

## File Map

| Area | Primary files |
|------|----------------|
| Oracle | `name_uri_analysis_rs/src/analysis/semantic_oracle.rs`, tests under `analysis/metadata/tests/` |
| Prepare IDs | `duckdb_prep.rs`, `metadata/prepare.rs`, `tests/sql_output.rs`, encode prepare-identity tests |
| Sharded arena | `metadata_engine/src/encode/payload_arena.rs` (+ new shard wrapper) |
| Encode stream | `name_uri_analysis_rs/src/analysis/metadata/encode.rs` |
| Feature SoA | `metadata_engine/src/encode/feature_soa.rs`, `csr.rs` |
| Blocking | `metadata_engine/src/blocking/*` |
| Match | `metadata_engine/src/pipeline.rs`, scoring helpers |
| Revisions | `ENCODE_SCHEMA_REVISION`, `BLOCKING_REVISION`, `METADATA_ENCODE_STAGE_REVISION`, `METADATA_MATCH_STAGE_REVISION`, `artifacts.rs` |

---

### Task 1: Semantic consistency oracle

**Files:**
- Create: `name_uri_analysis_rs/src/analysis/semantic_oracle.rs`
- Modify: `name_uri_analysis_rs/src/analysis.rs` (mod + re-export for tests)
- Modify: `name_uri_analysis_rs/src/analysis/metadata/tests/encode.rs` (replace byte-determinism assertions)

**Produces:**
- `normalize_summary_rows(&[SummaryRow]) -> Vec<SummaryRow>`
- `normalize_metadata_summary_rows(&[MetadataSummaryRow]) -> Vec<MetadataSummaryRow>`
- `normalize_duplicate_groups(groups: Vec<Vec<ContractKey>>) -> Vec<Vec<ContractKey>>` where `ContractKey = (chain, address)`
- `assert_semantic_eq(left, right)` helpers

- [x] **Step 1:** Add oracle module with sort keys: summary by `(field_name, scope, primary_chain, secondary_chain, match_mode, metric)`; groups by sorted member keys then group lex order.
- [x] **Step 2:** Unit tests: permutations of equivalent groups/summaries compare equal; differing membership fails.
- [x] **Step 3:** Replace `parallel_encode_is_byte_deterministic_across_thread_counts` with Encode→Match summary oracle across threads 1/4.
- [x] **Step 4:** Replace `prepare_dense_identities_are_deterministic_across_thread_counts` with business-key set equality (chain, address, retained token strings), not dense ID equality.

---

### Task 2: Prepare — unordered dense internal IDs

**Files:**
- Modify: `duckdb_prep.rs` (`build_core_rows_sql`, `analysis_contracts_sql` / `metadata_contract_index`)
- Modify: `metadata/prepare.rs` (`metadata_token_dictionary` token_index)
- Modify: `analysis/tests/sql_output.rs` expectations
- Keep: `selected_chains` `ORDER BY chain`
- Keep: URI drop before compact (already in `analysis.rs` via `drop_prepare_only_uri_tables`)

- [x] **Step 1:** `contract_id`: `row_number() OVER () - 1` (no `ORDER BY chain, contract_address`).
- [x] **Step 2:** `metadata_contract_index`: `row_number() OVER () - 1` (no `ORDER BY contract_id`).
- [x] **Step 3:** `token_index`: `row_number() OVER () - 1` (no `ORDER BY token_id`).
- [x] **Step 4:** Update SQL tests; run prepare oracle test from Task 1. URI spill already dropped before compact.

---

### Task 3: Encode — sharded PayloadArena + JSON-free source catalog

**Files:**
- Modify/Create: `metadata_engine/src/encode/payload_arena.rs` (shard facade)
- Modify: `encode.rs` registration path
- Remove JSON from `TokenSourceRelation` / replace with `SourceCatalogEntry { source_file, source_row_number, payload_ref }`

**Produces:**
- `PayloadRef { shard_id: u16, local_id: u32 }`
- After registration: prefix-sum shard lengths → global `u32 payload_id`
- Arena insert still: digest → length check → full byte compare

- [x] **Step 1:** Implement `ShardedPayloadArena` with fixed shard count (e.g. next_pow2 of threads, clamped).
- [x] **Step 2:** Arrow batch: parallel presence → shard-local insert by digest high bits.
- [x] **Step 3:** Source catalog inserts JSON once into shards; membership stores `PayloadRef` only.
- [x] **Step 4:** Tests: collision fail-safe; remap to global IDs; no double JSON residency in relation.
- [x] Task 5 Step 3 follow-up: drop row-oriented `EncodeSourceRow`/`EncodeContractRow` from encode hot path (`EncodeSourceSoA` / `EncodeContractSoA`; row types remain for tests/compat wrappers).

---

### Task 4: Encode — streaming fallback (Arrow)

**Files:**
- Modify: `encode.rs` `resolve_pending_fallback_contracts`

- [x] **Step 1:** Arrow-batch stream ordered by contract/token/source coords.
- [x] **Step 2:** Keep only cross-batch contract cursor + selected flag; first presence hit records `payload_ref`.
- [x] **Step 3:** Admit memory per selected row only.

---

### Task 5: Encode — fused parse + two-level term dict + direct SoA/CSR

**Files:**
- Modify: `encode.rs`, `feature_soa.rs`, term interning helpers
- Delete intermediate `Vec<EncodeContractRow>` / `Vec<EncodeSourceRow>` / per-payload `Vec<(term,freq)>` / `Vec<Vec<u32>> fallback_atoms` from hot path
  (Done: `EncodeContractSoA` / `EncodeSourceSoA` / `PayloadTermSoA` / `FallbackAtomCsr`; row types kept for test/compat APIs.)

- [x] **Step 1:** Per payload shard: bounded parse → local term freq → local CSR → drop parse.
  (Parse+intern fused per unique-payload batch; full local CSR builder still uses EncodePayloadRow.)
- [x] **Step 2:** Merge local dicts via term-hash shards + full string compare → arbitrary global term IDs → remap tables (no lexical sort).
  (`ShardedPayloadTermInterner` assigns arbitrary global IDs under hash shards.)
- [x] **Step 3:** Two-pass SoA: count → prefix offsets → parallel fill flat arrays → persist.
  (`PayloadTermSoA::from_term_lists_parallel` + `append_soa`; persist via `write_encode_artifacts_soa_*`. `EncodePayloadRow` remains for tests/compat wrappers.)

---

### Task 6: Atom / Blocking

**Files:**
- Modify: `blocking/*`, atom build in `encode.rs`

- [x] **Step 3:** Hot-block tiles `(ti,tj)` as independent Rayon catalog jobs.
- [x] **Step 1:** Atom via hash shard of `(chain_id, payload_feature_identity)` with full CSR content compare.
  (Identity stage still does content compare; atom map is hash-sharded.)
- [x] **Step 2:** Keep only required local sorts (block IDs per atom; CSR legality; scorer term-ID order).

---

### Task 7: Match — proof-safe signatures + drop edge sort

**Files:**
- Modify: `feature_soa.rs` (persist 256-bit template/content sigs; optional retained-token sig)
- Modify: `pipeline.rs` / scoring (AND-zero reject before CSR intersect)
- Modify: `compact_scope_edges` — remove `sort_unstable`+`dedup`; lane-local forests then merge

- [x] **Step 1:** Signature bits from term IDs (multi-bit); document FP-only property.
- [x] **Step 2:** Wire reject path before BM25/intersect.
- [x] **Step 3:** Replace edge sort with unordered UF; merge lane forests.

---

### Task 8: Revisions + staging + recovery tests

**Files:**
- Bump: `ENCODE_SCHEMA_REVISION` 2→3, `BLOCKING_REVISION` 2→3, `METADATA_ENCODE_STAGE_REVISION` 4→5, `METADATA_MATCH_STAGE_REVISION` 11→12
- Modify: `artifacts.rs` for run-scoped staging dir; atomic ready flip
- Tests: upgrade from encode-2 leftovers; no CAS reuse; oracle across 1/4/max threads

- [x] **Step 1:** Bump revisions together.
- [x] **Step 2:** Write to staging, checksum, then publish ready (never reuse old `encode-2`).
- [x] **Step 3:** End-to-end oracle + wall-time smoke on tiny fixture (no regression vs previous on same machine/fixture).

## Verification gates (every task)

1. Normalized groups equal across threads 1/4/(max if cheap).
2. Summary rows equal field-for-field after normalize.
3. Representative / fallback / source-membership business rules unchanged.
4. `cargo fmt` + relevant `cargo test` + clippy on touched crates.
5. No e2e wall-time regression on the tiny fixture path used in CI (informational timer in test stdout only if already present; do not add profiles).
