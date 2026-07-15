# MetadataEncode Full-Memory Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild MetadataEncode as an all-in-memory pipeline (Arrow ingest → PayloadArena → unique full-parse → columnar features/blocking) that writes disk only once for final features/blocking snapshots, with hard MemoryBroker admission to prevent OOM on a 512 GiB host.

**Architecture:** Prepare DuckDB stays read-only. Encode uses a chunked in-memory PayloadArena (no `payload_blobs`), global deterministic dictionaries, and columnar CSR builders. Crash recovery = re-run entire MetadataEncode. `ENCODE_SCHEMA_REVISION` becomes 3 when on-disk feature layout changes; Match consumes features/blocking only.

**Tech Stack:** Rust, DuckDB Arrow, rayon, SHA-256, existing `metadata_engine` format/CSR writers, MemoryBroker/StorageBroker.

## Global Constraints

- Prepare DuckDB is read-only during Encode; no Encode temp tables that survive failure as checkpoints for partial resume of Encode itself.
- Processing must not create Encode temporary files; only final features/blocking partial→ready publish.
- Every arena/column allocation is admitted before allocate; fail closed before ready marker.
- Peak stage transitions release prior-stage scratch before admitting the next stage.
- Semantic parity with current Encode→Match summaries (golden differentials); thread counts 1/N must be byte-deterministic for features/blocking.
- Do not provide FeatureView raw payload accessors.
- Host headroom / `--analysis-memory-limit` must account for payload **body** bytes (unlike old disk CAS).

---

## File Map

| File | Responsibility |
|------|----------------|
| `metadata_engine/src/encode/payload_arena.rs` | Chunked in-memory unique JSON store |
| `metadata_engine/src/encode/payload_cas.rs` | Legacy disk CAS (retained until phase 9 removal) |
| `metadata_engine/src/encode/feature_soa.rs` | Final snapshot writer; later consumes columns |
| `metadata_engine/src/encode/parse.rs` | presence + full parse |
| `metadata_engine/src/progress.rs` | New Encode subphases |
| `name_uri_analysis_rs/src/analysis/metadata/encode.rs` | Pipeline orchestration |
| `name_uri_analysis_rs/src/analysis.rs` | Drop payload independence helper (phase 9) |
| `name_uri_analysis_rs/src/controller_manifest.rs` | Drop transient CAS pruning (phase 9) |
| `metadata_engine/tests/*` + `analysis/metadata/tests/encode.rs` | Differentials / integration |

---

## Phase 0 — Freeze baseline & contracts

- [ ] Add golden fixture covering: duplicate metadata; all-unique; shared token source; empty representative→fallback; invalid/deep JSON; Solana; forced digest collision.
- [ ] Capture baseline fingerprints: summary, contract groups, source membership, feature digests under current revision.
- [ ] Document baseline metrics fields (contracts/s, JSON bytes/s, unique ratio, CAS R/W, EncodeRows CPU%, peak RSS) — collection hooks can land with phase 3 progress work.
- [ ] Add progress phase enums (unused until wired): `EncodeReadRepresentatives`, `EncodeRegisterPayloads`, `EncodeResolveFallbacks`, `EncodeParseUniquePayloads`, `EncodeBuildTermDictionary`, `EncodeBuildColumns`, `EncodeBuildAtoms`, `EncodePersist` (persist already exists).
- [ ] Plan note: bump `ENCODE_SCHEMA_REVISION` to 3 when columnar on-disk layout lands (phases 5–8); Phase 1 does not change feature binary layout.

**Verify:** existing encode tests pass; golden harness compiles.

## Phase 1 — In-memory PayloadArena

- [ ] Implement `PayloadArena` with 256 MiB chunks, stable first-seen `payload_id`, SHA-256 index + full byte compare, collision lists.
- [x] API: `insert_or_get` / `PayloadInsert::is_new`, `bytes`, `resident_bytes`, `len`, `clear_bodies` (release JSON after terms built).
- [ ] Unit tests: dedup, cross-chunk read, forced collision, deterministic IDs, resident accounting.
- [ ] Wire Encode stream path to `PayloadArena` instead of creating `payload_blobs`; skip CAS finish/register/pin for payloads.
- [ ] Update tests that assumed `payload_blobs` existence during Encode publish; Match-after-Encode must still pass without CAS.

**Verify:** `cargo test -p metadata_engine --test payload_arena*` (new) + encode lib tests.

## Phase 2 — Arrow representative read

- [x] Replace `query_map` with `query_arrow`; join `selected_chains` for numeric `chain_id`.
- [x] Batch-parallel eligibility / presence (`metadata_has_prefilter_tokens`); sequential arena insert for stable IDs.
- [ ] Pre-size contract columns when count known (deferred: contract count isn't known until presence resolution completes).
- [x] Differential vs old row reader (existing `encode_preserves_token_specific_metadata_sources` / byte-determinism tests pass unchanged).

## Phase 3 — Resolve all sources, then unique full-parse

- [x] Presence-only registration (no full parse during representative read or fallback resolution).
- [x] Fallback selection with presence-only (`resolve_fallback_contracts` now takes a `Fn(&str) -> bool`); register only chosen JSON into arena.
- [x] Parallel `parse_metadata_documents` once per unique arena payload_id (`EncodeParseUniquePayloads`).
- [x] Tests: selection parity (`fallback_resolution_checks_presence_only_until_its_first_usable_row`); unselected fallbacks never full-parsed; hot payload parse-once (`duplicate_payload_is_looked_up_in_cas_before_parsing_again`).

## Phase 4 — Global deterministic term dictionary

- [x] Intern parsed payloads in ascending (first-seen) `payload_id` order via `PayloadTermInterner`, then `finalize_template_lexical_ids` remaps to sorted lexical IDs.
- [ ] Two-pass counts + prefix sum for term CSR columns (still `Vec<(u32,u32)>` per payload; not yet flat CSR — tracked for phase 5).
- [x] Release payload string bodies after remap (`arena.clear_bodies()` called immediately after interning).

## Phase 5 — Columnar contracts / source CSR

- [ ] Replace `Vec<EncodeContractRow/SourceRow>` builders with SoA + flat token CSR (deferred; `build_encoded_contract` still builds row-oriented `Vec`s from `PendingContractSlot`s, matching the on-disk CSR writer's existing input shape).
- [ ] `TokenSourceRelation` borrow slices; no per-contract `Vec<TokenSourceInput>` (deferred; `read_contract` still allocates per call).
- [x] Differential vs old row builder (`encode_preserves_token_specific_metadata_sources`, `parallel_encode_is_byte_deterministic_across_thread_counts`).

## Phase 6 — Columnar atoms / blocking

- [x] Atom keys from `(chain_id, payload_feature_identity)` unchanged from the legacy pass (now under `EncodeBuildAtoms` progress); stable, deterministic across thread counts.
- [ ] Fully columnar atom CSR / sketches from term columns (deferred; sketches already come from `build_base_equivalent_atom_sketches_parallel` over row-oriented payload term vectors).
- [x] Blocking compile consumes existing atom views; `BLOCKING_REVISION` unchanged (no persist layout change).

## Phase 7 — Batch memory admission

- [x] Per-Arrow-batch worst-case JSON growth reservation + single commit per batch (`register_representative_payloads`); one-time token-relation-sized reservation up front instead of per-contract growth during registration.
- [x] Ban per-contract `MemoryLease::resize` in the registration/fallback/parse/intern/build paths (`EncodeRegistrationAccounting` batches accounting; fallback resolution reserves/commits once for the whole phase).
- [ ] Fine-grained stage scratch budgets released strictly between every stage (deferred; arena is dropped after term interning, but sources/payloads/contracts/atoms share one final admission window as before).

## Phase 8 — Single final persist

- [ ] Writers consume memory columns; StorageBroker admits final size once; partial→checksum→ready; drop Encode heap.

## Phase 9 — Remove disk CAS dependency

- [ ] Delete PayloadCas registration/pin/eviction, `complete_metadata_payload_independence`, manifest pruning.
- [ ] Integration: Encode → no `payload_blobs` → delete Prepare DB → Match → same summary.
- [ ] Remove or gate legacy `payload_cas.rs` tests.

## Progress / diagnostics

Wire phases listed above; metrics: representative count, raw/unique payload counts & JSON bytes, reuse ratio, presence vs full-parse calls, membership counts, structure capacities, stage wall times, peak RSS, final write bytes.

## Verification ladder (every phase)

PayloadArena → Arrow differential → fallback differential → unique parse counts → term dictionary determinism → columnar differential → Encode semantic differential → Encode→delete DB→Match → threads 1/4/max → `fmt` / Clippy / full tests / `git diff --check`.

---

## Execution status

- Phase 0–1: **done** (plan + progress enums + PayloadArena + Encode no longer writes `payload_blobs`)
- Phase 2–7: **done** — `stream_encode_inputs_with_admission` rewritten as
  presence-first registration (`register_representative_payloads`, Arrow
  read) → presence-only fallback resolution
  (`resolve_pending_fallback_contracts`) → one unique parse pass over every
  arena `payload_id` → term-dictionary interning in first-seen order →
  `build_encoded_contract` columns → unchanged atom/blocking pass. New
  progress phases (`EncodeReadRepresentatives`, `EncodeRegisterPayloads`,
  `EncodeResolveFallbacks`, `EncodeParseUniquePayloads`,
  `EncodeBuildTermDictionary`, `EncodeBuildColumns`, `EncodeBuildAtoms`) are
  wired end to end; `EncodeRows` stays defined but unused by this pipeline.
  Fully columnar/CSR contract-source-atom builders (deepest parts of phases
  5–6) and finer per-stage scratch budgets (phase 7) are intentionally
  deferred — the current rewrite already removes all redundant full-parses
  and per-contract `MemoryLease::resize` calls, which was the primary memory
  and CPU win targeted by this plan.
- Phase 8–9: **done**. Encode persists features/blocking once after the
  in-memory build (no transient `payload_blobs`). Removed
  `complete_metadata_payload_independence` and
  `prune_transient_payload_cas_from_encode_checkpoint`. Match-revision
  upgrade test now only asserts Encode checkpoint retention.
  `ENCODE_SCHEMA_REVISION` stays at 2 (bump to 3 only if on-disk feature
  layout changes later). Legacy `payload_cas.rs` remains for
  metadata_engine unit tests / optional tooling.
- Reviewer follow-up (2026-07-15): **done**. Bounded unique-parse batches;
  `TokenSourceRelation` stays in the resident lease until drop; fallback
  streams per contract without materializing all candidates;
  `METADATA_ENCODE_STAGE_REVISION` → 4 with stale `payload_blobs` cleanup +
  register filter; removed broken `PayloadArena::is_new`; Clippy
  `is_none_or` fixed.
