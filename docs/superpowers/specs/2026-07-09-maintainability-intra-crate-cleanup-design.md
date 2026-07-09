# Maintainability Intra-Crate Cleanup Design

**Date:** 2026-07-09  
**Status:** Approved for planning  
**Scope:** `name_uri_analysis_rs`, `top_contract_analysis_rs`

## Goal

Reduce maintainability debt introduced by the recent performance work: unify tokenizer and eligibility semantics, split oversized modules, collapse dual-track BM25 to a single production path, and finish small cleanup items — **without** extracting a shared crate between the two scripts.

## Constraints

- Tokenizer semantics must match `top_contract_analysis_rs`: `[\p{L}\p{N}_]+` with token length ≥ 2. `name_uri_analysis_rs` changes to this rule.
- Do **not** create a workspace shared library.
- Do **not** make `name_uri_analysis_rs` depend on `top_contract_analysis_rs` (or vice versa).
- Do **not** replace hand-rolled Okapi BM25 or SimHash with a general search library.
- Cross-crate duplication is allowed; duplicated symbols must carry `// Keep in sync with <other_crate>::...` comments.
- CLI / report field semantics stay unchanged (thresholds, ratio denominators, `arg_min` representative selection rules).

## Non-Goals (this round)

- Shared crate / workspace extraction.
- Replacing BM25 with tantivy/lindera/etc.
- Removing name-scoring Sparse scratch mode.
- `metadata_row_id` deferred JOIN for `analysis_contracts` (observe memory first).
- Behavioral changes to scoring thresholds or recall policy.

## Approach

**Approach 1 (chosen):** Intra-crate convergence. Each crate cleans its own modules; cross-crate copies stay but are documented as sync contracts.

Rejected:

- Shared crate (user constraint).
- One-way crate dependency (couples analysis and collection tools).
- P0+P2 only without module split (leaves the `metadata.rs` god file).

## P0 — Semantic Alignment

### Tokenizer

- **Source of truth:** `top_contract_analysis_rs` `TOKEN_RE = [\p{L}\p{N}_]+`, keep tokens with `len() >= 2`.
- **Change:** Replace `name_uri_analysis_rs` hand-rolled `char::is_alphanumeric || '_'` tokenizer with the same regex rule (local copy of `TOKEN_RE` / `tokenize`, not a shared dependency beyond both crates already using `regex` if needed — add `regex` to `name_uri` if missing).
- Add or extend a Unicode boundary test so both crates agree on representative inputs (e.g. NFKC edge cases already covered in top_contract `has_terms` tests).

### Eligibility (Rust + SQL, same semantics, separate implementations)

Unified rule:

1. `trim` the metadata string.
2. Non-empty.
3. `len() <= MAX_METADATA_BYTES_FOR_DEDUP` (64 KiB).
4. First character is `{` or `[`.

- Rust: both crates implement the same predicate (already close; keep identical).
- SQL: both crates generate predicates that apply the same trim/coalesce/length/prefix checks. Prefer explicit `trim` in SQL even where `analysis_rows` already trims, so later projection changes cannot desync.
- Leading-whitespace JSON must be eligible in both Rust and SQL.

### Ops documentation

- In `top_contract_analysis_rs/README.md`, at `--alchemy-api-max-concurrency 16`, add one ops note: the target Alchemy account/environment must tolerate 16 concurrent requests; otherwise override with `--alchemy-api-max-concurrency`.

## P1 — Intra-Crate Structure

### `name_uri_analysis_rs`: split `metadata.rs`

Replace the ~4.6k-line `src/analysis/metadata.rs` with:

```
src/analysis/metadata/
  mod.rs       // re-exports; preserve `analysis::metadata::*` paths
  parse.rs     // documents_from_json, prefilter, content, eligibility, normalize, tokenize
  bm25.rs      // Okapi term score shared by compact/interned representations
  load.rs      // DuckDB/Arrow load of representative rows, fallback load
  index.rs     // interned/content atoms, postings, scratch pool/lease
  sketch.rs    // SimHash / band / anchor (only if cleanly extractable)
```

Rules:

- Prefer move + re-export over rewriting call sites.
- Extract a single `bm25_term_score(...)` (or equivalent) used by all Okapi paths in this crate.
- Keep production on the compact/interned path; any string-only BM25 helpers that exist only for tests should be `#[cfg(test)]` where practical.

### `top_contract_analysis_rs`: collapse dual-track BM25

- Production (`duplicate.rs`, `duckdb_store.rs`, support): only `CompactMetadataBm25*`.
- String `MetadataBm25Corpus` / `MetadataBm25Query` / related scorers: `#[cfg(test)]` or a `#[cfg(test)]` oracle module used to assert compact equivalence.
- Public test crates / `tests/algorithms.rs` (if any) must still compile against the oracle or compact API as needed — no production string path.

### Cross-crate sync comments

Add `// Keep in sync with ...` on:

- Tokenizer / `TOKEN_RE`
- `metadata_is_dedup_eligible` and `MAX_METADATA_BYTES_FOR_DEDUP`
- SQL eligibility template semantics
- `PreparedNameQuery` behavior (rapidfuzz Jaro–Winkler cutoff)

## P2 — Cleanup

- Remove `strsim` from `name_uri_analysis_rs` dev-dependencies; name tests use `PreparedNameQuery` / rapidfuzz with tolerance where needed.
- Delete `execute_duckdb_progress_batch` if it only forwards to `execute_progress_batch`; call the latter directly.
- Keep thread resolution helpers local to each crate (`resolve_threads` / `resolve_resource_threads`); optionally shorten comments, no shared module.
- Remove test-only forks that rebuild full indexes when production constructors suffice (`NameCandidateScratch::new` / `score_name_pairs_for_left_chunk` style helpers), if low-risk.
- Do not remove Dense/Sparse name scratch; document that Sparse is defensive for large atom sets.

## Testing / Acceptance

Commands:

```bash
cd name_uri_analysis_rs && cargo test
cd top_contract_analysis_rs && cargo test
```

Must hold:

1. Tokenizer: `name_uri` matches `[\p{L}\p{N}_]+` / len≥2; existing “matches top_contract template semantics” style tests still pass; add Unicode edge coverage if gaps appear.
2. Eligibility: Rust and each crate’s SQL agree on the same inputs, including leading-whitespace JSON.
3. top_contract production uses only compact BM25; string path is test-only.
4. `analysis::metadata::*` (or equivalent re-exports) still compile for existing call sites after the split.
5. No new shared crate; no inter-crate dependency between the two scripts.
6. Sync comments present on duplicated symbols listed above.
7. Alchemy default-16 ops note present in top_contract README.
8. `strsim` removed from name_uri; useless progress-batch wrapper removed if applicable.

## Risks

| Risk | Mitigation |
|------|------------|
| Tokenizer change shifts some `name_uri` BM25 scores | Pin with equivalence / template tests; accept Unicode-correct alignment to top_contract |
| SQL eligibility wording change alters representative rows | Add/adjust unit or SQL-level tests for trim + prefix; compare before/after on fixture rows |
| Large file split causes import breakage | Re-export from `metadata/mod.rs`; compile frequently |
| cfg(test)-only string BM25 breaks external callers | Grep for public string BM25 API usage before gating; keep thin wrappers if required |

## Implementation Order

1. P0 tokenizer + eligibility + Alchemy README note (smallest semantic change, highest sync value).
2. P1 top_contract BM25 collapse (smaller surface than metadata split).
3. P1 name_uri `metadata/` split + shared term score.
4. P2 cleanup (strsim, wrappers, test helpers, sync comments pass).
5. Full `cargo test` on both crates.
