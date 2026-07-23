# analysis2 Experimental In-Memory Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship standalone workspace `analysis2/` that loads four-chain Parquet fully into memory, runs seed-scoped Name/URI/Metadata dedup, enriches candidates, runs deep analysis, and emits the report hierarchy in `docs/superpowers/specs/2026-07-23-analysis2-experimental-design.md`.

**Architecture:** Thin `analysis2_core` (resident store, query-to-index dedup, enrich, analysis, reporting) + `analysis2_cli` (clap, progress/ETA, pipeline orchestration). Single process; Rayon for CPU; Tokio for HTTP. No DuckDB, no spill, no checkpoint.

**Tech Stack:** Rust edition 2021, clap, parquet+arrow, rapidfuzz, rayon, tokio, reqwest, ahash, serde_json, sha2, num-bigint, unicode-normalization (only if needed for metadata canonicalization parity).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-23-analysis2-experimental-design.md`
- Business metrics/definitions: `docs/analysis/REWRITE_DESIGN.md` except intentional divergences in the spec (Solana NFT-level Name→collection; descending anchors; OpenSea minimized)
- Algorithm reference only: `dedup/crates/core` — **do not** path-depend or import it
- Do **not** depend on or modify `analysis/`, `top_contract_analysis_rs`
- Workspace: `analysis2/` standalone (not root workspace member)
- Packages: `analysis2_core`, `analysis2_cli`; binary: `analysis2`
- CLI flags only; no run TOML
- No spill / resume / memory-lease gates / MinHash-LSH-quotas
- OpenSea only for `select-seeds` EVM ranking and any evidence with no Alchemy/Helius alternative
- Do not commit unless the user explicitly asks

## File map (create)

```text
analysis2/
  Cargo.toml
  README.md
  crates/core/Cargo.toml
  crates/core/src/
    lib.rs
    error.rs
    progress.rs
    entity/{mod.rs, ids.rs, store.rs, string_pool.rs, csr.rs}
    parquet/{mod.rs, validate.rs, pass1.rs, pass2.rs, merge.rs}
    dedup/{mod.rs, hits.rs, candidates.rs, uri.rs, name/{mod.rs, bounds.rs, representative.rs}, metadata/{mod.rs, canonical_json.rs, anchors.rs, bm25.rs}}
    seed/{mod.rs, select.rs, manifest.rs}
    enrich/{mod.rs, http.rs, alchemy.rs, helius.rs, etherscan.rs, opensea.rs, prices.rs, quality.rs}
    analysis/{mod.rs, legit.rs, attribution.rs, lifecycle.rs, behavior.rs, economics.rs, graph.rs}
    reporting/{mod.rs, json.rs, markdown.rs, aggregate.rs, manifest.rs}
  crates/cli/Cargo.toml
  crates/cli/src/
    main.rs
    progress.rs
    pipeline.rs
  crates/core/tests/
    load_entities.rs
    dedup_oracle.rs
    report_golden.rs
  crates/core/testdata/   # tiny parquet fixtures
```

## Phased delivery

| Phase | Tasks | Independently runnable |
|---|---|---|
| A Offline core | 1–8 | `analysis2 run-dedup` on fixtures |
| B Seeds + enrich | 9–10 | `select-seeds`; enrich with HTTP mocks |
| C Analysis + full run | 11–13 | `analysis2 run` end-to-end on fixtures |

Stop after Phase A if only offline verification is needed; Phases B–C complete the spec.

---

### Task 1: Workspace scaffold + CLI skeleton

**Files:**
- Create: `analysis2/Cargo.toml`
- Create: `analysis2/README.md` (stub pointing to spec)
- Create: `analysis2/crates/core/Cargo.toml`
- Create: `analysis2/crates/core/src/lib.rs`
- Create: `analysis2/crates/core/src/error.rs`
- Create: `analysis2/crates/cli/Cargo.toml`
- Create: `analysis2/crates/cli/src/main.rs`

**Interfaces:**
- Produces: binary `analysis2` with subcommands `select-seeds | run | run-dedup` and flags from the spec example; engines print `not implemented` until later tasks

- [ ] **Step 1:** Create workspace `Cargo.toml` with members `crates/core`, `crates/cli`; packages named `analysis2_core` / `analysis2_cli`; binary name `analysis2`
- [ ] **Step 2:** Define `Analysis2Error` with variants `Invalid`, `Io`, `Parquet`, `Http`, `Cancelled` and `Display`/`From` for common sources
- [ ] **Step 3:** Clap CLI with required flags for `run` / `run-dedup` (`--input` repeatable, `--seeds`, `--output-dir`, thresholds, API keys optional, `--rayon-threads`, `--http-concurrency`, `--progress`)
- [ ] **Step 4:** Run `cargo build --manifest-path analysis2/Cargo.toml` — expect success
- [ ] **Step 5:** Run `cargo run --manifest-path analysis2/Cargo.toml -- run-dedup --help` — expect flags listed

---

### Task 2: Progress observer + EWMA ETA

**Files:**
- Create: `analysis2/crates/core/src/progress.rs`
- Create: `analysis2/crates/cli/src/progress.rs`
- Test: unit tests in `analysis2/crates/core/src/progress.rs` or `crates/cli`

**Interfaces:**
- Produces:

```rust
pub trait ProgressObserver: Send + Sync {
    fn set_stage(&self, stage: &str);
    fn begin_phase(&self, phase: &str, total: Option<u64>);
    fn add_completed(&self, n: u64);
    fn check_cancelled(&self) -> Result<(), Analysis2Error>;
    fn finish(&self);
}
```

- CLI: `auto|tty|json|off`; EWMA alpha `0.25`; ETA confident after ≥3 positive throughput samples (same UX as `dedup`)

- [ ] **Step 1:** Write failing test for EWMA ETA after three samples
- [ ] **Step 2:** Implement `NoopProgress` + CLI reporters
- [ ] **Step 3:** `cargo test --manifest-path analysis2/Cargo.toml -p analysis2_core progress` — PASS

---

### Task 3: Entity IDs + ResidentStore skeleton

**Files:**
- Create: `analysis2/crates/core/src/entity/mod.rs`
- Create: `analysis2/crates/core/src/entity/ids.rs`
- Create: `analysis2/crates/core/src/entity/string_pool.rs`
- Create: `analysis2/crates/core/src/entity/csr.rs`
- Create: `analysis2/crates/core/src/entity/store.rs`

**Interfaces:**
- Produces:

```rust
pub type ChainId = u16;
pub type ContractId = u32;
pub type NftId = u32;
pub type StringId = u32;

pub struct SourceOrder { pub file_ordinal: u32, pub file_row_number: u64 }
pub struct Contract { /* chain_id, address, nft_count, name_id: Option<StringId>, ... */ }
pub struct Nft { /* contract_id, token_id, name_id, token_uri_id, image_uri_id, source_order */ }
pub struct ResidentStore { /* chains, contracts, nfts, strings, uri/name csr stubs, totals */ }
```

- [ ] **Step 1:** Implement `StringPool::intern(&mut self, s: &str) -> StringId` with ahash map
- [ ] **Step 2:** Unit test intern dedup + empty-string → `None` helpers
- [ ] **Step 3:** `cargo test -p analysis2_core string_pool` — PASS

---

### Task 4: Two-pass Parquet load

**Files:**
- Create: `analysis2/crates/core/src/parquet/*.rs`
- Create: `analysis2/crates/core/testdata/` tiny multi-chain parquet (script or checked-in bytes)
- Test: `analysis2/crates/core/tests/load_entities.rs`

**Interfaces:**
- Consumes: `ProgressObserver`, `LoadOptions { allowed_chains, evm_chains, metadata_anchors }`
- Produces:

```rust
pub fn load_resident_store(
    inputs: &[PathBuf],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, Analysis2Error>;
```

Behavior:
- Pass 1: `chain, contract_address, token_id, name_norm, token_uri_norm, image_uri_norm`
- Pass 2: `chain, contract_address, token_id, metadata_json` only
- Parallel per-file scan → ordered merge by input file order
- Conflicting non-empty URI for same logical key → error
- Build URI CSR postings; defer Name representative finalize to Task 6 helpers called at end of load
- Metadata anchors: token id **descending**, first `k` valid records

- [ ] **Step 1:** Write fixture parquet (2 EVM contracts + 1 Solana collection, few NFTs, one duplicate URI)
- [ ] **Step 2:** Failing integration test: totals, URI posting counts, descending anchors order
- [ ] **Step 3:** Implement validate + pass1 + pass2 + merge
- [ ] **Step 4:** `cargo test --manifest-path analysis2/Cargo.toml --test load_entities` — PASS

---

### Task 5: HitGraph + CandidateRegistry + scopes

**Files:**
- Create: `analysis2/crates/core/src/dedup/mod.rs`
- Create: `analysis2/crates/core/src/dedup/hits.rs`
- Create: `analysis2/crates/core/src/dedup/candidates.rs`
- Create: `analysis2/crates/core/src/reporting/aggregate.rs` (scope counter helpers only)

**Interfaces:**
- Produces:

```rust
pub enum Dimension { Name, TokenUri, ImageUri, Metadata }
pub enum ScopeKind { IntraChain, ChainMatrix, CrossChainSummary }

pub struct HitEdge {
    pub seed_contract: ContractId,
    pub candidate_contract: ContractId,
    pub candidate_nft: Option<NftId>, // URI hits; Name/Metadata may be None meaning whole contract
    pub dimension: Dimension,
    pub score: f64, // 1.0 for exact
    pub primary_chain: ChainId,
    pub secondary_chain: ChainId,
}

pub struct HitGraph { /* push edge; exclude seed self; union helpers */ }
pub struct CandidateRegistry { /* from HitGraph: unique candidates, per-seed relations */ }
```

Counting rules follow `REWRITE_DESIGN.md` scopes; Name/Metadata contract hits expand to all NFTs of that contract when computing NFT numerators.

- [ ] **Step 1:** Unit tests: self-hit excluded; four-dimension union does not double-count NFT; image_uri supplemental not double-counted with token_uri same NFT/scope
- [ ] **Step 2:** Implement structures
- [ ] **Step 3:** Tests PASS

---

### Task 6: URI query-to-index engine

**Files:**
- Create: `analysis2/crates/core/src/dedup/uri.rs`
- Test: unit + extend `dedup_oracle.rs`

**Interfaces:**
- Consumes: `&ResidentStore`, seed `ContractId`, thresholds unused
- Produces: edges into `&mut HitGraph`

```rust
pub fn query_uri_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error>;
```

- Exact token_uri then image_uri fallback per NFT/scope
- Intra-chain requires ≥2 distinct contracts sharing URI
- Cross-chain requires presence on both chains

- [ ] **Step 1:** Oracle fixture: known URI duplicates across chains
- [ ] **Step 2:** Implement posting lookup
- [ ] **Step 3:** Tests PASS

---

### Task 7: Name engine (EVM representative + Solana NFT→collection)

**Files:**
- Create: `analysis2/crates/core/src/dedup/name/mod.rs`
- Create: `analysis2/crates/core/src/dedup/name/bounds.rs`
- Create: `analysis2/crates/core/src/dedup/name/representative.rs`
- Reference algorithm: `dedup/crates/core/src/name/candidate_bounds.rs` (copy logic, rewrite)

**Interfaces:**

```rust
pub fn finalize_name_index(store: &mut ResidentStore) -> Result<(), Analysis2Error>;
pub fn query_name_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64, // default 0.98
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error>;
```

Rules:
- EVM: mode representative Name; query that string against all-chain name postings
- Solana seed: each NFT name queries index; **any hit marks whole seed collection** (and candidate side: Solana NFT hit marks that collection)
- Lossless CandidateBounds + JW verify; byte-equal short circuit
- Threshold is fraction in `[0,1]` (spec), not percent

- [ ] **Step 1:** Port/adapt bounds unit tests from dedup semantics for threshold 0.98
- [ ] **Step 2:** Test Solana: one NFT JW hit → candidate collection edge + all NFTs counted in scope helpers
- [ ] **Step 3:** Implement finalize + query
- [ ] **Step 4:** Tests PASS

---

### Task 8: Metadata engine (descending anchors + BM25)

**Files:**
- Create: `analysis2/crates/core/src/dedup/metadata/*.rs`
- Reference: `dedup/crates/core/src/metadata/bm25.rs` + `canonical_json.rs` (rewrite, no template/LSH)

**Interfaces:**

```rust
pub fn finalize_metadata_index(store: &mut ResidentStore) -> Result<(), Analysis2Error>;
pub fn query_metadata_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64, // default 0.6
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error>;
```

- Anchors: descending token id, first k valid
- Align largest shared / largest each side
- Exact canonical JSON or BM25 cosine; lossless prune only

- [ ] **Step 1:** Unit test descending anchor selection (ids 1,2,10 with k=2 → 10 then 2)
- [ ] **Step 2:** BM25 threshold match/mismatch oracle
- [ ] **Step 3:** Implement
- [ ] **Step 4:** Tests PASS

---

### Task 9: Offline reporting + `run-dedup` pipeline

**Files:**
- Create: `analysis2/crates/cli/src/pipeline.rs`
- Create: `analysis2/crates/core/src/reporting/{mod.rs,json.rs,markdown.rs,manifest.rs}`
- Modify: `analysis2/crates/cli/src/main.rs`
- Test: `analysis2/crates/core/tests/report_golden.rs`

**Interfaces:**
- `run_dedup(config) -> Result<()>`: load → finalize indexes → for each seed in manifest: URI/Name/Metadata queries → CandidateRegistry → write:
  - `seeds/<chain>__<addr>/report.json|.md` (dedup sections only)
  - `intra_chain`, `chain_matrix`, `cross_chain`, `summary` stubs with duplicate-scale metrics
  - `run_manifest.json`, `failures.jsonl`

- [ ] **Step 1:** Golden fixture expected JSON fields for duplicate_nft_count / ratios
- [ ] **Step 2:** Wire CLI `run-dedup`
- [ ] **Step 3:** `cargo test --test report_golden` + manual `run-dedup` on testdata — PASS
- [ ] **Step 4:** Update README with Phase A commands

**Checkpoint:** Phase A complete — offline path usable.

---

### Task 10: Seed selection (`select-seeds`)

**Files:**
- Create: `analysis2/crates/core/src/seed/*.rs`
- Create: `analysis2/crates/core/src/enrich/http.rs` (shared client)
- Create: `analysis2/crates/core/src/enrich/opensea.rs` (ranking only)
- Create: `analysis2/crates/core/src/enrich/helius.rs` (DAS resolve for ME samples)

**Interfaces:**

```rust
pub struct SeedRecord { /* chain, address, rank, name, metric, window, source, collected_at */ }
pub fn select_seeds(opts: &SelectSeedsOptions) -> Result<(Vec<SeedRecord>, serde_json::Value /*audit*/), Analysis2Error>;
```

- EVM: OpenSea `thirty_days_volume` (necessary OpenSea use)
- Solana: Magic Eden `popular_collections?timeRange=30d` + Helius resolve collection address
- Default top 25 per chain; incomplete chain recorded in audit, not filled from other chains
- Write `seeds.json` + `seeds.audit.json`

- [ ] **Step 1:** HTTP mock tests for ranking parse + Solana resolve
- [ ] **Step 2:** Implement + CLI `select-seeds`
- [ ] **Step 3:** Tests PASS

---

### Task 11: Enrichment (Alchemy / Helius / Etherscan / Prices; OpenSea rare)

**Files:**
- Create: `analysis2/crates/core/src/enrich/*.rs`

**Interfaces:**

```rust
pub struct EvidenceBundle { /* events, transfers, sales, holders, gas, prices, quality flags */ }
pub async fn enrich_candidates(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error>;
```

Rules:
- One fetch per candidate contract
- Prefer Alchemy (EVM) and Helius (Solana) for sales/activity
- OpenSea sales only if required field missing from preferred sources
- Missing keys → `not_requested` quality, continue
- Bounded concurrency via semaphores; finite retries

- [ ] **Step 1:** Mock provider tests for quality state distinctions
- [ ] **Step 2:** Implement clients + orchestrator
- [ ] **Step 3:** Tests PASS

---

### Task 12: Deep analysis modules

**Files:**
- Create: `analysis2/crates/core/src/analysis/*.rs`
- Method hints: `top_contract_analysis_rs/src/analysis/{lifecycle,address_records,paper_stats}` and/or `analysis/src/analysis/*` — **read only, rewrite**

**Interfaces:**

```rust
pub struct CandidateAnalysis { /* attribution, lifecycle, behaviors, economics, legit flag */ }
pub fn analyze_candidate(
    store: &ResidentStore,
    contract: ContractId,
    evidence: &EvidenceBundle,
    cfg: &PaperConfig,
) -> Result<CandidateAnalysis, Analysis2Error>;
```

Implement in order with unit tests on synthetic graphs:
1. `legit.rs` — mark verified legit duplicates
2. `graph.rs` — transfer/sale graph + SCC once
3. `attribution.rs` — role labels + evidence
4. `lifecycle.rs` — timelines + value flow aggregates
5. `behavior.rs` — wash / pump-exit / sybil / fraud / poisoning / layered / inventory
6. `economics.rs` — Setup/Lure/Exit costs, output ratios, honest loss

Default paper knobs: min cycle 2, layered path 3, fan-out 3, top 10% concentration.

- [ ] **Step 1:** Synthetic wash-cycle test (2-node SCC)
- [ ] **Step 2:** Implement modules behind `analyze_candidate`
- [ ] **Step 3:** Tests PASS

---

### Task 13: Full `run` pipeline + complete reports

**Files:**
- Modify: `analysis2/crates/cli/src/pipeline.rs`
- Modify: `analysis2/crates/core/src/reporting/*`
- Modify: `analysis2/README.md`

**Flow:**

```text
load → finalize indexes → dedup all seeds → CandidateRegistry
  → enrich (Tokio) overlapped with any remaining CPU finalize
  → analyze_candidate per unique candidate (Rayon)
  → write candidate JSONs, per-seed reports, aggregates, summary, manifest
  → drop indexes / bundles after last write
```

- Incomplete four-scope seeds excluded from formal denominators
- Failures append `failures.jsonl`; do not write `complete` on cancel/OOM path
- Cross-chain summary sums USD only

- [ ] **Step 1:** Fixture `run` with mocked enrich (feature or inject trait) produces summary with expected keys from `REWRITE_DESIGN.md` report sections
- [ ] **Step 2:** Wire real `run` CLI
- [ ] **Step 3:** README: build, `select-seeds`, `run-dedup`, `run`, hardware notes
- [ ] **Step 4:** `cargo test --manifest-path analysis2/Cargo.toml` and `cargo build --release --manifest-path analysis2/Cargo.toml` — PASS

**Checkpoint:** Spec v1 complete.

---

## Spec coverage

| Spec item | Task |
|---|---|
| Standalone workspace / CLI flags | 1 |
| Progress + ETA | 2 |
| ResidentStore + StringPool | 3 |
| Two-pass Arrow load, descending anchors | 4 |
| HitGraph / scopes / candidate merge | 5 |
| URI exact query-to-index | 6 |
| Name JW + Solana NFT→collection | 7 |
| Metadata BM25 no LSH | 8 |
| `run-dedup` + offline reports | 9 |
| `select-seeds` (OpenSea EVM necessary) | 10 |
| Enrich, OpenSea minimized | 11 |
| Deep analysis modules | 12 |
| Full `run` + report hierarchy | 13 |
| Non-goals (no DuckDB/spill/gates) | Global + Tasks 1–13 |

## Notes for implementers

- Prefer copying **algorithms** from `dedup` (bounds, BM25 math) rather than inventing new thresholds
- Prefer reading `REWRITE_DESIGN.md` for metric formulas when filling report fields
- Keep files focused; if a module exceeds ~800 lines, split by the file map above
- When live API keys are absent, tests must use mocks; never require network for `cargo test`
