# top_contract_analysis_rs Production Hardening Design

Date: 2026-07-13

Status: Approved

Target host: 128 vCPU, 512 GiB RAM

Target input: 200 million Parquet rows, about 16 GB compressed and 200 GB logical size

## 1. Objective

Bring `top_contract_analysis_rs` from a production candidate to a production-grade, interruptible batch-analysis program. The design prioritizes deterministic and complete analysis over silent degradation. It must:

- produce metadata recall candidates with no false negatives relative to the exact BM25 threshold;
- enforce a closed memory envelope on the 512 GiB target host;
- resume an in-place feature build after interruption without repeating committed phases;
- reject stale prepared data and result caches across format, algorithm, or binary changes;
- preserve deterministic output across input order, thread count, and recall batch-size changes;
- fail explicitly when an adversarial work set exceeds configured resource limits;
- retain high throughput through compact resident indexes, two-seed pipelining, and delayed payload loading.

This is a data-analysis executable, not a continuously available service. The feature database may be unavailable while an in-place rebuild is unfinished. Zero-downtime blue/green database replacement is therefore outside this design.

## 2. Accepted Semantics

### 2.1 Authoritative input

Every `prepare-features` invocation supplies the complete authoritative snapshot for every chain present in the Parquet inputs. Import replaces those chains. Rows absent from the new snapshot must not survive from an older generation.

Incremental merge is not the production default. If retained for library compatibility, it must be an explicitly named API or CLI mode and must never be selected implicitly by `prepare-features`.

### 2.2 Exact metadata recall

Metadata source filtering must not remove a document that satisfies the existing exact predicate `has_term_overlap && bm25_score >= metadata_threshold`. SimHash may influence processing order or diagnostics, but it must not be a hard exclusion predicate.

For the supported metadata threshold range `0..=1`, a positive BM25 match requires at least one query term in the candidate document. Therefore, the union of postings for all known query terms is a complete candidate set for exact BM25 scoring.

### 2.3 Resource limits

Resource limits reject a work unit; they never silently truncate it. A rejected work unit reports the measured cardinality and byte estimate so an operator can raise the limit intentionally.

## 3. Architecture

```text
authoritative Parquet files
  -> stable input fingerprint
  -> transactional chain replacement in nft_features
  -> import checkpoint
  -> per-chain representatives / URI postings / metadata documents
  -> per-chain checkpoints
  -> global DuckDB index rebuild
  -> prepared format and algorithm-version activation
  -> final checkpoint
  -> read-only analyze / batch

read-only analysis
  -> validate generation and all format/version fingerprints
  -> load/reuse compact Name index
  -> load/reuse exact Metadata term index
  -> stage URI, Name, and Metadata matches as fixed-width candidate rows
  -> deterministic SQL aggregation and per-contract ranking
  -> cardinality and byte admission checks
  -> chunked late payload fetch
  -> compact candidate plan
  -> provider-backed contract analysis
  -> durable versioned scoped reports and manifest
```

The major boundaries are:

- `PrepareCoordinator`: input fingerprinting, journal transitions, authoritative import, per-chain preparation, and final activation.
- `PreparedState`: generation and format/version validation used by every read-only entry point.
- `MemoryGovernor`: process-wide accounting for resident indexes and their in-flight builds.
- `CandidateStager`: fixed-width, connection-local DuckDB TEMP tables for URI, Name, and Metadata matches.
- `ExactMetadataRecallIndex`: compact BM25 corpus plus lossless term postings.
- `SnapshotBudget`: per-seed row, contract, and estimated-byte admission with no silent truncation.
- `RunIdentity`: input, algorithm, prepared format, report schema, and build fingerprints for batch recovery.

## 4. In-Place Prepare and Recovery

### 4.1 Journal

The feature database contains a prepare journal with one active run and explicit phases:

1. `fingerprinted`
2. `imported`
3. `prepared:<chain>` for every involved chain
4. `indexes_built`
5. `ready`

The journal stores:

- sorted canonical input paths;
- SHA-256 of every input and a combined input fingerprint;
- file length and modification time observed before and after hashing;
- expected chains;
- feature generation ID for every chain;
- prepared-format, normalization, recall-algorithm, and report-schema versions;
- phase timestamps and row counts;
- last failure text for operational diagnosis.

If a file changes while it is being fingerprinted, preparation fails before import. An unfinished journal with a different input fingerprint is rejected unless the operator uses `--restart-prepare`.

### 4.2 Authoritative import

The import transaction:

- exposes all input files through a non-materialized view;
- validates required columns and identity fields;
- materializes one deduplicated staging table;
- uses a complete deterministic tie-break over every persisted field;
- deletes every existing row for each chain present in staging;
- inserts staging rows ordered by `(chain, contract_address, token_id)`;
- records chain counts and new feature generations;
- invalidates prepared readiness;
- records the `imported` journal phase in the same commit.

A checkpoint follows the committed import. A restart with the same combined input fingerprint skips Parquet scanning and import.

### 4.3 Per-chain preparation

Each chain is prepared in its own transaction. The transaction replaces:

- contract representatives;
- fixed-width exact URI postings;
- compact metadata recall documents;
- the chain prepared-state row, including all format and algorithm versions.

After commit, the journal records `prepared:<chain>` and DuckDB performs a checkpoint. A restart skips matching completed chains. Global indexes are rebuilt after all chains are prepared. Final activation writes `indexes_built` and then `ready`, followed by a checkpoint.

Read-only analysis requires global `ready` plus matching state for every requested chain. Partially prepared databases are never treated as usable.

### 4.4 CLI recovery controls

- Default `prepare-features`: fingerprint inputs, resume a matching unfinished run, or start a new authoritative generation.
- `--prepare-only`: continue derived preparation without requiring Parquet arguments, but only when the journal identifies a committed import whose feature generations still match.
- `--restart-prepare`: discard unfinished journal state and start the authoritative import again.

## 5. Exact Recall and Candidate Staging

### 5.1 Metadata index

The resident metadata index contains:

- one compact metadata document per prepared contract;
- the shared compact BM25 corpus;
- `term_id -> Vec<u32 candidate_index>` postings;
- fixed-width candidate metadata and contract identity;
- generation scratch for posting-union deduplication.

For each seed profile:

1. compact the seed document through the resident corpus;
2. enumerate postings for every known seed term;
3. deduplicate candidate indexes with generation scratch;
4. execute exact BM25 scoring for every enumerated document;
5. append passing `(seed_index, feature_rowid, METADATA_REASON)` rows to candidate staging.

The implementation must have differential tests against exhaustive exact BM25. SimHash may remain for statistics or ordering but cannot remove candidates.

### 5.2 Unified candidate staging

Connection-local TEMP tables store only fixed-width candidate information until final selection:

- seed URI keys include `seed_index`, URI kind, full normalized URI, and hash;
- staged matches contain `seed_index`, `feature_rowid`, and reason bits;
- selected rows contain the deterministic ranked result.

URI matches are inserted directly with DuckDB joins. The query checks both the hash and full normalized URI, so hash collisions cannot become matches. Name and Metadata matches are appended in bounded chunks rather than accumulated into global Rust vectors.

After all recall channels finish, SQL:

- aggregates reason bits per `(seed_index, feature_rowid)`;
- joins the light identity projection from `nft_features`;
- counts contracts and rows for resource admission;
- ranks deterministically by seed, contract, strong-reason priority, token ID, and stable identity;
- applies `max_tokens_per_contract` in SQL;
- fetches full payload only for admitted selected rows, in bounded chunks.

The selection order cannot depend on DuckDB physical row ID, input order, worker count, or payload-fetch batch size.

## 6. Memory and CPU Model

### 6.1 Default envelope

The 512 GiB host has about 550 GB in decimal units. Default read-only allocation is:

- DuckDB total memory: 96 GB;
- managed Rust memory: 324 GB;
  - resident Name and Metadata indexes: at most 260 GB;
  - two seed snapshots: at most 24 GB each, 48 GB total;
  - Name/Metadata scratch: at most 16 GB;
- about 130 GB remains for Arrow, allocator overhead, API responses, stacks, page cache, and the OS.

Prepare retains 96 DuckDB threads and a 300 GB DuckDB memory limit.

Read-only defaults are:

- DuckDB: 64 threads total across two connections, 32 each;
- Rayon: 96 threads;
- seed recall/candidate-plan concurrency: 2;
- resident-index build concurrency: 1;
- matched-contract concurrency: 8.

DuckDB and Rayon are configured independently. The design intentionally allows bounded overlap between database work and CPU scoring, while memory admission remains the governing constraint.

### 6.2 MemoryGovernor

All resident indexes use one process-wide governor. A resident index is wrapped with a lease owned by the same `Arc` returned to callers. Removing the index from LRU does not release its lease while an active caller still holds the `Arc`.

Before construction, the loader estimates its build reservation from prepared row counts, string lengths, and posting counts. It acquires a lease before allocating. During build, growth requires the lease to grow first. If capacity is unavailable, inactive cache entries are evicted; active allocations remain accounted. Failure to reserve aborts construction before crossing the configured boundary.

Name and Metadata builds share one build gate. Cache statistics expose resident, active-evicted, building, scratch, and available bytes.

### 6.3 Per-seed guardrails

Defaults are:

- `max_snapshot_bytes_per_seed = 24GB`
- `max_candidate_contracts_per_seed = 100000`
- `max_selected_rows_per_seed = 2000000`
- `max_tokens_per_contract = 200`

The first three limits reject the seed and report diagnostics. They never change the result silently. Snapshot memory estimates include owned string capacities and a safety margin. A seed snapshot lease is acquired before loading and held through candidate-plan compaction.

After duplicate candidates are built, the plan removes metadata and derived structures no longer needed by provider analysis. The local provider fallback retains only contract address, token ID, token URI, image URI, name, and symbol.

## 7. Version and Cache Identity

Prepared and result-cache identity includes:

- prepared format version;
- normalization version;
- recall algorithm version;
- report schema version;
- compiled source/build fingerprint;
- chain feature generation;
- analysis parameters and seed-set identity.

Read-only startup rejects a mismatch with an actionable rebuild message. Batch recovery starts a new run when any identity component changes.

Recovery granularity is a complete seed. If any of the four secondary-chain scopes for a seed is missing or failed, recovery recomputes all four scopes for that seed so they share one provider context. Fully completed seeds may be reused.

## 8. Failure Handling and Durability

Resource-limit, stale-prepared, version-mismatch, interrupted-prepare, provider, and storage failures have distinct error variants. Resource-limit errors include chain, seed, measured rows/contracts/bytes, current reservations, and configured limits.

CLI parsing rejects non-finite or out-of-range thresholds, zero or unsafe concurrency, and inconsistent memory settings.

The first interrupt stops scheduling new work and requests exit at the current atomic phase boundary. A second interrupt may terminate immediately. A hard process kill relies on DuckDB rollback plus the persisted prepare or batch journal.

Scoped reports, summaries, and manifests are written using a temporary file, file sync, atomic rename, and parent-directory sync.

## 9. Observability

Human-readable logs and `run-metrics.jsonl` expose:

- build, algorithm, prepared, and report versions;
- DuckDB/Rayon/connection and memory configuration;
- prepare phase, chain, input and output row counts;
- governor resident, active-evicted, building, snapshot, scratch, and available bytes;
- URI, Name, and Metadata candidate and selected counts;
- cache hits, misses, evictions, build size, and build time;
- TEMP/WAL paths and checkpoint duration;
- current and peak RSS when supported;
- phase elapsed time and throughput.

Metrics must be useful after interruption and must not expose API keys or sensitive URLs.

## 10. Test-Driven Implementation and Acceptance

Every production behavior starts with a failing test. Required regression suites include:

1. Metadata posting candidates equal exhaustive exact BM25 for boundary and generated corpora.
2. Equivalent Parquet inputs produce identical rows and recall output across input order, thread count, and recall batch size.
3. A second authoritative import removes rows absent from the new snapshot.
4. Failure after import, each chain preparation, and index build resumes at the next unfinished phase.
5. Prepared, algorithm, build, and report-version changes reject old prepared data and batch caches.
6. An active `Arc` retains its governor lease after LRU eviction.
7. Concurrent seed, index-build, and scratch reservations never exceed the managed limit.
8. High-cardinality common URI/name fixtures fail admission before unbounded full-payload loading.
9. Per-contract token selection is invariant to insertion and batching order.
10. An incomplete seed recovery recomputes all four chain scopes.
11. Threshold and memory CLI validation rejects invalid production configurations.
12. Atomic-output and corrupted-manifest tests safely rerun rather than accept partial data.

Final repository verification is:

- `cargo test --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo check --release --all-features`
- `cargo fmt --all -- --check`
- `git diff --check`
- synthetic high-cardinality benchmark
- prepare interruption and resume exercise

Deployment acceptance on the target dataset is:

- 200 million-row cold import peak RSS below 400 GiB;
- Analyze/Batch peak RSS below 460 GiB and steady-state target below 400 GiB;
- managed memory never exceeds its configured envelope;
- exact Metadata recall has zero false negatives relative to exact BM25;
- committed prepare phases are not repeated after interruption;
- no silent degradation, truncation, or cross-version cache reuse.

Optimal parameter search is not required. The approved defaults are the theoretical production profile for the specified host; operators retain explicit overrides for smaller systems.
