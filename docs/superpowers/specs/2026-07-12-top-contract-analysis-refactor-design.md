# Top Contract Analysis Architecture Refactor

## Objective

Refactor `top_contract_analysis_rs` into a small set of explicit provider,
evidence, pipeline, analysis, and reporting layers. Preserve the existing
duplicate-detection, malicious-behavior, scoring, threshold, and paper-statistics
algorithms. Preserve existing report fields and their intended semantics, while
allowing additive data-quality fields. CLI compatibility, internal APIs, cache
formats, and legacy compatibility code may change.

The supported runtime entry points after the refactor are:

1. `analyze`: analyze one seed against one selected candidate chain.
2. `batch`: run the four-chain seed matrix workflow.

The legacy single-chain `run_batch` entry point and plain-text seed format are
removed.

## Compatibility Boundary

- Existing report field names, types, and intended meanings remain stable.
- Existing duplicate, behavior, scoring, and paper-statistics algorithms remain
  unchanged.
- New fields are limited to data-quality disclosure.
- The cache schema may change. Old caches must be rejected safely and recomputed.
- CLI flags and internal Rust APIs may be redesigned when this removes legacy or
  ambiguous behavior.

## Architecture

### Provider Layer

Providers expose explicit capabilities rather than a large trait with generic
fallback methods:

- contract metadata;
- collection assets;
- contract history;
- transaction details;
- native/USD rate.

The provider layer contains:

- `http`: retry, rate limiting, in-flight limits, and error redaction;
- `evm`: Alchemy, OpenSea, Etherscan, EVM receipts, and native pricing;
- `solana`: collection discovery, signature discovery, and transaction details.

Alchemy error redaction recognizes an Alchemy host and redacts the path segment
immediately following any `v1`, `v2`, or `v3` segment. This applies to RPC, NFT,
and Prices URLs. Sensitive query values and matching response-body echoes are
also redacted. Legitimate non-secret path segments and query values remain
visible.

### Evidence Layer

Provider-specific responses are converted into `ContractEvidence` before
analysis. Analysis algorithms consume evidence and do not call Helius or
Alchemy directly.

`SolanaCollectionEvidence` owns:

- collection metadata and assets;
- per-asset signature-discovery state;
- globally deduplicated transaction details;
- normalized sales and transfers;
- provider data-quality measurements.

Transaction JSON is shared through `Arc` values while evidence is alive and is
released with the evidence. Target transactions outside collection evidence use
a small bounded `OnceCell` LRU. No unbounded process-wide transaction cache is
introduced.

### Analysis Layer

Existing duplicate detection, candidate filtering, address classification,
behavior detection, scoring, and paper-statistics algorithms retain their
current inputs and decisions.

Value-flow evidence is chain-specific:

- EVM may use block receipts, same-block transfers, and bounded multi-hop
  cashout evidence.
- Solana uses target-transaction evidence only. Deployment cost, same-slot
  expansion, and later same-slot cashout inference are unavailable unless the
  target transaction itself proves them.

All Solana `getBlock` code, block caches, and same-block compatibility methods
are removed.

### Pipeline Layer

Both entry points use the same ordered pipeline:

1. fetch the primary seed context once;
2. build the candidate-chain recall plan;
3. fetch candidate contract evidence;
4. run unchanged analysis algorithms;
5. aggregate data quality;
6. assemble the stable report DTO.

The four-chain batch retains global network, CPU, and matched-contract
semaphores. Candidate plans and large evidence values are released as soon as
their stage completes. A seed does not retain multiple large collection JSON
responses longer than required.

### Reporting Layer

Reporting uses explicit DTO conversion. Internal native-currency values map to
the existing external `*_native` fields and `native_symbol` without recursively
renaming arbitrary JSON keys. The generic `rename_native_amount_keys` and
`restore_internal_native_amount_keys` functions are removed.

## Solana Collection and History Flow

### Collection Discovery

- Read collection metadata and collection assets.
- Sort assets by mint address for deterministic allocation and output.
- Downstream the request `limit` to the remaining asset allowance.
- Read collection authority only from collection metadata. Missing authority is
  not inferred from a member asset.

### Signature Discovery

Each asset tracks its mint, cursor, discovered count, reported total, and
complete/failed state.

Discovery is a bounded fair round-robin:

1. Divide remaining signature budget across active assets.
2. Clamp each quota by the per-asset remaining allowance and provider maximum.
3. Continue using the asset's `before` cursor.
4. Deduplicate returned signatures before charging the collection budget.
5. Charge only newly discovered signatures.
6. Remove complete or failed assets from the active set.

An empty or failed plan does not consume signature budget. Asset discovery work
is bounded by the collection asset limit and the HTTP retry/rate-limit policy.
When the signature budget is exhausted, incomplete assets are recorded as
truncated or unrequested.

### Transaction Stage

- Deduplicate signatures globally within the collection.
- Fetch every signature at most once.
- Share parsed transaction details between receipt, pre-balance, sale, transfer,
  and mint value-flow extraction.
- Preserve per-asset event references without parsing the same transaction JSON
  once per consumer.

## Data Quality

Existing fields retain their intended coverage meaning. The implementation must
not report complete coverage when assets were skipped, truncated, or failed.

Add these fields:

- `history_unrequested_asset_count`;
- `history_complete_asset_count`;
- `history_signature_discovery_failure_count`;
- `history_transaction_detail_failure_count`;
- `mint_pre_balance_unavailable_count`;
- `collection_authority_missing_count`;
- `provider_quality_lookup_failure_count`;
- `asset_listing_unknown_total_contract_count`;
- `history_complete`.

The existing `history_asset_coverage_ratio` and
`history_transaction_coverage_ratio` retain their prior numerator, denominator,
and nullability semantics. Strict completeness is represented only by the new
complete/unrequested/failure counts and `history_complete`. A failed quality
lookup increments its own counter and makes strict completeness false without
discarding the main report. A truncated collection snapshot also makes strict
completeness false even when every listed asset has complete history. If the
provider omits the collection total and the local asset cap is reached, the
asset-listing coverage ratio remains `None`; the implementation must not turn
the analyzed count into a synthetic denominator and report `1.0`.

Quality travels with `ContractEvidence`. The process-wide
`ProviderQualityRegistry` is removed. This prevents both LRU truth loss and
unbounded registry growth. A missing pre-transaction balance is counted rather
than silently discarded.

## EVM and Currency Rules

- Existing Alchemy, OpenSea, and Etherscan behavior remains.
- Polygon receipt gas uses the receipt's POL amount and a POL/USD rate.
- Sale price must not be used to infer Polygon gas USD.
- Existing ETH/Base pricing behavior remains.

## Cache Design

The cache schema changes and includes:

- report schema version;
- analysis algorithm version;
- thresholds and budget digest;
- explicit analysis timestamp and provider-network identity;
- feature snapshot identity;
- primary and secondary chain;
- normalized seed identity.

Solana cache filenames use a stable case-safe encoding. Two Base58 addresses
that differ only in letter case cannot collide on Windows. Cache writes remain
atomic, and a mismatched fingerprint causes recomputation.

Cross-process reuse is controlled by `run-manifest.json`. The manifest contains
its schema version, an analysis run identifier, the fixed analysis timestamp,
the complete analysis configuration fingerprint, all four feature snapshot
identities, creation/update timestamps, and one of these states:

- `incomplete`: a run failed, was interrupted, or still has missing chain work;
- `complete`: every requested seed/secondary-chain work unit succeeded.

The first invocation creates an `incomplete` manifest before starting seed
work. A later invocation resumes cached scoped reports only when the existing
manifest is still `incomplete` and its configuration and four snapshot
identities match. It reuses the manifest's analysis timestamp, so successful
work is not invalidated merely because the retry starts later. A successful run
atomically marks the manifest `complete`. The next invocation starts a fresh
run and fetches provider evidence again rather than indefinitely reusing old
reports.

`--refresh-scoped-cache` always starts a fresh run and ignores an incomplete
manifest. Corrupt, unsupported, or mismatched manifests are treated the same
way. Existing cache files remain on disk but cannot match the new run
fingerprint. The manifest is an explicit recovery boundary, not a claim that
live Alchemy, OpenSea, or Helius data has a provider-supplied snapshot version.

All four feature snapshot identities are computed exactly once before seed
tasks are spawned. Seed tasks receive the immutable identity map; they never
perform their own full-table identity scan.

## Shared Solana Transaction Parsing

Signature discovery and HTTP fetching remain globally deduplicated within a
collection. In addition, the common transaction receipt, native flow, account
keys, and balance details are parsed once per fetched signature. Asset-specific
owner-change and compressed-NFT evidence may still be evaluated for every asset
reference, but they reuse the common parsed transaction details. Existing
report counters retain their current asset-reference semantics.

## Cleanup Rules

Remove:

- legacy single-chain batch types, functions, progress paths, and tests;
- plain-text seed parsing;
- Solana block adapters and caches;
- generic same-block Solana provider methods;
- redundant compatibility wrappers;
- repeated seller filtering;
- duplicate address-normalization helpers when a chain-aware helper exists;
- unused helpers proven unreferenced by search and compilation;
- recursive native-key JSON rewriting and suffix-based native-field deletion.

Cross-chain USD-only output may traverse the serialized report, but it removes
only an explicit, reviewed set of current native-amount field names. A future
non-monetary field ending in `_native` must remain untouched.

Large files are split only when the resulting module has one clear
responsibility. Do not introduce an abstraction with only one implementation
unless it enforces a meaningful provider or evidence boundary.

Document that multi-file Parquet import chooses the richest duplicate within one
invocation and that a later invocation replaces an existing identical key.

## Testing Strategy

Add regression tests in one batch and confirm the group fails before production
changes. Required coverage:

- RPC, NFT, and Prices Alchemy URL redaction;
- response-body and query-secret redaction;
- fair signature allocation;
- empty and failed plans not consuming signature budget;
- dynamic DAS limits;
- collection authority not inferred from member assets;
- one transaction request per signature;
- case-sensitive Solana self-contract exclusion;
- more than sixteen candidates contributing quality to the final report;
- quality failure preserving the main report;
- no Solana `getBlock` request path;
- cache fingerprint invalidation;
- Windows-safe Solana cache names;
- stable existing report fields and additive quality fields;
- Bubblegum mint parsing continuing after an incomplete instruction;
- missing pre-balance quality disclosure;
- truncated/unknown-total collections never reporting complete or 100% covered;
- incomplete manifest retry reusing only successful scoped reports;
- complete, corrupt, refreshed, config-mismatched, and snapshot-mismatched
  manifests starting fresh runs;
- one common transaction-detail parse for a signature shared by multiple assets;
- exactly one snapshot-identity calculation per chain before concurrent seeds;
- explicit native-field removal preserving an unrelated `_native` key.

After the failing group is established, implement production changes in coherent
modules and run tests as grouped verification rather than after each edit.

Final verification requires:

- `cargo fmt --check`;
- `cargo clippy --all-targets -- -D warnings`;
- all Rust tests;
- Python script tests;
- `git diff --check`;
- report-field snapshot comparison;
- search proving removal of legacy `run_batch`, Solana `getBlock`, recursive
  native-key rewriting, and obsolete compatibility helpers;
- security bypass review for RPC, NFT, Prices, query, and response-body secret
  variants.

## Non-goals

- Changing duplicate, scoring, malicious-behavior, or paper-statistics
  algorithms.
- Reconstructing Solana same-slot behavior without target-transaction evidence.
- Preserving old CLI, internal Rust API, cache, or single-chain batch
  compatibility.
- Broad refactoring outside `top_contract_analysis_rs` except its design
  documentation.
