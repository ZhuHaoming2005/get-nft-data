# Top Contract Analysis Review Remediation Design

## Scope

This change fixes the validated correctness, work-boundary, cache-lifecycle, and regression-coverage issues in `top_contract_analysis_rs`. It preserves the existing report fields, attack-detection algorithms, scoring thresholds, and statistical formulas.

## Currency correctness

All native/USD normalization must be chain aware. Ethereum and Base use ETH/USD, Polygon uses POL/USD, and Solana uses SOL/USD. The two mint-payment transfer entry points will use the same chain-aware rate lookup already used by sales and Polygon receipt enrichment.

Receipt gas USD must come from `receipt.fee_usd`. A helper that lacks chain identity must not infer a native-token exchange rate from an arbitrary transfer. This prevents an ETH-derived or otherwise mismatched transfer ratio from being applied to POL gas.

## Provider quality from existing evidence

Quality reporting must observe evidence; it must not create expensive evidence solely for reporting.

`fetch_provider_data_quality` will inspect already initialized collection snapshot and history cache entries without inserting a new cache cell or issuing network requests. When snapshot evidence exists but history evidence does not, it will report listing quality, mark history incomplete, and increment the existing provider-quality failure/degradation counter. When neither exists, it will return an explicitly degraded quality payload.

Matched-contract quality remains collected immediately after contract analysis, while the corresponding evidence is hot. Early-filtered candidates will not trigger collection-history discovery. Seed quality will likewise read only evidence produced by seed analysis. Cache eviction outside the bounded active-work window may degrade disclosed quality but must never cause a hidden full-history retry.

No unbounded provider-quality registry will be introduced.

When snapshot evidence exists but history evidence does not, every asset present in the snapshot is counted as history-unrequested. If the snapshot total is known, assets omitted by snapshot truncation are added as well; if the total is unknown, only the listed assets are counted. This keeps the existing fields while making their partial-evidence semantics explicit.

The bounded collection snapshot and history caches are sized from the configured matched-contract concurrency. Their capacity is at least 16 and otherwise covers all matched-contract tasks plus up to eight simultaneously active seed pipeline contexts. This prevents evidence used by active analysis from being evicted before its immediate quality read, without introducing an unbounded registry. Eviction remains permitted after the bounded active-work window.

## Helius pagination and bounded work

### Unknown totals

A missing `total` is represented explicitly rather than replaced by the current page length. For unknown totals, a full page with an advancing cursor is not complete and pagination continues. Completion is established only by an empty or short page, a non-advancing cursor, or an explicit known total being reached. Per-asset limits still mark the asset truncated when more history may exist.

Each asset also tracks every pagination cursor it has used. A repeated cursor, an alternating cursor cycle, or a full page that contributes no new signature terminates discovery for that asset. The collected prefix is retained, while the asset is marked truncated and collection history remains incomplete. The same stop rule applies to single-asset discovery so malformed provider pagination cannot loop indefinitely.

### Collection budget

`max_history_transactions_per_collection` will bound total asset-signature references, not only globally unique signatures. A shared signature referenced by N assets consumes N units of collection work because it creates N asset-specific owner-change evaluations and references. Transaction detail HTTP calls remain deduplicated by signature.

To keep the reference graph bounded even when callers request an unlimited or excessively large collection budget, `0` and values above 100,000 use a 100,000-reference safety ceiling. Reaching the ceiling is disclosed through the existing truncation and coverage fields.

The scheduler will reserve only the remaining reference budget, process assets in deterministic token-ID order, and mark assets that receive no remaining budget as unrequested/truncated. Quality fields continue to disclose the resulting coverage.

### Streaming and shared parsing

Signature discovery runs in fixed-size asset batches and processes completed page responses incrementally instead of collecting the entire active set. Common transaction details, including account keys, are parsed once per unique signature and passed into compressed-NFT owner-change parsing. Asset-specific token and Bubblegum evidence remains evaluated per asset reference.

## Batch manifest ownership

### Work-set identity

The manifest configuration fingerprint includes a digest of the canonical seed set. Canonicalization uses parsed `Chain` values, chain-aware normalized addresses, sorting, and deterministic serialization. Reordering an equivalent seed file does not invalidate a run; adding, removing, or changing a seed does.

### Cross-process lock

Each output directory owns a persistent lock file opened and exclusively locked for the full `run_multichain_batch` lifetime. The lock contains the current PID, run ID when available, and start time for diagnostics. The operating system releases the lock after process termination; the file may remain and be reused.

Lock acquisition is non-blocking and returns a clear error when another batch owns the directory. Manifest resolution, scoped report reads/writes, summary generation, and final status updates all occur while the lock is held.

## Regression coverage

Grouped fail-first tests will cover:

- Polygon mint transfers and receipt gas using POL/USD rather than ETH/USD;
- quality lookup never initiating snapshot or history network work, including cache eviction and early filters;
- snapshot-only quality counting listed assets, and known omitted assets, as history-unrequested;
- more than sixteen active Solana candidates retaining their snapshot and history evidence until quality aggregation;
- unknown-total full pages continuing pagination;
- repeated and cyclic Helius cursors terminating with explicit truncation rather than looping;
- strict asset-signature reference budgeting and fixed-size discovery batching;
- shared compressed transactions parsing account keys once;
- seed-set changes invalidating an incomplete manifest while seed reordering remains equivalent;
- concurrent ownership of one output directory being rejected;
- missing collection authority and unavailable mint pre-balance counters reaching final quality output;
- more than eight seeds respecting the pipeline backlog bound;
- unrelated non-monetary `*_native` fields remaining present in USD-only output.

Tests are executed in groups after each implementation group, followed by formatting, strict Clippy, all-feature Rust tests, Python helper tests, and diff hygiene checks.

## Non-goals

- No report field removal or renaming.
- No changes to duplicate matching, malicious-address classification, propagation, cost attribution formulas, or paper-statistics algorithms.
- No compatibility layer for concurrent writers to the same output directory; the second writer is rejected.
- No unbounded caches or registries.
- No change to partial-resume behavior that can combine cached scoped reports with freshly fetched seed context; that separately identified cross-time consistency issue is explicitly excluded from this remediation.
