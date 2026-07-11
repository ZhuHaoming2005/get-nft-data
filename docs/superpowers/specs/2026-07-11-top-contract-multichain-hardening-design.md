# Top Contract Multichain Hardening Design

## Scope

Fix final review findings 1 and 3 through 12 for `top_contract_analysis_rs`.
Finding 2, cache fingerprinting and cache invalidation by configuration or snapshot version, is explicitly out of scope and must remain unchanged.

## Security and HTTP behavior

- HTTP errors must never expose API keys embedded in URL path or query parameters.
- Helius keeps an independent concurrency limit and gains request-rate pacing.
- A collection-level history budget bounds total signature and transaction work in addition to the existing per-asset limit.
- Rate-limit and budget truncation must be reflected in provider data-quality fields rather than silently presented as complete history.

## Bounded Helius history pipeline

- Asset signature discovery and transaction loading use a bounded producer/consumer pipeline.
- The implementation must not retain every collection asset's signature plan and every signature reference before transaction processing starts.
- Shared signatures are fetched once while they remain in the bounded work set.
- Collection history preserves deterministic output ordering.

## Solana balance and block access

- Mint-payment pre-balance comes from the target transaction's own account-key/pre-balance pair.
- Missing target-account balance is represented as unavailable and remains visible through data-quality reporting.
- Ordinary receipt, balance, and same-transaction flow analysis must not fetch an entire slot.
- Full `getBlock` is retained only for analyses that explicitly require block-wide ordering or relationships, and those calls remain bounded and cached.

## Provider data quality

- Collection snapshot/history quality travels with the analyzed contract result; an evictable LRU is not a source of statistical truth.
- Quality aggregation may run concurrently within existing API limits.
- Failure to obtain supplemental quality information records degradation but does not discard an otherwise completed report.
- Existing coverage fields remain backward compatible.

## Identity, import, and currency correctness

- Solana transaction signatures retain case in propagation sale-transfer keys.
- Single-file Parquet duplicate selection uses the same richer-row preference as multi-file loading.
- Polygon receipt gas is converted with the Polygon native/USD rate when available; it must never use the ETH fallback rate.
- Existing Ethereum/Base behavior remains unchanged.

## Compressed NFTs and Solana metadata

- Bubblegum mint events are decoded when the transaction provides sufficient owner/account evidence.
- Unresolvable compressed mints remain explicitly counted; owners must never be fabricated from the current snapshot.
- Helius collection metadata extracts available authority information into the existing contract metadata fields without inventing deployment data.

## Testing

Each production change follows a failing-test-first cycle. Required regression coverage includes:

- URL error redaction;
- bounded collection history work and truncation reporting;
- no full-block request for target-transaction balance lookup;
- data-quality correctness with more than 16 Solana candidate contracts;
- data-quality failure preserving the main report;
- case-sensitive Solana signature association;
- richer-row selection in the single-file Parquet path;
- Polygon native gas/USD conversion;
- compressed mint parsing and authority extraction;
- Helius rate pacing behavior.

Final verification runs formatting, strict Clippy, all Rust tests, Python script tests, and `git diff --check`.

## Non-goals

- Do not add a cache fingerprint or change v2 cache reuse semantics.
- Do not restore the removed single-chain batch CLI.
- Do not redesign output schema v2.
- Do not attempt complete historical reconstruction when the provider response lacks required evidence.
