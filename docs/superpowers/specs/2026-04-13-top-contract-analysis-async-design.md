# Top Contract Analysis Async API Design

## Summary

`top_contract_analysis` currently mixes CPU-heavy analysis with a large synchronous HTTP request surface. The most expensive path is high-confidence contract analysis, where a single candidate contract can trigger transfer pagination, owner pagination, sales pagination, and per-sale follow-up RPC calls. The current implementation parallelizes only across contracts with a thread pool, which improves throughput but does not provide centralized request throttling or a clean way to control nested fan-out.

This design converts the external API request layer to native `asyncio` + `aiohttp`, adds explicit concurrency limits at the global, contract, and per-sale levels, and preserves the current synchronous CLI and public entrypoints through thin wrappers.

## Goals

- Convert all Alchemy, Etherscan, and OpenSea HTTP requests used by `top_contract_analysis` to native async I/O.
- Add explicit concurrency limits so nested request fan-out cannot overload remote APIs or local resources.
- Preserve current result semantics and output schema.
- Preserve the existing synchronous CLI UX.
- Keep CPU-bound logic largely unchanged unless async boundaries require local reshaping.

## Non-Goals

- Reworking duplicate-detection algorithms.
- Replacing Rust-assisted computation paths.
- Changing report fields or analysis heuristics unless required to preserve correctness under async execution.
- Optimizing PostgreSQL snapshot fallback beyond current behavior.

## Current Bottlenecks

1. High-confidence contract analysis expands into multiple remote request families.
2. Sale metric calculation performs several follow-up calls per sale:
   - transaction receipt
   - prior-block ETH balance
   - same-block ETH transfers
   - block receipts when same-block transfers exist
3. Contract-level threading is uncontrolled relative to nested per-sale work.
4. Retry, timeout, and connection reuse are not centralized around a true async transport.

## Proposed Architecture

### 1. Introduce a shared async HTTP layer

Add a new internal async client abstraction in `top_contract_analysis`:

- `AsyncApiClient`
- Owns one `aiohttp.ClientSession`
- Exposes:
  - `async get_json(...)`
  - `async post_json(...)`
- Handles:
  - request timeout
  - retry loop
  - `raise_for_status`
  - JSON decoding
  - global request semaphore

The client is the single chokepoint for outbound HTTP. Every external request must go through it.

### 2. Convert external request helpers to async

Convert the HTTP-facing functions in these modules to async-first implementations:

- `top_contract_analysis/alchemy_api.py`
- `top_contract_analysis/sales.py`

Representative functions:

- `fetch_contract_metadata_async`
- `fetch_seed_contract_nfts_async`
- `fetch_nft_metadata_async`
- `fetch_contract_transfers_async`
- `fetch_contract_owners_async`
- `fetch_transaction_receipt_async`
- `fetch_transaction_receipts_for_block_async`
- `fetch_eth_balance_async`
- `fetch_same_block_eth_transfers_for_address_async`
- `fetch_contract_sales_async`

Pagination remains sequential when driven by `pageKey` or equivalent cursor state.

### 3. Convert analysis orchestration to async

Add async orchestration in `top_contract_analysis/analysis.py`:

- `async_analyze_seed_contract(...)`
- `async _analyze_high_confidence_contract(...)`

The current synchronous `analyze_seed_contract(...)` remains as a compatibility wrapper:

- It validates arguments
- It invokes `asyncio.run(async_analyze_seed_contract(...))`

This keeps the current CLI and tests usable while making async the real implementation path.

### 4. Replace thread-pool contract parallelism with async task scheduling

Current cross-contract parallelism uses `ThreadPoolExecutor`. Replace it with async task fan-out:

- collect high-confidence contract work items
- execute them under a contract-level semaphore
- gather results with stable result collection

This keeps cross-contract concurrency but moves it into the same async control plane as the HTTP layer.

## Concurrency Controls

Three explicit limits are introduced.

### Global request limit

CLI/config parameter:

- `api_max_concurrency`

Default:

- `16`

Semantics:

- Every outbound HTTP request acquires the global semaphore first.
- This is the hard ceiling across all APIs and all contracts.

### Contract analysis limit

CLI/config parameter:

- `contract_max_concurrency`

Default:

- `4`

Semantics:

- Limits how many high-confidence contracts can be analyzed concurrently.
- Prevents multiplicative fan-out when several contracts each trigger many nested requests.

### Sale-metric request limit

CLI/config parameter:

- `sale_metric_max_concurrency`

Default:

- `8`

Semantics:

- Applies inside one contract during sale metric expansion.
- Each sale can issue several follow-up RPCs, but the number of in-flight sale metric tasks is bounded.

## Request Scheduling Rules

### Sequential paths

The following remain sequential because they depend on cursor chaining or ordered accumulation:

- Alchemy NFT pagination
- owner pagination
- transfer pagination
- Alchemy sales pagination

### Concurrent paths

The following become concurrent:

- independent high-confidence contract analyses
- independent sale metric computations within one contract
- independent per-sale calls that do not depend on each other

Within one sale metric computation, these calls can run together:

- transaction receipt
- prior-block ETH balance
- same-block ETH transfers

`fetch_transaction_receipts_for_block_async` remains conditional and uses a shared per-contract cache keyed by block number.

## Caching and Reuse

Keep and preserve existing logical caches.

### Session reuse

- One `AsyncApiClient`
- One `aiohttp.ClientSession`
- Reused for the entire seed analysis run

### Block receipt cache

- Per-contract dictionary keyed by `block_number`
- Deduplicates repeated block receipt lookups across many sales in the same block

### Signal cache integration

`ContractSignalCache` behavior remains unchanged at the API level. Async analysis can still:

- short-circuit transfer and owner fetches when cache hits
- repopulate cache after fresh fetches

The cache implementation itself does not need to become async in this change because it is local DuckDB I/O and not the dominant bottleneck.

## Public API Compatibility

Keep existing synchronous imports working.

### Synchronous compatibility layer

- `analyze_seed_contract(...)` remains public
- It wraps `async_analyze_seed_contract(...)`
- Existing scripts using `python -m top_contract_analysis ...` continue to work

### Async entrypoints

Expose async entrypoints for internal use and future batch runners:

- `async_analyze_seed_contract(...)`

Do not remove the current sync interface in this change.

## CLI Changes

Add three CLI options:

- `--api-max-concurrency`
- `--contract-max-concurrency`
- `--sale-metric-max-concurrency`

Defaults:

- `16`
- `4`
- `8`

These values flow from CLI to `async_analyze_seed_contract(...)` and then into the async client and orchestration semaphores.

## Error Handling

- Preserve current retry count semantics.
- Preserve current timeout semantics, but route them through `aiohttp`.
- Preserve current fallback from Alchemy transfers to Etherscan transfers.
- Preserve partial degradation behavior for sale metric computation failures:
  - if a sale metric sub-call fails, log warning and return the current degraded metric payload rather than failing the whole contract analysis

Task aggregation rules:

- One contract failure should not silently cancel unrelated contract tasks unless the current synchronous code would have aborted the whole run.
- Individual sale metric failures should degrade that sale only.

## Testing Strategy

### Unit tests for async client

Add tests covering:

- global semaphore limiting request concurrency
- retry behavior on transient failures
- timeout propagation
- shared session reuse

### Analysis-layer async tests

Add tests covering:

- high-confidence contract concurrency respects `contract_max_concurrency`
- sale metric concurrency respects `sale_metric_max_concurrency`
- block receipt cache deduplicates repeated block fetches
- sync wrapper delegates to async implementation and preserves output shape

### Regression coverage

Retain existing behavioral assertions for:

- duplicate candidate structure
- legit duplicate detection
- victim signal output
- signal cache reuse
- CLI output shape

Existing accelerated tests should be updated rather than discarded where possible.

## Rollout Plan

1. Introduce async client and async HTTP helpers.
2. Convert sales and RPC request functions to async.
3. Convert high-confidence contract analysis to async with semaphores.
4. Convert top-level seed analysis to async and keep sync wrapper.
5. Add CLI concurrency parameters.
6. Update tests and run the focused test suite.

## Risks

### Nested event-loop misuse

Risk:

- `asyncio.run(...)` fails if called from an already-running event loop.

Mitigation:

- Keep sync wrapper for CLI and normal script use.
- Document async entrypoint for programmatic callers already inside an event loop.

### Overlapping semaphore scopes

Risk:

- Incorrect semaphore placement could serialize too much work or fail to bound the true hot path.

Mitigation:

- Use distinct semaphores for global requests, contract tasks, and sale metric tasks.
- Cover with concurrency-counting tests.

### Behavioral drift

Risk:

- Async refactor changes ordering or fallback behavior.

Mitigation:

- Keep data-shaping logic intact.
- Preserve fallback conditions and return payload schema.
- Reuse existing regression tests.

## Success Criteria

- Seed analysis completes faster than the current sync implementation on identical inputs when multiple high-confidence contracts or many sales are present.
- No uncontrolled API fan-out occurs.
- Existing CLI usage remains valid.
- Output payload shape remains backward compatible.
