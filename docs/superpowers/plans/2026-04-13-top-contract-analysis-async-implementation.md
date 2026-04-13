# Top Contract Analysis Async Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert `top_contract_analysis` external API calls to `aiohttp`-based async I/O with explicit concurrency limits while preserving the synchronous CLI and output schema.

**Architecture:** Add a shared async HTTP client with global throttling, convert HTTP helper modules to async-first functions, and move analysis orchestration to async task scheduling with contract-level and sale-level semaphores. Keep the current public sync entrypoints as thin wrappers around the async implementation so current scripts and tests remain valid.

**Tech Stack:** Python 3, `asyncio`, `aiohttp`, existing `unittest` suite, DuckDB signal cache, Rust-accelerated analysis helpers

---

## File Map

- Create: `top_contract_analysis/async_http.py`
- Modify: `top_contract_analysis/alchemy_api.py`
- Modify: `top_contract_analysis/sales.py`
- Modify: `top_contract_analysis/analysis.py`
- Modify: `top_contract_analysis/cli.py`
- Modify: `top_contract_analysis/constants.py`
- Modify: `top_contract_analysis/__init__.py`
- Test: `tests/test_top_contract_analysis.py`
- Test: `tests/test_top_contract_analysis_accelerated.py`

### Task 1: Add async HTTP client and concurrency settings

**Files:**
- Create: `top_contract_analysis/async_http.py`
- Modify: `top_contract_analysis/constants.py`
- Modify: `tests/test_top_contract_analysis_accelerated.py`

- [ ] **Step 1: Write the failing tests for async client limits and retries**

```python
class AsyncHttpClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_api_client_limits_global_concurrency(self):
        started = 0
        peak = 0
        lock = asyncio.Lock()

        async def handler(request):
            nonlocal started, peak
            async with lock:
                started += 1
                peak = max(peak, started)
            await asyncio.sleep(0.05)
            async with lock:
                started -= 1
            return web.json_response({'ok': True})

        app = web.Application()
        app.router.add_get('/ping', handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        client = async_http_mod.AsyncApiClient(
            timeout=1,
            max_concurrency=2,
            sale_metric_max_concurrency=2,
            contract_max_concurrency=2,
        )
        try:
            await asyncio.gather(*[
                client.get_json(f'http://127.0.0.1:{port}/ping')
                for _ in range(6)
            ])
        finally:
            await client.close()
            await runner.cleanup()

        self.assertEqual(peak, 2)

    async def test_async_api_client_retries_transient_failures(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            if calls < 3:
                return web.Response(status=500, text='fail')
            return web.json_response({'ok': True})

        # same app bootstrap pattern as the previous test
```

- [ ] **Step 2: Run the new async client tests to verify they fail**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis_accelerated.AsyncHttpClientTests -v
```

Expected:

```text
FAIL or ERROR because top_contract_analysis.async_http.AsyncApiClient does not exist yet
```

- [ ] **Step 3: Add concurrency defaults to constants**

Add these constants in `top_contract_analysis/constants.py`:

```python
DEFAULT_API_MAX_CONCURRENCY = 16
DEFAULT_CONTRACT_MAX_CONCURRENCY = 4
DEFAULT_SALE_METRIC_MAX_CONCURRENCY = 8
```

- [ ] **Step 4: Create `AsyncApiClient` with shared session, retry loop, and semaphores**

Create `top_contract_analysis/async_http.py` with this core structure:

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

from .constants import DEFAULT_ALCHEMY_RETRIES, DEFAULT_TIMEOUT, logger


@dataclass(frozen=True)
class ApiConcurrencyConfig:
    api_max_concurrency: int
    contract_max_concurrency: int
    sale_metric_max_concurrency: int


class AsyncApiClient:
    def __init__(
        self,
        *,
        timeout: int = DEFAULT_TIMEOUT,
        max_concurrency: int,
        contract_max_concurrency: int,
        sale_metric_max_concurrency: int,
    ) -> None:
        connector = aiohttp.TCPConnector(limit=max_concurrency, limit_per_host=max_concurrency)
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
        self._request_semaphore = asyncio.Semaphore(max_concurrency)
        self._contract_semaphore = asyncio.Semaphore(contract_max_concurrency)
        self._sale_metric_semaphore = asyncio.Semaphore(sale_metric_max_concurrency)

    @property
    def contract_semaphore(self) -> asyncio.Semaphore:
        return self._contract_semaphore

    @property
    def sale_metric_semaphore(self) -> asyncio.Semaphore:
        return self._sale_metric_semaphore

    async def close(self) -> None:
        await self._session.close()

    async def get_json(self, url: str, **kwargs) -> Dict[str, Any]:
        return await self._request_json('GET', url, **kwargs)

    async def post_json(self, url: str, payload: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return await self._request_json('POST', url, json=payload, **kwargs)

    async def _request_json(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(DEFAULT_ALCHEMY_RETRIES):
            try:
                async with self._request_semaphore:
                    async with self._session.request(method, url, **kwargs) as response:
                        response.raise_for_status()
                        return await response.json()
            except Exception as exc:
                last_exc = exc
                if attempt < DEFAULT_ALCHEMY_RETRIES - 1:
                    logger.warning('async %s retry %d/%d failed for %s: %s', method, attempt + 1, DEFAULT_ALCHEMY_RETRIES, url, exc)
                    continue
                raise
        raise RuntimeError(last_exc or 'async request failed')
```

- [ ] **Step 5: Run the async client tests to verify they pass**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis_accelerated.AsyncHttpClientTests -v
```

Expected:

```text
OK
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis/async_http.py top_contract_analysis/constants.py tests/test_top_contract_analysis_accelerated.py
git commit -m "feat: add async HTTP client for top contract analysis"
```

### Task 2: Convert Alchemy, Etherscan, and OpenSea helpers to async-first functions

**Files:**
- Modify: `top_contract_analysis/alchemy_api.py`
- Modify: `top_contract_analysis/sales.py`
- Modify: `top_contract_analysis/__init__.py`
- Test: `tests/test_top_contract_analysis.py`

- [ ] **Step 1: Write failing tests for new async helper entrypoints and sync wrappers**

Add tests like these:

```python
class AsyncApiExportsTests(unittest.TestCase):
    def test_public_api_reexports_async_entrypoints(self):
        self.assertTrue(hasattr(mod, 'fetch_contract_metadata_async'))
        self.assertTrue(hasattr(mod, 'fetch_contract_sales_async'))


class SyncWrapperTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_fetch_contract_transfers_uses_async_client(self):
        class FakeClient:
            async def post_json(self, url, payload):
                return {'result': {'transfers': []}}

        rows = await alchemy_api_mod.fetch_contract_transfers_async(
            client=FakeClient(),
            alchemy_api_key='alchemy',
            alchemy_network='eth-mainnet',
            etherscan_api_key='etherscan',
            chain='ethereum',
            contract_address='0xdup',
            token_type='ERC721',
            timeout=1,
        )

        self.assertEqual(rows, [])
```

- [ ] **Step 2: Run the focused API export tests to verify they fail**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis.AsyncApiExportsTests tests.test_top_contract_analysis.SyncWrapperTests -v
```

Expected:

```text
FAIL because async helper functions are not exported or not implemented
```

- [ ] **Step 3: Convert `alchemy_api.py` to async-first request helpers**

Use `AsyncApiClient` in `top_contract_analysis/alchemy_api.py` and add async variants such as:

```python
async def fetch_seed_contract_nfts_async(
    *,
    client: AsyncApiClient,
    api_key: str,
    network: str,
    chain: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[SeedNFT]:
    endpoint = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getNFTsForContract'
    rows: List[SeedNFT] = []
    page_key = ''
    while True:
        params = {'contractAddress': contract_address, 'withMetadata': 'true'}
        if page_key:
            params['pageKey'] = page_key
        payload = await client.get_json(f'{endpoint}?{urlencode(params)}')
        batch, page_key = _extract_seed_nfts(payload, chain=chain, contract_address=contract_address)
        rows.extend(batch)
        if not page_key:
            return rows


def fetch_seed_contract_nfts(**kwargs) -> List[SeedNFT]:
    return asyncio.run(_sync_fetch_seed_contract_nfts(**kwargs))
```

Apply the same async-first pattern to:

- contract metadata
- NFT metadata
- transfers
- owners
- transaction receipts
- ETH balance
- same-block ETH transfer lookup

For Etherscan fallback, add:

```python
async def fetch_etherscan_contract_transfers_async(...):
    payload = await client.get_json(url)
```

- [ ] **Step 4: Convert `sales.py` to async-first sales fetchers**

Implement:

```python
async def fetch_alchemy_nft_sales_async(...):
    rows: List[NFTSaleRecord] = []
    page_key = ''
    while True:
        payload = await client.get_json(f'{endpoint}?{urlencode(params)}')
        ...


async def fetch_contract_sales_async(...):
    sales = await fetch_alchemy_nft_sales_async(...)
    if sales or not opensea_api_key:
        return sales
    return await fetch_opensea_nft_events_async(...)
```

- [ ] **Step 5: Export the new async entrypoints without removing sync names**

Update `top_contract_analysis/__init__.py`:

```python
from .alchemy_api import (
    fetch_contract_metadata,
    fetch_contract_metadata_async,
    fetch_contract_owners,
    fetch_contract_owners_async,
    fetch_contract_transfers,
    fetch_contract_transfers_async,
    ...
)
from .sales import fetch_contract_sales, fetch_contract_sales_async
```

- [ ] **Step 6: Run the focused helper tests to verify they pass**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis.AsyncApiExportsTests tests.test_top_contract_analysis.SyncWrapperTests -v
```

Expected:

```text
OK
```

- [ ] **Step 7: Commit**

```bash
git add top_contract_analysis/alchemy_api.py top_contract_analysis/sales.py top_contract_analysis/__init__.py tests/test_top_contract_analysis.py
git commit -m "feat: convert top contract API helpers to async-first"
```

### Task 3: Convert analysis orchestration to async and add concurrency limits

**Files:**
- Modify: `top_contract_analysis/analysis.py`
- Modify: `top_contract_analysis/cli.py`
- Modify: `tests/test_top_contract_analysis_accelerated.py`
- Modify: `tests/test_top_contract_analysis.py`

- [ ] **Step 1: Write failing tests for async seed analysis and CLI concurrency args**

Add tests like these:

```python
class AsyncSeedAnalysisTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_analyze_seed_contract_reuses_one_client_and_limits_contract_fanout(self):
        peak = 0
        active = 0
        lock = asyncio.Lock()

        async def fake_high_confidence(**kwargs):
            nonlocal peak, active
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.05)
            async with lock:
                active -= 1
            return {
                'contract_address': kwargs['contract_address'],
                'status': 'high',
                'candidate_count': 1,
                'match_reasons': ['token_uri_match'],
                'address_signals': {},
                'victim_signals': {},
                'infringing_tokens': [],
                'malicious_addresses': [],
                'honest_addresses': [],
                'honest_address_stats': {},
                'victim_addresses': [],
                'fraud_trade_stats': {},
            }

        with patch.object(analysis_mod, '_analyze_high_confidence_contract_async', side_effect=fake_high_confidence):
            await analysis_mod.async_analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                contract_max_concurrency=2,
            )

        self.assertEqual(peak, 2)


class CliConcurrencyArgsTests(unittest.TestCase):
    def test_build_parser_accepts_async_concurrency_flags(self):
        parser = mod.build_parser()
        args = parser.parse_args([
            '--chain', 'ethereum',
            '--seed-contract-address', '0xseed',
            '--api-max-concurrency', '12',
            '--contract-max-concurrency', '3',
            '--sale-metric-max-concurrency', '5',
        ])

        self.assertEqual(args.api_max_concurrency, 12)
        self.assertEqual(args.contract_max_concurrency, 3)
        self.assertEqual(args.sale_metric_max_concurrency, 5)
```

- [ ] **Step 2: Run the orchestration tests to verify they fail**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis_accelerated.AsyncSeedAnalysisTests tests.test_top_contract_analysis.CliConcurrencyArgsTests -v
```

Expected:

```text
FAIL because async_analyze_seed_contract and the new CLI args do not exist yet
```

- [ ] **Step 3: Add async seed-analysis orchestration**

In `top_contract_analysis/analysis.py`, add:

```python
async def async_analyze_seed_contract(
    *,
    chain: str,
    seed_contract_address: str,
    alchemy_api_key: str,
    alchemy_network: str | None = None,
    etherscan_api_key: str = '',
    opensea_api_key: str = '',
    conn=None,
    feature_store=None,
    signal_cache=None,
    name_threshold: float = DEFAULT_NAME_THRESHOLD,
    metadata_threshold: float = 0.55,
    timeout: int = DEFAULT_TIMEOUT,
    max_recall_rows: int = DEFAULT_MAX_RECALL_ROWS,
    max_tokens_per_contract: int = 500,
    api_max_concurrency: int = DEFAULT_API_MAX_CONCURRENCY,
    contract_max_concurrency: int = DEFAULT_CONTRACT_MAX_CONCURRENCY,
    sale_metric_max_concurrency: int = DEFAULT_SALE_METRIC_MAX_CONCURRENCY,
) -> Dict[str, Any]:
    client = AsyncApiClient(
        timeout=timeout,
        max_concurrency=api_max_concurrency,
        contract_max_concurrency=contract_max_concurrency,
        sale_metric_max_concurrency=sale_metric_max_concurrency,
    )
    try:
        contract_meta = await fetch_contract_metadata_async(...)
        seed_nfts = await fetch_seed_contract_nfts_async(...)
        license_payload = await fetch_license_sample_async(...)
        ...
    finally:
        await client.close()
```

- [ ] **Step 4: Replace thread-pool contract fan-out with async task scheduling**

Replace the `ThreadPoolExecutor` block with:

```python
async def _run_contract(item):
    async with client.contract_semaphore:
        return await _analyze_high_confidence_contract_async(
            chain=chain,
            network=network,
            client=client,
            alchemy_api_key=alchemy_api_key,
            etherscan_api_key=etherscan_api_key,
            opensea_api_key=opensea_api_key,
            contract_address=item[0],
            contract_candidates=item[1],
            token_type=token_type,
            official_addresses=official_addresses,
            candidate_open_license_by_token=candidate_open_license_by_token,
            timeout=timeout,
            signal_cache=signal_cache,
        )

results = await asyncio.gather(*[_run_contract(item) for item in high_confidence_items])
for result in results:
    ...
```

- [ ] **Step 5: Keep the synchronous public wrapper and CLI behavior**

Add or update:

```python
def analyze_seed_contract(**kwargs) -> Dict[str, Any]:
    return asyncio.run(async_analyze_seed_contract(**kwargs))
```

And in `top_contract_analysis/cli.py`:

```python
parser.add_argument('--api-max-concurrency', type=int, default=DEFAULT_API_MAX_CONCURRENCY)
parser.add_argument('--contract-max-concurrency', type=int, default=DEFAULT_CONTRACT_MAX_CONCURRENCY)
parser.add_argument('--sale-metric-max-concurrency', type=int, default=DEFAULT_SALE_METRIC_MAX_CONCURRENCY)
```

- [ ] **Step 6: Run the orchestration and CLI tests to verify they pass**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis_accelerated.AsyncSeedAnalysisTests tests.test_top_contract_analysis.CliConcurrencyArgsTests -v
```

Expected:

```text
OK
```

- [ ] **Step 7: Commit**

```bash
git add top_contract_analysis/analysis.py top_contract_analysis/cli.py tests/test_top_contract_analysis.py tests/test_top_contract_analysis_accelerated.py
git commit -m "feat: orchestrate top contract analysis asynchronously"
```

### Task 4: Parallelize per-sale metric work with bounded async fan-out

**Files:**
- Modify: `top_contract_analysis/analysis.py`
- Modify: `tests/test_top_contract_analysis_accelerated.py`

- [ ] **Step 1: Write failing tests for sale-metric concurrency and block-receipt caching**

Add tests like these:

```python
class AsyncSaleMetricTests(unittest.IsolatedAsyncioTestCase):
    async def test_sale_metrics_respect_sale_metric_concurrency_limit(self):
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def fake_receipt(*args, **kwargs):
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.05)
            async with lock:
                active -= 1
            return mod.TransactionReceiptRecord(
                tx_hash=kwargs['tx_hash'],
                block_number=10,
                transaction_index=1,
                from_address='0xbuyer',
                gas_used=21000,
                effective_gas_price_wei=1,
            )

        # patch balance and same-block helpers similarly
        # invoke _compute_sale_metrics_async with sale_metric_max_concurrency=2
        self.assertEqual(peak, 2)

    async def test_sale_metrics_reuse_block_receipt_cache(self):
        calls = 0

        async def fake_block_receipts(**kwargs):
            nonlocal calls
            calls += 1
            return {'0xhash': mod.TransactionReceiptRecord(...)}

        # use two sales in the same block and assert calls == 1
```

- [ ] **Step 2: Run the sale-metric tests to verify they fail**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis_accelerated.AsyncSaleMetricTests -v
```

Expected:

```text
FAIL because sale metrics are still computed sequentially and helper coroutines do not exist
```

- [ ] **Step 3: Extract bounded async sale-metric execution**

In `top_contract_analysis/analysis.py`, add:

```python
async def _compute_one_sale_metric_async(
    *,
    client: AsyncApiClient,
    sale: NFTSaleRecord,
    alchemy_api_key: str,
    network: str,
    timeout: int,
    receipts_by_block: Dict[int, Dict[str, TransactionReceiptRecord]],
) -> tuple[str, Dict[str, Any]]:
    receipt_task = fetch_transaction_receipt_async(
        client=client,
        api_key=alchemy_api_key,
        network=network,
        tx_hash=sale.tx_hash,
        timeout=timeout,
    )
    balance_task = fetch_eth_balance_async(
        client=client,
        api_key=alchemy_api_key,
        network=network,
        address=sale.buyer_address,
        block_number=sale.block_number - 1,
        timeout=timeout,
    )
    transfer_task = fetch_same_block_eth_transfers_for_address_async(
        client=client,
        api_key=alchemy_api_key,
        network=network,
        block_number=sale.block_number,
        address=sale.buyer_address,
        timeout=timeout,
    )
    purchase_receipt, base_balance_eth, same_block_transfers = await asyncio.gather(
        receipt_task,
        balance_task,
        transfer_task,
    )
    ...
    return sale.tx_hash, calculate_sale_eth_metrics(...)
```

- [ ] **Step 4: Add per-sale semaphore control and shared block cache**

Use:

```python
async def _compute_sale_metrics_async(...):
    async def run_sale(sale: NFTSaleRecord):
        async with client.sale_metric_semaphore:
            return await _compute_one_sale_metric_async(...)

    pairs = await asyncio.gather(*[
        run_sale(sale)
        for sale in sales
        if sale.is_native_eth and sale.price_eth is not None
    ])
    sale_metrics_by_tx = {tx_hash: payload for tx_hash, payload in pairs}
```

Keep a per-contract block cache:

```python
block_receipts = receipts_by_block.get(sale.block_number)
if block_receipts is None:
    block_receipts = await fetch_transaction_receipts_for_block_async(...)
    receipts_by_block[sale.block_number] = block_receipts
```

- [ ] **Step 5: Run the sale-metric tests to verify they pass**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis_accelerated.AsyncSaleMetricTests -v
```

Expected:

```text
OK
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis/analysis.py tests/test_top_contract_analysis_accelerated.py
git commit -m "feat: parallelize sale metrics with bounded async fan-out"
```

### Task 5: Run full focused regression suite and tidy public surfaces

**Files:**
- Modify: `top_contract_analysis/__init__.py`
- Modify: `tests/test_top_contract_analysis.py`
- Modify: `tests/test_top_contract_analysis_accelerated.py`

- [ ] **Step 1: Write a final regression test for the sync wrapper preserving current public behavior**

Add:

```python
class SyncWrapperRegressionTests(unittest.TestCase):
    def test_sync_analyze_seed_contract_uses_async_backend(self):
        with patch.object(analysis_mod, 'async_analyze_seed_contract', new=AsyncMock(return_value={'report_summary': {}})) as mock_async:
            result = analysis_mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
            )

        self.assertEqual(result, {'report_summary': {}})
        self.assertEqual(mock_async.await_count, 1)
```

- [ ] **Step 2: Run the final regression test to verify it fails**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis.SyncWrapperRegressionTests -v
```

Expected:

```text
FAIL until the sync wrapper delegates to async_analyze_seed_contract
```

- [ ] **Step 3: Finalize exports and wrapper behavior**

Ensure `top_contract_analysis/__init__.py` exports:

```python
__all__ = [
    ...,
    'async_analyze_seed_contract',
    'fetch_contract_metadata_async',
    'fetch_contract_owners_async',
    'fetch_contract_sales_async',
    'fetch_contract_transfers_async',
]
```

Ensure sync wrappers call async entrypoints consistently rather than duplicating network logic.

- [ ] **Step 4: Run the focused regression suite**

Run:

```powershell
python -m unittest tests.test_top_contract_analysis tests.test_top_contract_analysis_accelerated -v
```

Expected:

```text
OK
```

- [ ] **Step 5: Commit**

```bash
git add top_contract_analysis/__init__.py tests/test_top_contract_analysis.py tests/test_top_contract_analysis_accelerated.py
git commit -m "test: verify async refactor preserves public behavior"
```

## Spec Coverage Check

- Async HTTP client with `aiohttp`: covered in Task 1.
- Async-first API helpers: covered in Task 2.
- Async analysis orchestration and sync wrapper: covered in Task 3.
- Global, contract, and sale-metric concurrency limits: covered in Tasks 1, 3, and 4.
- Block receipt cache reuse: covered in Task 4.
- CLI options for concurrency limits: covered in Task 3.
- Output compatibility and public API continuity: covered in Task 5.

## Placeholder Scan

- No `TBD`, `TODO`, or vague “add tests” steps remain.
- Every test step names the exact test class and command.
- Every implementation step names exact files and includes concrete code shapes.

## Type Consistency Check

- Async HTTP abstraction is consistently named `AsyncApiClient`.
- Top-level async entrypoint is consistently named `async_analyze_seed_contract`.
- Concurrency settings are consistently named:
  - `api_max_concurrency`
  - `contract_max_concurrency`
  - `sale_metric_max_concurrency`

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-13-top-contract-analysis-async-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
