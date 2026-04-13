from __future__ import annotations

import asyncio
from typing import Any, Dict

import aiohttp

from .constants import DEFAULT_ALCHEMY_RETRIES, DEFAULT_TIMEOUT, logger


class AsyncApiClient:
    def __init__(
        self,
        *,
        timeout: int = DEFAULT_TIMEOUT,
        max_concurrency: int,
        contract_max_concurrency: int,
        sale_metric_max_concurrency: int,
    ) -> None:
        connector = aiohttp.TCPConnector(
            limit=max_concurrency,
            limit_per_host=max_concurrency,
        )
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
            connector=connector,
        )
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
        if not self._session.closed:
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
                    logger.warning(
                        'async %s retry %d/%d failed for %s: %s',
                        method,
                        attempt + 1,
                        DEFAULT_ALCHEMY_RETRIES,
                        url,
                        exc,
                    )
                    continue
                raise
        raise RuntimeError(last_exc or 'async request failed')
