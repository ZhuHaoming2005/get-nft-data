#!/usr/bin/env python3
"""
EVM 历史 metadata 回填脚本。

默认只补主表 nft_assets_{chain} 中 metadata 缺失的记录，并顺手补 image_uri。
支持两种模式（二选一）：
  - alchemy: 通过 Alchemy getNFTMetadataBatch 拉取 raw.metadata
  - token_uri: 直接请求 token_uri 指向的 JSON

所有参数通过环境变量读取。
"""

# # ── metadata_backfill 专用 ────────────────────────────────────────
# # 回填链列表（逗号分隔）
# METADATA_BACKFILL_CHAINS=ethereum,base,polygon

# # 回填模式：alchemy 或 token_uri（二选一）
# METADATA_BACKFILL_MODE=alchemy

# # 每轮从主表读取的缺失 metadata 记录数
# METADATA_BACKFILL_SEGMENT_SIZE=1000

# # 单次网络批量请求携带的 token 数
# METADATA_BACKFILL_BATCH_SIZE=100

# # 网络并发数（Alchemy 批次数或 token_uri HTTP 并发）
# METADATA_BACKFILL_WORKERS=5

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import sys
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
METADATA_BACKFILL_CHAINS = [
    chain.strip()
    for chain in os.getenv("METADATA_BACKFILL_CHAINS", "ethereum,base,polygon").split(",")
    if chain.strip()
]
METADATA_BACKFILL_MODE = os.getenv("METADATA_BACKFILL_MODE", "alchemy").strip().lower()
METADATA_BACKFILL_SEGMENT_SIZE = int(os.getenv("METADATA_BACKFILL_SEGMENT_SIZE", "1000"))
METADATA_BACKFILL_BATCH_SIZE = int(os.getenv("METADATA_BACKFILL_BATCH_SIZE", "100"))
METADATA_BACKFILL_WORKERS = int(os.getenv("METADATA_BACKFILL_WORKERS", "5"))
METADATA_BACKFILL_CHAIN_CONCURRENCY = max(
    1,
    int(os.getenv("METADATA_BACKFILL_CHAIN_CONCURRENCY", "2")),
)
REQUEST_STARTUP_STAGGER_SECONDS = max(
    0.0,
    float(os.getenv("REQUEST_STARTUP_STAGGER_SECONDS", "0")),
)
METADATA_BACKFILL_RETRY_MAX_ATTEMPTS = max(1, int(os.getenv("METADATA_BACKFILL_RETRY_MAX_ATTEMPTS", "3")))
METADATA_BACKFILL_RETRY_BASE_DELAY_SECONDS = max(
    0.0,
    float(os.getenv("METADATA_BACKFILL_RETRY_BASE_DELAY_SECONDS", "1")),
)
METADATA_TIMEOUT = int(os.getenv("METADATA_TIMEOUT", "15"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "15"))

IPFS_GATEWAYS: List[str] = [
    gateway.strip()
    for gateway in os.getenv(
        "IPFS_GATEWAYS",
        # "https://pink-official-shrimp-252.mypinata.cloud/ipfs," \
        "https://gateway.pinata.cloud/ipfs," \
        "https://ipfs.io/ipfs," \
        "https://dweb.link/ipfs"
    ).split(",")
    if gateway.strip()
]
ARWEAVE_GATEWAY = os.getenv("ARWEAVE_GATEWAY", "https://arweave.net")

CHAIN_NETWORK = {
    "ethereum": "eth-mainnet",
    "base": "base-mainnet",
    "polygon": "polygon-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
}


def _table(chain: str) -> str:
    return f"nft_assets_{re.sub(r'[^a-z0-9_]', '', chain.lower()) or 'default'}"


def _conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=10,
    )


def _safe_close(conn) -> None:
    if conn is None:
        return
    close = getattr(conn, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _is_stop_requested(stop_event) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


@contextmanager
def _install_stop_signal_handlers(stop_event):
    handlers = {}

    def _handle_stop(signum, frame):
        if not _is_stop_requested(stop_event):
            logger.info("收到停止信号，停止拉取新批次，等待当前 metadata 回填完成...")
        stop_event.set()

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _handle_stop)
        except (OSError, RuntimeError, ValueError):
            continue
    try:
        yield
    finally:
        for sig, previous in handlers.items():
            try:
                signal.signal(sig, previous)
            except (OSError, RuntimeError, ValueError):
                continue


def _alchemy_network(chain: str) -> str:
    network = CHAIN_NETWORK.get(chain.lower())
    if not network:
        raise ValueError(f"不支持的链: {chain}")
    return network


def _split_batches(items: Sequence[Tuple], batch_size: int) -> List[List[Tuple]]:
    size = max(1, batch_size)
    return [list(items[i: i + size]) for i in range(0, len(items), size)]


def ensure_columns(conn, chain: str) -> None:
    tbl = _table(chain)
    required_columns = ("metadata", "retry_checked_at")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ANY(current_schemas(false))
              AND table_name = %s
              AND column_name IN ('metadata', 'retry_checked_at')
            """,
            (tbl,),
        )
        existing = {row[0] for row in cur.fetchall()}

        altered = False
        if "metadata" not in existing:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS metadata JSONB")
            altered = True
        if "retry_checked_at" not in existing:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS retry_checked_at TIMESTAMPTZ")
            altered = True

    if altered:
        conn.commit()
        logger.info("已补齐列 metadata/retry_checked_at：%s", tbl)
    else:
        logger.info("列 metadata/retry_checked_at 已存在，跳过 DDL：%s", tbl)


def fetch_segment(conn, chain: str, *, limit: int, mode: str) -> List[Tuple]:
    tbl = _table(chain)
    token_uri_filter = "AND token_uri IS NOT NULL" if mode == "token_uri" else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, lower(contract_address), token_id, token_standard, token_uri, image_uri
            FROM {tbl}
            WHERE contract_address IS NOT NULL
              AND (metadata IS NULL OR metadata = '{{}}'::jsonb)
              {token_uri_filter}
            ORDER BY retry_checked_at ASC NULLS FIRST, id ASC
            LIMIT %s
            """,
            (limit,),
        )
        return [
            (row[0], row[1], int(row[2]), row[3], row[4], row[5])
            for row in cur.fetchall()
        ]


def mark_rows_checked(conn, chain: str, ids: List[int]) -> None:
    if not ids:
        return
    tbl = _table(chain)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {tbl}
            SET retry_checked_at = NOW()
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
    conn.commit()


def bulk_update_rows(conn, chain: str, rows: List[Tuple[int, Dict, Optional[str]]]) -> int:
    if not rows:
        return 0
    tbl = _table(chain)
    values = [
        (
            row_id,
            json.dumps(metadata, ensure_ascii=False),
            image_uri,
        )
        for row_id, metadata, image_uri in rows
        if isinstance(metadata, dict)
    ]
    if not values:
        return 0
    sql = f"""
        UPDATE {tbl} t
        SET metadata = v.metadata::jsonb,
            image_uri = COALESCE(t.image_uri, v.image_uri),
            retry_checked_at = NOW()
        FROM (VALUES %s) AS v(id, metadata, image_uri)
        WHERE t.id = v.id
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            sql,
            values,
            template="(%s, %s, %s)",
            page_size=min(len(values), 500),
        )
    conn.commit()
    return len(values)


def _to_alchemy_type(token_standard: Optional[str]) -> str:
    return (token_standard or "ERC-721").replace("-", "")


def _extract_image_from_metadata(nft: Dict, metadata: Optional[Dict]) -> Optional[str]:
    if isinstance(metadata, dict):
        image = metadata.get("image") or metadata.get("image_url") or None
        if isinstance(image, str) and image.strip():
            return image.strip()
    image_obj = nft.get("image")
    if isinstance(image_obj, dict):
        for key in ("originalUrl", "cachedUrl", "thumbnailUrl", "pngUrl"):
            value = image_obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


async def _retry_async(
    action,
    *,
    operation_name: str,
    chain: str,
    swallow_exception: bool,
    fallback: Any = None,
):
    last_error: Exception | None = None
    for attempt in range(1, METADATA_BACKFILL_RETRY_MAX_ATTEMPTS + 1):
        try:
            return await action()
        except Exception as exc:
            last_error = exc
            if attempt >= METADATA_BACKFILL_RETRY_MAX_ATTEMPTS:
                logger.exception(
                    "链 %s %s 失败，重试 %d 次后仍未恢复",
                    chain,
                    operation_name,
                    METADATA_BACKFILL_RETRY_MAX_ATTEMPTS,
                )
                if swallow_exception:
                    return fallback
                raise

            delay = METADATA_BACKFILL_RETRY_BASE_DELAY_SECONDS * attempt
            logger.warning(
                "链 %s %s 失败，将在 %.1f 秒后进行第 %d/%d 次重试: %s",
                chain,
                operation_name,
                delay,
                attempt + 1,
                METADATA_BACKFILL_RETRY_MAX_ATTEMPTS,
                exc,
            )
            await asyncio.sleep(delay)

    if swallow_exception:
        return fallback
    raise last_error if last_error is not None else RuntimeError(f"{operation_name} failed")


async def fetch_alchemy_batch(
    session: aiohttp.ClientSession,
    chain: str,
    sem: asyncio.Semaphore,
    rows: List[Tuple[int, str, int, Optional[str]]],
    *,
    startup_delay_seconds: float = 0.0,
) -> List[Tuple[int, Optional[Dict], Optional[str]]]:
    url = (
        f"https://{_alchemy_network(chain)}.g.alchemy.com"
        f"/nft/v3/{ALCHEMY_API_KEY}/getNFTMetadataBatch"
    )
    payload = {
        "tokens": [
            {
                "contractAddress": contract_address,
                "tokenId": str(token_id),
                # "tokenType": _to_alchemy_type(token_standard),
            }
            for row_id, contract_address, token_id, token_standard in rows
        ],
        "tokenUriTimeoutInMs": 5000,
        "refreshCache": False,
    }
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT,
        connect=METADATA_CONNECT_TIMEOUT,
    )
    if startup_delay_seconds > 0:
        await asyncio.sleep(startup_delay_seconds)
    async with sem:
        async with session.post(url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
    nft_list = data if isinstance(data, list) else data.get("nfts", [])
    results: List[Tuple[int, Optional[Dict], Optional[str]]] = []
    for idx, row in enumerate(rows):
        row_id = row[0]
        nft = nft_list[idx] if idx < len(nft_list) and isinstance(nft_list[idx], dict) else {}
        raw = nft.get("raw") or {}
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else None
        results.append((row_id, metadata, _extract_image_from_metadata(nft, metadata)))
    return results


async def _fetch_alchemy_batch_with_retry(
    session: aiohttp.ClientSession,
    chain: str,
    sem: asyncio.Semaphore,
    rows: List[Tuple[int, str, int, Optional[str]]],
    *,
    startup_delay_seconds: float = 0.0,
    batch_index: int,
    total_batches: int,
) -> List[Tuple[int, Optional[Dict], Optional[str]]]:
    return await _retry_async(
        lambda: fetch_alchemy_batch(
            session,
            chain,
            sem,
            rows,
            startup_delay_seconds=startup_delay_seconds,
        ),
        operation_name=f"Alchemy 批次请求 {batch_index}/{total_batches}",
        chain=chain,
        swallow_exception=True,
        fallback=[],
    )


def _metadata_url(token_uri: str, ipfs_gateway: Optional[str] = None) -> str:
    s = token_uri.strip()
    if s.startswith("ipfs://ipfs/"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[12:].lstrip("/")
    if s.startswith("ipfs://"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[7:].lstrip("/")
    if s.startswith("ar://"):
        return ARWEAVE_GATEWAY.rstrip("/") + "/" + s[5:].lstrip("/")
    return s


def _candidate_gateways(token_uri: str) -> List[Optional[str]]:
    if token_uri.strip().startswith("ipfs://"):
        return IPFS_GATEWAYS or [None]
    return [None]


async def _fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url, headers=None, allow_redirects=True) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def _fetch_one_token_uri(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    row_id: int,
    token_uri: str,
) -> Tuple[int, Optional[Dict], Optional[str]]:
    if not token_uri or token_uri.startswith("data:application/"):
        return row_id, None, None

    async with sem:
        for gateway in _candidate_gateways(token_uri):
            url = _metadata_url(token_uri, gateway)
            if not url.startswith("http"):
                continue
            try:
                data = await _fetch_json(session, url)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            image = data.get("image") or data.get("image_url")
            image_uri = image.strip() if isinstance(image, str) else None
            return row_id, data, image_uri
    return row_id, None, None


async def fetch_token_uri_metadata_batch(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    rows: List[Tuple[int, str]],
    *,
    startup_delay_seconds: float = 0.0,
) -> List[Tuple[int, Optional[Dict], Optional[str]]]:
    if startup_delay_seconds > 0:
        await asyncio.sleep(startup_delay_seconds)
    return list(
        await asyncio.gather(
            *[
                _fetch_one_token_uri(session, sem, row_id, token_uri)
                for row_id, token_uri in rows
            ]
        )
    )


async def _fetch_token_uri_metadata_batch_with_retry(
    session: aiohttp.ClientSession,
    chain: str,
    sem: asyncio.Semaphore,
    rows: List[Tuple[int, str]],
    *,
    startup_delay_seconds: float = 0.0,
    batch_index: int,
    total_batches: int,
) -> List[Tuple[int, Optional[Dict], Optional[str]]]:
    return await _retry_async(
        lambda: fetch_token_uri_metadata_batch(
            session,
            sem,
            rows,
            startup_delay_seconds=startup_delay_seconds,
        ),
        operation_name=f"token_uri 批次请求 {batch_index}/{total_batches}",
        chain=chain,
        swallow_exception=True,
        fallback=[],
    )


async def _process_chain(chain: str, stop_event=None) -> int:
    conn = _conn()
    try:
        ensure_columns(conn, chain)
        timeout = aiohttp.ClientTimeout(
            total=METADATA_CONNECT_TIMEOUT + METADATA_TIMEOUT,
            connect=METADATA_CONNECT_TIMEOUT,
            sock_connect=METADATA_CONNECT_TIMEOUT,
            sock_read=METADATA_TIMEOUT,
        )
        connector = aiohttp.TCPConnector(limit=max(1, METADATA_BACKFILL_WORKERS))
        sem = asyncio.Semaphore(max(1, METADATA_BACKFILL_WORKERS))
        total_updated = 0

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            while not _is_stop_requested(stop_event):
                rows = fetch_segment(
                    conn,
                    chain,
                    limit=METADATA_BACKFILL_SEGMENT_SIZE,
                    mode=METADATA_BACKFILL_MODE,
                )
                if not rows:
                    break

                logger.info("On fetching %d metadatas", len(rows))

                if METADATA_BACKFILL_MODE == "alchemy":
                    payload_rows = [(row[0], row[1], row[2], row[3]) for row in rows]
                    batches = _split_batches(payload_rows, METADATA_BACKFILL_BATCH_SIZE)
                    total_batches = len(batches)
                    parts = await asyncio.gather(*[
                        _fetch_alchemy_batch_with_retry(
                            session,
                            chain,
                            sem,
                            batch,
                            startup_delay_seconds=REQUEST_STARTUP_STAGGER_SECONDS * max(batch_index - 1, 0),
                            batch_index=batch_index,
                            total_batches=total_batches,
                        )
                        for batch_index, batch in enumerate(batches, start=1)
                    ])
                elif METADATA_BACKFILL_MODE == "token_uri":
                    payload_rows = [(row[0], row[4]) for row in rows if row[4]]
                    batches = _split_batches(payload_rows, METADATA_BACKFILL_BATCH_SIZE)
                    total_batches = len(batches)
                    parts = await asyncio.gather(*[
                        _fetch_token_uri_metadata_batch_with_retry(
                            session,
                            chain,
                            sem,
                            batch,
                            startup_delay_seconds=REQUEST_STARTUP_STAGGER_SECONDS * max(batch_index - 1, 0),
                            batch_index=batch_index,
                            total_batches=total_batches,
                        )
                        for batch_index, batch in enumerate(batches, start=1)
                    ])
                else:
                    raise ValueError(f"不支持的 METADATA_BACKFILL_MODE: {METADATA_BACKFILL_MODE}")

                flat = [item for part in parts for item in part if item[1] is not None]
                updated = bulk_update_rows(conn, chain, flat)
                updated_ids = {row_id for row_id, _, _ in flat}
                unresolved_ids = [row[0] for row in rows if row[0] not in updated_ids]
                mark_rows_checked(conn, chain, unresolved_ids)
                total_updated += updated
                logger.info(
                    "链 %s 本轮读取 %d 条，成功回填 %d 条 metadata（累计 %d）",
                    chain,
                    len(rows),
                    updated,
                    total_updated,
                )
        if _is_stop_requested(stop_event):
            logger.info("链 %s 收到停止信号，当前批次已完成，累计更新 %d 条", chain, total_updated)
        else:
            logger.info("链 %s metadata 回填完成，共更新 %d 条", chain, total_updated)
        return total_updated
    finally:
        _safe_close(conn)


async def _amain(stop_event=None) -> None:
    async def _run_chain(chain: str, sem: asyncio.Semaphore) -> int:
        async with sem:
            try:
                return await _process_chain(chain, stop_event=stop_event)
            except Exception:
                logger.exception("链 %s metadata 回填失败，已跳过并继续下一条链", chain)
                return 0

    sem = asyncio.Semaphore(
        max(1, min(METADATA_BACKFILL_CHAIN_CONCURRENCY, len(METADATA_BACKFILL_CHAINS) or 1))
    )
    results = await asyncio.gather(*[
        _run_chain(chain, sem)
        for chain in METADATA_BACKFILL_CHAINS
    ])
    grand_total = sum(results)
    if _is_stop_requested(stop_event):
        logger.info("metadata 回填已停止拉取新批次，当前在途任务已收尾，共更新 %d 条", grand_total)
    else:
        logger.info("全部 metadata 回填完成，共更新 %d 条", grand_total)


def main() -> None:
    stop_event = threading.Event()
    with _install_stop_signal_handlers(stop_event):
        asyncio.run(_amain(stop_event=stop_event))


if __name__ == "__main__":
    main()
