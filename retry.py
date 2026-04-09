#!/usr/bin/env python3
"""
NFT image_uri 重试脚本：对 image_uri 为空的记录重新拉取 metadata 并更新。
支持可选链：默认 base，可通过 --chain 指定，如 --chain eth。
表名：nft_assets_{chain}，与 nft_collector 保持一致。
"""

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from typing import Any, Dict, List, Optional

import aiohttp
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# 数据库连接配置 iic12345!
DB_HOST = "localhost"  # "pgm-2zevls2414y7mw6d8o.pg.rds.aliyuncs.com"
DB_PORT = 5432
DB_NAME = "nft_data"
DB_USER = "postgres" # "user1"
DB_PASS = "123456" # "_JC!y7XWygm$94f"

CHAIN_NAME = "polygon"

METADATA_TIMEOUT = 15
METADATA_CONNECT_TIMEOUT = 15
CONCURRENT_REQUESTS = 20

IPFS_GATEWAYS: List[str] = [
    "https://pink-official-shrimp-252.mypinata.cloud/ipfs",
    "https://gateway.pinata.cloud/ipfs",
    "https://dweb.link/ipfs",
    "https://ipfs.io/ipfs",
]

IPFS_GATEWAY_HEADERS: Dict[str, Dict[str, str]] = {
    "https://pink-official-shrimp-252.mypinata.cloud/ipfs": {
        "x-pinata-gateway-token": "P2Z1YGDiTOuEjgDKJdhn41CmcbGe9HNn0BMYTZFSWe8J-9NwPEpyPauxPaSlI_i0",
    },
}

ARWEAVE_GATEWAY = "https://arweave.net"
BATCH_SIZE = 1000


def _nft_table_name(chain_name: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower())
    return f"nft_assets_{safe}" if safe else "nft_assets_default"


def _metadata_url(token_uri: str, ipfs_gateway: Optional[str] = None) -> str:
    s = token_uri.strip()
    if s.startswith("ipfs://ipfs/"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[12:].lstrip("/")
    if s.startswith("ipfs://"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[7:].lstrip("/")
    if s.startswith("ar://"):
        gw = ARWEAVE_GATEWAY.rstrip("/")
        return gw + "/" + s[5:].lstrip("/")
    return s


def _candidate_gateways(token_uri: str) -> List[Optional[str]]:
    if token_uri.strip().startswith("ipfs://"):
        return IPFS_GATEWAYS
    return [None]


def _request_headers(gateway: Optional[str], url: str) -> Dict[str, str]:
    if not gateway:
        return {}
    gateway_base = gateway.rstrip("/")
    if gateway_base in IPFS_GATEWAY_HEADERS and url.startswith(gateway_base):
        return dict(IPFS_GATEWAY_HEADERS[gateway_base])
    return {}


async def _fetch_json(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]):
    async with session.get(url, headers=headers or None, allow_redirects=True) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def fetch_metadata_for_token_uri_async(
    token_uri: str,
    session: aiohttp.ClientSession,
) -> Optional[Dict[str, Any]]:
    if not token_uri or token_uri.startswith("data:application/"):
        return None

    last_exc: Optional[Exception] = None
    gateways = _candidate_gateways(token_uri)

    for idx, gateway in enumerate(gateways):
        url = _metadata_url(token_uri, gateway)
        if not url.startswith("http"):
            continue

        headers = _request_headers(gateway, url)
        try:
            data = await _fetch_json(session, url, headers)
        except Exception as exc:
            last_exc = exc
            logger.info(
                "[retry image_uri=NULL] token_uri=%s uri=%s gateway[%d]=%s 原因: HTTP 请求失败 [%s] %s",
                token_uri[:80],
                url,
                idx,
                gateway or "<direct>",
                type(exc).__name__,
                str(exc) or repr(exc),
            )
            continue

        if not isinstance(data, dict):
            logger.info(
                "[retry image_uri=NULL] token_uri=%s gateway[%d]=%s 原因: metadata 不是 JSON 对象",
                token_uri[:80],
                idx,
                gateway or "<direct>",
            )
            continue

        return data

    if last_exc is not None:
        logger.debug(
            "metadata 重试最终失败 token_uri=%s，最后错误 [%s] %s",
            token_uri[:80],
            type(last_exc).__name__,
            str(last_exc) or repr(last_exc),
        )
    return None


async def fetch_image_uri_for_token_uri_async(
    token_uri: str,
    session: aiohttp.ClientSession,
) -> Optional[str]:
    metadata = await fetch_metadata_for_token_uri_async(token_uri, session)
    if not metadata:
        return None

    image = metadata.get("image") or metadata.get("image_url")
    if not image or not isinstance(image, str):
        logger.info(
            "[retry image_uri=NULL] token_uri=%s 原因: JSON 中缺少 image/image_url",
            token_uri[:100],
        )
        return None
    return image.strip()


async def fetch_metadata_records_for_token_uris(
    token_uris: List[str],
    concurrency: int = CONCURRENT_REQUESTS,
) -> Dict[str, Optional[Dict[str, Any]]]:
    if not token_uris:
        return {}

    timeout = aiohttp.ClientTimeout(
        total=METADATA_CONNECT_TIMEOUT + METADATA_TIMEOUT,
        connect=METADATA_CONNECT_TIMEOUT,
        sock_connect=METADATA_CONNECT_TIMEOUT,
        sock_read=METADATA_TIMEOUT,
    )
    connector = aiohttp.TCPConnector(limit=max(1, concurrency))
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def fetch_one(token_uri: str):
            async with semaphore:
                metadata = await fetch_metadata_for_token_uri_async(token_uri, session)
                return token_uri, metadata

        pairs = await asyncio.gather(*(fetch_one(token_uri) for token_uri in token_uris))

    return {token_uri: metadata for token_uri, metadata in pairs}


async def fetch_image_uris_for_token_uris(
    token_uris: List[str],
    concurrency: int = CONCURRENT_REQUESTS,
) -> Dict[str, Optional[str]]:
    metadata_records = await fetch_metadata_records_for_token_uris(
        token_uris,
        concurrency=concurrency,
    )
    image_uris: Dict[str, Optional[str]] = {}
    for token_uri, metadata in metadata_records.items():
        image = None
        if isinstance(metadata, dict):
            candidate = metadata.get("image") or metadata.get("image_url")
            if isinstance(candidate, str):
                image = candidate.strip()
        image_uris[token_uri] = image
    return image_uris


def fetch_image_uri_for_token_uri(token_uri: str) -> Optional[str]:
    return asyncio.run(fetch_image_uris_for_token_uris([token_uri], concurrency=1)).get(token_uri)


def ensure_retry_checked_column(conn, chain_name: str) -> None:
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE {tbl}
            ADD COLUMN IF NOT EXISTS retry_checked_at TIMESTAMPTZ
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {tbl}
            ADD COLUMN IF NOT EXISTS metadata JSONB
            """
        )
    conn.commit()
    logger.info("已确保 %s.retry_checked_at / metadata 列存在", tbl)


def get_conn() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=10,
    )
    conn.autocommit = True
    return conn


def fetch_missing_image_rows(
    cur: psycopg2.extensions.cursor, chain_name: str, limit: int
) -> list:
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        SELECT token_uri
        FROM (
            SELECT token_uri, MIN(retry_checked_at) AS oldest
            FROM {tbl}
            WHERE image_uri IS NULL
              AND token_uri IS NOT NULL
              AND (
                  token_uri LIKE 'ipfs://%%'
                  OR token_uri LIKE 'ar://%%'
                  OR token_uri LIKE 'http%%'
              )
            GROUP BY token_uri
            ORDER BY oldest ASC NULLS FIRST
            LIMIT %s
        ) sub
        """,
        (limit,),
    )
    return cur.fetchall()


def mark_retry_checked(
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    chain_name: str,
    token_uris: list,
) -> None:
    if not token_uris:
        return
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        UPDATE {tbl}
        SET retry_checked_at = NOW()
        WHERE image_uri IS NULL
          AND token_uri = ANY(%s)
        """,
        ([r["token_uri"] for r in token_uris],),
    )
    conn.commit()


def update_metadata_by_token_uri(
    cur: psycopg2.extensions.cursor,
    token_uri: str,
    metadata: Dict[str, Any],
    chain_name: str,
) -> int:
    tbl = _nft_table_name(chain_name)
    image_uri = metadata.get("image") or metadata.get("image_url")
    image_uri = image_uri.strip() if isinstance(image_uri, str) else None
    cur.execute(
        f"""
        UPDATE {tbl}
        SET image_uri = %s,
            metadata = %s::jsonb
        WHERE token_uri = %s
          AND (image_uri IS NULL OR metadata IS NULL)
        """,
        (image_uri, json.dumps(metadata, ensure_ascii=False), token_uri),
    )
    return cur.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="NFT image_uri 重试脚本")
    parser.add_argument(
        "--chain",
        default=CHAIN_NAME,
        help="链名，对应表 nft_assets_{chain} (默认: %(default)s)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=CONCURRENT_REQUESTS,
        help="并发抓取 metadata 的请求数 (默认: %(default)s)",
    )
    args = parser.parse_args()
    chain_name = args.chain
    concurrency = max(1, args.concurrency)

    logger.info(
        "开始执行 NFT image_uri 重试脚本 | 链: %s | 表: %s | 并发: %s",
        chain_name,
        _nft_table_name(chain_name),
        concurrency,
    )

    conn = get_conn()
    ensure_retry_checked_column(conn, chain_name)
    conn.close()

    total_processed = 0
    total_updated = 0

    while True:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            rows = fetch_missing_image_rows(cur, chain_name, BATCH_SIZE)
            if rows:
                mark_retry_checked(cur, conn, chain_name, rows)
        finally:
            cur.close()
            conn.close()

        if not rows:
            logger.info("暂无待重试记录，关闭数据库连接后休眠 600 秒")
            time.sleep(600)
            continue

        token_uris = [row["token_uri"] for row in rows]
        logger.info(
            "取到待补 image_uri 的 token_uri 数量: %d，累计处理: %d，累计更新: %d",
            len(token_uris),
            total_processed,
            total_updated,
        )

        metadata_records = asyncio.run(
            fetch_metadata_records_for_token_uris(
                token_uris,
                concurrency=concurrency,
            )
        )

        batch_updated = 0
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            for token_uri in token_uris:
                total_processed += 1
                metadata = metadata_records.get(token_uri)
                if not metadata:
                    continue

                updated_count = update_metadata_by_token_uri(
                    cur,
                    token_uri,
                    metadata,
                    chain_name,
                )
                batch_updated += updated_count
                total_updated += updated_count
                image_uri = metadata.get("image") or metadata.get("image_url") or ""

                logger.info(
                    "更新成功 token_uri=%s 更新行数=%d image_uri=%s metadata_keys=%s",
                    token_uri[:80],
                    updated_count,
                    image_uri[:80],
                    sorted(metadata.keys())[:10],
                )
        finally:
            cur.close()
            conn.close()
            logger.info("数据库连接已关闭")

        logger.info(
            "本批处理完成，新增更新 %d 条，累计更新 %d 条",
            batch_updated,
            total_updated,
        )


if __name__ == "__main__":
    main()
