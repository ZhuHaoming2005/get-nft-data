#!/usr/bin/env python3
"""
临时修复脚本：为 metadata 非空但 image_uri 为空的 NFT 记录回填图片地址。

处理逻辑：
1. 按链扫描主表 nft_assets_{chain}
2. 仅挑选 metadata 非空且 image_uri 为空的记录
3. 从 metadata JSON 中尝试提取 image / image_url 等字段
4. 批量写回 image_uri

用法：
  python run/evm/backfill_image_uri_from_metadata.py
  python run/evm/backfill_image_uri_from_metadata.py --chains base polygon --dry-run
  python run/evm/backfill_image_uri_from_metadata.py --batch-size 2000 --limit 50000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from typing import Any, Iterable, Iterator

import psycopg2
import psycopg2.extras
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    def load_dotenv() -> bool:
        return False

load_dotenv()


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

DEFAULT_CHAINS = os.getenv("CHAIN_NAMES", "ethereum,base,polygon").split(",")
DEFAULT_BATCH_SIZE = 1000

for _stream in (sys.stdout, sys.stderr):
    reconfigure = getattr(_stream, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8")
        except Exception:
            pass

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_IMAGE_KEYS = {
    "image",
    "image_url",
    "imageurl",
    "image_uri",
    "imageuri",
    "image_data",
    "image_preview_url",
    "imagepreviewurl",
    "image_original_url",
    "imageoriginalurl",
    "featured_image",
    "featuredimage",
}
_URL_KEYS = (
    "originalUrl",
    "original_url",
    "cachedUrl",
    "cached_url",
    "gateway",
    "url",
    "uri",
    "href",
    "src",
    "pngUrl",
    "png_url",
    "thumbnailUrl",
    "thumbnail_url",
)
_CONTAINER_KEYS = (
    "properties",
    "content",
    "media",
    "asset",
    "assets",
    "collection",
    "data",
)
_LIST_KEYS = ("files", "images", "media", "assets")


def _table(chain: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain.lower()) or "default"
    return f"nft_assets_{safe}"


def _conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=10,
    )


def _normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.lower() in {"null", "none"}:
        return None
    return text


def _key_name(key: Any) -> str:
    return str(key).strip().replace("-", "_").replace(" ", "_").lower()


def _candidate_from_object(value: Any) -> str | None:
    direct = _normalize_text(value)
    if direct:
        return direct
    if isinstance(value, dict):
        for key in _URL_KEYS:
            candidate = _normalize_text(value.get(key))
            if candidate:
                return candidate
        for key in ("image", "image_url", "imageUrl", "image_uri", "imageUri"):
            candidate = _normalize_text(value.get(key))
            if candidate:
                return candidate
    return None


def _iter_image_candidates(payload: Any, *, _seen: set[int] | None = None) -> Iterator[str]:
    if _seen is None:
        _seen = set()
    if isinstance(payload, (dict, list)):
        obj_id = id(payload)
        if obj_id in _seen:
            return
        _seen.add(obj_id)

    direct = _normalize_text(payload)
    if direct:
        yield direct
        return

    if isinstance(payload, dict):
        for key in ("image", "image_url", "imageUrl", "image_uri", "imageUri"):
            candidate = _candidate_from_object(payload.get(key))
            if candidate:
                yield candidate

        for key in _CONTAINER_KEYS:
            nested = payload.get(key)
            if nested is not None:
                yield from _iter_image_candidates(nested, _seen=_seen)

        for key in _LIST_KEYS:
            nested = payload.get(key)
            if isinstance(nested, list):
                yield from _iter_image_candidates(nested, _seen=_seen)

        for raw_key, value in payload.items():
            normalized_key = _key_name(raw_key)
            if normalized_key in _IMAGE_KEYS:
                candidate = _candidate_from_object(value)
                if candidate:
                    yield candidate
                    continue
            if normalized_key in {_key_name(k) for k in _URL_KEYS} and "image" in normalized_key:
                candidate = _candidate_from_object(value)
                if candidate:
                    yield candidate
                    continue
            if isinstance(value, (dict, list)):
                yield from _iter_image_candidates(value, _seen=_seen)
        return

    if isinstance(payload, list):
        image_like = []
        other = []
        for item in payload:
            if isinstance(item, dict):
                mime = _key_name(item.get("mimeType") or item.get("mime_type") or item.get("type") or "")
                kind = _key_name(item.get("kind") or item.get("category") or "")
                if "image" in mime or "image" in kind:
                    image_like.append(item)
                else:
                    other.append(item)
            else:
                other.append(item)
        for item in image_like + other:
            candidate = _candidate_from_object(item)
            if candidate:
                yield candidate
                continue
            if isinstance(item, (dict, list)):
                yield from _iter_image_candidates(item, _seen=_seen)


def extract_image_uri(metadata: Any) -> str | None:
    if metadata is None:
        return None
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            return None

    for candidate in _iter_image_candidates(metadata):
        return candidate
    return None


def _has_columns(conn, table: str, columns: Iterable[str]) -> bool:
    required = list(columns)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
              AND column_name = ANY(%s)
            """,
            (table, required),
        )
        found = {row[0] for row in cur.fetchall()}
    return all(col in found for col in required)


def _fetch_batch(
    conn,
    table: str,
    *,
    last_id: int,
    batch_size: int,
    remaining: int | None,
) -> list[tuple[int, str, Any]]:
    size = batch_size if remaining is None else min(batch_size, remaining)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, contract_address || ':' || token_id::text AS nft_key, metadata
            FROM {table}
            WHERE id > %s
              AND metadata IS NOT NULL
              AND (image_uri IS NULL OR btrim(image_uri) = '')
            ORDER BY id
            LIMIT %s
            """,
            (last_id, size),
        )
        return cur.fetchall()


def _bulk_update(conn, table: str, rows: list[tuple[int, str]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            UPDATE {table} AS t
            SET image_uri = v.image_uri
            FROM (VALUES %s) AS v(id, image_uri)
            WHERE t.id = v.id
              AND (t.image_uri IS NULL OR btrim(t.image_uri) = '')
            """,
            rows,
            template="(%s, %s)",
            page_size=500,
        )
        updated = max(cur.rowcount, 0)
    conn.commit()
    return updated


def _process_chain(
    conn,
    chain: str,
    *,
    batch_size: int,
    dry_run: bool,
    limit: int | None,
) -> tuple[int, int, int]:
    table = _table(chain)
    required_columns = ("id", "metadata", "image_uri")
    if not _has_columns(conn, table, required_columns):
        log.warning("链 %s 跳过：表 %s 缺少必要列 %s", chain, table, ",".join(required_columns))
        return 0, 0, 0

    scanned = extracted = updated = 0
    last_id = 0
    remaining = limit

    while True:
        rows = _fetch_batch(
            conn,
            table,
            last_id=last_id,
            batch_size=batch_size,
            remaining=remaining,
        )
        if not rows:
            break

        scanned += len(rows)
        last_id = rows[-1][0]
        if remaining is not None:
            remaining -= len(rows)

        patch_rows: list[tuple[int, str]] = []
        for row_id, nft_key, metadata in rows:
            image_uri = extract_image_uri(metadata)
            if not image_uri:
                continue
            extracted += 1
            patch_rows.append((row_id, image_uri))

        if dry_run:
            if patch_rows:
                sample_key = next(
                    (nft_key for row_id, nft_key, metadata in rows if extract_image_uri(metadata)),
                    None,
                )
                log.info(
                    "链 %s 本批命中 %d 条，示例=%s",
                    chain,
                    len(patch_rows),
                    sample_key or "N/A",
                )
        else:
            updated += _bulk_update(conn, table, patch_rows)

        log.info(
            "链 %s 进度：已扫描 %d 条，提取到 image_uri %d 条，%s %d 条",
            chain,
            scanned,
            extracted,
            "预计更新" if dry_run else "已更新",
            extracted if dry_run else updated,
        )

        if remaining is not None and remaining <= 0:
            break

    return scanned, extracted, updated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="回填 metadata 非空但 image_uri 为空的 NFT 记录"
    )
    parser.add_argument(
        "--chains",
        nargs="+",
        default=DEFAULT_CHAINS,
        help="要处理的链，例如: --chains base polygon",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="每批读取的记录数",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="每条链最多处理多少条候选记录",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅统计和预览，不实际写库",
    )
    args = parser.parse_args()

    conn = _conn()
    try:
        grand_scanned = grand_extracted = grand_updated = 0
        for chain in args.chains:
            log.info(
                "开始处理链 %s | batch_size=%d | limit=%s | dry_run=%s",
                chain,
                args.batch_size,
                args.limit if args.limit is not None else "ALL",
                args.dry_run,
            )
            scanned, extracted, updated = _process_chain(
                conn,
                chain,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
                limit=args.limit,
            )
            grand_scanned += scanned
            grand_extracted += extracted
            grand_updated += updated
            log.info(
                "链 %s 完成：扫描 %d 条，提取 %d 条，%s %d 条",
                chain,
                scanned,
                extracted,
                "预计更新" if args.dry_run else "已更新",
                extracted if args.dry_run else updated,
            )

        log.info(
            "全部完成：扫描 %d 条，提取 %d 条，%s %d 条",
            grand_scanned,
            grand_extracted,
            "预计更新" if args.dry_run else "已更新",
            grand_extracted if args.dry_run else grand_updated,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
