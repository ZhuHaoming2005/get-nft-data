#!/usr/bin/env python3
"""
以太坊 NFT 数据导入工具（Python）

从 JSON 文件流式读取以太坊 NFT 数据，写入 PostgreSQL 表（默认 nft_assets_ethereum）。

过滤规则与 Go 版一致；入库策略 ON CONFLICT (contract_address, token_id) DO NOTHING。

依赖：
  pip install psycopg2-binary ijson

用法：
  python import_eth.py
  python import_eth.py "data/*.json"
  python import_eth.py -w 8 -batch 500 -table nft_assets_polygon
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from glob import glob
from typing import Any, Dict, Iterator, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from psycopg2 import sql

try:
    import ijson

    _HAS_IJSON = True
except ImportError:
    _HAS_IJSON = False

# ── 占位符跳过集合 ────────────────────────────────────────────────────────────
_SKIP_VALUES = frozenset(
    {
        "nano",
        "null",
        "none",
        "undefined",
        "n/a",
        "na",
        "-",
        ".",
        "false",
        "true",
        "0",
    }
)


def _is_valid_url(raw: str) -> bool:
    s = raw.strip()
    if not s:
        return False
    lo = s.lower()
    if lo.startswith("data:"):
        return False
    return lo not in _SKIP_VALUES


def _parse_identifier_field(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ("null", "none"):
            return None
        return s
    if isinstance(v, int):
        return str(v)
    if isinstance(v, Decimal):
        try:
            iv = int(v)
        except (ValueError, OverflowError):
            return None
        if Decimal(iv) != v:
            return None
        return str(iv)
    if isinstance(v, float):
        return None
    return None


def _iter_json_records(path: str) -> Iterator[Dict[str, Any]]:
    if _HAS_IJSON:
        with open(path, "rb") as f:
            yield from ijson.items(f, "item")
        return
    import json

    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        yield from data
    elif isinstance(data, dict):
        yield data


def _row_from_record(rec: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    contract = (rec.get("contract") or "").strip().lower()
    if not contract:
        return None
    tid = _parse_identifier_field(rec.get("identifier"))
    if tid is None:
        return None
    meta = rec.get("metadata_url")
    img = rec.get("image_url")
    meta = meta.strip() if isinstance(meta, str) else str(meta or "").strip()
    img = img.strip() if isinstance(img, str) else str(img or "").strip()
    if not _is_valid_url(meta) or not _is_valid_url(img):
        return None
    return (contract, tid, meta, img)


def _read_file_to_queue(path: str, record_q: "queue.Queue[Optional[Tuple[str, str, str, str]]]") -> Tuple[int, int]:
    read_n = 0
    skipped = 0
    base = os.path.basename(path)
    for rec in _iter_json_records(path):
        read_n += 1
        row = _row_from_record(rec)
        if row is None:
            skipped += 1
            continue
        record_q.put(row)
    logging.info("  [JSON] %-60s  读取 %d  跳过 %d", base, read_n, skipped)
    return read_n, skipped


# ── 数据库 ────────────────────────────────────────────────────────────────────
def _env_or(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v else default


def _connect():
    return psycopg2.connect(
        host=_env_or("DB_HOST", "pgm-2zevls2414y7mw6d8o.pg.rds.aliyuncs.com"),
        port=int(_env_or("DB_PORT", "5432")),
        dbname=_env_or("DB_NAME", "nft_data"),
        user=_env_or("DB_USER", "user1"),
        password=_env_or("DB_PASS", "_JC!y7XWygm$94f"),
        connect_timeout=10,
    )


def _ensure_table(cur, table: str) -> None:
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(42)  NOT NULL,
                token_id         NUMERIC      NOT NULL,
                token_uri        TEXT,
                image_uri        TEXT,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (contract_address, token_id)
            )
            """
        ).format(sql.Identifier(table))
    )
    safe = table.replace(".", "_")
    idx_name = "idx_" + safe + "_contract"
    cur.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (contract_address)").format(
            sql.Identifier(idx_name), sql.Identifier(table)
        )
    )


def _insert_batch(
    cur, conn, table: str, batch: List[Tuple[str, str, str, str]]
) -> Tuple[int, int]:
    if not batch:
        return 0, 0
    stmt = sql.SQL(
        "INSERT INTO {} (contract_address, token_id, token_uri, image_uri) "
        "VALUES %s ON CONFLICT (contract_address, token_id) DO NOTHING "
        "RETURNING 1"
    ).format(sql.Identifier(table))
    qstr = stmt.as_string(conn)
    returned = psycopg2.extras.execute_values(
        cur, qstr, batch, page_size=len(batch), fetch=True
    )
    inserted = len(returned) if returned else 0
    conflicts = len(batch) - inserted
    return inserted, conflicts


def _insert_worker(
    table: str,
    batch_size: int,
    record_q: "queue.Queue[Optional[Tuple[str, str, str, str]]]",
    inserted: List[int],
    conflicts: List[int],
    thread_exc: List[Optional[BaseException]],
) -> None:
    conn = _connect()
    conn.autocommit = False
    batch: List[Tuple[str, str, str, str]] = []
    try:
        with conn.cursor() as cur:
            while True:
                row = record_q.get()
                if row is None:
                    break
                batch.append(row)
                if len(batch) >= batch_size:
                    ins, dup = _insert_batch(cur, conn, table, batch)
                    inserted[0] += ins
                    conflicts[0] += dup
                    conn.commit()
                    batch = []
            if batch:
                ins, dup = _insert_batch(cur, conn, table, batch)
                inserted[0] += ins
                conflicts[0] += dup
                conn.commit()
    except Exception as e:
        conn.rollback()
        logging.exception("批量写入失败")
        thread_exc[0] = e
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    ap = argparse.ArgumentParser(description="以太坊 NFT JSON 导入 PostgreSQL")
    ap.add_argument(
        "-pattern",
        "--pattern",
        default="classification&status/*.json",
        help="JSON 文件 glob（默认 classification&status/*.json）",
    )
    ap.add_argument(
        "positional_pattern",
        nargs="?",
        default=None,
        help="若提供则覆盖 -pattern",
    )
    ap.add_argument("-w", "--workers", type=int, default=4, help="并行读取文件线程数")
    ap.add_argument(
        "-batch", "--batch", type=int, default=500, help="每批 INSERT 行数"
    )
    ap.add_argument(
        "-table", "--table", default="nft_assets_ethereum", help="目标表名"
    )
    args = ap.parse_args()

    if not _HAS_IJSON:
        logging.warning(
            "未安装 ijson，将整文件 json.load；大文件请: pip install ijson"
        )

    glob_pattern = (
        args.positional_pattern
        if args.positional_pattern is not None
        else args.pattern
    )
    files = sorted(glob(glob_pattern))
    if not files:
        logging.error("未找到匹配文件: %s", glob_pattern)
        sys.exit(1)

    table = args.table
    workers = max(1, args.workers)
    if workers > len(files):
        workers = len(files)
    batch_size = max(1, args.batch)

    logging.info(
        "文件 %d 个  读取线程 %d  目标表 %s  批量 %d",
        len(files),
        workers,
        table,
        batch_size,
    )

    conn = _connect()
    try:
        with conn.cursor() as cur:
            _ensure_table(cur, table)
        conn.commit()
        logging.info("表 %s 已就绪", table)
    except Exception:
        conn.rollback()
        logging.exception("建表失败")
        sys.exit(1)
    finally:
        conn.close()

    record_q: "queue.Queue[Optional[Tuple[str, str, str, str]]]" = queue.Queue(
        maxsize=batch_size * 4
    )
    inserted = [0]
    conflicts = [0]
    t_start = time.monotonic()
    stop_progress = threading.Event()

    def progress_loop() -> None:
        while not stop_progress.is_set():
            if stop_progress.wait(5.0):
                break
            elapsed = time.monotonic() - t_start
            sys.stderr.write(
                "\r  [进度] 已写入 %-10d 条  冲突跳过 %-8d 条  耗时 %.0fs  "
                % (inserted[0], conflicts[0], elapsed)
            )
            sys.stderr.flush()

    ticker = threading.Thread(target=progress_loop, daemon=True)
    ticker.start()

    thread_exc: List[Optional[BaseException]] = [None]
    writer = threading.Thread(
        target=_insert_worker,
        args=(table, batch_size, record_q, inserted, conflicts, thread_exc),
        daemon=False,
    )
    writer.start()

    total_read = 0
    total_skip = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_path = {ex.submit(_read_file_to_queue, p, record_q): p for p in files}
        for fut in as_completed(future_to_path):
            path = future_to_path[fut]
            try:
                r, sk = fut.result()
                total_read += r
                total_skip += sk
            except Exception:
                logging.exception("读取 %s 失败", path)

    record_q.put(None)
    writer.join()
    stop_progress.set()

    if thread_exc[0]:
        logging.error("写入线程异常，已中止")
        sys.exit(1)

    sys.stderr.write("\n")
    sys.stderr.flush()

    elapsed = time.monotonic() - t_start
    sep = "═" * 60
    print()
    print(sep)
    print("  导入完成  耗时 %.1fs" % elapsed)
    print("  JSON 总读取:   %d 条" % total_read)
    print("  过滤跳过:      %d 条（URL为空/占位符）" % total_skip)
    print("  实际写入:      %d 条" % inserted[0])
    print("  冲突跳过:      %d 条（已存在）" % conflicts[0])
    if elapsed > 0:
        print("  写入速度:      %.0f 条/s" % (inserted[0] / elapsed))
    print(sep)


if __name__ == "__main__":
    main()
