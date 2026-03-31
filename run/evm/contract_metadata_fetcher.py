#!/usr/bin/env python3
"""
EVM：通过 Alchemy JSON-RPC（alchemy_getTokenMetadata）拉取合约级 name、symbol，写回各链主表 nft_assets_{chain}。

数据流：按「库段」从 PostgreSQL 读取 distinct 合约（默认每段 512），段内并发调用 Alchemy，
每段拉完立即批量 UPDATE 写库，再读下一段，避免一次性载入全表。

用法：
  python run/evm/contract_metadata_fetcher.py
  python run/evm/contract_metadata_fetcher.py --chains ethereum base --force
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import urllib.error
import urllib.request

import psycopg2
import psycopg2.extras

# ─── 硬编码配置 ───────────────────────────────────────────────────────────────
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS = "localhost", 5432, "nft_data", "user1", "_JC!y7XWygm$94f"
ALCHEMY_API_KEY = "O6O-K8fkagLHjOa-LLM3_"

CHAIN_RPC = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2",
    "base":     "https://base-mainnet.g.alchemy.com/v2",
    "polygon":  "https://polygon-mainnet.g.alchemy.com/v2",
}

DEFAULT_BATCH   = 32
DEFAULT_WORKERS = 12
DEFAULT_SEGMENT = 512

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _table(chain: str) -> str:
    return f"nft_assets_{re.sub(r'[^a-z0-9_]', '', chain.lower()) or 'default'}"


def _conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS, connect_timeout=10)


def _rpc_url(chain: str) -> str:
    base = CHAIN_RPC.get(chain.lower())
    if not base:
        raise ValueError(f"不支持的链: {chain}")
    return f"{base}/{ALCHEMY_API_KEY}"


def _norm_addr(raw: str) -> str | None:
    a = raw.strip().lower()
    return a if re.fullmatch(r"0x[0-9a-f]{40}", a) else None


# ─── DB ───────────────────────────────────────────────────────────────────────

def ensure_columns(conn, chain: str) -> None:
    """幂等：在 nft_assets_{chain} 上追加 name / symbol 列。"""
    tbl = _table(chain)
    with conn.cursor() as cur:
        for col, typedef in (("name", "VARCHAR(200)"), ("symbol", "VARCHAR(20)")):
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = '{tbl}'
                          AND column_name = '{col}'
                    ) THEN ALTER TABLE {tbl} ADD COLUMN {col} {typedef}; END IF;
                END $$;
            """)
    conn.commit()
    log.info("已检查列 name/symbol：%s", tbl)


def fetch_segment(conn, chain: str, *, limit: int, only_missing: bool,
                  after: str | None) -> tuple[list[str], str | None]:
    """
    取一段 distinct 合约地址。
    - only_missing：只取 name IS NULL 的行（UPDATE 后自动消失，无需 keyset）。
    - force：keyset 分页，用 after 游标翻页。
    返回 (地址列表, 下一段游标)。
    """
    tbl = _table(chain)
    if only_missing:
        sql = f"""
            SELECT DISTINCT lower(contract_address) FROM {tbl}
            WHERE contract_address IS NOT NULL AND name IS NULL
            ORDER BY 1 LIMIT %s
        """
        params: tuple = (limit,)
    elif after is None:
        sql = f"""
            SELECT DISTINCT lower(contract_address) FROM {tbl}
            WHERE contract_address IS NOT NULL
            ORDER BY 1 LIMIT %s
        """
        params = (limit,)
    else:
        sql = f"""
            SELECT DISTINCT lower(contract_address) FROM {tbl}
            WHERE contract_address IS NOT NULL AND lower(contract_address) > %s
            ORDER BY 1 LIMIT %s
        """
        params = (after, limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        addrs = [r[0] for r in cur if r[0]]

    # only_missing 每轮从头读，不需要游标
    next_after = None if only_missing else (addrs[-1] if addrs else None)
    return addrs, next_after


def bulk_update(conn, table: str, rows: list[tuple[str, str | None, str | None]]) -> int:
    """
    单条 UPDATE ... FROM (VALUES ...) 批量写库，比 executemany 快数倍。
    rows: [(contract_address_lower, name, symbol), ...]
    """
    if not rows:
        return 0
    values = [(addr, n[:200] if n else None, s[:20] if s else None) for addr, n, s in rows]
    sql = f"""
        UPDATE {table} t
        SET name = v.name, symbol = v.symbol
        FROM (VALUES %s) AS v(ca, name, symbol)
        WHERE lower(t.contract_address) = v.ca
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, template="(%s, %s, %s)", page_size=500)
    conn.commit()
    return len(rows)


# ─── Alchemy ──────────────────────────────────────────────────────────────────

def _call_alchemy(url: str, addr: str) -> tuple[str | None, str | None]:
    body = json.dumps({"jsonrpc": "2.0", "id": 1,
                       "method": "alchemy_getTokenMetadata", "params": [addr]}).encode()
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        data = json.loads(e.read().decode(errors="replace"))
    except Exception:
        return None, None

    res = data.get("result") if not data.get("error") else None
    if not isinstance(res, dict):
        return None, None
    name = (res.get("name") or "").strip() or None
    sym  = (res.get("symbol") or "").strip() or None
    return name, sym


def _fetch_batch_sync(url: str, addrs: list[str]) -> list[tuple[str, str | None, str | None]]:
    return [(a, *_call_alchemy(url, a)) for a in addrs if _norm_addr(a)]


async def _fetch_batch_async(url: str, addrs: list[str], sem: asyncio.Semaphore):
    async with sem:
        return await asyncio.to_thread(_fetch_batch_sync, url, addrs)


# ─── 主流程 ───────────────────────────────────────────────────────────────────

async def _process_chain(conn, chain: str, *, only_missing: bool,
                         batch: int, segment: int, sem: asyncio.Semaphore) -> int:
    tbl = _table(chain)
    url = _rpc_url(chain)
    log.info("链 %s | %s | batch=%d segment=%d", chain, "补缺" if only_missing else "全量", batch, segment)

    total, seg_idx, after = 0, 0, None
    while True:
        addrs, after = fetch_segment(conn, chain, limit=segment, only_missing=only_missing, after=after)
        if not addrs:
            break

        chunks = [addrs[i:i + batch] for i in range(0, len(addrs), batch)]
        results = await asyncio.gather(*[_fetch_batch_async(url, ch, sem) for ch in chunks])
        flat = [item for part in results for item in part]

        n = bulk_update(conn, tbl, flat)
        total += n
        seg_idx += 1
        log.info("链 %s 第 %d 段：%d 合约（累计 %d）", chain, seg_idx, n, total)

    log.info("链 %s 完成，共 %d 合约", chain, total)
    return total


async def _amain(args: argparse.Namespace) -> None:
    conn0 = _conn()
    try:
        for ch in args.chains:
            ensure_columns(conn0, ch)
    finally:
        conn0.close()

    sem = asyncio.Semaphore(max(1, args.workers))
    grand = 0
    for chain in args.chains:
        conn = _conn()
        try:
            grand += await _process_chain(conn, chain, only_missing=not args.force,
                                          batch=args.batch, segment=args.segment, sem=sem)
        finally:
            conn.close()
    log.info("全部完成，共处理约 %d 个合约", grand)


def main() -> None:
    p = argparse.ArgumentParser(description="EVM 合约 name/symbol 写入 nft_assets_{chain}")
    p.add_argument("--chains", nargs="+", default=["ethereum", "base", "polygon"],
                   choices=list(CHAIN_RPC), help="要处理的链")
    p.add_argument("--batch",    type=int, default=DEFAULT_BATCH,   help="Alchemy 每批合约数")
    p.add_argument("--segment",  type=int, default=DEFAULT_SEGMENT, help="每段读取的 distinct 合约数")
    p.add_argument("--workers",  type=int, default=DEFAULT_WORKERS, help="并发批次数（信号量）")
    p.add_argument("--force", action="store_true", help="全量重拉（默认仅补缺 name 为空的合约）")
    asyncio.run(_amain(p.parse_args()))


if __name__ == "__main__":
    main()
