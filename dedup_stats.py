#!/usr/bin/env python3
"""
跨链 NFT 重复数量统计

通过执行 SQL 统计两张 NFT 表的重叠情况（normalize_url 规范化在 DB 侧完成）：
  · 两表各自的重复 NFT 数及比例
  · 两表各自涉及的重复合约数及比例

执行步骤（3 次流式 SQL，DB 端无临时文件）：
  Pass 1  流式扫描 t2，DB 侧调用 normalize_url()，结果按段传回 Python，
          Python 构建 t2 key 集合（set）
  Pass 2  流式扫描 t1，Python 对比 t2 key 集合，统计 t1 侧重复数；
          同时构建 t1 key 集合
  Pass 3  流式扫描 t2，Python 对比 t1 key 集合，统计 t2 侧重复数

关键点：
  · DB 端只做顺序扫描 + normalize_url()，不产生任何临时文件
  · 集合对比在 Python 侧完成（O(1) 查找）
  · 服务端命名游标按段 FETCH，峰值内存 = 两张表 key 集合之和

链名 → 表名规则：  nft_assets_{chain}
  例：base → nft_assets_base   polygon → nft_assets_polygon

用法：
  python dedup_stats.py
  python dedup_stats.py --c1 base --c2 polygon
  python dedup_stats.py --c1 ethereum --c2 solana --seg 50000 -o report.txt
"""

import argparse
import logging
import os
import sys
import time

import psycopg2
import psycopg2.extras

# ── 日志 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── 数据库配置 ────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "123456")

# ── SQL：Step 1 — 创建规范化函数 ──────────────────────────────────────────────
SQL_NORMALIZE_FUNC = """
CREATE OR REPLACE FUNCTION normalize_url(url TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE STRICT
AS $$
DECLARE
    s   TEXT;
    lo  TEXT;
    cid TEXT;
    tx  TEXT;
BEGIN
    s  := trim(url);
    lo := lower(s);

    IF lo = ANY(ARRAY[
        'nano','null','none','undefined','n/a','na',
        '-', '.', 'false', 'true', '0'
    ]) OR lo LIKE 'data:%%' THEN
        RETURN NULL;
    END IF;

    IF lo LIKE 'ipfs://%%' THEN
        cid := substring(s FROM 8);
        IF lower(cid) LIKE 'ipfs/%%' THEN cid := substring(cid FROM 6); END IF;
        cid := btrim(split_part(split_part(cid, '?', 1), '#', 1), '/');
        RETURN CASE WHEN cid <> '' THEN 'ipfs:' || cid ELSE NULL END;
    END IF;

    IF lo LIKE 'ar://%%' THEN
        tx := btrim(split_part(split_part(substring(s FROM 6), '?', 1), '#', 1), '/');
        RETURN CASE WHEN tx <> '' THEN 'ar:' || tx ELSE NULL END;
    END IF;

    cid := (regexp_match(s,
        'https?://[^/]+/ipfs/([A-Za-z0-9][^?#\\s]*)', 'i'))[1];
    IF cid IS NOT NULL THEN
        cid := rtrim(split_part(split_part(cid, '?', 1), '#', 1), '/');
        RETURN CASE WHEN cid <> '' THEN 'ipfs:' || cid ELSE NULL END;
    END IF;

    tx := (regexp_match(s,
        'https?://(?:[^/]+\\.)?arweave\\.net/([A-Za-z0-9_-]{43}(?:/[^?#\\s]*)?)',
        'i'))[1];
    IF tx IS NOT NULL THEN
        tx := rtrim(split_part(split_part(tx, '?', 1), '#', 1), '/');
        RETURN CASE WHEN tx <> '' THEN 'ar:' || tx ELSE NULL END;
    END IF;

    RETURN rtrim(lo, '/');
END;
$$
"""

# ── SQL：流式读取一张表的规范化 key（DB 侧执行 normalize_url，无聚合无排序）──
# 顺序扫描 + 函数调用，DB 端不产生临时文件，结果流式传回客户端。
# 表名通过 Python format() 插入（已在 chain_to_table() 中校验合法性）。
SQL_STREAM = """
    SELECT contract_address,
           normalize_url(token_uri)  AS tkey,
           normalize_url(image_uri)  AS ikey
    FROM   {table}
    WHERE  token_uri IS NOT NULL
      AND  image_uri IS NOT NULL
"""


# ── 工具函数 ──────────────────────────────────────────────────────────────────
def chain_to_table(chain: str) -> str:
    """链名 → 表名：nft_assets_{chain}，仅允许字母/数字/下划线。"""
    import re
    safe = re.sub(r"[^a-z0-9_]", "", chain.lower().strip())
    if not safe:
        raise ValueError(f"非法链名: {chain!r}")
    return f"nft_assets_{safe}"


def pct(part: int, total: int) -> str:
    if total == 0:
        return "  N/A  "
    return f"{part / total * 100:6.2f}%"


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10,
    )


# ── 执行各步骤 ────────────────────────────────────────────────────────────────
def step1_create_function(cur):
    logger.info("Step 1：创建 normalize_url() 函数...")
    cur.execute(SQL_NORMALIZE_FUNC)
    logger.info("  完成")


def _stream_cursor(conn, name: str, table: str, seg_size: int):
    """
    通过服务端命名游标按段 FETCH，将 DB 端 normalize_url() 的结果流式传回 Python。
    DB 端只做顺序扫描 + 函数调用，无聚合/排序/JOIN → 不产生临时文件。
    """
    sql = SQL_STREAM.format(table=table)
    cur = conn.cursor(name)
    cur.itersize = seg_size
    cur.execute(sql)
    return cur


def step2_load_keys(conn, table: str, seg_size: int):
    """Pass 1 / Pass 2 前置：把整张表的规范化 key 装入 Python set，同时统计总行数与合约数。"""
    tkeys: set = set()
    ikeys: set = set()
    contracts: set = set()
    total = 0
    seg = 0
    t0 = time.monotonic()
    logger.info("  加载 %s key 集合...", table)

    with _stream_cursor(conn, f"load_{table}", table, seg_size) as cur:
        while True:
            rows = cur.fetchmany(seg_size)
            if not rows:
                break
            for contract, tkey, ikey in rows:
                total += 1
                contracts.add(contract)
                if tkey:
                    tkeys.add(tkey)
                if ikey:
                    ikeys.add(ikey)
            seg += 1
            if seg % 20 == 0:
                logger.info("    [%s] %d 段 / %d 行 / %.1fs", table, seg, total,
                            time.monotonic() - t0)

    logger.info("  [%s] 完成：%d 行，%d 合约，tkeys=%d，ikeys=%d，耗时 %.1fs",
                table, total, len(contracts), len(tkeys), len(ikeys),
                time.monotonic() - t0)
    return tkeys, ikeys, total, len(contracts)


def step3_count_dups(conn, table: str,
                     other_tkeys: set, other_ikeys: set,
                     seg_size: int):
    """
    Pass 2/3：流式扫描 table，对比 other_*keys，分三种维度统计重复。

    返回 (stats: dict, own_tkeys: set, own_ikeys: set)

    stats 包含：
      v1_nfts / v1_contracts  — token_uri 相同
      v2_nfts / v2_contracts  — token_uri 不同（或规范化后为 NULL），image_uri 相同
      v3_nfts / v3_contracts  — token_uri 或 image_uri 任一相同（v1 ∪ v2）
      total / all_contracts   — 参与比对的全量数
    """
    # v1：tkey 命中
    v1_nfts = 0
    v1_contracts: set = set()
    # v2：tkey 未命中，但 ikey 命中
    v2_nfts = 0
    v2_contracts: set = set()

    own_tkeys: set = set()
    own_ikeys: set = set()
    all_contracts: set = set()
    total = 0
    seg = 0
    t0 = time.monotonic()
    logger.info("  扫描 %s 并对比...", table)

    with _stream_cursor(conn, f"scan_{table}_{int(t0)}", table, seg_size) as cur:
        while True:
            rows = cur.fetchmany(seg_size)
            if not rows:
                break
            for contract, tkey, ikey in rows:
                total += 1
                all_contracts.add(contract)
                if tkey:
                    own_tkeys.add(tkey)
                if ikey:
                    own_ikeys.add(ikey)

                tkey_hit = bool(tkey and tkey in other_tkeys)
                ikey_hit = bool(ikey and ikey in other_ikeys)

                if tkey_hit:
                    # v1：token_uri 相同
                    v1_nfts += 1
                    v1_contracts.add(contract)
                elif ikey_hit:
                    # v2：token_uri 不同，image_uri 相同
                    v2_nfts += 1
                    v2_contracts.add(contract)
                # else：两者均不命中，不计入任何维度

            seg += 1
            if seg % 20 == 0:
                logger.info("    [%s] %d 段 / %d 行 / v1=%d v2=%d / %.1fs",
                            table, seg, total, v1_nfts, v2_nfts,
                            time.monotonic() - t0)

    dup_nfts      = v1_nfts + v2_nfts
    dup_contracts = v1_contracts | v2_contracts
    logger.info(
        "  [%s] 完成：%d 行  v1(token)=%d  v2(image-only)=%d  合计=%d  耗时 %.1fs",
        table, total, v1_nfts, v2_nfts, dup_nfts, time.monotonic() - t0,
    )
    stats = {
        "v1_nfts":        v1_nfts,
        "v1_contracts":   len(v1_contracts),
        "v2_nfts":        v2_nfts,
        "v2_contracts":   len(v2_contracts),
        "total_nfts":     dup_nfts,
        "total_contracts": len(dup_contracts),
        "total":          total,
        "all_contracts":  len(all_contracts),
    }
    return stats, own_tkeys, own_ikeys


# ── 输出报告 ──────────────────────────────────────────────────────────────────
def _dup_rows(label: str, w: int, cw: int, dash: str,
              t1r: dict, t2r: dict, vk: str, total1: int, total2: int,
              ac1: int, ac2: int) -> list:
    """生成某个重复维度（v1/v2/v3）的报告行组。"""
    nk = f"{vk}_nfts"
    ck = f"{vk}_contracts"
    return [
        dash,
        f"  {label:>{w}}",
        f"  {'  重复 NFT 数':>{w}}  {t1r[nk]:>{cw},}  {t2r[nk]:>{cw},}",
        f"  {'  重复 NFT 比例':>{w}}  "
        f"{pct(t1r[nk], total1):>{cw}}  "
        f"{pct(t2r[nk], total2):>{cw}}",
        f"  {'  涉及合约数':>{w}}  {t1r[ck]:>{cw},}  {t2r[ck]:>{cw},}",
        f"  {'  涉及合约比例':>{w}}  "
        f"{pct(t1r[ck], ac1):>{cw}}  "
        f"{pct(t2r[ck], ac2):>{cw}}",
    ]


def print_report(
    c1: str, t1: str,
    c2: str, t2: str,
    t1r: dict, t2r: dict,
    elapsed: float,
    out_file: str = "",
) -> None:
    sep  = "═" * 70
    dash = "─" * 70
    w    = 34
    cw   = max(20, len(t1), len(t2))

    ac1 = t1r["all_contracts"]
    ac2 = t2r["all_contracts"]

    lines = [
        "",
        sep,
        f"  跨链 NFT 重复统计  （总耗时 {elapsed:.1f}s）",
        sep,
        f"  {'链名':>{w}}  {c1.upper():>{cw}}  {c2.upper():>{cw}}",
        f"  {'表名':>{w}}  {t1:>{cw}}  {t2:>{cw}}",
        dash,
        f"  {'有效 NFT 总数（双字段均非空）':>{w}}  {t1r['total']:>{cw},}  {t2r['total']:>{cw},}",
        f"  {'唯一合约总数':>{w}}  {ac1:>{cw},}  {ac2:>{cw},}",
    ]

    lines += _dup_rows(
        "▶ v1  token_uri 相同",
        w, cw, dash, t1r, t2r, "v1", t1r["total"], t2r["total"], ac1, ac2,
    )
    lines += _dup_rows(
        "▶ v2  token_uri 不同、image_uri 相同",
        w, cw, dash, t1r, t2r, "v2", t1r["total"], t2r["total"], ac1, ac2,
    )
    lines += _dup_rows(
        "▶ 合计  token_uri 或 image_uri 任一相同（v1 ∪ v2）",
        w, cw, dash, t1r, t2r, "total", t1r["total"], t2r["total"], ac1, ac2,
    )

    lines += [sep, ""]
    text = "\n".join(lines)
    print(text)

    if out_file:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(text)
        logger.info("报告已写入：%s", out_file)


# ── 主流程 ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="通过流式 SQL 统计两张 NFT 表（按链名）的重复 NFT 数量与涉及合约数",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--c1", default="ethereum", metavar="CHAIN",
                        help="链名 1（表名 = nft_assets_{chain}）")
    parser.add_argument("--c2", default="solana",   metavar="CHAIN",
                        help="链名 2（表名 = nft_assets_{chain}）")
    parser.add_argument("--seg", type=int, default=100_000, metavar="N",
                        help="每次 FETCH 的行数（服务端游标批量大小）")
    parser.add_argument("-o", "--output", default="", metavar="FILE",
                        help="将报告写入指定文件（默认自动命名）")
    args = parser.parse_args()

    c1, t1 = args.c1, chain_to_table(args.c1)
    c2, t2 = args.c2, chain_to_table(args.c2)

    logger.info("链名: %s → 表名: %s", c1, t1)
    logger.info("链名: %s → 表名: %s", c2, t2)

    out_file = args.output or f"dedup_{c1}_{c2}_{time.strftime('%Y%m%d_%H%M%S')}.txt"

    t_start = time.monotonic()
    conn = get_conn()
    try:
        # ── Step 1：创建 normalize_url() ──────────────────────────────────────
        conn.autocommit = False
        with conn.cursor() as cur:
            step1_create_function(cur)
        conn.commit()

        # ── Pass 1：加载 t2 key 集合 ───────────────────────────────────────────
        logger.info("Pass 1：流式加载 %s key 集合（DB 端 normalize_url）...", t2)
        conn.autocommit = False
        t2_tkeys, t2_ikeys, t2_total, t2_all_contracts = \
            step2_load_keys(conn, t2, args.seg)
        conn.commit()

        # ── Pass 2：扫描 t1，对比 t2 key，同时构建 t1 key 集合 ────────────────
        logger.info("Pass 2：流式扫描 %s，对比 %s...", t1, t2)
        conn.autocommit = False
        t1_stats, t1_tkeys, t1_ikeys = \
            step3_count_dups(conn, t1, t2_tkeys, t2_ikeys, args.seg)
        conn.commit()

        # ── Pass 3：扫描 t2，对比 t1 key，得 t2 侧重复数 ─────────────────────
        logger.info("Pass 3：流式扫描 %s，对比 %s...", t2, t1)
        conn.autocommit = False
        t2_stats, _, _ = \
            step3_count_dups(conn, t2, t1_tkeys, t1_ikeys, args.seg)
        conn.commit()

    finally:
        conn.close()

    print_report(c1, t1, c2, t2, t1_stats, t2_stats,
                 time.monotonic() - t_start, out_file)


if __name__ == "__main__":
    main()
