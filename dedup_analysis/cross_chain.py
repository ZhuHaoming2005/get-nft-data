#!/usr/bin/env python3
"""
脚本 2：四张表联合 — 对每条链统计「URI 是否出现在另外三条链上」（不与本链对比）。

三维度、两种对比方式（严格 trim 串 / normalize_url）与 dedup_stats 一致。
跨链只判断「他链是否出现过相同 URI」，不区分合约是否相同。

索引后端：
  · sqlite（默认）：临时文件，仅存 sha256(uri)，省内存，需磁盘空间
  · memory：四张 Set 存完整 URI 字符串，速度快，大表易 OOM

用法（在项目根目录）：
  python -m dedup_analysis.cross_chain
  python -m dedup_analysis.cross_chain --index memory
  python -m dedup_analysis.cross_chain --index sqlite --seg 20000 --other-cache-kb 512 -o cross.txt
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dedup_analysis.common import (  # noqa: E402
    CrossChainReportEVM,
    CrossChainReportSolana,
    OtherChainsIndex,
    OtherChainsIndexMemory,
    OtherChainsIndexSQLite,
    merge_table_into_other,
    pct,
    run_cross_chain_evm,
    run_cross_chain_solana,
)
from dedup_stats import chain_to_table, get_conn, step1_create_function  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

ALL_CHAINS = ("ethereum", "base", "polygon", "solana")
EVM_CHAINS = frozenset({"ethereum", "base", "polygon"})


def _evm_block(rep: CrossChainReportEVM, cw: int, dash: str) -> list:
    lines = [dash, f"  目标链 {rep.chain.upper()}  （对比另外三条链的 URI 集合）"]
    modes = [
        ("  [严格] trim 后字符串匹配", rep.strict),
        ("  [规范化] normalize_url 匹配", rep.norm),
    ]
    for label, d in modes:
        lines.append(f"    {label}")
        for vk, vlabel in (
            ("v1", "v1  token_uri 命中他链"),
            ("v2", "v2  token 未命中、image_uri 命中他链"),
            ("v3", "v3  任一 URI 命中他链"),
        ):
            dc = d[vk]
            lines.append(
                f"      {vlabel}: 重复 NFT {dc.nfts:>{cw},} ({pct(dc.nfts, rep.total_nfts)})  "
                f"涉及合约 {len(dc.contracts):>{cw},} ({pct(len(dc.contracts), rep.all_contracts)})"
            )
    return lines


def _sol_block(rep: CrossChainReportSolana, cw: int, dash: str) -> list:
    lines = [dash, f"  目标链 {rep.chain.upper()}  （对比另外三条链）"]
    for label, d in (
        ("  [严格]", rep.strict),
        ("  [规范化]", rep.norm),
    ):
        lines.append(f"    {label}")
        for vk, vlabel in (
            ("v1", "v1  token_uri 命中他链"),
            ("v2", "v2  token 未命中、image_uri 命中他链"),
            ("v3", "v3  任一 URI 命中他链"),
        ):
            dc = d[vk]
            lines.append(
                f"      {vlabel}: 重复 NFT {dc.nfts:>{cw},} ({pct(dc.nfts, rep.total_nfts)})  "
                f"涉及合约 {len(dc.contracts):>{cw},} ({pct(len(dc.contracts), rep.all_contracts)})"
            )
    return lines


def print_report_evm(rep: CrossChainReportEVM) -> str:
    sep = "═" * 72
    dash = "─" * 72
    cw = 18
    lines = [
        "",
        sep,
        f"  跨链重复统计（不含本链）  {rep.chain.upper()}",
        sep,
        f"  {'有效 NFT 数':>36}  {rep.total_nfts:>{cw},}",
        f"  {'唯一合约数':>36}  {rep.all_contracts:>{cw},}",
    ]
    lines += _evm_block(rep, cw, dash)
    lines += [sep, ""]
    return "\n".join(lines)


def print_report_solana(rep: CrossChainReportSolana) -> str:
    sep = "═" * 72
    dash = "─" * 72
    cw = 18
    lines = [
        "",
        sep,
        f"  跨链重复统计（不含本链）  {rep.chain.upper()}",
        sep,
        f"  {'有效 NFT 数':>36}  {rep.total_nfts:>{cw},}",
        f"  {'唯一合约数':>36}  {rep.all_contracts:>{cw},}",
    ]
    lines += _sol_block(rep, cw, dash)
    lines += [sep, ""]
    return "\n".join(lines)


def build_other_index_sqlite(
    conn,
    others: tuple[str, ...],
    seg: int,
    *,
    gc_every: int,
    sqlite_path: str,
    cache_kb: int,
    commit_every: int,
) -> OtherChainsIndexSQLite:
    idx = OtherChainsIndexSQLite(sqlite_path, cache_kb=cache_kb, commit_every=commit_every)
    try:
        for c in others:
            tbl = chain_to_table(c)
            logger.info("  合并他链索引：%s (%s) → SQLite", c, tbl)
            merge_table_into_other(conn, tbl, seg, idx, gc_every=gc_every)
    except Exception:
        idx.close()
        raise
    return idx


def build_other_index_memory(
    conn,
    others: tuple[str, ...],
    seg: int,
    *,
    gc_every: int,
) -> OtherChainsIndexMemory:
    idx = OtherChainsIndexMemory()
    for c in others:
        tbl = chain_to_table(c)
        logger.info("  合并他链索引：%s (%s) → 内存 Set", c, tbl)
        merge_table_into_other(conn, tbl, seg, idx, gc_every=gc_every)
    return idx


def main() -> None:
    parser = argparse.ArgumentParser(
        description="四链联合：每条链与他链 URI 重复统计（仅 URI 命中，不判跨合约）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--index",
        choices=("sqlite", "memory"),
        default="sqlite",
        help="他链索引：sqlite 落盘省内存；memory 全内存 Set，大表慎用",
    )
    parser.add_argument(
        "--seg",
        type=int,
        default=50_000,
        metavar="N",
        help="每次 FETCH 行数；大表或客户端内存紧时可降低",
    )
    parser.add_argument(
        "--gc-every",
        type=int,
        default=0,
        metavar="BATCHES",
        help="每多少批 fetchmany 后 gc.collect() 一次（0 关闭）",
    )
    parser.add_argument(
        "--other-cache-kb",
        type=int,
        default=16,
        metavar="GB",
        help="仅 sqlite：SQLite 页缓存（KB，PRAGMA cache_size）",
    )
    parser.add_argument(
        "--other-commit-every",
        type=int,
        default=80_000,
        metavar="N",
        help="仅 sqlite：合并他链时每隔多少行提交一次事务",
    )
    parser.add_argument("-o", "--output", default="cross-chain-report.txt", help="写入文件")
    args = parser.parse_args()

    t_start = time.monotonic()
    conn = get_conn()
    chunks: list[str] = []
    gc_every = args.gc_every if args.gc_every > 0 else 0

    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            step1_create_function(cur)
        conn.commit()

        for target in ALL_CHAINS:
            others = tuple(c for c in ALL_CHAINS if c != target)
            logger.info("目标链 %s，他链 %s，索引=%s", target, others, args.index)

            other_idx: OtherChainsIndex | None = None
            sqlite_path: str | None = None
            try:
                conn.autocommit = False
                if args.index == "memory":
                    other_idx = build_other_index_memory(
                        conn, others, args.seg, gc_every=gc_every,
                    )
                else:
                    fd, sqlite_path = tempfile.mkstemp(prefix="dedup_other_", suffix=".sqlite")
                    os.close(fd)
                    other_idx = build_other_index_sqlite(
                        conn,
                        others,
                        args.seg,
                        gc_every=gc_every,
                        sqlite_path=sqlite_path,
                        cache_kb=args.other_cache_kb,
                        commit_every=args.other_commit_every,
                    )
                conn.commit()

                tbl = chain_to_table(target)
                conn.autocommit = False
                if target in EVM_CHAINS:
                    rep = run_cross_chain_evm(
                        conn, tbl, other_idx, args.seg, target, gc_every=gc_every,
                    )
                    conn.commit()
                    text = print_report_evm(rep)
                else:
                    rep = run_cross_chain_solana(
                        conn, tbl, other_idx, args.seg, target, gc_every=gc_every,
                    )
                    conn.commit()
                    text = print_report_solana(rep)

                print(text)
                chunks.append(text)
            finally:
                if isinstance(other_idx, OtherChainsIndexSQLite):
                    other_idx.close()
                if sqlite_path:
                    try:
                        os.unlink(sqlite_path)
                    except OSError:
                        pass
                    for suf in ("-wal", "-shm"):
                        try:
                            os.unlink(sqlite_path + suf)
                        except OSError:
                            pass

    finally:
        conn.close()

    elapsed = time.monotonic() - t_start
    foot = f"\n总耗时 {elapsed:.1f}s\n"
    print(foot)
    chunks.append(foot)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("".join(chunks))
        logger.info("已写入 %s", args.output)


if __name__ == "__main__":
    main()
