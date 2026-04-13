#!/usr/bin/env python3
"""
脚本 1：单链内 NFT 重复统计（仅 token_uri 与 image_uri 均非空的行）。

三维度（与 dedup_stats 一致）：
  v1  token_uri 相同
  v2  token_uri 未形成重复，但 image_uri 相同
  v3  v1 ∪ v2

对比方式：
  · 严格字符串匹配：trim 后的原始 URI 字符串
  · 统一分布式存储网关匹配：PostgreSQL normalize_url()（与 dedup_stats 一致）

统计规则（仅 EVM：ethereum / base / polygon）：
  · 任意重复：同一 key 在链上出现 ≥2 次即计为重复
  · 仅跨合约重复：同一 key 出现在 ≥2 个不同合约上才计为重复

Solana：仅输出「严格」与「规范化」两种对比方式（各三维度），不区分上述两种合约规则。

大规模表（数十 GB）：全表仅流式扫描；聚合为按 URI key 的字典，峰值内存主要取决于「不同 URI 数量」。
可选 --gc-every 定期 gc，缓解长时间运行碎片。

用法（在项目根目录）：
  python -m dedup_analysis.intra_chain
  python -m dedup_analysis.intra_chain --chains ethereum base -o intra.txt --seg 50000 --gc-every 50
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# 允许 python dedup_analysis/intra_chain.py 与 python -m dedup_analysis.intra_chain
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dedup_analysis.common import (  # noqa: E402
    IntraChainReportEVM,
    IntraChainReportSolana,
    pct,
    run_intra_chain_evm,
    run_intra_chain_solana,
)
from dedup_stats import chain_to_table, get_conn, step1_create_function  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

EVM_CHAINS = frozenset({"ethereum", "base", "polygon"})


def _dim_block(title: str, rep_evm: IntraChainReportEVM, w: int, cw: int, dash: str) -> list:
    lines = [dash, f"  {title}"]
    modes = [
        ("  [严格] 任意重复", rep_evm.strict_any),
        ("  [严格] 仅跨合约重复", rep_evm.strict_cross),
        ("  [规范化] 任意重复", rep_evm.norm_any),
        ("  [规范化] 仅跨合约重复", rep_evm.norm_cross),
    ]
    for label, d in modes:
        lines.append(f"    {label}")
        for vk, vlabel in (
            ("v1", "v1  token_uri 相同"),
            ("v2", "v2  token 不同、image 相同"),
            ("v3", "v3  token 或 image 任一相同"),
        ):
            dc = d[vk]
            lines.append(
                f"      {vlabel}: 重复 NFT {dc.nfts:>{cw},} ({pct(dc.nfts, rep_evm.total_nfts)})  "
                f"涉及合约 {len(dc.contracts):>{cw},} ({pct(len(dc.contracts), rep_evm.all_contracts)})"
            )
    return lines


def _sol_block(title: str, rep: IntraChainReportSolana, w: int, cw: int, dash: str) -> list:
    lines = [dash, f"  {title}"]
    for label, d in (
        ("  [严格] 任意重复", rep.strict_any),
        ("  [规范化] 任意重复", rep.norm_any),
    ):
        lines.append(f"    {label}")
        for vk, vlabel in (
            ("v1", "v1  token_uri 相同"),
            ("v2", "v2  token 不同、image 相同"),
            ("v3", "v3  token 或 image 任一相同"),
        ):
            dc = d[vk]
            lines.append(
                f"      {vlabel}: 重复 NFT {dc.nfts:>{cw},} ({pct(dc.nfts, rep.total_nfts)})  "
                f"涉及合约 {len(dc.contracts):>{cw},} ({pct(len(dc.contracts), rep.all_contracts)})"
            )
    return lines


def print_report_evm(chain: str, table: str, rep: IntraChainReportEVM) -> str:
    sep = "═" * 72
    dash = "─" * 72
    w, cw = 36, 18
    lines = [
        "",
        sep,
        f"  单链内重复统计  {chain.upper()}  表 {table}",
        sep,
        f"  {'有效 NFT 数（双 URI 非空）':>{w}}  {rep.total_nfts:>{cw},}",
        f"  {'唯一合约数':>{w}}  {rep.all_contracts:>{cw},}",
    ]
    lines += _dim_block("EVM：三维度 × 四种规则（严格/规范化 × 任意/跨合约）", rep, w, cw, dash)
    lines += [sep, ""]
    return "\n".join(lines)


def print_report_solana(chain: str, table: str, rep: IntraChainReportSolana) -> str:
    sep = "═" * 72
    dash = "─" * 72
    w, cw = 36, 18
    lines = [
        "",
        sep,
        f"  单链内重复统计  {chain.upper()}  表 {table}",
        sep,
        f"  {'有效 NFT 数（双 URI 非空）':>{w}}  {rep.total_nfts:>{cw},}",
        f"  {'唯一合约数':>{w}}  {rep.all_contracts:>{cw},}",
    ]
    lines += _sol_block("Solana：三维度 × 两种对比（严格 / 规范化）", rep, w, cw, dash)
    lines += [sep, ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="单链内 NFT 重复统计（双 URI 非空）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--chains",
        nargs="*",
        default=["ethereum", "base", "polygon", "solana"],
        metavar="CHAIN",
        help="链名列表（表 nft_assets_{chain}）",
    )
    parser.add_argument(
        "--seg",
        type=int,
        default=50_000,
        metavar="N",
        help="每次 FETCH 行数；列很宽或客户端内存紧时可降到 10000～20000",
    )
    parser.add_argument(
        "--gc-every",
        type=int,
        default=0,
        metavar="BATCHES",
        help="每处理多少批 fetchmany 后执行 gc.collect()（0 表示关闭）；超长任务可设 30～100",
    )
    parser.add_argument("-o", "--output", default="intra-chain-report.txt", help="汇总写入文件")
    args = parser.parse_args()

    t_start = time.monotonic()
    conn = get_conn()
    out_chunks: list[str] = []

    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            step1_create_function(cur)
        conn.commit()

        gc_every = args.gc_every if args.gc_every > 0 else 0

        for chain in args.chains:
            c = chain.lower().strip()
            tbl = chain_to_table(c)
            logger.info("单链分析：%s → %s", c, tbl)

            conn.autocommit = False
            if c in EVM_CHAINS:
                rep, _ = run_intra_chain_evm(conn, tbl, args.seg, gc_every=gc_every)
                conn.commit()
                text = print_report_evm(c, tbl, rep)
            elif c == "solana":
                rep, _ = run_intra_chain_solana(conn, tbl, args.seg, gc_every=gc_every)
                conn.commit()
                text = print_report_solana(c, tbl, rep)
            else:
                logger.warning("未知链 %s，跳过（EVM 仅支持 ethereum/base/polygon，Solana 为 solana）", c)
                continue

            print(text)
            out_chunks.append(text)

    finally:
        conn.close()

    elapsed = time.monotonic() - t_start
    footer = f"\n总耗时 {elapsed:.1f}s\n"
    print(footer)
    out_chunks.append(footer)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("".join(out_chunks))
        logger.info("已写入 %s", args.output)


if __name__ == "__main__":
    main()
