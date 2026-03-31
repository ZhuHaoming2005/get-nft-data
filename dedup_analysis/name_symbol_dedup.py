#!/usr/bin/env python3
"""
symbol 与 name 查重已解耦：分别输出「仅 symbol」「仅 name」两套统计（各含单链内 + 跨链）。

内存与 IO：
  · 数据库：每链用服务端游标 + fetchmany 流式读取，整个分析只扫描一次（无重复 IO）。
  · 跨链比较完全基于第一阶段的内存 Counter，不再重复扫描数据库。
  · 单链内模糊：_LenIndex 按长度分桶，跳过必然不满足阈值的配对；并查集 O(n²) 降至近 O(n)。
  · 跨链模糊：per-key 用长度桶过滤候选，再用 rapidfuzz.extractOne（C 层批量）比较。

每套维度（symbol / name 各自独立）：
  单链内：v1 精确重复（≥2 合约）| v2 仅模糊 | v3 v1∪v2
  跨链  ：v1 精确命中他链       | v2 仅模糊命中 | v3 v1∪v2

用法：
  python -m dedup_analysis.name_symbol_dedup
  python -m dedup_analysis.name_symbol_dedup --chains ethereum base polygon solana --threshold 85 --fetch 20000 -o report.txt
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import psycopg2  # noqa: E402

from dedup_stats import chain_to_table, get_conn  # noqa: E402

# ── 模糊比较（优先 rapidfuzz，fallback difflib）────────────────────────────────

try:
    from rapidfuzz import fuzz as _rfuzz
    from rapidfuzz.process import extractOne as _rf_extract_one

    def _fuzzy_ratio(a: str, b: str) -> float:
        return float(_rfuzz.ratio(a, b))

    def _fuzzy_any(k: str, candidates: List[str], threshold: float) -> bool:
        """使用 rapidfuzz C 层批量比较，score_cutoff 让内层提前退出。"""
        return _rf_extract_one(k, candidates, scorer=_rfuzz.ratio, score_cutoff=threshold) is not None

except ImportError:
    import difflib

    def _fuzzy_ratio(a: str, b: str) -> float:  # type: ignore[misc]
        return difflib.SequenceMatcher(None, a, b).ratio() * 100.0

    def _fuzzy_any(k: str, candidates: List[str], threshold: float) -> bool:  # type: ignore[misc]
        return any(_fuzzy_ratio(k, c) >= threshold for c in candidates)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── 文本规范化 ────────────────────────────────────────────────────────────────

_TRAILING_ID_PATTERNS = [
    re.compile(r"\s*#\s*[0-9a-fA-FxX]+\s*$"),
    re.compile(r"\s*#\s*\d+\s*$"),
    re.compile(r"\s*-\s*\d+\s*$"),
    re.compile(r"\s*:\s*\d+\s*$"),
    re.compile(r"\s*\(\s*\d+\s*\)\s*$"),
    re.compile(r"\s*\[\s*\d+\s*\]\s*$"),
    re.compile(r"\s*/\s*\d+\s*$"),
    re.compile(r"\s+No\.?\s*\d+\s*$", re.I),
    re.compile(r"\s+nr\.?\s*\d+\s*$", re.I),
    re.compile(r"\s+\d{1,12}\s*$"),
]


def strip_trailing_number_suffix(raw: str) -> str:
    s = unicodedata.normalize("NFKC", (raw or "").strip())
    if not s:
        return ""
    changed, guard = True, 0
    while changed and guard < 20:
        guard += 1
        changed = False
        for pat in _TRAILING_ID_PATTERNS:
            ns = pat.sub("", s)
            if ns != s:
                s = ns.strip()
                changed = True
                break
    return re.sub(r"\s+", " ", s).strip()


def normalize_symbol(sym: str) -> str:
    return unicodedata.normalize("NFKC", (sym or "").strip()).casefold()


def normalize_name_core(raw: str) -> str:
    s = unicodedata.normalize("NFKC", (raw or "").strip())
    s = strip_trailing_number_suffix(s)
    return re.sub(r"\s+", " ", s).strip().casefold()


# ── 长度桶索引（模糊比较前置过滤）────────────────────────────────────────────

class _LenIndex:
    """
    按字符串长度分桶，快速过滤模糊候选。

    对 fuzz.ratio(a, b) = 2M/(len_a+len_b) >= threshold/100，必要条件：
      len_b ∈ [len_a * t/(200-t),  len_a * (200-t)/t]
    其中 t = threshold（0–100）。
    """

    def __init__(self, keys: List[str]) -> None:
        buckets: Dict[int, List[str]] = defaultdict(list)
        for k in keys:
            buckets[len(k)].append(k)
        self._buckets = dict(buckets)
        self._sorted_lens: List[int] = sorted(buckets)

    def candidates(self, k: str, threshold: float) -> List[str]:
        la = len(k)
        if la == 0:
            return []
        factor = (200.0 - threshold) / threshold  # (200-t)/t
        lb_min = max(1, math.ceil(la / factor))
        lb_max = int(la * factor)
        out: List[str] = []
        for lb in self._sorted_lens:
            if lb < lb_min:
                continue
            if lb > lb_max:
                break
            out.extend(self._buckets[lb])
        return out


# ── SQL ───────────────────────────────────────────────────────────────────────

def _sql_contract_one_row_per_contract(tbl: str) -> str:
    # 与 dedup_analysis.common.SQL_STREAM_FULL 一致：双 URI 非空后再谈 name/symbol
    return f"""
        SELECT DISTINCT ON (lower(contract_address))
            coalesce(name, ''),
            coalesce(symbol, ''),
            COUNT(*) OVER (PARTITION BY lower(trim(contract_address)))::bigint AS nft_n
        FROM {tbl}
        WHERE token_uri IS NOT NULL
          AND image_uri IS NOT NULL
          AND name IS NOT NULL AND trim(name) <> ''
          AND symbol IS NOT NULL AND trim(symbol) <> ''
        ORDER BY lower(contract_address), id DESC
    """


# ── 数据库读取（每链只扫一次）────────────────────────────────────────────────

def stream_chain_key_counters(
    conn: Any,
    chain: str,
    batch: int,
) -> Tuple[int, int, Counter, Counter, Counter, Counter]:
    """
    流式扫描一链，返回：
      (合约数, NFT 总行数,
       sym_cc: key→合约数, sym_nc: key→NFT 数,
       name_cc: key→合约数, name_nc: key→NFT 数)
    """
    tbl = chain_to_table(chain)
    sql = _sql_contract_one_row_per_contract(tbl)
    sym_cc: Counter = Counter()
    sym_nc: Counter = Counter()
    name_cc: Counter = Counter()
    name_nc: Counter = Counter()
    n_contracts = 0
    n_nfts = 0
    with conn.cursor(name=f"ns_dedup_{chain.replace('-', '_')}") as cur:
        cur.itersize = max(1000, batch)
        cur.execute(sql)
        while True:
            chunk = cur.fetchmany(batch)
            if not chunk:
                break
            for raw_name, raw_sym, nft_n in chunk:
                sk = normalize_symbol(raw_sym)
                nc = normalize_name_core(raw_name)
                if not sk or not nc:
                    continue
                nn = int(nft_n) if nft_n else 0
                sym_cc[sk] += 1
                sym_nc[sk] += nn
                name_cc[nc] += 1
                name_nc[nc] += nn
                n_contracts += 1
                n_nfts += nn
    conn.commit()
    logger.info(
        "  %s: 合约 %d、NFT %d；唯一 symbol 键 %d、name 键 %d",
        chain, n_contracts, n_nfts, len(sym_cc), len(name_cc),
    )
    return n_contracts, n_nfts, sym_cc, sym_nc, name_cc, name_nc


# ── 并查集 ────────────────────────────────────────────────────────────────────

class UnionFind:
    def __init__(self, n: int) -> None:
        self.p = list(range(n))

    def find(self, a: int) -> int:
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _build_fuzzy_uf(keys: List[str], threshold: float) -> UnionFind:
    """
    用长度桶过滤无效配对后做模糊比较，构建并查集。
    长度约束将配对数从 O(n²) 降至接近 O(n)（当 threshold 较高时）。
    """
    k_n = len(keys)
    uf = UnionFind(k_n)
    if k_n < 2:
        return uf
    len_idx = _LenIndex(keys)
    key_to_idx: Dict[str, int] = {k: i for i, k in enumerate(keys)}
    for i, ka in enumerate(keys):
        for kb in len_idx.candidates(ka, threshold):
            j = key_to_idx.get(kb, -1)
            # j > i：每对只处理一次；ka != kb：排除同字符串自比
            if j > i and ka != kb and _fuzzy_ratio(ka, kb) >= threshold:
                uf.union(i, j)
    return uf


# ── 辅助 ──────────────────────────────────────────────────────────────────────

def _pct(part: int, total: int) -> float:
    return 100.0 * part / total if total else 0.0


# ── 单链内统计 ────────────────────────────────────────────────────────────────

def _intra_from_counters(
    chain: str,
    n_contracts: int,
    n_nfts: int,
    ctr_c: Counter,
    ctr_n: Counter,
    threshold: float,
    field_label: str,
) -> List[str]:
    """单链内 v1/v2/v3：ctr_c=key→合约数，ctr_n=key→NFT 行数之和。"""
    if n_contracts == 0:
        return [f"  [{chain.upper()}] 无数据", ""]

    keys = list(ctr_c.keys())
    k_n = len(keys)
    if k_n > 8000:
        logger.warning("[%s] %s 唯一 key %d，模糊计算可能较慢", chain, field_label, k_n)

    exact_dup_groups = sum(1 for c in ctr_c.values() if c >= 2)
    v1_keys: Set[str] = {k for k, c in ctr_c.items() if c >= 2}
    v1_contracts = sum(ctr_c[k] for k in v1_keys)
    v1_nfts = sum(ctr_n[k] for k in v1_keys)

    uf = _build_fuzzy_uf(keys, threshold)
    comp: Dict[int, List[int]] = defaultdict(list)
    for i in range(k_n):
        comp[uf.find(i)].append(i)

    multi_key_components = [ids for ids in comp.values() if len(ids) >= 2]
    fuzzy_cluster_n = len(multi_key_components)

    v2_keys: Set[str] = set()
    for ids in multi_key_components:
        v2_keys.update(keys[i] for i in ids)

    v2_only_keys = v2_keys - v1_keys  # 仅模糊、不含精确重复组
    v2_contracts = sum(ctr_c[k] for k in v2_keys)
    v2_nfts = sum(ctr_n[k] for k in v2_keys)
    v2_only_contracts = sum(ctr_c[k] for k in v2_only_keys)
    v2_only_nfts = sum(ctr_n[k] for k in v2_only_keys)

    v3_keys = v1_keys | v2_keys
    v3_contracts = sum(ctr_c[k] for k in v3_keys)
    v3_nfts = sum(ctr_n[k] for k in v3_keys)

    pc = lambda x: _pct(x, n_contracts)
    pn = lambda x: _pct(x, n_nfts)

    lines = [
        f"  [{chain.upper()}] 合约 {n_contracts:,} | NFT {n_nfts:,}（唯一 {field_label} 键 {k_n:,}）",
        f"    v1  同 {field_label} 精确重复（≥2 合约）: "
        f"组 {exact_dup_groups:,}  卷入合约 {v1_contracts:,} ({pc(v1_contracts):.2f}%)  "
        f"卷入 NFT {v1_nfts:,} ({pn(v1_nfts):.2f}%)",
        f"    v2  不同 {field_label} 但模糊≥阈值: "
        f"卷入合约 {v2_contracts:,} ({pc(v2_contracts):.2f}%)  卷入 NFT {v2_nfts:,} ({pn(v2_nfts):.2f}%)  "
        f"仅 v2（不含 v1）合约 {v2_only_contracts:,}、NFT {v2_only_nfts:,}",
        f"    v3  v1∪v2: 合约 {v3_contracts:,} ({pc(v3_contracts):.2f}%)  |  NFT {v3_nfts:,} ({pn(v3_nfts):.2f}%)",
        f"    （模糊并查集簇数 ≥2 键: {fuzzy_cluster_n:,}）",
    ]
    shown = 0
    for k, c in sorted(ctr_c.items(), key=lambda x: (-x[1], x[0])):
        if c < 2 or shown >= 5:
            break
        lines.append(f"      v1 示例: {field_label}={k[:48]!r} … {c} 合约 / {ctr_n.get(k, 0)} NFT")
        shown += 1
    lines.append("")
    return lines


# ── 跨链统计（纯内存，不重复扫库）────────────────────────────────────────────

def _merge_other(
    per_chain_ctr_c: Dict[str, Counter],
    exclude: str,
) -> Tuple[Set[str], _LenIndex]:
    """合并他链的 key，返回（精确集合，长度桶索引）。"""
    exact: Set[str] = set()
    all_keys: List[str] = []
    for ch, ctr in per_chain_ctr_c.items():
        if ch == exclude:
            continue
        for k in ctr:
            if k not in exact:
                exact.add(k)
                all_keys.append(k)
    return exact, _LenIndex(all_keys)


def _cross_from_counters(
    ctr_c: Counter,
    ctr_n: Counter,
    n_contracts: int,
    n_nfts: int,
    other_exact: Set[str],
    other_len_idx: _LenIndex,
    threshold: float,
) -> Tuple[int, int, int, int, int, int, int, int]:
    """
    基于内存 Counter 做跨链比较，无 DB IO。
    每个唯一 key 只做一次模糊查找，结果乘以 ctr_c[k] 得合约数。
    精确命中时短路，跳过模糊计算。
    """
    n_ex_c = n_ex_n = 0
    n_fz_c = n_fz_n = 0
    n_any_c = n_any_n = 0
    for k, cc in ctr_c.items():
        nn = ctr_n.get(k, 0)
        if k in other_exact:
            # 精确命中直接计入 v1 和 v3，跳过模糊
            n_ex_c += cc
            n_ex_n += nn
            n_any_c += cc
            n_any_n += nn
        else:
            cands = other_len_idx.candidates(k, threshold)
            if cands and _fuzzy_any(k, cands, threshold):
                n_fz_c += cc
                n_fz_n += nn
                n_any_c += cc
                n_any_n += nn
    return n_contracts, n_nfts, n_ex_c, n_ex_n, n_any_c, n_any_n, n_fz_c, n_fz_n


def _cross_block(
    chains: List[str],
    per_chain_n_contract: Dict[str, int],
    per_chain_n_nft: Dict[str, int],
    per_chain_ctr_c: Dict[str, Counter],
    per_chain_ctr_n: Dict[str, Counter],
    threshold: float,
    field_label: str,
    title: str,
) -> List[str]:
    lines = ["", "─" * 72, f"  {title}", "─" * 72]
    for target in chains:
        if per_chain_n_contract.get(target, 0) == 0:
            lines.append(f"  [{target.upper()}] 无数据")
            continue
        other_exact, other_len_idx = _merge_other(per_chain_ctr_c, target)
        nt, nt_nft, n_ex_c, n_ex_n, n_any_c, n_any_n, n_fz_c, n_fz_n = _cross_from_counters(
            per_chain_ctr_c[target],
            per_chain_ctr_n[target],
            per_chain_n_contract[target],
            per_chain_n_nft[target],
            other_exact,
            other_len_idx,
            threshold,
        )
        lines += [
            f"  [{target.upper()}] 本链 合约 {nt:,} | NFT {nt_nft:,}",
            f"    v1  他链已有相同 {field_label}（精确）: "
            f"合约 {n_ex_c:,} ({_pct(n_ex_c, nt):.2f}%)  |  NFT {n_ex_n:,} ({_pct(n_ex_n, nt_nft):.2f}%)",
            f"    v2  无精确命中、仅模糊命中他链: "
            f"合约 {n_fz_c:,} ({_pct(n_fz_c, nt):.2f}%)  |  NFT {n_fz_n:,} ({_pct(n_fz_n, nt_nft):.2f}%)",
            f"    v3  v1∪v2: 合约 {n_any_c:,} ({_pct(n_any_c, nt):.2f}%)  |  NFT {n_any_n:,} ({_pct(n_any_n, nt_nft):.2f}%)",
            "",
        ]
    return lines


# ── 报告组装 ──────────────────────────────────────────────────────────────────

def build_report(
    conn: Any,
    chains: List[str],
    threshold: float,
    batch: int,
) -> str:
    per_chain_sym_c: Dict[str, Counter] = {}
    per_chain_sym_n: Dict[str, Counter] = {}
    per_chain_name_c: Dict[str, Counter] = {}
    per_chain_name_n: Dict[str, Counter] = {}
    per_chain_n_contract: Dict[str, int] = {}
    per_chain_n_nft: Dict[str, int] = {}

    for c in chains:
        n_c, n_nft, scc, sn, ncc, nn = stream_chain_key_counters(conn, c, batch)
        per_chain_n_contract[c] = n_c
        per_chain_n_nft[c] = n_nft
        per_chain_sym_c[c] = scc
        per_chain_sym_n[c] = sn
        per_chain_name_c[c] = ncc
        per_chain_name_n[c] = nn

    lines = [
        "",
        "═" * 72,
        "  symbol / name 查重报告（流式扫描 + 内存聚合；两套独立）",
        "═" * 72,
        f"  模糊阈值: {threshold}  |  链: {', '.join(chains)}  |  fetchmany: {batch}",
        "",
        "═" * 72,
        "  A. 仅 symbol（symbol_key）",
        "═" * 72,
        "",
        "─" * 72,
        "  A1. 单链内（不与他链比较）",
        "─" * 72,
    ]
    for c in chains:
        lines.extend(_intra_from_counters(
            c,
            per_chain_n_contract.get(c, 0),
            per_chain_n_nft.get(c, 0),
            per_chain_sym_c.get(c, Counter()),
            per_chain_sym_n.get(c, Counter()),
            threshold,
            "symbol",
        ))
    lines.extend(_cross_block(
        chains,
        per_chain_n_contract,
        per_chain_n_nft,
        per_chain_sym_c,
        per_chain_sym_n,
        threshold,
        "symbol",
        "A2. 跨链（目标链仅与他链对比，不含本链）",
    ))

    lines += [
        "",
        "═" * 72,
        "  B. 仅 name（name_core，与 symbol 无关）",
        "═" * 72,
        "",
        "─" * 72,
        "  B1. 单链内（不与他链比较）",
        "─" * 72,
    ]
    for c in chains:
        lines.extend(_intra_from_counters(
            c,
            per_chain_n_contract.get(c, 0),
            per_chain_n_nft.get(c, 0),
            per_chain_name_c.get(c, Counter()),
            per_chain_name_n.get(c, Counter()),
            threshold,
            "name",
        ))
    lines.extend(_cross_block(
        chains,
        per_chain_n_contract,
        per_chain_n_nft,
        per_chain_name_c,
        per_chain_name_n,
        threshold,
        "name",
        "B2. 跨链（目标链仅与他链对比，不含本链）",
    ))

    lines += ["═" * 72, ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="symbol 与 name 查重（流式 + 按 key 聚合，低内存）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--chains", nargs="+", default=["ethereum", "base", "polygon", "solana"],
        metavar="CHAIN", help="链名，对应 nft_assets_{chain}",
    )
    parser.add_argument(
        "--threshold", type=float, default=88.0,
        help="symbol / name 字符串模糊阈值 0–100（两套共用）",
    )
    parser.add_argument(
        "--fetch", type=int, default=10_000, metavar="N",
        help="游标每次 fetchmany 行数",
    )
    parser.add_argument(
        "-o", "--output", default="",
        help="报告文件路径；默认不写文件仅打印",
    )
    args = parser.parse_args()

    chains = [c.lower().strip() for c in args.chains]
    t0 = time.monotonic()
    conn = get_conn()
    try:
        text = build_report(conn, chains, args.threshold, args.fetch)
    finally:
        conn.close()

    print(text)
    logger.info("耗时 %.1fs", time.monotonic() - t0)

    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
        logger.info("已写入 %s", args.output)


if __name__ == "__main__":
    main()
