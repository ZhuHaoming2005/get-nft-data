#!/usr/bin/env python3
"""
与 dedup_stats 配合：normalize_url、流式扫描、紧凑内存索引。

面向数十 GB 表：仅流式 FETCH + 按 URI key 聚合（不把整表载入内存）；
跨链「他链」索引可选：内存 Set（完整 URI 字符串）或临时 SQLite（仅存 sha256(key)）。
跨链对比只判断「他链是否出现过相同 URI」，不区分是否跨合约。
统计中的「涉及合约」用 8 字节指纹集合，避免存完整地址字符串。
"""

from __future__ import annotations

import gc
import hashlib
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, Optional, Set, Tuple, Union

# 保证从任意工作目录运行子包脚本时能找到项目根下的 dedup_stats
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dedup_stats import chain_to_table, get_conn, step1_create_function  # noqa: E402

logger = logging.getLogger(__name__)

__all__ = [
    "chain_to_table",
    "get_conn",
    "step1_create_function",
    "stream_rows",
    "count_distinct_contracts",
    "run_intra_chain_evm",
    "run_intra_chain_solana",
    "OtherChainsIndex",
    "OtherChainsIndexMemory",
    "OtherChainsIndexSQLite",
    "merge_table_into_other",
    "run_cross_chain_evm",
    "run_cross_chain_solana",
    "pct",
    "IntraChainReportEVM",
    "IntraChainReportSolana",
    "CrossChainReportEVM",
    "CrossChainReportSolana",
]

# 双字段均非空；DB 侧计算规范化 key
SQL_STREAM_FULL = """
    SELECT contract_address,
           trim(token_uri)              AS t_raw,
           trim(image_uri)              AS i_raw,
           normalize_url(token_uri)     AS t_norm,
           normalize_url(image_uri)     AS i_norm
    FROM   {table}
    WHERE  token_uri IS NOT NULL
      AND  image_uri IS NOT NULL
"""

SQL_COUNT_DISTINCT_CONTRACTS = """
    SELECT COUNT(DISTINCT contract_address)
    FROM   {table}
    WHERE  token_uri IS NOT NULL
      AND  image_uri IS NOT NULL
"""


def _contract_fp(addr: str) -> int:
    """8 字节指纹，用于统计「涉及合约数」时替代存完整地址字符串。"""
    return int.from_bytes(hashlib.blake2b(addr.encode(), digest_size=8).digest(), "big")


def count_distinct_contracts(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_COUNT_DISTINCT_CONTRACTS.format(table=table))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def stream_rows(
    conn,
    table: str,
    seg_size: int,
    *,
    gc_every: int = 0,
) -> Iterator[Tuple[str, str, str, Optional[str], Optional[str]]]:
    """
    流式返回 (contract, t_raw, i_raw, t_norm, i_norm)。
    gc_every: 每处理这么多批（fetchmany 次数）调用 gc.collect() 一次，利于长时间跑 tens of GB 时缓解碎片。
    """
    sql = SQL_STREAM_FULL.format(table=table)
    cur = conn.cursor(name=f"stream_{table}_{int(time.monotonic())}")
    cur.itersize = seg_size
    cur.execute(sql)
    n_batch = 0
    try:
        while True:
            rows = cur.fetchmany(seg_size)
            if not rows:
                break
            yield from rows
            n_batch += 1
            if gc_every and n_batch % gc_every == 0:
                gc.collect()
    finally:
        cur.close()


@dataclass
class KeyStats:
    """
    单 URI key：总行数 + 是否出现 ≥2 个不同合约（紧凑表示，不存全量合约 set）。
    """

    count: int = 0
    _c1: Optional[str] = None
    _c2: Optional[str] = None
    _many: bool = False

    def add(self, contract: str) -> None:
        self.count += 1
        if self._c1 is None:
            self._c1 = contract
        elif contract == self._c1:
            return
        elif self._c2 is None:
            self._c2 = contract
        elif contract == self._c2:
            return
        else:
            self._many = True

    def multi_contract(self) -> bool:
        return self._c2 is not None or self._many


class IntraChainKeyIndex:
    """单链内：严格 key 与 normalize_url key 的聚合。"""

    def __init__(self) -> None:
        self.st_t: Dict[str, KeyStats] = {}
        self.st_i: Dict[str, KeyStats] = {}
        self.nt_t: Dict[str, KeyStats] = {}
        self.nt_i: Dict[str, KeyStats] = {}

    def _get(self, d: Dict[str, KeyStats], k: str) -> KeyStats:
        if k not in d:
            d[k] = KeyStats()
        return d[k]

    def ingest_row(self, contract: str, t_raw: str, i_raw: str, t_norm: Optional[str], i_norm: Optional[str]) -> None:
        self._get(self.st_t, t_raw).add(contract)
        self._get(self.st_i, i_raw).add(contract)
        if t_norm:
            self._get(self.nt_t, t_norm).add(contract)
        if i_norm:
            self._get(self.nt_i, i_norm).add(contract)


def _v1_v2_v3(
    contract: str,
    t_raw: str,
    i_raw: str,
    t_norm: Optional[str],
    i_norm: Optional[str],
    st_t: Dict[str, KeyStats],
    st_i: Dict[str, KeyStats],
    nt_t: Dict[str, KeyStats],
    nt_i: Dict[str, KeyStats],
    *,
    use_norm: bool,
) -> Tuple[bool, bool, bool]:
    if use_norm:
        tt, ti = nt_t, nt_i
        tk, ik = t_norm, i_norm
    else:
        tt, ti = st_t, st_i
        tk, ik = t_raw, i_raw

    v1 = bool(tk and tk in tt and tt[tk].count >= 2)
    if v1:
        return True, False, True

    v2 = bool(ik and ik in ti and ti[ik].count >= 2)
    return False, v2, v2


def _v1_v2_v3_cross_contract(
    contract: str,
    t_raw: str,
    i_raw: str,
    t_norm: Optional[str],
    i_norm: Optional[str],
    st_t: Dict[str, KeyStats],
    st_i: Dict[str, KeyStats],
    nt_t: Dict[str, KeyStats],
    nt_i: Dict[str, KeyStats],
    *,
    use_norm: bool,
) -> Tuple[bool, bool, bool]:
    if use_norm:
        tt, ti = nt_t, nt_i
        tk, ik = t_norm, i_norm
    else:
        tt, ti = st_t, st_i
        tk, ik = t_raw, i_raw

    v1 = bool(tk and tk in tt and tt[tk].multi_contract())
    if v1:
        return True, False, True

    v2 = bool(ik and ik in ti and ti[ik].multi_contract())
    return False, v2, v2


@dataclass
class DimCounts:
    nfts: int = 0
    contracts: Set[int] = field(default_factory=set)  # contract 指纹，非原始地址


@dataclass
class IntraChainReportEVM:
    total_nfts: int = 0
    all_contracts: int = 0

    strict_any: Dict[str, DimCounts] = field(default_factory=dict)
    strict_cross: Dict[str, DimCounts] = field(default_factory=dict)
    norm_any: Dict[str, DimCounts] = field(default_factory=dict)
    norm_cross: Dict[str, DimCounts] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for d in (self.strict_any, self.strict_cross, self.norm_any, self.norm_cross):
            for k in ("v1", "v2", "v3"):
                if k not in d:
                    d[k] = DimCounts()


@dataclass
class IntraChainReportSolana:
    total_nfts: int = 0
    all_contracts: int = 0
    strict_any: Dict[str, DimCounts] = field(default_factory=dict)
    norm_any: Dict[str, DimCounts] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for d in (self.strict_any, self.norm_any):
            for k in ("v1", "v2", "v3"):
                if k not in d:
                    d[k] = DimCounts()


def _bump(dim: DimCounts, contract: str, hit: bool) -> None:
    if hit:
        dim.nfts += 1
        dim.contracts.add(_contract_fp(contract))


def run_intra_chain_evm(conn, table: str, seg_size: int, *, gc_every: int = 0) -> Tuple[IntraChainReportEVM, None]:
    idx = IntraChainKeyIndex()
    total = 0

    for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, table, seg_size, gc_every=gc_every):
        total += 1
        idx.ingest_row(contract, t_raw, i_raw, t_norm, i_norm)

    all_c = count_distinct_contracts(conn, table)
    rep = IntraChainReportEVM(total_nfts=total, all_contracts=all_c)

    for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, table, seg_size, gc_every=gc_every):
        v1, v2, v3 = _v1_v2_v3(
            contract, t_raw, i_raw, t_norm, i_norm,
            idx.st_t, idx.st_i, idx.nt_t, idx.nt_i, use_norm=False,
        )
        _bump(rep.strict_any["v1"], contract, v1)
        _bump(rep.strict_any["v2"], contract, v2)
        _bump(rep.strict_any["v3"], contract, v3)

        v1, v2, v3 = _v1_v2_v3_cross_contract(
            contract, t_raw, i_raw, t_norm, i_norm,
            idx.st_t, idx.st_i, idx.nt_t, idx.nt_i, use_norm=False,
        )
        _bump(rep.strict_cross["v1"], contract, v1)
        _bump(rep.strict_cross["v2"], contract, v2)
        _bump(rep.strict_cross["v3"], contract, v3)

        v1, v2, v3 = _v1_v2_v3(
            contract, t_raw, i_raw, t_norm, i_norm,
            idx.st_t, idx.st_i, idx.nt_t, idx.nt_i, use_norm=True,
        )
        _bump(rep.norm_any["v1"], contract, v1)
        _bump(rep.norm_any["v2"], contract, v2)
        _bump(rep.norm_any["v3"], contract, v3)

        v1, v2, v3 = _v1_v2_v3_cross_contract(
            contract, t_raw, i_raw, t_norm, i_norm,
            idx.st_t, idx.st_i, idx.nt_t, idx.nt_i, use_norm=True,
        )
        _bump(rep.norm_cross["v1"], contract, v1)
        _bump(rep.norm_cross["v2"], contract, v2)
        _bump(rep.norm_cross["v3"], contract, v3)

    return rep, None


def run_intra_chain_solana(conn, table: str, seg_size: int, *, gc_every: int = 0) -> Tuple[IntraChainReportSolana, None]:
    idx = IntraChainKeyIndex()
    total = 0

    for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, table, seg_size, gc_every=gc_every):
        total += 1
        idx.ingest_row(contract, t_raw, i_raw, t_norm, i_norm)

    all_c = count_distinct_contracts(conn, table)
    rep = IntraChainReportSolana(total_nfts=total, all_contracts=all_c)

    for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, table, seg_size, gc_every=gc_every):
        v1, v2, v3 = _v1_v2_v3(
            contract, t_raw, i_raw, t_norm, i_norm,
            idx.st_t, idx.st_i, idx.nt_t, idx.nt_i, use_norm=False,
        )
        _bump(rep.strict_any["v1"], contract, v1)
        _bump(rep.strict_any["v2"], contract, v2)
        _bump(rep.strict_any["v3"], contract, v3)

        v1, v2, v3 = _v1_v2_v3(
            contract, t_raw, i_raw, t_norm, i_norm,
            idx.st_t, idx.st_i, idx.nt_t, idx.nt_i, use_norm=True,
        )
        _bump(rep.norm_any["v1"], contract, v1)
        _bump(rep.norm_any["v2"], contract, v2)
        _bump(rep.norm_any["v3"], contract, v3)

    return rep, None


# ── 跨链：他链是否出现过相同 URI（仅命中 / 未命中；不讨论跨合约）──


def _v1_v2_v3_from_hits(t_hit: bool, i_hit: bool) -> Tuple[bool, bool, bool]:
    if t_hit:
        return True, False, True
    if i_hit:
        return False, True, True
    return False, False, False


@dataclass
class OtherChainsIndexMemory:
    """他链 URI 全集（trim 后严格串 / normalize 结果）；内存占用随唯一 URI 数增长。"""

    st_t: Set[str] = field(default_factory=set)
    st_i: Set[str] = field(default_factory=set)
    nt_t: Set[str] = field(default_factory=set)
    nt_i: Set[str] = field(default_factory=set)

    def contains(self, tbl: str, key: Optional[str]) -> bool:
        if not key:
            return False
        return key in getattr(self, tbl)


class OtherChainsIndexSQLite:
    """他链 URI 仅存 sha256(key)，INSERT OR IGNORE；磁盘换内存。"""

    _TABLES = ("st_t", "st_i", "nt_t", "nt_i")

    def __init__(self, path: str, *, cache_kb: int = 2048, commit_every: int = 80_000) -> None:
        self.path = path
        self._commit_every = max(1, commit_every)
        self._ops = 0
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=FILE")
        self._conn.execute("PRAGMA mmap_size=0")
        self._conn.execute(f"PRAGMA cache_size={-max(64, cache_kb)}")
        for name in self._TABLES:
            self._conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {name} (
                    h BLOB PRIMARY KEY
                )
                """
            )
        self._conn.commit()

    @staticmethod
    def key_hash(key: str) -> bytes:
        return hashlib.sha256(key.encode("utf-8")).digest()

    def _maybe_commit(self) -> None:
        self._ops += 1
        if self._ops >= self._commit_every:
            self._conn.commit()
            self._ops = 0

    def _merge_key(self, tbl: str, key: str) -> None:
        h = self.key_hash(key)
        self._conn.execute(f"INSERT OR IGNORE INTO {tbl} (h) VALUES (?)", (h,))
        self._maybe_commit()

    def contains(self, tbl: str, key: Optional[str]) -> bool:
        if not key:
            return False
        h = self.key_hash(key)
        cur = self._conn.cursor()
        cur.execute(f"SELECT 1 FROM {tbl} WHERE h = ? LIMIT 1", (h,))
        return cur.fetchone() is not None

    def commit(self) -> None:
        self._conn.commit()
        self._ops = 0

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.commit()
            except Exception:
                pass
            self._conn.close()
            self._conn = None


def merge_table_into_other(
    conn,
    table: str,
    seg_size: int,
    out: Union[OtherChainsIndexMemory, OtherChainsIndexSQLite],
    *,
    gc_every: int = 0,
) -> None:
    if isinstance(out, OtherChainsIndexMemory):
        for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, table, seg_size, gc_every=gc_every):
            out.st_t.add(t_raw)
            out.st_i.add(i_raw)
            if t_norm:
                out.nt_t.add(t_norm)
            if i_norm:
                out.nt_i.add(i_norm)
        return

    if isinstance(out, OtherChainsIndexSQLite):
        for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, table, seg_size, gc_every=gc_every):
            out._merge_key("st_t", t_raw)
            out._merge_key("st_i", i_raw)
            if t_norm:
                out._merge_key("nt_t", t_norm)
            if i_norm:
                out._merge_key("nt_i", i_norm)
        out.commit()
        return

    raise TypeError(f"不支持的索引类型: {type(out)!r}")


@dataclass
class CrossChainReportEVM:
    chain: str
    total_nfts: int = 0
    all_contracts: int = 0
    strict: Dict[str, DimCounts] = field(default_factory=dict)
    norm: Dict[str, DimCounts] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for d in (self.strict, self.norm):
            for k in ("v1", "v2", "v3"):
                if k not in d:
                    d[k] = DimCounts()


@dataclass
class CrossChainReportSolana:
    chain: str
    total_nfts: int = 0
    all_contracts: int = 0
    strict: Dict[str, DimCounts] = field(default_factory=dict)
    norm: Dict[str, DimCounts] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for d in (self.strict, self.norm):
            for k in ("v1", "v2", "v3"):
                if k not in d:
                    d[k] = DimCounts()


OtherChainsIndex = Union[OtherChainsIndexMemory, OtherChainsIndexSQLite]


def run_cross_chain_evm(
    conn,
    target_table: str,
    other_index: OtherChainsIndex,
    seg_size: int,
    chain_name: str,
    *,
    gc_every: int = 0,
) -> CrossChainReportEVM:
    total = 0
    rep = CrossChainReportEVM(chain=chain_name)

    for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, target_table, seg_size, gc_every=gc_every):
        total += 1
        t_hit_s = other_index.contains("st_t", t_raw)
        i_hit_s = other_index.contains("st_i", i_raw)
        v1, v2, v3 = _v1_v2_v3_from_hits(t_hit_s, i_hit_s)
        _bump(rep.strict["v1"], contract, v1)
        _bump(rep.strict["v2"], contract, v2)
        _bump(rep.strict["v3"], contract, v3)

        t_hit_n = other_index.contains("nt_t", t_norm)
        i_hit_n = other_index.contains("nt_i", i_norm)
        v1, v2, v3 = _v1_v2_v3_from_hits(t_hit_n, i_hit_n)
        _bump(rep.norm["v1"], contract, v1)
        _bump(rep.norm["v2"], contract, v2)
        _bump(rep.norm["v3"], contract, v3)

    rep.total_nfts = total
    rep.all_contracts = count_distinct_contracts(conn, target_table)
    return rep


def run_cross_chain_solana(
    conn,
    target_table: str,
    other_index: OtherChainsIndex,
    seg_size: int,
    chain_name: str,
    *,
    gc_every: int = 0,
) -> CrossChainReportSolana:
    total = 0
    rep = CrossChainReportSolana(chain=chain_name)

    for contract, t_raw, i_raw, t_norm, i_norm in stream_rows(conn, target_table, seg_size, gc_every=gc_every):
        total += 1
        t_hit = other_index.contains("st_t", t_raw)
        i_hit = other_index.contains("st_i", i_raw)
        v1, v2, v3 = _v1_v2_v3_from_hits(t_hit, i_hit)
        _bump(rep.strict["v1"], contract, v1)
        _bump(rep.strict["v2"], contract, v2)
        _bump(rep.strict["v3"], contract, v3)

        t_hit = other_index.contains("nt_t", t_norm)
        i_hit = other_index.contains("nt_i", i_norm)
        v1, v2, v3 = _v1_v2_v3_from_hits(t_hit, i_hit)
        _bump(rep.norm["v1"], contract, v1)
        _bump(rep.norm["v2"], contract, v2)
        _bump(rep.norm["v3"], contract, v3)

    rep.total_nfts = total
    rep.all_contracts = count_distinct_contracts(conn, target_table)
    return rep


def pct(part: int, total: int) -> str:
    if total == 0:
        return "  N/A  "
    return f"{part / total * 100:6.2f}%"
