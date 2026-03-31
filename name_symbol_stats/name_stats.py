from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from psycopg2.extras import execute_values

from dedup_stats import get_conn

from .blocking import BlockingRecord, iter_candidate_pairs, partition_records
from .cluster import ScoredPair, cluster_pairs_by_threshold
from .report import DuplicateGroupStats, SummaryRow, summarize_groups

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parent / 'sql'

try:
    from rapidfuzz import fuzz as rapidfuzz_fuzz

    def score_name_similarity(left: str, right: str) -> float:
        return float(rapidfuzz_fuzz.ratio(left, right))
except ImportError:  # pragma: no cover
    import difflib

    def score_name_similarity(left: str, right: str) -> float:
        return difflib.SequenceMatcher(None, left, right).ratio() * 100.0


def _load_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding='utf-8')


def ensure_name_schema(conn) -> None:
    with conn.cursor() as cur:
        for filename in ('03_name_blocks.sql', '04_result_tables.sql'):
            cur.execute(_load_sql(filename))
    conn.commit()


class CandidatePair:
    __slots__ = (
        'scope', 'primary_chain', 'secondary_chain',
        'left_chain', 'left_contract_address', 'left_name_norm', 'left_nft_count',
        'right_chain', 'right_contract_address', 'right_name_norm', 'right_nft_count',
        'trigram_score', 'similarity_score'
    )

    def __init__(self, *, scope: str, primary_chain: str, secondary_chain: str, left_chain: str, left_contract_address: str, left_name_norm: str, left_nft_count: int, right_chain: str, right_contract_address: str, right_name_norm: str, right_nft_count: int, trigram_score: float, similarity_score: float) -> None:
        self.scope = scope
        self.primary_chain = primary_chain
        self.secondary_chain = secondary_chain
        self.left_chain = left_chain
        self.left_contract_address = left_contract_address
        self.left_name_norm = left_name_norm
        self.left_nft_count = left_nft_count
        self.right_chain = right_chain
        self.right_contract_address = right_contract_address
        self.right_name_norm = right_name_norm
        self.right_nft_count = right_nft_count
        self.trigram_score = trigram_score
        self.similarity_score = similarity_score


def _score_candidates(items: Sequence[tuple[BlockingRecord, BlockingRecord, float]], *, workers: int, scope: str, primary_chain: str, secondary_chain: str) -> list[CandidatePair]:
    def build(item: tuple[BlockingRecord, BlockingRecord, float]) -> CandidatePair:
        left, right, trigram_score = item
        return CandidatePair(
            scope=scope,
            primary_chain=primary_chain,
            secondary_chain=secondary_chain,
            left_chain=left.chain,
            left_contract_address=left.contract_address,
            left_name_norm=left.name_norm,
            left_nft_count=left.nft_count,
            right_chain=right.chain,
            right_contract_address=right.contract_address,
            right_name_norm=right.name_norm,
            right_nft_count=right.nft_count,
            trigram_score=trigram_score,
            similarity_score=score_name_similarity(left.name_norm, right.name_norm),
        )

    if workers <= 1 or len(items) < 64:
        return [build(item) for item in items]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(build, items))


def _stream_name_blocks(conn, chains: Sequence[str], *, fetch_size: int = 10000) -> Iterator[list[BlockingRecord]]:
    with conn.cursor(name='name_block_stream') as cur:
        cur.itersize = fetch_size
        cur.execute(
            """
            SELECT chain, contract_address, nft_count, name_norm, name_len, name_block_key
            FROM analysis_contract_identity
            WHERE chain = ANY(%s)
              AND name_norm <> ''
              AND name_block_key <> ''
            ORDER BY name_block_key, chain, contract_address
            """,
            (list(chains),),
        )
        current_key = None
        bucket: list[BlockingRecord] = []
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            for chain, contract_address, nft_count, name_norm, name_len, name_block_key in rows:
                if current_key is None:
                    current_key = name_block_key
                if name_block_key != current_key:
                    yield bucket
                    bucket = []
                    current_key = name_block_key
                bucket.append(
                    BlockingRecord(
                        chain=chain,
                        contract_address=contract_address,
                        nft_count=int(nft_count),
                        name_norm=name_norm,
                        name_len=int(name_len),
                        name_block_key=name_block_key,
                    )
                )
        if bucket:
            yield bucket


def _insert_candidate_pairs(conn, rows: Sequence[CandidatePair]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analysis_name_candidate_pairs (
                scope, primary_chain, secondary_chain, left_chain, left_contract_address, left_name_norm,
                left_nft_count, right_chain, right_contract_address, right_name_norm, right_nft_count,
                trigram_score, similarity_score
            ) VALUES %s
            """,
            [
                (
                    row.scope,
                    row.primary_chain,
                    row.secondary_chain,
                    row.left_chain,
                    row.left_contract_address,
                    row.left_name_norm,
                    row.left_nft_count,
                    row.right_chain,
                    row.right_contract_address,
                    row.right_name_norm,
                    row.right_nft_count,
                    row.trigram_score,
                    row.similarity_score,
                )
                for row in rows
            ],
            page_size=1000,
        )


def _delete_name_outputs(conn, chains: Sequence[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM analysis_name_candidate_pairs
            WHERE primary_chain = ANY(%s)
               OR secondary_chain = ANY(%s)
               OR left_chain = ANY(%s)
               OR right_chain = ANY(%s)
            """,
            (list(chains), list(chains), list(chains), list(chains)),
        )
        cur.execute('DELETE FROM analysis_name_duplicate_groups WHERE primary_chain = ANY(%s) OR secondary_chain = ANY(%s)', (list(chains), list(chains)))
        cur.execute(
            """
            DELETE FROM analysis_duplicate_summary
            WHERE field_name = 'name'
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (list(chains), list(chains)),
        )
    conn.commit()


def _generate_candidate_pairs(conn, *, chains: Sequence[str], max_block_size: int, trigram_cutoff: float, max_len_delta: int, workers: int) -> int:
    inserted = 0
    buffer: list[CandidatePair] = []
    read_conn = get_conn()
    try:
        for block_records in _stream_name_blocks(read_conn, chains):
            by_chain: dict[str, list[BlockingRecord]] = defaultdict(list)
            for partition in partition_records(block_records, max_block_size=max_block_size):
                by_chain.clear()
                for record in partition.records:
                    by_chain[record.chain].append(record)
                for primary_chain in chains:
                    primary_records = by_chain.get(primary_chain, [])
                    if len(primary_records) >= 2:
                        buffer.extend(
                            _score_candidates(
                                list(iter_candidate_pairs(primary_records, trigram_cutoff=trigram_cutoff, max_len_delta=max_len_delta)),
                                workers=workers,
                                scope='intra_chain',
                                primary_chain=primary_chain,
                                secondary_chain='',
                            )
                        )
                    other_records = [record for chain, records in by_chain.items() if chain != primary_chain for record in records]
                    if primary_records and other_records:
                        buffer.extend(
                            _score_candidates(
                                list(iter_candidate_pairs(primary_records, other_records, trigram_cutoff=trigram_cutoff, max_len_delta=max_len_delta)),
                                workers=workers,
                                scope='cross_chain_summary',
                                primary_chain=primary_chain,
                                secondary_chain='',
                            )
                        )
                    for secondary_chain in chains:
                        if secondary_chain == primary_chain:
                            continue
                        secondary_records = by_chain.get(secondary_chain, [])
                        if primary_records and secondary_records:
                            buffer.extend(
                                _score_candidates(
                                    list(iter_candidate_pairs(primary_records, secondary_records, trigram_cutoff=trigram_cutoff, max_len_delta=max_len_delta)),
                                    workers=workers,
                                    scope='chain_matrix',
                                    primary_chain=primary_chain,
                                    secondary_chain=secondary_chain,
                                )
                            )
                if len(buffer) >= 5000:
                    _insert_candidate_pairs(conn, buffer)
                    inserted += len(buffer)
                    buffer.clear()
                    conn.commit()
    finally:
        read_conn.close()

    if buffer:
        _insert_candidate_pairs(conn, buffer)
        inserted += len(buffer)
        conn.commit()
    return inserted


def _scope_rows(conn, *, scope: str, primary_chain: str, secondary_chain: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT left_chain, left_contract_address, left_name_norm, left_nft_count,
                   right_chain, right_contract_address, right_name_norm, right_nft_count, similarity_score
            FROM analysis_name_candidate_pairs
            WHERE scope = %s AND primary_chain = %s AND secondary_chain = %s
            """,
            (scope, primary_chain, secondary_chain),
        )
        return cur.fetchall()


def _cluster_rows(rows: Sequence[tuple], *, primary_chain: str, threshold: float) -> list[DuplicateGroupStats]:
    node_meta: dict[str, tuple[str, int, str]] = {}
    scored_pairs: list[ScoredPair] = []
    for left_chain, left_contract_address, left_name_norm, left_nft_count, right_chain, right_contract_address, right_name_norm, right_nft_count, similarity_score in rows:
        left_key = f'{left_chain}:{left_contract_address}'
        right_key = f'{right_chain}:{right_contract_address}'
        node_meta[left_key] = (left_chain, int(left_nft_count), left_name_norm)
        node_meta[right_key] = (right_chain, int(right_nft_count), right_name_norm)
        scored_pairs.append(ScoredPair(left_key, right_key, float(similarity_score)))

    groups: list[DuplicateGroupStats] = []
    for members in cluster_pairs_by_threshold(list(node_meta.keys()), scored_pairs, threshold=threshold):
        primary_members = [member for member in members if node_meta[member][0] == primary_chain]
        if not primary_members:
            continue
        digest = hashlib.sha1('|'.join(sorted(members)).encode('utf-8')).hexdigest()[:16]
        groups.append(
            DuplicateGroupStats(
                group_key=digest,
                primary_contract_count=len(primary_members),
                primary_nft_count=sum(node_meta[member][1] for member in primary_members),
                total_member_count=len(members),
                total_member_nft_count=sum(node_meta[member][1] for member in members),
                sample_value=min(node_meta[member][2] for member in members),
            )
        )
    return groups


def _load_chain_totals(conn, chains: Sequence[str]) -> dict[str, tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chain, count(*)::int AS contract_count, coalesce(sum(nft_count), 0)::bigint AS nft_count
            FROM analysis_contract_identity
            WHERE chain = ANY(%s)
            GROUP BY chain
            """,
            (list(chains),),
        )
        rows = {chain: (contract_count, nft_count) for chain, contract_count, nft_count in cur.fetchall()}
    return {chain: rows.get(chain, (0, 0)) for chain in chains}


def _insert_name_groups(conn, *, scope: str, primary_chain: str, secondary_chain: str, threshold: float, groups: Sequence[DuplicateGroupStats]) -> None:
    if not groups:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analysis_name_duplicate_groups (
                scope, primary_chain, secondary_chain, threshold, group_key, sample_value,
                primary_contract_count, primary_nft_count, total_member_count, total_member_nft_count
            ) VALUES %s
            """,
            [
                (
                    scope,
                    primary_chain,
                    secondary_chain,
                    threshold,
                    group.group_key,
                    group.sample_value,
                    group.primary_contract_count,
                    group.primary_nft_count,
                    group.total_member_count,
                    group.total_member_nft_count or group.primary_nft_count,
                )
                for group in groups
            ],
            page_size=1000,
        )


def _insert_summary_rows(conn, rows: Iterable[SummaryRow]) -> None:
    values = [
        (
            row.field_name,
            row.scope,
            row.primary_chain,
            row.secondary_chain,
            -1.0 if row.threshold is None else row.threshold,
            row.total_contracts,
            row.total_nfts,
            row.group_count,
            row.duplicate_contract_count,
            row.duplicate_nft_count,
            row.duplicate_contract_ratio,
            row.duplicate_nft_ratio,
            row.group_size_ge_2_count,
            row.group_size_gt_2_count,
        )
        for row in rows
    ]
    if not values:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analysis_duplicate_summary (
                field_name, scope, primary_chain, secondary_chain, threshold, total_contracts, total_nfts,
                group_count, duplicate_contract_count, duplicate_nft_count, duplicate_contract_ratio,
                duplicate_nft_ratio, group_size_ge_2_count, group_size_gt_2_count
            ) VALUES %s
            """,
            values,
            page_size=1000,
        )


def run_name_stats(conn, *, chains: Sequence[str], thresholds: Sequence[float], max_block_size: int, trigram_cutoff: float, max_len_delta: int, workers: int) -> list[SummaryRow]:
    ensure_name_schema(conn)
    _delete_name_outputs(conn, chains)
    totals = _load_chain_totals(conn, chains)
    inserted = _generate_candidate_pairs(
        conn,
        chains=chains,
        max_block_size=max_block_size,
        trigram_cutoff=trigram_cutoff,
        max_len_delta=max_len_delta,
        workers=workers,
    )
    logger.info('Inserted %d name candidate pairs', inserted)

    summary_rows: list[SummaryRow] = []
    for primary_chain in chains:
        total_contracts, total_nfts = totals[primary_chain]
        intra_rows = _scope_rows(conn, scope='intra_chain', primary_chain=primary_chain, secondary_chain='')
        cross_rows = _scope_rows(conn, scope='cross_chain_summary', primary_chain=primary_chain, secondary_chain='')
        matrix_rows = {secondary_chain: _scope_rows(conn, scope='chain_matrix', primary_chain=primary_chain, secondary_chain=secondary_chain) for secondary_chain in chains if secondary_chain != primary_chain}
        for threshold in thresholds:
            for scope, secondary_chain, rows in [('intra_chain', '', intra_rows), ('cross_chain_summary', '', cross_rows)]:
                groups = _cluster_rows(rows, primary_chain=primary_chain, threshold=threshold)
                _insert_name_groups(conn, scope=scope, primary_chain=primary_chain, secondary_chain=secondary_chain, threshold=threshold, groups=groups)
                summary_rows.append(
                    summarize_groups(
                        field_name='name',
                        scope=scope,
                        primary_chain=primary_chain,
                        secondary_chain=secondary_chain or None,
                        threshold=threshold,
                        total_contracts=total_contracts,
                        total_nfts=total_nfts,
                        groups=groups,
                    )
                )
            for secondary_chain, rows in matrix_rows.items():
                groups = _cluster_rows(rows, primary_chain=primary_chain, threshold=threshold)
                _insert_name_groups(conn, scope='chain_matrix', primary_chain=primary_chain, secondary_chain=secondary_chain, threshold=threshold, groups=groups)
                summary_rows.append(
                    summarize_groups(
                        field_name='name',
                        scope='chain_matrix',
                        primary_chain=primary_chain,
                        secondary_chain=secondary_chain,
                        threshold=threshold,
                        total_contracts=total_contracts,
                        total_nfts=total_nfts,
                        groups=groups,
                    )
                )
    _insert_summary_rows(conn, summary_rows)
    conn.commit()
    return summary_rows
