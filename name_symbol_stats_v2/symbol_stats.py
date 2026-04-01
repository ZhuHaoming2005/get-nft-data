from __future__ import annotations

import logging
from typing import Sequence

from psycopg2.extras import execute_values

from .progress import ProgressPrinter
from .report import DuplicateGroupStats, SummaryRow, summarize_groups

logger = logging.getLogger(__name__)


def _load_chain_totals(conn, run_label: str, chains: Sequence[str]) -> dict[str, tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chain, count(*)::int, coalesce(sum(nft_count), 0)::bigint
            FROM nsv2_contract_identity
            WHERE run_label = %s AND chain = ANY(%s)
            GROUP BY chain
            """,
            (run_label, list(chains)),
        )
        rows = {chain: (contract_count, nft_count) for chain, contract_count, nft_count in cur.fetchall()}
    return {chain: rows.get(chain, (0, 0)) for chain in chains}


def _rebuild_symbol_rollup(conn, run_label: str, chains: Sequence[str]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM nsv2_symbol_rollup
            WHERE run_label = %s AND chain = ANY(%s)
            """,
            (run_label, list(chains)),
        )
        cur.execute(
            """
            INSERT INTO nsv2_symbol_rollup (
                run_label, chain, symbol_norm, contract_count, nft_count
            )
            SELECT run_label,
                   chain,
                   symbol_norm,
                   count(*)::bigint AS contract_count,
                   coalesce(sum(nft_count), 0)::bigint AS nft_count
            FROM nsv2_contract_identity
            WHERE run_label = %s
              AND chain = ANY(%s)
              AND symbol_norm <> ''
            GROUP BY run_label, chain, symbol_norm
            """,
            (run_label, list(chains)),
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def _fetch_groups(conn, run_label: str, *, scope: str, primary_chain: str, secondary_chain: str = '') -> list[DuplicateGroupStats]:
    if scope == 'intra_chain':
        sql = """
            SELECT symbol_norm,
                   contract_count,
                   nft_count,
                   contract_count,
                   nft_count
            FROM nsv2_symbol_rollup
            WHERE run_label = %s
              AND chain = %s
              AND contract_count >= 2
            ORDER BY nft_count DESC, symbol_norm
        """
        params = (run_label, primary_chain)
    elif scope == 'cross_chain_summary':
        sql = """
            WITH matching_keys AS (
                SELECT symbol_norm
                FROM nsv2_symbol_rollup
                WHERE run_label = %s
                GROUP BY symbol_norm
                HAVING count(DISTINCT chain) >= 2
                   AND bool_or(chain = %s)
            )
            SELECT symbol_norm,
                   coalesce(sum(contract_count) FILTER (WHERE chain = %s), 0)::bigint AS primary_contract_count,
                   coalesce(sum(nft_count) FILTER (WHERE chain = %s), 0)::bigint AS primary_nft_count,
                   coalesce(sum(contract_count), 0)::bigint AS total_contract_count,
                   coalesce(sum(nft_count), 0)::bigint AS total_nft_count
            FROM nsv2_symbol_rollup
            WHERE run_label = %s
              AND symbol_norm IN (SELECT symbol_norm FROM matching_keys)
            GROUP BY symbol_norm
            HAVING coalesce(sum(contract_count) FILTER (WHERE chain = %s), 0) > 0
            ORDER BY primary_nft_count DESC, symbol_norm
        """
        params = (run_label, primary_chain, primary_chain, primary_chain, run_label, primary_chain)
    else:
        sql = """
            SELECT symbol_norm,
                   coalesce(sum(contract_count) FILTER (WHERE chain = %s), 0)::bigint AS primary_contract_count,
                   coalesce(sum(nft_count) FILTER (WHERE chain = %s), 0)::bigint AS primary_nft_count,
                   coalesce(sum(contract_count), 0)::bigint AS total_contract_count,
                   coalesce(sum(nft_count), 0)::bigint AS total_nft_count
            FROM nsv2_symbol_rollup
            WHERE run_label = %s
              AND chain = ANY(%s)
            GROUP BY symbol_norm
            HAVING coalesce(sum(contract_count) FILTER (WHERE chain = %s), 0) > 0
               AND coalesce(sum(contract_count) FILTER (WHERE chain = %s), 0) > 0
            ORDER BY primary_nft_count DESC, symbol_norm
        """
        params = (primary_chain, primary_chain, run_label, [primary_chain, secondary_chain], primary_chain, secondary_chain)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            DuplicateGroupStats(
                group_key=symbol_norm,
                primary_contract_count=int(primary_contract_count),
                primary_nft_count=int(primary_nft_count),
                total_contract_count=int(total_contract_count),
                total_nft_count=int(total_nft_count),
                node_count=int(total_contract_count),
                sample_value=symbol_norm,
            )
            for symbol_norm, primary_contract_count, primary_nft_count, total_contract_count, total_nft_count in cur.fetchall()
        ]


def _delete_outputs(conn, run_label: str, chains: Sequence[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM nsv2_symbol_duplicate_groups
            WHERE run_label = %s
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (run_label, list(chains), list(chains)),
        )
        cur.execute(
            """
            DELETE FROM nsv2_duplicate_summary
            WHERE run_label = %s
              AND field_name = 'symbol'
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (run_label, list(chains), list(chains)),
        )
    conn.commit()


def _insert_group_rows(conn, run_label: str, field_name: str, scope: str, primary_chain: str, secondary_chain: str, threshold: float | None, groups: Sequence[DuplicateGroupStats]) -> None:
    if not groups:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO nsv2_symbol_duplicate_groups (
                run_label, field_name, scope, primary_chain, secondary_chain, threshold,
                group_key, sample_value, primary_contract_count, primary_nft_count,
                total_contract_count, total_nft_count, node_count
            ) VALUES %s
            """,
            [
                (
                    run_label,
                    field_name,
                    scope,
                    primary_chain,
                    secondary_chain,
                    -1.0 if threshold is None else threshold,
                    group.group_key,
                    group.sample_value,
                    group.primary_contract_count,
                    group.primary_nft_count,
                    group.total_contract_count,
                    group.total_nft_count,
                    group.node_count,
                )
                for group in groups
            ],
            page_size=1000,
        )


def _insert_summary_rows(conn, rows: Sequence[SummaryRow]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO nsv2_duplicate_summary (
                run_label, field_name, scope, primary_chain, secondary_chain, threshold,
                total_contracts, total_nfts, group_count, duplicate_contract_count, duplicate_nft_count,
                duplicate_contract_ratio, duplicate_nft_ratio, group_size_ge_2_count, group_size_gt_2_count
            ) VALUES %s
            """,
            [
                (
                    row.run_label,
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
            ],
            page_size=1000,
        )


def run_symbol_stats(conn, run_label: str, chains: Sequence[str]) -> list[SummaryRow]:
    _delete_outputs(conn, run_label, chains)
    chain_totals = _load_chain_totals(conn, run_label, chains)
    rollup_rows = _rebuild_symbol_rollup(conn, run_label, chains)
    summary_rows: list[SummaryRow] = []
    total_steps = len(chains) * (len(chains) + 1) + 1
    tracker = ProgressPrinter('symbol-stats', total_steps, 'steps', logger)
    completed_steps = 1
    tracker.update(completed_steps, extra=f'rollup_rows={rollup_rows}')

    for primary_chain in chains:
        total_contracts, total_nfts = chain_totals[primary_chain]

        groups = _fetch_groups(conn, run_label, scope='intra_chain', primary_chain=primary_chain)
        _insert_group_rows(conn, run_label, 'symbol', 'intra_chain', primary_chain, '', None, groups)
        summary_rows.append(
            summarize_groups(
                run_label=run_label,
                field_name='symbol',
                scope='intra_chain',
                primary_chain=primary_chain,
                secondary_chain='',
                threshold=None,
                total_contracts=total_contracts,
                total_nfts=total_nfts,
                groups=groups,
            )
        )
        completed_steps += 1
        tracker.update(completed_steps, extra=f'{primary_chain} intra groups={len(groups)}')

        groups = _fetch_groups(conn, run_label, scope='cross_chain_summary', primary_chain=primary_chain)
        _insert_group_rows(conn, run_label, 'symbol', 'cross_chain_summary', primary_chain, '', None, groups)
        summary_rows.append(
            summarize_groups(
                run_label=run_label,
                field_name='symbol',
                scope='cross_chain_summary',
                primary_chain=primary_chain,
                secondary_chain='',
                threshold=None,
                total_contracts=total_contracts,
                total_nfts=total_nfts,
                groups=groups,
            )
        )
        completed_steps += 1
        tracker.update(completed_steps, extra=f'{primary_chain} cross groups={len(groups)}')

        for secondary_chain in chains:
            if secondary_chain == primary_chain:
                continue
            groups = _fetch_groups(conn, run_label, scope='chain_matrix', primary_chain=primary_chain, secondary_chain=secondary_chain)
            _insert_group_rows(conn, run_label, 'symbol', 'chain_matrix', primary_chain, secondary_chain, None, groups)
            summary_rows.append(
                summarize_groups(
                    run_label=run_label,
                    field_name='symbol',
                    scope='chain_matrix',
                    primary_chain=primary_chain,
                    secondary_chain=secondary_chain,
                    threshold=None,
                    total_contracts=total_contracts,
                    total_nfts=total_nfts,
                    groups=groups,
                )
            )
            completed_steps += 1
            tracker.update(completed_steps, extra=f'{primary_chain}->{secondary_chain} groups={len(groups)}')

    _insert_summary_rows(conn, summary_rows)
    conn.commit()
    tracker.close(extra=f'rollup_rows={rollup_rows} summary_rows={len(summary_rows)}')
    logger.info('wrote %d symbol summary rows for run %s', len(summary_rows), run_label)
    return summary_rows
