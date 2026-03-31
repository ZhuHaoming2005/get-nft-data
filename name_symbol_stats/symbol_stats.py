from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Sequence

from psycopg2.extras import execute_values

from dedup_stats import chain_to_table, get_conn

from .normalize import build_name_block_key, name_length_bucket, normalize_name, normalize_symbol
from .report import DuplicateGroupStats, SummaryRow, summarize_groups

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parent / 'sql'


def _load_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding='utf-8')


def ensure_symbol_schema(conn) -> None:
    with conn.cursor() as cur:
        for filename in ('01_contract_identity.sql', '02_symbol_stats.sql', '04_result_tables.sql'):
            cur.execute(_load_sql(filename))
    conn.commit()


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            """,
            (table_name, column_name),
        )
        return cur.fetchone() is not None


def _rollup_sql(table_name: str, *, has_name: bool, has_symbol: bool) -> str:
    name_expr = "coalesce(name, '')" if has_name else "''"
    symbol_expr = "coalesce(symbol, '')" if has_symbol else "''"
    name_present = "name IS NOT NULL AND trim(name) <> ''" if has_name else 'FALSE'
    symbol_present = "symbol IS NOT NULL AND trim(symbol) <> ''" if has_symbol else 'FALSE'
    name_variant = (
        "count(DISTINCT lower(trim(name))) FILTER (WHERE name IS NOT NULL AND trim(name) <> '')::int"
        if has_name else '0::int'
    )
    symbol_variant = (
        "count(DISTINCT lower(trim(symbol))) FILTER (WHERE symbol IS NOT NULL AND trim(symbol) <> '')::int"
        if has_symbol else '0::int'
    )
    return f"""
        WITH contract_rollup AS (
            SELECT lower(trim(contract_address)) AS contract_address,
                   count(*)::bigint AS nft_count,
                   {name_variant} AS name_variant_count,
                   {symbol_variant} AS symbol_variant_count
            FROM {table_name}
            WHERE contract_address IS NOT NULL AND trim(contract_address) <> ''
            GROUP BY 1
        ),
        ranked AS (
            SELECT lower(trim(contract_address)) AS contract_address,
                   {name_expr} AS raw_name,
                   {symbol_expr} AS raw_symbol,
                   row_number() OVER (
                       PARTITION BY lower(trim(contract_address))
                       ORDER BY CASE WHEN {name_present} THEN 0 ELSE 1 END,
                                CASE WHEN {symbol_present} THEN 0 ELSE 1 END,
                                id DESC
                   ) AS rn
            FROM {table_name}
            WHERE contract_address IS NOT NULL AND trim(contract_address) <> ''
        )
        SELECT ranked.contract_address, contract_rollup.nft_count, ranked.raw_name, ranked.raw_symbol,
               contract_rollup.name_variant_count, contract_rollup.symbol_variant_count
        FROM ranked
        JOIN contract_rollup USING (contract_address)
        WHERE ranked.rn = 1
        ORDER BY ranked.contract_address
    """


def _identity_row(chain: str, contract_address: str, nft_count: int, raw_name: str, raw_symbol: str, name_variant_count: int, symbol_variant_count: int) -> tuple[object, ...]:
    name_norm = normalize_name(raw_name)
    symbol_norm = normalize_symbol(raw_symbol)
    name_len = len(name_norm)
    return (
        chain,
        contract_address,
        int(nft_count),
        raw_name or '',
        raw_symbol or '',
        name_norm,
        symbol_norm,
        name_len,
        name_length_bucket(name_len),
        build_name_block_key(name_norm),
        int(name_variant_count),
        int(symbol_variant_count),
    )


def _insert_identity_batch(conn, rows: Sequence[tuple[object, ...]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analysis_contract_identity (
                chain, contract_address, nft_count, raw_name, raw_symbol, name_norm, symbol_norm,
                name_len, name_len_bucket, name_block_key, name_variant_count, symbol_variant_count
            ) VALUES %s
            ON CONFLICT (chain, contract_address) DO UPDATE SET
                nft_count = EXCLUDED.nft_count,
                raw_name = EXCLUDED.raw_name,
                raw_symbol = EXCLUDED.raw_symbol,
                name_norm = EXCLUDED.name_norm,
                symbol_norm = EXCLUDED.symbol_norm,
                name_len = EXCLUDED.name_len,
                name_len_bucket = EXCLUDED.name_len_bucket,
                name_block_key = EXCLUDED.name_block_key,
                name_variant_count = EXCLUDED.name_variant_count,
                symbol_variant_count = EXCLUDED.symbol_variant_count,
                updated_at = NOW()
            """,
            rows,
            page_size=1000,
        )


def build_contract_identity(conn, chains: Sequence[str], *, batch_size: int = 5000) -> dict[str, int]:
    ensure_symbol_schema(conn)
    counts: dict[str, int] = {}
    for chain in chains:
        table_name = chain_to_table(chain)
        sql = _rollup_sql(
            table_name,
            has_name=_table_has_column(conn, table_name, 'name'),
            has_symbol=_table_has_column(conn, table_name, 'symbol'),
        )
        with conn.cursor() as cur:
            cur.execute('DELETE FROM analysis_contract_identity WHERE chain = %s', (chain,))
        conn.commit()

        inserted = 0
        buffer: list[tuple[object, ...]] = []
        read_conn = get_conn()
        try:
            with read_conn.cursor(name=f'identity_{chain}') as cur:
                cur.itersize = batch_size
                cur.execute(sql)
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        buffer.append(_identity_row(chain, *row))
                    if len(buffer) >= batch_size:
                        _insert_identity_batch(conn, buffer)
                        inserted += len(buffer)
                        buffer.clear()
                        conn.commit()
        finally:
            read_conn.close()

        if buffer:
            _insert_identity_batch(conn, buffer)
            inserted += len(buffer)
            conn.commit()
        counts[chain] = inserted
    return counts


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


def _fetch_symbol_groups(conn, *, scope: str, primary_chain: str, secondary_chain: str = '') -> list[DuplicateGroupStats]:
    if scope == 'intra_chain':
        sql = """
            WITH shared_keys AS (
                SELECT symbol_norm
                FROM analysis_contract_identity
                WHERE chain = %s AND symbol_norm <> ''
                GROUP BY symbol_norm
                HAVING count(*) >= 2
            )
            SELECT symbol_norm,
                   count(*)::int,
                   coalesce(sum(nft_count), 0)::bigint,
                   count(*)::int,
                   coalesce(sum(nft_count), 0)::bigint
            FROM analysis_contract_identity
            WHERE chain = %s AND symbol_norm IN (SELECT symbol_norm FROM shared_keys)
            GROUP BY symbol_norm
            ORDER BY 3 DESC, 1
        """
        params = (primary_chain, primary_chain)
    elif scope == 'cross_chain_summary':
        sql = """
            WITH shared_keys AS (
                SELECT DISTINCT target.symbol_norm
                FROM analysis_contract_identity AS target
                JOIN analysis_contract_identity AS other
                  ON target.symbol_norm = other.symbol_norm
                 AND other.chain <> target.chain
                WHERE target.chain = %s AND target.symbol_norm <> ''
            )
            SELECT symbol_norm,
                   count(*) FILTER (WHERE chain = %s)::int,
                   coalesce(sum(nft_count) FILTER (WHERE chain = %s), 0)::bigint,
                   count(*)::int,
                   coalesce(sum(nft_count), 0)::bigint
            FROM analysis_contract_identity
            WHERE symbol_norm IN (SELECT symbol_norm FROM shared_keys)
            GROUP BY symbol_norm
            HAVING count(*) FILTER (WHERE chain = %s) > 0
            ORDER BY 3 DESC, 1
        """
        params = (primary_chain, primary_chain, primary_chain, primary_chain)
    else:
        sql = """
            WITH shared_keys AS (
                SELECT DISTINCT left_side.symbol_norm
                FROM analysis_contract_identity AS left_side
                JOIN analysis_contract_identity AS right_side
                  ON left_side.symbol_norm = right_side.symbol_norm
                 AND right_side.chain = %s
                WHERE left_side.chain = %s AND left_side.symbol_norm <> ''
            )
            SELECT symbol_norm,
                   count(*) FILTER (WHERE chain = %s)::int,
                   coalesce(sum(nft_count) FILTER (WHERE chain = %s), 0)::bigint,
                   count(*)::int,
                   coalesce(sum(nft_count), 0)::bigint
            FROM analysis_contract_identity
            WHERE chain = ANY(%s) AND symbol_norm IN (SELECT symbol_norm FROM shared_keys)
            GROUP BY symbol_norm
            HAVING count(*) FILTER (WHERE chain = %s) > 0
            ORDER BY 3 DESC, 1
        """
        params = (secondary_chain, primary_chain, primary_chain, primary_chain, [primary_chain, secondary_chain], primary_chain)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            DuplicateGroupStats(
                group_key=group_key,
                primary_contract_count=primary_contract_count,
                primary_nft_count=primary_nft_count,
                total_member_count=total_member_count,
                total_member_nft_count=total_member_nft_count,
                sample_value=group_key,
            )
            for group_key, primary_contract_count, primary_nft_count, total_member_count, total_member_nft_count in cur.fetchall()
        ]


def _delete_symbol_outputs(conn, chains: Sequence[str]) -> None:
    with conn.cursor() as cur:
        cur.execute('DELETE FROM analysis_symbol_duplicate_groups WHERE primary_chain = ANY(%s) OR secondary_chain = ANY(%s)', (list(chains), list(chains)))
        cur.execute(
            """
            DELETE FROM analysis_duplicate_summary
            WHERE field_name = 'symbol'
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (list(chains), list(chains)),
        )
    conn.commit()


def _insert_symbol_groups(conn, *, scope: str, primary_chain: str, secondary_chain: str, groups: Sequence[DuplicateGroupStats]) -> None:
    if not groups:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analysis_symbol_duplicate_groups (
                scope, primary_chain, secondary_chain, match_key, primary_contract_count,
                primary_nft_count, total_member_count, total_member_nft_count, sample_value
            ) VALUES %s
            """,
            [
                (
                    scope,
                    primary_chain,
                    secondary_chain,
                    group.group_key,
                    group.primary_contract_count,
                    group.primary_nft_count,
                    group.total_member_count,
                    group.total_member_nft_count or group.primary_nft_count,
                    group.sample_value,
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


def run_symbol_stats(conn, chains: Sequence[str]) -> list[SummaryRow]:
    ensure_symbol_schema(conn)
    _delete_symbol_outputs(conn, chains)
    totals = _load_chain_totals(conn, chains)
    summary_rows: list[SummaryRow] = []
    for chain in chains:
        total_contracts, total_nfts = totals[chain]
        for scope, secondary_chain in (('intra_chain', ''), ('cross_chain_summary', '')):
            groups = _fetch_symbol_groups(conn, scope=scope, primary_chain=chain, secondary_chain=secondary_chain)
            _insert_symbol_groups(conn, scope=scope, primary_chain=chain, secondary_chain=secondary_chain, groups=groups)
            summary_rows.append(
                summarize_groups(
                    field_name='symbol',
                    scope=scope,
                    primary_chain=chain,
                    secondary_chain=secondary_chain or None,
                    threshold=None,
                    total_contracts=total_contracts,
                    total_nfts=total_nfts,
                    groups=groups,
                )
            )
        for other_chain in chains:
            if other_chain == chain:
                continue
            groups = _fetch_symbol_groups(conn, scope='chain_matrix', primary_chain=chain, secondary_chain=other_chain)
            _insert_symbol_groups(conn, scope='chain_matrix', primary_chain=chain, secondary_chain=other_chain, groups=groups)
            summary_rows.append(
                summarize_groups(
                    field_name='symbol',
                    scope='chain_matrix',
                    primary_chain=chain,
                    secondary_chain=other_chain,
                    threshold=None,
                    total_contracts=total_contracts,
                    total_nfts=total_nfts,
                    groups=groups,
                )
            )
    _insert_summary_rows(conn, summary_rows)
    conn.commit()
    return summary_rows
