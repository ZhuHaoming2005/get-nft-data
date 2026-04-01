from __future__ import annotations

import logging
from typing import Sequence

from psycopg2.extras import execute_values

from .config import chain_to_table
from .db import connect, ensure_schema
from .normalize import (
    build_name_block_key,
    build_name_signature,
    build_name_signature_hash,
    name_length_bucket,
    normalize_name,
    normalize_symbol,
)

logger = logging.getLogger(__name__)


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


def _identity_row(run_label: str, chain: str, contract_address: str, nft_count: int, raw_name: str, raw_symbol: str, name_variant_count: int, symbol_variant_count: int) -> tuple[object, ...]:
    name_norm = normalize_name(raw_name)
    symbol_norm = normalize_symbol(raw_symbol)
    name_len = len(name_norm)
    return (
        run_label,
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
        build_name_signature(name_norm),
        build_name_signature_hash(name_norm),
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
            INSERT INTO nsv2_contract_identity (
                run_label, chain, contract_address, nft_count, raw_name, raw_symbol, name_norm, symbol_norm,
                name_len, name_len_bucket, name_block_key, name_signature, name_signature_hash,
                name_variant_count, symbol_variant_count
            ) VALUES %s
            ON CONFLICT (run_label, chain, contract_address) DO UPDATE SET
                nft_count = EXCLUDED.nft_count,
                raw_name = EXCLUDED.raw_name,
                raw_symbol = EXCLUDED.raw_symbol,
                name_norm = EXCLUDED.name_norm,
                symbol_norm = EXCLUDED.symbol_norm,
                name_len = EXCLUDED.name_len,
                name_len_bucket = EXCLUDED.name_len_bucket,
                name_block_key = EXCLUDED.name_block_key,
                name_signature = EXCLUDED.name_signature,
                name_signature_hash = EXCLUDED.name_signature_hash,
                name_variant_count = EXCLUDED.name_variant_count,
                symbol_variant_count = EXCLUDED.symbol_variant_count,
                updated_at = NOW()
            """,
            rows,
            page_size=1000,
        )


def _rebuild_name_atoms(conn, run_label: str, chains: Sequence[str]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM nsv2_name_atoms
            WHERE run_label = %s AND chain = ANY(%s)
            """,
            (run_label, list(chains)),
        )
        cur.execute(
            """
            INSERT INTO nsv2_name_atoms (
                run_label, chain, name_norm, sample_contract_address, contract_count, nft_count,
                name_len, name_len_bucket, name_block_key, name_signature, name_signature_hash
            )
            SELECT run_label,
                   chain,
                   name_norm,
                   min(contract_address) AS sample_contract_address,
                   count(*)::bigint AS contract_count,
                   coalesce(sum(nft_count), 0)::bigint AS nft_count,
                   min(name_len)::int AS name_len,
                   min(name_len_bucket)::int AS name_len_bucket,
                   min(name_block_key) AS name_block_key,
                   min(name_signature) AS name_signature,
                   min(name_signature_hash) AS name_signature_hash
            FROM nsv2_contract_identity
            WHERE run_label = %s
              AND chain = ANY(%s)
              AND name_norm <> ''
              AND name_block_key <> ''
            GROUP BY run_label, chain, name_norm
            """,
            (run_label, list(chains)),
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def build_contract_identity(conn, run_label: str, chains: Sequence[str], *, batch_size: int = 5000) -> dict[str, int]:
    ensure_schema(conn)
    counts: dict[str, int] = {}
    for chain in chains:
        table_name = chain_to_table(chain)
        sql = _rollup_sql(
            table_name,
            has_name=_table_has_column(conn, table_name, 'name'),
            has_symbol=_table_has_column(conn, table_name, 'symbol'),
        )
        with conn.cursor() as cur:
            cur.execute('DELETE FROM nsv2_contract_identity WHERE run_label = %s AND chain = %s', (run_label, chain))
        conn.commit()

        inserted = 0
        buffer: list[tuple[object, ...]] = []
        read_conn = connect()
        try:
            with read_conn.cursor(name=f'nsv2_identity_{chain}') as cur:
                cur.itersize = batch_size
                cur.execute(sql)
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        buffer.append(_identity_row(run_label, chain, *row))
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

    atom_count = _rebuild_name_atoms(conn, run_label, chains)
    logger.info('rebuilt %d name atoms for run %s', atom_count, run_label)
    return counts
