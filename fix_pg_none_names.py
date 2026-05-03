#!/usr/bin/env python3
"""One-off PostgreSQL repair for contracts with both None-like and real names."""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import Any

import psycopg2
from dotenv import load_dotenv


BAD_NAME_VALUES = {"", "none", "null", "nan"}


@dataclass(frozen=True)
class FixStats:
    fixable_contracts: int
    rows_to_update: int
    skipped_ambiguous_contracts: int


def table_name(chain: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain.lower()) or "default"
    return f"nft_assets_{safe}"


def quote_ident(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return value


def is_bad_name(value: str | None) -> bool:
    return value is None or value.strip().lower() in BAD_NAME_VALUES


def is_bad_name_sql(expr: str = "name") -> str:
    return f"({expr} IS NULL OR lower(btrim({expr})) = ANY(%s))"


def fixable_contracts_cte(table: str) -> str:
    table = quote_ident(table)
    return f"""
        WITH per_contract AS (
            SELECT
                lower(contract_address) AS contract_address,
                COUNT(*) FILTER (WHERE {is_bad_name_sql("name")}) AS bad_name_rows,
                COUNT(DISTINCT btrim(name)) FILTER (
                    WHERE NOT {is_bad_name_sql("name")}
                ) AS good_name_count,
                MIN(btrim(name)) FILTER (
                    WHERE NOT {is_bad_name_sql("name")}
                ) AS canonical_name
            FROM {table}
            WHERE contract_address IS NOT NULL
            GROUP BY lower(contract_address)
        ),
        fixable AS (
            SELECT
                contract_address,
                canonical_name,
                bad_name_rows,
                true AS has_bad_name
            FROM per_contract
            WHERE bad_name_rows > 0
              AND good_name_count = 1
              AND canonical_name IS NOT NULL
              AND canonical_name <> ''
        )
    """


def select_fixable_contracts_sql(table: str) -> str:
    return (
        fixable_contracts_cte(table)
        + """
        SELECT contract_address, canonical_name, bad_name_rows, has_bad_name
        FROM fixable
        ORDER BY bad_name_rows DESC, contract_address
        LIMIT %s
        """
    )


def count_fixable_sql(table: str) -> str:
    table = quote_ident(table)
    return (
        fixable_contracts_cte(table)
        + f"""
        SELECT
            COUNT(*) AS fixable_contracts,
            COALESCE(SUM(bad_name_rows), 0) AS rows_to_update,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT lower(contract_address) AS contract_address
                    FROM {table}
                    WHERE contract_address IS NOT NULL
                    GROUP BY lower(contract_address)
                    HAVING COUNT(*) FILTER (WHERE {is_bad_name_sql("name")}) > 0
                       AND COUNT(DISTINCT btrim(name)) FILTER (
                           WHERE NOT {is_bad_name_sql("name")}
                       ) > 1
                ) ambiguous
            ) AS skipped_ambiguous_contracts
        FROM fixable
        """
    )


def apply_fix_sql(table: str) -> str:
    table = quote_ident(table)
    return (
        fixable_contracts_cte(table)
        + f"""
        UPDATE {table} AS target
        SET name = fixable.canonical_name
        FROM fixable
        WHERE lower(target.contract_address) = fixable.contract_address
          AND {is_bad_name_sql("target.name")}
        """
    )


def bad_name_params(repeats: int = 1) -> tuple[list[str], ...]:
    values = sorted(BAD_NAME_VALUES)
    return tuple(values for _ in range(repeats))


def connect_from_env():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "nft_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", ""),
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
    )


def fetch_stats(conn, table: str) -> FixStats:
    with conn.cursor() as cur:
        cur.execute(count_fixable_sql(table), (*bad_name_params(5),))
        fixable_contracts, rows_to_update, skipped_ambiguous_contracts = cur.fetchone()
    return FixStats(
        fixable_contracts=int(fixable_contracts or 0),
        rows_to_update=int(rows_to_update or 0),
        skipped_ambiguous_contracts=int(skipped_ambiguous_contracts or 0),
    )


def fetch_examples(conn, table: str, limit: int) -> list[tuple[Any, ...]]:
    if limit <= 0:
        return []
    with conn.cursor() as cur:
        cur.execute(select_fixable_contracts_sql(table), (*bad_name_params(3), limit))
        return list(cur.fetchall())


def apply_fix(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(apply_fix_sql(table), (*bad_name_params(4),))
        return int(cur.rowcount or 0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fix nft_assets_{chain}.name rows where a contract has both None-like "
            "names and exactly one real canonical name."
        )
    )
    parser.add_argument("--chain", default=os.getenv("CHAIN_NAME", "ethereum"))
    parser.add_argument("--table", help="Override table name; defaults to nft_assets_{chain}")
    parser.add_argument("--apply", action="store_true", help="Commit updates. Default is dry-run.")
    parser.add_argument("--examples", type=int, default=20)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    table = args.table or table_name(args.chain)

    with connect_from_env() as conn:
        stats = fetch_stats(conn, table)
        print(f"table: {table}")
        print(f"fixable contracts: {stats.fixable_contracts}")
        print(f"rows to update: {stats.rows_to_update}")
        print(f"skipped ambiguous contracts: {stats.skipped_ambiguous_contracts}")

        examples = fetch_examples(conn, table, args.examples)
        if examples:
            print("examples:")
            for contract_address, canonical_name, bad_rows, _has_bad_name in examples:
                print(f"  {contract_address}: {bad_rows} rows -> {canonical_name}")

        if not args.apply:
            conn.rollback()
            print("dry-run only; pass --apply to update PostgreSQL")
            return 0

        updated = apply_fix(conn, table)
        conn.commit()
        print(f"updated rows: {updated}")
        if updated != stats.rows_to_update:
            print(
                "warning: updated row count differs from dry-run estimate; "
                "data may have changed concurrently",
                file=sys.stderr,
            )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
