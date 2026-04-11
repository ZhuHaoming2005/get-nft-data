from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from . import chain_to_table, get_conn, normalize_name, normalize_symbol, normalize_url
from .rust_bridge import metadata_document_from_json


_METADATA_COLUMN_CANDIDATES = ('metadata_json', 'raw_metadata', 'metadata', 'raw_json', 'json_metadata')


def _detect_metadata_column(conn, table: str) -> str | None:
    candidates = ', '.join(f"'{value}'" for value in _METADATA_COLUMN_CANDIDATES)
    with conn.cursor() as cur:
        cur.execute(
            f'''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = '{table}'
              AND column_name IN ({candidates})
            ORDER BY array_position(ARRAY[{candidates}], column_name)
            LIMIT 1
            '''
        )
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def export_chain_snapshot_to_parquet(
    conn,
    *,
    chain: str,
    output_path: str | Path,
    fetch_size: int = 100_000,
    keep_metadata_json: bool = False,
) -> Path:
    table = chain_to_table(chain)
    metadata_column = _detect_metadata_column(conn, table)
    metadata_sql = f'coalesce({metadata_column}::text, \'\')' if metadata_column else '\'\''
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    writer: pq.ParquetWriter | None = None
    schema_fields = [
        ('chain', pa.string()),
        ('contract_address', pa.string()),
        ('token_id', pa.string()),
        ('token_uri', pa.string()),
        ('image_uri', pa.string()),
        ('name', pa.string()),
        ('symbol', pa.string()),
    ]
    if keep_metadata_json:
        schema_fields.append(('metadata_json', pa.string()))
    schema_fields.extend(
        [
            ('token_uri_norm', pa.string()),
            ('image_uri_norm', pa.string()),
            ('name_norm', pa.string()),
            ('symbol_norm', pa.string()),
            ('metadata_doc', pa.string()),
        ]
    )
    schema = pa.schema(schema_fields)
    with conn.cursor(name=f'export_snapshot_{chain}') as cur:
        cur.itersize = fetch_size
        cur.execute(
            f'''
            SELECT lower(contract_address), token_id::text, coalesce(token_uri, ''), coalesce(image_uri, ''),
                   coalesce(name, ''), coalesce(symbol, ''), {metadata_sql}
            FROM {table}
            ORDER BY id
            '''
        )
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            arrays = list(zip(*rows))
            token_uri_values = list(arrays[2])
            image_uri_values = list(arrays[3])
            name_values = list(arrays[4])
            symbol_values = list(arrays[5])
            metadata_json_values = list(arrays[6])
            batch_payload = {
                'chain': pa.array([chain] * len(rows), type=pa.string()),
                'contract_address': pa.array(arrays[0], type=pa.string()),
                'token_id': pa.array(arrays[1], type=pa.string()),
                'token_uri': pa.array(token_uri_values, type=pa.string()),
                'image_uri': pa.array(image_uri_values, type=pa.string()),
                'name': pa.array(name_values, type=pa.string()),
                'symbol': pa.array(symbol_values, type=pa.string()),
                'token_uri_norm': pa.array([normalize_url(value) or '' for value in token_uri_values], type=pa.string()),
                'image_uri_norm': pa.array([normalize_url(value) or '' for value in image_uri_values], type=pa.string()),
                'name_norm': pa.array([normalize_name(value) for value in name_values], type=pa.string()),
                'symbol_norm': pa.array([normalize_symbol(value) for value in symbol_values], type=pa.string()),
                'metadata_doc': pa.array(
                    [metadata_document_from_json(value) for value in metadata_json_values],
                    type=pa.string(),
                ),
            }
            if keep_metadata_json:
                batch_payload['metadata_json'] = pa.array(metadata_json_values, type=pa.string())
            batch = pa.table(batch_payload, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(target, schema=schema)
            writer.write_table(batch)
    if writer is None:
        writer = pq.ParquetWriter(target, schema=schema)
    writer.close()
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Export one NFT asset chain snapshot from PostgreSQL to Parquet.')
    parser.add_argument('--chain', default='ethereum')
    parser.add_argument('--output', required=True)
    parser.add_argument('--fetch-size', type=int, default=100_000)
    parser.add_argument('--keep-metadata-json', action='store_true', help='include raw metadata_json in the parquet output')
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    conn = get_conn()
    try:
        export_chain_snapshot_to_parquet(
            conn,
            chain=args.chain,
            output_path=args.output,
            fetch_size=args.fetch_size,
            keep_metadata_json=args.keep_metadata_json,
        )
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
