from __future__ import annotations

from collections import defaultdict
from threading import RLock
from typing import Sequence

import duckdb
from duckdb import sqltypes
import pyarrow.parquet as pq

from . import (
    ContractNameRecord,
    DatabaseNFTRecord,
    DatabaseSnapshot,
    SeedNFT,
    build_seed_index,
    normalize_name,
    normalize_symbol,
    normalize_url,
)
from .rust_bridge import metadata_document_from_json, metadata_keywords


class DuckDBFeatureStore:
    def __init__(self, database_path: str = ':memory:') -> None:
        self._lock = RLock()
        self._conn = duckdb.connect(database=database_path)
        with self._lock:
            self._conn.create_function('py_normalize_url', normalize_url, return_type=sqltypes.VARCHAR)
            self._conn.create_function('py_normalize_name', normalize_name, return_type=sqltypes.VARCHAR)
            self._conn.create_function('py_normalize_symbol', normalize_symbol, return_type=sqltypes.VARCHAR)
            self._conn.create_function(
                'py_metadata_document_from_json',
                metadata_document_from_json,
                return_type=sqltypes.VARCHAR,
            )
            self._conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS nft_features (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_id VARCHAR NOT NULL,
                    token_uri VARCHAR,
                    image_uri VARCHAR,
                    name VARCHAR,
                    symbol VARCHAR,
                    metadata_json VARCHAR,
                    token_uri_norm VARCHAR,
                    image_uri_norm VARCHAR,
                    name_norm VARCHAR,
                    symbol_norm VARCHAR,
                    metadata_doc VARCHAR
                )
                '''
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def replace_chain_rows(self, chain: str, rows: Sequence[DatabaseNFTRecord]) -> None:
        prepared_rows = [
            (
                chain,
                row.contract_address.lower(),
                row.token_id,
                row.token_uri,
                row.image_uri,
                row.name,
                row.symbol,
                row.metadata_json,
                normalize_url(row.token_uri),
                normalize_url(row.image_uri),
                normalize_name(row.name),
                normalize_symbol(row.symbol),
                row.metadata_doc or metadata_document_from_json(row.metadata_json),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.execute('DELETE FROM nft_features WHERE chain = ?', [chain])
            if prepared_rows:
                self._conn.executemany(
                    '''
                    INSERT INTO nft_features (
                        chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                        token_uri_norm, image_uri_norm, name_norm, symbol_norm, metadata_doc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    prepared_rows,
                )

    def load_parquet_dataset(self, chain: str, parquet_path: str) -> None:
        column_names = set(pq.read_schema(parquet_path).names)
        metadata_json_expr = (
            "''"
            if 'metadata_doc' in column_names
            else ("coalesce(metadata_json, '')" if 'metadata_json' in column_names else "''")
        )
        token_uri_norm_expr = (
            "coalesce(token_uri_norm, '')"
            if 'token_uri_norm' in column_names
            else "py_normalize_url(coalesce(token_uri, ''))"
        )
        image_uri_norm_expr = (
            "coalesce(image_uri_norm, '')"
            if 'image_uri_norm' in column_names
            else "py_normalize_url(coalesce(image_uri, ''))"
        )
        name_norm_expr = (
            "coalesce(name_norm, '')"
            if 'name_norm' in column_names
            else "py_normalize_name(coalesce(name, ''))"
        )
        symbol_norm_expr = (
            "coalesce(symbol_norm, '')"
            if 'symbol_norm' in column_names
            else "py_normalize_symbol(coalesce(symbol, ''))"
        )
        metadata_doc_expr = (
            "coalesce(metadata_doc, '')"
            if 'metadata_doc' in column_names
            else (
                "py_metadata_document_from_json(coalesce(metadata_json, ''))"
                if 'metadata_json' in column_names
                else "''"
            )
        )
        with self._lock:
            self._conn.execute('DELETE FROM nft_features WHERE chain = ?', [chain])
            self._conn.execute(
                f'''
                INSERT INTO nft_features (
                    chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                    token_uri_norm, image_uri_norm, name_norm, symbol_norm, metadata_doc
                )
                SELECT
                    ?,
                    lower(contract_address),
                    cast(token_id as varchar),
                    coalesce(token_uri, ''),
                    coalesce(image_uri, ''),
                    coalesce(name, ''),
                    coalesce(symbol, ''),
                    {metadata_json_expr},
                    {token_uri_norm_expr},
                    {image_uri_norm_expr},
                    {name_norm_expr},
                    {symbol_norm_expr},
                    {metadata_doc_expr}
                FROM read_parquet(?)
                ''',
                [chain, parquet_path],
            )

    def load_snapshot(
        self,
        chain: str,
        *,
        seed_nfts: Sequence[SeedNFT],
    ) -> DatabaseSnapshot:
        seed_index = build_seed_index(seed_nfts)
        excluded_contracts = sorted(seed_index['seed_contracts'])
        exact_token_keys = sorted(seed_index['token_uri_keys'])
        exact_image_keys = sorted(seed_index['image_uri_keys'])
        exact_symbols = sorted(seed_index['symbol_norms'])
        name_prefixes = sorted({normalize_name(item.name)[:8] for item in seed_nfts if normalize_name(item.name)})
        metadata_terms = sorted(
            {
                keyword
                for item in seed_nfts
                for keyword in metadata_keywords(metadata_document_from_json(item.metadata_json))
            }
        )

        query = [
            '''
            SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json
                   , metadata_doc
            FROM nft_features
            WHERE chain = ?
            '''
        ]
        params: list[object] = [chain]
        if excluded_contracts:
            placeholders = ', '.join('?' for _ in excluded_contracts)
            query.append(f'AND contract_address NOT IN ({placeholders})')
            params.extend(excluded_contracts)

        recall_conditions: list[str] = []
        if exact_token_keys:
            placeholders = ', '.join('?' for _ in exact_token_keys)
            recall_conditions.append(f'token_uri_norm IN ({placeholders})')
            params.extend(exact_token_keys)
        if exact_image_keys:
            placeholders = ', '.join('?' for _ in exact_image_keys)
            recall_conditions.append(f'image_uri_norm IN ({placeholders})')
            params.extend(exact_image_keys)
        if exact_symbols:
            placeholders = ', '.join('?' for _ in exact_symbols)
            recall_conditions.append(f'symbol_norm IN ({placeholders})')
            params.extend(exact_symbols)
        if name_prefixes:
            placeholders = ', '.join('?' for _ in name_prefixes)
            recall_conditions.append(f'substr(name_norm, 1, 8) IN ({placeholders})')
            params.extend(name_prefixes)
        for term in metadata_terms[:8]:
            recall_conditions.append('metadata_doc LIKE ?')
            params.append(f'%{term}%')

        if recall_conditions:
            query.append(f"AND ({' OR '.join(recall_conditions)})")

        with self._lock:
            rows = self._conn.execute('\n'.join(query), params).fetchall()
        nft_rows = [
            DatabaseNFTRecord(
                contract_address=contract_address,
                token_id=token_id,
                token_uri=token_uri or '',
                image_uri=image_uri or '',
                name=name or '',
                symbol=symbol or '',
                metadata_json=metadata_json or '',
                metadata_doc=metadata_doc or '',
            )
            for contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, metadata_doc in rows
        ]

        contract_names: list[ContractNameRecord] = []
        symbol_contracts: dict[str, set[str]] = defaultdict(set)
        for row in nft_rows:
            name_norm = normalize_name(row.name)
            if name_norm:
                contract_names.append(ContractNameRecord(contract_address=row.contract_address, name_norm=name_norm))
            symbol_norm = normalize_symbol(row.symbol)
            if symbol_norm:
                symbol_contracts[symbol_norm].add(row.contract_address)

        return DatabaseSnapshot(
            nft_rows=nft_rows,
            contract_names=contract_names,
            symbol_contracts=dict(symbol_contracts),
        )
