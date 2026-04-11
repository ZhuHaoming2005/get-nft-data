from __future__ import annotations

from collections import defaultdict
from threading import RLock
from typing import Sequence

import duckdb
from duckdb import sqltypes
import pyarrow as pa
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
        self._database_path = database_path
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
                    metadata_doc VARCHAR,
                    metadata_keywords_arr VARCHAR[]
                )
                '''
            )
            # Migrate existing databases that predate the metadata_keywords_arr column.
            existing_cols = {
                row[0]
                for row in self._conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nft_features'"
                ).fetchall()
            }
            if 'metadata_keywords_arr' not in existing_cols:
                self._conn.execute('ALTER TABLE nft_features ADD COLUMN metadata_keywords_arr VARCHAR[]')

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def replace_chain_rows(self, chain: str, rows: Sequence[DatabaseNFTRecord]) -> None:
        chains = [chain] * len(rows)
        contract_addresses = [row.contract_address.lower() for row in rows]
        token_ids = [row.token_id for row in rows]
        token_uris = [row.token_uri for row in rows]
        image_uris = [row.image_uri for row in rows]
        names = [row.name for row in rows]
        symbols = [row.symbol for row in rows]
        metadata_jsons = [row.metadata_json for row in rows]
        token_uri_norms = [normalize_url(v) or '' for v in token_uris]
        image_uri_norms = [normalize_url(v) or '' for v in image_uris]
        name_norms = [normalize_name(v) for v in names]
        symbol_norms_list = [normalize_symbol(v) for v in symbols]
        metadata_docs = [row.metadata_doc or metadata_document_from_json(row.metadata_json) for row in rows]
        keywords_arr = [metadata_keywords(doc, limit=8) for doc in metadata_docs]
        arrow_table = pa.table({
            'chain': pa.array(chains, type=pa.string()),
            'contract_address': pa.array(contract_addresses, type=pa.string()),
            'token_id': pa.array(token_ids, type=pa.string()),
            'token_uri': pa.array(token_uris, type=pa.string()),
            'image_uri': pa.array(image_uris, type=pa.string()),
            'name': pa.array(names, type=pa.string()),
            'symbol': pa.array(symbols, type=pa.string()),
            'metadata_json': pa.array(metadata_jsons, type=pa.string()),
            'token_uri_norm': pa.array(token_uri_norms, type=pa.string()),
            'image_uri_norm': pa.array(image_uri_norms, type=pa.string()),
            'name_norm': pa.array(name_norms, type=pa.string()),
            'symbol_norm': pa.array(symbol_norms_list, type=pa.string()),
            'metadata_doc': pa.array(metadata_docs, type=pa.string()),
            'metadata_keywords_arr': pa.array(keywords_arr, type=pa.list_(pa.string())),
        })
        with self._lock:
            self._conn.execute('DELETE FROM nft_features WHERE chain = ?', [chain])
            if len(rows) > 0:
                self._conn.register('_tmp_replace', arrow_table)
                self._conn.execute('INSERT INTO nft_features SELECT * FROM _tmp_replace')
                self._conn.unregister('_tmp_replace')

    def load_parquet_dataset(self, chain: str, parquet_path: str, *, strict: bool = False) -> None:
        column_names = set(pq.read_schema(parquet_path).names)

        _PRECOMPUTED = ('token_uri_norm', 'image_uri_norm', 'name_norm', 'symbol_norm', 'metadata_doc')
        missing = [c for c in _PRECOMPUTED if c not in column_names]
        if missing and strict:
            raise ValueError(
                f'Parquet file {parquet_path!r} is missing pre-computed columns {missing}. '
                'Re-export the snapshot with export_snapshot.py or disable strict mode.'
            )
        if missing:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                'Parquet %r missing pre-computed columns %s — falling back to Python UDFs. '
                'This is slow at 50M+ rows; re-export with export_snapshot.py.',
                parquet_path, missing,
            )
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
        # Prefer pre-computed keyword arrays shipped in the Parquet file; fall back to NULL
        # so the column exists but keyword-recall simply won't fire for old snapshots.
        metadata_keywords_arr_expr = (
            "coalesce(metadata_keywords_arr, []::VARCHAR[])"
            if 'metadata_keywords_arr' in column_names
            else "[]::VARCHAR[]"
        )
        with self._lock:
            self._conn.execute('DELETE FROM nft_features WHERE chain = ?', [chain])
            self._conn.execute(
                f'''
                INSERT INTO nft_features (
                    chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                    token_uri_norm, image_uri_norm, name_norm, symbol_norm, metadata_doc, metadata_keywords_arr
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
                    {metadata_doc_expr},
                    {metadata_keywords_arr_expr}
                FROM read_parquet(?)
                ''',
                [chain, parquet_path],
            )

    def load_snapshot(
        self,
        chain: str,
        *,
        seed_nfts: Sequence[SeedNFT],
        max_tokens_per_contract: int = 500,
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
            params.append(excluded_contracts)
            query.append('AND NOT list_contains(?, contract_address)')

        recall_conditions: list[str] = []
        if exact_token_keys:
            params.append(exact_token_keys)
            recall_conditions.append('list_contains(?, token_uri_norm)')
        if exact_image_keys:
            params.append(exact_image_keys)
            recall_conditions.append('list_contains(?, image_uri_norm)')
        if exact_symbols:
            params.append(exact_symbols)
            recall_conditions.append('list_contains(?, symbol_norm)')
        if name_prefixes:
            params.append(name_prefixes)
            recall_conditions.append('list_contains(?, substr(name_norm, 1, 8))')
        if metadata_terms:
            # Use pre-computed keyword arrays when available; each row only needs one
            # shared keyword with the seed set — a single list_has_any replaces 8× LIKE scans.
            params.append(list(metadata_terms[:8]))
            recall_conditions.append('list_has_any(metadata_keywords_arr, ?)')

        if recall_conditions:
            query.append(f"AND ({' OR '.join(recall_conditions)})")

        # Per-contract token cap: prevents a single high-match contract from flooding the
        # candidate set with tens of thousands of tokens.  The goal is to confirm a contract
        # is infringing, not to enumerate every duplicate token — so a bounded sample suffices.
        if max_tokens_per_contract > 0:
            query.append(
                'QUALIFY row_number() OVER (PARTITION BY contract_address ORDER BY token_id) <= ?'
            )
            params.append(max_tokens_per_contract)

        query_str = '\n'.join(query)

        # For file-based databases, open a dedicated read-only connection so that
        # multiple batch workers can execute queries in parallel without contending
        # on the write-connection lock.
        if self._database_path != ':memory:':
            rconn = duckdb.connect(database=self._database_path, read_only=True)
            try:
                arrow_result = rconn.execute(query_str, params).fetch_arrow_table()
            finally:
                rconn.close()
        else:
            with self._lock:
                arrow_result = self._conn.execute(query_str, params).fetch_arrow_table()

        # Batch-convert Arrow columns to Python lists in one vectorised call per column.
        # This replaces N×8 individual [i].as_py() calls with 8 to_pylist() calls.
        col_addr = arrow_result['contract_address'].to_pylist()
        col_tid = arrow_result['token_id'].to_pylist()
        col_turi = arrow_result['token_uri'].to_pylist()
        col_iuri = arrow_result['image_uri'].to_pylist()
        col_name = arrow_result['name'].to_pylist()
        col_sym = arrow_result['symbol'].to_pylist()
        col_mj = arrow_result['metadata_json'].to_pylist()
        col_md = arrow_result['metadata_doc'].to_pylist()
        nft_rows = [
            DatabaseNFTRecord(
                contract_address=a or '',
                token_id=t or '',
                token_uri=u or '',
                image_uri=iu or '',
                name=n or '',
                symbol=s or '',
                metadata_json=mj or '',
                metadata_doc=md or '',
            )
            for a, t, u, iu, n, s, mj, md in zip(
                col_addr, col_tid, col_turi, col_iuri, col_name, col_sym, col_mj, col_md
            )
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
