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
    ContractSignal,
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

        _PRECOMPUTED = ('token_uri_norm', 'image_uri_norm', 'name_norm', 'symbol_norm', 'metadata_doc',
                        'metadata_keywords_arr')
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
        max_recall_rows: int = 0,
    ) -> DatabaseSnapshot:
        seed_index = build_seed_index(seed_nfts)
        excluded_contracts = sorted(seed_index['seed_contracts'])
        exact_token_keys = sorted(seed_index['token_uri_keys'])
        exact_image_keys = sorted(seed_index['image_uri_keys'])
        exact_symbols = sorted(seed_index['symbol_norms'])
        name_prefixes = sorted({normalize_name(item.name)[:8] for item in seed_nfts if normalize_name(item.name)})
        metadata_recall_terms = list(
            sorted(
                {
                    keyword
                    for item in seed_nfts
                    for keyword in metadata_keywords(metadata_document_from_json(item.metadata_json))
                }
            )[:8]
        )
        select_clause = '''
            SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                   metadata_doc, token_uri_norm, image_uri_norm, symbol_norm, name_norm, metadata_keywords_arr
            FROM nft_features
            WHERE chain = ?
        '''
        base_query = [select_clause]
        base_params: list[object] = [chain]
        if excluded_contracts:
            base_params.append(excluded_contracts)
            base_query.append('AND NOT list_contains(?, contract_address)')

        recall_conditions: list[str] = []
        if exact_token_keys:
            base_params.append(exact_token_keys)
            recall_conditions.append('list_contains(?, token_uri_norm)')
        if exact_image_keys:
            base_params.append(exact_image_keys)
            recall_conditions.append('list_contains(?, image_uri_norm)')
        if exact_symbols:
            base_params.append(exact_symbols)
            recall_conditions.append('list_contains(?, symbol_norm)')
        if name_prefixes:
            base_params.append(name_prefixes)
            recall_conditions.append('list_contains(?, substr(name_norm, 1, 8))')
        if metadata_recall_terms:
            # Use pre-computed keyword arrays when available; each row only needs one
            # shared keyword with the seed set — a single list_has_any replaces 8× LIKE scans.
            base_params.append(metadata_recall_terms)
            recall_conditions.append('list_has_any(metadata_keywords_arr, ?)')

        if recall_conditions:
            base_query.append(f"AND ({' OR '.join(recall_conditions)})")

        # Per-contract token cap: applied via a two-step query to work around a DuckDB 1.5.x
        # bug where QUALIFY + multiple list_contains(?) OR conditions triggers an internal error.
        # Step 1 finds matching contract_addresses; step 2 applies the row cap per contract.
        if max_tokens_per_contract > 0:
            # Build step-1 query: same filters, returns only DISTINCT contract_address.
            step1_query = base_query[:]
            step1_query[0] = step1_query[0].replace(
                'SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,\n                   metadata_doc, token_uri_norm, image_uri_norm, symbol_norm, name_norm, metadata_keywords_arr',
                'SELECT DISTINCT contract_address'
            )
            step1_str = '\n'.join(step1_query)
            step1_params = base_params[:]

            # Step-2 query fetches token details with per-contract cap, filtering by the
            # contract set returned in step 1 (passed as a list parameter).
            step2_parts = [
                '''
                SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                       metadata_doc, token_uri_norm, image_uri_norm, symbol_norm, name_norm, metadata_keywords_arr
                FROM nft_features
                WHERE chain = ?
                  AND list_contains(?, contract_address)
                QUALIFY row_number() OVER (PARTITION BY contract_address ORDER BY token_id) <= ?
                ORDER BY contract_address, token_id
                '''
            ]
            if max_recall_rows > 0:
                step2_parts.append('LIMIT ?')
            step2_str = '\n'.join(step2_parts)
            use_two_step = True
        else:
            use_two_step = False
            selected_rows_parts = base_query[:]
            selected_rows_parts.append('ORDER BY contract_address, token_id')
            if max_recall_rows > 0:
                selected_rows_parts.append('LIMIT ?')
            query_str = '\n'.join(selected_rows_parts)
            query_params = base_params[:] + ([max_recall_rows] if max_recall_rows > 0 else [])

        # For file-based databases, open a dedicated read-only connection so that
        # multiple batch workers can execute queries in parallel without contending
        # on the write-connection lock.
        def _run(conn_obj: duckdb.DuckDBPyConnection):
            if use_two_step:
                contract_addrs = [row[0] for row in conn_obj.execute(step1_str, step1_params).fetchall()]
                step2_params_: list[object] = [chain, contract_addrs, max_tokens_per_contract]
                if max_recall_rows > 0:
                    step2_params_.append(max_recall_rows)
                return conn_obj.execute(step2_str, step2_params_).to_arrow_table()
            return conn_obj.execute(query_str, query_params).to_arrow_table()

        if self._database_path != ':memory:':
            rconn = duckdb.connect(database=self._database_path, read_only=True)
            try:
                arrow_result = _run(rconn)
            finally:
                rconn.close()
        else:
            with self._lock:
                arrow_result = _run(self._conn)

        # Batch-convert Arrow columns to Python lists in one vectorised call per column.
        # This replaces N×individual [i].as_py() calls with a small set of to_pylist() calls.
        col_addr = arrow_result['contract_address'].to_pylist()
        col_tid = arrow_result['token_id'].to_pylist()
        col_turi = arrow_result['token_uri'].to_pylist()
        col_iuri = arrow_result['image_uri'].to_pylist()
        col_name = arrow_result['name'].to_pylist()
        col_sym = arrow_result['symbol'].to_pylist()
        col_mj = arrow_result['metadata_json'].to_pylist()
        col_md = arrow_result['metadata_doc'].to_pylist()
        col_turi_norm = arrow_result['token_uri_norm'].to_pylist()
        col_iuri_norm = arrow_result['image_uri_norm'].to_pylist()
        col_sym_norm = arrow_result['symbol_norm'].to_pylist()
        col_name_norm = arrow_result['name_norm'].to_pylist()
        col_keywords = arrow_result['metadata_keywords_arr'].to_pylist()
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

        exact_token_set = set(exact_token_keys)
        exact_image_set = set(exact_image_keys)
        exact_symbol_set = set(exact_symbols)
        name_prefix_set = set(name_prefixes)
        metadata_term_set = set(metadata_recall_terms)

        seen_contract_name_pairs: set[tuple[str, str]] = set()
        contract_names: list[ContractNameRecord] = []
        symbol_contracts_raw: dict[str, set[str]] = defaultdict(set)
        contract_signal_counts: dict[str, dict[str, object]] = {}
        for addr, token_uri_norm, image_uri_norm, sym_norm, name_norm, keywords in zip(
            col_addr, col_turi_norm, col_iuri_norm, col_sym_norm, col_name_norm, col_keywords
        ):
            contract_address = addr or ''
            symbol_norm = sym_norm or ''
            normalized_name = name_norm or ''
            keyword_values = set(keywords or [])

            if normalized_name:
                key = (contract_address, normalized_name)
                if key not in seen_contract_name_pairs:
                    contract_names.append(ContractNameRecord(contract_address=contract_address, name_norm=normalized_name))
                    seen_contract_name_pairs.add(key)
            if symbol_norm:
                symbol_contracts_raw[symbol_norm].add(contract_address)

            signal = contract_signal_counts.setdefault(
                contract_address,
                {
                    'token_count': 0,
                    'uri_match_count': 0,
                    'image_match_count': 0,
                    'symbol_match': False,
                    'name_prefix_match': False,
                    'keyword_match': False,
                },
            )
            signal['token_count'] += 1
            if (token_uri_norm or '') in exact_token_set:
                signal['uri_match_count'] += 1
            if (image_uri_norm or '') in exact_image_set:
                signal['image_match_count'] += 1
            if symbol_norm in exact_symbol_set:
                signal['symbol_match'] = True
            if normalized_name[:8] in name_prefix_set:
                signal['name_prefix_match'] = True
            if metadata_term_set and keyword_values & metadata_term_set:
                signal['keyword_match'] = True

        symbol_contracts = dict(symbol_contracts_raw)

        contract_signals: dict[str, ContractSignal] = {
            contract_address: ContractSignal(
                contract_address=contract_address,
                token_count=int(signal['token_count']),
                uri_match_count=int(signal['uri_match_count']),
                image_match_count=int(signal['image_match_count']),
                symbol_match=bool(signal['symbol_match']),
                name_prefix_match=bool(signal['name_prefix_match']),
                keyword_match=bool(signal['keyword_match']),
            )
            for contract_address, signal in contract_signal_counts.items()
        }

        return DatabaseSnapshot(
            nft_rows=nft_rows,
            contract_names=contract_names,
            symbol_contracts=symbol_contracts,
            contract_signals=contract_signals,
        )
