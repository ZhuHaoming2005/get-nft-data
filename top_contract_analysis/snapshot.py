from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .alchemy_api import _normalize_token_id
from .constants import DEFAULT_NAME_THRESHOLD, logger
from .db import chain_to_table
from .models import ContractNameRecord, DatabaseNFTRecord, DatabaseSnapshot, DuplicateCandidate, SeedNFT, _LenIndex
from .normalize import normalize_name, normalize_symbol, normalize_url, similarity_score


def build_seed_index(seed_nfts: Sequence[SeedNFT]) -> Dict[str, Any]:
    token_uri_keys = {key for nft in seed_nfts if (key := normalize_url(nft.token_uri))}
    image_uri_keys = {key for nft in seed_nfts if (key := normalize_url(nft.image_uri))}
    name_norms = {key for nft in seed_nfts if (key := normalize_name(nft.name))}
    symbol_norms = {key for nft in seed_nfts if (key := normalize_symbol(nft.symbol))}
    return {
        'seed_contracts': {nft.contract_address.lower() for nft in seed_nfts},
        'token_uri_keys': token_uri_keys,
        'image_uri_keys': image_uri_keys,
        'name_norms': name_norms,
        'symbol_norms': symbol_norms,
        'name_index': _LenIndex(list(name_norms)),
    }


def load_database_snapshot(
    conn,
    chain: str,
    *,
    seed_nfts: Optional[Sequence[SeedNFT]] = None,
    fetch_size: int = 10000,
    max_rows: Optional[int] = None,
) -> DatabaseSnapshot:
    logger.warning(
        'load_database_snapshot (PostgreSQL fallback) is not suitable for large-scale '
        'production use — token_uri/image_uri normalisation and name fuzzy matching run '
        'in Python. For 50M+ rows, use DuckDBFeatureStore with a pre-exported Parquet snapshot.'
    )
    table = chain_to_table(chain)
    seed_index = build_seed_index(seed_nfts or [])
    token_uri_keys = seed_index['token_uri_keys']
    image_uri_keys = seed_index['image_uri_keys']
    symbol_norms = seed_index['symbol_norms']
    seed_name_norms = seed_index['name_norms']
    seed_contracts = seed_index['seed_contracts']
    name_index: _LenIndex = seed_index['name_index']

    excluded = list(seed_contracts) or ['__no_match__']
    symbol_list = list(symbol_norms) if symbol_norms else None
    name_prefix_list = sorted({n[:8] for n in seed_name_norms if n}) if seed_name_norms else None

    where_parts = ['lower(contract_address) <> ALL(%s)']
    query_args: list = [excluded]

    server_recall: list[str] = []
    if symbol_list:
        server_recall.append('lower(symbol) = ANY(%s)')
        query_args.append(symbol_list)
    if name_prefix_list:
        server_recall.append('left(lower(name), 8) = ANY(%s)')
        query_args.append(name_prefix_list)
    if token_uri_keys or image_uri_keys:
        server_recall.append('token_uri IS NOT NULL')

    if server_recall:
        where_parts.append(f"AND ({' OR '.join(server_recall)})")

    where_clause = '\n'.join(where_parts)

    nft_rows: List[DatabaseNFTRecord] = []
    with conn.cursor(name=f'nft_rows_{chain}') as cur:
        cur.itersize = fetch_size
        cur.execute(
            f'''
            SELECT lower(contract_address), token_id, coalesce(token_uri, ''), coalesce(image_uri, ''),
                   coalesce(name, ''), coalesce(symbol, ''), coalesce(metadata::text, '')
            FROM {table}
            WHERE {where_clause}
            ''',
            query_args,
        )
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            for contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json in rows:
                if contract_address in seed_contracts:
                    continue
                token_key = normalize_url(token_uri)
                image_key = normalize_url(image_uri)
                symbol_norm = normalize_symbol(symbol)
                name_norm = normalize_name(name)
                strong = bool(token_key and token_key in token_uri_keys) or bool(image_key and image_key in image_uri_keys)
                weak_symbol = bool(symbol_norm and symbol_norm in symbol_norms)
                weak_name = False
                if name_norm:
                    for candidate in name_index.candidates(name_norm, DEFAULT_NAME_THRESHOLD):
                        if candidate in seed_name_norms and similarity_score(name_norm, candidate) >= DEFAULT_NAME_THRESHOLD:
                            weak_name = True
                            break
                if strong or weak_symbol or weak_name:
                    nft_rows.append(
                        DatabaseNFTRecord(
                            contract_address=contract_address,
                            token_id=_normalize_token_id(token_id),
                            token_uri=token_uri,
                            image_uri=image_uri,
                            name=name,
                            symbol=symbol,
                            metadata_json=metadata_json or '',
                        )
                    )
                    if max_rows is not None and len(nft_rows) >= max_rows:
                        logger.warning(
                            'load_database_snapshot hit max_rows=%d for chain %s — stopping early. '
                            'Results may be incomplete.',
                            max_rows, chain,
                        )
                        break
            else:
                continue
            break
    conn.commit()

    contract_names: List[ContractNameRecord] = []
    symbol_contracts: Dict[str, set[str]] = defaultdict(set)
    with conn.cursor(name=f'contract_snapshot_{chain}') as cur:
        cur.itersize = fetch_size
        cur.execute(
            f'''
            SELECT DISTINCT ON (lower(contract_address))
                lower(contract_address), coalesce(name, ''), coalesce(symbol, '')
            FROM {table}
            WHERE lower(contract_address) <> ALL(%s)
            ORDER BY lower(contract_address), id DESC
            ''',
            (excluded,),
        )
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            for contract_address, name, symbol in rows:
                symbol_norm = normalize_symbol(symbol)
                if symbol_norm:
                    symbol_contracts[symbol_norm].add(contract_address)
                name_norm = normalize_name(name)
                if name_norm:
                    contract_names.append(ContractNameRecord(contract_address=contract_address, name_norm=name_norm))
    conn.commit()
    return DatabaseSnapshot(
        nft_rows=nft_rows,
        contract_names=contract_names,
        symbol_contracts={key: set(value) for key, value in symbol_contracts.items()},
    )


def find_duplicate_candidates(
    seed_nfts: Sequence[SeedNFT],
    snapshot: DatabaseSnapshot,
    *,
    name_threshold: float = DEFAULT_NAME_THRESHOLD,
    metadata_threshold: float = 0.55,
) -> List[DuplicateCandidate]:
    from .rust_bridge import (
        metadata_document_from_json,
        metadata_keywords,
        score_metadata_documents,
        score_name_pairs,
    )

    seed_index = build_seed_index(seed_nfts)
    token_uri_keys = seed_index['token_uri_keys']
    image_uri_keys = seed_index['image_uri_keys']
    seed_name_norms = seed_index['name_norms']
    seed_symbol_norms = seed_index['symbol_norms']
    seed_contracts = seed_index['seed_contracts']
    name_index: _LenIndex = seed_index['name_index']

    filtered_rows = [row for row in snapshot.nft_rows if row.contract_address not in seed_contracts]

    name_pair_indices: List[int] = []
    name_left: List[str] = []
    name_right: List[str] = []
    for idx, row in enumerate(filtered_rows):
        name_norm = normalize_name(row.name)
        if not name_norm:
            continue
        for candidate in name_index.candidates(name_norm, name_threshold):
            if candidate in seed_name_norms:
                name_pair_indices.append(idx)
                name_left.append(name_norm)
                name_right.append(candidate)

    name_matches: set[int] = set()
    for idx, score in zip(name_pair_indices, score_name_pairs(name_left, name_right)):
        if score >= name_threshold:
            name_matches.add(idx)

    seed_metadata_docs: List[str] = []
    seed_metadata_keyword_sets: List[set[str]] = []
    seed_metadata_keyword_union: set[str] = set()
    for item in seed_nfts:
        seed_doc = item.metadata_doc or metadata_document_from_json(item.metadata_json)
        if not seed_doc:
            continue
        seed_keywords = set(metadata_keywords(seed_doc, limit=12))
        seed_metadata_docs.append(seed_doc)
        seed_metadata_keyword_sets.append(seed_keywords)
        seed_metadata_keyword_union.update(seed_keywords)
    metadata_pair_indices: List[int] = []
    metadata_left: List[str] = []
    metadata_right: List[str] = []
    for idx, row in enumerate(filtered_rows):
        row_doc = row.metadata_doc or metadata_document_from_json(row.metadata_json)
        if not row_doc:
            continue
        row_keywords = set(metadata_keywords(row_doc, limit=12))
        if seed_metadata_keyword_union and not (row_keywords & seed_metadata_keyword_union):
            continue
        for seed_doc, seed_keywords in zip(seed_metadata_docs, seed_metadata_keyword_sets):
            if row_keywords and seed_keywords and not (row_keywords & seed_keywords):
                continue
            metadata_pair_indices.append(idx)
            metadata_left.append(seed_doc)
            metadata_right.append(row_doc)

    metadata_matches: set[int] = set()
    for idx, score in zip(metadata_pair_indices, score_metadata_documents(metadata_left, metadata_right)):
        if score >= metadata_threshold:
            metadata_matches.add(idx)

    candidates: Dict[Tuple[str, str], DuplicateCandidate] = {}
    for idx, row in enumerate(filtered_rows):
        if row.contract_address in seed_contracts:
            continue
        reasons: List[str] = []
        token_key = normalize_url(row.token_uri)
        image_key = normalize_url(row.image_uri)
        symbol_norm = normalize_symbol(row.symbol)
        if token_key and token_key in token_uri_keys:
            reasons.append('token_uri_match')
        if image_key and image_key in image_uri_keys:
            reasons.append('image_uri_match')
        if symbol_norm and symbol_norm in seed_symbol_norms:
            reasons.append('symbol_match')
        if idx in name_matches:
            reasons.append('name_match')
        if idx in metadata_matches:
            reasons.append('metadata_match')
        if not reasons:
            continue
        unique_reasons = tuple(sorted(set(reasons)))
        confidence = 'low'
        if (
            'token_uri_match' in unique_reasons
            or 'image_uri_match' in unique_reasons
            or 'metadata_match' in unique_reasons
        ):
            confidence = 'high'
        elif 'name_match' in unique_reasons and 'symbol_match' in unique_reasons:
            confidence = 'high'
        candidates[(row.contract_address, row.token_id)] = DuplicateCandidate(
            contract_address=row.contract_address,
            token_id=row.token_id,
            match_reasons=unique_reasons,
            confidence=confidence,
            token_uri=row.token_uri,
            image_uri=row.image_uri,
            name=row.name,
            symbol=row.symbol,
        )
    return list(candidates.values())


def group_candidates_by_contract(candidates: Sequence[DuplicateCandidate]) -> Dict[str, List[DuplicateCandidate]]:
    grouped: Dict[str, List[DuplicateCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.contract_address].append(candidate)
    return dict(grouped)
