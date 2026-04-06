#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

REQUESTS_HTTP_ERROR = requests.HTTPError if requests is not None else Exception
REQUESTS_REQUEST_ERROR = requests.RequestException if requests is not None else Exception

try:
    from rapidfuzz import fuzz as rapidfuzz_fuzz
except ImportError:  # pragma: no cover
    rapidfuzz_fuzz = None


if load_dotenv is not None:
    load_dotenv()


logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


DEFAULT_OPENSEA_ENDPOINT = 'https://api.opensea.io/api/v2/tokens/top'
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100

_RE_IPFS_HTTP = re.compile(
    r'https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)',
    re.IGNORECASE,
)
_RE_ARWEAVE_HTTP = re.compile(
    r'https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)',
    re.IGNORECASE,
)
_TRAILING_ID_PATTERNS = [
    re.compile(r'\s*#\s*[0-9a-fA-FxX]+\s*$'),
    re.compile(r'\s*#\s*\d+\s*$'),
    re.compile(r'\s*-\s*\d+\s*$'),
    re.compile(r'\s*:\s*\d+\s*$'),
    re.compile(r'\s*\(\s*\d+\s*\)\s*$'),
    re.compile(r'\s*\[\s*\d+\s*\]\s*$'),
    re.compile(r'\s*/\s*\d+\s*$'),
    re.compile(r'\s+No\.?\s*\d+\s*$', re.I),
    re.compile(r'\s+nr\.?\s*\d+\s*$', re.I),
    re.compile(r'\s+\d{1,12}\s*$'),
]


@dataclass(frozen=True)
class TargetNFT:
    chain: str
    contract_address: str
    token_id: str
    name: str
    symbol: str
    image_url: str


@dataclass(frozen=True)
class ContractNameRecord:
    contract_address: str
    name_norm: str


@dataclass(frozen=True)
class AnalysisRow:
    chain: str
    contract_address: str
    token_id: str
    name: str
    symbol: str
    image_url: str
    image_key: str
    name_norm: str
    symbol_norm: str
    image_duplicate_nft_count: int
    symbol_duplicate_contract_count: int
    name_duplicate_contract_count: int


class _LenIndex:
    def __init__(self, keys: Sequence[str]) -> None:
        buckets: Dict[int, List[str]] = defaultdict(list)
        for key in keys:
            buckets[len(key)].append(key)
        self._buckets = dict(buckets)
        self._sorted_lengths = sorted(buckets)

    def candidates(self, key: str, threshold: float) -> List[str]:
        if not key:
            return []
        key_length = len(key)
        factor = (200.0 - threshold) / threshold
        min_length = max(1, math.ceil(key_length / factor))
        max_length = int(key_length * factor)
        out: List[str] = []
        for length in self._sorted_lengths:
            if length < min_length:
                continue
            if length > max_length:
                break
            out.extend(self._buckets[length])
        return out


class DatabaseSnapshot:
    def __init__(
        self,
        *,
        image_total_counts: Optional[Dict[str, int]] = None,
        image_contract_counts: Optional[Dict[Tuple[str, str], int]] = None,
        symbol_contracts: Optional[Dict[str, set[str]]] = None,
        contract_names: Optional[List[ContractNameRecord]] = None,
    ) -> None:
        self.image_total_counts = image_total_counts or {}
        self.image_contract_counts = image_contract_counts or {}
        self.symbol_contracts = symbol_contracts or {}
        self.contract_names = contract_names or []
        self._name_contracts: Dict[str, set[str]] = defaultdict(set)
        for record in self.contract_names:
            self._name_contracts[record.name_norm].add(record.contract_address)
        self._name_keys = list(self._name_contracts.keys())
        self._name_index = _LenIndex(self._name_keys)

    def count_image_duplicates(self, image_key: str, contract_address: str) -> int:
        if not image_key:
            return 0
        total = self.image_total_counts.get(image_key, 0)
        own = self.image_contract_counts.get((image_key, contract_address), 0)
        return max(total - own, 0)

    def count_symbol_duplicates(self, symbol_norm: str, contract_address: str) -> int:
        if not symbol_norm:
            return 0
        contracts = self.symbol_contracts.get(symbol_norm, set())
        return len(contracts - {contract_address})

    def count_name_duplicates(self, name_norm: str, contract_address: str, threshold: float) -> int:
        if not name_norm:
            return 0
        matched_contracts: set[str] = set()
        for candidate in self._name_index.candidates(name_norm, threshold):
            if similarity_score(name_norm, candidate) >= threshold:
                matched_contracts.update(self._name_contracts[candidate])
        matched_contracts.discard(contract_address)
        return len(matched_contracts)


def strip_trailing_number_suffix(raw: str) -> str:
    text = unicodedata.normalize('NFKC', (raw or '').strip())
    changed = True
    guard = 0
    while changed and guard < 20:
        changed = False
        guard += 1
        for pattern in _TRAILING_ID_PATTERNS:
            updated = pattern.sub('', text)
            if updated != text:
                text = updated.strip()
                changed = True
                break
    return re.sub(r'\s+', ' ', text).strip()


def normalize_name(raw: str) -> str:
    text = strip_trailing_number_suffix(raw or '')
    text = unicodedata.normalize('NFKC', text)
    return re.sub(r'\s+', ' ', text).strip().casefold()


def normalize_symbol(raw: str) -> str:
    return unicodedata.normalize('NFKC', (raw or '').strip()).casefold()


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    text = url.strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {'nano', 'null', 'none', 'undefined', 'n/a', 'na', '-', '.', 'false', 'true', '0'}:
        return None
    if lowered.startswith('data:'):
        return None
    if lowered.startswith('ipfs://'):
        tail = text[7:]
        if tail.lower().startswith('ipfs/'):
            tail = tail[5:]
        cid_path = tail.split('?', 1)[0].split('#', 1)[0].strip('/')
        return f'ipfs:{cid_path}' if cid_path else None
    if lowered.startswith('ar://'):
        tx_path = text[5:].split('?', 1)[0].split('#', 1)[0].strip('/')
        return f'ar:{tx_path}' if tx_path else None
    match = _RE_IPFS_HTTP.match(text)
    if match:
        cid_path = match.group(1).split('?', 1)[0].split('#', 1)[0].rstrip('/')
        return f'ipfs:{cid_path}' if cid_path else None
    match = _RE_ARWEAVE_HTTP.match(text)
    if match:
        tx_path = match.group(1).split('?', 1)[0].split('#', 1)[0].rstrip('/')
        return f'ar:{tx_path}' if tx_path else None
    return lowered.rstrip('/')


def similarity_score(left: str, right: str) -> float:
    if rapidfuzz_fuzz is not None:
        return float(rapidfuzz_fuzz.ratio(left, right))
    return SequenceMatcher(None, left, right).ratio() * 100.0


def chain_to_table(chain: str) -> str:
    safe = re.sub(r'[^a-z0-9_]', '', chain.lower().strip())
    if not safe:
        raise ValueError(f'illegal chain name: {chain!r}')
    return f'nft_assets_{safe}'


def get_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'nft_data'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASS', ''),
        connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
    )


def extract_items_and_cursor(payload: Dict[str, Any], default_chain: str = '') -> Tuple[List[TargetNFT], str]:
    raw_items = payload.get('tokens')
    if not isinstance(raw_items, list):
        return [], ''
    items: List[TargetNFT] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        address = str(raw.get('address') or '').strip().lower()
        if not address:
            continue
        items.append(
            TargetNFT(
                chain=str(raw.get('chain') or default_chain),
                contract_address=address,
                token_id='',
                name=str(raw.get('name') or ''),
                symbol=str(raw.get('symbol') or ''),
                image_url=str(raw.get('image_url') or ''),
            )
        )
    cursor = str(payload.get('next') or '')
    return items, cursor


def build_trending_request_url(
    *,
    endpoint: str,
    chain: str,
    page_limit: int,
    next_cursor: str = '',
) -> str:
    query = f'chains={chain}&limit={page_limit}'
    if next_cursor:
        # OpenSea returns `next` and expects it to be sent back as `cursor`.
        query += f'&cursor={next_cursor}'
    return f'{endpoint}?{query}'


def normalize_page_size(page_size: int) -> int:
    if page_size < 1:
        return 1
    if page_size > MAX_PAGE_SIZE:
        return MAX_PAGE_SIZE
    return page_size


def fetch_trending_targets(
    *,
    api_key: str,
    chain: str,
    limit: int,
    endpoint: str = DEFAULT_OPENSEA_ENDPOINT,
    page_size: int = 50,
    timeout: int = 30,
) -> List[TargetNFT]:
    if not api_key:
        raise ValueError('missing OpenSea API key')
    if requests is None:
        raise RuntimeError('requests is required to call the OpenSea API')
    effective_page_size = normalize_page_size(page_size)
    if effective_page_size != page_size:
        logger.info(
            'adjusted requested page_size from %d to %d to match OpenSea limit',
            page_size,
            effective_page_size,
        )
    seen: set[Tuple[str, str]] = set()
    results: List[TargetNFT] = []
    cursor = ''
    page_number = 0
    while len(results) < limit:
        page_number += 1
        request_limit = min(effective_page_size, limit - len(results))
        url = build_trending_request_url(
            endpoint=endpoint,
            chain=chain,
            page_limit=request_limit,
            next_cursor=cursor,
        )
        try:
            response = requests.get(
                url,
                headers={
                    'accept': '*/*',
                    'x-api-key': api_key,
                },
                timeout=timeout,
            )
        except REQUESTS_REQUEST_ERROR as exc:
            raise RuntimeError(f'OpenSea API request failed: {exc}') from exc
        try:
            response.raise_for_status()
        except REQUESTS_HTTP_ERROR as exc:
            body = (response.text or '').strip()
            detail = f'OpenSea API returned {response.status_code} {response.reason}'
            if response.status_code in {401, 403}:
                detail += '; check OPENSEA_API_KEY or --opensea-api-key permissions'
            if body:
                detail += f' | body={body}'
            raise RuntimeError(detail) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f'OpenSea API returned non-JSON response with status {response.status_code}'
            ) from exc
        items, next_cursor = extract_items_and_cursor(payload, default_chain=chain)
        logger.info(
            'page=%d requested=%d received=%d accumulated=%d has_next=%s url=%s',
            page_number,
            request_limit,
            len(items),
            len(results),
            bool(next_cursor),
            url,
        )
        if not items:
            break
        before_count = len(results)
        for item in items:
            key = (item.contract_address, item.token_id)
            if key in seen:
                continue
            seen.add(key)
            results.append(item)
            if len(results) >= limit:
                break
        logger.info(
            'page=%d unique_added=%d accumulated=%d next_cursor=%s',
            page_number,
            len(results) - before_count,
            len(results),
            '<present>' if next_cursor else '<empty>',
        )
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return results


def load_database_snapshot(conn, chain: str, fetch_size: int = 10000) -> DatabaseSnapshot:
    table = chain_to_table(chain)
    image_total_counts: Dict[str, int] = defaultdict(int)
    image_contract_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    symbol_contracts: Dict[str, set[str]] = defaultdict(set)
    contract_names: List[ContractNameRecord] = []

    image_sql = f'''
        SELECT lower(contract_address), image_uri
        FROM {table}
        WHERE image_uri IS NOT NULL
          AND trim(image_uri) <> ''
    '''
    with conn.cursor(name=f'image_snapshot_{chain}') as cur:
        cur.itersize = fetch_size
        cur.execute(image_sql)
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            for contract_address, image_uri in rows:
                image_key = normalize_url(image_uri)
                if not image_key:
                    continue
                image_total_counts[image_key] += 1
                image_contract_counts[(image_key, contract_address)] += 1
    conn.commit()

    contract_sql = f'''
        SELECT DISTINCT ON (lower(contract_address))
            lower(contract_address) AS contract_address,
            coalesce(name, '') AS name,
            coalesce(symbol, '') AS symbol
        FROM {table}
        WHERE (name IS NOT NULL AND trim(name) <> '')
           OR (symbol IS NOT NULL AND trim(symbol) <> '')
        ORDER BY lower(contract_address), id DESC
    '''
    with conn.cursor(name=f'contract_snapshot_{chain}') as cur:
        cur.itersize = fetch_size
        cur.execute(contract_sql)
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
                    contract_names.append(
                        ContractNameRecord(
                            contract_address=contract_address,
                            name_norm=name_norm,
                        )
                    )
    conn.commit()

    return DatabaseSnapshot(
        image_total_counts=dict(image_total_counts),
        image_contract_counts=dict(image_contract_counts),
        symbol_contracts={key: set(value) for key, value in symbol_contracts.items()},
        contract_names=contract_names,
    )


def analyze_targets(
    targets: Sequence[TargetNFT],
    snapshot: DatabaseSnapshot,
    *,
    name_threshold: float,
) -> List[AnalysisRow]:
    rows: List[AnalysisRow] = []
    for target in targets:
        contract_address = target.contract_address.lower()
        image_key = normalize_url(target.image_url) or ''
        symbol_norm = normalize_symbol(target.symbol)
        name_norm = normalize_name(target.name)
        rows.append(
            AnalysisRow(
                chain=target.chain,
                contract_address=contract_address,
                token_id=target.token_id,
                name=target.name,
                symbol=target.symbol,
                image_url=target.image_url,
                image_key=image_key,
                name_norm=name_norm,
                symbol_norm=symbol_norm,
                image_duplicate_nft_count=snapshot.count_image_duplicates(image_key, contract_address),
                symbol_duplicate_contract_count=snapshot.count_symbol_duplicates(symbol_norm, contract_address),
                name_duplicate_contract_count=snapshot.count_name_duplicates(name_norm, contract_address, name_threshold),
            )
        )
    return rows


def build_summary(rows: Sequence[AnalysisRow]) -> Dict[str, Any]:
    total = len(rows)
    image_positive = sum(1 for row in rows if row.image_duplicate_nft_count > 0)
    symbol_positive = sum(1 for row in rows if row.symbol_duplicate_contract_count > 0)
    name_positive = sum(1 for row in rows if row.name_duplicate_contract_count > 0)
    any_positive = sum(
        1
        for row in rows
        if row.image_duplicate_nft_count > 0
        or row.symbol_duplicate_contract_count > 0
        or row.name_duplicate_contract_count > 0
    )
    return {
        'total_trending_nfts': total,
        'image_duplicate_nft_hits': image_positive,
        'symbol_duplicate_contract_hits': symbol_positive,
        'name_duplicate_contract_hits': name_positive,
        'any_duplicate_hits': any_positive,
        'image_duplicate_nft_sum': sum(row.image_duplicate_nft_count for row in rows),
        'symbol_duplicate_contract_sum': sum(row.symbol_duplicate_contract_count for row in rows),
        'name_duplicate_contract_sum': sum(row.name_duplicate_contract_count for row in rows),
    }


def dump_results(rows: Sequence[AnalysisRow], output_path: Optional[str]) -> None:
    payload = {
        'summary': build_summary(rows),
        'rows': [asdict(row) for row in rows],
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if output_path:
        Path(output_path).write_text(text, encoding='utf-8')
    else:
        print(text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Analyze fake-NFT signals among OpenSea trending NFTs against local PostgreSQL data.',
    )
    parser.add_argument('--chain', default='ethereum')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--page-size', type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument('--name-threshold', type=float, default=95.0)
    parser.add_argument('--endpoint', default=DEFAULT_OPENSEA_ENDPOINT)
    parser.add_argument('--timeout', type=int, default=30)
    parser.add_argument('--output', default='results.json', help='output file path (default: stdout)')
    parser.add_argument('--opensea-api-key', default=os.getenv('OPENSEA_API_KEY', ''))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    with get_conn() as conn:
        snapshot = load_database_snapshot(conn, args.chain)
    logger.info('loaded database snapshot for chain=%s', args.chain)
    targets = fetch_trending_targets(
        api_key=args.opensea_api_key,
        chain=args.chain,
        limit=args.limit,
        endpoint=args.endpoint,
        page_size=args.page_size,
        timeout=args.timeout,
    )
    logger.info('fetched %d trending NFTs from OpenSea', len(targets))
    rows = analyze_targets(targets, snapshot, name_threshold=args.name_threshold)
    dump_results(rows, args.output)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
