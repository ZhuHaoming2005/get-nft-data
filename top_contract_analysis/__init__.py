from __future__ import annotations

# Example:
#   C:\Users\z1766\.conda\envs\codex\python.exe -m top_contract_analysis ^
#     --chain ethereum ^
#     --seed-contract-address 0xbd3531da5cf5857e7cfaa92426877b022e612cf8 ^
#     --alchemy-api-key your_alchemy_api_key ^
#     --etherscan-api-key your_etherscan_api_key

import argparse
import json
import logging
import math
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

if load_dotenv is not None:
    load_dotenv()


logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

REQUESTS_HTTP_ERROR = requests.HTTPError if requests is not None else Exception
REQUESTS_REQUEST_ERROR = requests.RequestException if requests is not None else Exception

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
DEFAULT_TIMEOUT = 30
DEFAULT_NAME_THRESHOLD = 95.0
DEFAULT_ALCHEMY_RETRIES = 3

DEFAULT_NETWORKS = {
    'ethereum': 'eth-mainnet',
    'base': 'base-mainnet',
    'polygon': 'polygon-mainnet',
}

ETHERSCAN_CHAIN_IDS = {
    'ethereum': '1',
    'base': '8453',
    'polygon': '137',
}

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
class SeedNFT:
    chain: str
    contract_address: str
    token_id: str
    name: str
    symbol: str
    token_uri: str
    image_uri: str


@dataclass(frozen=True)
class DatabaseNFTRecord:
    contract_address: str
    token_id: str
    token_uri: str
    image_uri: str
    name: str
    symbol: str


@dataclass(frozen=True)
class ContractNameRecord:
    contract_address: str
    name_norm: str


@dataclass(frozen=True)
class ContractMetadata:
    chain: str
    contract_address: str
    token_type: str
    contract_deployer: str
    deployed_block_number: int
    name: str
    symbol: str


@dataclass(frozen=True)
class DuplicateCandidate:
    contract_address: str
    token_id: str
    match_reasons: Tuple[str, ...]
    confidence: str
    token_uri: str
    image_uri: str
    name: str
    symbol: str


@dataclass(frozen=True)
class TransferRecord:
    contract_address: str
    token_id: str
    tx_hash: str
    log_index: int
    block_number: int
    block_time: int
    from_address: str
    to_address: str
    event_type: str
    source: str


@dataclass(frozen=True)
class OwnerBalance:
    owner_address: str
    token_balances: Dict[str, int]


class DatabaseSnapshot:
    def __init__(
        self,
        *,
        nft_rows: Optional[List[DatabaseNFTRecord]] = None,
        contract_names: Optional[List[ContractNameRecord]] = None,
        symbol_contracts: Optional[Dict[str, set[str]]] = None,
    ) -> None:
        self.nft_rows = nft_rows or []
        self.contract_names = contract_names or []
        self.symbol_contracts = symbol_contracts or {}


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
        factor = (200.0 - threshold) / threshold
        min_length = max(1, math.ceil(len(key) / factor))
        max_length = int(len(key) * factor)
        out: List[str] = []
        for length in self._sorted_lengths:
            if length < min_length:
                continue
            if length > max_length:
                break
            out.extend(self._buckets[length])
        return out


def normalize_network(chain: str, explicit_network: str | None = None) -> str:
    if explicit_network:
        return explicit_network
    network = DEFAULT_NETWORKS.get(chain.lower())
    if not network:
        raise ValueError(f'unsupported chain for Alchemy network lookup: {chain}')
    return network


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


def normalize_name(raw: str | None) -> str:
    text = strip_trailing_number_suffix(raw or '')
    text = unicodedata.normalize('NFKC', text)
    return re.sub(r'\s+', ' ', text).strip().casefold()


def normalize_symbol(raw: str | None) -> str:
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


def _require_requests() -> None:
    if requests is None:
        raise RuntimeError('requests is required for API access')


def _alchemy_nft_base(network: str) -> str:
    return f'https://{network}.g.alchemy.com'


def _alchemy_rpc_base(network: str, api_key: str) -> str:
    return f'https://{network}.g.alchemy.com/v2/{api_key}'


def _normalize_token_id(raw: Any) -> str:
    text = str(raw or '').strip()
    if text.startswith('0x'):
        return str(int(text, 16))
    return text


def _alchemy_get_json(
    *,
    url: str,
    timeout: int,
    retries: int = DEFAULT_ALCHEMY_RETRIES,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            logger.warning('alchemy GET retry %d/%d failed for %s: %s', attempt, retries, url, exc)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError('alchemy GET failed without exception')


def _alchemy_post_json(
    *,
    url: str,
    payload: Dict[str, Any],
    timeout: int,
    retries: int = DEFAULT_ALCHEMY_RETRIES,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            logger.warning('alchemy POST retry %d/%d failed for %s: %s', attempt, retries, url, exc)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError('alchemy POST failed without exception')


def _parse_alchemy_block_timestamp(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    if text.startswith('0x'):
        return int(text, 16)
    try:
        return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp())
    except ValueError:
        return 0


def _fetch_block_timestamp(
    *,
    api_key: str,
    network: str,
    block_num: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> int:
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_getBlockByNumber',
            'params': [block_num, False],
        },
        timeout=timeout,
    )
    result = body.get('result') or {}
    return _parse_alchemy_block_timestamp(result.get('timestamp'))


def _extract_seed_nfts(payload: Dict[str, Any], *, chain: str, contract_address: str) -> Tuple[List[SeedNFT], str]:
    raw_items = payload.get('nfts')
    if not isinstance(raw_items, list):
        return [], ''
    rows: List[SeedNFT] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        raw_contract = raw.get('contract') or {}
        raw_id = raw.get('id') or {}
        raw_token_uri = raw.get('tokenUri') or {}
        raw_image = raw.get('image') or {}
        if isinstance(raw_token_uri, dict):
            token_uri = str(raw_token_uri.get('raw') or raw_token_uri.get('gateway') or '')
        else:
            token_uri = str(raw_token_uri or '')
        if isinstance(raw_image, dict):
            image_uri = str(
                raw_image.get('originalUrl')
                or raw_image.get('cachedUrl')
                or raw_image.get('pngUrl')
                or raw.get('image_url')
                or ''
            )
        else:
            image_uri = str(raw_image or raw.get('image_url') or '')
        rows.append(
            SeedNFT(
                chain=chain,
                contract_address=str(raw_contract.get('address') or contract_address).lower(),
                token_id=_normalize_token_id(raw_id.get('tokenId')),
                name=str(raw.get('title') or raw.get('name') or ''),
                symbol=str((raw.get('contractMetadata') or {}).get('symbol') or ''),
                token_uri=token_uri,
                image_uri=image_uri,
            )
        )
    return rows, str(payload.get('pageKey') or '')


def fetch_seed_contract_nfts(
    *,
    api_key: str,
    network: str,
    chain: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[SeedNFT]:
    _require_requests()
    endpoint = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getNFTsForContract'
    rows: List[SeedNFT] = []
    page_key = ''
    while True:
        params = {
            'contractAddress': contract_address,
            'withMetadata': 'true',
        }
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = _alchemy_get_json(url=url, timeout=timeout)
        batch, page_key = _extract_seed_nfts(payload, chain=chain, contract_address=contract_address)
        rows.extend(batch)
        if not page_key:
            break
    return rows


def fetch_nft_metadata(
    *,
    api_key: str,
    network: str,
    contract_address: str,
    token_id: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    _require_requests()
    params = {
        'contractAddress': contract_address,
        'tokenId': token_id,
        'refreshCache': 'false',
    }
    url = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getNFTMetadata?{urlencode(params)}'
    return _alchemy_get_json(url=url, timeout=timeout)


def fetch_license_sample(
    *,
    api_key: str,
    network: str,
    chain: str,
    seed_nfts: Sequence[SeedNFT],
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    for nft in seed_nfts:
        if nft.token_id:
            return fetch_nft_metadata(
                api_key=api_key,
                network=network,
                contract_address=nft.contract_address,
                token_id=nft.token_id,
                timeout=timeout,
            )
    return {}


def is_open_license_payload(payload: Dict[str, Any]) -> bool:
    texts: List[str] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                _walk(item)
        elif isinstance(value, list):
            for item in value:
                _walk(item)
        elif isinstance(value, str):
            texts.append(value)

    _walk(payload)
    haystack = ' '.join(texts).casefold()
    needles = [
        'cc0-1.0',
        'license: cc0',
        'creative commons zero',
        'public domain',
        'cc zero',
    ]
    return any(needle in haystack for needle in needles)


def fetch_contract_metadata(
    *,
    api_key: str,
    network: str,
    chain: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> ContractMetadata:
    _require_requests()
    params = {'contractAddress': contract_address}
    url = f'{_alchemy_nft_base(network)}/nft/v2/{api_key}/getContractMetadata?{urlencode(params)}'
    payload = _alchemy_get_json(url=url, timeout=timeout)
    meta = payload.get('contractMetadata') or {}
    return ContractMetadata(
        chain=chain,
        contract_address=str(payload.get('address') or contract_address).lower(),
        token_type=str(meta.get('tokenType') or ''),
        contract_deployer=str(meta.get('contractDeployer') or '').lower(),
        deployed_block_number=int(meta.get('deployedBlockNumber') or 0),
        name=str(meta.get('name') or ''),
        symbol=str(meta.get('symbol') or ''),
    )


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
) -> DatabaseSnapshot:
    table = chain_to_table(chain)
    seed_index = build_seed_index(seed_nfts or [])
    token_uri_keys = seed_index['token_uri_keys']
    image_uri_keys = seed_index['image_uri_keys']
    symbol_norms = seed_index['symbol_norms']
    seed_name_norms = seed_index['name_norms']
    seed_contracts = seed_index['seed_contracts']
    name_index: _LenIndex = seed_index['name_index']

    nft_rows: List[DatabaseNFTRecord] = []
    with conn.cursor(name=f'nft_rows_{chain}') as cur:
        cur.itersize = fetch_size
        cur.execute(
            f'''
            SELECT lower(contract_address), token_id, coalesce(token_uri, ''), coalesce(image_uri, ''),
                   coalesce(name, ''), coalesce(symbol, '')
            FROM {table}
            '''
        )
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            for contract_address, token_id, token_uri, image_uri, name, symbol in rows:
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
                        )
                    )
    conn.commit()

    contract_names: List[ContractNameRecord] = []
    symbol_contracts: Dict[str, set[str]] = defaultdict(set)
    excluded = list(seed_contracts) or ['']
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
) -> List[DuplicateCandidate]:
    seed_index = build_seed_index(seed_nfts)
    token_uri_keys = seed_index['token_uri_keys']
    image_uri_keys = seed_index['image_uri_keys']
    seed_name_norms = seed_index['name_norms']
    seed_symbol_norms = seed_index['symbol_norms']
    seed_contracts = seed_index['seed_contracts']
    name_index: _LenIndex = seed_index['name_index']

    candidates: Dict[Tuple[str, str], DuplicateCandidate] = {}
    for row in snapshot.nft_rows:
        if row.contract_address in seed_contracts:
            continue
        reasons: List[str] = []
        token_key = normalize_url(row.token_uri)
        image_key = normalize_url(row.image_uri)
        name_norm = normalize_name(row.name)
        symbol_norm = normalize_symbol(row.symbol)
        if token_key and token_key in token_uri_keys:
            reasons.append('token_uri_match')
        if image_key and image_key in image_uri_keys:
            reasons.append('image_uri_match')
        if symbol_norm and symbol_norm in seed_symbol_norms:
            reasons.append('symbol_match')
        if name_norm:
            for candidate in name_index.candidates(name_norm, name_threshold):
                if candidate in seed_name_norms and similarity_score(name_norm, candidate) >= name_threshold:
                    reasons.append('name_match')
                    break
        if not reasons:
            continue
        unique_reasons = tuple(sorted(set(reasons)))
        confidence = 'low'
        if 'token_uri_match' in unique_reasons or 'image_uri_match' in unique_reasons:
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


def fetch_alchemy_contract_transfers(
    *,
    api_key: str,
    network: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[TransferRecord]:
    _require_requests()
    url = _alchemy_rpc_base(network, api_key)
    params: Dict[str, Any] = {
        'fromBlock': '0x0',
        'toBlock': 'latest',
        'category': ['erc721', 'erc1155'],
        'contractAddresses': [contract_address],
        'withMetadata': True,
        'excludeZeroValue': False,
        'maxCount': '0x3e8',
        'order': 'asc',
    }
    transfers: List[TransferRecord] = []
    page_key = None
    block_time_cache: Dict[str, int] = {}
    while True:
        if page_key:
            params['pageKey'] = page_key
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'alchemy_getAssetTransfers',
            'params': [params],
        }
        body = _alchemy_post_json(url=url, payload=payload, timeout=timeout)
        if body.get('error'):
            raise RuntimeError(body['error'])
        result = body.get('result') or {}
        for item in result.get('transfers') or []:
            block_num = str(item.get('blockNum') or '0')
            block_time = _parse_alchemy_block_timestamp(item.get('metadata', {}).get('blockTimestamp'))
            if not block_time and block_num not in {'', '0'}:
                cached = block_time_cache.get(block_num)
                if cached is None:
                    cached = _fetch_block_timestamp(
                        api_key=api_key,
                        network=network,
                        block_num=block_num,
                        timeout=timeout,
                    )
                    block_time_cache[block_num] = cached
                block_time = cached
            transfers.append(
                TransferRecord(
                    contract_address=str(item.get('rawContract', {}).get('address') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('erc721TokenId') or item.get('tokenId') or ''),
                    tx_hash=str(item.get('hash') or ''),
                    log_index=int(item.get('logIndex') or 0),
                    block_number=int(block_num, 16) if block_num.startswith('0x') else int(block_num or 0),
                    block_time=block_time,
                    from_address=str(item.get('from') or '').lower(),
                    to_address=str(item.get('to') or '').lower(),
                    event_type=str(item.get('category') or ''),
                    source='alchemy',
                )
            )
        page_key = result.get('pageKey')
        if not page_key:
            break
    return transfers


def fetch_etherscan_contract_transfers(
    *,
    api_key: str,
    chain: str,
    contract_address: str,
    token_type: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[TransferRecord]:
    _require_requests()
    chain_id = ETHERSCAN_CHAIN_IDS.get(chain.lower())
    if not chain_id:
        raise ValueError(f'unsupported chain for etherscan fallback: {chain}')
    action = 'token1155tx' if token_type.upper() == 'ERC1155' else 'tokennfttx'
    base_url = 'https://api.etherscan.io/v2/api'
    page = 1
    transfers: List[TransferRecord] = []
    while True:
        params = {
            'chainid': chain_id,
            'module': 'account',
            'action': action,
            'contractaddress': contract_address,
            'page': page,
            'offset': 1000,
            'startblock': 0,
            'endblock': 9999999999,
            'sort': 'asc',
            'apikey': api_key,
        }
        url = f'{base_url}?{urlencode(params)}'
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        body = response.json()
        items = body.get('result') or []
        if not isinstance(items, list):
            break
        for item in items:
            transfers.append(
                TransferRecord(
                    contract_address=str(item.get('contractAddress') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('tokenID') or ''),
                    tx_hash=str(item.get('hash') or ''),
                    log_index=int(item.get('transactionIndex') or 0),
                    block_number=int(item.get('blockNumber') or 0),
                    block_time=int(item.get('timeStamp') or 0),
                    from_address=str(item.get('from') or '').lower(),
                    to_address=str(item.get('to') or '').lower(),
                    event_type='erc1155' if action == 'token1155tx' else 'erc721',
                    source='etherscan',
                )
            )
        if len(items) < 1000:
            break
        page += 1
    return transfers


def fetch_contract_transfers(
    *,
    alchemy_api_key: str,
    alchemy_network: str,
    etherscan_api_key: str,
    chain: str,
    contract_address: str,
    token_type: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[TransferRecord]:
    try:
        return fetch_alchemy_contract_transfers(
            api_key=alchemy_api_key,
            network=alchemy_network,
            contract_address=contract_address,
            timeout=timeout,
        )
    except Exception as exc:
        logger.warning('alchemy transfer fetch failed for %s: %s', contract_address, exc)
        return fetch_etherscan_contract_transfers(
            api_key=etherscan_api_key,
            chain=chain,
            contract_address=contract_address,
            token_type=token_type,
            timeout=timeout,
        )


def fetch_contract_owners(
    *,
    api_key: str,
    network: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[OwnerBalance]:
    _require_requests()
    endpoint = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getOwnersForContract'
    page_key = ''
    owners: List[OwnerBalance] = []
    while True:
        params = {
            'contractAddress': contract_address,
            'withTokenBalances': 'true',
        }
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = _alchemy_get_json(url=url, timeout=timeout)
        for row in payload.get('owners') or []:
            balances: Dict[str, int] = {}
            for balance in row.get('tokenBalances') or []:
                balances[_normalize_token_id(balance.get('tokenId'))] = int(balance.get('balance') or 0)
            owners.append(
                OwnerBalance(
                    owner_address=str(row.get('ownerAddress') or '').lower(),
                    token_balances=balances,
                )
            )
        page_key = str(payload.get('pageKey') or '')
        if not page_key:
            break
    return owners


def _calculate_cycle_edge_count(transfers: Sequence[TransferRecord]) -> int:
    seen_pairs = set()
    cycle_pairs = set()
    for transfer in transfers:
        if transfer.from_address == ZERO_ADDRESS or transfer.to_address == ZERO_ADDRESS:
            continue
        pair = (transfer.from_address, transfer.to_address)
        reverse = (transfer.to_address, transfer.from_address)
        if reverse in seen_pairs:
            cycle_pairs.add(tuple(sorted(pair)))
        seen_pairs.add(pair)
    return len(cycle_pairs)


def _calculate_star_distributors(transfers: Sequence[TransferRecord]) -> int:
    outgoing: Dict[str, set[str]] = defaultdict(set)
    incoming: Counter[str] = Counter()
    for transfer in transfers:
        if transfer.from_address == ZERO_ADDRESS or transfer.to_address == ZERO_ADDRESS:
            continue
        outgoing[transfer.from_address].add(transfer.to_address)
        incoming[transfer.to_address] += 1
    count = 0
    for sender, recipients in outgoing.items():
        if len(recipients) >= 3 and incoming.get(sender, 0) <= 1:
            count += 1
    return count


def analyze_contract_transfers(transfers: Sequence[TransferRecord]) -> Dict[str, Any]:
    mint_transfers = [row for row in transfers if row.from_address == ZERO_ADDRESS]
    non_mint = [row for row in transfers if row.from_address != ZERO_ADDRESS]
    receivers = {row.to_address for row in transfers if row.to_address and row.to_address != ZERO_ADDRESS}
    first_mint_time = min((row.block_time for row in mint_transfers), default=0)
    first_non_mint_time = min((row.block_time for row in non_mint), default=0)
    first_transfer_delay = 0
    if first_mint_time and first_non_mint_time and first_non_mint_time >= first_mint_time:
        first_transfer_delay = first_non_mint_time - first_mint_time
    return {
        'mint_address_count': len({row.to_address for row in mint_transfers if row.to_address}),
        'mint_count': len(mint_transfers),
        'unique_receiver_count': len(receivers),
        'cycle_edge_count': _calculate_cycle_edge_count(transfers),
        'star_distributor_count': _calculate_star_distributors(transfers),
        'mint_to_first_transfer_seconds': first_transfer_delay,
        'fast_spread': bool(first_transfer_delay and first_transfer_delay <= 24 * 3600),
    }


def analyze_contract_victims(
    transfers: Sequence[TransferRecord],
    owners: Sequence[OwnerBalance],
) -> Dict[str, Any]:
    active_sellers = {
        row.from_address for row in transfers
        if row.from_address and row.from_address != ZERO_ADDRESS
    }
    owners_with_balance = [owner for owner in owners if any(balance > 0 for balance in owner.token_balances.values())]
    stuck = [owner.owner_address for owner in owners_with_balance if owner.owner_address not in active_sellers]
    owner_count = len(owners_with_balance)
    return {
        'owner_count': owner_count,
        'stuck_holder_count': len(stuck),
        'stuck_holder_ratio': (len(stuck) / owner_count) if owner_count else 0.0,
        'victim_wallet_count': len(stuck),
    }


def analyze_seed_contract(
    *,
    chain: str,
    seed_contract_address: str,
    alchemy_api_key: str,
    alchemy_network: str | None = None,
    etherscan_api_key: str = '',
    conn=None,
    name_threshold: float = DEFAULT_NAME_THRESHOLD,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    network = normalize_network(chain, alchemy_network)
    own_conn = False
    if conn is None:
        conn = get_conn()
        own_conn = True
    try:
        contract_meta = fetch_contract_metadata(
            api_key=alchemy_api_key,
            network=network,
            chain=chain,
            contract_address=seed_contract_address,
            timeout=timeout,
        )
        seed_nfts = fetch_seed_contract_nfts(
            api_key=alchemy_api_key,
            network=network,
            chain=chain,
            contract_address=seed_contract_address,
            timeout=timeout,
        )
        license_payload = fetch_license_sample(
            api_key=alchemy_api_key,
            network=network,
            chain=chain,
            seed_nfts=seed_nfts,
            timeout=timeout,
        )
        open_license = is_open_license_payload(license_payload)
        snapshot = load_database_snapshot(conn, chain, seed_nfts=seed_nfts)
        candidates = find_duplicate_candidates(seed_nfts, snapshot, name_threshold=name_threshold)
        grouped = group_candidates_by_contract(candidates)

        official_addresses = {addr for addr in [contract_meta.contract_deployer, contract_meta.contract_address] if addr}
        legit_duplicates: List[Dict[str, Any]] = []
        high_confidence: List[Dict[str, Any]] = []
        low_confidence: List[Dict[str, Any]] = []
        address_signals: Dict[str, Any] = {}
        victim_signals: Dict[str, Any] = {}

        for contract_address, contract_candidates in grouped.items():
            contract_confidence = 'high' if any(item.confidence == 'high' for item in contract_candidates) else 'low'
            if open_license:
                continue
            if contract_confidence != 'high':
                low_confidence.append({
                    'contract_address': contract_address,
                    'candidate_count': len(contract_candidates),
                    'match_reasons': sorted({reason for item in contract_candidates for reason in item.match_reasons}),
                })
                continue
            transfers = fetch_contract_transfers(
                alchemy_api_key=alchemy_api_key,
                alchemy_network=network,
                etherscan_api_key=etherscan_api_key,
                chain=chain,
                contract_address=contract_address,
                token_type=contract_meta.token_type or 'ERC721',
                timeout=timeout,
            )
            mint_recipients = {row.to_address for row in transfers if row.from_address == ZERO_ADDRESS}
            if mint_recipients & official_addresses:
                legit_duplicates.append({
                    'contract_address': contract_address,
                    'candidate_count': len(contract_candidates),
                    'mint_recipients': sorted(mint_recipients),
                })
                continue
            owners = fetch_contract_owners(
                api_key=alchemy_api_key,
                network=network,
                contract_address=contract_address,
                timeout=timeout,
            )
            high_confidence.append({
                'contract_address': contract_address,
                'candidate_count': len(contract_candidates),
                'match_reasons': sorted({reason for item in contract_candidates for reason in item.match_reasons}),
            })
            address_signals[contract_address] = analyze_contract_transfers(transfers)
            victim_signals[contract_address] = analyze_contract_victims(transfers, owners)

        if open_license:
            high_confidence = []
            low_confidence = []

        return {
            'seed_contract': asdict(contract_meta),
            'seed_collection_stats': {
                'seed_nft_count': len(seed_nfts),
                'unique_token_uri_count': len({normalize_url(item.token_uri) for item in seed_nfts if normalize_url(item.token_uri)}),
                'unique_image_uri_count': len({normalize_url(item.image_uri) for item in seed_nfts if normalize_url(item.image_uri)}),
                'unique_name_count': len({normalize_name(item.name) for item in seed_nfts if normalize_name(item.name)}),
                'unique_symbol_count': len({normalize_symbol(item.symbol) for item in seed_nfts if normalize_symbol(item.symbol)}),
            },
            'duplicate_candidates': [asdict(item) for item in candidates],
            'legit_duplicates': legit_duplicates,
            'suspected_infringing_duplicates_high_confidence': high_confidence,
            'suspected_infringing_duplicates_low_confidence': low_confidence,
            'contract_level_summary': {
                contract_address: {
                    'candidate_count': len(items),
                    'high_confidence_token_count': sum(1 for item in items if item.confidence == 'high'),
                    'low_confidence_token_count': sum(1 for item in items if item.confidence == 'low'),
                }
                for contract_address, items in grouped.items()
            },
            'address_signals': address_signals,
            'victim_signals': victim_signals,
            'report_summary': {
                'open_license_detected': open_license,
                'candidate_contract_count': len(grouped),
                'high_confidence_contract_count': len(high_confidence),
                'low_confidence_contract_count': len(low_confidence),
                'legit_duplicate_contract_count': len(legit_duplicates),
            },
        }
    finally:
        if own_conn and conn is not None:
            conn.close()


def dump_results(payload: Dict[str, Any], output_path: Optional[str]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    Path(output_path).write_text(text, encoding='utf-8')


def _slugify_filename_part(value: str) -> str:
    text = unicodedata.normalize('NFKC', value or '').strip().casefold()
    text = re.sub(r'[^0-9a-zA-Z\u4e00-\u9fff]+', '_', text)
    text = text.strip('_')
    return text or 'unknown_collection'


def default_output_basename(payload: Dict[str, Any]) -> str:
    seed = payload.get('seed_contract') or {}
    name = str(seed.get('name') or '').strip()
    if not name:
        name = str(seed.get('contract_address') or 'unknown_collection')
    return f'top_contract_analysis__{_slugify_filename_part(name)}'


def write_default_outputs(payload: Dict[str, Any], output_path: str = '') -> tuple[Path, Path]:
    if output_path:
        json_path = Path(output_path)
    else:
        result_dir = Path.cwd() / 'result'
        result_dir.mkdir(parents=True, exist_ok=True)
        json_path = result_dir / f'{default_output_basename(payload)}.json'
    md_path = json_path.with_suffix('.md')
    dump_results(payload, str(json_path))
    md_path.write_text(render_human_readable_report(payload), encoding='utf-8')
    return json_path, md_path


def _format_ratio(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f'{value:.2%}'
    return 'n/a'


def render_human_readable_report(payload: Dict[str, Any]) -> str:
    seed = payload.get('seed_contract') or {}
    seed_stats = payload.get('seed_collection_stats') or {}
    summary = payload.get('report_summary') or {}
    high = payload.get('suspected_infringing_duplicates_high_confidence') or []
    low = payload.get('suspected_infringing_duplicates_low_confidence') or []
    legit = payload.get('legit_duplicates') or []
    address_signals = payload.get('address_signals') or {}
    victim_signals = payload.get('victim_signals') or {}

    lines = [
        '# Top NFT 合约重复样本分析报告',
        '',
        '## 种子合约',
        f"- 链: {seed.get('chain', '')}",
        f"- 合约地址: {seed.get('contract_address', '')}",
        f"- 名称: {seed.get('name', '')}",
        f"- 符号: {seed.get('symbol', '')}",
        f"- Token 类型: {seed.get('token_type', '')}",
        f"- 合约部署者: {seed.get('contract_deployer', '') or 'unknown'}",
        f"- 部署区块: {seed.get('deployed_block_number', 0)}",
        '',
        '## 种子集合统计',
        f"- 拉取到的种子 NFT 数: {seed_stats.get('seed_nft_count', 0)}",
        f"- 唯一 token URI 数: {seed_stats.get('unique_token_uri_count', 0)}",
        f"- 唯一 image URI 数: {seed_stats.get('unique_image_uri_count', 0)}",
        f"- 唯一规范化名称数: {seed_stats.get('unique_name_count', 0)}",
        f"- 唯一规范化符号数: {seed_stats.get('unique_symbol_count', 0)}",
        '',
        '## 摘要',
        f"- 检测到开放许可: {'是' if summary.get('open_license_detected') else '否'}",
        f"- 重复候选合约数: {summary.get('candidate_contract_count', 0)}",
        f"- 高置信疑似侵权合约数: {summary.get('high_confidence_contract_count', 0)}",
        f"- 低置信疑似侵权合约数: {summary.get('low_confidence_contract_count', 0)}",
        f"- 归为官方参与型重复的合约数: {summary.get('legit_duplicate_contract_count', 0)}",
        '',
        '## 高置信疑似侵权合约',
    ]

    if high:
        for item in high:
            lines.append(
                f"- {item.get('contract_address', '')}: {item.get('candidate_count', 0)} 个重复 NFT "
                f"| 命中原因={', '.join(item.get('match_reasons') or [])}"
            )
    else:
        lines.append('- 无')

    lines.extend(['', '## 低置信疑似侵权合约'])
    if low:
        for item in low:
            lines.append(
                f"- {item.get('contract_address', '')}: {item.get('candidate_count', 0)} 个重复 NFT "
                f"| 命中原因={', '.join(item.get('match_reasons') or [])}"
            )
    else:
        lines.append('- 无')

    lines.extend([
        '',
        '## 被算法归为官方参与型重复的合约',
        '- 说明: 该分组仅表示 mint 接收地址与官方地址集合存在交集。',
    ])
    if legit:
        for item in legit:
            recipients = ', '.join(item.get('mint_recipients') or [])
            lines.append(
                f"- {item.get('contract_address', '')}: {item.get('candidate_count', 0)} 个重复 NFT "
                f"| mint 接收地址(命中官方地址规则)={recipients}"
            )
    else:
        lines.append('- 无')

    lines.extend(['', '## 地址行为信号'])
    if address_signals:
        for contract, signal in address_signals.items():
            lines.extend([
                f"### {contract}",
                f"- Mint 地址数: {signal.get('mint_address_count', 0)}",
                f"- Mint 交易数: {signal.get('mint_count', 0)}",
                f"- 唯一接收地址数: {signal.get('unique_receiver_count', 0)}",
                f"- 循环交易边数: {signal.get('cycle_edge_count', 0)}",
                f"- 星状扩散中心数: {signal.get('star_distributor_count', 0)}",
                f"- Mint 到首次转手时间: {signal.get('mint_to_first_transfer_seconds', 0)} 秒",
                f"- 快速扩散: {'是' if signal.get('fast_spread') else '否'}",
            ])
    else:
        lines.append('- 无')

    lines.extend(['', '## 受害者信号'])
    if victim_signals:
        for contract, signal in victim_signals.items():
            lines.extend([
                f"### {contract}",
                f"- 当前持有地址数: {signal.get('owner_count', 0)}",
                f"- 套牢地址数: {signal.get('stuck_holder_count', 0)}",
                f"- 套牢地址占比: {_format_ratio(signal.get('stuck_holder_ratio'))}",
                f"- 疑似受害地址数: {signal.get('victim_wallet_count', 0)}",
            ])
    else:
        lines.append('- 无')

    return '\n'.join(lines) + '\n'


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Analyze duplicate NFT samples for a seed top-NFT contract.')
    parser.add_argument('--chain', default='ethereum')
    parser.add_argument('--seed-contract-address', required=True)
    parser.add_argument('--alchemy-api-key', default=os.getenv('ALCHEMY_API_KEY', ''))
    parser.add_argument('--alchemy-network', default='')
    parser.add_argument('--etherscan-api-key', default=os.getenv('ETHERSCAN_API_KEY', ''))
    parser.add_argument('--name-threshold', type=float, default=DEFAULT_NAME_THRESHOLD)
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument('--output', default='')
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    payload = analyze_seed_contract(
        chain=args.chain,
        seed_contract_address=args.seed_contract_address.lower(),
        alchemy_api_key=args.alchemy_api_key,
        alchemy_network=args.alchemy_network or None,
        etherscan_api_key=args.etherscan_api_key,
        name_threshold=args.name_threshold,
        timeout=args.timeout,
    )
    write_default_outputs(payload, args.output)
    return 0
