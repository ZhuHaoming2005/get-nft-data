from __future__ import annotations

# Example:
#   C:\Users\z1766\.conda\envs\codex\python.exe -m top_contract_analysis ^
#     --chain ethereum ^
#     --seed-contract-address 0xbd3531da5cf5857e7cfaa92426877b022e612cf8 ^
#     --alchemy-api-key your_alchemy_api_key ^
#     --etherscan-api-key your_etherscan_api_key

import argparse
import contextlib
import json
import logging
import math
import os
import re
import sys
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
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
# Safety cap: if a single seed recall exceeds this many token rows, we emit a warning
# and truncate before passing to find_duplicate_candidates.  Set to 0 to disable.
DEFAULT_MAX_RECALL_ROWS = 50_000

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
    metadata_json: str = ''
    metadata_doc: str = ''


@dataclass(frozen=True)
class DatabaseNFTRecord:
    contract_address: str
    token_id: str
    token_uri: str
    image_uri: str
    name: str
    symbol: str
    metadata_json: str = ''
    metadata_doc: str = ''


@dataclass(frozen=True)
class ContractNameRecord:
    contract_address: str
    name_norm: str


@dataclass(frozen=True)
class ContractSignal:
    contract_address: str
    token_count: int
    uri_match_count: int
    image_match_count: int
    symbol_match: bool
    name_prefix_match: bool
    keyword_match: bool


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
class NFTSaleRecord:
    contract_address: str
    token_id: str
    tx_hash: str
    block_number: int
    log_index: int
    bundle_index: int
    buyer_address: str
    seller_address: str
    marketplace: str
    taker: str
    payment_token_symbol: str
    payment_token_address: str = ''
    price_eth: Optional[float] = None
    seller_fee_eth: float = 0.0
    protocol_fee_eth: float = 0.0
    royalty_fee_eth: float = 0.0
    source: str = 'alchemy'
    is_native_eth: bool = False


@dataclass(frozen=True)
class TransactionReceiptRecord:
    tx_hash: str
    block_number: int
    transaction_index: int
    from_address: str
    gas_used: int
    effective_gas_price_wei: int


@dataclass(frozen=True)
class EthTransferRecord:
    tx_hash: str
    block_number: int
    from_address: str
    to_address: str
    value_eth: float
    category: str


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
        contract_signals: Optional[Dict[str, 'ContractSignal']] = None,
    ) -> None:
        self.nft_rows = nft_rows or []
        self.contract_names = contract_names or []
        self.symbol_contracts = symbol_contracts or {}
        self.contract_signals = contract_signals or {}


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


def build_nft_metadata_json(raw_nft: Dict[str, Any], *, token_uri: str = '', image_uri: str = '') -> str:
    payload: Dict[str, Any] = {}
    raw_meta = raw_nft.get('rawMetadata') or raw_nft.get('metadata') or {}
    if isinstance(raw_meta, dict):
        payload.update(raw_meta)
    for source_key, target_key in (
        ('title', 'name'),
        ('name', 'name'),
        ('description', 'description'),
    ):
        value = raw_nft.get(source_key)
        if value and target_key not in payload:
            payload[target_key] = value
    if token_uri and 'token_uri' not in payload:
        payload['token_uri'] = token_uri
    if image_uri and 'image' not in payload:
        payload['image'] = image_uri
    if not payload:
        return ''
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


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


def _build_requests_session():
    _require_requests()
    session = requests.Session()
    try:
        from requests.adapters import HTTPAdapter
    except Exception:  # pragma: no cover
        return session
    adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


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
    session=None,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    client = session or requests
    for attempt in range(1, retries + 1):
        try:
            response = client.get(url, timeout=timeout)
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
    session=None,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    client = session or requests
    for attempt in range(1, retries + 1):
        try:
            response = client.post(url, json=payload, timeout=timeout)
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
    session=None,
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
        session=session,
    )
    result = body.get('result') or {}
    return _parse_alchemy_block_timestamp(result.get('timestamp'))


def _extract_seed_nfts(payload: Dict[str, Any], *, chain: str, contract_address: str) -> Tuple[List[SeedNFT], str]:
    from .rust_bridge import metadata_document_from_json

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
        metadata_json = build_nft_metadata_json(raw, token_uri=token_uri, image_uri=image_uri)
        rows.append(
            SeedNFT(
                chain=chain,
                contract_address=str(raw_contract.get('address') or contract_address).lower(),
                token_id=_normalize_token_id(raw_id.get('tokenId')),
                name=str(raw.get('title') or raw.get('name') or ''),
                symbol=str((raw.get('contractMetadata') or {}).get('symbol') or ''),
                token_uri=token_uri,
                image_uri=image_uri,
                metadata_json=metadata_json,
                metadata_doc=metadata_document_from_json(metadata_json),
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
    session=None,
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
        payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
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
    session=None,
) -> Dict[str, Any]:
    _require_requests()
    params = {
        'contractAddress': contract_address,
        'tokenId': token_id,
        'refreshCache': 'false',
    }
    url = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getNFTMetadata?{urlencode(params)}'
    return _alchemy_get_json(url=url, timeout=timeout, session=session)


def fetch_license_sample(
    *,
    api_key: str,
    network: str,
    chain: str,
    seed_nfts: Sequence[SeedNFT],
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> Dict[str, Any]:
    for nft in seed_nfts:
        if nft.token_id:
            return fetch_nft_metadata(
                api_key=api_key,
                network=network,
                contract_address=nft.contract_address,
                token_id=nft.token_id,
                timeout=timeout,
                session=session,
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
    session=None,
) -> ContractMetadata:
    _require_requests()
    params = {'contractAddress': contract_address}
    url = f'{_alchemy_nft_base(network)}/nft/v2/{api_key}/getContractMetadata?{urlencode(params)}'
    payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
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
    max_rows: Optional[int] = None,
) -> DatabaseSnapshot:
    """Load a matching snapshot from PostgreSQL.

    .. warning::
        This is the PostgreSQL fallback path intended for debugging and small-scale
        use only.  At 50M+ rows, token_uri / image_uri normalization and name fuzzy
        matching still run in Python after the server-side pre-filter, making this
        path unsuitable for production batch jobs.  Use
        ``export_snapshot.export_chain_snapshot_to_parquet`` + ``DuckDBFeatureStore``
        for production scale.

    Args:
        max_rows: Safety cap on how many candidate rows to process in Python.
                  Rows beyond this limit are silently dropped.  ``None`` means
                  unlimited (original behaviour; not recommended at scale).
    """
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

    # Build a server-side pre-filter: push symbol and 8-char name prefix matching
    # down to PostgreSQL so the cursor returns a much smaller candidate set.
    # token_uri / image_uri normalization (IPFS CID extraction etc.) can't be
    # replicated in SQL without custom functions, so those still run in Python.
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
    # token_uri and image_uri: include unconditionally when seed keys exist so
    # Python-level normalization can still catch IPFS/Arweave matches.
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
                   coalesce(name, ''), coalesce(symbol, '')
            FROM {table}
            WHERE {where_clause}
            ''',
            query_args,
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
                            metadata_json='',
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
    from .rust_bridge import metadata_document_from_json, metadata_keywords, score_metadata_pairs, score_name_pairs

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
    for idx, score in zip(metadata_pair_indices, score_metadata_pairs(metadata_left, metadata_right)):
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


def fetch_alchemy_contract_transfers(
    *,
    api_key: str,
    network: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
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
        body = _alchemy_post_json(url=url, payload=payload, timeout=timeout, session=session)
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
                        session=session,
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
    session=None,
) -> List[TransferRecord]:
    _require_requests()
    chain_id = ETHERSCAN_CHAIN_IDS.get(chain.lower())
    if not chain_id:
        raise ValueError(f'unsupported chain for etherscan fallback: {chain}')
    action = 'token1155tx' if token_type.upper() == 'ERC1155' else 'tokennfttx'
    base_url = 'https://api.etherscan.io/v2/api'
    page = 1
    transfers: List[TransferRecord] = []
    client = session or requests
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
        response = client.get(url, timeout=timeout)
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
    session=None,
) -> List[TransferRecord]:
    try:
        return fetch_alchemy_contract_transfers(
            api_key=alchemy_api_key,
            network=alchemy_network,
            contract_address=contract_address,
            timeout=timeout,
            session=session,
        )
    except Exception as exc:
        logger.warning('alchemy transfer fetch failed for %s: %s', contract_address, exc)
        return fetch_etherscan_contract_transfers(
            api_key=etherscan_api_key,
            chain=chain,
            contract_address=contract_address,
            token_type=token_type,
            timeout=timeout,
            session=session,
        )


def fetch_contract_owners(
    *,
    api_key: str,
    network: str,
    contract_address: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
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
        payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
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


def _decode_fee_eth(payload: Dict[str, Any]) -> tuple[float, str, str]:
    amount_raw = str(payload.get('amount') or '0').strip()
    symbol = str(payload.get('symbol') or '').strip().upper()
    token_address = str(payload.get('contractAddress') or payload.get('tokenAddress') or '').lower()
    try:
        decimals = int(payload.get('decimals') or 18)
    except (TypeError, ValueError):
        decimals = 18
    try:
        amount = int(amount_raw or '0')
    except ValueError:
        amount = 0
    return amount / float(10 ** max(decimals, 0)), symbol, token_address


def _alchemy_sales_base(network: str, api_key: str) -> str:
    return f'https://{network}.g.alchemy.com/nft/v2/{api_key}/getNFTSales'


def _looks_like_real_api_key(value: str) -> bool:
    text = (value or '').strip()
    if len(text) < 12:
        return False
    return text.casefold() not in {'alchemy', 'etherscan', 'opensea', 'key'}


def fetch_alchemy_nft_sales(
    *,
    api_key: str,
    network: str,
    contract_address: str,
    token_id: str = '',
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> List[NFTSaleRecord]:
    _require_requests()
    endpoint = _alchemy_sales_base(network, api_key)
    page_key = ''
    rows: List[NFTSaleRecord] = []
    while True:
        params = {
            'fromBlock': '0',
            'toBlock': 'latest',
            'order': 'asc',
            'contractAddress': contract_address,
        }
        if token_id:
            params['tokenId'] = token_id
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
        for item in payload.get('nftSales') or []:
            seller_fee_eth, fee_symbol, fee_token_address = _decode_fee_eth(item.get('sellerFee') or {})
            protocol_fee_eth, protocol_symbol, protocol_token_address = _decode_fee_eth(item.get('protocolFee') or {})
            royalty_fee_eth, royalty_symbol, royalty_token_address = _decode_fee_eth(item.get('royaltyFee') or {})
            symbols = {value for value in [fee_symbol, protocol_symbol, royalty_symbol] if value}
            native_eth = bool(symbols) and symbols == {'ETH'}
            payment_symbol = fee_symbol or protocol_symbol or royalty_symbol
            payment_token_address = fee_token_address or protocol_token_address or royalty_token_address
            rows.append(
                NFTSaleRecord(
                    contract_address=str(item.get('contractAddress') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('tokenId') or ''),
                    tx_hash=str(item.get('transactionHash') or '').lower(),
                    block_number=int(item.get('blockNumber') or 0),
                    log_index=int(item.get('logIndex') or 0),
                    bundle_index=int(item.get('bundleIndex') or 0),
                    buyer_address=str(item.get('buyerAddress') or '').lower(),
                    seller_address=str(item.get('sellerAddress') or '').lower(),
                    marketplace=str(item.get('marketplace') or ''),
                    taker=str(item.get('taker') or ''),
                    payment_token_symbol=payment_symbol,
                    payment_token_address=payment_token_address,
                    price_eth=(seller_fee_eth + protocol_fee_eth + royalty_fee_eth) if native_eth else None,
                    seller_fee_eth=seller_fee_eth,
                    protocol_fee_eth=protocol_fee_eth,
                    royalty_fee_eth=royalty_fee_eth,
                    source='alchemy',
                    is_native_eth=native_eth,
                )
            )
        page_key = str(payload.get('pageKey') or '')
        if not page_key:
            break
    return rows


def fetch_opensea_nft_events(
    *,
    contract_address: str,
    token_id: str = '',
    opensea_api_key: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> List[NFTSaleRecord]:
    _require_requests()
    client = session or requests
    headers = {'accept': 'application/json', 'x-api-key': opensea_api_key}
    params = {'event_type': 'sale'}
    if token_id:
        url = f'https://api.opensea.io/api/v2/events/chain/ethereum/contract/{contract_address}/nfts/{token_id}'
    else:
        url = 'https://api.opensea.io/api/v2/events'
        params['asset_contract_address'] = contract_address
        params['chain'] = 'ethereum'
    response = client.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    events = payload.get('asset_events') or payload.get('events') or []
    rows: List[NFTSaleRecord] = []
    for item in events:
        event_type = str(item.get('event_type') or item.get('eventType') or '').casefold()
        if event_type and event_type != 'sale':
            continue
        nft = item.get('nft') or item.get('asset') or {}
        payment = item.get('payment') or item.get('payment_token') or {}
        payment_symbol = str(payment.get('symbol') or item.get('payment_token_symbol') or '').upper()
        payment_token_address = str(payment.get('address') or payment.get('token_address') or '').lower()
        value_eth = None
        if payment_symbol == 'ETH':
            raw_value = item.get('payment_quantity') or item.get('price') or item.get('total_price') or '0'
            try:
                value_eth = int(str(raw_value), 10) / float(10 ** 18)
            except ValueError:
                try:
                    value_eth = float(raw_value)
                except (TypeError, ValueError):
                    value_eth = None
        rows.append(
            NFTSaleRecord(
                contract_address=str(
                    nft.get('contract')
                    or nft.get('contract_address')
                    or item.get('asset_contract_address')
                    or contract_address
                ).lower(),
                token_id=_normalize_token_id(nft.get('identifier') or nft.get('token_id') or token_id),
                tx_hash=str(item.get('transaction') or item.get('transaction_hash') or item.get('order_hash') or '').lower(),
                block_number=int(item.get('block_number') or 0),
                log_index=int(item.get('event_index') or item.get('log_index') or 0),
                bundle_index=int(item.get('bundle_index') or 0),
                buyer_address=str(item.get('to_account', {}).get('address') or item.get('winner_account', {}).get('address') or '').lower(),
                seller_address=str(item.get('from_account', {}).get('address') or item.get('seller', {}).get('address') or '').lower(),
                marketplace='opensea',
                taker=str(item.get('taker') or ''),
                payment_token_symbol=payment_symbol,
                payment_token_address=payment_token_address,
                price_eth=value_eth if payment_symbol == 'ETH' else None,
                seller_fee_eth=value_eth or 0.0 if payment_symbol == 'ETH' else 0.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='opensea',
                is_native_eth=(payment_symbol == 'ETH'),
            )
        )
    return rows


def fetch_contract_sales(
    *,
    alchemy_api_key: str,
    alchemy_network: str,
    contract_address: str,
    opensea_api_key: str = '',
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> List[NFTSaleRecord]:
    sales = fetch_alchemy_nft_sales(
        api_key=alchemy_api_key,
        network=alchemy_network,
        contract_address=contract_address,
        timeout=timeout,
        session=session,
    )
    if sales or not opensea_api_key:
        return sales
    return fetch_opensea_nft_events(
        contract_address=contract_address,
        opensea_api_key=opensea_api_key,
        timeout=timeout,
        session=session,
    )


def fetch_transaction_receipt(
    *,
    api_key: str,
    network: str,
    tx_hash: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> TransactionReceiptRecord:
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_getTransactionReceipt',
            'params': [tx_hash],
        },
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    return TransactionReceiptRecord(
        tx_hash=str(result.get('transactionHash') or tx_hash).lower(),
        block_number=int(str(result.get('blockNumber') or '0'), 16) if str(result.get('blockNumber') or '').startswith('0x') else int(result.get('blockNumber') or 0),
        transaction_index=int(str(result.get('transactionIndex') or '0'), 16) if str(result.get('transactionIndex') or '').startswith('0x') else int(result.get('transactionIndex') or 0),
        from_address=str(result.get('from') or '').lower(),
        gas_used=int(str(result.get('gasUsed') or '0'), 16) if str(result.get('gasUsed') or '').startswith('0x') else int(result.get('gasUsed') or 0),
        effective_gas_price_wei=int(str(result.get('effectiveGasPrice') or '0'), 16) if str(result.get('effectiveGasPrice') or '').startswith('0x') else int(result.get('effectiveGasPrice') or 0),
    )


def fetch_transaction_receipts_for_block(
    *,
    api_key: str,
    network: str,
    block_number: int,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> Dict[str, TransactionReceiptRecord]:
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'alchemy_getTransactionReceipts',
            'params': [{'blockNumber': hex(block_number)}],
        },
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    rows: Dict[str, TransactionReceiptRecord] = {}
    for item in result.get('receipts') or []:
        tx_hash = str(item.get('transactionHash') or '').lower()
        if not tx_hash:
            continue
        rows[tx_hash] = TransactionReceiptRecord(
            tx_hash=tx_hash,
            block_number=block_number,
            transaction_index=int(str(item.get('transactionIndex') or '0'), 16) if str(item.get('transactionIndex') or '').startswith('0x') else int(item.get('transactionIndex') or 0),
            from_address=str(item.get('from') or '').lower(),
            gas_used=int(str(item.get('gasUsed') or '0'), 16) if str(item.get('gasUsed') or '').startswith('0x') else int(item.get('gasUsed') or 0),
            effective_gas_price_wei=int(str(item.get('effectiveGasPrice') or '0'), 16) if str(item.get('effectiveGasPrice') or '').startswith('0x') else int(item.get('effectiveGasPrice') or 0),
        )
    return rows


def fetch_eth_balance(
    *,
    api_key: str,
    network: str,
    address: str,
    block_number: int,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> float:
    if block_number < 0:
        return 0.0
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_getBalance',
            'params': [address, hex(block_number)],
        },
        timeout=timeout,
        session=session,
    )
    value = str(body.get('result') or '0x0')
    return int(value, 16) / float(10 ** 18)


def _fetch_address_eth_transfers(
    *,
    api_key: str,
    network: str,
    block_number: int,
    address: str,
    direction: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> List[EthTransferRecord]:
    params: Dict[str, Any] = {
        'fromBlock': hex(block_number),
        'toBlock': hex(block_number),
        'category': ['external', 'internal'],
        'withMetadata': False,
        'excludeZeroValue': True,
        'maxCount': '0x3e8',
        'order': 'asc',
    }
    if direction == 'from':
        params['fromAddress'] = address
    else:
        params['toAddress'] = address

    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'alchemy_getAssetTransfers',
            'params': [params],
        },
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    rows: List[EthTransferRecord] = []
    for item in result.get('transfers') or []:
        value_raw = item.get('value')
        if value_raw is None:
            value_raw = (item.get('rawContract') or {}).get('value')
        if isinstance(value_raw, str) and value_raw.startswith('0x'):
            value_eth = int(value_raw, 16) / float(10 ** 18)
        else:
            try:
                value_eth = float(value_raw or 0)
            except (TypeError, ValueError):
                value_eth = 0.0
        rows.append(
            EthTransferRecord(
                tx_hash=str(item.get('hash') or '').lower(),
                block_number=block_number,
                from_address=str(item.get('from') or '').lower(),
                to_address=str(item.get('to') or '').lower(),
                value_eth=value_eth,
                category=str(item.get('category') or ''),
            )
        )
    return rows


def fetch_same_block_eth_transfers_for_address(
    *,
    api_key: str,
    network: str,
    block_number: int,
    address: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> List[EthTransferRecord]:
    rows = _fetch_address_eth_transfers(
        api_key=api_key,
        network=network,
        block_number=block_number,
        address=address,
        direction='from',
        timeout=timeout,
        session=session,
    )
    rows.extend(
        _fetch_address_eth_transfers(
            api_key=api_key,
            network=network,
            block_number=block_number,
            address=address,
            direction='to',
            timeout=timeout,
            session=session,
        )
    )
    deduped: Dict[tuple[str, str, str, float], EthTransferRecord] = {}
    for row in rows:
        key = (row.tx_hash, row.from_address, row.to_address, row.value_eth)
        deduped[key] = row
    return list(deduped.values())


def calculate_sale_eth_metrics(
    *,
    sale: NFTSaleRecord,
    purchase_receipt: TransactionReceiptRecord,
    base_balance_eth: float,
    same_block_transfers: Sequence[EthTransferRecord],
    receipts_by_hash: Dict[str, TransactionReceiptRecord],
) -> Dict[str, Any]:
    metrics = {
        'buy_before_eth_balance': None,
        'buy_amount_eth': sale.price_eth,
        'buy_total_eth_out': sale.price_eth,
        'buy_asset_ratio': None,
        'buy_asset_ratio_with_gas': None,
        'gas_not_attributed': False,
        'ratio_status': 'unavailable',
    }
    if not sale.is_native_eth or sale.price_eth is None:
        return metrics

    same_block_delta = 0.0
    for transfer in same_block_transfers:
        receipt = receipts_by_hash.get(transfer.tx_hash)
        if receipt is None:
            return metrics
        if receipt.transaction_index >= purchase_receipt.transaction_index:
            continue
        if transfer.to_address == sale.buyer_address:
            same_block_delta += transfer.value_eth
        if transfer.from_address == sale.buyer_address:
            same_block_delta -= transfer.value_eth

    buy_before_eth_balance = base_balance_eth + same_block_delta
    buy_total_eth_out = sale.price_eth
    gas_not_attributed = purchase_receipt.from_address != sale.buyer_address
    if not gas_not_attributed:
        buy_total_eth_out += (purchase_receipt.gas_used * purchase_receipt.effective_gas_price_wei) / float(10 ** 18)
    metrics.update(
        {
            'buy_before_eth_balance': buy_before_eth_balance,
            'buy_total_eth_out': buy_total_eth_out,
            'gas_not_attributed': gas_not_attributed,
        }
    )
    if buy_before_eth_balance > 0:
        metrics['buy_asset_ratio'] = sale.price_eth / buy_before_eth_balance
        metrics['buy_asset_ratio_with_gas'] = buy_total_eth_out / buy_before_eth_balance
        metrics['ratio_status'] = 'ok'
    return metrics


def analyze_contract_transfers(transfers: Sequence[TransferRecord]) -> Dict[str, Any]:
    from .rust_bridge import analyze_transfer_signals

    return analyze_transfer_signals(transfers)


def analyze_contract_victims(
    transfers: Sequence[TransferRecord],
    owners: Sequence[OwnerBalance],
) -> Dict[str, Any]:
    from .rust_bridge import analyze_victim_signals

    return analyze_victim_signals(transfers, owners)


def _transfer_sort_key(transfer: TransferRecord) -> tuple[int, int, str]:
    return (int(transfer.block_number or 0), int(transfer.log_index or 0), transfer.tx_hash)


def _sale_sort_key(sale: NFTSaleRecord) -> tuple[int, int, int, str]:
    return (int(sale.block_number or 0), int(sale.log_index or 0), int(sale.bundle_index or 0), sale.tx_hash)


def build_infringing_token_records(
    *,
    contract_address: str,
    contract_candidates: Sequence[DuplicateCandidate],
    transfers: Sequence[TransferRecord],
    official_addresses: set[str],
) -> List[Dict[str, Any]]:
    transfers_by_token: Dict[str, List[TransferRecord]] = defaultdict(list)
    for transfer in transfers:
        if transfer.token_id:
            transfers_by_token[transfer.token_id].append(transfer)

    rows: List[Dict[str, Any]] = []
    for candidate in sorted(contract_candidates, key=lambda item: (item.token_id, item.contract_address)):
        token_transfers = sorted(transfers_by_token.get(candidate.token_id, []), key=_transfer_sort_key)
        mint_transfer = next((row for row in token_transfers if row.from_address == ZERO_ADDRESS), None)
        first_transfer = token_transfers[0] if token_transfers else None
        minter_address = ''
        mint_tx_hash = ''
        mint_block = 0
        first_transfer_time = 0
        if mint_transfer is not None:
            minter_address = mint_transfer.to_address
            mint_tx_hash = mint_transfer.tx_hash
            mint_block = mint_transfer.block_number
            first_transfer_time = mint_transfer.block_time
        elif first_transfer is not None:
            minter_address = first_transfer.to_address
            mint_tx_hash = first_transfer.tx_hash
            mint_block = first_transfer.block_number
            first_transfer_time = first_transfer.block_time
        rows.append(
            {
                'contract_address': contract_address,
                'token_id': candidate.token_id,
                'mint_tx_hash': mint_tx_hash,
                'mint_block': mint_block,
                'minter_address': minter_address,
                'first_transfer_time': first_transfer_time,
                'history_window': 'full',
                'match_reasons': list(candidate.match_reasons),
                'official_or_legit_reissue': bool(minter_address and minter_address in official_addresses),
            }
        )
    return rows


def build_malicious_address_records(
    *,
    contract_address: str,
    transfers: Sequence[TransferRecord],
    infringing_tokens: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    relevant_token_ids = {str(item.get('token_id') or '') for item in infringing_tokens if item.get('token_id')}
    mint_addresses = {str(item.get('minter_address') or '') for item in infringing_tokens if item.get('minter_address')}
    outgoing: Dict[str, set[str]] = defaultdict(set)
    cycle_counts: Dict[str, int] = defaultdict(int)
    seen_pairs: set[tuple[str, str]] = set()
    rapid_addresses: set[str] = set()
    mint_times: Dict[str, int] = {}

    for transfer in sorted(transfers, key=_transfer_sort_key):
        if relevant_token_ids and transfer.token_id not in relevant_token_ids:
            continue
        if transfer.from_address == ZERO_ADDRESS:
            if transfer.to_address:
                mint_times[transfer.token_id] = transfer.block_time
            continue
        if transfer.from_address and transfer.to_address:
            outgoing[transfer.from_address].add(transfer.to_address)
            pair = (transfer.from_address, transfer.to_address)
            reverse = (transfer.to_address, transfer.from_address)
            if reverse in seen_pairs:
                cycle_counts[transfer.from_address] += 1
                cycle_counts[transfer.to_address] += 1
            seen_pairs.add(pair)
        mint_time = mint_times.get(transfer.token_id, 0)
        if mint_time and transfer.block_time and transfer.block_time - mint_time <= 24 * 3600:
            if transfer.from_address:
                rapid_addresses.add(transfer.from_address)
            if transfer.to_address:
                rapid_addresses.add(transfer.to_address)

    candidate_addresses = sorted(
        {
            *mint_addresses,
            *outgoing.keys(),
            *(transfer.to_address for transfer in transfers if transfer.to_address and transfer.token_id in relevant_token_ids),
        }
    )
    rows: List[Dict[str, Any]] = []
    for address in candidate_addresses:
        if not address:
            continue
        mint_role = address in mint_addresses
        wash_cycle_count = cycle_counts.get(address, 0)
        star_out_degree = len(outgoing.get(address, set()))
        if not mint_role and not wash_cycle_count and not star_out_degree and address not in rapid_addresses:
            continue
        rows.append(
            {
                'address': address,
                'mint_role': mint_role,
                'wash_cycle_count': wash_cycle_count,
                'star_out_degree': star_out_degree,
                'rapid_spread_contracts': [contract_address] if address in rapid_addresses else [],
                'evidence_contracts': [contract_address],
            }
        )
    return rows


def build_victim_address_records(
    *,
    contract_address: str,
    sales: Sequence[NFTSaleRecord],
    transfers: Sequence[TransferRecord],
    owners: Sequence[OwnerBalance],
    sale_metrics_by_tx: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    owner_token_map: Dict[str, set[str]] = {}
    for owner in owners:
        held_tokens = {token_id for token_id, balance in owner.token_balances.items() if balance > 0}
        if held_tokens:
            owner_token_map[owner.owner_address] = held_tokens

    grouped: Dict[str, Dict[str, Any]] = {}
    last_buy_key: Dict[str, tuple[int, int, int, str]] = {}
    sorted_transfers = sorted(transfers, key=_transfer_sort_key)
    for sale in sorted(sales, key=_sale_sort_key):
        buyer = sale.buyer_address
        if not buyer:
            continue
        metrics = sale_metrics_by_tx.get(sale.tx_hash, {})
        later_transfer_out = any(
            transfer.token_id == sale.token_id
            and transfer.from_address == buyer
            and _transfer_sort_key(transfer) > (sale.block_number, sale.log_index, sale.tx_hash)
            for transfer in sorted_transfers
        )
        is_stuck = sale.token_id in owner_token_map.get(buyer, set()) and not later_transfer_out
        entry = grouped.setdefault(
            buyer,
            {
                'address': buyer,
                'buy_tx_hashes': [],
                'buy_amount_eth': 0.0,
                'buy_before_eth_balance': None,
                'buy_asset_ratio': None,
                'buy_asset_ratio_with_gas': None,
                'is_stuck': False,
                'last_buy_tx_hash': '',
                'ratio_status': 'unavailable',
            },
        )
        entry['buy_tx_hashes'].append(sale.tx_hash)
        if sale.is_native_eth and sale.price_eth is not None:
            entry['buy_amount_eth'] += sale.price_eth
        current_key = _sale_sort_key(sale)
        if buyer not in last_buy_key or current_key >= last_buy_key[buyer]:
            last_buy_key[buyer] = current_key
            entry['last_buy_tx_hash'] = sale.tx_hash
            entry['buy_before_eth_balance'] = metrics.get('buy_before_eth_balance')
            entry['buy_asset_ratio'] = metrics.get('buy_asset_ratio')
            entry['buy_asset_ratio_with_gas'] = metrics.get('buy_asset_ratio_with_gas')
            entry['ratio_status'] = metrics.get('ratio_status', 'unavailable')
        entry['is_stuck'] = bool(entry['is_stuck'] or is_stuck)
    return sorted(grouped.values(), key=lambda item: item['address'])


def build_fraud_trade_stats(
    *,
    contract_address: str,
    sales: Sequence[NFTSaleRecord],
    victim_addresses: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    native_sales = [sale for sale in sales if sale.is_native_eth and sale.price_eth is not None]
    return {
        contract_address: {
            'unique_buyers': len({sale.buyer_address for sale in sales if sale.buyer_address}),
            'native_eth_sale_count': len(native_sales),
            'native_eth_volume': sum(sale.price_eth or 0.0 for sale in native_sales),
            'stuck_wallet_count': sum(1 for item in victim_addresses if item.get('is_stuck')),
            'stuck_cost_eth': sum(float(item.get('buy_amount_eth') or 0.0) for item in victim_addresses if item.get('is_stuck')),
        }
    }


def _analyze_high_confidence_contract(
    *,
    chain: str,
    network: str,
    alchemy_api_key: str,
    etherscan_api_key: str,
    opensea_api_key: str,
    contract_address: str,
    contract_candidates: Sequence[DuplicateCandidate],
    token_type: str,
    official_addresses: set[str],
    timeout: int,
    signal_cache,
    session=None,
) -> Dict[str, Any]:
    cached = None
    if signal_cache is not None:
        cached = signal_cache.get(chain=chain, contract_address=contract_address, token_type=token_type)
    if cached is not None:
        mint_recipients = set(cached.get('mint_recipients') or [])
        active_sellers = cached.get('active_sellers') or []
        cached_address_signals = cached.get('address_signals') or {}
        cached_victim_signals = cached.get('victim_signals')
        transfers = cached.get('transfers') or []
        owners = cached.get('owners') or []
    else:
        transfers = fetch_contract_transfers(
            alchemy_api_key=alchemy_api_key,
            alchemy_network=network,
            etherscan_api_key=etherscan_api_key,
            chain=chain,
            contract_address=contract_address,
            token_type=token_type,
            timeout=timeout,
            session=session,
        )
        owners = []
        mint_recipients = {row.to_address for row in transfers if row.from_address == ZERO_ADDRESS}
        active_sellers = [
            row.from_address
            for row in transfers
            if row.from_address and row.from_address != ZERO_ADDRESS
        ]
        cached_address_signals = analyze_contract_transfers(transfers)
        cached_victim_signals = None

    infringing_tokens = build_infringing_token_records(
        contract_address=contract_address,
        contract_candidates=contract_candidates,
        transfers=transfers,
        official_addresses=official_addresses,
    )

    result = {
        'contract_address': contract_address,
        'candidate_count': len(contract_candidates),
        'match_reasons': sorted({reason for item in contract_candidates for reason in item.match_reasons}),
        'infringing_tokens': infringing_tokens,
    }
    if infringing_tokens and all(item.get('official_or_legit_reissue') for item in infringing_tokens):
        if signal_cache is not None and cached is None:
            signal_cache.put(
                chain=chain,
                contract_address=contract_address,
                token_type=token_type,
                transfers=transfers,
                owners=owners,
            )
        result['status'] = 'legit'
        result['mint_recipients'] = sorted(mint_recipients)
        return result

    if cached_victim_signals is None or not owners:
        owners = fetch_contract_owners(
            api_key=alchemy_api_key,
            network=network,
            contract_address=contract_address,
            timeout=timeout,
            session=session,
        )
        from .rust_bridge import analyze_victim_signals_from_active_sellers

        cached_victim_signals = analyze_victim_signals_from_active_sellers(active_sellers, owners)
        if signal_cache is not None:
            signal_cache.put(
                chain=chain,
                contract_address=contract_address,
                token_type=token_type,
                transfers=transfers,
                owners=owners,
            )

    sales: List[NFTSaleRecord] = []
    if _looks_like_real_api_key(alchemy_api_key) or opensea_api_key:
        try:
            sales = fetch_contract_sales(
                alchemy_api_key=alchemy_api_key,
                alchemy_network=network,
                contract_address=contract_address,
                opensea_api_key=opensea_api_key,
                timeout=timeout,
                session=session,
            )
        except Exception as exc:
            logger.warning('contract sales fetch failed for %s: %s', contract_address, exc)
    sale_metrics_by_tx: Dict[str, Dict[str, Any]] = {}
    receipts_by_block: Dict[int, Dict[str, TransactionReceiptRecord]] = {}
    for sale in sales:
        if not sale.is_native_eth or sale.price_eth is None:
            sale_metrics_by_tx[sale.tx_hash] = {
                'buy_before_eth_balance': None,
                'buy_amount_eth': None,
                'buy_total_eth_out': None,
                'buy_asset_ratio': None,
                'buy_asset_ratio_with_gas': None,
                'gas_not_attributed': False,
                'ratio_status': 'unavailable',
            }
            continue
        try:
            purchase_receipt = fetch_transaction_receipt(
                api_key=alchemy_api_key,
                network=network,
                tx_hash=sale.tx_hash,
                timeout=timeout,
                session=session,
            )
            base_balance_eth = fetch_eth_balance(
                api_key=alchemy_api_key,
                network=network,
                address=sale.buyer_address,
                block_number=sale.block_number - 1,
                timeout=timeout,
                session=session,
            )
            same_block_transfers = fetch_same_block_eth_transfers_for_address(
                api_key=alchemy_api_key,
                network=network,
                block_number=sale.block_number,
                address=sale.buyer_address,
                timeout=timeout,
                session=session,
            )
            block_receipts: Dict[str, TransactionReceiptRecord] = {}
            if same_block_transfers:
                block_receipts = receipts_by_block.get(sale.block_number) or {}
                if not block_receipts:
                    block_receipts = fetch_transaction_receipts_for_block(
                        api_key=alchemy_api_key,
                        network=network,
                        block_number=sale.block_number,
                        timeout=timeout,
                        session=session,
                    )
                    receipts_by_block[sale.block_number] = block_receipts
            sale_metrics_by_tx[sale.tx_hash] = calculate_sale_eth_metrics(
                sale=sale,
                purchase_receipt=purchase_receipt,
                base_balance_eth=base_balance_eth,
                same_block_transfers=same_block_transfers,
                receipts_by_hash=block_receipts,
            )
        except Exception as exc:
            logger.warning('sale ETH metric computation failed for %s %s: %s', contract_address, sale.tx_hash, exc)
            sale_metrics_by_tx[sale.tx_hash] = {
                'buy_before_eth_balance': None,
                'buy_amount_eth': sale.price_eth,
                'buy_total_eth_out': sale.price_eth,
                'buy_asset_ratio': None,
                'buy_asset_ratio_with_gas': None,
                'gas_not_attributed': False,
                'ratio_status': 'unavailable',
            }

    malicious_addresses = build_malicious_address_records(
        contract_address=contract_address,
        transfers=transfers,
        infringing_tokens=infringing_tokens,
    )
    victim_addresses = build_victim_address_records(
        contract_address=contract_address,
        sales=sales,
        transfers=transfers,
        owners=owners,
        sale_metrics_by_tx=sale_metrics_by_tx,
    )
    fraud_trade_stats = build_fraud_trade_stats(
        contract_address=contract_address,
        sales=sales,
        victim_addresses=victim_addresses,
    )

    result['status'] = 'high'
    result['address_signals'] = cached_address_signals
    result['victim_signals'] = cached_victim_signals
    result['malicious_addresses'] = malicious_addresses
    result['victim_addresses'] = victim_addresses
    result['fraud_trade_stats'] = fraud_trade_stats
    return result


def analyze_seed_contract(
    *,
    chain: str,
    seed_contract_address: str,
    alchemy_api_key: str,
    alchemy_network: str | None = None,
    etherscan_api_key: str = '',
    opensea_api_key: str = '',
    conn=None,
    feature_store=None,
    signal_cache=None,
    name_threshold: float = DEFAULT_NAME_THRESHOLD,
    metadata_threshold: float = 0.55,
    timeout: int = DEFAULT_TIMEOUT,
    max_recall_rows: int = DEFAULT_MAX_RECALL_ROWS,
    max_tokens_per_contract: int = 500,
) -> Dict[str, Any]:
    network = normalize_network(chain, alchemy_network)
    own_conn = False
    if feature_store is None and conn is None:
        conn = get_conn()
        own_conn = True
    try:
        session_context = contextlib.closing(_build_requests_session()) if requests is not None else contextlib.nullcontext(None)
        with session_context as session:
            contract_meta = fetch_contract_metadata(
                api_key=alchemy_api_key,
                network=network,
                chain=chain,
                contract_address=seed_contract_address,
                timeout=timeout,
                session=session,
            )
            seed_nfts = fetch_seed_contract_nfts(
                api_key=alchemy_api_key,
                network=network,
                chain=chain,
                contract_address=seed_contract_address,
                timeout=timeout,
                session=session,
            )
            license_payload = fetch_license_sample(
                api_key=alchemy_api_key,
                network=network,
                chain=chain,
                seed_nfts=seed_nfts,
                timeout=timeout,
                session=session,
            )
            open_license = is_open_license_payload(license_payload)
            if feature_store is not None:
                snapshot = feature_store.load_snapshot(
                    chain,
                    seed_nfts=seed_nfts,
                    max_tokens_per_contract=max_tokens_per_contract,
                    max_recall_rows=max_recall_rows,
                )
            else:
                snapshot = load_database_snapshot(conn, chain, seed_nfts=seed_nfts)

            # Recall telemetry: log size of every snapshot so runaway seeds are visible.
            recall_token_count = len(snapshot.nft_rows)
            recall_contract_count = len({r.contract_address for r in snapshot.nft_rows})
            logger.info(
                'seed %s recall: %d tokens across %d candidate contracts',
                seed_contract_address, recall_token_count, recall_contract_count,
            )
            if feature_store is None and max_recall_rows > 0 and recall_token_count > max_recall_rows:
                logger.warning(
                    'seed %s recall %d tokens exceeds max_recall_rows=%d — truncating. '
                    'Increase max_recall_rows or tighten the seed set if results are incomplete.',
                    seed_contract_address, recall_token_count, max_recall_rows,
                )
                snapshot = DatabaseSnapshot(
                    nft_rows=snapshot.nft_rows[:max_recall_rows],
                    contract_names=snapshot.contract_names,
                    symbol_contracts=snapshot.symbol_contracts,
                )

            candidates = find_duplicate_candidates(
                seed_nfts,
                snapshot,
                name_threshold=name_threshold,
                metadata_threshold=metadata_threshold,
            )
            grouped = group_candidates_by_contract(candidates)

            official_addresses = {addr for addr in [contract_meta.contract_deployer, contract_meta.contract_address] if addr}
            legit_duplicates: List[Dict[str, Any]] = []
            high_confidence: List[Dict[str, Any]] = []
            low_confidence: List[Dict[str, Any]] = []
            address_signals: Dict[str, Any] = {}
            victim_signals: Dict[str, Any] = {}
            infringing_tokens: List[Dict[str, Any]] = []
            malicious_addresses: List[Dict[str, Any]] = []
            victim_addresses: List[Dict[str, Any]] = []
            fraud_trade_stats: Dict[str, Dict[str, Any]] = {}
            token_type = contract_meta.token_type or 'ERC721'
            high_confidence_items: List[Tuple[str, Sequence[DuplicateCandidate]]] = []

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
                high_confidence_items.append((contract_address, contract_candidates))

            if not open_license and high_confidence_items:
                max_workers = max(1, min(8, len(high_confidence_items)))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for result in executor.map(
                        lambda item: _analyze_high_confidence_contract(
                            chain=chain,
                            network=network,
                            alchemy_api_key=alchemy_api_key,
                            etherscan_api_key=etherscan_api_key,
                            opensea_api_key=opensea_api_key,
                            contract_address=item[0],
                            contract_candidates=item[1],
                            token_type=token_type,
                            official_addresses=official_addresses,
                            timeout=timeout,
                            signal_cache=signal_cache,
                            session=session,
                        ),
                        high_confidence_items,
                    ):
                        contract_address = result['contract_address']
                        if result['status'] == 'legit':
                            legit_duplicates.append({
                                'contract_address': contract_address,
                                'candidate_count': result['candidate_count'],
                                'mint_recipients': result['mint_recipients'],
                            })
                            continue
                        high_confidence.append({
                            'contract_address': contract_address,
                            'candidate_count': result['candidate_count'],
                            'match_reasons': result['match_reasons'],
                        })
                        address_signals[contract_address] = result['address_signals']
                        victim_signals[contract_address] = result['victim_signals']
                        infringing_tokens.extend(result.get('infringing_tokens') or [])
                        malicious_addresses.extend(result.get('malicious_addresses') or [])
                        victim_addresses.extend(result.get('victim_addresses') or [])
                        fraud_trade_stats.update(result.get('fraud_trade_stats') or {})

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
                'infringing_tokens': infringing_tokens,
                'malicious_addresses': malicious_addresses,
                'victim_addresses': victim_addresses,
                'fraud_trade_stats': fraud_trade_stats,
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


def write_outputs_to_directory(payload: Dict[str, Any], output_dir: str | Path) -> tuple[Path, Path]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / f'{default_output_basename(payload)}.json'
    md_path = json_path.with_suffix('.md')
    dump_results(payload, str(json_path))
    md_path.write_text(render_human_readable_report(payload), encoding='utf-8')
    return json_path, md_path


def build_batch_summary_payload(
    payloads: Sequence[Dict[str, Any]],
    output_index: Optional[Sequence[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    reports: list[Dict[str, Any]] = []
    total_candidates = 0
    total_high = 0
    total_low = 0
    total_legit = 0
    open_license_count = 0
    chains: list[str] = []
    output_index = output_index or []

    for index, payload in enumerate(payloads):
        seed = payload.get('seed_contract') or {}
        summary = payload.get('report_summary') or {}
        if summary.get('open_license_detected'):
            open_license_count += 1
        total_candidates += int(summary.get('candidate_contract_count') or 0)
        total_high += int(summary.get('high_confidence_contract_count') or 0)
        total_low += int(summary.get('low_confidence_contract_count') or 0)
        total_legit += int(summary.get('legit_duplicate_contract_count') or 0)
        chain = str(seed.get('chain') or '').strip()
        if chain:
            chains.append(chain)

        report_entry: Dict[str, Any] = {
            'seed_contract': seed,
            'report_summary': summary,
        }
        if index < len(output_index):
            report_entry['output_files'] = output_index[index]
        elif payload.get('output_files'):
            report_entry['output_files'] = payload['output_files']
        reports.append(report_entry)

    distinct_chains = sorted(set(chains))
    return {
        'batch_summary': {
            'seed_report_count': len(payloads),
            'chain': distinct_chains[0] if len(distinct_chains) == 1 else '',
            'chains': distinct_chains,
            'open_license_detected_count': open_license_count,
            'candidate_contract_count_total': total_candidates,
            'high_confidence_contract_count_total': total_high,
            'low_confidence_contract_count_total': total_low,
            'legit_duplicate_contract_count_total': total_legit,
            'generated_at': datetime.now(timezone.utc).isoformat(),
        },
        'seed_reports': reports,
    }


def render_batch_human_readable_report(payload: Dict[str, Any]) -> str:
    summary = payload.get('batch_summary') or {}
    seed_reports = payload.get('seed_reports') or []
    lines = [
        '# Top NFT 合约批量分析总报告',
        '',
        '## 汇总',
        f"- 种子合约报告数: {summary.get('seed_report_count', 0)}",
        f"- 链: {summary.get('chain') or ', '.join(summary.get('chains') or []) or 'unknown'}",
        f"- 检测到开放许可的 seed 数: {summary.get('open_license_detected_count', 0)}",
        f"- 重复候选合约总数: {summary.get('candidate_contract_count_total', 0)}",
        f"- 高置信疑似侵权合约总数: {summary.get('high_confidence_contract_count_total', 0)}",
        f"- 低置信疑似侵权合约总数: {summary.get('low_confidence_contract_count_total', 0)}",
        f"- 官方参与型重复合约总数: {summary.get('legit_duplicate_contract_count_total', 0)}",
        f"- 生成时间(UTC): {summary.get('generated_at', '')}",
        '',
        '## Seed 报告索引',
    ]

    if not seed_reports:
        lines.append('- 无')
    else:
        for item in seed_reports:
            seed = item.get('seed_contract') or {}
            report_summary = item.get('report_summary') or {}
            output_files = item.get('output_files') or {}
            seed_name = seed.get('name') or seed.get('contract_address') or 'unknown'
            lines.append(
                f"- {seed_name} ({seed.get('contract_address', '')}) | "
                f"高置信={report_summary.get('high_confidence_contract_count', 0)} | "
                f"低置信={report_summary.get('low_confidence_contract_count', 0)} | "
                f"官方参与={report_summary.get('legit_duplicate_contract_count', 0)} | "
                f"JSON={output_files.get('json', '')} | MD={output_files.get('markdown', '')}"
            )

    return '\n'.join(lines) + '\n'


def write_batch_summary_outputs(
    payloads: Sequence[Dict[str, Any]],
    output_dir: str | Path,
    output_index: Optional[Sequence[Dict[str, str]]] = None,
) -> tuple[Path, Path]:
    summary_payload = build_batch_summary_payload(payloads, output_index=output_index)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / 'top_contract_analysis__summary.json'
    md_path = target_dir / 'top_contract_analysis__summary.md'
    dump_results(summary_payload, str(json_path))
    md_path.write_text(render_batch_human_readable_report(summary_payload), encoding='utf-8')
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
    infringing_tokens = payload.get('infringing_tokens') or []
    malicious_addresses = payload.get('malicious_addresses') or []
    victim_addresses = payload.get('victim_addresses') or []
    fraud_trade_stats = payload.get('fraud_trade_stats') or {}

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

    lines.extend(['', '## 侵权 NFT 历史记录'])
    if infringing_tokens:
        for item in infringing_tokens:
            lines.append(
                f"- {item.get('contract_address', '')}#{item.get('token_id', '')}: "
                f"mint_tx={item.get('mint_tx_hash', '') or 'n/a'} | "
                f"mint_block={item.get('mint_block', 0)} | "
                f"minter={item.get('minter_address', '') or 'unknown'} | "
                f"match_reasons={', '.join(item.get('match_reasons') or [])}"
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

    lines.extend(['', '## 恶意地址画像'])
    if malicious_addresses:
        for item in malicious_addresses:
            lines.append(
                f"- {item.get('address', '')}: mint_role={'是' if item.get('mint_role') else '否'} | "
                f"wash_cycle_count={item.get('wash_cycle_count', 0)} | "
                f"star_out_degree={item.get('star_out_degree', 0)} | "
                f"evidence_contracts={', '.join(item.get('evidence_contracts') or [])}"
            )
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

    lines.extend(['', '## 被骗地址画像'])
    if victim_addresses:
        for item in victim_addresses:
            lines.append(
                f"- {item.get('address', '')}: buy_tx_count={len(item.get('buy_tx_hashes') or [])} | "
                f"买入金额(ETH)={item.get('buy_amount_eth', 0)} | "
                f"买入前 ETH 余额: {item.get('buy_before_eth_balance')} | "
                f"买入占比={_format_ratio(item.get('buy_asset_ratio'))} | "
                f"套牢={'是' if item.get('is_stuck') else '否'} | "
                f"last_buy_tx={item.get('last_buy_tx_hash', '') or 'n/a'}"
            )
    else:
        lines.append('- 无')

    lines.extend(['', '## 被骗交易与套牢资金'])
    if fraud_trade_stats:
        for contract, stats in fraud_trade_stats.items():
            lines.append(
                f"- {contract}: unique_buyers={stats.get('unique_buyers', 0)} | "
                f"native_eth_sale_count={stats.get('native_eth_sale_count', 0)} | "
                f"native_eth_volume={stats.get('native_eth_volume', 0)} | "
                f"stuck_wallet_count={stats.get('stuck_wallet_count', 0)} | "
                f"stuck_cost_eth={stats.get('stuck_cost_eth', 0)}"
            )
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
    parser.add_argument('--opensea-api-key', default=os.getenv('OPENSEA_API_KEY', ''))
    parser.add_argument('--name-threshold', type=float, default=DEFAULT_NAME_THRESHOLD)
    parser.add_argument('--metadata-threshold', type=float, default=0.55)
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument('--max-tokens-per-contract', type=int, default=500,
                        help='per-contract token cap in DuckDB recall query (default: 500)')
    parser.add_argument('--max-recall-rows', type=int, default=DEFAULT_MAX_RECALL_ROWS,
                        help='safety cap on total recall token rows (0 = unlimited)')
    parser.add_argument('--output', default='')
    parser.add_argument('--feature-parquet', default='', help='optional parquet snapshot path to preload into DuckDB')
    parser.add_argument('--feature-db', default=':memory:', help='duckdb database path for the feature store')
    parser.add_argument('--signal-cache-db', default=':memory:', help='duckdb database path for cached chain signals')
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    from .duckdb_store import DuckDBFeatureStore
    from .signal_cache import ContractSignalCache

    args = build_parser().parse_args(argv)
    feature_store = None
    signal_cache = ContractSignalCache(database_path=args.signal_cache_db)
    if args.feature_parquet:
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
        feature_store.load_parquet_dataset(args.chain, args.feature_parquet)
    elif args.feature_db != ':memory:' and Path(args.feature_db).exists():
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
    try:
        payload = analyze_seed_contract(
            chain=args.chain,
            seed_contract_address=args.seed_contract_address.lower(),
            alchemy_api_key=args.alchemy_api_key,
            alchemy_network=args.alchemy_network or None,
            etherscan_api_key=args.etherscan_api_key,
            opensea_api_key=args.opensea_api_key,
            feature_store=feature_store,
            signal_cache=signal_cache,
            name_threshold=args.name_threshold,
            metadata_threshold=args.metadata_threshold,
            timeout=args.timeout,
            max_tokens_per_contract=args.max_tokens_per_contract,
            max_recall_rows=args.max_recall_rows,
        )
        write_default_outputs(payload, args.output)
    finally:
        if feature_store is not None:
            feature_store.close()
        signal_cache.close()
    return 0
