from __future__ import annotations

import json
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict, Optional

from .constants import DEFAULT_NETWORKS, RE_ARWEAVE_HTTP, RE_IPFS_HTTP, TRAILING_ID_PATTERNS


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
        for pattern in TRAILING_ID_PATTERNS:
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
    match = RE_IPFS_HTTP.match(text)
    if match:
        cid_path = match.group(1).split('?', 1)[0].split('#', 1)[0].rstrip('/')
        return f'ipfs:{cid_path}' if cid_path else None
    match = RE_ARWEAVE_HTTP.match(text)
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
