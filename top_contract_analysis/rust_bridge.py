from __future__ import annotations

import json
import re
import sys
import unicodedata
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from . import OwnerBalance, TransferRecord

_LOCAL_EXTENSION_DIR = Path(__file__).resolve().parent.parent / '.runtime' / 'pydeps'
if _LOCAL_EXTENSION_DIR.exists() and str(_LOCAL_EXTENSION_DIR) not in sys.path:
    sys.path.insert(0, str(_LOCAL_EXTENSION_DIR))

try:
    from top_contract_analysis_rust import analyze_transfer_signals as _rust_analyze_transfer_signals
    from top_contract_analysis_rust import analyze_victim_signals as _rust_analyze_victim_signals
    from top_contract_analysis_rust import metadata_document_from_json as _rust_metadata_document_from_json
    from top_contract_analysis_rust import metadata_keywords as _rust_metadata_keywords
    from top_contract_analysis_rust import score_metadata_pairs as _rust_score_metadata_pairs
    from top_contract_analysis_rust import score_name_pairs as _rust_score_name_pairs

    MATCHING_BACKEND = 'rust'
except ImportError:  # pragma: no cover
    _rust_analyze_transfer_signals = None
    _rust_analyze_victim_signals = None
    _rust_metadata_document_from_json = None
    _rust_metadata_keywords = None
    _rust_score_metadata_pairs = None
    _rust_score_name_pairs = None
    MATCHING_BACKEND = 'python'


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
_TOKEN_RE = re.compile(r'[\w]+', re.UNICODE)
_ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'


def _strip_trailing_number_suffix(raw: str) -> str:
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


def _normalize_name(raw: str) -> str:
    return re.sub(r'\s+', ' ', _strip_trailing_number_suffix(raw)).strip().casefold()


def _normalize_text(raw: str) -> str:
    text = unicodedata.normalize('NFKC', raw or '')
    text = text.casefold()
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _flatten_metadata(value: Any, out: list[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            key_norm = str(key).casefold()
            if key_norm in {
                'description',
                'trait_type',
                'value',
                'display_type',
                'image',
                'image_url',
                'animation_url',
                'external_url',
            }:
                _flatten_metadata(item, out)
                continue
            if key_norm in {'attributes', 'metadata', 'rawmetadata', 'raw'}:
                _flatten_metadata(item, out)
    elif isinstance(value, list):
        for item in value:
            _flatten_metadata(item, out)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped:
            out.append(stripped)


@lru_cache(maxsize=200_000)
def metadata_document_from_json(raw: str) -> str:
    if not raw:
        return ''
    if _rust_metadata_document_from_json is not None:
        return str(_rust_metadata_document_from_json(raw))
    try:
        payload = json.loads(raw)
    except Exception:
        return _normalize_text(raw)
    parts: list[str] = []
    _flatten_metadata(payload, parts)
    return _normalize_text(' '.join(parts))


def metadata_keywords(document: str, *, limit: int = 8) -> list[str]:
    if _rust_metadata_keywords is not None:
        return list(_rust_metadata_keywords(document, limit=limit))
    counts: dict[str, int] = {}
    for token in _TOKEN_RE.findall(document):
        if len(token) < 4:
            continue
        key = token.casefold()
        counts[key] = counts.get(key, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    return [token for token, _ in ranked[:limit]]


def _score_name_pairs_python(left: Sequence[str], right: Sequence[str]) -> list[float]:
    scores: list[float] = []
    for left_item, right_item in zip(left, right):
        left_norm = _normalize_name(left_item)
        right_norm = _normalize_name(right_item)
        if not left_norm or not right_norm:
            scores.append(0.0)
            continue
        if left_norm == right_norm:
            scores.append(100.0)
            continue
        scores.append(SequenceMatcher(None, left_norm, right_norm).ratio() * 100.0)
    return scores


def _tokenize(document: str) -> set[str]:
    return {token.casefold() for token in _TOKEN_RE.findall(document) if len(token) >= 2}


def _score_metadata_pairs_python(left: Sequence[str], right: Sequence[str]) -> list[float]:
    scores: list[float] = []
    for left_item, right_item in zip(left, right):
        left_doc = metadata_document_from_json(left_item)
        right_doc = metadata_document_from_json(right_item)
        if not left_doc or not right_doc:
            scores.append(0.0)
            continue
        left_tokens = _tokenize(left_doc)
        right_tokens = _tokenize(right_doc)
        overlap = len(left_tokens & right_tokens)
        union = len(left_tokens | right_tokens)
        jaccard = (overlap / union) if union else 0.0
        sequence = SequenceMatcher(None, left_doc, right_doc).ratio()
        scores.append((0.45 * jaccard) + (0.55 * sequence))
    return scores


def _validate_parallel_inputs(left: Sequence[str], right: Sequence[str]) -> None:
    if len(left) != len(right):
        raise ValueError('left and right sequences must have identical lengths')


def score_name_pairs(left: Sequence[str], right: Sequence[str]) -> list[float]:
    _validate_parallel_inputs(left, right)
    if not left:
        return []
    if _rust_score_name_pairs is not None:
        return list(_rust_score_name_pairs(list(left), list(right)))
    return _score_name_pairs_python(left, right)


def score_metadata_pairs(left: Sequence[str], right: Sequence[str]) -> list[float]:
    _validate_parallel_inputs(left, right)
    if not left:
        return []
    if _rust_score_metadata_pairs is not None:
        return list(_rust_score_metadata_pairs(list(left), list(right)))
    return _score_metadata_pairs_python(left, right)


def _calculate_cycle_edge_count(transfers: Sequence['TransferRecord']) -> int:
    seen_pairs = set()
    cycle_pairs = set()
    for transfer in transfers:
        if transfer.from_address == _ZERO_ADDRESS or transfer.to_address == _ZERO_ADDRESS:
            continue
        pair = (transfer.from_address, transfer.to_address)
        reverse = (transfer.to_address, transfer.from_address)
        if reverse in seen_pairs:
            cycle_pairs.add(tuple(sorted(pair)))
        seen_pairs.add(pair)
    return len(cycle_pairs)


def _calculate_star_distributors(transfers: Sequence['TransferRecord']) -> int:
    outgoing: dict[str, set[str]] = {}
    incoming: dict[str, int] = {}
    for transfer in transfers:
        if transfer.from_address == _ZERO_ADDRESS or transfer.to_address == _ZERO_ADDRESS:
            continue
        outgoing.setdefault(transfer.from_address, set()).add(transfer.to_address)
        incoming[transfer.to_address] = incoming.get(transfer.to_address, 0) + 1
    count = 0
    for sender, recipients in outgoing.items():
        if len(recipients) >= 3 and incoming.get(sender, 0) <= 1:
            count += 1
    return count


def _analyze_transfer_signals_python(transfers: Sequence['TransferRecord']) -> dict[str, Any]:
    mint_transfers = [row for row in transfers if row.from_address == _ZERO_ADDRESS]
    non_mint = [row for row in transfers if row.from_address != _ZERO_ADDRESS]
    receivers = {row.to_address for row in transfers if row.to_address and row.to_address != _ZERO_ADDRESS}
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


def analyze_transfer_signals(transfers: Sequence['TransferRecord']) -> dict[str, Any]:
    if _rust_analyze_transfer_signals is not None:
        packed = [(row.from_address, row.to_address, int(row.block_time or 0)) for row in transfers]
        return dict(_rust_analyze_transfer_signals(packed))
    return _analyze_transfer_signals_python(transfers)


def _analyze_victim_signals_python(
    transfers: Sequence['TransferRecord'],
    owners: Sequence['OwnerBalance'],
) -> dict[str, Any]:
    active_sellers = {
        row.from_address for row in transfers
        if row.from_address and row.from_address != _ZERO_ADDRESS
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


def analyze_victim_signals(
    transfers: Sequence['TransferRecord'],
    owners: Sequence['OwnerBalance'],
) -> dict[str, Any]:
    if _rust_analyze_victim_signals is not None:
        packed_transfers = [(row.from_address, row.to_address, int(row.block_time or 0)) for row in transfers]
        packed_owners = [
            (owner.owner_address, any(balance > 0 for balance in owner.token_balances.values()))
            for owner in owners
        ]
        return dict(_rust_analyze_victim_signals(packed_transfers, packed_owners))
    return _analyze_victim_signals_python(transfers, owners)


def analyze_victim_signals_from_active_sellers(
    active_sellers: Sequence[str],
    owners: Sequence['OwnerBalance'],
) -> dict[str, Any]:
    active_seller_set = {seller for seller in active_sellers if seller and seller != _ZERO_ADDRESS}
    owner_count = 0
    stuck_holder_count = 0
    for owner in owners:
        has_positive_balance = any(balance > 0 for balance in owner.token_balances.values())
        if not has_positive_balance:
            continue
        owner_count += 1
        if owner.owner_address not in active_seller_set:
            stuck_holder_count += 1
    return {
        'owner_count': owner_count,
        'stuck_holder_count': stuck_holder_count,
        'stuck_holder_ratio': (stuck_holder_count / owner_count) if owner_count else 0.0,
        'victim_wallet_count': stuck_holder_count,
    }
