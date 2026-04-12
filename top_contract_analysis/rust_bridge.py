from __future__ import annotations

import json
import re
import sys
import time
import unicodedata
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from . import NFTSaleRecord, OwnerBalance, TransferRecord

_LOCAL_EXTENSION_DIR = Path(__file__).resolve().parent.parent / '.runtime' / 'pydeps'
if _LOCAL_EXTENSION_DIR.exists() and str(_LOCAL_EXTENSION_DIR) not in sys.path:
    sys.path.insert(0, str(_LOCAL_EXTENSION_DIR))

try:
    from top_contract_analysis_rust import analyze_transfer_signals as _rust_analyze_transfer_signals
    from top_contract_analysis_rust import analyze_victim_signals as _rust_analyze_victim_signals
    from top_contract_analysis_rust import build_database_snapshot as _rust_build_database_snapshot
    from top_contract_analysis_rust import build_duplicate_candidates as _rust_build_duplicate_candidates
    from top_contract_analysis_rust import build_honest_address_records as _rust_build_honest_address_records
    from top_contract_analysis_rust import build_infringing_token_records as _rust_build_infringing_token_records
    from top_contract_analysis_rust import build_malicious_address_records as _rust_build_malicious_address_records
    from top_contract_analysis_rust import build_victim_address_records as _rust_build_victim_address_records
    from top_contract_analysis_rust import metadata_document_from_json as _rust_metadata_document_from_json
    from top_contract_analysis_rust import metadata_keywords as _rust_metadata_keywords
    from top_contract_analysis_rust import score_metadata_documents as _rust_score_metadata_documents
    from top_contract_analysis_rust import score_metadata_pairs as _rust_score_metadata_pairs
    from top_contract_analysis_rust import score_name_pairs as _rust_score_name_pairs

    MATCHING_BACKEND = 'rust'
except ImportError:  # pragma: no cover
    _rust_analyze_transfer_signals = None
    _rust_analyze_victim_signals = None
    _rust_build_database_snapshot = None
    _rust_build_duplicate_candidates = None
    _rust_build_honest_address_records = None
    _rust_build_infringing_token_records = None
    _rust_build_malicious_address_records = None
    _rust_build_victim_address_records = None
    _rust_metadata_document_from_json = None
    _rust_metadata_keywords = None
    _rust_score_metadata_documents = None
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


def metadata_document_from_json(raw: str) -> str:
    if not raw:
        return ''
    if _rust_metadata_document_from_json is not None:
        # Rust path: fast enough that a Python-level LRU cache adds more overhead
        # (string hashing + dict lookup) than it saves at 50M+ unique token scale.
        return str(_rust_metadata_document_from_json(raw))
    return _metadata_document_from_json_python(raw)


@lru_cache(maxsize=200_000)
def _metadata_document_from_json_python(raw: str) -> str:
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
    left_docs = [metadata_document_from_json(item) for item in left]
    right_docs = [metadata_document_from_json(item) for item in right]
    return _score_metadata_documents_python(left_docs, right_docs)


def _score_metadata_documents_python(left: Sequence[str], right: Sequence[str]) -> list[float]:
    scores: list[float] = []
    for left_item, right_item in zip(left, right):
        left_doc = _normalize_text(left_item)
        right_doc = _normalize_text(right_item)
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


def score_metadata_documents(left: Sequence[str], right: Sequence[str]) -> list[float]:
    _validate_parallel_inputs(left, right)
    if not left:
        return []
    if _rust_score_metadata_documents is not None:
        return list(_rust_score_metadata_documents(list(left), list(right)))
    return _score_metadata_documents_python(left, right)


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


def build_victim_address_records(
    sales: Sequence['NFTSaleRecord'],
    transfers: Sequence['TransferRecord'],
    owners: Sequence['OwnerBalance'],
    sale_metrics_by_tx: dict[str, dict[str, Any]],
    *,
    contract_address: str = '',
) -> list[dict[str, Any]]:
    del contract_address

    if _rust_build_victim_address_records is None:
        from .analysis import _build_victim_address_records_python

        return _build_victim_address_records_python(
            contract_address='',
            sales=sales,
            transfers=transfers,
            owners=owners,
            sale_metrics_by_tx=sale_metrics_by_tx,
        )

    from .sales import ETH_PRICED_SYMBOLS

    packed_sales = [
        (
            sale.token_id,
            sale.tx_hash,
            int(sale.block_number or 0),
            int(sale.log_index or 0),
            int(sale.bundle_index or 0),
            sale.buyer_address,
            bool(sale.price_eth is not None and sale.payment_token_symbol in ETH_PRICED_SYMBOLS),
            sale.price_eth,
            sale_metrics_by_tx.get(sale.tx_hash, {}).get('buy_before_eth_balance'),
            sale_metrics_by_tx.get(sale.tx_hash, {}).get('buy_asset_ratio'),
            sale_metrics_by_tx.get(sale.tx_hash, {}).get('buy_asset_ratio_with_gas'),
            str(sale_metrics_by_tx.get(sale.tx_hash, {}).get('ratio_status', 'unavailable')),
        )
        for sale in sales
    ]
    packed_transfers = [
        (
            transfer.token_id,
            transfer.tx_hash,
            int(transfer.block_number or 0),
            int(transfer.log_index or 0),
            transfer.from_address,
        )
        for transfer in transfers
    ]
    packed_owners = [
        (
            owner.owner_address,
            [token_id for token_id, balance in owner.token_balances.items() if balance > 0],
        )
        for owner in owners
    ]
    return list(_rust_build_victim_address_records(packed_sales, packed_transfers, packed_owners))


def build_malicious_address_records(
    *,
    contract_address: str,
    transfers: Sequence['TransferRecord'],
    infringing_tokens: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    if _rust_build_malicious_address_records is None:
        from .analysis import _build_malicious_address_records_python

        return _build_malicious_address_records_python(
            contract_address=contract_address,
            transfers=transfers,
            infringing_tokens=infringing_tokens,
        )

    packed_transfers = [
        (
            transfer.token_id,
            transfer.tx_hash,
            int(transfer.block_number or 0),
            int(transfer.log_index or 0),
            int(transfer.block_time or 0),
            transfer.from_address,
            transfer.to_address,
        )
        for transfer in transfers
    ]
    packed_infringing_tokens = [
        (
            str(item.get('token_id') or ''),
            str(item.get('minter_address') or ''),
        )
        for item in infringing_tokens
        if item.get('token_id')
    ]
    return list(
        _rust_build_malicious_address_records(
            contract_address,
            packed_transfers,
            packed_infringing_tokens,
        )
    )


def build_infringing_token_records(
    *,
    contract_address: str,
    contract_candidates: Sequence['DuplicateCandidate'],
    transfers: Sequence['TransferRecord'],
    official_addresses: set[str],
    candidate_open_license_by_token: dict[tuple[str, str], bool] | None = None,
) -> list[dict[str, Any]]:
    if _rust_build_infringing_token_records is None:
        from .analysis import _build_infringing_token_records_python

        return _build_infringing_token_records_python(
            contract_address=contract_address,
            contract_candidates=contract_candidates,
            transfers=transfers,
            official_addresses=official_addresses,
            candidate_open_license_by_token=candidate_open_license_by_token,
        )

    packed_candidates = [
        (
            candidate.token_id,
            list(candidate.match_reasons),
        )
        for candidate in contract_candidates
    ]
    packed_transfers = [
        (
            transfer.token_id,
            transfer.tx_hash,
            int(transfer.block_number or 0),
            int(transfer.log_index or 0),
            int(transfer.block_time or 0),
            transfer.from_address,
            transfer.to_address,
        )
        for transfer in transfers
    ]
    packed_official_addresses = sorted(address for address in official_addresses if address)
    packed_open_license = [
        (token_id, bool(flag))
        for (candidate_contract, token_id), flag in (candidate_open_license_by_token or {}).items()
        if candidate_contract == contract_address and token_id
    ]
    return list(
        _rust_build_infringing_token_records(
            contract_address,
            packed_candidates,
            packed_transfers,
            packed_official_addresses,
            packed_open_license,
        )
    )


def build_duplicate_candidates(
    *,
    seed_nfts: Sequence['SeedNFT'],
    snapshot_rows: Sequence['DatabaseNFTRecord'],
    name_threshold: float,
    metadata_threshold: float,
) -> list['DuplicateCandidate']:
    if _rust_build_duplicate_candidates is None:
        from .snapshot import _find_duplicate_candidates_python

        from .models import DatabaseSnapshot

        return _find_duplicate_candidates_python(
            seed_nfts,
            DatabaseSnapshot(nft_rows=list(snapshot_rows)),
            name_threshold=name_threshold,
            metadata_threshold=metadata_threshold,
        )

    from . import DuplicateCandidate

    packed_seed_nfts = [
        (
            nft.contract_address,
            nft.token_id,
            nft.name,
            nft.symbol,
            nft.token_uri,
            nft.image_uri,
            nft.metadata_json,
            nft.metadata_doc,
        )
        for nft in seed_nfts
    ]
    packed_snapshot_rows = [
        (
            row.contract_address,
            row.token_id,
            row.name,
            row.symbol,
            row.token_uri,
            row.image_uri,
            row.metadata_json,
            row.metadata_doc,
        )
        for row in snapshot_rows
    ]
    packed = _rust_build_duplicate_candidates(
        packed_seed_nfts,
        packed_snapshot_rows,
        float(name_threshold),
        float(metadata_threshold),
    )
    return [
        DuplicateCandidate(
            contract_address=contract_address,
            token_id=token_id,
            match_reasons=tuple(match_reasons),
            confidence=confidence,
            token_uri=token_uri,
            image_uri=image_uri,
            name=name,
            symbol=symbol,
        )
        for (
            contract_address,
            token_id,
            match_reasons,
            confidence,
            token_uri,
            image_uri,
            name,
            symbol,
        ) in packed
    ]


def build_honest_address_records(
    *,
    contract_address: str,
    transfers: Sequence['TransferRecord'],
    sales: Sequence['NFTSaleRecord'],
    owners: Sequence['OwnerBalance'],
    infringing_tokens: Sequence[dict[str, Any]],
    malicious_addresses: Sequence[dict[str, Any]],
    analysis_timestamp: int | None = None,
) -> list[dict[str, Any]]:
    if _rust_build_honest_address_records is None:
        from .analysis import _build_honest_address_records_python

        return _build_honest_address_records_python(
            contract_address=contract_address,
            transfers=transfers,
            sales=sales,
            owners=owners,
            infringing_tokens=infringing_tokens,
            malicious_addresses=malicious_addresses,
            analysis_timestamp=analysis_timestamp,
        )

    packed_transfers = [
        (
            transfer.token_id,
            transfer.tx_hash,
            int(transfer.block_number or 0),
            int(transfer.log_index or 0),
            int(transfer.block_time or 0),
            transfer.from_address,
            transfer.to_address,
        )
        for transfer in transfers
    ]
    packed_sales = [
        (
            sale.token_id,
            sale.buyer_address,
            sale.seller_address,
        )
        for sale in sales
    ]
    packed_owners = [
        (
            owner.owner_address,
            [token_id for token_id, balance in owner.token_balances.items() if balance > 0],
        )
        for owner in owners
    ]
    packed_infringing_tokens = [str(item.get('token_id') or '') for item in infringing_tokens if item.get('token_id')]
    packed_malicious_addresses = [str(item.get('address') or '') for item in malicious_addresses if item.get('address')]
    return list(
        _rust_build_honest_address_records(
            contract_address,
            packed_transfers,
            packed_sales,
            packed_owners,
            packed_infringing_tokens,
            packed_malicious_addresses,
            int(analysis_timestamp or time.time()),
        )
    )


def build_database_snapshot(
    *,
    contract_addresses: Sequence[str],
    token_ids: Sequence[str],
    token_uris: Sequence[str],
    image_uris: Sequence[str],
    names: Sequence[str],
    symbols: Sequence[str],
    metadata_jsons: Sequence[str],
    metadata_docs: Sequence[str],
    token_uri_norms: Sequence[str],
    image_uri_norms: Sequence[str],
    symbol_norms: Sequence[str],
    name_norms: Sequence[str],
    metadata_keywords_arr: Sequence[Sequence[str]],
    exact_token_keys: Sequence[str],
    exact_image_keys: Sequence[str],
    exact_symbols: Sequence[str],
    name_prefixes: Sequence[str],
    metadata_recall_terms: Sequence[str],
):
    if _rust_build_database_snapshot is None:
        raise RuntimeError('rust snapshot builder unavailable')

    return _rust_build_database_snapshot(
        list(contract_addresses),
        list(token_ids),
        list(token_uris),
        list(image_uris),
        list(names),
        list(symbols),
        list(metadata_jsons),
        list(metadata_docs),
        list(token_uri_norms),
        list(image_uri_norms),
        list(symbol_norms),
        list(name_norms),
        [list(items) for items in metadata_keywords_arr],
        list(exact_token_keys),
        list(exact_image_keys),
        list(exact_symbols),
        list(name_prefixes),
        list(metadata_recall_terms),
    )
