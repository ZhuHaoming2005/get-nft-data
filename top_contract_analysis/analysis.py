from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import asdict
from statistics import median
from typing import Any, Dict, List, Sequence, Tuple

from .alchemy_api import (
    fetch_contract_metadata,
    fetch_contract_metadata_async,
    fetch_contract_owners,
    fetch_contract_owners_async,
    fetch_contract_transfers,
    fetch_contract_transfers_async,
    fetch_eth_balance,
    fetch_eth_balance_async,
    fetch_license_sample,
    fetch_license_sample_async,
    fetch_same_block_eth_transfers_for_address,
    fetch_same_block_eth_transfers_for_address_async,
    fetch_seed_contract_nfts,
    fetch_seed_contract_nfts_async,
    fetch_transaction_receipt,
    fetch_transaction_receipt_async,
    fetch_transaction_receipts_for_block,
    fetch_transaction_receipts_for_block_async,
    is_open_license_payload,
)
from .async_http import AsyncApiClient
from .constants import (
    DEFAULT_API_MAX_CONCURRENCY,
    DEFAULT_CONTRACT_MAX_CONCURRENCY,
    DEFAULT_MAX_RECALL_ROWS,
    DEFAULT_NAME_THRESHOLD,
    DEFAULT_SALE_METRIC_MAX_CONCURRENCY,
    DEFAULT_TIMEOUT,
    ZERO_ADDRESS,
    logger,
)
from .db import get_conn
from .models import DatabaseSnapshot, DuplicateCandidate, NFTSaleRecord, OwnerBalance, TransactionReceiptRecord, TransferRecord
from .normalize import normalize_name, normalize_network, normalize_symbol, normalize_url
from .sales import ETH_PRICED_SYMBOLS, _looks_like_real_api_key, fetch_contract_sales, fetch_contract_sales_async
from .snapshot import find_duplicate_candidates, group_candidates_by_contract, load_database_snapshot


def _notify_progress(progress_reporter, method_name: str, *args, **kwargs) -> None:
    if progress_reporter is None:
        return
    method = getattr(progress_reporter, method_name, None)
    if method is None:
        return
    try:
        method(*args, **kwargs)
    except Exception:
        logger.debug('progress reporter method failed: %s', method_name, exc_info=True)


def _is_eth_priced_sale(sale: NFTSaleRecord) -> bool:
    return sale.price_eth is not None and sale.payment_token_symbol in ETH_PRICED_SYMBOLS


def calculate_sale_eth_metrics(
    *,
    sale: NFTSaleRecord,
    purchase_receipt: TransactionReceiptRecord,
    base_balance_eth: float,
    same_block_transfers: Sequence,
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


def _median_or_none(values: Sequence[float | int]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return float(median(cleaned))


def _mean_or_none(values: Sequence[float | int]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def _format_timing_breakdown(stages: Dict[str, float]) -> str:
    ordered = sorted(stages.items(), key=lambda item: item[1], reverse=True)
    return ', '.join(f'{name}={duration:.3f}s' for name, duration in ordered if duration > 0)


def _is_candidate_open_license(*, metadata_json: str = '', metadata_doc: str = '') -> bool:
    payload: Dict[str, Any] = {}
    if metadata_json:
        try:
            payload = json.loads(metadata_json)
        except json.JSONDecodeError:
            payload = {'metadata_json': metadata_json}
    if metadata_doc:
        if payload:
            payload = {'metadata': payload, 'metadata_doc': metadata_doc}
        else:
            payload = {'metadata_doc': metadata_doc}
    return bool(payload) and is_open_license_payload(payload)


def _build_infringing_token_records_python(
    *,
    contract_address: str,
    contract_candidates: Sequence[DuplicateCandidate],
    transfers: Sequence[TransferRecord],
    official_addresses: set[str],
    candidate_open_license_by_token: Dict[tuple[str, str], bool] | None = None,
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
                'candidate_open_license': bool(
                    (candidate_open_license_by_token or {}).get((contract_address, candidate.token_id), False)
                ),
                'official_or_legit_reissue': bool(minter_address and minter_address in official_addresses),
            }
        )
    return rows


def build_infringing_token_records(
    *,
    contract_address: str,
    contract_candidates: Sequence[DuplicateCandidate],
    transfers: Sequence[TransferRecord],
    official_addresses: set[str],
    candidate_open_license_by_token: Dict[tuple[str, str], bool] | None = None,
) -> List[Dict[str, Any]]:
    from .rust_bridge import build_infringing_token_records as build_infringing_rows

    return build_infringing_rows(
        contract_address=contract_address,
        contract_candidates=contract_candidates,
        transfers=transfers,
        official_addresses=official_addresses,
        candidate_open_license_by_token=candidate_open_license_by_token,
    )


def _build_malicious_address_records_python(
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
        is_star_distributor = star_out_degree >= 3
        if not mint_role and not wash_cycle_count and not is_star_distributor:
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


def build_malicious_address_records(
    *,
    contract_address: str,
    transfers: Sequence[TransferRecord],
    infringing_tokens: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    from .rust_bridge import build_malicious_address_records as build_malicious_rows

    return build_malicious_rows(
        contract_address=contract_address,
        transfers=transfers,
        infringing_tokens=infringing_tokens,
    )


def _build_victim_address_records_python(
    *,
    contract_address: str,
    sales: Sequence[NFTSaleRecord],
    transfers: Sequence[TransferRecord],
    owners: Sequence[OwnerBalance],
    sale_metrics_by_tx: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    del contract_address

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
                'last_buy_amount_eth': None,
                'buy_before_eth_balance': None,
                'buy_asset_ratio': None,
                'buy_asset_ratio_with_gas': None,
                'is_stuck': False,
                'last_buy_tx_hash': '',
                'ratio_status': 'unavailable',
            },
        )
        entry['buy_tx_hashes'].append(sale.tx_hash)
        if _is_eth_priced_sale(sale):
            entry['buy_amount_eth'] += sale.price_eth
        current_key = _sale_sort_key(sale)
        if buyer not in last_buy_key or current_key >= last_buy_key[buyer]:
            last_buy_key[buyer] = current_key
            entry['last_buy_tx_hash'] = sale.tx_hash
            entry['last_buy_amount_eth'] = sale.price_eth if _is_eth_priced_sale(sale) else None
            entry['buy_before_eth_balance'] = metrics.get('buy_before_eth_balance')
            entry['buy_asset_ratio'] = metrics.get('buy_asset_ratio')
            entry['buy_asset_ratio_with_gas'] = metrics.get('buy_asset_ratio_with_gas')
            entry['ratio_status'] = metrics.get('ratio_status', 'unavailable')
        entry['is_stuck'] = bool(entry['is_stuck'] or is_stuck)
    return sorted(grouped.values(), key=lambda item: item['address'])


def build_victim_address_records(
    *,
    contract_address: str,
    sales: Sequence[NFTSaleRecord],
    transfers: Sequence[TransferRecord],
    owners: Sequence[OwnerBalance],
    sale_metrics_by_tx: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    from .rust_bridge import build_victim_address_records as build_victim_rows

    return build_victim_rows(
        sales=sales,
        transfers=transfers,
        owners=owners,
        sale_metrics_by_tx=sale_metrics_by_tx,
        contract_address=contract_address,
    )


def _build_honest_address_records_python(
    *,
    contract_address: str,
    transfers: Sequence[TransferRecord],
    sales: Sequence[NFTSaleRecord],
    owners: Sequence[OwnerBalance],
    infringing_tokens: Sequence[Dict[str, Any]],
    malicious_addresses: Sequence[Dict[str, Any]],
    analysis_timestamp: int | None = None,
) -> List[Dict[str, Any]]:
    cutoff_time = int(analysis_timestamp or time.time())
    relevant_token_ids = {str(item.get('token_id') or '') for item in infringing_tokens if item.get('token_id')}
    malicious_set = {str(item.get('address') or '') for item in malicious_addresses if item.get('address')}

    owner_token_map: Dict[str, set[str]] = {}
    for owner in owners:
        if not owner.owner_address or owner.owner_address == ZERO_ADDRESS:
            continue
        held_tokens = {
            token_id
            for token_id, balance in owner.token_balances.items()
            if balance > 0 and (not relevant_token_ids or token_id in relevant_token_ids)
        }
        if held_tokens:
            owner_token_map[owner.owner_address] = held_tokens

    relevant_transfers = [
        transfer
        for transfer in sorted(transfers, key=_transfer_sort_key)
        if not relevant_token_ids or transfer.token_id in relevant_token_ids
    ]
    relevant_sales = [
        sale
        for sale in sales
        if not relevant_token_ids or sale.token_id in relevant_token_ids
    ]

    all_addresses: set[str] = set()
    for transfer in relevant_transfers:
        if transfer.from_address and transfer.from_address != ZERO_ADDRESS:
            all_addresses.add(transfer.from_address)
        if transfer.to_address and transfer.to_address != ZERO_ADDRESS:
            all_addresses.add(transfer.to_address)
    for sale in relevant_sales:
        if sale.buyer_address:
            all_addresses.add(sale.buyer_address)
        if sale.seller_address:
            all_addresses.add(sale.seller_address)
    all_addresses.update(owner_token_map)

    honest_addresses = sorted(address for address in all_addresses if address and address not in malicious_set)
    honest_set = set(honest_addresses)
    token_interactions_by_address: Dict[str, set[str]] = defaultdict(set)
    durations_by_address: Dict[str, List[int]] = defaultdict(list)
    mint_to_honest_samples_by_address: Dict[str, List[int]] = defaultdict(list)
    honest_to_honest_count: Dict[str, int] = defaultdict(int)
    corrupted_addresses: set[str] = set()
    open_holds: Dict[tuple[str, str], int] = {}
    transfers_by_token: Dict[str, List[TransferRecord]] = defaultdict(list)

    for transfer in relevant_transfers:
        transfers_by_token[transfer.token_id].append(transfer)

    for token_id, token_transfers in transfers_by_token.items():
        mint_time = 0
        first_honest_recorded = False
        for transfer in token_transfers:
            if transfer.from_address == ZERO_ADDRESS and transfer.block_time:
                mint_time = transfer.block_time
            if transfer.from_address in honest_set:
                token_interactions_by_address[transfer.from_address].add(token_id)
                start_time = open_holds.pop((token_id, transfer.from_address), None)
                if start_time is not None and transfer.block_time >= start_time:
                    durations_by_address[transfer.from_address].append(transfer.block_time - start_time)
            if transfer.from_address in honest_set and transfer.to_address in honest_set:
                corrupted_addresses.add(transfer.from_address)
                honest_to_honest_count[transfer.from_address] += 1
            if transfer.to_address in honest_set:
                token_interactions_by_address[transfer.to_address].add(token_id)
                if transfer.block_time:
                    open_holds[(token_id, transfer.to_address)] = transfer.block_time
                    if mint_time and not first_honest_recorded:
                        mint_to_honest_samples_by_address[transfer.to_address].append(max(0, transfer.block_time - mint_time))
                        first_honest_recorded = True

    for (token_id, address), start_time in open_holds.items():
        if owner_token_map and token_id not in owner_token_map.get(address, set()):
            continue
        if cutoff_time >= start_time:
            durations_by_address[address].append(cutoff_time - start_time)

    rows: List[Dict[str, Any]] = []
    for address in honest_addresses:
        hold_durations = durations_by_address.get(address, [])
        current_tokens = owner_token_map.get(address, set())
        rows.append(
            {
                'contract_address': contract_address,
                'address': address,
                'interacted_token_count': len(token_interactions_by_address.get(address, set()) | current_tokens),
                'currently_holding_token_count': len(current_tokens),
                'hold_duration_median_seconds': _median_or_none(hold_durations),
                'hold_duration_count': len(hold_durations),
                'is_corrupted_address': address in corrupted_addresses,
                'honest_sale_to_honest_count': honest_to_honest_count.get(address, 0),
                'mint_to_honest_seconds_samples': list(mint_to_honest_samples_by_address.get(address, [])),
            }
        )
    return rows


def build_honest_address_records(
    *,
    contract_address: str,
    transfers: Sequence[TransferRecord],
    sales: Sequence[NFTSaleRecord],
    owners: Sequence[OwnerBalance],
    infringing_tokens: Sequence[Dict[str, Any]],
    malicious_addresses: Sequence[Dict[str, Any]],
    analysis_timestamp: int | None = None,
) -> List[Dict[str, Any]]:
    from .rust_bridge import build_honest_address_records as build_honest_rows

    return build_honest_rows(
        contract_address=contract_address,
        transfers=transfers,
        sales=sales,
        owners=owners,
        infringing_tokens=infringing_tokens,
        malicious_addresses=malicious_addresses,
        analysis_timestamp=analysis_timestamp,
    )


def build_honest_address_stats(
    *,
    contract_address: str,
    honest_addresses: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    corrupted = sorted(item['address'] for item in honest_addresses if item.get('is_corrupted_address'))
    holding_medians = [
        item.get('hold_duration_median_seconds')
        for item in honest_addresses
        if item.get('hold_duration_median_seconds') is not None
    ]
    mint_to_honest_samples = [
        float(sample)
        for item in honest_addresses
        for sample in (item.get('mint_to_honest_seconds_samples') or [])
    ]
    return {
        contract_address: {
            'honest_address_count': len(honest_addresses),
            'corrupted_address_count': len(corrupted),
            'honest_to_honest_transfer_count': sum(
                int(item.get('honest_sale_to_honest_count') or 0) for item in honest_addresses
            ),
            'median_holding_seconds': _median_or_none(holding_medians),
            'avg_seconds_to_honest_holder': (
                sum(mint_to_honest_samples) / len(mint_to_honest_samples) if mint_to_honest_samples else None
            ),
            'corrupted_addresses': corrupted,
        }
    }


def build_fraud_trade_stats(
    *,
    contract_address: str,
    sales: Sequence[NFTSaleRecord],
    victim_addresses: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    native_sales = [sale for sale in sales if sale.is_native_eth and sale.price_eth is not None]
    eth_priced_sales = [sale for sale in sales if _is_eth_priced_sale(sale)]
    return {
        contract_address: {
            'unique_buyers': len({sale.buyer_address for sale in sales if sale.buyer_address}),
            'native_eth_sale_count': len(native_sales),
            'native_eth_volume': sum(sale.price_eth or 0.0 for sale in native_sales),
            'eth_priced_sale_count': len(eth_priced_sales),
            'eth_priced_volume': sum(sale.price_eth or 0.0 for sale in eth_priced_sales),
            'stuck_wallet_count': sum(1 for item in victim_addresses if item.get('is_stuck')),
            'stuck_cost_eth': sum(float(item.get('last_buy_amount_eth') or 0.0) for item in victim_addresses if item.get('is_stuck')),
        }
    }


def build_report_summary(
    *,
    open_license: bool,
    grouped: Dict[str, Sequence[DuplicateCandidate]],
    high_confidence: Sequence[Dict[str, Any]],
    low_confidence: Sequence[Dict[str, Any]],
    legit_duplicates: Sequence[Dict[str, Any]],
    infringing_tokens: Sequence[Dict[str, Any]],
    honest_addresses: Sequence[Dict[str, Any]],
    victim_addresses: Sequence[Dict[str, Any]],
    address_signals: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    candidate_open_license_tokens = [
        item for item in infringing_tokens
        if item.get('candidate_open_license')
    ]
    candidate_open_license_contracts = {
        item.get('contract_address')
        for item in candidate_open_license_tokens
        if item.get('contract_address')
    }
    buy_ratio_values = [
        float(item.get('buy_asset_ratio'))
        for item in victim_addresses
        if item.get('buy_asset_ratio') is not None
    ]
    ratio_known_count = len(buy_ratio_values)
    ratio_over_60_count = sum(1 for value in buy_ratio_values if value > 0.6)
    ratio_over_80_count = sum(1 for value in buy_ratio_values if value > 0.8)
    stuck_honest_address_count = sum(1 for item in victim_addresses if item.get('is_stuck'))
    corrupted_honest_address_count = sum(1 for item in honest_addresses if item.get('is_corrupted_address'))
    victim_address_count = len(victim_addresses)
    mint_to_honest_samples = [
        float(sample)
        for item in honest_addresses
        for sample in (item.get('mint_to_honest_seconds_samples') or [])
    ]
    mint_to_first_transfer_values = [
        float(signal.get('mint_to_first_transfer_seconds'))
        for signal in address_signals.values()
        if signal.get('mint_to_first_transfer_seconds') is not None
    ]
    unique_receiver_values = [
        float(signal.get('unique_receiver_count'))
        for signal in address_signals.values()
        if signal.get('unique_receiver_count') is not None
    ]
    return {
        'open_license_detected': open_license,
        'candidate_contract_count': len(grouped),
        'high_confidence_contract_count': len(high_confidence),
        'low_confidence_contract_count': len(low_confidence),
        'legit_duplicate_contract_count': len(legit_duplicates),
        'candidate_open_license_token_count': len(candidate_open_license_tokens),
        'candidate_open_license_contract_count': len(candidate_open_license_contracts),
        'honest_purchase_total_eth': sum(float(item.get('buy_amount_eth') or 0.0) for item in victim_addresses),
        'buy_asset_ratio_known_address_count': ratio_known_count,
        'ratio_over_60_address_count': ratio_over_60_count,
        'ratio_over_60_address_ratio': (ratio_over_60_count / ratio_known_count) if ratio_known_count else None,
        'ratio_over_80_address_count': ratio_over_80_count,
        'ratio_over_80_address_ratio': (ratio_over_80_count / ratio_known_count) if ratio_known_count else None,
        'stuck_honest_address_count': stuck_honest_address_count,
        'stuck_honest_address_ratio': (stuck_honest_address_count / victim_address_count) if victim_address_count else None,
        'corrupted_honest_address_count': corrupted_honest_address_count,
        'avg_seconds_to_honest_holder': _mean_or_none(mint_to_honest_samples),
        'avg_mint_to_first_transfer_seconds': _mean_or_none(mint_to_first_transfer_values),
        'avg_unique_receiver_count': _mean_or_none(unique_receiver_values),
    }


def _unavailable_sale_metrics(*, sale: NFTSaleRecord | None = None) -> Dict[str, Any]:
    price_eth = sale.price_eth if sale is not None else None
    return {
        'buy_before_eth_balance': None,
        'buy_amount_eth': price_eth,
        'buy_total_eth_out': price_eth,
        'buy_asset_ratio': None,
        'buy_asset_ratio_with_gas': None,
        'gas_not_attributed': False,
        'ratio_status': 'unavailable',
    }


def _is_mocked_callable(func: Any) -> bool:
    return getattr(func.__class__, '__module__', '') == 'unittest.mock'


async def _call_api(
    *,
    sync_func,
    async_func,
    client: AsyncApiClient,
    **kwargs,
):
    if _is_mocked_callable(sync_func):
        call_kwargs = dict(kwargs)
        call_kwargs.setdefault('session', client)
        return await asyncio.to_thread(sync_func, **call_kwargs)
    return await async_func(client=client, **kwargs)


async def _compute_sale_metrics_async(
    *,
    client: AsyncApiClient,
    alchemy_api_key: str,
    network: str,
    contract_address: str,
    sale: NFTSaleRecord,
    timeout: int,
    receipts_by_block_tasks: Dict[int, asyncio.Task[Dict[str, TransactionReceiptRecord]]],
) -> tuple[str, Dict[str, Any]]:
    if not sale.is_native_eth or sale.price_eth is None:
        return sale.tx_hash, _unavailable_sale_metrics()
    try:
        async with client.sale_metric_semaphore:
            purchase_receipt, base_balance_eth, same_block_transfers = await asyncio.gather(
                _call_api(
                    sync_func=fetch_transaction_receipt,
                    async_func=fetch_transaction_receipt_async,
                    client=client,
                    api_key=alchemy_api_key,
                    network=network,
                    tx_hash=sale.tx_hash,
                    timeout=timeout,
                ),
                _call_api(
                    sync_func=fetch_eth_balance,
                    async_func=fetch_eth_balance_async,
                    client=client,
                    api_key=alchemy_api_key,
                    network=network,
                    address=sale.buyer_address,
                    block_number=sale.block_number - 1,
                    timeout=timeout,
                ),
                _call_api(
                    sync_func=fetch_same_block_eth_transfers_for_address,
                    async_func=fetch_same_block_eth_transfers_for_address_async,
                    client=client,
                    api_key=alchemy_api_key,
                    network=network,
                    block_number=sale.block_number,
                    address=sale.buyer_address,
                    timeout=timeout,
                ),
            )
        block_receipts: Dict[str, TransactionReceiptRecord] = {}
        if same_block_transfers:
            block_task = receipts_by_block_tasks.get(sale.block_number)
            if block_task is None:
                block_task = asyncio.create_task(
                    _call_api(
                        sync_func=fetch_transaction_receipts_for_block,
                        async_func=fetch_transaction_receipts_for_block_async,
                        client=client,
                        api_key=alchemy_api_key,
                        network=network,
                        block_number=sale.block_number,
                        timeout=timeout,
                    )
                )
                receipts_by_block_tasks[sale.block_number] = block_task
            block_receipts = await block_task
        return sale.tx_hash, calculate_sale_eth_metrics(
            sale=sale,
            purchase_receipt=purchase_receipt,
            base_balance_eth=base_balance_eth,
            same_block_transfers=same_block_transfers,
            receipts_by_hash=block_receipts,
        )
    except Exception as exc:
        logger.warning('sale ETH metric computation failed for %s %s: %s', contract_address, sale.tx_hash, exc)
        return sale.tx_hash, _unavailable_sale_metrics(sale=sale)


async def _analyze_high_confidence_contract_async(
    *,
    client: AsyncApiClient,
    chain: str,
    network: str,
    alchemy_api_key: str,
    etherscan_api_key: str,
    opensea_api_key: str,
    contract_address: str,
    contract_candidates: Sequence[DuplicateCandidate],
    token_type: str,
    official_addresses: set[str],
    candidate_open_license_by_token: Dict[tuple[str, str], bool],
    timeout: int,
    signal_cache,
) -> Dict[str, Any]:
    stage_times: Dict[str, float] = {}
    contract_started = time.perf_counter()
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
        started = time.perf_counter()
        transfers = await _call_api(
            sync_func=fetch_contract_transfers,
            async_func=fetch_contract_transfers_async,
            client=client,
            alchemy_api_key=alchemy_api_key,
            alchemy_network=network,
            etherscan_api_key=etherscan_api_key,
            chain=chain,
            contract_address=contract_address,
            token_type=token_type,
            timeout=timeout,
        )
        stage_times['fetch_transfers'] = time.perf_counter() - started
        owners = []
        mint_recipients = {row.to_address for row in transfers if row.from_address == ZERO_ADDRESS}
        active_sellers = [
            row.from_address
            for row in transfers
            if row.from_address and row.from_address != ZERO_ADDRESS
        ]
        started = time.perf_counter()
        cached_address_signals = analyze_contract_transfers(transfers)
        stage_times['analyze_transfer_signals'] = time.perf_counter() - started
        cached_victim_signals = None

    started = time.perf_counter()
    infringing_tokens = build_infringing_token_records(
        contract_address=contract_address,
        contract_candidates=contract_candidates,
        transfers=transfers,
        official_addresses=official_addresses,
        candidate_open_license_by_token=candidate_open_license_by_token,
    )
    stage_times['build_infringing_tokens'] = time.perf_counter() - started

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
        stage_times['contract_total'] = time.perf_counter() - contract_started
        logger.info(
            'contract timing seed=%s contract=%s %s',
            chain,
            contract_address,
            _format_timing_breakdown(stage_times),
        )
        result['status'] = 'legit'
        result['mint_recipients'] = sorted(mint_recipients)
        return result

    if cached_victim_signals is None or not owners:
        started = time.perf_counter()
        owners = await _call_api(
            sync_func=fetch_contract_owners,
            async_func=fetch_contract_owners_async,
            client=client,
            api_key=alchemy_api_key,
            network=network,
            contract_address=contract_address,
            timeout=timeout,
        )
        stage_times['fetch_owners'] = time.perf_counter() - started
        from .rust_bridge import analyze_victim_signals_from_active_sellers

        started = time.perf_counter()
        cached_victim_signals = analyze_victim_signals_from_active_sellers(active_sellers, owners)
        stage_times['analyze_victim_signals'] = time.perf_counter() - started
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
            started = time.perf_counter()
            sales = await _call_api(
                sync_func=fetch_contract_sales,
                async_func=fetch_contract_sales_async,
                client=client,
                alchemy_api_key=alchemy_api_key,
                alchemy_network=network,
                contract_address=contract_address,
                opensea_api_key=opensea_api_key,
                timeout=timeout,
            )
            stage_times['fetch_sales'] = time.perf_counter() - started
        except Exception as exc:
            logger.warning('contract sales fetch failed for %s: %s', contract_address, exc)
    started = time.perf_counter()
    receipts_by_block_tasks: Dict[int, asyncio.Task[Dict[str, TransactionReceiptRecord]]] = {}
    sale_metric_rows = await asyncio.gather(*[
        _compute_sale_metrics_async(
            client=client,
            alchemy_api_key=alchemy_api_key,
            network=network,
            contract_address=contract_address,
            sale=sale,
            timeout=timeout,
            receipts_by_block_tasks=receipts_by_block_tasks,
        )
        for sale in sales
    ])
    sale_metrics_by_tx = dict(sale_metric_rows)
    stage_times['sale_metrics'] = time.perf_counter() - started

    started = time.perf_counter()
    malicious_addresses = build_malicious_address_records(
        contract_address=contract_address,
        transfers=transfers,
        infringing_tokens=infringing_tokens,
    )
    stage_times['build_malicious_addresses'] = time.perf_counter() - started
    started = time.perf_counter()
    victim_addresses = build_victim_address_records(
        contract_address=contract_address,
        sales=sales,
        transfers=transfers,
        owners=owners,
        sale_metrics_by_tx=sale_metrics_by_tx,
    )
    stage_times['build_victim_addresses'] = time.perf_counter() - started
    started = time.perf_counter()
    honest_addresses = build_honest_address_records(
        contract_address=contract_address,
        transfers=transfers,
        sales=sales,
        owners=owners,
        infringing_tokens=infringing_tokens,
        malicious_addresses=malicious_addresses,
        analysis_timestamp=int(time.time()),
    )
    stage_times['build_honest_addresses'] = time.perf_counter() - started
    started = time.perf_counter()
    honest_address_stats = build_honest_address_stats(
        contract_address=contract_address,
        honest_addresses=honest_addresses,
    )
    stage_times['build_honest_stats'] = time.perf_counter() - started
    started = time.perf_counter()
    fraud_trade_stats = build_fraud_trade_stats(
        contract_address=contract_address,
        sales=sales,
        victim_addresses=victim_addresses,
    )
    stage_times['build_fraud_stats'] = time.perf_counter() - started
    stage_times['contract_total'] = time.perf_counter() - contract_started

    result['status'] = 'high'
    result['address_signals'] = cached_address_signals
    result['victim_signals'] = cached_victim_signals
    result['malicious_addresses'] = malicious_addresses
    result['honest_addresses'] = honest_addresses
    result['honest_address_stats'] = honest_address_stats
    result['victim_addresses'] = victim_addresses
    result['fraud_trade_stats'] = fraud_trade_stats
    logger.info(
        'contract timing seed=%s contract=%s %s',
        chain,
        contract_address,
        _format_timing_breakdown(stage_times),
    )
    return result


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
    candidate_open_license_by_token: Dict[tuple[str, str], bool],
    timeout: int,
    signal_cache,
    session=None,
) -> Dict[str, Any]:
    stage_times: Dict[str, float] = {}
    contract_started = time.perf_counter()
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
        started = time.perf_counter()
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
        stage_times['fetch_transfers'] = time.perf_counter() - started
        owners = []
        mint_recipients = {row.to_address for row in transfers if row.from_address == ZERO_ADDRESS}
        active_sellers = [
            row.from_address
            for row in transfers
            if row.from_address and row.from_address != ZERO_ADDRESS
        ]
        started = time.perf_counter()
        cached_address_signals = analyze_contract_transfers(transfers)
        stage_times['analyze_transfer_signals'] = time.perf_counter() - started
        cached_victim_signals = None

    started = time.perf_counter()
    infringing_tokens = build_infringing_token_records(
        contract_address=contract_address,
        contract_candidates=contract_candidates,
        transfers=transfers,
        official_addresses=official_addresses,
        candidate_open_license_by_token=candidate_open_license_by_token,
    )
    stage_times['build_infringing_tokens'] = time.perf_counter() - started

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
        stage_times['contract_total'] = time.perf_counter() - contract_started
        logger.info(
            'contract timing seed=%s contract=%s %s',
            chain,
            contract_address,
            _format_timing_breakdown(stage_times),
        )
        result['status'] = 'legit'
        result['mint_recipients'] = sorted(mint_recipients)
        return result

    if cached_victim_signals is None or not owners:
        started = time.perf_counter()
        owners = fetch_contract_owners(
            api_key=alchemy_api_key,
            network=network,
            contract_address=contract_address,
            timeout=timeout,
            session=session,
        )
        stage_times['fetch_owners'] = time.perf_counter() - started
        from .rust_bridge import analyze_victim_signals_from_active_sellers

        started = time.perf_counter()
        cached_victim_signals = analyze_victim_signals_from_active_sellers(active_sellers, owners)
        stage_times['analyze_victim_signals'] = time.perf_counter() - started
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
            started = time.perf_counter()
            sales = fetch_contract_sales(
                alchemy_api_key=alchemy_api_key,
                alchemy_network=network,
                contract_address=contract_address,
                opensea_api_key=opensea_api_key,
                timeout=timeout,
                session=session,
            )
            stage_times['fetch_sales'] = time.perf_counter() - started
        except Exception as exc:
            logger.warning('contract sales fetch failed for %s: %s', contract_address, exc)
    sale_metrics_by_tx: Dict[str, Dict[str, Any]] = {}
    receipts_by_block: Dict[int, Dict[str, TransactionReceiptRecord]] = {}
    started = time.perf_counter()
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
    stage_times['sale_metrics'] = time.perf_counter() - started

    started = time.perf_counter()
    malicious_addresses = build_malicious_address_records(
        contract_address=contract_address,
        transfers=transfers,
        infringing_tokens=infringing_tokens,
    )
    stage_times['build_malicious_addresses'] = time.perf_counter() - started
    started = time.perf_counter()
    victim_addresses = build_victim_address_records(
        contract_address=contract_address,
        sales=sales,
        transfers=transfers,
        owners=owners,
        sale_metrics_by_tx=sale_metrics_by_tx,
    )
    stage_times['build_victim_addresses'] = time.perf_counter() - started
    started = time.perf_counter()
    honest_addresses = build_honest_address_records(
        contract_address=contract_address,
        transfers=transfers,
        sales=sales,
        owners=owners,
        infringing_tokens=infringing_tokens,
        malicious_addresses=malicious_addresses,
        analysis_timestamp=int(time.time()),
    )
    stage_times['build_honest_addresses'] = time.perf_counter() - started
    started = time.perf_counter()
    honest_address_stats = build_honest_address_stats(
        contract_address=contract_address,
        honest_addresses=honest_addresses,
    )
    stage_times['build_honest_stats'] = time.perf_counter() - started
    started = time.perf_counter()
    fraud_trade_stats = build_fraud_trade_stats(
        contract_address=contract_address,
        sales=sales,
        victim_addresses=victim_addresses,
    )
    stage_times['build_fraud_stats'] = time.perf_counter() - started
    stage_times['contract_total'] = time.perf_counter() - contract_started

    result['status'] = 'high'
    result['address_signals'] = cached_address_signals
    result['victim_signals'] = cached_victim_signals
    result['malicious_addresses'] = malicious_addresses
    result['honest_addresses'] = honest_addresses
    result['honest_address_stats'] = honest_address_stats
    result['victim_addresses'] = victim_addresses
    result['fraud_trade_stats'] = fraud_trade_stats
    logger.info(
        'contract timing seed=%s contract=%s %s',
        chain,
        contract_address,
        _format_timing_breakdown(stage_times),
    )
    return result


async def async_analyze_seed_contract(
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
    api_max_concurrency: int = DEFAULT_API_MAX_CONCURRENCY,
    contract_max_concurrency: int = DEFAULT_CONTRACT_MAX_CONCURRENCY,
    sale_metric_max_concurrency: int = DEFAULT_SALE_METRIC_MAX_CONCURRENCY,
    progress_reporter=None,
) -> Dict[str, Any]:
    stage_times: Dict[str, float] = {}
    seed_started = time.perf_counter()
    network = normalize_network(chain, alchemy_network)
    own_conn = False
    if feature_store is None and conn is None:
        conn = get_conn()
        own_conn = True
    client = AsyncApiClient(
        timeout=timeout,
        max_concurrency=api_max_concurrency,
        contract_max_concurrency=contract_max_concurrency,
        sale_metric_max_concurrency=sale_metric_max_concurrency,
    )
    try:
        _notify_progress(progress_reporter, 'on_seed_stage', 'fetch_seed_context', seed_contract_address=seed_contract_address)
        started = time.perf_counter()
        contract_meta, seed_nfts = await asyncio.gather(
            _call_api(
                sync_func=fetch_contract_metadata,
                async_func=fetch_contract_metadata_async,
                client=client,
                api_key=alchemy_api_key,
                network=network,
                chain=chain,
                contract_address=seed_contract_address,
                timeout=timeout,
            ),
            _call_api(
                sync_func=fetch_seed_contract_nfts,
                async_func=fetch_seed_contract_nfts_async,
                client=client,
                api_key=alchemy_api_key,
                network=network,
                chain=chain,
                contract_address=seed_contract_address,
                timeout=timeout,
            ),
        )
        elapsed = time.perf_counter() - started
        stage_times['fetch_contract_metadata'] = elapsed
        stage_times['fetch_seed_nfts'] = elapsed
        started = time.perf_counter()
        _notify_progress(progress_reporter, 'on_seed_stage', 'fetch_license_sample', seed_contract_address=seed_contract_address)
        license_payload = await _call_api(
            sync_func=fetch_license_sample,
            async_func=fetch_license_sample_async,
            client=client,
            api_key=alchemy_api_key,
            network=network,
            chain=chain,
            seed_nfts=seed_nfts,
            timeout=timeout,
        )
        stage_times['fetch_license_sample'] = time.perf_counter() - started
        open_license = is_open_license_payload(license_payload)
        if feature_store is not None:
            started = time.perf_counter()
            _notify_progress(progress_reporter, 'on_seed_stage', 'load_snapshot', seed_contract_address=seed_contract_address)
            snapshot = feature_store.load_snapshot(
                chain,
                seed_nfts=seed_nfts,
                max_tokens_per_contract=max_tokens_per_contract,
                max_recall_rows=max_recall_rows,
            )
            stage_times['load_snapshot'] = time.perf_counter() - started
        else:
            started = time.perf_counter()
            _notify_progress(progress_reporter, 'on_seed_stage', 'load_snapshot', seed_contract_address=seed_contract_address)
            snapshot = load_database_snapshot(conn, chain, seed_nfts=seed_nfts)
            stage_times['load_snapshot'] = time.perf_counter() - started

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

        started = time.perf_counter()
        _notify_progress(progress_reporter, 'on_seed_stage', 'find_duplicate_candidates', seed_contract_address=seed_contract_address)
        candidates = find_duplicate_candidates(
            seed_nfts,
            snapshot,
            name_threshold=name_threshold,
            metadata_threshold=metadata_threshold,
        )
        stage_times['find_duplicate_candidates'] = time.perf_counter() - started
        started = time.perf_counter()
        _notify_progress(progress_reporter, 'on_seed_stage', 'postprocess_candidates', seed_contract_address=seed_contract_address)
        grouped = group_candidates_by_contract(candidates)
        snapshot_rows_by_key = {
            (row.contract_address, row.token_id): row
            for row in snapshot.nft_rows
        }
        candidate_open_license_by_token = {
            (candidate.contract_address, candidate.token_id): _is_candidate_open_license(
                metadata_json=(snapshot_rows_by_key.get((candidate.contract_address, candidate.token_id)).metadata_json or '')
                if snapshot_rows_by_key.get((candidate.contract_address, candidate.token_id))
                else '',
                metadata_doc=(snapshot_rows_by_key.get((candidate.contract_address, candidate.token_id)).metadata_doc or '')
                if snapshot_rows_by_key.get((candidate.contract_address, candidate.token_id))
                else '',
            )
            for candidate in candidates
        }
        stage_times['postprocess_candidates'] = time.perf_counter() - started

        official_addresses = {addr for addr in [contract_meta.contract_deployer, contract_meta.contract_address] if addr}
        legit_duplicates: List[Dict[str, Any]] = []
        high_confidence: List[Dict[str, Any]] = []
        low_confidence: List[Dict[str, Any]] = []
        address_signals: Dict[str, Any] = {}
        victim_signals: Dict[str, Any] = {}
        infringing_tokens: List[Dict[str, Any]] = []
        malicious_addresses: List[Dict[str, Any]] = []
        honest_addresses: List[Dict[str, Any]] = []
        honest_address_stats: Dict[str, Dict[str, Any]] = {}
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
            started = time.perf_counter()
            _notify_progress(
                progress_reporter,
                'on_seed_stage',
                'analyze_high_confidence_contracts',
                seed_contract_address=seed_contract_address,
            )
            _notify_progress(progress_reporter, 'on_high_confidence_contracts_started', total=len(high_confidence_items))

            async def _run_high_confidence(item: Tuple[str, Sequence[DuplicateCandidate]]) -> Dict[str, Any]:
                async with client.contract_semaphore:
                    return await _analyze_high_confidence_contract_async(
                        client=client,
                        chain=chain,
                        network=network,
                        alchemy_api_key=alchemy_api_key,
                        etherscan_api_key=etherscan_api_key,
                        opensea_api_key=opensea_api_key,
                        contract_address=item[0],
                        contract_candidates=item[1],
                        token_type=token_type,
                        official_addresses=official_addresses,
                        candidate_open_license_by_token=candidate_open_license_by_token,
                        timeout=timeout,
                        signal_cache=signal_cache,
                    )

            completed_contracts = 0
            for result in await asyncio.gather(*[_run_high_confidence(item) for item in high_confidence_items]):
                completed_contracts += 1
                contract_address = result['contract_address']
                _notify_progress(
                    progress_reporter,
                    'on_high_confidence_contract_completed',
                    contract_address=contract_address,
                    completed=completed_contracts,
                    total=len(high_confidence_items),
                )
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
                honest_addresses.extend(result.get('honest_addresses') or [])
                honest_address_stats.update(result.get('honest_address_stats') or {})
                victim_addresses.extend(result.get('victim_addresses') or [])
                fraud_trade_stats.update(result.get('fraud_trade_stats') or {})
            stage_times['analyze_high_confidence_contracts'] = time.perf_counter() - started
        else:
            _notify_progress(progress_reporter, 'on_high_confidence_contracts_started', total=0)

        if open_license:
            high_confidence = []
            low_confidence = []

        _notify_progress(progress_reporter, 'on_seed_stage', 'finalize_report', seed_contract_address=seed_contract_address)
        report_summary = build_report_summary(
            open_license=open_license,
            grouped=grouped,
            high_confidence=high_confidence,
            low_confidence=low_confidence,
            legit_duplicates=legit_duplicates,
            infringing_tokens=infringing_tokens,
            honest_addresses=honest_addresses,
            victim_addresses=victim_addresses,
            address_signals=address_signals,
        )
        stage_times['seed_total'] = time.perf_counter() - seed_started
        logger.info(
            'seed timing seed=%s %s',
            seed_contract_address,
            _format_timing_breakdown(stage_times),
        )
        _notify_progress(
            progress_reporter,
            'on_seed_completed',
            seed_contract_address=seed_contract_address,
            report_summary=report_summary,
        )

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
            'honest_addresses': honest_addresses,
            'honest_address_stats': honest_address_stats,
            'victim_addresses': victim_addresses,
            'fraud_trade_stats': fraud_trade_stats,
            'report_summary': report_summary,
        }
    finally:
        await client.close()
        if own_conn and conn is not None:
            conn.close()


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
    api_max_concurrency: int = DEFAULT_API_MAX_CONCURRENCY,
    contract_max_concurrency: int = DEFAULT_CONTRACT_MAX_CONCURRENCY,
    sale_metric_max_concurrency: int = DEFAULT_SALE_METRIC_MAX_CONCURRENCY,
    progress_reporter=None,
) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            async_analyze_seed_contract(
                chain=chain,
                seed_contract_address=seed_contract_address,
                alchemy_api_key=alchemy_api_key,
                alchemy_network=alchemy_network,
                etherscan_api_key=etherscan_api_key,
                opensea_api_key=opensea_api_key,
                conn=conn,
                feature_store=feature_store,
                signal_cache=signal_cache,
                name_threshold=name_threshold,
                metadata_threshold=metadata_threshold,
                timeout=timeout,
                max_recall_rows=max_recall_rows,
                max_tokens_per_contract=max_tokens_per_contract,
                api_max_concurrency=api_max_concurrency,
                contract_max_concurrency=contract_max_concurrency,
                sale_metric_max_concurrency=sale_metric_max_concurrency,
                progress_reporter=progress_reporter,
            )
        )
    raise RuntimeError('analyze_seed_contract cannot be called from a running event loop; use async_analyze_seed_contract instead')
