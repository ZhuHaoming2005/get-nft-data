from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import math
from typing import Dict, List, Optional, Sequence


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
    match_reasons: tuple[str, ...]
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
