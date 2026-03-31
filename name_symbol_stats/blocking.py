from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations, product
from typing import Iterable, Iterator, Sequence

from .normalize import build_name_signature


@dataclass(frozen=True)
class BlockingRecord:
    chain: str
    contract_address: str
    nft_count: int
    name_norm: str
    name_len: int
    name_block_key: str


@dataclass(frozen=True)
class BlockPartition:
    partition_key: str
    records: tuple[BlockingRecord, ...]


def _trigrams(value: str) -> set[str]:
    if len(value) < 3:
        return {value} if value else set()
    return {value[index:index + 3] for index in range(len(value) - 2)}


def trigram_jaccard(left: str, right: str) -> float:
    left_trigrams = _trigrams(left)
    right_trigrams = _trigrams(right)
    if not left_trigrams or not right_trigrams:
        return 0.0
    intersection = len(left_trigrams & right_trigrams)
    union = len(left_trigrams | right_trigrams)
    return intersection / union if union else 0.0


def should_compare_names(
    left: BlockingRecord,
    right: BlockingRecord,
    *,
    trigram_cutoff: float,
    max_len_delta: int,
) -> bool:
    if not left.name_norm or not right.name_norm:
        return False
    if abs(left.name_len - right.name_len) > max_len_delta:
        return False
    if left.name_norm == right.name_norm:
        return True
    if left.name_norm in right.name_norm or right.name_norm in left.name_norm:
        return True
    return trigram_jaccard(left.name_norm, right.name_norm) >= trigram_cutoff


def _secondary_partition_key(record: BlockingRecord) -> str:
    signature = build_name_signature(record.name_norm) or record.name_norm[:16]
    return f"{record.name_block_key}|{signature}"


def partition_records(
    records: Sequence[BlockingRecord],
    *,
    max_block_size: int,
) -> list[BlockPartition]:
    primary_groups: dict[str, list[BlockingRecord]] = defaultdict(list)
    for record in records:
        primary_groups[record.name_block_key].append(record)

    partitions: list[BlockPartition] = []
    for block_key in sorted(primary_groups):
        block_records = primary_groups[block_key]
        if len(block_records) <= max_block_size:
            partitions.append(BlockPartition(block_key, tuple(block_records)))
            continue

        secondary_groups: dict[str, list[BlockingRecord]] = defaultdict(list)
        for record in block_records:
            secondary_groups[_secondary_partition_key(record)].append(record)

        for secondary_key in sorted(secondary_groups):
            secondary_records = secondary_groups[secondary_key]
            for chunk_index in range(0, len(secondary_records), max_block_size):
                chunk = tuple(secondary_records[chunk_index:chunk_index + max_block_size])
                partitions.append(BlockPartition(f"{secondary_key}|{chunk_index}", chunk))

    return partitions


def iter_candidate_pairs(
    left_records: Sequence[BlockingRecord],
    right_records: Sequence[BlockingRecord] | None = None,
    *,
    trigram_cutoff: float,
    max_len_delta: int,
) -> Iterator[tuple[BlockingRecord, BlockingRecord, float]]:
    if right_records is None:
        pair_iterable: Iterable[tuple[BlockingRecord, BlockingRecord]] = combinations(left_records, 2)
    else:
        pair_iterable = product(left_records, right_records)

    for left, right in pair_iterable:
        if left.chain == right.chain and left.contract_address == right.contract_address:
            continue
        if right_records is None and left.contract_address >= right.contract_address:
            continue
        if not should_compare_names(left, right, trigram_cutoff=trigram_cutoff, max_len_delta=max_len_delta):
            continue
        yield left, right, trigram_jaccard(left.name_norm, right.name_norm)
