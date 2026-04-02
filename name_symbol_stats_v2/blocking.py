from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdaptiveBlockingConfig:
    small_canopy_max: int = 64
    medium_canopy_max: int = 8000
    large_canopy_max: int = 30000
    length_bucket_size: int = 6


@dataclass(frozen=True)
class AdaptiveCanopyCounts:
    p3_count: int
    p3_len_count: int
    p4_len_count: int


def collapsed_length_bucket(length: int, *, bucket_size: int = 6) -> int:
    if length <= 0:
        return 0
    return (length // bucket_size) * bucket_size


def collapsed_prefix(value: str, length: int) -> str:
    if length <= 0:
        return ""
    return value[:length]


def choose_adaptive_block_key(
    name_collapsed: str,
    name_collapsed_len: int,
    counts: AdaptiveCanopyCounts,
    config: AdaptiveBlockingConfig,
) -> str:
    if not name_collapsed:
        return ""

    length_bucket = collapsed_length_bucket(name_collapsed_len, bucket_size=config.length_bucket_size)
    p3 = collapsed_prefix(name_collapsed, 3)
    if counts.p3_count < config.small_canopy_max:
        return p3
    if counts.p3_count <= config.medium_canopy_max:
        return f"{p3}|{length_bucket}"
    if counts.p4_len_count > config.large_canopy_max:
        return f"{collapsed_prefix(name_collapsed, 5)}|{length_bucket}"
    return f"{collapsed_prefix(name_collapsed, 4)}|{length_bucket}"

