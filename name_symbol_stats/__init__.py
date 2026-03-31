"""Name and symbol duplicate statistics package."""

from .config import DEFAULT_CHAINS, DEFAULT_THRESHOLDS
from .normalize import build_name_block_key, normalize_name, normalize_symbol
from .report import DuplicateGroupStats, SummaryRow, summarize_groups

__all__ = [
    "DEFAULT_CHAINS",
    "DEFAULT_THRESHOLDS",
    "DuplicateGroupStats",
    "SummaryRow",
    "build_name_block_key",
    "normalize_name",
    "normalize_symbol",
    "summarize_groups",
]
