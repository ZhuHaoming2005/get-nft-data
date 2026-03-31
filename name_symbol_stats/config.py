from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_CHAINS: tuple[str, ...] = ("ethereum", "base", "polygon", "solana")
DEFAULT_THRESHOLDS: tuple[float, ...] = (85.0, 90.0, 95.0)
DEFAULT_WORKERS = 4
DEFAULT_MAX_BLOCK_SIZE = 2_000
DEFAULT_TRIGRAM_CUTOFF = 0.35
DEFAULT_MAX_LEN_DELTA = 12


def _unique_ordered(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        key = value.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return tuple(ordered)


def parse_thresholds(values: Sequence[float] | None) -> tuple[float, ...]:
    if not values:
        return DEFAULT_THRESHOLDS
    uniq = sorted({float(value) for value in values})
    return tuple(uniq)


@dataclass(frozen=True)
class StatsConfig:
    chains: tuple[str, ...] = DEFAULT_CHAINS
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS
    workers: int = DEFAULT_WORKERS
    max_block_size: int = DEFAULT_MAX_BLOCK_SIZE
    trigram_cutoff: float = DEFAULT_TRIGRAM_CUTOFF
    max_len_delta: int = DEFAULT_MAX_LEN_DELTA
    export_dir: Path = Path("name_symbol_stats_output")

    @classmethod
    def from_args(
        cls,
        *,
        chains: Sequence[str] | None = None,
        thresholds: Sequence[float] | None = None,
        workers: int = DEFAULT_WORKERS,
        max_block_size: int = DEFAULT_MAX_BLOCK_SIZE,
        trigram_cutoff: float = DEFAULT_TRIGRAM_CUTOFF,
        max_len_delta: int = DEFAULT_MAX_LEN_DELTA,
        export_dir: str | Path = "name_symbol_stats_output",
    ) -> "StatsConfig":
        return cls(
            chains=_unique_ordered(chains or DEFAULT_CHAINS),
            thresholds=parse_thresholds(thresholds),
            workers=max(1, int(workers)),
            max_block_size=max(2, int(max_block_size)),
            trigram_cutoff=float(trigram_cutoff),
            max_len_delta=max(0, int(max_len_delta)),
            export_dir=Path(export_dir),
        )
