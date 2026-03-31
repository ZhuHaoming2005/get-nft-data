from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

try:
    import polars as pl
except ImportError:  # pragma: no cover - optional dependency
    pl = None


@dataclass(frozen=True)
class DuplicateGroupStats:
    group_key: str
    primary_contract_count: int
    primary_nft_count: int
    total_member_count: int
    total_member_nft_count: int | None = None
    sample_value: str = ""


@dataclass(frozen=True)
class SummaryRow:
    field_name: str
    scope: str
    primary_chain: str
    secondary_chain: str
    threshold: float | None
    total_contracts: int
    total_nfts: int
    group_count: int
    duplicate_contract_count: int
    duplicate_nft_count: int
    duplicate_contract_ratio: float
    duplicate_nft_ratio: float
    group_size_ge_2_count: int
    group_size_gt_2_count: int


def _pct(part: int, total: int) -> float:
    return (part * 100.0 / total) if total else 0.0


def summarize_groups(
    *,
    field_name: str,
    scope: str,
    primary_chain: str,
    secondary_chain: str | None,
    threshold: float | None,
    total_contracts: int,
    total_nfts: int,
    groups: Sequence[DuplicateGroupStats],
) -> SummaryRow:
    group_count = len(groups)
    duplicate_contract_count = sum(group.primary_contract_count for group in groups)
    duplicate_nft_count = sum(group.primary_nft_count for group in groups)
    group_size_ge_2_count = sum(1 for group in groups if group.total_member_count >= 2)
    group_size_gt_2_count = sum(1 for group in groups if group.total_member_count > 2)
    return SummaryRow(
        field_name=field_name,
        scope=scope,
        primary_chain=primary_chain,
        secondary_chain=secondary_chain or "",
        threshold=threshold,
        total_contracts=total_contracts,
        total_nfts=total_nfts,
        group_count=group_count,
        duplicate_contract_count=duplicate_contract_count,
        duplicate_nft_count=duplicate_nft_count,
        duplicate_contract_ratio=_pct(duplicate_contract_count, total_contracts),
        duplicate_nft_ratio=_pct(duplicate_nft_count, total_nfts),
        group_size_ge_2_count=group_size_ge_2_count,
        group_size_gt_2_count=group_size_gt_2_count,
    )


def summary_rows_to_dicts(rows: Iterable[SummaryRow]) -> list[dict[str, object]]:
    return [asdict(row) for row in rows]


def write_csv(rows: Sequence[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_parquet(rows: Sequence[dict[str, object]], path: Path) -> bool:
    if pl is None:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return True


def render_text_summary(rows: Sequence[SummaryRow]) -> str:
    lines = [
        "Name/Symbol Duplicate Summary",
        "=" * 32,
    ]
    for row in rows:
        threshold_label = f"{row.threshold:.1f}" if row.threshold is not None else "exact"
        pair_label = row.primary_chain.upper()
        if row.secondary_chain:
            pair_label = f"{pair_label} -> {row.secondary_chain.upper()}"
        lines.extend(
            [
                f"{row.field_name} | {row.scope} | {pair_label} | threshold={threshold_label}",
                (
                    f"  groups={row.group_count}  duplicate_contracts={row.duplicate_contract_count}"
                    f" ({row.duplicate_contract_ratio:.2f}%)  duplicate_nfts={row.duplicate_nft_count}"
                    f" ({row.duplicate_nft_ratio:.2f}%)"
                ),
                (
                    f"  group_size>=2={row.group_size_ge_2_count}"
                    f"  group_size>2={row.group_size_gt_2_count}"
                ),
            ]
        )
    return "\n".join(lines) + "\n"


def write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_group_samples(groups: Sequence[DuplicateGroupStats], path: Path) -> None:
    serializable = [asdict(group) for group in groups]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
