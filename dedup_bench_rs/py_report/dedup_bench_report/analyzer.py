from __future__ import annotations

import json
from pathlib import Path

from .models import (
    BenchmarkAnalysis,
    BenchmarkPayload,
    NormalizedAlgorithmRow,
    PairwiseCoverageRow,
)


def load_benchmark_result(path: Path) -> BenchmarkPayload:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return BenchmarkPayload(
        input_path=path,
        chain=raw["chain"],
        source_kind=raw["source"]["kind"],
        source_location=raw["source"]["location"],
        sample_name=raw["sample"].get("name", ""),
        sample_contract_address=raw["sample"].get("contract_address", ""),
        recall_elapsed_ms=float(raw["recall_elapsed_ms"]),
        recall_candidate_count=int(raw["recall_candidate_count"]),
        name_algorithms=raw["name_algorithms"],
        metadata_algorithms=raw["metadata_algorithms"],
    )


def normalize_algorithms(payload: BenchmarkPayload) -> list[NormalizedAlgorithmRow]:
    rows: list[NormalizedAlgorithmRow] = []
    for entry in payload.name_algorithms:
        contract_hits = {item["contract_address"] for item in entry["duplicates"]}
        rows.append(
            NormalizedAlgorithmRow(
                algorithm_id=entry["algorithm_id"],
                group="name",
                decision_rule=entry["decision_rule"],
                repeat=int(entry["repeat"]),
                runs_ms=[float(value) for value in entry["runs_ms"]],
                avg_ms=float(entry["avg_ms"]),
                min_ms=float(entry["min_ms"]),
                contract_hits=contract_hits,
                contract_hit_count=len(contract_hits),
            )
        )
    for entry in payload.metadata_algorithms:
        contract_hits = {item["contract_address"] for item in entry["duplicates"]}
        rows.append(
            NormalizedAlgorithmRow(
                algorithm_id=entry["algorithm_id"],
                group="metadata",
                decision_rule=entry["decision_rule"],
                repeat=int(entry["repeat"]),
                runs_ms=[float(value) for value in entry["runs_ms"]],
                avg_ms=float(entry["avg_ms"]),
                min_ms=float(entry["min_ms"]),
                contract_hits=contract_hits,
                contract_hit_count=len(contract_hits),
            )
        )
    return rows


def build_analysis(payload: BenchmarkPayload) -> BenchmarkAnalysis:
    normalized_rows = normalize_algorithms(payload)
    name_rows = [row for row in normalized_rows if row.group == "name"]
    metadata_rows = [row for row in normalized_rows if row.group == "metadata"]

    coverage_rows: list[PairwiseCoverageRow] = []
    for name_row in name_rows:
        for metadata_row in metadata_rows:
            intersection = sorted(name_row.contract_hits & metadata_row.contract_hits)
            metadata_only = sorted(metadata_row.contract_hits - name_row.contract_hits)
            metadata_contract_count = metadata_row.contract_hit_count
            coverage_rows.append(
                PairwiseCoverageRow(
                    name_algorithm=name_row.algorithm_id,
                    metadata_algorithm=metadata_row.algorithm_id,
                    name_contract_count=name_row.contract_hit_count,
                    metadata_contract_count=metadata_contract_count,
                    intersection_contract_count=len(intersection),
                    coverage_rate=(
                        len(intersection) / metadata_contract_count
                        if metadata_contract_count
                        else None
                    ),
                    metadata_only_contract_count=len(metadata_only),
                    metadata_only_contracts=metadata_only,
                )
            )

    recommendation_lines = _build_recommendations(name_rows, metadata_rows, coverage_rows)
    return BenchmarkAnalysis(
        payload=payload,
        normalized_rows=normalized_rows,
        coverage_rows=coverage_rows,
        recommendation_lines=recommendation_lines,
    )


def _build_recommendations(
    name_rows: list[NormalizedAlgorithmRow],
    metadata_rows: list[NormalizedAlgorithmRow],
    coverage_rows: list[PairwiseCoverageRow],
) -> list[str]:
    lines: list[str] = []
    for metadata_row in metadata_rows:
        related = [
            row for row in coverage_rows if row.metadata_algorithm == metadata_row.algorithm_id
        ]
        best = max(
            related,
            key=lambda row: -1.0 if row.coverage_rate is None else row.coverage_rate,
        )
        best_rate = 0.0 if best.coverage_rate is None else best.coverage_rate
        if best.metadata_only_contract_count > 0:
            lines.append(
                f"{metadata_row.algorithm_id} 仍然有补充价值：最佳 name 覆盖率为 "
                f"{best_rate:.1%}，仍遗漏 {best.metadata_only_contract_count} 个仅由 metadata 命中的合约。"
            )
        else:
            lines.append(
                f"{metadata_row.algorithm_id} 已被 {best.name_algorithm} 完全覆盖，覆盖率为 {best_rate:.1%}。"
            )

    if name_rows and metadata_rows:
        fastest_name = min(name_rows, key=lambda row: row.avg_ms)
        fastest_metadata = min(metadata_rows, key=lambda row: row.avg_ms)
        lines.append(
            f"最快的 name 算法是 {fastest_name.algorithm_id}（{fastest_name.avg_ms:.3f} ms）；"
            f"最快的 metadata 算法是 {fastest_metadata.algorithm_id}（{fastest_metadata.avg_ms:.3f} ms）。"
        )

    return lines
