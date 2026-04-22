from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BenchmarkPayload:
    input_path: Path
    chain: str
    source_kind: str
    source_location: str
    sample_name: str
    sample_contract_address: str
    recall_elapsed_ms: float
    recall_candidate_count: int
    name_algorithms: list[dict[str, object]]
    metadata_algorithms: list[dict[str, object]]


@dataclass(frozen=True)
class NormalizedAlgorithmRow:
    algorithm_id: str
    group: str
    decision_rule: str
    repeat: int
    runs_ms: list[float]
    avg_ms: float
    min_ms: float
    contract_hits: set[str]
    contract_hit_count: int


@dataclass(frozen=True)
class PairwiseCoverageRow:
    name_algorithm: str
    metadata_algorithm: str
    name_contract_count: int
    metadata_contract_count: int
    intersection_contract_count: int
    coverage_rate: float | None
    metadata_only_contract_count: int
    metadata_only_contracts: list[str]


@dataclass(frozen=True)
class BenchmarkAnalysis:
    payload: BenchmarkPayload
    normalized_rows: list[NormalizedAlgorithmRow]
    coverage_rows: list[PairwiseCoverageRow]
    recommendation_lines: list[str]
