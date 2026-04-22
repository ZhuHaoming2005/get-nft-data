from pathlib import Path

from dedup_bench_report.analyzer import build_analysis, load_benchmark_result, normalize_algorithms


FIXTURE_PATH = Path("tests/fixtures/result_sample.json")


def test_metadata_duplicates_are_folded_to_contract_level():
    payload = load_benchmark_result(FIXTURE_PATH)
    normalized = normalize_algorithms(payload)
    metadata_row = next(row for row in normalized if row.algorithm_id == "metadata_token_jaccard")
    assert metadata_row.group == "metadata"
    assert metadata_row.contract_hit_count == 2
    assert metadata_row.contract_hits == {"0xbbb", "0xccc"}


def test_name_duplicates_preserve_contract_level_hits():
    payload = load_benchmark_result(FIXTURE_PATH)
    normalized = normalize_algorithms(payload)
    name_row = next(row for row in normalized if row.algorithm_id == "name_jaro_winkler")
    assert name_row.group == "name"
    assert name_row.contract_hit_count == 2
    assert name_row.contract_hits == {"0xaaa", "0xbbb"}


def test_pairwise_coverage_is_measured_against_metadata_contracts():
    payload = load_benchmark_result(FIXTURE_PATH)
    analysis = build_analysis(payload)
    pair = analysis.coverage_rows[0]
    assert pair.name_algorithm == "name_jaro_winkler"
    assert pair.metadata_algorithm == "metadata_token_jaccard"
    assert pair.intersection_contract_count == 1
    assert pair.metadata_contract_count == 2
    assert pair.coverage_rate == 0.5
    assert pair.metadata_only_contract_count == 1
    assert pair.metadata_only_contracts == ["0xccc"]


def test_recommendation_flags_metadata_gaps_as_evidence():
    payload = load_benchmark_result(FIXTURE_PATH)
    analysis = build_analysis(payload)
    joined = "\n".join(analysis.recommendation_lines)
    assert "metadata_token_jaccard" in joined
    assert "仍遗漏 1 个" in joined


def test_empty_metadata_match_set_yields_na_coverage():
    payload = load_benchmark_result(FIXTURE_PATH)
    payload.metadata_algorithms[0]["duplicates"] = []
    analysis = build_analysis(payload)
    pair = analysis.coverage_rows[0]
    assert pair.metadata_contract_count == 0
    assert pair.coverage_rate is None
