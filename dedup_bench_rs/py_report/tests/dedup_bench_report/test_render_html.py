from pathlib import Path

from dedup_bench_report.analyzer import build_analysis, load_benchmark_result
from dedup_bench_report.render_html import render_html_report


FIXTURE_PATH = Path("tests/fixtures/result_sample.json")


def test_rendered_html_contains_all_major_sections():
    analysis = build_analysis(load_benchmark_result(FIXTURE_PATH))
    html = render_html_report(analysis, title="Demo Report")
    assert "摘要" in html
    assert "时间成本" in html
    assert "查重效果" in html
    assert "Name 与 Metadata 覆盖关系" in html
    assert "结论建议" in html


def test_rendered_html_states_contract_level_semantics():
    analysis = build_analysis(load_benchmark_result(FIXTURE_PATH))
    html = render_html_report(analysis, title="Demo Report")
    assert "按合约级命中统计" in html
    assert "Plotly.newPlot" in html
    assert "Name 算法耗时" in html
    assert "Metadata 算法耗时" in html


def test_rendered_html_does_not_embed_empty_plot_container_in_head():
    analysis = build_analysis(load_benchmark_result(FIXTURE_PATH))
    html = render_html_report(analysis, title="Demo Report")
    head = html.split("</head>", 1)[0]
    assert "<div" not in head


def test_real_result_json_builds_analysis_without_schema_errors():
    real_path = Path("../results/result.json")
    payload = load_benchmark_result(real_path)
    analysis = build_analysis(payload)
    assert analysis.payload.chain
    assert analysis.normalized_rows
    assert analysis.coverage_rows
