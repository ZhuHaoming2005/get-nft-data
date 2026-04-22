# Dedup Benchmark HTML Analysis Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python CLI specialized to the current `dedup_bench_rs/results/result.json` shape and write a single-file HTML analysis report focused on time cost, contract-level dedup effectiveness, and pairwise `name`-vs-`metadata` coverage.

**Architecture:** Keep the Rust benchmark output unchanged and add a dedicated Python workspace under `dedup_bench_rs/py_report`. That subtree will contain the package, tests, fixtures, and dependency file, while the CLI reads the current `dedup_bench_rs/results/result.json` by direct field access, with no forward-compatibility layer, and writes a self-contained Plotly-backed HTML report back into `dedup_bench_rs/results/`.

**Tech Stack:** Python 3, dataclasses, pathlib, json, pandas, plotly, pytest

---

### File Map

**Create:**
- `dedup_bench_rs/py_report/dedup_bench_report/__init__.py`
- `dedup_bench_rs/py_report/dedup_bench_report/models.py`
- `dedup_bench_rs/py_report/dedup_bench_report/analyzer.py`
- `dedup_bench_rs/py_report/dedup_bench_report/render_html.py`
- `dedup_bench_rs/py_report/dedup_bench_report/cli.py`
- `dedup_bench_rs/py_report/tests/dedup_bench_report/test_analyzer.py`
- `dedup_bench_rs/py_report/tests/dedup_bench_report/test_render_html.py`
- `dedup_bench_rs/py_report/tests/dedup_bench_report/test_cli.py`
- `dedup_bench_rs/py_report/tests/fixtures/result_sample.json`
- `dedup_bench_rs/py_report/requirements.txt`

**Modify:**
- `docs/superpowers/specs/2026-04-22-dedup-benchmark-html-analysis-report-design.md` (only if plan execution exposes a spec contradiction)

**Primary test targets:**
- Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_analyzer.py -q`
- Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_render_html.py -q`
- Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_cli.py -q`
- Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report -q`

### Task 1: Scaffold The Package And Normalize Input Shape

**Files:**
- Create: `dedup_bench_rs/py_report/dedup_bench_report/__init__.py`
- Create: `dedup_bench_rs/py_report/dedup_bench_report/models.py`
- Create: `dedup_bench_rs/py_report/tests/fixtures/result_sample.json`
- Create: `dedup_bench_rs/py_report/tests/dedup_bench_report/test_analyzer.py`

- [ ] **Step 1: Write the fixture and failing normalization tests**

```python
# dedup_bench_rs/py_report/tests/dedup_bench_report/test_analyzer.py
from pathlib import Path

from dedup_bench_report.analyzer import load_benchmark_result, normalize_algorithms


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
```

- [ ] **Step 2: Run the analyzer test to verify it fails**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_analyzer.py -q`
Expected: FAIL with `ModuleNotFoundError` for `dedup_bench_report`

- [ ] **Step 3: Add the sample fixture JSON**

```json
{
  "chain": "ethereum",
  "source": { "kind": "duckdb_table", "location": "../output/features.duckdb" },
  "sample": {
    "contract_address": "0xseed",
    "token_id": "1",
    "name": "Seed NFT"
  },
  "recall_elapsed_ms": 1234.5,
  "recall_candidate_count": 456789,
  "reference": {
    "algorithm_id": "current_name_metadata_reference",
    "field": "reference",
    "decision_rule": "name_score >= 95.0 OR metadata_score >= 0.55",
    "repeat": 1,
    "runs_ms": [100.0],
    "avg_ms": 100.0,
    "min_ms": 100.0,
    "duplicate_count": 3,
    "duplicates": []
  },
  "name_algorithms": [
    {
      "algorithm_id": "name_jaro_winkler",
      "field": "name",
      "decision_rule": "score >= 97.0",
      "repeat": 1,
      "runs_ms": [10.0],
      "avg_ms": 10.0,
      "min_ms": 10.0,
      "duplicate_count": 2,
      "duplicates": [
        { "contract_address": "0xaaa", "max_score": 99.0, "duplicate_token_count": 2 },
        { "contract_address": "0xbbb", "max_score": 98.0, "duplicate_token_count": 1 }
      ]
    }
  ],
  "metadata_algorithms": [
    {
      "algorithm_id": "metadata_token_jaccard",
      "field": "metadata",
      "decision_rule": "score >= 0.80",
      "repeat": 1,
      "runs_ms": [50.0],
      "avg_ms": 50.0,
      "min_ms": 50.0,
      "duplicate_count": 3,
      "duplicates": [
        { "contract_address": "0xbbb", "token_id": "1", "metadata_doc": "x", "score": 0.9 },
        { "contract_address": "0xbbb", "token_id": "2", "metadata_doc": "y", "score": 0.85 },
        { "contract_address": "0xccc", "token_id": "3", "metadata_doc": "z", "score": 0.82 }
      ]
    }
  ]
}
```

- [ ] **Step 4: Create the models and loader**

```python
# dedup_bench_rs/py_report/dedup_bench_report/models.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


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
class BenchmarkPayload:
    input_path: Path
    chain: str
    source_kind: str
    source_location: str
    sample_name: str
    sample_contract_address: str
    recall_elapsed_ms: float
    recall_candidate_count: int
    reference: dict[str, object]
    name_algorithms: list[dict[str, object]]
    metadata_algorithms: list[dict[str, object]]
```

```python
# dedup_bench_rs/py_report/dedup_bench_report/analyzer.py
from __future__ import annotations

import json
from pathlib import Path

from .models import BenchmarkPayload, NormalizedAlgorithmRow


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
        reference=raw["reference"],
        name_algorithms=raw["name_algorithms"],
        metadata_algorithms=raw["metadata_algorithms"],
    )


def normalize_algorithms(payload: BenchmarkPayload) -> list[NormalizedAlgorithmRow]:
    rows: list[NormalizedAlgorithmRow] = []
    for entry in payload.name_algorithms:
        contract_hits = {dup["contract_address"] for dup in entry["duplicates"]}
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
        contract_hits = {dup["contract_address"] for dup in entry["duplicates"]}
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
```

- [ ] **Step 5: Add the package entrypoint**

```python
# dedup_bench_rs/py_report/dedup_bench_report/__init__.py
__all__ = ["__version__"]

__version__ = "0.1.0"
```

- [ ] **Step 6: Run the analyzer test to verify it passes**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_analyzer.py -q`
Expected: PASS with `2 passed`

- [ ] **Step 7: Commit**

```bash
git add dedup_bench_report/__init__.py dedup_bench_report/models.py dedup_bench_report/analyzer.py tests/fixtures/result_sample.json tests/dedup_bench_report/test_analyzer.py
git commit -m "feat: add benchmark report input normalization"
```

### Task 2: Compute Pairwise Coverage And Recommendation Evidence

**Files:**
- Modify: `dedup_bench_rs/py_report/dedup_bench_report/models.py`
- Modify: `dedup_bench_rs/py_report/dedup_bench_report/analyzer.py`
- Modify: `dedup_bench_rs/py_report/tests/dedup_bench_report/test_analyzer.py`

- [ ] **Step 1: Extend the failing tests with pairwise coverage assertions**

```python
from dedup_bench_report.analyzer import build_analysis


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
    assert "0xccc" not in joined
    assert "1" in joined
```

- [ ] **Step 2: Run the analyzer tests to verify they fail**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_analyzer.py -q`
Expected: FAIL with missing `build_analysis`

- [ ] **Step 3: Add analysis result models**

```python
# dedup_bench_rs/py_report/dedup_bench_report/models.py
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
```

- [ ] **Step 4: Implement pairwise coverage and recommendation analysis**

```python
# dedup_bench_rs/py_report/dedup_bench_report/analyzer.py
from .models import BenchmarkAnalysis, PairwiseCoverageRow


def build_analysis(payload: BenchmarkPayload) -> BenchmarkAnalysis:
    normalized = normalize_algorithms(payload)
    name_rows = [row for row in normalized if row.group == "name"]
    metadata_rows = [row for row in normalized if row.group == "metadata"]
    coverage_rows: list[PairwiseCoverageRow] = []
    for name_row in name_rows:
        for metadata_row in metadata_rows:
            intersection = sorted(name_row.contract_hits & metadata_row.contract_hits)
            metadata_only = sorted(metadata_row.contract_hits - name_row.contract_hits)
            metadata_contract_count = metadata_row.contract_hit_count
            coverage_rate = (
                len(intersection) / metadata_contract_count
                if metadata_contract_count
                else None
            )
            coverage_rows.append(
                PairwiseCoverageRow(
                    name_algorithm=name_row.algorithm_id,
                    metadata_algorithm=metadata_row.algorithm_id,
                    name_contract_count=name_row.contract_hit_count,
                    metadata_contract_count=metadata_contract_count,
                    intersection_contract_count=len(intersection),
                    coverage_rate=coverage_rate,
                    metadata_only_contract_count=len(metadata_only),
                    metadata_only_contracts=metadata_only,
                )
            )
    recommendation_lines = _build_recommendations(name_rows, metadata_rows, coverage_rows)
    return BenchmarkAnalysis(
        payload=payload,
        normalized_rows=normalized,
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
        related = [row for row in coverage_rows if row.metadata_algorithm == metadata_row.algorithm_id]
        best = max(related, key=lambda row: (-1.0 if row.coverage_rate is None else row.coverage_rate))
        best_rate = 0.0 if best.coverage_rate is None else best.coverage_rate
        if best.metadata_only_contract_count > 0:
            lines.append(
                f"{metadata_row.algorithm_id} still adds value: best name coverage is "
                f"{best_rate:.1%}, leaving {best.metadata_only_contract_count} metadata-only contracts."
            )
        else:
            lines.append(
                f"{metadata_row.algorithm_id} is fully covered by {best.name_algorithm} "
                f"at {best_rate:.1%} coverage."
            )
    fastest_name = min(name_rows, key=lambda row: row.avg_ms)
    fastest_metadata = min(metadata_rows, key=lambda row: row.avg_ms)
    lines.append(
        f"Fastest name algorithm: {fastest_name.algorithm_id} ({fastest_name.avg_ms:.3f} ms); "
        f"fastest metadata algorithm: {fastest_metadata.algorithm_id} ({fastest_metadata.avg_ms:.3f} ms)."
    )
    return lines
```

- [ ] **Step 5: Add a zero-metadata-count regression test**

```python
def test_empty_metadata_match_set_yields_na_coverage():
    payload = load_benchmark_result(FIXTURE_PATH)
    payload.metadata_algorithms[0]["duplicates"] = []
    analysis = build_analysis(payload)
    pair = analysis.coverage_rows[0]
    assert pair.metadata_contract_count == 0
    assert pair.coverage_rate is None
```

- [ ] **Step 6: Run the analyzer tests to verify they pass**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_analyzer.py -q`
Expected: PASS with all analyzer tests green

- [ ] **Step 7: Commit**

```bash
git add dedup_bench_report/models.py dedup_bench_report/analyzer.py tests/dedup_bench_report/test_analyzer.py
git commit -m "feat: add benchmark coverage and recommendation analysis"
```

### Task 3: Render The Single-File HTML Report

**Files:**
- Create: `dedup_bench_rs/py_report/dedup_bench_report/render_html.py`
- Create: `dedup_bench_rs/py_report/tests/dedup_bench_report/test_render_html.py`
- Create: `dedup_bench_rs/py_report/requirements.txt`

- [ ] **Step 1: Write the failing renderer tests**

```python
from pathlib import Path

from dedup_bench_report.analyzer import build_analysis, load_benchmark_result
from dedup_bench_report.render_html import render_html_report


FIXTURE_PATH = Path("tests/fixtures/result_sample.json")


def test_rendered_html_contains_all_major_sections():
    analysis = build_analysis(load_benchmark_result(FIXTURE_PATH))
    html = render_html_report(analysis, title="Demo Report")
    assert "Summary" in html
    assert "Time Cost" in html
    assert "Dedup Effectiveness" in html
    assert "Name-vs-Metadata Coverage" in html
    assert "Recommendation" in html


def test_rendered_html_states_contract_level_semantics():
    analysis = build_analysis(load_benchmark_result(FIXTURE_PATH))
    html = render_html_report(analysis, title="Demo Report")
    assert "按合约级命中统计" in html
    assert "Plotly.newPlot" in html
```

- [ ] **Step 2: Run the renderer test to verify it fails**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_render_html.py -q`
Expected: FAIL with missing `render_html_report`

- [ ] **Step 3: Add the dedicated dependency list**

```text
# dedup_bench_rs/py_report/requirements.txt
pandas>=2.2,<3
plotly>=5.24,<6
pytest>=8,<9
```

- [ ] **Step 4: Install the reporting dependencies**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pip install -r requirements.txt`
Expected: pip installs or confirms `pandas`, `plotly`, and `pytest`

- [ ] **Step 5: Implement the HTML renderer with embedded Plotly charts**

```python
# dedup_bench_rs/py_report/dedup_bench_report/render_html.py
from __future__ import annotations

import html

import pandas as pd
import plotly.express as px
import plotly.io as pio

from .models import BenchmarkAnalysis


def _figure_div(figure) -> str:
    return pio.to_html(figure, include_plotlyjs=False, full_html=False)


def render_html_report(analysis: BenchmarkAnalysis, title: str) -> str:
    normalized_df = pd.DataFrame(
        [
            {
                "algorithm_id": row.algorithm_id,
                "group": row.group,
                "avg_ms": row.avg_ms,
                "min_ms": row.min_ms,
                "contract_hit_count": row.contract_hit_count,
            }
            for row in analysis.normalized_rows
        ]
    )
    coverage_df = pd.DataFrame(
        [
            {
                "name_algorithm": row.name_algorithm,
                "metadata_algorithm": row.metadata_algorithm,
                "coverage_rate": row.coverage_rate,
                "metadata_only_contract_count": row.metadata_only_contract_count,
            }
            for row in analysis.coverage_rows
        ]
    )
    time_chart = px.bar(
        normalized_df,
        x="avg_ms",
        y="algorithm_id",
        color="group",
        orientation="h",
        title="Algorithm Avg Time Cost",
    )
    effect_chart = px.bar(
        normalized_df,
        x="algorithm_id",
        y="contract_hit_count",
        color="group",
        title="Contract-Level Dedup Hits",
    )
    coverage_chart = px.density_heatmap(
        coverage_df,
        x="metadata_algorithm",
        y="name_algorithm",
        z="coverage_rate",
        text_auto=".0%",
        title="Name-vs-Metadata Coverage",
    )
    summary_items = [
        ("Chain", analysis.payload.chain),
        ("Sample Name", analysis.payload.sample_name),
        ("Sample Contract", analysis.payload.sample_contract_address or "-"),
        ("Recall Candidates", f"{analysis.payload.recall_candidate_count:,}"),
        ("Recall Elapsed", f"{analysis.payload.recall_elapsed_ms:.3f} ms"),
    ]
    summary_html = "".join(
        f"<div class='card'><div class='label'>{html.escape(label)}</div><div class='value'>{html.escape(value)}</div></div>"
        for label, value in summary_items
    )
    recommendation_html = "".join(f"<li>{html.escape(line)}</li>" for line in analysis.recommendation_lines)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <script>{pio.to_html(time_chart, include_plotlyjs=True, full_html=False).split('<script type="text/javascript">')[1].split('</script>')[0]}</script>
  <style>
    body {{ font-family: Segoe UI, sans-serif; margin: 24px; color: #1f2937; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 12px; padding: 12px 14px; background: #f8fafc; }}
    .label {{ font-size: 12px; color: #6b7280; text-transform: uppercase; }}
    .value {{ font-size: 20px; font-weight: 700; margin-top: 6px; }}
    section {{ margin-top: 28px; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p>按合约级命中统计；只要发现合约涉及重复即算成功查重。</p>
  <section><h2>Summary</h2><div class="cards">{summary_html}</div></section>
  <section><h2>Time Cost</h2>{_figure_div(time_chart)}</section>
  <section><h2>Dedup Effectiveness</h2>{_figure_div(effect_chart)}</section>
  <section><h2>Name-vs-Metadata Coverage</h2>{_figure_div(coverage_chart)}</section>
  <section><h2>Recommendation</h2><ul>{recommendation_html}</ul></section>
</body>
</html>"""
```

- [ ] **Step 6: Refine the renderer to avoid double-injecting Plotly JS**

```python
PLOTLY_JS = pio.to_html(px.scatter(), include_plotlyjs=True, full_html=False)
PLOTLY_JS = PLOTLY_JS.split("<body>")[-1].split("</body>")[0]


def render_html_report(analysis: BenchmarkAnalysis, title: str) -> str:
    ...
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  {PLOTLY_JS}
  <style>...</style>
</head>
<body>...</body>
</html>"""
```

- [ ] **Step 7: Run the renderer tests to verify they pass**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_render_html.py -q`
Expected: PASS with renderer tests green

- [ ] **Step 8: Commit**

```bash
git add dedup_bench_report/render_html.py tests/dedup_bench_report/test_render_html.py requirements.txt
git commit -m "feat: render benchmark html analysis report"
```

### Task 4: Add The CLI And File Output Flow

**Files:**
- Create: `dedup_bench_rs/py_report/dedup_bench_report/cli.py`
- Create: `dedup_bench_rs/py_report/tests/dedup_bench_report/test_cli.py`

- [ ] **Step 1: Write the failing CLI test**

```python
import subprocess
import sys
from pathlib import Path


FIXTURE_PATH = Path("tests/fixtures/result_sample.json")


def test_cli_writes_single_file_html(tmp_path):
    output_path = tmp_path / "report.html"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dedup_bench_report.cli",
            "--input",
            str(FIXTURE_PATH),
            "--output",
            str(output_path),
            "--title",
            "CLI Demo",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert output_path.exists()
    html = output_path.read_text(encoding="utf-8")
    assert "<html" in html.lower()
    assert "CLI Demo" in html
```

- [ ] **Step 2: Run the CLI test to verify it fails**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_cli.py -q`
Expected: FAIL with missing CLI module

- [ ] **Step 3: Implement the CLI**

```python
# dedup_bench_rs/py_report/dedup_bench_report/cli.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

from .analyzer import build_analysis, load_benchmark_result
from .render_html import render_html_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render dedup_bench_rs benchmark JSON into a single-file HTML analysis report."
    )
    parser.add_argument("--input", required=True, help="path to benchmark result JSON")
    parser.add_argument("--output", required=True, help="path to output HTML report")
    parser.add_argument("--title", default="Dedup Benchmark Analysis Report", help="report title")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input)
    output_path = Path(args.output)
    payload = load_benchmark_result(input_path)
    analysis = build_analysis(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html_report(analysis, title=args.title), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add a default-title regression test**

```python
def test_cli_uses_default_title_when_omitted(tmp_path):
    output_path = tmp_path / "report.html"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dedup_bench_report.cli",
            "--input",
            str(FIXTURE_PATH),
            "--output",
            str(output_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    html = output_path.read_text(encoding="utf-8")
    assert "Dedup Benchmark Analysis Report" in html
```

- [ ] **Step 5: Run the CLI tests to verify they pass**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report/test_cli.py -q`
Expected: PASS with CLI tests green

- [ ] **Step 6: Commit**

```bash
git add dedup_bench_report/cli.py tests/dedup_bench_report/test_cli.py
git commit -m "feat: add dedup benchmark html report cli"
```

### Task 5: End-To-End Verification On The Real Benchmark Result

**Files:**
- Modify: `dedup_bench_rs/py_report/tests/dedup_bench_report/test_render_html.py`
- Test: `dedup_bench_rs/results/result.json`

- [ ] **Step 1: Add a smoke test against the real report shape**

```python
from pathlib import Path


def test_real_result_json_builds_analysis_without_schema_errors():
    real_path = Path("../results/result.json")
    payload = load_benchmark_result(real_path)
    analysis = build_analysis(payload)
    assert analysis.payload.chain
    assert analysis.normalized_rows
    assert analysis.coverage_rows
```

- [ ] **Step 2: Run the package test suite**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests/dedup_bench_report -q`
Expected: PASS with all dedup benchmark report tests green

- [ ] **Step 3: Generate the real HTML report**

Run from `dedup_bench_rs/py_report`: `C:\Users\z1766\.conda\envs\codex\python.exe -m dedup_bench_report.cli --input ..\results\result.json --output ..\results\result_analysis.html`
Expected: exit code `0` and a new `dedup_bench_rs/results/result_analysis.html`

- [ ] **Step 4: Verify the output file contains the key sections**

Run from `dedup_bench_rs/py_report`: `@'
from pathlib import Path
html = Path("../results/result_analysis.html").read_text(encoding="utf-8")
for marker in ["Summary", "Time Cost", "Dedup Effectiveness", "Name-vs-Metadata Coverage", "Recommendation"]:
    assert marker in html, marker
print("ok")
'@ | C:\Users\z1766\.conda\envs\codex\python.exe -`
Expected: prints `ok`

- [ ] **Step 5: Commit only the smoke test, not the generated artifact**

```bash
git add tests/dedup_bench_report/test_render_html.py
git commit -m "test: verify dedup benchmark html analysis end to end"
```
