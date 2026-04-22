from __future__ import annotations

import html

import pandas as pd
import plotly.express as px
import plotly.io as pio
from plotly.offline.offline import get_plotlyjs

from .models import BenchmarkAnalysis


PLOTLY_JS = get_plotlyjs()


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
                "repeat": row.repeat,
                "contract_hit_count": row.contract_hit_count,
            }
            for row in analysis.normalized_rows
        ]
    )
    name_df = normalized_df[normalized_df["group"] == "name"].copy()
    metadata_df = normalized_df[normalized_df["group"] == "metadata"].copy()
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

    name_time_chart = px.bar(
        name_df,
        x="avg_ms",
        y="algorithm_id",
        orientation="h",
        title="Name 算法耗时",
        labels={"avg_ms": "平均耗时 (ms)", "algorithm_id": "算法"},
        color_discrete_sequence=["#2563eb"],
    )
    name_time_chart.update_layout(margin={"l": 20, "r": 20, "t": 48, "b": 20}, height=420)

    metadata_time_chart = px.bar(
        metadata_df,
        x="avg_ms",
        y="algorithm_id",
        orientation="h",
        title="Metadata 算法耗时",
        labels={"avg_ms": "平均耗时 (ms)", "algorithm_id": "算法"},
        color_discrete_sequence=["#ea580c"],
    )
    metadata_time_chart.update_layout(
        margin={"l": 20, "r": 20, "t": 48, "b": 20}, height=420
    )

    effectiveness_chart = px.bar(
        normalized_df,
        x="algorithm_id",
        y="contract_hit_count",
        color="group",
        title="合约级命中合约数",
        labels={"algorithm_id": "算法", "contract_hit_count": "命中合约数", "group": "类别"},
    )
    effectiveness_chart.update_layout(margin={"l": 20, "r": 20, "t": 48, "b": 20}, height=420)

    coverage_chart = px.density_heatmap(
        coverage_df,
        x="metadata_algorithm",
        y="name_algorithm",
        z="coverage_rate",
        text_auto=".0%",
        color_continuous_scale="Blues",
        title="Name 对 Metadata 的覆盖率",
        labels={
            "metadata_algorithm": "Metadata 算法",
            "name_algorithm": "Name 算法",
            "coverage_rate": "覆盖率",
        },
    )
    coverage_chart.update_layout(margin={"l": 20, "r": 20, "t": 48, "b": 20}, height=420)

    gap_chart = px.density_heatmap(
        coverage_df,
        x="metadata_algorithm",
        y="name_algorithm",
        z="metadata_only_contract_count",
        text_auto=True,
        color_continuous_scale="Oranges",
        title="Metadata 独有合约缺口",
        labels={
            "metadata_algorithm": "Metadata 算法",
            "name_algorithm": "Name 算法",
            "metadata_only_contract_count": "未覆盖合约数",
        },
    )
    gap_chart.update_layout(margin={"l": 20, "r": 20, "t": 48, "b": 20}, height=420)

    summary_items = [
        ("输入文件", str(analysis.payload.input_path)),
        ("链", analysis.payload.chain),
        ("样本名称", analysis.payload.sample_name),
        ("样本合约", analysis.payload.sample_contract_address or "-"),
        ("召回候选数", f"{analysis.payload.recall_candidate_count:,}"),
        ("召回耗时", f"{analysis.payload.recall_elapsed_ms:.3f} ms"),
    ]
    summary_html = "".join(
        "<div class='card'>"
        f"<div class='label'>{html.escape(label)}</div>"
        f"<div class='value'>{html.escape(value)}</div>"
        "</div>"
        for label, value in summary_items
    )

    metrics_table = normalized_df.rename(
        columns={
            "algorithm_id": "算法",
            "group": "类别",
            "avg_ms": "平均耗时(ms)",
            "min_ms": "最短耗时(ms)",
            "repeat": "重复次数",
            "contract_hit_count": "命中合约数",
        }
    ).to_html(index=False, classes="metrics-table", border=0)
    coverage_table = coverage_df.fillna("N/A").rename(
        columns={
            "name_algorithm": "Name 算法",
            "metadata_algorithm": "Metadata 算法",
            "coverage_rate": "覆盖率",
            "metadata_only_contract_count": "未覆盖合约数",
        }
    ).to_html(
        index=False, classes="metrics-table", border=0
    )
    recommendation_html = "".join(
        f"<li>{html.escape(line)}</li>" for line in analysis.recommendation_lines
    )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <script type="text/javascript">{PLOTLY_JS}</script>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --card: #ffffff;
      --border: #d7dfeb;
      --text: #162033;
      --muted: #5f6b85;
      --accent: #1d4ed8;
    }}
    body {{
      margin: 0;
      padding: 24px;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background: linear-gradient(180deg, #edf4ff 0%, var(--bg) 220px);
      color: var(--text);
    }}
    main {{
      max-width: 1440px;
      margin: 0 auto;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 34px;
    }}
    h2 {{
      margin: 0 0 12px;
      font-size: 24px;
    }}
    h3 {{
      margin: 0 0 10px;
      font-size: 18px;
    }}
    p.lead {{
      margin: 0 0 24px;
      color: var(--muted);
    }}
    section {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 20px;
      margin-top: 18px;
      box-shadow: 0 10px 30px rgba(31, 51, 91, 0.06);
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    .card {{
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px 16px;
      background: #fbfdff;
    }}
    .label {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .value {{
      margin-top: 8px;
      font-size: 20px;
      font-weight: 700;
      word-break: break-word;
    }}
    .grid-2 {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      gap: 16px;
    }}
    .metrics-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    .metrics-table th,
    .metrics-table td {{
      border-bottom: 1px solid var(--border);
      padding: 10px 8px;
      text-align: left;
    }}
    .metrics-table th {{
      color: var(--muted);
      font-weight: 600;
    }}
    ul {{
      margin: 0;
      padding-left: 20px;
    }}
    li {{
      margin: 8px 0;
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(title)}</h1>
    <p class="lead">按合约级命中统计；只要发现合约涉及重复即算成功查重。不为未来结果结构变化做兼容层。</p>
    <section>
      <h2>摘要</h2>
      <div class="cards">{summary_html}</div>
    </section>
    <section>
      <h2>时间成本</h2>
      <div class="grid-2">
        <div><h3>Name 算法耗时</h3>{_figure_div(name_time_chart)}</div>
        <div><h3>Metadata 算法耗时</h3>{_figure_div(metadata_time_chart)}</div>
      </div>
      {metrics_table}
    </section>
    <section>
      <h2>查重效果</h2>
      {_figure_div(effectiveness_chart)}
    </section>
    <section>
      <h2>Name 与 Metadata 覆盖关系</h2>
      <div class="grid-2">
        <div>{_figure_div(coverage_chart)}</div>
        <div>{_figure_div(gap_chart)}</div>
      </div>
      {coverage_table}
    </section>
    <section>
      <h2>结论建议</h2>
      <ul>{recommendation_html}</ul>
    </section>
  </main>
</body>
</html>"""
