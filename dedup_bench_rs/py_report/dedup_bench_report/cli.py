from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

from .analyzer import build_analysis, load_benchmark_result
from .render_html import render_html_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render the current dedup_bench_rs result.json into a single-file HTML analysis report."
    )
    parser.add_argument("--input", required=True, help="path to benchmark result JSON")
    parser.add_argument("--output", required=True, help="path to output HTML report")
    parser.add_argument(
        "--title",
        default="Dedup Benchmark 可视化分析报告",
        help="report title",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input)
    output_path = Path(args.output)
    payload = load_benchmark_result(input_path)
    analysis = build_analysis(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html_report(analysis, args.title), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
