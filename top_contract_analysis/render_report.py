from __future__ import annotations

# Example:
#   C:\Users\z1766\.conda\envs\codex\python.exe -m top_contract_analysis.render_report ^
#     --input top_contract_analysis.json ^
#     --output top_contract_analysis.md

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from .reporting import render_human_readable_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Render top_contract_analysis JSON into a Markdown report.')
    parser.add_argument('--input', required=True, help='path to top_contract_analysis JSON output')
    parser.add_argument('--output', default='', help='path to output Markdown file (default: <input>.md)')
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_suffix('.md')
    payload = json.loads(input_path.read_text(encoding='utf-8'))
    output_path.write_text(render_human_readable_report(payload), encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
