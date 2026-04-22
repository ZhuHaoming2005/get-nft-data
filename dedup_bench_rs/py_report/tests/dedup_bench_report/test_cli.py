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
    assert "Dedup Benchmark 可视化分析报告" in html
