from __future__ import annotations

import argparse
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Optional, Sequence


def _crate_dir() -> Path:
    return Path(__file__).resolve().parent.parent / 'rust_ext' / 'top_contract_analysis_rust'


def _build_wheel(*, interpreter: str) -> Path:
    crate_dir = _crate_dir()
    subprocess.run(
        [
            interpreter,
            '-m',
            'maturin',
            'build',
            '--release',
            '--interpreter',
            interpreter,
            '--manifest-path',
            str(crate_dir / 'Cargo.toml'),
        ],
        check=True,
        cwd=crate_dir.parent.parent,
    )
    wheels = sorted((crate_dir / 'target' / 'wheels').glob('top_contract_analysis_rust-*.whl'))
    if not wheels:
        raise FileNotFoundError('no wheel produced by maturin build')
    return wheels[-1]


def _install_wheel_to_runtime_dir(wheel_path: Path, runtime_dir: Path) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    package_dir = runtime_dir / 'top_contract_analysis_rust'
    for path in runtime_dir.glob('top_contract_analysis_rust-*.dist-info'):
        shutil.rmtree(path, ignore_errors=True)
    if package_dir.exists():
        shutil.rmtree(package_dir, ignore_errors=True)
    with zipfile.ZipFile(wheel_path) as archive:
        archive.extractall(runtime_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Build and install the Rust matcher extension into the local runtime dir.')
    parser.add_argument('--interpreter', required=True)
    parser.add_argument('--runtime-dir', default=str(Path(__file__).resolve().parent.parent / '.runtime' / 'pydeps'))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    wheel_path = _build_wheel(interpreter=args.interpreter)
    _install_wheel_to_runtime_dir(wheel_path, Path(args.runtime_dir))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
