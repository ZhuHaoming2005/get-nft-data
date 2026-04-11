from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Sequence

from . import analyze_seed_contract, write_batch_summary_outputs, write_outputs_to_directory, DEFAULT_MAX_RECALL_ROWS
from .duckdb_store import DuckDBFeatureStore
from .signal_cache import ContractSignalCache


def _read_seed_addresses(seed_file: str) -> list[str]:
    lines = Path(seed_file).read_text(encoding='utf-8').splitlines()
    seeds: list[str] = []
    for line in lines:
        value = line.strip()
        if not value or value.startswith('#'):
            continue
        seeds.append(value.lower())
    return seeds


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Batch analyze duplicate NFT samples for many seed contracts.')
    parser.add_argument('--chain', default='ethereum')
    parser.add_argument('--seed-file', required=True, help='text file with one seed contract address per line')
    parser.add_argument('--alchemy-api-key', default='')
    parser.add_argument('--alchemy-network', default='')
    parser.add_argument('--etherscan-api-key', default='')
    parser.add_argument('--name-threshold', type=float, default=95.0)
    parser.add_argument('--metadata-threshold', type=float, default=0.55)
    parser.add_argument('--timeout', type=int, default=30)
    parser.add_argument('--output-dir', default='result')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--feature-parquet', default='', help='optional parquet snapshot path to preload into DuckDB')
    parser.add_argument('--feature-db', default=':memory:', help='duckdb database path for the feature store')
    parser.add_argument('--signal-cache-db', default=':memory:', help='duckdb database path for cached chain signals')
    parser.add_argument(
        '--strict-parquet',
        action='store_true',
        help='fail fast if the parquet snapshot is missing pre-computed feature columns instead of falling back to slow Python UDFs',
    )
    parser.add_argument(
        '--max-recall-rows',
        type=int,
        default=DEFAULT_MAX_RECALL_ROWS,
        help='safety cap on token-level recall per seed; set 0 to disable (default: %(default)s)',
    )
    return parser


def _analyze_one_seed(
    seed_address: str,
    args: argparse.Namespace,
    feature_store: DuckDBFeatureStore | None = None,
    signal_cache: ContractSignalCache | None = None,
) -> dict:
    return analyze_seed_contract(
        chain=args.chain,
        seed_contract_address=seed_address,
        alchemy_api_key=args.alchemy_api_key,
        alchemy_network=args.alchemy_network or None,
        etherscan_api_key=args.etherscan_api_key,
        feature_store=feature_store,
        signal_cache=signal_cache,
        name_threshold=args.name_threshold,
        metadata_threshold=args.metadata_threshold,
        timeout=args.timeout,
        max_recall_rows=args.max_recall_rows,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    seed_addresses = _read_seed_addresses(args.seed_file)
    feature_store: DuckDBFeatureStore | None = None
    signal_cache = ContractSignalCache(database_path=args.signal_cache_db)
    summary_entries: list[dict] = []
    if args.feature_parquet:
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
        feature_store.load_parquet_dataset(args.chain, args.feature_parquet, strict=args.strict_parquet)
    elif args.feature_db != ':memory:' and Path(args.feature_db).exists():
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
    try:
        if args.workers <= 1:
            for seed_address in seed_addresses:
                payload = _analyze_one_seed(seed_address, args, feature_store=feature_store, signal_cache=signal_cache)
                json_path, md_path = write_outputs_to_directory(payload, args.output_dir)
                summary_entries.append({
                    'seed_contract': payload.get('seed_contract') or {},
                    'report_summary': payload.get('report_summary') or {},
                    'output_files': {'json': json_path.name, 'markdown': md_path.name},
                })
            write_batch_summary_outputs(summary_entries, args.output_dir)
            return 0

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for payload in executor.map(
                lambda seed: _analyze_one_seed(seed, args, feature_store=feature_store, signal_cache=signal_cache),
                seed_addresses,
            ):
                json_path, md_path = write_outputs_to_directory(payload, args.output_dir)
                summary_entries.append({
                    'seed_contract': payload.get('seed_contract') or {},
                    'report_summary': payload.get('report_summary') or {},
                    'output_files': {'json': json_path.name, 'markdown': md_path.name},
                })
        write_batch_summary_outputs(summary_entries, args.output_dir)
    finally:
        if feature_store is not None:
            feature_store.close()
        signal_cache.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
