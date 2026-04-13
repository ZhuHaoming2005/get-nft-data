from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Sequence

from .analysis import analyze_seed_contract
from .constants import DEFAULT_MAX_RECALL_ROWS
from .reporting import write_batch_summary_outputs, write_outputs_to_directory
from .duckdb_store import DuckDBFeatureStore
from .progress import create_batch_progress_reporter
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


def _load_cached_seed_entries(output_dir: str | Path, *, chain: str) -> dict[str, dict]:
    target_dir = Path(output_dir)
    if not target_dir.exists():
        return {}

    cached: dict[str, dict] = {}
    for path in sorted(target_dir.glob('top_contract_analysis__*.json')):
        if path.name == 'top_contract_analysis__summary.json':
            continue
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            continue
        seed = payload.get('seed_contract') or {}
        contract_address = str(seed.get('contract_address') or '').strip().lower()
        payload_chain = str(seed.get('chain') or '').strip().lower()
        if not contract_address or payload_chain != chain.lower():
            continue
        cached[contract_address] = {
            'seed_contract': seed,
            'report_summary': payload.get('report_summary') or {},
            'output_files': {'json': path.name, 'markdown': path.with_suffix('.md').name},
        }
    return cached


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Batch analyze duplicate NFT samples for many seed contracts.')
    parser.add_argument('--chain', default='ethereum')
    parser.add_argument('--seed-file', required=True, help='text file with one seed contract address per line')
    parser.add_argument('--alchemy-api-key', default='')
    parser.add_argument('--alchemy-network', default='')
    parser.add_argument('--etherscan-api-key', default='')
    parser.add_argument('--opensea-api-key', default='')
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
    parser.add_argument(
        '--max-tokens-per-contract',
        type=int,
        default=500,
        help='per-contract token cap in DuckDB recall query (default: %(default)s)',
    )
    return parser


def _analyze_one_seed(
    seed_address: str,
    args: argparse.Namespace,
    feature_store: DuckDBFeatureStore | None = None,
    signal_cache: ContractSignalCache | None = None,
    progress_reporter=None,
) -> dict:
    return analyze_seed_contract(
        chain=args.chain,
        seed_contract_address=seed_address,
        alchemy_api_key=args.alchemy_api_key,
        alchemy_network=args.alchemy_network or None,
        etherscan_api_key=args.etherscan_api_key,
        opensea_api_key=args.opensea_api_key,
        feature_store=feature_store,
        signal_cache=signal_cache,
        name_threshold=args.name_threshold,
        metadata_threshold=args.metadata_threshold,
        timeout=args.timeout,
        max_recall_rows=args.max_recall_rows,
        max_tokens_per_contract=args.max_tokens_per_contract,
        progress_reporter=progress_reporter,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    seed_addresses = _read_seed_addresses(args.seed_file)
    feature_store: DuckDBFeatureStore | None = None
    signal_cache = ContractSignalCache(database_path=args.signal_cache_db)
    cached_entries = _load_cached_seed_entries(args.output_dir, chain=args.chain)
    if args.feature_parquet:
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
        feature_store.load_parquet_dataset(args.chain, args.feature_parquet, strict=args.strict_parquet)
    elif args.feature_db != ':memory:' and Path(args.feature_db).exists():
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
    try:
        pending_seeds = [seed for seed in seed_addresses if seed not in cached_entries]
        fresh_entries: dict[str, dict] = {}
        with create_batch_progress_reporter(
            seed_addresses=seed_addresses,
            workers=args.workers,
            initial_completed=len(cached_entries),
        ) as progress_reporter:
            if args.workers <= 1:
                for seed_address in pending_seeds:
                    progress_reporter.on_seed_started(seed_address)
                    try:
                        payload = _analyze_one_seed(
                            seed_address,
                            args,
                            feature_store=feature_store,
                            signal_cache=signal_cache,
                            progress_reporter=progress_reporter.create_seed_reporter(seed_address),
                        )
                    except Exception as exc:
                        progress_reporter.on_seed_failed(seed_address, exc)
                        raise
                    progress_reporter.on_seed_finished(seed_address)
                    json_path, md_path = write_outputs_to_directory(payload, args.output_dir)
                    fresh_entries[seed_address] = {
                        'seed_contract': payload.get('seed_contract') or {},
                        'report_summary': payload.get('report_summary') or {},
                        'output_files': {'json': json_path.name, 'markdown': md_path.name},
                    }
            else:
                def _run(seed: str):
                    progress_reporter.on_seed_started(seed)
                    try:
                        payload = _analyze_one_seed(
                            seed,
                            args,
                            feature_store=feature_store,
                            signal_cache=signal_cache,
                            progress_reporter=progress_reporter.create_seed_reporter(seed),
                        )
                    except Exception as exc:
                        progress_reporter.on_seed_failed(seed, exc)
                        raise
                    progress_reporter.on_seed_finished(seed)
                    return payload

                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    for seed_address, payload in zip(
                        pending_seeds,
                        executor.map(_run, pending_seeds),
                    ):
                        json_path, md_path = write_outputs_to_directory(payload, args.output_dir)
                        fresh_entries[seed_address] = {
                            'seed_contract': payload.get('seed_contract') or {},
                            'report_summary': payload.get('report_summary') or {},
                            'output_files': {'json': json_path.name, 'markdown': md_path.name},
                        }

        summary_entries = [
            cached_entries.get(seed_address) or fresh_entries[seed_address]
            for seed_address in seed_addresses
        ]
        write_batch_summary_outputs(summary_entries, args.output_dir)
    finally:
        if feature_store is not None:
            feature_store.close()
        signal_cache.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
