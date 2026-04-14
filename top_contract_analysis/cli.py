from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional, Sequence

from .constants import (
    DEFAULT_API_MAX_CONCURRENCY,
    DEFAULT_CONTRACT_MAX_CONCURRENCY,
    DEFAULT_MAX_RECALL_ROWS,
    DEFAULT_NAME_THRESHOLD,
    DEFAULT_SALE_METRIC_MAX_CONCURRENCY,
    DEFAULT_TIMEOUT,
)
from .analysis import analyze_seed_contract
from .progress import create_single_seed_progress_reporter
from .reporting import write_default_outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Analyze duplicate NFT samples for a seed top-NFT contract.')
    parser.add_argument('--chain', default='ethereum')
    parser.add_argument('--seed-contract-address', required=True)
    parser.add_argument('--alchemy-api-key', default=os.getenv('ALCHEMY_API_KEY', ''))
    parser.add_argument('--alchemy-network', default='')
    parser.add_argument('--etherscan-api-key', default=os.getenv('ETHERSCAN_API_KEY', ''))
    parser.add_argument('--opensea-api-key', default=os.getenv('OPENSEA_API_KEY', ''))
    parser.add_argument('--name-threshold', type=float, default=DEFAULT_NAME_THRESHOLD)
    parser.add_argument('--metadata-threshold', type=float, default=0.55)
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument(
        '--max-tokens-per-contract',
        type=int,
        default=0,
        help='per-contract token cap in DuckDB recall query (default: no limit)',
    )
    parser.add_argument(
        '--max-recall-rows',
        type=int,
        default=DEFAULT_MAX_RECALL_ROWS,
        help='safety cap on total recall token rows (0 = unlimited)',
    )
    parser.add_argument('--api-max-concurrency', type=int, default=DEFAULT_API_MAX_CONCURRENCY)
    parser.add_argument('--contract-max-concurrency', type=int, default=DEFAULT_CONTRACT_MAX_CONCURRENCY)
    parser.add_argument('--sale-metric-max-concurrency', type=int, default=DEFAULT_SALE_METRIC_MAX_CONCURRENCY)
    parser.add_argument('--output', default='')
    parser.add_argument('--feature-parquet', default='', help='optional parquet snapshot path to preload into DuckDB')
    parser.add_argument('--feature-db', default=':memory:', help='duckdb database path for the feature store')
    parser.add_argument('--signal-cache-db', default=':memory:', help='duckdb database path for cached chain signals')
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    from .duckdb_store import DuckDBFeatureStore
    from .signal_cache import ContractSignalCache

    args = build_parser().parse_args(argv)
    feature_store = None
    signal_cache = ContractSignalCache(database_path=args.signal_cache_db)
    if args.feature_parquet:
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
        feature_store.load_parquet_dataset(args.chain, args.feature_parquet)
    elif args.feature_db != ':memory:' and Path(args.feature_db).exists():
        feature_store = DuckDBFeatureStore(database_path=args.feature_db)
    try:
        with create_single_seed_progress_reporter(seed_address=args.seed_contract_address.lower()) as progress_reporter:
            payload = analyze_seed_contract(
                chain=args.chain,
                seed_contract_address=args.seed_contract_address.lower(),
                alchemy_api_key=args.alchemy_api_key,
                alchemy_network=args.alchemy_network or None,
                etherscan_api_key=args.etherscan_api_key,
                opensea_api_key=args.opensea_api_key,
                feature_store=feature_store,
                signal_cache=signal_cache,
                name_threshold=args.name_threshold,
                metadata_threshold=args.metadata_threshold,
                timeout=args.timeout,
                max_tokens_per_contract=args.max_tokens_per_contract,
                max_recall_rows=args.max_recall_rows,
                api_max_concurrency=args.api_max_concurrency,
                contract_max_concurrency=args.contract_max_concurrency,
                sale_metric_max_concurrency=args.sale_metric_max_concurrency,
                progress_reporter=progress_reporter,
            )
        write_default_outputs(payload, args.output)
    finally:
        if feature_store is not None:
            feature_store.close()
        signal_cache.close()
    return 0
