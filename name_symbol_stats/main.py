from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dedup_stats import get_conn

from .config import StatsConfig
from .name_stats import run_name_stats
from .report import SummaryRow, render_text_summary, summary_rows_to_dicts, write_csv, write_parquet, write_text
from .symbol_stats import build_contract_identity, run_symbol_stats

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def _fetch_summary_rows(conn, *, chains: tuple[str, ...], thresholds: tuple[float, ...]) -> list[SummaryRow]:
    threshold_values = [-1.0, *thresholds]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT field_name, scope, primary_chain, secondary_chain, threshold, total_contracts, total_nfts,
                   group_count, duplicate_contract_count, duplicate_nft_count, duplicate_contract_ratio,
                   duplicate_nft_ratio, group_size_ge_2_count, group_size_gt_2_count
            FROM analysis_duplicate_summary
            WHERE primary_chain = ANY(%s)
              AND threshold = ANY(%s)
            ORDER BY field_name, scope, primary_chain, secondary_chain, threshold
            """,
            (list(chains), threshold_values),
        )
        rows = cur.fetchall()
    return [
        SummaryRow(
            field_name=row[0],
            scope=row[1],
            primary_chain=row[2],
            secondary_chain=row[3],
            threshold=None if float(row[4]) < 0 else float(row[4]),
            total_contracts=int(row[5]),
            total_nfts=int(row[6]),
            group_count=int(row[7]),
            duplicate_contract_count=int(row[8]),
            duplicate_nft_count=int(row[9]),
            duplicate_contract_ratio=float(row[10]),
            duplicate_nft_ratio=float(row[11]),
            group_size_ge_2_count=int(row[12]),
            group_size_gt_2_count=int(row[13]),
        )
        for row in rows
    ]


def _fetch_top_groups(conn, *, table_name: str, limit: int) -> list[dict[str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT scope, primary_chain, secondary_chain, sample_value, primary_contract_count, primary_nft_count
            FROM {table_name}
            ORDER BY primary_nft_count DESC, primary_contract_count DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [
            {
                'scope': scope,
                'primary_chain': primary_chain,
                'secondary_chain': secondary_chain,
                'sample_value': sample_value,
                'primary_contract_count': int(primary_contract_count),
                'primary_nft_count': int(primary_nft_count),
            }
            for scope, primary_chain, secondary_chain, sample_value, primary_contract_count, primary_nft_count in cur.fetchall()
        ]


def build_contract_identity_command(args: argparse.Namespace) -> None:
    config = StatsConfig.from_args(chains=args.chains)
    conn = get_conn()
    try:
        counts = build_contract_identity(conn, config.chains, batch_size=args.batch_size)
    finally:
        conn.close()
    for chain, count in counts.items():
        logger.info('%s contract identities built: %s', chain, count)


def symbol_stats_command(args: argparse.Namespace) -> None:
    config = StatsConfig.from_args(chains=args.chains)
    conn = get_conn()
    try:
        rows = run_symbol_stats(conn, config.chains)
    finally:
        conn.close()
    logger.info('symbol summary rows written: %d', len(rows))


def name_stats_command(args: argparse.Namespace) -> None:
    config = StatsConfig.from_args(
        chains=args.chains,
        thresholds=args.thresholds,
        workers=args.workers,
        max_block_size=args.max_block_size,
        trigram_cutoff=args.trigram_cutoff,
        max_len_delta=args.max_len_delta,
    )
    conn = get_conn()
    try:
        rows = run_name_stats(
            conn,
            chains=config.chains,
            thresholds=config.thresholds,
            max_block_size=config.max_block_size,
            trigram_cutoff=config.trigram_cutoff,
            max_len_delta=config.max_len_delta,
            workers=config.workers,
        )
    finally:
        conn.close()
    logger.info('name summary rows written: %d', len(rows))


def export_report_command(args: argparse.Namespace) -> None:
    config = StatsConfig.from_args(chains=args.chains, thresholds=args.thresholds, export_dir=args.output_dir)
    conn = get_conn()
    try:
        summary_rows = _fetch_summary_rows(conn, chains=config.chains, thresholds=config.thresholds)
        symbol_samples = _fetch_top_groups(conn, table_name='analysis_symbol_duplicate_groups', limit=args.top_n)
        name_samples = _fetch_top_groups(conn, table_name='analysis_name_duplicate_groups', limit=args.top_n)
    finally:
        conn.close()

    output_dir = Path(config.export_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_text(render_text_summary(summary_rows), output_dir / 'summary.txt')
    row_dicts = summary_rows_to_dicts(summary_rows)
    write_csv(row_dicts, output_dir / 'summary.csv')
    if not write_parquet(row_dicts, output_dir / 'summary.parquet'):
        logger.warning('polars is not installed, skipped summary.parquet')
    (output_dir / 'symbol_groups.json').write_text(json.dumps(symbol_samples, ensure_ascii=False, indent=2), encoding='utf-8')
    (output_dir / 'name_groups.json').write_text(json.dumps(name_samples, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info('reports exported to %s', output_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Name/symbol duplicate statistics')
    subparsers = parser.add_subparsers(dest='command', required=True)

    build_identity = subparsers.add_parser('build-contract-identity')
    build_identity.add_argument('--chains', nargs='+', default=None)
    build_identity.add_argument('--batch-size', type=int, default=5000)
    build_identity.set_defaults(func=build_contract_identity_command)

    symbol_stats = subparsers.add_parser('symbol-stats')
    symbol_stats.add_argument('--chains', nargs='+', default=None)
    symbol_stats.set_defaults(func=symbol_stats_command)

    name_stats = subparsers.add_parser('name-stats')
    name_stats.add_argument('--chains', nargs='+', default=None)
    name_stats.add_argument('--thresholds', nargs='+', type=float, default=None)
    name_stats.add_argument('--workers', type=int, default=4)
    name_stats.add_argument('--max-block-size', type=int, default=2000)
    name_stats.add_argument('--trigram-cutoff', type=float, default=0.35)
    name_stats.add_argument('--max-len-delta', type=int, default=12)
    name_stats.set_defaults(func=name_stats_command)

    export_report = subparsers.add_parser('export-report')
    export_report.add_argument('--chains', nargs='+', default=None)
    export_report.add_argument('--thresholds', nargs='+', type=float, default=None)
    export_report.add_argument('--output-dir', default='name_symbol_stats_output')
    export_report.add_argument('--top-n', type=int, default=20)
    export_report.set_defaults(func=export_report_command)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
