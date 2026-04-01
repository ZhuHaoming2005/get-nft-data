from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import DEFAULT_CHAINS, DEFAULT_THRESHOLDS, load_settings, normalize_run_label
from .cpp_runner import run_worker_processes
from .db import connect, ensure_schema
from .identity import build_contract_identity
from .name_stats import finalize_name_stats
from .report import SummaryRow, render_text_summary, write_csv, write_parquet, write_text
from .symbol_stats import run_symbol_stats
from .work_items import build_work_items

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def _parse_chains(values: list[str] | None) -> list[str]:
    return list(values or DEFAULT_CHAINS)


def build_contract_identity_command(args: argparse.Namespace) -> None:
    run_label = normalize_run_label(args.run_label)
    chains = _parse_chains(args.chains)
    with connect() as conn:
        counts = build_contract_identity(conn, run_label, chains, batch_size=args.batch_size)
    for chain, count in counts.items():
        logger.info('%s: %d contract identity rows', chain, count)


def symbol_stats_command(args: argparse.Namespace) -> None:
    run_label = normalize_run_label(args.run_label)
    chains = _parse_chains(args.chains)
    with connect() as conn:
        ensure_schema(conn)
        rows = run_symbol_stats(conn, run_label, chains)
    logger.info('wrote %d symbol summary rows', len(rows))


def prepare_name_tasks_command(args: argparse.Namespace) -> None:
    run_label = normalize_run_label(args.run_label)
    chains = _parse_chains(args.chains)
    with connect() as conn:
        ensure_schema(conn)
        work_items = build_work_items(conn, run_label, chains, max_atoms_per_task=args.max_atoms_per_task)
    logger.info('prepared %d name work items', len(work_items))


def run_name_worker_command(args: argparse.Namespace) -> None:
    run_label = normalize_run_label(args.run_label)
    worker_exe = Path(args.worker_exe).resolve()
    run_worker_processes(
        worker_exe,
        run_label=run_label,
        thresholds=args.thresholds,
        parallel_workers=args.parallel_workers,
        trigram_cutoff=args.trigram_cutoff,
        max_len_delta=args.max_len_delta,
    )


def finalize_name_stats_command(args: argparse.Namespace) -> None:
    run_label = normalize_run_label(args.run_label)
    chains = _parse_chains(args.chains)
    with connect() as conn:
        rows = finalize_name_stats(conn, run_label, chains, args.thresholds)
    logger.info('wrote %d name summary rows', len(rows))


def export_report_command(args: argparse.Namespace) -> None:
    run_label = normalize_run_label(args.run_label)
    output_dir = Path(args.output_dir).resolve()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_label, field_name, scope, primary_chain, secondary_chain,
                       NULLIF(threshold, -1.0), total_contracts, total_nfts, group_count,
                       duplicate_contract_count, duplicate_nft_count, duplicate_contract_ratio,
                       duplicate_nft_ratio, group_size_ge_2_count, group_size_gt_2_count
                FROM nsv2_duplicate_summary
                WHERE run_label = %s
                ORDER BY field_name, scope, primary_chain, secondary_chain, threshold
                """,
                (run_label,),
            )
            dict_rows = []
            text_rows = []
            for row in cur.fetchall():
                summary = SummaryRow(
                    run_label=row[0],
                    field_name=row[1],
                    scope=row[2],
                    primary_chain=row[3],
                    secondary_chain=row[4],
                    threshold=row[5],
                    total_contracts=row[6],
                    total_nfts=row[7],
                    group_count=row[8],
                    duplicate_contract_count=row[9],
                    duplicate_nft_count=row[10],
                    duplicate_contract_ratio=row[11],
                    duplicate_nft_ratio=row[12],
                    group_size_ge_2_count=row[13],
                    group_size_gt_2_count=row[14],
                )
                text_rows.append(summary)
                dict_rows.append(summary.__dict__)

    write_text(render_text_summary(text_rows), output_dir / 'summary.txt')
    write_csv(dict_rows, output_dir / 'summary.csv')
    write_parquet(dict_rows, output_dir / 'summary.parquet')
    logger.info('exported report to %s', output_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Name/symbol duplicate statistics V2')
    subparsers = parser.add_subparsers(dest='command', required=True)

    build_parser_cmd = subparsers.add_parser('build-contract-identity')
    build_parser_cmd.add_argument('--run-label', default='default')
    build_parser_cmd.add_argument('--chains', nargs='*', default=list(DEFAULT_CHAINS))
    build_parser_cmd.add_argument('--batch-size', type=int, default=5000)
    build_parser_cmd.set_defaults(func=build_contract_identity_command)

    symbol_parser = subparsers.add_parser('symbol-stats')
    symbol_parser.add_argument('--run-label', default='default')
    symbol_parser.add_argument('--chains', nargs='*', default=list(DEFAULT_CHAINS))
    symbol_parser.set_defaults(func=symbol_stats_command)

    work_parser = subparsers.add_parser('prepare-name-tasks')
    work_parser.add_argument('--run-label', default='default')
    work_parser.add_argument('--chains', nargs='*', default=list(DEFAULT_CHAINS))
    work_parser.add_argument('--max-atoms-per-task', type=int, default=50000)
    work_parser.set_defaults(func=prepare_name_tasks_command)

    worker_parser = subparsers.add_parser('run-name-worker')
    worker_parser.add_argument('--run-label', default='default')
    worker_parser.add_argument('--worker-exe', required=True)
    worker_parser.add_argument('--thresholds', nargs='*', type=float, default=list(DEFAULT_THRESHOLDS))
    worker_parser.add_argument('--parallel-workers', type=int, default=4)
    worker_parser.add_argument('--trigram-cutoff', type=float, default=0.35)
    worker_parser.add_argument('--max-len-delta', type=int, default=12)
    worker_parser.set_defaults(func=run_name_worker_command)

    finalize_parser = subparsers.add_parser('finalize-name-stats')
    finalize_parser.add_argument('--run-label', default='default')
    finalize_parser.add_argument('--chains', nargs='*', default=list(DEFAULT_CHAINS))
    finalize_parser.add_argument('--thresholds', nargs='*', type=float, default=list(DEFAULT_THRESHOLDS))
    finalize_parser.set_defaults(func=finalize_name_stats_command)

    export_parser = subparsers.add_parser('export-report')
    export_parser.add_argument('--run-label', default='default')
    export_parser.add_argument('--output-dir', default='name_symbol_stats_v2_output')
    export_parser.set_defaults(func=export_report_command)

    return parser


def main() -> None:
    load_settings()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
