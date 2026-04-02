from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Sequence

from psycopg2.extras import execute_values

from .blocking import AdaptiveBlockingConfig
from .progress import ProgressPrinter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkItem:
    run_label: str
    chains_csv: str
    name_block_key: str
    signature_prefix: str
    atom_count: int

    @property
    def task_key(self) -> str:
        return f'{self.name_block_key}|{self.signature_prefix}' if self.signature_prefix else self.name_block_key


@dataclass(frozen=True)
class PrepareNameTaskOptions:
    blocking_strategy: str = 'legacy'
    adaptive: AdaptiveBlockingConfig = field(default_factory=AdaptiveBlockingConfig)
    max_atoms_per_task: int = 50000
    max_prefix_len: int = 10


def _delete_outputs(conn, run_label: str, chains: Sequence[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM nsv2_name_duplicate_groups
            WHERE run_label = %s
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (run_label, list(chains), list(chains)),
        )
        cur.execute(
            """
            DELETE FROM nsv2_duplicate_summary
            WHERE run_label = %s
              AND field_name = 'name'
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (run_label, list(chains), list(chains)),
        )
        cur.execute("DELETE FROM nsv2_name_match_edges WHERE run_label = %s", (run_label,))
        cur.execute("DELETE FROM nsv2_name_work_items WHERE run_label = %s", (run_label,))
    conn.commit()


def _assign_name_block_keys(conn, run_label: str, chains: Sequence[str], options: PrepareNameTaskOptions) -> None:
    if options.blocking_strategy == 'legacy':
        return
    if options.blocking_strategy != 'adaptive_v1':
        raise ValueError(f'unsupported blocking strategy: {options.blocking_strategy}')

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH base AS (
                SELECT atom_id,
                       name_collapsed,
                       name_collapsed_len,
                       left(name_collapsed, 3) AS p3,
                       left(name_collapsed, 4) AS p4,
                       left(name_collapsed, 5) AS p5,
                       (name_collapsed_len / %s) * %s AS len6
                FROM nsv2_name_atoms
                WHERE run_label = %s
                  AND chain = ANY(%s)
                  AND name_collapsed <> ''
            ),
            p3_counts AS (
                SELECT p3, count(*)::int AS p3_count
                FROM base
                GROUP BY p3
            ),
            p4_len_counts AS (
                SELECT p4, len6, count(*)::int AS p4_len_count
                FROM base
                GROUP BY p4, len6
            ),
            assigned AS (
                SELECT base.atom_id,
                       CASE
                           WHEN p3_counts.p3_count < %s THEN base.p3
                           WHEN p3_counts.p3_count <= %s THEN base.p3 || '|' || base.len6::text
                           WHEN p4_len_counts.p4_len_count > %s THEN base.p5 || '|' || base.len6::text
                           ELSE base.p4 || '|' || base.len6::text
                       END AS final_block_key
                FROM base
                JOIN p3_counts ON p3_counts.p3 = base.p3
                JOIN p4_len_counts ON p4_len_counts.p4 = base.p4 AND p4_len_counts.len6 = base.len6
            )
            UPDATE nsv2_name_atoms atoms
            SET name_block_key = assigned.final_block_key
            FROM assigned
            WHERE atoms.atom_id = assigned.atom_id
            """,
            (
                options.adaptive.length_bucket_size,
                options.adaptive.length_bucket_size,
                run_label,
                list(chains),
                options.adaptive.small_canopy_max,
                options.adaptive.medium_canopy_max,
                options.adaptive.large_canopy_max,
            ),
        )
    conn.commit()


def _fetch_block_sizes(conn, run_label: str, chains: Sequence[str]) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name_block_key, count(*)::int AS atom_count
            FROM nsv2_name_atoms
            WHERE run_label = %s
              AND chain = ANY(%s)
              AND name_block_key <> ''
            GROUP BY name_block_key
            ORDER BY atom_count DESC, name_block_key
            """,
            (run_label, list(chains)),
        )
        return list(cur.fetchall())


def _fetch_signature_splits(conn, run_label: str, chains: Sequence[str], block_key: str, prefix_len: int) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT left(name_signature_hash, %s) AS signature_prefix, count(*)::int AS atom_count
            FROM nsv2_name_atoms
            WHERE run_label = %s
              AND chain = ANY(%s)
              AND name_block_key = %s
            GROUP BY left(name_signature_hash, %s)
            ORDER BY atom_count DESC, signature_prefix
            """,
            (prefix_len, run_label, list(chains), block_key, prefix_len),
        )
        return [(prefix or '', atom_count) for prefix, atom_count in cur.fetchall()]


def build_work_items(
    conn,
    run_label: str,
    chains: Sequence[str],
    *,
    blocking_strategy: str = 'legacy',
    max_atoms_per_task: int = 50000,
    max_prefix_len: int = 10,
) -> list[WorkItem]:
    options = PrepareNameTaskOptions(
        blocking_strategy=blocking_strategy,
        max_atoms_per_task=max_atoms_per_task,
        max_prefix_len=max_prefix_len,
    )
    _delete_outputs(conn, run_label, chains)
    _assign_name_block_keys(conn, run_label, chains, options)
    chains_csv = ','.join(chains)
    work_items: list[WorkItem] = []
    block_sizes = _fetch_block_sizes(conn, run_label, chains)
    tracker = ProgressPrinter('prepare-name-tasks', len(block_sizes), 'blocks', logger)
    processed_blocks = 0
    processed_atoms = 0

    for block_key, atom_count in block_sizes:
        processed_atoms += atom_count
        if atom_count <= options.max_atoms_per_task:
            work_items.append(WorkItem(run_label, chains_csv, block_key, '', atom_count))
            processed_blocks += 1
            tracker.update(
                processed_blocks,
                extra=f'work_items={len(work_items)} atoms={processed_atoms}',
            )
            continue

        prefix_len = 4
        splits = _fetch_signature_splits(conn, run_label, chains, block_key, prefix_len)
        while any(count > options.max_atoms_per_task for _, count in splits) and prefix_len < options.max_prefix_len:
            prefix_len += 2
            splits = _fetch_signature_splits(conn, run_label, chains, block_key, prefix_len)

        for signature_prefix, split_count in splits:
            work_items.append(WorkItem(run_label, chains_csv, block_key, signature_prefix, split_count))
        processed_blocks += 1
        tracker.update(
            processed_blocks,
            extra=f'work_items={len(work_items)} atoms={processed_atoms}',
        )

    if work_items:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO nsv2_name_work_items (
                    run_label, task_key, chains_csv, name_block_key, signature_prefix, atom_count, status
                ) VALUES %s
                """,
                [
                    (
                        item.run_label,
                        item.task_key,
                        item.chains_csv,
                        item.name_block_key,
                        item.signature_prefix,
                        item.atom_count,
                        'pending',
                    )
                    for item in work_items
                ],
                page_size=1000,
            )
        conn.commit()

    tracker.close(extra=f'work_items={len(work_items)} atoms={processed_atoms}')
    logger.info('created %d name work items for run %s', len(work_items), run_label)
    return work_items
