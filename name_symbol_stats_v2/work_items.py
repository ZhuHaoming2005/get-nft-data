from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence

from psycopg2.extras import execute_values

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


def build_work_items(conn, run_label: str, chains: Sequence[str], *, max_atoms_per_task: int = 50000, max_prefix_len: int = 10) -> list[WorkItem]:
    _delete_outputs(conn, run_label, chains)
    chains_csv = ','.join(chains)
    work_items: list[WorkItem] = []
    block_sizes = _fetch_block_sizes(conn, run_label, chains)
    tracker = ProgressPrinter('prepare-name-tasks', len(block_sizes), 'blocks', logger)
    processed_blocks = 0
    processed_atoms = 0

    for block_key, atom_count in block_sizes:
        processed_atoms += atom_count
        if atom_count <= max_atoms_per_task:
            work_items.append(WorkItem(run_label, chains_csv, block_key, '', atom_count))
            processed_blocks += 1
            tracker.update(
                processed_blocks,
                extra=f'work_items={len(work_items)} atoms={processed_atoms}',
            )
            continue

        prefix_len = 4
        splits = _fetch_signature_splits(conn, run_label, chains, block_key, prefix_len)
        while any(count > max_atoms_per_task for _, count in splits) and prefix_len < max_prefix_len:
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
