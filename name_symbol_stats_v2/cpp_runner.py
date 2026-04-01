from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .db import connect
from .progress import ProgressPrinter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkItemProgress:
    total: int
    pending: int
    running: int
    done: int
    failed: int
    edge_count: int


def _work_item_progress(conn, run_label: str) -> WorkItemProgress:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, count(*)::int, coalesce(sum(edge_count), 0)::bigint
            FROM nsv2_name_work_items
            WHERE run_label = %s
            GROUP BY status
            """,
            (run_label,),
        )
        rows = list(cur.fetchall())
    counts = {status: int(count) for status, count, _ in rows}
    return WorkItemProgress(
        total=sum(counts.values()),
        pending=counts.get('pending', 0),
        running=counts.get('running', 0),
        done=counts.get('done', 0),
        failed=counts.get('failed', 0),
        edge_count=sum(int(edge_count) for _, _, edge_count in rows),
    )


def run_worker_processes(worker_exe: Path, *, run_label: str, thresholds: Sequence[float], parallel_workers: int, trigram_cutoff: float, max_len_delta: int) -> None:
    threshold_arg = ','.join(f'{value:.1f}' for value in thresholds)
    processes: list[subprocess.Popen] = []
    for worker_index in range(parallel_workers):
        command = [
            str(worker_exe),
            '--run-label',
            run_label,
            '--thresholds',
            threshold_arg,
            '--trigram-cutoff',
            f'{trigram_cutoff:.4f}',
            '--max-len-delta',
            str(max_len_delta),
            '--worker-id',
            f'worker-{worker_index + 1}',
        ]
        logger.info('starting worker %s', ' '.join(command))
        processes.append(subprocess.Popen(command))

    read_conn = connect()
    read_conn.autocommit = True
    try:
        initial = _work_item_progress(read_conn, run_label)
        tracker = ProgressPrinter('run-name-worker', initial.total, 'tasks', logger)
        last_progress = initial

        while True:
            progress = _work_item_progress(read_conn, run_label)
            last_progress = progress
            tracker.update(
                progress.done + progress.failed,
                extra=(
                    f'running={progress.running} pending={progress.pending} '
                    f'failed={progress.failed} edges={progress.edge_count}'
                ),
            )
            if all(process.poll() is not None for process in processes):
                break
            time.sleep(2.0)

        tracker.close(
            extra=(
                f'done={last_progress.done} failed={last_progress.failed} '
                f'edges={last_progress.edge_count}'
            )
        )
    finally:
        read_conn.close()

    failures = []
    for process in processes:
        return_code = process.wait()
        if return_code != 0:
            failures.append(return_code)
    if failures:
        raise RuntimeError(f'name worker failed with exit codes: {failures}')
