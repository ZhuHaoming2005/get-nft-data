from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Sequence

logger = logging.getLogger(__name__)


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

    failures = []
    for process in processes:
        return_code = process.wait()
        if return_code != 0:
            failures.append(return_code)
    if failures:
        raise RuntimeError(f'name worker failed with exit codes: {failures}')
