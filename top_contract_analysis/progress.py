from __future__ import annotations

import time
from dataclasses import dataclass
from threading import RLock
from typing import Any, Sequence

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
    from rich.table import Table
except Exception:  # pragma: no cover - fallback when rich is unavailable
    Console = None
    Group = None
    Live = None
    Panel = None
    Progress = None
    BarColumn = None
    TaskProgressColumn = None
    TextColumn = None
    TimeElapsedColumn = None
    TimeRemainingColumn = None
    Table = None


def create_single_seed_progress_reporter(*, seed_address: str):
    if Console is None:
        return _NoOpSingleSeedProgressReporter()
    console = Console(stderr=True)
    if not console.is_terminal:
        return _NoOpSingleSeedProgressReporter()
    return _RichSingleSeedProgressReporter(console=console, seed_address=seed_address)


def create_batch_progress_reporter(*, seed_addresses: Sequence[str], workers: int, initial_completed: int = 0):
    if Console is None:
        return _NoOpBatchProgressReporter()
    console = Console(stderr=True)
    if not console.is_terminal:
        return _NoOpBatchProgressReporter()
    return _RichBatchProgressReporter(
        console=console,
        seed_addresses=seed_addresses,
        workers=workers,
        initial_completed=initial_completed,
    )


def _format_seconds(value: float | None) -> str:
    if value is None or value < 0:
        return 'n/a'
    total = int(round(value))
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}h {minutes:02d}m'
    if minutes:
        return f'{minutes}m {seconds:02d}s'
    return f'{seconds}s'


def _estimate_remaining(*, started_at: float, completed: int, total: int) -> float | None:
    if completed <= 0 or total <= 0 or completed > total:
        return None
    elapsed = time.perf_counter() - started_at
    return max(0.0, elapsed * (total - completed) / completed)


class _NoOpSingleSeedProgressReporter:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def on_seed_stage(self, stage: str, **kwargs):
        del stage, kwargs

    def on_high_confidence_contracts_started(self, *, total: int):
        del total

    def on_high_confidence_contract_completed(self, *, contract_address: str, completed: int, total: int):
        del contract_address, completed, total

    def on_seed_completed(self, **kwargs):
        del kwargs


class _NoOpBatchProgressReporter:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def on_seed_started(self, seed_address: str):
        del seed_address

    def on_seed_finished(self, seed_address: str):
        del seed_address

    def on_seed_failed(self, seed_address: str, exc: Exception):
        del seed_address, exc

    def create_seed_reporter(self, seed_address: str):
        del seed_address
        return _NoOpSingleSeedProgressReporter()


class _RichSingleSeedProgressReporter(_NoOpSingleSeedProgressReporter):
    _STAGE_LABELS = {
        'fetch_seed_context': 'Fetching seed metadata',
        'fetch_license_sample': 'Checking license',
        'load_snapshot': 'Loading recall snapshot',
        'find_duplicate_candidates': 'Finding duplicate candidates',
        'postprocess_candidates': 'Post-processing candidates',
        'analyze_high_confidence_contracts': 'Analyzing high-confidence contracts',
        'finalize_report': 'Finalizing report',
    }
    _STAGE_PROGRESS = {
        'fetch_seed_context': 10,
        'fetch_license_sample': 20,
        'load_snapshot': 45,
        'find_duplicate_candidates': 60,
        'postprocess_candidates': 70,
        'analyze_high_confidence_contracts': 70,
        'finalize_report': 95,
    }
    _ANALYZE_START = 70
    _ANALYZE_END = 95

    def __init__(self, *, console: Console, seed_address: str) -> None:
        self._seed_address = seed_address
        self._progress = Progress(
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=True,
        )
        self._task_id = self._progress.add_task(self._build_description('Starting'), total=100, completed=0)
        self._contracts_total = 0

    def __enter__(self):
        self._progress.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._progress.stop()
        return False

    def _build_description(self, label: str) -> str:
        short_seed = f'{self._seed_address[:10]}...' if len(self._seed_address) > 10 else self._seed_address
        return f'{short_seed} | {label}'

    def on_seed_stage(self, stage: str, **kwargs):
        label = self._STAGE_LABELS.get(stage, stage.replace('_', ' '))
        completed = self._STAGE_PROGRESS.get(stage)
        if completed is not None:
            self._progress.update(self._task_id, completed=completed, description=self._build_description(label))
        else:
            self._progress.update(self._task_id, description=self._build_description(label))
        del kwargs

    def on_high_confidence_contracts_started(self, *, total: int):
        self._contracts_total = max(0, total)
        if total == 0:
            self._progress.update(
                self._task_id,
                completed=self._ANALYZE_END,
                description=self._build_description('No high-confidence contracts'),
            )
            return
        self._progress.update(
            self._task_id,
            completed=self._ANALYZE_START,
            description=self._build_description(f'Analyzing contracts 0/{total}'),
        )

    def on_high_confidence_contract_completed(self, *, contract_address: str, completed: int, total: int):
        total = max(total, self._contracts_total, 1)
        ratio = min(max(completed / total, 0.0), 1.0)
        progress = self._ANALYZE_START + (self._ANALYZE_END - self._ANALYZE_START) * ratio
        short_contract = f'{contract_address[:10]}...' if len(contract_address) > 10 else contract_address
        self._progress.update(
            self._task_id,
            completed=progress,
            description=self._build_description(f'Analyzing contracts {completed}/{total} ({short_contract})'),
        )

    def on_seed_completed(self, **kwargs):
        self._progress.update(self._task_id, completed=100, description=self._build_description('Completed'))
        del kwargs


@dataclass
class _SeedProgressState:
    seed_address: str
    started_at: float
    stage_label: str = 'Queued'
    contract_completed: int = 0
    contract_total: int = 0
    status: str = 'queued'


class _BatchSeedProgressReporter(_NoOpSingleSeedProgressReporter):
    _LABELS = _RichSingleSeedProgressReporter._STAGE_LABELS

    def __init__(self, owner: '_RichBatchProgressReporter', seed_address: str) -> None:
        self._owner = owner
        self._seed_address = seed_address

    def on_seed_stage(self, stage: str, **kwargs):
        label = self._LABELS.get(stage, stage.replace('_', ' '))
        self._owner._update_seed_state(
            self._seed_address,
            stage_label=label,
            status='running',
        )
        del kwargs

    def on_high_confidence_contracts_started(self, *, total: int):
        label = 'Analyzing high-confidence contracts'
        if total == 0:
            label = 'No high-confidence contracts'
        self._owner._update_seed_state(
            self._seed_address,
            stage_label=label,
            contract_completed=0,
            contract_total=total,
            status='running',
        )

    def on_high_confidence_contract_completed(self, *, contract_address: str, completed: int, total: int):
        short_contract = f'{contract_address[:8]}...' if len(contract_address) > 8 else contract_address
        self._owner._update_seed_state(
            self._seed_address,
            stage_label=f'Analyzing contracts {completed}/{total} ({short_contract})',
            contract_completed=completed,
            contract_total=total,
            status='running',
        )

    def on_seed_completed(self, **kwargs):
        self._owner._update_seed_state(
            self._seed_address,
            stage_label='Completed',
            status='completed',
        )
        del kwargs


class _RichBatchProgressReporter(_NoOpBatchProgressReporter):
    def __init__(self, *, console: Console, seed_addresses: Sequence[str], workers: int, initial_completed: int = 0) -> None:
        self._console = console
        self._workers = max(1, workers)
        self._lock = RLock()
        self._states: dict[str, _SeedProgressState] = {
            seed: _SeedProgressState(seed_address=seed, started_at=time.perf_counter())
            for seed in seed_addresses
        }
        self._progress = Progress(
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=True,
        )
        self._task_id = self._progress.add_task(
            'Batch progress',
            total=max(len(seed_addresses), 1),
            completed=min(max(initial_completed, 0), max(len(seed_addresses), 1)),
        )
        self._live = Live(self._render(), console=console, refresh_per_second=5, transient=True)

    def __enter__(self):
        self._progress.start()
        self._live.start()
        self._refresh()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._live.stop()
        self._progress.stop()
        return False

    def _render(self):
        return Group(self._progress, self._render_seed_states())

    def _render_seed_states(self):
        table = Table(show_header=True, header_style='bold')
        table.add_column('Seed')
        table.add_column('Status')
        table.add_column('Stage')
        table.add_column('ETA', justify='right')
        active_states = [
            state
            for state in self._states.values()
            if state.status in {'running', 'failed'}
        ][:self._workers]
        if not active_states:
            table.add_row('-', 'idle', 'Waiting for work', 'n/a')
        else:
            for state in active_states:
                eta = _estimate_remaining(
                    started_at=state.started_at,
                    completed=state.contract_completed,
                    total=state.contract_total,
                ) if state.contract_total > 0 else None
                table.add_row(
                    state.seed_address,
                    state.status,
                    state.stage_label,
                    _format_seconds(eta),
                )
        return Panel(table, title='Active seeds')

    def _refresh(self):
        with self._lock:
            self._live.update(self._render(), refresh=True)

    def _update_seed_state(
        self,
        seed_address: str,
        *,
        stage_label: str | None = None,
        contract_completed: int | None = None,
        contract_total: int | None = None,
        status: str | None = None,
    ) -> None:
        with self._lock:
            state = self._states.setdefault(
                seed_address,
                _SeedProgressState(seed_address=seed_address, started_at=time.perf_counter()),
            )
            if stage_label is not None:
                state.stage_label = stage_label
            if contract_completed is not None:
                state.contract_completed = contract_completed
            if contract_total is not None:
                state.contract_total = contract_total
            if status is not None:
                state.status = status
        self._refresh()

    def on_seed_started(self, seed_address: str):
        self._update_seed_state(seed_address, stage_label='Starting', status='running')

    def on_seed_finished(self, seed_address: str):
        self._update_seed_state(seed_address, stage_label='Completed', status='completed')
        with self._lock:
            self._progress.advance(self._task_id, 1)
        self._refresh()

    def on_seed_failed(self, seed_address: str, exc: Exception):
        self._update_seed_state(seed_address, stage_label=f'Failed: {exc}', status='failed')

    def create_seed_reporter(self, seed_address: str):
        self._states.setdefault(seed_address, _SeedProgressState(seed_address=seed_address, started_at=time.perf_counter()))
        return _BatchSeedProgressReporter(self, seed_address)
