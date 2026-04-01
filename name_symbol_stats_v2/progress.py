from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass
from typing import TextIO


CLEAR_LINE = '\x1b[2K'
MOVE_UP_ONE = '\x1b[1A'


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f'{hours:02d}:{minutes:02d}:{secs:02d}'
    return f'{minutes:02d}:{secs:02d}'


def _render_progress_lines(
    *,
    label: str,
    completed: int,
    total: int,
    elapsed_seconds: float,
    unit: str,
    extra: str = '',
    width: int = 24,
) -> tuple[str, str]:
    safe_total = max(total, 0)
    safe_completed = min(max(completed, 0), safe_total) if safe_total else max(completed, 0)
    ratio = (safe_completed / safe_total) if safe_total else 1.0
    ratio = max(0.0, min(ratio, 1.0))
    filled = min(width, int(ratio * width))
    bar = '#' * filled + '-' * (width - filled)
    rate = safe_completed / elapsed_seconds if elapsed_seconds > 0 else 0.0
    eta_seconds = ((safe_total - safe_completed) / rate) if rate > 0 and safe_total > safe_completed else 0.0
    first_line = f'[{label}] [{bar}] {ratio * 100:5.1f}%'
    pieces = [
        f'{safe_completed}/{safe_total} {unit}',
        f'{rate:.1f}/s',
        f'ETA {format_duration(eta_seconds)}',
        f'elapsed {format_duration(elapsed_seconds)}',
    ]
    if extra:
        pieces.append(extra)
    second_line = ' | '.join(pieces)
    return first_line, second_line


def render_progress_line(
    *,
    label: str,
    completed: int,
    total: int,
    elapsed_seconds: float,
    unit: str,
    extra: str = '',
    width: int = 24,
) -> str:
    return '\n'.join(
        _render_progress_lines(
            label=label,
            completed=completed,
            total=total,
            elapsed_seconds=elapsed_seconds,
            unit=unit,
            extra=extra,
            width=width,
        )
    )


@dataclass
class ProgressPrinter:
    label: str
    total: int
    unit: str
    logger: logging.Logger
    stream: TextIO = sys.stderr
    log_interval_seconds: float = 10.0
    width: int = 24
    enabled: bool = True

    def __post_init__(self) -> None:
        self._start = time.monotonic()
        self._last_log = self._start
        self._completed = 0
        self._tty = bool(getattr(self.stream, 'isatty', lambda: False)()) and self.enabled
        self._rendered_once = False

    def update(self, completed: int, *, extra: str = '') -> None:
        self._completed = max(0, completed)
        self._emit(extra=extra)

    def advance(self, amount: int = 1, *, extra: str = '') -> None:
        self.update(self._completed + amount, extra=extra)

    def close(self, *, extra: str = '') -> None:
        self._completed = max(self._completed, self.total)
        self._emit(extra=extra, force_log=True, final=True)

    def _emit(self, *, extra: str, force_log: bool = False, final: bool = False) -> None:
        elapsed = max(0.0001, time.monotonic() - self._start)
        first_line, second_line = _render_progress_lines(
            label=self.label,
            completed=self._completed,
            total=self.total,
            elapsed_seconds=elapsed,
            unit=self.unit,
            extra=extra,
            width=self.width,
        )
        line = f'{first_line}\n{second_line}'
        if self._tty:
            if self._rendered_once:
                self.stream.write(f'\r{CLEAR_LINE}{MOVE_UP_ONE}\r{CLEAR_LINE}')
            self.stream.write(f'{first_line}\n{second_line}')
            if final:
                self.stream.write('\n')
            self.stream.flush()
            self._rendered_once = True
        now = time.monotonic()
        should_log = force_log or (not self._tty and (now - self._last_log >= self.log_interval_seconds or self._completed >= self.total))
        if should_log:
            self.logger.info('%s', line)
            self._last_log = now
