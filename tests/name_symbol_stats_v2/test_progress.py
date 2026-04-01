import io
import logging
import unittest

from name_symbol_stats_v2.cpp_runner import WorkItemProgress, _work_item_progress
from name_symbol_stats_v2.progress import ProgressPrinter, render_progress_line


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.statement = None
        self.params = None

    def execute(self, sql, params=None):
        self.statement = sql
        self.params = params

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, rows):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj


class _FakeTty(io.StringIO):
    def isatty(self):
        return True


class _FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(message % args if args else message)


class ProgressTests(unittest.TestCase):
    def test_render_progress_line_uses_two_line_layout(self):
        line = render_progress_line(
            label='finalize-name-stats',
            completed=25,
            total=100,
            elapsed_seconds=5.0,
            unit='tasks',
            extra='groups=42',
            width=10,
        )

        first_line, second_line = line.splitlines()
        self.assertEqual(first_line, '[finalize-name-stats] [##--------]  25.0%')
        self.assertIn('25/100 tasks', second_line)
        self.assertIn('5.0/s', second_line)
        self.assertIn('ETA 00:15', second_line)
        self.assertIn('elapsed 00:05', second_line)
        self.assertIn('groups=42', second_line)

    def test_progress_printer_does_not_log_intermediate_tty_frames(self):
        stream = _FakeTty()
        logger = _FakeLogger()
        printer = ProgressPrinter('prepare-name-tasks', 10, 'blocks', logger, stream=stream, log_interval_seconds=0.0)

        printer.update(3, extra='work_items=8')
        printer.update(5, extra='work_items=10')
        printer.close(extra='work_items=12')

        self.assertEqual(len(logger.messages), 1)
        self.assertIn('work_items=12', logger.messages[0])
        self.assertIn('[prepare-name-tasks]', stream.getvalue())

    def test_work_item_progress_aggregates_status_counts(self):
        conn = _FakeConnection(
            [
                ('pending', 3, 0),
                ('running', 2, 120),
                ('done', 5, 450),
                ('failed', 1, 30),
            ]
        )

        progress = _work_item_progress(conn, 'apr01')

        self.assertEqual(
            progress,
            WorkItemProgress(
                total=11,
                pending=3,
                running=2,
                done=5,
                failed=1,
                edge_count=600,
            ),
        )
        self.assertIn('FROM nsv2_name_work_items', conn.cursor_obj.statement)
        self.assertEqual(conn.cursor_obj.params, ('apr01',))


if __name__ == '__main__':
    unittest.main()
