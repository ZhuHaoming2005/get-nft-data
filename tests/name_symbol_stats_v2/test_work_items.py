import unittest
from unittest.mock import patch

from name_symbol_stats_v2.work_items import WorkItem, build_work_items


class _Cursor:
    def __init__(self, block_rows, split_rows=()):
        self.block_rows = list(block_rows)
        self.split_rows = list(split_rows)
        self.statements = []
        self._rows = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if 'SELECT name_block_key, count(*)::int AS atom_count' in sql:
            self._rows = list(self.block_rows)
        elif 'SELECT left(name_signature_hash' in sql:
            self._rows = list(self.split_rows)
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self, block_rows, split_rows=()):
        self.cursor_obj = _Cursor(block_rows, split_rows)
        self.commits = 0
        self.inserted_values = []

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class WorkItemTests(unittest.TestCase):
    @patch('name_symbol_stats_v2.work_items.execute_values', autospec=True)
    def test_build_work_items_applies_adaptive_block_assignment_before_grouping(self, mock_execute_values):
        conn = _Connection([('and', 12)])

        def _capture(_cur, _sql, values, page_size=1000):
            conn.inserted_values = list(values)

        mock_execute_values.side_effect = _capture

        items = build_work_items(
            conn,
            'apr01',
            ['ethereum', 'base'],
            blocking_strategy='adaptive_v1',
            max_atoms_per_task=30000,
        )

        executed_sql = '\n'.join(sql for sql, _ in conn.cursor_obj.statements)
        self.assertIn('UPDATE nsv2_name_atoms', executed_sql)
        self.assertIn('name_collapsed', executed_sql)
        self.assertEqual(items[0], WorkItem('apr01', 'ethereum,base', 'and', '', 12))
        self.assertEqual(conn.inserted_values[0][3], 'and')

    @patch('name_symbol_stats_v2.work_items.execute_values', autospec=True)
    def test_build_work_items_keeps_signature_split_for_oversized_blocks(self, mock_execute_values):
        conn = _Connection([('space|6', 240)], [('abcd', 120), ('abef', 120)])

        def _capture(_cur, _sql, values, page_size=1000):
            conn.inserted_values = list(values)

        mock_execute_values.side_effect = _capture

        items = build_work_items(
            conn,
            'apr01',
            ['ethereum'],
            blocking_strategy='adaptive_v1',
            max_atoms_per_task=100,
        )

        self.assertEqual([item.signature_prefix for item in items], ['abcd', 'abef'])
        self.assertEqual([value[4] for value in conn.inserted_values], ['abcd', 'abef'])


if __name__ == '__main__':
    unittest.main()
