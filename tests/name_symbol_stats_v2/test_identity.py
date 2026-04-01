import unittest
import re
from pathlib import Path

from name_symbol_stats_v2.identity import _rebuild_name_atoms


class _FakeCursor:
    def __init__(self):
        self.rowcount = 0
        self.statements = []

    def execute(self, sql, params=None):
        placeholder_count = sql.count('%s')
        param_count = len(params or ())
        if placeholder_count != param_count:
            raise TypeError('not all arguments converted during string formatting')
        self.statements.append((sql, params))
        if 'INSERT INTO nsv2_name_atoms' in sql:
            self.rowcount = 7

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self):
        self.cursor_obj = _FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class IdentityTests(unittest.TestCase):
    def test_schema_includes_upgrade_statements_for_name_atoms(self):
        schema_path = Path(__file__).resolve().parents[2] / 'name_symbol_stats_v2' / 'sql' / '01_schema.sql'
        schema_sql = schema_path.read_text()

        self.assertIn("ALTER TABLE nsv2_name_atoms\n    ADD COLUMN IF NOT EXISTS name_collapsed TEXT NOT NULL DEFAULT '';", schema_sql)
        self.assertIn("ALTER TABLE nsv2_name_atoms\n    ADD COLUMN IF NOT EXISTS name_collapsed_len INTEGER NOT NULL DEFAULT 0;", schema_sql)
    def test_rebuild_name_atoms_uses_matching_sql_parameters(self):
        conn = _FakeConnection()

        inserted = _rebuild_name_atoms(conn, 'apr01', ['ethereum', 'base'])

        self.assertEqual(inserted, 7)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.cursor_obj.statements), 2)
        insert_sql, insert_params = conn.cursor_obj.statements[1]
        match = re.search(r'INSERT INTO nsv2_name_atoms\s*\((.*?)\)\s*SELECT', insert_sql, re.S)
        self.assertIsNotNone(match)
        column_list = [column.strip() for column in match.group(1).split(',')]
        self.assertIn('name_collapsed', column_list)
        self.assertIn('name_collapsed_len', column_list)
        self.assertEqual(insert_params, ('apr01', ['ethereum', 'base']))


if __name__ == '__main__':
    unittest.main()
