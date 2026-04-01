import unittest

from name_symbol_stats_v2.symbol_stats import _fetch_groups, _rebuild_symbol_rollup


class _FakeCursor:
    def __init__(self):
        self.rowcount = 0
        self.statements = []
        self._results = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if 'INSERT INTO nsv2_symbol_rollup' in sql:
            self.rowcount = 4
        elif 'FROM nsv2_symbol_rollup' in sql:
            self._results = [('azuki', 2, 10, 3, 15)]
        else:
            self._results = []

    def fetchall(self):
        return list(self._results)

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


class SymbolStatsTests(unittest.TestCase):
    def test_rebuild_symbol_rollup_uses_rollup_table(self):
        conn = _FakeConnection()

        inserted = _rebuild_symbol_rollup(conn, 'apr01', ['ethereum', 'base'])

        self.assertEqual(inserted, 4)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.cursor_obj.statements), 2)
        delete_sql, delete_params = conn.cursor_obj.statements[0]
        insert_sql, insert_params = conn.cursor_obj.statements[1]
        self.assertIn('DELETE FROM nsv2_symbol_rollup', delete_sql)
        self.assertEqual(delete_params, ('apr01', ['ethereum', 'base']))
        self.assertIn('INSERT INTO nsv2_symbol_rollup', insert_sql)
        self.assertEqual(insert_params, ('apr01', ['ethereum', 'base']))

    def test_fetch_groups_reads_from_rollup_for_intra_chain(self):
        conn = _FakeConnection()

        groups = _fetch_groups(conn, 'apr01', scope='intra_chain', primary_chain='ethereum')

        sql, params = conn.cursor_obj.statements[-1]
        self.assertIn('FROM nsv2_symbol_rollup', sql)
        self.assertNotIn('FROM nsv2_contract_identity', sql)
        self.assertEqual(params, ('apr01', 'ethereum'))
        self.assertEqual(groups[0].group_key, 'azuki')
        self.assertEqual(groups[0].primary_contract_count, 2)


if __name__ == '__main__':
    unittest.main()
