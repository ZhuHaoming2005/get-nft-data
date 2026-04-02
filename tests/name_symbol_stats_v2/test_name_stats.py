import unittest
from unittest.mock import patch

from name_symbol_stats_v2.name_stats import Atom, DuplicateGroupStats, Edge, finalize_name_stats


class _Cursor:
    def __init__(self, tasks, chain_totals):
        self.tasks = list(tasks)
        self.chain_totals = list(chain_totals)
        self.statements = []
        self._rows = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if 'FROM nsv2_name_work_items' in sql:
            self._rows = list(self.tasks)
            return
        if 'FROM nsv2_contract_identity' in sql:
            self._rows = list(self.chain_totals)
            return
        self._rows = []

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self, tasks, chain_totals):
        self.cursor_obj = _Cursor(tasks, chain_totals)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class _Tracker:
    def __init__(self, *_args, **_kwargs):
        self.updates = []
        self.closed = False

    def update(self, completed, extra=''):
        self.updates.append((completed, extra))

    def close(self, extra=''):
        self.closed = True


class FinalizeNameStatsTests(unittest.TestCase):
    @patch('name_symbol_stats_v2.name_stats.execute_values', autospec=True)
    @patch('name_symbol_stats_v2.name_stats.ProgressPrinter', autospec=True)
    @patch('name_symbol_stats_v2.name_stats._load_edges_for_task', autospec=True)
    @patch('name_symbol_stats_v2.name_stats._load_atoms_for_task', autospec=True)
    @patch('name_symbol_stats_v2.name_stats._groups_for_scope', autospec=True)
    def test_finalize_name_stats_batches_group_inserts_per_task(
        self,
        mock_groups_for_scope,
        mock_load_atoms,
        mock_load_edges,
        mock_progress_printer,
        mock_execute_values,
    ):
        conn = _Connection(
            tasks=[
                (1, 'task-1', 'block-a', ''),
                (2, 'task-2', 'block-b', ''),
            ],
            chain_totals=[
                ('ethereum', 10, 100),
                ('base', 20, 200),
            ],
        )
        mock_progress_printer.side_effect = _Tracker
        mock_load_atoms.return_value = {
            1: Atom(atom_id=1, chain='ethereum', name_norm='azuki', contract_count=1, nft_count=10),
            2: Atom(atom_id=2, chain='base', name_norm='azuki', contract_count=1, nft_count=20),
        }
        mock_load_edges.return_value = []
        mock_groups_for_scope.return_value = [
            DuplicateGroupStats(
                group_key='g1',
                primary_contract_count=1,
                primary_nft_count=10,
                total_contract_count=2,
                total_nft_count=30,
                node_count=2,
                sample_value='azuki',
            )
        ]

        rows = finalize_name_stats(conn, 'apr01', ['ethereum', 'base'], [85.0, 90.0])

        self.assertEqual(len(rows), 12)
        self.assertEqual(mock_execute_values.call_count, 3)
        self.assertEqual(conn.commits, 2)

        first_batch_rows = mock_execute_values.call_args_list[0].args[2]
        self.assertEqual(len(first_batch_rows), 12)
        self.assertIn('intra_chain', {row[2] for row in first_batch_rows})
        self.assertIn('cross_chain_summary', {row[2] for row in first_batch_rows})
        self.assertIn('chain_matrix', {row[2] for row in first_batch_rows})

    @patch('name_symbol_stats_v2.name_stats.execute_values', autospec=True)
    @patch('name_symbol_stats_v2.name_stats.ProgressPrinter', autospec=True)
    @patch('name_symbol_stats_v2.name_stats._load_edges_for_task', autospec=True)
    @patch('name_symbol_stats_v2.name_stats._load_atoms_for_task', autospec=True)
    @patch('name_symbol_stats_v2.name_stats._component_groups', autospec=True)
    def test_finalize_name_stats_reuses_components_across_primary_chains(
        self,
        mock_component_groups,
        mock_load_atoms,
        mock_load_edges,
        mock_progress_printer,
        _mock_execute_values,
    ):
        conn = _Connection(
            tasks=[
                (1, 'task-1', 'block-a', ''),
            ],
            chain_totals=[
                ('ethereum', 10, 100),
                ('base', 20, 200),
            ],
        )
        mock_progress_printer.side_effect = _Tracker
        atoms = {
            1: Atom(atom_id=1, chain='ethereum', name_norm='azuki', contract_count=1, nft_count=10),
            2: Atom(atom_id=2, chain='base', name_norm='azuki', contract_count=1, nft_count=20),
            3: Atom(atom_id=3, chain='ethereum', name_norm='beanz', contract_count=1, nft_count=5),
        }
        edges = [
            Edge(left_atom_id=1, right_atom_id=3, similarity_score=95.0),
            Edge(left_atom_id=1, right_atom_id=2, similarity_score=92.0),
        ]
        mock_load_atoms.return_value = atoms
        mock_load_edges.return_value = edges
        mock_component_groups.side_effect = lambda scoped_atoms, scoped_edges, threshold: [list(scoped_atoms.keys())]

        finalize_name_stats(conn, 'apr01', ['ethereum', 'base'], [85.0, 90.0])

        self.assertLessEqual(mock_component_groups.call_count, 4)


if __name__ == '__main__':
    unittest.main()


