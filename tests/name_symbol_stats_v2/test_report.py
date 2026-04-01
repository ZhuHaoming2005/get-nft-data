import unittest

from name_symbol_stats_v2.report import DuplicateGroupStats, summarize_groups


class ReportTests(unittest.TestCase):
    def test_summary_uses_contract_weight_for_group_sizes(self):
        groups = [
            DuplicateGroupStats(
                group_key='g1',
                primary_contract_count=2,
                primary_nft_count=10,
                total_contract_count=3,
                total_nft_count=15,
                node_count=2,
                sample_value='azuki',
            )
        ]
        row = summarize_groups(
            run_label='r1',
            field_name='name',
            scope='intra_chain',
            primary_chain='ethereum',
            secondary_chain='',
            threshold=90.0,
            total_contracts=100,
            total_nfts=1000,
            groups=groups,
        )
        self.assertEqual(row.group_count, 1)
        self.assertEqual(row.duplicate_contract_count, 2)
        self.assertEqual(row.group_size_ge_2_count, 1)
        self.assertEqual(row.group_size_gt_2_count, 1)


if __name__ == '__main__':
    unittest.main()
