import unittest

from name_symbol_stats.report import DuplicateGroupStats, summarize_groups


class StatsSemanticsTests(unittest.TestCase):
    def test_summary_uses_primary_chain_nft_counts_and_group_size_buckets(self) -> None:
        groups = [
            DuplicateGroupStats(
                group_key="g1",
                primary_contract_count=2,
                primary_nft_count=300,
                total_member_count=2,
            ),
            DuplicateGroupStats(
                group_key="g2",
                primary_contract_count=3,
                primary_nft_count=90,
                total_member_count=3,
            ),
        ]

        summary = summarize_groups(
            field_name="name",
            scope="intra_chain",
            primary_chain="ethereum",
            secondary_chain=None,
            threshold=90.0,
            total_contracts=10,
            total_nfts=1000,
            groups=groups,
        )

        self.assertEqual(summary.group_count, 2)
        self.assertEqual(summary.duplicate_contract_count, 5)
        self.assertEqual(summary.duplicate_nft_count, 390)
        self.assertAlmostEqual(summary.duplicate_contract_ratio, 50.0)
        self.assertAlmostEqual(summary.duplicate_nft_ratio, 39.0)
        self.assertEqual(summary.group_size_ge_2_count, 2)
        self.assertEqual(summary.group_size_gt_2_count, 1)


if __name__ == "__main__":
    unittest.main()
