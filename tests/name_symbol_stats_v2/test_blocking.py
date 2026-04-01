import unittest

from name_symbol_stats_v2.blocking import (
    AdaptiveBlockingConfig,
    AdaptiveCanopyCounts,
    choose_adaptive_block_key,
)


class AdaptiveBlockingTests(unittest.TestCase):
    def test_small_canopies_return_p3_only(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=12, p3_len_count=12, p4_len_count=12)
        self.assertEqual(choose_adaptive_block_key('andyduboc', 10, counts, config), 'and')

    def test_small_canopy_boundary_uses_medium_path(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=64, p3_len_count=64, p4_len_count=64)
        self.assertEqual(choose_adaptive_block_key('andyduboc', 10, counts, config), 'and|6')

    def test_medium_canopies_return_p3_len6(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=8000, p3_len_count=2400, p4_len_count=2400)
        self.assertEqual(choose_adaptive_block_key('andyduboc', 10, counts, config), 'and|6')

    def test_hot_canopies_with_oversized_p4_len_count_return_p5_len6(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=12000, p3_len_count=45000, p4_len_count=32000)
        self.assertEqual(choose_adaptive_block_key('spaceape', 8, counts, config), 'space|6')


if __name__ == '__main__':
    unittest.main()
