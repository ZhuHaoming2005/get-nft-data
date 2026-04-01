import unittest

from name_symbol_stats_v2.config import chain_to_table, normalize_run_label


class ConfigTests(unittest.TestCase):
    def test_chain_to_table(self):
        self.assertEqual(chain_to_table('Ethereum'), 'nft_assets_ethereum')

    def test_normalize_run_label(self):
        self.assertEqual(normalize_run_label('apr 01 / test'), 'apr-01-test')


if __name__ == '__main__':
    unittest.main()
