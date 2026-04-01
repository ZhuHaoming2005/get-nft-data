import unittest

from name_symbol_stats_v2.normalize import (
    build_name_block_key,
    build_name_signature,
    build_name_signature_hash,
    normalize_name,
    normalize_symbol,
)


class NormalizeTests(unittest.TestCase):
    def test_normalize_name_strips_numeric_suffix(self):
        self.assertEqual(normalize_name('CryptoPunks #123'), 'cryptopunks')
        self.assertEqual(normalize_name('CryptoPunks #456'), 'cryptopunks')

    def test_normalize_symbol_collapses_space(self):
        self.assertEqual(normalize_symbol(' A Z U K I '), 'azuki')

    def test_blocking_artifacts_are_stable(self):
        name = normalize_name('Moonbirds No. 777')
        self.assertEqual(build_name_block_key(name), 'moonbirds|8')
        self.assertEqual(build_name_signature(name), 'moonbirds')
        self.assertEqual(len(build_name_signature_hash(name)), 40)


if __name__ == '__main__':
    unittest.main()
