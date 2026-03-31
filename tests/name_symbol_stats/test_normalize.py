import unittest

from name_symbol_stats.normalize import (
    build_name_block_key,
    normalize_name,
    normalize_symbol,
)


class NormalizeTests(unittest.TestCase):
    def test_normalize_name_strips_trailing_number_suffix(self) -> None:
        self.assertEqual(normalize_name("CryptoPunks #123"), "cryptopunks")
        self.assertEqual(normalize_name("CryptoPunks #456"), "cryptopunks")

    def test_normalize_name_canonicalizes_unicode_and_spaces(self) -> None:
        self.assertEqual(normalize_name(" Ａpe   Yacht\tClub "), "ape yacht club")

    def test_normalize_symbol_casefolds(self) -> None:
        self.assertEqual(normalize_symbol("  PuNk "), "punk")

    def test_build_name_block_key_uses_canonical_name(self) -> None:
        self.assertEqual(
            build_name_block_key(normalize_name("Mutant Ape Yacht Club #999")),
            build_name_block_key(normalize_name("mutant ape yacht club")),
        )


if __name__ == "__main__":
    unittest.main()
