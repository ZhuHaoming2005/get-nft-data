import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "fix_pg_none_names.py"
spec = importlib.util.spec_from_file_location("fix_pg_none_names", SCRIPT_PATH)
fix_pg_none_names = importlib.util.module_from_spec(spec)
sys.modules["fix_pg_none_names"] = fix_pg_none_names
spec.loader.exec_module(fix_pg_none_names)


class FixPgNoneNamesTest(unittest.TestCase):
    def test_table_name_sanitizes_chain(self):
        self.assertEqual(fix_pg_none_names.table_name("Ethereum"), "nft_assets_ethereum")
        self.assertEqual(fix_pg_none_names.table_name("base-mainnet"), "nft_assets_basemainnet")

    def test_bad_name_predicate_matches_none_like_values_only(self):
        self.assertTrue(fix_pg_none_names.is_bad_name(None))
        self.assertTrue(fix_pg_none_names.is_bad_name(""))
        self.assertTrue(fix_pg_none_names.is_bad_name(" None "))
        self.assertTrue(fix_pg_none_names.is_bad_name("NULL"))
        self.assertFalse(fix_pg_none_names.is_bad_name("FearCity"))
        self.assertFalse(fix_pg_none_names.is_bad_name("Nonks"))

    def test_select_fixable_contracts_sql_requires_one_good_name(self):
        sql = fix_pg_none_names.select_fixable_contracts_sql("nft_assets_ethereum")

        self.assertIn("COUNT(DISTINCT btrim(name))", sql)
        self.assertIn("good_name_count = 1", sql)
        self.assertIn("has_bad_name", sql)
        self.assertIn("canonical_name", sql)

    def test_apply_fix_sql_updates_only_bad_rows_for_fixable_contracts(self):
        sql = fix_pg_none_names.apply_fix_sql("nft_assets_ethereum")

        self.assertIn("UPDATE nft_assets_ethereum AS target", sql)
        self.assertIn("SET name = fixable.canonical_name", sql)
        self.assertIn("lower(target.contract_address) = fixable.contract_address", sql)
        self.assertIn("target.name IS NULL", sql)

    def test_parameter_counts_match_generated_sql_placeholders(self):
        self.assertEqual(fix_pg_none_names.count_fixable_sql("nft_assets_ethereum").count("%s"), 5)
        self.assertEqual(
            fix_pg_none_names.select_fixable_contracts_sql("nft_assets_ethereum").count("%s"),
            4,
        )
        self.assertEqual(fix_pg_none_names.apply_fix_sql("nft_assets_ethereum").count("%s"), 4)


if __name__ == "__main__":
    unittest.main()
