import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "fix_pg_name_spaces.py"
spec = importlib.util.spec_from_file_location("fix_pg_name_spaces", SCRIPT_PATH)
fix_pg_name_spaces = importlib.util.module_from_spec(spec)
sys.modules["fix_pg_name_spaces"] = fix_pg_name_spaces
spec.loader.exec_module(fix_pg_name_spaces)


class FixPgNameSpacesTest(unittest.TestCase):
    def test_table_name_sanitizes_chain(self):
        self.assertEqual(fix_pg_name_spaces.table_name("Ethereum"), "nft_assets_ethereum")
        self.assertEqual(fix_pg_name_spaces.table_name("base-mainnet"), "nft_assets_basemainnet")

    def test_normalize_edge_spaces_removes_leading_and_trailing_spaces(self):
        self.assertEqual(fix_pg_name_spaces.normalize_edge_spaces("Name  "), "Name")
        self.assertEqual(fix_pg_name_spaces.normalize_edge_spaces("  Name"), "Name")
        self.assertEqual(fix_pg_name_spaces.normalize_edge_spaces("  Name  "), "Name")
        self.assertEqual(fix_pg_name_spaces.normalize_edge_spaces("Na me"), "Na me")
        self.assertIsNone(fix_pg_name_spaces.normalize_edge_spaces(None))

    def test_select_fixable_contracts_sql_requires_single_trimmed_name(self):
        sql = fix_pg_name_spaces.select_fixable_contracts_sql("nft_assets_ethereum")

        self.assertIn("COUNT(DISTINCT name)", sql)
        self.assertIn("COUNT(DISTINCT btrim(name))", sql)
        self.assertIn("trimmed_name_count = 1", sql)
        self.assertIn("raw_name_count > 1", sql)
        self.assertIn("canonical_name", sql)

    def test_apply_fix_sql_updates_only_edge_space_rows(self):
        sql = fix_pg_name_spaces.apply_fix_sql("nft_assets_ethereum")

        self.assertIn("UPDATE nft_assets_ethereum AS target", sql)
        self.assertIn("SET name = fixable.canonical_name", sql)
        self.assertIn("lower(target.contract_address) = fixable.contract_address", sql)
        self.assertIn("target.name <> btrim(target.name)", sql)

    def test_generated_sql_uses_only_limit_placeholder_for_examples(self):
        self.assertEqual(fix_pg_name_spaces.count_fixable_sql("nft_assets_ethereum").count("%s"), 0)
        self.assertEqual(
            fix_pg_name_spaces.select_fixable_contracts_sql("nft_assets_ethereum").count("%s"),
            1,
        )
        self.assertEqual(fix_pg_name_spaces.apply_fix_sql("nft_assets_ethereum").count("%s"), 0)


if __name__ == "__main__":
    unittest.main()
