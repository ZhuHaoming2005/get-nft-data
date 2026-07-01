import importlib.util
import json
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "fetch_opensea_top_seeds.py"
)
spec = importlib.util.spec_from_file_location("fetch_opensea_top_seeds", SCRIPT_PATH)
fetch_opensea_top_seeds = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = fetch_opensea_top_seeds
spec.loader.exec_module(fetch_opensea_top_seeds)


class FetchOpenSeaTopSeedsTest(unittest.TestCase):
    def test_normalize_contract_address_preserves_valid_solana_case(self):
        address = "So11111111111111111111111111111111111111112"

        self.assertEqual(
            fetch_opensea_top_seeds.normalize_contract_address(address, "solana"),
            address,
        )
        self.assertIsNone(
            fetch_opensea_top_seeds.normalize_contract_address(
                "O0not-base58", "solana"
            )
        )

    def test_normalize_contract_address_lowercases_evm(self):
        address = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD"

        self.assertEqual(
            fetch_opensea_top_seeds.normalize_contract_address(address, "base"),
            address.lower(),
        )

    def test_default_chains_cover_all_four_datasets(self):
        args = fetch_opensea_top_seeds.parse_args([])

        self.assertEqual(
            args.chains,
            ["ethereum", "base", "polygon", "solana"],
        )
        self.assertIsNone(args.api_key)

    def test_parse_json_response_accepts_bytes_json(self):
        payload = b'{"collections":[{"contracts":[{"address":"0x2222222222222222222222222222222222222222","chain":"ethereum"}]}]}'

        self.assertEqual(
            fetch_opensea_top_seeds.parse_json_response(payload),
            json.loads(payload.decode("utf-8")),
        )


if __name__ == "__main__":
    unittest.main()
