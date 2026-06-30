import importlib.util
import json
import unittest
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "fetch_opensea_top_seeds.py"
)
spec = importlib.util.spec_from_file_location("fetch_opensea_top_seeds", SCRIPT_PATH)
fetch_opensea_top_seeds = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_opensea_top_seeds)


class FetchOpenSeaTopSeedsTest(unittest.TestCase):
    def test_extract_trending_collection_addresses_filters_chain_and_deduplicates(self):
        payload = {
            "collections": [
                {
                    "collection": "bayc",
                    "contracts": [
                        {
                            "address": "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
                            "chain": "ethereum",
                        },
                        {
                            "address": "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
                            "chain": "ethereum",
                        },
                        {
                            "address": "0x1111111111111111111111111111111111111111",
                            "chain": "polygon",
                        },
                    ],
                },
                {
                    "collection": "punks",
                    "contract_address": "0x2222222222222222222222222222222222222222",
                    "chain": "ethereum",
                },
                {
                    "collection": "bad",
                    "contracts": [{"address": "not-an-address", "chain": "ethereum"}],
                },
            ],
            "next": "cursor-1",
        }

        self.assertEqual(
            fetch_opensea_top_seeds.extract_trending_collection_addresses(
                payload, "ethereum"
            ),
            [
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x2222222222222222222222222222222222222222",
            ],
        )

    def test_build_trending_collections_url_uses_trending_endpoint_params(self):
        url = fetch_opensea_top_seeds.build_trending_collections_url(
            "https://api.opensea.io/api/v2/collections/trending",
            chain="ethereum",
            page_size=50,
            timeframe="thirty_days",
            cursor=None,
        )

        self.assertEqual(
            url,
            "https://api.opensea.io/api/v2/collections/trending?"
            "chains=ethereum&limit=50&timeframe=thirty_days",
        )

    def test_build_trending_collections_url_adds_cursor_when_present(self):
        url = fetch_opensea_top_seeds.build_trending_collections_url(
            "https://api.opensea.io/api/v2/collections/trending",
            chain="ethereum",
            page_size=50,
            timeframe="thirty_days",
            cursor="abc",
        )

        self.assertIn("cursor=abc", url)

    def test_format_seeds_lowercases_evm_addresses(self):
        self.assertEqual(
            fetch_opensea_top_seeds.format_seeds(
                [
                    "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
                    "0x1111111111111111111111111111111111111111",
                ],
                "ethereum",
            ),
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd\n"
            "0x1111111111111111111111111111111111111111\n",
        )

    def test_format_seeds_preserves_solana_address_case(self):
        address = "So11111111111111111111111111111111111111112"

        self.assertEqual(
            fetch_opensea_top_seeds.format_seeds([address], "solana"),
            f"{address}\n",
        )

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

    def test_legacy_single_chain_argument_is_preserved(self):
        args = fetch_opensea_top_seeds.parse_args(
            ["--chain", "polygon", "--output", "polygon.txt"]
        )

        self.assertEqual(args.chains, ["polygon"])
        self.assertEqual(args.output, Path("polygon.txt"))

    def test_collect_seed_addresses_by_chain_calls_each_chain_independently(self):
        calls = []

        def collector(**kwargs):
            calls.append(kwargs["chain"])
            return [kwargs["chain"]]

        result = fetch_opensea_top_seeds.collect_seed_addresses_by_chain(
            chains=["ethereum", "base", "polygon", "solana"],
            collector=collector,
            api_key="key",
            limit=1,
            page_size=1,
            trending_collections_url="https://example.test/trending",
            timeframe="thirty_days",
            timeout=1.0,
        )

        self.assertEqual(calls, ["ethereum", "base", "polygon", "solana"])
        self.assertEqual(
            result,
            {
                "ethereum": ["ethereum"],
                "base": ["base"],
                "polygon": ["polygon"],
                "solana": ["solana"],
            },
        )

    def test_seed_output_path_uses_chain_specific_file_for_multi_chain_run(self):
        self.assertEqual(
            fetch_opensea_top_seeds.seed_output_path(
                chain="solana",
                selected_chains=["ethereum", "base", "polygon", "solana"],
                output=None,
                output_dir=Path("seeds"),
            ),
            Path("seeds/solana.seeds.txt"),
        )

    def test_parse_json_response_accepts_bytes_json(self):
        payload = b'{"collections":[{"contracts":[{"address":"0x2222222222222222222222222222222222222222","chain":"ethereum"}]}]}'

        self.assertEqual(
            fetch_opensea_top_seeds.parse_json_response(payload),
            json.loads(payload.decode("utf-8")),
        )


if __name__ == "__main__":
    unittest.main()
