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

    def test_format_seeds_writes_one_lowercase_address_per_line(self):
        self.assertEqual(
            fetch_opensea_top_seeds.format_seeds(
                [
                    "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
                    "0x1111111111111111111111111111111111111111",
                ]
            ),
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd\n"
            "0x1111111111111111111111111111111111111111\n",
        )

    def test_parse_json_response_accepts_bytes_json(self):
        payload = b'{"collections":[{"contracts":[{"address":"0x2222222222222222222222222222222222222222","chain":"ethereum"}]}]}'

        self.assertEqual(
            fetch_opensea_top_seeds.parse_json_response(payload),
            json.loads(payload.decode("utf-8")),
        )


if __name__ == "__main__":
    unittest.main()
