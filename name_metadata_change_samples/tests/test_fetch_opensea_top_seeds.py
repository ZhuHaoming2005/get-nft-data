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
    def test_extract_top_collection_addresses_filters_chain_and_deduplicates(self):
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
            fetch_opensea_top_seeds.extract_top_collection_addresses(
                payload, "ethereum"
            ),
            [
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x2222222222222222222222222222222222222222",
            ],
        )

    def test_build_top_collections_url_uses_30_day_volume_sort(self):
        url = fetch_opensea_top_seeds.build_top_collections_url(
            "https://api.opensea.io/api/v2/collections/top",
            chain="ethereum",
            page_size=50,
            sort_by="thirty_days_volume",
            cursor=None,
        )

        self.assertEqual(
            url,
            "https://api.opensea.io/api/v2/collections/top?"
            "chains=ethereum&limit=50&sort_by=thirty_days_volume",
        )

    def test_build_top_collections_url_adds_cursor_when_present(self):
        url = fetch_opensea_top_seeds.build_top_collections_url(
            "https://api.opensea.io/api/v2/collections/top",
            chain="ethereum",
            page_size=50,
            sort_by="thirty_days_volume",
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
