import csv
import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "fetch_opensea_top_seeds.py"
)
spec = importlib.util.spec_from_file_location("fetch_opensea_global_top", SCRIPT_PATH)
fetch_opensea_top_seeds = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = fetch_opensea_top_seeds
spec.loader.exec_module(fetch_opensea_top_seeds)


class FetchOpenSeaGlobalTopTest(unittest.TestCase):
    def test_build_top_url_uses_one_four_chain_ranking(self):
        url = fetch_opensea_top_seeds.build_top_collections_url(
            "https://api.opensea.io/api/v2/collections/top",
            chains=["ethereum", "base", "polygon", "solana"],
            page_size=50,
            cursor=None,
        )

        parsed = urlparse(url)
        self.assertEqual(parsed.path, "/api/v2/collections/top")
        self.assertEqual(
            parse_qs(parsed.query),
            {
                "chains": ["ethereum,base,polygon,solana"],
                "limit": ["50"],
                "sort_by": ["thirty_days_volume"],
            },
        )

    def test_collect_ranked_collections_uses_global_limit_and_one_cursor(self):
        payloads = iter(
            [
                {
                    "collections": [
                        {
                            "collection": "first",
                            "name": "First",
                            "thirty_days_volume": 20,
                            "contracts": [
                                {
                                    "chain": "ethereum",
                                    "address": "0x1111111111111111111111111111111111111111",
                                },
                                {
                                    "chain": "base",
                                    "address": "0x2222222222222222222222222222222222222222",
                                },
                            ],
                        }
                    ],
                    "next": "page-2",
                },
                {
                    "collections": [
                        {
                            "collection": "invalid",
                            "contracts": [
                                {"chain": "ethereum", "address": "bad-address"}
                            ],
                        },
                        {
                            "collection": "second",
                            "name": "Second",
                            "stats": {"thirty_day_volume": 10},
                            "contracts": [
                                {
                                    "chain": "polygon",
                                    "address": "0x3333333333333333333333333333333333333333",
                                },
                                {
                                    "chain": "solana",
                                    "address": "So11111111111111111111111111111111111111112",
                                },
                            ],
                        },
                    ]
                },
            ]
        )
        urls = []

        def fake_fetch(url, api_key, timeout):
            urls.append(url)
            return json.dumps(next(payloads)).encode()

        ranked = fetch_opensea_top_seeds.collect_ranked_collections(
            api_key="key",
            chains=["ethereum", "base", "polygon", "solana"],
            limit=2,
            page_size=100,
            top_collections_url="https://api.opensea.io/api/v2/collections/top",
            timeout=1.0,
            fetcher=fake_fetch,
        )

        self.assertEqual([item.global_rank for item in ranked], [1, 2])
        self.assertEqual([item.slug for item in ranked], ["first", "second"])
        self.assertEqual([item.ranking_value for item in ranked], [20, 10])
        self.assertEqual(len(urls), 2)
        self.assertNotIn("cursor=", urls[0])
        self.assertIn("cursor=page-2", urls[1])

    def test_collection_pairs_are_chain_aware_and_keep_raw_alias(self):
        address = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD"
        pairs = fetch_opensea_top_seeds.collection_contract_pairs(
            {
                "contracts": [
                    {"chain": "Ethereum", "address": address},
                    {"chain": "ethereum", "address": address},
                    {
                        "chain": "polygon",
                        "address": "0x1111111111111111111111111111111111111111",
                    },
                ]
            },
            {"ethereum"},
        )

        self.assertEqual(
            pairs,
            (
                fetch_opensea_top_seeds.ContractPair(
                    chain="ethereum",
                    address=address.lower(),
                    raw_chain="Ethereum",
                ),
            ),
        )

    def test_collect_ranked_collections_rejects_repeated_cursor(self):
        def fake_fetch(url, api_key, timeout):
            return json.dumps(
                {
                    "collections": [
                        {
                            "collection": "only",
                            "contracts": [
                                {
                                    "chain": "ethereum",
                                    "address": "0x1111111111111111111111111111111111111111",
                                }
                            ],
                        }
                    ],
                    "next": "same-cursor",
                }
            ).encode()

        with self.assertRaisesRegex(ValueError, "repeated pagination cursor"):
            fetch_opensea_top_seeds.collect_ranked_collections(
                api_key="key",
                chains=["ethereum"],
                limit=3,
                page_size=1,
                top_collections_url="https://example.test/top",
                timeout=1.0,
                fetcher=fake_fetch,
            )

    def test_collect_ranked_collections_rejects_short_result(self):
        def fake_fetch(url, api_key, timeout):
            return json.dumps(
                {
                    "collections": [
                        {
                            "collection": "only",
                            "contracts": [
                                {
                                    "chain": "ethereum",
                                    "address": "0x1111111111111111111111111111111111111111",
                                }
                            ],
                        }
                    ]
                }
            ).encode()

        with self.assertRaisesRegex(ValueError, "requested 2.*collected 1"):
            fetch_opensea_top_seeds.collect_ranked_collections(
                api_key="key",
                chains=["ethereum"],
                limit=2,
                page_size=2,
                top_collections_url="https://example.test/top",
                timeout=1.0,
                fetcher=fake_fetch,
            )

    def test_collection_items_rejects_missing_or_malformed_list(self):
        with self.assertRaisesRegex(ValueError, "does not contain"):
            fetch_opensea_top_seeds.collection_items({})
        with self.assertRaisesRegex(ValueError, "must be a list"):
            fetch_opensea_top_seeds.collection_items({"collections": {}})

    def test_manifest_pairs_keep_chain_identity_and_earliest_rank(self):
        address = "0x1111111111111111111111111111111111111111"
        ranked = [
            fetch_opensea_top_seeds.RankedCollection(
                global_rank=1,
                slug="first",
                name="First",
                ranking_value=20,
                contract_pairs=(
                    fetch_opensea_top_seeds.ContractPair("ethereum", address),
                    fetch_opensea_top_seeds.ContractPair("base", address),
                ),
            ),
            fetch_opensea_top_seeds.RankedCollection(
                global_rank=2,
                slug="second",
                name="Second",
                ranking_value=10,
                contract_pairs=(
                    fetch_opensea_top_seeds.ContractPair("ethereum", address),
                ),
            ),
        ]

        self.assertEqual(
            fetch_opensea_top_seeds.manifest_pairs(ranked),
            [
                fetch_opensea_top_seeds.ContractPair("ethereum", address),
                fetch_opensea_top_seeds.ContractPair("base", address),
            ],
        )

    def test_rank_output_serializers_emit_exact_pair_csv_and_audit_json(self):
        solana = "So11111111111111111111111111111111111111112"
        ranked = [
            fetch_opensea_top_seeds.RankedCollection(
                global_rank=1,
                slug="shared",
                name="Shared",
                ranking_value=42.5,
                contract_pairs=(
                    fetch_opensea_top_seeds.ContractPair(
                        "solana", solana, "Solana"
                    ),
                ),
            )
        ]

        csv_text = fetch_opensea_top_seeds.render_manifest_csv(ranked)
        self.assertEqual(
            list(csv.reader(io.StringIO(csv_text))),
            [["chain", "address"], ["solana", solana]],
        )
        audit = fetch_opensea_top_seeds.audit_payload(
            ranked,
            ["ethereum", "base", "polygon", "solana"],
            1,
        )
        self.assertEqual(audit["ranking_criterion"], "thirty_days_volume")
        self.assertEqual(audit["collections"][0]["global_rank"], 1)
        self.assertEqual(
            audit["collections"][0]["contract_pairs"],
            [{"chain": "solana", "address": solana, "raw_chain": "Solana"}],
        )

    def test_parse_args_defaults_to_shared_four_chain_outputs(self):
        args = fetch_opensea_top_seeds.parse_args([])

        self.assertEqual(
            args.chains,
            ["ethereum", "base", "polygon", "solana"],
        )
        self.assertEqual(args.contracts_output, Path("../seeds/top_contracts.csv"))
        self.assertEqual(args.audit_output, Path("../seeds/top_collections.json"))


if __name__ == "__main__":
    unittest.main()
