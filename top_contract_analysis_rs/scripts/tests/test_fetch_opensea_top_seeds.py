import importlib.util
import csv
import hashlib
import io
import json
import sys
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "fetch_opensea_top_seeds.py"
spec = importlib.util.spec_from_file_location("top_contract_fetch_seeds", SCRIPT_PATH)
fetch_seeds = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = fetch_seeds
spec.loader.exec_module(fetch_seeds)


class FetchOpenSeaTopSeedsTest(unittest.TestCase):
    def test_collect_ranked_contracts_for_chain_expands_and_deduplicates(self):
        address_a = "0x1111111111111111111111111111111111111111"
        address_b = "0x2222222222222222222222222222222222222222"

        def fake_fetch(url, api_key, timeout):
            return json.dumps(
                {
                    "collections": [
                        {
                            "collection": "first",
                            "thirty_days_volume": 20,
                            "contracts": [
                                {"chain": "ethereum", "address": address_a},
                                {"chain": "ethereum", "address": address_b},
                            ],
                        },
                        {
                            "collection": "second",
                            "thirty_days_volume": 10,
                            "contracts": [
                                {"chain": "ethereum", "address": address_a}
                            ],
                        },
                    ]
                }
            ).encode()

        ranked = fetch_seeds.collect_ranked_contracts_for_chain(
            api_key="key",
            chain="ethereum",
            limit=2,
            page_size=100,
            top_collections_url="https://example.test/top",
            timeout=1.0,
            fetcher=fake_fetch,
        )

        self.assertEqual(
            [(row.chain_contract_rank, row.collection_rank, row.address) for row in ranked],
            [(1, 1, address_a), (2, 1, address_b)],
        )

    def test_render_ranked_contract_outputs_keep_per_chain_ranks(self):
        solana = "So11111111111111111111111111111111111111112"
        rows = [
            fetch_seeds.RankedContract(
                chain="ethereum",
                address="0x1111111111111111111111111111111111111111",
                chain_contract_rank=1,
                collection_rank=1,
                slug="eth-first",
                name="ETH First",
                ranking_value=20,
            ),
            fetch_seeds.RankedContract(
                chain="solana",
                address=solana,
                chain_contract_rank=1,
                collection_rank=2,
                slug="sol-first",
                name="SOL First",
                ranking_value=10,
                raw_chain="Solana",
            ),
        ]

        self.assertEqual(
            list(csv.reader(io.StringIO(fetch_seeds.render_contract_manifest_csv(rows)))),
            [
                ["chain", "address"],
                ["ethereum", "0x1111111111111111111111111111111111111111"],
                ["solana", solana],
            ],
        )
        audit = fetch_seeds.contract_audit_payload(rows, ["ethereum", "solana"], 1)
        self.assertEqual(audit["requested_contract_limit_per_chain"], 1)
        self.assertEqual(audit["contracts"][1]["chain_contract_rank"], 1)
        self.assertEqual(audit["contracts"][1]["collection_rank"], 2)
        self.assertEqual(audit["contracts"][1]["raw_chain"], "Solana")
        self.assertEqual(audit["contracts"][0]["raw_chain"], "ethereum")
        self.assertEqual(audit["provider_by_chain"]["solana"], "magic_eden")
        self.assertEqual(
            audit["ranking_criterion_by_chain"]["solana"],
            "magic_eden_30d_popularity",
        )

    def test_collect_ranked_solana_contracts_uses_magic_eden_addresses(self):
        address_a = "So11111111111111111111111111111111111111112"
        address_b = "11111111111111111111111111111111"
        calls = []

        def fake_fetch(url, timeout):
            calls.append((url, timeout))
            if "/listings?" in url or "/activities?" in url:
                return b"[]"
            return json.dumps(
                {
                    "collections": [
                        {
                            "symbol": "missing-certified-address",
                            "name": "Skipped",
                            "candyMachineIds": [address_a],
                        },
                        {
                            "symbol": "first",
                            "name": "First",
                            "onChainCollectionAddress": address_a,
                            "volume": 20,
                        },
                        {
                            "symbol": "duplicate",
                            "onChainCollectionAddress": address_a,
                        },
                        {
                            "symbol": "second",
                            "collectionAddress": address_b,
                            "volumeAll": 10,
                        },
                    ]
                }
            ).encode()

        ranked = fetch_seeds.collect_ranked_solana_contracts(
            limit=2,
            top_collections_url="https://magic.example/top?source=test",
            magic_eden_api_url="https://magic.example/v2",
            helius_rpc_url="https://helius.example/?api-key=key",
            timeout=1.0,
            magic_eden_fetcher=fake_fetch,
        )

        self.assertEqual(
            [(row.address, row.collection_rank, row.provider) for row in ranked],
            [
                (address_a, 2, "magic_eden"),
                (address_b, 4, "magic_eden"),
            ],
        )
        self.assertIn("source=test&timeRange=30d", calls[0][0])

    def test_magic_eden_symbol_is_resolved_through_listing_and_helius(self):
        mint = "Cs4TtkiphY3Yzor5qPrfSoAzWqHUD1JnTWAxyXrS3sS3"
        collection_address = "So11111111111111111111111111111111111111112"
        magic_calls = []
        helius_calls = []

        def fake_magic_fetch(url, timeout):
            magic_calls.append(url)
            if "popular_collections" in url:
                return json.dumps(
                    [{"symbol": "first/collection", "name": "First"}]
                ).encode()
            return json.dumps([{"tokenMint": mint}]).encode()

        def fake_helius_fetch(url, payload, timeout):
            helius_calls.append((url, payload))
            return json.dumps(
                {
                    "jsonrpc": "2.0",
                    "result": {
                        "grouping": [
                            {
                                "group_key": "collection",
                                "group_value": collection_address,
                            }
                        ]
                    },
                }
            ).encode()

        ranked = fetch_seeds.collect_ranked_solana_contracts(
            limit=1,
            top_collections_url="https://magic.example/v2/marketplace/popular_collections",
            magic_eden_api_url="https://magic.example/v2",
            helius_rpc_url="https://helius.example/?api-key=key",
            timeout=1.0,
            magic_eden_fetcher=fake_magic_fetch,
            helius_fetcher=fake_helius_fetch,
        )

        self.assertEqual(ranked[0].address, collection_address)
        self.assertIn("first%2Fcollection/listings", magic_calls[1])
        self.assertEqual(helius_calls[0][1]["method"], "getAsset")
        self.assertEqual(helius_calls[0][1]["params"]["id"], mint)

    def test_short_magic_eden_result_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "Magic Eden.*collected 0"):
            fetch_seeds.collect_ranked_solana_contracts(
                limit=1,
                top_collections_url="https://magic.example/top",
                magic_eden_api_url="https://magic.example/v2",
                helius_rpc_url="https://helius.example/?api-key=key",
                timeout=1.0,
                magic_eden_fetcher=lambda *_args, **_kwargs: b'{"collections": []}',
            )

    def test_parse_args_does_not_embed_an_opensea_api_key(self):
        args = fetch_seeds.parse_args([])

        self.assertEqual(args.api_key, "")

    def test_solana_only_run_does_not_require_an_opensea_api_key(self):
        row = fetch_seeds.RankedContract(
            chain="solana",
            address="So11111111111111111111111111111111111111112",
            chain_contract_rank=1,
            collection_rank=1,
            slug="solana-first",
            name="Solana First",
            ranking_value=1,
            provider="magic_eden",
            ranking_criterion=fetch_seeds.SOLANA_SORT_BY,
        )
        with patch.dict(
            fetch_seeds.os.environ,
            {"OPENSEA_API_KEY": "", "HELIUS_API_KEY": "helius-key"},
        ), patch.object(
            fetch_seeds,
            "collect_ranked_solana_contracts",
            return_value=[row],
        ) as solana_collector, patch.object(
            fetch_seeds, "collect_ranked_contracts_for_chain"
        ) as opensea_collector, patch.object(
            fetch_seeds, "write_contract_rank_outputs"
        ) as writer:
            result = fetch_seeds.main(["--chains", "solana", "--limit", "1"])

        self.assertEqual(result, 0)
        solana_collector.assert_called_once()
        opensea_collector.assert_not_called()
        writer.assert_called_once()

    def test_each_chain_uses_an_independent_cursor_sequence(self):
        calls = []

        def fake_fetch(url, api_key, timeout):
            calls.append(url)
            chain = "ethereum" if "chains=ethereum" in url else "base"
            page_two = "cursor=next" in url
            suffix = "2" if page_two else "1"
            payload = {
                "collections": [
                    {
                        "collection": f"{chain}-{suffix}",
                        "contracts": [
                            {
                                "chain": chain,
                                "address": f"0x{suffix * 40}",
                            }
                        ],
                    }
                ]
            }
            if not page_two:
                payload["next"] = "next"
            return json.dumps(payload).encode()

        for chain in ("ethereum", "base"):
            rows = fetch_seeds.collect_ranked_contracts_for_chain(
                api_key="key",
                chain=chain,
                limit=2,
                page_size=1,
                top_collections_url="https://example.test/top",
                timeout=1.0,
                fetcher=fake_fetch,
            )
            self.assertEqual(len(rows), 2)

        self.assertEqual(sum("cursor=next" in url for url in calls), 2)
        self.assertEqual(sum("chains=ethereum" in url for url in calls), 2)
        self.assertEqual(sum("chains=base" in url for url in calls), 2)

    def test_short_chain_result_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "collected 1"):
            fetch_seeds.collect_ranked_contracts_for_chain(
                api_key="key",
                chain="ethereum",
                limit=2,
                page_size=100,
                top_collections_url="https://example.test/top",
                timeout=1.0,
                fetcher=lambda *_args, **_kwargs: json.dumps(
                    {
                        "collections": [
                            {
                                "collection": "only",
                                "contracts": [
                                    {
                                        "chain": "ethereum",
                                        "address": "0x" + "1" * 40,
                                    }
                                ],
                            }
                        ]
                    }
                ).encode(),
            )

    def test_failed_chain_does_not_replace_existing_outputs(self):
        with TemporaryDirectory() as directory:
            csv_path = Path(directory) / "seeds.csv"
            audit_path = Path(directory) / "audit.json"
            csv_path.write_text("old csv", encoding="utf-8")
            audit_path.write_text("old audit", encoding="utf-8")
            row = fetch_seeds.RankedContract(
                chain="ethereum",
                address="0x" + "1" * 40,
                chain_contract_rank=1,
                collection_rank=1,
                slug="ok",
                name="ok",
                ranking_value=1,
            )
            with patch.object(
                fetch_seeds,
                "collect_ranked_contracts_for_chain",
                side_effect=[[row], ValueError("base failed")],
            ), patch.object(fetch_seeds, "write_contract_rank_outputs") as writer:
                result = fetch_seeds.main(
                    [
                        "--api-key",
                        "key",
                        "--chains",
                        "ethereum",
                        "base",
                        "--limit",
                        "1",
                        "--contracts-output",
                        str(csv_path),
                        "--audit-output",
                        str(audit_path),
                    ]
                )
            self.assertEqual(result, 1)
            writer.assert_not_called()
            self.assertEqual(csv_path.read_text(encoding="utf-8"), "old csv")
            self.assertEqual(audit_path.read_text(encoding="utf-8"), "old audit")

    def test_second_output_replace_failure_rolls_back_both_outputs(self):
        with TemporaryDirectory() as directory:
            csv_path = Path(directory) / "seeds.csv"
            audit_path = Path(directory) / "audit.json"
            csv_path.write_text("old csv", encoding="utf-8")
            audit_path.write_text("old audit", encoding="utf-8")
            row = fetch_seeds.RankedContract(
                chain="ethereum",
                address="0x" + "1" * 40,
                chain_contract_rank=1,
                collection_rank=1,
                slug="ok",
                name="ok",
                ranking_value=1,
            )
            real_replace = fetch_seeds.os.replace
            failed = False

            def fail_audit_replace(source, destination):
                nonlocal failed
                if Path(destination) == audit_path and Path(source).suffix == ".tmp" and not failed:
                    failed = True
                    raise OSError("audit replace failed")
                return real_replace(source, destination)

            with patch.object(fetch_seeds.os, "replace", side_effect=fail_audit_replace):
                with self.assertRaisesRegex(OSError, "audit replace failed"):
                    fetch_seeds.write_contract_rank_outputs(
                        csv_path=csv_path,
                        audit_path=audit_path,
                        ranked=[row],
                        chains=["ethereum"],
                        requested_limit_per_chain=1,
                    )

            self.assertEqual(csv_path.read_text(encoding="utf-8"), "old csv")
            self.assertEqual(audit_path.read_text(encoding="utf-8"), "old audit")

    def test_committed_audit_identifies_and_hashes_exact_csv_generation(self):
        with TemporaryDirectory() as directory:
            csv_path = Path(directory) / "seeds.csv"
            audit_path = Path(directory) / "audit.json"
            row = fetch_seeds.RankedContract(
                chain="ethereum",
                address="0x" + "1" * 40,
                chain_contract_rank=1,
                collection_rank=1,
                slug="ok",
                name="ok",
                ranking_value=1,
            )

            fetch_seeds.write_contract_rank_outputs(
                csv_path=csv_path,
                audit_path=audit_path,
                ranked=[row],
                chains=["ethereum"],
                requested_limit_per_chain=1,
            )

            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            self.assertTrue(audit["generation_id"])
            self.assertEqual(
                audit["contracts_csv_sha256"],
                hashlib.sha256(csv_path.read_bytes()).hexdigest(),
            )
            fetch_seeds.validate_output_pair(csv_path, audit_path)

    def test_recover_output_transaction_rolls_back_interrupted_pair_commit(self):
        with TemporaryDirectory() as directory:
            csv_path = Path(directory) / "seeds.csv"
            audit_path = Path(directory) / "audit.json"
            csv_path.write_text("new csv", encoding="utf-8")
            audit_path.write_text("old audit", encoding="utf-8")
            csv_backup = csv_path.with_name(f".{csv_path.name}.txn.bak")
            audit_backup = audit_path.with_name(f".{audit_path.name}.txn.bak")
            csv_backup.write_text("old csv", encoding="utf-8")
            audit_backup.write_text("old audit", encoding="utf-8")
            journal = csv_path.with_name(f".{csv_path.name}.output-transaction.json")
            journal.write_text(
                json.dumps(
                    {
                        "csv_path": str(csv_path),
                        "audit_path": str(audit_path),
                        "csv_backup": str(csv_backup),
                        "audit_backup": str(audit_backup),
                        "csv_existed": True,
                        "audit_existed": True,
                    }
                ),
                encoding="utf-8",
            )

            fetch_seeds.recover_output_transaction(csv_path, audit_path)

            self.assertEqual(csv_path.read_text(encoding="utf-8"), "old csv")
            self.assertEqual(audit_path.read_text(encoding="utf-8"), "old audit")
            self.assertFalse(journal.exists())

    def test_address_normalization_lowercases_evm_but_preserves_solana(self):
        evm = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD"
        solana = "So11111111111111111111111111111111111111112"
        self.assertEqual(fetch_seeds.normalize_contract_address(evm, "base"), evm.lower())
        self.assertEqual(
            fetch_seeds.normalize_contract_address(solana, "solana"), solana
        )
        polygon_url = fetch_seeds.build_top_collections_url(
            "https://example.test/top",
            chains=["polygon"],
            page_size=100,
            cursor=None,
        )
        self.assertIn("chains=matic", polygon_url)
        pair = fetch_seeds.contract_pair_from_value(
            {"chain": "matic", "address": evm}, {"polygon"}
        )
        self.assertEqual(pair.chain, "polygon")
        self.assertEqual(pair.raw_chain, "matic")


if __name__ == "__main__":
    unittest.main()
