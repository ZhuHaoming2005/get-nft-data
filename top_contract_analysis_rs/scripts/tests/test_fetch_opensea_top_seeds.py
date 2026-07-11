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

    def test_parse_args_does_not_embed_an_opensea_api_key(self):
        args = fetch_seeds.parse_args([])

        self.assertEqual(args.api_key, "")

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
