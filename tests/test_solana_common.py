import asyncio
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path


def _load_solana_common():
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.SimpleNamespace(connection=object)

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None

    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            pass

    class _ClientResponseError(Exception):
        def __init__(self, *args, status=None, **kwargs):
            super().__init__(status)
            self.status = status

    aiohttp.ClientSession = _ClientSession
    aiohttp.ClientTimeout = _ClientTimeout
    aiohttp.ClientResponseError = _ClientResponseError

    injected = {
        "psycopg2": psycopg2,
        "dotenv": dotenv,
        "aiohttp": aiohttp,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "run" / "solana" / "common.py"
        spec = importlib.util.spec_from_file_location("solana_common_under_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for name, value in original.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


class _RecordingCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "INSERT INTO" in sql:
            self.rowcount = 1

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self):
        self.cursor_obj = _RecordingCursor()
        self.commit_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


class _FakeHttpResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, content_type=None):
        return self._payload


class _FakePostContext:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return _FakeHttpResponse(200, self.payload)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload

    def post(self, url, json=None, timeout=None):
        return _FakePostContext(self.payload)


class SolanaCommonDbFormatTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_solana_common()

    def test_init_db_creates_evm_compatible_asset_shape(self):
        conn = _RecordingConn()

        self.common.init_db(conn, "solana")

        all_sql = "\n".join(sql for sql, _params in conn.cursor_obj.executed)
        self.assertIn("token_id         NUMERIC", all_sql)
        self.assertIn("metadata         JSONB", all_sql)
        self.assertIn("UNIQUE (contract_address, token_id)", all_sql)
        self.assertIn("ADD CONSTRAINT nft_assets_solana_contract_token_key", all_sql)
        self.assertIn("ADD CONSTRAINT temp_solana_contract_token_key", all_sql)
        self.assertNotIn("CREATE UNIQUE INDEX IF NOT EXISTS idx_sol_nft_contract_token", all_sql)
        self.assertNotIn("CREATE UNIQUE INDEX IF NOT EXISTS idx_sol_temp_contract_token", all_sql)

    def test_batch_insert_main_serializes_metadata_and_conflicts_on_contract_token(self):
        conn = _RecordingConn()
        metadata = {"name": "NFT\x00 #1", "attributes": [{"trait_type": "rank\x00"}]}

        inserted = self.common.batch_insert_main(
            conn,
            "solana",
            [
                (
                    "Mint111111111111111111111111111111111111111",
                    1,
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "NFT\x00 #1",
                    "SYM",
                    metadata,
                    "Metaplex",
                    123456,
                )
            ],
        )

        self.assertEqual(inserted, 1)
        insert_sql, params = conn.cursor_obj.executed[-1]
        self.assertIn("metadata", insert_sql)
        self.assertIn("%s::jsonb", insert_sql)
        self.assertIn("ON CONFLICT (contract_address, token_id)", insert_sql)
        self.assertEqual(params[4], "NFT #1")
        self.assertEqual(json.loads(params[6]), {"name": "NFT #1", "attributes": [{"trait_type": "rank"}]})


class SolanaHeliusMetadataTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_solana_common()

    async def test_fetch_helius_metadata_batch_returns_raw_metadata_payload(self):
        self.common.HELIUS_API_KEY = "test-key"
        payload = {
            "result": [
                {
                    "id": "Mint111",
                    "content": {
                        "json_uri": "ipfs://token/1",
                        "links": {"image": "https://image.example/1.png"},
                        "metadata": {
                            "name": "Collection #1",
                            "symbol": "COL",
                            "attributes": [{"trait_type": "rank", "value": "1"}],
                        },
                    },
                }
            ]
        }

        result = await self.common.fetch_helius_metadata_batch(
            _FakeSession(payload),
            asyncio.Semaphore(1),
            ["Mint111"],
        )

        self.assertEqual(
            result,
            [
                (
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Collection #1",
                    "COL",
                    {
                        "name": "Collection #1",
                        "symbol": "COL",
                        "attributes": [{"trait_type": "rank", "value": "1"}],
                    },
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
