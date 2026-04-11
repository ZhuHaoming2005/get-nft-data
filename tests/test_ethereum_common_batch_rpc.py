import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _load_ethereum_common():
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.SimpleNamespace(connection=object)
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None

    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp.ClientSession = _ClientSession
    aiohttp.TCPConnector = _TCPConnector
    aiohttp.ClientTimeout = _ClientTimeout

    web3 = types.ModuleType("web3")

    class _AsyncWeb3:
        @staticmethod
        def to_checksum_address(value):
            return value

    web3.AsyncWeb3 = _AsyncWeb3

    providers = types.ModuleType("web3.providers")

    class _AsyncHTTPProvider:
        def __init__(self, *args, **kwargs):
            pass

    providers.AsyncHTTPProvider = _AsyncHTTPProvider

    injected = {
        "psycopg2": psycopg2,
        "psycopg2.extras": psycopg2.extras,
        "dotenv": dotenv,
        "aiohttp": aiohttp,
        "web3": web3,
        "web3.providers": providers,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "run" / "ethereum" / "common.py"
        spec = importlib.util.spec_from_file_location("ethereum_common_under_test", path)
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


class _FakeHttpResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, content_type=None):
        return self._payload


class _FakeAlchemyPostContext:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return _FakeHttpResponse(200, self.payload)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeAlchemySession:
    def __init__(self, payload):
        self.payload = payload

    def post(self, url, json=None, timeout=None):
        return _FakeAlchemyPostContext(self.payload)


class EthereumCommonBatchRpcTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_ethereum_common()

    async def test_fetch_alchemy_batch_normalizes_nested_image_object(self):
        payload = [
            {
                "contract": {"name": "Collection A", "symbol": "COLA"},
                "image": {"originalUrl": "https://cdn.example/top-level.png"},
                "raw": {
                    "tokenUri": "ipfs://token/1",
                    "metadata": {
                        "name": "NFT #1",
                        "image": {"originalUrl": "https://cdn.example/raw.png"},
                    },
                },
            }
        ]

        result = await self.common.fetch_alchemy_batch(
            _FakeAlchemySession(payload),
            asyncio.Semaphore(1),
            [("0xabc", 1, "ERC-721")],
        )

        self.assertEqual(
            result,
            [
                (
                    "ipfs://token/1",
                    "https://cdn.example/raw.png",
                    "Collection A",
                    "COLA",
                    {
                        "name": "NFT #1",
                        "image": {"originalUrl": "https://cdn.example/raw.png"},
                    },
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
