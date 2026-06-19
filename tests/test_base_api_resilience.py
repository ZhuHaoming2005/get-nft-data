import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _encode_abi_string(value: str) -> str:
    payload = value.encode("utf-8")
    padded = payload + (b"\x00" * ((32 - len(payload) % 32) % 32))
    encoded = (32).to_bytes(32, "big") + len(payload).to_bytes(32, "big") + padded
    return "0x" + encoded.hex()


def _install_base_common_stubs():
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
    return original


def _restore_modules(original):
    for name, value in original.items():
        if value is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = value


def _load_base_common():
    original = _install_base_common_stubs()
    try:
        path = Path(__file__).resolve().parents[1] / "run" / "base" / "common.py"
        spec = importlib.util.spec_from_file_location("base_common_resilience_under_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        _restore_modules(original)


def _load_base_fetcher():
    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp.ClientSession = _ClientSession
    aiohttp.TCPConnector = _TCPConnector

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

    common = types.ModuleType("common")
    common.CHAIN_NAME = "base"
    common.RPC_URL = "https://rpc.example"
    common.ALCHEMY_BATCH_SIZE = 2
    common.CONCURRENT_ALCHEMY = 5
    common.CONCURRENT_RPC = 10
    common.RPC_BATCH_SIZE = 2
    common.FETCH_IDLE_WAIT = 30
    common.FETCH_CLAIM_BATCH_SIZE = 100
    common.CLAIM_RETRY_AFTER_SECONDS = 600
    common.REQUEST_STARTUP_STAGGER_SECONDS = 0.0
    common.logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )
    common.get_conn = lambda: None
    common.init_db = lambda conn, chain_name: None
    common.claim_pending_nfts = lambda *args, **kwargs: []
    common.batch_insert_main = lambda *args, **kwargs: 0
    common.delete_temp_nfts = lambda *args, **kwargs: 0
    common.release_temp_claims = lambda *args, **kwargs: 0
    common.delete_contract_nfts = lambda *args, **kwargs: (0, 0)
    common.append_blacklist_env = lambda *args, **kwargs: None
    common.load_blacklist = lambda *args, **kwargs: set()
    common.fetch_alchemy_batch = None
    common.fetch_token_uri_batch = None
    common.fetch_token_uri = None
    common._decode_inline_image = lambda *args, **kwargs: None
    common.replace_token_id_placeholder = lambda uri, tid: uri
    common.fix_token_id_placeholders = lambda conn, chain_name: 0

    injected = {
        "aiohttp": aiohttp,
        "web3": web3,
        "web3.providers": providers,
        "common": common,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "run" / "base" / "metadata_fetcher.py"
        spec = importlib.util.spec_from_file_location("base_metadata_fetcher_limits_under_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        _restore_modules(original)


class _FakeHttpResponse:
    def __init__(self, status: int, payload, headers=None):
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, content_type=None):
        return self._payload


class _PostContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _AlwaysFailingTopicSession:
    def __init__(self, failing_topic: str):
        self.failing_topic = failing_topic
        self.calls = []

    def post(self, url, json=None, timeout=None):
        topic = json["params"][0]["topics"][0]
        self.calls.append(topic)
        if topic == self.failing_topic:
            return _PostContext(_FakeHttpResponse(500, {"error": {"message": "busy"}}))
        return _PostContext(_FakeHttpResponse(200, {"result": []}))


class _FailingPostSession:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    def post(self, url, json=None, timeout=None):
        self.calls += 1
        return _PostContext(self.response)


class _RpcBatchSession:
    def __init__(self, result_uri: str):
        self.result_uri = result_uri

    def post(self, url, json=None, timeout=None):
        return _PostContext(
            _FakeHttpResponse(
                200,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 0,
                        "result": _encode_abi_string(self.result_uri),
                    }
                ],
            )
        )


class _Call:
    def __init__(self, value: str):
        self.value = value

    async def call(self):
        return self.value


class _FakeFunctions:
    def uri(self, token_id):
        return _Call("ipfs://metadata/{id}.json")


class _FakeContract:
    functions = _FakeFunctions()


class _FakeEth:
    def contract(self, address=None, abi=None):
        return _FakeContract()


class _FakeW3:
    eth = _FakeEth()


class _FailingCall:
    async def call(self):
        raise RuntimeError("temporary web3 failure")


class _FailingFunctions:
    def tokenURI(self, token_id):
        return _FailingCall()


class _FailingContract:
    functions = _FailingFunctions()


class _FailingEth:
    def contract(self, address=None, abi=None):
        return _FailingContract()


class _FailingW3:
    eth = _FailingEth()


class BaseApiResilienceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_base_common()

    async def asyncSetUp(self):
        self.sleep_delays = []

        async def _fake_sleep(delay):
            self.sleep_delays.append(delay)

        self.sleep_patch = patch.object(self.common.asyncio, "sleep", side_effect=_fake_sleep)
        self.sleep_patch.start()

    async def asyncTearDown(self):
        self.sleep_patch.stop()

    async def test_fetch_logs_http_raises_when_one_topic_request_keeps_failing(self):
        session = _AlwaysFailingTopicSession(self.common.ERC721_TRANSFER_TOPIC)
        error_type = getattr(self.common, "ApiRequestError", RuntimeError)

        with self.assertRaises(error_type):
            await self.common.fetch_logs_http(
                session,
                "https://rpc.example",
                10,
                11,
            )

    async def test_fetch_alchemy_batch_raises_after_request_failures_and_uses_retry_after(self):
        session = _FailingPostSession(
            _FakeHttpResponse(429, {"error": "limited"}, headers={"Retry-After": "7"})
        )
        error_type = getattr(self.common, "ApiRequestError", RuntimeError)

        with self.assertRaises(error_type):
            await self.common.fetch_alchemy_batch(
                session,
                asyncio.Semaphore(1),
                [("0xabc", 1, "ERC-721")],
            )

        self.assertIn(7.0, self.sleep_delays)

    async def test_fetch_token_uri_batch_raises_after_request_failures(self):
        session = _FailingPostSession(_FakeHttpResponse(503, {"error": "busy"}))
        error_type = getattr(self.common, "ApiRequestError", RuntimeError)

        with self.assertRaises(error_type):
            await self.common.fetch_token_uri_batch(
                session,
                "https://rpc.example",
                asyncio.Semaphore(1),
                [("0xabc", 1, "ERC-721")],
            )

    async def test_fetch_token_uri_batch_expands_erc1155_placeholder_as_64_hex(self):
        result = await self.common.fetch_token_uri_batch(
            _RpcBatchSession("ipfs://metadata/{id}.json"),
            "https://rpc.example",
            asyncio.Semaphore(1),
            [("0xabc", 15, "ERC-1155")],
        )

        self.assertEqual(
            result,
            ["ipfs://metadata/000000000000000000000000000000000000000000000000000000000000000f.json"],
        )

    async def test_fetch_token_uri_expands_erc1155_placeholder_as_64_hex(self):
        result = await self.common.fetch_token_uri(
            _FakeW3(),
            asyncio.Semaphore(1),
            "0xabc",
            15,
            "ERC-1155",
        )

        self.assertEqual(
            result,
            "ipfs://metadata/000000000000000000000000000000000000000000000000000000000000000f.json",
        )

    async def test_fetch_token_uri_raises_after_web3_call_failure(self):
        error_type = getattr(self.common, "ApiRequestError", RuntimeError)

        with self.assertRaises(error_type):
            await self.common.fetch_token_uri(
                _FailingW3(),
                asyncio.Semaphore(1),
                "0xabc",
                15,
                "ERC-721",
            )


class BaseMetadataFetcherLimitTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.fetcher = _load_base_fetcher()

    def test_worker_concurrency_limit_distributes_global_limit_across_workers(self):
        limits = [
            self.fetcher._worker_concurrency_limit(5, worker_index, 2)
            for worker_index in (1, 2)
        ]

        self.assertEqual(limits, [3, 2])
        self.assertEqual(sum(limits), 5)

    def test_worker_concurrency_limit_returns_zero_for_surplus_workers(self):
        limits = [
            self.fetcher._worker_concurrency_limit(2, worker_index, 5)
            for worker_index in range(1, 6)
        ]

        self.assertEqual(limits, [1, 1, 0, 0, 0])
        self.assertEqual(sum(limits), 2)

    async def test_process_batch_waits_for_all_alchemy_chunks_before_raising(self):
        completed = []
        second_started = asyncio.Event()
        original_batch_size = self.fetcher.ALCHEMY_BATCH_SIZE
        self.fetcher.ALCHEMY_BATCH_SIZE = 1

        async def _fake_fetch_alchemy_batch(_session, _sem, chunk, **_kwargs):
            if chunk[0][0] == "0xfail":
                await second_started.wait()
                raise RuntimeError("first chunk failed")
            second_started.set()
            await asyncio.sleep(0.05)
            completed.append(chunk[0][0])
            return [("ipfs://token/ok", None, None, None, None)]

        pending = [
            (1, "0xfail", 1, "ERC-721", 100),
            (2, "0xslow", 2, "ERC-721", 101),
        ]

        try:
            with patch.object(self.fetcher, "fetch_alchemy_batch", side_effect=_fake_fetch_alchemy_batch):
                with self.assertRaises(RuntimeError):
                    await self.fetcher._process_batch(
                        object(),
                        object(),
                        asyncio.Semaphore(2),
                        asyncio.Semaphore(2),
                        pending,
                    )
        finally:
            self.fetcher.ALCHEMY_BATCH_SIZE = original_batch_size

        self.assertEqual(completed, ["0xslow"])


if __name__ == "__main__":
    unittest.main()
