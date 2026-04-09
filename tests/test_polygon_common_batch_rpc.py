import asyncio
import importlib.util
import json
import sys
import threading
import time
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import request


def _load_polygon_common():
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
        path = Path(__file__).resolve().parents[1] / "run" / "polygon" / "common.py"
        spec = importlib.util.spec_from_file_location("polygon_common_under_test", path)
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


def _encode_abi_string(value: str) -> str:
    payload = value.encode("utf-8")
    padded = payload + (b"\x00" * ((32 - len(payload) % 32) % 32))
    encoded = (32).to_bytes(32, "big") + len(payload).to_bytes(32, "big") + padded
    return "0x" + encoded.hex()


class _BatchRpcHandler(BaseHTTPRequestHandler):
    request_delay = 0.2
    request_count = 0

    def do_POST(self):
        type(self).request_count += 1
        time.sleep(self.request_delay)

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        payload = json.loads(body)

        responses = []
        for item in payload:
            token_id = int(item["params"][0]["data"][-64:], 16)
            if item["params"][0]["data"].startswith("0xc87b56dd"):
                uri = f"https://erc721.example/{token_id}"
            else:
                uri = f"https://erc1155.example/{token_id}"
            responses.append(
                {"jsonrpc": "2.0", "id": item["id"], "result": _encode_abi_string(uri)}
            )

        encoded = json.dumps(responses).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        return


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
    def __init__(self, url: str, payload):
        self.url = url
        self.payload = payload
        self.response = None

    async def __aenter__(self):
        def _send():
            body = json.dumps(self.payload).encode("utf-8")
            req = request.Request(
                self.url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=5) as resp:
                return _FakeHttpResponse(resp.status, json.loads(resp.read()))

        self.response = await asyncio.to_thread(_send)
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeClientSession:
    def post(self, url, json=None, timeout=None):
        return _FakePostContext(url, json)


class PolygonCommonBatchRpcTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_polygon_common()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _BatchRpcHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.rpc_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    async def test_fetch_token_uri_batch_uses_single_batch_request(self):
        _BatchRpcHandler.request_count = 0
        tokens = [
            ("0xabc", 1, "ERC-721"),
            ("0xdef", 2, "ERC-1155"),
            ("0xghi", 3, "ERC-721"),
        ]

        session = _FakeClientSession()
        start = time.perf_counter()
        result = await self.common.fetch_token_uri_batch(
            session,
            self.rpc_url,
            asyncio.Semaphore(4),
            tokens,
        )
        elapsed = time.perf_counter() - start

        self.assertEqual(
            result,
            [
                "https://erc721.example/1",
                "https://erc1155.example/2",
                "https://erc721.example/3",
            ],
        )
        self.assertEqual(_BatchRpcHandler.request_count, 1)
        self.assertLess(elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()
