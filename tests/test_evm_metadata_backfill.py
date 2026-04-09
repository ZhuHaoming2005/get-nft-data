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


def _load_backfill():
    aiohttp = types.ModuleType("aiohttp")

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    class _ClientSession:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    aiohttp.ClientTimeout = _ClientTimeout
    aiohttp.TCPConnector = _TCPConnector
    aiohttp.ClientSession = _ClientSession

    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.SimpleNamespace(connection=object, cursor=object)
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None

    injected = {
        "aiohttp": aiohttp,
        "psycopg2": psycopg2,
        "psycopg2.extras": psycopg2.extras,
        "dotenv": dotenv,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "run" / "evm" / "metadata_backfill.py"
        spec = importlib.util.spec_from_file_location("evm_metadata_backfill_under_test", path)
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


class _MetadataHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        token = self.path.rsplit("/", 1)[-1]
        body = json.dumps(
            {
                "image": f"https://cdn.example/{token}.png",
                "name": f"NFT {token}",
                "attributes": [{"trait_type": "token", "value": token}],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


class _FakeGetContext:
    def __init__(self, url, headers=None):
        self.url = url
        self.headers = headers or {}
        self.response = None

    async def __aenter__(self):
        def _send():
            req = request.Request(self.url, headers=self.headers, method="GET")
            with request.urlopen(req, timeout=5) as resp:
                return _FakeHttpResponse(resp.status, json.loads(resp.read()))

        self.response = await asyncio.to_thread(_send)
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeMetadataSession:
    def get(self, url, headers=None, allow_redirects=True):
        return _FakeGetContext(url, headers=headers)


class _RecordingCursor:
    def __init__(self, fetchall_result=None):
        self.fetchall_result = fetchall_result or []
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self.fetchall_result)

    def __iter__(self):
        return iter(self.fetchall_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self, fetchall_result=None):
        self.cursor_obj = _RecordingCursor(fetchall_result)
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


backfill = _load_backfill()


class EvmMetadataBackfillAsyncTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _MetadataHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    async def test_fetch_alchemy_batch_returns_metadata_and_image(self):
        result = await backfill.fetch_alchemy_batch(
            _FakeAlchemySession(
                [
                    {
                        "raw": {
                            "metadata": {
                                "name": "NFT 1",
                                "image": "https://cdn.example/1.png",
                            }
                        }
                    }
                ]
            ),
            "polygon",
            asyncio.Semaphore(1),
            [(101, "0xabc", 1, "ERC-721")],
        )

        self.assertEqual(
            result,
            [
                (
                    101,
                    {"name": "NFT 1", "image": "https://cdn.example/1.png"},
                    "https://cdn.example/1.png",
                )
            ],
        )

    async def test_fetch_token_uri_batch_returns_metadata_and_image(self):
        token_uri = f"{self.base_url}/metadata/7"

        result = await backfill.fetch_token_uri_metadata_batch(
            _FakeMetadataSession(),
            asyncio.Semaphore(1),
            [(7, token_uri)],
        )

        self.assertEqual(
            result,
            [
                (
                    7,
                    {
                        "image": "https://cdn.example/7.png",
                        "name": "NFT 7",
                        "attributes": [{"trait_type": "token", "value": "7"}],
                    },
                    "https://cdn.example/7.png",
                )
            ],
        )


class EvmMetadataBackfillDbTests(unittest.TestCase):
    def test_fetch_segment_selects_only_missing_metadata(self):
        conn = _RecordingConn(
            [
                (1, "0xabc", 10, "ERC-721", "ipfs://token/10", None),
            ]
        )

        rows = backfill.fetch_segment(conn, "polygon", limit=50, mode="token_uri")

        sql, params = conn.cursor_obj.executed[0]
        self.assertEqual(
            rows,
            [
                (1, "0xabc", 10, "ERC-721", "ipfs://token/10", None),
            ],
        )
        self.assertIn("metadata IS NULL OR metadata = '{}'::jsonb", sql)
        self.assertIn("token_uri IS NOT NULL", sql)
        self.assertIn("ORDER BY retry_checked_at ASC NULLS FIRST, id ASC", sql)
        self.assertEqual(params, (50,))

    def test_bulk_update_rows_updates_metadata_image_and_retry_checked(self):
        conn = _RecordingConn()
        calls = []

        def _fake_execute_values(cur, sql, page, template=None, page_size=None):
            calls.append((sql, list(page), template, page_size))
            cur.rowcount = len(page)

        original = backfill.psycopg2.extras.execute_values
        backfill.psycopg2.extras.execute_values = _fake_execute_values
        try:
            updated = backfill.bulk_update_rows(
                conn,
                "polygon",
                [
                    (
                        11,
                        {"name": "NFT 11", "image": "https://cdn.example/11.png"},
                        "https://cdn.example/11.png",
                    )
                ],
            )
        finally:
            backfill.psycopg2.extras.execute_values = original

        self.assertEqual(updated, 1)
        self.assertEqual(len(calls), 1)
        sql, page, template, page_size = calls[0]
        self.assertIn("SET metadata = v.metadata::jsonb", sql)
        self.assertIn("image_uri = COALESCE(t.image_uri, v.image_uri)", sql)
        self.assertIn("retry_checked_at = NOW()", sql)
        self.assertEqual(page[0][0], 11)
        self.assertEqual(page[0][1], json.dumps({"name": "NFT 11", "image": "https://cdn.example/11.png"}, ensure_ascii=False))
        self.assertEqual(page[0][2], "https://cdn.example/11.png")
        self.assertEqual(template, "(%s, %s, %s)")
        self.assertEqual(page_size, 1)


class EvmMetadataBackfillLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_chain_once_flushes_each_token_uri_batch(self):
        conn = _RecordingConn()
        update_calls = []

        async def _fake_fetch(session, sem, rows):
            row_id, token_uri = rows[0]
            return [(row_id, {"name": f"NFT {row_id}", "image": f"https://cdn.example/{row_id}.png"}, f"https://cdn.example/{row_id}.png")]

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        original_fetch = backfill.fetch_token_uri_metadata_batch
        original_update = backfill.bulk_update_rows
        try:
            backfill.METADATA_BACKFILL_MODE = "token_uri"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            backfill.fetch_token_uri_metadata_batch = _fake_fetch
            backfill.bulk_update_rows = lambda inner_conn, chain, rows: update_calls.append(list(rows)) or len(rows)

            batch_size, updated = await backfill._process_chain_once(
                conn,
                object(),
                asyncio.Semaphore(1),
                "polygon",
                [
                    (1, "0xabc", 10, "ERC-721", "https://example/10", None),
                    (2, "0xdef", 11, "ERC-721", "https://example/11", None),
                ],
            )
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch
            backfill.fetch_token_uri_metadata_batch = original_fetch
            backfill.bulk_update_rows = original_update

        self.assertEqual(batch_size, 2)
        self.assertEqual(updated, 2)
        self.assertEqual(
            update_calls,
            [
                [(1, {"name": "NFT 1", "image": "https://cdn.example/1.png"}, "https://cdn.example/1.png")],
                [(2, {"name": "NFT 2", "image": "https://cdn.example/2.png"}, "https://cdn.example/2.png")],
            ],
        )

    async def test_run_loop_waits_when_no_rows_are_available(self):
        sleeps = []

        async def _fake_sleep(seconds):
            sleeps.append(seconds)

        original_conn = backfill._conn
        original_fetch_segment = backfill.fetch_segment
        original_ensure_columns = backfill.ensure_columns
        try:
            backfill._conn = lambda: _RecordingConn()
            backfill.fetch_segment = lambda conn, chain, limit, mode: []
            backfill.ensure_columns = lambda conn, chain: None

            updated = await backfill._process_chain_loop(
                "polygon",
                sleep_fn=_fake_sleep,
                max_idle_rounds=1,
            )
        finally:
            backfill._conn = original_conn
            backfill.fetch_segment = original_fetch_segment
            backfill.ensure_columns = original_ensure_columns

        self.assertEqual(updated, 0)
        self.assertEqual(sleeps, [backfill.METADATA_BACKFILL_IDLE_WAIT])


if __name__ == "__main__":
    unittest.main()
