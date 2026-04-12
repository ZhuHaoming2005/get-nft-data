import asyncio
import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch


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
        path = Path(__file__).resolve().parents[1] / "run" / "ethereum" / "metadata_backfill.py"
        spec = importlib.util.spec_from_file_location("ethereum_metadata_backfill_under_test", path)
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


backfill = _load_backfill()


class _RecordingCursor:
    def __init__(self, fetchall_result=None):
        self.fetchall_result = fetchall_result or []
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self.fetchall_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self, fetchall_result=None):
        self.cursor_obj = _RecordingCursor(fetchall_result)
        self.commit_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1

    def close(self):
        return None


class RetryAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_async_retries_before_success(self):
        attempts = []

        async def flaky():
            attempts.append(len(attempts) + 1)
            if len(attempts) < 3:
                raise RuntimeError("temporary failure")
            return "ok"

        with patch.object(backfill.asyncio, "sleep") as sleep_mock:
            result = await backfill._retry_async(
                flaky,
                operation_name="alchemy批次请求",
                chain="ethereum",
                swallow_exception=False,
            )

        self.assertEqual(result, "ok")
        self.assertEqual(attempts, [1, 2, 3])
        self.assertEqual(sleep_mock.await_count, 2)

    async def test_fetch_alchemy_batch_uses_session_level_timeout(self):
        captured = {}

        class _FakeResponse:
            def raise_for_status(self):
                return None

            async def json(self, content_type=None):
                return {"nfts": [{"raw": {"metadata": {"name": "NFT 1"}}}]}

        class _PostContext:
            async def __aenter__(self):
                return _FakeResponse()

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _FakeSession:
            def post(self, url, json=None, timeout=None):
                captured["url"] = url
                captured["json"] = json
                captured["timeout"] = timeout
                return _PostContext()

        result = await backfill.fetch_alchemy_batch(
            _FakeSession(),
            "ethereum",
            asyncio.Semaphore(1),
            [(101, "0xabc", 1, "ERC-721")],
        )

        self.assertEqual(captured["timeout"], None)
        self.assertEqual(result, [(101, {"name": "NFT 1"}, None)])


class ProcessChainRetryTests(unittest.IsolatedAsyncioTestCase):
    def test_ensure_columns_skips_alter_when_columns_already_exist(self):
        conn = _RecordingConn([("metadata",), ("retry_checked_at",)])

        backfill.ensure_columns(conn, "ethereum")

        sqls = [sql for sql, _ in conn.cursor_obj.executed]
        self.assertEqual(len(sqls), 1)
        self.assertIn("information_schema.columns", sqls[0])
        self.assertEqual(conn.commit_count, 0)

    def test_ensure_columns_creates_partial_backfill_index(self):
        conn = _RecordingConn([("metadata",), ("retry_checked_at",)])

        backfill.ensure_backfill_indexes(conn, "ethereum")

        sqls = [sql for sql, _ in conn.cursor_obj.executed]
        self.assertEqual(len(sqls), 1)
        self.assertIn("CREATE INDEX IF NOT EXISTS", sqls[0])
        self.assertIn("retry_checked_at, id", sqls[0])
        self.assertIn("WHERE metadata IS NULL", sqls[0])
        self.assertEqual(conn.commit_count, 1)

    def test_claim_segment_marks_rows_in_single_skip_locked_query(self):
        conn = _RecordingConn(
            [
                (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            ]
        )

        rows = backfill.claim_segment(conn, "ethereum", limit=25, mode="token_uri")

        sql, params = conn.cursor_obj.executed[0]
        self.assertEqual(
            rows,
            [
                (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            ],
        )
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("UPDATE nft_assets_ethereum", sql)
        self.assertIn("RETURNING claimed.claim_order", sql)
        self.assertIn("claimed.id", sql)
        self.assertIn("token_uri IS NOT NULL", sql)
        self.assertIn("retry_checked_at IS NULL OR retry_checked_at <= NOW()", sql)
        self.assertNotIn("metadata = '{}'::jsonb", sql)
        self.assertEqual(params, (25,))
        self.assertEqual(conn.commit_count, 1)

    async def test_process_chain_skips_failed_batch_and_continues(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            (2, "0xdef", 11, "ERC-721", "https://example/11", None),
        ]
        update_calls = []

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "claim_segment", side_effect=[rows, []]), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(
                     backfill,
                     "_fetch_alchemy_batch_with_retry",
                     side_effect=[
                         [],
                         [(2, {"name": "NFT 2", "image": "https://cdn.example/2.png"}, "https://cdn.example/2.png")],
                     ],
                 ), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: update_calls.append(list(batch)) or len(batch)):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch

        self.assertEqual(updated, 1)
        self.assertEqual(
            update_calls,
            [[(2, {"name": "NFT 2", "image": "https://cdn.example/2.png"}, "https://cdn.example/2.png")]],
        )

    async def test_process_chain_runs_alchemy_batches_concurrently(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            (2, "0xdef", 11, "ERC-721", "https://example/11", None),
            (3, "0xghi", 12, "ERC-721", "https://example/12", None),
        ]
        state = {"current": 0, "max": 0}

        async def _fake_fetch(*args, **kwargs):
            batch = args[3]
            state["current"] += 1
            state["max"] = max(state["max"], state["current"])
            await asyncio.sleep(0.01)
            state["current"] -= 1
            row_id = batch[0][0]
            return [
                (
                    row_id,
                    {"name": f"NFT {row_id}", "image": f"https://cdn.example/{row_id}.png"},
                    f"https://cdn.example/{row_id}.png",
                )
            ]

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        original_workers = backfill.METADATA_BACKFILL_WORKERS
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            backfill.METADATA_BACKFILL_WORKERS = 3
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "claim_segment", side_effect=[rows, []]), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(backfill, "_fetch_alchemy_batch_with_retry", side_effect=_fake_fetch), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: len(batch)):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch
            backfill.METADATA_BACKFILL_WORKERS = original_workers

        self.assertEqual(updated, 3)
        self.assertGreaterEqual(state["max"], 2)

    async def test_process_chain_uses_dedicated_api_concurrency_limit(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            (2, "0xdef", 11, "ERC-721", "https://example/11", None),
            (3, "0xghi", 12, "ERC-721", "https://example/12", None),
        ]
        state = {"current": 0, "max": 0}

        async def _fake_fetch(*args, **kwargs):
            sem = args[2]
            batch = args[3]
            async with sem:
                state["current"] += 1
                state["max"] = max(state["max"], state["current"])
                await asyncio.sleep(0.01)
                state["current"] -= 1
                row_id = batch[0][0]
                return [(row_id, {"name": f"NFT {row_id}"}, None)]

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        original_workers = backfill.METADATA_BACKFILL_WORKERS
        original_api = getattr(backfill, "METADATA_BACKFILL_API_CONCURRENCY", 0)
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            backfill.METADATA_BACKFILL_WORKERS = 10
            backfill.METADATA_BACKFILL_API_CONCURRENCY = 1
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "ensure_backfill_indexes"), \
                 patch.object(backfill, "claim_segment", side_effect=[rows, []]), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(backfill, "_fetch_alchemy_batch_with_retry", side_effect=_fake_fetch), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: len(batch)):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch
            backfill.METADATA_BACKFILL_WORKERS = original_workers
            backfill.METADATA_BACKFILL_API_CONCURRENCY = original_api

        self.assertEqual(updated, 3)
        self.assertEqual(state["max"], 1)

    async def test_process_chain_runs_multiple_segment_workers_per_chain(self):
        state = {"current": 0, "max": 0}

        async def _fake_worker(chain, worker_index, stop_event=None):
            state["current"] += 1
            state["max"] = max(state["max"], state["current"])
            await asyncio.sleep(0.01)
            state["current"] -= 1
            return worker_index

        original_workers = getattr(backfill, "METADATA_BACKFILL_CHAIN_WORKERS", 1)
        try:
            backfill.METADATA_BACKFILL_CHAIN_WORKERS = 3
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "_worker_main", side_effect=_fake_worker):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_CHAIN_WORKERS = original_workers

        self.assertEqual(updated, 6)
        self.assertGreaterEqual(state["max"], 2)

    async def test_process_chain_passes_startup_stagger_to_batches(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            (2, "0xdef", 11, "ERC-721", "https://example/11", None),
        ]
        delays = []

        async def _fake_fetch(*args, **kwargs):
            delays.append(kwargs.get("startup_delay_seconds"))
            batch = args[3]
            row_id = batch[0][0]
            return [
                (
                    row_id,
                    {"name": f"NFT {row_id}", "image": f"https://cdn.example/{row_id}.png"},
                    f"https://cdn.example/{row_id}.png",
                )
            ]

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        original_stagger = backfill.REQUEST_STARTUP_STAGGER_SECONDS
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            backfill.REQUEST_STARTUP_STAGGER_SECONDS = 0.5
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "claim_segment", side_effect=[rows, []]), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(backfill, "_fetch_alchemy_batch_with_retry", side_effect=_fake_fetch), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: len(batch)):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch
            backfill.REQUEST_STARTUP_STAGGER_SECONDS = original_stagger

        self.assertEqual(updated, 2)
        self.assertEqual(delays, [0.0, 0.5])

    async def test_process_chain_updates_only_successful_rows(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
            (2, "0xdef", 11, "ERC-721", "https://example/11", None),
        ]
        update_calls = []

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "claim_segment", side_effect=[rows, []]), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(
                     backfill,
                     "_fetch_alchemy_batch_with_retry",
                     side_effect=[
                         [],
                         [(2, {"name": "NFT 2", "image": "https://cdn.example/2.png"}, "https://cdn.example/2.png")],
                     ],
                 ), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: update_calls.append(list(batch)) or len(batch)):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch

        self.assertEqual(updated, 1)
        self.assertEqual(
            update_calls,
            [[(2, {"name": "NFT 2", "image": "https://cdn.example/2.png"}, "https://cdn.example/2.png")]],
        )

    async def test_process_chain_marks_failed_rows_checked_without_writing_placeholder(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
        ]
        update_calls = []
        checked_calls = []

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "ensure_backfill_indexes"), \
                 patch.object(backfill, "claim_segment", side_effect=[rows, []]), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(
                     backfill,
                     "_fetch_alchemy_batch_with_retry",
                     return_value=[(1, None, None)],
                 ), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: update_calls.append(list(batch)) or len(batch)), \
                 patch.object(backfill, "mark_rows_checked", side_effect=lambda conn, chain, ids: checked_calls.append(list(ids))):
                updated = await backfill._process_chain("ethereum")
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch

        self.assertEqual(updated, 0)
        self.assertEqual(update_calls, [])
        self.assertEqual(checked_calls, [[1]])

    async def test_process_chain_stops_before_fetching_next_segment_when_stop_requested(self):
        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        rows = [
            (1, "0xabc", 10, "ERC-721", "https://example/10", None),
        ]
        stop_event = threading.Event()
        fetch_segment_calls = []

        def _fake_claim_segment(conn, chain, *, limit, mode):
            fetch_segment_calls.append((chain, limit, mode))
            return rows if len(fetch_segment_calls) == 1 else [
                (2, "0xdef", 11, "ERC-721", "https://example/11", None),
            ]

        async def _fake_fetch(*args, **kwargs):
            stop_event.set()
            return [(1, {"name": "NFT 1"}, "https://cdn.example/1.png")]

        original_mode = backfill.METADATA_BACKFILL_MODE
        original_batch = backfill.METADATA_BACKFILL_BATCH_SIZE
        try:
            backfill.METADATA_BACKFILL_MODE = "alchemy"
            backfill.METADATA_BACKFILL_BATCH_SIZE = 1
            with patch.object(backfill, "_conn", side_effect=lambda: _RecordingConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "claim_segment", side_effect=_fake_claim_segment), \
                 patch.object(backfill.aiohttp, "ClientSession", return_value=_FakeSession()), \
                 patch.object(backfill, "_fetch_alchemy_batch_with_retry", side_effect=_fake_fetch), \
                 patch.object(backfill, "bulk_update_rows", side_effect=lambda conn, chain, batch: len(batch)):
                updated = await backfill._process_chain("ethereum", stop_event=stop_event)
        finally:
            backfill.METADATA_BACKFILL_MODE = original_mode
            backfill.METADATA_BACKFILL_BATCH_SIZE = original_batch

        self.assertEqual(updated, 1)
        self.assertEqual(len(fetch_segment_calls), 1)


class MainRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_amain_continues_to_next_chain_after_chain_failure(self):
        original_chains = backfill.METADATA_BACKFILL_CHAINS
        try:
            backfill.METADATA_BACKFILL_CHAINS = ["ethereum", "base"]
            with patch.object(backfill, "_process_chain", side_effect=[RuntimeError("chain failed"), 2]):
                await backfill._amain()
        finally:
            backfill.METADATA_BACKFILL_CHAINS = original_chains

    async def test_amain_processes_multiple_chains_concurrently(self):
        original_chains = backfill.METADATA_BACKFILL_CHAINS
        original_chain_concurrency = backfill.METADATA_BACKFILL_CHAIN_CONCURRENCY
        state = {"current": 0, "max": 0}

        async def _fake_process_chain(chain, stop_event=None):
            state["current"] += 1
            state["max"] = max(state["max"], state["current"])
            await asyncio.sleep(0.01)
            state["current"] -= 1
            return len(chain)

        try:
            backfill.METADATA_BACKFILL_CHAINS = ["ethereum", "base", "polygon"]
            backfill.METADATA_BACKFILL_CHAIN_CONCURRENCY = 3
            with patch.object(backfill, "_process_chain", side_effect=_fake_process_chain):
                await backfill._amain()
        finally:
            backfill.METADATA_BACKFILL_CHAINS = original_chains
            backfill.METADATA_BACKFILL_CHAIN_CONCURRENCY = original_chain_concurrency

        self.assertGreaterEqual(state["max"], 2)


if __name__ == "__main__":
    unittest.main()
