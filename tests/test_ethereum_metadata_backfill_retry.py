import asyncio
import importlib.util
import sys
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


class ProcessChainRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_chain_skips_failed_batch_and_continues(self):
        class _FakeConn:
            def close(self):
                return None

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
            with patch.object(backfill, "_conn", return_value=_FakeConn()), \
                 patch.object(backfill, "ensure_columns"), \
                 patch.object(backfill, "fetch_segment", side_effect=[rows, []]), \
                 patch.object(backfill, "mark_rows_checked"), \
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


class MainRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_amain_continues_to_next_chain_after_chain_failure(self):
        original_chains = backfill.METADATA_BACKFILL_CHAINS
        try:
            backfill.METADATA_BACKFILL_CHAINS = ["ethereum", "base"]
            with patch.object(backfill, "_process_chain", side_effect=[RuntimeError("chain failed"), 2]):
                await backfill._amain()
        finally:
            backfill.METADATA_BACKFILL_CHAINS = original_chains


if __name__ == "__main__":
    unittest.main()
