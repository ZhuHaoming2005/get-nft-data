import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_module():
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.Error = Exception
    psycopg2.OperationalError = Exception
    psycopg2.InterfaceError = Exception
    psycopg2.connect = lambda **kwargs: object()
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: False

    injected = {
        "psycopg2": psycopg2,
        "psycopg2.extras": psycopg2.extras,
        "dotenv": dotenv,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "run" / "ethereum" / "backfill_image_uri_from_metadata.py"
        spec = importlib.util.spec_from_file_location("ethereum_backfill_under_test", path)
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


backfill = _load_module()


class RetryCallTests(unittest.TestCase):
    def test_retry_call_retries_before_success(self):
        attempts = []

        def flaky():
            attempts.append(len(attempts) + 1)
            if len(attempts) < 3:
                raise RuntimeError("temporary failure")
            return "ok"

        with patch.object(backfill.time, "sleep") as sleep_mock:
            result = backfill._retry_call(
                flaky,
                operation_name="读取批次",
                chain="ethereum",
                swallow_exception=False,
            )

        self.assertEqual(result, "ok")
        self.assertEqual(attempts, [1, 2, 3])
        self.assertEqual(sleep_mock.call_count, 2)


class ProcessChainRetryTests(unittest.TestCase):
    def test_process_chain_skips_failed_update_batch_and_continues(self):
        batch_one = [(1, "0x1:1", {"image": "ipfs://image-1"})]
        batch_two = [(2, "0x1:2", {"image": "ipfs://image-2"})]

        with patch.object(backfill, "_has_columns", return_value=True), \
             patch.object(backfill, "_fetch_batch_with_retry", side_effect=[batch_one, batch_two, []]), \
             patch.object(backfill, "_bulk_update_with_retry", side_effect=[RuntimeError("write failed"), 1]):
            scanned, extracted, updated = backfill._process_chain(
                conn=object(),
                chain="ethereum",
                batch_size=100,
                dry_run=False,
                limit=None,
            )

        self.assertEqual((scanned, extracted, updated), (2, 2, 1))


class MainRetryTests(unittest.TestCase):
    def test_main_continues_to_next_chain_after_chain_failure(self):
        class _Args:
            chains = ["ethereum", "base"]
            batch_size = 100
            limit = None
            dry_run = False

        class _Conn:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        conns = [_Conn(), _Conn()]

        with patch.object(backfill.argparse.ArgumentParser, "parse_args", return_value=_Args()), \
             patch.object(backfill, "_connect_with_retry", side_effect=conns), \
             patch.object(backfill, "_process_chain", side_effect=[RuntimeError("chain failed"), (3, 2, 2)]):
            backfill.main()

        self.assertTrue(all(conn.closed for conn in conns))


if __name__ == "__main__":
    unittest.main()
