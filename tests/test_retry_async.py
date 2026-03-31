import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import retry


class _MetadataHandler(BaseHTTPRequestHandler):
    response_delay = 0.25

    def do_GET(self):
        time.sleep(self.response_delay)
        token = self.path.rsplit('/', 1)[-1]
        body = json.dumps({'image': f'https://cdn.example/{token}.png'}).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


class RetryAsyncFetchTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), _MetadataHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base_url = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    async def test_fetch_image_uris_concurrently(self):
        token_uris = [f'{self.base_url}/metadata/{idx}' for idx in range(3)]

        start = time.perf_counter()
        result = await retry.fetch_image_uris_for_token_uris(token_uris, concurrency=3)
        elapsed = time.perf_counter() - start

        self.assertEqual(
            result,
            {
                token_uri: f'https://cdn.example/{idx}.png'
                for idx, token_uri in enumerate(token_uris)
            },
        )
        self.assertLess(elapsed, 0.55)


if __name__ == '__main__':
    unittest.main()
