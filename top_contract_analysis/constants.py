from __future__ import annotations

import logging
import os
import re
import sys

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

if load_dotenv is not None:
    load_dotenv()


logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

REQUESTS_HTTP_ERROR = requests.HTTPError if requests is not None else Exception
REQUESTS_REQUEST_ERROR = requests.RequestException if requests is not None else Exception

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
DEFAULT_TIMEOUT = 60
DEFAULT_NAME_THRESHOLD = 95.0
DEFAULT_ALCHEMY_RETRIES = 3
DEFAULT_MAX_RECALL_ROWS = 0
DEFAULT_API_MAX_CONCURRENCY = 32
DEFAULT_CONTRACT_MAX_CONCURRENCY = 12
DEFAULT_SALE_METRIC_MAX_CONCURRENCY = 16

DEFAULT_NETWORKS = {
    'ethereum': 'eth-mainnet',
    'base': 'base-mainnet',
    'polygon': 'polygon-mainnet',
}

ETHERSCAN_CHAIN_IDS = {
    'ethereum': '1',
    'base': '8453',
    'polygon': '137',
}

RE_IPFS_HTTP = re.compile(
    r'https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)',
    re.IGNORECASE,
)
RE_ARWEAVE_HTTP = re.compile(
    r'https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)',
    re.IGNORECASE,
)
TRAILING_ID_PATTERNS = [
    re.compile(r'\s*#\s*[0-9a-fA-FxX]+\s*$'),
    re.compile(r'\s*#\s*\d+\s*$'),
    re.compile(r'\s*-\s*\d+\s*$'),
    re.compile(r'\s*:\s*\d+\s*$'),
    re.compile(r'\s*\(\s*\d+\s*\)\s*$'),
    re.compile(r'\s*\[\s*\d+\s*\]\s*$'),
    re.compile(r'\s*/\s*\d+\s*$'),
    re.compile(r'\s+No\.?\s*\d+\s*$', re.I),
    re.compile(r'\s+nr\.?\s*\d+\s*$', re.I),
    re.compile(r'\s+\d{1,12}\s*$'),
]
