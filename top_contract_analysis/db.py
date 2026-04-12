from __future__ import annotations

import os
import re


def chain_to_table(chain: str) -> str:
    safe = re.sub(r'[^a-z0-9_]', '', chain.lower().strip())
    if not safe:
        raise ValueError(f'illegal chain name: {chain!r}')
    return f'nft_assets_{safe}'


def get_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'nft_data'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASS', ''),
        connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
    )
