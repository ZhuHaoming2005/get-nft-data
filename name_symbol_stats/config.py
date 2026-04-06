from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CHAINS = ('ethereum', 'base', 'polygon', 'solana')
DEFAULT_THRESHOLDS = (85.0, 90.0, 95.0)
DEFAULT_RUN_LABEL = 'default'


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    connect_timeout: int = 10


@dataclass(frozen=True)
class Settings:
    db: DbConfig
    root_dir: Path
    sql_dir: Path
    default_run_label: str = DEFAULT_RUN_LABEL


def load_settings() -> Settings:
    root_dir = Path(__file__).resolve().parent
    return Settings(
        db=DbConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            dbname=os.getenv('DB_NAME', 'nft_data'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASS', '123456'),
            connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
        ),
        root_dir=root_dir,
        sql_dir=root_dir / 'sql',
    )


def normalize_run_label(run_label: str | None) -> str:
    value = (run_label or DEFAULT_RUN_LABEL).strip()
    if not value:
        return DEFAULT_RUN_LABEL
    safe = re.sub(r'[^0-9A-Za-z_.-]+', '-', value)
    return safe[:120]


def chain_to_table(chain: str) -> str:
    safe = re.sub(r'[^a-z0-9_]', '', chain.lower().strip())
    if not safe:
        raise ValueError(f'invalid chain: {chain!r}')
    return f'nft_assets_{safe}'
