from __future__ import annotations

from pathlib import Path

import psycopg2

from .config import Settings, load_settings


def connect(settings: Settings | None = None):
    settings = settings or load_settings()
    db = settings.db
    return psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.dbname,
        user=db.user,
        password=db.password,
        connect_timeout=db.connect_timeout,
    )


def load_sql(path: Path) -> str:
    return path.read_text(encoding='utf-8')


def ensure_schema(conn, settings: Settings | None = None) -> None:
    settings = settings or load_settings()
    with conn.cursor() as cur:
        cur.execute(load_sql(settings.sql_dir / '01_schema.sql'))
    conn.commit()
