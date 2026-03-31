#!/usr/bin/env python3
"""
NFT image_uri 重试脚本：对 image_uri 为空的记录重新拉取 metadata 并更新。

支持可选链：默认 base，可通过 --chain 指定（如 --chain eth）。
表名：nft_assets_{chain}，与 nft_collector 一致。
"""

import argparse
import logging
import re
import sys
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras
import requests
import time

# ─── 日志配置 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # logging.FileHandler("retry.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# 数据库连接配置 iic12345!
DB_HOST = "localhost"  # "pgm-2zevls2414y7mw6d8o.pg.rds.aliyuncs.com"
DB_PORT = 5432
DB_NAME = "nft_data"
DB_USER = "postgres" # "user1"
DB_PASS = "123456" # "_JC!y7XWygm$94f"

# 链名称，对应表 nft_assets_{chain}
CHAIN_NAME = "polygon"

# metadata 请求超时（秒）
METADATA_TIMEOUT = 15
METADATA_CONNECT_TIMEOUT = 15

# IPFS 网关列表（轮询重试顺序）
IPFS_GATEWAYS: List[str] = [
    "https://gateway.pinata.cloud/ipfs",
    "https://dweb.link/ipfs",
    "https://ipfs.io/ipfs",
    # "https://pink-official-shrimp-252.mypinata.cloud/ipfs",
]

# pink-official-shrimp 网关专用请求头
IPFS_GATEWAY_HEADERS: Dict[str, Dict[str, str]] = {
    "https://pink-official-shrimp-252.mypinata.cloud/ipfs": {
        "x-pinata-gateway-token": "P2Z1YGDiTOuEjgDKJdhn41CmcbGe9HNn0BMYTZFSWe8J-9NwPEpyPauxPaSlI_i0",
    },
}

# Arweave 网关
ARWEAVE_GATEWAY = "https://arweave.net"

# 每批处理多少条 image_uri 为空的记录
BATCH_SIZE = 500


def _nft_table_name(chain_name: str) -> str:
    """根据 chain_name 生成表名，仅允许 [a-z0-9_] 防止注入。"""
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower())
    return f"nft_assets_{safe}" if safe else "nft_assets_default"


# ────────────────────────────────────────────────────────────────────────────────
# metadata & image_uri 解析
# ────────────────────────────────────────────────────────────────────────────────

def _metadata_url(token_uri: str, ipfs_gateway: Optional[str] = None) -> str:
    """
    将 token_uri 转为可 HTTP GET 的 URL。
    - ipfs://...  通过指定的 IPFS 网关转发
    - ar://...    通过 ARWEAVE_GATEWAY 转发
    - 其他        原样返回
    """
    s = token_uri.strip()
    if s.startswith("ipfs://ipfs/"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[12:].lstrip("/")
    if s.startswith("ipfs://"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[7:].lstrip("/")
    if s.startswith("ar://"):
        gw = ARWEAVE_GATEWAY.rstrip("/")
        return gw + "/" + s[5:].lstrip("/")
    return s


def fetch_image_uri_for_token_uri(token_uri: str) -> Optional[str]:
    """
    对单个 token_uri 重新拉取 metadata，并从 JSON 中解析 image/image_url 字段。
    - 对 ipfs:// 链接按 IPFS_GATEWAYS 列表顺序轮询重试；
    - 对其他协议仅尝试一次（直连）。
    """
    if not token_uri or token_uri.startswith("data:application/"):
        return None

    s = token_uri.strip()
    if s.startswith("ipfs://"):
        gateways: List[Optional[str]] = IPFS_GATEWAYS
    else:
        gateways = [None]

    last_exc: Optional[Exception] = None
    for idx, gw in enumerate(gateways):
        url = _metadata_url(token_uri, gw)
        if not url.startswith("http"):
            continue

        headers: Dict[str, str] = {}
        if gw:
            gw_base = gw.rstrip("/")
            if gw_base in IPFS_GATEWAY_HEADERS and url.startswith(gw_base):
                headers.update(IPFS_GATEWAY_HEADERS[gw_base])

        try:
            resp = requests.get(
                url,
                headers=headers or None,
                timeout=(METADATA_CONNECT_TIMEOUT, METADATA_TIMEOUT),
                allow_redirects=True,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            last_exc = exc
            logger.info(
                "[retry image_uri=NULL] token_uri=%s uri=%s gateway[%d]=%s 原因: HTTP 请求失败 [%s] %s",
                token_uri[:80],
                url,
                idx,
                (gw or "<direct>"),
                type(exc).__name__,
                str(exc) or repr(exc),
            )
            # 失败时尝试下一个网关
            continue

        if not isinstance(data, dict):
            logger.info(
                "[retry image_uri=NULL] token_uri=%s gateway[%d]=%s 原因: metadata 不是 JSON 对象",
                token_uri[:80],
                idx,
                (gw or "<direct>"),
            )
            # 换网关意义不大，但实现上继续尝试
            continue

        image = data.get("image") or data.get("image_url")
        if not image or not isinstance(image, str):
            logger.info(
                "[retry image_uri=NULL] token_uri=%s gateway[%d]=%s 原因: JSON 中缺少 image/image_url",
                token_uri[:100],
                idx,
                (gw or "<direct>"),
            )
            return None

        image = image.strip()
        return image

    if last_exc is not None:
        logger.debug(
            "metadata 重试最终失败 token_uri=%s，最后错误: [%s] %s",
            token_uri[:80],
            type(last_exc).__name__,
            str(last_exc) or repr(last_exc),
        )
    return None


# ────────────────────────────────────────────────────────────────────────────────
# 数据库处理
# ────────────────────────────────────────────────────────────────────────────────

def ensure_retry_checked_column(conn, chain_name: str) -> None:
    """确保表存在 retry_checked_at 列，用于轮询时优先选择最久未重试的记录。"""
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE {tbl}
            ADD COLUMN IF NOT EXISTS retry_checked_at TIMESTAMPTZ
            """
        )
    conn.commit()
    logger.info("已确保 %s.retry_checked_at 列存在", tbl)


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=10,
    )


def fetch_missing_image_rows(
    cur: psycopg2.extensions.cursor, chain_name: str, limit: int
) -> list:
    """
    拉取一批 image_uri 为空、且 token_uri 非空的记录。
    按 retry_checked_at 升序（NULL 优先），优先选择最久未重试的 token_uri。
    token_uri 重复时只取一个，返回 [(token_uri,), ...]。
    """
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        SELECT token_uri
        FROM (
            SELECT token_uri, MIN(retry_checked_at) AS oldest
            FROM {tbl}
            WHERE image_uri IS NULL
              AND token_uri IS NOT NULL
              AND (
                  token_uri LIKE 'ipfs://%%'
                  OR token_uri LIKE 'ar://%%'
                  OR token_uri LIKE 'http%%'
              )
            GROUP BY token_uri
            ORDER BY oldest ASC NULLS FIRST
            LIMIT %s
        ) sub
        """,
        (limit,),
    )
    return cur.fetchall()


def mark_retry_checked(
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    chain_name: str,
    token_uris: list,
) -> None:
    """
    将本批选中的 token_uri 的 retry_checked_at 更新为 NOW()，
    避免下次 SELECT 仍优先选中这些（可能不可达）的记录。
    """
    if not token_uris:
        return
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        UPDATE {tbl}
        SET retry_checked_at = NOW()
        WHERE image_uri IS NULL
          AND token_uri = ANY(%s)
        """,
        ([r["token_uri"] for r in token_uris],),
    )
    conn.commit()


def update_image_uri_by_token_uri(
    cur: psycopg2.extensions.cursor,
    token_uri: str,
    image_uri: str,
    conn: psycopg2.extensions.connection,
    chain_name: str,
) -> int:
    """
    按 token_uri 更新所有匹配行的 image_uri，返回实际更新的行数。
    """
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        UPDATE {tbl}
        SET image_uri = %s
        WHERE image_uri IS NULL
          AND token_uri = %s
        """,
        (image_uri, token_uri),
    )
    conn.commit()
    return cur.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="NFT image_uri 重试脚本")
    parser.add_argument(
        "--chain",
        default=CHAIN_NAME,
        help="链名称，对应表 nft_assets_{chain} (默认: %(default)s)",
    )
    args = parser.parse_args()
    chain_name = args.chain

    logger.info("═══ NFT image_uri 重试脚本启动 ═══  链: %s | 表: %s", chain_name, _nft_table_name(chain_name))

    conn = get_conn()
    ensure_retry_checked_column(conn, chain_name)
    conn.close()

    while True:

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        total_processed = 0
        total_updated = 0

        try:
            while True:

                time.sleep(10)

                fail_count = 0

                rows = fetch_missing_image_rows(cur, chain_name, BATCH_SIZE)
                if not rows:
                    time.sleep(600)
                    continue

                mark_retry_checked(cur, conn, chain_name, rows)

                logger.info(
                    "本批待重试 token_uri 数: %d（累计处理 %d 个，已更新 %d 条记录）",
                    len(rows),
                    total_processed,
                    total_updated,
                )

                batch_updated = 0

                for row in rows:
                    token_uri = row["token_uri"]

                    total_processed += 1

                    image_uri = fetch_image_uri_for_token_uri(token_uri)
                    if not image_uri:
                        fail_count += 1

                        # if fail_count >= 5:
                        #     logger.warning(
                        #         "连续 %d 个 token_uri 重试失败，暂停 10 分钟后继续",
                        #         fail_count,
                        #     )
                        #     time.sleep(600)
                        #     fail_count = 0

                        continue

                    updated_count = update_image_uri_by_token_uri(cur, token_uri, image_uri, conn, chain_name)
                    fail_count = 0
                    batch_updated += updated_count
                    total_updated += updated_count

                    logger.info(
                        "更新成功 token_uri=%s 共 %d 条 image_uri=%s",
                        token_uri[:80],
                        updated_count,
                        image_uri[:80],
                    )

                logger.info(
                    "本批处理完成：成功更新 %d 条，累计更新 %d 条",
                    batch_updated,
                    total_updated,
                )
        finally:
            cur.close()
            conn.close()
            logger.info("数据库连接已关闭")


if __name__ == "__main__":
    main()

