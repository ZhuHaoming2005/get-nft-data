#!/usr/bin/env python3
"""
Solana NFT 数据采集 - 共享模块

适配 Metaplex Token Metadata Program 标准（即 Solana 主流 NFT 标准）。
由 tx_scanner.py 和 metadata_fetcher.py 共同引用。

NFT 发现策略（tx_scanner.py）：
  Helius getProgramAccountsV2(Token Metadata Program)
  → 直接枚举全链 Metaplex MetadataV1 账户 → mint 地址

元数据获取策略（metadata_fetcher.py）：
  1. 优先：Helius DAS API getAssetBatch（单批 1000 mint）
  2. 回退：链上 getMultipleAccounts → borsh 解码 → HTTP 拉取 JSON
"""

import asyncio
import base64
import json as _json
import logging
import os
import re
import struct
import sys
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ─── 日志配置 ──────────────────────────────────────────────────────────────────
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── Solana 程序 ID ────────────────────────────────────────────────────────────
TOKEN_METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
SPL_TOKEN_PROGRAM_ID      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SYSTEM_PROGRAM_ID         = "11111111111111111111111111111111"

# ─── 配置（优先读取 .env）──────────────────────────────────────────────────────
CHAIN_NAME = os.getenv("CHAIN_NAME", "solana")
RPC_URL    = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

# Helius DAS API（getAssetBatch，替代 Alchemy 单条查询）
HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY", "")
# 单次 getAssetBatch 携带的 mint 数量（Helius 上限 1000）
HELIUS_BATCH_SIZE = int(os.getenv("HELIUS_BATCH_SIZE", "1000"))

METADATA_TIMEOUT         = int(os.getenv("METADATA_TIMEOUT", "30"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "30"))

# 同时在途的 getAssetBatch HTTP 请求数
# 5 并发 × 1000 mint/请求 = 每轮 5000 条，远超 Alchemy 单条并发模式
CONCURRENT_HELIUS = int(os.getenv("CONCURRENT_HELIUS", "5"))
CONCURRENT_RPC    = int(os.getenv("CONCURRENT_RPC", "10"))
CONCURRENT_IMAGE  = int(os.getenv("CONCURRENT_IMAGE", "50"))
FETCH_IDLE_WAIT   = int(os.getenv("FETCH_IDLE_WAIT", "30"))

# ── getProgramAccountsV2 配置 ──────────────────────────────────────────────────
# Helius 专属 RPC URL（getProgramAccountsV2 是 Helius 私有方法，需要 Helius 节点）
HELIUS_RPC_URL = (
    f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY', '')}"
    if os.getenv("HELIUS_API_KEY") else ""
)
# 每页拉取的账户数（1~10000，建议 1000；节点不稳定时可调低至 500）
GPA_PAGE_SIZE  = int(os.getenv("GPA_PAGE_SIZE", "1000"))
# 增量模式：只拉取在此 Slot 之后发生变化的账户（0 = 全量扫描）
# 首次运行保持 0；扫描完成后由脚本自动写入上次完成时的最新 Slot
GPA_SINCE_SLOT = int(os.getenv("GPA_SINCE_SLOT", "0"))


# ══════════════════════════════════════════════════════════════════════════════
# Base58 编解码（纯 Python，无外部依赖）
# ══════════════════════════════════════════════════════════════════════════════

_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_MAP = {b: i for i, b in enumerate(_B58_ALPHABET)}


def b58decode(s: str) -> bytes:
    """Base58 解码，返回原始字节。"""
    n = 0
    for c in s.encode():
        n = n * 58 + _B58_MAP[c]
    result = []
    while n > 0:
        result.append(n & 0xFF)
        n >>= 8
    pad = len(s) - len(s.lstrip("1"))
    return bytes(pad) + bytes(reversed(result))


def b58encode(data: bytes) -> str:
    """Base58 编码。"""
    n = int.from_bytes(data, "big")
    result = []
    while n > 0:
        n, r = divmod(n, 58)
        result.append(_B58_ALPHABET[r])
    pad = len(data) - len(data.lstrip(b"\x00"))
    return "1" * pad + bytes(reversed(result)).decode()


# ══════════════════════════════════════════════════════════════════════════════
# Solana PDA（Program Derived Address）计算
# 无需外部库，使用纯 Python 实现 Ed25519 曲线点验证
# ══════════════════════════════════════════════════════════════════════════════

import hashlib

# Ed25519 椭圆曲线参数（用于判断点是否在曲线上）
_P  = 2**255 - 19
_A  = -121665 * pow(121666, _P - 2, _P) % _P
_D  = -121665 * pow(121666, _P - 2, _P) % _P  # noqa: F841
_D2 = (2 * (-121665 * pow(121666, _P - 2, _P) % _P)) % _P  # noqa: F841


def _is_on_curve(point_bytes: bytes) -> bool:
    """
    判断 32 字节是否为 Ed25519 曲线上的有效点。
    用于 find_program_address 的 bump 枚举。
    """
    if len(point_bytes) != 32:
        return False
    # 解压 y 坐标
    b = bytearray(point_bytes)
    x_sign = (b[-1] & 0x80) != 0
    b[-1] &= 0x7F
    y = int.from_bytes(b, "little")
    if y >= _P:
        return False
    # 计算 x^2 = (y^2 - 1) / (d*y^2 + 1) mod p
    y2 = y * y % _P
    x2 = (y2 - 1) * pow(_A * y2 + 1, _P - 2, _P) % _P
    if x2 == 0:
        return x_sign is False
    # 候选 x
    x = pow(x2, (_P + 3) // 8, _P)
    if pow(x, 2, _P) != x2 % _P:
        # 修正
        modp_sqrt_m1 = pow(2, (_P - 1) // 4, _P)
        x = x * modp_sqrt_m1 % _P
    if pow(x, 2, _P) != x2 % _P:
        return False
    return True


def find_program_address(seeds: List[bytes], program_id_bytes: bytes) -> Tuple[bytes, int]:
    """
    计算 Solana PDA（Program Derived Address）。
    返回 (pda_bytes_32, bump_seed)。
    Solana 官方算法：SHA256(seeds... || bump || program_id || "ProgramDerivedAddress")，
    找到第一个使结果不在 Ed25519 曲线上的 bump（从 255 向下枚举）。
    """
    for nonce in range(255, -1, -1):
        hash_input = b"".join(seeds) + bytes([nonce]) + program_id_bytes + b"ProgramDerivedAddress"
        h = hashlib.sha256(hash_input).digest()
        if not _is_on_curve(h):
            return h, nonce
    raise ValueError("Could not find program address")


def get_metadata_pda(mint_address: str) -> str:
    """
    根据 mint 地址计算 Metaplex Token Metadata PDA。
    PDA seeds: ["metadata", TOKEN_METADATA_PROGRAM_ID, mint]
    """
    program_id_bytes = b58decode(TOKEN_METADATA_PROGRAM_ID)
    mint_bytes       = b58decode(mint_address)
    pda_bytes, _     = find_program_address(
        [b"metadata", program_id_bytes, mint_bytes],
        program_id_bytes,
    )
    return b58encode(pda_bytes)


# ══════════════════════════════════════════════════════════════════════════════
# Metaplex Metadata Account borsh 解码
# ══════════════════════════════════════════════════════════════════════════════

def _read_string(data: bytes, pos: int) -> Tuple[str, int]:
    """读取 borsh 字符串（u32 长度前缀 + UTF-8 内容）。"""
    if pos + 4 > len(data):
        return "", pos + 4
    length = struct.unpack_from("<I", data, pos)[0]
    pos += 4
    raw = data[pos: pos + length]
    pos += length
    return raw.decode("utf-8", errors="replace").rstrip("\x00").strip(), pos


def parse_metadata_account(data: bytes) -> Optional[Dict]:
    """
    解码 Metaplex Token Metadata Program 的 metadata 账户数据（borsh 格式）。

    账户布局（v2.x / Metaplex Token Metadata Standard）：
      1  byte  : key（4 = MetadataV1）
      32 bytes : update_authority
      32 bytes : mint
      4+n bytes: name（最长 32 字节内容）
      4+n bytes: symbol（最长 10 字节内容）
      4+n bytes: uri（最长 200 字节内容）
      2  bytes : seller_fee_basis_points
      1  byte  : creators option flag（0/1）
      ...后续字段忽略

    返回包含 name / symbol / uri 的字典，解析失败返回 None。
    """
    try:
        if not data or len(data) < 70:
            return None
        pos = 0

        key = data[pos]; pos += 1
        if key != 4:  # 仅接受 MetadataV1
            return None

        pos += 32  # skip update_authority
        mint_bytes = data[pos: pos + 32]; pos += 32
        mint_b58 = b58encode(bytes(mint_bytes))

        name,   pos = _read_string(data, pos)
        symbol, pos = _read_string(data, pos)
        uri,    pos = _read_string(data, pos)

        return {"mint": mint_b58, "name": name, "symbol": symbol, "uri": uri}
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 数据库工具
# ══════════════════════════════════════════════════════════════════════════════

def _nft_table_name(chain_name: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"nft_assets_{safe}"


def _temp_table_name(chain_name: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"temp_{safe}"


def _ensure_contract_token_unique_constraint_sql(
    table_name: str,
    constraint_name: str,
) -> str:
    return f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = current_schema()
                      AND t.relname = '{table_name}'
                      AND c.contype = 'u'
                      AND (
                          SELECT array_agg(a.attname::text ORDER BY k.ord)
                          FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                          JOIN pg_attribute a
                            ON a.attrelid = c.conrelid
                           AND a.attnum = k.attnum
                      ) = ARRAY['contract_address', 'token_id']
                ) THEN
                    ALTER TABLE {table_name}
                        ADD CONSTRAINT {constraint_name}
                        UNIQUE (contract_address, token_id);
                END IF;
            END $$;
        """


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10,
    )


def init_db(conn, chain_name: str) -> None:
    """
    建表（幂等）：
      - nft_assets_{chain}    : 主表，存储已获取 token_uri 的有效 NFT
      - temp_{chain}          : 临时表，扫描阶段写入，由 metadata_fetcher 消费
      - scan_progress_gpa     : GPA 扫描进度（分页游标 + 增量 slot）
    """
    tbl = _nft_table_name(chain_name)
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        # 主表：contract_address 存 mint address（base58，最长 44 字符）
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(44)  NOT NULL,
                token_id         NUMERIC      NOT NULL DEFAULT 1,
                token_uri        TEXT,
                image_uri        TEXT,
                name             TEXT,
                symbol           TEXT,
                metadata         JSONB,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                CONSTRAINT {tbl}_contract_token_key UNIQUE (contract_address, token_id)
            )
        """)
        # 兼容已有旧表：迁移到与 EVM 主表一致的数据列语义。
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS name TEXT")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS symbol TEXT")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS metadata JSONB")
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN name TYPE TEXT")
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN symbol TYPE TEXT")
        cur.execute(
            f"ALTER TABLE {tbl} ALTER COLUMN token_id TYPE NUMERIC USING token_id::numeric"
        )
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN token_id SET DEFAULT 1")
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN token_standard TYPE VARCHAR(10)")
        cur.execute(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS {tbl}_contract_address_key")
        cur.execute("DROP INDEX IF EXISTS idx_sol_nft_contract_token")
        cur.execute(
            _ensure_contract_token_unique_constraint_sql(
                tbl,
                f"{tbl}_contract_token_key",
            )
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sol_nft_contract"
            f" ON {tbl} (contract_address)"
        )
        # 临时表
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tmp} (
                id               BIGSERIAL   PRIMARY KEY,
                contract_address VARCHAR(44) NOT NULL,
                token_id         NUMERIC     NOT NULL DEFAULT 1,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT {tmp}_contract_token_key UNIQUE (contract_address, token_id)
            )
        """)
        cur.execute(
            f"ALTER TABLE {tmp} ALTER COLUMN token_id TYPE NUMERIC USING token_id::numeric"
        )
        cur.execute(f"ALTER TABLE {tmp} ALTER COLUMN token_id SET DEFAULT 1")
        cur.execute(f"ALTER TABLE {tmp} ALTER COLUMN token_standard TYPE VARCHAR(10)")
        cur.execute(f"ALTER TABLE {tmp} DROP CONSTRAINT IF EXISTS {tmp}_contract_address_key")
        cur.execute("DROP INDEX IF EXISTS idx_sol_temp_contract_token")
        cur.execute(
            _ensure_contract_token_unique_constraint_sql(
                tmp,
                f"{tmp}_contract_token_key",
            )
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sol_temp_contract"
            f" ON {tmp} (contract_address)"
        )
        # GPA 扫描进度表（getProgramAccountsV2 游标分页断点续扫）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress_gpa (
                chain_name      VARCHAR(50)  PRIMARY KEY,
                pagination_key  TEXT,
                since_slot      BIGINT       NOT NULL DEFAULT 0,
                total_pages     BIGINT       NOT NULL DEFAULT 0,
                updated_at      TIMESTAMPTZ  DEFAULT NOW()
            )
        """)
    conn.commit()
    logger.info("数据库表初始化完成: 主表=%s  临时表=%s", tbl, tmp)


def get_gpa_progress(conn, chain_name: str) -> Tuple[Optional[str], int, int]:
    """
    读取 GPA 扫描进度。
    返回 (pagination_key, since_slot, total_pages)：
      pagination_key : 上次中断时的游标（None = 从头开始或已全部扫完）
      since_slot     : 上次全量扫描完成时的链上 Slot（0 = 从未完成过全量扫描）
      total_pages    : 历史累计分页数（仅供日志参考）
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pagination_key, since_slot, total_pages"
            " FROM scan_progress_gpa WHERE chain_name = %s",
            (chain_name,),
        )
        row = cur.fetchone()
    conn.commit()
    if row:
        return row[0], row[1] or 0, row[2] or 0
    return None, 0, 0


def save_gpa_progress(
    conn,
    chain_name: str,
    pagination_key: Optional[str],
    since_slot: int,
    total_pages: int,
) -> None:
    """
    保存 GPA 扫描进度。
      pagination_key : 当前页游标（None 表示本次全量已扫完）
      since_slot     : 全量扫完后记录的链上最新 Slot，供下次增量扫描使用
      total_pages    : 已处理页数（累加）
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_progress_gpa
                (chain_name, pagination_key, since_slot, total_pages)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chain_name) DO UPDATE
                SET pagination_key = EXCLUDED.pagination_key,
                    since_slot     = EXCLUDED.since_slot,
                    total_pages    = EXCLUDED.total_pages,
                    updated_at     = NOW()
            """,
            (chain_name, pagination_key, since_slot, total_pages),
        )
    conn.commit()


def batch_insert_temp(conn, chain_name: str, records: List[Tuple]) -> int:
    """
    tx_scanner 专用：批量写入扫描原始记录到临时表。
    records: [(mint_address, 1, token_standard, slot), ...]
    """
    if not records:
        return 0
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(records))
        flat = [item for rec in records for item in rec]
        cur.execute(
            f"""
            INSERT INTO {tmp} (contract_address, token_id, token_standard, first_seen_block)
            VALUES {placeholders}
            ON CONFLICT (contract_address, token_id) DO NOTHING
            """,
            flat,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def load_pending_nfts(conn, chain_name: str, limit: int = 5000) -> List[Tuple]:
    """
    metadata_fetcher 专用：从临时表读取待处理记录。
    返回 [(id, mint_address, token_id, token_standard, first_seen_slot), ...]

    limit 建议设为 HELIUS_BATCH_SIZE × CONCURRENT_HELIUS × 若干倍，
    确保单次读取能覆盖多个完整的 getAssetBatch 请求。
    """
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, contract_address, token_id, token_standard, first_seen_block
            FROM {tmp}
            ORDER BY id
            LIMIT %s
            """,
            (limit,),
        )
        result = [(row[0], row[1], int(row[2]), row[3], row[4]) for row in cur.fetchall()]
    conn.commit()
    return result


def batch_insert_main(conn, chain_name: str, records: List[Tuple]) -> int:
    """
    metadata_fetcher 专用：将有效 NFT 写入主表。
    records: [
        (
            mint, 1, token_uri, image_uri,
            name, symbol, metadata, token_standard, first_seen_slot,
        ),
        ...
    ]
    """
    if not records:
        return 0
    tbl = _nft_table_name(chain_name)

    def _clean(v: Any) -> Any:
        if isinstance(v, str):
            v = v.replace("\x00", "")
            # 过滤掉无效的 Unicode 代理字符（\uD800-\uDFFF），PostgreSQL/UTF-8 不允许
            return v.encode("utf-8", errors="ignore").decode("utf-8")
        if isinstance(v, dict):
            return {_clean(k): _clean(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_clean(item) for item in v]
        if isinstance(v, tuple):
            return tuple(_clean(item) for item in v)
        return v

    normalized_records = []
    for rec in records:
        cleaned = tuple(_clean(v) for v in rec)
        if len(cleaned) == 8:
            cleaned = cleaned[:6] + (None,) + cleaned[6:]
        metadata = cleaned[6]
        if metadata is not None and not isinstance(metadata, str):
            metadata = _json.dumps(metadata, ensure_ascii=False)
        normalized_records.append(cleaned[:6] + (metadata,) + cleaned[7:])

    with conn.cursor() as cur:
        placeholders = ", ".join(
            ["(%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)"] * len(normalized_records)
        )
        flat = [item for rec in normalized_records for item in rec]
        cur.execute(
            f"""
            INSERT INTO {tbl}
                (contract_address, token_id, token_uri, image_uri,
                 name, symbol, metadata, token_standard, first_seen_block)
            VALUES {placeholders}
            ON CONFLICT (contract_address, token_id) DO UPDATE SET
                token_uri        = COALESCE(EXCLUDED.token_uri,  {tbl}.token_uri),
                image_uri        = COALESCE(EXCLUDED.image_uri,  {tbl}.image_uri),
                name             = COALESCE(EXCLUDED.name,       {tbl}.name),
                symbol           = COALESCE(EXCLUDED.symbol,     {tbl}.symbol),
                metadata         = COALESCE(EXCLUDED.metadata,   {tbl}.metadata),
                token_standard   = COALESCE(EXCLUDED.token_standard, {tbl}.token_standard)
            """,
            flat,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def delete_temp_nfts(conn, chain_name: str, ids: List[int]) -> int:
    if not ids:
        return 0
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tmp} WHERE id = ANY(%s)", (ids,))
        deleted = cur.rowcount
    conn.commit()
    return deleted


# ══════════════════════════════════════════════════════════════════════════════
# Solana RPC 工具
# ══════════════════════════════════════════════════════════════════════════════

async def get_latest_slot(
    session: aiohttp.ClientSession,
    rpc_url: str,
) -> int:
    """获取链上最新已确认 Slot 号。"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "finalized"}],
    }
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with session.post(rpc_url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            body = await resp.json(content_type=None)
            return int(body.get("result", 0))
    except Exception as exc:
        logger.error("获取最新 Slot 失败: %s", exc)
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# 元数据获取：Helius DAS API（getAssetBatch）+ 链上 getMultipleAccounts 回退
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_helius_metadata_batch(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    mints: List[str],
) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
    """
    通过 Helius DAS API（getAssetBatch）批量获取 NFT 元数据。

    与 Alchemy 逐条查询的核心差异：
      - 单次请求最多携带 HELIUS_BATCH_SIZE=1000 个 mint
      - CONCURRENT_HELIUS=5 并发 × 1000 mint = 每轮 5000 条，效率提升 1000 倍
      - 直接返回 content.json_uri（token_uri）和图片 URL，无需再拉取 JSON
      - 同时支持普通 NFT（V1_NFT）、可编程 NFT（PROGRAMMABLE_NFT）和压缩 NFT（cNFT）

    Helius DAS API 文档：
      https://docs.helius.dev/compression-and-das-api/digital-asset-standard-das-api/get-assets
    """
    if not HELIUS_API_KEY:
        return [(None, None, None, None, None)] * len(mints)

    helius_url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT, connect=METADATA_CONNECT_TIMEOUT
    )

    async def _fetch_chunk(
        ci: int, chunk: List[str]
    ) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
        payload = {
            "jsonrpc": "2.0",
            "id": f"helius-batch-{ci}",
            "method": "getAssetBatch",
            "params": {
                "ids": chunk,
                "options": {
                    "showUnverifiedCollections": True,
                    "showCollectionMetadata": True
                },
            },
        }
        max_retries = 4
        body: Optional[Dict] = None

        for attempt in range(1, max_retries + 1):
            try:
                # sem 只包住单次 HTTP 调用，重试 sleep 期间释放
                async with sem:
                    async with session.post(
                        helius_url, json=payload, timeout=timeout
                    ) as resp:
                        if resp.status == 429:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=429
                            )
                        resp.raise_for_status()
                        body = await resp.json(content_type=None)
                break
            except aiohttp.ClientResponseError as exc:
                wait = min(2 ** attempt, 32) if exc.status == 429 else 2 ** (attempt - 1)
                if attempt < max_retries:
                    logger.debug(
                        "Helius getAssetBatch 第%d/%d次失败（HTTP %d），%.0fs后重试",
                        attempt, max_retries, exc.status, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.warning("Helius getAssetBatch 全部重试失败（%d mint）", len(chunk))
                    return [(None, None, None, None, None)] * len(chunk)
            except Exception as exc:
                if attempt < max_retries:
                    await asyncio.sleep(2 ** (attempt - 1))
                else:
                    logger.warning("Helius getAssetBatch 请求失败: %s", exc)
                    return [(None, None, None, None, None)] * len(chunk)

        if body is None:
            return [(None, None, None, None, None)] * len(chunk)

        assets = body.get("result") or []

        # 构建 mint → (token_uri, image_url, name, symbol, metadata) 映射
        # getAssetBatch 结果顺序不保证与输入一致，用 id 字段对齐
        asset_map: Dict[
            str,
            Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]],
        ] = {}
        for asset in (assets if isinstance(assets, list) else []):
            mint = asset.get("id")
            if not mint:
                continue
            
            content   = asset.get("content") or {}
            token_uri = content.get("json_uri") or None
            meta_raw  = content.get("metadata")
            meta      = meta_raw if isinstance(meta_raw, dict) else {}

            # name / symbol 直接来自 Helius DAS metadata 字段（链上权威值）
            name:   Optional[str] = meta.get("name")   or None
            symbol: Optional[str] = meta.get("symbol") or None

            # 图片 URL 按优先级：links.image > metadata.image > files 中第一个图片文件
            image_url: Optional[str] = None
            links = content.get("links") or {}
            image_url = links.get("image") or None

            if not image_url:
                image_url = meta.get("image") or None

            if not image_url:
                for f in (content.get("files") or []):
                    if isinstance(f, dict) and (f.get("mime") or "").startswith("image/"):
                        image_url = f.get("uri") or f.get("cdn_uri") or None
                        break

            asset_map[mint] = (
                token_uri or None,
                image_url or None,
                name,
                symbol,
                meta_raw if isinstance(meta_raw, dict) else None,
            )

        return [asset_map.get(m, (None, None, None, None, None)) for m in chunk]

    # 按 HELIUS_BATCH_SIZE 分块后全部并发，sem 在 _fetch_chunk 内控流
    chunk_size = HELIUS_BATCH_SIZE
    chunks = [mints[i: i + chunk_size] for i in range(0, len(mints), chunk_size)]
    chunk_results = await asyncio.gather(*[
        _fetch_chunk(ci, chunk) for ci, chunk in enumerate(chunks)
    ])
    return [item for chunk in chunk_results for item in chunk]


async def fetch_onchain_metadata_batch(
    session: aiohttp.ClientSession,
    rpc_sem: asyncio.Semaphore,
    image_sem: asyncio.Semaphore,
    mints: List[str],
) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
    """
    链上元数据批量获取（getMultipleAccounts + borsh 解码 + HTTP 拉取 JSON）。

    关键优化：
      1. PDA 计算（CPU密集）放到线程池，不阻塞事件循环
      2. 每个 100-mint chunk 独立持有 rpc_sem，而非整个函数锁定信号量
      3. 图片 HTTP 请求由 image_sem 限流，避免同时发出几千个请求

    返回 [(token_uri, image_url, name, symbol, metadata), ...] 与 mints 同序。
    """
    if not mints:
        return []

    # ── 步骤1：PDA 计算（CPU密集，放入线程池）────────────────────────────────
    def _compute_pdas() -> List[Optional[str]]:
        result = []
        for mint in mints:
            try:
                result.append(get_metadata_pda(mint))
            except Exception:
                result.append(None)
        return result

    pda_list: List[Optional[str]] = await asyncio.to_thread(_compute_pdas)

    # ── 步骤2：并发 getMultipleAccounts（per-chunk，sem 只包住 HTTP 调用）────
    chunk_size  = 100
    chunks      = [pda_list[i: i + chunk_size] for i in range(0, len(pda_list),  chunk_size)]
    mint_chunks = [mints[i: i + chunk_size]     for i in range(0, len(mints),     chunk_size)]
    # uri_map[mint] = (uri, name, symbol)  ← 三元组，name/symbol 来自链上 borsh 解码
    uri_map: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}
    rpc_timeout = aiohttp.ClientTimeout(total=30)

    async def _fetch_chunk(ci: int, chunk: List[Optional[str]], mint_chunk: List[str]) -> None:
        valid_pdas = [p for p in chunk if p]
        if not valid_pdas:
            for m in mint_chunk:
                uri_map[m] = (None, None, None)
            return

        payload = {
            "jsonrpc": "2.0",
            "id": ci,
            "method": "getMultipleAccounts",
            "params": [valid_pdas, {"encoding": "base64", "commitment": "finalized"}],
        }
        body: Optional[Dict] = None
        for attempt in range(1, 4):
            try:
                async with rpc_sem:
                    async with session.post(RPC_URL, json=payload, timeout=rpc_timeout) as resp:
                        if resp.status == 429:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=429
                            )
                        resp.raise_for_status()
                        body = await resp.json(content_type=None)
                break
            except Exception:
                if attempt < 3:
                    await asyncio.sleep(2 ** (attempt - 1))

        accs = ((body or {}).get("result") or {}).get("value") or []
        pda_idx = 0
        for mint, pda in zip(mint_chunk, chunk):
            if pda is None:
                uri_map[mint] = (None, None, None)
                continue
            acc = accs[pda_idx] if pda_idx < len(accs) else None
            pda_idx += 1
            if not acc or not acc.get("data"):
                uri_map[mint] = (None, None, None)
                continue
            try:
                raw_data = base64.b64decode(acc["data"][0])
                parsed   = parse_metadata_account(raw_data)
                if parsed:
                    uri_map[mint] = (
                        parsed.get("uri")    or None,
                        parsed.get("name")   or None,
                        parsed.get("symbol") or None,
                    )
                else:
                    uri_map[mint] = (None, None, None)
            except Exception:
                uri_map[mint] = (None, None, None)

    await asyncio.gather(*[
        _fetch_chunk(ci, chunk, mint_chunk)
        for ci, (chunk, mint_chunk) in enumerate(zip(chunks, mint_chunks))
    ])

    # ── 步骤3：并发 HTTP 拉取 metadata JSON → 提取 image_url（image_sem 限流）
    http_timeout = aiohttp.ClientTimeout(total=METADATA_TIMEOUT)

    async def _fetch_image(
        mint: str,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]:
        entry = uri_map.get(mint, (None, None, None))
        uri, on_name, on_symbol = entry
        if not uri:
            return None, None, on_name, on_symbol, None
        async with image_sem:
            try:
                async with session.get(uri, timeout=http_timeout, allow_redirects=True) as resp:
                    if resp.status != 200:
                        return uri, None, on_name, on_symbol, None
                    ct = resp.headers.get("Content-Type", "")
                    if "json" not in ct and not uri.endswith(".json"):
                        return uri, None, on_name, on_symbol, None
                    meta_json = await resp.json(content_type=None)
                    if not isinstance(meta_json, dict):
                        return uri, None, on_name, on_symbol, None
                    image = (
                        meta_json.get("image")
                        or meta_json.get("image_url")
                        or meta_json.get("imageUrl")
                    )
                    # JSON 中的 name/symbol 作为补充（链上值优先）
                    name   = on_name   or meta_json.get("name")   or None
                    symbol = on_symbol or meta_json.get("symbol") or None
                    return (
                        uri,
                        (image.strip() if isinstance(image, str) else None),
                        name,
                        symbol,
                        meta_json,
                    )
            except Exception:
                return uri, None, on_name, on_symbol, None

    return list(await asyncio.gather(*[_fetch_image(mint) for mint in mints]))


async def fetch_metadata_batch(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
    rpc_sem: asyncio.Semaphore,
    mints: List[str],
    image_sem: Optional[asyncio.Semaphore] = None,
) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
    """
    元数据获取主入口，返回 [(token_uri, image_url, name, symbol, metadata), ...] 与 mints 同序。

      1. 优先 Helius DAS API getAssetBatch（配置了 HELIUS_API_KEY 时）
         - 单批 1000 mint，直接返回 token_uri / image_url / name / symbol / metadata
      2. Helius 未命中（token_uri=None）或未配置 key → 回退链上：
         getMultipleAccounts → borsh 解码（含 name/symbol）→ HTTP 拉取 JSON

    helius_sem : 控制同时在途的 getAssetBatch 请求数
    rpc_sem    : 控制链上 getMultipleAccounts 并发数
    image_sem  : 限流链上路径的图片 HTTP 请求（未传则用 CONCURRENT_IMAGE 新建）
    """
    _img_sem = image_sem or asyncio.Semaphore(CONCURRENT_IMAGE)
    _empty: Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]] = (
        None, None, None, None, None
    )
    results: List[
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]
    ] = [_empty] * len(mints)

    if HELIUS_API_KEY:
        helius_results = await fetch_helius_metadata_batch(session, helius_sem, mints)
        results = list(helius_results)

        # token_uri 或 image_url 任意一个缺失，均触发链上回退
        # 链上路径会通过 getMultipleAccounts + borsh 解码 + HTTP 拉 JSON
        # 补全 Helius 未能返回的字段（partial miss 也能覆盖）
        fallback_indices = [
            i for i, (uri, img, *_) in enumerate(results)
            if not uri or not img
        ]
        if fallback_indices:
            fallback_mints = [mints[i] for i in fallback_indices]
            onchain = await fetch_onchain_metadata_batch(
                session, rpc_sem, _img_sem, fallback_mints
            )
            # 字段级合并：Helius 已有的字段保留，缺失字段由链上结果补充
            for idx, (o_uri, o_img, o_name, o_symbol, o_metadata) in zip(
                fallback_indices, onchain
            ):
                h_uri, h_img, h_name, h_symbol, h_metadata = results[idx]
                results[idx] = (
                    h_uri    or o_uri,
                    h_img    or o_img,
                    h_name   or o_name,
                    h_symbol or o_symbol,
                    h_metadata or o_metadata,
                )
    else:
        # 无 Helius key：全量走链上
        results = await fetch_onchain_metadata_batch(
            session, rpc_sem, _img_sem, mints
        )

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Helius getProgramAccountsV2：全链 NFT Mint 发现
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_gpa_page(
    session: aiohttp.ClientSession,
    helius_rpc_url: str,
    page_size: int,
    pagination_key: Optional[str] = None,
    changed_since_slot: Optional[int] = None,
) -> Tuple[List[str], Optional[str]]:
    """
    调用 Helius getProgramAccountsV2 拉取一页 Token Metadata Program 账户，
    提取其中所有 Metaplex MetadataV1 账户的 mint 地址。

    原理：
      每个 Solana NFT 都拥有一个由 Token Metadata Program 管理的 PDA 账户，
      账户数据布局（borsh）：
        offset  0 : key（1 byte）= 4 → MetadataV1
        offset  1 : update_authority（32 bytes）
        offset 33 : mint（32 bytes）← 我们只需要这 32 字节
      通过 dataSlice {offset:33, length:32} 仅传输 mint 字节，网络开销极小。

    参数：
      helius_rpc_url    : 含 API Key 的 Helius RPC URL
      page_size         : 每页账户数（1~10000）
      pagination_key    : 上一页返回的游标（None = 从第一页开始）
      changed_since_slot: 仅返回该 Slot 之后发生变化的账户（None = 全量）

    返回：
      (mint_addresses, next_pagination_key)
      next_pagination_key 为 None 时表示已到最后一页。
    """
    # memcmp filter：offset=0，bytes="5" 即 base58([0x04])，匹配 key=4（MetadataV1）
    params: Dict = {
        "encoding":  "base64",
        "filters":   [{"memcmp": {"offset": 0, "bytes": "5"}}],
        "dataSlice": {"offset": 33, "length": 32},
        "limit":     page_size,
    }
    if pagination_key:
        params["paginationKey"] = pagination_key
    if changed_since_slot:
        params["changedSinceSlot"] = changed_since_slot

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccountsV2",
        "params": [TOKEN_METADATA_PROGRAM_ID, params],
    }

    timeout = aiohttp.ClientTimeout(total=90)
    body: Optional[Dict] = None

    for attempt in range(1, 6):
        try:
            async with session.post(helius_rpc_url, json=payload, timeout=timeout) as resp:
                if resp.status == 429:
                    wait = min(2 ** attempt, 60)
                    logger.warning("GPA 429 限速（第%d次），%ds 后重试...", attempt, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                body = await resp.json(content_type=None)
            break
        except asyncio.TimeoutError:
            wait = min(2 ** attempt, 30)
            logger.warning("GPA 请求超时（第%d次），%ds 后重试...", attempt, wait)
            await asyncio.sleep(wait)
        except Exception as exc:
            wait = min(2 ** attempt, 30)
            logger.warning("GPA 请求异常（第%d次）: %s，%ds 后重试...", attempt, exc, wait)
            await asyncio.sleep(wait)

    if body is None:
        logger.error("GPA 全部重试失败，跳过本页")
        return [], pagination_key  # 返回原 key，上层可重试

    if body.get("error"):
        logger.error("GPA RPC 错误: %s", body["error"])
        return [], pagination_key

    result   = body.get("result") or {}
    accounts = result.get("accounts") or []
    next_key = result.get("paginationKey")  # None = 最后一页

    mints: List[str] = []
    for acc in accounts:
        data_field = (acc.get("account") or {}).get("data")
        if not data_field:
            continue
        # data 格式: ["base64string", "base64"]
        raw_b64 = data_field[0] if isinstance(data_field, list) else data_field
        try:
            raw = base64.b64decode(raw_b64)
        except Exception:
            continue
        if len(raw) != 32:
            continue
        # 全零地址（System Program）过滤
        if raw == b"\x00" * 32:
            continue
        mint = b58encode(raw)
        # Solana pubkey base58 编码后长度为 32~44；过短说明解码有误
        if 32 <= len(mint) <= 44:
            mints.append(mint)

    return mints, next_key
