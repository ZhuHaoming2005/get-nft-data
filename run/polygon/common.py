#!/usr/bin/env python3
"""
共享模块：配置、数据库工具、Alchemy / RPC 函数、链上日志解码。
由 log_scanner.py 和 metadata_fetcher.py 共同引用。
"""

import asyncio
import base64
import json as _json
import logging
import os
import re
import sys
from typing import List, Optional, Set, Tuple
from urllib.parse import unquote

import aiohttp
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv()

# ─── 日志配置 ──────────────────────────────────────────────────────────────────
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── 事件签名 Topic0 ────────────────────────────────────────────────────────────
ERC721_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ERC1155_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
ERC1155_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)
ALL_TOPICS = [ERC721_TRANSFER_TOPIC, ERC1155_SINGLE_TOPIC, ERC1155_BATCH_TOPIC]

_ERC721_TRANSFER_B = bytes.fromhex(ERC721_TRANSFER_TOPIC[2:])
_ERC1155_SINGLE_B  = bytes.fromhex(ERC1155_SINGLE_TOPIC[2:])
_ERC1155_BATCH_B   = bytes.fromhex(ERC1155_BATCH_TOPIC[2:])

# ─── 合约 ABI（仅 tokenURI / uri 方法）──────────────────────────────────────────
ERC721_ABI = [
    {
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "name": "tokenURI",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    }
]
ERC1155_ABI = [
    {
        "inputs": [{"name": "id", "type": "uint256"}],
        "name": "uri",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# ─── 配置（优先读取 .env）──────────────────────────────────────────────────────
CHAIN_NAME       = os.getenv("CHAIN_NAME", "polygon")
RPC_URL          = os.getenv("RPC_URL", "https://polygon-rpc.com")
START_BLOCK      = int(os.getenv("START_BLOCK", "0"))
END_BLOCK        = int(os.getenv("END_BLOCK", "0"))
BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "2000"))
REQUEST_DELAY    = float(os.getenv("REQUEST_DELAY", "0.1"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

METADATA_TIMEOUT         = int(os.getenv("METADATA_TIMEOUT", "10"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "15"))

ALCHEMY_API_KEY    = os.getenv("ALCHEMY_API_KEY", "")
ALCHEMY_NETWORK    = os.getenv("ALCHEMY_NETWORK", "polygon-mainnet")
ALCHEMY_BATCH_SIZE = int(os.getenv("ALCHEMY_BATCH_SIZE", "100"))
RPC_BATCH_SIZE     = int(os.getenv("RPC_BATCH_SIZE", "100"))

DEFI_BLACKLIST_ENV = os.getenv("DEFI_BLACKLIST", "")

CONCURRENT_ALCHEMY = int(os.getenv("CONCURRENT_ALCHEMY", "5"))
CONCURRENT_RPC     = int(os.getenv("CONCURRENT_RPC", "10"))

# log_scanner 滑动窗口宽度：同时在途的 eth_getLogs 批次数
SCAN_WINDOW = int(os.getenv("SCAN_WINDOW", "3"))

# metadata_fetcher 专用
# 无待处理记录时等待的秒数
FETCH_IDLE_WAIT  = int(os.getenv("FETCH_IDLE_WAIT", "30"))
_ERC721_TOKEN_URI_SELECTOR = "c87b56dd"
_ERC1155_URI_SELECTOR = "0e89341c"
DB_INSERT_PAGE_SIZE = int(os.getenv("DB_INSERT_PAGE_SIZE", "1000"))


# ══════════════════════════════════════════════════════════════════════════════
# 数据库工具
# ══════════════════════════════════════════════════════════════════════════════

def _nft_table_name(chain_name: str) -> str:
    """主表：nft_assets_{chain}，存储最终有效 NFT 数据。"""
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"nft_assets_{safe}"


def _temp_table_name(chain_name: str) -> str:
    """临时表：temp_{chain}，存储扫描阶段的中间数据，由 metadata_fetcher 消费后清空。"""
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"temp_{safe}"


def load_blacklist() -> Set[str]:
    out: Set[str] = set()
    for part in DEFI_BLACKLIST_ENV.split(","):
        a = part.strip()
        if a.startswith("0x"):
            out.add(a.lower())
    return out


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10,
    )


def init_db(conn, chain_name: str) -> None:
    """建表（幂等）：主表 nft_assets_{chain}、临时表 temp_{chain}、进度表。"""
    tbl = _nft_table_name(chain_name)
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress (
                chain_name         VARCHAR(50) PRIMARY KEY,
                last_scanned_block BIGINT      NOT NULL,
                updated_at         TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 主表：存储已获取 tokenUri 的有效 NFT
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(42)  NOT NULL,
                token_id         NUMERIC      NOT NULL,
                token_uri        TEXT,
                image_uri        TEXT,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (contract_address, token_id)
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_nft_contract ON {tbl} (contract_address)"
        )
        # 临时表：log_scanner 写入原始扫描记录，无 tokenUri 字段
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tmp} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(42)  NOT NULL,
                token_id         NUMERIC      NOT NULL,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (contract_address, token_id)
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_temp_contract ON {tmp} (contract_address)"
        )
    conn.commit()
    logger.info("数据库表初始化完成: 主表=%s  临时表=%s", tbl, tmp)


def load_seen_nfts(conn, chain_name: str) -> Set[Tuple[str, int]]:
    """
    预加载去重集合：合并主表与临时表中已有的 (contract_address, token_id)。
    log_scanner 用此集合跳过重复扫描结果。
    """
    tbl = _nft_table_name(chain_name)
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT contract_address, token_id FROM {tbl}
            UNION
            SELECT contract_address, token_id FROM {tmp}
        """)
        result = {(row[0], int(row[1])) for row in cur.fetchall()}
    conn.commit()  # 立即关闭隐式事务，避免长期 idle in transaction
    return result


def get_last_block(conn, chain_name: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_scanned_block FROM scan_progress WHERE chain_name = %s",
            (chain_name,),
        )
        row = cur.fetchone()
    conn.commit()  # 立即关闭隐式事务，避免长期 idle in transaction
    return row[0] if row else None


def save_progress(conn, chain_name: str, block: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_progress (chain_name, last_scanned_block)
            VALUES (%s, %s)
            ON CONFLICT (chain_name) DO UPDATE
                SET last_scanned_block = EXCLUDED.last_scanned_block,
                    updated_at = NOW()
            """,
            (chain_name, block),
        )
    conn.commit()


def batch_insert_temp(conn, chain_name: str, records: List[Tuple]) -> int:
    """
    log_scanner 专用：批量写入扫描原始记录到临时表。
    records: [(contract_address, token_id, token_standard, first_seen_block), ...]
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


def load_pending_nfts(conn, chain_name: str) -> List[Tuple]:
    """
    metadata_fetcher 专用：从临时表读取全部待处理记录。
    返回 [(id, contract_address, token_id, token_standard, first_seen_block), ...]
    """
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, contract_address, token_id, token_standard, first_seen_block
            FROM {tmp}
            ORDER BY id
            LIMIT 5000
            """,
        )
        result = [(row[0], row[1], int(row[2]), row[3], row[4]) for row in cur.fetchall()]
    # 立即提交关闭隐式事务：后续需经历长时间异步网络 IO（Alchemy/RPC），
    # 若事务持续开启会导致连接长期 idle in transaction，可能触发
    # idle_in_transaction_session_timeout 断连，也会妨碍 autovacuum。
    conn.commit()
    return result


def batch_insert_main(conn, chain_name: str, records: List[Tuple]) -> int:
    """
    metadata_fetcher 专用：将有效 NFT 记录批量写入主表。
    records: [(contract_address, token_id, token_uri, image_uri, token_standard, first_seen_block), ...]
    """
    if not records:
        return 0
    tbl = _nft_table_name(chain_name)

    def _clean(v):
        return v.replace("\x00", "") if isinstance(v, str) else v

    records = [tuple(_clean(v) for v in rec) for rec in records]
    sql = f"""
        INSERT INTO {tbl}
            (contract_address, token_id, token_uri, image_uri, token_standard, first_seen_block)
        VALUES %s
        ON CONFLICT (contract_address, token_id) DO NOTHING
    """

    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(records), DB_INSERT_PAGE_SIZE):
            page = records[start: start + DB_INSERT_PAGE_SIZE]
            execute_values(
                cur,
                sql,
                page,
                template="(%s, %s, %s, %s, %s, %s)",
                page_size=len(page),
            )
            inserted += max(cur.rowcount, 0)
    conn.commit()
    return inserted


def delete_temp_nfts(conn, chain_name: str, ids: List[int]) -> int:
    """metadata_fetcher 专用：从临时表删除已处理的记录（无论有效与否）。"""
    if not ids:
        return 0
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tmp} WHERE id = ANY(%s)", (ids,))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def delete_contract_nfts(
    conn, chain_name: str, contract_addrs: Set[str]
) -> Tuple[int, int]:
    """
    从主表和临时表中删除指定合约的全部记录（按合约地址批量删除）。
    用于将整个合约列入黑名单后清理历史数据。
    返回 (主表删除数, 临时表删除数)。
    """
    if not contract_addrs:
        return 0, 0
    tbl  = _nft_table_name(chain_name)
    tmp  = _temp_table_name(chain_name)
    addrs = [a.lower() for a in contract_addrs]
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tbl} WHERE contract_address = ANY(%s)", (addrs,))
        main_del = cur.rowcount
        cur.execute(f"DELETE FROM {tmp} WHERE contract_address = ANY(%s)", (addrs,))
        temp_del = cur.rowcount
    conn.commit()
    return main_del, temp_del


def append_blacklist_env(new_addrs: Set[str], env_path: str = ".env") -> None:
    """
    将新合约地址追加到 .env 文件的 DEFI_BLACKLIST 键中（自动去重、保留原有地址）。
    若文件中不存在该键则追加一行；若文件不存在则新建。
    """
    if not new_addrs:
        return

    try:
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []

    existing: Set[str] = set()
    bl_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("DEFI_BLACKLIST="):
            bl_idx = i
            val = stripped[len("DEFI_BLACKLIST="):]
            for part in val.split(","):
                a = part.strip()
                if a.startswith("0x"):
                    existing.add(a.lower())
            break

    all_addrs = existing | {a.lower() for a in new_addrs}
    new_line  = "DEFI_BLACKLIST=" + ",".join(sorted(all_addrs)) + "\n"

    if bl_idx >= 0:
        lines[bl_idx] = new_line
    else:
        lines.append(new_line)

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    logger.info(
        "黑名单 .env 已更新，新增 %d 个合约: %s",
        len(new_addrs), sorted(new_addrs),
    )


# ══════════════════════════════════════════════════════════════════════════════
# token URI 工具：{id} 占位符替换（ERC-1155）
# ══════════════════════════════════════════════════════════════════════════════

def replace_token_id_placeholder(uri: str, token_id: int) -> str:
    """
    将 ERC-1155 token URI 中的 {id} 占位符替换为真实 token ID。

    EIP-1155 规定：URI 中的字面量 "{id}" 必须被替换为 token ID 的
    64 位零填充小写十六进制字符串，例如 token_id=1 → 000...0001。
    """
    if "{id}" not in uri:
        return uri
    return uri.replace("{id}", format(token_id, "064x"))


def fix_token_id_placeholders(conn, chain_name: str) -> int:
    """
    修复主表中 token_uri 含 {id} 占位符的历史记录，将其替换为真实 token ID。

    用于一次性迁移：对已入库但 token_uri 未展开 {id} 的 ERC-1155 记录做修正。
    返回实际更新的行数。
    """
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, token_id, token_uri FROM {tbl} WHERE token_uri LIKE %s",
            ("%{id}%",),
        )
        rows = cur.fetchall()
    conn.commit()

    if not rows:
        return 0

    updates = []
    for row_id, token_id, token_uri in rows:
        fixed = replace_token_id_placeholder(token_uri, int(token_id))
        if fixed != token_uri:
            updates.append((fixed, row_id))

    if not updates:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            f"UPDATE {tbl} SET token_uri = %s WHERE id = %s",
            updates,
        )
        updated = cur.rowcount
    conn.commit()
    return updated


# ══════════════════════════════════════════════════════════════════════════════
# Alchemy NFT API + 链上内嵌 data URI 解码
# ══════════════════════════════════════════════════════════════════════════════

def _decode_inline_image(uri: str) -> Optional[str]:
    """
    解码链上内嵌的 data:application/ tokenURI，提取 image/image_url 字段。
    """
    comma = uri.find(",")
    if comma == -1:
        return None
    header  = uri[:comma].lower()
    payload = uri[comma + 1:]
    try:
        obj = (
            _json.loads(base64.b64decode(payload + "=="))
            if ";base64" in header
            else _json.loads(unquote(payload))
        )
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    image = obj.get("image") or obj.get("image_url")
    return image.strip() if image and isinstance(image, str) else None


async def fetch_alchemy_batch(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    tokens: List[Tuple[str, int, str]],
) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    调用 Alchemy getNFTMetadataBatch，批量获取 NFT 元数据。
    tokens: [(contract_address, token_id, standard), ...]
    返回与 tokens 同序的 [(token_uri, image_url), ...]，失败条目为 (None, None)。
    """
    def _to_alchemy_type(std: str) -> str:
        return std.replace("-", "")

    url = (
        f"https://{ALCHEMY_NETWORK}.g.alchemy.com"
        f"/nft/v3/{ALCHEMY_API_KEY}/getNFTMetadataBatch"
    )
    payload = {
        "tokens": [
            {
                "contractAddress": addr,
                "tokenId": str(tid),
                "tokenType": _to_alchemy_type(std),
            }
            for addr, tid, std in tokens
        ],
        "tokenUriTimeoutInMs": 5000,
        "refreshCache": False,
    }
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT, connect=METADATA_CONNECT_TIMEOUT
    )

    max_retries = 3
    async with sem:
        data = None
        for attempt in range(1, max_retries + 1):
            try:
                async with session.post(url, json=payload, timeout=timeout) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                break
            except Exception as exc:
                if attempt < max_retries:
                    wait = 2 ** (attempt - 1)  # 1s, 2s
                    logger.warning(
                        "Alchemy batch 第 %d/%d 次失败 %d tokens: [%s] %s，%.0fs 后重试",
                        attempt, max_retries, len(tokens), type(exc).__name__, exc, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.info(
                        "Alchemy batch 全部重试失败（%d 次）%d tokens: [%s] %s",
                        max_retries, len(tokens), type(exc).__name__, exc,
                    )
                    return [(None, None)] * len(tokens)

    nft_list = data if isinstance(data, list) else data.get("nfts", [])
    results: List[Tuple[Optional[str], Optional[str]]] = []
    for nft in nft_list:
        raw = nft.get("raw") or {}
        if raw.get("error"):
            results.append((None, None))
            continue
        token_uri: Optional[str] = raw.get("tokenUri") or None
        image_url: Optional[str] = None
        raw_meta = raw.get("metadata")
        if isinstance(raw_meta, dict):
            image_url = raw_meta.get("image") or None
        results.append((token_uri, image_url))

    while len(results) < len(tokens):
        results.append((None, None))
    return results


async def fetch_token_uri(
    w3: AsyncWeb3,
    sem: asyncio.Semaphore,
    contract_address: str,
    token_id: int,
    standard: str,
) -> Optional[str]:
    """异步调用链上合约获取 tokenURI（ERC-721）或 uri（ERC-1155）。"""
    async with sem:
        try:
            addr = AsyncWeb3.to_checksum_address(contract_address)
            if standard == "ERC-721":
                contract = w3.eth.contract(address=addr, abi=ERC721_ABI)
                uri = await contract.functions.tokenURI(token_id).call()
            else:
                contract = w3.eth.contract(address=addr, abi=ERC1155_ABI)
                uri = await contract.functions.uri(token_id).call()
            if uri and "{id}" in uri:
                uri = uri.replace("{id}", str(token_id))
            return uri
        except Exception:
            return None


def _build_token_uri_call_data(token_id: int, standard: str) -> str:
    selector = (
        _ERC721_TOKEN_URI_SELECTOR if standard == "ERC-721" else _ERC1155_URI_SELECTOR
    )
    return "0x" + selector + format(token_id, "064x")


def _decode_abi_string_result(result: object) -> Optional[str]:
    if not isinstance(result, str) or not result.startswith("0x"):
        return None
    try:
        raw = bytes.fromhex(result[2:])
    except ValueError:
        return None
    if len(raw) < 64:
        return None

    offset = int.from_bytes(raw[:32], "big")
    if offset + 32 > len(raw):
        return None

    strlen = int.from_bytes(raw[offset: offset + 32], "big")
    start = offset + 32
    end = start + strlen
    if end > len(raw):
        return None

    try:
        return raw[start:end].decode("utf-8")
    except UnicodeDecodeError:
        return None


async def fetch_token_uri_batch(
    session: aiohttp.ClientSession,
    rpc_url: str,
    sem: asyncio.Semaphore,
    tokens: List[Tuple[str, int, str]],
) -> List[Optional[str]]:
    """通过单次 JSON-RPC batch eth_call 批量获取 tokenURI/uri。"""
    if not tokens:
        return []

    payload = [
        {
            "jsonrpc": "2.0",
            "id": idx,
            "method": "eth_call",
            "params": [
                {
                    "to": contract_address,
                    "data": _build_token_uri_call_data(token_id, standard),
                },
                "latest",
            ],
        }
        for idx, (contract_address, token_id, standard) in enumerate(tokens)
    ]
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT, connect=METADATA_CONNECT_TIMEOUT
    )

    max_retries = 3
    async with sem:
        data = None
        for attempt in range(1, max_retries + 1):
            try:
                async with session.post(rpc_url, json=payload, timeout=timeout) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                if not isinstance(data, list):
                    raise ValueError("RPC batch response is not a list")
                break
            except Exception as exc:
                if attempt < max_retries:
                    wait = 2 ** (attempt - 1)
                    logger.warning(
                        "RPC batch 第 %d/%d 次失败 %d tokens: [%s] %s，%.0fs 后重试",
                        attempt, max_retries, len(tokens), type(exc).__name__, exc, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.info(
                        "RPC batch 全部重试失败（%d 次）%d tokens: [%s] %s",
                        max_retries, len(tokens), type(exc).__name__, exc,
                    )
                    return [None] * len(tokens)

    results: List[Optional[str]] = [None] * len(tokens)
    for item in data:
        if not isinstance(item, dict):
            continue
        idx = item.get("id")
        if not isinstance(idx, int) or idx < 0 or idx >= len(tokens):
            continue
        if item.get("error"):
            continue

        uri = _decode_abi_string_result(item.get("result"))
        if uri and "{id}" in uri:
            uri = uri.replace("{id}", str(tokens[idx][1]))
        results[idx] = uri
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 链上日志解码
# ══════════════════════════════════════════════════════════════════════════════

def _to_bytes(data) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    s = str(data)
    return bytes.fromhex(s[2:] if s.startswith("0x") else s)


def _decode_single(raw: bytes) -> int:
    return int.from_bytes(raw[0:32], "big")


def _decode_batch(raw: bytes) -> List[int]:
    if len(raw) < 64:
        return []
    ids_offset = int.from_bytes(raw[0:32], "big")
    if ids_offset + 32 > len(raw):
        return []
    ids_len = int.from_bytes(raw[ids_offset: ids_offset + 32], "big")
    result: List[int] = []
    for i in range(ids_len):
        start = ids_offset + 32 + i * 32
        if start + 32 > len(raw):
            break
        result.append(int.from_bytes(raw[start: start + 32], "big"))
    return result


def extract_nfts(log) -> List[Tuple[str, int, str]]:
    """
    从单条事件日志提取 NFT 信息。
    返回：[(contract_address_lower, token_id, standard), ...]
    """
    topics = log["topics"]
    if not topics or len(topics) < 3:
        return []
    topic0  = bytes(topics[0])
    address = log["address"].lower()
    results: List[Tuple[str, int, str]] = []

    if topic0 == _ERC721_TRANSFER_B and len(topics) == 4:
        results.append((address, int.from_bytes(topics[3], "big"), "ERC-721"))
    elif topic0 == _ERC1155_SINGLE_B and len(topics) == 4:
        raw = _to_bytes(log["data"])
        if len(raw) >= 32:
            results.append((address, _decode_single(raw), "ERC-1155"))
    elif topic0 == _ERC1155_BATCH_B and len(topics) == 4:
        raw = _to_bytes(log["data"])
        for tid in _decode_batch(raw):
            results.append((address, tid, "ERC-1155"))
    return results


_TOPIC_LABEL = {
    ERC721_TRANSFER_TOPIC: "ERC-721 Transfer    ",
    ERC1155_SINGLE_TOPIC:  "ERC-1155 Single     ",
    ERC1155_BATCH_TOPIC:   "ERC-1155 Batch      ",
}


def _parse_raw_log(raw: dict) -> dict:
    """
    将 JSON-RPC 原始日志（hex 字段）转换为 extract_nfts 兼容格式。
      address    : 小写字符串（已是字符串，无需转换）
      topics     : List[bytes]（从 hex 字符串转换）
      data       : bytes
      blockNumber: int
    """
    def h2b(h: str) -> bytes:
        h = h or "0x"
        return bytes.fromhex(h[2:] if h.startswith("0x") else h)

    bn = raw.get("blockNumber", "0x0")
    return {
        "address":     raw.get("address", "").lower(),
        "topics":      [h2b(t) for t in raw.get("topics", [])],
        "data":        h2b(raw.get("data", "0x")),
        "blockNumber": int(bn, 16) if isinstance(bn, str) else int(bn),
    }


async def _fetch_logs_one_topic_http(
    session: aiohttp.ClientSession,
    rpc_url: str,
    from_block: int,
    to_block: int,
    topic: str,
    label: str,
) -> list:
    """
    直接通过 aiohttp 发起单个 eth_getLogs JSON-RPC 请求。
    绕过 web3.py AsyncHTTPProvider 的内部连接串行化，
    aiohttp 的连接池可为每个请求分配独立连接，实现真正 HTTP 并发。
    """
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_getLogs",
        "params":  [{"fromBlock": hex(from_block), "toBlock": hex(to_block), "topics": [topic]}],
        "id":      1,
    }
    timeout = aiohttp.ClientTimeout(total=120)
    try:
        async with session.post(rpc_url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            body = await resp.json(content_type=None)
    except Exception as exc:
        logger.warning("    %s → eth_getLogs 失败: %s", label, exc)
        return []

    if "error" in body:
        logger.warning("    %s → RPC 错误: %s", label, body["error"])
        return []

    raw_logs = body.get("result") or []
    logs = [_parse_raw_log(r) for r in raw_logs]

    if topic == ERC721_TRANSFER_TOPIC:
        nft_count = sum(1 for lg in logs if len(lg["topics"]) == 4)
        logger.info(
            "    %s → %d 条（NFT: %d，ERC-20: %d）",
            label, len(logs), nft_count, len(logs) - nft_count,
        )
    else:
        logger.info("    %s → %d 条", label, len(logs))
    return logs


async def fetch_logs_http(
    session: aiohttp.ClientSession,
    rpc_url: str,
    from_block: int,
    to_block: int,
) -> list:
    """
    直接通过 aiohttp 并发请求三种 topic 的 eth_getLogs。
    与 fetch_logs（依赖 web3.py AsyncHTTPProvider）相比，
    此函数中每个 topic 请求独占连接池中的一条连接，HTTP 层真正并发。
    """
    results = await asyncio.gather(*[
        _fetch_logs_one_topic_http(
            session, rpc_url, from_block, to_block, topic, _TOPIC_LABEL[topic]
        )
        for topic in ALL_TOPICS
    ])
    if REQUEST_DELAY > 0:
        await asyncio.sleep(REQUEST_DELAY)
    return [log for logs in results for log in logs]
