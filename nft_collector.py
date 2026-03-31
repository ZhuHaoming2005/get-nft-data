#!/usr/bin/env python3
"""
NFT 数据采集脚本

从 EVM 兼容链（Base、Ethereum 等）基于链上交易事件采集 NFT 信息，
写入 PostgreSQL，每个 NFT（chain + contract + tokenID）仅存储一次。

支持事件类型：
  - ERC-721  Transfer(address,address,uint256)
  - ERC-1155 TransferSingle(address,address,address,uint256,uint256)
  - ERC-1155 TransferBatch(address,address,address,uint256[],uint256[])
"""

import asyncio
import base64
import json as _json
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import unquote

import aiohttp
import psycopg2
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv()

# ─── 日志配置 ──────────────────────────────────────────────────────────────────
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # logging.FileHandler("nft_collector.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─── 事件签名 Topic0 ────────────────────────────────────────────────────────────
# keccak256("Transfer(address,address,uint256)")
ERC721_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
# keccak256("TransferSingle(address,address,address,uint256,uint256)")
ERC1155_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
# keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
ERC1155_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)

ALL_TOPICS = [ERC721_TRANSFER_TOPIC, ERC1155_SINGLE_TOPIC, ERC1155_BATCH_TOPIC]

# 字节形式，用于与 web3.py 返回的 HexBytes topics 直接比较（避免 hex() 前缀歧义）
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
CHAIN_NAME       = os.getenv("CHAIN_NAME", "base")
RPC_URL          = os.getenv("RPC_URL", "https://mainnet.base.org")
START_BLOCK      = int(os.getenv("START_BLOCK", "0"))
END_BLOCK        = int(os.getenv("END_BLOCK", "0"))   # 0 = 链上最新
BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "2000"))
REQUEST_DELAY    = float(os.getenv("REQUEST_DELAY", "0.1"))  # 请求间隔（秒）

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

# metadata / Alchemy 请求超时（秒）
METADATA_TIMEOUT         = int(os.getenv("METADATA_TIMEOUT", "10"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "15"))

# Alchemy NFT API
ALCHEMY_API_KEY  = os.getenv("ALCHEMY_API_KEY", "cqc3UTO2nogk8iTo3SHcK")
# Alchemy 网络子域名（如 polygon-mainnet、eth-mainnet、base-mainnet）
ALCHEMY_NETWORK  = os.getenv("ALCHEMY_NETWORK", "polygon-mainnet")
# 每次批量请求最多携带的 token 数（Alchemy 上限 100）
ALCHEMY_BATCH_SIZE = int(os.getenv("ALCHEMY_BATCH_SIZE", "100"))

# 合约地址黑名单：来自 DeFi 等协议的 NFT 不采集。
# - DEFI_BLACKLIST：逗号分隔的地址（如 0xabc...,0xdef...）
DEFI_BLACKLIST_ENV = os.getenv("DEFI_BLACKLIST", "")

# 并发：Alchemy 批量请求并发数 / RPC 链上回退并发数
CONCURRENT_ALCHEMY = int(os.getenv("CONCURRENT_ALCHEMY", "5"))
CONCURRENT_RPC     = int(os.getenv("CONCURRENT_RPC", "10"))


# ══════════════════════════════════════════════════════════════════════════════
# 数据库操作（每链独立表：nft_assets_base、nft_assets_eth 等）
# ══════════════════════════════════════════════════════════════════════════════


def _nft_table_name(chain_name: str) -> str:
    """根据 chain_name 生成表名，仅允许 [a-z0-9_] 防止注入。"""
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower())
    if not safe:
        safe = "default"
    return f"nft_assets_{safe}"


def load_blacklist() -> Set[str]:
    """加载合约地址黑名单（小写），用于过滤 DeFi 等协议的 NFT。"""
    out: Set[str] = set()
    if DEFI_BLACKLIST_ENV:
        for part in DEFI_BLACKLIST_ENV.split(","):
            a = part.strip()
            if a and a.startswith("0x"):
                out.add(a.lower())
    return out


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=10,
    )


def init_db(conn, chain_name: str) -> None:
    """建表（幂等）：为该链创建 nft_assets_{chain} 表及 scan_progress 表。"""
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress (
                chain_name         VARCHAR(50) PRIMARY KEY,
                last_scanned_block BIGINT      NOT NULL,
                updated_at         TIMESTAMPTZ DEFAULT NOW()
            )
        """)
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
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_nft_contract ON {tbl} (contract_address)
        """)
    conn.commit()
    logger.info("数据库表初始化完成: %s", tbl)


def load_seen_nfts(conn, chain_name: str) -> Set[Tuple[str, int]]:
    """预加载已存储的 (contract_address, token_id) 集合，用于内存去重。"""
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"SELECT contract_address, token_id FROM {tbl}")
        return {(row[0], int(row[1])) for row in cur.fetchall()}


def get_last_block(conn, chain_name: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_scanned_block FROM scan_progress WHERE chain_name = %s",
            (chain_name,),
        )
        row = cur.fetchone()
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


def batch_insert(conn, chain_name: str, records: List[Tuple]) -> int:
    """
    批量写入 NFT 记录到该链专属表，ON CONFLICT DO NOTHING 保证幂等。
    records: [(contract_address, token_id, token_uri, image_uri, token_standard, first_seen_block), ...]
    返回实际插入行数。
    """
    if not records:
        return 0
    tbl = _nft_table_name(chain_name)

    def _clean(v):
        """移除 PostgreSQL text 字段不允许的 NUL 字节（\x00）。"""
        if isinstance(v, str):
            return v.replace("\x00", "")
        return v

    records = [tuple(_clean(v) for v in rec) for rec in records]

    with conn.cursor() as cur:
        placeholders = ", ".join(
            ["(%s, %s, %s, %s, %s, %s)"] * len(records)
        )
        flat = [item for rec in records for item in rec]
        cur.execute(
            f"""
            INSERT INTO {tbl}
                (contract_address, token_id, token_uri, image_uri, token_standard, first_seen_block)
            VALUES """ + placeholders + """
            ON CONFLICT (contract_address, token_id) DO NOTHING
            """,
            flat,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# tokenURI 获取（异步）
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_token_uri(
    w3: AsyncWeb3,
    sem: asyncio.Semaphore,
    contract_address: str,
    token_id: int,
    standard: str,
) -> Optional[str]:
    """
    异步调用链上合约获取 tokenURI（ERC-721）或 uri（ERC-1155）。
    通过 sem 控制最大并发数，避免触发节点限频。
    合约不支持或调用失败时返回 None。
    """
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


# ══════════════════════════════════════════════════════════════════════════════
# Alchemy NFT API 批量调用 + 链上内嵌 data URI 解码
# ══════════════════════════════════════════════════════════════════════════════


def _decode_inline_image(uri: str) -> Optional[str]:
    """
    解码链上内嵌的 data:application/ tokenURI，提取 image/image_url 字段。

    常见格式：
      data:application/json;base64,<base64 encoded JSON>
      data:application/json,<URL encoded or plain JSON>
    有 image/image_url 字段时返回其值，否则返回 None。
    """
    comma = uri.find(",")
    if comma == -1:
        return None

    header  = uri[:comma].lower()
    payload = uri[comma + 1:]

    try:
        if ";base64" in header:
            data = base64.b64decode(payload + "==")
            obj  = _json.loads(data)
        else:
            obj = _json.loads(unquote(payload))
    except Exception:
        return None

    if not isinstance(obj, dict):
        return None

    image = obj.get("image") or obj.get("image_url")
    if not image or not isinstance(image, str):
        return None
    return image.strip()


async def fetch_alchemy_batch(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    tokens: List[Tuple[str, int, str]],
) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    调用 Alchemy getNFTMetadataBatch，批量获取 NFT 元数据。
    每次最多 100 条（ALCHEMY_BATCH_SIZE），通过 sem 控制并发批次数。

    POST https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadataBatch
    请求体：{ "tokens": [{"contractAddress": ..., "tokenId": ..., "tokenType": ...}], ... }
    响应体：{ "nfts": [ { "raw": { "tokenUri": ..., "metadata": { "image": ... } } }, ... ] }

    tokens 为 (contract_address, token_id, standard) 三元组列表，
    standard 为 "ERC-721" 或 "ERC-1155"（自动转换为 Alchemy 格式）。
    返回与 tokens 同序的 [(token_uri, image_url), ...] 列表，失败条目为 (None, None)。
    """
    # Alchemy 要求 tokenType 不含连字符：ERC721 / ERC1155
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
        total=METADATA_TIMEOUT,
        connect=METADATA_CONNECT_TIMEOUT,
    )

    async with sem:
        try:
            async with session.post(url, json=payload, timeout=timeout) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.info(
                "Alchemy batch 失败 %d tokens: [%s] %s",
                len(tokens), type(exc).__name__, exc,
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

    # 补齐：若返回数量不足（部分条目缺失），用 (None, None) 填充
    while len(results) < len(tokens):
        results.append((None, None))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 日志解码
# ══════════════════════════════════════════════════════════════════════════════

def _to_bytes(data) -> bytes:
    """将 data 字段（HexBytes / bytes / 十六进制字符串）统一转为 bytes。"""
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    s = str(data)
    return bytes.fromhex(s[2:] if s.startswith("0x") else s)


def _decode_single(raw: bytes) -> int:
    """
    解码 TransferSingle 的 data 字段。
    ABI 编码：abi.encode(uint256 id, uint256 value)
    前 32 字节即为 id。
    """
    return int.from_bytes(raw[0:32], "big")


def _decode_batch(raw: bytes) -> List[int]:
    """
    解码 TransferBatch 的 data 字段。
    ABI 编码：abi.encode(uint256[] ids, uint256[] values)
    布局：
      [0:32]   ids 数组的偏移量（通常为 0x40 = 64）
      [32:64]  values 数组的偏移量
      [ids_offset : ids_offset+32]     ids 数组长度
      [ids_offset+32 : ...]            ids 元素（每个 32 字节）
    """
    if len(raw) < 64:
        return []
    ids_offset = int.from_bytes(raw[0:32], "big")
    if ids_offset + 32 > len(raw):
        return []
    ids_len = int.from_bytes(raw[ids_offset : ids_offset + 32], "big")
    result: List[int] = []
    for i in range(ids_len):
        start = ids_offset + 32 + i * 32
        if start + 32 > len(raw):
            break
        result.append(int.from_bytes(raw[start : start + 32], "big"))
    return result


def extract_nfts(log) -> List[Tuple[str, int, str]]:
    """
    从单条事件日志提取 NFT 信息。
    返回：[(contract_address_lower, token_id, standard), ...]

    ERC-721 Transfer  : topics = [sig, from, to, tokenId]  → 4 个 topics
    ERC-1155 Single   : topics = [sig, op, from, to]       → 4 个 topics，id 在 data
    ERC-1155 Batch    : topics = [sig, op, from, to]       → 4 个 topics，ids 在 data

    注意：ERC-20 Transfer 同签名但只有 3 个 topics，通过长度过滤。

    topic 比较直接使用 bytes，避免 hexbytes >= 1.0 中 hex() 返回值带 "0x" 前缀
    与手动拼接 "0x" 叠加导致的比较失败问题。
    """
    topics = log["topics"]
    if not topics or len(topics) < 3:
        return []

    # bytes() 将 HexBytes/bytes/bytearray 统一转为普通 bytes 再比较
    topic0  = bytes(topics[0])
    address = log["address"].lower()
    results: List[Tuple[str, int, str]] = []

    # ── ERC-721 Transfer ────────────────────────────────────────────────────
    if topic0 == _ERC721_TRANSFER_B and len(topics) == 4:
        # tokenId 在 topics[3]，直接从字节大端解析
        token_id = int.from_bytes(topics[3], "big")
        results.append((address, token_id, "ERC-721"))

    # ── ERC-1155 TransferSingle ─────────────────────────────────────────────
    elif topic0 == _ERC1155_SINGLE_B and len(topics) == 4:
        raw = _to_bytes(log["data"])
        if len(raw) >= 32:
            token_id = _decode_single(raw)
            results.append((address, token_id, "ERC-1155"))

    # ── ERC-1155 TransferBatch ──────────────────────────────────────────────
    elif topic0 == _ERC1155_BATCH_B and len(topics) == 4:
        raw = _to_bytes(log["data"])
        for tid in _decode_batch(raw):
            results.append((address, tid, "ERC-1155"))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 日志抓取
# ══════════════════════════════════════════════════════════════════════════════

_TOPIC_LABEL = {
    ERC721_TRANSFER_TOPIC: "ERC-721 Transfer    ",
    ERC1155_SINGLE_TOPIC:  "ERC-1155 Single     ",
    ERC1155_BATCH_TOPIC:   "ERC-1155 Batch      ",
}


async def _fetch_logs_one_topic(
    w3: AsyncWeb3, from_block: int, to_block: int, topic: str, label: str
) -> list:
    """单 topic 的 eth_getLogs（异步）。"""
    try:
        logs = await w3.eth.get_logs(
            {"fromBlock": from_block, "toBlock": to_block, "topics": [topic]}
        )
        if topic == ERC721_TRANSFER_TOPIC:
            nft_count   = sum(1 for lg in logs if len(lg["topics"]) == 4)
            erc20_count = len(logs) - nft_count
            logger.info(
                "    %s → %d 条（NFT/4-topics: %d，ERC-20/3-topics: %d）",
                label, len(logs), nft_count, erc20_count,
            )
        else:
            logger.info("    %s → %d 条", label, len(logs))
        return list(logs)
    except Exception as exc:
        logger.warning("    %s → eth_getLogs 失败: %s", label, exc)
        return []


async def fetch_logs(w3: AsyncWeb3, from_block: int, to_block: int) -> list:
    """
    对三种事件并发调用 eth_getLogs，合并返回。
    使用 asyncio.gather 同时发起 3 次 RPC。
    """
    results = await asyncio.gather(*[
        _fetch_logs_one_topic(w3, from_block, to_block, topic, _TOPIC_LABEL[topic])
        for topic in ALL_TOPICS
    ])
    if REQUEST_DELAY > 0:
        await asyncio.sleep(REQUEST_DELAY)
    return [log for logs in results for log in logs]


# ══════════════════════════════════════════════════════════════════════════════
# 主流程（异步）
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    logger.info("═══ NFT 数据采集启动 ═══  链: %s | RPC: %s", CHAIN_NAME, RPC_URL)

    # ── 连接节点 ──────────────────────────────────────────────────────────────
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    if not await w3.is_connected():
        logger.error("无法连接 RPC 节点，请检查 RPC_URL")
        sys.exit(1)

    latest_block = await w3.eth.block_number
    logger.info("节点连接成功，当前最新区块: %d", latest_block)

    # ── 连接数据库 ────────────────────────────────────────────────────────────
    conn = get_conn()
    init_db(conn, CHAIN_NAME)

    # ── 确定扫描范围（从新到旧：高区块 → 低区块）────────────────────────────────
    # top_block  : 扫描起点（最新区块），END_BLOCK=0 时取链上最新
    # stop_block : 扫描终点（最旧区块），START_BLOCK=0 时扫到创世块
    top_block  = END_BLOCK if END_BLOCK > 0 else latest_block
    stop_block = max(START_BLOCK, 0)

    # last_scanned_block 在倒序模式下保存的是上次已扫描批次的最低区块
    # 断点续扫时从该值的前一块继续向下
    last_saved = get_last_block(conn, CHAIN_NAME)
    cur_block  = (last_saved - 1) if last_saved is not None else top_block

    if cur_block < stop_block:
        logger.info("已扫描至底部区块（%d），无需继续", stop_block)
        conn.close()
        return

    logger.info(
        "扫描方向: %d → %d（共约 %d 个区块，每批 %d 个）",
        cur_block, stop_block, cur_block - stop_block + 1, BLOCK_BATCH_SIZE,
    )

    # ── 预加载已有 NFT 集合（内存去重，避免对已知 NFT 重复调 tokenURI）──────────
    logger.info("预加载已有 NFT 集合...")
    seen: Set[Tuple[str, int]] = load_seen_nfts(conn, CHAIN_NAME)
    logger.info("数据库已有记录: %d 条", len(seen))

    # ── 加载合约地址黑名单（DeFi 等协议 NFT 不采集）────────────────────────────
    blacklist: Set[str] = load_blacklist()
    if blacklist:
        logger.info("合约黑名单已加载: %d 个地址", len(blacklist))

    total_new = 0

    # ── 逐批扫描（从高区块向低区块）─────────────────────────────────────────
    while cur_block >= stop_block:
        from_block = max(cur_block - BLOCK_BATCH_SIZE + 1, stop_block)
        to_block   = cur_block
        logger.info("► 扫描区块 [%d - %d] ↓", to_block, from_block)

        logs = await fetch_logs(w3, from_block, to_block)

        # 提取候选 NFT，去重并过滤黑名单
        candidates: List[Tuple[str, int, str, int]] = []  # (addr, tid, std, block_num)
        dup_skip       = 0
        blacklist_skip = 0
        for log in logs:
            block_num = log["blockNumber"]
            for addr, tid, std in extract_nfts(log):
                key = (addr, tid)
                if key in seen:
                    dup_skip += 1
                    continue
                if addr in blacklist:
                    blacklist_skip += 1
                    continue
                seen.add(key)
                candidates.append((addr, tid, std, block_num))

        # ── 步骤1：并发批量调 Alchemy API，一次获取 raw.tokenUri + raw.metadata.image
        alchemy_sem = asyncio.Semaphore(CONCURRENT_ALCHEMY)
        token_pairs = [(addr, tid, std) for addr, tid, std, _ in candidates]
        # 按 ALCHEMY_BATCH_SIZE 切块，并发发起多批请求
        chunks = [
            token_pairs[i: i + ALCHEMY_BATCH_SIZE]
            for i in range(0, len(token_pairs), ALCHEMY_BATCH_SIZE)
        ]
        # trust_env=True 使 aiohttp 读取系统代理（HTTP_PROXY / HTTPS_PROXY）
        async with aiohttp.ClientSession(trust_env=True) as session:
            chunk_results = list(await asyncio.gather(*[
                fetch_alchemy_batch(session, alchemy_sem, chunk)
                for chunk in chunks
            ])) if chunks else []
        alchemy_results: List[Tuple[Optional[str], Optional[str]]] = [
            item for chunk in chunk_results for item in chunk
        ]

        # ── 步骤2：tokenUri 为空时 fallback 到链上合约（异步并发）──────────────
        fallback_indices = [
            i for i, (uri, _) in enumerate(alchemy_results) if not uri
        ]
        if fallback_indices:
            uri_sem = asyncio.Semaphore(CONCURRENT_RPC)
            fallback_uris: List[Optional[str]] = list(await asyncio.gather(*[
                fetch_token_uri(
                    w3, uri_sem,
                    candidates[i][0], candidates[i][1], candidates[i][2],
                )
                for i in fallback_indices
            ]))
            for idx, chain_uri in zip(fallback_indices, fallback_uris):
                _, img = alchemy_results[idx]
                alchemy_results[idx] = (chain_uri, img)

        # ── 步骤3：组装写库 batch ─────────────────────────────────────────────
        batch: List[Tuple] = []
        uri_skip = 0
        for (addr, tid, std, block_num), (token_uri, image_url) in zip(
            candidates, alchemy_results
        ):
            if not token_uri or token_uri.startswith("api.tierlock.com/uri/"):
                uri_skip += 1
                continue
            # 链上内嵌 data URI：尝试从中提取 image（不涉及网络 IO）
            if image_url is None and token_uri.startswith("data:application/"):
                inline_img = _decode_inline_image(token_uri)
                if inline_img and not inline_img.startswith("data:application/"):
                    image_url = inline_img
            batch.append((addr, str(tid), token_uri, image_url, std, block_num))

        inserted = batch_insert(conn, CHAIN_NAME, batch)
        total_new += inserted
        logger.info(
            "  本批结果: 待写入 %d，实际落库 %d，已有跳过 %d，黑名单 %d，URI过滤 %d，累计 %d",
            len(batch), inserted, dup_skip, blacklist_skip, uri_skip, total_new,
        )

        await asyncio.sleep(1)

        # 保存进度：记录本批已扫描到的最低区块
        save_progress(conn, CHAIN_NAME, from_block)
        cur_block = from_block - 1

    logger.info("═══ 采集完成 ═══  本次共写入 NFT: %d 条", total_new)
    conn.close()


if __name__ == "__main__":
    # Windows 上 ProactorEventLoop 与 aiohttp SSL 存在兼容性问题，切换为 SelectorEventLoop
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
