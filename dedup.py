#!/usr/bin/env python3
"""
NFT 数据查重工具

从 JSON 分类文件中读取 metadata_url / image_url，与数据库 nft_assets 表中的
token_uri / image_uri 进行模糊对比：
  - IPFS    → 提取 CID（不受 gateway 影响）
  - Arweave → 提取 TX ID（不受 gateway 影响）
  - 其他    → 小写规范化后精确对比

特性：
  - 数据库全量加载：fetchmany 分批拉取，全部规范化 key 驻留内存，保证 O(1) 查找
  - JSON 流式处理：基于 ijson 逐记录读取，不将文件整体载入内存
  - 多进程并行对比（默认 CPU 核心数），Pool initializer 避免重复序列化大 set
  - 每完成一个批次立即输出阶段性进度与新发现的重复合约

依赖：
  pip install psycopg2-binary ijson

用法：
  python dedup.py                          # 扫描 classification&status/*.json
  python dedup.py path/to/file.json        # 指定单个文件
  python dedup.py "data/part*.json"        # 指定 glob 模式（需加引号）
  python dedup.py -w 4 -c 200             # 4 个工作进程，每批 200 条
  python dedup.py -o result.txt           # 同时将最终结果写入文件
"""

import argparse
import json
import logging
import multiprocessing as mp
import os
import re
import sys
import time
from glob import glob
from typing import Dict, Generator, Iterator, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

try:
    import ijson
    _HAS_IJSON = True
except ImportError:
    _HAS_IJSON = False

# ── 日志 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── 数据库配置 ────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "pgm-2zevls2414y7mw6d8o.pg.rds.aliyuncs.com")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "user1")
DB_PASS = os.getenv("DB_PASS", "_JC!y7XWygm$94f")

# ── URL 规范化 ────────────────────────────────────────────────────────────────
_RE_IPFS_HTTP = re.compile(
    r"https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)",
    re.IGNORECASE,
)
_RE_ARWEAVE_HTTP = re.compile(
    r"https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)",
    re.IGNORECASE,
)


def normalize_url(url: Optional[str]) -> Optional[str]:
    """
    将 URL 规范化为去重 key：
      - IPFS    → "ipfs:<CID>[/path]"（剥离 gateway、query、fragment）
      - Arweave → "ar:<TXID>[/path]"（同上）
      - 其他    → 小写 + 去末尾斜杠
    返回 None 表示无法规范化（空值、data: URI 等）。
    """
    if not url:
        return None
    url = url.strip()
    if not url or url.startswith("data:"):
        return None

    lo = url.lower()

    # ipfs:// 协议
    if lo.startswith("ipfs://"):
        tail = url[7:]
        if tail.lower().startswith("ipfs/"):
            tail = tail[5:]
        cid_path = tail.split("?")[0].split("#")[0].strip("/")
        return ("ipfs:" + cid_path) if cid_path else None

    # ar:// 协议
    if lo.startswith("ar://"):
        tx_path = url[5:].split("?")[0].split("#")[0].strip("/")
        return ("ar:" + tx_path) if tx_path else None

    # HTTP IPFS 网关
    m = _RE_IPFS_HTTP.match(url)
    if m:
        cid_path = m.group(1).split("?")[0].split("#")[0].rstrip("/")
        return ("ipfs:" + cid_path) if cid_path else None

    # HTTP Arweave 网关
    m = _RE_ARWEAVE_HTTP.match(url)
    if m:
        tx_path = m.group(1).split("?")[0].split("#")[0].rstrip("/")
        return ("ar:" + tx_path) if tx_path else None

    return lo.rstrip("/")


# ── 数据库 ────────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10,
    )


_DB_SELECT_SIZE = 100_000  # 每段 SELECT 的行数（LIMIT / 服务端游标 FETCH）


def load_db_keys() -> Tuple[Set[str], Set[str]]:
    """
    分段加载 nft_assets_base 的 URI 字段：
      - 使用 PostgreSQL 服务端命名游标（named cursor），每次 fetchmany 触发
        一条 FETCH n FROM cursor SQL，服务器只在需要时才向客户端发送数据，
        避免一次性将海量结果集传输到客户端缓冲区。
      - 若服务端游标因事务或权限问题失败，自动回退到 LIMIT/OFFSET 分段查询。
      - 全部规范化 key 驻留内存，保证后续比对 O(1) 查找。
    """
    logger.info(
        "正在分段加载数据库 URI 数据（每段 %d 行）...", _DB_SELECT_SIZE
    )
    t0 = time.monotonic()

    _SQL = """
        SELECT token_uri, image_uri
        FROM   nft_assets_base
        WHERE  token_uri IS NOT NULL
          AND  image_uri IS NOT NULL
    """

    token_keys: Set[str] = set()
    image_keys: Set[str] = set()
    count = 0

    def _ingest(rows: list) -> None:
        nonlocal count
        for token_uri, image_uri in rows:
            k = normalize_url(token_uri)
            if k:
                token_keys.add(k)
            k = normalize_url(image_uri)
            if k:
                image_keys.add(k)
        count += len(rows)

    conn = get_conn()
    try:
        # ── 方案 A：服务端命名游标（真正的分段 SELECT）──────────────────────
        try:
            # autocommit=False 时命名游标在事务内有效
            conn.autocommit = False
            with conn.cursor("_load_keys_cur") as cur:
                cur.itersize = _DB_SELECT_SIZE
                cur.execute(_SQL)
                seg = 0
                while True:
                    rows = cur.fetchmany(_DB_SELECT_SIZE)
                    if not rows:
                        break
                    _ingest(rows)
                    seg += 1
                    logger.info(
                        "  [服务端游标] 第 %d 段，累计已加载 %d 条...",
                        seg, count,
                    )
            conn.commit()

        except Exception as exc:
            # ── 方案 B：回退到 LIMIT/OFFSET 分段查询 ───────────────────────
            logger.warning(
                "服务端游标不可用（%s），回退到 LIMIT/OFFSET 分段查询。", exc
            )
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()
            conn = get_conn()
            conn.autocommit = True

            token_keys.clear()
            image_keys.clear()
            count = 0
            offset = 0
            seg = 0
            while True:
                with conn.cursor() as cur:
                    cur.execute(
                        _SQL + " ORDER BY ctid LIMIT %s OFFSET %s",
                        (_DB_SELECT_SIZE, offset),
                    )
                    rows = cur.fetchall()
                if not rows:
                    break
                _ingest(rows)
                offset += len(rows)
                seg += 1
                logger.info(
                    "  [LIMIT/OFFSET] 第 %d 段 offset=%d，累计已加载 %d 条...",
                    seg, offset - len(rows), count,
                )
    finally:
        conn.close()

    elapsed = time.monotonic() - t0
    logger.info(
        "数据库分段加载完毕：%d 条记录，token keys=%d，image keys=%d，耗时 %.1fs",
        count, len(token_keys), len(image_keys), elapsed,
    )
    return token_keys, image_keys


# ── JSON 流式读取 ─────────────────────────────────────────────────────────────

def _iter_json_file(path: str) -> Iterator[dict]:
    """
    逐记录流式读取单个 JSON 文件（顶层为数组或单对象均可），
    优先使用 ijson；若未安装则回退到 json.load（整体加载）。
    """
    if _HAS_IJSON:
        with open(path, "rb") as f:
            # prefix='item' 逐个 yield 顶层数组中的每个对象
            yield from ijson.items(f, "item")
    else:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            yield data


def resolve_files(pattern: str) -> List[str]:
    """将 glob 模式展开为有序文件路径列表，找不到时退出。"""
    files = sorted(glob(pattern))
    if not files:
        logger.error("未找到匹配的文件：%s", pattern)
        sys.exit(1)
    return files


def stream_json_files(files: List[str]) -> Generator[dict, None, None]:
    """
    依次流式读取多个 JSON 文件，逐条 yield 记录。
    整个过程内存中最多只持有一个 chunk 的记录。
    """
    if not _HAS_IJSON:
        logger.warning(
            "未检测到 ijson，将回退为整体加载模式。"
            "建议执行 pip install ijson 以启用真正的流式处理。"
        )
    for path in files:
        logger.info("流式读取：%s", path)
        yield from _iter_json_file(path)


# ── 多进程查重 ────────────────────────────────────────────────────────────────

# 每个工作进程持有自己的 key set 副本（通过 initializer 一次性写入，避免每个 task 重复 pickle）
_worker_token_keys: Set[str] = set()
_worker_image_keys: Set[str] = set()


def _worker_init(token_keys: Set[str], image_keys: Set[str]) -> None:
    """Pool initializer：将共享 key set 写入进程全局变量。"""
    global _worker_token_keys, _worker_image_keys
    _worker_token_keys = token_keys
    _worker_image_keys = image_keys


def _process_chunk(chunk: List[dict]) -> Tuple[int, Dict[str, List[str]], Set[str], int]:
    """
    工作函数：对一批记录逐条比对。
    返回 (实际处理条数, { contract: [原因, ...] }, 本批出现的合约集合, 本批重复NFT条数)。
    """
    result: Dict[str, List[str]] = {}
    contracts_in_chunk: Set[str] = set()
    duplicate_nft_count = 0
    for rec in chunk:
        contract = (rec.get("contract") or "unknown").lower()
        contracts_in_chunk.add(contract)
        identifier = rec.get("identifier", "?")
        reasons: List[str] = []

        meta_key = normalize_url(rec.get("metadata_url"))
        if meta_key and meta_key in _worker_token_keys:
            reasons.append(f"[#{identifier}] metadata_url 重复 → {meta_key}")

        img_key = normalize_url(rec.get("image_url"))
        if img_key and img_key in _worker_image_keys:
            reasons.append(f"[#{identifier}] image_url 重复 → {img_key}")

        if reasons:
            result.setdefault(contract, []).extend(reasons)
            duplicate_nft_count += 1

    return len(chunk), result, contracts_in_chunk, duplicate_nft_count


def _chunk_stream(
    stream: Iterator[dict], chunk_size: int
) -> Generator[List[dict], None, None]:
    """
    将记录流切分为固定大小的 chunk 列表，供 imap_unordered 消费。
    惰性求值：主进程读多少、工作进程消费多少，天然背压控制。
    """
    chunk: List[dict] = []
    for rec in stream:
        chunk.append(rec)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def run_parallel_dedup(
    files: List[str],
    token_keys: Set[str],
    image_keys: Set[str],
    workers: int,
    chunk_size: int,
) -> Tuple[Dict[str, List[str]], int, Set[str], int]:
    """
    流式读取文件 → 切 chunk → 多进程并行比对。
    每完成一个 chunk 立即打印阶段性进度和新发现的重复合约。
    返回 (all_duplicates, total_processed, all_contracts, total_duplicate_nfts)。
    """
    logger.info(
        "启动多进程查重：workers=%d，chunk_size=%d，文件数=%d",
        workers, chunk_size, len(files),
    )

    all_duplicates: Dict[str, List[str]] = {}
    all_contracts: Set[str] = set()
    total_duplicate_nfts = 0
    processed = 0
    batch_idx = 0
    t0 = time.monotonic()

    record_stream = stream_json_files(files)
    chunks = _chunk_stream(record_stream, chunk_size)

    # spawn 上下文在 Windows 下是必须的，同时也是跨平台安全选择
    ctx = mp.get_context("spawn")
    with ctx.Pool(
        processes=workers,
        initializer=_worker_init,
        initargs=(token_keys, image_keys),
    ) as pool:
        # imap_unordered 惰性调度：chunks 生成器被按需消费，
        # 哪个 worker 先完成就先返回结果，保证最优吞吐
        for chunk_len, batch_result, contracts_in_chunk, dup_nfts in pool.imap_unordered(
            _process_chunk, chunks
        ):
            batch_idx += 1
            processed += chunk_len
            all_contracts |= contracts_in_chunk
            total_duplicate_nfts += dup_nfts
            elapsed = time.monotonic() - t0
            speed = processed / elapsed if elapsed > 0 else 0

            # 合并本批结果，记录新增合约
            new_contracts: List[str] = []
            for contract, reasons in batch_result.items():
                if contract not in all_duplicates:
                    new_contracts.append(contract)
                all_duplicates.setdefault(contract, []).extend(reasons)

            # ── 阶段性进度（原地刷新）─────────────────────────────────────
            print(
                f"\r  已处理 {processed:,} 条"
                f"  批次 #{batch_idx}"
                f"  速度 {speed:,.0f} 条/s"
                f"  重复合约 {len(all_duplicates)} 个"
                f"  耗时 {elapsed:.1f}s   ",  # 尾部空格覆盖可能的旧字符
                end="",
                flush=True,
            )

            # 有新增重复合约时立即换行输出详情
            if new_contracts:
                print()  # 结束进度行
                print(f"  ↳ 批次 #{batch_idx} 新发现 {len(new_contracts)} 个重复合约：")
                for c in sorted(new_contracts):
                    print(f"    合约：{c}")
                    for r in batch_result.get(c, []):
                        print(f"      · {r}")

    print()  # 进度行换行
    return all_duplicates, processed, all_contracts, total_duplicate_nfts


# ── 入口 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NFT 数据查重：比对 JSON 文件与数据库中的 metadata/image URL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "pattern",
        nargs="?",
        default="classification&status/*.json",
        help="JSON 文件路径或 glob 模式",
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=min(16, os.cpu_count() or 10),
        metavar="N",
        help="工作进程数",
    )
    parser.add_argument(
        "--chunk-size", "-c",
        type=int,
        default=1000,
        metavar="N",
        help="每批处理的记录数（越小阶段输出越频繁，越大吞吐量越高）",
    )
    parser.add_argument(
        "--output", "-o",
        default=f"result{time.strftime('%Y-%m-%d_%H-%M-%S')}.txt",
        metavar="FILE",
        help="将最终汇总结果写入指定文件",
    )
    args = parser.parse_args()

    t_start = time.monotonic()

    # 1. 数据库全量加载，构建 key set
    token_keys, image_keys = load_db_keys()

    # 2. 解析文件列表（仅展开 glob，不加载内容）
    files = resolve_files(args.pattern)
    logger.info("待处理文件 %d 个：%s", len(files), ", ".join(files))

    # 3. 流式读取文件 + 多进程并行查重（含阶段性输出）
    duplicates, total_records, all_contracts, total_duplicate_nfts = run_parallel_dedup(
        files, token_keys, image_keys,
        workers=args.workers,
        chunk_size=args.chunk_size,
    )

    # 4. 计算重复比例
    total_contracts = len(all_contracts)
    dup_contract_ratio = (
        (len(duplicates) / total_contracts * 100) if total_contracts else 0.0
    )
    dup_nft_ratio = (
        (total_duplicate_nfts / total_records * 100) if total_records else 0.0
    )

    # 5. 最终汇总输出
    total_elapsed = time.monotonic() - t_start
    sep = "═" * 70
    lines: List[str] = [
        "",
        sep,
        f"  查重完成  总耗时 {total_elapsed:.1f}s  共比对 {total_records:,} 条记录",
        f"  总合约数 {total_contracts:,}  重复合约 {len(duplicates):,}  重复合约比例 {dup_contract_ratio:.2f}%",
        f"  重复 NFT 条数 {total_duplicate_nfts:,}  重复 NFT 比例 {dup_nft_ratio:.2f}%",
        sep,
    ]
    if not duplicates:
        lines.append("  ✓ 未发现任何重复数据。")
    else:
        lines.append(f"  共发现 {len(duplicates)} 个合约地址存在重复：")
        lines.append("─" * 70)
        for contract in sorted(duplicates):
            lines.append(f"  合约地址：{contract}")
            for reason in duplicates[contract]:
                lines.append(f"    · {reason}")
            lines.append("")
    lines.append(sep)

    result_text = "\n".join(lines)
    print(result_text)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result_text)
        logger.info("汇总结果已写入：%s", args.output)


if __name__ == "__main__":
    main()
