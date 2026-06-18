#!/usr/bin/env python3
"""
脚本二：Solana NFT Metadata 获取器（高性能版）

持续从临时表（temp_{chain}）读取 mint 地址，并发获取元数据后写入主表。

性能优化点：
  1. CONCURRENT_HELIUS=5  — 同时在途的 getAssetBatch 请求数（每批 1000 mint）
  2. 所有就绪批次一轮并发投入 _process_batch，helius_sem 在内部统一限流
  3. 流水线：DB写[N] 作为后台 Task，与 DB读 + HTTP获取[N+1] 完全重叠（隐藏写延迟）
  4. sem 仅持有 HTTP 调用期间，重试 sleep 期间释放，吞吐量 ≈ 并发数 / 平均延迟
  5. PDA 计算（CPU密集）放入线程池，不阻塞事件循环
  6. getMultipleAccounts per-chunk 并发，rpc_sem 控制同时在途的 RPC 请求数
  7. image_sem 限制图片 HTTP 并发（CONCURRENT_IMAGE=50），避免 CDN 限速
  8. 双 DB 连接（conn_insert / conn_delete）并发写库，insert 与 delete 互不阻塞
  9. TCPConnector(limit=0) 不限连接数，由信号量在应用层统一控流

获取策略：
  1. 优先：Helius DAS API getAssetBatch（HELIUS_API_KEY 配置时启用，单批 1000 mint）
  2. 回退：链上 getMultipleAccounts → borsh 解码 → HTTP 拉 JSON
  - token_uri 非空 → 写主表；为空 → 不写但仍从临时表删除
  - data:image 开头的链上图像 mint → 不写主表，但仍从临时表删除
"""

import asyncio
import sys
from typing import Any, List, Optional, Set, Tuple

import aiohttp

from common import (
    CHAIN_NAME,
    RPC_URL,
    HELIUS_BATCH_SIZE,
    CONCURRENT_HELIUS,
    CONCURRENT_RPC,
    CONCURRENT_IMAGE,
    FETCH_IDLE_WAIT,
    logger,
    get_conn,
    init_db,
    load_pending_nfts,
    batch_insert_main,
    delete_temp_nfts,
    fetch_metadata_batch,
)


# token_uri 黑名单前缀：命中则不写主表
_INVALID_URI_PREFIXES = (
    "data:text/",
    "data:application/xml",
    "data:application/json",
)

# 连续 N 轮从 DB 读不到新数据时，将缓冲区中的不足-1000 的尾部强制刷出
_DRAIN_AFTER_IDLE = 3


async def _process_batch(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
    rpc_sem: asyncio.Semaphore,
    image_sem: asyncio.Semaphore,
    pending: List[Tuple],
) -> Tuple[List[Tuple], List[int]]:
    """
    对一批临时表记录并发获取元数据，每次传入的 pending 恰好是 HELIUS_BATCH_SIZE 的整数倍
    （或强制刷出的尾部），保证每个 getAssetBatch HTTP 请求都凑满 HELIUS_BATCH_SIZE 条。
    返回 (inserts, all_ids)。
    """
    mints   = [row[1] for row in pending]
    all_ids = [row[0] for row in pending]

    # 按 HELIUS_BATCH_SIZE 分块，每块对应一次 getAssetBatch 请求
    chunk_size = HELIUS_BATCH_SIZE
    chunks = [mints[i: i + chunk_size] for i in range(0, len(mints), chunk_size)]

    chunk_results = list(await asyncio.gather(*[
        fetch_metadata_batch(session, helius_sem, rpc_sem, chunk, image_sem)
        for chunk in chunks
    ]))

    # 每项结果为 (token_uri, image_url, name, symbol, metadata) 5-元组
    results: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]] = [
        item for chunk in chunk_results for item in chunk
    ]

    # 组装有效记录（包含 name / symbol / metadata），顺序与 EVM batch_insert_main 对齐
    inserts: List[Tuple] = []
    for (
        (_, mint, token_id, std, first_seen_slot),
        (token_uri, image_url, name, symbol, metadata),
    ) in zip(pending, results):
        if not isinstance(image_url, str):
            continue
        if image_url and image_url.startswith("data:image"):
            continue
        if not token_uri or any(token_uri.startswith(p) for p in _INVALID_URI_PREFIXES):
            continue
        inserts.append((
            mint, token_id, token_uri, image_url,
            name, symbol, metadata,
            std or "Metaplex", first_seen_slot,
        ))

    return inserts, all_ids


async def _fetch_all(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
    rpc_sem: asyncio.Semaphore,
    image_sem: asyncio.Semaphore,
    batches: List[List[Tuple]],
    forced: bool = False,
) -> Tuple[List[Tuple], List[int], int, int]:
    """
    并发获取所有批次的元数据（纯 HTTP，不写库）。
    helius_sem 在 fetch_metadata_batch 内自动限流到 CONCURRENT_HELIUS。
    返回 (all_inserts, all_ids, total_raw, n_inserts)。
    """
    tag = "[强制刷出]" if forced else f"[{len(batches)} 批 / {sum(len(b) for b in batches)} 条]"
    logger.info("► 并发获取 %s", tag)

    all_results: List[Tuple[List[Tuple], List[int]]] = list(
        await asyncio.gather(*[
            _process_batch(session, helius_sem, rpc_sem, image_sem, batch)
            for batch in batches
        ])
    )

    all_inserts: List[Tuple] = [row for inserts, _ in all_results for row in inserts]
    all_ids:     List[int]   = [rid for _, ids    in all_results for rid in ids]
    total_raw  = sum(len(b) for b in batches)
    n_inserts  = len(all_inserts)
    return all_inserts, all_ids, total_raw, n_inserts


async def _do_write(
    conn_insert,
    conn_delete,
    all_inserts: List[Tuple],
    all_ids: List[int],
) -> Tuple[int, int]:
    """并发写库（两个独立连接，insert / delete 互不阻塞）。"""
    return await asyncio.gather(
        asyncio.to_thread(batch_insert_main, conn_insert, CHAIN_NAME, all_inserts),
        asyncio.to_thread(delete_temp_nfts,  conn_delete, CHAIN_NAME, all_ids),
    )


async def main() -> None:
    logger.info(
        "═══ Solana Metadata 获取器启动 ═══  链: %s | RPC: %s",
        CHAIN_NAME, RPC_URL,
    )

    conn        = get_conn()   # 用于读临时表 + init_db
    conn_insert = get_conn()   # 专用写主表连接
    conn_delete = get_conn()   # 专用删临时表连接
    init_db(conn, CHAIN_NAME)

    # 信号量（全局复用，跨批次共享，避免每批重建）
    helius_sem = asyncio.Semaphore(CONCURRENT_HELIUS)
    rpc_sem    = asyncio.Semaphore(CONCURRENT_RPC)
    image_sem  = asyncio.Semaphore(CONCURRENT_IMAGE)

    # TCPConnector：不限总连接数，由信号量在应用层统一控流
    connector = aiohttp.TCPConnector(
        limit=0,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    total_inserted = total_deleted = 0

    # ── 缓冲区：跨轮次积累，凑满 HELIUS_BATCH_SIZE 再触发请求 ──────────────────
    buf: List[Tuple] = []       # 待处理行（(id, mint, token_id, std, slot)）
    buf_ids: Set[int] = set()   # 已在 buf 中的 DB id，用于跨轮去重
    idle_rounds = 0             # 连续读不到新数据的轮次计数

    # ── 流水线状态：后台写库任务（与下一轮 HTTP 获取重叠）──────────────────────
    # 时序：HTTP获取[N] → 启动写库Task[N] → continue → DB读 + HTTP获取[N+1]
    #                                        → await写库Task[N]（通常已完成）→ 启动写库Task[N+1]
    pending_write: Optional[asyncio.Task] = None  # 上一轮的写库后台任务
    pw_total_raw:  int = 0   # 上一轮批次的原始记录数（用于日志）
    pw_n_inserts:  int = 0   # 上一轮批次的有效记录数（用于日志）

    async with aiohttp.ClientSession(
        connector=connector,
        trust_env=True,
        connector_owner=True,
    ) as session:

        while True:
            # 每轮读取足够多的记录，确保能填满若干个完整批次
            load_limit = HELIUS_BATCH_SIZE * max(CONCURRENT_HELIUS * 3, 10)
            pending = load_pending_nfts(conn, CHAIN_NAME, limit=load_limit)

            # 将 DB 新行合并进缓冲区（按 DB id 去重，避免重复处理）
            added = 0
            for row in pending:
                if row[0] not in buf_ids:
                    buf.append(row)
                    buf_ids.add(row[0])
                    added += 1

            if added > 0:
                idle_rounds = 0
                logger.info(
                    "从临时表新增 %d 条，缓冲区共 %d 条",
                    added, len(buf),
                )

            # ── 并发获取所有就绪批次，流水线化写库 ──────────────────────────────
            ready_batches: List[List[Tuple]] = []
            while len(buf) >= HELIUS_BATCH_SIZE:
                batch = buf[:HELIUS_BATCH_SIZE]
                buf   = buf[HELIUS_BATCH_SIZE:]
                buf_ids -= {row[0] for row in batch}
                ready_batches.append(batch)

            if ready_batches:
                # HTTP 获取（此时 pending_write 可能仍在后台运行 → 两者重叠）
                all_inserts, all_ids, total_raw, n_inserts = await _fetch_all(
                    session, helius_sem, rpc_sem, image_sem, ready_batches
                )

                # 获取完成后，结清上一轮写库（通常已结束，等待时间趋近于零）
                if pending_write:
                    ins, del_ = await pending_write
                    total_inserted += ins
                    total_deleted  += del_
                    logger.info(
                        "  写入 %d，删除 %d（无效 %d）| 累计写入 %d 删除 %d",
                        ins, del_, pw_total_raw - pw_n_inserts,
                        total_inserted, total_deleted,
                    )
                    pending_write = None

                # 本轮写库作为后台任务启动，立即 continue 进入下一轮读取+获取
                pending_write = asyncio.create_task(
                    _do_write(conn_insert, conn_delete, all_inserts, all_ids)
                )
                pw_total_raw = total_raw
                pw_n_inserts = n_inserts

                continue  # 立即再读，填满下一批（与 pending_write 并行）

            # ── 无完整批次：先结清写库，再处理空闲 ──────────────────────────────
            if pending_write:
                ins, del_ = await pending_write
                total_inserted += ins
                total_deleted  += del_
                logger.info(
                    "  写入 %d，删除 %d（无效 %d）| 累计写入 %d 删除 %d",
                    ins, del_, pw_total_raw - pw_n_inserts,
                    total_inserted, total_deleted,
                )
                pending_write = None

            if not buf:
                logger.info(
                    "临时表与缓冲区均为空，等待扫描器产出（%d 秒）...",
                    FETCH_IDLE_WAIT,
                )
                await asyncio.sleep(FETCH_IDLE_WAIT)

            elif idle_rounds >= _DRAIN_AFTER_IDLE:
                # 连续多轮无新数据 → 扫描器可能已停止，强制刷出尾部
                logger.info(
                    "缓冲区剩余 %d 条（<%d），连续 %d 轮无新增，强制刷出",
                    len(buf), HELIUS_BATCH_SIZE, idle_rounds,
                )
                all_inserts, all_ids, total_raw, n_inserts = await _fetch_all(
                    session, helius_sem, rpc_sem, image_sem, [buf], forced=True
                )
                ins, del_ = await _do_write(conn_insert, conn_delete, all_inserts, all_ids)
                total_inserted += ins
                total_deleted  += del_
                logger.info(
                    "  写入 %d，删除 %d（无效 %d）| 累计写入 %d 删除 %d",
                    ins, del_, total_raw - n_inserts,
                    total_inserted, total_deleted,
                )
                buf.clear()
                buf_ids.clear()
                idle_rounds = 0

            else:
                idle_rounds += 1
                logger.info(
                    "缓冲区 %d 条（<%d），等待更多数据（%d/%d 轮，%d 秒后重试）...",
                    len(buf), HELIUS_BATCH_SIZE,
                    idle_rounds, _DRAIN_AFTER_IDLE, FETCH_IDLE_WAIT,
                )
                await asyncio.sleep(FETCH_IDLE_WAIT)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
