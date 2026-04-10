#!/usr/bin/env python3
"""
脚本一：链上日志扫描器

扫描 EVM 链上的 NFT Transfer 事件，将
  (contract_address, token_id, token_standard, first_seen_block)
写入数据库，token_uri / image_uri 初始留空，由 metadata_fetcher.py 异步填充。

支持断点续扫（scan_progress 表记录进度）。
"""

import asyncio
import sys
from collections import deque
from typing import Deque, List, Optional, Set, Tuple

import aiohttp
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

from common import (
    CHAIN_NAME,
    RPC_URL,
    START_BLOCK,
    END_BLOCK,
    BLOCK_BATCH_SIZE,
    SCAN_WINDOW,
    logger,
    get_conn,
    init_db,
    load_seen_nfts,
    get_last_block,
    save_progress,
    batch_insert_temp,
    load_blacklist,
    fetch_logs_http,
    extract_nfts,
)


async def main() -> None:
    logger.info("═══ 日志扫描器启动 ═══  链: %s | RPC: %s", CHAIN_NAME, RPC_URL)

    # ── 连接节点 ──────────────────────────────────────────────────────────────
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    if not await w3.is_connected():
        logger.error("无法连接 RPC 节点，请检查 RPC_URL")
        sys.exit(1)

    latest_block = await w3.eth.block_number
    logger.info("节点连接成功，当前最新区块: %d", latest_block)

    # ── 连接数据库 ────────────────────────────────────────────────────────────
    # conn      : 用于初始化、读取进度等一次性操作（主线程同步调用）
    # write_conn: 专用于热循环中的 DB 写操作，通过 asyncio.to_thread 在
    #             线程池中执行，避免阻塞事件循环
    conn       = get_conn()
    init_db(conn, CHAIN_NAME)
    write_conn = get_conn()

    # ── 创建 aiohttp Session（直接发 JSON-RPC，绕过 web3.py 内部串行化）──────────
    # limit=0：不限制连接池总数；每个请求独占一条连接，保证真正的 HTTP 并发
    rpc_connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    rpc_session   = aiohttp.ClientSession(connector=rpc_connector, trust_env=True)

    # ── 确定扫描范围（从新到旧：高区块 → 低区块）────────────────────────────────
    top_block  = END_BLOCK if END_BLOCK > 0 else latest_block
    stop_block = max(START_BLOCK, 0)

    last_saved = get_last_block(conn, CHAIN_NAME)
    cur_block  = (last_saved - 1) if last_saved is not None else top_block

    if cur_block < stop_block:
        logger.info("已扫描至底部区块（%d），无需继续", stop_block)
        conn.close()
        write_conn.close()
        return

    logger.info(
        "扫描方向: %d → %d（共约 %d 个区块，每批 %d 个）",
        cur_block, stop_block, cur_block - stop_block + 1, BLOCK_BATCH_SIZE,
    )

    # ── 预加载已有 NFT 集合（内存去重）──────────────────────────────────────────
    logger.info("预加载已有 NFT 集合...")
    seen: Set[Tuple[str, int]] = load_seen_nfts(conn, CHAIN_NAME)
    logger.info("数据库已有记录: %d 条", len(seen))

    blacklist = load_blacklist()
    if blacklist:
        logger.info("合约黑名单已加载: %d 个地址", len(blacklist))

    total_new = 0

    def _next_range() -> Optional[Tuple[int, int]]:
        """划出下一个待扫区块范围，更新 cur_block；无剩余范围时返回 None。"""
        nonlocal cur_block
        if cur_block < stop_block:
            return None
        from_b = max(cur_block - BLOCK_BATCH_SIZE + 1, stop_block)
        to_b   = cur_block
        cur_block = from_b - 1
        return from_b, to_b

    def _extract_records(logs) -> Tuple[List[Tuple], int, int]:
        """从日志列表提取去重后的 NFT 记录，同步更新 seen 集合（必须顺序调用）。"""
        records: List[Tuple] = []
        dup_skip = blacklist_skip = 0
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
                records.append((addr, str(tid), std, block_num))
        return records, dup_skip, blacklist_skip

    # ── 滑动窗口扫描（W 个批次同时在途）──────────────────────────────────────────
    #
    # 时间线（SCAN_WINDOW=3 示例）：
    # 处理顺序严格按入队顺序（FIFO），保证 seen 集合去重的正确性。
    # 每完成一批处理立即补入一个新 fetch，窗口始终保持满载。

    # Task 队列：每项为 (asyncio.Task, from_block, to_block)
    window: Deque[Tuple[asyncio.Task, int, int]] = deque()

    def _enqueue() -> bool:
        """尝试向窗口追加一个新 fetch 任务，返回是否成功追加。"""
        rng = _next_range()
        if rng is None:
            return False
        from_b, to_b = rng
        logger.info("► 预取区块 [%d - %d] ↓", to_b, from_b)
        window.append((
            asyncio.create_task(fetch_logs_http(rpc_session, RPC_URL, from_b, to_b)),
            from_b, to_b,
        ))
        return True

    # 初始填满窗口
    for _ in range(SCAN_WINDOW):
        if not _enqueue():
            break

    while window:
        task, from_block, _ = window.popleft()

        # 在等待队头任务的同时，补入新任务保持窗口满载
        _enqueue()

        # 等待队头批次就绪（此时窗口其余任务继续推进）
        logs = await task

        # seen 集合修改必须顺序执行，但纯计算开销很小
        records, dup_skip, blacklist_skip = _extract_records(logs)

        # DB 写操作放到线程池执行：事件循环不阻塞，
        # 窗口内其余 fetch 任务的 HTTP 响应可在此期间被处理
        inserted = await asyncio.to_thread(
            batch_insert_temp, write_conn, CHAIN_NAME, records
        )
        await asyncio.to_thread(save_progress, write_conn, CHAIN_NAME, from_block)

        total_new += inserted
        logger.info(
            "  本批: 新增候选 %d，落库 %d，已有跳过 %d，黑名单 %d，累计 %d",
            len(records), inserted, dup_skip, blacklist_skip, total_new,
        )

    logger.info("═══ 扫描完成 ═══  本次共写入 %d 条", total_new)
    write_conn.close()
    conn.close()
    await rpc_session.close()
    rpc_connector.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
