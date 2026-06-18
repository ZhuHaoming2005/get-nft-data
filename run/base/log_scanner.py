#!/usr/bin/env python3
"""
脚本一：链上日志扫描器

扫描 EVM 链上的 NFT Transfer 事件，将
  (contract_address, token_id, token_standard, first_seen_block)
写入数据库，token_uri / image_uri 初始留空，由 metadata_fetcher.py 异步填充。

支持断点续扫（scan_progress 表记录进度）。
"""

import asyncio
import signal
import sys
import threading
from collections import deque
from contextlib import contextmanager
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
    REQUEST_STARTUP_STAGGER_SECONDS,
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


def _is_stop_requested(stop_event) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


@contextmanager
def _install_stop_signal_handlers(stop_event):
    handlers = {}

    def _handle_stop(signum, frame):
        if not _is_stop_requested(stop_event):
            logger.info("收到停止信号，停止领取新区块，等待当前在途批次完成...")
        stop_event.set()

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _handle_stop)
        except (OSError, RuntimeError, ValueError):
            continue
    try:
        yield
    finally:
        for sig, previous in handlers.items():
            try:
                signal.signal(sig, previous)
            except (OSError, RuntimeError, ValueError):
                continue


def _close_sync_resource(resource) -> None:
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


async def _close_async_resource(resource) -> None:
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        result = close()
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        pass


async def main(stop_event=None) -> None:
    stop_event = stop_event or threading.Event()
    logger.info("═══ 日志扫描器启动 ═══  链: %s | RPC: %s", CHAIN_NAME, RPC_URL)
    conn = None
    write_conn = None
    rpc_connector = None
    rpc_session = None
    window: Deque[Tuple[asyncio.Task, int, int]] = deque()

    try:
        w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
        if not await w3.is_connected():
            logger.error("无法连接 RPC 节点，请检查 RPC_URL")
            sys.exit(1)

        latest_block = await w3.eth.block_number
        logger.info("节点连接成功，当前最新区块: %d", latest_block)

        conn = get_conn()
        init_db(conn, CHAIN_NAME)
        write_conn = get_conn()

        rpc_connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        rpc_session = aiohttp.ClientSession(connector=rpc_connector, trust_env=True)

        top_block = END_BLOCK if END_BLOCK > 0 else latest_block
        stop_block = max(START_BLOCK, 0)

        last_saved = get_last_block(conn, CHAIN_NAME)
        cur_block = (last_saved - 1) if last_saved is not None else top_block

        if cur_block < stop_block:
            logger.info("已扫描至底部区块（%d），无需继续", stop_block)
            return

        logger.info(
            "扫描方向: %d → %d（共约 %d 个区块，每批 %d 个）",
            cur_block, stop_block, cur_block - stop_block + 1, BLOCK_BATCH_SIZE,
        )

        logger.info("预加载已有 NFT 集合...")
        seen: Set[Tuple[str, int]] = load_seen_nfts(conn, CHAIN_NAME)
        logger.info("数据库已有记录: %d 条", len(seen))

        blacklist = load_blacklist()
        if blacklist:
            logger.info("合约黑名单已加载: %d 个地址", len(blacklist))

        total_new = 0
        enqueued_batches = 0

        def _next_range() -> Optional[Tuple[int, int]]:
            nonlocal cur_block
            if cur_block < stop_block:
                return None
            from_b = max(cur_block - BLOCK_BATCH_SIZE + 1, stop_block)
            to_b = cur_block
            cur_block = from_b - 1
            return from_b, to_b

        def _extract_records(logs) -> Tuple[List[Tuple], int, int]:
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

        def _persist_batch(records: List[Tuple], from_block: int) -> int:
            inserted = batch_insert_temp(write_conn, CHAIN_NAME, records)
            save_progress(write_conn, CHAIN_NAME, from_block)
            return inserted

        def _enqueue() -> bool:
            nonlocal enqueued_batches
            if _is_stop_requested(stop_event):
                return False
            rng = _next_range()
            if rng is None:
                return False
            from_b, to_b = rng
            startup_delay_seconds = (
                REQUEST_STARTUP_STAGGER_SECONDS * enqueued_batches
                if enqueued_batches < SCAN_WINDOW
                else 0.0
            )
            enqueued_batches += 1
            logger.info("► 预取区块 [%d - %d] ↓", to_b, from_b)
            window.append((
                asyncio.create_task(
                    fetch_logs_http(
                        rpc_session,
                        RPC_URL,
                        from_b,
                        to_b,
                        startup_delay_seconds=startup_delay_seconds,
                    )
                ),
                from_b, to_b,
            ))
            return True

        for _ in range(SCAN_WINDOW):
            if not _enqueue():
                break

        while window:
            task, from_block, _ = window.popleft()
            if not _is_stop_requested(stop_event):
                _enqueue()
            logs = await task
            records, dup_skip, blacklist_skip = _extract_records(logs)
            inserted = await asyncio.to_thread(_persist_batch, records, from_block)
            total_new += inserted
            logger.info(
                "  本批: 新增候选 %d，落库 %d，已有跳过 %d，黑名单 %d，累计 %d",
                len(records), inserted, dup_skip, blacklist_skip, total_new,
            )

        if _is_stop_requested(stop_event):
            logger.info("═══ 扫描中断退出 ═══  已停止领取新区块，在途批次已收尾，共写入 %d 条", total_new)
        else:
            logger.info("═══ 扫描完成 ═══  本次共写入 %d 条", total_new)
    finally:
        for task, _, _ in list(window):
            if not task.done():
                task.cancel()
        if window:
            await asyncio.gather(*(task for task, _, _ in window), return_exceptions=True)
        await _close_async_resource(rpc_session)
        _close_sync_resource(rpc_connector)
        _close_sync_resource(write_conn)
        _close_sync_resource(conn)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    _stop_event = threading.Event()
    with _install_stop_signal_handlers(_stop_event):
        asyncio.run(main(stop_event=_stop_event))
