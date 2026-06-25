#!/usr/bin/env python3
"""
脚本二：Solana NFT Metadata 获取器（高性能版）

持续从临时表（temp_{chain}）读取 mint 地址，并发获取元数据后写入主表。

性能优化点：
  1. CONCURRENT_HELIUS=5  — 同时在途的 getAssetBatch 请求数（每批 1000 mint）
  2. 所有就绪批次一轮并发投入 _process_batch，helius_sem 在内部统一限流
  3. sem 仅持有 HTTP 调用期间，重试 sleep 期间释放，吞吐量 ≈ 并发数 / 平均延迟
  4. TCPConnector(limit=0) 不限连接数，由信号量在应用层统一控流

获取策略：
  1. 仅使用 Helius DAS API getAssetBatch（HELIUS_API_KEY 配置时启用，单批 1000 mint）
  - token_uri 非空 → 写主表；为空 → 不写但仍从临时表删除
  - 无 collection 的 mint → 以 mint 自身作为 singleton collection，避免错误聚合
  - data:image 开头的链上图像 mint → 不写主表，但仍从临时表删除
"""

import asyncio
import multiprocessing
import os
import signal
import socket
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, List, Optional, Tuple

import aiohttp

from common import (
    CHAIN_NAME,
    HELIUS_BATCH_SIZE,
    CONCURRENT_HELIUS,
    FETCH_IDLE_WAIT,
    FETCH_CLAIM_BATCH_SIZE,
    CLAIM_RETRY_AFTER_SECONDS,
    REQUEST_STARTUP_STAGGER_SECONDS,
    logger,
    get_conn,
    init_db,
    claim_pending_nfts,
    batch_insert_main,
    delete_temp_nfts,
    release_temp_claims,
    fetch_metadata_batch,
)


# token_uri 黑名单前缀：命中则不写主表
_INVALID_URI_PREFIXES = (
    "data:text/",
    "data:application/xml",
    "data:application/json",
)

FETCHER_WORKERS = max(int(os.getenv("FETCHER_WORKERS", "1")), 1)
WORKER_STARTUP_DELAY_SECONDS = max(
    0.0,
    float(os.getenv("WORKER_STARTUP_DELAY_SECONDS", str(REQUEST_STARTUP_STAGGER_SECONDS))),
)
WORKER_SHUTDOWN_GRACE_SECONDS = max(
    1.0,
    float(os.getenv("WORKER_SHUTDOWN_GRACE_SECONDS", "5")),
)


def _is_stop_requested(stop_event) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


@contextmanager
def _install_stop_signal_handlers(stop_event):
    handlers = {}

    def _handle_stop(signum, frame):
        if not _is_stop_requested(stop_event):
            logger.info("收到停止信号，停止认领新记录，等待当前批次和善后完成...")
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


async def _sleep_interruptibly(delay: float, stop_event=None, *, interval: float = 0.5) -> bool:
    remaining = max(0.0, delay)
    while remaining > 0:
        if _is_stop_requested(stop_event):
            return True
        step = min(interval, remaining)
        await asyncio.sleep(step)
        remaining -= step
    return _is_stop_requested(stop_event)


async def _maybe_wait_worker_startup(worker_index: int, stop_event=None) -> None:
    delay = WORKER_STARTUP_DELAY_SECONDS * max(worker_index - 1, 0)
    if delay <= 0:
        return
    logger.info("worker-%d 启动延迟 %.2f 秒，避免冷启动洪流", worker_index, delay)
    await _sleep_interruptibly(delay, stop_event)


async def _process_batch(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
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
        fetch_metadata_batch(session, helius_sem, chunk)
        for chunk in chunks
    ]))

    # 每项结果为 (collection, token_uri, image_url, name, symbol, metadata) 6-元组
    results: List[
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]
    ] = [
        item for chunk in chunk_results for item in chunk
    ]

    # 组装有效记录（包含 name / symbol / metadata），顺序与 EVM batch_insert_main 对齐
    inserts: List[Tuple] = []
    for (
        (_, mint, std, first_seen_slot),
        (collection_address, token_uri, image_url, name, symbol, metadata),
    ) in zip(pending, results):
        if not isinstance(image_url, str):
            continue
        if image_url and image_url.startswith("data:image"):
            continue
        if not token_uri or any(token_uri.startswith(p) for p in _INVALID_URI_PREFIXES):
            continue
        contract_address = collection_address or mint
        inserts.append((
            contract_address, mint, token_uri, image_url,
            name, symbol, metadata,
            std or "Metaplex", first_seen_slot,
        ))

    return inserts, all_ids


async def _fetch_all(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
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
            _process_batch(session, helius_sem, batch)
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
    """先写主表，成功后再删临时表；插入失败时保留临时表记录以便重试。"""
    inserted = await asyncio.to_thread(
        batch_insert_main, conn_insert, CHAIN_NAME, all_inserts
    )
    deleted = await asyncio.to_thread(
        delete_temp_nfts, conn_delete, CHAIN_NAME, all_ids
    )
    return inserted, deleted


def _build_worker_id(worker_index: int) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:worker-{worker_index}"


async def _worker_main(worker_index: int, stop_event=None) -> None:
    stop_event = stop_event or threading.Event()
    await _maybe_wait_worker_startup(worker_index, stop_event)
    worker_id = _build_worker_id(worker_index)
    logger.info(
        "═══ Solana Metadata 获取器启动 ═══  链: %s | worker=%s | "
        "claim_batch=%d | reclaim_after=%ds",
        CHAIN_NAME,
        worker_id,
        FETCH_CLAIM_BATCH_SIZE,
        CLAIM_RETRY_AFTER_SECONDS,
    )

    conn_claim = conn_insert = conn_delete = None
    pending_ids: List[int] = []
    try:
        conn_claim = get_conn()
        conn_insert = get_conn()
        conn_delete = get_conn()

        total_inserted = total_deleted = 0
        helius_sem = asyncio.Semaphore(CONCURRENT_HELIUS)
        connector = aiohttp.TCPConnector(
            limit=max(CONCURRENT_HELIUS * 2, 20),
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(
            connector=connector,
            trust_env=True,
            connector_owner=True,
        ) as session:
            while not _is_stop_requested(stop_event):
                pending = claim_pending_nfts(
                    conn_claim,
                    CHAIN_NAME,
                    worker_id=worker_id,
                    batch_size=FETCH_CLAIM_BATCH_SIZE,
                    reclaim_after_seconds=CLAIM_RETRY_AFTER_SECONDS,
                )
                pending_ids = [row[0] for row in pending]

                if _is_stop_requested(stop_event):
                    break

                if not pending:
                    logger.info(
                        "worker=%s 未认领到记录，等待 %d 秒后重试...",
                        worker_id,
                        FETCH_IDLE_WAIT,
                    )
                    if await _sleep_interruptibly(FETCH_IDLE_WAIT, stop_event):
                        break
                    continue

                logger.info("► worker=%s 认领并处理临时表 %d 条记录", worker_id, len(pending))
                try:
                    all_inserts, all_ids, total_raw, n_inserts = await _fetch_all(
                        session,
                        helius_sem,
                        [pending],
                    )
                    ins, del_ = await _do_write(
                        conn_insert,
                        conn_delete,
                        all_inserts,
                        all_ids,
                    )
                    pending_ids = []
                except Exception:
                    released = release_temp_claims(
                        conn_claim,
                        CHAIN_NAME,
                        pending_ids,
                        worker_id,
                    )
                    pending_ids = []
                    logger.exception(
                        "worker=%s 批次处理失败，已释放 %d 条认领记录等待重试",
                        worker_id,
                        released,
                    )
                    if await _sleep_interruptibly(1, stop_event):
                        break
                    continue

                total_inserted += ins
                total_deleted  += del_
                logger.info(
                    "  worker=%s 本批: 写入主表 %d，从临时表删除 %d（无效丢弃 %d）"
                    " | 累计写入 %d，累计删除 %d",
                    worker_id,
                    ins,
                    del_,
                    total_raw - n_inserts,
                    total_inserted,
                    total_deleted,
                )

        if _is_stop_requested(stop_event):
            logger.info("worker=%s 已停止认领新记录，当前批次和善后已完成", worker_id)
    finally:
        if pending_ids and conn_claim is not None:
            try:
                released = release_temp_claims(conn_claim, CHAIN_NAME, pending_ids, worker_id)
                logger.info("worker=%s 退出前释放 %d 条未完成认领记录", worker_id, released)
            except Exception:
                logger.exception("worker=%s 退出释放认领记录失败", worker_id)
        for conn in (conn_delete, conn_insert, conn_claim):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def _run_worker_process(worker_index: int, stop_event=None) -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    local_stop_event = stop_event or threading.Event()
    with _install_stop_signal_handlers(local_stop_event):
        asyncio.run(_worker_main(worker_index, stop_event=local_stop_event))


def _wait_for_workers(workers: List[multiprocessing.Process], stop_event) -> None:
    deadline: Optional[float] = None
    while any(proc.is_alive() for proc in workers):
        for proc in workers:
            proc.join(timeout=0.2)
        if _is_stop_requested(stop_event):
            if deadline is None:
                deadline = time.monotonic() + WORKER_SHUTDOWN_GRACE_SECONDS
            elif time.monotonic() >= deadline:
                break

    lingering = [proc for proc in workers if proc.is_alive()]
    if lingering:
        logger.info("仍有 %d 个 metadata worker 未在宽限期内退出，执行强制停止", len(lingering))
        for proc in lingering:
            proc.terminate()
        for proc in lingering:
            proc.join(timeout=1)


def main() -> None:
    conn = get_conn()
    try:
        init_db(conn, CHAIN_NAME)
    finally:
        conn.close()

    stop_event = multiprocessing.Event()
    with _install_stop_signal_handlers(stop_event):
        if FETCHER_WORKERS <= 1:
            _run_worker_process(1, stop_event)
            return

        logger.info(
            "启动 %d 个 Solana metadata worker 进程；每进程并发: Helius=%d",
            FETCHER_WORKERS,
            CONCURRENT_HELIUS,
        )
        workers = [
            multiprocessing.Process(
                target=_run_worker_process,
                args=(index, stop_event),
                name=f"{CHAIN_NAME}-metadata-{index}",
            )
            for index in range(1, FETCHER_WORKERS + 1)
        ]
        for proc in workers:
            proc.start()

        try:
            _wait_for_workers(workers, stop_event)
        except KeyboardInterrupt:
            stop_event.set()
            _wait_for_workers(workers, stop_event)


if __name__ == "__main__":
    main()
