#!/usr/bin/env python3
"""
脚本一：Solana NFT 发现器（GPA 快照扫描）

核心策略：使用 Helius getProgramAccountsV2 直接枚举 Token Metadata Program 下的
所有账户，每个账户对应一个 Solana NFT 的 Metaplex Metadata PDA，包含 mint 地址。

相比原始区块扫描方式：
  旧：遍历所有 Slot 的 getBlock → postTokenBalances → 过滤 decimals=0, amount=1
  新：getProgramAccountsV2(Token Metadata Program) → 直接拿全量 NFT mint 地址

  速度对比（1 亿 NFT，4 亿历史 Slot）：
    旧方案 ≈ 320 万次 HTTP 请求，耗时数十小时
    新方案 ≈ 10 万次分页请求，耗时约 1 小时（含流水线并发）

技术细节：
  - dataSlice {offset:33, length:32}：只传 mint 字段的 32 字节，节省约 95% 带宽
  - memcmp filter {offset:0, bytes:"5"}：只返回 key=4（MetadataV1）的账户
  - paginationKey 游标：分页拉取，支持断点续扫
  - changedSinceSlot：全量扫描完成后，后续只拉增量变化账户，并支持分页断点续扫
  - 流水线：DB 写入与下一页 HTTP 请求并发执行，消除等待空档

配置参数（.env）：
  HELIUS_API_KEY : Helius API Key（必填，getProgramAccountsV2 是 Helius 私有方法）
  GPA_PAGE_SIZE  : 每页账户数（默认 1000，最大 10000）
  GPA_SINCE_SLOT : 增量起始 Slot（0 = 全量/自动；>0 = 指定增量起点）
"""

import asyncio
import signal
import sys
import threading
from contextlib import contextmanager
from typing import List, NamedTuple, Optional, Tuple

import aiohttp

from common import (
    CHAIN_NAME,
    HELIUS_RPC_URL,
    GPA_PAGE_SIZE,
    GPA_SINCE_SLOT,
    logger,
    get_conn,
    init_db,
    batch_insert_temp,
    get_gpa_progress,
    save_gpa_progress,
    fetch_gpa_page,
    get_latest_slot,
)


class _ScanState(NamedTuple):
    since_slot: int
    resume_key: Optional[str]
    total_pages_base: int
    mode: str


def _is_stop_requested(stop_event) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


@contextmanager
def _install_stop_signal_handlers(stop_event):
    handlers = {}

    def _handle_stop(signum, frame):
        if not _is_stop_requested(stop_event):
            logger.info("收到停止信号，停止领取新 GPA 页，当前页处理并保存断点后退出...")
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


def _resolve_scan_state(
    *,
    env_since: int,
    saved_key: Optional[str],
    saved_since_slot: int,
    saved_pages: int,
) -> _ScanState:
    if env_since > 0:
        if saved_since_slot > env_since:
            if saved_key is not None:
                return _ScanState(
                    saved_since_slot,
                    saved_key,
                    saved_pages,
                    f"增量断点续扫（DB since={saved_since_slot}，忽略旧 .env since={env_since}）",
                )
            return _ScanState(
                saved_since_slot,
                None,
                0,
                f"增量（DB 记录 since={saved_since_slot}，忽略旧 .env since={env_since}）",
            )
        if saved_key is not None and saved_since_slot == env_since:
            return _ScanState(
                env_since,
                saved_key,
                saved_pages,
                f"增量断点续扫（since={env_since}）",
            )
        return _ScanState(
            env_since,
            None,
            0,
            f"增量（.env 指定 since={env_since}）",
        )

    if saved_key is not None:
        since_slot = saved_since_slot or 0
        mode = (
            f"增量断点续扫（since={since_slot}）"
            if since_slot > 0
            else "全量断点续扫"
        )
        return _ScanState(since_slot, saved_key, saved_pages, mode)

    if saved_since_slot > 0:
        return _ScanState(
            saved_since_slot,
            None,
            0,
            f"增量（DB 记录 since={saved_since_slot}）",
        )

    return _ScanState(0, None, 0, "全量")


def _progress_since_slot(
    *,
    current_since_slot: int,
    latest_slot: int,
    next_key: Optional[str],
) -> int:
    if next_key is not None:
        return current_since_slot
    if latest_slot > 0:
        return latest_slot
    return current_since_slot


async def main(stop_event=None) -> None:
    stop_event = stop_event or threading.Event()
    logger.info("═══ Solana NFT 发现器（GPA）启动 ═══  链: %s", CHAIN_NAME)

    # ── 前置检查 ──────────────────────────────────────────────────────────────
    if not HELIUS_RPC_URL:
        logger.error(
            "HELIUS_API_KEY 未配置！getProgramAccountsV2 是 Helius 私有方法，"
            "请在 .env 中填写 HELIUS_API_KEY。"
        )
        return

    logger.info("Helius RPC: %s", HELIUS_RPC_URL.split("?")[0])
    logger.info("每页大小: %d", GPA_PAGE_SIZE)

    # ── 数据库连接 ────────────────────────────────────────────────────────────
    conn       = get_conn()
    write_conn = get_conn()
    init_db(conn, CHAIN_NAME)

    # ── 读取断点 / 增量起始 Slot ──────────────────────────────────────────────
    saved_key, saved_since_slot, saved_pages = get_gpa_progress(conn, CHAIN_NAME)

    scan_state = _resolve_scan_state(
        env_since=GPA_SINCE_SLOT,
        saved_key=saved_key,
        saved_since_slot=saved_since_slot,
        saved_pages=saved_pages,
    )
    since_slot       = scan_state.since_slot
    resume_key       = scan_state.resume_key
    total_pages_base = scan_state.total_pages_base
    logger.info("扫描起点: %s", scan_state.mode)

    # ── 获取当前最新 Slot（全量完成后存入 since_slot 供下次增量用）──────────
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
    session   = aiohttp.ClientSession(connector=connector, trust_env=True)
    prefetch_task: Optional[asyncio.Task] = None

    try:
        latest_slot = await get_latest_slot(session, HELIUS_RPC_URL)
        if latest_slot == 0:
            logger.warning("无法获取最新 Slot，将在全量扫描完成后以 0 存入 since_slot")

        mode_str = (
            f"增量（since={since_slot}）" if since_slot > 0
            else "全量"
        )
        logger.info("扫描模式: %s | 当前最新 Slot: %d", mode_str, latest_slot)

        # ── 流水线主循环 ──────────────────────────────────────────────────────────
        #
        # 由于 GPA 分页是串行的（下一页 key 来自上一页响应），
        # 无法真正并行；但可以流水线：
        #   拉取第 N+1 页（HTTP）与处理第 N 页（去重 + 写库）并发执行。
        #
        #   时间线：
        #     HTTP[0] → await → process[0] + HTTP[1] 并发 → await HTTP[1]
        #                       → process[1] + HTTP[2] 并发 → ...
        #
        total_new    = 0
        total_pages  = total_pages_base
        current_key  = resume_key

        # 启动第一页的预取任务
        prefetch_task = asyncio.create_task(
            fetch_gpa_page(
                session, HELIUS_RPC_URL, GPA_PAGE_SIZE,
                pagination_key=current_key,
                changed_since_slot=since_slot or None,
            )
        )

        while True:
            # 等待当前页数据就绪；异常会退出进程且不会保存伪完成进度。
            mints, next_key = await prefetch_task

            # 立即启动下一页的预取（与本页处理并发）
            if next_key is not None:
                prefetch_task = asyncio.create_task(
                    fetch_gpa_page(
                        session, HELIUS_RPC_URL, GPA_PAGE_SIZE,
                        pagination_key=next_key,
                        changed_since_slot=since_slot or None,
                    )
                )

            total_pages += 1

            # ── 处理本页 mint ─────────────────────────────────────────────────
            records: List[Tuple] = [(mint, "Metaplex", 0) for mint in mints]

            # ── 写库（线程池，不阻塞事件循环）───────────────────────────────────
            inserted = await asyncio.to_thread(
                batch_insert_temp, write_conn, CHAIN_NAME, records
            )

            # 保存断点：若未到最后一页，记录 next_key 用于意外中断后续扫
            await asyncio.to_thread(
                save_gpa_progress,
                write_conn, CHAIN_NAME,
                next_key,       # None 表示全量已完成
                _progress_since_slot(
                    current_since_slot=since_slot,
                    latest_slot=latest_slot,
                    next_key=next_key,
                ),
                total_pages,
            )

            del records

            total_new += inserted
            logger.info(
                "  第%d页：本页 %d 个 mint，落库 %d | 累计新增 %d",
                total_pages, len(mints), inserted, total_new,
            )

            # ── 判断是否到最后一页 ────────────────────────────────────────────
            if next_key is None:
                break
            if _is_stop_requested(stop_event):
                logger.info(
                    "═══ 扫描中断退出 ═══  当前页已落库并保存 paginationKey=%s",
                    next_key,
                )
                break

            current_key = next_key

        # ── 全量扫描完成后的日志提示 ──────────────────────────────────────────────
        if since_slot == 0 and latest_slot > 0:
            logger.info(
                "═══ 全量扫描完成 ═══  共处理 %d 页，本次新增 %d 条 mint",
                total_pages, total_new,
            )
            logger.info(
                "已将 since_slot=%d 写入 DB。下次运行将自动切换为增量模式，"
                "只拉取该 Slot 之后新增/变化的账户。",
                latest_slot,
            )
        else:
            logger.info(
                "═══ 扫描完成 ═══  共处理 %d 页，本次新增 %d 条 mint",
                total_pages, total_new,
            )
    finally:
        if prefetch_task is not None:
            if not prefetch_task.done():
                prefetch_task.cancel()
            await asyncio.gather(prefetch_task, return_exceptions=True)
        await session.close()
        await connector.close()
        write_conn.close()
        conn.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    _stop_event = threading.Event()
    with _install_stop_signal_handlers(_stop_event):
        asyncio.run(main(stop_event=_stop_event))
