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
  - changedSinceSlot：全量扫描完成后，后续只拉增量变化账户（极快）
  - 流水线：DB 写入与下一页 HTTP 请求并发执行，消除等待空档

配置参数（.env）：
  HELIUS_API_KEY : Helius API Key（必填，getProgramAccountsV2 是 Helius 私有方法）
  GPA_PAGE_SIZE  : 每页账户数（默认 1000，最大 10000）
  GPA_SINCE_SLOT : 增量起始 Slot（0 = 全量；首次运行留 0，之后由脚本自动管理）
"""

import asyncio
import sys
from typing import List, Optional, Tuple

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


async def main() -> None:
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

    # 优先级：.env GPA_SINCE_SLOT > DB 保存的 since_slot
    # GPA_SINCE_SLOT=0 且 DB 有记录时，说明上次已完成全量，走增量模式
    env_since = GPA_SINCE_SLOT  # 来自 .env，0 = 不强制指定
    if env_since > 0:
        # .env 显式指定了增量起点，忽略 DB 保存值
        since_slot       = env_since
        resume_key       = None      # 增量模式不使用游标断点
        total_pages_base = 0
        logger.info("增量模式（.env 指定）: changedSinceSlot=%d", since_slot)
    elif saved_key is not None:
        # 上次中断，从游标断点继续全量扫描
        since_slot       = 0
        resume_key       = saved_key
        total_pages_base = saved_pages
        logger.info(
            "断点续扫：从 paginationKey 继续全量扫描（已完成 %d 页）",
            total_pages_base,
        )
    elif saved_since_slot > 0:
        # 上次全量已完成，进入增量模式
        since_slot       = saved_since_slot
        resume_key       = None
        total_pages_base = 0
        logger.info("增量模式（DB 记录）: changedSinceSlot=%d", since_slot)
    else:
        # 首次运行，全量扫描
        since_slot       = 0
        resume_key       = None
        total_pages_base = 0
        logger.info("首次运行：开始全量扫描")

    # ── 获取当前最新 Slot（全量完成后存入 since_slot 供下次增量用）──────────
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
    session   = aiohttp.ClientSession(connector=connector, trust_env=True)

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
    prefetch_task: asyncio.Task = asyncio.create_task(
        fetch_gpa_page(
            session, HELIUS_RPC_URL, GPA_PAGE_SIZE,
            pagination_key=current_key,
            changed_since_slot=since_slot or None,
        )
    )

    while True:
        # 等待当前页数据就绪
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
        records: List[Tuple] = [(mint, 1, "Metaplex", 0) for mint in mints]

        # ── 写库（线程池，不阻塞事件循环）───────────────────────────────────
        inserted = await asyncio.to_thread(
            batch_insert_temp, write_conn, CHAIN_NAME, records
        )

        # 保存断点：若未到最后一页，记录 next_key 用于意外中断后续扫
        await asyncio.to_thread(
            save_gpa_progress,
            write_conn, CHAIN_NAME,
            next_key,       # None 表示全量已完成
            latest_slot if next_key is None else 0,
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

    await session.close()
    await connector.close()
    write_conn.close()
    conn.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
