#!/usr/bin/env python3
"""
脚本二：Metadata 获取器

持续从临时表（temp_{chain}）读取扫描阶段原始记录，通过 Alchemy API 批量获取元数据：
  - 获取成功（token_uri 非空且合法）→ 写入主表 nft_assets_{chain}
  - 获取失败（Alchemy 空 + 链上合约也为空）→ 不写主表
  - 无论结果如何，处理完的记录全部从临时表删除
  - 临时表记录不足 100 条时等待 FETCH_IDLE_WAIT 秒后重试

全程使用异步加速网络 IO（Alchemy 批量 + RPC 并发）。
"""

import asyncio
import multiprocessing
import os
import socket
import sys
from typing import List, Optional, Set, Tuple

import aiohttp
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

from common import (
    CHAIN_NAME,
    RPC_URL,
    ALCHEMY_BATCH_SIZE,
    CONCURRENT_ALCHEMY,
    CONCURRENT_RPC,
    RPC_BATCH_SIZE,
    FETCH_IDLE_WAIT,
    FETCH_CLAIM_BATCH_SIZE,
    CLAIM_RETRY_AFTER_SECONDS,
    logger,
    get_conn,
    init_db,
    ensure_temp_table_claim_columns,
    claim_pending_nfts,
    batch_insert_main,
    delete_temp_nfts,
    release_temp_claims,
    delete_contract_nfts,
    append_blacklist_env,
    fetch_alchemy_batch,
    fetch_token_uri_batch,
    fetch_token_uri,
    _decode_inline_image,
    replace_token_id_placeholder,
    fix_token_id_placeholders,
)


# tokenUri 黑名单前缀：匹配则视为无效，不写入主表
_INVALID_URI_PREFIXES = ("api.tierlock.com/uri/",)
FETCHER_WORKERS = max(int(os.getenv("FETCHER_WORKERS", "1")), 1)


async def _process_batch(
    session: aiohttp.ClientSession,
    w3: AsyncWeb3,
    alchemy_sem: asyncio.Semaphore,
    uri_sem: asyncio.Semaphore,
    pending: List[Tuple],
) -> Tuple[List[Tuple], List[int], Set[str]]:
    """
    对一批临时表记录执行元数据获取，返回 (inserts, all_ids, onchain_image_contracts)：
      inserts:               写入主表的有效记录列表
      all_ids:               全部从临时表删除的 id 列表
      onchain_image_contracts: 链上存储图像的合约地址集合（DeFi 合约）：
                               - Alchemy 返回的 image_url 以 'data:image' 开头，或
                               - token_uri 为 data:application/ 解码后 image 仍以 'data:image' 开头
                               此类合约整体加入黑名单并清库
    """
    # pending 每项：(id, contract_address, token_id, token_standard, first_seen_block)
    tokens  = [(addr, tid, std) for _, addr, tid, std, _ in pending]
    all_ids = [row[0] for row in pending]

    # ── 步骤1：并发批量调 Alchemy API ──────────────────────────────────────────
    chunks = [
        tokens[i: i + ALCHEMY_BATCH_SIZE]
        for i in range(0, len(tokens), ALCHEMY_BATCH_SIZE)
    ]
    chunk_results = await asyncio.gather(*[
        fetch_alchemy_batch(session, alchemy_sem, chunk)
        for chunk in chunks
    ]) if chunks else []

    alchemy_results: List[
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[object]]
    ] = [
        item for chunk in chunk_results for item in chunk
    ]

    # ── 步骤2：tokenUri 为空时 fallback 到链上合约（异步并发）──────────────────
    fallback_indices = [i for i, (uri, _, _, _, _) in enumerate(alchemy_results) if not uri]
    if fallback_indices:
        fallback_tokens = [tokens[i] for i in fallback_indices]
        rpc_chunks = [
            fallback_tokens[i: i + RPC_BATCH_SIZE]
            for i in range(0, len(fallback_tokens), RPC_BATCH_SIZE)
        ]
        chunk_uris = await asyncio.gather(*[
            fetch_token_uri_batch(session, RPC_URL, uri_sem, chunk)
            for chunk in rpc_chunks
        ])
        fallback_uris = [uri for chunk in chunk_uris for uri in chunk]

        missing_positions = [pos for pos, uri in enumerate(fallback_uris) if not uri]
        if missing_positions:
            legacy_uris: List[Optional[str]] = list(await asyncio.gather(*[
                fetch_token_uri(
                    w3,
                    uri_sem,
                    fallback_tokens[pos][0],
                    fallback_tokens[pos][1],
                    fallback_tokens[pos][2],
                )
                for pos in missing_positions
            ]))
            for pos, uri in zip(missing_positions, legacy_uris):
                fallback_uris[pos] = uri

        for idx, chain_uri in zip(fallback_indices, fallback_uris):
            _, img, name, symbol, metadata = alchemy_results[idx]
            alchemy_results[idx] = (chain_uri, img, name, symbol, metadata)

    # ── 步骤2.5：解码链上内嵌 data URI，提取 image_url ──────────────────────────
    # token_uri 为 data:application/... 格式时（链上存储的 JSON 元数据），
    # base64 解码后提取 image 字段写回 alchemy_results，
    # 供后续步骤3统一检测 data:image 链上图像并加入黑名单。
    for i, (token_uri, image_url, name, symbol, metadata) in enumerate(alchemy_results):
        if image_url is not None and not isinstance(image_url, str):
            image_url = None
            alchemy_results[i] = (token_uri, image_url, name, symbol, metadata)
        if token_uri and token_uri.startswith("data:application/") and image_url is None:
            decoded_img = _decode_inline_image(token_uri)
            if decoded_img:
                alchemy_results[i] = (token_uri, decoded_img, name, symbol, metadata)

    # ── 步骤3：检测链上存储图像（data:image）的 DeFi 合约 ───────────────────────
    # 若某合约任意 token 的 image_url 以 'data:image' 开头（无论来自 Alchemy
    # 直接返回，还是步骤2.5从链上 JSON 元数据解码所得），均说明该合约将图像
    # 直接编码在链上，属于 DeFi/功能性合约，整体加入黑名单。
    onchain_image_contracts: Set[str] = set()
    for (_, addr, _, _, _), (_, image_url, _, _, _) in zip(pending, alchemy_results):
        if isinstance(image_url, str) and image_url.startswith("data:image"):
            onchain_image_contracts.add(addr)

    # ── 步骤4：有效记录组装为主表 insert 列表 ───────────────────────────────────
    inserts: List[Tuple] = []

    for (_, addr, tid, std, first_seen_block), (
        token_uri,
        image_url,
        contract_name,
        contract_symbol,
        raw_metadata,
    ) in zip(
        pending, alchemy_results
    ):
        # 跳过链上存储图像的合约（整个合约列入黑名单）
        if addr in onchain_image_contracts:
            continue

        if not token_uri or any(token_uri.startswith(p) for p in _INVALID_URI_PREFIXES):
            continue  # 无效，不写主表（但仍从临时表删除）

        # ERC-1155 {id} 占位符展开：替换为 64 位零填充小写十六进制 token ID
        token_uri = replace_token_id_placeholder(token_uri, tid)

        inserts.append(
            (
                addr,
                str(tid),
                token_uri,
                image_url,
                contract_name,
                contract_symbol,
                raw_metadata,
                std,
                first_seen_block,
            )
        )

    return inserts, all_ids, onchain_image_contracts


def _build_worker_id(worker_index: int) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:worker-{worker_index}"


async def _worker_main(worker_index: int) -> None:
    worker_id = _build_worker_id(worker_index)
    logger.info(
        "═══ Metadata 获取器启动 ═══  链: %s | worker=%s | RPC: %s | claim_batch=%d | reclaim_after=%ds",
        CHAIN_NAME,
        worker_id,
        RPC_URL,
        FETCH_CLAIM_BATCH_SIZE,
        CLAIM_RETRY_AFTER_SECONDS,
    )

    # ── 连接节点（用于链上合约 fallback）────────────────────────────────────────
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    if not await w3.is_connected():
        logger.error("无法连接 RPC 节点，请检查 RPC_URL")
        sys.exit(1)
    logger.info("RPC 节点连接成功")

    # ── 连接数据库 ────────────────────────────────────────────────────────────
    conn = get_conn()
    init_db(conn, CHAIN_NAME)
    ensure_temp_table_claim_columns(conn, CHAIN_NAME)

    # 一次性修复：将主表中历史写入但未展开 {id} 的 ERC-1155 token URI 补全
    fixed = fix_token_id_placeholders(conn, CHAIN_NAME)
    if fixed:
        logger.info("历史记录 {id} 占位符修复完成，共更新 %d 条", fixed)

    total_inserted = total_deleted = 0
    alchemy_sem = asyncio.Semaphore(CONCURRENT_ALCHEMY)
    uri_sem = asyncio.Semaphore(CONCURRENT_RPC)
    connector = aiohttp.TCPConnector(limit=max(CONCURRENT_ALCHEMY * 2, 20), ttl_dns_cache=300)

    # ── 主循环：持续消费临时表 ────────────────────────────────────────────────
    async with aiohttp.ClientSession(
        trust_env=True,
        connector=connector,
        connector_owner=True,
    ) as session:
        while True:
            pending = claim_pending_nfts(
                conn,
                CHAIN_NAME,
                worker_id=worker_id,
                batch_size=FETCH_CLAIM_BATCH_SIZE,
                reclaim_after_seconds=CLAIM_RETRY_AFTER_SECONDS,
            )

            if not pending:
                logger.info(
                    "worker=%s 未认领到记录，等待 %d 秒后重试...",
                    worker_id,
                    FETCH_IDLE_WAIT,
                )
                await asyncio.sleep(FETCH_IDLE_WAIT)
                continue

            logger.info("► worker=%s 认领并处理临时表 %d 条记录", worker_id, len(pending))
            try:
                inserts, all_ids, onchain_image_contracts = await _process_batch(
                    session,
                    w3,
                    alchemy_sem,
                    uri_sem,
                    pending,
                )
            except Exception:
                released = release_temp_claims(
                    conn,
                    CHAIN_NAME,
                    [row[0] for row in pending],
                    worker_id,
                )
                logger.exception(
                    "worker=%s 批次处理失败，已释放 %d 条认领记录等待重试",
                    worker_id,
                    released,
                )
                await asyncio.sleep(1)
                continue
            blacklisted_rows = sum(
                1 for _, addr, _, _, _ in pending if addr in onchain_image_contracts
            )

            # ── 处理链上存储图像合约（data:image）────────────────────────────
            if onchain_image_contracts:
                logger.info(
                    "  检测到 %d 个链上存储图像合约（image_uri 以 data:image 开头），"
                    "加入黑名单: %s",
                    len(onchain_image_contracts), sorted(onchain_image_contracts),
                )
                # 1. 更新 .env 黑名单（持久化，重启后生效）
                append_blacklist_env(onchain_image_contracts)
                # 2. 清除数据库中该合约的所有历史记录（主表 + 临时表）
                main_del, temp_del = delete_contract_nfts(
                    conn, CHAIN_NAME, onchain_image_contracts
                )
                logger.info(
                    "  黑名单清库完成: 主表删除 %d 条，临时表删除 %d 条",
                    main_del, temp_del,
                )
                # 重新过滤 all_ids（临时表中属于黑名单合约的记录已在上一步按地址删除）
                blacklisted_set = onchain_image_contracts
                all_ids = [
                    rid for rid, row in zip(all_ids, pending)
                    if row[1] not in blacklisted_set
                ]

            # 有效记录写主表
            inserted = batch_insert_main(conn, CHAIN_NAME, inserts)
            # 全部处理过的记录从临时表删除
            deleted = delete_temp_nfts(conn, CHAIN_NAME, all_ids)
            discarded = len(pending) - len(inserts) - blacklisted_rows

            total_inserted += inserted
            total_deleted += deleted
            logger.info(
                "  worker=%s 本批: 写入主表 %d，从临时表删除 %d（无效丢弃 %d，黑名单合约 %d 个）"
                " | 累计写入 %d，累计删除 %d",
                worker_id,
                inserted, deleted,
                discarded,
                len(onchain_image_contracts),
                total_inserted, total_deleted,
            )


def _run_worker_process(worker_index: int) -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_worker_main(worker_index))


def main() -> None:
    if FETCHER_WORKERS <= 1:
        _run_worker_process(1)
        return

    logger.info(
        "启动 %d 个 metadata worker 进程；每进程并发: Alchemy=%d, RPC=%d",
        FETCHER_WORKERS,
        CONCURRENT_ALCHEMY,
        CONCURRENT_RPC,
    )
    workers = [
        multiprocessing.Process(
            target=_run_worker_process,
            args=(index,),
            name=f"{CHAIN_NAME}-metadata-{index}",
        )
        for index in range(1, FETCHER_WORKERS + 1)
    ]
    for proc in workers:
        proc.start()

    try:
        for proc in workers:
            proc.join()
    except KeyboardInterrupt:
        logger.info("收到中断信号，准备停止所有 metadata worker...")
        for proc in workers:
            if proc.is_alive():
                proc.terminate()
        for proc in workers:
            proc.join(timeout=5)


if __name__ == "__main__":
    main()
