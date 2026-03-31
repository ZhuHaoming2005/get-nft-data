-- ===========================================================================
-- NFT 跨链重复数量分析
-- 对比 nft_assets_ethereum（以太坊）与 nft_assets_solana（Solana）
--
-- 执行顺序：
--   Step 1  建立 normalize_url() 函数（幂等）
--   Step 2  构建两个链的规范化临时索引表
--   Step 3  统计查询（4 个维度）
--
-- 修改表名：
--   tbl_eth  默认 nft_assets_ethereum
--   tbl_sol  默认 nft_assets_solana
--
-- 在 psql 中可通过 \set 覆盖：
--   \set tbl_eth nft_assets_polygon
--   \i dedup_analysis.sql
--
-- 在其他客户端（DBeaver / DataGrip 等）直接修改下方两行即可。
-- ===========================================================================

-- ── 配置：两张对比表 ──────────────────────────────────────────────────────────
\set tbl_eth 'nft_assets_ethereum'
\set tbl_sol 'nft_assets_solana'


-- ===========================================================================
-- Step 1：URL 规范化函数（IMMUTABLE，结果可被索引缓存）
-- ===========================================================================
CREATE OR REPLACE FUNCTION normalize_url(url TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE STRICT
AS $$
DECLARE
    s   TEXT;
    lo  TEXT;
    cid TEXT;
    tx  TEXT;
BEGIN
    s  := trim(url);
    lo := lower(s);

    -- 跳过占位符（nano/null/none/undefined 等）
    IF lo = ANY(ARRAY[
        'nano','null','none','undefined','n/a','na',
        '-', '.', 'false', 'true', '0'
    ]) OR lo LIKE 'data:%' THEN
        RETURN NULL;
    END IF;

    -- ipfs://
    IF lo LIKE 'ipfs://%' THEN
        cid := substring(s FROM 8);
        IF lower(cid) LIKE 'ipfs/%' THEN cid := substring(cid FROM 6); END IF;
        cid := btrim(split_part(split_part(cid, '?', 1), '#', 1), '/');
        RETURN CASE WHEN cid <> '' THEN 'ipfs:' || cid ELSE NULL END;
    END IF;

    -- ar://
    IF lo LIKE 'ar://%' THEN
        tx := btrim(split_part(split_part(substring(s FROM 6), '?', 1), '#', 1), '/');
        RETURN CASE WHEN tx <> '' THEN 'ar:' || tx ELSE NULL END;
    END IF;

    -- HTTP IPFS 网关（任意 gateway）
    cid := (regexp_match(s,
        'https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)', 'i'))[1];
    IF cid IS NOT NULL THEN
        cid := rtrim(split_part(split_part(cid, '?', 1), '#', 1), '/');
        RETURN CASE WHEN cid <> '' THEN 'ipfs:' || cid ELSE NULL END;
    END IF;

    -- HTTP Arweave 网关
    tx := (regexp_match(s,
        'https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)',
        'i'))[1];
    IF tx IS NOT NULL THEN
        tx := rtrim(split_part(split_part(tx, '?', 1), '#', 1), '/');
        RETURN CASE WHEN tx <> '' THEN 'ar:' || tx ELSE NULL END;
    END IF;

    RETURN rtrim(lo, '/');
END;
$$;


-- ===========================================================================
-- Step 2：构建两个链的规范化临时索引表
--   · 不修改任何生产表 schema，session 结束自动销毁
--   · 对各自原表做一次全量规范化并落盘，后续分析全走内存索引
-- ===========================================================================

-- ── 以太坊规范化表 ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS _eth_norm_tmp;

CREATE TEMP TABLE _eth_norm_tmp AS
SELECT
    contract_address,
    token_id,
    normalize_url(token_uri)  AS token_key,
    normalize_url(image_uri)  AS image_key
FROM nft_assets_ethereum          -- ← tbl_eth
WHERE token_uri  IS NOT NULL
   OR image_uri  IS NOT NULL;

DELETE FROM _eth_norm_tmp
WHERE token_key IS NULL AND image_key IS NULL;

CREATE INDEX ON _eth_norm_tmp USING HASH (token_key) WHERE token_key IS NOT NULL;
CREATE INDEX ON _eth_norm_tmp USING HASH (image_key) WHERE image_key IS NOT NULL;
CREATE INDEX ON _eth_norm_tmp USING HASH (contract_address);
ANALYZE _eth_norm_tmp;

-- ── Solana 规范化表 ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS _sol_norm_tmp;

CREATE TEMP TABLE _sol_norm_tmp AS
SELECT
    contract_address,
    token_id,
    normalize_url(token_uri)  AS token_key,
    normalize_url(image_uri)  AS image_key
FROM nft_assets_solana            -- ← tbl_sol
WHERE token_uri  IS NOT NULL
   OR image_uri  IS NOT NULL;

DELETE FROM _sol_norm_tmp
WHERE token_key IS NULL AND image_key IS NULL;

CREATE INDEX ON _sol_norm_tmp USING HASH (token_key) WHERE token_key IS NOT NULL;
CREATE INDEX ON _sol_norm_tmp USING HASH (image_key) WHERE image_key IS NOT NULL;
CREATE INDEX ON _sol_norm_tmp USING HASH (contract_address);
ANALYZE _sol_norm_tmp;


-- ===========================================================================
-- Step 3：分析查询
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 3.1  总体汇总
--
--  eth_total      — 以太坊表有效行数
--  sol_total      — Solana 表有效行数
--  eth_dup_nfts   — ETH 中与 SOL 重叠的 NFT 数（ETH 侧计数）
--  sol_dup_nfts   — SOL 中与 ETH 重叠的 NFT 数（SOL 侧计数）
--  eth_dup_pct    — ETH 重叠率（%）
--  dup_contracts  — ETH 侧涉及的合约数
-- ---------------------------------------------------------------------------
WITH
-- ETH token_uri 命中 SOL token_uri
eth_token_hits AS (
    SELECT DISTINCT e.contract_address, e.token_id
    FROM _eth_norm_tmp e
    JOIN _sol_norm_tmp s ON s.token_key = e.token_key
    WHERE e.token_key IS NOT NULL
),
-- ETH image_uri 命中 SOL image_uri
eth_image_hits AS (
    SELECT DISTINCT e.contract_address, e.token_id
    FROM _eth_norm_tmp e
    JOIN _sol_norm_tmp s ON s.image_key = e.image_key
    WHERE e.image_key IS NOT NULL
),
-- ETH 侧：两种命中合并，每条 NFT 最多计 1 次
eth_all_hits AS (
    SELECT contract_address, token_id FROM eth_token_hits
    UNION
    SELECT contract_address, token_id FROM eth_image_hits
),
-- SOL token_uri 命中 ETH token_uri
sol_token_hits AS (
    SELECT DISTINCT s.contract_address, s.token_id
    FROM _sol_norm_tmp s
    JOIN _eth_norm_tmp e ON e.token_key = s.token_key
    WHERE s.token_key IS NOT NULL
),
-- SOL image_uri 命中 ETH image_uri
sol_image_hits AS (
    SELECT DISTINCT s.contract_address, s.token_id
    FROM _sol_norm_tmp s
    JOIN _eth_norm_tmp e ON e.image_key = s.image_key
    WHERE s.image_key IS NOT NULL
),
-- SOL 侧合并
sol_all_hits AS (
    SELECT contract_address, token_id FROM sol_token_hits
    UNION
    SELECT contract_address, token_id FROM sol_image_hits
)
SELECT
    (SELECT COUNT(*) FROM _eth_norm_tmp)                    AS eth_total,
    (SELECT COUNT(*) FROM _sol_norm_tmp)                    AS sol_total,
    (SELECT COUNT(*) FROM eth_all_hits)                     AS eth_dup_nfts,
    (SELECT COUNT(*) FROM sol_all_hits)                     AS sol_dup_nfts,
    ROUND(
        (SELECT COUNT(*) FROM eth_all_hits)::NUMERIC
        / NULLIF((SELECT COUNT(*) FROM _eth_norm_tmp), 0)
        * 100, 2
    )                                                       AS eth_dup_pct,
    ROUND(
        (SELECT COUNT(*) FROM sol_all_hits)::NUMERIC
        / NULLIF((SELECT COUNT(*) FROM _sol_norm_tmp), 0)
        * 100, 2
    )                                                       AS sol_dup_pct,
    (SELECT COUNT(DISTINCT contract_address)
     FROM eth_all_hits)                                     AS eth_dup_contracts,
    (SELECT COUNT(DISTINCT contract_address)
     FROM sol_all_hits)                                     AS sol_dup_contracts;


-- ---------------------------------------------------------------------------
-- 3.2  ETH 侧——按合约汇总重复数（从多到少）
--
--  contract_address  — ETH 合约地址
--  eth_total         — 该合约在 ETH 表中的总行数
--  dup_nft_count     — 该合约与 SOL 重叠的 NFT 数
--  token_uri_dup     — 其中通过 token_uri 命中的数量
--  image_uri_dup     — 其中通过 image_uri 命中的数量
--  dup_pct           — 该合约的重复率（%）
-- ---------------------------------------------------------------------------
WITH
eth_token_hits AS (
    SELECT DISTINCT e.contract_address, e.token_id, 'token_uri' AS hit_field
    FROM _eth_norm_tmp e
    JOIN _sol_norm_tmp s ON s.token_key = e.token_key
    WHERE e.token_key IS NOT NULL
),
eth_image_hits AS (
    SELECT DISTINCT e.contract_address, e.token_id, 'image_uri' AS hit_field
    FROM _eth_norm_tmp e
    JOIN _sol_norm_tmp s ON s.image_key = e.image_key
    WHERE e.image_key IS NOT NULL
),
eth_all_hits AS (
    SELECT contract_address, token_id, hit_field FROM eth_token_hits
    UNION
    SELECT contract_address, token_id, hit_field FROM eth_image_hits
),
eth_contract_total AS (
    SELECT contract_address, COUNT(*) AS total
    FROM _eth_norm_tmp
    GROUP BY contract_address
)
SELECT
    h.contract_address,
    t.total                                                       AS eth_total,
    COUNT(DISTINCT h.token_id)                                    AS dup_nft_count,
    COUNT(DISTINCT h.token_id) FILTER (WHERE hit_field='token_uri') AS token_uri_dup,
    COUNT(DISTINCT h.token_id) FILTER (WHERE hit_field='image_uri') AS image_uri_dup,
    ROUND(
        COUNT(DISTINCT h.token_id)::NUMERIC
        / NULLIF(t.total, 0) * 100, 2
    )                                                             AS dup_pct
FROM eth_all_hits h
JOIN eth_contract_total t USING (contract_address)
GROUP BY h.contract_address, t.total
ORDER BY dup_nft_count DESC;


-- ---------------------------------------------------------------------------
-- 3.3  重复明细（每条重叠 NFT 的双链对应信息）
--
--  eth_contract / eth_token_id  — ETH 侧标识
--  sol_contract / sol_token_id  — SOL 侧对应记录标识
--  hit_field                    — 命中字段（token_uri / image_uri）
--  norm_key                     — 规范化后的公共 URL（即重叠依据）
-- ---------------------------------------------------------------------------
WITH
token_matches AS (
    SELECT
        e.contract_address  AS eth_contract,
        e.token_id          AS eth_token_id,
        s.contract_address  AS sol_contract,
        s.token_id          AS sol_token_id,
        'token_uri'         AS hit_field,
        e.token_key         AS norm_key
    FROM _eth_norm_tmp e
    JOIN _sol_norm_tmp s ON s.token_key = e.token_key
    WHERE e.token_key IS NOT NULL
),
image_matches AS (
    SELECT
        e.contract_address  AS eth_contract,
        e.token_id          AS eth_token_id,
        s.contract_address  AS sol_contract,
        s.token_id          AS sol_token_id,
        'image_uri'         AS hit_field,
        e.image_key         AS norm_key
    FROM _eth_norm_tmp e
    JOIN _sol_norm_tmp s ON s.image_key = e.image_key
    WHERE e.image_key IS NOT NULL
)
SELECT * FROM token_matches
UNION ALL
SELECT * FROM image_matches
ORDER BY eth_contract, eth_token_id, hit_field;


-- ---------------------------------------------------------------------------
-- 3.4  热点规范化 URL（同一 URL 在两链中各出现多少次）
--
--  norm_key     — 规范化后的 URL
--  hit_field    — token_uri 或 image_uri
--  eth_count    — 该 key 在 ETH 中出现行数
--  sol_count    — 该 key 在 SOL 中出现行数
--  total        — 两链合计
--
--  可用于发现：
--    · 同一 IPFS CID 在两链大量引用（真正的跨链 NFT）
--    · 占位符/默认图被大量使用（数据质量问题）
-- ---------------------------------------------------------------------------
WITH
eth_token_keys AS (
    SELECT token_key AS k, 'token_uri' AS field, COUNT(*) AS n
    FROM _eth_norm_tmp
    WHERE token_key IS NOT NULL
    GROUP BY token_key
),
sol_token_keys AS (
    SELECT token_key AS k, 'token_uri' AS field, COUNT(*) AS n
    FROM _sol_norm_tmp
    WHERE token_key IS NOT NULL
    GROUP BY token_key
),
eth_image_keys AS (
    SELECT image_key AS k, 'image_uri' AS field, COUNT(*) AS n
    FROM _eth_norm_tmp
    WHERE image_key IS NOT NULL
    GROUP BY image_key
),
sol_image_keys AS (
    SELECT image_key AS k, 'image_uri' AS field, COUNT(*) AS n
    FROM _sol_norm_tmp
    WHERE image_key IS NOT NULL
    GROUP BY image_key
)
SELECT
    COALESCE(e.k, s.k)      AS norm_key,
    COALESCE(e.field, s.field) AS hit_field,
    COALESCE(e.n, 0)        AS eth_count,
    COALESCE(s.n, 0)        AS sol_count,
    COALESCE(e.n, 0) + COALESCE(s.n, 0) AS total
FROM
    (SELECT * FROM eth_token_keys UNION ALL SELECT * FROM eth_image_keys) e
FULL OUTER JOIN
    (SELECT * FROM sol_token_keys UNION ALL SELECT * FROM sol_image_keys) s
    ON s.k = e.k AND s.field = e.field
WHERE e.k IS NOT NULL AND s.k IS NOT NULL   -- 只展示两链均出现的 key
ORDER BY total DESC, eth_count DESC
LIMIT 200;
