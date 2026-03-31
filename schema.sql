-- 扫描进度表（每链一行，支持断点续扫）
CREATE TABLE IF NOT EXISTS scan_progress (
    chain_name         VARCHAR(50) PRIMARY KEY,
    last_scanned_block BIGINT      NOT NULL,
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

-- 每链独立表：nft_assets_base、nft_assets_eth 等（由 nft_collector 按 CHAIN_NAME 自动创建）
-- 示例：base 链的表结构
CREATE TABLE IF NOT EXISTS nft_assets_base (
    id               BIGSERIAL    PRIMARY KEY,
    contract_address VARCHAR(42)  NOT NULL,
    token_id         NUMERIC      NOT NULL,
    token_uri        TEXT,
    image_uri        TEXT,
    token_standard   VARCHAR(10),
    first_seen_block BIGINT,
    created_at       TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (contract_address, token_id)
);

CREATE INDEX IF NOT EXISTS idx_nft_contract ON nft_assets_base (contract_address);
