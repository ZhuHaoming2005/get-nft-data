CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS analysis_contract_identity (
    chain                TEXT        NOT NULL,
    contract_address     TEXT        NOT NULL,
    nft_count            BIGINT      NOT NULL,
    raw_name             TEXT        NOT NULL DEFAULT '',
    raw_symbol           TEXT        NOT NULL DEFAULT '',
    name_norm            TEXT        NOT NULL DEFAULT '',
    symbol_norm          TEXT        NOT NULL DEFAULT '',
    name_len             INTEGER     NOT NULL DEFAULT 0,
    name_len_bucket      INTEGER     NOT NULL DEFAULT 0,
    name_block_key       TEXT        NOT NULL DEFAULT '',
    name_variant_count   INTEGER     NOT NULL DEFAULT 0,
    symbol_variant_count INTEGER     NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain, contract_address)
);

CREATE INDEX IF NOT EXISTS idx_analysis_contract_identity_chain
    ON analysis_contract_identity (chain);

CREATE INDEX IF NOT EXISTS idx_analysis_contract_identity_symbol_norm
    ON analysis_contract_identity (symbol_norm);

CREATE INDEX IF NOT EXISTS idx_analysis_contract_identity_name_block
    ON analysis_contract_identity (name_block_key, name_len_bucket);

CREATE INDEX IF NOT EXISTS idx_analysis_contract_identity_name_norm_trgm
    ON analysis_contract_identity USING gin (name_norm gin_trgm_ops);
