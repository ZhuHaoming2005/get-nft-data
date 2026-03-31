CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS analysis_name_candidate_pairs (
    id                     BIGSERIAL   PRIMARY KEY,
    scope                  TEXT        NOT NULL,
    primary_chain          TEXT        NOT NULL,
    secondary_chain        TEXT        NOT NULL DEFAULT '',
    left_chain             TEXT        NOT NULL,
    left_contract_address  TEXT        NOT NULL,
    left_name_norm         TEXT        NOT NULL,
    left_nft_count         BIGINT      NOT NULL,
    right_chain            TEXT        NOT NULL,
    right_contract_address TEXT        NOT NULL,
    right_name_norm        TEXT        NOT NULL,
    right_nft_count        BIGINT      NOT NULL,
    trigram_score          DOUBLE PRECISION NOT NULL,
    similarity_score       DOUBLE PRECISION NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_name_candidate_pairs_scope
    ON analysis_name_candidate_pairs (scope, primary_chain, secondary_chain);

CREATE TABLE IF NOT EXISTS analysis_name_duplicate_groups (
    id                     BIGSERIAL   PRIMARY KEY,
    scope                  TEXT        NOT NULL,
    primary_chain          TEXT        NOT NULL,
    secondary_chain        TEXT        NOT NULL DEFAULT '',
    threshold              DOUBLE PRECISION NOT NULL,
    group_key              TEXT        NOT NULL,
    sample_value           TEXT        NOT NULL DEFAULT '',
    primary_contract_count BIGINT      NOT NULL,
    primary_nft_count      BIGINT      NOT NULL,
    total_member_count     BIGINT      NOT NULL,
    total_member_nft_count BIGINT      NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_name_duplicate_groups_scope
    ON analysis_name_duplicate_groups (scope, primary_chain, secondary_chain, threshold);
