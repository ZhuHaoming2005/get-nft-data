CREATE TABLE IF NOT EXISTS analysis_duplicate_summary (
    id                       BIGSERIAL   PRIMARY KEY,
    field_name               TEXT        NOT NULL,
    scope                    TEXT        NOT NULL,
    primary_chain            TEXT        NOT NULL,
    secondary_chain          TEXT        NOT NULL DEFAULT '',
    threshold                DOUBLE PRECISION NOT NULL DEFAULT -1,
    total_contracts          BIGINT      NOT NULL,
    total_nfts               BIGINT      NOT NULL,
    group_count              BIGINT      NOT NULL,
    duplicate_contract_count BIGINT      NOT NULL,
    duplicate_nft_count      BIGINT      NOT NULL,
    duplicate_contract_ratio DOUBLE PRECISION NOT NULL,
    duplicate_nft_ratio      DOUBLE PRECISION NOT NULL,
    group_size_ge_2_count    BIGINT      NOT NULL,
    group_size_gt_2_count    BIGINT      NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_duplicate_summary_scope
    ON analysis_duplicate_summary (field_name, scope, primary_chain, secondary_chain, threshold);
