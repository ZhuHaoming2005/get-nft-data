CREATE TABLE IF NOT EXISTS analysis_symbol_duplicate_groups (
    id                     BIGSERIAL   PRIMARY KEY,
    scope                  TEXT        NOT NULL,
    primary_chain          TEXT        NOT NULL,
    secondary_chain        TEXT        NOT NULL DEFAULT '',
    match_key              TEXT        NOT NULL,
    primary_contract_count BIGINT      NOT NULL,
    primary_nft_count      BIGINT      NOT NULL,
    total_member_count     BIGINT      NOT NULL,
    total_member_nft_count BIGINT      NOT NULL,
    sample_value           TEXT        NOT NULL DEFAULT '',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_symbol_duplicate_groups_scope
    ON analysis_symbol_duplicate_groups (scope, primary_chain, secondary_chain);
