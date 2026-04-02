CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS nsv2_contract_identity (
    run_label            TEXT        NOT NULL,
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
    name_signature       TEXT        NOT NULL DEFAULT '',
    name_signature_hash  TEXT        NOT NULL DEFAULT '',
    name_variant_count   INTEGER     NOT NULL DEFAULT 0,
    symbol_variant_count INTEGER     NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_label, chain, contract_address)
);

CREATE INDEX IF NOT EXISTS idx_nsv2_identity_run_chain_symbol
    ON nsv2_contract_identity (run_label, chain, symbol_norm);

CREATE INDEX IF NOT EXISTS idx_nsv2_identity_run_block
    ON nsv2_contract_identity (run_label, name_block_key, name_signature_hash);

CREATE INDEX IF NOT EXISTS idx_nsv2_identity_name_trgm
    ON nsv2_contract_identity USING gin (name_norm gin_trgm_ops);

CREATE TABLE IF NOT EXISTS nsv2_symbol_rollup (
    run_label      TEXT        NOT NULL,
    chain          TEXT        NOT NULL,
    symbol_norm    TEXT        NOT NULL,
    contract_count BIGINT      NOT NULL,
    nft_count      BIGINT      NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_label, chain, symbol_norm)
);

CREATE INDEX IF NOT EXISTS idx_nsv2_symbol_rollup_lookup
    ON nsv2_symbol_rollup (run_label, symbol_norm, chain);

CREATE TABLE IF NOT EXISTS nsv2_name_atoms (
    atom_id                 BIGSERIAL   PRIMARY KEY,
    run_label               TEXT        NOT NULL,
    chain                   TEXT        NOT NULL,
    name_norm               TEXT        NOT NULL,
    sample_contract_address TEXT        NOT NULL DEFAULT '',
    contract_count          BIGINT      NOT NULL,
    nft_count               BIGINT      NOT NULL,
    name_len                INTEGER     NOT NULL DEFAULT 0,
    name_len_bucket         INTEGER     NOT NULL DEFAULT 0,
    name_block_key          TEXT        NOT NULL DEFAULT '',
    name_signature          TEXT        NOT NULL DEFAULT '',
    name_signature_hash     TEXT        NOT NULL DEFAULT '',
    name_collapsed          TEXT        NOT NULL DEFAULT '',
    name_collapsed_len      INTEGER     NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_label, chain, name_norm)
);

ALTER TABLE nsv2_name_atoms
    ADD COLUMN IF NOT EXISTS name_collapsed TEXT NOT NULL DEFAULT '';

ALTER TABLE nsv2_name_atoms
    ADD COLUMN IF NOT EXISTS name_collapsed_len INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_nsv2_atoms_run_block
    ON nsv2_name_atoms (run_label, name_block_key, name_signature_hash);

CREATE INDEX IF NOT EXISTS idx_nsv2_atoms_run_collapsed
    ON nsv2_name_atoms (run_label, name_collapsed, name_collapsed_len);

CREATE TABLE IF NOT EXISTS nsv2_name_work_items (
    id               BIGSERIAL   PRIMARY KEY,
    run_label        TEXT        NOT NULL,
    task_key         TEXT        NOT NULL,
    chains_csv       TEXT        NOT NULL,
    name_block_key   TEXT        NOT NULL,
    signature_prefix TEXT        NOT NULL DEFAULT '',
    atom_count       BIGINT      NOT NULL DEFAULT 0,
    status           TEXT        NOT NULL DEFAULT 'pending',
    worker_id        TEXT        NOT NULL DEFAULT '',
    attempt_count    INTEGER     NOT NULL DEFAULT 0,
    edge_count       BIGINT      NOT NULL DEFAULT 0,
    error_message    TEXT        NOT NULL DEFAULT '',
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_label, task_key)
);

CREATE INDEX IF NOT EXISTS idx_nsv2_work_items_claim
    ON nsv2_name_work_items (run_label, status, atom_count DESC, id);

CREATE TABLE IF NOT EXISTS nsv2_name_match_edges (
    id               BIGSERIAL   PRIMARY KEY,
    run_label        TEXT        NOT NULL,
    task_id          BIGINT      NOT NULL REFERENCES nsv2_name_work_items(id) ON DELETE CASCADE,
    left_atom_id     BIGINT      NOT NULL REFERENCES nsv2_name_atoms(atom_id) ON DELETE CASCADE,
    right_atom_id    BIGINT      NOT NULL REFERENCES nsv2_name_atoms(atom_id) ON DELETE CASCADE,
    similarity_score DOUBLE PRECISION NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_label, task_id, left_atom_id, right_atom_id)
);

CREATE INDEX IF NOT EXISTS idx_nsv2_edges_run_task
    ON nsv2_name_match_edges (run_label, task_id, similarity_score);

CREATE TABLE IF NOT EXISTS nsv2_name_duplicate_groups (
    id                     BIGSERIAL   PRIMARY KEY,
    run_label              TEXT        NOT NULL,
    field_name             TEXT        NOT NULL,
    scope                  TEXT        NOT NULL,
    primary_chain          TEXT        NOT NULL,
    secondary_chain        TEXT        NOT NULL DEFAULT '',
    threshold              DOUBLE PRECISION NOT NULL,
    task_key               TEXT        NOT NULL,
    group_key              TEXT        NOT NULL,
    sample_value           TEXT        NOT NULL DEFAULT '',
    primary_contract_count BIGINT      NOT NULL,
    primary_nft_count      BIGINT      NOT NULL,
    total_contract_count   BIGINT      NOT NULL,
    total_nft_count        BIGINT      NOT NULL,
    node_count             BIGINT      NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nsv2_name_groups_summary
    ON nsv2_name_duplicate_groups (run_label, scope, primary_chain, secondary_chain, threshold);

CREATE TABLE IF NOT EXISTS nsv2_symbol_duplicate_groups (
    id                     BIGSERIAL   PRIMARY KEY,
    run_label              TEXT        NOT NULL,
    field_name             TEXT        NOT NULL,
    scope                  TEXT        NOT NULL,
    primary_chain          TEXT        NOT NULL,
    secondary_chain        TEXT        NOT NULL DEFAULT '',
    threshold              DOUBLE PRECISION NOT NULL DEFAULT -1.0,
    group_key              TEXT        NOT NULL,
    sample_value           TEXT        NOT NULL DEFAULT '',
    primary_contract_count BIGINT      NOT NULL,
    primary_nft_count      BIGINT      NOT NULL,
    total_contract_count   BIGINT      NOT NULL,
    total_nft_count        BIGINT      NOT NULL,
    node_count             BIGINT      NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nsv2_symbol_groups_summary
    ON nsv2_symbol_duplicate_groups (run_label, scope, primary_chain, secondary_chain);

CREATE TABLE IF NOT EXISTS nsv2_duplicate_summary (
    id                       BIGSERIAL   PRIMARY KEY,
    run_label                TEXT        NOT NULL,
    field_name               TEXT        NOT NULL,
    scope                    TEXT        NOT NULL,
    primary_chain            TEXT        NOT NULL,
    secondary_chain          TEXT        NOT NULL DEFAULT '',
    threshold                DOUBLE PRECISION NOT NULL DEFAULT -1.0,
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

CREATE INDEX IF NOT EXISTS idx_nsv2_summary_lookup
    ON nsv2_duplicate_summary (run_label, field_name, scope, primary_chain, secondary_chain, threshold);
