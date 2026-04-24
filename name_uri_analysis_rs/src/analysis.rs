use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use strsim::jaro_winkler;
use sysinfo::System;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("database_path must be a disk path, not :memory:")]
    MemoryDatabaseDisabled,
    #[error("at least one parquet input is required")]
    MissingParquetInput,
    #[error("invalid data: {0}")]
    InvalidData(String),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Clone, Debug)]
pub struct AnalysisOptions {
    pub database_path: PathBuf,
    pub parquet_inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub thresholds: Vec<f64>,
    pub threads: usize,
    pub memory_limit: String,
    pub analysis_memory_limit: Option<String>,
    pub temp_directory: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct SummaryRow {
    pub field_name: String,
    pub scope: String,
    pub primary_chain: String,
    pub secondary_chain: String,
    pub threshold: Option<f64>,
    pub match_mode: String,
    pub metric: String,
    pub total_contracts: i64,
    pub total_nfts: i64,
    pub group_count: i64,
    pub duplicate_contract_count: i64,
    pub duplicate_nft_count: i64,
    pub duplicate_contract_ratio: f64,
    pub duplicate_nft_ratio: f64,
    pub group_size_ge_2_count: i64,
    pub group_size_gt_2_count: i64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct AnalysisReport {
    pub summary_rows: Vec<SummaryRow>,
}

#[derive(Clone, Debug)]
struct NameAtom {
    chain_index: usize,
    name_norm: String,
    contract_count: i64,
    nft_count: i64,
}

const RIGHT_SCORE_CHUNK_SIZE: usize = 8192;
const ANALYSIS_STATE_MEMORY_PERCENT: usize = 50;
const DEFAULT_SYSTEM_RESERVE_PERCENT: usize = 10;
const MIN_DUCKDB_MEMORY_PERCENT: usize = 20;
const MIN_ANALYSIS_MEMORY_PERCENT: usize = 10;
const SPARSE_UNION_NODE_BYTES: usize = 96;

#[derive(Clone, Copy)]
struct ScoredRight {
    right: usize,
    score: f64,
}

#[derive(Clone, Copy)]
struct NameTotals {
    contracts: i64,
    nfts: i64,
}

#[derive(Clone, Copy)]
struct UriCounts {
    total_nfts: i64,
    total_contracts: i64,
    v1_nfts: i64,
    v1_contracts: i64,
    v2_nfts: i64,
    v2_contracts: i64,
    v3_nfts: i64,
    v3_contracts: i64,
}

#[derive(Clone, Copy, Default)]
struct GroupSummary {
    group_count: i64,
    duplicate_contract_count: i64,
    duplicate_nft_count: i64,
    group_size_ge_2_count: i64,
    group_size_gt_2_count: i64,
}

struct SummarySpec<'a> {
    field_name: &'a str,
    scope: &'a str,
    primary_chain: &'a str,
    secondary_chain: &'a str,
    threshold: Option<f64>,
    match_mode: &'a str,
    metric: &'a str,
    total_contracts: i64,
    total_nfts: i64,
}

struct ChainMatrixRowSpec<'a> {
    chains: &'a [String],
    totals: &'a HashMap<String, NameTotals>,
    primary_index: usize,
    secondary_index: usize,
    threshold: f64,
}

struct MatrixUnionState {
    threshold: f64,
    union_find: SparseUnionFind,
}

struct MemoryPlan {
    duckdb_bytes: usize,
    analysis_bytes: usize,
}

struct ProgressTracker {
    bar: ProgressBar,
}

impl ProgressTracker {
    fn new() -> Self {
        let bar = ProgressBar::new(0);
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {percent}% {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        Self { bar }
    }

    fn add_work(&self, units: u64) {
        self.bar.inc_length(units);
    }

    fn step(&self, message: impl Into<String>) {
        self.bar.set_message(message.into());
        self.bar.inc(1);
    }

    fn inc(&self, units: u64) {
        self.bar.inc(units);
    }

    fn set_message(&self, message: impl Into<String>) {
        self.bar.set_message(message.into());
    }

    fn finish(&self) {
        self.bar
            .finish_with_message("analysis complete; writing outputs finished");
    }
}

struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, node: usize) -> usize {
        let parent = self.parent[node];
        if parent != node {
            let root = self.find(parent);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    fn union(&mut self, left: usize, right: usize) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            return;
        }
        if self.rank[left_root] < self.rank[right_root] {
            self.parent[left_root] = right_root;
        } else if self.rank[left_root] > self.rank[right_root] {
            self.parent[right_root] = left_root;
        } else {
            self.parent[right_root] = left_root;
            self.rank[left_root] += 1;
        }
    }
}

struct ThresholdUnionState {
    threshold: f64,
    intra: UnionFind,
    cross: Option<SparseUnionFind>,
}

#[derive(Default)]
struct SparseUnionFind {
    index_by_atom: HashMap<usize, usize>,
    atoms: Vec<usize>,
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl SparseUnionFind {
    fn get_or_insert(&mut self, atom: usize) -> usize {
        if let Some(index) = self.index_by_atom.get(&atom).copied() {
            return index;
        }

        let index = self.atoms.len();
        self.index_by_atom.insert(atom, index);
        self.atoms.push(atom);
        self.parent.push(index);
        self.rank.push(0);
        index
    }

    fn find_local(&mut self, node: usize) -> usize {
        let parent = self.parent[node];
        if parent != node {
            let root = self.find_local(parent);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    fn union(&mut self, left: usize, right: usize) {
        let left = self.get_or_insert(left);
        let right = self.get_or_insert(right);

        let left_root = self.find_local(left);
        let right_root = self.find_local(right);
        if left_root == right_root {
            return;
        }

        let left_rank = self.rank[left_root];
        let right_rank = self.rank[right_root];
        if left_rank < right_rank {
            self.parent[left_root] = right_root;
        } else if left_rank > right_rank {
            self.parent[right_root] = left_root;
        } else {
            self.parent[right_root] = left_root;
            self.rank[left_root] += 1;
        }
    }

    fn atom_count(&self) -> usize {
        self.atoms.len()
    }

    fn atom_at(&self, local_index: usize) -> usize {
        self.atoms[local_index]
    }
}

pub fn run_analysis(options: AnalysisOptions) -> Result<AnalysisReport, AnalysisError> {
    if options.database_path.to_string_lossy() == ":memory:" {
        return Err(AnalysisError::MemoryDatabaseDisabled);
    }
    if options.parquet_inputs.is_empty() {
        return Err(AnalysisError::MissingParquetInput);
    }
    if options.thresholds.is_empty() {
        return Err(AnalysisError::InvalidData(
            "at least one name threshold is required".to_string(),
        ));
    }

    if let Some(parent) = options.database_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir_all(&options.output_dir)?;

    let progress = ProgressTracker::new();
    progress.add_work(2);
    let conn = Connection::open(&options.database_path)?;
    configure_duckdb(&conn, &options)?;
    progress.step("DuckDB configured");
    let selected_chains = prepare_base_tables(&conn, &options, &progress)?;

    let mut summary_rows = Vec::new();
    summary_rows.extend(run_uri_analysis(&conn, &selected_chains, &progress)?);
    summary_rows.extend(run_name_analysis(
        &conn,
        &selected_chains,
        &options.thresholds,
        options.threads,
        &options.memory_limit,
        options.analysis_memory_limit.as_deref(),
        &progress,
    )?);

    summary_rows.sort_by(|left, right| {
        (
            left.field_name.as_str(),
            left.scope.as_str(),
            left.primary_chain.as_str(),
            left.secondary_chain.as_str(),
            left.threshold.unwrap_or(-1.0).to_bits(),
            left.match_mode.as_str(),
            left.metric.as_str(),
        )
            .cmp(&(
                right.field_name.as_str(),
                right.scope.as_str(),
                right.primary_chain.as_str(),
                right.secondary_chain.as_str(),
                right.threshold.unwrap_or(-1.0).to_bits(),
                right.match_mode.as_str(),
                right.metric.as_str(),
            ))
    });

    let report = AnalysisReport { summary_rows };
    write_outputs(&report, &options.output_dir)?;
    progress.step("outputs written");
    progress.finish();
    Ok(report)
}

fn configure_duckdb(conn: &Connection, options: &AnalysisOptions) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        PRAGMA preserve_insertion_order=false;
        ",
    )?;
    conn.execute(&format!("PRAGMA threads={}", options.threads.max(1)), [])?;
    let duckdb_memory_limit = initial_duckdb_memory_limit(&options.memory_limit)?;
    conn.execute(
        &format!("PRAGMA memory_limit='{}'", sql_string(&duckdb_memory_limit)),
        [],
    )?;
    if let Some(temp_directory) = &options.temp_directory {
        fs::create_dir_all(temp_directory)?;
        conn.execute(
            &format!(
                "PRAGMA temp_directory='{}'",
                sql_string(&temp_directory.display().to_string().replace('\\', "/"))
            ),
            [],
        )?;
    }
    Ok(())
}

fn prepare_base_tables(
    conn: &Connection,
    options: &AnalysisOptions,
    progress: &ProgressTracker,
) -> Result<Vec<String>, AnalysisError> {
    let inputs = parquet_input_sql(&options.parquet_inputs);
    progress.add_work(6);
    execute_progress_batch(
        conn,
        "
        DROP TABLE IF EXISTS selected_chains;
        DROP TABLE IF EXISTS uri_rows;
        DROP TABLE IF EXISTS uri_key_stats;
        DROP TABLE IF EXISTS contract_names;
        DROP TABLE IF EXISTS name_atoms;
        ",
        progress,
        "dropped stale DuckDB tables",
    )?;
    execute_progress_batch(
        conn,
        &format!(
            "
            CREATE TABLE selected_chains AS
            SELECT DISTINCT lower(trim(CAST(chain AS VARCHAR))) AS chain
            FROM read_parquet({inputs})
            WHERE chain IS NOT NULL
              AND trim(CAST(chain AS VARCHAR)) <> '';
            ",
            inputs = inputs,
        ),
        progress,
        "loaded selected chains",
    )?;
    execute_progress_batch(
        conn,
        &format!(
            "
            CREATE TABLE uri_rows AS
            SELECT
                lower(trim(CAST(chain AS VARCHAR))) AS chain,
                lower(trim(CAST(contract_address AS VARCHAR))) AS contract_address,
                trim(coalesce(CAST(token_uri AS VARCHAR), '')) AS token_uri,
                trim(coalesce(CAST(image_uri AS VARCHAR), '')) AS image_uri,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm
            FROM read_parquet({inputs})
            WHERE contract_address IS NOT NULL
              AND trim(CAST(contract_address AS VARCHAR)) <> ''
              AND (
                  trim(coalesce(CAST(token_uri AS VARCHAR), '')) <> ''
                  OR trim(coalesce(CAST(image_uri AS VARCHAR), '')) <> ''
                  OR coalesce(CAST(token_uri_norm AS VARCHAR), '') <> ''
                  OR coalesce(CAST(image_uri_norm AS VARCHAR), '') <> ''
              );
            ",
            inputs = inputs,
        ),
        progress,
        "materialized URI rows",
    )?;
    execute_progress_batch(
        conn,
        "
            CREATE TABLE uri_key_stats AS
            SELECT chain, key_kind, key_value,
                   count(*)::BIGINT AS nft_count,
                   count(DISTINCT contract_address)::BIGINT AS contract_count
            FROM (
                SELECT chain, 'strict_token' AS key_kind, token_uri AS key_value, contract_address
                FROM uri_rows WHERE token_uri <> ''
                UNION ALL
                SELECT chain, 'strict_image' AS key_kind, image_uri AS key_value, contract_address
                FROM uri_rows WHERE image_uri <> ''
                UNION ALL
                SELECT chain, 'norm_token' AS key_kind, token_uri_norm AS key_value, contract_address
                FROM uri_rows WHERE token_uri_norm <> ''
                UNION ALL
                SELECT chain, 'norm_image' AS key_kind, image_uri_norm AS key_value, contract_address
                FROM uri_rows WHERE image_uri_norm <> ''
            ) keys
            GROUP BY chain, key_kind, key_value;
        ",
        progress,
        "built URI key stats",
    )?;
    execute_progress_batch(
        conn,
        &format!(
            "
            CREATE TABLE contract_names AS
            WITH ranked AS (
                SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                       lower(trim(CAST(contract_address AS VARCHAR))) AS contract_address,
                       coalesce(CAST(name AS VARCHAR), '') AS name,
                       trim(coalesce(CAST(name_norm AS VARCHAR), '')) AS name_norm,
                       count(*) OVER (
                           PARTITION BY lower(trim(CAST(chain AS VARCHAR))),
                                        lower(trim(CAST(contract_address AS VARCHAR)))
                       )::BIGINT AS nft_count,
                       row_number() OVER (
                           PARTITION BY lower(trim(CAST(chain AS VARCHAR))),
                                        lower(trim(CAST(contract_address AS VARCHAR)))
                           ORDER BY CASE WHEN trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> '' THEN 0 ELSE 1 END,
                                    coalesce(CAST(token_id AS VARCHAR), '') DESC
                       ) AS rn
                FROM read_parquet({inputs})
                WHERE contract_address IS NOT NULL
                  AND trim(CAST(contract_address AS VARCHAR)) <> ''
            )
            SELECT chain, contract_address, nft_count, name, name_norm
            FROM ranked
            WHERE rn = 1
              AND name_norm <> '';
            ",
            inputs = inputs,
        ),
        progress,
        "materialized contract names",
    )?;
    execute_progress_batch(
        conn,
        "
            CREATE TABLE name_atoms AS
            WITH atoms AS (
                SELECT chain,
                       name_norm,
                       min(name) AS sample_name,
                       count(*)::BIGINT AS contract_count,
                       coalesce(sum(nft_count), 0)::BIGINT AS nft_count
                FROM contract_names
                GROUP BY chain, name_norm
            )
            SELECT row_number() OVER ()::BIGINT AS atom_id, *
            FROM atoms
            WHERE name_norm <> '';
        ",
        progress,
        "built name atoms",
    )?;
    load_selected_chains(conn)
}

fn execute_progress_batch(
    conn: &Connection,
    sql: &str,
    progress: &ProgressTracker,
    message: &str,
) -> Result<(), AnalysisError> {
    progress.set_message(message);
    conn.execute_batch(sql)?;
    progress.step(message);
    Ok(())
}

fn load_selected_chains(conn: &Connection) -> Result<Vec<String>, AnalysisError> {
    let mut stmt = conn.prepare("SELECT chain FROM selected_chains ORDER BY chain")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut chains = Vec::new();
    for row in rows {
        chains.push(row?);
    }
    if chains.is_empty() {
        return Err(AnalysisError::InvalidData(
            "Parquet inputs do not contain any chain values".to_string(),
        ));
    }
    Ok(chains)
}

fn run_uri_analysis(
    conn: &Connection,
    chains: &[String],
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    let mut rows = Vec::new();
    let uri_steps = chains
        .len()
        .saturating_mul(if chains.len() > 1 { 6 } else { 4 });
    progress.add_work(uri_steps as u64);
    for chain in chains {
        for (match_mode, token_kind, image_kind, min_contracts) in [
            ("strict_any", "strict_token", "strict_image", false),
            ("strict_cross", "strict_token", "strict_image", true),
            ("norm_any", "norm_token", "norm_image", false),
            ("norm_cross", "norm_token", "norm_image", true),
        ] {
            let counts = query_uri_intra_counts(
                conn,
                chain,
                token_kind,
                image_kind,
                if min_contracts {
                    "contract_count"
                } else {
                    "nft_count"
                },
            )?;
            push_uri_rows(&mut rows, "intra_chain", chain, "", match_mode, counts);
            progress.step(format!("URI intra {chain} {match_mode}"));
        }

        if chains.len() > 1 {
            for (match_mode, token_kind, image_kind) in [
                ("strict", "strict_token", "strict_image"),
                ("norm", "norm_token", "norm_image"),
            ] {
                let counts = query_uri_cross_counts(conn, chain, token_kind, image_kind)?;
                push_uri_rows(
                    &mut rows,
                    "cross_chain_summary",
                    chain,
                    "",
                    match_mode,
                    counts,
                );
                progress.step(format!("URI cross {chain} {match_mode}"));
            }
        }
    }
    Ok(rows)
}

fn query_uri_intra_counts(
    conn: &Connection,
    chain: &str,
    token_kind: &str,
    image_kind: &str,
    count_column: &str,
) -> Result<UriCounts, AnalysisError> {
    let sql = format!(
        "
        WITH keyed AS (
            SELECT r.contract_address,
                   coalesce(t.{count_column} >= 2, false) AS token_hit,
                   coalesce(i.{count_column} >= 2, false) AS image_hit
            FROM uri_rows r
            LEFT JOIN uri_key_stats t
              ON t.chain = r.chain
             AND t.key_kind = '{token_kind}'
             AND t.key_value = CASE WHEN '{token_kind}' = 'strict_token' THEN r.token_uri ELSE r.token_uri_norm END
            LEFT JOIN uri_key_stats i
              ON i.chain = r.chain
             AND i.key_kind = '{image_kind}'
             AND i.key_value = CASE WHEN '{image_kind}' = 'strict_image' THEN r.image_uri ELSE r.image_uri_norm END
            WHERE r.chain = ?
        )
        SELECT count(*)::BIGINT,
               count(DISTINCT contract_address)::BIGINT,
               coalesce(sum(CASE WHEN token_hit THEN 1 ELSE 0 END), 0)::BIGINT,
               count(DISTINCT CASE WHEN token_hit THEN contract_address END)::BIGINT,
               coalesce(sum(CASE WHEN NOT token_hit AND image_hit THEN 1 ELSE 0 END), 0)::BIGINT,
               count(DISTINCT CASE WHEN NOT token_hit AND image_hit THEN contract_address END)::BIGINT,
               coalesce(sum(CASE WHEN token_hit OR image_hit THEN 1 ELSE 0 END), 0)::BIGINT,
               count(DISTINCT CASE WHEN token_hit OR image_hit THEN contract_address END)::BIGINT
        FROM keyed
        "
    );
    query_uri_counts(conn, &sql, chain)
}

fn query_uri_cross_counts(
    conn: &Connection,
    chain: &str,
    token_kind: &str,
    image_kind: &str,
) -> Result<UriCounts, AnalysisError> {
    let token_expr = if token_kind == "strict_token" {
        "r.token_uri"
    } else {
        "r.token_uri_norm"
    };
    let image_expr = if image_kind == "strict_image" {
        "r.image_uri"
    } else {
        "r.image_uri_norm"
    };
    let sql = format!(
        "
        WITH other_token AS (
            SELECT DISTINCT key_value
            FROM uri_key_stats
            WHERE key_kind = '{token_kind}' AND chain <> ?
        ),
        other_image AS (
            SELECT DISTINCT key_value
            FROM uri_key_stats
            WHERE key_kind = '{image_kind}' AND chain <> ?
        ),
        keyed AS (
            SELECT r.contract_address,
                   ot.key_value IS NOT NULL AS token_hit,
                   oi.key_value IS NOT NULL AS image_hit
            FROM uri_rows r
            LEFT JOIN other_token ot ON ot.key_value = {token_expr}
            LEFT JOIN other_image oi ON oi.key_value = {image_expr}
            WHERE r.chain = ?
        )
        SELECT count(*)::BIGINT,
               count(DISTINCT contract_address)::BIGINT,
               coalesce(sum(CASE WHEN token_hit THEN 1 ELSE 0 END), 0)::BIGINT,
               count(DISTINCT CASE WHEN token_hit THEN contract_address END)::BIGINT,
               coalesce(sum(CASE WHEN NOT token_hit AND image_hit THEN 1 ELSE 0 END), 0)::BIGINT,
               count(DISTINCT CASE WHEN NOT token_hit AND image_hit THEN contract_address END)::BIGINT,
               coalesce(sum(CASE WHEN token_hit OR image_hit THEN 1 ELSE 0 END), 0)::BIGINT,
               count(DISTINCT CASE WHEN token_hit OR image_hit THEN contract_address END)::BIGINT
        FROM keyed
        "
    );
    conn.query_row(&sql, params![chain, chain, chain], uri_counts_from_row)
        .map_err(AnalysisError::from)
}

fn query_uri_counts(conn: &Connection, sql: &str, chain: &str) -> Result<UriCounts, AnalysisError> {
    conn.query_row(sql, params![chain], uri_counts_from_row)
        .map_err(AnalysisError::from)
}

fn uri_counts_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<UriCounts> {
    Ok(UriCounts {
        total_nfts: row.get(0)?,
        total_contracts: row.get(1)?,
        v1_nfts: row.get(2)?,
        v1_contracts: row.get(3)?,
        v2_nfts: row.get(4)?,
        v2_contracts: row.get(5)?,
        v3_nfts: row.get(6)?,
        v3_contracts: row.get(7)?,
    })
}

fn push_uri_rows(
    rows: &mut Vec<SummaryRow>,
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    match_mode: &str,
    counts: UriCounts,
) {
    for (metric, duplicate_nfts, duplicate_contracts) in [
        ("v1", counts.v1_nfts, counts.v1_contracts),
        ("v2", counts.v2_nfts, counts.v2_contracts),
        ("v3", counts.v3_nfts, counts.v3_contracts),
    ] {
        rows.push(summary_row(
            SummarySpec {
                field_name: "uri",
                scope,
                primary_chain,
                secondary_chain,
                threshold: None,
                match_mode,
                metric,
                total_contracts: counts.total_contracts,
                total_nfts: counts.total_nfts,
            },
            GroupSummary {
                duplicate_contract_count: duplicate_contracts,
                duplicate_nft_count: duplicate_nfts,
                ..GroupSummary::default()
            },
        ));
    }
}

fn run_name_analysis(
    conn: &Connection,
    chains: &[String],
    thresholds: &[f64],
    threads: usize,
    memory_limit: &str,
    analysis_memory_limit: Option<&str>,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    progress.add_work(3);
    let totals = load_name_totals(conn, chains)?;
    progress.step("loaded name totals");
    let atoms = load_all_name_atoms(conn, chains)?;
    progress.step(format!("loaded {} name atoms", atoms.len()));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;

    let mut rows = Vec::new();
    let memory_plan = name_analysis_memory_plan(
        thresholds,
        atoms.len(),
        chains.len(),
        memory_limit,
        analysis_memory_limit,
    )?;
    set_duckdb_memory_limit(conn, memory_plan.duckdb_bytes)?;
    progress.step(format!(
        "balanced memory: DuckDB {}, Rust {}",
        format_byte_size(memory_plan.duckdb_bytes),
        format_byte_size(memory_plan.analysis_bytes)
    ));
    let threshold_batches = threshold_batches(
        thresholds,
        atoms.len(),
        chains.len(),
        memory_plan.analysis_bytes,
    );
    for threshold_batch in &threshold_batches {
        progress.add_work(full_name_chunk_count(atoms.len()));
        let mut states = threshold_batch
            .iter()
            .copied()
            .map(|threshold| ThresholdUnionState {
                threshold,
                intra: UnionFind::new(atoms.len()),
                cross: (chains.len() > 1).then(SparseUnionFind::default),
            })
            .collect::<Vec<_>>();
        pool.install(|| union_full_name_pairs(&atoms, &mut states, progress));

        progress.add_work(states.len() as u64 * chains.len() as u64);
        for state in &mut states {
            push_name_summary_rows(&mut rows, &atoms, chains, &totals, state);
            progress.inc(chains.len() as u64);
        }
    }
    if chains.len() > 1 {
        rows.extend(run_chain_matrix_analysis(
            &atoms,
            chains,
            &threshold_batches,
            &totals,
            &pool,
            progress,
        ));
    }
    Ok(rows)
}

fn load_name_totals(
    conn: &Connection,
    chains: &[String],
) -> Result<HashMap<String, NameTotals>, AnalysisError> {
    let mut totals = HashMap::new();
    let mut stmt = conn.prepare(
        "
        SELECT count(*)::BIGINT, coalesce(sum(nft_count), 0)::BIGINT
        FROM contract_names
        WHERE chain = ?
        ",
    )?;
    for chain in chains {
        let total = stmt.query_row(params![chain], |row| {
            Ok(NameTotals {
                contracts: row.get(0)?,
                nfts: row.get(1)?,
            })
        })?;
        totals.insert(chain.clone(), total);
    }
    Ok(totals)
}

fn load_all_name_atoms(
    conn: &Connection,
    chains: &[String],
) -> Result<Vec<NameAtom>, AnalysisError> {
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt = conn.prepare(
        "
        SELECT chain, name_norm, contract_count, nft_count
        FROM name_atoms
        ORDER BY chain, name_norm
        ",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    let mut atoms = Vec::new();
    for row in rows {
        let (chain, name_norm, contract_count, nft_count) = row?;
        if let Some(chain_index) = chain_indexes.get(chain.as_str()).copied() {
            atoms.push(NameAtom {
                chain_index,
                name_norm,
                contract_count,
                nft_count,
            });
        }
    }
    Ok(atoms)
}

fn push_name_summary_rows(
    rows: &mut Vec<SummaryRow>,
    atoms: &[NameAtom],
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut ThresholdUnionState,
) {
    for (chain_index, primary) in chains.iter().enumerate() {
        let total = totals.get(primary).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        let intra = summarize_components_for_primary(atoms, &mut state.intra, chain_index);
        rows.push(summary_row(
            SummarySpec {
                field_name: "name",
                scope: "intra_chain",
                primary_chain: primary,
                secondary_chain: "",
                threshold: Some(state.threshold),
                match_mode: "jaro_winkler",
                metric: "duplicate_group",
                total_contracts: total.contracts,
                total_nfts: total.nfts,
            },
            intra,
        ));

        if let Some(cross) = &mut state.cross {
            let cross_summary = summarize_sparse_components_for_primary(atoms, cross, chain_index);
            rows.push(summary_row(
                SummarySpec {
                    field_name: "name",
                    scope: "cross_chain_summary",
                    primary_chain: primary,
                    secondary_chain: "",
                    threshold: Some(state.threshold),
                    match_mode: "jaro_winkler",
                    metric: "duplicate_group",
                    total_contracts: total.contracts,
                    total_nfts: total.nfts,
                },
                cross_summary,
            ));
        }
    }
}

fn unique_thresholds(thresholds: &[f64]) -> Vec<f64> {
    let mut unique = thresholds.to_vec();
    unique.sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
    unique.dedup_by(|left, right| left.to_bits() == right.to_bits());
    unique
}

fn threshold_batches(
    thresholds: &[f64],
    atom_count: usize,
    chain_count: usize,
    analysis_budget: usize,
) -> Vec<Vec<f64>> {
    let thresholds = unique_thresholds(thresholds);
    let state_bytes = threshold_state_bytes(atom_count, chain_count).max(1);
    let state_budget = analysis_budget
        .saturating_mul(ANALYSIS_STATE_MEMORY_PERCENT)
        .saturating_div(100);
    let batch_size = (state_budget / state_bytes)
        .max(1)
        .min(thresholds.len().max(1));
    thresholds.chunks(batch_size).map(<[f64]>::to_vec).collect()
}

fn full_name_chunk_count(atom_count: usize) -> u64 {
    if atom_count < 2 {
        return 0;
    }
    triangular_chunk_count(atom_count - 1)
}

fn triangular_chunk_count(max_right_count: usize) -> u64 {
    let chunk = RIGHT_SCORE_CHUNK_SIZE as u128;
    let count = max_right_count as u128;
    let full_groups = count / chunk;
    let remainder = count % chunk;
    let total = chunk
        .saturating_mul(full_groups)
        .saturating_mul(full_groups + 1)
        .saturating_div(2)
        .saturating_add(remainder.saturating_mul(full_groups + 1));
    total.min(u64::MAX as u128) as u64
}

fn chain_pair_chunk_count(left_count: usize, right_count: usize) -> u64 {
    if left_count == 0 || right_count == 0 {
        return 0;
    }
    let chunks_per_left = right_count.div_ceil(RIGHT_SCORE_CHUNK_SIZE);
    (left_count as u64).saturating_mul(chunks_per_left as u64)
}

fn threshold_state_bytes(atom_count: usize, chain_count: usize) -> usize {
    let dense = dense_union_find_bytes(atom_count);
    if chain_count > 1 {
        dense.saturating_add(sparse_union_find_bytes(atom_count))
    } else {
        dense
    }
}

fn dense_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(std::mem::size_of::<usize>() + std::mem::size_of::<u8>())
}

fn sparse_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(SPARSE_UNION_NODE_BYTES)
}

fn initial_duckdb_memory_limit(memory_limit: &str) -> Result<String, AnalysisError> {
    Ok(format_byte_size(total_memory_budget_bytes(memory_limit)?))
}

fn set_duckdb_memory_limit(conn: &Connection, bytes: usize) -> Result<(), AnalysisError> {
    conn.execute(
        &format!(
            "PRAGMA memory_limit='{}'",
            sql_string(&format_byte_size(bytes))
        ),
        [],
    )?;
    Ok(())
}

fn name_analysis_memory_plan(
    thresholds: &[f64],
    atom_count: usize,
    chain_count: usize,
    memory_limit: &str,
    analysis_memory_limit: Option<&str>,
) -> Result<MemoryPlan, AnalysisError> {
    let total_budget = total_memory_budget_bytes(memory_limit)?;
    if let Some(value) = analysis_memory_limit
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let analysis_bytes = if value.eq_ignore_ascii_case("auto") {
            Ok(auto_analysis_memory_budget_bytes())
        } else {
            parse_byte_size(value)
        }?;
        return Ok(MemoryPlan {
            duckdb_bytes: total_budget,
            analysis_bytes,
        });
    }

    Ok(auto_balanced_memory_plan(
        total_budget,
        thresholds.len(),
        atom_count,
        chain_count,
    ))
}

fn auto_balanced_memory_plan(
    total_budget: usize,
    threshold_count: usize,
    atom_count: usize,
    chain_count: usize,
) -> MemoryPlan {
    let reserve = percent_of(total_budget, DEFAULT_SYSTEM_RESERVE_PERCENT);
    let min_duckdb = percent_of(total_budget, MIN_DUCKDB_MEMORY_PERCENT);
    let min_analysis = percent_of(total_budget, MIN_ANALYSIS_MEMORY_PERCENT);
    let available = total_budget.saturating_sub(reserve);
    let max_analysis = available.saturating_sub(min_duckdb).max(min_analysis);
    let desired_analysis = desired_analysis_budget(threshold_count, atom_count, chain_count);
    let analysis_bytes = desired_analysis.clamp(min_analysis, max_analysis);
    let duckdb_bytes = available.saturating_sub(analysis_bytes).max(min_duckdb);

    MemoryPlan {
        duckdb_bytes,
        analysis_bytes,
    }
}

fn desired_analysis_budget(threshold_count: usize, atom_count: usize, chain_count: usize) -> usize {
    let thresholds = threshold_count.max(1);
    threshold_state_bytes(atom_count, chain_count)
        .saturating_mul(thresholds)
        .saturating_mul(100)
        .saturating_div(ANALYSIS_STATE_MEMORY_PERCENT)
}

fn percent_of(value: usize, percent: usize) -> usize {
    value.saturating_mul(percent).saturating_div(100)
}

fn total_memory_budget_bytes(value: &str) -> Result<usize, AnalysisError> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("auto") {
        Ok(auto_memory_budget_bytes())
    } else {
        parse_byte_size(value)
    }
}

fn auto_analysis_memory_budget_bytes() -> usize {
    percent_of(auto_memory_budget_bytes(), MIN_ANALYSIS_MEMORY_PERCENT)
}

fn auto_memory_budget_bytes() -> usize {
    let mut system = System::new();
    system.refresh_memory();
    let available = system.available_memory() as usize;
    let mib = 1024usize * 1024;
    available
        .saturating_mul(80)
        .saturating_div(100)
        .max(512 * mib)
}

fn format_byte_size(bytes: usize) -> String {
    let mib = 1024usize * 1024;
    format!("{}MB", (bytes / mib).max(1))
}

fn parse_byte_size(value: &str) -> Result<usize, AnalysisError> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split_at);
    let number = number.trim().parse::<f64>().map_err(|_| {
        AnalysisError::InvalidData(format!("invalid analysis memory limit: {value}"))
    })?;
    if !number.is_finite() || number <= 0.0 {
        return Err(AnalysisError::InvalidData(format!(
            "invalid analysis memory limit: {value}"
        )));
    }

    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => {
            return Err(AnalysisError::InvalidData(format!(
                "invalid analysis memory limit unit: {value}"
            )))
        }
    };
    Ok((number * multiplier) as usize)
}

fn union_full_name_pairs(
    atoms: &[NameAtom],
    states: &mut [ThresholdUnionState],
    progress: &ProgressTracker,
) {
    if atoms.len() < 2 || states.is_empty() {
        return;
    }
    let min_threshold = states
        .iter()
        .map(|state| state.threshold)
        .fold(f64::INFINITY, f64::min);

    for left in 0..atoms.len() - 1 {
        for chunk_start in (left + 1..atoms.len()).step_by(RIGHT_SCORE_CHUNK_SIZE) {
            let chunk_end = (chunk_start + RIGHT_SCORE_CHUNK_SIZE).min(atoms.len());
            let matching_rights =
                score_name_pairs_for_left_chunk(atoms, left, chunk_start, chunk_end, min_threshold);
            apply_matching_name_pairs(atoms, states, left, &matching_rights);
            progress.inc(1);
        }
    }
}

fn score_name_pairs_for_left_chunk(
    atoms: &[NameAtom],
    left: usize,
    chunk_start: usize,
    chunk_end: usize,
    threshold: f64,
) -> Vec<ScoredRight> {
    (chunk_start..chunk_end)
        .into_par_iter()
        .filter_map(|right| {
            let score = name_pair_score(atoms, left, right);
            (score >= threshold).then_some(ScoredRight { right, score })
        })
        .collect()
}

fn name_pair_score(atoms: &[NameAtom], left: usize, right: usize) -> f64 {
    let left_name = atoms[left].name_norm.as_str();
    let right_name = atoms[right].name_norm.as_str();
    if left_name == right_name {
        100.0
    } else {
        jaro_winkler(left_name, right_name) * 100.0
    }
}

fn apply_matching_name_pairs(
    atoms: &[NameAtom],
    states: &mut [ThresholdUnionState],
    left: usize,
    matching_rights: &[ScoredRight],
) {
    let left_chain = atoms[left].chain_index;
    for hit in matching_rights {
        let right_chain = atoms[hit.right].chain_index;
        for state in states.iter_mut() {
            if hit.score < state.threshold {
                continue;
            }
            if left_chain == right_chain {
                state.intra.union(left, hit.right);
            } else if let Some(cross) = &mut state.cross {
                cross.union(left, hit.right);
            }
        }
    }
}

fn run_chain_matrix_analysis(
    atoms: &[NameAtom],
    chains: &[String],
    threshold_batches: &[Vec<f64>],
    totals: &HashMap<String, NameTotals>,
    pool: &rayon::ThreadPool,
    progress: &ProgressTracker,
) -> Vec<SummaryRow> {
    let atoms_by_chain = atoms_by_chain(atoms, chains.len());
    let mut rows = Vec::new();

    for left_chain in 0..chains.len() {
        for right_chain in left_chain + 1..chains.len() {
            for threshold_batch in threshold_batches {
                progress.add_work(chain_pair_chunk_count(
                    atoms_by_chain[left_chain].len(),
                    atoms_by_chain[right_chain].len(),
                ));
                let mut states = threshold_batch
                    .iter()
                    .copied()
                    .map(|threshold| MatrixUnionState {
                        threshold,
                        union_find: SparseUnionFind::default(),
                    })
                    .collect::<Vec<_>>();
                pool.install(|| {
                    union_chain_pair_name_pairs(
                        atoms,
                        &atoms_by_chain[left_chain],
                        &atoms_by_chain[right_chain],
                        &mut states,
                        progress,
                    )
                });
                progress.add_work(states.len() as u64 * 2);
                for state in &mut states {
                    push_chain_matrix_row(
                        &mut rows,
                        atoms,
                        ChainMatrixRowSpec {
                            chains,
                            totals,
                            primary_index: left_chain,
                            secondary_index: right_chain,
                            threshold: state.threshold,
                        },
                        &mut state.union_find,
                    );
                    progress.inc(1);
                    push_chain_matrix_row(
                        &mut rows,
                        atoms,
                        ChainMatrixRowSpec {
                            chains,
                            totals,
                            primary_index: right_chain,
                            secondary_index: left_chain,
                            threshold: state.threshold,
                        },
                        &mut state.union_find,
                    );
                    progress.inc(1);
                }
            }
        }
    }

    rows
}

fn atoms_by_chain(atoms: &[NameAtom], chain_count: usize) -> Vec<Vec<usize>> {
    let mut indexes = vec![Vec::new(); chain_count];
    for (index, atom) in atoms.iter().enumerate() {
        indexes[atom.chain_index].push(index);
    }
    indexes
}

fn union_chain_pair_name_pairs(
    atoms: &[NameAtom],
    left_atoms: &[usize],
    right_atoms: &[usize],
    states: &mut [MatrixUnionState],
    progress: &ProgressTracker,
) {
    if left_atoms.is_empty() || right_atoms.is_empty() || states.is_empty() {
        return;
    }
    let min_threshold = states
        .iter()
        .map(|state| state.threshold)
        .fold(f64::INFINITY, f64::min);

    for &left in left_atoms {
        for right_chunk in right_atoms.chunks(RIGHT_SCORE_CHUNK_SIZE) {
            let matching_rights: Vec<ScoredRight> = right_chunk
                .par_iter()
                .copied()
                .filter_map(|right| {
                    let score = name_pair_score(atoms, left, right);
                    (score >= min_threshold).then_some(ScoredRight { right, score })
                })
                .collect();
            for hit in matching_rights {
                for state in states.iter_mut() {
                    if hit.score >= state.threshold {
                        state.union_find.union(left, hit.right);
                    }
                }
            }
            progress.inc(1);
        }
    }
}

fn push_chain_matrix_row(
    rows: &mut Vec<SummaryRow>,
    atoms: &[NameAtom],
    spec: ChainMatrixRowSpec<'_>,
    union_find: &mut SparseUnionFind,
) {
    let primary = &spec.chains[spec.primary_index];
    let total = spec.totals.get(primary).copied().unwrap_or(NameTotals {
        contracts: 0,
        nfts: 0,
    });
    let summary = summarize_sparse_components_for_primary(atoms, union_find, spec.primary_index);
    rows.push(summary_row(
        SummarySpec {
            field_name: "name",
            scope: "chain_matrix",
            primary_chain: primary,
            secondary_chain: &spec.chains[spec.secondary_index],
            threshold: Some(spec.threshold),
            match_mode: "jaro_winkler",
            metric: "duplicate_group",
            total_contracts: total.contracts,
            total_nfts: total.nfts,
        },
        summary,
    ));
}

#[derive(Default)]
struct ComponentAccumulator {
    primary_contract_count: i64,
    primary_nft_count: i64,
    total_contract_count: i64,
    first_chain: Option<usize>,
    multiple_chains: bool,
    has_secondary: bool,
}

fn summarize_components_for_primary(
    atoms: &[NameAtom],
    union_find: &mut UnionFind,
    primary: usize,
) -> GroupSummary {
    let mut components: HashMap<usize, ComponentAccumulator> = HashMap::new();
    for (index, atom) in atoms.iter().enumerate() {
        if atom.chain_index != primary {
            continue;
        }

        let root = union_find.find(index);
        let component = components.entry(root).or_default();
        component.total_contract_count += atom.contract_count;
        match component.first_chain {
            Some(first) if first != atom.chain_index => component.multiple_chains = true,
            None => component.first_chain = Some(atom.chain_index),
            _ => {}
        }
        if atom.chain_index == primary {
            component.primary_contract_count += atom.contract_count;
            component.primary_nft_count += atom.nft_count;
        }
    }

    let mut summary = GroupSummary::default();
    for component in components.values() {
        if component.primary_contract_count == 0 || component.total_contract_count < 2 {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    summary
}

fn summarize_sparse_components_for_primary(
    atoms: &[NameAtom],
    union_find: &mut SparseUnionFind,
    primary: usize,
) -> GroupSummary {
    let mut components: HashMap<usize, ComponentAccumulator> = HashMap::new();
    for local_index in 0..union_find.atom_count() {
        let index = union_find.atom_at(local_index);
        let atom = &atoms[index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += atom.contract_count;
        match component.first_chain {
            Some(first) if first != atom.chain_index => component.multiple_chains = true,
            None => component.first_chain = Some(atom.chain_index),
            _ => {}
        }
        if atom.chain_index != primary {
            component.has_secondary = true;
        } else {
            component.primary_contract_count += atom.contract_count;
            component.primary_nft_count += atom.nft_count;
        }
    }

    let mut summary = GroupSummary::default();
    for component in components.values() {
        if component.primary_contract_count == 0
            || !component.has_secondary
            || component.total_contract_count < 2
        {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    summary
}

fn summary_row(spec: SummarySpec<'_>, groups: GroupSummary) -> SummaryRow {
    SummaryRow {
        field_name: spec.field_name.to_string(),
        scope: spec.scope.to_string(),
        primary_chain: spec.primary_chain.to_string(),
        secondary_chain: spec.secondary_chain.to_string(),
        threshold: spec.threshold,
        match_mode: spec.match_mode.to_string(),
        metric: spec.metric.to_string(),
        total_contracts: spec.total_contracts,
        total_nfts: spec.total_nfts,
        group_count: groups.group_count,
        duplicate_contract_count: groups.duplicate_contract_count,
        duplicate_nft_count: groups.duplicate_nft_count,
        duplicate_contract_ratio: pct(groups.duplicate_contract_count, spec.total_contracts),
        duplicate_nft_ratio: pct(groups.duplicate_nft_count, spec.total_nfts),
        group_size_ge_2_count: groups.group_size_ge_2_count,
        group_size_gt_2_count: groups.group_size_gt_2_count,
    }
}

fn write_outputs(report: &AnalysisReport, output_dir: &Path) -> Result<(), AnalysisError> {
    let json_path = output_dir.join("summary.json");
    fs::write(&json_path, serde_json::to_string_pretty(report)?)?;

    let csv_path = output_dir.join("summary.csv");
    let mut file = fs::File::create(csv_path)?;
    writeln!(
        file,
        "field_name,scope,primary_chain,secondary_chain,threshold,match_mode,metric,total_contracts,total_nfts,group_count,duplicate_contract_count,duplicate_nft_count,duplicate_contract_ratio,duplicate_nft_ratio,group_size_ge_2_count,group_size_gt_2_count"
    )?;
    for row in &report.summary_rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{}",
            csv_cell(&row.field_name),
            csv_cell(&row.scope),
            csv_cell(&row.primary_chain),
            csv_cell(&row.secondary_chain),
            row.threshold
                .map(|value| format!("{value:.6}"))
                .unwrap_or_default(),
            csv_cell(&row.match_mode),
            csv_cell(&row.metric),
            row.total_contracts,
            row.total_nfts,
            row.group_count,
            row.duplicate_contract_count,
            row.duplicate_nft_count,
            row.duplicate_contract_ratio,
            row.duplicate_nft_ratio,
            row.group_size_ge_2_count,
            row.group_size_gt_2_count,
        )?;
    }
    Ok(())
}

fn pct(part: i64, total: i64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn parquet_input_sql(paths: &[PathBuf]) -> String {
    if paths.len() == 1 {
        format!(
            "'{}'",
            sql_string(&paths[0].display().to_string().replace('\\', "/"))
        )
    } else {
        let values = paths
            .iter()
            .map(|path| {
                format!(
                    "'{}'",
                    sql_string(&path.display().to_string().replace('\\', "/"))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{values}]")
    }
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn csv_cell(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_name_pair_scoring_keeps_only_threshold_matches() {
        let atoms = vec![
            NameAtom {
                chain_index: 0,
                name_norm: "azuki".into(),
                contract_count: 1,
                nft_count: 1,
            },
            NameAtom {
                chain_index: 1,
                name_norm: "azuki".into(),
                contract_count: 1,
                nft_count: 1,
            },
            NameAtom {
                chain_index: 1,
                name_norm: "moonbirds".into(),
                contract_count: 1,
                nft_count: 1,
            },
        ];

        let hits = score_name_pairs_for_left_chunk(&atoms, 0, 1, atoms.len(), 90.0);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].right, 1);
        assert_eq!(hits[0].score, 100.0);
    }

    #[test]
    fn threshold_batches_reuse_memory_limit_by_default() {
        let plan = name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 1, "1MB", None).unwrap();
        let batches = threshold_batches(&[90.0, 95.0, 98.0], 1_000, 1, plan.analysis_bytes);

        assert_eq!(batches, vec![vec![98.0, 95.0, 90.0]]);
    }

    #[test]
    fn threshold_batches_honor_analysis_memory_override() {
        let plan =
            name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 2, "1GB", Some("16KB")).unwrap();
        let batches = threshold_batches(&[90.0, 95.0, 98.0], 1_000, 2, plan.analysis_bytes);

        assert_eq!(batches, vec![vec![98.0], vec![95.0], vec![90.0]]);
    }

    #[test]
    fn default_memory_budget_is_auto_balanced_between_duckdb_and_rust() {
        let small = name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 1, "10GB", None).unwrap();
        let large =
            name_analysis_memory_plan(&[90.0, 95.0, 98.0], 20_000_000, 2, "10GB", None).unwrap();

        assert!(small.duckdb_bytes > small.analysis_bytes);
        assert!(large.analysis_bytes > small.analysis_bytes);
        assert!(large.duckdb_bytes < small.duckdb_bytes);
    }

    #[test]
    fn explicit_analysis_memory_limit_disables_auto_split() {
        let plan =
            name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 2, "10GB", Some("16KB")).unwrap();

        assert_eq!(plan.duckdb_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(plan.analysis_bytes, 16 * 1024);
    }

    #[test]
    fn chunk_count_matches_nested_loop_chunks() {
        let atom_count = RIGHT_SCORE_CHUNK_SIZE + 3;
        let mut expected = 0;
        for left in 0..atom_count - 1 {
            let right_count = atom_count - left - 1;
            expected += right_count.div_ceil(RIGHT_SCORE_CHUNK_SIZE);
        }

        assert_eq!(full_name_chunk_count(atom_count), expected as u64);
        assert_eq!(chain_pair_chunk_count(3, RIGHT_SCORE_CHUNK_SIZE + 1), 6);
    }
}
