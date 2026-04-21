use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use crate::algorithms::{
    all_algorithms, build_reference_candidates, sort_algorithm_candidates, AlgorithmReport,
    ReferenceReport,
};
use crate::error::BenchError;
use crate::report::BenchmarkReport;
use crate::sample::BenchmarkSample;
use crate::store::{FeatureRow, FeatureStore};

#[derive(Clone, Debug)]
pub struct BenchmarkConfig {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub metadata_file: PathBuf,
    pub feature_db: PathBuf,
    pub feature_parquet: Option<PathBuf>,
    pub output: PathBuf,
    pub top_k: usize,
    pub repeat: usize,
}

fn is_recall_match(sample: &BenchmarkSample, row: &FeatureRow) -> bool {
    if !sample.contract_address.is_empty()
        && !sample.token_id.is_empty()
        && sample.contract_address.eq_ignore_ascii_case(&row.contract_address)
        && sample.token_id == row.token_id
    {
        return false;
    }

    let name_match = sample
        .name_prefix()
        .map(|prefix| !row.name_norm.is_empty() && row.name_norm.starts_with(&prefix))
        .unwrap_or(false);
    let metadata_match = !sample.metadata_keywords.is_empty()
        && !row.metadata_keywords.is_empty()
        && row
            .metadata_keywords
            .iter()
            .any(|keyword| sample.metadata_keywords.contains(keyword));
    name_match || metadata_match
}

pub fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkReport, BenchError> {
    let sample = BenchmarkSample::load(
        &config.chain,
        &config.contract_address,
        &config.token_id,
        &config.name,
        &config.metadata_file,
    )?;
    let store = FeatureStore::new(config.feature_db.clone(), config.feature_parquet.clone());
    let (source, rows) = store.load_rows(&config.chain)?;

    let recall_started = Instant::now();
    let recall_rows: Vec<FeatureRow> = rows
        .into_iter()
        .filter(|row| is_recall_match(&sample, row))
        .collect();
    let recall_elapsed_ms = recall_started.elapsed().as_secs_f64() * 1000.0;

    let repeat = config.repeat.max(1);
    let mut algorithm_reports = Vec::new();
    for algorithm in all_algorithms() {
        let mut runs_ms = Vec::new();
        let mut final_scores = Vec::new();
        for _ in 0..repeat {
            let started = Instant::now();
            let scores: Vec<f64> = recall_rows
                .iter()
                .map(|row| (algorithm.scorer)(&sample, row))
                .collect();
            let _ = sort_algorithm_candidates(&recall_rows, &scores, config.top_k);
            runs_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            final_scores = scores;
        }
        let (candidate_count, top_candidates) =
            sort_algorithm_candidates(&recall_rows, &final_scores, config.top_k);
        algorithm_reports.push(AlgorithmReport {
            algorithm_id: algorithm.id.to_string(),
            field: algorithm.field,
            repeat,
            avg_ms: runs_ms.iter().sum::<f64>() / runs_ms.len() as f64,
            min_ms: runs_ms.iter().copied().fold(f64::INFINITY, f64::min),
            candidate_count,
            top_candidates,
        });
    }

    let mut reference_runs_ms = Vec::new();
    let mut reference_top_candidates = Vec::new();
    let mut reference_candidate_count = 0;
    for _ in 0..repeat {
        let started = Instant::now();
        let (candidate_count, top_candidates) =
            build_reference_candidates(&sample, &recall_rows, config.top_k);
        reference_runs_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        reference_candidate_count = candidate_count;
        reference_top_candidates = top_candidates;
    }

    let report = BenchmarkReport {
        chain: config.chain.clone(),
        source,
        sample,
        recall_elapsed_ms,
        recall_candidate_count: recall_rows.len(),
        reference: ReferenceReport {
            algorithm_id: "current_name_metadata_reference".to_string(),
            field: crate::algorithms::AlgorithmField::Reference,
            repeat,
            avg_ms: reference_runs_ms.iter().sum::<f64>() / reference_runs_ms.len() as f64,
            min_ms: reference_runs_ms
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min),
            candidate_count: reference_candidate_count,
            top_candidates: reference_top_candidates,
        },
        algorithms: algorithm_reports,
    };
    write_report_outputs(&report, &config.output)?;
    Ok(report)
}

fn write_report_outputs(report: &BenchmarkReport, output_path: &PathBuf) -> Result<(), BenchError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, serde_json::to_string_pretty(report)?)?;
    let markdown_path = output_path.with_extension("md");
    fs::write(markdown_path, report.to_markdown())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;
    use tempfile::tempdir;

    fn write_sample_inputs(dir: &std::path::Path) -> PathBuf {
        let metadata_path = dir.join("metadata.json");
        fs::write(&metadata_path, r#"{"description":"rare dragon gold"}"#).unwrap();
        metadata_path
    }

    fn create_duckdb(db_path: &std::path::Path) {
        let conn = Connection::open(db_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE nft_features (
                chain VARCHAR,
                contract_address VARCHAR,
                token_id VARCHAR,
                token_uri VARCHAR,
                image_uri VARCHAR,
                name VARCHAR,
                symbol VARCHAR,
                metadata_json VARCHAR,
                metadata_doc VARCHAR,
                name_norm VARCHAR,
                metadata_keywords_arr VARCHAR
            );
            INSERT INTO nft_features VALUES
            ('ethereum', '0xby_uri_only', '1', 'ipfs://seed/meta-1', '', 'Completely Different', 'AZUKI', '{\"description\":\"nothing here\"}', 'nothing here', 'completely different', '[\"nothing\"]'),
            ('ethereum', '0xby_name', '2', '', '', 'Azuki Mirror #1', 'MIRROR', '{\"description\":\"nothing here\"}', 'nothing here', 'azuki mirror', '[\"nothing\"]'),
            ('ethereum', '0xby_metadata', '3', '', '', 'Totally Different', 'OTHER', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'totally different', '[\"rare\",\"dragon\",\"gold\"]');
            ",
        )
        .unwrap();
    }

    fn write_parquet(sql: &str, path: &std::path::Path) {
        let conn = Connection::open_in_memory().unwrap();
        let path = path.to_string_lossy().replace('\\', "/");
        conn.execute_batch(&format!("COPY ({sql}) TO '{path}' (FORMAT PARQUET)"))
            .unwrap();
    }

    #[test]
    fn benchmark_ignores_uri_and_symbol_only_matches() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        create_duckdb(&db_path);
        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");

        let report = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            top_k: 10,
            repeat: 1,
        })
        .unwrap();

        assert_eq!(report.recall_candidate_count, 2);
        assert!(
            report
                .reference
                .top_candidates
                .iter()
                .all(|candidate| candidate.contract_address != "0xby_uri_only")
        );
    }

    #[test]
    fn benchmark_falls_back_to_parquet_when_duckdb_chain_is_missing() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE nft_features (
                chain VARCHAR,
                contract_address VARCHAR,
                token_id VARCHAR,
                name VARCHAR
            );
            INSERT INTO nft_features VALUES ('base', '0xunused', '1', 'Unused');
            ",
        )
        .unwrap();
        drop(conn);

        let parquet_path = dir.path().join("snapshot.parquet");
        write_parquet(
            "
            SELECT
                'ethereum' AS chain,
                '0xparquet' AS contract_address,
                '7' AS token_id,
                'Azuki Replica' AS name,
                '{\"description\":\"rare dragon gold\"}' AS metadata_json,
                'rare dragon gold' AS metadata_doc,
                'azuki replica' AS name_norm,
                '[\"rare\",\"dragon\",\"gold\"]' AS metadata_keywords_arr
            ",
            &parquet_path,
        );

        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");
        let report = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: Some(parquet_path),
            output: output_path,
            top_k: 10,
            repeat: 1,
        })
        .unwrap();

        assert_eq!(report.source.kind, crate::store::SourceKind::ParquetFile);
        assert_eq!(report.recall_candidate_count, 1);
        assert_eq!(report.reference.top_candidates[0].contract_address, "0xparquet");
    }

    #[test]
    fn repeat_changes_only_timing_not_candidate_sets() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        create_duckdb(&db_path);
        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");

        let single = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path.clone(),
            feature_db: db_path.clone(),
            feature_parquet: None,
            output: output_path.clone(),
            top_k: 10,
            repeat: 1,
        })
        .unwrap();
        let repeated = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            top_k: 10,
            repeat: 3,
        })
        .unwrap();

        let single_reference: Vec<(String, String)> = single
            .reference
            .top_candidates
            .iter()
            .map(|candidate| (candidate.contract_address.clone(), candidate.token_id.clone()))
            .collect();
        let repeated_reference: Vec<(String, String)> = repeated
            .reference
            .top_candidates
            .iter()
            .map(|candidate| (candidate.contract_address.clone(), candidate.token_id.clone()))
            .collect();
        assert_eq!(single_reference, repeated_reference);

        let single_name: Vec<(String, String)> = single.algorithms[0]
            .top_candidates
            .iter()
            .map(|candidate| (candidate.contract_address.clone(), candidate.token_id.clone()))
            .collect();
        let repeated_name: Vec<(String, String)> = repeated.algorithms[0]
            .top_candidates
            .iter()
            .map(|candidate| (candidate.contract_address.clone(), candidate.token_id.clone()))
            .collect();
        assert_eq!(single_name, repeated_name);
    }
}
