use std::fs;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::algorithms::{
    build_algorithm_duplicates_raw_from_scores, metadata_duplicates_from_candidates,
    name_duplicates_from_candidates, score_metadata_bm25_all_rows_raw, score_rows_parallel_raw,
    timing_algorithms, AlgorithmField, CandidateScore, MetadataAlgorithmReport,
    NameAlgorithmReport,
};
use crate::decision_rules::duplicate_score_rule;
use crate::error::BenchError;
use crate::report::BenchmarkReport;
use crate::sample::BenchmarkSample;
use crate::store::FeatureStore;

enum TimedAlgorithmResult {
    Name(usize, Vec<crate::algorithms::NameContractDuplicate>),
    Metadata(usize, Vec<CandidateScore>),
}

#[derive(Clone, Debug)]
pub struct BenchmarkConfig {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub metadata_file: PathBuf,
    pub feature_db: PathBuf,
    pub feature_parquet: Option<PathBuf>,
    pub output: PathBuf,
    pub repeat: usize,
    pub algorithm_threads: usize,
}

pub fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkReport, BenchError> {
    let sample = BenchmarkSample::load(
        &config.chain,
        &config.contract_address,
        &config.token_id,
        &config.token_uri,
        &config.image_uri,
        &config.name,
        &config.metadata_file,
    )?;
    let store = FeatureStore::new(config.feature_db.clone(), config.feature_parquet.clone());
    let recall_started = Instant::now();
    let (source, recall_rows) = store.load_recall_rows(&sample)?;
    let recall_elapsed_ms = recall_started.elapsed().as_secs_f64() * 1000.0;
    let uri_matched_contracts = uri_matched_contracts(&sample, &recall_rows);
    let algorithm_thread_pool = algorithm_thread_pool(config.algorithm_threads)?;

    let repeat = config.repeat.max(1);
    let algorithms = timing_algorithms();
    for algorithm in &algorithms {
        algorithm_thread_pool.install(|| -> Result<(), BenchError> {
            let scores = score_algorithm_rows_raw(&sample, &recall_rows, *algorithm);
            let _ = build_algorithm_duplicates_raw_from_scores(algorithm.id, &recall_rows, &scores)
                .map_err(BenchError::InvalidData)?;
            Ok(())
        })?;
    }

    let mut algorithm_runs_ms: Vec<Vec<f64>> = (0..algorithms.len())
        .map(|_| Vec::with_capacity(repeat))
        .collect();
    let mut algorithm_results: Vec<Option<TimedAlgorithmResult>> =
        (0..algorithms.len()).map(|_| None).collect();
    let total_timed_units = algorithms.len();
    for repeat_index in 0..repeat {
        for offset in 0..total_timed_units {
            let timed_unit_index = (repeat_index + offset) % total_timed_units;
            let algorithm = algorithms[timed_unit_index];
            let started = Instant::now();
            let result = algorithm_thread_pool.install(|| -> Result<TimedAlgorithmResult, BenchError> {
                let scores = score_algorithm_rows_raw(&sample, &recall_rows, algorithm);
                let (_, duplicates) = build_algorithm_duplicates_raw_from_scores(
                    algorithm.id,
                    &recall_rows,
                    &scores,
                )
                .map_err(BenchError::InvalidData)?;
                Ok(match algorithm.field {
                    AlgorithmField::Name => {
                        let duplicates = name_duplicates_from_candidates(&recall_rows, duplicates);
                        TimedAlgorithmResult::Name(duplicates.len(), duplicates)
                    }
                    AlgorithmField::Metadata => {
                        TimedAlgorithmResult::Metadata(duplicates.len(), duplicates)
                    }
                })
            })?;
            algorithm_runs_ms[timed_unit_index]
                .push(started.elapsed().as_secs_f64() * 1000.0);
            algorithm_results[timed_unit_index] = Some(result);
        }
    }
    let mut name_algorithm_reports = Vec::new();
    let mut metadata_algorithm_reports = Vec::new();
    for (index, algorithm) in algorithms.iter().enumerate() {
        let runs_ms = std::mem::take(&mut algorithm_runs_ms[index]);
        let avg_ms = runs_ms.iter().sum::<f64>() / runs_ms.len() as f64;
        let min_ms = runs_ms.iter().copied().fold(f64::INFINITY, f64::min);
        let decision_rule = duplicate_score_rule(algorithm.id)
            .map(|rule| rule.description.to_string())
            .unwrap_or_else(|_| "missing duplicate rule".to_string());
        match algorithm_results[index]
            .take()
            .unwrap_or_else(|| match algorithm.field {
                AlgorithmField::Name => TimedAlgorithmResult::Name(0, Vec::new()),
                AlgorithmField::Metadata => TimedAlgorithmResult::Metadata(0, Vec::new()),
            }) {
            TimedAlgorithmResult::Name(_duplicate_count, duplicates) => {
                let duplicates = filter_name_duplicates(duplicates, &uri_matched_contracts);
                name_algorithm_reports.push(NameAlgorithmReport {
                    algorithm_id: algorithm.id.to_string(),
                    field: algorithm.field,
                    decision_rule,
                    repeat,
                    runs_ms,
                    avg_ms,
                    min_ms,
                    duplicate_count: duplicates.len(),
                    duplicates,
                });
            }
            TimedAlgorithmResult::Metadata(_duplicate_count, duplicates) => {
                let duplicates = metadata_duplicates_from_candidates(
                    &sample,
                    &recall_rows,
                    duplicates,
                    algorithm.id,
                );
                let duplicates = filter_metadata_duplicates(duplicates, &uri_matched_contracts);
                metadata_algorithm_reports.push(MetadataAlgorithmReport {
                    algorithm_id: algorithm.id.to_string(),
                    field: algorithm.field,
                    decision_rule,
                    repeat,
                    runs_ms,
                    avg_ms,
                    min_ms,
                    duplicate_count: duplicates.len(),
                    duplicates,
                });
            }
        }
    }
    let report = BenchmarkReport {
        chain: config.chain.clone(),
        source,
        sample,
        recall_elapsed_ms,
        recall_candidate_count: recall_rows.len(),
        name_algorithms: name_algorithm_reports,
        metadata_algorithms: metadata_algorithm_reports,
    };
    write_report_outputs(&report, &config.output)?;
    Ok(report)
}

fn uri_matched_contracts(
    sample: &BenchmarkSample,
    rows: &[crate::store::FeatureRow],
) -> HashSet<String> {
    rows.iter()
        .filter(|row| {
            let token_uri_match = !sample.token_uri.is_empty()
                && row
                    .token_uris
                    .iter()
                    .any(|uri| uri.starts_with(&sample.token_uri));
            let image_uri_match = !sample.image_uri.is_empty()
                && row
                    .image_uris
                    .iter()
                    .any(|uri| uri.starts_with(&sample.image_uri));
            token_uri_match || image_uri_match
        })
        .map(|row| row.contract_address.clone())
        .collect()
}

fn filter_name_duplicates(
    duplicates: Vec<crate::algorithms::NameContractDuplicate>,
    uri_matched_contracts: &HashSet<String>,
) -> Vec<crate::algorithms::NameContractDuplicate> {
    duplicates
        .into_iter()
        .filter(|candidate| !uri_matched_contracts.contains(&candidate.contract_address))
        .collect()
}

fn filter_metadata_duplicates(
    duplicates: Vec<crate::algorithms::MetadataDuplicate>,
    uri_matched_contracts: &HashSet<String>,
) -> Vec<crate::algorithms::MetadataDuplicate> {
    duplicates
        .into_iter()
        .filter(|candidate| !uri_matched_contracts.contains(&candidate.contract_address))
        .collect()
}

fn score_algorithm_rows_raw(
    sample: &BenchmarkSample,
    rows: &[crate::store::FeatureRow],
    algorithm: crate::algorithms::TimingAlgorithmDefinition,
) -> Vec<f64> {
    if algorithm.id == "metadata_bm25" {
        score_metadata_bm25_all_rows_raw(sample, rows)
    } else {
        score_rows_parallel_raw(sample, rows, algorithm.scorer)
    }
}

fn algorithm_thread_pool(threads: usize) -> Result<ThreadPool, BenchError> {
    if threads == 0 {
        return Err(BenchError::InvalidData(
            "algorithm_threads must be greater than 0".to_string(),
        ));
    }
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|error| {
            BenchError::InvalidData(format!("failed to create rayon thread pool: {error}"))
        })
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
            ('ethereum', '0xseed', '9', '', '', 'Excluded Seed #9', 'SEED', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]'),
            ('ethereum', '0xby_name', '2', '', '', 'Azuki #2', 'MIRROR', '{\"description\":\"nothing here\"}', 'nothing here', 'azuki', '[\"nothing\"]'),
            ('ethereum', '0xby_name', '4', '', '', 'Azuki #4', 'MIRROR', '{\"description\":\"nothing here\"}', 'nothing here', 'azuki', '[\"nothing\"]'),
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
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();

        assert_eq!(report.recall_candidate_count, 2);
        assert!(
            report
                .metadata_algorithms
                .iter()
                .flat_map(|algorithm| algorithm.duplicates.iter())
                .all(|candidate| candidate.contract_address != "0xby_uri_only")
        );
        assert!(
            report
                .metadata_algorithms
                .iter()
                .flat_map(|algorithm| algorithm.duplicates.iter())
                .all(|candidate| candidate.contract_address != "0xseed")
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
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path.clone(),
            feature_db: db_path.clone(),
            feature_parquet: Some(parquet_path),
            output: output_path,
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();

        assert_eq!(report.source.kind, crate::store::SourceKind::DuckdbTable);
        assert_eq!(report.recall_candidate_count, 1);
        assert_eq!(
            report.metadata_algorithms[0].duplicates[0].contract_address,
            "0xparquet"
        );

        let second_output_path = dir.path().join("report-second.json");
        let second_report = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: second_output_path,
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();

        assert_eq!(second_report.source.kind, crate::store::SourceKind::DuckdbTable);
        assert_eq!(second_report.recall_candidate_count, 1);
        assert_eq!(
            second_report.metadata_algorithms[0].duplicates[0].contract_address,
            "0xparquet"
        );
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
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path.clone(),
            feature_db: db_path.clone(),
            feature_parquet: None,
            output: output_path.clone(),
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();
        let repeated = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            repeat: 3,
            algorithm_threads: 30,
        })
        .unwrap();

        let single_name: Vec<String> = single.name_algorithms[0]
            .duplicates
            .iter()
            .map(|candidate| candidate.contract_address.clone())
            .collect();
        let repeated_name: Vec<String> = repeated.name_algorithms[0]
            .duplicates
            .iter()
            .map(|candidate| candidate.contract_address.clone())
            .collect();
        assert_eq!(single_name, repeated_name);
    }

    #[test]
    fn zero_algorithm_threads_is_rejected() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        create_duckdb(&db_path);
        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");

        let err = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            repeat: 1,
            algorithm_threads: 0,
        })
        .unwrap_err();

        assert!(err.to_string().contains("algorithm_threads"));
    }

    #[test]
    fn markdown_report_mentions_duplicate_counts() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        create_duckdb(&db_path);
        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");

        let report = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();

        let markdown = report.to_markdown();
        assert!(markdown.contains("duplicate_count"));
        assert!(!markdown.contains("top_candidates"));
        assert!(markdown.contains("## Name Algorithms"));
        assert!(markdown.contains("## Metadata Algorithms"));
    }

    #[test]
    fn benchmark_splits_name_and_metadata_reports() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        create_duckdb(&db_path);
        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");

        let report = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();

        let name_report = report
            .name_algorithms
            .iter()
            .find(|algorithm| algorithm.algorithm_id == "name_exact_normalized")
            .unwrap();
        assert_eq!(name_report.duplicate_count, 1);
        assert_eq!(name_report.duplicates[0].contract_address, "0xby_name");
        assert_eq!(name_report.duplicates[0].name, "Azuki #2");
        assert_eq!(name_report.duplicates[0].duplicate_token_count, 2);

        let metadata_report = report
            .metadata_algorithms
            .iter()
            .find(|algorithm| algorithm.algorithm_id == "metadata_bm25")
            .unwrap();
        assert_eq!(metadata_report.duplicate_count, 1);
        assert_eq!(metadata_report.duplicates[0].contract_address, "0xby_metadata");
        assert_eq!(metadata_report.duplicates[0].metadata_doc, "rare dragon gold");
    }

    #[test]
    fn benchmark_exports_only_uri_novel_name_and_metadata_duplicates() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        let conn = Connection::open(&db_path).unwrap();
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
            ('ethereum', '0xuri', '1', 'ipfs://Seed/1', 'ipfs://Images/1.PNG', 'Azuki #2', 'MIRROR', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]'),
            ('ethereum', '0xuri2', '3', 'ipfs://Seed/2', 'ipfs://Images/2.PNG', 'Azuki #4', 'MIRROR', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]'),
            ('ethereum', '0xnovel', '2', 'ipfs://Other/2', 'ipfs://OtherImage/2.PNG', 'Azuki #3', 'MIRROR', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]');
            ",
        )
        .unwrap();
        drop(conn);

        let metadata_path = write_sample_inputs(dir.path());
        let output_path = dir.path().join("report.json");

        let report = run_benchmark(&BenchmarkConfig {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: "ipfs://Seed/".into(),
            image_uri: "ipfs://Images/".into(),
            name: "Azuki #1".into(),
            metadata_file: metadata_path,
            feature_db: db_path,
            feature_parquet: None,
            output: output_path,
            repeat: 1,
            algorithm_threads: 30,
        })
        .unwrap();

        assert!(report
            .name_algorithms
            .iter()
            .all(|algorithm| algorithm
                .duplicates
                .iter()
                .all(|candidate| candidate.contract_address != "0xuri")));
        assert!(report
            .name_algorithms
            .iter()
            .all(|algorithm| algorithm
                .duplicates
                .iter()
                .all(|candidate| candidate.contract_address != "0xuri2")));
        assert!(report
            .metadata_algorithms
            .iter()
            .all(|algorithm| algorithm
                .duplicates
                .iter()
                .all(|candidate| candidate.contract_address != "0xuri")));
        assert!(report
            .metadata_algorithms
            .iter()
            .all(|algorithm| algorithm
                .duplicates
                .iter()
                .all(|candidate| candidate.contract_address != "0xuri2")));
        assert!(report
            .name_algorithms
            .iter()
            .any(|algorithm| algorithm
                .duplicates
                .iter()
                .any(|candidate| candidate.contract_address == "0xnovel")));
        assert!(report
            .metadata_algorithms
            .iter()
            .any(|algorithm| algorithm
                .duplicates
                .iter()
                .any(|candidate| candidate.contract_address == "0xnovel")));
    }

    #[test]
    fn uri_prefix_matching_filters_all_children_under_prefix() {
        let sample = BenchmarkSample {
            chain: "ethereum".into(),
            contract_address: String::new(),
            token_id: "1".into(),
            token_uri: "ipfs://Seed/".into(),
            image_uri: "ipfs://Images/".into(),
            name: "Azuki #1".into(),
            name_norm: crate::algorithms::derive_name_norm("Azuki #1"),
            metadata_json: "{}".into(),
            metadata_doc: String::new(),
            metadata_display_doc: String::new(),
            metadata_keywords: Vec::new(),
        };
        let rows = vec![
            crate::store::FeatureRow {
                contract_address: "0xuri".into(),
                token_id: "2".into(),
                token_uri: "ipfs://Seed/2".into(),
                image_uri: "ipfs://Images/image_002.PNG".into(),
                name: String::new(),
                name_norm: String::new(),
                metadata_doc: String::new(),
                metadata_display_doc: String::new(),
                metadata_docs: Vec::new(),
                metadata_display_docs: Vec::new(),
                token_uris: vec!["ipfs://Seed/2".into()],
                image_uris: vec!["ipfs://Images/image_002.PNG".into()],
                metadata_keywords: Vec::new(),
                token_count: 1,
            },
        ];

        let matched = uri_matched_contracts(&sample, &rows);
        assert!(matched.contains("0xuri"));
    }
}
