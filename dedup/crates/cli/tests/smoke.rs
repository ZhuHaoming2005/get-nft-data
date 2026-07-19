use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

fn write_parquet(path: &Path, rows: &[[&str; 7]]) {
    let schema = Arc::new(Schema::new(
        [
            "chain",
            "contract_address",
            "token_id",
            "name_norm",
            "token_uri_norm",
            "image_uri_norm",
            "metadata_json",
        ]
        .into_iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect::<Vec<_>>(),
    ));
    let mut columns = vec![Vec::new(); 7];
    for row in rows {
        for (i, value) in row.iter().enumerate() {
            columns[i].push((*value).to_owned());
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn all_writes_summary_files() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(
        &input,
        &[
            [
                "ethereum",
                "0xa",
                "1",
                "collection",
                "ipfs://shared/1",
                "",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
            [
                "ethereum",
                "0xb",
                "1",
                "collection",
                "ipfs://shared/1",
                "",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
            [
                "base",
                "0xc",
                "1",
                "collection",
                "ipfs://other/1",
                "",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
        ],
    );
    let out = temp.path().join("out");
    let exe = env!("CARGO_BIN_EXE_dedup");
    let status = Command::new(exe)
        .args([
            "all",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--chains",
            "ethereum,base",
            "--evm-chains",
            "ethereum,base",
            "--progress",
            "off",
            "--metadata-anchors",
            "2",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    for name in ["summary.csv", "chain_matrix.csv"] {
        let path = out.join(name);
        assert!(path.is_file());
        let mut reader = csv::Reader::from_path(path).unwrap();
        let headers = reader.headers().unwrap().clone();
        let contract_count = headers
            .iter()
            .position(|header| header == "duplicate_contract_count")
            .unwrap();
        let contract_ratio = headers
            .iter()
            .position(|header| header == "duplicate_contract_ratio")
            .unwrap();
        let nft_count = headers
            .iter()
            .position(|header| header == "duplicate_nft_count")
            .unwrap();
        let nft_ratio = headers
            .iter()
            .position(|header| header == "duplicate_nft_ratio")
            .unwrap();
        let total_contracts = headers
            .iter()
            .position(|header| header == "total_contracts")
            .unwrap();
        let total_nfts = headers
            .iter()
            .position(|header| header == "total_nfts")
            .unwrap();
        let mut row_count = 0;
        for row in reader.records() {
            let row = row.unwrap();
            let contracts = row[contract_count].parse::<u64>().unwrap();
            let contract_total = row[total_contracts].parse::<u64>().unwrap();
            let actual_contract_ratio = row[contract_ratio].parse::<f64>().unwrap();
            let nfts = row[nft_count].parse::<u64>().unwrap();
            let nft_total = row[total_nfts].parse::<u64>().unwrap();
            let actual_nft_ratio = row[nft_ratio].parse::<f64>().unwrap();
            let expected_contract_ratio = contracts as f64 / contract_total as f64;
            let expected_nft_ratio = nfts as f64 / nft_total as f64;
            assert!((actual_contract_ratio - expected_contract_ratio).abs() < f64::EPSILON);
            assert!((actual_nft_ratio - expected_nft_ratio).abs() < f64::EPSILON);
            row_count += 1;
        }
        assert!(row_count > 0);
    }
    for (name, allowed_dimensions) in [
        ("name_summary.csv", &["name"][..]),
        ("name_chain_matrix.csv", &["name"][..]),
        ("uri_summary.csv", &["token_uri", "image_uri"][..]),
        ("uri_chain_matrix.csv", &["token_uri", "image_uri"][..]),
    ] {
        let path = out.join(name);
        assert!(path.is_file(), "missing partition report {name}");
        let mut reader = csv::Reader::from_path(path).unwrap();
        let dimension = reader
            .headers()
            .unwrap()
            .iter()
            .position(|header| header == "dimension")
            .unwrap();
        for row in reader.records() {
            let row = row.unwrap();
            assert!(
                allowed_dimensions.contains(&&row[dimension]),
                "unexpected dimension in {name}: {}",
                &row[dimension]
            );
        }
    }
    let manifest_path = out.join("run_manifest.json");
    assert!(manifest_path.is_file());
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    assert!(
        manifest["phase_timings"]
            .as_array()
            .is_some_and(|timings| !timings.is_empty())
    );
    assert!(manifest["metadata_direct"]["logical_contract_pairs"].is_u64());
    assert!(manifest["metadata_direct"]["profile_pair_tasks"].is_u64());
    assert!(manifest["metadata_direct"]["unique_terms"].is_u64());
    assert!(manifest["metadata_direct"]["profile_pair_reduction_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_index_used"].is_boolean());
    assert!(manifest["metadata_direct"]["candidate_posting_entries"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_posting_bytes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_range_bytes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_index_bytes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_posting_budget_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_index_budget_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_pair_bytes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_pair_budget_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_prefix_terms"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_prefix_term_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_pair_emissions"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_pair_emission_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_pair_dedup_reduction_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_profile_pairs"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_profile_pair_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_zero_overlap_prunes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_zero_overlap_prune_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_generation_fallback"].is_boolean());
    assert!(manifest["metadata_direct"]["full_prepass_pairs"].is_u64());
    assert!(manifest["metadata_direct"]["full_prepass_pair_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["block_saturated_profile_pairs"].is_u64());
    assert!(manifest["metadata_direct"]["block_saturated_profile_pair_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_scores"].is_u64());
    assert!(manifest["metadata_direct"]["bm25_score_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_cache_probes"].is_u64());
    assert!(manifest["metadata_direct"]["bm25_cache_hit_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_cache_bypass_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_upper_bound_prune_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["matched_profile_pair_ratio"].is_f64());
}
