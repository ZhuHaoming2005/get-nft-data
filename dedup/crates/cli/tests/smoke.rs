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
                "data:image/png;base64,iVBORw0KGgo=",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
            [
                "ethereum",
                "0xb",
                "1",
                "collection",
                "ipfs://shared/1",
                "data:image/png;base64,iVBORw0KGgo=",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
            [
                "base",
                "0xc",
                "1",
                "collection",
                "ipfs://other/1",
                "data:image/png;base64,iVBORw0KGgo=",
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
            "--name-threshold",
            "98",
            "--metadata-anchors",
            "2",
            "--sample-pairs",
            "1",
            "--threads",
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
    for (name, expected_rows) in [
        ("name_duplicate_pairs.csv", 1),
        ("metadata_duplicate_pairs.csv", 1),
    ] {
        let path = out.join(name);
        assert!(path.is_file(), "missing duplicate-pair sample {name}");
        let mut reader = csv::Reader::from_path(path).unwrap();
        assert_eq!(
            reader.headers().unwrap(),
            [
                "contract_a_chain",
                "contract_a_address",
                "contract_b_chain",
                "contract_b_address",
            ]
            .as_slice()
        );
        let rows = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            rows.len(),
            expected_rows,
            "unexpected image-qualified sample count in {name}"
        );
        assert!(
            rows.iter()
                .all(|row| row.iter().all(|value| !value.is_empty()))
        );
    }
    let image_manifest = out.join("metadata_image_samples.csv");
    assert!(image_manifest.is_file());
    let mut image_reader = csv::Reader::from_path(image_manifest).unwrap();
    assert_eq!(
        image_reader.headers().unwrap(),
        [
            "row",
            "contract_a_chain",
            "contract_a_address",
            "token_id_a",
            "image_uri_a",
            "file_a",
            "error_a",
            "contract_b_chain",
            "contract_b_address",
            "token_id_b",
            "image_uri_b",
            "file_b",
            "error_b",
        ]
        .as_slice()
    );
    assert_eq!(image_reader.records().count(), 1);
    assert!(out.join("metadata_sample_images/1/1a.png").is_file());
    assert!(out.join("metadata_sample_images/1/1b.png").is_file());
    for dimension in ["name", "metadata"] {
        for (suffix, group_columns) in [
            ("intra_chain", &["chain"][..]),
            ("chain_matrix", &["primary_chain", "secondary_chain"][..]),
            ("cross_chain_summary", &["chain"][..]),
        ] {
            let name = format!("{dimension}_duplicate_pairs_{suffix}.csv");
            let path = out.join(&name);
            assert!(
                path.is_file(),
                "missing scoped duplicate-pair sample {name}"
            );
            let mut reader = csv::Reader::from_path(path).unwrap();
            let headers = reader.headers().unwrap();
            assert_eq!(
                &headers.iter().collect::<Vec<_>>()[..group_columns.len()],
                group_columns
            );
            assert_eq!(
                &headers.iter().collect::<Vec<_>>()[group_columns.len()..],
                [
                    "contract_a_chain",
                    "contract_a_address",
                    "contract_b_chain",
                    "contract_b_address",
                ]
            );
            let mut group_counts = std::collections::HashMap::<Vec<String>, usize>::new();
            for row in reader.records() {
                let row = row.unwrap();
                let group = row
                    .iter()
                    .take(group_columns.len())
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                *group_counts.entry(group.clone()).or_default() += 1;
                let a_chain = &row[group_columns.len()];
                let b_chain = &row[group_columns.len() + 2];
                match suffix {
                    "intra_chain" => {
                        assert_eq!(a_chain, b_chain);
                        assert_eq!(a_chain, &group[0]);
                    }
                    "chain_matrix" => {
                        assert_ne!(a_chain, b_chain);
                        assert!(group.contains(&a_chain.to_owned()));
                        assert!(group.contains(&b_chain.to_owned()));
                    }
                    "cross_chain_summary" => {
                        assert_ne!(a_chain, b_chain);
                        assert!(a_chain == group[0] || b_chain == group[0]);
                    }
                    _ => unreachable!(),
                }
            }
            assert!(group_counts.values().all(|&count| count <= 1));
        }
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
    assert!(manifest["metadata_direct"]["candidate_pair_bytes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_prefix_terms"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_prefix_term_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_pair_emissions"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_pair_emission_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_pair_dedup_reduction_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_profile_pairs"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_profile_pair_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["candidate_zero_overlap_prunes"].is_u64());
    assert!(manifest["metadata_direct"]["candidate_zero_overlap_prune_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["block_saturated_profile_pairs"].is_u64());
    assert!(manifest["metadata_direct"]["block_saturated_profile_pair_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_scores"].is_u64());
    assert!(manifest["metadata_direct"]["bm25_score_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_cache_probes"].is_u64());
    assert!(manifest["metadata_direct"]["bm25_cache_hit_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_cache_bypass_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_upper_bound_prune_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_initial_upper_bound_prunes"].is_u64());
    assert!(manifest["metadata_direct"]["bm25_initial_upper_bound_prune_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["bm25_iterative_upper_bound_prunes"].is_u64());
    assert!(manifest["metadata_direct"]["bm25_iterative_upper_bound_prune_ratio"].is_f64());
    assert!(manifest["metadata_direct"]["matched_profile_pair_ratio"].is_f64());
    assert_eq!(manifest["sample_pairs"], 1);
    assert_eq!(manifest["threads"], 2);
    assert!(
        manifest["interned_strings"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    assert!(
        manifest["token_uri_postings"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
}

#[test]
fn omitted_name_threshold_disables_name_and_omitted_anchors_are_unbounded() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(
        &input,
        &[[
            "ethereum",
            "0xa",
            "1",
            "name-is-not-loaded",
            "",
            "",
            r#"{"name":"metadata"}"#,
        ]],
    );
    let out = temp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "all",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--evm-chains",
            "ethereum",
            "--progress",
            "off",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(!out.join("name_summary.csv").exists());
    assert!(!out.join("name_chain_matrix.csv").exists());
    for name in [
        "name_duplicate_pairs.csv",
        "metadata_duplicate_pairs.csv",
        "metadata_image_samples.csv",
    ] {
        assert!(
            !out.join(name).exists(),
            "sampling must be explicitly enabled"
        );
    }
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("run_manifest.json")).unwrap()).unwrap();
    assert!(manifest["name_threshold"].is_null());
    assert!(manifest["metadata_anchors"].is_null());
    assert_eq!(manifest["interned_strings"], 0);
}

#[test]
fn explicit_sampling_does_not_change_dedup_results() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(
        &input,
        &[
            [
                "ethereum",
                "0xa",
                "1",
                "",
                "",
                "data:image/png;base64,iVBORw0KGgo=",
                r#"{"collection":"shared","name":"one"}"#,
            ],
            [
                "ethereum",
                "0xb",
                "1",
                "",
                "",
                "data:image/png;base64,iVBORw0KGgo=",
                r#"{"collection":"shared","name":"one"}"#,
            ],
        ],
    );
    let without_sampling = temp.path().join("without-sampling");
    let with_sampling = temp.path().join("with-sampling");
    let exe = env!("CARGO_BIN_EXE_dedup");

    for (output, sample_pairs) in [(&without_sampling, None), (&with_sampling, Some("1"))] {
        let mut command = Command::new(exe);
        command.args([
            "run-metadata",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            output.to_str().unwrap(),
            "--evm-chains",
            "ethereum",
            "--progress",
            "off",
        ]);
        if let Some(limit) = sample_pairs {
            command.args(["--sample-pairs", limit]);
        }
        assert!(command.status().unwrap().success());
    }

    for report in ["summary.csv", "chain_matrix.csv"] {
        assert_eq!(
            std::fs::read(without_sampling.join(report)).unwrap(),
            std::fs::read(with_sampling.join(report)).unwrap(),
            "sampling changed {report}"
        );
    }
    assert!(
        !without_sampling
            .join("metadata_duplicate_pairs.csv")
            .exists()
    );
    assert!(with_sampling.join("metadata_duplicate_pairs.csv").exists());
    assert!(with_sampling.join("metadata_image_samples.csv").exists());
}

#[test]
fn failed_media_pairs_are_replaced_until_the_target_is_complete() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    let good = "data:image/png;base64,iVBORw0KGgo=";
    write_parquet(
        &input,
        &[
            [
                "ethereum",
                "0x0",
                "1",
                "",
                "",
                "unsupported://zero",
                r#"{"collection":"shared"}"#,
            ],
            [
                "ethereum",
                "0x1",
                "1",
                "",
                "",
                good,
                r#"{"collection":"shared"}"#,
            ],
            [
                "ethereum",
                "0x2",
                "1",
                "",
                "",
                good,
                r#"{"collection":"shared"}"#,
            ],
            [
                "ethereum",
                "0x3",
                "1",
                "",
                "",
                "unsupported://three",
                r#"{"collection":"shared"}"#,
            ],
        ],
    );
    let out = temp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "run-metadata",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--evm-chains",
            "ethereum",
            "--progress",
            "off",
            "--sample-pairs",
            "1",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    let mut manifest = csv::Reader::from_path(out.join("metadata_image_samples.csv")).unwrap();
    let rows = manifest.records().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][2], "0x1");
    assert_eq!(&rows[0][8], "0x2");
    assert!(out.join("metadata_sample_images/1/1a.png").is_file());
    assert!(out.join("metadata_sample_images/1/1b.png").is_file());
}

#[test]
fn exhausted_media_candidates_fail_without_hiding_dedup_reports() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(
        &input,
        &[
            [
                "ethereum",
                "0xa",
                "1",
                "",
                "",
                "unsupported://a",
                r#"{"collection":"shared"}"#,
            ],
            [
                "ethereum",
                "0xb",
                "1",
                "",
                "",
                "unsupported://b",
                r#"{"collection":"shared"}"#,
            ],
        ],
    );
    let out = temp.path().join("out");
    std::fs::create_dir_all(out.join("metadata_sample_images/1")).unwrap();
    std::fs::write(out.join("metadata_sample_images/1/stale.png"), b"stale").unwrap();
    std::fs::write(out.join("metadata_image_samples.csv"), b"stale").unwrap();
    std::fs::write(out.join("metadata_duplicate_pairs.csv"), b"stale").unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "run-metadata",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--evm-chains",
            "ethereum",
            "--progress",
            "off",
            "--sample-pairs",
            "1",
        ])
        .status()
        .unwrap();

    assert!(!status.success());
    assert!(out.join("summary.csv").is_file());
    assert!(out.join("chain_matrix.csv").is_file());
    assert!(!out.join("metadata_image_samples.csv").exists());
    assert!(!out.join("metadata_sample_images").exists());
    assert!(!out.join("metadata_duplicate_pairs.csv").exists());
}
