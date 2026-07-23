//! Golden field shapes for offline duplicate-scale reports.

use analysis2_core::parquet::{write_report_golden_fixture, LoadOptions};
use analysis2_core::reporting::{
    build_contract_nft_map, build_seed_dedup_report, load_seeds_json, write_dedup_outputs,
    DedupRunParams, SeedRecord,
};
use analysis2_core::{
    load_resident_store, query_metadata_for_seed, query_name_for_seed, query_uri_for_seed,
    CandidateRegistry, HitGraph, NoopProgress, DEFAULT_METADATA_THRESHOLD, DEFAULT_NAME_THRESHOLD,
};
use serde_json::Value;
use std::path::PathBuf;

fn golden_paths() -> (PathBuf, PathBuf, PathBuf) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata");
    std::fs::create_dir_all(&dir).expect("testdata");
    let parquet = dir.join("report_golden.parquet");
    write_report_golden_fixture(&parquet).expect("fixture");
    let seeds = dir.join("report_golden_seeds.json");
    let seeds_json = serde_json::to_vec_pretty(&[SeedRecord {
        chain: "ethereum".into(),
        address: "0xseed".into(),
        rank: Some(1),
    }])
    .unwrap();
    std::fs::write(&seeds, seeds_json).unwrap();
    let out = dir.join("report_golden_out");
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    (parquet, seeds, out)
}

fn find_row<'a>(rows: &'a [Value], category: &str) -> &'a Value {
    rows.iter()
        .find(|r| r["category"] == category)
        .unwrap_or_else(|| panic!("missing category {category}"))
}

#[test]
fn report_golden_duplicate_scale_fields_and_ratios() {
    let (parquet, seeds_path, out) = golden_paths();
    let options = LoadOptions::new(
        ["ethereum", "base", "solana"].map(str::to_owned),
        ["ethereum", "base"].map(str::to_owned),
        8,
    );
    let store = load_resident_store(&[parquet], &options, &NoopProgress).expect("load");
    let seeds = load_seeds_json(&seeds_path).expect("seeds");
    assert_eq!(seeds.len(), 1);

    let seed_id = store
        .contract_id("ethereum", "0xseed")
        .expect("seed contract");

    let mut graph = HitGraph::new();
    query_uri_for_seed(&store, seed_id, &mut graph, &NoopProgress).unwrap();
    query_name_for_seed(
        &store,
        seed_id,
        DEFAULT_NAME_THRESHOLD,
        &mut graph,
        &NoopProgress,
    )
    .unwrap();
    query_metadata_for_seed(
        &store,
        seed_id,
        DEFAULT_METADATA_THRESHOLD,
        &mut graph,
        &NoopProgress,
    )
    .unwrap();

    let contract_nfts = build_contract_nft_map(&store);
    let registry = CandidateRegistry::from_hit_graph(&graph, &contract_nfts);
    let report = build_seed_dedup_report(
        &store,
        &seeds[0],
        seed_id,
        &graph,
        &registry,
        &contract_nfts,
    );

    let intra = &report.duplicate_scale.intra_chain;
    let token = intra.iter().find(|r| r.category == "token_uri").unwrap();
    assert_eq!(token.duplicate_nft_count, 1);
    assert_eq!(token.duplicate_contract_count, 1);
    assert_eq!(token.duplicate_nft_ratio_numerator, 1);
    assert_eq!(token.duplicate_nft_ratio_denominator, 4); // ethereum total NFTs
    assert_eq!(token.duplicate_contract_ratio_denominator, 2);
    assert!((token.duplicate_nft_ratio.unwrap() - 0.25).abs() < 1e-12);

    let total = intra.iter().find(|r| r.category == "total").unwrap();
    assert_eq!(total.duplicate_nft_count, 1);
    assert_eq!(total.duplicate_contract_count, 1);

    let matrix_base = report
        .duplicate_scale
        .chain_matrix
        .iter()
        .find(|b| b.secondary_chain == "base")
        .expect("eth→base matrix");
    let m_token = matrix_base
        .rows
        .iter()
        .find(|r| r.category == "token_uri")
        .unwrap();
    assert_eq!(m_token.duplicate_nft_count, 1);
    assert_eq!(m_token.duplicate_nft_ratio_denominator, 4);

    let cross = report
        .duplicate_scale
        .cross_chain_summary
        .iter()
        .find(|r| r.category == "token_uri")
        .unwrap();
    assert_eq!(cross.duplicate_nft_count, 1);

    let params = DedupRunParams {
        command: "run-dedup".into(),
        inputs: vec!["report_golden.parquet".into()],
        chains: vec!["ethereum".into(), "base".into(), "solana".into()],
        evm_chains: vec!["ethereum".into(), "base".into()],
        name_threshold: DEFAULT_NAME_THRESHOLD,
        metadata_threshold: DEFAULT_METADATA_THRESHOLD,
        metadata_anchors: 8,
    };
    write_dedup_outputs(
        &out,
        &params,
        &store,
        &seeds,
        &[Ok((seeds[0].clone(), report))],
        &[],
    )
    .expect("write");

    let seed_json: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("seeds/ethereum__0xseed/report.json")).unwrap(),
    )
    .unwrap();
    let intra_rows = seed_json["duplicate_scale"]["intra_chain"]
        .as_array()
        .unwrap();
    let token_row = find_row(intra_rows, "token_uri");
    assert_eq!(token_row["duplicate_nft_count"], 1);
    assert_eq!(token_row["duplicate_nft_ratio_denominator"], 4);
    assert!(token_row.get("duplicate_nft_ratio").is_some());
    assert!(token_row.get("duplicate_contract_ratio_numerator").is_some());
    assert!(find_row(intra_rows, "total").get("duplicate_nft_count").is_some());
    assert!(find_row(intra_rows, "name").get("duplicate_nft_count").is_some());
    assert!(find_row(intra_rows, "image_uri").get("duplicate_nft_count").is_some());
    assert!(find_row(intra_rows, "metadata").get("duplicate_nft_count").is_some());

    assert!(out.join("seeds/ethereum__0xseed/report.md").is_file());
    assert!(out.join("intra_chain.json").is_file());
    assert!(out.join("chain_matrix.json").is_file());
    assert!(out.join("cross_chain.json").is_file());
    assert!(out.join("summary.json").is_file());
    assert!(out.join("run_manifest.json").is_file());
    assert!(out.join("failures.jsonl").is_file());

    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("run_manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["seeds"]["analyzed"], 1);

    let summary: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();
    assert_eq!(summary["analyzed_seed_count"], 1);
    assert_eq!(summary["seed_with_duplicate_count"], 1);
}
