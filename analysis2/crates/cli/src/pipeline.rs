//! Offline `run-dedup` and full `run` orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use analysis2_core::{
    analyze_candidate, build_contract_nft_map, build_seed_analysis_rollup, build_seed_dedup_report,
    enrich_candidates, load_resident_store, load_seeds_json, query_metadata_for_seed,
    query_name_for_seed, query_uri_for_seed, resolve_seed_contract, scopes_complete_for_seed,
    write_candidate_json, write_dedup_outputs, write_run_outputs, Analysis2Error, ApiKeys,
    CandidateAnalysis, CandidateRegistry, ContractId, DedupRunParams, EvidenceBundle,
    FailureRecord, HitGraph, HttpLimits, LoadOptions, PaperConfig, ProgressObserver, ResidentStore,
    SeedFullReport, SeedRecord,
};
use rayon::prelude::*;

/// Configuration for the offline dedup pipeline.
pub struct RunDedupConfig {
    pub inputs: Vec<PathBuf>,
    pub seeds: PathBuf,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
    pub rayon_threads: Option<usize>,
}

/// Optional sync enrich override for fixture / unit tests (skips live HTTP).
pub type EnrichOverride = Arc<
    dyn Fn(
            &CandidateRegistry,
            &ResidentStore,
            &dyn ProgressObserver,
        ) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error>
        + Send
        + Sync,
>;

/// Configuration for the full end-to-end `run` pipeline.
pub struct RunConfig {
    pub inputs: Vec<PathBuf>,
    pub seeds: PathBuf,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
    pub rayon_threads: Option<usize>,
    pub api_keys: ApiKeys,
    pub http_concurrency: usize,
    pub paper: PaperConfig,
    /// When set, used instead of Tokio `enrich_candidates` (tests / offline fixtures).
    pub enrich_override: Option<EnrichOverride>,
}

fn configure_rayon(threads: Option<usize>) -> Result<(), Analysis2Error> {
    if let Some(n) = threads {
        if let Err(e) = rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
        {
            // Global pool may already exist from a prior test or CLI setup.
            let msg = e.to_string();
            if !msg.contains("already been initialized") {
                return Err(Analysis2Error::invalid(format!("rayon pool: {msg}")));
            }
        }
    }
    Ok(())
}

/// Preserve cancellation checks inside parallel seed queries without letting
/// concurrent workers overwrite the single terminal progress phase.
struct CancellationOnlyProgress<'a> {
    inner: &'a dyn ProgressObserver,
}

impl ProgressObserver for CancellationOnlyProgress<'_> {
    fn set_stage(&self, _stage: &str) {}
    fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
    fn add_completed(&self, _n: u64) {}
    fn check_cancelled(&self) -> Result<(), Analysis2Error> {
        self.inner.check_cancelled()
    }
    fn finish(&self) {}
}

fn query_seed_dimensions(
    store: &ResidentStore,
    seed_id: ContractId,
    name_threshold: f64,
    metadata_threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<HitGraph, Analysis2Error> {
    let mut graph = HitGraph::new();
    query_uri_for_seed(store, seed_id, &mut graph, progress)?;
    query_name_for_seed(store, seed_id, name_threshold, &mut graph, progress)?;
    query_metadata_for_seed(store, seed_id, metadata_threshold, &mut graph, progress)?;
    Ok(graph)
}

/// Load snapshot → query URI/Name/Metadata per seed → write offline reports.
pub fn run_dedup(
    config: &RunDedupConfig,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    configure_rayon(config.rayon_threads)?;

    let options = LoadOptions::new(
        config.chains.clone(),
        config.evm_chains.clone(),
        config.metadata_anchors,
    );
    let store = load_resident_store(&config.inputs, &options, progress)?;
    let seeds = load_seeds_json(&config.seeds)?;
    let contract_nfts = build_contract_nft_map(&store);

    progress.set_stage("dedup");
    progress.begin_phase("seeds", Some(seeds.len() as u64));

    let quiet_progress = CancellationOnlyProgress { inner: progress };
    let analyzed = seeds
        .par_iter()
        .map(
            |seed| -> Result<
                Result<(SeedRecord, analysis2_core::SeedDedupReport), FailureRecord>,
                Analysis2Error,
            > {
                progress.check_cancelled()?;
                let outcome = match resolve_seed_contract(&store, seed) {
                    Ok(seed_id) => match query_seed_dimensions(
                        &store,
                        seed_id,
                        config.name_threshold,
                        config.metadata_threshold,
                        &quiet_progress,
                    ) {
                        Ok(graph) => {
                            let registry =
                                CandidateRegistry::from_hit_graph(&graph, &contract_nfts);
                            let report = build_seed_dedup_report(
                                &store,
                                seed,
                                seed_id,
                                &graph,
                                &registry,
                                &contract_nfts,
                            );
                            Ok((seed.clone(), report))
                        }
                        // Cancel must propagate (exit 130); never record as FailureRecord.
                        Err(Analysis2Error::Cancelled) => {
                            return Err(Analysis2Error::Cancelled);
                        }
                        Err(e) => Err(FailureRecord::seed_stage(
                            &seed.chain,
                            &seed.address,
                            "dedup_query",
                            e.to_string(),
                        )),
                    },
                    Err(e) => Err(FailureRecord::seed_stage(
                        &seed.chain,
                        &seed.address,
                        "resolve_seed",
                        e.to_string(),
                    )),
                };
                progress.add_completed(1);
                Ok(outcome)
            },
        )
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    progress.set_stage("report");
    progress.begin_phase("write", Some(1));
    let params = DedupRunParams {
        command: "run-dedup".into(),
        inputs: config
            .inputs
            .iter()
            .map(|p| p.display().to_string())
            .collect(),
        chains: config.chains.clone(),
        evm_chains: config.evm_chains.clone(),
        name_threshold: config.name_threshold,
        metadata_threshold: config.metadata_threshold,
        metadata_anchors: config.metadata_anchors,
    };
    write_dedup_outputs(&config.output_dir, &params, &store, &seeds, &analyzed, &[])?;
    progress.add_completed(1);
    Ok(())
}

/// End-to-end: load → dedup → enrich → analyze → full reports.
pub fn run(config: &RunConfig, progress: &dyn ProgressObserver) -> Result<(), Analysis2Error> {
    configure_rayon(config.rayon_threads)?;

    let options = LoadOptions::new(
        config.chains.clone(),
        config.evm_chains.clone(),
        config.metadata_anchors,
    );
    let store = load_resident_store(&config.inputs, &options, progress)?;
    let seeds = load_seeds_json(&config.seeds)?;
    let contract_nfts = build_contract_nft_map(&store);

    progress.set_stage("dedup");
    progress.begin_phase("seeds", Some(seeds.len() as u64));

    let quiet_progress = CancellationOnlyProgress { inner: progress };
    let seed_results = seeds
        .par_iter()
        .map(
            |seed| -> Result<
                Result<(SeedRecord, ContractId, HitGraph), FailureRecord>,
                Analysis2Error,
            > {
                progress.check_cancelled()?;
                let outcome = match resolve_seed_contract(&store, seed) {
                    Ok(seed_id) => match query_seed_dimensions(
                        &store,
                        seed_id,
                        config.name_threshold,
                        config.metadata_threshold,
                        &quiet_progress,
                    ) {
                        Ok(graph) => Ok((seed.clone(), seed_id, graph)),
                        Err(Analysis2Error::Cancelled) => {
                            return Err(Analysis2Error::Cancelled);
                        }
                        Err(e) => Err(FailureRecord::seed_stage(
                            &seed.chain,
                            &seed.address,
                            "dedup_query",
                            e.to_string(),
                        )),
                    },
                    Err(e) => Err(FailureRecord::seed_stage(
                        &seed.chain,
                        &seed.address,
                        "resolve_seed",
                        e.to_string(),
                    )),
                };
                progress.add_completed(1);
                Ok(outcome)
            },
        )
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let mut graph = HitGraph::new();
    let mut seed_ids: Vec<(SeedRecord, ContractId)> = Vec::with_capacity(seed_results.len());
    let mut failures: Vec<FailureRecord> = Vec::new();
    for result in seed_results {
        match result {
            Ok((seed, seed_id, mut seed_graph)) => {
                seed_ids.push((seed, seed_id));
                graph.append(&mut seed_graph);
            }
            Err(failure) => failures.push(failure),
        }
    }

    let registry = CandidateRegistry::from_hit_graph(&graph, &contract_nfts);

    progress.set_stage("enrich");
    let evidence = match &config.enrich_override {
        Some(hook) => hook(&registry, &store, progress)?,
        None => {
            let limits = HttpLimits {
                concurrency: config.http_concurrency.max(1),
                ..HttpLimits::default()
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| Analysis2Error::http(format!("tokio runtime: {e}")))?;
            runtime.block_on(enrich_candidates(
                &registry,
                &store,
                &config.api_keys,
                &limits,
                progress,
            ))?
        }
    };

    progress.check_cancelled()?;
    progress.set_stage("analyze");
    let candidates = registry.candidate_contracts().to_vec();
    progress.begin_phase("analyze_candidates", Some(candidates.len() as u64));

    std::fs::create_dir_all(config.output_dir.join("candidates"))?;
    let paper = config.paper.clone();
    let out_dir = config.output_dir.clone();

    // Err arm carries candidate identity so failures.jsonl is never unknown/unknown.
    let analyze_results: Vec<Result<CandidateAnalysis, (String, String, Analysis2Error)>> =
        candidates
            .par_iter()
            .map(|&cid| {
                let contract = &store.contracts[cid as usize];
                let chain = store.chain_name(contract.chain_id).to_owned();
                let address = contract.address.clone();
                if let Err(e) = progress.check_cancelled() {
                    return Err((chain, address, e));
                }
                let bundle = evidence
                    .get(&cid)
                    .cloned()
                    .unwrap_or_else(|| EvidenceBundle::empty(cid, chain.clone(), address.clone()));
                let analysis = match analyze_candidate(&store, cid, &bundle, &paper) {
                    Ok(a) => a,
                    Err(e) => return Err((chain, address, e)),
                };
                // Candidate paths are unique, so serialization and writes can
                // proceed on the same Rayon workers without a global lock.
                write_candidate_json(&out_dir, &analysis)
                    .map_err(|e| (chain.clone(), address.clone(), e))?;
                progress.add_completed(1);
                Ok(analysis)
            })
            .collect();

    let mut analyses_map: AHashMap<ContractId, CandidateAnalysis> = AHashMap::new();
    let mut analyses_list: Vec<CandidateAnalysis> = Vec::new();
    for result in analyze_results {
        match result {
            Ok(analysis) => {
                analyses_map.insert(analysis.contract_id, analysis.clone());
                analyses_list.push(analysis);
            }
            Err((_, _, Analysis2Error::Cancelled)) => return Err(Analysis2Error::Cancelled),
            Err((chain, address, e)) => {
                failures.push(FailureRecord::candidate_stage(
                    &chain,
                    &address,
                    "analyze_candidate",
                    e.to_string(),
                ));
            }
        }
    }

    // Mark seeds incomplete when any related candidate is missing from analyses_map.
    progress.set_stage("report");
    progress.begin_phase("write", Some(1));

    let mut analyzed: Vec<Result<(SeedRecord, SeedFullReport), FailureRecord>> =
        Vec::with_capacity(seed_ids.len());
    for (seed, seed_id) in &seed_ids {
        let dedup =
            build_seed_dedup_report(&store, seed, *seed_id, &graph, &registry, &contract_nfts);
        let scopes_complete = scopes_complete_for_seed(&store, &dedup);
        let (rollup, analysis_ok) = build_seed_analysis_rollup(
            &registry,
            *seed_id,
            &seed.chain,
            &seed.address,
            &analyses_map,
            "candidates",
        );
        let analysis_complete = analysis_ok
            && registry
                .relations_for_seed(*seed_id)
                .iter()
                .all(|rel| analyses_map.contains_key(&rel.candidate_contract));
        analyzed.push(Ok((
            seed.clone(),
            SeedFullReport {
                dedup,
                scopes_complete,
                analysis_complete,
                analysis: Some(rollup),
            },
        )));
    }
    // Failed resolve/dedup seeds are recorded only in `failures` (extra_failures).

    let params = DedupRunParams {
        command: "run".into(),
        inputs: config
            .inputs
            .iter()
            .map(|p| p.display().to_string())
            .collect(),
        chains: config.chains.clone(),
        evm_chains: config.evm_chains.clone(),
        name_threshold: config.name_threshold,
        metadata_threshold: config.metadata_threshold,
        metadata_anchors: config.metadata_anchors,
    };
    write_run_outputs(
        &config.output_dir,
        &params,
        &store,
        &seeds,
        &analyzed,
        &analyses_list,
        &failures,
    )?;
    progress.add_completed(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use analysis2_core::parquet::write_report_golden_fixture;
    use analysis2_core::{SaleEvent, DEFAULT_METADATA_THRESHOLD, DEFAULT_NAME_THRESHOLD};
    use serde_json::Value;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Allows load-time checks; after `dedup` stage, skips the seed-loop gate once,
    /// then cancels on the next check (mid-query inside `query_uri_for_seed`).
    struct CancelOnFirstQueryCheck {
        in_dedup: AtomicBool,
        dedup_checks: AtomicUsize,
    }

    impl CancelOnFirstQueryCheck {
        fn new() -> Self {
            Self {
                in_dedup: AtomicBool::new(false),
                dedup_checks: AtomicUsize::new(0),
            }
        }
    }

    impl ProgressObserver for CancelOnFirstQueryCheck {
        fn set_stage(&self, stage: &str) {
            if stage == "dedup" {
                self.in_dedup.store(true, Ordering::SeqCst);
            }
        }
        fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
        fn add_completed(&self, _n: u64) {}
        fn check_cancelled(&self) -> Result<(), Analysis2Error> {
            if !self.in_dedup.load(Ordering::SeqCst) {
                return Ok(());
            }
            let n = self.dedup_checks.fetch_add(1, Ordering::SeqCst);
            // n==0: seed-loop `check_cancelled`; n>=1: first mid-query check.
            if n >= 1 {
                Err(Analysis2Error::Cancelled)
            } else {
                Ok(())
            }
        }
        fn finish(&self) {}
    }

    #[test]
    fn mid_query_cancel_propagates_without_complete_manifest() {
        let dir =
            std::env::temp_dir().join(format!("analysis2_cancel_mid_query_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let parquet = dir.join("fixture.parquet");
        write_report_golden_fixture(&parquet).expect("fixture");
        let seeds = dir.join("seeds.json");
        std::fs::write(
            &seeds,
            r#"[{"chain":"ethereum","address":"0xseed","rank":1}]"#,
        )
        .unwrap();
        let out = dir.join("out");
        std::fs::create_dir_all(&out).unwrap();

        let progress = CancelOnFirstQueryCheck::new();
        let err = run_dedup(
            &RunDedupConfig {
                inputs: vec![parquet],
                seeds,
                output_dir: out.clone(),
                chains: vec!["ethereum".into(), "base".into(), "solana".into()],
                evm_chains: vec!["ethereum".into(), "base".into()],
                name_threshold: DEFAULT_NAME_THRESHOLD,
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: None,
            },
            &progress,
        )
        .expect_err("mid-query cancel must return Err");

        assert!(
            matches!(err, Analysis2Error::Cancelled),
            "expected Cancelled, got {err:?}"
        );
        assert!(
            !out.join("run_manifest.json").exists(),
            "cancel must not write run_manifest (no false complete)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fixture_run_with_mocked_enrich_writes_summary_keys() {
        let dir =
            std::env::temp_dir().join(format!("analysis2_run_fixture_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let parquet = dir.join("fixture.parquet");
        write_report_golden_fixture(&parquet).expect("fixture");
        let seeds = dir.join("seeds.json");
        std::fs::write(
            &seeds,
            r#"[{"chain":"ethereum","address":"0xseed","rank":1}]"#,
        )
        .unwrap();
        let out = dir.join("out");
        std::fs::create_dir_all(&out).unwrap();

        let enrich: EnrichOverride = Arc::new(|registry, store, progress| {
            progress.set_stage("enrich");
            progress.begin_phase(
                "enrich_candidates",
                Some(registry.candidate_contracts().len() as u64),
            );
            let mut map = AHashMap::new();
            for &cid in registry.candidate_contracts() {
                let c = &store.contracts[cid as usize];
                let chain = store.chain_name(c.chain_id).to_owned();
                let mut bundle = EvidenceBundle::empty(cid, chain, c.address.clone());
                // Synthetic USD sale so cross-chain economics can sum USD only.
                bundle.sales.push(SaleEvent {
                    tx_hash: "0xmock".into(),
                    token_id: "1".into(),
                    seller: "0xop".into(),
                    buyer: "0xbuyer".into(),
                    timestamp: Some(1_700_000_000),
                    block_number: Some(1),
                    marketplace: None,
                    native_amount: Some(1.0),
                    usd_amount: Some(42.0),
                    currency_symbol: Some("ETH".into()),
                });
                bundle.controllers.push("0xop".into());
                map.insert(cid, bundle);
                progress.add_completed(1);
            }
            Ok(map)
        });

        run(
            &RunConfig {
                inputs: vec![parquet],
                seeds,
                output_dir: out.clone(),
                chains: vec!["ethereum".into(), "base".into(), "solana".into()],
                evm_chains: vec!["ethereum".into(), "base".into()],
                name_threshold: DEFAULT_NAME_THRESHOLD,
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: Some(2),
                api_keys: ApiKeys::default(),
                http_concurrency: 4,
                paper: PaperConfig {
                    analysis_timestamp: 1_700_000_100,
                    ..PaperConfig::default()
                },
                enrich_override: Some(enrich),
            },
            &analysis2_core::NoopProgress,
        )
        .expect("fixture run");

        assert!(out.join("run_manifest.json").is_file());
        assert!(out.join("summary.json").is_file());
        assert!(out.join("failures.jsonl").is_file());
        assert!(out.join("seeds/ethereum__0xseed/report.json").is_file());

        let summary: Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap())
                .unwrap();
        for key in [
            "selected_seed_count",
            "analyzed_seed_count",
            "incomplete_seed_count",
            "failed_seed_count",
            "seed_completion_ratio",
            "seed_with_duplicate_count",
            "seed_duplicate_ratio",
            "representative_candidate_count",
            "candidate_contract_count",
            "suspected_duplicate_contract_count",
            "legit_duplicate_contract_count",
            "infringing_nft_count",
            "address_classification",
            "behaviors",
            "economics",
            "data_quality",
            "all_chains",
        ] {
            assert!(summary.get(key).is_some(), "missing summary key {key}");
        }
        assert!(summary["economics"].get("operator_output_usd").is_some());
        assert!(summary["economics"].get("honest_loss_usd").is_some());
        assert!(summary["economics"].get("operator_output_native").is_none());

        let manifest: Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("run_manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["command"], "run");
        assert!(manifest["status"] == "complete" || manifest["status"] == "complete_with_failures");

        let seed_report: Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("seeds/ethereum__0xseed/report.json")).unwrap(),
        )
        .unwrap();
        assert!(seed_report.get("scopes_complete").is_some());
        assert!(seed_report.get("analysis").is_some());

        // At least one candidate artifact streamed.
        let cand_dir = out.join("candidates");
        assert!(cand_dir.is_dir());
        let cand_count = std::fs::read_dir(&cand_dir).unwrap().count();
        assert!(cand_count >= 1, "expected streamed candidate JSON");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_cancel_before_report_skips_complete_manifest() {
        let dir = std::env::temp_dir().join(format!("analysis2_run_cancel_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let parquet = dir.join("fixture.parquet");
        write_report_golden_fixture(&parquet).expect("fixture");
        let seeds = dir.join("seeds.json");
        std::fs::write(
            &seeds,
            r#"[{"chain":"ethereum","address":"0xseed","rank":1}]"#,
        )
        .unwrap();
        let out = dir.join("out");
        std::fs::create_dir_all(&out).unwrap();

        struct CancelOnEnrich;
        impl ProgressObserver for CancelOnEnrich {
            fn set_stage(&self, _stage: &str) {}
            fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
            fn add_completed(&self, _n: u64) {}
            fn check_cancelled(&self) -> Result<(), Analysis2Error> {
                Ok(())
            }
            fn finish(&self) {}
        }

        let enrich: EnrichOverride = Arc::new(|_, _, _| Err(Analysis2Error::Cancelled));
        let err = run(
            &RunConfig {
                inputs: vec![parquet],
                seeds,
                output_dir: out.clone(),
                chains: vec!["ethereum".into(), "base".into(), "solana".into()],
                evm_chains: vec!["ethereum".into(), "base".into()],
                name_threshold: DEFAULT_NAME_THRESHOLD,
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: Some(2),
                api_keys: ApiKeys::default(),
                http_concurrency: 4,
                paper: PaperConfig::default(),
                enrich_override: Some(enrich),
            },
            &CancelOnEnrich,
        )
        .expect_err("cancel");
        assert!(matches!(err, Analysis2Error::Cancelled));
        assert!(!out.join("run_manifest.json").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn analyze_failure_records_candidate_identity() {
        let dir =
            std::env::temp_dir().join(format!("analysis2_run_analyze_fail_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let parquet = dir.join("fixture.parquet");
        write_report_golden_fixture(&parquet).expect("fixture");
        let seeds = dir.join("seeds.json");
        std::fs::write(
            &seeds,
            r#"[{"chain":"ethereum","address":"0xseed","rank":1}]"#,
        )
        .unwrap();
        let out = dir.join("out");
        std::fs::create_dir_all(&out).unwrap();

        // Mismatched evidence.contract_id forces analyze_candidate to fail.
        let enrich: EnrichOverride = Arc::new(|registry, store, progress| {
            progress.set_stage("enrich");
            progress.begin_phase(
                "enrich_candidates",
                Some(registry.candidate_contracts().len() as u64),
            );
            let mut map = AHashMap::new();
            for &cid in registry.candidate_contracts() {
                let c = &store.contracts[cid as usize];
                let chain = store.chain_name(c.chain_id).to_owned();
                let bad_id = cid.wrapping_add(1_000_000);
                map.insert(cid, EvidenceBundle::empty(bad_id, chain, c.address.clone()));
                progress.add_completed(1);
            }
            Ok(map)
        });

        run(
            &RunConfig {
                inputs: vec![parquet],
                seeds,
                output_dir: out.clone(),
                chains: vec!["ethereum".into(), "base".into(), "solana".into()],
                evm_chains: vec!["ethereum".into(), "base".into()],
                name_threshold: DEFAULT_NAME_THRESHOLD,
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: Some(2),
                api_keys: ApiKeys::default(),
                http_concurrency: 4,
                paper: PaperConfig::default(),
                enrich_override: Some(enrich),
            },
            &analysis2_core::NoopProgress,
        )
        .expect("run completes with analyze failures");

        let failures_path = out.join("failures.jsonl");
        assert!(failures_path.is_file());
        let body = std::fs::read_to_string(&failures_path).unwrap();
        let mut saw_analyze = false;
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            let row: Value = serde_json::from_str(line).unwrap();
            if row["stage"] == "analyze_candidate" {
                saw_analyze = true;
                assert_ne!(row["seed_chain"], "unknown");
                assert_ne!(row["seed_address"], "unknown");
                assert!(!row["seed_chain"].as_str().unwrap_or("").is_empty());
                assert!(!row["seed_address"].as_str().unwrap_or("").is_empty());
            }
        }
        assert!(saw_analyze, "expected analyze_candidate failure rows");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
