//! Offline `run-dedup` and full `run` orchestration.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use ahash::{AHashMap, AHashSet};
use analysis2_core::{
    Analysis2Error, ApiKeys, CandidateAnalysis, CandidateRegistry, ContractId,
    DEFAULT_EVIDENCE_CACHE_BATCH, DedupCacheParams, DedupRunParams, EvidenceBundle,
    EvidenceCacheSink, EvidenceStatus, FailureRecord, HitGraph, HttpLimits, LegitSignals,
    LoadOptions, MetadataQueryScratch, NameQueryScratch, PaperConfig, PendingDedupLoad,
    ProgressObserver, ResidentStore, ScopeAnalysisSets, SeedDedupReport, SeedFullReport,
    SeedRecord, UriQueryScratch, analyze_candidate, build_contract_nft_map, build_dedup_cache,
    build_evidence_cache, build_seed_analysis_rollup, build_seed_dedup_report,
    candidate_json_rel_path, default_dedup_cache_path, default_evidence_cache_path,
    enrich_candidates_with_hook, evidence_cache_artifacts_present, evidence_cache_params,
    finalize_legit_signals, load_dedup_cache, load_evidence_cache_resumable,
    load_resident_store_uri_ready, load_seeds_json, query_metadata_for_seed_with_scratch,
    query_name_for_seed_with_scratch, query_uri_for_seed_with_scratch, rematerialize_dedup_batch,
    rematerialize_evidence, resolve_seed_contract, scopes_complete_for_seed,
    serialize_candidate_json, validate_dedup_cache, validate_evidence_cache,
    write_candidate_json_bytes, write_dedup_cache, write_dedup_outputs, write_evidence_cache,
    write_run_outputs,
};
use rayon::prelude::*;

/// Configuration for the offline dedup pipeline.
pub struct RunDedupConfig {
    pub inputs: Vec<PathBuf>,
    pub seeds: PathBuf,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: Option<f64>,
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

#[derive(Clone, Debug, Default)]
struct ScopeEvidenceSelector {
    token_ids: AHashSet<String>,
    seed_keys: AHashSet<String>,
    token_ids_by_seed: AHashMap<String, AHashSet<String>>,
}

impl ScopeEvidenceSelector {
    /// Keep relation classifications for the whole scope. When at least one
    /// relation is suspicious, the candidate is a contract-level hit and deep
    /// analysis receives every NFT event in that contract.
    fn filtered_bundle(&self, bundle: &EvidenceBundle) -> EvidenceBundle {
        let mut suspicious_token_ids = AHashSet::new();
        for (seed_key, token_ids) in &self.token_ids_by_seed {
            let expected_key = parse_relation_key(seed_key);
            let is_legit = bundle
                .relation_legit
                .iter()
                .find(|(key, _)| parse_relation_key(key) == expected_key)
                .is_some_and(|(_, signals)| signals.is_legit_duplicate());
            if !is_legit {
                suspicious_token_ids.extend(token_ids.iter().cloned());
            }
        }
        if self.token_ids_by_seed.is_empty() {
            suspicious_token_ids.extend(self.token_ids.iter().cloned());
        }
        bundle.filtered_for_analysis(&suspicious_token_ids, &self.seed_keys)
    }
}

#[derive(Clone, Debug, Default)]
struct CandidateScopeSelectors {
    all: ScopeEvidenceSelector,
    intra: ScopeEvidenceSelector,
    cross: ScopeEvidenceSelector,
    matrix: BTreeMap<(String, String), ScopeEvidenceSelector>,
}

struct CandidateAnalysisBatch {
    per_seed: Vec<(String, CandidateAnalysis)>,
}

fn build_scope_selectors(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    allowed_seeds: Option<&AHashSet<ContractId>>,
) -> AHashMap<ContractId, CandidateScopeSelectors> {
    let mut selectors = AHashMap::new();
    for relation in registry.relations() {
        if allowed_seeds.is_some_and(|allowed| !allowed.contains(&relation.seed_contract)) {
            continue;
        }
        let seed = &store.contracts[relation.seed_contract as usize];
        let candidate = &store.contracts[relation.candidate_contract as usize];
        let primary = store.chain_name(seed.chain_id).to_ascii_lowercase();
        let secondary = store.chain_name(candidate.chain_id).to_ascii_lowercase();
        let seed_key = canonical_relation_key(&primary, &seed.address);
        let token_ids: Vec<String> = relation
            .nft_ids
            .iter()
            .filter_map(|id| store.nfts.get(*id as usize))
            .map(|nft| nft.token_id.clone())
            .collect();
        let candidate_selectors = selectors
            .entry(relation.candidate_contract)
            .or_insert_with(CandidateScopeSelectors::default);
        candidate_selectors
            .all
            .token_ids
            .extend(token_ids.iter().cloned());
        candidate_selectors.all.seed_keys.insert(seed_key.clone());
        candidate_selectors
            .all
            .token_ids_by_seed
            .entry(seed_key.clone())
            .or_default()
            .extend(token_ids.iter().cloned());
        let same_chain = primary.eq_ignore_ascii_case(&secondary);
        let scope_selector = if same_chain {
            &mut candidate_selectors.intra
        } else {
            &mut candidate_selectors.cross
        };
        scope_selector.token_ids.extend(token_ids.iter().cloned());
        scope_selector.seed_keys.insert(seed_key.clone());
        scope_selector
            .token_ids_by_seed
            .entry(seed_key.clone())
            .or_default()
            .extend(token_ids.iter().cloned());
        if !same_chain {
            let matrix = candidate_selectors
                .matrix
                .entry((primary, secondary))
                .or_insert_with(ScopeEvidenceSelector::default);
            matrix.token_ids.extend(token_ids.iter().cloned());
            matrix.seed_keys.insert(seed_key.clone());
            matrix
                .token_ids_by_seed
                .entry(seed_key)
                .or_default()
                .extend(token_ids);
        }
    }
    selectors
}

fn retain_formal_seed_selectors(
    selectors: &AHashMap<ContractId, CandidateScopeSelectors>,
    formal_seed_keys: &AHashSet<String>,
) -> AHashMap<ContractId, CandidateScopeSelectors> {
    fn filtered(
        source: &ScopeEvidenceSelector,
        formal_seed_keys: &AHashSet<String>,
    ) -> ScopeEvidenceSelector {
        let token_ids_by_seed: AHashMap<String, AHashSet<String>> = source
            .token_ids_by_seed
            .iter()
            .filter(|(seed_key, _)| formal_seed_keys.contains(*seed_key))
            .map(|(seed_key, token_ids)| (seed_key.clone(), token_ids.clone()))
            .collect();
        let token_ids = token_ids_by_seed
            .values()
            .flat_map(|ids| ids.iter().cloned())
            .collect();
        let seed_keys = token_ids_by_seed.keys().cloned().collect();
        ScopeEvidenceSelector {
            token_ids,
            seed_keys,
            token_ids_by_seed,
        }
    }

    selectors
        .iter()
        .filter_map(|(&candidate_id, source)| {
            let all = filtered(&source.all, formal_seed_keys);
            if all.token_ids_by_seed.is_empty() {
                return None;
            }
            let intra = filtered(&source.intra, formal_seed_keys);
            let cross = filtered(&source.cross, formal_seed_keys);
            let matrix = source
                .matrix
                .iter()
                .filter_map(|(direction, selector)| {
                    let selector = filtered(selector, formal_seed_keys);
                    (!selector.token_ids_by_seed.is_empty())
                        .then_some((direction.clone(), selector))
                })
                .collect();
            Some((
                candidate_id,
                CandidateScopeSelectors {
                    all,
                    intra,
                    cross,
                    matrix,
                },
            ))
        })
        .collect()
}

/// Configuration for the full end-to-end `run` pipeline.
pub struct RunConfig {
    pub inputs: Vec<PathBuf>,
    pub seeds: PathBuf,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: Option<f64>,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
    pub rayon_threads: Option<usize>,
    pub api_keys: ApiKeys,
    pub http_concurrency: usize,
    pub paper: PaperConfig,
    /// When set, used instead of Tokio `enrich_candidates` (tests / offline fixtures).
    pub enrich_override: Option<EnrichOverride>,
    /// Path for durable dedup cache (`intermediate/dedup_cache.json` by default).
    pub dedup_cache_path: Option<PathBuf>,
    /// Load dedup results from cache and skip URI/Name/Metadata query stages.
    pub reuse_dedup: bool,
    /// Path for durable evidence cache (`intermediate/evidence_cache.json` by default).
    pub evidence_cache_path: Option<PathBuf>,
    /// Load enrich evidence from cache; only HTTP-fetch candidates missing from cache.
    pub reuse_evidence: bool,
}

fn with_rayon_pool<T>(
    threads: Option<usize>,
    run: impl FnOnce() -> Result<T, Analysis2Error> + Send,
) -> Result<T, Analysis2Error>
where
    T: Send,
{
    let Some(threads) = threads else {
        return run();
    };
    if threads == 0 {
        return Err(Analysis2Error::invalid(
            "--rayon-threads must be greater than zero",
        ));
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("analysis2-cpu-{index}"))
        .build()
        .map_err(|error| Analysis2Error::invalid(format!("rayon pool: {error}")))?;
    pool.install(run)
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

struct SeedDedupState {
    seed: SeedRecord,
    seed_id: ContractId,
    graph: HitGraph,
    failure: Option<FailureRecord>,
}

struct SeedDedupBatch {
    completed: Vec<(SeedRecord, ContractId, HitGraph)>,
    failures: Vec<FailureRecord>,
}

fn run_seed_stage<S, Init, Query>(
    states: &mut [SeedDedupState],
    phase: &str,
    progress: &dyn ProgressObserver,
    init: Init,
    query: Query,
) -> Result<(), Analysis2Error>
where
    S: Send,
    Init: Fn() -> S + Send + Sync,
    Query: Fn(&mut S, ContractId, &mut HitGraph) -> Result<(), Analysis2Error> + Send + Sync,
{
    let active = states
        .iter()
        .enumerate()
        .filter_map(|(index, state)| state.failure.is_none().then_some(index))
        .collect::<Vec<_>>();
    progress.begin_phase(phase, Some(active.len() as u64));

    // Always allow nested query parallelism. Seed costs are highly skewed
    // (Solana collections with thousands of Name queries vs single EVM reps);
    // work-stealing into heavy seeds beats the jitter cost of nested pools.
    analysis2_core::set_inner_query_parallel(true);
    let outcomes = active
        .par_iter()
        .map_init(init, |scratch, &state_index| {
            progress.check_cancelled()?;
            let state = &states[state_index];
            let mut graph = HitGraph::new();
            let outcome = query(scratch, state.seed_id, &mut graph);
            progress.add_completed(1);
            match outcome {
                Ok(()) => Ok((state_index, Ok(graph))),
                Err(Analysis2Error::Cancelled) => Err(Analysis2Error::Cancelled),
                Err(error) => Ok((state_index, Err(error))),
            }
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, Analysis2Error>>();
    analysis2_core::set_inner_query_parallel(true);
    let outcomes = outcomes?;

    for (state_index, outcome) in outcomes {
        let state = &mut states[state_index];
        match outcome {
            Ok(mut graph) => state.graph.append(&mut graph),
            Err(error) => {
                state.failure = Some(FailureRecord::seed_stage(
                    &state.seed.chain,
                    &state.seed.address,
                    "dedup_query",
                    error.to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn resolve_seed_states(
    store: &ResidentStore,
    seeds: &[SeedRecord],
    progress: &dyn ProgressObserver,
) -> Result<(Vec<SeedDedupState>, Vec<FailureRecord>), Analysis2Error> {
    progress.begin_phase("resolve_seeds", Some(seeds.len() as u64));
    let resolved = seeds
        .par_iter()
        .map(|seed| {
            progress.check_cancelled()?;
            let result = resolve_seed_contract(store, seed);
            progress.add_completed(1);
            Ok::<_, Analysis2Error>((seed.clone(), result))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let mut states = Vec::with_capacity(resolved.len());
    let mut failures = Vec::new();
    for (seed, result) in resolved {
        match result {
            Ok(seed_id) => states.push(SeedDedupState {
                seed,
                seed_id,
                graph: HitGraph::new(),
                failure: None,
            }),
            Err(error) => failures.push(FailureRecord::seed_stage(
                &seed.chain,
                &seed.address,
                "resolve_seed",
                error.to_string(),
            )),
        }
    }
    Ok((states, failures))
}

fn finish_seed_batch(
    states: Vec<SeedDedupState>,
    mut failures: Vec<FailureRecord>,
) -> SeedDedupBatch {
    let mut completed = Vec::with_capacity(states.len());
    for state in states {
        if let Some(failure) = state.failure {
            failures.push(failure);
        } else {
            completed.push((state.seed, state.seed_id, state.graph));
        }
    }
    SeedDedupBatch {
        completed,
        failures,
    }
}

fn query_name_and_metadata_stages(
    store: &mut ResidentStore,
    states: &mut [SeedDedupState],
    name_threshold: Option<f64>,
    metadata_threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    let quiet = CancellationOnlyProgress { inner: progress };
    if let Some(name_threshold) = name_threshold {
        run_seed_stage(
            states,
            "name_seeds",
            progress,
            || {
                NameQueryScratch::for_worker_pool(
                    store.name_keys_by_len.len(),
                    rayon::current_num_threads(),
                )
            },
            |scratch, seed, graph| {
                query_name_for_seed_with_scratch(
                    store,
                    seed,
                    name_threshold,
                    graph,
                    &quiet,
                    scratch,
                )
            },
        )?;
    }
    store.drop_name_indexes();

    run_seed_stage(
        states,
        "metadata_seeds",
        progress,
        MetadataQueryScratch::default,
        |scratch, seed, graph| {
            query_metadata_for_seed_with_scratch(
                store,
                seed,
                metadata_threshold,
                graph,
                &quiet,
                scratch,
            )
        },
    )?;
    store.drop_metadata_index();
    Ok(())
}

/// Full-index path: URI → Name → Metadata with dimension barriers.
fn query_seeds_staged(
    store: &mut ResidentStore,
    seeds: &[SeedRecord],
    name_threshold: Option<f64>,
    metadata_threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<SeedDedupBatch, Analysis2Error> {
    progress.set_stage("dedup");
    let (mut states, failures) = resolve_seed_states(store, seeds, progress)?;
    let quiet = CancellationOnlyProgress { inner: progress };
    // Dimensions are barriers so each index stays hot; drop after use for RSS.
    run_seed_stage(
        &mut states,
        "uri_seeds",
        progress,
        || UriQueryScratch::for_chain_count(store.chains.len()),
        |scratch, seed, graph| query_uri_for_seed_with_scratch(store, seed, graph, &quiet, scratch),
    )?;
    store.drop_uri_indexes();
    query_name_and_metadata_stages(
        store,
        &mut states,
        name_threshold,
        metadata_threshold,
        progress,
    )?;
    Ok(finish_seed_batch(states, failures))
}

/// Overlap pass-2 Parquet I/O with URI seed queries, then finish name/metadata.
fn query_seeds_with_pass2_overlap(
    store: &mut ResidentStore,
    pending: PendingDedupLoad,
    seeds: &[SeedRecord],
    name_threshold: Option<f64>,
    metadata_threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<SeedDedupBatch, Analysis2Error> {
    progress.set_stage("dedup");
    let (mut states, failures) = resolve_seed_states(store, seeds, progress)?;
    let quiet = CancellationOnlyProgress { inner: progress };
    let chain_count = store.chains.len();

    // URI queries only read URI CSR + identity; pass-2 collect never touches the
    // store. Overlapping them hides a full Parquet metadata scan behind URI work.
    let (uri_result, anchors_result) = rayon::join(
        || {
            run_seed_stage(
                &mut states,
                "uri_seeds",
                progress,
                || UriQueryScratch::for_chain_count(chain_count),
                |scratch, seed, graph| {
                    query_uri_for_seed_with_scratch(store, seed, graph, &quiet, scratch)
                },
            )
        },
        || pending.collect_pass2(&quiet),
    );
    uri_result?;
    let anchors = anchors_result?;
    store.drop_uri_indexes();

    progress.set_stage("load");
    pending.finish(store, anchors, progress)?;

    progress.set_stage("dedup");
    query_name_and_metadata_stages(
        store,
        &mut states,
        name_threshold,
        metadata_threshold,
        progress,
    )?;
    Ok(finish_seed_batch(states, failures))
}

/// Load snapshot → query URI/Name/Metadata per seed → write offline reports.
pub fn run_dedup(
    config: &RunDedupConfig,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    with_rayon_pool(config.rayon_threads, || run_dedup_inner(config, progress))
}

fn run_dedup_inner(
    config: &RunDedupConfig,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    let mut options = LoadOptions::new(
        config.chains.clone(),
        config.evm_chains.clone(),
        config.metadata_anchors,
    );
    options.build_name_index = config.name_threshold.is_some();
    let seeds = load_seeds_json(&config.seeds)?;
    let (mut store, pending) = load_resident_store_uri_ready(&config.inputs, &options, progress)?;
    // Built before dimension drops so CSR slices stay valid for reporting.
    let contract_nfts = build_contract_nft_map(&store);

    let seed_batch = match pending {
        Some(pending) => query_seeds_with_pass2_overlap(
            &mut store,
            pending,
            &seeds,
            config.name_threshold,
            config.metadata_threshold,
            progress,
        )?,
        None => query_seeds_staged(
            &mut store,
            &seeds,
            config.name_threshold,
            config.metadata_threshold,
            progress,
        )?,
    };
    progress.set_stage("report");
    progress.begin_phase("aggregate_seeds", Some(seed_batch.completed.len() as u64));
    let reports = seed_batch
        .completed
        .into_par_iter()
        .map(|(seed, seed_id, graph)| {
            let registry = CandidateRegistry::from_hit_graph(&graph, &contract_nfts);
            let report =
                build_seed_dedup_report(&store, &seed, seed_id, &graph, &registry, &contract_nfts);
            progress.add_completed(1);
            (seed, report)
        })
        .collect::<Vec<_>>();
    let mut analyzed = reports.into_iter().map(Ok).collect::<Vec<_>>();
    analyzed.extend(seed_batch.failures.into_iter().map(Err));

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
    with_rayon_pool(config.rayon_threads, || run_inner(config, progress))
}

fn dedup_cache_path(config: &RunConfig) -> PathBuf {
    config
        .dedup_cache_path
        .clone()
        .unwrap_or_else(|| default_dedup_cache_path(&config.output_dir))
}

fn evidence_cache_path(config: &RunConfig) -> PathBuf {
    if let Some(path) = &config.evidence_cache_path {
        return path.clone();
    }
    let primary = default_evidence_cache_path(&config.output_dir);
    if evidence_cache_artifacts_present(&primary) {
        return primary;
    }
    // Legacy layout (pre intermediate/ split): <output-dir>/evidence_cache.json
    let legacy = config
        .output_dir
        .join(analysis2_core::DEFAULT_EVIDENCE_CACHE_FILE);
    if evidence_cache_artifacts_present(&legacy) {
        eprintln!(
            "evidence: using legacy cache path {} (prefer intermediate/ for new runs)",
            legacy.display()
        );
        return legacy;
    }
    primary
}

fn make_dedup_cache_params(config: &RunConfig, seeds: &[SeedRecord]) -> DedupCacheParams {
    DedupCacheParams {
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
        seeds_path: config.seeds.display().to_string(),
        seeds: seeds.to_vec(),
    }
}

fn normalized_relation_key(chain: &str, address: &str) -> (String, String) {
    let chain = chain.trim().to_ascii_lowercase();
    let address = if chain == "solana" {
        address.trim().to_owned()
    } else {
        address.trim().to_ascii_lowercase()
    };
    (chain, address)
}

fn canonical_relation_key(chain: &str, address: &str) -> String {
    let (chain, address) = normalized_relation_key(chain, address);
    format!("{chain}:{address}")
}

fn parse_relation_key(key: &str) -> Option<(String, String)> {
    let (chain, address) = key.split_once(':')?;
    Some(normalized_relation_key(chain, address))
}

/// Reuse candidate-scoped HTTP evidence while aligning seed-scoped legitimacy
/// results to the current registry.
fn reconcile_cached_relation_legit(
    evidence: &mut AHashMap<ContractId, EvidenceBundle>,
    registry: &CandidateRegistry,
    store: &ResidentStore,
) -> AHashSet<ContractId> {
    let mut expected: AHashMap<ContractId, Vec<(String, (String, String))>> = AHashMap::new();
    for relation in registry.relations() {
        let seed = &store.contracts[relation.seed_contract as usize];
        let chain = store.chain_name(seed.chain_id);
        let display_key = format!("{chain}:{}", seed.address);
        expected
            .entry(relation.candidate_contract)
            .or_default()
            .push((display_key, normalized_relation_key(chain, &seed.address)));
    }

    let mut refresh = AHashSet::new();
    for (&candidate_id, bundle) in evidence.iter_mut() {
        let Some(relations) = expected.get(&candidate_id) else {
            bundle.relation_legit.clear();
            bundle.legit = LegitSignals::default();
            continue;
        };
        let mut cached: AHashMap<(String, String), LegitSignals> =
            std::mem::take(&mut bundle.relation_legit)
                .into_iter()
                .filter_map(|(key, signals)| parse_relation_key(&key).map(|key| (key, signals)))
                .collect();
        for (display_key, normalized_key) in relations {
            let signals = cached.remove(normalized_key);
            if signals.is_none() {
                refresh.insert(candidate_id);
            }
            bundle
                .relation_legit
                .insert(display_key.clone(), signals.unwrap_or_default());
        }
        bundle.legit = LegitSignals::default();
        finalize_legit_signals(bundle);
    }
    refresh
}

fn cached_evidence_needs_retry(bundle: &EvidenceBundle) -> bool {
    let mut statuses = vec![
        bundle.quality.transfers,
        bundle.quality.sales,
        bundle.quality.holders,
        bundle.quality.prices,
        bundle.quality.gas,
        bundle.quality.value_flows,
    ];
    if bundle.chain.eq_ignore_ascii_case("solana") {
        statuses.extend([bundle.quality.assets, bundle.quality.histories]);
    }
    let failed = statuses.contains(&EvidenceStatus::Failed);
    // A cap-limited Truncated result is stable for the same pagination
    // parameters. Retry only when the recorded failure belongs to the same
    // truncated evidence family, rather than pairing an unrelated warning
    // (for example royalty lookup) with a stable page cap.
    let transient_partial = bundle.quality.failures.iter().any(|failure| {
        let failure = failure.to_ascii_lowercase();
        (bundle.quality.transfers == EvidenceStatus::Truncated
            && (failure.contains("alchemy_transfers") || failure.contains("etherscan_transfers")))
            || (bundle.quality.sales == EvidenceStatus::Truncated
                && (failure.contains("alchemy_sales") || failure.contains("opensea_sales")))
            || (bundle.quality.holders == EvidenceStatus::Truncated
                && failure.contains("alchemy_holders"))
            || (bundle.quality.gas == EvidenceStatus::Truncated
                && (failure.contains("receipt") || failure.contains("helius")))
            || (bundle.quality.value_flows == EvidenceStatus::Truncated
                && failure.contains("value_flow"))
            || (bundle.chain.eq_ignore_ascii_case("solana")
                && (bundle.quality.assets == EvidenceStatus::Truncated
                    || bundle.quality.histories == EvidenceStatus::Truncated)
                && failure.contains("helius"))
    });
    failed || transient_partial
}

fn load_seed_batch_from_cache(
    store: &ResidentStore,
    cache: &analysis2_core::DedupCacheFile,
    cache_path: &Path,
    progress: &dyn ProgressObserver,
) -> Result<SeedDedupBatch, Analysis2Error> {
    progress.set_stage("dedup");
    progress.begin_phase("load_dedup_cache", Some(1));
    let (completed, failures) = rematerialize_dedup_batch(store, cache)?;
    progress.add_completed(1);
    eprintln!(
        "dedup: reused cache {} ({} seeds, {} failures)",
        cache_path.display(),
        completed.len(),
        failures.len()
    );
    Ok(SeedDedupBatch {
        completed,
        failures,
    })
}

/// Try to open + validate a dedup cache before choosing load options.
///
/// - Cache present + params match → `Ok(Some(cache))` (auto-reuse, same as evidence).
/// - `--reuse-dedup` and missing/invalid → hard error.
/// - Cache missing/invalid without flag → `Ok(None)` and fall through to full query.
fn try_load_validated_dedup_cache(
    cache_path: &Path,
    expected: &DedupCacheParams,
    require: bool,
) -> Result<Option<analysis2_core::DedupCacheFile>, Analysis2Error> {
    if !cache_path.is_file() {
        if require {
            return Err(Analysis2Error::invalid(format!(
                "--reuse-dedup requires existing cache file {}",
                cache_path.display()
            )));
        }
        return Ok(None);
    }
    match load_dedup_cache(cache_path) {
        Ok(cache) => match validate_dedup_cache(&cache, expected) {
            Ok(()) => Ok(Some(cache)),
            Err(e) => {
                if require {
                    return Err(e);
                }
                eprintln!("dedup: ignoring incompatible cache: {e}");
                Ok(None)
            }
        },
        Err(e) => {
            if require {
                return Err(Analysis2Error::invalid(format!(
                    "--reuse-dedup failed to load {}: {e}",
                    cache_path.display()
                )));
            }
            eprintln!("dedup: ignoring unreadable cache ({e})");
            Ok(None)
        }
    }
}

/// End-to-end: load → dedup (or cache) → enrich → analyze → full reports.
fn run_inner(config: &RunConfig, progress: &dyn ProgressObserver) -> Result<(), Analysis2Error> {
    let seeds = load_seeds_json(&config.seeds)?;
    let cache_path = dedup_cache_path(config);
    let cache_params = make_dedup_cache_params(config, &seeds);

    // Resolve dedup reuse *before* Parquet load so we can skip index build when
    // a compatible cache is available (auto-resume; --reuse-dedup only hard-fails).
    let dedup_cache =
        try_load_validated_dedup_cache(&cache_path, &cache_params, config.reuse_dedup)?;
    if dedup_cache.is_some() {
        eprintln!(
            "dedup: will reuse {} (identity-only Parquet load)",
            cache_path.display()
        );
    } else if cache_path.is_file() {
        eprintln!(
            "dedup: cache present but not reused; running full query ({})",
            cache_path.display()
        );
    } else {
        eprintln!(
            "dedup: no cache at {}; running full Name/URI/Metadata query",
            cache_path.display()
        );
    }

    let options = if dedup_cache.is_some() {
        LoadOptions::identity_only(
            config.chains.clone(),
            config.evm_chains.clone(),
            config.metadata_anchors,
        )
    } else {
        let mut options = LoadOptions::new(
            config.chains.clone(),
            config.evm_chains.clone(),
            config.metadata_anchors,
        );
        options.build_name_index = config.name_threshold.is_some();
        options
    };
    let (mut store, pending) = load_resident_store_uri_ready(&config.inputs, &options, progress)?;
    let contract_nfts = build_contract_nft_map(&store);

    let seed_batch = if let Some(cache) = dedup_cache {
        // Identity-only load leaves no pending pass-2 work.
        let _ = pending;
        load_seed_batch_from_cache(&store, &cache, &cache_path, progress)?
    } else {
        let batch = match pending {
            Some(pending) => query_seeds_with_pass2_overlap(
                &mut store,
                pending,
                &seeds,
                config.name_threshold,
                config.metadata_threshold,
                progress,
            )?,
            None => query_seeds_staged(
                &mut store,
                &seeds,
                config.name_threshold,
                config.metadata_threshold,
                progress,
            )?,
        };
        // Persist immediately so a later run can auto-reuse / --reuse-dedup.
        progress.begin_phase("write_dedup_cache", Some(1));
        let cache = build_dedup_cache(&store, cache_params, &batch.completed, &batch.failures);
        write_dedup_cache(&cache_path, &cache)?;
        progress.add_completed(1);
        eprintln!(
            "dedup: wrote cache {} ({} seeds, {} failures)",
            cache_path.display(),
            batch.completed.len(),
            batch.failures.len()
        );
        batch
    };

    let mut failures = seed_batch.failures;

    // Build registry while graphs are still alive, then materialize compact seed
    // dedup reports and drop HitGraphs immediately (largest post-dedup CPU structure).
    // This does not re-run matching — only aggregates already-found edges.
    let registry = CandidateRegistry::from_hit_graphs(
        seed_batch.completed.iter().map(|(_, _, graph)| graph),
        &contract_nfts,
    );
    // Still part of offline aggregation (not final paper reports).
    progress.begin_phase(
        "materialize_seed_dedup",
        Some(seed_batch.completed.len() as u64),
    );
    let seed_dedups: Vec<(SeedRecord, ContractId, SeedDedupReport)> = seed_batch
        .completed
        .into_par_iter()
        .map(|(seed, seed_id, graph)| {
            let dedup =
                build_seed_dedup_report(&store, &seed, seed_id, &graph, &registry, &contract_nfts);
            progress.add_completed(1);
            // `graph` is dropped here — edges are no longer needed after the report.
            (seed, seed_id, dedup)
        })
        .collect();
    // contract_nfts only served HitGraph expansion + seed reports.
    drop(contract_nfts);

    progress.set_stage("enrich");
    let limits = HttpLimits {
        concurrency: config.http_concurrency.max(1),
        ..HttpLimits::default()
    };
    let evidence_path = evidence_cache_path(config);
    let evidence_params = evidence_cache_params(
        &seeds,
        &config.seeds.display().to_string(),
        &config.api_keys,
        &limits,
    );

    // Auto-resume from incremental jsonl/snapshot when present and params match.
    // `--reuse-evidence` makes a missing cache fatal; otherwise a bad cache is skipped.
    let mut evidence = AHashMap::new();
    let mut forced_refresh = AHashSet::new();
    let cache_exists = evidence_cache_artifacts_present(&evidence_path);
    eprintln!(
        "evidence: cache path {} (artifacts_present={})",
        evidence_path.display(),
        cache_exists
    );
    if cache_exists || config.reuse_evidence {
        progress.begin_phase("load_evidence_cache", Some(1));
        match load_evidence_cache_resumable(&evidence_path) {
            Ok(cache) => {
                if let Err(e) = validate_evidence_cache(&cache, &evidence_params) {
                    if config.reuse_evidence {
                        return Err(e);
                    }
                    eprintln!("evidence: IGNORING incompatible cache (will re-fetch HTTP): {e}");
                } else {
                    evidence = rematerialize_evidence(&store, &cache)?;
                    forced_refresh =
                        reconcile_cached_relation_legit(&mut evidence, &registry, &store);
                    forced_refresh.extend(
                        evidence
                            .iter()
                            .filter(|(_, bundle)| cached_evidence_needs_retry(bundle))
                            .map(|(&candidate_id, _)| candidate_id),
                    );
                    eprintln!(
                        "evidence: resumed {} in-memory bundles from {}",
                        evidence.len(),
                        evidence_path.display()
                    );
                }
            }
            Err(e) => {
                if config.reuse_evidence {
                    return Err(Analysis2Error::invalid(format!(
                        "--reuse-evidence requires cache at {}: {e}",
                        evidence_path.display()
                    )));
                }
                eprintln!("evidence: no usable cache yet ({e})");
            }
        }
        progress.add_completed(1);
    } else {
        eprintln!(
            "evidence: no cache artifacts at {}; full HTTP enrich",
            evidence_path.display()
        );
    }

    let mut evidence = match &config.enrich_override {
        Some(hook) => {
            // Test / offline hooks replace evidence entirely (still written to cache).
            let map = hook(&registry, &store, progress)?;
            progress.begin_phase("write_evidence_cache", Some(1));
            let evidence_file = build_evidence_cache(evidence_params.clone(), &map);
            write_evidence_cache(&evidence_path, &evidence_file)?;
            progress.add_completed(1);
            map
        }
        None => {
            let total_cands = registry.candidate_contract_count();
            let missing: ahash::AHashSet<ContractId> = registry
                .candidate_contracts()
                .iter()
                .copied()
                .filter(|cid| !evidence.contains_key(cid) || forced_refresh.contains(cid))
                .collect();
            let cached_hits = total_cands.saturating_sub(missing.len());
            eprintln!(
                "evidence: registry candidates={total_cands} cache_hits={cached_hits} missing={}",
                missing.len()
            );
            if missing.is_empty() {
                eprintln!(
                    "evidence: all {total_cands} candidates covered by cache; skipping HTTP enrich (no snapshot rewrite)"
                );
                // Do not rewrite evidence_cache.json here: multi‑GB rewrite can
                // dominate re-run wall time when HTTP is already fully cached.
                evidence
            } else {
                let subset = registry.filter_candidates(&missing);
                eprintln!(
                    "evidence: fetching {} / {total_cands} candidates via HTTP (batch flush every {})",
                    subset.candidate_contract_count(),
                    DEFAULT_EVIDENCE_CACHE_BATCH
                );
                let mut sink = EvidenceCacheSink::create(
                    &evidence_path,
                    evidence_params.clone(),
                    DEFAULT_EVIDENCE_CACHE_BATCH,
                )?;
                // Seed in-memory snapshot index only (do not re-append jsonl).
                for bundle in evidence.values() {
                    sink.note_cached(bundle);
                }

                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Analysis2Error::http(format!("tokio runtime: {e}")))?;
                let fetch_result = {
                    let mut on_bundle = |bundle: &EvidenceBundle| -> Result<(), Analysis2Error> {
                        sink.push(bundle)
                    };
                    runtime.block_on(enrich_candidates_with_hook(
                        &subset,
                        &store,
                        &config.api_keys,
                        &limits,
                        progress,
                        Some(&mut on_bundle),
                    ))
                };
                // Drop Tokio worker stacks before the CPU-heavy analyze phase.
                drop(runtime);
                // Flush even on cancel / error so partial progress is reusable.
                match sink.finish() {
                    Ok(final_cache) => {
                        eprintln!(
                            "evidence: checkpoint {} ({} bundles on disk)",
                            evidence_path.display(),
                            final_cache.bundles.len()
                        );
                    }
                    Err(e) => eprintln!("evidence: final cache flush failed: {e}"),
                }
                let fetched = fetch_result?;
                evidence.extend(fetched);
                evidence
            }
        }
    };

    // P0: provenance is on disk; strip before analyze to shrink RSS.
    for bundle in evidence.values_mut() {
        bundle.strip_for_analysis_memory();
    }

    // Resolve direct-hit NFT ids to token strings before dropping resident identity.
    // Every reporting scope reuses the same cached evidence, filtered by these sets.
    let all_scope_selectors = build_scope_selectors(&registry, &store, None);

    // P1: seed reports + selectors are self-contained for NFT numerators; analyze
    // only needs contract id → chain/address. Drop full NFT/string universe now.
    store.shrink_identity_for_analysis();

    progress.check_cancelled()?;
    progress.set_stage("analyze");
    let candidates = registry.candidate_contracts().to_vec();
    progress.begin_phase("analyze_candidates", Some(candidates.len() as u64));

    analysis2_core::ensure_output_layout(&config.output_dir).map_err(Analysis2Error::from)?;
    let paper = config.paper.clone();
    let out_dir = config.output_dir.clone();

    // Take ownership of each candidate's evidence up front so Rayon workers can
    // free transfers/sales/holders as soon as that candidate finishes analysis —
    // no shared mutex, no second peak of graph+evidence+analyses.
    let owned_evidence: Vec<(ContractId, Option<EvidenceBundle>)> = candidates
        .iter()
        .map(|&cid| (cid, evidence.remove(&cid)))
        .collect();
    drop(evidence);

    // P1: background writer — Rayon only serializes; fs::write runs off-CPU pool.
    let (write_tx, write_rx) = mpsc::sync_channel::<(String, Vec<u8>)>(
        rayon::current_num_threads().saturating_mul(4).max(8),
    );
    let writer_out = out_dir.clone();
    let writer = thread::Builder::new()
        .name("analysis2-cand-writer".into())
        .spawn(move || -> Result<(), Analysis2Error> {
            while let Ok((rel, body)) = write_rx.recv() {
                write_candidate_json_bytes(&writer_out, &rel, &body)?;
            }
            Ok(())
        })
        .map_err(|e| Analysis2Error::invalid(format!("spawn candidate writer: {e}")))?;

    // Err arm carries candidate identity so failures.jsonl is never unknown/unknown.
    let analyze_results: Vec<Result<CandidateAnalysisBatch, (String, String, Analysis2Error)>> =
        owned_evidence
            .into_par_iter()
            .map(|(cid, bundle_owned)| {
                let contract = &store.contracts[cid as usize];
                let chain = store.chain_name(contract.chain_id).to_owned();
                let address = contract.address.clone();
                if let Err(e) = progress.check_cancelled() {
                    return Err((chain, address, e));
                }
                let empty;
                let bundle = match bundle_owned.as_ref() {
                    Some(bundle) => bundle,
                    None => {
                        empty = EvidenceBundle::empty(cid, chain.clone(), address.clone());
                        &empty
                    }
                };
                let selectors = all_scope_selectors.get(&cid).cloned().unwrap_or_default();
                let all_bundle = selectors.all.filtered_bundle(bundle);
                let mut analysis = match analyze_candidate(&store, cid, &all_bundle, &paper) {
                    Ok(a) => a,
                    Err(e) => return Err((chain, address, e)),
                };
                drop(all_bundle);

                // Persist the full all-chains detail before shrinking it; scoped
                // analyses below only need summary fields.
                let rel = candidate_json_rel_path(&analysis.chain, &analysis.address);
                let body = serialize_candidate_json(&analysis)
                    .map_err(|e| (chain.clone(), address.clone(), e))?;
                write_tx.send((rel, body)).map_err(|e| {
                    (
                        chain.clone(),
                        address.clone(),
                        Analysis2Error::invalid(format!("candidate write queue closed: {e}")),
                    )
                })?;
                analysis.shrink_for_summary_memory();
                let mut per_seed = Vec::with_capacity(selectors.all.token_ids_by_seed.len());
                if selectors.all.token_ids_by_seed.len() == 1 {
                    let seed_key = selectors
                        .all
                        .token_ids_by_seed
                        .keys()
                        .next()
                        .cloned()
                        .expect("one per-seed selector");
                    per_seed.push((seed_key, analysis.clone()));
                } else {
                    for (seed_key, token_ids) in &selectors.all.token_ids_by_seed {
                        let per_seed_selector = ScopeEvidenceSelector {
                            token_ids: token_ids.clone(),
                            seed_keys: AHashSet::from([seed_key.clone()]),
                            token_ids_by_seed: AHashMap::from([(
                                seed_key.clone(),
                                token_ids.clone(),
                            )]),
                        };
                        let scoped = per_seed_selector.filtered_bundle(bundle);
                        let mut scoped_analysis =
                            analyze_candidate(&store, cid, &scoped, &paper)
                                .map_err(|e| (chain.clone(), address.clone(), e))?;
                        scoped_analysis.shrink_for_summary_memory();
                        per_seed.push((seed_key.clone(), scoped_analysis));
                    }
                }
                // Drop large transfer/sale/holder payloads before the next candidate
                // is scheduled on this worker.
                drop(bundle_owned);

                progress.add_completed(1);
                Ok(CandidateAnalysisBatch { per_seed })
            })
            .collect();

    // Close the queue and wait for disk flushes before reporting.
    drop(write_tx);
    writer
        .join()
        .map_err(|_| Analysis2Error::invalid("candidate writer thread panicked"))??;

    let mut per_seed_analyses = AHashMap::<String, AHashMap<ContractId, CandidateAnalysis>>::new();
    for result in analyze_results {
        match result {
            Ok(batch) => {
                for (seed_key, analysis) in batch.per_seed {
                    per_seed_analyses
                        .entry(seed_key)
                        .or_default()
                        .insert(analysis.contract_id, analysis);
                }
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

    // Attach analysis rollups to already-materialized seed dedup reports.
    progress.set_stage("report");
    progress.begin_phase("aggregate_seeds", Some(seed_dedups.len() as u64));
    let empty_seed_analyses = AHashMap::new();
    let reports = seed_dedups
        .into_par_iter()
        .map(|(seed, seed_id, dedup)| {
            let scopes_complete = scopes_complete_for_seed(&store, &dedup);
            let seed_key = canonical_relation_key(&seed.chain, &seed.address);
            let seed_analyses = per_seed_analyses
                .get(&seed_key)
                .unwrap_or(&empty_seed_analyses);
            let (rollup, analysis_ok) = build_seed_analysis_rollup(
                &registry,
                seed_id,
                &seed.chain,
                &seed.address,
                seed_analyses,
                analysis2_core::DETAIL_CANDIDATES_REL,
            );
            let analysis_complete = analysis_ok
                && registry.relations_for_seed(seed_id).iter().all(|rel| {
                    seed_analyses
                        .get(&rel.candidate_contract)
                        .is_some_and(CandidateAnalysis::is_formal_complete)
                });
            progress.add_completed(1);
            (
                seed,
                SeedFullReport {
                    dedup,
                    scopes_complete,
                    analysis_complete,
                    analysis: Some(rollup),
                },
            )
        })
        .collect::<Vec<_>>();

    // A seed becomes formal only after every one of its candidate analyses has
    // passed the evidence-quality gates. Build summary selectors from that exact
    // final set. This prevents an incomplete seed's token-specific sales from
    // leaking into another formal seed that happens to share the candidate.
    let formal_seed_keys: AHashSet<String> = reports
        .iter()
        .filter(|(_, report)| report.is_formal())
        .map(|(seed, _)| canonical_relation_key(&seed.chain, &seed.address))
        .collect();
    let formal_scope_selectors =
        retain_formal_seed_selectors(&all_scope_selectors, &formal_seed_keys);

    // Candidate-wide funding/withdrawal evidence is deliberately present in
    // every related seed analysis. Re-analyze the union of *formal* relations
    // once per candidate for run summaries, which de-duplicates those amounts
    // while retaining the exact formal token scope. Reloading the durable cache
    // avoids retaining all raw HTTP evidence across the first analysis barrier.
    progress.set_stage("analyze");
    progress.begin_phase(
        "analyze_formal_scopes",
        Some(formal_scope_selectors.len() as u64),
    );
    let cache = load_evidence_cache_resumable(&evidence_path)?;
    validate_evidence_cache(&cache, &evidence_params)?;
    let mut summary_evidence = rematerialize_evidence(&store, &cache)?;
    summary_evidence.retain(|candidate_id, _| formal_scope_selectors.contains_key(candidate_id));
    for bundle in summary_evidence.values_mut() {
        bundle.strip_for_analysis_memory();
    }
    type FormalScopeBatch = (
        CandidateAnalysis,
        Option<CandidateAnalysis>,
        Option<CandidateAnalysis>,
        Vec<((String, String), CandidateAnalysis)>,
    );
    let formal_work: Vec<_> = formal_scope_selectors
        .into_iter()
        .map(|(candidate_id, selectors)| {
            let bundle = summary_evidence.remove(&candidate_id);
            (candidate_id, selectors, bundle)
        })
        .collect();
    drop(summary_evidence);
    let formal_results: Vec<Result<FormalScopeBatch, (String, String, Analysis2Error)>> =
        formal_work
            .into_par_iter()
            .map(|(candidate_id, selectors, bundle)| {
                let contract = &store.contracts[candidate_id as usize];
                let chain = store.chain_name(contract.chain_id).to_owned();
                let address = contract.address.clone();
                let bundle = bundle.ok_or_else(|| {
                    (
                        chain.clone(),
                        address.clone(),
                        Analysis2Error::invalid("formal summary evidence missing from cache"),
                    )
                })?;
                let analyze_scope = |selector: &ScopeEvidenceSelector| {
                    let scoped = selector.filtered_bundle(&bundle);
                    let mut analysis = analyze_candidate(&store, candidate_id, &scoped, &paper)?;
                    analysis.shrink_for_summary_memory();
                    Ok::<_, Analysis2Error>(analysis)
                };
                let all = analyze_scope(&selectors.all)
                    .map_err(|error| (chain.clone(), address.clone(), error))?;
                if !all.is_formal_complete() {
                    return Err((
                        chain,
                        address,
                        Analysis2Error::invalid(
                            "formal seed union failed candidate completeness invariant",
                        ),
                    ));
                }
                let intra = (!selectors.intra.token_ids_by_seed.is_empty())
                    .then(|| analyze_scope(&selectors.intra))
                    .transpose()
                    .map_err(|error| (chain.clone(), address.clone(), error))?;
                let cross = (!selectors.cross.token_ids_by_seed.is_empty())
                    .then(|| analyze_scope(&selectors.cross))
                    .transpose()
                    .map_err(|error| (chain.clone(), address.clone(), error))?;
                let mut matrix = Vec::with_capacity(selectors.matrix.len());
                for (direction, selector) in &selectors.matrix {
                    matrix.push((
                        direction.clone(),
                        analyze_scope(selector)
                            .map_err(|error| (chain.clone(), address.clone(), error))?,
                    ));
                }
                progress.add_completed(1);
                Ok((all, intra, cross, matrix))
            })
            .collect();

    let mut analyses_map: AHashMap<ContractId, CandidateAnalysis> = AHashMap::new();
    let mut scope_analyses = ScopeAnalysisSets::default();
    for result in formal_results {
        match result {
            Ok((all, intra, cross, matrix)) => {
                if let Some(analysis) = intra {
                    scope_analyses.intra_chain.push(analysis);
                }
                if let Some(analysis) = cross {
                    scope_analyses.cross_chain.push(analysis);
                }
                for (direction, analysis) in matrix {
                    scope_analyses
                        .chain_matrix
                        .entry(direction)
                        .or_default()
                        .push(analysis);
                }
                analyses_map.insert(all.contract_id, all);
            }
            Err((chain, address, error)) => {
                return Err(Analysis2Error::invalid(format!(
                    "formal summary analysis failed for {chain}:{address}: {error}"
                )));
            }
        }
    }
    // Registry and per-seed analyses are no longer needed after exact formal
    // summaries have been materialized.
    drop(registry);
    drop(per_seed_analyses);

    let analyzed: Vec<Result<(SeedRecord, SeedFullReport), FailureRecord>> =
        reports.into_iter().map(Ok).collect();
    // Failed resolve/dedup seeds are recorded only in `failures` (extra_failures).

    let analyses_list: Vec<CandidateAnalysis> = analyses_map.into_values().collect();

    progress.begin_phase("write", Some(1));
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
        &scope_analyses,
        &failures,
    )?;
    progress.add_completed(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use analysis2_core::parquet::write_report_golden_fixture;
    use analysis2_core::{DEFAULT_METADATA_THRESHOLD, DEFAULT_NAME_THRESHOLD, SaleEvent};
    use serde_json::Value;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct PhaseRecordingProgress {
        phases: Mutex<Vec<String>>,
    }

    impl ProgressObserver for PhaseRecordingProgress {
        fn set_stage(&self, _stage: &str) {}

        fn begin_phase(&self, phase: &str, _total: Option<u64>) {
            self.phases.lock().unwrap().push(phase.to_owned());
        }

        fn add_completed(&self, _n: u64) {}

        fn check_cancelled(&self) -> Result<(), Analysis2Error> {
            Ok(())
        }

        fn finish(&self) {}
    }

    #[test]
    fn cached_truncation_retries_only_when_a_provider_failure_was_recorded() {
        let mut cap_limited = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        cap_limited.quality.sales = EvidenceStatus::Truncated;
        assert!(!cached_evidence_needs_retry(&cap_limited));

        cap_limited
            .quality
            .failures
            .push("alchemy_sales: partial page failure".into());
        assert!(cached_evidence_needs_retry(&cap_limited));

        let mut failed = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        failed.quality.sales = EvidenceStatus::Failed;
        assert!(cached_evidence_needs_retry(&failed));
    }

    #[test]
    fn explicit_rayon_threads_use_a_run_local_pool() {
        let _ = rayon::current_num_threads();
        let workers = with_rayon_pool(Some(3), || {
            Ok::<_, Analysis2Error>(rayon::current_num_threads())
        })
        .unwrap();
        assert_eq!(workers, 3);
        assert!(with_rayon_pool(Some(0), || Ok::<_, Analysis2Error>(())).is_err());
    }

    #[test]
    fn scope_filter_keeps_legit_labels_and_contract_wide_nft_events() {
        let legit_seed = "ethereum:0xlegit".to_owned();
        let suspicious_seed = "ethereum:0xsuspicious".to_owned();
        let mut selector = ScopeEvidenceSelector::default();
        selector
            .token_ids
            .extend(["legit-nft".to_owned(), "suspect-nft".to_owned()]);
        selector
            .seed_keys
            .extend([legit_seed.clone(), suspicious_seed.clone()]);
        selector
            .token_ids_by_seed
            .insert(legit_seed.clone(), AHashSet::from(["legit-nft".to_owned()]));
        selector.token_ids_by_seed.insert(
            suspicious_seed.clone(),
            AHashSet::from(["suspect-nft".to_owned()]),
        );

        let mut bundle = EvidenceBundle::empty(0, "base", "0xcandidate");
        bundle.relation_legit.insert(
            legit_seed.clone(),
            LegitSignals {
                verified_migration: true,
                verification_complete: true,
                ..LegitSignals::default()
            },
        );
        bundle
            .relation_legit
            .insert(suspicious_seed.clone(), LegitSignals::default());
        for token_id in ["legit-nft", "suspect-nft"] {
            bundle.sales.push(SaleEvent {
                tx_hash: format!("tx-{token_id}"),
                token_id: token_id.into(),
                seller: "seller".into(),
                buyer: "buyer".into(),
                timestamp: None,
                block_number: None,
                marketplace: None,
                native_amount: Some(1.0),
                usd_amount: Some(2_000.0),
                currency_symbol: Some("ETH".into()),
                currency_address: None,
                seller_proceeds_native: Some(1.0),
                seller_proceeds_usd: Some(2_000.0),
                ..SaleEvent::default()
            });
        }

        let filtered = selector.filtered_bundle(&bundle);
        assert_eq!(filtered.sales.len(), 2);
        assert!(
            filtered
                .sales
                .iter()
                .any(|sale| sale.token_id == "legit-nft")
        );
        assert!(
            filtered
                .sales
                .iter()
                .any(|sale| sale.token_id == "suspect-nft")
        );
        assert_eq!(filtered.relation_legit.len(), 2);
        assert!(filtered.relation_legit[&legit_seed].is_legit_duplicate());
        assert!(!filtered.relation_legit[&suspicious_seed].is_legit_duplicate());
    }

    #[test]
    fn formal_selector_filter_removes_incomplete_seed_tokens() {
        let mut source = CandidateScopeSelectors::default();
        source.all.token_ids_by_seed.insert(
            "ethereum:0xformal".into(),
            AHashSet::from(["formal-token".into()]),
        );
        source.all.token_ids_by_seed.insert(
            "ethereum:0xincomplete".into(),
            AHashSet::from(["incomplete-token".into()]),
        );
        source.all.token_ids = AHashSet::from(["formal-token".into(), "incomplete-token".into()]);
        source.all.seed_keys =
            AHashSet::from(["ethereum:0xformal".into(), "ethereum:0xincomplete".into()]);
        let filtered = retain_formal_seed_selectors(
            &AHashMap::from([(7, source)]),
            &AHashSet::from(["ethereum:0xformal".into()]),
        );
        let all = &filtered[&7].all;
        assert_eq!(all.token_ids, AHashSet::from(["formal-token".into()]));
        assert_eq!(all.seed_keys, AHashSet::from(["ethereum:0xformal".into()]));
        assert!(!all.token_ids_by_seed.contains_key("ethereum:0xincomplete"));
    }

    #[test]
    fn omitted_name_threshold_skips_name_dedup_stage() {
        let dir =
            std::env::temp_dir().join(format!("analysis2_no_name_dedup_{}", std::process::id()));
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
        let progress = PhaseRecordingProgress::default();

        run_dedup(
            &RunDedupConfig {
                inputs: vec![parquet],
                seeds,
                output_dir: out.clone(),
                chains: vec!["ethereum".into(), "base".into(), "solana".into()],
                evm_chains: vec!["ethereum".into(), "base".into()],
                name_threshold: None,
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: None,
            },
            &progress,
        )
        .expect("run-dedup without Name dedup");

        let phases = progress.phases.lock().unwrap();
        assert!(!phases.iter().any(|phase| phase.starts_with("name_")));
        assert!(phases.iter().any(|phase| phase == "metadata_seeds"));
        drop(phases);

        let manifest: Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("intermediate/run_manifest.json")).unwrap(),
        )
        .unwrap();
        assert!(manifest["params"]["name_threshold"].is_null());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Allows load-time checks; after `dedup` stage, skips the resolve and
    /// URI-stage worker gates, then cancels inside `query_uri_for_seed`.
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
            // n==0: resolve worker; n==1: URI-stage worker; n>=2: query.
            if n >= 2 {
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
                name_threshold: Some(DEFAULT_NAME_THRESHOLD),
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
            !out.join("intermediate/run_manifest.json").exists()
                && !out.join("run_manifest.json").exists(),
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
                    currency_address: None,
                    seller_proceeds_native: Some(1.0),
                    seller_proceeds_usd: Some(42.0),
                    ..SaleEvent::default()
                });
                bundle.controllers.push("0xop".into());
                bundle.quality = analysis2_core::EvidenceQuality {
                    transfers: EvidenceStatus::Empty,
                    sales: EvidenceStatus::Complete,
                    holders: EvidenceStatus::Empty,
                    prices: EvidenceStatus::Complete,
                    assets: EvidenceStatus::Empty,
                    histories: EvidenceStatus::Empty,
                    gas: EvidenceStatus::Empty,
                    value_flows: EvidenceStatus::Empty,
                    failures: Vec::new(),
                };
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
                name_threshold: Some(DEFAULT_NAME_THRESHOLD),
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
                dedup_cache_path: None,
                reuse_dedup: false,
                evidence_cache_path: None,
                reuse_evidence: false,
            },
            &analysis2_core::NoopProgress,
        )
        .expect("fixture run");

        assert!(out.join("intermediate/run_manifest.json").is_file());
        assert!(out.join("summary/all_chains.json").is_file());
        assert!(out.join("summary/intra_chain.json").is_file());
        assert!(out.join("summary/chain_matrix.json").is_file());
        assert!(out.join("summary/cross_chain.json").is_file());
        assert!(out.join("intermediate/failures.jsonl").is_file());
        assert!(
            out.join("detail/seeds/ethereum__0xseed/report.json")
                .is_file()
        );

        let summary: Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("summary/all_chains.json")).unwrap(),
        )
        .unwrap();
        for key in [
            "scope",
            "duplicate_scale",
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
            "scope_summary",
        ] {
            assert!(summary.get(key).is_some(), "missing summary key {key}");
        }
        assert_eq!(summary["scope"], "all_chains");
        assert!(summary["economics"].get("operator_output_usd").is_some());
        assert!(
            summary["economics"]
                .get("honest_paid_exposure_usd")
                .is_some()
        );
        assert!(summary["economics"].get("operator_output_native").is_none());
        assert!(!serde_json::to_string(&summary).unwrap().contains("_native"));

        let matrix: Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("summary/chain_matrix.json")).unwrap(),
        )
        .unwrap();
        let blocks = matrix["matrix_blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().all(|block| {
            block["primary_chain"] == "ethereum"
                && block.get("secondary_chain").is_some()
                && block["summary"].is_object()
        }));

        let manifest: Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("intermediate/run_manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["command"], "run");
        assert!(manifest["status"] == "complete" || manifest["status"] == "complete_with_failures");
        assert_eq!(manifest["output_layout"]["detail"], "detail");

        let seed_report: Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("detail/seeds/ethereum__0xseed/report.json"))
                .unwrap(),
        )
        .unwrap();
        assert!(seed_report.get("scopes_complete").is_some());
        assert!(seed_report.get("analysis").is_some());

        // At least one candidate artifact streamed under detail/candidates.
        let cand_dir = out.join("detail/candidates");
        assert!(cand_dir.is_dir());
        let cand_count = std::fs::read_dir(&cand_dir).unwrap().count();
        assert!(cand_count >= 1, "expected streamed candidate JSON");
        let first_candidate = std::fs::read_to_string(
            std::fs::read_dir(&cand_dir)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap();
        assert!(!first_candidate.contains("_native"));

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
                name_threshold: Some(DEFAULT_NAME_THRESHOLD),
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: Some(2),
                api_keys: ApiKeys::default(),
                http_concurrency: 4,
                paper: PaperConfig::default(),
                enrich_override: Some(enrich),
                dedup_cache_path: None,
                reuse_dedup: false,
                evidence_cache_path: None,
                reuse_evidence: false,
            },
            &CancelOnEnrich,
        )
        .expect_err("cancel");
        assert!(matches!(err, Analysis2Error::Cancelled));
        assert!(
            !out.join("intermediate/run_manifest.json").exists()
                && !out.join("run_manifest.json").exists()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_writes_dedup_cache_and_reuse_skips_query() {
        let dir =
            std::env::temp_dir().join(format!("analysis2_run_dedup_cache_{}", std::process::id()));
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
        let cache_path = out.join("dedup_cache.json");
        let evidence_path = out.join("evidence_cache.json");

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
                map.insert(cid, EvidenceBundle::empty(cid, chain, c.address.clone()));
                progress.add_completed(1);
            }
            Ok(map)
        });

        let base_config = || RunConfig {
            inputs: vec![parquet.clone()],
            seeds: seeds.clone(),
            output_dir: out.clone(),
            chains: vec!["ethereum".into(), "base".into(), "solana".into()],
            evm_chains: vec!["ethereum".into(), "base".into()],
            name_threshold: Some(DEFAULT_NAME_THRESHOLD),
            metadata_threshold: DEFAULT_METADATA_THRESHOLD,
            metadata_anchors: 8,
            rayon_threads: Some(2),
            api_keys: ApiKeys::default(),
            http_concurrency: 4,
            paper: PaperConfig {
                analysis_timestamp: 1_700_000_100,
                ..PaperConfig::default()
            },
            enrich_override: Some(enrich.clone()),
            dedup_cache_path: Some(cache_path.clone()),
            reuse_dedup: false,
            evidence_cache_path: Some(evidence_path.clone()),
            reuse_evidence: false,
        };

        run(&base_config(), &analysis2_core::NoopProgress).expect("first run");
        assert!(cache_path.is_file(), "dedup cache must be written");
        assert!(evidence_path.is_file(), "evidence cache must be written");

        // Explicit flags still work.
        let mut reuse = base_config();
        reuse.reuse_dedup = true;
        reuse.reuse_evidence = true;
        run(&reuse, &analysis2_core::NoopProgress).expect("reuse-dedup+evidence run");
        assert!(out.join("summary/all_chains.json").is_file());

        // Auto-reuse without flags when cache files are present.
        let auto = base_config();
        assert!(!auto.reuse_dedup && !auto.reuse_evidence);
        run(&auto, &analysis2_core::NoopProgress).expect("auto-reuse run");
        assert!(out.join("summary/all_chains.json").is_file());

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
                name_threshold: Some(DEFAULT_NAME_THRESHOLD),
                metadata_threshold: DEFAULT_METADATA_THRESHOLD,
                metadata_anchors: 8,
                rayon_threads: Some(2),
                api_keys: ApiKeys::default(),
                http_concurrency: 4,
                paper: PaperConfig::default(),
                enrich_override: Some(enrich),
                dedup_cache_path: None,
                reuse_dedup: false,
                evidence_cache_path: None,
                reuse_evidence: false,
            },
            &analysis2_core::NoopProgress,
        )
        .expect("run completes with analyze failures");

        let failures_path = out.join("intermediate/failures.jsonl");
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
