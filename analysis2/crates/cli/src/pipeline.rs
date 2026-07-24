//! Offline `run-dedup` and full `run` orchestration.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::AHashMap;
use analysis2_core::{
    analyze_candidate, build_contract_nft_map, build_dedup_cache, build_evidence_cache,
    build_seed_analysis_rollup, build_seed_dedup_report, default_dedup_cache_path,
    default_evidence_cache_path, enrich_candidates_with_hook, evidence_cache_artifacts_present,
    evidence_cache_params, load_dedup_cache, load_evidence_cache_resumable,
    load_resident_store_uri_ready, load_seeds_json, query_metadata_for_seed_with_scratch,
    query_name_for_seed_with_scratch, query_uri_for_seed_with_scratch, rematerialize_dedup_batch,
    rematerialize_evidence, resolve_seed_contract, scopes_complete_for_seed, validate_dedup_cache,
    validate_evidence_cache, write_candidate_json, write_dedup_cache, write_dedup_outputs,
    write_evidence_cache, write_run_outputs, Analysis2Error, ApiKeys, CandidateAnalysis,
    CandidateRegistry, ContractId, DedupCacheParams, DedupRunParams, EvidenceBundle,
    EvidenceCacheSink, FailureRecord, HitGraph, HttpLimits, LoadOptions, MetadataQueryScratch,
    NameQueryScratch, PaperConfig, PendingDedupLoad, ProgressObserver, ResidentStore,
    SeedFullReport, SeedRecord, UriQueryScratch, DEFAULT_EVIDENCE_CACHE_BATCH,
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
    name_threshold: f64,
    metadata_threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    let quiet = CancellationOnlyProgress { inner: progress };
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
            query_name_for_seed_with_scratch(store, seed, name_threshold, graph, &quiet, scratch)
        },
    )?;
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
    name_threshold: f64,
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
    name_threshold: f64,
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
    let options = LoadOptions::new(
        config.chains.clone(),
        config.evm_chains.clone(),
        config.metadata_anchors,
    );
    let seeds = load_seeds_json(&config.seeds)?;
    let (mut store, pending) =
        load_resident_store_uri_ready(&config.inputs, &options, progress)?;
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
    config
        .evidence_cache_path
        .clone()
        .unwrap_or_else(|| default_evidence_cache_path(&config.output_dir))
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

fn load_seed_batch_from_cache(
    store: &ResidentStore,
    cache_path: &Path,
    expected: &DedupCacheParams,
    progress: &dyn ProgressObserver,
) -> Result<SeedDedupBatch, Analysis2Error> {
    progress.set_stage("dedup");
    progress.begin_phase("load_dedup_cache", Some(1));
    let cache = load_dedup_cache(cache_path)?;
    validate_dedup_cache(&cache, expected)?;
    let (completed, failures) = rematerialize_dedup_batch(store, &cache)?;
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

/// End-to-end: load → dedup (or cache) → enrich → analyze → full reports.
fn run_inner(config: &RunConfig, progress: &dyn ProgressObserver) -> Result<(), Analysis2Error> {
    let seeds = load_seeds_json(&config.seeds)?;
    let cache_path = dedup_cache_path(config);
    let cache_params = make_dedup_cache_params(config, &seeds);

    let options = if config.reuse_dedup {
        LoadOptions::identity_only(
            config.chains.clone(),
            config.evm_chains.clone(),
            config.metadata_anchors,
        )
    } else {
        LoadOptions::new(
            config.chains.clone(),
            config.evm_chains.clone(),
            config.metadata_anchors,
        )
    };
    let (mut store, pending) =
        load_resident_store_uri_ready(&config.inputs, &options, progress)?;
    let contract_nfts = build_contract_nft_map(&store);

    let seed_batch = if config.reuse_dedup {
        if !cache_path.is_file() {
            return Err(Analysis2Error::invalid(format!(
                "--reuse-dedup requires existing cache file {}",
                cache_path.display()
            )));
        }
        // Identity-only load leaves no pending pass-2 work.
        let _ = pending;
        load_seed_batch_from_cache(&store, &cache_path, &cache_params, progress)?
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
        // Persist immediately so a later `--reuse-dedup` can skip query work.
        progress.begin_phase("write_dedup_cache", Some(1));
        let cache = build_dedup_cache(
            &store,
            cache_params,
            &batch.completed,
            &batch.failures,
        );
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

    let completed = seed_batch.completed;
    let mut failures = seed_batch.failures;

    let registry = CandidateRegistry::from_hit_graphs(
        completed.iter().map(|(_, _, graph)| graph),
        &contract_nfts,
    );

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
    let cache_exists = evidence_cache_artifacts_present(&evidence_path);
    if cache_exists || config.reuse_evidence {
        progress.begin_phase("load_evidence_cache", Some(1));
        match load_evidence_cache_resumable(&evidence_path) {
            Ok(cache) => {
                if let Err(e) = validate_evidence_cache(&cache, &evidence_params) {
                    if config.reuse_evidence {
                        return Err(e);
                    }
                    eprintln!("evidence: ignoring incompatible cache: {e}");
                } else {
                    evidence = rematerialize_evidence(&store, &cache)?;
                    eprintln!(
                        "evidence: resumed cache {} ({} bundles)",
                        evidence_path.display(),
                        evidence.len()
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
    }

    let evidence = match &config.enrich_override {
        Some(hook) => {
            // Test / offline hooks replace evidence entirely (still written to cache).
            let map = hook(&registry, &store, progress)?;
            progress.begin_phase("write_evidence_cache", Some(1));
            let evidence_file = build_evidence_cache(evidence_params, &map);
            write_evidence_cache(&evidence_path, &evidence_file)?;
            progress.add_completed(1);
            map
        }
        None => {
            let missing: ahash::AHashSet<ContractId> = registry
                .candidate_contracts()
                .iter()
                .copied()
                .filter(|cid| !evidence.contains_key(cid))
                .collect();
            if missing.is_empty() {
                eprintln!(
                    "evidence: all {} candidates covered by cache; skipping HTTP enrich",
                    registry.candidate_contract_count()
                );
                // Refresh compact snapshot from in-memory set.
                progress.begin_phase("write_evidence_cache", Some(1));
                let evidence_file = build_evidence_cache(evidence_params, &evidence);
                write_evidence_cache(&evidence_path, &evidence_file)?;
                progress.add_completed(1);
                evidence
            } else {
                let subset = registry.filter_candidates(&missing);
                eprintln!(
                    "evidence: fetching {} / {} candidates via HTTP (batch flush every {})",
                    subset.candidate_contract_count(),
                    registry.candidate_contract_count(),
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
                    let mut on_bundle =
                        |bundle: &EvidenceBundle| -> Result<(), Analysis2Error> {
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

    progress.check_cancelled()?;
    progress.set_stage("analyze");
    let candidates = registry.candidate_contracts().to_vec();
    progress.begin_phase("analyze_candidates", Some(candidates.len() as u64));

    analysis2_core::ensure_output_layout(&config.output_dir)
        .map_err(Analysis2Error::from)?;
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
                let empty;
                let bundle = match evidence.get(&cid) {
                    Some(bundle) => bundle,
                    None => {
                        empty = EvidenceBundle::empty(cid, chain.clone(), address.clone());
                        &empty
                    }
                };
                let analysis = match analyze_candidate(&store, cid, bundle, &paper) {
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
    for result in analyze_results {
        match result {
            Ok(analysis) => {
                analyses_map.insert(analysis.contract_id, analysis);
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
    progress.begin_phase("aggregate_seeds", Some(completed.len() as u64));
    let reports = completed
        .par_iter()
        .map(|(seed, seed_id, graph)| {
            let dedup =
                build_seed_dedup_report(&store, seed, *seed_id, graph, &registry, &contract_nfts);
            let scopes_complete = scopes_complete_for_seed(&store, &dedup);
            let (rollup, analysis_ok) = build_seed_analysis_rollup(
                &registry,
                *seed_id,
                &seed.chain,
                &seed.address,
                &analyses_map,
                analysis2_core::DETAIL_CANDIDATES_REL,
            );
            let analysis_complete = analysis_ok
                && registry
                    .relations_for_seed(*seed_id)
                    .iter()
                    .all(|rel| analyses_map.contains_key(&rel.candidate_contract));
            progress.add_completed(1);
            (
                seed.clone(),
                SeedFullReport {
                    dedup,
                    scopes_complete,
                    analysis_complete,
                    analysis: Some(rollup),
                },
            )
        })
        .collect::<Vec<_>>();
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
        assert!(out
            .join("detail/seeds/ethereum__0xseed/report.json")
            .is_file());

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
            "all_chains",
        ] {
            assert!(summary.get(key).is_some(), "missing summary key {key}");
        }
        assert_eq!(summary["scope"], "all_chains");
        assert!(summary["economics"].get("operator_output_usd").is_some());
        assert!(summary["economics"].get("honest_loss_usd").is_some());
        assert!(summary["economics"].get("operator_output_native").is_none());

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
        let dir = std::env::temp_dir().join(format!(
            "analysis2_run_dedup_cache_{}",
            std::process::id()
        ));
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
            enrich_override: Some(enrich.clone()),
            dedup_cache_path: Some(cache_path.clone()),
            reuse_dedup: false,
            evidence_cache_path: Some(evidence_path.clone()),
            reuse_evidence: false,
        };

        run(&base_config(), &analysis2_core::NoopProgress).expect("first run");
        assert!(cache_path.is_file(), "dedup cache must be written");
        assert!(evidence_path.is_file(), "evidence cache must be written");

        let mut reuse = base_config();
        reuse.reuse_dedup = true;
        reuse.reuse_evidence = true;
        run(&reuse, &analysis2_core::NoopProgress).expect("reuse-dedup+evidence run");
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
                name_threshold: DEFAULT_NAME_THRESHOLD,
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
