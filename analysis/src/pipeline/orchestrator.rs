use crate::analysis::{analyze_candidate, legit_duplicate, normalize_evidence, relation_projector};
use crate::config::RunConfig;
use crate::enrich::{fetch_with_retry, ApiClients};
use crate::error::{AnalysisError, Result};
use crate::input::load_resident_store;
use crate::model::{
    AggregateDelta, CandidateId, EvidenceBundle, EvidenceObservation, EvidenceQuality,
    EvidenceStatus, NftSelection,
};
use crate::pipeline::CandidateRecord;
use crate::pipeline::{execute_dedup, CandidateRegistry, CpuExecutor, DedupOutput};
use crate::progress::Progress;
use crate::reporting::{
    csv, json, markdown, AggregateState, ArtifactIndex, ContractArtifact, ContractWriter,
    PayloadAdmission,
};
use crate::seed::SeedManifest;
use ahash::AHashMap;
#[cfg(test)]
use async_trait::async_trait;
use futures_util::FutureExt;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub async fn run(config_path: &Path, seeds_override: Option<&Path>) -> Result<()> {
    let mut config = RunConfig::from_path(config_path)?;
    if let Some(seeds) = seeds_override {
        config.seed_manifest = seeds.to_path_buf();
    }
    let platform = crate::platform::inspect_production_platform()?;
    config.memory_limit = config.memory_limit.min(platform.effective_memory_limit);
    let manifest = SeedManifest::from_path(&config.seed_manifest, config.seed_top)?;
    let progress = Arc::new(Progress::default());
    let worker_placements = platform.worker_placements(config.numa_mode, config.cpu_workers)?;
    let executor = Arc::new(CpuExecutor::new_numa_bounded(
        config.cpu_workers,
        config.cpu_queue_capacity,
        &worker_placements,
    )?);
    let (progress_stop, mut progress_stop_rx) = tokio::sync::oneshot::channel();
    let reported_progress = progress.clone();
    let reported_executor = executor.clone();
    let progress_reporter = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let (active, queued) = reported_executor.utilization();
                    reported_progress.record_cpu(reported_executor.workers(), active, queued);
                    eprintln!("{}", reported_progress.snapshot().human_line());
                }
                _ = &mut progress_stop_rx => break,
            }
        }
    });
    let store = load_resident_store(
        &config.snapshot_files,
        config.metadata_anchor_count,
        config.index_shards,
        config.memory_limit,
        &executor,
        &progress,
    )?;
    if let Some(memory) = crate::platform::current_memory_usage()? {
        progress.record_memory(memory);
        if memory > config.memory_limit {
            return Err(AnalysisError::MemoryBudget {
                required: memory,
                limit: config.memory_limit,
            });
        }
    }
    progress.set_entities(
        store.quality.logical_nfts,
        store.contracts.contracts.len() as u64,
    );
    let memory_limit = config.memory_limit;
    let base_bytes = store.logical_bytes();
    let name_bytes = store
        .name_features
        .as_ref()
        .map(|features| {
            features.values.bytes().saturating_mul(16).saturating_add(
                features.contract_names.len() as u64
                    * std::mem::size_of::<Option<crate::model::NameValueId>>() as u64,
            )
        })
        .unwrap_or(0);
    let metadata_bytes = store
        .metadata_features
        .as_ref()
        .map(|features| {
            features
                .documents
                .bytes()
                .saturating_mul(3)
                .saturating_add(features.profiles.len() as u64 * 64)
        })
        .unwrap_or(0);
    let uri_bytes = store
        .uri_features
        .as_ref()
        .map(|features| features.values.bytes())
        .unwrap_or(0);
    let current_dimension = name_bytes.max(metadata_bytes).max(uri_bytes);
    let overlap_increment = uri_bytes
        .saturating_add(name_bytes)
        .saturating_sub(current_dimension);
    let response_limit =
        provider_response_limit(config.memory_limit, config.analysis_queue_capacity);
    let candidate_inflight = (config.analysis_queue_capacity as u64)
        .saturating_mul(response_limit)
        .saturating_mul(4);
    let memory_plan = crate::platform::MemoryPlan {
        long_lived: base_bytes,
        current_dimension,
        next_dimension: overlap_increment,
        worker_scratch: config.cpu_workers as u64 * 64 * 1024 * 1024,
        candidate_inflight,
        writer_queue: config.writer_queue_bytes,
        allocator_reserve: 32 * 1024 * 1024 * 1024,
    };
    config.next_dimension_overlap =
        memory_plan.choose_overlap(memory_limit, config.next_dimension_overlap)?;
    let run_id = deterministic_run_id(&config, &manifest)?;
    let writer = Arc::new(ContractWriter::create(
        &config.output_dir,
        &run_id,
        config.writer_queue_bytes,
    )?);
    let mut aggregate = AggregateState::default();
    let mut artifact_index = ArtifactIndex::default();
    let identities = Arc::new(crate::resident::AnalysisIdentityStore::default());
    let provider: Arc<dyn crate::api::EvidenceProvider> = Arc::new(ApiClients::new(
        Duration::from_millis(config.provider_timeout_ms),
        &config,
        response_limit,
        executor.clone(),
    )?);
    let mut registry = CandidateRegistry::default();
    // Dedup publishes from its spawn_blocking coordinator, never from a Rayon
    // worker. This bounded channel therefore applies backpressure without
    // occupying one of the 128 CPU workers while the consumer catches up.
    let (candidate_tx, candidate_rx) =
        tokio::sync::mpsc::channel(config.network_queue_capacity.max(1));
    let dedup_executor = executor.clone();
    let dedup_manifest = manifest.clone();
    let dedup_config = config.clone();
    let dedup_progress = progress.clone();
    let dedup_task = tokio::task::spawn_blocking(move || {
        execute_dedup(
            store,
            &dedup_manifest,
            &dedup_config,
            &dedup_executor,
            &dedup_progress,
            candidate_tx,
        )
    });

    write_manifest(writer.run_dir(), &run_id, &config, &platform)?;
    let (dedup, mut all_relations) = process_candidates(
        CandidatePipelineContext {
            registry: &mut registry,
            provider,
            executor: &executor,
            writer: writer.clone(),
            aggregate: &mut aggregate,
            artifact_index: &mut artifact_index,
            config: &config,
            progress: &progress,
            identities: identities.clone(),
        },
        candidate_rx,
        dedup_task,
    )
    .await?;
    all_relations.sort_by(|left, right| {
        (left.seed_id, &left.candidate).cmp(&(right.seed_id, &right.candidate))
    });
    write_final_reports(FinalReportContext {
        run_dir: writer.run_dir(),
        manifest: &manifest,
        relations: &all_relations,
        artifacts: &mut artifact_index,
        aggregate: &mut aggregate,
        catalog: &dedup.store.contracts,
        input_quality: &dedup.store.quality,
        identities: &identities,
        failed_seeds: &dedup.failed_seeds,
    })?;
    writer.finish_success()?;
    progress.mark_all_written_durable();
    let _ = progress_stop.send(());
    let _ = progress_reporter.await;
    Ok(())
}

fn provider_response_limit(memory_limit: u64, active_candidate_capacity: usize) -> u64 {
    const MIN_RESPONSE: u64 = 64 * 1024 * 1024;
    const MAX_RESPONSE: u64 = 1024 * 1024 * 1024;
    memory_limit
        .checked_div((active_candidate_capacity.max(1) as u64).saturating_mul(16))
        .unwrap_or(MIN_RESPONSE)
        .clamp(MIN_RESPONSE, MAX_RESPONSE)
}

struct CandidatePipelineContext<'a> {
    registry: &'a mut CandidateRegistry,
    provider: Arc<dyn crate::api::EvidenceProvider>,
    executor: &'a CpuExecutor,
    writer: Arc<ContractWriter>,
    aggregate: &'a mut AggregateState,
    artifact_index: &'a mut ArtifactIndex,
    config: &'a RunConfig,
    progress: &'a Progress,
    identities: Arc<crate::resident::AnalysisIdentityStore>,
}

async fn process_candidates(
    context: CandidatePipelineContext<'_>,
    mut candidate_rx: tokio::sync::mpsc::Receiver<crate::pipeline::CandidateRelationsEvent>,
    dedup_task: tokio::task::JoinHandle<Result<DedupOutput>>,
) -> Result<(DedupOutput, Vec<crate::reporting::scope::ScopeRelation>)> {
    let CandidatePipelineContext {
        registry,
        provider,
        executor,
        writer,
        aggregate,
        artifact_index,
        config,
        progress,
        identities,
    } = context;
    let mut final_fetches: VecDeque<CandidateRecord> = VecDeque::new();
    let mut final_bases = BTreeMap::<CandidateId, (EvidenceBundle, bool)>::new();
    let mut prefetch = PrefetchCoordinator::default();
    let mut candidate_stream_open = true;
    let mut all_relations = Vec::<crate::reporting::scope::ScopeRelation>::new();
    let network_limit = config.network_queue_capacity.min(
        config
            .provider_concurrency
            .alchemy
            .saturating_add(config.provider_concurrency.helius)
            .saturating_add(config.provider_concurrency.other),
    );
    let mut network = tokio::task::JoinSet::new();
    let (completion_tx, mut completion_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut active_analyses = BTreeSet::new();
    let mut active_compressions = BTreeSet::new();
    let mut compression_ready: VecDeque<AnalyzedCandidate> = VecDeque::new();
    let mut admission_ready: VecDeque<PreparedCandidate> = VecDeque::new();
    let mut write_ready: VecDeque<ProcessedCandidate> = VecDeque::new();
    let mut writes = tokio::task::JoinSet::new();
    let mut memory_usage = None;
    let mut telemetry = tokio::time::interval(Duration::from_secs(1));
    telemetry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while candidate_stream_open
        || !final_fetches.is_empty()
        || prefetch.has_queued()
        || !network.is_empty()
        || !active_analyses.is_empty()
        || !active_compressions.is_empty()
        || !compression_ready.is_empty()
        || !admission_ready.is_empty()
        || !write_ready.is_empty()
        || !writes.is_empty()
    {
        let memory_pressure = memory_usage.is_some_and(|bytes| bytes > config.memory_limit);
        if memory_pressure
            && network.is_empty()
            && active_analyses.is_empty()
            && active_compressions.is_empty()
            && compression_ready.is_empty()
            && admission_ready.is_empty()
            && write_ready.is_empty()
            && writes.is_empty()
        {
            return Err(AnalysisError::MemoryBudget {
                required: memory_usage.unwrap_or(config.memory_limit),
                limit: config.memory_limit,
            });
        }
        let mut active_candidates = network.len()
            + active_analyses.len()
            + active_compressions.len()
            + compression_ready.len()
            + admission_ready.len()
            + write_ready.len()
            + writes.len();
        while !memory_pressure
            && network.len() < network_limit
            && active_candidates < config.analysis_queue_capacity
        {
            let (candidate, fetch_kind, base) = if let Some(candidate) = final_fetches.pop_front() {
                let base = final_bases.remove(&candidate.id);
                (candidate, FetchKind::Final, base)
            } else if prefetch.reserved_network_slots() < network_limit {
                let Some(candidate) = prefetch.pop_queued() else {
                    break;
                };
                (candidate, FetchKind::Prefetch, None)
            } else {
                break;
            };
            let provider = provider.clone();
            let observed_at = config.analysis_timestamp.timestamp();
            network.spawn(async move {
                let selection = candidate_selection(&candidate.relations);
                let fetched = std::panic::AssertUnwindSafe(fetch_or_extend(
                    provider.as_ref(),
                    &candidate,
                    &selection,
                    base,
                    observed_at,
                ))
                .catch_unwind()
                .await;
                let (evidence, succeeded) = match fetched {
                    Ok(Ok(bundle)) if bundle.candidate == candidate.key => (bundle, true),
                    Ok(Ok(bundle)) => (
                        failed_evidence(
                            &candidate,
                            AnalysisError::Provider(format!(
                                "evidence identity mismatch: expected {:?}, received {:?}",
                                candidate.key, bundle.candidate
                            )),
                            observed_at,
                        ),
                        false,
                    ),
                    Ok(Err(error)) => (failed_evidence(&candidate, error, observed_at), false),
                    Err(payload) => (
                        failed_evidence(
                            &candidate,
                            AnalysisError::Provider(format!(
                                "network evidence task panicked: {}",
                                panic_message(payload)
                            )),
                            observed_at,
                        ),
                        false,
                    ),
                };
                NetworkCompletion {
                    candidate,
                    evidence,
                    succeeded,
                    fetch_kind,
                }
            });
            active_candidates += 1;
        }

        while active_compressions.len() + admission_ready.len() < config.compression_concurrency {
            let Some(analyzed) = compression_ready.pop_front() else {
                break;
            };
            let candidate_id = analyzed.candidate.id;
            let writer_for_cpu = writer.clone();
            if !active_compressions.insert(candidate_id) {
                return Err(AnalysisError::State(format!(
                    "candidate {} was queued for compression twice",
                    candidate_id.0
                )));
            }
            executor.submit_with_notification_kind_routed(
                crate::pipeline::CpuTaskKind::Compress,
                candidate_id,
                completion_tx.clone(),
                move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        compress_candidate(analyzed, &writer_for_cpu)
                    }))
                    .unwrap_or_else(|payload| {
                        Err(AnalysisError::Analysis(format!(
                            "candidate compression panicked: {}",
                            panic_message(payload)
                        )))
                    });
                    CpuCompletion::Compression(candidate_id, Box::new(result))
                },
            );
        }

        let pending_admissions = admission_ready.len();
        for _ in 0..pending_admissions {
            let prepared = admission_ready
                .pop_front()
                .expect("pending admission count is stable");
            match admit_prepared(&writer, prepared)? {
                AdmissionResult::Admitted(processed) => write_ready.push_back(processed),
                AdmissionResult::Pending(prepared) => admission_ready.push_back(prepared),
            }
        }

        while writes.len() < config.writer_threads {
            let Some(processed) = write_ready.pop_front() else {
                break;
            };
            let writer = writer.clone();
            writes.spawn_blocking(move || persist_candidate(&writer, processed));
        }

        let completion = tokio::select! {
            batch = candidate_rx.recv(),
                if candidate_stream_open
                    && !memory_pressure
                    && final_fetches.len() < config.network_queue_capacity =>
            {
                Some(WorkEvent::CandidateBatch(batch))
            }
            completion = completion_rx.recv(),
                if !active_analyses.is_empty() || !active_compressions.is_empty() =>
            {
                completion.map(WorkEvent::Cpu)
            }
            joined = network.join_next(), if !network.is_empty() => {
                joined.map(WorkEvent::Network)
            }
            joined = writes.join_next(), if !writes.is_empty() => {
                joined.map(WorkEvent::Writer)
            }
            _ = telemetry.tick() => Some(WorkEvent::Telemetry),
        }
        .ok_or_else(|| {
            AnalysisError::State("candidate pipeline ended without completion".into())
        })?;
        match completion {
            WorkEvent::CandidateBatch(Some(
                crate::pipeline::CandidateRelationsEvent::Prefetch(relations),
            )) => {
                for candidate in prefetch_records(relations)? {
                    // Prefetch both `WholeCollection` (AllInContract) and
                    // `ExplicitNftSet` (Explicit) selections: URI-only hits
                    // still benefit from early deployer/authority/market
                    // evidence per REWRITE_ARCHITECTURE §8.4, they simply
                    // upgrade to a full fetch plan once matches freeze.
                    if !prefetch.offer(candidate, config.network_queue_capacity)? {
                        // Bounded by `network_queue_capacity`; record the skip
                        // instead of silently dropping the candidate forever so
                        // operators can see prefetch pressure in progress output.
                        progress.add_prefetch_skipped();
                    }
                }
            }
            WorkEvent::CandidateBatch(Some(crate::pipeline::CandidateRelationsEvent::Frozen(
                relations,
            ))) => {
                all_relations.extend(
                    relations
                        .iter()
                        .map(crate::reporting::scope::ScopeRelation::from),
                );
                for candidate in registry.insert_frozen_relations(relations)? {
                    match prefetch.freeze(candidate) {
                        FrozenPrefetch::Complete { candidate, result } => {
                            if selection_covers(
                                &result.selection,
                                &candidate_selection(&candidate.relations),
                            ) {
                                let final_seeds = relation_seed_ids(&candidate.relations);
                                if prefetch_can_finalize(
                                    result.succeeded,
                                    &result.selection,
                                    &candidate_selection(&candidate.relations),
                                    &result.seeds,
                                    &final_seeds,
                                ) {
                                    queue_candidate_analysis(
                                        AnalysisQueueContext {
                                            executor,
                                            progress,
                                            identities: identities.clone(),
                                            timestamp: config.analysis_timestamp.timestamp(),
                                            completion_tx: &completion_tx,
                                            active_analyses: &mut active_analyses,
                                        },
                                        candidate,
                                        result.evidence,
                                        result.succeeded,
                                    )?;
                                } else if result.succeeded {
                                    final_bases
                                        .insert(candidate.id, (result.evidence, result.succeeded));
                                    final_fetches.push_back(candidate);
                                } else {
                                    final_fetches.push_back(candidate);
                                }
                            } else {
                                final_fetches.push_back(candidate);
                            }
                        }
                        FrozenPrefetch::Active => {}
                        FrozenPrefetch::Final(candidate) => {
                            final_fetches.push_back(candidate);
                        }
                    }
                }
            }
            WorkEvent::CandidateBatch(None) => {
                candidate_stream_open = false;
                executor.set_owner_shards_open(false);
            }
            WorkEvent::Network(joined) => {
                let completion =
                    joined.map_err(|error| AnalysisError::Analysis(error.to_string()))?;
                match completion.fetch_kind {
                    FetchKind::Final => queue_candidate_analysis(
                        AnalysisQueueContext {
                            executor,
                            progress,
                            identities: identities.clone(),
                            timestamp: config.analysis_timestamp.timestamp(),
                            completion_tx: &completion_tx,
                            active_analyses: &mut active_analyses,
                        },
                        completion.candidate,
                        completion.evidence,
                        completion.succeeded,
                    )?,
                    FetchKind::Prefetch => {
                        if let PrefetchFinish::Frozen { candidate, result } =
                            prefetch.finish(completion)?
                        {
                            if selection_covers(
                                &result.selection,
                                &candidate_selection(&candidate.relations),
                            ) {
                                let final_seeds = relation_seed_ids(&candidate.relations);
                                if prefetch_can_finalize(
                                    result.succeeded,
                                    &result.selection,
                                    &candidate_selection(&candidate.relations),
                                    &result.seeds,
                                    &final_seeds,
                                ) {
                                    queue_candidate_analysis(
                                        AnalysisQueueContext {
                                            executor,
                                            progress,
                                            identities: identities.clone(),
                                            timestamp: config.analysis_timestamp.timestamp(),
                                            completion_tx: &completion_tx,
                                            active_analyses: &mut active_analyses,
                                        },
                                        candidate,
                                        result.evidence,
                                        result.succeeded,
                                    )?;
                                } else if result.succeeded {
                                    final_bases
                                        .insert(candidate.id, (result.evidence, result.succeeded));
                                    final_fetches.push_back(candidate);
                                } else {
                                    final_fetches.push_back(candidate);
                                }
                            } else {
                                final_fetches.push_back(candidate);
                            }
                        }
                    }
                }
            }
            WorkEvent::Cpu(CpuCompletion::Analysis(candidate_id, analyzed)) => {
                if !active_analyses.remove(&candidate_id) {
                    return Err(AnalysisError::State("unknown analysis completion".into()));
                }
                let analyzed = (*analyzed)?;
                let analysis_succeeded = analyzed.analysis_error.is_none();
                progress.add_analyzed(analysis_succeeded);
                compression_ready.push_back(analyzed);
            }
            WorkEvent::Cpu(CpuCompletion::Compression(candidate_id, processed)) => {
                if !active_compressions.remove(&candidate_id) {
                    return Err(AnalysisError::State(
                        "unknown compression completion".into(),
                    ));
                }
                let mut prepared = (*processed)?;
                if let Some(delta) = prepared.aggregate.take() {
                    aggregate.merge_once(*delta)?;
                }
                admission_ready.push_back(prepared);
            }
            WorkEvent::Writer(joined) => {
                let persisted =
                    joined.map_err(|error| AnalysisError::Analysis(error.to_string()))??;
                progress.add_written();
                artifact_index.push(persisted.artifact);
            }
            WorkEvent::Telemetry => {
                memory_usage = crate::platform::current_memory_usage()?;
                if let Some(bytes) = memory_usage {
                    progress.record_memory(bytes);
                }
                let (cpu_active, cpu_queued) = executor.utilization();
                progress.record_cpu(executor.workers(), cpu_active, cpu_queued);
                progress.record_pipeline_queues(
                    final_fetches.len() + prefetch.queued_len(),
                    network.len(),
                    active_analyses.len(),
                    compression_ready.len() + admission_ready.len(),
                    active_compressions.len(),
                    write_ready.len(),
                    writes.len(),
                    0,
                );
            }
        }
    }
    let dedup = dedup_task
        .await
        .map_err(|error| AnalysisError::Analysis(error.to_string()))??;
    progress.record_pipeline_queues(0, 0, 0, 0, 0, 0, 0, 0);
    let (cpu_active, cpu_queued) = executor.utilization();
    progress.record_cpu(executor.workers(), cpu_active, cpu_queued);
    Ok((dedup, all_relations))
}

enum CpuCompletion {
    Analysis(CandidateId, Box<Result<AnalyzedCandidate>>),
    Compression(CandidateId, Box<Result<PreparedCandidate>>),
}

#[derive(Clone, Copy)]
enum FetchKind {
    Prefetch,
    Final,
}

struct NetworkCompletion {
    candidate: CandidateRecord,
    evidence: EvidenceBundle,
    succeeded: bool,
    fetch_kind: FetchKind,
}

struct PrefetchResult {
    evidence: EvidenceBundle,
    succeeded: bool,
    selection: NftSelection,
    seeds: BTreeSet<crate::model::SeedId>,
}

#[derive(Default)]
struct PrefetchCoordinator {
    order: VecDeque<CandidateId>,
    entries: BTreeMap<CandidateId, PrefetchEntry>,
}

struct PrefetchEntry {
    relations: Arc<Vec<crate::model::SeedCandidateRelation>>,
    stage: PrefetchStage,
}

enum PrefetchStage {
    Queued(CandidateRecord),
    Active {
        upgrade: Option<CandidateRecord>,
        frozen: Option<CandidateRecord>,
    },
    Complete(Box<PrefetchResult>),
}

enum FrozenPrefetch {
    Complete {
        candidate: CandidateRecord,
        result: Box<PrefetchResult>,
    },
    Active,
    Final(CandidateRecord),
}

enum PrefetchFinish {
    Deferred,
    Frozen {
        candidate: CandidateRecord,
        result: Box<PrefetchResult>,
    },
}

impl PrefetchCoordinator {
    fn offer(&mut self, candidate: CandidateRecord, capacity: usize) -> Result<bool> {
        let candidate_id = candidate.id;
        if let Some(mut entry) = self.entries.remove(&candidate_id) {
            if matches!(
                &entry.stage,
                PrefetchStage::Active {
                    frozen: Some(_),
                    ..
                }
            ) {
                // A frozen record is authoritative. A late hint cannot expand
                // its final match set and therefore has no useful work to add.
                self.entries.insert(candidate_id, entry);
                return Ok(true);
            }
            let merged = merge_prefetch_relations(&entry.relations, &candidate.relations);
            let upgraded = prefetch_records(merged)?
                .into_iter()
                .next()
                .expect("merged prefetch contains one candidate");
            entry.relations = upgraded.relations.clone();
            entry.stage = match entry.stage {
                PrefetchStage::Queued(_) => PrefetchStage::Queued(upgraded),
                PrefetchStage::Active { frozen, .. } => PrefetchStage::Active {
                    upgrade: Some(upgraded),
                    frozen,
                },
                PrefetchStage::Complete(result) => {
                    if selection_covers(
                        &result.selection,
                        &candidate_selection(&upgraded.relations),
                    ) {
                        PrefetchStage::Complete(result)
                    } else {
                        self.order.push_front(candidate_id);
                        PrefetchStage::Queued(upgraded)
                    }
                }
            };
            self.entries.insert(candidate_id, entry);
            return Ok(true);
        }

        if self.plan_count() >= capacity {
            return Ok(false);
        }
        self.order.push_back(candidate_id);
        self.entries.insert(
            candidate_id,
            PrefetchEntry {
                relations: candidate.relations.clone(),
                stage: PrefetchStage::Queued(candidate),
            },
        );
        Ok(true)
    }

    fn pop_queued(&mut self) -> Option<CandidateRecord> {
        while let Some(candidate_id) = self.order.pop_front() {
            let Some(entry) = self.entries.get_mut(&candidate_id) else {
                continue;
            };
            let stage = std::mem::replace(
                &mut entry.stage,
                PrefetchStage::Active {
                    upgrade: None,
                    frozen: None,
                },
            );
            match stage {
                PrefetchStage::Queued(candidate) => return Some(candidate),
                other => entry.stage = other,
            }
        }
        None
    }

    fn freeze(&mut self, candidate: CandidateRecord) -> FrozenPrefetch {
        let candidate_id = candidate.id;
        let Some(mut entry) = self.entries.remove(&candidate_id) else {
            return FrozenPrefetch::Final(candidate);
        };
        match entry.stage {
            PrefetchStage::Complete(result) => FrozenPrefetch::Complete { candidate, result },
            PrefetchStage::Queued(_) => FrozenPrefetch::Final(candidate),
            PrefetchStage::Active { .. } => {
                entry.stage = PrefetchStage::Active {
                    upgrade: None,
                    frozen: Some(candidate),
                };
                self.entries.insert(candidate_id, entry);
                FrozenPrefetch::Active
            }
        }
    }

    fn finish(&mut self, completion: NetworkCompletion) -> Result<PrefetchFinish> {
        let candidate_id = completion.candidate.id;
        let selection = candidate_selection(&completion.candidate.relations);
        let seeds = relation_seed_ids(&completion.candidate.relations);
        let result = PrefetchResult {
            evidence: completion.evidence,
            succeeded: completion.succeeded,
            selection,
            seeds,
        };
        let Some(mut entry) = self.entries.remove(&candidate_id) else {
            return Ok(PrefetchFinish::Deferred);
        };
        let PrefetchStage::Active { upgrade, frozen } = entry.stage else {
            return Err(AnalysisError::State(format!(
                "candidate {} completed prefetch outside the active stage",
                candidate_id.0
            )));
        };
        if let Some(candidate) = frozen {
            return Ok(PrefetchFinish::Frozen {
                candidate,
                result: Box::new(result),
            });
        }
        if let Some(upgrade) = upgrade {
            if selection_covers(&result.selection, &candidate_selection(&upgrade.relations)) {
                entry.stage = PrefetchStage::Complete(Box::new(result));
            } else {
                entry.stage = PrefetchStage::Queued(upgrade);
                self.order.push_front(candidate_id);
            }
        } else {
            entry.stage = PrefetchStage::Complete(Box::new(result));
        }
        self.entries.insert(candidate_id, entry);
        Ok(PrefetchFinish::Deferred)
    }

    fn has_queued(&self) -> bool {
        self.entries
            .values()
            .any(|entry| matches!(entry.stage, PrefetchStage::Queued(_)))
    }

    fn queued_len(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(entry.stage, PrefetchStage::Queued(_)))
            .count()
    }

    fn reserved_network_slots(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| {
                matches!(
                    entry.stage,
                    PrefetchStage::Active { .. } | PrefetchStage::Complete(_)
                )
            })
            .count()
    }

    fn plan_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| {
                !matches!(
                    entry.stage,
                    PrefetchStage::Active {
                        frozen: Some(_),
                        ..
                    }
                )
            })
            .count()
    }
}

// Keeping the completion inline avoids one heap allocation for every network
// result; the short-lived coordinator stack slot is deliberately larger.
#[allow(clippy::large_enum_variant)]
enum WorkEvent {
    CandidateBatch(Option<crate::pipeline::CandidateRelationsEvent>),
    Network(std::result::Result<NetworkCompletion, tokio::task::JoinError>),
    Cpu(CpuCompletion),
    Writer(std::result::Result<Result<PersistedCandidate>, tokio::task::JoinError>),
    Telemetry,
}

fn prefetch_records(
    relations: Vec<crate::model::SeedCandidateRelation>,
) -> Result<Vec<CandidateRecord>> {
    let mut grouped = BTreeMap::<CandidateId, Vec<crate::model::SeedCandidateRelation>>::new();
    for relation in relations {
        grouped
            .entry(relation.candidate_id)
            .or_default()
            .push(relation);
    }
    grouped
        .into_iter()
        .map(|(id, mut relations)| {
            relations.sort_by_key(|relation| relation.seed_id);
            let key = relations
                .first()
                .ok_or_else(|| AnalysisError::State("empty prefetch relation group".into()))?
                .candidate
                .clone();
            Ok(CandidateRecord {
                id,
                key,
                relations: Arc::new(relations),
            })
        })
        .collect()
}

fn relation_seed_ids(
    relations: &[crate::model::SeedCandidateRelation],
) -> BTreeSet<crate::model::SeedId> {
    relations.iter().map(|relation| relation.seed_id).collect()
}

fn merge_prefetch_relations(
    left: &[crate::model::SeedCandidateRelation],
    right: &[crate::model::SeedCandidateRelation],
) -> Vec<crate::model::SeedCandidateRelation> {
    let mut merged = BTreeMap::<crate::model::SeedId, crate::model::SeedCandidateRelation>::new();
    for relation in left.iter().chain(right).cloned() {
        merged
            .entry(relation.seed_id)
            .and_modify(|existing| {
                existing.dimensions |= relation.dimensions;
                existing.selection.union_assign(relation.selection.clone());
                existing.evidence.extend(relation.evidence.clone());
                existing.incomplete |= relation.incomplete;
            })
            .or_insert(relation);
    }
    for relation in merged.values_mut() {
        relation.selection.normalize();
        relation
            .evidence
            .sort_by(crate::dedup::reducer::compare_evidence);
        relation.evidence.dedup();
    }
    merged.into_values().collect()
}

fn selection_covers(prefetched: &NftSelection, required: &NftSelection) -> bool {
    match (prefetched, required) {
        (
            NftSelection::AllInContract {
                contract: prefetched,
                nft_count: prefetched_count,
            },
            NftSelection::AllInContract {
                contract: required,
                nft_count: required_count,
            },
        ) => prefetched == required && prefetched_count >= required_count,
        (NftSelection::AllInContract { contract, .. }, NftSelection::Explicit { nfts }) => {
            nfts.iter().all(|nft| &nft.contract_key() == contract)
        }
        (
            NftSelection::Explicit { nfts: prefetched },
            NftSelection::Explicit { nfts: required },
        ) => {
            let prefetched = prefetched.iter().collect::<BTreeSet<_>>();
            required.iter().all(|nft| prefetched.contains(nft))
        }
        (NftSelection::Explicit { .. }, NftSelection::AllInContract { .. }) => false,
    }
}

fn prefetch_can_finalize(
    succeeded: bool,
    prefetched_selection: &NftSelection,
    required_selection: &NftSelection,
    prefetched_seeds: &BTreeSet<crate::model::SeedId>,
    required_seeds: &BTreeSet<crate::model::SeedId>,
) -> bool {
    succeeded
        && selection_covers(prefetched_selection, required_selection)
        && required_seeds.is_subset(prefetched_seeds)
}

struct AnalysisQueueContext<'a> {
    executor: &'a CpuExecutor,
    progress: &'a Progress,
    identities: Arc<crate::resident::AnalysisIdentityStore>,
    timestamp: i64,
    completion_tx: &'a tokio::sync::mpsc::UnboundedSender<CpuCompletion>,
    active_analyses: &'a mut BTreeSet<CandidateId>,
}

fn queue_candidate_analysis(
    context: AnalysisQueueContext<'_>,
    candidate: CandidateRecord,
    evidence: EvidenceBundle,
    fetch_succeeded: bool,
) -> Result<()> {
    let AnalysisQueueContext {
        executor,
        progress,
        identities,
        timestamp,
        completion_tx,
        active_analyses,
    } = context;
    if !active_analyses.insert(candidate.id) {
        return Err(AnalysisError::State(format!(
            "candidate {} was queued for analysis twice",
            candidate.id.0
        )));
    }
    let truncated = evidence_has_status(&evidence, crate::model::EvidenceStatus::Truncated);
    progress.add_fetched(fetch_succeeded, truncated);
    let candidate_id = candidate.id;
    executor.submit_with_notification_kind_routed(
        crate::pipeline::CpuTaskKind::Analysis,
        candidate_id,
        completion_tx.clone(),
        move || {
            CpuCompletion::Analysis(
                candidate_id,
                Box::new(analyze_candidate_work(
                    candidate,
                    evidence,
                    timestamp,
                    &identities,
                )),
            )
        },
    );
    Ok(())
}

async fn fetch_or_extend(
    provider: &dyn crate::api::EvidenceProvider,
    candidate: &CandidateRecord,
    selection: &NftSelection,
    base: Option<(EvidenceBundle, bool)>,
    observed_at: i64,
) -> Result<EvidenceBundle> {
    let Some((mut evidence, _)) = base else {
        return fetch_with_retry(provider, &candidate.key, selection, &candidate.relations, 0)
            .await;
    };
    match provider
        .fetch_relation_verifications(&evidence, &candidate.relations)
        .await
    {
        Ok(mut verifications) => {
            verifications.sort_by_key(|verification| verification.seed_id);
            verifications.dedup_by_key(|verification| verification.seed_id);
            evidence.relation_verifications = verifications;
        }
        Err(error) => {
            let failure = error.to_string();
            evidence.relation_verifications = candidate
                .relations
                .iter()
                .map(|relation| crate::model::RelationVerification {
                    seed_id: relation.seed_id,
                    official_controller_continuity: false,
                    authorized_reissue: false,
                    verified_migration: false,
                    official_collection_relation: false,
                    complete: false,
                    evidence_keys: Vec::new(),
                    failures: vec![failure.clone()],
                })
                .collect();
            evidence
                .quality
                .failures
                .push(format!("relation_verification: {failure}"));
            evidence.provenance.push(EvidenceObservation {
                source: "relation_provider".into(),
                request_key: "relation_verification".into(),
                observed_at,
                status: EvidenceStatus::Failed,
            });
            evidence.quality.supplemental_query_failures = evidence
                .quality
                .supplemental_query_failures
                .saturating_add(1);
        }
    }
    Ok(evidence)
}

fn failed_evidence(
    candidate: &CandidateRecord,
    error: AnalysisError,
    observed_at: i64,
) -> EvidenceBundle {
    EvidenceBundle {
        candidate: candidate.key.clone(),
        deployment_timestamp: None,
        duplicate_content_timestamp: None,
        events: Vec::new(),
        holders: Vec::new(),
        controllers: Vec::new(),
        relation_verifications: Vec::new(),
        provenance: vec![EvidenceObservation {
            source: "pipeline".into(),
            request_key: "candidate_evidence".into(),
            observed_at,
            status: EvidenceStatus::Failed,
        }],
        quality: EvidenceQuality {
            assets: Some(EvidenceStatus::Failed),
            histories: Some(EvidenceStatus::Failed),
            transactions: Some(EvidenceStatus::Failed),
            prices: Some(EvidenceStatus::Failed),
            authority: Some(EvidenceStatus::Failed),
            supplemental_query_failures: 1,
            failures: vec![error.to_string()],
            ..Default::default()
        },
    }
}

struct ProcessedCandidate {
    candidate_id: CandidateId,
    candidate: crate::model::ContractKey,
    payload: crate::reporting::ReservedPayload,
    analysis_status: &'static str,
    summary: serde_json::Value,
}

struct PreparedCandidate {
    candidate_id: CandidateId,
    candidate: crate::model::ContractKey,
    payload: crate::reporting::PreparedPayload,
    analysis_status: &'static str,
    aggregate: Option<Box<AggregateDelta>>,
    summary: serde_json::Value,
}

struct AnalyzedCandidate {
    candidate: CandidateRecord,
    evidence: EvidenceBundle,
    labels: Vec<crate::model::RelationLabel>,
    facts: crate::model::CandidateFacts,
    deltas: Vec<crate::model::RelationDelta>,
    suspected_economics: crate::model::EconomicFacts,
    suspected_behaviors: crate::model::BehaviorFacts,
    behavior_entities: Vec<crate::model::BehaviorEntityDelta>,
    matrix_suspected: BTreeMap<
        (crate::model::ChainId, crate::model::ChainId),
        crate::model::ScopedSuspectedBundle,
    >,
    global_address_roles: Vec<(crate::model::GlobalAddressId, crate::model::AddressRoleKind)>,
    global_nft_ids: Vec<crate::model::GlobalNftId>,
    global_transaction_ids: Vec<crate::model::GlobalTxId>,
    analysis_error: Option<String>,
}

struct AnalysisParts {
    labels: Vec<crate::model::RelationLabel>,
    facts: crate::model::CandidateFacts,
    deltas: Vec<crate::model::RelationDelta>,
    suspected_economics: crate::model::EconomicFacts,
    suspected_behaviors: crate::model::BehaviorFacts,
    behavior_entities: Vec<crate::model::BehaviorEntityDelta>,
    matrix_suspected: BTreeMap<
        (crate::model::ChainId, crate::model::ChainId),
        crate::model::ScopedSuspectedBundle,
    >,
    global_address_roles: Vec<(crate::model::GlobalAddressId, crate::model::AddressRoleKind)>,
    global_nft_ids: Vec<crate::model::GlobalNftId>,
    global_transaction_ids: Vec<crate::model::GlobalTxId>,
}

fn analyze_candidate_work(
    candidate: CandidateRecord,
    mut evidence: EvidenceBundle,
    analysis_timestamp: i64,
    identities: &crate::resident::AnalysisIdentityStore,
) -> Result<AnalyzedCandidate> {
    let mut frozen_labels = None;
    let attempted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        analyze_candidate_core(
            &candidate,
            &mut evidence,
            analysis_timestamp,
            identities,
            &mut frozen_labels,
        )
    }));
    let (parts, analysis_error) = match attempted {
        Ok(Ok(parts)) => (parts, None),
        Ok(Err(error)) => {
            let message = error.to_string();
            (
                failed_analysis_parts(
                    &candidate,
                    &mut evidence,
                    &message,
                    analysis_timestamp,
                    frozen_labels.take(),
                ),
                Some(message),
            )
        }
        Err(payload) => {
            let message = format!("candidate analysis panicked: {}", panic_message(payload));
            (
                failed_analysis_parts(
                    &candidate,
                    &mut evidence,
                    &message,
                    analysis_timestamp,
                    frozen_labels.take(),
                ),
                Some(message),
            )
        }
    };
    let analyzed = AnalyzedCandidate {
        candidate,
        evidence,
        labels: parts.labels,
        facts: parts.facts,
        deltas: parts.deltas,
        suspected_economics: parts.suspected_economics,
        suspected_behaviors: parts.suspected_behaviors,
        behavior_entities: parts.behavior_entities,
        matrix_suspected: parts.matrix_suspected,
        global_address_roles: parts.global_address_roles,
        global_nft_ids: parts.global_nft_ids,
        global_transaction_ids: parts.global_transaction_ids,
        analysis_error,
    };
    Ok(analyzed)
}

fn analyze_candidate_core(
    candidate: &CandidateRecord,
    evidence: &mut EvidenceBundle,
    analysis_timestamp: i64,
    identities: &crate::resident::AnalysisIdentityStore,
    frozen_labels: &mut Option<Vec<crate::model::RelationLabel>>,
) -> Result<AnalysisParts> {
    if evidence.candidate != candidate.key {
        return Err(AnalysisError::Provider(format!(
            "evidence identity mismatch: expected {:?}, received {:?}",
            candidate.key, evidence.candidate
        )));
    }
    normalize_evidence(evidence)?;
    let relation_seeds = candidate
        .relations
        .iter()
        .map(|relation| relation.seed_id)
        .collect::<BTreeSet<_>>();
    if evidence.relation_verifications.iter().any(|verification| {
        !relation_seeds.contains(&verification.seed_id)
            || ((verification.official_controller_continuity
                || verification.authorized_reissue
                || verification.verified_migration
                || verification.official_collection_relation)
                && verification.evidence_keys.is_empty())
    }) {
        return Err(AnalysisError::Provider(
            "relation verification is unrelated or lacks self-contained evidence".into(),
        ));
    }
    let mut addresses = evidence
        .events
        .iter()
        .flat_map(|event| {
            [
                event
                    .from
                    .as_ref()
                    .map(|address| (event.chain, address.clone())),
                event
                    .to
                    .as_ref()
                    .map(|address| (event.chain, address.clone())),
                event
                    .fee_payer
                    .as_ref()
                    .map(|address| (event.chain, address.clone())),
                event
                    .payment_payer
                    .as_ref()
                    .map(|address| (event.chain, address.clone())),
                event
                    .payment_recipient
                    .as_ref()
                    .map(|address| (event.chain, address.clone())),
            ]
        })
        .flatten()
        .chain(
            evidence
                .controllers
                .iter()
                .map(|address| (candidate.key.chain, address.clone())),
        )
        .chain(
            evidence
                .holders
                .iter()
                .map(|(nft, address)| (nft.chain, address.clone())),
        )
        .collect::<Vec<_>>();
    let mut transactions = evidence
        .events
        .iter()
        .map(|event| (event.chain, event.tx_id.clone()))
        .collect::<Vec<_>>();
    let mut nfts = evidence
        .events
        .iter()
        .filter_map(|event| event.nft.clone())
        .chain(evidence.holders.iter().map(|(nft, _)| nft.clone()))
        .collect::<Vec<_>>();
    let interned = identities.intern_batch(&mut addresses, &mut transactions, &mut nfts)?;
    let labels = legit_duplicate::classify_relations(candidate.id, &candidate.relations, evidence);
    *frozen_labels = Some(labels.clone());
    let facts = analyze_candidate(evidence, analysis_timestamp);
    let projection = relation_projector::project_relations(
        candidate.id,
        &facts,
        &candidate.relations,
        &labels,
        Some(evidence),
    );
    let address_roles = &projection.suspected_address_roles;
    let global_address_roles = interned
        .addresses
        .iter()
        .filter_map(|((_, address), id)| address_roles.get(address).map(|role| (*id, *role)))
        .collect();
    let address_ids = interned
        .addresses
        .iter()
        .map(|((_, address), id)| (address.as_ref(), *id))
        .collect::<BTreeMap<_, _>>();
    let nft_ids = interned
        .nfts
        .iter()
        .map(|(nft, id)| (nft, *id))
        .collect::<BTreeMap<_, _>>();
    let behavior_entities = intern_behavior_entities(
        &projection.suspected_behavior_instances,
        &address_ids,
        &nft_ids,
    )?;
    let mut matrix_suspected = BTreeMap::new();
    for (key, projected) in projection.matrix_suspected {
        let address_roles = interned
            .addresses
            .iter()
            .filter_map(|((_, address), id)| {
                projected
                    .address_roles
                    .get(address)
                    .map(|role| (*id, *role))
            })
            .collect();
        matrix_suspected.insert(
            key,
            crate::model::ScopedSuspectedBundle {
                economics: projected.economics,
                behaviors: projected.behaviors,
                behavior_entities: intern_behavior_entities(
                    &projected.behavior_instances,
                    &address_ids,
                    &nft_ids,
                )?,
                address_roles,
            },
        );
    }
    Ok(AnalysisParts {
        labels,
        facts,
        deltas: projection.deltas,
        suspected_economics: projection.suspected_economics,
        suspected_behaviors: projection.suspected_behaviors,
        behavior_entities,
        matrix_suspected,
        global_address_roles,
        global_nft_ids: interned.nfts.into_iter().map(|(_, id)| id).collect(),
        global_transaction_ids: interned
            .transactions
            .into_iter()
            .map(|(_, id)| id)
            .collect(),
    })
}

fn failed_analysis_parts(
    candidate: &CandidateRecord,
    evidence: &mut EvidenceBundle,
    message: &str,
    analysis_timestamp: i64,
    frozen_labels: Option<Vec<crate::model::RelationLabel>>,
) -> AnalysisParts {
    evidence.quality.failures.push(message.to_owned());
    let labels = frozen_labels.unwrap_or_else(|| {
        let verifications = std::mem::take(&mut evidence.relation_verifications);
        let labels =
            legit_duplicate::classify_relations(candidate.id, &candidate.relations, evidence);
        evidence.relation_verifications = verifications;
        labels
    });
    let facts = crate::model::CandidateFacts {
        candidate: candidate.key.clone(),
        analysis_timestamp,
        lifecycle: Default::default(),
        propagation: Default::default(),
        economics: Default::default(),
        gas_cost_records: Vec::new(),
        behaviors: Default::default(),
        behavior_instances: Vec::new(),
        address_count: 0,
        nft_count: 0,
        transaction_count: 0,
        event_count: 0,
        event_kind_counts: Default::default(),
        address_attributions: Default::default(),
        honest_buyers: Vec::new(),
        quality: evidence.quality.clone(),
    };
    let projection = relation_projector::project_relations(
        candidate.id,
        &facts,
        &candidate.relations,
        &labels,
        None,
    );
    AnalysisParts {
        labels,
        facts,
        deltas: projection.deltas,
        suspected_economics: projection.suspected_economics,
        suspected_behaviors: projection.suspected_behaviors,
        behavior_entities: Vec::new(),
        matrix_suspected: BTreeMap::new(),
        global_address_roles: Vec::new(),
        global_nft_ids: Vec::new(),
        global_transaction_ids: Vec::new(),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

fn compress_candidate(
    analyzed: AnalyzedCandidate,
    writer: &ContractWriter,
) -> Result<PreparedCandidate> {
    let AnalyzedCandidate {
        candidate,
        evidence,
        labels,
        facts,
        deltas,
        suspected_economics,
        suspected_behaviors,
        behavior_entities,
        matrix_suspected,
        global_address_roles,
        global_nft_ids,
        global_transaction_ids,
        analysis_error,
    } = analyzed;
    let analysis_complete = analysis_error.is_none() && !evidence_incomplete(&facts.quality);
    let artifact = ContractArtifact {
        candidate: &candidate.key,
        matches: &candidate.relations,
        relation_labels: &labels,
        evidence: &evidence,
        facts: &facts,
        relation_deltas: &deltas,
        analysis_error: analysis_error.as_deref(),
    };
    let payload = writer.serialize_contract(&artifact)?;
    let analysis_status = if analysis_error.is_some() {
        "failed"
    } else if evidence_incomplete(&facts.quality) {
        "incomplete"
    } else {
        "analyzed"
    };
    let summary = serde_json::json!({
        "event_count": facts.event_count,
        "address_count": facts.address_count,
    });
    Ok(PreparedCandidate {
        candidate_id: candidate.id,
        candidate: candidate.key,
        payload,
        analysis_status,
        aggregate: (!deltas.is_empty()).then(|| {
            Box::new(AggregateDelta {
                candidate_id: candidate.id,
                analysis_complete,
                relation_deltas: deltas,
                suspected_economics,
                suspected_behaviors,
                behavior_entities,
                matrix_suspected,
                candidate_quality: facts.quality,
                global_address_roles,
                global_nft_ids,
                global_transaction_ids,
            })
        }),
        summary,
    })
}

enum AdmissionResult {
    Admitted(ProcessedCandidate),
    Pending(PreparedCandidate),
}

fn admit_prepared(writer: &ContractWriter, prepared: PreparedCandidate) -> Result<AdmissionResult> {
    let PreparedCandidate {
        candidate_id,
        candidate,
        payload,
        analysis_status,
        aggregate,
        summary,
    } = prepared;
    Ok(match writer.try_admit(payload)? {
        PayloadAdmission::Admitted(payload) => AdmissionResult::Admitted(ProcessedCandidate {
            candidate_id,
            candidate,
            payload,
            analysis_status,
            summary,
        }),
        PayloadAdmission::Pending(payload) => AdmissionResult::Pending(PreparedCandidate {
            candidate_id,
            candidate,
            payload,
            analysis_status,
            aggregate,
            summary,
        }),
    })
}

fn intern_behavior_entities(
    instances: &[crate::model::BehaviorInstance],
    address_ids: &BTreeMap<&str, crate::model::GlobalAddressId>,
    nft_ids: &BTreeMap<&crate::model::NftKey, crate::model::GlobalNftId>,
) -> Result<Vec<crate::model::BehaviorEntityDelta>> {
    instances
        .iter()
        .map(|instance| -> Result<_> {
            let mut addresses = instance
                .addresses
                .iter()
                .map(|address| {
                    address_ids.get(address.as_ref()).copied().ok_or_else(|| {
                        AnalysisError::State(format!(
                            "behavior address `{address}` was not globally interned"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let mut nfts = instance
                .nfts
                .iter()
                .map(|nft| {
                    nft_ids.get(nft).copied().ok_or_else(|| {
                        AnalysisError::State(format!(
                            "behavior NFT {:?} was not globally interned",
                            nft
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let mut linked_buyers = instance
                .linked_buyers
                .iter()
                .map(|address| {
                    address_ids.get(address.as_ref()).copied().ok_or_else(|| {
                        AnalysisError::State(format!(
                            "behavior buyer `{address}` was not globally interned"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            addresses.sort_unstable();
            addresses.dedup();
            nfts.sort_unstable();
            nfts.dedup();
            linked_buyers.sort_unstable();
            linked_buyers.dedup();
            Ok(crate::model::BehaviorEntityDelta {
                kind: instance.kind,
                addresses,
                nfts,
                linked_buyers,
                linked_loss_native: instance.linked_loss_native,
                linked_loss_usd_micros: instance.linked_loss_usd_micros,
            })
        })
        .collect()
}

struct PersistedCandidate {
    artifact: crate::reporting::ArtifactRef,
}

fn persist_candidate(
    writer: &ContractWriter,
    processed: ProcessedCandidate,
) -> Result<PersistedCandidate> {
    let artifact = writer.write(
        processed.candidate_id,
        processed.candidate,
        processed.payload,
        processed.analysis_status,
        processed.summary,
    )?;
    Ok(PersistedCandidate { artifact })
}

fn evidence_incomplete(quality: &EvidenceQuality) -> bool {
    quality.supplemental_query_failures > 0
        || [
            quality.assets,
            quality.histories,
            quality.transactions,
            quality.prices,
            quality.authority,
        ]
        .into_iter()
        .any(|status| {
            matches!(
                status,
                None | Some(
                    crate::model::EvidenceStatus::NotRequested
                        | crate::model::EvidenceStatus::Requested
                        | crate::model::EvidenceStatus::Failed
                        | crate::model::EvidenceStatus::Truncated
                )
            )
        })
}

fn evidence_has_status(evidence: &EvidenceBundle, expected: crate::model::EvidenceStatus) -> bool {
    [
        evidence.quality.assets,
        evidence.quality.histories,
        evidence.quality.transactions,
        evidence.quality.prices,
        evidence.quality.authority,
    ]
    .into_iter()
    .flatten()
    .any(|status| status == expected)
}

fn candidate_selection(relations: &[crate::model::SeedCandidateRelation]) -> NftSelection {
    let mut selection = relations[0].selection.clone();
    for relation in &relations[1..] {
        selection.union_assign(relation.selection.clone());
    }
    selection.normalize();
    selection
}

fn deterministic_run_id(config: &RunConfig, manifest: &SeedManifest) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(config)?);
    hasher.update([
        u8::from(!config.api_keys.alchemy.is_empty()),
        u8::from(!config.api_keys.etherscan.is_empty()),
        u8::from(!config.api_keys.opensea.is_empty()),
        u8::from(!config.api_keys.helius.is_empty()),
    ]);
    hasher.update(serde_json::to_vec(manifest)?);
    let digest = hasher.finalize();
    Ok(format!(
        "{}-{:02x}{:02x}{:02x}{:02x}",
        config.analysis_timestamp.format("%Y%m%dT%H%M%SZ"),
        digest[0],
        digest[1],
        digest[2],
        digest[3]
    ))
}

fn write_manifest(
    run_dir: &Path,
    run_id: &str,
    config: &RunConfig,
    platform: &crate::platform::PlatformResources,
) -> Result<()> {
    json::write_json(
        &run_dir.join("run_manifest.json"),
        &serde_json::json!({
            "run_id": run_id,
            "config": config,
            "pricing_policy": {
                "native_usd": "same_calendar_day_utc",
                "description": "Native-asset USD conversions use the Alchemy Prices same-UTC-calendar-day rate for each event timestamp (interval=1d historical series keyed by day bucket). Cross-chain aggregates sum only USD micros; per-chain native base units are retained separately and are never added across chains.",
                "provider": "alchemy_prices_tokens_historical",
                "day_bucket": "floor(unix_timestamp / 86400) * 86400",
            },
            "configured_providers": {
                "alchemy": !config.api_keys.alchemy.is_empty(),
                "etherscan": !config.api_keys.etherscan.is_empty(),
                "opensea": !config.api_keys.opensea.is_empty(),
                "helius": !config.api_keys.helius.is_empty(),
            },
            "platform": platform,
            "provider_response_limit_bytes": provider_response_limit(
                config.memory_limit,
                config.analysis_queue_capacity,
            ),
        }),
    )
}

struct FinalReportContext<'a> {
    run_dir: &'a Path,
    manifest: &'a SeedManifest,
    relations: &'a [crate::reporting::scope::ScopeRelation],
    artifacts: &'a mut ArtifactIndex,
    aggregate: &'a mut AggregateState,
    catalog: &'a crate::resident::ContractCatalog,
    input_quality: &'a crate::model::InputQuality,
    identities: &'a crate::resident::AnalysisIdentityStore,
    failed_seeds: &'a BTreeSet<crate::model::SeedId>,
}

fn write_final_reports(context: FinalReportContext<'_>) -> Result<()> {
    let FinalReportContext {
        run_dir,
        manifest,
        relations,
        artifacts,
        aggregate,
        catalog,
        input_quality,
        identities,
        failed_seeds,
    } = context;
    json::write_json(&run_dir.join("seed_audit.json"), manifest)?;
    let ordered_artifacts = artifacts.take_ordered();
    json::write_json_lines(
        &run_dir.join("contract_index.jsonl"),
        ordered_artifacts.iter(),
    )?;
    let mut artifact_by_candidate = AHashMap::with_capacity(ordered_artifacts.len());
    for artifact in &ordered_artifacts {
        artifact_by_candidate.insert(artifact.candidate_id, artifact);
    }
    let mut relations_by_seed =
        BTreeMap::<crate::model::SeedId, Vec<&crate::reporting::scope::ScopeRelation>>::new();
    for relation in relations {
        relations_by_seed
            .entry(relation.seed_id)
            .or_default()
            .push(relation);
    }
    for seed in &manifest.seeds {
        let seed_relations = relations_by_seed
            .get(&seed.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let filename = format!(
            "{:03}-{}",
            seed.id.0,
            safe_component(&seed.stable_identifier)
        );
        json::write_json(
            &run_dir.join("seeds").join(format!("{filename}.json")),
            &SeedReport {
                seed,
                relations: SeedRelations {
                    relations: seed_relations,
                    artifacts: &artifact_by_candidate,
                },
            },
        )?;
        write_seed_markdown(
            &run_dir.join("seeds").join(format!("{filename}.md")),
            seed,
            seed_relations,
            &artifact_by_candidate,
        )?;
    }
    let relation_suspected = aggregate.take_relation_suspected();
    let dedup_report = crate::reporting::scope::build_scope_report(
        manifest,
        catalog,
        relations,
        &relation_suspected,
        failed_seeds,
    );
    csv::write_scope_csvs(run_dir, &dedup_report)?;
    json::write_json(&run_dir.join("dedup_scopes.json"), &dedup_report)?;
    let snapshot = aggregate.snapshot();
    csv::write_analysis_scope_csvs(run_dir, &snapshot)?;
    json::write_json(&run_dir.join("analysis_scopes.json"), &snapshot.scopes)?;
    json::write_json(
        &run_dir.join("all_chains.json"),
        &AllChainsReport {
            dedup: &dedup_report,
            analysis: &snapshot,
            unique_identities: identities.counts(),
        },
    )?;
    markdown::write_all_chains(&run_dir.join("all_chains.md"), &snapshot)?;
    json::write_json(
        &run_dir.join("data_quality.json"),
        &DataQualityReport {
            candidate_artifact_count: ordered_artifacts.len(),
            input: input_quality,
            evidence: &snapshot.data_quality,
        },
    )
}

fn write_seed_markdown(
    path: &Path,
    seed: &crate::seed::SeedDefinition,
    relations: &[&crate::reporting::scope::ScopeRelation],
    artifacts: &AHashMap<CandidateId, &crate::reporting::ArtifactRef>,
) -> Result<()> {
    crate::reporting::contract_writer::atomic_write_stream_deferred(path, |file| {
        use std::io::Write as _;

        writeln!(
            file,
            "# {}\n\n- 链：{}\n- 排名：{}\n- 候选关系：{}\n\n\
             | 候选链 | 合约或 collection | 维度 | NFT 作用域 | 完整性 | artifact |\n\
             |---|---|---|---:|---|---|",
            seed.collection_name,
            seed.chain,
            seed.rank,
            relations.len()
        )?;
        for relation in relations {
            write!(
                file,
                "| {} | {} | ",
                relation.candidate.chain, relation.candidate.contract_address
            )?;
            let mut first_dimension = true;
            for dimension in crate::model::Dimension::ALL
                .into_iter()
                .filter(|dimension| relation.dimensions & dimension.bit() != 0)
            {
                if !first_dimension {
                    file.write_all(b",")?;
                }
                file.write_all(dimension.as_str().as_bytes())?;
                first_dimension = false;
            }
            file.write_all(b" | ")?;
            match &relation.selection {
                crate::model::NftSelection::AllInContract { nft_count, .. } => {
                    write!(file, "all:{nft_count}")?;
                }
                crate::model::NftSelection::Explicit { nfts } => {
                    write!(file, "{}", nfts.len())?;
                }
            }
            write!(
                file,
                " | {} | ",
                if relation.incomplete {
                    "incomplete"
                } else {
                    "complete"
                }
            )?;
            if let Some(artifact) = artifacts.get(&relation.candidate_id) {
                write!(file, "{}", artifact.artifact_path.display())?;
            } else {
                file.write_all(b"missing")?;
            }
            file.write_all(b" |\n")?;
        }
        Ok(())
    })
}

#[derive(serde::Serialize)]
struct SeedReport<'a> {
    seed: &'a crate::seed::SeedDefinition,
    relations: SeedRelations<'a>,
}

#[derive(serde::Serialize)]
struct SeedRelationReport<'a> {
    candidate_id: crate::model::CandidateId,
    candidate: &'a crate::model::ContractKey,
    dimensions: DimensionNames,
    selection: &'a crate::model::NftSelection,
    incomplete: bool,
    artifact: Option<&'a crate::reporting::ArtifactRef>,
}

struct SeedRelations<'a> {
    relations: &'a [&'a crate::reporting::scope::ScopeRelation],
    artifacts: &'a AHashMap<CandidateId, &'a crate::reporting::ArtifactRef>,
}

impl serde::Serialize for SeedRelations<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut sequence = serializer.serialize_seq(Some(self.relations.len()))?;
        for relation in self.relations {
            sequence.serialize_element(&SeedRelationReport {
                candidate_id: relation.candidate_id,
                candidate: &relation.candidate,
                dimensions: DimensionNames(relation.dimensions),
                selection: &relation.selection,
                incomplete: relation.incomplete,
                artifact: self.artifacts.get(&relation.candidate_id).copied(),
            })?;
        }
        sequence.end()
    }
}

struct DimensionNames(u8);

impl serde::Serialize for DimensionNames {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut sequence = serializer.serialize_seq(None)?;
        for dimension in crate::model::Dimension::ALL {
            if self.0 & dimension.bit() != 0 {
                sequence.serialize_element(dimension.as_str())?;
            }
        }
        sequence.end()
    }
}

#[derive(serde::Serialize)]
struct AllChainsReport<'a> {
    dedup: &'a crate::reporting::scope::DedupScopeReport,
    analysis: &'a crate::reporting::AggregateSnapshot,
    unique_identities: crate::resident::AnalysisIdentityCounts,
}

#[derive(serde::Serialize)]
struct DataQualityReport<'a> {
    candidate_artifact_count: usize,
    input: &'a crate::model::InputQuality,
    evidence: &'a crate::reporting::QualityAggregateSnapshot,
}

fn safe_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainId, ContractKey, NftKey};

    #[test]
    fn explicit_prefetch_cannot_satisfy_a_later_whole_collection_upgrade() {
        let contract = ContractKey::new(ChainId::Ethereum, "0x1");
        let explicit = NftSelection::Explicit {
            nfts: vec![NftKey {
                chain: ChainId::Ethereum,
                contract_address: contract.contract_address.clone(),
                token_id: Arc::from("1"),
            }],
        };
        let whole = NftSelection::AllInContract {
            contract,
            nft_count: 10,
        };
        assert!(!selection_covers(&explicit, &whole));
        assert!(selection_covers(&whole, &explicit));
    }

    #[test]
    fn failed_prefetch_never_satisfies_the_final_fetch_plan() {
        let contract = ContractKey::new(ChainId::Ethereum, "0x1");
        let selection = NftSelection::AllInContract {
            contract,
            nft_count: 10,
        };
        let seeds = BTreeSet::from([crate::model::SeedId(0)]);
        assert!(!prefetch_can_finalize(
            false, &selection, &selection, &seeds, &seeds
        ));
        assert!(prefetch_can_finalize(
            true, &selection, &selection, &seeds, &seeds
        ));
    }

    #[test]
    fn deep_analysis_failure_preserves_frozen_relation_labels_and_verifications() {
        let candidate_key = ContractKey::new(ChainId::Ethereum, "0xcandidate");
        let relation = crate::model::SeedCandidateRelation {
            seed_id: crate::model::SeedId(1),
            seed: ContractKey::new(ChainId::Ethereum, "0xseed"),
            candidate_id: CandidateId(7),
            candidate: candidate_key.clone(),
            dimensions: crate::model::Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate_key.clone(),
                nft_count: 1,
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let candidate = prefetch_records(vec![relation]).unwrap().remove(0);
        let verification = crate::model::RelationVerification {
            seed_id: crate::model::SeedId(1),
            official_controller_continuity: true,
            authorized_reissue: false,
            verified_migration: false,
            official_collection_relation: false,
            complete: true,
            evidence_keys: vec![Arc::from("controller_continuity")],
            failures: Vec::new(),
        };
        let mut evidence = EvidenceBundle {
            candidate: candidate_key,
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: Vec::new(),
            holders: Vec::new(),
            controllers: Vec::new(),
            relation_verifications: vec![verification],
            provenance: Vec::new(),
            quality: EvidenceQuality::default(),
        };
        let frozen_labels = vec![crate::model::RelationLabel {
            seed_id: crate::model::SeedId(1),
            candidate_id: CandidateId(7),
            classification: crate::model::RelationClassification::LegitDuplicate {
                evidence: vec![Arc::from("controller_continuity")],
            },
        }];

        let parts = failed_analysis_parts(
            &candidate,
            &mut evidence,
            "deep analysis failed",
            1_700_000_000,
            Some(frozen_labels),
        );

        assert_eq!(evidence.relation_verifications.len(), 1);
        assert_eq!(evidence.quality.failures, ["deep analysis failed"]);
        assert!(matches!(
            &parts.labels[0].classification,
            crate::model::RelationClassification::LegitDuplicate { evidence }
                if evidence == &[Arc::from("controller_continuity")]
        ));
    }

    #[test]
    fn successful_fallback_failure_record_does_not_make_evidence_incomplete() {
        let complete_with_primary_failure = EvidenceQuality {
            assets: Some(EvidenceStatus::Complete),
            histories: Some(EvidenceStatus::Complete),
            transactions: Some(EvidenceStatus::Complete),
            prices: Some(EvidenceStatus::Complete),
            authority: Some(EvidenceStatus::Complete),
            failures: vec!["Alchemy failed; OpenSea fallback succeeded".into()],
            ..EvidenceQuality::default()
        };
        assert!(!evidence_incomplete(&complete_with_primary_failure));

        let mut supplemental_failure = complete_with_primary_failure;
        supplemental_failure.supplemental_query_failures = 1;
        assert!(evidence_incomplete(&supplemental_failure));
    }

    #[test]
    fn response_limit_keeps_concurrent_decode_expansion_bounded() {
        let memory = 464 * 1024 * 1024 * 1024;
        let capacity = 256;
        let limit = provider_response_limit(memory, capacity);
        assert!((64 * 1024 * 1024..=1024 * 1024 * 1024).contains(&limit));
        assert!((capacity as u64).saturating_mul(limit).saturating_mul(4) <= memory / 4);
    }

    struct IncrementalProvider {
        full_fetches: std::sync::atomic::AtomicUsize,
        relation_fetches: std::sync::atomic::AtomicUsize,
        fail_relation_fetch: bool,
    }

    #[async_trait]
    impl crate::api::EvidenceProvider for IncrementalProvider {
        async fn fetch_candidate(
            &self,
            _candidate: &ContractKey,
            _selection: &NftSelection,
            _relations: &[crate::model::SeedCandidateRelation],
        ) -> Result<EvidenceBundle> {
            self.full_fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(AnalysisError::Provider(
                "full fetch should not run for relation-only extension".into(),
            ))
        }

        async fn fetch_relation_verifications(
            &self,
            _evidence: &EvidenceBundle,
            relations: &[crate::model::SeedCandidateRelation],
        ) -> Result<Vec<crate::model::RelationVerification>> {
            self.relation_fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_relation_fetch {
                return Err(AnalysisError::Provider(
                    "relation verification fixture failure".into(),
                ));
            }
            Ok(relations
                .iter()
                .map(|relation| crate::model::RelationVerification {
                    seed_id: relation.seed_id,
                    official_controller_continuity: true,
                    authorized_reissue: false,
                    verified_migration: false,
                    official_collection_relation: false,
                    complete: true,
                    evidence_keys: vec![Arc::from("controller_continuity")],
                    failures: Vec::new(),
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn relation_only_upgrade_reuses_prefetched_evidence() {
        let candidate_key = ContractKey::new(ChainId::Ethereum, "0xcandidate");
        let relation = crate::model::SeedCandidateRelation {
            seed_id: crate::model::SeedId(1),
            seed: ContractKey::new(ChainId::Ethereum, "0xseed"),
            candidate_id: CandidateId(7),
            candidate: candidate_key.clone(),
            dimensions: crate::model::Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate_key.clone(),
                nft_count: 1,
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let candidate = prefetch_records(vec![relation]).unwrap().remove(0);
        let base = EvidenceBundle {
            candidate: candidate_key,
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: Vec::new(),
            holders: Vec::new(),
            controllers: vec![Arc::from("0xcontroller")],
            relation_verifications: Vec::new(),
            provenance: Vec::new(),
            quality: EvidenceQuality {
                assets: Some(EvidenceStatus::Empty),
                histories: Some(EvidenceStatus::Empty),
                transactions: Some(EvidenceStatus::Empty),
                prices: Some(EvidenceStatus::Empty),
                authority: Some(EvidenceStatus::Complete),
                ..EvidenceQuality::default()
            },
        };
        let provider = IncrementalProvider {
            full_fetches: std::sync::atomic::AtomicUsize::new(0),
            relation_fetches: std::sync::atomic::AtomicUsize::new(0),
            fail_relation_fetch: false,
        };
        let result = fetch_or_extend(
            &provider,
            &candidate,
            &candidate_selection(&candidate.relations),
            Some((base, true)),
            1_700_000_000,
        )
        .await
        .unwrap();
        assert_eq!(result.relation_verifications.len(), 1);
        assert_eq!(
            provider
                .full_fetches
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            provider
                .relation_fetches
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn relation_extension_failure_is_terminal_and_uses_analysis_timestamp() {
        let candidate_key = ContractKey::new(ChainId::Ethereum, "0xcandidate");
        let relation = crate::model::SeedCandidateRelation {
            seed_id: crate::model::SeedId(1),
            seed: ContractKey::new(ChainId::Ethereum, "0xseed"),
            candidate_id: CandidateId(7),
            candidate: candidate_key.clone(),
            dimensions: crate::model::Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate_key.clone(),
                nft_count: 1,
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let candidate = prefetch_records(vec![relation]).unwrap().remove(0);
        let base = EvidenceBundle {
            candidate: candidate_key,
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: Vec::new(),
            holders: Vec::new(),
            controllers: Vec::new(),
            relation_verifications: Vec::new(),
            provenance: Vec::new(),
            quality: EvidenceQuality {
                assets: Some(EvidenceStatus::Empty),
                histories: Some(EvidenceStatus::Empty),
                transactions: Some(EvidenceStatus::Empty),
                prices: Some(EvidenceStatus::Empty),
                authority: Some(EvidenceStatus::Empty),
                ..EvidenceQuality::default()
            },
        };
        let provider = IncrementalProvider {
            full_fetches: std::sync::atomic::AtomicUsize::new(0),
            relation_fetches: std::sync::atomic::AtomicUsize::new(0),
            fail_relation_fetch: true,
        };
        let observed_at = 1_700_000_123;
        let result = fetch_or_extend(
            &provider,
            &candidate,
            &candidate_selection(&candidate.relations),
            Some((base, true)),
            observed_at,
        )
        .await
        .unwrap();
        assert_eq!(result.quality.supplemental_query_failures, 1);
        assert_eq!(result.provenance[0].observed_at, observed_at);
        assert_eq!(result.provenance[0].status, EvidenceStatus::Failed);
        assert_eq!(result.quality.failures.len(), 1);
        assert_eq!(result.relation_verifications.len(), 1);
        assert!(!result.relation_verifications[0].failures.is_empty());
    }

    struct FixtureProvider {
        observed_at: i64,
        fetches: Arc<std::sync::atomic::AtomicUsize>,
        fail_first: bool,
    }

    #[async_trait]
    impl crate::api::EvidenceProvider for FixtureProvider {
        async fn fetch_candidate(
            &self,
            candidate: &ContractKey,
            _selection: &NftSelection,
            relations: &[crate::model::SeedCandidateRelation],
        ) -> Result<EvidenceBundle> {
            let attempt = self
                .fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_first && attempt == 0 {
                return Err(AnalysisError::Provider(
                    "fixture speculative prefetch failure".into(),
                ));
            }
            Ok(EvidenceBundle {
                candidate: candidate.clone(),
                deployment_timestamp: Some(self.observed_at - 100),
                duplicate_content_timestamp: Some(self.observed_at - 50),
                events: Vec::new(),
                holders: Vec::new(),
                controllers: Vec::new(),
                relation_verifications: relations
                    .iter()
                    .map(|relation| crate::model::RelationVerification {
                        seed_id: relation.seed_id,
                        official_controller_continuity: false,
                        authorized_reissue: false,
                        verified_migration: false,
                        official_collection_relation: false,
                        complete: true,
                        evidence_keys: Vec::new(),
                        failures: Vec::new(),
                    })
                    .collect(),
                provenance: vec![EvidenceObservation {
                    source: "fixture".into(),
                    request_key: "candidate".into(),
                    observed_at: self.observed_at,
                    status: EvidenceStatus::Empty,
                }],
                quality: EvidenceQuality {
                    assets: Some(EvidenceStatus::Empty),
                    histories: Some(EvidenceStatus::Empty),
                    transactions: Some(EvidenceStatus::Empty),
                    prices: Some(EvidenceStatus::Empty),
                    authority: Some(EvidenceStatus::Empty),
                    candidate_assets_total: 1,
                    ..EvidenceQuality::default()
                },
            })
        }
    }

    async fn run_deterministic_fixture(
        output_dir: &Path,
        writer_queue_bytes: u64,
        send_prefetch: bool,
        fail_first: bool,
    ) -> Result<PathBuf> {
        let mut config = RunConfig::from_path_unvalidated(Path::new("config/default.toml"))?;
        config.output_dir = output_dir.to_path_buf();
        config.analysis_queue_capacity = 4;
        config.network_queue_capacity = 4;
        config.compression_concurrency = 1;
        config.writer_threads = 1;
        config.writer_queue_bytes = writer_queue_bytes;

        let candidate_key = ContractKey::new(
            ChainId::Ethereum,
            "0x0000000000000000000000000000000000000002",
        );
        let seed_key = ContractKey::new(
            ChainId::Ethereum,
            "0x0000000000000000000000000000000000000001",
        );
        let relation = crate::model::SeedCandidateRelation {
            seed_id: crate::model::SeedId(0),
            seed: seed_key.clone(),
            candidate_id: CandidateId(0),
            candidate: candidate_key.clone(),
            dimensions: crate::model::Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate_key.clone(),
                nft_count: 1,
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let second_seed_key = ContractKey::new(
            ChainId::Ethereum,
            "0x0000000000000000000000000000000000000003",
        );
        let mut second_relation = relation.clone();
        second_relation.seed_id = crate::model::SeedId(1);
        second_relation.seed = second_seed_key.clone();
        let manifest = SeedManifest {
            generated_at: config.analysis_timestamp,
            seeds: [seed_key, second_seed_key]
                .into_iter()
                .enumerate()
                .map(|(index, seed)| crate::seed::SeedDefinition {
                    id: crate::model::SeedId(index as u16),
                    chain: ChainId::Ethereum,
                    contract_address: seed.contract_address.to_string(),
                    rank: (index + 1) as u8,
                    collection_name: format!("fixture seed {}", index + 1),
                    stable_identifier: format!("fixture-seed-{}", index + 1),
                    ranking_metric: "thirty_day_volume".into(),
                    ranking_value: 1.0,
                    ranking_window: "thirty_day".into(),
                    source: "fixture".into(),
                    collected_at: config.analysis_timestamp,
                })
                .collect(),
        };
        let store = crate::resident::ResidentBaseStore {
            contracts: crate::resident::ContractCatalog {
                contracts: vec![crate::model::ContractRecord {
                    chain: candidate_key.chain,
                    address: candidate_key.contract_address.clone(),
                    nft_count: 1,
                    name_value_id: None,
                    metadata_profile_id: None,
                    name_owner_shard: None,
                    metadata_owner_shard: None,
                }],
            },
            uri_identity: None,
            uri_features: None,
            name_features: None,
            metadata_features: None,
            quality: crate::model::InputQuality {
                physical_rows: 1,
                logical_nfts: 1,
                ..crate::model::InputQuality::default()
            },
        };
        let writer = Arc::new(ContractWriter::create(
            &config.output_dir,
            "fixture-run",
            config.writer_queue_bytes,
        )?);
        write_manifest(
            writer.run_dir(),
            "fixture-run",
            &config,
            &crate::platform::PlatformResources {
                allowed_cpus: vec![0, 1],
                physical_memory: config.memory_limit,
                cgroup_memory_limit: Some(config.memory_limit),
                effective_memory_limit: config.memory_limit,
                numa_nodes: Vec::new(),
            },
        )?;
        let mut registry = CandidateRegistry::default();
        let mut aggregate = AggregateState::default();
        let mut artifact_index = ArtifactIndex::default();
        let identities = Arc::new(crate::resident::AnalysisIdentityStore::default());
        let executor = CpuExecutor::new_bounded(2, 8)?;
        let progress = Progress::default();
        let (candidate_tx, candidate_rx) = tokio::sync::mpsc::channel(2);
        if send_prefetch {
            candidate_tx
                .send(crate::pipeline::CandidateRelationsEvent::Prefetch(vec![
                    relation.clone(),
                    second_relation.clone(),
                ]))
                .await
                .map_err(|_| AnalysisError::State("fixture candidate channel closed".into()))?;
        }
        candidate_tx
            .send(crate::pipeline::CandidateRelationsEvent::Frozen(vec![
                relation,
                second_relation,
            ]))
            .await
            .map_err(|_| AnalysisError::State("fixture candidate channel closed".into()))?;
        drop(candidate_tx);
        let dedup_task = tokio::spawn(async move {
            Ok(DedupOutput {
                store,
                failed_seeds: BTreeSet::new(),
            })
        });
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (dedup, relations) = process_candidates(
            CandidatePipelineContext {
                registry: &mut registry,
                provider: Arc::new(FixtureProvider {
                    observed_at: config.analysis_timestamp.timestamp(),
                    fetches: fetches.clone(),
                    fail_first,
                }),
                executor: &executor,
                writer: writer.clone(),
                aggregate: &mut aggregate,
                artifact_index: &mut artifact_index,
                config: &config,
                progress: &progress,
                identities: identities.clone(),
            },
            candidate_rx,
            dedup_task,
        )
        .await?;
        let expected_fetches = if send_prefetch && fail_first { 2 } else { 1 };
        if fetches.load(std::sync::atomic::Ordering::SeqCst) != expected_fetches {
            return Err(AnalysisError::State(
                "multi-seed fixture fetched one candidate more than once".into(),
            ));
        }
        write_final_reports(FinalReportContext {
            run_dir: writer.run_dir(),
            manifest: &manifest,
            relations: &relations,
            artifacts: &mut artifact_index,
            aggregate: &mut aggregate,
            catalog: &dedup.store.contracts,
            input_quality: &dedup.store.quality,
            identities: &identities,
            failed_seeds: &dedup.failed_seeds,
        })?;
        writer.finish_success()?;
        if registry.seen_count() != 1 {
            return Err(AnalysisError::State(
                "fixture candidate registry did not record exactly one candidate".into(),
            ));
        }
        Ok(writer.run_dir().to_path_buf())
    }

    fn collect_fixture_files(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>> {
        fn visit(
            root: &Path,
            directory: &Path,
            files: &mut BTreeMap<PathBuf, Vec<u8>>,
        ) -> Result<()> {
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    visit(root, &path, files)?;
                } else {
                    let relative = path
                        .strip_prefix(root)
                        .map_err(|error| AnalysisError::State(error.to_string()))?
                        .to_path_buf();
                    if relative != Path::new("run_manifest.json") {
                        files.insert(relative, std::fs::read(path)?);
                    }
                }
            }
            Ok(())
        }

        let mut files = BTreeMap::new();
        visit(root, root, &mut files)?;
        Ok(files)
    }

    #[tokio::test]
    async fn complete_fixture_is_byte_stable_durable_and_marks_success() {
        let root = tempfile::tempdir().unwrap();
        let first =
            run_deterministic_fixture(&root.path().join("first"), 1024 * 1024, false, false)
                .await
                .unwrap();
        let second =
            run_deterministic_fixture(&root.path().join("second"), 1024 * 1024, false, false)
                .await
                .unwrap();
        let first_files = collect_fixture_files(&first).unwrap();
        let second_files = collect_fixture_files(&second).unwrap();
        assert_eq!(first_files, second_files);
        assert!(first.join("_SUCCESS").is_file());

        let index = std::fs::read_to_string(first.join("contract_index.jsonl")).unwrap();
        let artifact: crate::reporting::ArtifactRef =
            serde_json::from_str(index.lines().next().unwrap()).unwrap();
        let payload = std::fs::read(first.join(&artifact.artifact_path)).unwrap();
        let checksum = Sha256::digest(&payload)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(checksum, artifact.checksum);
        assert!(zstd::decode_all(payload.as_slice()).is_ok());
    }

    #[tokio::test]
    async fn oversized_candidate_fails_without_success_marker() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("result");
        let error = run_deterministic_fixture(&output, 1, false, false)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            AnalysisError::MemoryBudget { limit: 1, .. }
        ));
        assert!(!output.join("fixture-run").join("_SUCCESS").exists());
    }

    #[tokio::test]
    async fn failed_speculative_prefetch_is_retried_after_relations_freeze() {
        let root = tempfile::tempdir().unwrap();
        let run = run_deterministic_fixture(root.path(), 1024 * 1024, true, true)
            .await
            .unwrap();
        assert!(run.join("_SUCCESS").is_file());
        let index = std::fs::read_to_string(run.join("contract_index.jsonl")).unwrap();
        let artifact: crate::reporting::ArtifactRef =
            serde_json::from_str(index.lines().next().unwrap()).unwrap();
        assert_eq!(artifact.analysis_status, "analyzed");
    }
}
