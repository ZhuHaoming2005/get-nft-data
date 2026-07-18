use super::{
    ContractAnchors, MetadataCandidate, PrefilterResult, VerificationResult, verify_metadata_pair,
};
use crate::parallel::RayonChunkExecutor;
use dedup_index::{ExternalRadix, MemoryBudget, PairReducerBuffer, RadixRecord, SpillVolume};
use dedup_model::{
    ChunkExecutor, ContractId, DedupError, Dimension, EntityId, EntityKind, ErrorContext,
    ExecutionMode, HitEvent, HitEventSink, NoopProgress, ProgressObserver, ScopeId, StageCounters,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataMatch {
    pub candidate: MetadataCandidate,
    pub verification: VerificationResult,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataRunResult {
    pub matches: Vec<MetadataMatch>,
    pub counters: StageCounters,
}

#[derive(Clone, Debug)]
pub struct MetadataExecutionConfig {
    pub mode: ExecutionMode,
    pub spill_root: PathBuf,
    pub resident_candidate_limit: usize,
    pub radix_partition_bits: u8,
    pub max_open_spill_files: usize,
    pub max_records_per_partition: usize,
    pub radix_memory_budget: Option<(MemoryBudget, u64)>,
    pub radix_volumes: Vec<SpillVolume>,
    pub workers: usize,
}

impl MetadataExecutionConfig {
    pub fn new(
        mode: ExecutionMode,
        spill_root: impl Into<PathBuf>,
        resident_candidate_limit: usize,
        radix_partition_bits: u8,
        max_open_spill_files: usize,
        max_records_per_partition: usize,
    ) -> Result<Self, DedupError> {
        if resident_candidate_limit == 0
            || max_open_spill_files == 0
            || max_records_per_partition == 0
        {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_verify"),
                message: "metadata spill capacities must be positive".to_owned(),
            });
        }
        let spill_root = spill_root.into();
        Ok(Self {
            mode,
            spill_root: spill_root.clone(),
            resident_candidate_limit,
            radix_partition_bits,
            max_open_spill_files,
            max_records_per_partition,
            radix_memory_budget: None,
            radix_volumes: vec![SpillVolume::new(spill_root, 1)?],
            workers: 1,
        })
    }

    pub fn with_radix_memory_budget(
        mut self,
        budget: MemoryBudget,
        bytes: u64,
    ) -> Result<Self, DedupError> {
        if bytes == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_verify"),
                message: "metadata radix memory budget must be positive".to_owned(),
            });
        }
        self.radix_memory_budget = Some((budget, bytes));
        Ok(self)
    }

    pub fn with_workers(mut self, workers: usize) -> Result<Self, DedupError> {
        if workers == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_verify"),
                message: "metadata worker count must be positive".to_owned(),
            });
        }
        self.workers = workers;
        Ok(self)
    }

    pub fn with_radix_volumes(mut self, volumes: Vec<SpillVolume>) -> Result<Self, DedupError> {
        if volumes.is_empty() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_verify"),
                message: "metadata radix requires at least one temporary volume".to_owned(),
            });
        }
        self.radix_volumes = volumes;
        Ok(self)
    }
}

pub fn run_metadata_verification(
    contracts: &[ContractAnchors],
    prefilter: &PrefilterResult,
    threshold: f64,
    verification_budget: u64,
    sink: &mut impl HitEventSink,
) -> Result<MetadataRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(1, "metadata_verify")?;
    run_metadata_verification_internal(
        contracts,
        prefilter,
        threshold,
        verification_budget,
        sink,
        None,
        &NoopProgress,
        &executor,
    )
}

pub fn run_metadata_verification_with_config(
    contracts: &[ContractAnchors],
    prefilter: &PrefilterResult,
    threshold: f64,
    verification_budget: u64,
    sink: &mut impl HitEventSink,
    config: &MetadataExecutionConfig,
) -> Result<MetadataRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(config.workers, "metadata_verify")?;
    run_metadata_verification_with_config_progress_and_executor(
        contracts,
        prefilter,
        threshold,
        verification_budget,
        sink,
        config,
        &NoopProgress,
        &executor,
    )
}

pub fn run_metadata_verification_with_config_and_progress(
    contracts: &[ContractAnchors],
    prefilter: &PrefilterResult,
    threshold: f64,
    verification_budget: u64,
    sink: &mut impl HitEventSink,
    config: &MetadataExecutionConfig,
    progress: &dyn ProgressObserver,
) -> Result<MetadataRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(config.workers, "metadata_verify")?;
    run_metadata_verification_with_config_progress_and_executor(
        contracts,
        prefilter,
        threshold,
        verification_budget,
        sink,
        config,
        progress,
        &executor,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_metadata_verification_with_config_progress_and_executor(
    contracts: &[ContractAnchors],
    prefilter: &PrefilterResult,
    threshold: f64,
    verification_budget: u64,
    sink: &mut impl HitEventSink,
    config: &MetadataExecutionConfig,
    progress: &dyn ProgressObserver,
    executor: &impl ChunkExecutor,
) -> Result<MetadataRunResult, DedupError> {
    if executor.worker_count() != config.workers {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("metadata_verify"),
            message: "executor worker count does not match metadata configuration".to_owned(),
        });
    }
    run_metadata_verification_internal(
        contracts,
        prefilter,
        threshold,
        verification_budget,
        sink,
        Some(config),
        progress,
        executor,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_metadata_verification_internal(
    contracts: &[ContractAnchors],
    prefilter: &PrefilterResult,
    threshold: f64,
    verification_budget: u64,
    sink: &mut impl HitEventSink,
    config: Option<&MetadataExecutionConfig>,
    progress: &dyn ProgressObserver,
    executor: &impl ChunkExecutor,
) -> Result<MetadataRunResult, DedupError> {
    let by_id: BTreeMap<ContractId, &ContractAnchors> = contracts
        .iter()
        .map(|contract| (contract.contract_id, contract))
        .collect();
    let mut counters = StageCounters::default();
    let mut matches = Vec::new();
    let candidate_count = prefilter.candidates.count();
    if candidate_count > verification_budget {
        return Err(DedupError::BudgetExhausted {
            context: ErrorContext::stage("metadata_verify"),
            counter: "metadata_verify_pairs",
            limit: verification_budget,
        });
    }
    if prefilter.candidates.resident().is_none() {
        progress.begin_phase("verify_external_metadata_candidates", Some(candidate_count));
        let mut batch = Vec::with_capacity(1_024);
        prefilter.candidates.visit(|candidate| {
            batch.push(candidate);
            if batch.len() == batch.capacity() {
                let completed = batch.len();
                flush_verified_candidates(
                    &batch,
                    &by_id,
                    threshold,
                    sink,
                    &mut counters,
                    &mut matches,
                    executor,
                )?;
                batch.clear();
                progress.advance(u64::try_from(completed).unwrap_or(u64::MAX));
                progress.check_cancelled("metadata_verify")?;
            }
            Ok(())
        })?;
        let completed = batch.len();
        flush_verified_candidates(
            &batch,
            &by_id,
            threshold,
            sink,
            &mut counters,
            &mut matches,
            executor,
        )?;
        progress.advance(u64::try_from(completed).unwrap_or(u64::MAX));
        progress.check_cancelled("metadata_verify")?;
        return Ok(MetadataRunResult { matches, counters });
    }
    let candidates =
        prefilter
            .candidates
            .resident()
            .ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("metadata_verify"),
                message: "resident candidate source changed during verification".to_owned(),
            })?;
    let mode = config.map_or(ExecutionMode::InMemory, |value| match value.mode {
        ExecutionMode::Auto => ExecutionMode::InMemory,
        mode => mode,
    });
    let resident_count = match mode {
        ExecutionMode::Hybrid => config
            .map_or(0, |value| value.resident_candidate_limit)
            .min(candidates.len()),
        ExecutionMode::InMemory | ExecutionMode::Auto => candidates.len(),
        ExecutionMode::External => 0,
    };
    progress.begin_phase(
        "verify_metadata_candidates",
        u64::try_from(candidates.len()).ok(),
    );
    if resident_count > 0 {
        progress.begin_phase(
            "verify_resident_metadata_candidates",
            u64::try_from(resident_count).ok(),
        );
        let resident = &candidates[..resident_count];
        let verified = verify_resident_candidates(resident, &by_id, threshold, executor, progress)?;
        for (matched, local_counters) in verified {
            counters.merge(&local_counters)?;
            if let Some(metadata_match) = matched {
                let left = by_id[&metadata_match.candidate.left];
                let right = by_id[&metadata_match.candidate.right];
                emit_pair(left, right, sink, &mut counters)?;
                matches.push(metadata_match);
            }
        }
    }
    if resident_count < candidates.len() {
        let config = config.ok_or_else(|| DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_verify"),
            message: "external metadata mode has no capacity configuration".to_owned(),
        })?;
        let volumes = config
            .radix_volumes
            .iter()
            .map(|volume| SpillVolume::new(volume.root.join("metadata-candidates"), volume.weight))
            .collect::<Result<Vec<_>, _>>()?;
        let mut radix = if let Some((budget, bytes)) = &config.radix_memory_budget {
            ExternalRadix::create_budgeted_striped(
                volumes,
                config.radix_partition_bits,
                config.max_open_spill_files,
                config.max_records_per_partition,
                budget,
                *bytes,
            )?
        } else {
            ExternalRadix::create_striped(
                volumes,
                config.radix_partition_bits,
                config.max_open_spill_files,
                config.max_records_per_partition,
            )?
        };
        for candidate in &candidates[resident_count..] {
            radix.push(RadixRecord {
                key: candidate.left.as_u64(),
                payload: [candidate.right.as_u64(), 0, 0],
            })?;
        }
        let mut batch = Vec::with_capacity(1_024);
        let stats = radix.finish_with_progress(
            progress,
            "metadata_candidate_external_sort",
            "verify_external_metadata_candidates",
            |record| {
                let candidate = MetadataCandidate {
                    left: ContractId::new(EntityId::try_from(record.key).map_err(|_| {
                        invalid_candidate_record(record, "left ContractId exceeds EntityId")
                    })?),
                    right: ContractId::new(EntityId::try_from(record.payload[0]).map_err(
                        |_| invalid_candidate_record(record, "right ContractId exceeds EntityId"),
                    )?),
                };
                batch.push(candidate);
                if batch.len() == batch.capacity() {
                    flush_verified_candidates(
                        &batch,
                        &by_id,
                        threshold,
                        sink,
                        &mut counters,
                        &mut matches,
                        executor,
                    )?;
                    batch.clear();
                }
                Ok(())
            },
        )?;
        flush_verified_candidates(
            &batch,
            &by_id,
            threshold,
            sink,
            &mut counters,
            &mut matches,
            executor,
        )?;
        progress.check_cancelled("metadata_verify")?;
        counters.metadata_radix_handle_touches(stats.handle_touches)?;
        counters.spill_bytes(stats.spill_bytes)?;
    }
    Ok(MetadataRunResult { matches, counters })
}

fn verify_resident_candidates(
    candidates: &[MetadataCandidate],
    by_id: &BTreeMap<ContractId, &ContractAnchors>,
    threshold: f64,
    executor: &impl ChunkExecutor,
    progress: &dyn ProgressObserver,
) -> Result<Vec<(Option<MetadataMatch>, StageCounters)>, DedupError> {
    let verify_chunk = |chunk: &[MetadataCandidate]| {
        let mut output = PairReducerBuffer::new(chunk.len().max(1))?;
        for candidate in chunk {
            let verified = verify_candidate(*candidate, by_id, threshold)?;
            output
                .push(verified)
                .map_err(|_| DedupError::InvariantViolation {
                    context: ErrorContext::stage("metadata_verify"),
                    message: "bounded verification output exceeded its input chunk".to_owned(),
                })?;
        }
        progress.advance(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        progress.check_cancelled("metadata_verify")?;
        Ok::<_, DedupError>(output.drain().collect::<Vec<_>>())
    };
    if executor.worker_count() <= 1 || candidates.len() < 512 {
        return verify_chunk(candidates);
    }
    let chunks = executor.map_chunks(candidates, 256, verify_chunk)?;
    Ok(chunks.into_iter().flatten().collect())
}

fn verify_candidate(
    candidate: MetadataCandidate,
    by_id: &BTreeMap<ContractId, &ContractAnchors>,
    threshold: f64,
) -> Result<(Option<MetadataMatch>, StageCounters), DedupError> {
    let mut counters = StageCounters::default();
    counters.metadata_verify_pairs(1)?;
    let left = by_id
        .get(&candidate.left)
        .ok_or_else(|| DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_verify"),
            message: "candidate references missing left contract".to_owned(),
        })?;
    let right = by_id
        .get(&candidate.right)
        .ok_or_else(|| DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_verify"),
            message: "candidate references missing right contract".to_owned(),
        })?;
    let verification = verify_metadata_pair(left, right, threshold, &mut counters)?;
    let matched = verification.matched.then_some(MetadataMatch {
        candidate,
        verification,
    });
    Ok((matched, counters))
}

fn flush_verified_candidates(
    candidates: &[MetadataCandidate],
    by_id: &BTreeMap<ContractId, &ContractAnchors>,
    threshold: f64,
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
    matches: &mut Vec<MetadataMatch>,
    executor: &impl ChunkExecutor,
) -> Result<(), DedupError> {
    if candidates.is_empty() {
        return Ok(());
    }
    for (matched, local_counters) in
        verify_resident_candidates(candidates, by_id, threshold, executor, &NoopProgress)?
    {
        counters.merge(&local_counters)?;
        if let Some(metadata_match) = matched {
            let left = by_id[&metadata_match.candidate.left];
            let right = by_id[&metadata_match.candidate.right];
            emit_pair(left, right, sink, counters)?;
            matches.push(metadata_match);
        }
    }
    Ok(())
}

fn invalid_candidate_record(record: RadixRecord, message: &str) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext {
            stage: "metadata_verify",
            partition: None,
            stable_object_id: Some(record.key),
        },
        message: message.to_owned(),
    }
}

fn emit_pair(
    left: &ContractAnchors,
    right: &ContractAnchors,
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
) -> Result<(), DedupError> {
    if left.chain_id == right.chain_id {
        emit_contract(
            left.contract_id,
            ScopeId::Intra(left.chain_id),
            sink,
            counters,
        )?;
        emit_contract(
            right.contract_id,
            ScopeId::Intra(right.chain_id),
            sink,
            counters,
        )?;
    } else {
        for (primary, secondary) in [(left, right), (right, left)] {
            emit_contract(
                primary.contract_id,
                ScopeId::CrossSummary(primary.chain_id),
                sink,
                counters,
            )?;
            emit_contract(
                primary.contract_id,
                ScopeId::Matrix {
                    primary: primary.chain_id,
                    secondary: secondary.chain_id,
                },
                sink,
                counters,
            )?;
        }
    }
    Ok(())
}

fn emit_contract(
    contract: ContractId,
    scope: ScopeId,
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
) -> Result<(), DedupError> {
    sink.submit(HitEvent {
        dimension: Dimension::Metadata,
        scope,
        entity_kind: EntityKind::Contract,
        entity_id: contract.as_u64(),
    })?;
    counters.hit_events(1)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetadataRecord, PrefilterAudit, select_anchors};
    use dedup_model::{ChainId, HitEvent};
    use std::collections::BTreeSet;

    #[derive(Default)]
    struct RecordingSink(Vec<HitEvent>);

    impl HitEventSink for RecordingSink {
        fn submit(&mut self, event: HitEvent) -> Result<(), DedupError> {
            self.0.push(event);
            Ok(())
        }
    }

    #[test]
    fn cross_chain_pair_is_verified_once_and_emitted_both_directions() {
        let records = [0_u16, 1]
            .into_iter()
            .map(|chain| MetadataRecord {
                doc_id: dedup_model::MetadataDocId::new(dedup_model::EntityId::from(u32::from(
                    chain,
                ))),
                contract_id: ContractId::new(dedup_model::EntityId::from(u32::from(chain))),
                chain_id: ChainId::new(chain),
                token_id: "7".to_owned(),
                content: r#"{"collection":"same","token":"shared"}"#.to_owned(),
            })
            .collect();
        let mut anchor_counters = StageCounters::default();
        let contracts = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0), ChainId::new(1)]),
            1,
            &mut anchor_counters,
        )
        .unwrap();
        let candidate = MetadataCandidate::new(ContractId::new(0), ContractId::new(1)).unwrap();
        let prefilter = PrefilterResult {
            candidates: vec![candidate].into(),
            audit: PrefilterAudit::default(),
        };
        let mut sink = RecordingSink::default();
        let result = run_metadata_verification(&contracts, &prefilter, 0.6, 1, &mut sink).unwrap();
        assert_eq!(result.counters.metadata_verify_pairs, 1);
        assert_eq!(result.matches.len(), 1);
        assert_eq!(sink.0.len(), 4);
        assert!(sink.0.iter().any(|event| {
            event.scope
                == ScopeId::Matrix {
                    primary: ChainId::new(0),
                    secondary: ChainId::new(1),
                }
        }));
        assert!(sink.0.iter().any(|event| {
            event.scope
                == ScopeId::Matrix {
                    primary: ChainId::new(1),
                    secondary: ChainId::new(0),
                }
        }));
    }

    #[test]
    fn metadata_verification_modes_are_identical_and_bounded() {
        let records = [0_u16, 1, 2]
            .into_iter()
            .map(|chain| MetadataRecord {
                doc_id: dedup_model::MetadataDocId::new(EntityId::from(u32::from(chain))),
                contract_id: ContractId::new(EntityId::from(u32::from(chain))),
                chain_id: ChainId::new(chain),
                token_id: "7".to_owned(),
                content: r#"{"collection":"same","token":"shared"}"#.to_owned(),
            })
            .collect();
        let mut anchor_counters = StageCounters::default();
        let contracts = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0), ChainId::new(1), ChainId::new(2)]),
            1,
            &mut anchor_counters,
        )
        .unwrap();
        let candidates: Vec<_> = [(0_u32, 1_u32), (0, 2), (1, 2)]
            .into_iter()
            .map(|(left, right)| {
                MetadataCandidate::new(
                    ContractId::new(EntityId::from(left)),
                    ContractId::new(EntityId::from(right)),
                )
                .unwrap()
            })
            .collect();
        let prefilter = PrefilterResult {
            candidates: candidates.into(),
            audit: PrefilterAudit::default(),
        };
        let run = |mode, root: PathBuf| {
            let config = MetadataExecutionConfig::new(mode, root, 1, 1, 2, 3).unwrap();
            let mut sink = RecordingSink::default();
            let result = run_metadata_verification_with_config(
                &contracts, &prefilter, 0.6, 3, &mut sink, &config,
            )
            .unwrap();
            let events: BTreeSet<_> = sink
                .0
                .into_iter()
                .map(|event| {
                    (
                        event.dimension,
                        event.scope,
                        event.entity_kind,
                        event.entity_id,
                    )
                })
                .collect();
            (result, events)
        };
        let directory = tempfile::tempdir().unwrap();
        let (resident, resident_events) =
            run(ExecutionMode::InMemory, directory.path().join("resident"));
        let (hybrid, hybrid_events) = run(ExecutionMode::Hybrid, directory.path().join("hybrid"));
        let (external, external_events) =
            run(ExecutionMode::External, directory.path().join("external"));
        assert_eq!(resident.matches, hybrid.matches);
        assert_eq!(resident.matches, external.matches);
        assert_eq!(resident_events, hybrid_events);
        assert_eq!(resident_events, external_events);
        assert_eq!(hybrid.counters.metadata_radix_handle_touches, 8);
        assert_eq!(external.counters.metadata_radix_handle_touches, 12);
    }

    #[test]
    fn parallel_metadata_verification_is_deterministic() {
        let records = (0_u32..601)
            .map(|id| MetadataRecord {
                doc_id: dedup_model::MetadataDocId::new(EntityId::from(id)),
                contract_id: ContractId::new(EntityId::from(id)),
                chain_id: ChainId::new(u16::try_from(id % 2).unwrap()),
                token_id: "7".to_owned(),
                content: r#"{"collection":"same","token":"shared"}"#.to_owned(),
            })
            .collect();
        let mut anchor_counters = StageCounters::default();
        let contracts = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0), ChainId::new(1)]),
            1,
            &mut anchor_counters,
        )
        .unwrap();
        let candidates: Vec<_> = (0_u32..600)
            .map(|left| {
                MetadataCandidate::new(
                    ContractId::new(EntityId::from(left)),
                    ContractId::new(EntityId::from(left + 1)),
                )
                .unwrap()
            })
            .collect();
        let prefilter = PrefilterResult {
            candidates: candidates.into(),
            audit: PrefilterAudit::default(),
        };
        let directory = tempfile::tempdir().unwrap();
        let config =
            MetadataExecutionConfig::new(ExecutionMode::InMemory, directory.path(), 600, 2, 4, 600)
                .unwrap();
        let run = |workers| {
            let mut sink = RecordingSink::default();
            let execution = config.clone().with_workers(workers).unwrap();
            let result = run_metadata_verification_with_config_and_progress(
                &contracts,
                &prefilter,
                0.6,
                600,
                &mut sink,
                &execution,
                &NoopProgress,
            )
            .unwrap();
            (result, sink.0)
        };
        let sequential = run(1);
        let parallel = run(4);
        assert_eq!(parallel, sequential);
    }
}
