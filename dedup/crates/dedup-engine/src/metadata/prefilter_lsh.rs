use super::{ContractAnchors, TemplateFingerprint, fingerprint_bytes_equal};
use ahash::{AHashMap, RandomState};
use dedup_index::{ExternalRadix, LshProbeAccumulator, MemoryBudget, RadixRecord, SpillVolume};
use dedup_model::{
    ChainId, ContractId, DedupError, ErrorContext, ExecutionMode, MetadataPrefilterParameters,
    NoopProgress, ProgressObserver, StageCounters,
};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CandidateEvidence {
    pub exact_template_digest_match: bool,
    pub shared_feature_count: u32,
    pub lsh_band_matches: u32,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetadataCandidate {
    pub left: ContractId,
    pub right: ContractId,
}

impl MetadataCandidate {
    pub fn new(left: ContractId, right: ContractId) -> Option<Self> {
        (left != right).then(|| {
            if left < right {
                Self { left, right }
            } else {
                Self {
                    left: right,
                    right: left,
                }
            }
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrefilterAudit {
    pub eligible_contracts: u64,
    pub low_information_contracts: u64,
    pub planned_probes: u64,
    pub emitted_probes: u64,
    pub probe_budget_truncations: u64,
    pub exact_bucket_pairs_possible: u64,
    pub exact_bucket_pairs_generated: u64,
    pub exact_bucket_cap_truncations: u64,
    pub generated_pairs_before_quota: BTreeSet<MetadataCandidate>,
    pub quota_truncations: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefilterResult {
    pub candidates: MetadataCandidateSet,
    pub audit: PrefilterAudit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataCandidateSet {
    Resident(Vec<MetadataCandidate>),
    External { path: PathBuf, count: u64 },
}

impl MetadataCandidateSet {
    #[must_use]
    pub fn count(&self) -> u64 {
        match self {
            Self::Resident(candidates) => u64::try_from(candidates.len()).unwrap_or(u64::MAX),
            Self::External { count, .. } => *count,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    #[must_use]
    pub fn resident(&self) -> Option<&[MetadataCandidate]> {
        match self {
            Self::Resident(candidates) => Some(candidates),
            Self::External { .. } => None,
        }
    }

    pub fn visit(
        &self,
        mut visitor: impl FnMut(MetadataCandidate) -> Result<(), DedupError>,
    ) -> Result<(), DedupError> {
        match self {
            Self::Resident(candidates) => {
                for candidate in candidates {
                    visitor(*candidate)?;
                }
            }
            Self::External { path, count } => {
                let mut reader = BufReader::new(File::open(path)?);
                let mut previous = None;
                for _ in 0..*count {
                    let candidate = read_candidate(&mut reader)?;
                    if previous.is_some_and(|value| value >= candidate) {
                        return Err(DedupError::ArtifactMismatch {
                            context: ErrorContext::stage("metadata_candidates"),
                            message: "external candidates are not strictly sorted".to_owned(),
                        });
                    }
                    visitor(candidate)?;
                    previous = Some(candidate);
                }
                let mut trailing = [0_u8; 1];
                if reader.read(&mut trailing)? != 0 {
                    return Err(DedupError::ArtifactMismatch {
                        context: ErrorContext::stage("metadata_candidates"),
                        message: "external candidate file has trailing bytes".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn to_vec(&self) -> Result<Vec<MetadataCandidate>, DedupError> {
        let capacity =
            usize::try_from(self.count()).map_err(|_| DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("metadata_candidates"),
                requested: self.count().saturating_mul(16),
            })?;
        let mut candidates = Vec::with_capacity(capacity);
        self.visit(|candidate| {
            candidates.push(candidate);
            Ok(())
        })?;
        Ok(candidates)
    }
}

impl From<Vec<MetadataCandidate>> for MetadataCandidateSet {
    fn from(candidates: Vec<MetadataCandidate>) -> Self {
        Self::Resident(candidates)
    }
}

#[derive(Clone, Debug)]
pub struct MetadataPrefilterExecutionConfig {
    pub mode: ExecutionMode,
    pub spill_root: PathBuf,
    pub radix_partition_bits: u8,
    pub max_open_spill_files: usize,
    pub max_records_per_partition: usize,
    pub radix_memory_budget: Option<(MemoryBudget, u64)>,
    pub radix_volumes: Vec<SpillVolume>,
}

impl MetadataPrefilterExecutionConfig {
    pub fn new(
        mode: ExecutionMode,
        spill_root: impl Into<PathBuf>,
        radix_partition_bits: u8,
        max_open_spill_files: usize,
        max_records_per_partition: usize,
    ) -> Result<Self, DedupError> {
        if max_open_spill_files == 0 || max_records_per_partition == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_prefilter"),
                message: "metadata prefilter spill capacities must be positive".to_owned(),
            });
        }
        let spill_root = spill_root.into();
        Ok(Self {
            mode,
            spill_root: spill_root.clone(),
            radix_partition_bits,
            max_open_spill_files,
            max_records_per_partition,
            radix_memory_budget: None,
            radix_volumes: vec![SpillVolume::new(spill_root, 1)?],
        })
    }

    pub fn with_radix_memory_budget(
        mut self,
        budget: MemoryBudget,
        bytes: u64,
    ) -> Result<Self, DedupError> {
        if bytes == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_prefilter"),
                message: "metadata prefilter radix memory must be positive".to_owned(),
            });
        }
        self.radix_memory_budget = Some((budget, bytes));
        Ok(self)
    }

    pub fn with_radix_volumes(mut self, volumes: Vec<SpillVolume>) -> Result<Self, DedupError> {
        if volumes.is_empty() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_prefilter"),
                message: "metadata prefilter radix requires a temporary volume".to_owned(),
            });
        }
        self.radix_volumes = volumes;
        Ok(self)
    }
}

pub struct MetadataPrefilterRequest<'a> {
    pub contracts: &'a [ContractAnchors],
    pub templates: &'a [TemplateFingerprint],
    pub parameters: &'a MetadataPrefilterParameters,
    pub probe_budget: u64,
    pub audited_pairs: Option<&'a BTreeSet<MetadataCandidate>>,
    pub execution: Option<&'a MetadataPrefilterExecutionConfig>,
}

pub fn generate_metadata_candidates(
    contracts: &[ContractAnchors],
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
    probe_budget: u64,
    counters: &mut StageCounters,
) -> Result<PrefilterResult, DedupError> {
    generate_metadata_candidates_with_progress(
        contracts,
        templates,
        parameters,
        probe_budget,
        counters,
        &NoopProgress,
    )
}

pub fn generate_metadata_candidates_with_progress(
    contracts: &[ContractAnchors],
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
    probe_budget: u64,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<PrefilterResult, DedupError> {
    generate_metadata_candidates_internal(
        MetadataPrefilterRequest {
            contracts,
            templates,
            parameters,
            probe_budget,
            audited_pairs: None,
            execution: None,
        },
        counters,
        PrequotaCapture::All,
        progress,
    )
}

pub fn generate_metadata_candidates_without_audit_pairs_with_progress(
    contracts: &[ContractAnchors],
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
    probe_budget: u64,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<PrefilterResult, DedupError> {
    generate_metadata_candidates_internal(
        MetadataPrefilterRequest {
            contracts,
            templates,
            parameters,
            probe_budget,
            audited_pairs: None,
            execution: None,
        },
        counters,
        PrequotaCapture::None,
        progress,
    )
}

pub fn generate_metadata_candidates_with_execution_and_progress(
    contracts: &[ContractAnchors],
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
    probe_budget: u64,
    counters: &mut StageCounters,
    execution: &MetadataPrefilterExecutionConfig,
    progress: &dyn ProgressObserver,
) -> Result<PrefilterResult, DedupError> {
    generate_metadata_candidates_internal(
        MetadataPrefilterRequest {
            contracts,
            templates,
            parameters,
            probe_budget,
            audited_pairs: None,
            execution: Some(execution),
        },
        counters,
        PrequotaCapture::None,
        progress,
    )
}

pub fn generate_metadata_candidates_for_audit_with_progress(
    contracts: &[ContractAnchors],
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
    probe_budget: u64,
    counters: &mut StageCounters,
    audited_pairs: &BTreeSet<MetadataCandidate>,
    progress: &dyn ProgressObserver,
) -> Result<PrefilterResult, DedupError> {
    generate_metadata_candidates_internal(
        MetadataPrefilterRequest {
            contracts,
            templates,
            parameters,
            probe_budget,
            audited_pairs: Some(audited_pairs),
            execution: None,
        },
        counters,
        PrequotaCapture::Selected(audited_pairs),
        progress,
    )
}

pub fn generate_metadata_candidates_for_request_with_progress(
    request: MetadataPrefilterRequest<'_>,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<PrefilterResult, DedupError> {
    let capture = request
        .audited_pairs
        .map_or(PrequotaCapture::None, PrequotaCapture::Selected);
    generate_metadata_candidates_internal(request, counters, capture, progress)
}

#[derive(Clone, Copy)]
enum PrequotaCapture<'a> {
    All,
    None,
    Selected(&'a BTreeSet<MetadataCandidate>),
}

enum EvidenceStore {
    Resident(AHashMap<MetadataCandidate, CandidateEvidence>),
    External(ExternalEvidenceWriter),
}

impl EvidenceStore {
    fn new(
        execution: Option<&MetadataPrefilterExecutionConfig>,
        pair_budget: u64,
    ) -> Result<Self, DedupError> {
        let Some(execution) = execution.filter(|config| {
            matches!(config.mode, ExecutionMode::Hybrid | ExecutionMode::External)
        }) else {
            return Ok(Self::Resident(AHashMap::with_hasher(
                RandomState::with_seeds(121, 122, 123, 124),
            )));
        };
        let radix = create_prefilter_radix(execution, "candidate-evidence-raw")?;
        Ok(Self::External(ExternalEvidenceWriter {
            radix: Some(radix),
            emitted: 0,
            pair_budget,
        }))
    }

    fn add_exact(&mut self, candidate: MetadataCandidate) -> Result<(), DedupError> {
        match self {
            Self::Resident(evidence) => {
                evidence
                    .entry(candidate)
                    .or_default()
                    .exact_template_digest_match = true;
                Ok(())
            }
            Self::External(writer) => writer.push(candidate, true),
        }
    }

    fn add_lsh(&mut self, candidate: MetadataCandidate) -> Result<(), DedupError> {
        match self {
            Self::Resident(evidence) => {
                let entry = evidence.entry(candidate).or_default();
                entry.lsh_band_matches =
                    entry
                        .lsh_band_matches
                        .checked_add(1)
                        .ok_or(DedupError::CounterOverflow {
                            counter: "metadata_lsh_band_matches",
                        })?;
                Ok(())
            }
            Self::External(writer) => writer.push(candidate, false),
        }
    }
}

struct ExternalEvidenceWriter {
    radix: Option<ExternalRadix>,
    emitted: u64,
    pair_budget: u64,
}

impl ExternalEvidenceWriter {
    fn push(&mut self, candidate: MetadataCandidate, exact: bool) -> Result<(), DedupError> {
        if self.emitted >= self.pair_budget {
            return Err(DedupError::BudgetExhausted {
                context: ErrorContext::stage("metadata_prefilter"),
                counter: "metadata_prefilter_pairs",
                limit: self.pair_budget,
            });
        }
        self.radix
            .as_mut()
            .ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("metadata_prefilter"),
                message: "candidate evidence radix was already consumed".to_owned(),
            })?
            .push(RadixRecord {
                key: candidate.left.as_u64(),
                payload: [
                    candidate.right.as_u64(),
                    u64::from(exact),
                    u64::from(!exact),
                ],
            })?;
        self.emitted = self
            .emitted
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_prefilter_pairs",
            })?;
        Ok(())
    }
}

fn create_prefilter_radix(
    execution: &MetadataPrefilterExecutionConfig,
    name: &str,
) -> Result<ExternalRadix, DedupError> {
    let volumes = execution
        .radix_volumes
        .iter()
        .map(|volume| SpillVolume::new(volume.root.join(name), volume.weight))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some((budget, bytes)) = &execution.radix_memory_budget {
        ExternalRadix::create_budgeted_striped(
            volumes,
            execution.radix_partition_bits,
            execution.max_open_spill_files,
            execution.max_records_per_partition,
            budget,
            *bytes,
        )
    } else {
        ExternalRadix::create_striped(
            volumes,
            execution.radix_partition_bits,
            execution.max_open_spill_files,
            execution.max_records_per_partition,
        )
    }
}

fn generate_metadata_candidates_internal(
    request: MetadataPrefilterRequest<'_>,
    counters: &mut StageCounters,
    capture_prequota_pairs: PrequotaCapture<'_>,
    progress: &dyn ProgressObserver,
) -> Result<PrefilterResult, DedupError> {
    let MetadataPrefilterRequest {
        contracts,
        templates,
        parameters,
        probe_budget,
        execution,
        ..
    } = request;
    validate_inputs(contracts, templates, parameters)?;
    let chain_by_contract: BTreeMap<ContractId, ChainId> = contracts
        .iter()
        .map(|contract| (contract.contract_id, contract.chain_id))
        .collect();
    let template_by_contract: BTreeMap<ContractId, &TemplateFingerprint> = templates
        .iter()
        .map(|template| (template.contract_id, template))
        .collect();
    let mut audit = PrefilterAudit {
        low_information_contracts: u64::try_from(
            templates
                .iter()
                .filter(|template| template.low_information)
                .count(),
        )
        .map_err(|_| DedupError::CounterOverflow {
            counter: "metadata_low_information_contracts",
        })?,
        ..PrefilterAudit::default()
    };
    let mut evidence = EvidenceStore::new(execution, probe_budget)?;
    progress.begin_phase(
        "metadata_exact_template_buckets",
        u64::try_from(templates.len()).ok(),
    );
    let resolved_exact = exact_bucket_candidates(
        templates,
        parameters,
        &template_by_contract,
        &mut evidence,
        &mut audit,
        progress,
    )?;

    let eligible: Vec<&TemplateFingerprint> = templates
        .iter()
        .filter(|template| {
            !template.low_information && !resolved_exact.contains(&template.contract_id)
        })
        .collect();
    audit.eligible_contracts =
        u64::try_from(eligible.len()).map_err(|_| DedupError::CounterOverflow {
            counter: "metadata_prefilter_eligible",
        })?;
    audit.planned_probes = audit
        .eligible_contracts
        .checked_mul(u64::from(parameters.lsh_bands))
        .ok_or(DedupError::CounterOverflow {
            counter: "metadata_prefilter_probes",
        })?;
    let permitted_contracts = if parameters.lsh_bands == 0 {
        0
    } else {
        usize::try_from(probe_budget / u64::from(parameters.lsh_bands)).unwrap_or(usize::MAX)
    };
    let active_eligible = &eligible[..eligible.len().min(permitted_contracts)];
    audit.emitted_probes = u64::try_from(active_eligible.len())
        .map_err(|_| DedupError::CounterOverflow {
            counter: "metadata_prefilter_probes",
        })?
        .checked_mul(u64::from(parameters.lsh_bands))
        .ok_or(DedupError::CounterOverflow {
            counter: "metadata_prefilter_probes",
        })?;
    audit.probe_budget_truncations = audit.planned_probes - audit.emitted_probes;
    counters.metadata_prefilter_probes(audit.emitted_probes)?;
    progress.begin_phase("metadata_lsh_probes", Some(audit.emitted_probes));
    let mut lsh_context = LshContext {
        chain_by_contract: &chain_by_contract,
        parameters,
        counters,
        execution,
        progress,
    };
    lsh_candidates(active_eligible, &mut evidence, &mut lsh_context)?;

    let mut evidence = match evidence {
        EvidenceStore::Resident(evidence) => evidence,
        external @ EvidenceStore::External(_) => {
            let execution = execution.ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("metadata_prefilter"),
                message: "external evidence store has no execution configuration".to_owned(),
            })?;
            return finish_external_evidence(
                external,
                ExternalFinishContext {
                    chain_by_contract: &chain_by_contract,
                    template_by_contract: &template_by_contract,
                    parameters,
                    capture: capture_prequota_pairs,
                    audit: &mut audit,
                    counters,
                    execution,
                    progress,
                },
            );
        }
    };
    progress.begin_phase(
        "metadata_candidate_evidence",
        u64::try_from(evidence.len()).ok(),
    );
    let mut evidence_work = 0_u64;
    for (candidate, candidate_evidence) in &mut evidence {
        evidence_work = evidence_work.saturating_add(1);
        if evidence_work == 256 {
            progress.advance(evidence_work);
            progress.check_cancelled("metadata_prefilter")?;
            evidence_work = 0;
        }
        candidate_evidence.shared_feature_count = shared_feature_count(
            template_by_contract[&candidate.left],
            template_by_contract[&candidate.right],
        )?;
    }
    progress.advance(evidence_work);
    progress.check_cancelled("metadata_prefilter")?;
    audit.generated_pairs_before_quota = match capture_prequota_pairs {
        PrequotaCapture::All => evidence.keys().copied().collect(),
        PrequotaCapture::None => BTreeSet::new(),
        PrequotaCapture::Selected(pairs) => pairs
            .iter()
            .filter(|candidate| evidence.contains_key(candidate))
            .copied()
            .collect(),
    };
    let candidates = apply_quotas(
        &evidence,
        &chain_by_contract,
        parameters,
        &mut audit.quota_truncations,
    )?;
    counters.metadata_prefilter_candidates(u64::try_from(candidates.len()).map_err(|_| {
        DedupError::CounterOverflow {
            counter: "metadata_prefilter_candidates",
        }
    })?)?;
    Ok(PrefilterResult {
        candidates: candidates.into(),
        audit,
    })
}

struct ExternalFinishContext<'a> {
    chain_by_contract: &'a BTreeMap<ContractId, ChainId>,
    template_by_contract: &'a BTreeMap<ContractId, &'a TemplateFingerprint>,
    parameters: &'a MetadataPrefilterParameters,
    capture: PrequotaCapture<'a>,
    audit: &'a mut PrefilterAudit,
    counters: &'a mut StageCounters,
    execution: &'a MetadataPrefilterExecutionConfig,
    progress: &'a dyn ProgressObserver,
}

fn finish_external_evidence(
    evidence: EvidenceStore,
    context: ExternalFinishContext<'_>,
) -> Result<PrefilterResult, DedupError> {
    let EvidenceStore::External(mut writer) = evidence else {
        return Err(DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_prefilter"),
            message: "resident evidence reached the external reducer".to_owned(),
        });
    };
    let raw_radix = writer
        .radix
        .take()
        .ok_or_else(|| DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_prefilter"),
            message: "external evidence radix was already consumed".to_owned(),
        })?;
    let mut quota_radix = create_prefilter_radix(context.execution, "candidate-quota")?;
    let raw_stats = {
        let mut raw_reducer = RawEvidenceReducer::new(
            &mut quota_radix,
            context.template_by_contract,
            context.capture,
            context.audit,
        );
        let stats = raw_radix.finish_with_progress(
            context.progress,
            "metadata_candidate_evidence_sort",
            "metadata_candidate_evidence_reduce",
            |record| raw_reducer.push(record),
        )?;
        raw_reducer.finish()?;
        stats
    };

    let mut final_radix = create_prefilter_radix(context.execution, "candidate-final")?;
    let quota_stats = {
        let mut quota_reducer = QuotaReducer::new(
            &mut final_radix,
            context.chain_by_contract,
            context.parameters,
            &mut context.audit.quota_truncations,
        );
        let stats = quota_radix.finish_with_progress(
            context.progress,
            "metadata_candidate_quota_sort",
            "metadata_candidate_quota_reduce",
            |record| quota_reducer.push(record),
        )?;
        quota_reducer.finish_source();
        stats
    };

    let candidate_path = context.execution.spill_root.join("metadata-candidates.bin");
    let file = File::create(&candidate_path)?;
    let mut output = BufWriter::new(file);
    let mut previous = None;
    let mut candidate_count = 0_u64;
    let final_stats = final_radix.finish_with_progress(
        context.progress,
        "metadata_candidate_final_sort",
        "metadata_candidate_final_write",
        |record| {
            let candidate = candidate_from_record(record)?;
            if previous != Some(candidate) {
                write_candidate(&mut output, candidate)?;
                candidate_count =
                    candidate_count
                        .checked_add(1)
                        .ok_or(DedupError::CounterOverflow {
                            counter: "metadata_prefilter_candidates",
                        })?;
                previous = Some(candidate);
            }
            Ok(())
        },
    )?;
    output.flush()?;
    output.get_ref().sync_all()?;
    context
        .counters
        .metadata_prefilter_candidates(candidate_count)?;
    context.counters.metadata_radix_handle_touches(
        raw_stats
            .handle_touches
            .saturating_add(quota_stats.handle_touches)
            .saturating_add(final_stats.handle_touches),
    )?;
    context.counters.spill_bytes(
        raw_stats
            .spill_bytes
            .saturating_add(quota_stats.spill_bytes)
            .saturating_add(final_stats.spill_bytes)
            .saturating_add(candidate_count.saturating_mul(16)),
    )?;
    context.progress.check_cancelled("metadata_prefilter")?;
    Ok(PrefilterResult {
        candidates: MetadataCandidateSet::External {
            path: candidate_path,
            count: candidate_count,
        },
        audit: context.audit.clone(),
    })
}

struct RawEvidenceReducer<'a> {
    current: Option<MetadataCandidate>,
    evidence: CandidateEvidence,
    quota_radix: &'a mut ExternalRadix,
    templates: &'a BTreeMap<ContractId, &'a TemplateFingerprint>,
    capture: PrequotaCapture<'a>,
    audit: &'a mut PrefilterAudit,
}

impl<'a> RawEvidenceReducer<'a> {
    fn new(
        quota_radix: &'a mut ExternalRadix,
        templates: &'a BTreeMap<ContractId, &'a TemplateFingerprint>,
        capture: PrequotaCapture<'a>,
        audit: &'a mut PrefilterAudit,
    ) -> Self {
        Self {
            current: None,
            evidence: CandidateEvidence::default(),
            quota_radix,
            templates,
            capture,
            audit,
        }
    }

    fn push(&mut self, record: RadixRecord) -> Result<(), DedupError> {
        let candidate = candidate_from_record(record)?;
        if self.current.is_some_and(|current| current != candidate) {
            self.finish_candidate()?;
        }
        self.current = Some(candidate);
        self.evidence.exact_template_digest_match |= record.payload[1] != 0;
        self.evidence.lsh_band_matches = self
            .evidence
            .lsh_band_matches
            .checked_add(u32::try_from(record.payload[2]).map_err(|_| {
                DedupError::CounterOverflow {
                    counter: "metadata_lsh_band_matches",
                }
            })?)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_lsh_band_matches",
            })?;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), DedupError> {
        self.finish_candidate()
    }

    fn finish_candidate(&mut self) -> Result<(), DedupError> {
        let Some(candidate) = self.current.take() else {
            return Ok(());
        };
        self.evidence.shared_feature_count = shared_feature_count(
            self.templates[&candidate.left],
            self.templates[&candidate.right],
        )?;
        let capture = match self.capture {
            PrequotaCapture::All => true,
            PrequotaCapture::None => false,
            PrequotaCapture::Selected(pairs) => pairs.contains(&candidate),
        };
        if capture {
            self.audit.generated_pairs_before_quota.insert(candidate);
        }
        let exact_rank = u64::from(!self.evidence.exact_template_digest_match);
        let score_rank = (u64::from(u32::MAX - self.evidence.shared_feature_count) << 32)
            | u64::from(u32::MAX - self.evidence.lsh_band_matches);
        for (source, target) in [
            (candidate.left, candidate.right),
            (candidate.right, candidate.left),
        ] {
            self.quota_radix.push(RadixRecord {
                key: source.as_u64(),
                payload: [exact_rank, score_rank, target.as_u64()],
            })?;
        }
        self.evidence = CandidateEvidence::default();
        Ok(())
    }
}

struct QuotaReducer<'a> {
    current_source: Option<ContractId>,
    total: usize,
    per_chain: BTreeMap<ChainId, usize>,
    final_radix: &'a mut ExternalRadix,
    chain_by_contract: &'a BTreeMap<ContractId, ChainId>,
    parameters: &'a MetadataPrefilterParameters,
    truncations: &'a mut u64,
}

impl<'a> QuotaReducer<'a> {
    fn new(
        final_radix: &'a mut ExternalRadix,
        chain_by_contract: &'a BTreeMap<ContractId, ChainId>,
        parameters: &'a MetadataPrefilterParameters,
        truncations: &'a mut u64,
    ) -> Self {
        Self {
            current_source: None,
            total: 0,
            per_chain: BTreeMap::new(),
            final_radix,
            chain_by_contract,
            parameters,
            truncations,
        }
    }

    fn push(&mut self, record: RadixRecord) -> Result<(), DedupError> {
        let source = contract_id_from_u64(record.key)?;
        let target = contract_id_from_u64(record.payload[2])?;
        if self.current_source != Some(source) {
            self.finish_source();
            self.current_source = Some(source);
        }
        let chain_count = self
            .per_chain
            .entry(self.chain_by_contract[&target])
            .or_default();
        if self.total < self.parameters.max_outgoing_candidates_per_contract
            && *chain_count < self.parameters.max_candidates_per_target_chain
        {
            let candidate = MetadataCandidate::new(source, target).ok_or_else(|| {
                DedupError::InvariantViolation {
                    context: ErrorContext::stage("metadata_prefilter"),
                    message: "quota reducer received a self-pair".to_owned(),
                }
            })?;
            self.final_radix.push(RadixRecord {
                key: candidate.left.as_u64(),
                payload: [candidate.right.as_u64(), 0, 0],
            })?;
            self.total += 1;
            *chain_count += 1;
        } else {
            *self.truncations =
                self.truncations
                    .checked_add(1)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "metadata_quota_truncations",
                    })?;
        }
        Ok(())
    }

    fn finish_source(&mut self) {
        self.total = 0;
        self.per_chain.clear();
    }
}

fn candidate_from_record(record: RadixRecord) -> Result<MetadataCandidate, DedupError> {
    MetadataCandidate::new(
        contract_id_from_u64(record.key)?,
        contract_id_from_u64(record.payload[0])?,
    )
    .ok_or_else(|| DedupError::ArtifactMismatch {
        context: ErrorContext::stage("metadata_prefilter"),
        message: "candidate record contains a self-pair".to_owned(),
    })
}

fn contract_id_from_u64(value: u64) -> Result<ContractId, DedupError> {
    Ok(ContractId::new(
        dedup_model::EntityId::try_from(value).map_err(|_| DedupError::ArtifactMismatch {
            context: ErrorContext::stage("metadata_prefilter"),
            message: "ContractId exceeds configured EntityId".to_owned(),
        })?,
    ))
}

fn write_candidate(
    writer: &mut impl Write,
    candidate: MetadataCandidate,
) -> Result<(), DedupError> {
    writer.write_all(&candidate.left.as_u64().to_le_bytes())?;
    writer.write_all(&candidate.right.as_u64().to_le_bytes())?;
    Ok(())
}

fn validate_inputs(
    contracts: &[ContractAnchors],
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
) -> Result<(), DedupError> {
    if contracts.len() != templates.len()
        || contracts
            .iter()
            .zip(templates)
            .any(|(contract, template)| contract.contract_id != template.contract_id)
    {
        return Err(DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_prefilter"),
            message: "anchors and templates are not aligned by ContractId".to_owned(),
        });
    }
    if parameters.lsh_bands == 0
        || parameters.lsh_rows_per_band == 0
        || parameters.neighbors_per_target_chain == 0
        || parameters.max_outgoing_candidates_per_contract == 0
        || parameters.max_candidates_per_target_chain == 0
        || parameters.exact_bucket_size_cap == 0
    {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("metadata_prefilter"),
            message: "prefilter capacities and LSH dimensions must be positive".to_owned(),
        });
    }
    Ok(())
}

fn exact_bucket_candidates(
    templates: &[TemplateFingerprint],
    parameters: &MetadataPrefilterParameters,
    template_by_contract: &BTreeMap<ContractId, &TemplateFingerprint>,
    evidence: &mut EvidenceStore,
    audit: &mut PrefilterAudit,
    progress: &dyn ProgressObserver,
) -> Result<BTreeSet<ContractId>, DedupError> {
    let mut digest_buckets = ExactDigestBuckets::new();
    let mut template_work = 0_u64;
    for template in templates {
        template_work = template_work.saturating_add(1);
        if template_work == 256 {
            progress.advance(template_work);
            progress.check_cancelled("metadata_prefilter")?;
            template_work = 0;
        }
        if !template.low_information {
            digest_buckets
                .entry(template.template_digest)
                .or_default()
                .entry(template.fingerprint_bytes.clone())
                .or_default()
                .push(template.contract_id);
        }
    }
    progress.advance(template_work);
    progress.check_cancelled("metadata_prefilter")?;
    let mut resolved = BTreeSet::new();
    for real_buckets in digest_buckets.values() {
        for bucket in real_buckets.values() {
            if bucket.len() < 2 {
                continue;
            }
            let mut bucket = bucket.clone();
            bucket.sort_unstable();
            resolved.extend(bucket.iter().copied());
            let possible = choose_two_saturating(bucket.len());
            audit.exact_bucket_pairs_possible =
                audit.exact_bucket_pairs_possible.saturating_add(possible);
            let neighbor_cap = parameters
                .exact_bucket_size_cap
                .min(parameters.max_outgoing_candidates_per_contract)
                .min(bucket.len() - 1);
            let mut generated_candidates = BTreeSet::new();
            for (source_position, source) in bucket.iter().copied().enumerate() {
                for offset in 1..=neighbor_cap {
                    let target = bucket[(source_position + offset) % bucket.len()];
                    if let Some(candidate) = MetadataCandidate::new(source, target) {
                        debug_assert!(fingerprint_bytes_equal(
                            template_by_contract[&source],
                            template_by_contract[&target]
                        ));
                        generated_candidates.insert(candidate);
                    }
                }
            }
            let generated = u64::try_from(generated_candidates.len()).unwrap_or(u64::MAX);
            for candidate in generated_candidates {
                evidence.add_exact(candidate)?;
            }
            audit.exact_bucket_pairs_generated =
                audit.exact_bucket_pairs_generated.saturating_add(generated);
        }
    }
    audit.exact_bucket_cap_truncations = audit
        .exact_bucket_pairs_possible
        .saturating_sub(audit.exact_bucket_pairs_generated);
    Ok(resolved)
}

struct LshContext<'a> {
    chain_by_contract: &'a BTreeMap<ContractId, ChainId>,
    parameters: &'a MetadataPrefilterParameters,
    counters: &'a mut StageCounters,
    execution: Option<&'a MetadataPrefilterExecutionConfig>,
    progress: &'a dyn ProgressObserver,
}

type ExactDigestBuckets = BTreeMap<[u8; 32], BTreeMap<Arc<[u8]>, Vec<ContractId>>>;

fn lsh_candidates(
    templates: &[&TemplateFingerprint],
    evidence: &mut EvidenceStore,
    context: &mut LshContext<'_>,
) -> Result<(), DedupError> {
    let parameters = context.parameters;
    let signature_length = usize::try_from(
        parameters
            .lsh_bands
            .checked_mul(parameters.lsh_rows_per_band)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_minhash_length",
            })?,
    )
    .map_err(|_| DedupError::InvalidInput {
        context: ErrorContext::stage("metadata_prefilter"),
        message: "MinHash signature length does not fit usize".to_owned(),
    })?;
    if let Some(execution) = context.execution.cloned()
        && matches!(
            execution.mode,
            ExecutionMode::Hybrid | ExecutionMode::External
        )
    {
        return lsh_candidates_external(templates, evidence, &execution, signature_length, context);
    }
    let probe_accumulator_capacity = templates.len().max(1);
    let mut band_buckets: BTreeMap<(u32, u64), LshProbeAccumulator<ContractId>> = BTreeMap::new();
    for template in templates {
        let signature = minhash_signature(template, signature_length);
        let rows = usize::try_from(parameters.lsh_rows_per_band).map_err(|_| {
            DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_prefilter"),
                message: "rows per band does not fit usize".to_owned(),
            }
        })?;
        for (band, values) in signature.chunks_exact(rows).enumerate() {
            let band = u32::try_from(band).map_err(|_| DedupError::CounterOverflow {
                counter: "metadata_lsh_band",
            })?;
            band_buckets
                .entry((band, hash_band(values)))
                .or_insert(LshProbeAccumulator::new(probe_accumulator_capacity)?)
                .push(template.contract_id)
                .map_err(|_| DedupError::BudgetExhausted {
                    context: ErrorContext::stage("metadata_prefilter"),
                    counter: "metadata_lsh_bucket_members",
                    limit: u64::try_from(probe_accumulator_capacity).unwrap_or(u64::MAX),
                })?;
        }
        context.progress.advance(u64::from(parameters.lsh_bands));
        context.progress.check_cancelled("metadata_prefilter")?;
    }
    for members in band_buckets.values_mut() {
        members.as_mut_slice().sort_unstable();
        let mut by_chain: BTreeMap<ChainId, Vec<ContractId>> = BTreeMap::new();
        for member in members.as_slice().iter().copied() {
            by_chain
                .entry(context.chain_by_contract[&member])
                .or_default()
                .push(member);
        }
        add_lsh_bucket_candidates(&by_chain, parameters, evidence)?;
    }
    Ok(())
}

fn lsh_candidates_external(
    templates: &[&TemplateFingerprint],
    evidence: &mut EvidenceStore,
    execution: &MetadataPrefilterExecutionConfig,
    signature_length: usize,
    context: &mut LshContext<'_>,
) -> Result<(), DedupError> {
    let parameters = context.parameters;
    let mut radix = create_prefilter_radix(execution, "lsh-probes")?;
    let rows =
        usize::try_from(parameters.lsh_rows_per_band).map_err(|_| DedupError::InvalidInput {
            context: ErrorContext::stage("metadata_prefilter"),
            message: "rows per band does not fit usize".to_owned(),
        })?;
    for template in templates {
        let signature = minhash_signature(template, signature_length);
        for (band, values) in signature.chunks_exact(rows).enumerate() {
            radix.push(RadixRecord {
                key: hash_band(values),
                payload: [
                    u64::try_from(band).map_err(|_| DedupError::CounterOverflow {
                        counter: "metadata_lsh_band",
                    })?,
                    template.contract_id.as_u64(),
                    0,
                ],
            })?;
        }
        context.progress.advance(u64::from(parameters.lsh_bands));
        context.progress.check_cancelled("metadata_prefilter")?;
    }
    let mut reducer = ExternalLshReducer::new(
        templates.len().max(1),
        context.chain_by_contract,
        parameters,
        evidence,
    )?;
    let stats = radix.finish_with_progress(
        context.progress,
        "metadata_lsh_external_sort",
        "metadata_lsh_external_reduce",
        |record| reducer.push(record),
    )?;
    reducer.finish()?;
    context
        .counters
        .metadata_radix_handle_touches(stats.handle_touches)?;
    context.counters.spill_bytes(stats.spill_bytes)?;
    Ok(())
}

struct ExternalLshReducer<'a> {
    current_bucket: Option<(u64, u64)>,
    members: LshProbeAccumulator<ContractId>,
    chain_by_contract: &'a BTreeMap<ContractId, ChainId>,
    parameters: &'a MetadataPrefilterParameters,
    evidence: &'a mut EvidenceStore,
}

impl<'a> ExternalLshReducer<'a> {
    fn new(
        capacity: usize,
        chain_by_contract: &'a BTreeMap<ContractId, ChainId>,
        parameters: &'a MetadataPrefilterParameters,
        evidence: &'a mut EvidenceStore,
    ) -> Result<Self, DedupError> {
        Ok(Self {
            current_bucket: None,
            members: LshProbeAccumulator::new(capacity)?,
            chain_by_contract,
            parameters,
            evidence,
        })
    }

    fn push(&mut self, record: RadixRecord) -> Result<(), DedupError> {
        let bucket = (record.key, record.payload[0]);
        if self.current_bucket.is_some_and(|current| current != bucket) {
            self.finish_bucket()?;
        }
        self.current_bucket = Some(bucket);
        let contract = ContractId::new(
            dedup_model::EntityId::try_from(record.payload[1]).map_err(|_| {
                DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("metadata_prefilter"),
                    message: "external LSH ContractId exceeds configured EntityId".to_owned(),
                }
            })?,
        );
        self.members
            .push(contract)
            .map_err(|_| DedupError::BudgetExhausted {
                context: ErrorContext::stage("metadata_prefilter"),
                counter: "metadata_lsh_bucket_members",
                limit: u64::try_from(self.members.capacity()).unwrap_or(u64::MAX),
            })
    }

    fn finish(mut self) -> Result<(), DedupError> {
        self.finish_bucket()
    }

    fn finish_bucket(&mut self) -> Result<(), DedupError> {
        if self.current_bucket.is_none() {
            return Ok(());
        }
        let mut by_chain: BTreeMap<ChainId, Vec<ContractId>> = BTreeMap::new();
        for member in self.members.as_slice().iter().copied() {
            by_chain
                .entry(self.chain_by_contract[&member])
                .or_default()
                .push(member);
        }
        add_lsh_bucket_candidates(&by_chain, self.parameters, self.evidence)?;
        self.members.clear();
        self.current_bucket = None;
        Ok(())
    }
}

fn add_lsh_bucket_candidates(
    by_chain: &BTreeMap<ChainId, Vec<ContractId>>,
    parameters: &MetadataPrefilterParameters,
    evidence: &mut EvidenceStore,
) -> Result<(), DedupError> {
    let mut bucket_candidates = BTreeSet::new();
    for chain_members in by_chain.values() {
        add_merge_neighbors(
            chain_members,
            chain_members,
            parameters.neighbors_per_target_chain,
            &mut bucket_candidates,
        );
    }
    let chains: Vec<_> = by_chain.values().collect();
    for left_chain in 0..chains.len() {
        for right_chain in left_chain + 1..chains.len() {
            add_merge_neighbors(
                chains[left_chain],
                chains[right_chain],
                parameters.neighbors_per_target_chain,
                &mut bucket_candidates,
            );
            add_merge_neighbors(
                chains[right_chain],
                chains[left_chain],
                parameters.neighbors_per_target_chain,
                &mut bucket_candidates,
            );
        }
    }
    for candidate in bucket_candidates {
        evidence.add_lsh(candidate)?;
    }
    Ok(())
}

fn minhash_signature(template: &TemplateFingerprint, length: usize) -> Vec<u64> {
    let base_hashes: Vec<u64> = template
        .feature_tokens
        .iter()
        .map(|feature| {
            let digest = Sha256::digest(feature);
            u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 prefix has 8 bytes"))
        })
        .collect();
    (0..length)
        .map(|permutation| {
            let seed = splitmix64(permutation as u64 ^ 0x9e37_79b9_7f4a_7c15);
            base_hashes
                .iter()
                .map(|hash| splitmix64(*hash ^ seed))
                .min()
                .unwrap_or(u64::MAX)
        })
        .collect()
}

fn hash_band(values: &[u64]) -> u64 {
    values.iter().fold(0x243f_6a88_85a3_08d3, |state, value| {
        splitmix64(state ^ value)
    })
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn add_merge_neighbors(
    sources: &[ContractId],
    targets: &[ContractId],
    neighbor_limit: usize,
    candidates: &mut BTreeSet<MetadataCandidate>,
) {
    for source in sources {
        let position = targets.partition_point(|target| target < source);
        let mut lower = position;
        let mut upper = position;
        let available = targets
            .len()
            .saturating_sub(usize::from(targets.binary_search(source).is_ok()));
        let target_count = neighbor_limit.min(available);
        let mut emitted = 0;
        while emitted < target_count {
            let previous = lower.checked_sub(1).and_then(|index| targets.get(index));
            let next = targets.get(upper);
            let target = match (previous, next) {
                (Some(previous), Some(next)) => {
                    let source_value = source.as_u64();
                    let previous_rank = (source_value.abs_diff(previous.as_u64()), *previous);
                    let next_rank = (source_value.abs_diff(next.as_u64()), *next);
                    if previous_rank <= next_rank {
                        lower -= 1;
                        previous
                    } else {
                        upper += 1;
                        next
                    }
                }
                (Some(previous), None) => {
                    lower -= 1;
                    previous
                }
                (None, Some(next)) => {
                    upper += 1;
                    next
                }
                (None, None) => break,
            };
            if let Some(candidate) = MetadataCandidate::new(*source, *target) {
                candidates.insert(candidate);
                emitted += 1;
            }
        }
    }
}

fn shared_feature_count(
    left: &TemplateFingerprint,
    right: &TemplateFingerprint,
) -> Result<u32, DedupError> {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0_u32;
    while left_index < left.feature_tokens.len() && right_index < right.feature_tokens.len() {
        match left.feature_tokens[left_index].cmp(&right.feature_tokens[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                count = count.checked_add(1).ok_or(DedupError::CounterOverflow {
                    counter: "metadata_shared_features",
                })?;
                left_index += 1;
                right_index += 1;
            }
        }
    }
    Ok(count)
}

fn apply_quotas(
    evidence: &AHashMap<MetadataCandidate, CandidateEvidence>,
    chain_by_contract: &BTreeMap<ContractId, ChainId>,
    parameters: &MetadataPrefilterParameters,
    truncations: &mut u64,
) -> Result<Vec<MetadataCandidate>, DedupError> {
    let mut outgoing: BTreeMap<
        ContractId,
        Vec<(ContractId, MetadataCandidate, CandidateEvidence)>,
    > = BTreeMap::new();
    for (candidate, candidate_evidence) in evidence {
        outgoing.entry(candidate.left).or_default().push((
            candidate.right,
            *candidate,
            *candidate_evidence,
        ));
        outgoing.entry(candidate.right).or_default().push((
            candidate.left,
            *candidate,
            *candidate_evidence,
        ));
    }
    let mut retained = BTreeSet::new();
    for candidates in outgoing.values_mut() {
        candidates.sort_unstable_by_key(|(target, _, candidate_evidence)| {
            (
                Reverse(candidate_evidence.exact_template_digest_match),
                Reverse(candidate_evidence.shared_feature_count),
                Reverse(candidate_evidence.lsh_band_matches),
                *target,
            )
        });
        let mut per_chain: BTreeMap<ChainId, usize> = BTreeMap::new();
        let mut total = 0_usize;
        for (target, candidate, _) in candidates.iter() {
            let chain_count = per_chain.entry(chain_by_contract[target]).or_default();
            if total < parameters.max_outgoing_candidates_per_contract
                && *chain_count < parameters.max_candidates_per_target_chain
            {
                retained.insert(*candidate);
                total += 1;
                *chain_count += 1;
            } else {
                *truncations = truncations
                    .checked_add(1)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "metadata_quota_truncations",
                    })?;
            }
        }
    }
    Ok(retained.into_iter().collect())
}

fn choose_two_saturating(count: usize) -> u64 {
    let count = count as u128;
    u64::try_from(count.saturating_mul(count.saturating_sub(1)) / 2).unwrap_or(u64::MAX)
}

fn read_candidate(reader: &mut impl Read) -> Result<MetadataCandidate, DedupError> {
    let mut left = [0_u8; 8];
    let mut right = [0_u8; 8];
    reader.read_exact(&mut left)?;
    reader.read_exact(&mut right)?;
    MetadataCandidate::new(
        ContractId::new(
            dedup_model::EntityId::try_from(u64::from_le_bytes(left)).map_err(|_| {
                DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("metadata_candidates"),
                    message: "left ContractId exceeds configured EntityId".to_owned(),
                }
            })?,
        ),
        ContractId::new(
            dedup_model::EntityId::try_from(u64::from_le_bytes(right)).map_err(|_| {
                DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("metadata_candidates"),
                    message: "right ContractId exceeds configured EntityId".to_owned(),
                }
            })?,
        ),
    )
    .ok_or_else(|| DedupError::ArtifactMismatch {
        context: ErrorContext::stage("metadata_candidates"),
        message: "external candidate is a self-pair".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        MetadataRecord, TemplateGuard, build_template_fingerprints, select_anchors,
    };
    use dedup_model::StageCounters;

    fn fixtures(
        contract_count: u32,
        identical_collection: bool,
    ) -> (Vec<ContractAnchors>, Vec<TemplateFingerprint>) {
        let mut records = Vec::new();
        for contract in 0..contract_count {
            for token in 0..2 {
                let collection = if identical_collection { 0 } else { contract };
                records.push(MetadataRecord {
                    doc_id: dedup_model::MetadataDocId::new(dedup_model::EntityId::from(
                        contract * 2 + token,
                    )),
                    contract_id: ContractId::new(dedup_model::EntityId::from(contract)),
                    chain_id: ChainId::new((contract % 2) as u16),
                    token_id: token.to_string(),
                    content: format!(
                        r#"{{"collection":{{"name":"c{collection}"}},"name":"n{token}"}}"#
                    ),
                });
            }
        }
        let mut counters = StageCounters::default();
        let anchors = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0), ChainId::new(1)]),
            2,
            &mut counters,
        )
        .unwrap();
        let templates = build_template_fingerprints(
            &anchors,
            TemplateGuard {
                min_anchor_documents: 2,
                stable_value_min_anchors: 2,
                stable_value_support_ratio: 0.8,
            },
            &mut counters,
        )
        .unwrap();
        (anchors, templates)
    }

    fn parameters(cap: usize) -> MetadataPrefilterParameters {
        MetadataPrefilterParameters {
            template_jaccard_threshold: 0.75,
            lsh_bands: 8,
            lsh_rows_per_band: 2,
            target_candidate_recall: 0.9,
            neighbors_per_target_chain: 4,
            max_candidates_per_target_chain: cap,
            max_outgoing_candidates_per_contract: cap,
            exact_bucket_size_cap: cap,
        }
    }

    #[test]
    fn huge_exact_bucket_is_linear_in_bucket_times_cap() {
        let (anchors, templates) = fixtures(200, true);
        let mut counters = StageCounters::default();
        let result = generate_metadata_candidates(
            &anchors,
            &templates,
            &parameters(3),
            10_000,
            &mut counters,
        )
        .unwrap();
        assert!(result.audit.exact_bucket_pairs_possible > 10_000);
        assert!(result.audit.exact_bucket_pairs_generated <= 200 * 3);
        assert!(result.candidates.count() <= 200 * 3);
    }

    #[test]
    fn low_information_contracts_emit_no_probe_or_candidate() {
        let records = (0..4)
            .map(|contract| MetadataRecord {
                doc_id: dedup_model::MetadataDocId::new(contract),
                contract_id: ContractId::new(contract),
                chain_id: ChainId::new(0),
                token_id: "0".to_owned(),
                content: r#"{"name":"placeholder"}"#.to_owned(),
            })
            .collect();
        let mut counters = StageCounters::default();
        let anchors = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0)]),
            1,
            &mut counters,
        )
        .unwrap();
        let templates = build_template_fingerprints(
            &anchors,
            TemplateGuard {
                min_anchor_documents: 2,
                stable_value_min_anchors: 2,
                stable_value_support_ratio: 0.8,
            },
            &mut counters,
        )
        .unwrap();
        let result =
            generate_metadata_candidates(&anchors, &templates, &parameters(3), 100, &mut counters)
                .unwrap();
        assert_eq!(result.audit.emitted_probes, 0);
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn merge_neighbor_count_is_driven_by_configuration() {
        let ids = |values: &[u64]| {
            values
                .iter()
                .copied()
                .map(|value| ContractId::new(dedup_model::EntityId::try_from(value).unwrap()))
                .collect::<Vec<_>>()
        };
        let sources = ids(&[10, 20]);
        let targets = ids(&[1, 11, 21, 31]);
        let mut one_neighbor = BTreeSet::new();
        add_merge_neighbors(&sources, &targets, 1, &mut one_neighbor);
        assert_eq!(one_neighbor.len(), 2);

        let mut three_neighbors = BTreeSet::new();
        add_merge_neighbors(&sources, &targets, 3, &mut three_neighbors);
        assert_eq!(three_neighbors.len(), 6);
        for source in sources {
            assert_eq!(
                three_neighbors
                    .iter()
                    .filter(|candidate| candidate.left == source || candidate.right == source)
                    .count(),
                3
            );
        }

        let mut same_chain = BTreeSet::new();
        add_merge_neighbors(&targets, &targets, 2, &mut same_chain);
        for source in targets {
            assert!(
                same_chain
                    .iter()
                    .filter(|candidate| candidate.left == source || candidate.right == source)
                    .count()
                    >= 2
            );
        }
    }

    #[test]
    fn external_lsh_probe_sort_matches_resident_candidates() {
        let (anchors, templates) = fixtures(24, false);
        let parameters = parameters(6);
        let mut resident_counters = StageCounters::default();
        let resident = generate_metadata_candidates_without_audit_pairs_with_progress(
            &anchors,
            &templates,
            &parameters,
            10_000,
            &mut resident_counters,
            &NoopProgress,
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let execution = MetadataPrefilterExecutionConfig::new(
            ExecutionMode::External,
            directory.path(),
            4,
            16,
            10_000,
        )
        .unwrap();
        let mut external_counters = StageCounters::default();
        let external = generate_metadata_candidates_with_execution_and_progress(
            &anchors,
            &templates,
            &parameters,
            10_000,
            &mut external_counters,
            &execution,
            &NoopProgress,
        )
        .unwrap();
        let (candidate_path, candidate_count) = match &external.candidates {
            MetadataCandidateSet::External { path, count } => (path, *count),
            MetadataCandidateSet::Resident(_) => panic!("external plan retained candidates"),
        };
        assert_eq!(
            std::fs::metadata(candidate_path).unwrap().len(),
            candidate_count * 16
        );
        assert_eq!(
            external.candidates.to_vec().unwrap(),
            resident.candidates.to_vec().unwrap()
        );
        assert_eq!(external.audit, resident.audit);
        assert!(external_counters.metadata_radix_handle_touches > 0);
        assert!(external_counters.spill_bytes > 0);
    }
}
