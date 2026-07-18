use super::CandidateBounds;
use crate::parallel::RayonChunkExecutor;
use ahash::{AHashMap, AHashSet, RandomState};
use dedup_index::{CandidateBuffer, StringDictionary};
use dedup_model::{
    CanonicalNameId, ChainId, ChunkExecutor, Contract, ContractId, DedupError, Dimension, EntityId,
    EntityKind, ErrorContext, HitEvent, HitEventSink, NameAtomId, NoopProgress, ProgressObserver,
    ScopeId, StageCounters, StringId,
};
use rapidfuzz::distance::jaro_winkler::{Args, BatchComparator};
use std::collections::BTreeMap;

type OccurrenceToken = (char, u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NameAtom {
    pub id: NameAtomId,
    pub chain_id: ChainId,
    pub name_ref: StringId,
    pub contract_offset: u64,
    pub contract_count: u64,
    pub nft_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalName {
    pub id: CanonicalNameId,
    pub name_ref: StringId,
    pub characters: Vec<char>,
    pub character_counts: Vec<(char, u32)>,
    pub atom_ids: Vec<NameAtomId>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NameMatch {
    pub left: CanonicalNameId,
    pub right: CanonicalNameId,
    pub similarity: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NameRunResult {
    pub atoms: Vec<NameAtom>,
    pub contract_ids: Vec<ContractId>,
    pub canonical_names: Vec<CanonicalName>,
    pub fuzzy_matches: Vec<NameMatch>,
    pub counters: StageCounters,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateStorageMode {
    ResidentPostings,
    OverlapScan,
}

#[derive(Clone, Copy, Debug)]
pub struct NameEngineConfig {
    pub threshold: f64,
    pub candidate_storage: CandidateStorageMode,
    pub candidate_pair_budget: u64,
    pub score_budget: u64,
}

impl NameEngineConfig {
    pub fn production_default(score_budget: u64) -> Self {
        Self {
            threshold: 0.95,
            candidate_storage: CandidateStorageMode::ResidentPostings,
            candidate_pair_budget: score_budget.saturating_mul(4),
            score_budget,
        }
    }
}

pub fn run_name(
    contracts: &[Contract],
    strings: &StringDictionary,
    config: NameEngineConfig,
    sink: &mut impl HitEventSink,
) -> Result<NameRunResult, DedupError> {
    run_name_with_progress(contracts, strings, config, sink, &NoopProgress)
}

pub fn run_name_with_progress(
    contracts: &[Contract],
    strings: &StringDictionary,
    config: NameEngineConfig,
    sink: &mut impl HitEventSink,
    progress: &dyn ProgressObserver,
) -> Result<NameRunResult, DedupError> {
    run_name_with_progress_and_workers(contracts, strings, config, sink, progress, 1)
}

pub fn run_name_with_progress_and_workers(
    contracts: &[Contract],
    strings: &StringDictionary,
    config: NameEngineConfig,
    sink: &mut impl HitEventSink,
    progress: &dyn ProgressObserver,
    workers: usize,
) -> Result<NameRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(workers, "name")?;
    run_name_with_progress_and_executor(contracts, strings, config, sink, progress, &executor)
}

pub fn run_name_with_progress_and_executor(
    contracts: &[Contract],
    strings: &StringDictionary,
    config: NameEngineConfig,
    sink: &mut impl HitEventSink,
    progress: &dyn ProgressObserver,
    executor: &impl ChunkExecutor,
) -> Result<NameRunResult, DedupError> {
    if !(0.0..=1.0).contains(&config.threshold) {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("name"),
            message: "name threshold must be in [0, 1]".to_owned(),
        });
    }
    progress.begin_phase("atomize_contracts", checked_total(contracts.len()));
    let AtomizedNames {
        atoms,
        contract_ids,
        canonical_names,
    } = atomize(contracts, strings, progress)?;
    let mut counters = StageCounters::default();
    counters.name_atoms(u64::try_from(atoms.len()).map_err(|_| {
        DedupError::CounterOverflow {
            counter: "name_atoms",
        }
    })?)?;
    counters.name_canonical_values(u64::try_from(canonical_names.len()).map_err(|_| {
        DedupError::CounterOverflow {
            counter: "name_canonical_values",
        }
    })?)?;

    progress.begin_phase(
        "identical_name_groups",
        checked_total(canonical_names.len()),
    );
    emit_identical_groups(
        &atoms,
        &contract_ids,
        &canonical_names,
        sink,
        &mut counters,
        progress,
    )?;
    let optimized_bounds_are_safe = config.threshold >= 0.95;
    let candidate_pairs = if !optimized_bounds_are_safe {
        progress.begin_phase(
            "exhaustive_name_candidates",
            checked_total(canonical_names.len()),
        );
        exhaustive_candidates(
            canonical_names.len(),
            config.candidate_pair_budget,
            progress,
        )?
    } else {
        match config.candidate_storage {
            CandidateStorageMode::ResidentPostings => {
                progress.begin_phase("name_posting_touches", None);
                posting_candidates(
                    &canonical_names,
                    config.candidate_pair_budget,
                    &mut counters,
                    progress,
                )?
            }
            CandidateStorageMode::OverlapScan => {
                progress.begin_phase(
                    "overlap_scan_left_names",
                    checked_total(canonical_names.len()),
                );
                overlap_scan_candidates(&canonical_names, config.candidate_pair_budget, progress)?
            }
        }
    };

    let mut fuzzy_matches = Vec::new();
    progress.begin_phase(
        "score_name_candidates",
        checked_total(candidate_pairs.len()),
    );
    let scored_candidates = candidate_pairs
        .iter()
        .try_fold(0_u64, |count, (left, right)| {
            let should_score = !optimized_bounds_are_safe
                || passes_overlap(&canonical_names[*left], &canonical_names[*right]);
            count
                .checked_add(u64::from(should_score))
                .ok_or(DedupError::CounterOverflow {
                    counter: "name_scored_candidates",
                })
        })?;
    if scored_candidates > config.score_budget {
        return Err(DedupError::BudgetExhausted {
            context: ErrorContext::stage("name"),
            counter: "name_scored_candidates",
            limit: config.score_budget,
        });
    }
    counters.name_scored_candidates(scored_candidates)?;
    let scored_matches = score_name_candidates(
        &candidate_pairs,
        &canonical_names,
        optimized_bounds_are_safe,
        config.threshold,
        executor,
        progress,
    )?;
    for scored in scored_matches {
        let left_name = &canonical_names[scored.left];
        let right_name = &canonical_names[scored.right];
        counters.name_matched_pairs(1)?;
        fuzzy_matches.push(NameMatch {
            left: left_name.id,
            right: right_name.id,
            similarity: scored.similarity,
        });
        emit_canonical_pair(
            left_name,
            right_name,
            &atoms,
            &contract_ids,
            sink,
            &mut counters,
        )?;
    }
    Ok(NameRunResult {
        atoms,
        contract_ids,
        canonical_names,
        fuzzy_matches,
        counters,
    })
}

#[derive(Clone, Copy, Debug)]
struct ScoredNameMatch {
    left: usize,
    right: usize,
    similarity: f64,
}

#[derive(Clone, Copy, Debug)]
struct CandidateBatch {
    start: usize,
    end: usize,
}

fn score_name_candidates(
    candidates: &[(usize, usize)],
    names: &[CanonicalName],
    apply_overlap: bool,
    threshold: f64,
    executor: &impl ChunkExecutor,
    progress: &dyn ProgressObserver,
) -> Result<Vec<ScoredNameMatch>, DedupError> {
    let score_chunk = |chunk: &[(usize, usize)]| {
        let mut matches = CandidateBuffer::new(chunk.len().max(1))?;
        let args = Args::default().score_cutoff(threshold);
        let mut position = 0;
        while position < chunk.len() {
            let left = chunk[position].0;
            let left_name = &names[left];
            let prepared = BatchComparator::new(left_name.characters.iter().copied());
            while position < chunk.len() && chunk[position].0 == left {
                let right = chunk[position].1;
                let right_name = &names[right];
                if (!apply_overlap || passes_overlap(left_name, right_name))
                    && let Some(similarity) =
                        prepared.similarity_with_args(right_name.characters.iter().copied(), &args)
                {
                    matches
                        .push(ScoredNameMatch {
                            left,
                            right,
                            similarity,
                        })
                        .map_err(|_| DedupError::InvariantViolation {
                            context: ErrorContext::stage("name"),
                            message: "bounded score output exceeded its input chunk".to_owned(),
                        })?;
                }
                position += 1;
            }
        }
        progress.advance(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        progress.check_cancelled("name")?;
        Ok::<_, DedupError>(matches.drain().collect::<Vec<_>>())
    };
    if executor.worker_count() <= 1 || candidates.len() < 2_048 {
        return score_chunk(candidates);
    }
    let mut batches = Vec::new();
    let mut start = 0;
    while start < candidates.len() {
        let mut end = start;
        while end < candidates.len()
            && (end - start < 1_024 || candidates[end].0 == candidates[end - 1].0)
        {
            end += 1;
        }
        batches.push(CandidateBatch { start, end });
        start = end;
    }
    Ok(executor
        .map_chunks(&batches, 1, |batch| {
            let batch = batch[0];
            score_chunk(&candidates[batch.start..batch.end])
        })?
        .into_iter()
        .flatten()
        .collect())
}

fn exhaustive_candidates(
    name_count: usize,
    pair_budget: u64,
    progress: &dyn ProgressObserver,
) -> Result<Vec<(usize, usize)>, DedupError> {
    let mut pairs = Vec::new();
    for left in 0..name_count {
        for right in left + 1..name_count {
            if u64::try_from(pairs.len()).unwrap_or(u64::MAX) >= pair_budget {
                return Err(DedupError::BudgetExhausted {
                    context: ErrorContext::stage("name"),
                    counter: "name_candidate_pairs",
                    limit: pair_budget,
                });
            }
            pairs.push((left, right));
        }
        progress.advance(1);
        progress.check_cancelled("name")?;
    }
    Ok(pairs)
}

fn atomize(
    contracts: &[Contract],
    strings: &StringDictionary,
    progress: &dyn ProgressObserver,
) -> Result<AtomizedNames, DedupError> {
    let mut grouped: AHashMap<(ChainId, StringId), (u64, u64)> =
        AHashMap::with_hasher(RandomState::with_seeds(41, 42, 43, 44));
    let mut work = 0_u64;
    for contract in contracts {
        work = work.saturating_add(1);
        if work == 4_096 {
            progress.advance(work);
            progress.check_cancelled("name")?;
            work = 0;
        }
        let Some(name_ref) = contract.name_ref else {
            continue;
        };
        let group = grouped
            .entry((contract.chain_id, name_ref))
            .or_insert((0, 0));
        group.0 = group.0.checked_add(1).ok_or(DedupError::CounterOverflow {
            counter: "name_atom_contract_count",
        })?;
        group.1 = group
            .1
            .checked_add(contract.nft_count)
            .ok_or(DedupError::CounterOverflow {
                counter: "name_atom_nft_count",
            })?;
    }
    progress.advance(work);
    progress.check_cancelled("name")?;

    let mut grouped: Vec<_> = grouped.into_iter().collect();
    grouped.sort_unstable_by_key(|(key, _)| *key);
    let mut atoms = Vec::with_capacity(grouped.len());
    let mut canonical_map: BTreeMap<StringId, Vec<NameAtomId>> = BTreeMap::new();
    let mut atom_by_key: AHashMap<(ChainId, StringId), usize> =
        AHashMap::with_hasher(RandomState::with_seeds(45, 46, 47, 48));
    let mut contract_offset = 0_u64;
    for ((chain_id, name_ref), (contract_count, nft_count)) in grouped {
        let id = NameAtomId::new(checked_entity_id(atoms.len(), "NameAtomId")?);
        canonical_map.entry(name_ref).or_default().push(id);
        atom_by_key.insert((chain_id, name_ref), atoms.len());
        atoms.push(NameAtom {
            id,
            chain_id,
            name_ref,
            contract_offset,
            contract_count,
            nft_count,
        });
        contract_offset =
            contract_offset
                .checked_add(contract_count)
                .ok_or(DedupError::CounterOverflow {
                    counter: "name_atom_contract_offset",
                })?;
    }
    let contract_len =
        usize::try_from(contract_offset).map_err(|_| DedupError::ResourceBudgetExceeded {
            context: ErrorContext::stage("name"),
            requested: contract_offset.saturating_mul(
                u64::try_from(std::mem::size_of::<ContractId>()).unwrap_or(u64::MAX),
            ),
        })?;
    let sentinel = ContractId::new(EntityId::from(0_u32));
    let mut contract_ids = vec![sentinel; contract_len];
    let mut cursors: Vec<u64> = atoms.iter().map(|atom| atom.contract_offset).collect();
    for contract in contracts {
        let Some(name_ref) = contract.name_ref else {
            continue;
        };
        let atom_index = atom_by_key[&(contract.chain_id, name_ref)];
        let position =
            usize::try_from(cursors[atom_index]).map_err(|_| DedupError::InvariantViolation {
                context: ErrorContext::stage("name"),
                message: "Name CSR cursor does not fit usize".to_owned(),
            })?;
        contract_ids[position] = contract.id;
        cursors[atom_index] =
            cursors[atom_index]
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "name_atom_contract_cursor",
                })?;
    }
    for (atom, cursor) in atoms.iter().zip(cursors) {
        if cursor != atom.contract_offset.saturating_add(atom.contract_count) {
            return Err(DedupError::InvariantViolation {
                context: ErrorContext::stage("name"),
                message: "Name CSR contract count did not fill its assigned range".to_owned(),
            });
        }
    }

    let mut canonical_names = Vec::with_capacity(canonical_map.len());
    for (name_ref, atom_ids) in canonical_map {
        let bytes = strings
            .resolve(name_ref)
            .ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("name"),
                message: format!("missing StringId {}", name_ref.get()),
            })?;
        let value = std::str::from_utf8(bytes).map_err(|error| DedupError::InvalidInput {
            context: ErrorContext::stage("name"),
            message: error.to_string(),
        })?;
        let characters: Vec<char> = value.chars().collect();
        canonical_names.push(CanonicalName {
            id: CanonicalNameId::new(checked_entity_id(canonical_names.len(), "CanonicalNameId")?),
            name_ref,
            character_counts: count_characters(&characters),
            characters,
            atom_ids,
        });
    }
    Ok(AtomizedNames {
        atoms,
        contract_ids,
        canonical_names,
    })
}

struct AtomizedNames {
    atoms: Vec<NameAtom>,
    contract_ids: Vec<ContractId>,
    canonical_names: Vec<CanonicalName>,
}

fn posting_candidates(
    names: &[CanonicalName],
    pair_budget: u64,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<Vec<(usize, usize)>, DedupError> {
    // Compute occurrence tokens twice instead of retaining one Vec per Name.
    // This trades a cheap linear character pass for a materially smaller peak.
    let mut frequencies: AHashMap<OccurrenceToken, u64> =
        AHashMap::with_hasher(RandomState::with_seeds(51, 52, 53, 54));
    for name in names {
        progress.check_cancelled("name")?;
        for token in occurrence_tokens(&name.character_counts) {
            let count = frequencies.entry(token).or_default();
            *count = count.checked_add(1).ok_or(DedupError::CounterOverflow {
                counter: "name_posting_entries",
            })?;
        }
    }

    let mut postings: AHashMap<OccurrenceToken, Vec<usize>> =
        AHashMap::with_hasher(RandomState::with_seeds(55, 56, 57, 58));
    for (name_id, name) in names.iter().enumerate() {
        progress.check_cancelled("name")?;
        let mut ordered = occurrence_tokens(&name.character_counts);
        ordered.sort_unstable_by_key(|token| (frequencies[token], *token));
        let minimum_partner_length = names[name_id]
            .characters
            .len()
            .saturating_mul(3)
            .div_ceil(4);
        let minimum_overlap =
            CandidateBounds::for_lengths(names[name_id].characters.len(), minimum_partner_length)
                .minimum_multiset_overlap;
        let prefix_length = ordered
            .len()
            .saturating_sub(minimum_overlap)
            .saturating_add(1)
            .min(ordered.len());
        for token in ordered.into_iter().take(prefix_length) {
            postings.entry(token).or_default().push(name_id);
            counters.name_posting_entries(1)?;
        }
    }

    let mut pairs: AHashSet<(usize, usize)> =
        AHashSet::with_hasher(RandomState::with_seeds(61, 62, 63, 64));
    let posting_touches = postings.values().fold(0_u64, |total, posting| {
        let length = u64::try_from(posting.len()).unwrap_or(u64::MAX);
        total.saturating_add(length.saturating_mul(length.saturating_sub(1)) / 2)
    });
    progress.set_total(posting_touches);
    for posting in postings.values() {
        for left_position in 0..posting.len() {
            progress.advance(
                u64::try_from(posting.len().saturating_sub(left_position + 1)).unwrap_or(u64::MAX),
            );
            progress.check_cancelled("name")?;
            for &right in &posting[left_position + 1..] {
                counters.name_posting_touches(1)?;
                let left = posting[left_position];
                if !CandidateBounds::can_pair_lengths(
                    names[left].characters.len(),
                    names[right].characters.len(),
                ) {
                    continue;
                }
                if !passes_overlap(&names[left], &names[right]) {
                    continue;
                }
                let pair = (left.min(right), left.max(right));
                if !pairs.contains(&pair)
                    && u64::try_from(pairs.len()).unwrap_or(u64::MAX) >= pair_budget
                {
                    return Err(DedupError::BudgetExhausted {
                        context: ErrorContext::stage("name"),
                        counter: "name_candidate_pairs",
                        limit: pair_budget,
                    });
                }
                pairs.insert(pair);
            }
        }
    }
    let mut pairs: Vec<_> = pairs.into_iter().collect();
    pairs.sort_unstable();
    Ok(pairs)
}

fn overlap_scan_candidates(
    names: &[CanonicalName],
    pair_budget: u64,
    progress: &dyn ProgressObserver,
) -> Result<Vec<(usize, usize)>, DedupError> {
    let mut pairs = Vec::new();
    let mut by_length: Vec<usize> = (0..names.len()).collect();
    by_length.sort_unstable_by_key(|index| (names[*index].characters.len(), *index));
    for left_position in 0..by_length.len() {
        let left = by_length[left_position];
        let left_length = names[left].characters.len();
        // Later entries are no shorter. The 3/4 safe Jaro-Winkler length
        // bound therefore gives an exact upper edge for this scan window.
        let maximum_partner_length = left_length.saturating_mul(4) / 3;
        for &right in &by_length[left_position + 1..] {
            if names[right].characters.len() > maximum_partner_length {
                break;
            }
            if !passes_overlap(&names[left], &names[right]) {
                continue;
            }
            if u64::try_from(pairs.len()).unwrap_or(u64::MAX) >= pair_budget {
                return Err(DedupError::BudgetExhausted {
                    context: ErrorContext::stage("name"),
                    counter: "name_candidate_pairs",
                    limit: pair_budget,
                });
            }
            pairs.push((left.min(right), left.max(right)));
        }
        progress.advance(1);
        progress.check_cancelled("name")?;
    }
    pairs.sort_unstable();
    Ok(pairs)
}

fn occurrence_tokens(character_counts: &[(char, u32)]) -> Vec<OccurrenceToken> {
    character_counts
        .iter()
        .flat_map(|(character, count)| (0..*count).map(move |rank| (*character, rank)))
        .collect()
}

fn passes_overlap(left: &CanonicalName, right: &CanonicalName) -> bool {
    if !CandidateBounds::can_pair_lengths(left.characters.len(), right.characters.len()) {
        return false;
    }
    let mut left_index = 0;
    let mut right_index = 0;
    let mut overlap = 0_usize;
    while left_index < left.character_counts.len() && right_index < right.character_counts.len() {
        match left.character_counts[left_index]
            .0
            .cmp(&right.character_counts[right_index].0)
        {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                overlap = overlap.saturating_add(
                    left.character_counts[left_index]
                        .1
                        .min(right.character_counts[right_index].1) as usize,
                );
                left_index += 1;
                right_index += 1;
            }
        }
    }
    let common_prefix = left
        .characters
        .iter()
        .zip(&right.characters)
        .take(4)
        .take_while(|(left, right)| left == right)
        .count();
    overlap
        >= CandidateBounds::for_lengths_and_prefix(
            left.characters.len(),
            right.characters.len(),
            common_prefix,
        )
        .minimum_multiset_overlap
}

fn count_characters(characters: &[char]) -> Vec<(char, u32)> {
    let mut counts = BTreeMap::new();
    for character in characters {
        let count = counts.entry(*character).or_insert(0_u32);
        *count = count.saturating_add(1);
    }
    counts.into_iter().collect()
}

fn emit_identical_groups(
    atoms: &[NameAtom],
    contract_ids: &[ContractId],
    names: &[CanonicalName],
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    for name in names {
        for left_position in 0..name.atom_ids.len() {
            let left = &atoms[id_index(name.atom_ids[left_position].get())?];
            if left.contract_count >= 2 {
                emit_contracts(
                    atom_contracts(left, contract_ids)?,
                    ScopeId::Intra(left.chain_id),
                    sink,
                    counters,
                )?;
            }
            for right_id in &name.atom_ids[left_position + 1..] {
                let right = &atoms[id_index(right_id.get())?];
                emit_cross_atoms(left, right, contract_ids, sink, counters)?;
            }
        }
        progress.advance(1);
        progress.check_cancelled("name")?;
    }
    Ok(())
}

fn emit_canonical_pair(
    left: &CanonicalName,
    right: &CanonicalName,
    atoms: &[NameAtom],
    contract_ids: &[ContractId],
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
) -> Result<(), DedupError> {
    for left_id in &left.atom_ids {
        let left_atom = &atoms[id_index(left_id.get())?];
        for right_id in &right.atom_ids {
            let right_atom = &atoms[id_index(right_id.get())?];
            if left_atom.chain_id == right_atom.chain_id {
                emit_contracts(
                    atom_contracts(left_atom, contract_ids)?,
                    ScopeId::Intra(left_atom.chain_id),
                    sink,
                    counters,
                )?;
                emit_contracts(
                    atom_contracts(right_atom, contract_ids)?,
                    ScopeId::Intra(right_atom.chain_id),
                    sink,
                    counters,
                )?;
            } else {
                emit_cross_atoms(left_atom, right_atom, contract_ids, sink, counters)?;
            }
        }
    }
    Ok(())
}

fn emit_cross_atoms(
    left: &NameAtom,
    right: &NameAtom,
    contract_ids: &[ContractId],
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
) -> Result<(), DedupError> {
    debug_assert_ne!(left.chain_id, right.chain_id);
    for (primary, secondary) in [(left, right), (right, left)] {
        emit_contracts(
            atom_contracts(primary, contract_ids)?,
            ScopeId::CrossSummary(primary.chain_id),
            sink,
            counters,
        )?;
        emit_contracts(
            atom_contracts(primary, contract_ids)?,
            ScopeId::Matrix {
                primary: primary.chain_id,
                secondary: secondary.chain_id,
            },
            sink,
            counters,
        )?;
    }
    Ok(())
}

fn atom_contracts<'a>(
    atom: &NameAtom,
    contract_ids: &'a [ContractId],
) -> Result<&'a [ContractId], DedupError> {
    let start =
        usize::try_from(atom.contract_offset).map_err(|_| DedupError::InvariantViolation {
            context: ErrorContext::stage("name"),
            message: "Name CSR offset does not fit usize".to_owned(),
        })?;
    let count =
        usize::try_from(atom.contract_count).map_err(|_| DedupError::InvariantViolation {
            context: ErrorContext::stage("name"),
            message: "Name CSR count does not fit usize".to_owned(),
        })?;
    let end = start
        .checked_add(count)
        .ok_or(DedupError::CounterOverflow {
            counter: "name_atom_contract_range",
        })?;
    contract_ids
        .get(start..end)
        .ok_or_else(|| DedupError::InvariantViolation {
            context: ErrorContext::stage("name"),
            message: "Name CSR range exceeds the contract array".to_owned(),
        })
}

fn emit_contracts(
    contracts: &[ContractId],
    scope: ScopeId,
    sink: &mut impl HitEventSink,
    counters: &mut StageCounters,
) -> Result<(), DedupError> {
    for contract in contracts {
        sink.submit(HitEvent {
            dimension: Dimension::Name,
            scope,
            entity_kind: EntityKind::Contract,
            entity_id: contract.as_u64(),
        })?;
        counters.hit_events(1)?;
    }
    Ok(())
}

fn checked_entity_id(value: usize, kind: &'static str) -> Result<EntityId, DedupError> {
    EntityId::try_from(value).map_err(|_| DedupError::InvalidInput {
        context: ErrorContext::stage("name"),
        message: format!("{kind} capacity exceeded"),
    })
}

fn id_index(value: EntityId) -> Result<usize, DedupError> {
    usize::try_from(value).map_err(|_| DedupError::InvariantViolation {
        context: ErrorContext::stage("name"),
        message: "entity ID does not fit usize".to_owned(),
    })
}

fn checked_total(value: usize) -> Option<u64> {
    u64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_model::{NftId, StringId};
    use std::collections::BTreeSet;

    fn fixture() -> (Vec<Contract>, StringDictionary) {
        let mut strings = StringDictionary::new(8).unwrap();
        let address = strings.intern(b"address").unwrap();
        let exact = strings.intern(b"cool collection").unwrap();
        let fuzzy = strings.intern(b"cool collectiom").unwrap();
        let unrelated = strings.intern(b"totally different").unwrap();
        let contracts = vec![
            (0, 0, exact, 10),
            (1, 0, exact, 12),
            (2, 1, exact, 8),
            (3, 1, fuzzy, 9),
            (4, 1, unrelated, 1),
        ]
        .into_iter()
        .map(|(id, chain, name, nft_count)| Contract {
            id: ContractId::new(id),
            chain_id: ChainId::new(chain),
            address_ref: address,
            name_ref: Some(name),
            first_nft_id: NftId::new(id),
            nft_count,
        })
        .collect();
        (contracts, strings)
    }

    #[test]
    fn atomization_avoids_contract_multiplicity_scoring() {
        let (mut contracts, strings) = fixture();
        let mut sink = RecordingSink::default();
        let first = run_name(
            &contracts,
            &strings,
            NameEngineConfig::production_default(100),
            &mut sink,
        )
        .unwrap();
        let prototype = contracts[0].clone();
        for id in 5..100 {
            let mut duplicate = prototype.clone();
            duplicate.id = ContractId::new(id);
            contracts.push(duplicate);
        }
        let second = run_name(
            &contracts,
            &strings,
            NameEngineConfig::production_default(100),
            &mut RecordingSink::default(),
        )
        .unwrap();
        assert_eq!(
            first.counters.name_scored_candidates,
            second.counters.name_scored_candidates
        );
        assert_eq!(second.canonical_names.len(), 3);
        assert_eq!(second.contract_ids.len(), contracts.len());
        assert_eq!(
            second
                .atoms
                .iter()
                .map(|atom| atom.contract_count)
                .sum::<u64>(),
            u64::try_from(second.contract_ids.len()).unwrap()
        );
    }

    #[test]
    fn resident_and_overlap_modes_match() {
        let (contracts, strings) = fixture();
        let run = |mode| {
            run_name(
                &contracts,
                &strings,
                NameEngineConfig {
                    threshold: 0.95,
                    candidate_storage: mode,
                    candidate_pair_budget: 100,
                    score_budget: 100,
                },
                &mut RecordingSink::default(),
            )
            .unwrap()
            .fuzzy_matches
        };
        assert_eq!(
            run(CandidateStorageMode::ResidentPostings),
            run(CandidateStorageMode::OverlapScan)
        );
    }

    #[test]
    fn parallel_scoring_is_deterministic_and_equivalent() {
        let mut strings = StringDictionary::new(8).unwrap();
        let address = strings.intern(b"address").unwrap();
        let mut contracts = Vec::new();
        for id in 0..80 {
            let name = strings
                .intern(format!("collection alpha {id:03}").as_bytes())
                .unwrap();
            contracts.push(Contract {
                id: ContractId::new(id),
                chain_id: ChainId::new(u16::try_from(id % 2).unwrap()),
                address_ref: address,
                name_ref: Some(name),
                first_nft_id: NftId::new(id),
                nft_count: 1,
            });
        }
        let config = NameEngineConfig {
            threshold: 0.95,
            candidate_storage: CandidateStorageMode::ResidentPostings,
            candidate_pair_budget: 100_000,
            score_budget: 100_000,
        };
        let mut sequential_sink = RecordingSink::default();
        let sequential = run_name_with_progress_and_workers(
            &contracts,
            &strings,
            config,
            &mut sequential_sink,
            &NoopProgress,
            1,
        )
        .unwrap();
        let mut parallel_sink = RecordingSink::default();
        let parallel = run_name_with_progress_and_workers(
            &contracts,
            &strings,
            config,
            &mut parallel_sink,
            &NoopProgress,
            4,
        )
        .unwrap();
        assert_eq!(parallel.fuzzy_matches, sequential.fuzzy_matches);
        assert_eq!(parallel.counters, sequential.counters);
        assert_eq!(parallel_sink.0, sequential_sink.0);
    }

    #[derive(Default)]
    struct RecordingSink(Vec<HitEvent>);

    impl HitEventSink for RecordingSink {
        fn submit(&mut self, event: HitEvent) -> Result<(), DedupError> {
            self.0.push(event);
            Ok(())
        }
    }

    #[test]
    fn strong_types_are_not_aliases_in_fixture() {
        assert_ne!(StringId::new(0).as_u64(), u64::MAX);
    }

    #[test]
    fn lossless_candidates_match_independent_exhaustive_reference() {
        let values = [
            "collection alpha",
            "collection alphi",
            "collection alphx",
            "collection beta",
            "collectiom alpha",
            "系列 collection alpha",
            "系列 collection alphi",
            "zzzzzzzzzzzzzzzz",
        ];
        let mut strings = StringDictionary::new(8).unwrap();
        let address = strings.intern(b"address").unwrap();
        let contracts: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(index, value)| Contract {
                id: ContractId::new(dedup_model::EntityId::from(index as u32)),
                chain_id: ChainId::new((index % 2) as u16),
                address_ref: address,
                name_ref: Some(strings.intern(value.as_bytes()).unwrap()),
                first_nft_id: NftId::new(dedup_model::EntityId::from(index as u32)),
                nft_count: 1,
            })
            .collect();
        let result = run_name(
            &contracts,
            &strings,
            NameEngineConfig {
                threshold: 0.95,
                candidate_storage: CandidateStorageMode::ResidentPostings,
                candidate_pair_budget: 1_000,
                score_budget: 1_000,
            },
            &mut RecordingSink::default(),
        )
        .unwrap();
        let actual: BTreeSet<_> = result
            .fuzzy_matches
            .iter()
            .map(|pair| {
                let left = id_index(pair.left.get()).unwrap();
                let right = id_index(pair.right.get()).unwrap();
                (
                    result.canonical_names[left].name_ref,
                    result.canonical_names[right].name_ref,
                )
            })
            .collect();
        let mut expected = BTreeSet::new();
        for left in 0..values.len() {
            for right in left + 1..values.len() {
                if reference_jaro_winkler(values[left], values[right]) >= 0.95 {
                    expected.insert((
                        contracts[left].name_ref.unwrap(),
                        contracts[right].name_ref.unwrap(),
                    ));
                }
            }
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn lower_threshold_uses_budgeted_exhaustive_fallback() {
        let values = ["abc", "axc", "xyz"];
        let mut strings = StringDictionary::new(8).unwrap();
        let address = strings.intern(b"address").unwrap();
        let contracts: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(index, value)| Contract {
                id: ContractId::new(EntityId::from(index as u32)),
                chain_id: ChainId::new(0),
                address_ref: address,
                name_ref: Some(strings.intern(value.as_bytes()).unwrap()),
                first_nft_id: NftId::new(EntityId::from(index as u32)),
                nft_count: 1,
            })
            .collect();
        let threshold = 0.8;
        let result = run_name(
            &contracts,
            &strings,
            NameEngineConfig {
                threshold,
                candidate_storage: CandidateStorageMode::ResidentPostings,
                candidate_pair_budget: 3,
                score_budget: 3,
            },
            &mut RecordingSink::default(),
        )
        .unwrap();
        let expected = (0..values.len())
            .flat_map(|left| (left + 1..values.len()).map(move |right| (left, right)))
            .filter(|(left, right)| {
                reference_jaro_winkler(values[*left], values[*right]) >= threshold
            })
            .count();
        assert_eq!(result.fuzzy_matches.len(), expected);
        assert_eq!(result.counters.name_scored_candidates, 3);
    }

    #[test]
    fn adversarial_name_candidates_fail_exactly_at_the_budget() {
        let values = ["aaaaab", "aaaaba", "aaabaa", "aabaaa", "abaaaa", "baaaaa"];
        let mut strings = StringDictionary::new(8).unwrap();
        let address = strings.intern(b"address").unwrap();
        let contracts: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(index, value)| Contract {
                id: ContractId::new(EntityId::from(index as u32)),
                chain_id: ChainId::new(0),
                address_ref: address,
                name_ref: Some(strings.intern(value.as_bytes()).unwrap()),
                first_nft_id: NftId::new(EntityId::from(index as u32)),
                nft_count: 1,
            })
            .collect();
        let error = run_name(
            &contracts,
            &strings,
            NameEngineConfig {
                threshold: 0.95,
                candidate_storage: CandidateStorageMode::OverlapScan,
                candidate_pair_budget: 2,
                score_budget: 100,
            },
            &mut RecordingSink::default(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DedupError::BudgetExhausted {
                counter: "name_candidate_pairs",
                limit: 2,
                ..
            }
        ));
    }

    #[test]
    fn candidate_bounds_cover_all_small_binary_jaro_winkler_hits() {
        let values: Vec<String> = (1_u8..=7)
            .flat_map(|length| {
                (0_u16..1_u16 << length).map(move |bits| {
                    (0..length)
                        .map(|position| {
                            if bits & (1 << position) == 0 {
                                'a'
                            } else {
                                'b'
                            }
                        })
                        .collect()
                })
            })
            .collect();
        for left in 0..values.len() {
            for right in left + 1..values.len() {
                let reference = reference_jaro_winkler(&values[left], &values[right]);
                let rapid = rapidfuzz::distance::jaro_winkler::similarity(
                    values[left].chars(),
                    values[right].chars(),
                );
                assert!((reference - rapid).abs() < 1e-12);
                if reference >= 0.95 {
                    let left_name = canonical_for_test(0, &values[left]);
                    let right_name = canonical_for_test(1, &values[right]);
                    assert!(passes_overlap(&left_name, &right_name));
                }
            }
        }
    }

    #[test]
    fn posting_candidates_filter_false_overlap_before_budgeting() {
        let names = [canonical_for_test(0, "abcd"), canonical_for_test(1, "aefg")];
        assert!(!passes_overlap(&names[0], &names[1]));

        let candidates =
            posting_candidates(&names, 0, &mut StageCounters::default(), &NoopProgress).unwrap();

        assert!(candidates.is_empty());
    }

    fn canonical_for_test(id: EntityId, value: &str) -> CanonicalName {
        let characters: Vec<_> = value.chars().collect();
        CanonicalName {
            id: CanonicalNameId::new(id),
            name_ref: StringId::new(id),
            character_counts: count_characters(&characters),
            characters,
            atom_ids: Vec::new(),
        }
    }

    fn reference_jaro_winkler(left: &str, right: &str) -> f64 {
        let left: Vec<char> = left.chars().collect();
        let right: Vec<char> = right.chars().collect();
        if left == right {
            return 1.0;
        }
        if left.is_empty() || right.is_empty() {
            return 0.0;
        }
        let window = left
            .len()
            .max(right.len())
            .saturating_div(2)
            .saturating_sub(1);
        let mut left_matches = vec![false; left.len()];
        let mut right_matches = vec![false; right.len()];
        let mut matches = 0_usize;
        for (left_index, character) in left.iter().enumerate() {
            let start = left_index.saturating_sub(window);
            let end = (left_index + window + 1).min(right.len());
            for right_index in start..end {
                if !right_matches[right_index] && *character == right[right_index] {
                    left_matches[left_index] = true;
                    right_matches[right_index] = true;
                    matches += 1;
                    break;
                }
            }
        }
        if matches == 0 {
            return 0.0;
        }
        let matched_left: Vec<_> = left
            .iter()
            .zip(left_matches)
            .filter_map(|(character, matched)| matched.then_some(*character))
            .collect();
        let matched_right: Vec<_> = right
            .iter()
            .zip(right_matches)
            .filter_map(|(character, matched)| matched.then_some(*character))
            .collect();
        let transpositions = matched_left
            .iter()
            .zip(matched_right)
            .filter(|(left, right)| left != &right)
            .count()
            / 2;
        let matches = matches as f64;
        let jaro = (matches / left.len() as f64
            + matches / right.len() as f64
            + (matches - transpositions as f64) / matches)
            / 3.0;
        let prefix = left
            .iter()
            .zip(&right)
            .take(4)
            .take_while(|(left, right)| left == right)
            .count() as f64;
        if jaro > 0.7 {
            jaro + prefix * 0.1 * (1.0 - jaro)
        } else {
            jaro
        }
    }
}
