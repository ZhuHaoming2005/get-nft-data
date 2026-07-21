use crate::dedup::DedupHit;
use crate::model::{CandidateId, SeedCandidateRelation};
use crate::resident::ContractCatalog;
use ahash::AHashMap;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;

pub struct RelationAccumulator<'a> {
    catalog: Option<&'a ContractCatalog>,
    relations: AHashMap<(crate::model::SeedId, crate::model::CandidateId), SeedCandidateRelation>,
}

impl<'a> RelationAccumulator<'a> {
    pub fn new(catalog: &'a ContractCatalog) -> Self {
        Self {
            catalog: Some(catalog),
            relations: AHashMap::new(),
        }
    }

    pub fn push_hit(&mut self, hit: DedupHit) {
        let catalog = self
            .catalog
            .expect("hit accumulator must be created with a contract catalog");
        let candidate_id = CandidateId(hit.candidate_contract.0);
        match self.relations.entry((hit.seed_id, candidate_id)) {
            Entry::Vacant(entry) => {
                entry.insert(SeedCandidateRelation {
                    seed_id: hit.seed_id,
                    seed: catalog.key(hit.seed_contract),
                    candidate_id,
                    candidate: catalog.key(hit.candidate_contract),
                    dimensions: hit.dimension.bit(),
                    selection: hit.selection,
                    evidence: vec![hit.evidence],
                    incomplete: false,
                });
            }
            Entry::Occupied(mut entry) => {
                let relation = entry.get_mut();
                relation.dimensions |= hit.dimension.bit();
                relation.selection.union_assign(hit.selection);
                relation.evidence.push(hit.evidence);
            }
        }
    }

    pub fn push_relation(&mut self, relation: SeedCandidateRelation) {
        match self
            .relations
            .entry((relation.seed_id, relation.candidate_id))
        {
            Entry::Vacant(entry) => {
                entry.insert(relation);
            }
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                existing.dimensions |= relation.dimensions;
                existing.selection.union_assign(relation.selection);
                existing.evidence.extend(relation.evidence);
                existing.incomplete |= relation.incomplete;
            }
        }
    }

    pub fn extend_hits(&mut self, hits: impl IntoIterator<Item = DedupHit>) {
        for hit in hits {
            self.push_hit(hit);
        }
    }

    pub fn extend_relations(&mut self, relations: impl IntoIterator<Item = SeedCandidateRelation>) {
        for relation in relations {
            self.push_relation(relation);
        }
    }

    pub fn merge(mut self, right: Self) -> Self {
        let mut right = right;
        if self.relations.len() < right.relations.len() {
            std::mem::swap(&mut self, &mut right);
        }
        self.relations.reserve(right.relations.len());
        self.extend_relations(right.relations.into_values());
        self
    }

    /// Returns the accumulated values without paying normalization and sort
    /// costs. This is intended for intermediate batch hand-off into another
    /// accumulator; externally observable relation vectors must use `finish`.
    pub(crate) fn into_relations_unfinished(self) -> Vec<SeedCandidateRelation> {
        self.relations.into_values().collect()
    }

    pub fn finish(self) -> Vec<SeedCandidateRelation> {
        finish_relations(self.relations)
    }
}

pub fn reduce_hits(
    catalog: &ContractCatalog,
    hits: impl IntoIterator<Item = DedupHit>,
) -> Vec<SeedCandidateRelation> {
    let mut accumulator = RelationAccumulator::new(catalog);
    accumulator.extend_hits(hits);
    accumulator.finish()
}

pub fn merge_relations(
    relations: impl IntoIterator<Item = SeedCandidateRelation>,
) -> Vec<SeedCandidateRelation> {
    let mut relations = relations.into_iter();
    let Some(first) = relations.next() else {
        return Vec::new();
    };
    let mut accumulator = RelationAccumulator {
        catalog: None,
        relations: AHashMap::new(),
    };
    accumulator.push_relation(first);
    accumulator.extend_relations(relations);
    accumulator.finish()
}

fn finish_relations(
    merged: AHashMap<(crate::model::SeedId, CandidateId), SeedCandidateRelation>,
) -> Vec<SeedCandidateRelation> {
    let mut output = merged.into_values().collect::<Vec<_>>();
    if output.len() >= 4_096 {
        output.par_iter_mut().for_each(normalize_relation);
        output.par_sort_unstable_by(compare_relations);
    } else {
        output.iter_mut().for_each(normalize_relation);
        output.sort_unstable_by(compare_relations);
    }
    output
}

fn normalize_relation(relation: &mut SeedCandidateRelation) {
    relation.selection.normalize();
    relation.evidence.sort_by(compare_evidence);
    relation.evidence.dedup();
}

fn compare_relations(left: &SeedCandidateRelation, right: &SeedCandidateRelation) -> Ordering {
    (left.seed_id, &left.candidate).cmp(&(right.seed_id, &right.candidate))
}

pub(crate) fn compare_evidence(
    left: &crate::model::MatchEvidence,
    right: &crate::model::MatchEvidence,
) -> Ordering {
    use crate::model::MatchEvidence;
    match (left, right) {
        (
            MatchEvidence::Name {
                left: left_seed,
                right: left_candidate,
                similarity: left_similarity,
                threshold: left_threshold,
            },
            MatchEvidence::Name {
                left: right_seed,
                right: right_candidate,
                similarity: right_similarity,
                threshold: right_threshold,
            },
        ) => left_candidate
            .cmp(right_candidate)
            .then_with(|| left_seed.cmp(right_seed))
            .then_with(|| left_similarity.total_cmp(right_similarity))
            .then_with(|| left_threshold.total_cmp(right_threshold)),
        (
            MatchEvidence::Uri {
                dimension: left_dimension,
                uri: left_uri,
                seed_nft: left_seed,
                candidate_nft: left_candidate,
            },
            MatchEvidence::Uri {
                dimension: right_dimension,
                uri: right_uri,
                seed_nft: right_seed,
                candidate_nft: right_candidate,
            },
        ) => (left_candidate, left_dimension, left_seed, left_uri).cmp(&(
            right_candidate,
            right_dimension,
            right_seed,
            right_uri,
        )),
        (
            MatchEvidence::Metadata {
                seed_token_id: left_seed_token,
                candidate_token_id: left_candidate_token,
                seed_digest: left_seed_digest,
                candidate_digest: left_candidate_digest,
                similarity: left_similarity,
                threshold: left_threshold,
            },
            MatchEvidence::Metadata {
                seed_token_id: right_seed_token,
                candidate_token_id: right_candidate_token,
                seed_digest: right_seed_digest,
                candidate_digest: right_candidate_digest,
                similarity: right_similarity,
                threshold: right_threshold,
            },
        ) => left_candidate_digest
            .cmp(right_candidate_digest)
            .then_with(|| left_candidate_token.cmp(right_candidate_token))
            .then_with(|| left_seed_digest.cmp(right_seed_digest))
            .then_with(|| left_seed_token.cmp(right_seed_token))
            .then_with(|| left_similarity.total_cmp(right_similarity))
            .then_with(|| left_threshold.total_cmp(right_threshold)),
        (MatchEvidence::Name { .. }, _) => Ordering::Less,
        (_, MatchEvidence::Name { .. }) => Ordering::Greater,
        (MatchEvidence::Uri { .. }, _) => Ordering::Less,
        (_, MatchEvidence::Uri { .. }) => Ordering::Greater,
    }
}
