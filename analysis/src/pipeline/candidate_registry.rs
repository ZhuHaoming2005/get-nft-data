use crate::error::{AnalysisError, Result};
use crate::model::{CandidateId, ContractKey, SeedCandidateRelation};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Immutable candidate input moved through the fetch, analysis and persistence
/// stages. Stage ownership is the lifecycle; no parallel shadow state is kept.
#[derive(Clone, Debug)]
pub struct CandidateRecord {
    pub id: CandidateId,
    pub key: ContractKey,
    pub relations: Arc<Vec<SeedCandidateRelation>>,
}

/// Compact duplicate guard for the single-owner candidate stream.
///
/// Candidate IDs are dense contract IDs, so a bitmap is both faster and much
/// smaller than retaining maps of candidate records and contract keys for the
/// duration of the run.
#[derive(Default)]
pub struct CandidateRegistry {
    seen: Vec<u64>,
    seen_count: usize,
}

impl CandidateRegistry {
    pub fn insert_frozen_relations(
        &mut self,
        relations: Vec<SeedCandidateRelation>,
    ) -> Result<Vec<CandidateRecord>> {
        let mut grouped = BTreeMap::<CandidateId, Vec<SeedCandidateRelation>>::new();
        for relation in relations {
            grouped
                .entry(relation.candidate_id)
                .or_default()
                .push(relation);
        }

        let mut inserted = Vec::with_capacity(grouped.len());
        for (candidate_id, mut relations) in grouped {
            if self.mark_seen(candidate_id) {
                return Err(AnalysisError::State(format!(
                    "candidate {} arrived more than once after its owner shard was sealed",
                    candidate_id.0
                )));
            }
            relations.sort_by_key(|relation| relation.seed_id);
            let key = relations
                .first()
                .ok_or_else(|| AnalysisError::State("empty relation group".into()))?
                .candidate
                .clone();
            if relations.iter().any(|relation| relation.candidate != key) {
                return Err(AnalysisError::State(format!(
                    "candidate {} relation group contains inconsistent contract keys",
                    candidate_id.0
                )));
            }
            inserted.push(CandidateRecord {
                id: candidate_id,
                key,
                relations: Arc::new(relations),
            });
        }
        inserted.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(inserted)
    }

    pub fn seen_count(&self) -> usize {
        self.seen_count
    }

    fn mark_seen(&mut self, id: CandidateId) -> bool {
        let index = id.0 as usize;
        let word = index / 64;
        if self.seen.len() <= word {
            self.seen.resize(word + 1, 0);
        }
        let mask = 1_u64 << (index % 64);
        let already_seen = self.seen[word] & mask != 0;
        if !already_seen {
            self.seen[word] |= mask;
            self.seen_count += 1;
        }
        already_seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainId, Dimension, NftSelection, SeedId};

    fn relation(candidate_id: u32) -> SeedCandidateRelation {
        let candidate = ContractKey::new(ChainId::Ethereum, format!("0x{candidate_id:x}"));
        SeedCandidateRelation {
            seed_id: SeedId(0),
            seed: ContractKey::new(ChainId::Ethereum, "0xseed"),
            candidate_id: CandidateId(candidate_id),
            candidate: candidate.clone(),
            dimensions: Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate,
                nft_count: 10,
            },
            evidence: Vec::new(),
            incomplete: false,
        }
    }

    #[test]
    fn registry_retains_only_dense_seen_bitmap() {
        let mut registry = CandidateRegistry::default();
        let records = registry
            .insert_frozen_relations(vec![relation(1), relation(70)])
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(registry.seen_count(), 2);
        assert_eq!(registry.seen.len(), 2);
    }

    #[test]
    fn duplicate_frozen_candidate_is_rejected() {
        let mut registry = CandidateRegistry::default();
        registry.insert_frozen_relations(vec![relation(1)]).unwrap();
        let error = registry
            .insert_frozen_relations(vec![relation(1)])
            .unwrap_err();
        assert!(error.to_string().contains("arrived more than once"));
    }
}
