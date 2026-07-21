pub mod aggregate;
pub mod artifact_index;
pub mod contract_writer;
pub mod csv;
pub mod json;
pub mod markdown;
pub mod scope;

pub use aggregate::*;
pub use artifact_index::*;
pub use contract_writer::*;

use crate::model::{
    CandidateFacts, ContractKey, EvidenceBundle, RelationDelta, RelationLabel,
    SeedCandidateRelation,
};
use serde::Serialize;

#[derive(Serialize)]
pub struct ContractArtifact<'a> {
    pub candidate: &'a ContractKey,
    pub matches: &'a [SeedCandidateRelation],
    pub relation_labels: &'a [RelationLabel],
    pub evidence: &'a EvidenceBundle,
    pub facts: &'a CandidateFacts,
    pub relation_deltas: &'a [RelationDelta],
    pub analysis_error: Option<&'a str>,
}
