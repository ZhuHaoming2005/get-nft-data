use crate::model::{CandidateId, ContractKey};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub candidate_id: CandidateId,
    pub contract: ContractKey,
    pub artifact_path: PathBuf,
    pub checksum: String,
    pub compressed_bytes: u64,
    pub analysis_status: String,
    pub lightweight_summary: serde_json::Value,
}

#[derive(Default)]
pub struct ArtifactIndex {
    values: Vec<ArtifactRef>,
}

impl ArtifactIndex {
    pub fn push(&mut self, artifact: ArtifactRef) {
        self.values.push(artifact);
    }

    pub fn take_ordered(&mut self) -> Vec<ArtifactRef> {
        let mut values = std::mem::take(&mut self.values);
        values.sort_by(|left, right| left.contract.cmp(&right.contract));
        values
    }
}
