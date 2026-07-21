pub mod metadata;
pub mod name;
pub mod reducer;
pub mod scratch;
pub mod uri;

pub use metadata::*;
pub use name::*;
pub use reducer::*;
pub use scratch::*;
pub use uri::*;

use crate::model::{ContractId, MatchEvidence, NftSelection, SeedId};

#[derive(Clone, Debug)]
pub struct DedupHit {
    pub seed_id: SeedId,
    pub seed_contract: ContractId,
    pub candidate_contract: ContractId,
    pub dimension: crate::model::Dimension,
    pub selection: NftSelection,
    pub evidence: MatchEvidence,
}
