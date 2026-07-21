use crate::model::{CandidateId, ContractKey, NftKey, SeedId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Name,
    TokenUri,
    ImageUri,
    Metadata,
}

impl Dimension {
    pub const ALL: [Self; 4] = [Self::Name, Self::TokenUri, Self::ImageUri, Self::Metadata];

    pub const fn bit(self) -> u8 {
        match self {
            Self::Name => 1,
            Self::TokenUri => 2,
            Self::ImageUri => 4,
            Self::Metadata => 8,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::TokenUri => "token_uri",
            Self::ImageUri => "image_uri",
            Self::Metadata => "metadata",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NftSelection {
    AllInContract {
        contract: ContractKey,
        nft_count: u64,
    },
    Explicit {
        nfts: Vec<NftKey>,
    },
}

impl NftSelection {
    pub fn declared_count(&self) -> u64 {
        match self {
            Self::AllInContract { nft_count, .. } => *nft_count,
            Self::Explicit { nfts } => nfts.len() as u64,
        }
    }

    pub fn normalize(&mut self) {
        if let Self::Explicit { nfts } = self {
            nfts.sort();
            nfts.dedup();
        }
    }

    pub fn union_assign(&mut self, other: Self) {
        match (&mut *self, other) {
            (Self::AllInContract { .. }, _) => {}
            (slot, whole @ Self::AllInContract { .. }) => *slot = whole,
            (Self::Explicit { nfts }, Self::Explicit { nfts: mut right }) => {
                nfts.append(&mut right);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MatchEvidence {
    Name {
        left: Arc<str>,
        right: Arc<str>,
        similarity: f64,
        threshold: f64,
    },
    Uri {
        dimension: Dimension,
        uri: Arc<str>,
        seed_nft: NftKey,
        candidate_nft: NftKey,
    },
    Metadata {
        seed_token_id: Arc<str>,
        candidate_token_id: Arc<str>,
        seed_digest: Arc<str>,
        candidate_digest: Arc<str>,
        similarity: f64,
        threshold: f64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedCandidateRelation {
    pub seed_id: SeedId,
    pub seed: ContractKey,
    pub candidate_id: CandidateId,
    pub candidate: ContractKey,
    pub dimensions: u8,
    pub selection: NftSelection,
    pub evidence: Vec<MatchEvidence>,
    pub incomplete: bool,
}
