use crate::ChainId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ScopeId {
    Intra(ChainId),
    CrossSummary(ChainId),
    Matrix {
        primary: ChainId,
        secondary: ChainId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Dimension {
    Name,
    TokenUri,
    ImageUri,
    Metadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EntityKind {
    Contract,
    Nft,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct HitEvent {
    pub dimension: Dimension,
    pub scope: ScopeId,
    pub entity_kind: EntityKind,
    pub entity_id: u64,
}

pub trait HitEventSink {
    fn submit(&mut self, event: HitEvent) -> Result<(), crate::DedupError>;
}
