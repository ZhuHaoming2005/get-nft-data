//! Engine-neutral work progress. Rendering belongs to the controller crate.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProgressPhase {
    EncodeCollectTokenSources,
    EncodeTokenSources,
    EncodePrepareFallbackTokenSources,
    EncodeTokenFallbackSources,
    EncodeResolveTokenMemberships,
    EncodeLoadTokenSources,
    EncodeLoadTokenMemberships,
    EncodeSortTokenMemberships,
    EncodeReadRepresentatives,
    EncodeRegisterPayloads,
    EncodeResolveFallbacks,
    EncodeParseUniquePayloads,
    EncodeBuildTermDictionary,
    EncodeBuildColumns,
    EncodeBuildAtoms,
    EncodeRows,
    EncodeFallbackSources,
    EncodeFinalize,
    EncodePersist,
    EncodePublish,
    BlockingCompile,
    BlockingFinalize,
    OpenSnapshot,
    BuildCatalog,
    PairExactIsland,
    PairExactFinalize,
    PairExactHoldout,
    PairExactHoldoutFinalize,
    SharedTokenExactIsland,
    SharedTokenExactFinalize,
    FallbackPairs,
    CatalogPairs,
    PlanSharedTokenPairs,
    SharedTokenPairs,
    PlanRescuePairs,
    RescuePairs,
    EdgeDispatch,
    FinalizeEdgeCollectors,
    CommitConnectivityRuns,
    ReduceScopes,
    BuildRecoveryChain,
    CommitComponents,
    BuildSummary,
    CommitArtifacts,
}

impl ProgressPhase {
    pub const fn label(self) -> &'static str {
        match self {
            Self::EncodeCollectTokenSources => "collect retained-token sources",
            Self::EncodeTokenSources => "classify retained-token sources",
            Self::EncodePrepareFallbackTokenSources => "prepare retained-token fallback sources",
            Self::EncodeTokenFallbackSources => "classify retained-token fallback sources",
            Self::EncodeResolveTokenMemberships => "resolve retained-token memberships",
            Self::EncodeLoadTokenSources => "load retained-token source documents",
            Self::EncodeLoadTokenMemberships => "load retained-token memberships",
            Self::EncodeSortTokenMemberships => "sort retained-token memberships",
            Self::EncodeReadRepresentatives => "read representative rows",
            Self::EncodeRegisterPayloads => "register unique payloads",
            Self::EncodeResolveFallbacks => "resolve fallback sources",
            Self::EncodeParseUniquePayloads => "parse unique payloads",
            Self::EncodeBuildTermDictionary => "build term dictionary",
            Self::EncodeBuildColumns => "build encode columns",
            Self::EncodeBuildAtoms => "build encode atoms",
            Self::EncodeRows => "encode representative rows",
            Self::EncodeFallbackSources => "resolve fallback metadata sources",
            Self::EncodeFinalize => "finalize encoded features",
            Self::EncodePersist => "persist encoded features",
            Self::EncodePublish => "publish encoded snapshot",
            Self::BlockingCompile => "compile blocking",
            Self::BlockingFinalize => "finalize blocking artifacts",
            Self::OpenSnapshot => "open snapshot",
            Self::BuildCatalog => "build catalog",
            Self::PairExactIsland => "pair exact island",
            Self::PairExactFinalize => "finalize pair exact evidence",
            Self::PairExactHoldout => "pair exact holdout",
            Self::PairExactHoldoutFinalize => "finalize pair exact holdout",
            Self::SharedTokenExactIsland => "shared-token exact island",
            Self::SharedTokenExactFinalize => "finalize shared-token exact evidence",
            Self::FallbackPairs => "fallback pairs",
            Self::CatalogPairs => "catalog pairs",
            Self::PlanSharedTokenPairs => "plan shared-token pairs",
            Self::SharedTokenPairs => "shared-token pairs",
            Self::PlanRescuePairs => "plan rescue pairs",
            Self::RescuePairs => "frozen rescue pairs",
            Self::EdgeDispatch => "edge dispatch",
            Self::FinalizeEdgeCollectors => "finalize edge collectors",
            Self::CommitConnectivityRuns => "commit connectivity runs",
            Self::ReduceScopes => "reduce scopes",
            Self::BuildRecoveryChain => "build recovery snapshot chain",
            Self::CommitComponents => "commit component snapshots",
            Self::BuildSummary => "build metadata summary",
            Self::CommitArtifacts => "commit artifacts",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnit {
    Work,
    Items,
    Pairs,
    Edges,
    Nodes,
    Bytes,
    Files,
}

/// Stable cost dimensions used to aggregate progress across Match subphases.
/// A class must describe one kind of work; callers must not fold conditional
/// contract expansion into catalog routing, for example.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WorkClass {
    Generic,
    SnapshotBytes,
    ScanItems,
    CatalogRoutes,
    AtomScores,
    ContractExpansions,
    SharedScores,
    ReduceItems,
    ArtifactBytes,
    ArtifactFiles,
}

/// Whether a phase total can support a finite ETA.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TotalKind {
    Exact,
    UpperBound,
    Estimate,
    Unknown,
}

impl WorkUnit {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Work => "work",
            Self::Items => "items",
            Self::Pairs => "pairs",
            Self::Edges => "edges",
            Self::Nodes => "nodes",
            Self::Bytes => "bytes",
            Self::Files => "files",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgressCounters {
    pub candidates: u64,
    pub scored: u64,
    pub expanded: u64,
    pub matched: u64,
    pub groups: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgressEvent {
    pub phase: ProgressPhase,
    pub completed: u64,
    pub total: Option<u64>,
    pub unit: WorkUnit,
    pub work_class: WorkClass,
    pub total_kind: TotalKind,
    pub counters: ProgressCounters,
}

impl ProgressEvent {
    pub const fn determinate(
        phase: ProgressPhase,
        completed: u64,
        total: u64,
        unit: WorkUnit,
        counters: ProgressCounters,
    ) -> Self {
        Self {
            phase,
            // Keep the engine's true position.  Consumers may clamp only for
            // drawing; hiding an overrun would corrupt planning evidence.
            completed,
            total: Some(total),
            unit,
            work_class: WorkClass::Generic,
            total_kind: TotalKind::Exact,
            counters,
        }
    }

    pub const fn indeterminate(
        phase: ProgressPhase,
        completed: u64,
        unit: WorkUnit,
        counters: ProgressCounters,
    ) -> Self {
        Self {
            phase,
            completed,
            total: None,
            unit,
            work_class: WorkClass::Generic,
            total_kind: TotalKind::Unknown,
            counters,
        }
    }

    pub const fn with_plan(mut self, work_class: WorkClass, total_kind: TotalKind) -> Self {
        self.work_class = work_class;
        self.total_kind = total_kind;
        self
    }

    pub const fn exact_total_overrun(self) -> Option<u64> {
        match (self.total_kind, self.total) {
            (TotalKind::Exact, Some(total)) if self.completed > total => {
                Some(self.completed - total)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_progress_preserves_and_reports_plan_overrun() {
        let event = ProgressEvent::determinate(
            ProgressPhase::CatalogPairs,
            11,
            10,
            WorkUnit::Pairs,
            ProgressCounters::default(),
        );

        assert_eq!(event.completed, 11);
        assert_eq!(event.exact_total_overrun(), Some(1));
    }

    #[test]
    fn unknown_progress_cannot_claim_a_finite_total() {
        let event = ProgressEvent::indeterminate(
            ProgressPhase::SharedTokenPairs,
            3,
            WorkUnit::Pairs,
            ProgressCounters::default(),
        );

        assert_eq!(event.total, None);
        assert_eq!(event.total_kind, TotalKind::Unknown);
        assert_eq!(event.exact_total_overrun(), None);
    }
}
