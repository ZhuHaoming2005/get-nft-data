#[derive(Clone, Debug)]
pub enum CandidateRelationsEvent {
    Prefetch(Vec<crate::model::SeedCandidateRelation>),
    Frozen(Vec<crate::model::SeedCandidateRelation>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CpuTaskKind {
    Dedup,
    ResponseDecode,
    Analysis,
    Compress,
}
