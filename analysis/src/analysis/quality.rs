use crate::model::EvidenceStatus;

pub fn complete_ratio(
    numerator: u64,
    denominator: u64,
    status: Option<EvidenceStatus>,
) -> Option<f64> {
    if denominator == 0 || status != Some(EvidenceStatus::Complete) {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}
