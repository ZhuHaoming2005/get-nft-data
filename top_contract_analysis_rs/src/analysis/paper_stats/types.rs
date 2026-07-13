use super::*;

#[derive(Default)]
pub(super) struct AddressSets {
    pub(super) malicious: BTreeSet<String>,
    pub(super) honest: BTreeSet<String>,
    pub(super) repeat_infringing_malicious: BTreeSet<String>,
}

pub(super) fn ratio_i64(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

pub(super) fn ratio_f64(numerator: f64, denominator: f64) -> Option<f64> {
    (denominator > 0.0).then_some(numerator / denominator)
}

pub(super) fn normalized_address(address: &str) -> String {
    normalize_chain_identity(address)
}

pub(super) fn normalized_contract(contract: &str) -> String {
    let contract = normalized_address(contract);
    if contract.is_empty() {
        "unknown".into()
    } else {
        contract
    }
}

pub(super) fn is_participant_address(address: &str) -> bool {
    !address.is_empty() && address != ZERO_ADDRESS
}

fn category_reason(category: &str) -> Option<&'static str> {
    match category {
        "token_uri" => Some("token_uri_match"),
        "image_uri" => Some("image_uri_match"),
        "metadata" => Some("metadata_match"),
        "name" => Some("name_match"),
        "total" => None,
        _ => None,
    }
}

pub(super) fn match_reasons_match_category(match_reasons: &[String], category: &str) -> bool {
    category_reason(category)
        .map(|reason| match_reasons.iter().any(|item| item == reason))
        .unwrap_or(true)
}
