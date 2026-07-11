use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::{stream, StreamExt};

use crate::error::AppError;
use crate::models::{
    normalize_chain_identity, DatabaseNftRecord, DuplicateCandidate, DuplicateContractPayload,
    SeedNft,
};

use super::{
    AnalysisDeps, AnalyzeRequest, CandidateContractFilterResult, CandidateSeedHolderRequest,
};

pub fn group_candidates_by_contract(
    candidates: &[DuplicateCandidate],
) -> BTreeMap<String, Vec<usize>> {
    let mut grouped = BTreeMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        grouped
            .entry(candidate.contract_address.clone())
            .or_insert_with(Vec::new)
            .push(index);
    }
    grouped
}

pub(super) enum CandidateSeedRelationCheck {
    Exclude(&'static str),
    Holder(Result<Option<bool>, AppError>),
}

pub(super) async fn filter_seed_related_candidate_contracts(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    candidates: Vec<DuplicateCandidate>,
    seed_token_type: &str,
    concurrency: usize,
) -> CandidateContractFilterResult {
    if candidates.is_empty() {
        return CandidateContractFilterResult {
            candidates,
            seed_related_legit_duplicates: vec![],
        };
    }

    let candidate_contracts: BTreeMap<String, String> = candidates
        .iter()
        .map(|candidate| {
            (
                normalize_chain_identity(&candidate.contract_address),
                candidate.contract_address.clone(),
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut exclusion_reasons_by_contract = BTreeMap::<String, BTreeSet<String>>::new();

    let seed_collection_slug = if candidate_contracts.is_empty() {
        None
    } else {
        match deps
            .api
            .fetch_seed_collection_slug(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                &request.opensea_api_key,
                &request.seed_contract_address,
            )
            .await
        {
            Ok(collection_slug) => collection_slug,
            Err(err) => {
                eprintln!(
                    "warning: OpenSea seed collection lookup failed for {}: {err}; falling back to Alchemy isHolderOfContract",
                    request.seed_contract_address
                );
                None
            }
        }
    };
    let normalized_seed_collection_slug = seed_collection_slug
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());

    let mut holder_checks = stream::iter(candidate_contracts.values().cloned().map(|contract_address| {
        let seed_collection_slug = seed_collection_slug.clone();
        let normalized_seed_collection_slug = normalized_seed_collection_slug.clone();
        async move {
            if let Some(seed_collection_slug) = normalized_seed_collection_slug.as_deref() {
                match deps
                    .api
                    .fetch_contract_collection_slug(
                        &request.chain,
                        &request.alchemy_api_key,
                        request.alchemy_network.as_deref(),
                        &request.opensea_api_key,
                        &contract_address,
                    )
                    .await
                {
                    Ok(Some(candidate_collection_slug))
                        if candidate_collection_slug
                            .trim()
                            .eq_ignore_ascii_case(seed_collection_slug) =>
                    {
                        return (
                            contract_address,
                            CandidateSeedRelationCheck::Exclude("OpenSea collection 与 seed 合约一致"),
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "warning: OpenSea candidate collection lookup failed for {contract_address}: {err}; continuing without collection-based candidate exclusion"
                        );
                    }
                }
            }
            let holds_seed_nft = deps
                .api
                .candidate_currently_holds_seed_nft(CandidateSeedHolderRequest {
                    chain: &request.chain,
                    alchemy_api_key: &request.alchemy_api_key,
                    alchemy_network: request.alchemy_network.as_deref(),
                    opensea_api_key: &request.opensea_api_key,
                    seed_contract_address: &request.seed_contract_address,
                    candidate_contract_address: &contract_address,
                    seed_collection_slug: seed_collection_slug.as_deref(),
                })
                .await;
            (
                contract_address,
                CandidateSeedRelationCheck::Holder(holds_seed_nft),
            )
        }
    }))
    .buffer_unordered(concurrency.max(1));

    while let Some((contract_address, check)) = holder_checks.next().await {
        let contract_key = normalize_chain_identity(&contract_address);
        match check {
            CandidateSeedRelationCheck::Exclude(reason) => {
                exclusion_reasons_by_contract
                    .entry(contract_key)
                    .or_default()
                    .insert(reason.to_string());
            }
            CandidateSeedRelationCheck::Holder(Ok(Some(true))) => {
                exclusion_reasons_by_contract
                    .entry(contract_key)
                    .or_default()
                    .insert("当前持有 seed 合约 NFT".to_string());
            }
            CandidateSeedRelationCheck::Holder(Ok(Some(false))) => {}
            CandidateSeedRelationCheck::Holder(Ok(None)) => {}
            CandidateSeedRelationCheck::Holder(Err(err)) => {
                eprintln!(
                    "warning: current seed NFT holder check failed for {contract_address}: {err}; continuing without holder-based candidate exclusion"
                );
            }
        }
    }

    let remaining_contracts: BTreeSet<String> = candidate_contracts
        .keys()
        .filter(|contract_key| !exclusion_reasons_by_contract.contains_key(*contract_key))
        .cloned()
        .collect();
    if !remaining_contracts.is_empty() {
        match deps
            .api
            .fetch_contract_transfers(
                &request.chain,
                &request.etherscan_api_key,
                request.alchemy_network.as_deref(),
                &request.alchemy_api_key,
                &request.seed_contract_address,
                seed_token_type,
            )
            .await
        {
            Ok(seed_transfers) => {
                for transfer in seed_transfers {
                    let to_address = normalize_chain_identity(&transfer.to_address);
                    if remaining_contracts.contains(&to_address) {
                        exclusion_reasons_by_contract
                            .entry(to_address)
                            .or_default()
                            .insert("链上历史 Transfer 显示接收过 seed 合约 NFT".to_string());
                    }
                    let from_address = normalize_chain_identity(&transfer.from_address);
                    if remaining_contracts.contains(&from_address) {
                        exclusion_reasons_by_contract
                            .entry(from_address)
                            .or_default()
                            .insert("链上历史 Transfer 显示转出过 seed 合约 NFT".to_string());
                    }
                }
            }
            Err(err) => {
                eprintln!(
                    "warning: seed NFT transfer history lookup failed for {}: {err}; continuing without historical holder-based candidate exclusion",
                    request.seed_contract_address
                );
            }
        }
    }

    let seed_related_legit_duplicates =
        build_seed_related_legit_duplicate_payloads(&candidates, &exclusion_reasons_by_contract);
    let candidates = candidates
        .into_iter()
        .filter(|candidate| {
            !exclusion_reasons_by_contract
                .contains_key(&normalize_chain_identity(&candidate.contract_address))
        })
        .collect();

    CandidateContractFilterResult {
        candidates,
        seed_related_legit_duplicates,
    }
}

pub(super) fn build_seed_related_legit_duplicate_payloads(
    candidates: &[DuplicateCandidate],
    exclusion_reasons_by_contract: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<DuplicateContractPayload> {
    exclusion_reasons_by_contract
        .iter()
        .filter_map(|(contract_key, reasons)| {
            let contract_candidates: Vec<&DuplicateCandidate> = candidates
                .iter()
                .filter(|candidate| {
                    normalize_chain_identity(&candidate.contract_address) == *contract_key
                })
                .collect();
            if contract_candidates.is_empty() {
                return None;
            }
            let mut match_reasons = BTreeSet::new();
            for candidate in &contract_candidates {
                match_reasons.extend(candidate.match_reasons.iter().cloned());
            }
            Some(DuplicateContractPayload {
                contract_address: contract_candidates[0].contract_address.clone(),
                candidate_count: contract_candidates.len() as i64,
                match_reasons: match_reasons.into_iter().collect(),
                exclusion_reasons: reasons.iter().cloned().collect(),
                ..DuplicateContractPayload::default()
            })
        })
        .collect()
}

pub(super) struct SnapshotTokenIndex<'a> {
    rows_by_contract: HashMap<String, Vec<&'a DatabaseNftRecord>>,
}

impl<'a> SnapshotTokenIndex<'a> {
    pub(super) fn new(snapshot_rows: &'a [DatabaseNftRecord]) -> Self {
        let mut rows_by_contract = HashMap::<String, Vec<&'a DatabaseNftRecord>>::new();
        for row in snapshot_rows {
            rows_by_contract
                .entry(normalize_chain_identity(&row.contract_address))
                .or_default()
                .push(row);
        }
        Self { rows_by_contract }
    }

    pub(super) fn expand_candidates_for_contract(
        &self,
        contract_address: &str,
        candidate_indexes: &[usize],
        candidates: &[DuplicateCandidate],
    ) -> Vec<DuplicateCandidate> {
        let rows = self
            .rows_by_contract
            .get(&normalize_chain_identity(contract_address))
            .into_iter()
            .flat_map(|rows| rows.iter().copied());
        expand_candidate_indexes_to_contract_tokens(
            contract_address,
            candidate_indexes,
            candidates,
            rows,
        )
    }

    pub(super) fn contract_token_count(&self, contract_address: &str) -> usize {
        self.rows_by_contract
            .get(&normalize_chain_identity(contract_address))
            .map(Vec::len)
            .unwrap_or_default()
    }
}

pub(super) async fn fetch_and_expand_contract_candidates(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    grouped: &BTreeMap<String, Vec<usize>>,
    candidates: &[DuplicateCandidate],
    snapshot_token_index: &SnapshotTokenIndex<'_>,
) -> Result<Vec<DuplicateCandidate>, AppError> {
    let candidate_indexes = grouped
        .get(contract_address)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let provider_tokens = deps
        .api
        .fetch_contract_nfts(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &request.etherscan_api_key,
            &request.opensea_api_key,
            contract_address,
        )
        .await;
    let expanded = match provider_tokens {
        Ok(tokens) => {
            let expanded = expand_candidate_indexes_to_contract_tokens(
                contract_address,
                candidate_indexes,
                candidates,
                tokens,
            );
            if expanded.is_empty() {
                eprintln!(
                    "warning: provider NFT expansion returned no tokens for {contract_address}; falling back to local snapshot rows"
                );
                snapshot_token_index.expand_candidates_for_contract(
                    contract_address,
                    candidate_indexes,
                    candidates,
                )
            } else {
                expanded
            }
        }
        Err(err) => {
            eprintln!(
                "warning: provider NFT expansion failed for {contract_address}: {err}; falling back to local snapshot rows"
            );
            snapshot_token_index.expand_candidates_for_contract(
                contract_address,
                candidate_indexes,
                candidates,
            )
        }
    };
    Ok(expanded)
}

trait ContractTokenFields {
    fn contract_address(&self) -> &str;
    fn token_id(&self) -> &str;
    fn token_uri(&self) -> &str;
    fn image_uri(&self) -> &str;
    fn name(&self) -> &str;
    fn symbol(&self) -> &str;
}

impl ContractTokenFields for SeedNft {
    fn contract_address(&self) -> &str {
        &self.contract_address
    }

    fn token_id(&self) -> &str {
        &self.token_id
    }

    fn token_uri(&self) -> &str {
        &self.token_uri
    }

    fn image_uri(&self) -> &str {
        &self.image_uri
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl ContractTokenFields for &DatabaseNftRecord {
    fn contract_address(&self) -> &str {
        &self.contract_address
    }

    fn token_id(&self) -> &str {
        &self.token_id
    }

    fn token_uri(&self) -> &str {
        &self.token_uri
    }

    fn image_uri(&self) -> &str {
        &self.image_uri
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

fn expand_candidate_indexes_to_contract_tokens<I, T>(
    contract_address: &str,
    candidate_indexes: &[usize],
    candidates: &[DuplicateCandidate],
    contract_tokens: I,
) -> Vec<DuplicateCandidate>
where
    I: IntoIterator<Item = T>,
    T: ContractTokenFields,
{
    let template = candidate_indexes
        .iter()
        .find_map(|index| candidates.get(*index))
        .cloned()
        .unwrap_or_else(|| DuplicateCandidate {
            contract_address: contract_address.to_string(),
            ..DuplicateCandidate::default()
        });
    let contract_key = normalize_chain_identity(contract_address);
    let mut seen_tokens = BTreeSet::new();
    let mut expanded: Vec<DuplicateCandidate> = contract_tokens
        .into_iter()
        .filter(|row| normalize_chain_identity(row.contract_address()) == contract_key)
        .filter_map(|row| {
            let token_id = row.token_id().trim();
            if token_id.is_empty() || !seen_tokens.insert(token_id.to_string()) {
                return None;
            }
            Some(DuplicateCandidate {
                contract_address: row.contract_address().to_string(),
                token_id: token_id.to_string(),
                match_reasons: template.match_reasons.clone(),
                confidence: template.confidence.clone(),
                token_uri: row.token_uri().to_string(),
                image_uri: row.image_uri().to_string(),
                name: row.name().to_string(),
                symbol: row.symbol().to_string(),
            })
        })
        .collect();
    expanded.sort_by(|left, right| left.token_id.cmp(&right.token_id));
    expanded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_token_expansion_skips_empty_token_ids() {
        let candidates = vec![DuplicateCandidate {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        }];
        let tokens = vec![
            SeedNft {
                contract_address: "0xdup".into(),
                token_id: String::new(),
                name: "bad token".into(),
                ..SeedNft::default()
            },
            SeedNft {
                contract_address: "0xdup".into(),
                token_id: "2".into(),
                name: "good token".into(),
                ..SeedNft::default()
            },
        ];

        let expanded =
            expand_candidate_indexes_to_contract_tokens("0xdup", &[0], &candidates, tokens);

        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].token_id, "2");
        assert_eq!(expanded[0].match_reasons, vec!["token_uri_match"]);
    }

    #[test]
    fn contract_token_expansion_preserves_case_sensitive_solana_identity() {
        let candidates = vec![DuplicateCandidate {
            contract_address: "CaseSensitiveSolanaAddress".into(),
            token_id: "1".into(),
            ..DuplicateCandidate::default()
        }];
        let tokens = vec![
            SeedNft {
                contract_address: "CaseSensitiveSolanaAddress".into(),
                token_id: "1".into(),
                ..SeedNft::default()
            },
            SeedNft {
                contract_address: "casesensitivesolanaaddress".into(),
                token_id: "2".into(),
                ..SeedNft::default()
            },
        ];

        let expanded = expand_candidate_indexes_to_contract_tokens(
            "CaseSensitiveSolanaAddress",
            &[0],
            &candidates,
            tokens,
        );

        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].token_id, "1");
    }
}
