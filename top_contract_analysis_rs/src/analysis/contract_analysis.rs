use std::collections::{BTreeSet, HashMap, HashSet};

use crate::error::AppError;
use crate::models::{ContractMetadata, DuplicateCandidate, DuplicateContractPayload};

use super::{
    address_records, analyze_victim_signals_from_active_sellers,
    compute_attacker_cost_edges_for_contract, compute_mint_payment_edges_for_contract,
    map_address_signals, propagation, signals, AnalysisDeps, AnalysisOutputState, AnalyzeRequest,
    ContractAnalysisResult,
};

const CURRENT_SUPPLY_MISMATCH_MIN_CANDIDATES: usize = 20;
const CURRENT_SUPPLY_MISMATCH_MIN_MULTIPLE: u64 = 5;
const CURRENT_SUPPLY_MISMATCH_ABSOLUTE_TOLERANCE: u64 = 10;
const CURRENT_SUPPLY_MISMATCH_RELATIVE_TOLERANCE_DIVISOR: u64 = 20;

pub(super) fn merge_contract_analysis_result(
    result: ContractAnalysisResult,
    state: &mut AnalysisOutputState,
) {
    if let Some(metadata) = result.contract_metadata {
        state
            .candidate_contract_metadata
            .insert(result.contract_address.clone(), metadata);
    }
    if result.implausible_candidate_filtered {
        state
            .implausible_candidate_contracts
            .insert(result.contract_address);
        return;
    }
    if let Some(legit_duplicate) = result.legit_duplicate {
        state
            .legit_contract_addresses
            .insert(result.contract_address.clone());
        state.legit_duplicates.push(legit_duplicate);
        return;
    }
    if let Some(address_signal) = result.address_signal {
        state
            .address_signals
            .insert(result.contract_address.clone(), address_signal);
    }
    if let Some(victim_signal) = result.victim_signal {
        state
            .victim_signals
            .insert(result.contract_address.clone(), victim_signal);
    }
    state.infringing_tokens.extend(result.infringing_tokens);
    state.malicious_addresses.extend(result.malicious_addresses);
    state.honest_addresses.extend(result.honest_addresses);
    state
        .secondary_sale_victim_addresses
        .extend(result.secondary_sale_victim_addresses);
    state
        .address_attributions
        .extend(result.address_attributions);
    state.mint_payment_edges.extend(result.mint_payment_edges);
    state.attacker_cost_edges.extend(result.attacker_cost_edges);
    if let Some(path) = result.nft_propagation_path {
        state
            .nft_propagation_paths
            .insert(result.contract_address, path);
    }
}

pub(super) fn payload_token_type(seed_contract: &ContractMetadata) -> String {
    if seed_contract.token_type.trim().is_empty() {
        "ERC721".into()
    } else {
        seed_contract.token_type.clone()
    }
}

pub(super) fn enrich_duplicate_contract_payload_with_metadata(
    mut payload: DuplicateContractPayload,
    metadata: Option<&ContractMetadata>,
) -> DuplicateContractPayload {
    if let Some(metadata) = metadata {
        payload.contract_deployer = metadata.contract_deployer.clone();
        payload.deployed_block_number = metadata.deployed_block_number;
        payload.deployed_block_time = metadata.deployed_block_time;
        payload.token_type = metadata.token_type.clone();
        payload.owner_address = metadata.owner_address.clone();
        payload.admin_address = metadata.admin_address.clone();
        payload.proxy_admin_address = metadata.proxy_admin_address.clone();
        payload.name = metadata.name.clone();
        payload.symbol = metadata.symbol.clone();
    }
    payload
}

pub(super) fn deployed_before_seed(
    seed_deployed_block_number: i64,
    metadata: Option<&ContractMetadata>,
) -> bool {
    seed_deployed_block_number > 0
        && metadata
            .map(|metadata| {
                metadata.deployed_block_number > 0
                    && metadata.deployed_block_number < seed_deployed_block_number
            })
            .unwrap_or(false)
}

pub(super) fn implausible_candidate_filtered_result(
    contract_address: &str,
    contract_metadata: Option<ContractMetadata>,
) -> ContractAnalysisResult {
    ContractAnalysisResult {
        contract_address: contract_address.to_string(),
        contract_metadata,
        implausible_candidate_filtered: true,
        legit_duplicate: None,
        address_signal: None,
        victim_signal: None,
        infringing_tokens: Vec::new(),
        malicious_addresses: Vec::new(),
        honest_addresses: Vec::new(),
        secondary_sale_victim_addresses: Vec::new(),
        address_attributions: Vec::new(),
        mint_payment_edges: Vec::new(),
        attacker_cost_edges: Vec::new(),
        nft_propagation_path: None,
    }
}

pub(super) async fn fetch_current_total_supply_for_candidate_filter(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
) -> Option<u64> {
    match deps
        .api
        .fetch_contract_total_supply(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            contract_address,
        )
        .await
    {
        Ok(Some(current_total_supply)) => Some(current_total_supply),
        Ok(None) => None,
        Err(err) => {
            eprintln!(
                "warning: totalSupply lookup failed for {contract_address}: {err}; continuing without current-supply hard exclusion"
            );
            None
        }
    }
}

pub(super) fn current_supply_implausibly_smaller_than_candidate_count(
    contract_address: &str,
    candidate_count: usize,
    current_total_supply: Option<u64>,
) -> bool {
    let Some(current_total_supply) = current_total_supply else {
        return false;
    };
    if !candidate_count_exceeds_current_supply(candidate_count, current_total_supply) {
        return false;
    }

    eprintln!(
        "warning: excluding implausible duplicate candidate {contract_address}: expanded candidate token count ({candidate_count}) conflicts with current totalSupply ({current_total_supply})"
    );
    true
}

pub(super) fn should_check_current_supply_for_candidate_count(candidate_count: usize) -> bool {
    candidate_count >= CURRENT_SUPPLY_MISMATCH_MIN_CANDIDATES
}

fn candidate_count_exceeds_current_supply(
    candidate_count: usize,
    current_total_supply: u64,
) -> bool {
    if !should_check_current_supply_for_candidate_count(candidate_count) {
        return false;
    }
    if current_total_supply == 0 {
        return true;
    }

    let candidate_count = candidate_count as u64;
    let tolerated_drift = CURRENT_SUPPLY_MISMATCH_ABSOLUTE_TOLERANCE
        .max(current_total_supply / CURRENT_SUPPLY_MISMATCH_RELATIVE_TOLERANCE_DIVISOR);
    candidate_count > current_total_supply.saturating_add(tolerated_drift)
        && candidate_count
            >= current_total_supply.saturating_mul(CURRENT_SUPPLY_MISMATCH_MIN_MULTIPLE)
}

pub(super) async fn fetch_candidate_contract_metadata(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
) -> Result<Option<ContractMetadata>, AppError> {
    match deps
        .api
        .fetch_contract_metadata(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &request.opensea_api_key,
            contract_address,
        )
        .await
    {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) => {
            eprintln!(
                "warning: contract metadata lookup failed for {contract_address}: {err}; continuing without deployment metadata"
            );
            Ok(None)
        }
    }
}

pub(super) struct DuplicateContractAnalysisInput<'a> {
    pub(super) request: &'a AnalyzeRequest,
    pub(super) deps: &'a AnalysisDeps,
    pub(super) token_type: &'a str,
    pub(super) contract_address: &'a str,
    pub(super) contract_candidates: &'a [DuplicateCandidate],
    pub(super) contract_metadata: Option<ContractMetadata>,
    pub(super) official_addresses: &'a HashSet<String>,
    pub(super) candidate_open_license_by_token: &'a HashMap<(String, String), bool>,
    pub(super) analysis_timestamp: i64,
}

pub(super) async fn analyze_duplicate_contract(
    input: DuplicateContractAnalysisInput<'_>,
) -> Result<ContractAnalysisResult, AppError> {
    let DuplicateContractAnalysisInput {
        request,
        deps,
        token_type,
        contract_address,
        contract_candidates,
        contract_metadata,
        official_addresses,
        candidate_open_license_by_token,
        analysis_timestamp,
    } = input;
    let contract_candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    let (transfers, owners) = tokio::try_join!(
        deps.api.fetch_contract_transfers(
            &request.chain,
            &request.etherscan_api_key,
            request.alchemy_network.as_deref(),
            &request.alchemy_api_key,
            contract_address,
            token_type,
        ),
        deps.api.fetch_contract_owners(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            contract_address,
        )
    )?;
    let transfer_signals = signals::analyze_transfer_signals(&transfers);
    let victim_signal = analyze_victim_signals_from_active_sellers(&transfers, &owners);

    let contract_infringing = address_records::build_infringing_token_records_with_context_refs(
        contract_address,
        &contract_candidate_refs,
        &transfers,
        official_addresses,
        candidate_open_license_by_token,
    );
    if !contract_infringing.is_empty()
        && contract_infringing
            .iter()
            .all(|item| item.official_or_legit_reissue)
    {
        return Ok(ContractAnalysisResult {
            contract_address: contract_address.to_string(),
            contract_metadata: contract_metadata.clone(),
            implausible_candidate_filtered: false,
            legit_duplicate: Some(enrich_duplicate_contract_payload_with_metadata(
                DuplicateContractPayload {
                    contract_address: contract_address.to_string(),
                    candidate_count: contract_candidates.len() as i64,
                    mint_recipients: contract_infringing
                        .iter()
                        .filter(|item| !item.minter_address.is_empty())
                        .map(|item| item.minter_address.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    ..DuplicateContractPayload::default()
                },
                contract_metadata.as_ref(),
            )),
            address_signal: None,
            victim_signal: None,
            infringing_tokens: vec![],
            malicious_addresses: vec![],
            honest_addresses: vec![],
            secondary_sale_victim_addresses: vec![],
            address_attributions: vec![],
            mint_payment_edges: vec![],
            attacker_cost_edges: vec![],
            nft_propagation_path: None,
        });
    }

    let sales_fut = async {
        deps.api
            .fetch_contract_sales(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                contract_address,
                &request.opensea_api_key,
            )
            .await
    };
    let mint_payment_edges_fut = compute_mint_payment_edges_for_contract(
        request,
        deps,
        contract_address,
        &contract_infringing,
        &transfers,
        contract_metadata.as_ref(),
    );
    let (sales, mint_payment_edges) = tokio::try_join!(sales_fut, mint_payment_edges_fut)?;

    let contract_activity = address_records::prepare_contract_activity(&transfers, &sales, &owners);
    let contract_malicious = address_records::build_malicious_address_records_from_activity(
        contract_address,
        &contract_activity,
        &contract_infringing,
        &mint_payment_edges,
    );
    let contract_secondary_sale_victims =
        address_records::build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
            contract_address,
            &contract_activity,
            &contract_malicious,
        );
    let contract_honest = address_records::build_honest_address_records_from_activity(
        contract_address,
        &contract_activity,
        &contract_infringing,
        &contract_malicious,
        &mint_payment_edges,
        contract_metadata
            .as_ref()
            .map(|metadata| metadata.deployed_block_time)
            .unwrap_or_default(),
        analysis_timestamp,
    );
    let address_attributions = address_records::build_address_attribution_records(
        contract_address,
        &contract_infringing,
        &sales,
        &mint_payment_edges,
        &contract_malicious,
        &contract_honest,
        &contract_secondary_sale_victims,
    );
    let nft_propagation_path =
        propagation::build_nft_propagation_path(propagation::NftPropagationInput {
            contract_address,
            transfers: &transfers,
            sales: &sales,
            owners: &owners,
            infringing_tokens: &contract_infringing,
            malicious_addresses: &contract_malicious,
            honest_addresses: &contract_honest,
            secondary_sale_victim_addresses: &contract_secondary_sale_victims,
        });
    let attacker_cost_edges = compute_attacker_cost_edges_for_contract(
        request,
        deps,
        contract_address,
        contract_metadata.as_ref(),
        &sales,
        &contract_malicious,
        &contract_honest,
    )
    .await?;

    Ok(ContractAnalysisResult {
        contract_address: contract_address.to_string(),
        contract_metadata,
        implausible_candidate_filtered: false,
        legit_duplicate: None,
        address_signal: Some(map_address_signals(&transfer_signals)),
        victim_signal: Some(victim_signal),
        infringing_tokens: contract_infringing,
        malicious_addresses: contract_malicious,
        honest_addresses: contract_honest,
        secondary_sale_victim_addresses: contract_secondary_sale_victims,
        address_attributions,
        mint_payment_edges,
        attacker_cost_edges,
        nft_propagation_path: Some(nft_propagation_path),
    })
}
