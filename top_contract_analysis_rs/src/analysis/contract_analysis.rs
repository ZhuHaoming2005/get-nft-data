use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Instant;

use crate::error::AppError;
use crate::models::{ContractMetadata, DuplicateCandidate, DuplicateContractPayload};

use super::{
    address_records, analyze_victim_signals_from_active_sellers,
    compute_mint_payment_edges_for_contract, compute_sale_metrics_for_contract,
    map_address_signals, propagation, signals, timing, AnalysisDeps, AnalysisOutputState,
    AnalyzeRequest, ContractAnalysisResult,
};

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
    state
        .honest_address_stats
        .extend(result.honest_address_stats);
    state.fraud_trade_stats.extend(result.fraud_trade_stats);
    state.infringing_tokens.extend(result.infringing_tokens);
    state.malicious_addresses.extend(result.malicious_addresses);
    state.honest_addresses.extend(result.honest_addresses);
    state
        .secondary_sale_victim_addresses
        .extend(result.secondary_sale_victim_addresses);
    state
        .address_attributions
        .extend(result.address_attributions);
    state.market_events.extend(result.market_events);
    state.mint_payment_edges.extend(result.mint_payment_edges);
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
        honest_address_stats: BTreeMap::new(),
        secondary_sale_victim_addresses: Vec::new(),
        address_attributions: Vec::new(),
        market_events: Vec::new(),
        mint_payment_edges: Vec::new(),
        fraud_trade_stats: BTreeMap::new(),
        nft_propagation_path: None,
    }
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
    let (transfers, owners) = timing::time_async(
        format!("contract:{contract_address}:fetch_transfers_and_owners"),
        async {
            tokio::try_join!(
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
            )
        },
    )
    .await?;
    let signal_timer = Instant::now();
    let transfer_signals = signals::analyze_transfer_signals(&transfers);
    let victim_signal = analyze_victim_signals_from_active_sellers(&transfers, &owners);
    timing::log_timing(
        &format!("contract:{contract_address}:analyze_transfer_and_victim_signals"),
        signal_timer,
    );

    let infringing_timer = Instant::now();
    let contract_infringing = address_records::build_infringing_token_records_with_context_refs(
        contract_address,
        &contract_candidate_refs,
        &transfers,
        official_addresses,
        candidate_open_license_by_token,
    );
    timing::log_timing(
        &format!("contract:{contract_address}:build_infringing_tokens"),
        infringing_timer,
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
            honest_address_stats: BTreeMap::new(),
            secondary_sale_victim_addresses: vec![],
            address_attributions: vec![],
            market_events: vec![],
            mint_payment_edges: vec![],
            fraud_trade_stats: BTreeMap::new(),
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
    let (sales, mint_payment_edges) = timing::time_async(
        format!("contract:{contract_address}:fetch_sales_and_mint_value_flow"),
        async { tokio::try_join!(sales_fut, mint_payment_edges_fut) },
    )
    .await?;
    let sale_metrics_by_tx = timing::time_async(
        format!("contract:{contract_address}:compute_sale_metrics"),
        compute_sale_metrics_for_contract(request, deps, &sales),
    )
    .await?;

    let address_timer = Instant::now();
    let contract_activity = address_records::prepare_contract_activity(&transfers, &sales, &owners);
    let contract_malicious = address_records::build_malicious_address_records_from_activity(
        contract_address,
        &contract_activity,
        &contract_infringing,
        &mint_payment_edges,
    );
    let contract_secondary_sale_victims =
        address_records::build_secondary_sale_victim_address_records_from_activity(
            contract_address,
            &contract_activity,
            &sale_metrics_by_tx,
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
    timing::log_timing(
        &format!("contract:{contract_address}:build_address_records"),
        address_timer,
    );
    let propagation_timer = Instant::now();
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
    timing::log_timing(
        &format!("contract:{contract_address}:build_propagation_path"),
        propagation_timer,
    );

    Ok(ContractAnalysisResult {
        contract_address: contract_address.to_string(),
        contract_metadata,
        implausible_candidate_filtered: false,
        legit_duplicate: None,
        address_signal: Some(map_address_signals(&transfer_signals)),
        victim_signal: Some(victim_signal),
        honest_address_stats: address_records::build_honest_address_stats(
            contract_address,
            &contract_honest,
        ),
        fraud_trade_stats: address_records::build_fraud_trade_stats(
            contract_address,
            &sales,
            &contract_secondary_sale_victims,
        ),
        infringing_tokens: contract_infringing,
        malicious_addresses: contract_malicious,
        honest_addresses: contract_honest,
        secondary_sale_victim_addresses: contract_secondary_sale_victims,
        address_attributions,
        market_events: Vec::new(),
        mint_payment_edges,
        nft_propagation_path: Some(nft_propagation_path),
    })
}
