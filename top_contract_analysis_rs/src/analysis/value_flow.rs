use std::collections::{BTreeMap, BTreeSet, VecDeque};

use futures::{stream, StreamExt};

use crate::currency::FALLBACK_ETH_USD_RATE;
use crate::error::AppError;
use crate::models::{
    normalize_chain_identity, Chain, ContractMetadata, EthTransferRecord, HonestAddressPayload,
    InfringingTokenRecord, MaliciousAddressPayload, NftSaleRecord, TransactionReceiptRecord,
    TransferRecord, ValueFlowEdgePayload, ZERO_ADDRESS,
};

use super::{AnalysisDeps, AnalyzeRequest};

const MAX_WITHDRAWAL_TRACE_HOPS: usize = 3;
const MAX_WITHDRAWAL_TRACE_FRONTIER: usize = 32;
const MIN_CASHOUT_VALUE_RATIO: f64 = 0.05;
const MAX_CASHOUT_VALUE_RATIO: f64 = 1.02;

#[derive(Clone, Copy)]
struct KnownValueFlowEntity {
    chain: &'static str,
    address: &'static str,
    role: &'static str,
    label: &'static str,
}

const KNOWN_VALUE_FLOW_ENTITIES: &[KnownValueFlowEntity] = &[
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x28c6c06298d514db089934071355e5743bf21d60",
        role: "cex",
        label: "binance_hot_wallet",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x503828976d22510aad0201ac7ec88293211d23da",
        role: "cex",
        label: "coinbase_hot_wallet",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0",
        role: "cex",
        label: "kraken_hot_wallet",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a",
        role: "bridge",
        label: "arbitrum_one_bridge",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1",
        role: "bridge",
        label: "optimism_standard_bridge",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0xa0c68c638235ee32657e8f720a23cec1bfc77c77",
        role: "bridge",
        label: "polygon_bridge",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b",
        role: "mixer",
        label: "tornado_cash_0_1_eth",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936",
        role: "mixer",
        label: "tornado_cash_1_eth",
    },
    KnownValueFlowEntity {
        chain: "ethereum",
        address: "0x910cbd523d972eb0a6f4cae4618ad62622b39dbf",
        role: "mixer",
        label: "tornado_cash_10_eth",
    },
];

pub(super) struct MintPaymentLookup {
    pub(super) tx_hash: String,
    pub(super) block_number: i64,
    pub(super) block_time: i64,
    pub(super) minter_address: String,
    pub(super) token_ids: Vec<String>,
}

pub(super) struct MintPaymentInputs {
    lookup: MintPaymentLookup,
    transfers: Vec<EthTransferRecord>,
    receipt: Option<TransactionReceiptRecord>,
    base_balance_eth: Option<f64>,
    block_receipts: BTreeMap<String, TransactionReceiptRecord>,
}

struct CashoutTraceNode {
    address: String,
    depth: usize,
    previous_tx_hash: String,
    previous_tx_index: Option<i64>,
    value_eth: Option<f64>,
    value_usd: Option<f64>,
    payment_token_symbol: String,
    payment_token_address: String,
    path_addresses: BTreeSet<String>,
}

struct ValueFlowEdgeInput<'a> {
    chain: &'a str,
    contract_address: &'a str,
    lookup: &'a MintPaymentLookup,
    transfer: &'a EthTransferRecord,
    receipt: Option<&'a TransactionReceiptRecord>,
    wallet_snapshot: MintPaymentWalletSnapshot,
    channel: String,
    from_role: String,
    to_role: String,
    evidence_type: String,
    evidence_flags: Vec<String>,
}

struct CashoutTraceInput<'a> {
    request: &'a AnalyzeRequest,
    deps: &'a AnalysisDeps,
    contract_address: &'a str,
    contract_metadata: Option<&'a ContractMetadata>,
    lookup: &'a MintPaymentLookup,
    seed_transfers: &'a [EthTransferRecord],
    direct_withdrawal_edges: &'a [ValueFlowEdgePayload],
    mint_receipt: Option<&'a TransactionReceiptRecord>,
    block_receipts: &'a BTreeMap<String, TransactionReceiptRecord>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct MintPaymentWalletSnapshot {
    before_eth_balance: Option<f64>,
    before_usd_balance: Option<f64>,
}

pub(super) async fn compute_mint_payment_edges_for_contract(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    infringing_tokens: &[InfringingTokenRecord],
    transfers: &[TransferRecord],
    contract_metadata: Option<&ContractMetadata>,
) -> Result<Vec<ValueFlowEdgePayload>, AppError> {
    let lookups = build_mint_payment_lookups(contract_address, infringing_tokens, transfers);
    if lookups.is_empty() {
        return Ok(vec![]);
    }
    let contract_deployer = contract_metadata
        .map(|metadata| metadata.contract_deployer.clone())
        .unwrap_or_default();
    let transfer_rows_by_request = fetch_mint_payment_transfers_for_lookups(
        request,
        deps,
        contract_address,
        &lookups,
        contract_metadata,
    )
    .await?;

    let mut inputs = Vec::with_capacity(lookups.len());
    let mut receipt_tx_hashes = BTreeSet::<String>::new();
    let mut balance_requests = BTreeSet::<(String, i64, String)>::new();
    let mut block_receipt_requests = BTreeSet::<i64>::new();
    let mut contract_transfer_requests = BTreeSet::<i64>::new();
    for lookup in lookups {
        let transfers = mint_payment_transfers_for_lookup(
            &lookup,
            contract_address,
            contract_metadata,
            &transfer_rows_by_request,
        );
        let has_mint_payment_transfer = transfers.iter().any(|transfer| {
            is_matching_mint_payment_transfer(
                transfer,
                &lookup,
                contract_address,
                &contract_deployer,
                contract_metadata,
            )
        });
        if has_mint_payment_transfer {
            receipt_tx_hashes.insert(lookup.tx_hash.clone());
            balance_requests.insert((
                lookup.minter_address.clone(),
                lookup.block_number - 1,
                if request.chain.eq_ignore_ascii_case("solana") {
                    lookup.tx_hash.clone()
                } else {
                    String::new()
                },
            ));
            if !request.chain.eq_ignore_ascii_case("solana") {
                block_receipt_requests.insert(lookup.block_number);
                contract_transfer_requests.insert(lookup.block_number);
            }
        }
        inputs.push(MintPaymentInputs {
            lookup,
            transfers,
            receipt: None,
            base_balance_eth: None,
            block_receipts: BTreeMap::new(),
        });
    }

    let receipts_by_tx = fetch_mint_payment_receipts(request, deps, receipt_tx_hashes).await?;
    let balances_by_request = fetch_mint_payment_balances(request, deps, balance_requests).await?;
    let block_receipts_by_block =
        fetch_mint_payment_block_receipts(request, deps, block_receipt_requests).await?;
    let contract_transfers_by_block = fetch_contract_value_transfers_for_blocks(
        request,
        deps,
        contract_address,
        contract_transfer_requests,
    )
    .await?;

    let mut rows = BTreeMap::<String, ValueFlowEdgePayload>::new();
    for mut inputs in inputs {
        if let Some(contract_transfers) =
            contract_transfers_by_block.get(&inputs.lookup.block_number)
        {
            inputs.transfers.extend(contract_transfers.clone());
            inputs.transfers = deduplicate_eth_transfer_rows(inputs.transfers);
        }
        inputs.receipt = receipts_by_tx.get(&inputs.lookup.tx_hash).cloned();
        inputs.base_balance_eth = balances_by_request
            .get(&(
                inputs.lookup.minter_address.clone(),
                inputs.lookup.block_number - 1,
                if request.chain.eq_ignore_ascii_case("solana") {
                    inputs.lookup.tx_hash.clone()
                } else {
                    String::new()
                },
            ))
            .copied();
        inputs.block_receipts = block_receipts_by_block
            .get(&inputs.lookup.block_number)
            .cloned()
            .unwrap_or_default();
        let lookup = inputs.lookup;
        let eth_transfers = inputs.transfers;
        let receipt = inputs.receipt.as_ref();
        let wallet_snapshot = mint_payment_wallet_snapshot(
            &lookup.minter_address,
            inputs.base_balance_eth,
            receipt,
            &eth_transfers,
            &inputs.block_receipts,
        );
        let mut direct_withdrawal_edges = Vec::new();
        for transfer in &eth_transfers {
            if transfer.tx_hash != lookup.tx_hash
                || (transfer.value_eth <= 0.0 && transfer.value_usd.unwrap_or(0.0) <= 0.0)
            {
                continue;
            }
            let Some((channel, from_role, to_role, evidence_type, evidence_flags)) =
                classify_mint_value_flow_transfer(
                    transfer,
                    &lookup,
                    contract_address,
                    contract_metadata,
                )
            else {
                continue;
            };
            let edge = value_flow_edge_from_transfer(ValueFlowEdgeInput {
                chain: request.chain.as_str(),
                contract_address,
                lookup: &lookup,
                transfer,
                receipt,
                wallet_snapshot,
                channel,
                from_role,
                to_role,
                evidence_type,
                evidence_flags,
            });
            if edge.channel == "withdrawal" {
                direct_withdrawal_edges.push(edge.clone());
            }
            rows.entry(edge.edge_id.clone()).or_insert(edge);
        }
        for edge in trace_withdrawal_cashout_edges(CashoutTraceInput {
            request,
            deps,
            contract_address,
            contract_metadata,
            lookup: &lookup,
            seed_transfers: &eth_transfers,
            direct_withdrawal_edges: &direct_withdrawal_edges,
            mint_receipt: receipt,
            block_receipts: &inputs.block_receipts,
        })
        .await?
        {
            rows.entry(edge.edge_id.clone()).or_insert(edge);
        }
    }

    Ok(rows.into_values().collect())
}

pub(super) async fn compute_attacker_cost_edges_for_contract(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    contract_metadata: Option<&ContractMetadata>,
    sales: &[NftSaleRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    honest_addresses: &[HonestAddressPayload],
) -> Result<Vec<ValueFlowEdgePayload>, AppError> {
    let malicious = malicious_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
        .filter(|address| is_nonzero_address(address))
        .collect::<BTreeSet<_>>();
    let honest = honest_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
        .filter(|address| is_nonzero_address(address))
        .collect::<BTreeSet<_>>();

    let (deployment_edge, sale_edges) = tokio::join!(
        fetch_deployment_cost_edge(request, deps, contract_address, contract_metadata),
        compute_malicious_sale_cost_edges(
            request,
            deps,
            contract_address,
            sales,
            &malicious,
            &honest
        )
    );

    let mut rows = BTreeMap::<String, ValueFlowEdgePayload>::new();
    if let Some(edge) = deployment_edge? {
        rows.insert(edge.edge_id.clone(), edge);
    }
    for edge in sale_edges? {
        rows.insert(edge.edge_id.clone(), edge);
    }
    Ok(rows.into_values().collect())
}

async fn fetch_deployment_cost_edge(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    contract_metadata: Option<&ContractMetadata>,
) -> Result<Option<ValueFlowEdgePayload>, AppError> {
    let Some(metadata) = contract_metadata else {
        return Ok(None);
    };
    if metadata.deployed_block_number <= 0 {
        return Ok(None);
    }
    let receipts = deps
        .api
        .fetch_transaction_receipts_for_block_on_chain(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            metadata.deployed_block_number,
        )
        .await
        .unwrap_or_default();
    let receipt = deployment_receipt_for_contract(&receipts, contract_address, metadata);
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    let Some(gas_eth) = receipt_gas_eth(receipt) else {
        return Ok(None);
    };
    let chain = request.chain.parse::<Chain>().unwrap_or_default();
    let gas_usd = receipt.fee_usd.or_else(|| {
        matches!(chain, Chain::Ethereum | Chain::Base).then_some(gas_eth * FALLBACK_ETH_USD_RATE)
    });
    let gas_payer = if receipt.from_address.trim().is_empty() {
        metadata.contract_deployer.clone()
    } else {
        receipt.from_address.clone()
    };
    if !is_nonzero_address(&normalized_address(&gas_payer)) {
        return Ok(None);
    }
    Ok(Some(ValueFlowEdgePayload {
        edge_id: format!(
            "value:contract_deploy:{}:{}",
            contract_address, receipt.tx_hash
        ),
        contract_address: contract_address.to_string(),
        from_address: gas_payer.clone(),
        to_address: contract_address.to_string(),
        tx_hash: receipt.tx_hash.clone(),
        block_number: metadata.deployed_block_number,
        block_time: metadata.deployed_block_time,
        token_id: String::new(),
        value_eth: None,
        value_usd: None,
        value_with_gas_eth: Some(gas_eth),
        value_with_gas_usd: gas_usd,
        gas_payer_address: gas_payer,
        gas_eth: Some(gas_eth),
        gas_usd,
        from_before_eth_balance: None,
        from_before_usd_balance: None,
        payment_token_symbol: chain.native_symbol().into(),
        payment_token_address: ZERO_ADDRESS.into(),
        channel: "contract_deploy".into(),
        marketplace: String::new(),
        evidence_type: "deployment_receipt_gas".into(),
        from_role: "contract_deployer".into(),
        to_role: "mint_contract".into(),
        recipient_known: true,
        evidence_flags: vec!["deployment_gas".into(), "receipt_gas".into()],
    }))
}

fn deployment_receipt_for_contract<'a>(
    receipts: &'a BTreeMap<String, TransactionReceiptRecord>,
    contract_address: &str,
    metadata: &ContractMetadata,
) -> Option<&'a TransactionReceiptRecord> {
    receipts
        .values()
        .find(|receipt| {
            !receipt.contract_address.trim().is_empty()
                && identities_equal(&receipt.contract_address, contract_address)
        })
        .or_else(|| {
            let deployer = metadata.contract_deployer.trim();
            if deployer.is_empty() {
                return None;
            }
            receipts
                .values()
                .filter(|receipt| identities_equal(&receipt.from_address, deployer))
                .min_by_key(|receipt| receipt.transaction_index)
        })
}

async fn compute_malicious_sale_cost_edges(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    sales: &[NftSaleRecord],
    malicious: &BTreeSet<String>,
    honest: &BTreeSet<String>,
) -> Result<Vec<ValueFlowEdgePayload>, AppError> {
    let mut tx_hashes = BTreeSet::<String>::new();
    for sale in sales {
        if !identities_equal(&sale.contract_address, contract_address)
            || sale.tx_hash.trim().is_empty()
            || !sale_has_malicious_participant(sale, malicious)
        {
            continue;
        }
        tx_hashes.insert(sale.tx_hash.clone());
    }
    if tx_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let receipts_by_tx = fetch_mint_payment_receipts(request, deps, tx_hashes).await?;
    let mut rows = BTreeMap::<String, ValueFlowEdgePayload>::new();
    for sale in sales {
        if !identities_equal(&sale.contract_address, contract_address)
            || !sale_has_malicious_participant(sale, malicious)
        {
            continue;
        }
        let Some(receipt) = receipts_by_tx.get(&sale.tx_hash) else {
            continue;
        };
        let gas_payer = normalized_address(&receipt.from_address);
        if honest.contains(&gas_payer) || !malicious.contains(&gas_payer) {
            continue;
        }
        let Some(gas_eth) = receipt_gas_eth(receipt) else {
            continue;
        };
        let chain = request.chain.parse::<Chain>().unwrap_or_default();
        let gas_usd = receipt.fee_usd.or_else(|| {
            sale_eth_usd_rate(sale)
                .map(|rate| gas_eth * rate)
                .or_else(|| {
                    matches!(chain, Chain::Ethereum | Chain::Base)
                        .then_some(gas_eth * FALLBACK_ETH_USD_RATE)
                })
        });
        let channel = sale_attacker_cost_channel(sale, &gas_payer, malicious);
        let edge = ValueFlowEdgePayload {
            edge_id: format!(
                "value:{}:{}:{}:{}:{}",
                channel, sale.tx_hash, sale.log_index, sale.bundle_index, receipt.from_address
            ),
            contract_address: contract_address.to_string(),
            from_address: receipt.from_address.clone(),
            to_address: contract_address.to_string(),
            tx_hash: sale.tx_hash.clone(),
            block_number: sale.block_number,
            block_time: 0,
            token_id: sale.token_id.clone(),
            value_eth: sale.price_eth.filter(|value| *value > 0.0),
            value_usd: sale.price_usd.filter(|value| *value > 0.0),
            value_with_gas_eth: sale.price_eth.map(|value| value + gas_eth),
            value_with_gas_usd: sale.price_usd.zip(gas_usd).map(|(value, gas)| value + gas),
            gas_payer_address: receipt.from_address.clone(),
            gas_eth: Some(gas_eth),
            gas_usd,
            from_before_eth_balance: None,
            from_before_usd_balance: None,
            payment_token_symbol: if sale.payment_token_symbol.is_empty() {
                chain.native_symbol().into()
            } else {
                sale.payment_token_symbol.clone()
            },
            payment_token_address: if sale.payment_token_address.is_empty() {
                ZERO_ADDRESS.into()
            } else {
                sale.payment_token_address.clone()
            },
            channel,
            marketplace: sale.marketplace.clone(),
            evidence_type: "sale_receipt_gas".into(),
            from_role: "attacker".into(),
            to_role: "marketplace_sale".into(),
            recipient_known: true,
            evidence_flags: vec![
                "attacker_sale_gas".into(),
                "receipt_gas".into(),
                format!("seller:{}", sale.seller_address),
                format!("buyer:{}", sale.buyer_address),
            ],
        };
        rows.insert(edge.edge_id.clone(), edge);
    }
    Ok(rows.into_values().collect())
}

fn sale_attacker_cost_channel(
    sale: &NftSaleRecord,
    gas_payer: &str,
    malicious: &BTreeSet<String>,
) -> String {
    let buyer = normalized_address(&sale.buyer_address);
    let seller = normalized_address(&sale.seller_address);
    if seller == gas_payer && malicious.contains(&seller) && !malicious.contains(&buyer) {
        "exit_payment".into()
    } else {
        "lure_payment".into()
    }
}

fn sale_has_malicious_participant(sale: &NftSaleRecord, malicious: &BTreeSet<String>) -> bool {
    malicious.contains(&normalized_address(&sale.buyer_address))
        || malicious.contains(&normalized_address(&sale.seller_address))
}

fn sale_eth_usd_rate(sale: &NftSaleRecord) -> Option<f64> {
    let eth = sale.price_eth?;
    let usd = sale.price_usd?;
    (eth > 0.0 && usd > 0.0).then_some(usd / eth)
}

fn mint_payment_receiver_addresses(
    contract_address: &str,
    contract_metadata: Option<&ContractMetadata>,
) -> BTreeSet<String> {
    let mut addresses = BTreeSet::from([contract_address.to_string()]);
    if let Some(metadata) = contract_metadata {
        for address in [
            metadata.contract_deployer.as_str(),
            metadata.owner_address.as_str(),
            metadata.admin_address.as_str(),
            metadata.proxy_admin_address.as_str(),
        ] {
            if !address.is_empty() {
                addresses.insert(address.to_string());
            }
        }
    }
    addresses
}

async fn fetch_mint_payment_transfers_for_lookups(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    lookups: &[MintPaymentLookup],
    contract_metadata: Option<&ContractMetadata>,
) -> Result<BTreeMap<(i64, String, String), Vec<EthTransferRecord>>, AppError> {
    let mut requests = BTreeMap::<(i64, String, String), String>::new();
    let receiver_addresses = mint_payment_receiver_addresses(contract_address, contract_metadata);
    for lookup in lookups {
        let transaction_scope = if request.chain.eq_ignore_ascii_case("solana") {
            lookup.tx_hash.clone()
        } else {
            String::new()
        };
        for address in &receiver_addresses {
            requests
                .entry((
                    lookup.block_number,
                    transaction_scope.clone(),
                    normalize_chain_identity(address),
                ))
                .or_insert_with(|| address.clone());
        }
    }

    let mut fetched = stream::iter(requests.into_iter().map(
        |((block_number, tx_hash, address_key), address)| async move {
            let result = deps
                .api
                .fetch_transaction_value_transfers_to_address_on_chain(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    &tx_hash,
                    block_number,
                    &address,
                )
                .await;
            match result {
                Ok(rows) => Ok::<_, AppError>(((block_number, tx_hash, address_key), rows)),
                Err(err) => {
                    eprintln!(
                        "warning: mint value-flow transfer lookup failed for {address} at block {block_number}: {err}; continuing without this value-flow evidence"
                    );
                    Ok::<_, AppError>(((block_number, tx_hash, address_key), Vec::new()))
                }
            }
        },
    ))
    .buffer_unordered(request.api_max_concurrency.max(1));

    let mut rows_by_request = BTreeMap::new();
    while let Some(result) = fetched.next().await {
        let (request_key, rows) = result?;
        rows_by_request.insert(request_key, rows);
    }
    Ok(rows_by_request)
}

fn mint_payment_transfers_for_lookup(
    lookup: &MintPaymentLookup,
    contract_address: &str,
    contract_metadata: Option<&ContractMetadata>,
    transfer_rows_by_request: &BTreeMap<(i64, String, String), Vec<EthTransferRecord>>,
) -> Vec<EthTransferRecord> {
    let mut transfers = Vec::new();
    let transaction_scope = lookup.tx_hash.clone();
    for address in mint_payment_receiver_addresses(contract_address, contract_metadata) {
        let address_key = normalize_chain_identity(&address);
        if let Some(rows) = transfer_rows_by_request
            .get(&(
                lookup.block_number,
                transaction_scope.clone(),
                address_key.clone(),
            ))
            .or_else(|| {
                transfer_rows_by_request.get(&(lookup.block_number, String::new(), address_key))
            })
        {
            transfers.extend(rows.iter().cloned());
        }
    }
    deduplicate_eth_transfer_rows(transfers)
}

async fn fetch_mint_payment_receipts(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    tx_hashes: BTreeSet<String>,
) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
    let mut fetched = stream::iter(tx_hashes.into_iter().map(|tx_hash| async move {
        let receipt = deps
            .api
            .fetch_transaction_receipt_on_chain(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                &tx_hash,
            )
            .await
            .ok();
        (tx_hash, receipt)
    }))
    .buffer_unordered(request.api_max_concurrency.max(1));

    let mut rows = BTreeMap::new();
    while let Some((tx_hash, receipt)) = fetched.next().await {
        if let Some(receipt) = receipt {
            rows.insert(tx_hash, receipt);
        }
    }
    Ok(rows)
}

async fn fetch_mint_payment_balances(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    balance_requests: BTreeSet<(String, i64, String)>,
) -> Result<BTreeMap<(String, i64, String), f64>, AppError> {
    let mut fetched = stream::iter(balance_requests.into_iter().map(
        |(address, block_number, tx_hash)| async move {
            let balance = deps
                .api
                .fetch_pre_transaction_native_balance_on_chain(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    &tx_hash,
                    &address,
                    block_number,
                )
                .await
                .ok();
            ((address, block_number, tx_hash), balance)
        },
    ))
    .buffer_unordered(request.api_max_concurrency.max(1));

    let mut rows = BTreeMap::new();
    while let Some((request_key, balance)) = fetched.next().await {
        if let Some(balance) = balance {
            rows.insert(request_key, balance);
        }
    }
    Ok(rows)
}

async fn fetch_mint_payment_block_receipts(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    block_numbers: BTreeSet<i64>,
) -> Result<BTreeMap<i64, BTreeMap<String, TransactionReceiptRecord>>, AppError> {
    let mut fetched = stream::iter(block_numbers.into_iter().map(|block_number| async move {
        let receipts = deps
            .api
            .fetch_transaction_receipts_for_block_on_chain(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                block_number,
            )
            .await
            .unwrap_or_default();
        (block_number, receipts)
    }))
    .buffer_unordered(request.api_max_concurrency.max(1));

    let mut rows = BTreeMap::new();
    while let Some((block_number, receipts)) = fetched.next().await {
        rows.insert(block_number, receipts);
    }
    Ok(rows)
}

async fn fetch_contract_value_transfers_for_blocks(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    block_numbers: BTreeSet<i64>,
) -> Result<BTreeMap<i64, Vec<EthTransferRecord>>, AppError> {
    let mut fetched = stream::iter(block_numbers.into_iter().map(|block_number| async move {
        let rows = deps
            .api
            .fetch_mint_payment_eth_transfers_on_chain(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                block_number,
                contract_address,
            )
            .await
            .unwrap_or_default();
        (block_number, rows)
    }))
    .buffer_unordered(request.api_max_concurrency.max(1));

    let mut rows_by_block = BTreeMap::new();
    while let Some((block_number, rows)) = fetched.next().await {
        rows_by_block.insert(block_number, rows);
    }
    Ok(rows_by_block)
}

fn deduplicate_eth_transfer_rows(rows: Vec<EthTransferRecord>) -> Vec<EthTransferRecord> {
    let mut deduped = BTreeMap::new();
    for row in rows {
        deduped.insert(
            (
                row.tx_hash.clone(),
                row.from_address.clone(),
                row.to_address.clone(),
                format!("{:.18}", row.value_eth),
                row.value_usd
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_default(),
                row.payment_token_symbol.clone(),
                row.payment_token_address.clone(),
                row.category.clone(),
            ),
            row,
        );
    }
    deduped.into_values().collect()
}

fn value_flow_edge_from_transfer(input: ValueFlowEdgeInput<'_>) -> ValueFlowEdgePayload {
    let ValueFlowEdgeInput {
        chain,
        contract_address,
        lookup,
        transfer,
        receipt,
        wallet_snapshot,
        channel,
        from_role,
        to_role,
        evidence_type,
        evidence_flags,
    } = input;
    let mut from_role = from_role;
    let mut to_role = to_role;
    let mut evidence_flags = evidence_flags;
    if channel == "withdrawal" || channel == "cashout_hop" {
        if let Some(entity) = known_value_flow_entity(chain, &transfer.to_address) {
            to_role = entity.role.into();
            push_unique_flag(
                &mut evidence_flags,
                format!("cashout_destination:{}", entity.role),
            );
            push_unique_flag(
                &mut evidence_flags,
                format!("known_entity:{}", entity.label),
            );
        }
    } else if channel == "funding" {
        if let Some(entity) = known_value_flow_entity(chain, &transfer.from_address) {
            from_role = entity.role.into();
            push_unique_flag(
                &mut evidence_flags,
                format!("funding_source:{}", entity.role),
            );
            push_unique_flag(
                &mut evidence_flags,
                format!("known_entity:{}", entity.label),
            );
        }
    }

    let gas_eth = receipt.and_then(receipt_gas_eth);
    let gas_usd = receipt.and_then(|receipt| receipt_gas_usd(transfer, receipt));
    ValueFlowEdgePayload {
        value_with_gas_eth: value_with_gas_eth(transfer, receipt),
        value_with_gas_usd: value_with_gas_usd(transfer, receipt),
        gas_payer_address: receipt
            .map(|receipt| receipt.from_address.clone())
            .unwrap_or_default(),
        gas_eth,
        gas_usd,
        from_before_eth_balance: wallet_snapshot.before_eth_balance,
        from_before_usd_balance: wallet_snapshot.before_usd_balance,
        edge_id: format!(
            "value:{}:{}:{}:{}",
            channel, transfer.tx_hash, transfer.from_address, transfer.to_address
        ),
        contract_address: contract_address.to_string(),
        from_address: transfer.from_address.clone(),
        to_address: transfer.to_address.clone(),
        tx_hash: transfer.tx_hash.clone(),
        block_number: lookup.block_number,
        block_time: lookup.block_time,
        token_id: lookup.token_ids.join(","),
        value_eth: (transfer.value_eth > 0.0).then_some(transfer.value_eth),
        value_usd: transfer.value_usd.filter(|value| *value > 0.0),
        payment_token_symbol: if transfer.payment_token_symbol.is_empty() {
            "ETH".into()
        } else {
            transfer.payment_token_symbol.clone()
        },
        payment_token_address: if transfer.payment_token_address.is_empty() {
            ZERO_ADDRESS.into()
        } else {
            transfer.payment_token_address.clone()
        },
        channel,
        marketplace: String::new(),
        evidence_type,
        from_role,
        to_role,
        recipient_known: true,
        evidence_flags,
    }
}

fn push_unique_flag(flags: &mut Vec<String>, flag: String) {
    if !flags.iter().any(|existing| existing == &flag) {
        flags.push(flag);
    }
}

fn known_value_flow_entity(chain: &str, address: &str) -> Option<KnownValueFlowEntity> {
    KNOWN_VALUE_FLOW_ENTITIES.iter().copied().find(|entity| {
        chain.eq_ignore_ascii_case(entity.chain) && address.eq_ignore_ascii_case(entity.address)
    })
}

pub(super) fn classify_mint_value_flow_transfer(
    transfer: &EthTransferRecord,
    lookup: &MintPaymentLookup,
    contract_address: &str,
    contract_metadata: Option<&ContractMetadata>,
) -> Option<(String, String, String, String, Vec<String>)> {
    let contract_deployer = contract_metadata
        .map(|metadata| metadata.contract_deployer.as_str())
        .unwrap_or("");
    if is_matching_mint_payment_transfer(
        transfer,
        lookup,
        contract_address,
        contract_deployer,
        contract_metadata,
    ) {
        let to_role =
            contract_control_role(&transfer.to_address, contract_address, contract_metadata)
                .unwrap_or("operator_wallet")
                .to_string();
        return Some((
            "mint_payment".into(),
            "paid_minter".into(),
            to_role,
            format!("same_tx_eth_transfer:{}", transfer.category),
            vec![
                "paid_mint".into(),
                "same_tx_eth_transfer".into(),
                transfer.category.clone(),
            ],
        ));
    }
    if identities_equal(&transfer.to_address, &lookup.minter_address)
        && !identities_equal(&transfer.from_address, &lookup.minter_address)
        && !identities_equal(&transfer.from_address, ZERO_ADDRESS)
        && transfer.category != "erc20"
    {
        return Some((
            "funding".into(),
            "external_funder".into(),
            "paid_minter".into(),
            format!("same_tx_mint_funding:{}", transfer.category),
            vec![
                "same_tx_mint_funding".into(),
                "pre_mint_capital_source".into(),
                transfer.category.clone(),
            ],
        ));
    }
    if identities_equal(&transfer.from_address, contract_address)
        && !identities_equal(&transfer.to_address, &lookup.minter_address)
        && !identities_equal(&transfer.to_address, ZERO_ADDRESS)
    {
        let to_role =
            contract_control_role(&transfer.to_address, contract_address, contract_metadata)
                .unwrap_or("external_wallet")
                .to_string();
        return Some((
            "withdrawal".into(),
            "mint_contract".into(),
            to_role,
            format!("same_tx_contract_outflow:{}", transfer.category),
            vec![
                "same_tx_contract_withdrawal".into(),
                "post_mint_value_extraction".into(),
                transfer.category.clone(),
            ],
        ));
    }
    None
}

async fn trace_withdrawal_cashout_edges(
    input: CashoutTraceInput<'_>,
) -> Result<Vec<ValueFlowEdgePayload>, AppError> {
    let CashoutTraceInput {
        request,
        deps,
        contract_address,
        contract_metadata,
        lookup,
        seed_transfers,
        direct_withdrawal_edges,
        mint_receipt,
        block_receipts,
    } = input;
    if request.chain.eq_ignore_ascii_case("solana") {
        // Solana cashout expansion previously required full-slot getBlock scans.
        // Preserve direct transaction evidence and omit speculative later hops.
        return Ok(Vec::new());
    }
    let mut rows = BTreeMap::<String, ValueFlowEdgePayload>::new();
    let mut seen_edge_ids: BTreeSet<String> = direct_withdrawal_edges
        .iter()
        .map(|edge| edge.edge_id.clone())
        .collect();
    let mut queue = VecDeque::<CashoutTraceNode>::new();

    for edge in direct_withdrawal_edges {
        enqueue_cashout_trace_node(
            &mut queue,
            request.chain.as_str(),
            edge,
            0,
            BTreeSet::from([
                normalized_address(contract_address),
                normalized_address(&edge.to_address),
            ]),
            block_receipts,
        );
    }

    for transfer in seed_transfers {
        if !is_contract_withdrawal_transfer(transfer, lookup, contract_address)
            || !transfer_is_at_or_after_mint(transfer, lookup, mint_receipt, block_receipts)
        {
            continue;
        }
        let to_role =
            contract_control_role(&transfer.to_address, contract_address, contract_metadata)
                .unwrap_or("external_wallet")
                .to_string();
        let timing_flag = if transfer.tx_hash == lookup.tx_hash {
            "same_tx_contract_withdrawal"
        } else {
            "same_block_contract_withdrawal"
        };
        let receipt = receipt_for_transfer(transfer, block_receipts).or(mint_receipt);
        let edge = value_flow_edge_from_transfer(ValueFlowEdgeInput {
            chain: request.chain.as_str(),
            contract_address,
            lookup,
            transfer,
            receipt,
            wallet_snapshot: MintPaymentWalletSnapshot::default(),
            channel: "withdrawal".into(),
            from_role: "mint_contract".into(),
            to_role,
            evidence_type: format!("same_block_contract_outflow:{}", transfer.category),
            evidence_flags: vec![
                timing_flag.into(),
                "post_mint_value_extraction".into(),
                transfer.category.clone(),
            ],
        });
        enqueue_cashout_trace_node(
            &mut queue,
            request.chain.as_str(),
            &edge,
            0,
            BTreeSet::from([
                normalized_address(contract_address),
                normalized_address(&edge.to_address),
            ]),
            block_receipts,
        );
        if seen_edge_ids.insert(edge.edge_id.clone()) {
            rows.insert(edge.edge_id.clone(), edge);
        }
    }

    let mut fetched_addresses = BTreeSet::<String>::new();
    let cashout_fetch_concurrency = request.api_max_concurrency.max(1);
    while !queue.is_empty() {
        let mut frontier = Vec::new();
        while let Some(node) = queue.pop_front() {
            if node.depth >= MAX_WITHDRAWAL_TRACE_HOPS
                || known_value_flow_entity(request.chain.as_str(), &node.address).is_some()
            {
                continue;
            }
            if fetched_addresses.len() >= MAX_WITHDRAWAL_TRACE_FRONTIER {
                queue.clear();
                break;
            }
            if !fetched_addresses.insert(node.address.clone()) {
                continue;
            }
            frontier.push(node);
            if frontier.len() >= cashout_fetch_concurrency {
                break;
            }
        }
        if frontier.is_empty() {
            continue;
        }

        let mut fetched = stream::iter(frontier.into_iter().enumerate().map(
            |(index, node)| async move {
                let transfers = fetch_cashout_trace_transfers(
                    request,
                    deps,
                    lookup.block_number,
                    &node.address,
                )
                .await;
                (index, node, transfers)
            },
        ))
        .buffer_unordered(cashout_fetch_concurrency);
        let mut fetched_frontier = Vec::new();
        while let Some(result) = fetched.next().await {
            fetched_frontier.push(result);
        }
        fetched_frontier.sort_by_key(|(index, _, _)| *index);
        for (_, node, transfers) in fetched_frontier {
            for transfer in transfers? {
                if !is_cashout_hop_transfer(&transfer, &node, lookup.block_number, block_receipts) {
                    continue;
                }
                let to_address = normalized_address(&transfer.to_address);
                if node.path_addresses.contains(&to_address) {
                    continue;
                }
                let next_depth = node.depth + 1;
                let value_ratio = cashout_value_ratio(&transfer, &node);
                let receipt = receipt_for_transfer(&transfer, block_receipts);
                let mut edge = value_flow_edge_from_transfer(ValueFlowEdgeInput {
                    chain: request.chain.as_str(),
                    contract_address,
                    lookup,
                    transfer: &transfer,
                    receipt,
                    wallet_snapshot: MintPaymentWalletSnapshot::default(),
                    channel: "cashout_hop".into(),
                    from_role: "cashout_intermediate".into(),
                    to_role: known_value_flow_entity(request.chain.as_str(), &transfer.to_address)
                        .map(|entity| entity.role.to_string())
                        .unwrap_or_else(|| "cashout_intermediate".into()),
                    evidence_type: format!("multi_hop_contract_cashout:{}", transfer.category),
                    evidence_flags: vec![
                        "multi_hop_cashout".into(),
                        "same_block_cashout_trace".into(),
                        "value_constrained_cashout".into(),
                        format!("cashout_hop:{next_depth}"),
                        transfer.category.clone(),
                    ],
                });
                if let Some(ratio) = value_ratio {
                    push_unique_flag(
                        &mut edge.evidence_flags,
                        format!("cashout_value_ratio:{ratio:.4}"),
                    );
                }
                edge.edge_id = format!(
                    "value:cashout_hop:{}:{}:{}:{}",
                    transfer.tx_hash, transfer.from_address, transfer.to_address, next_depth
                );
                if !seen_edge_ids.insert(edge.edge_id.clone()) {
                    continue;
                }
                let mut next_path = node.path_addresses.clone();
                next_path.insert(to_address);
                enqueue_cashout_trace_node(
                    &mut queue,
                    request.chain.as_str(),
                    &edge,
                    next_depth,
                    next_path,
                    block_receipts,
                );
                rows.insert(edge.edge_id.clone(), edge);
            }
        }
    }

    Ok(rows.into_values().collect())
}

async fn fetch_cashout_trace_transfers(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    block_number: i64,
    address: &str,
) -> Result<Vec<EthTransferRecord>, AppError> {
    match deps
        .api
        .fetch_mint_payment_eth_transfers_on_chain(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            block_number,
            address,
        )
        .await
    {
        Ok(rows) => Ok(rows),
        Err(err) => {
            eprintln!(
                "warning: cashout trace transfer lookup failed for {address} at block {block_number}: {err}; continuing without this cashout hop"
            );
            Ok(Vec::new())
        }
    }
}

fn enqueue_cashout_trace_node(
    queue: &mut VecDeque<CashoutTraceNode>,
    chain: &str,
    edge: &ValueFlowEdgePayload,
    depth: usize,
    path_addresses: BTreeSet<String>,
    block_receipts: &BTreeMap<String, TransactionReceiptRecord>,
) {
    let address = normalized_address(&edge.to_address);
    if address.is_empty()
        || address == ZERO_ADDRESS
        || depth >= MAX_WITHDRAWAL_TRACE_HOPS
        || known_value_flow_entity(chain, &address).is_some()
    {
        return;
    }
    queue.push_back(CashoutTraceNode {
        address,
        depth,
        previous_tx_hash: edge.tx_hash.clone(),
        previous_tx_index: receipt_index_for_tx(&edge.tx_hash, block_receipts),
        value_eth: edge.value_eth,
        value_usd: edge.value_usd,
        payment_token_symbol: edge.payment_token_symbol.clone(),
        payment_token_address: edge.payment_token_address.clone(),
        path_addresses,
    });
}

fn is_contract_withdrawal_transfer(
    transfer: &EthTransferRecord,
    lookup: &MintPaymentLookup,
    contract_address: &str,
) -> bool {
    transfer.block_number == lookup.block_number
        && transfer_value_positive(transfer)
        && identities_equal(&transfer.from_address, contract_address)
        && !identities_equal(&transfer.to_address, &lookup.minter_address)
        && !identities_equal(&transfer.to_address, ZERO_ADDRESS)
}

fn transfer_is_at_or_after_mint(
    transfer: &EthTransferRecord,
    lookup: &MintPaymentLookup,
    mint_receipt: Option<&TransactionReceiptRecord>,
    block_receipts: &BTreeMap<String, TransactionReceiptRecord>,
) -> bool {
    if transfer.tx_hash == lookup.tx_hash {
        return true;
    }
    let Some(mint_index) = mint_receipt.map(|receipt| receipt.transaction_index) else {
        return false;
    };
    receipt_for_transfer(transfer, block_receipts)
        .map(|receipt| receipt.transaction_index >= mint_index)
        .unwrap_or(false)
}

fn is_cashout_hop_transfer(
    transfer: &EthTransferRecord,
    node: &CashoutTraceNode,
    block_number: i64,
    block_receipts: &BTreeMap<String, TransactionReceiptRecord>,
) -> bool {
    if transfer.block_number != block_number
        || !transfer_value_positive(transfer)
        || !identities_equal(&transfer.from_address, &node.address)
        || identities_equal(&transfer.to_address, &node.address)
        || identities_equal(&transfer.to_address, ZERO_ADDRESS)
    {
        return false;
    }
    if transfer.tx_hash == node.previous_tx_hash {
        return cashout_value_is_trace_compatible(transfer, node);
    }
    let Some(previous_index) = node.previous_tx_index else {
        return false;
    };
    receipt_for_transfer(transfer, block_receipts)
        .map(|receipt| receipt.transaction_index > previous_index)
        .unwrap_or(false)
        && cashout_value_is_trace_compatible(transfer, node)
}

fn cashout_value_is_trace_compatible(
    transfer: &EthTransferRecord,
    node: &CashoutTraceNode,
) -> bool {
    cashout_token_matches(transfer, node)
        && cashout_value_ratio(transfer, node)
            .map(|ratio| (MIN_CASHOUT_VALUE_RATIO..=MAX_CASHOUT_VALUE_RATIO).contains(&ratio))
            .unwrap_or(false)
}

fn cashout_value_ratio(transfer: &EthTransferRecord, node: &CashoutTraceNode) -> Option<f64> {
    if transfer.value_eth > 0.0 {
        if let Some(previous_value) = node.value_eth.filter(|value| *value > 0.0) {
            return Some(transfer.value_eth / previous_value);
        }
    }
    match (
        transfer.value_usd.filter(|value| *value > 0.0),
        node.value_usd.filter(|value| *value > 0.0),
    ) {
        (Some(value), Some(previous_value)) => Some(value / previous_value),
        _ => None,
    }
}

fn cashout_token_matches(transfer: &EthTransferRecord, node: &CashoutTraceNode) -> bool {
    normalized_payment_token(
        &transfer.payment_token_symbol,
        &transfer.payment_token_address,
    ) == normalized_payment_token(&node.payment_token_symbol, &node.payment_token_address)
}

fn normalized_payment_token(symbol: &str, address: &str) -> (String, String) {
    let symbol = if symbol.trim().is_empty() {
        "ETH".to_string()
    } else {
        symbol.trim().to_ascii_uppercase()
    };
    let address = if address.trim().is_empty() {
        ZERO_ADDRESS.to_string()
    } else {
        normalize_chain_identity(address)
    };
    (symbol, address)
}

fn receipt_for_transfer<'a>(
    transfer: &EthTransferRecord,
    block_receipts: &'a BTreeMap<String, TransactionReceiptRecord>,
) -> Option<&'a TransactionReceiptRecord> {
    block_receipts.get(&normalize_chain_identity(&transfer.tx_hash))
}

fn receipt_index_for_tx(
    tx_hash: &str,
    block_receipts: &BTreeMap<String, TransactionReceiptRecord>,
) -> Option<i64> {
    block_receipts
        .get(&normalize_chain_identity(tx_hash))
        .map(|receipt| receipt.transaction_index)
}

fn normalized_address(address: &str) -> String {
    normalize_chain_identity(address)
}

fn identities_equal(left: &str, right: &str) -> bool {
    normalized_address(left) == normalized_address(right)
}

fn is_nonzero_address(address: &str) -> bool {
    !address.trim().is_empty() && !address.eq_ignore_ascii_case(ZERO_ADDRESS)
}

pub(super) fn mint_payment_wallet_snapshot(
    minter_address: &str,
    base_balance_eth: Option<f64>,
    receipt: Option<&TransactionReceiptRecord>,
    transfers: &[EthTransferRecord],
    receipts_by_hash: &BTreeMap<String, TransactionReceiptRecord>,
) -> MintPaymentWalletSnapshot {
    let Some(base_balance_eth) = base_balance_eth else {
        return MintPaymentWalletSnapshot::default();
    };
    let eth_usd_rate = infer_eth_usd_rate_from_transfers(transfers);
    let mut same_block_eth_delta = 0.0;
    let mut same_block_usd_delta = 0.0;
    if let Some(receipt) = receipt {
        for transfer in transfers {
            let Some(transfer_receipt) = receipts_by_hash.get(&transfer.tx_hash) else {
                continue;
            };
            if transfer_receipt.transaction_index >= receipt.transaction_index {
                continue;
            }
            let sign = if identities_equal(&transfer.to_address, minter_address) {
                1.0
            } else if identities_equal(&transfer.from_address, minter_address) {
                -1.0
            } else {
                continue;
            };
            same_block_eth_delta += sign * transfer.value_eth;
            if let Some(value_usd) = transfer_value_usd(transfer, eth_usd_rate) {
                same_block_usd_delta += sign * value_usd;
            }
        }
    }
    let before_eth_balance = (base_balance_eth + same_block_eth_delta).max(0.0);
    let before_usd_balance =
        eth_usd_rate.map(|rate| (base_balance_eth * rate + same_block_usd_delta).max(0.0));
    MintPaymentWalletSnapshot {
        before_eth_balance: Some(before_eth_balance),
        before_usd_balance,
    }
}

pub(super) fn value_with_gas_eth(
    transfer: &EthTransferRecord,
    receipt: Option<&TransactionReceiptRecord>,
) -> Option<f64> {
    let value_eth = (transfer.value_eth > 0.0).then_some(transfer.value_eth)?;
    let gas_eth = receipt
        .filter(|receipt| receipt_gas_paid_by_transfer_sender(transfer, receipt))
        .and_then(receipt_gas_eth)
        .unwrap_or_default();
    Some(value_eth + gas_eth)
}

pub(super) fn value_with_gas_usd(
    transfer: &EthTransferRecord,
    receipt: Option<&TransactionReceiptRecord>,
) -> Option<f64> {
    let value_usd = transfer.value_usd?;
    let gas_usd = receipt
        .filter(|receipt| receipt_gas_paid_by_transfer_sender(transfer, receipt))
        .and_then(|receipt| receipt_gas_usd(transfer, receipt))
        .unwrap_or_default();
    Some(value_usd + gas_usd)
}

pub(super) fn gas_eth_from_receipt(receipt: &TransactionReceiptRecord) -> f64 {
    if let Some(fee_native) = receipt.fee_native {
        return fee_native.max(0.0);
    }
    (receipt.gas_used as f64 * receipt.effective_gas_price_wei as f64)
        / 1_000_000_000_000_000_000_f64
}

fn receipt_gas_eth(receipt: &TransactionReceiptRecord) -> Option<f64> {
    let gas_eth = gas_eth_from_receipt(receipt);
    (gas_eth > 0.0).then_some(gas_eth)
}

fn receipt_gas_usd(
    transfer: &EthTransferRecord,
    receipt: &TransactionReceiptRecord,
) -> Option<f64> {
    if let Some(fee_usd) = receipt.fee_usd {
        return Some(fee_usd.max(0.0));
    }
    infer_eth_usd_rate_from_transfer(transfer).and_then(|rate| {
        let gas_usd = gas_eth_from_receipt(receipt) * rate;
        (gas_usd > 0.0).then_some(gas_usd)
    })
}

fn receipt_gas_paid_by_transfer_sender(
    transfer: &EthTransferRecord,
    receipt: &TransactionReceiptRecord,
) -> bool {
    identities_equal(&receipt.from_address, &transfer.from_address)
}

pub(super) fn infer_eth_usd_rate_from_transfers(transfers: &[EthTransferRecord]) -> Option<f64> {
    transfers.iter().find_map(infer_eth_usd_rate_from_transfer)
}

pub(super) fn infer_eth_usd_rate_from_transfer(transfer: &EthTransferRecord) -> Option<f64> {
    transfer
        .value_usd
        .filter(|value| *value > 0.0 && transfer.value_eth > 0.0)
        .map(|value| value / transfer.value_eth)
}

pub(super) fn transfer_value_usd(
    transfer: &EthTransferRecord,
    eth_usd_rate: Option<f64>,
) -> Option<f64> {
    transfer
        .value_usd
        .or_else(|| eth_usd_rate.map(|rate| transfer.value_eth * rate))
}

pub(super) fn contract_control_role<'a>(
    address: &str,
    contract_address: &str,
    metadata: Option<&'a ContractMetadata>,
) -> Option<&'a str> {
    if identities_equal(address, contract_address) {
        return Some("mint_contract");
    }
    let metadata = metadata?;
    if !metadata.contract_deployer.is_empty()
        && identities_equal(address, &metadata.contract_deployer)
    {
        return Some("contract_deployer");
    }
    if !metadata.owner_address.is_empty() && identities_equal(address, &metadata.owner_address) {
        return Some("contract_owner");
    }
    if !metadata.admin_address.is_empty() && identities_equal(address, &metadata.admin_address) {
        return Some("contract_admin");
    }
    if !metadata.proxy_admin_address.is_empty()
        && identities_equal(address, &metadata.proxy_admin_address)
    {
        return Some("proxy_admin");
    }
    None
}

pub(super) fn is_matching_mint_payment_transfer(
    transfer: &EthTransferRecord,
    lookup: &MintPaymentLookup,
    contract_address: &str,
    contract_deployer: &str,
    contract_metadata: Option<&ContractMetadata>,
) -> bool {
    transfer.tx_hash == lookup.tx_hash
        && transfer_value_positive(transfer)
        && identities_equal(&transfer.from_address, &lookup.minter_address)
        && (identities_equal(&transfer.to_address, contract_address)
            || (!contract_deployer.is_empty()
                && identities_equal(&transfer.to_address, contract_deployer))
            || contract_metadata
                .map(|metadata| {
                    (!metadata.owner_address.is_empty()
                        && identities_equal(&transfer.to_address, &metadata.owner_address))
                        || (!metadata.admin_address.is_empty()
                            && identities_equal(&transfer.to_address, &metadata.admin_address))
                        || (!metadata.proxy_admin_address.is_empty()
                            && identities_equal(
                                &transfer.to_address,
                                &metadata.proxy_admin_address,
                            ))
                })
                .unwrap_or(false))
}

pub(super) fn transfer_value_positive(transfer: &EthTransferRecord) -> bool {
    transfer.value_eth > 0.0 || transfer.value_usd.unwrap_or(0.0) > 0.0
}

pub(super) fn build_mint_payment_lookups(
    contract_address: &str,
    infringing_tokens: &[InfringingTokenRecord],
    transfers: &[TransferRecord],
) -> Vec<MintPaymentLookup> {
    let mut block_time_by_tx = BTreeMap::<String, i64>::new();
    for transfer in transfers {
        if transfer.contract_address == contract_address
            && !transfer.tx_hash.is_empty()
            && transfer.block_time > 0
        {
            block_time_by_tx
                .entry(transfer.tx_hash.clone())
                .or_insert(transfer.block_time);
        }
    }

    let mut grouped = BTreeMap::<(String, i64, String), BTreeSet<String>>::new();
    for token in infringing_tokens {
        if token.mint_tx_hash.is_empty()
            || token.mint_block <= 0
            || token.minter_address.is_empty()
            || token.minter_address == ZERO_ADDRESS
        {
            continue;
        }
        grouped
            .entry((
                token.mint_tx_hash.clone(),
                token.mint_block,
                token.minter_address.clone(),
            ))
            .or_default()
            .insert(token.token_id.clone());
    }

    grouped
        .into_iter()
        .map(
            |((tx_hash, block_number, minter_address), token_ids)| MintPaymentLookup {
                block_time: block_time_by_tx.get(&tx_hash).copied().unwrap_or_default(),
                tx_hash,
                block_number,
                minter_address,
                token_ids: token_ids.into_iter().collect(),
            },
        )
        .collect()
}
