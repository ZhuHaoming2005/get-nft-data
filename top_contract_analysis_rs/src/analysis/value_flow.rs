use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::sync::Semaphore;

use crate::error::AppError;
use crate::models::{
    ContractMetadata, EthTransferRecord, InfringingTokenRecord, TransactionReceiptRecord,
    TransferRecord, ValueFlowEdgePayload, ZERO_ADDRESS,
};

use super::{acquire_optional_limit, AnalysisDeps, AnalyzeRequest, RuntimeLimits};

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
    runtime_limits: &RuntimeLimits,
) -> Result<Vec<ValueFlowEdgePayload>, AppError> {
    let lookups = build_mint_payment_lookups(contract_address, infringing_tokens, transfers);
    if lookups.is_empty() {
        return Ok(vec![]);
    }
    let payment_limit = runtime_limits.sale_metric_limit.clone().or_else(|| {
        Some(Arc::new(Semaphore::new(
            request.sale_metric_max_concurrency.max(1),
        )))
    });
    let contract_deployer = contract_metadata
        .map(|metadata| metadata.contract_deployer.clone())
        .unwrap_or_default();

    let mut fetched = stream::iter(lookups.into_iter().map(|lookup| {
        let payment_limit = payment_limit.clone();
        let contract_deployer = contract_deployer.clone();
        async move {
            let _permit = acquire_optional_limit(&payment_limit).await?;
            let mut lookup_addresses = BTreeSet::from([
                lookup.minter_address.clone(),
                contract_address.to_string(),
            ]);
            if !contract_deployer.is_empty() {
                lookup_addresses.insert(contract_deployer.clone());
            }
            let mut transfers = Vec::new();
            for address in lookup_addresses {
                match deps
                    .api
                    .fetch_mint_payment_eth_transfers_on_chain(
                        &request.chain,
                        &request.alchemy_api_key,
                        request.alchemy_network.as_deref(),
                        lookup.block_number,
                        &address,
                    )
                    .await
                {
                    Ok(rows) => transfers.extend(rows),
                    Err(err) => {
                        eprintln!(
                            "warning: mint value-flow transfer lookup failed for {address} in {}: {err}; continuing without this value-flow evidence",
                            lookup.tx_hash
                        );
                    }
                }
            }
            let has_mint_payment_transfer = transfers.iter().any(|transfer| {
                is_matching_mint_payment_transfer(
                    transfer,
                    &lookup,
                    contract_address,
                    &contract_deployer,
                    contract_metadata,
                )
            });
            if !has_mint_payment_transfer {
                return Ok::<_, AppError>(MintPaymentInputs {
                    lookup,
                    transfers,
                    receipt: None,
                    base_balance_eth: None,
                    block_receipts: BTreeMap::new(),
                });
            }
            let (receipt, base_balance_eth, block_receipts) = tokio::join!(
                deps.api.fetch_transaction_receipt_on_chain(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    &lookup.tx_hash,
                ),
                deps.api.fetch_eth_balance_on_chain(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    &lookup.minter_address,
                    lookup.block_number - 1,
                ),
                deps.api.fetch_transaction_receipts_for_block_on_chain(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    lookup.block_number,
                )
            );
            Ok::<_, AppError>(MintPaymentInputs {
                lookup,
                transfers,
                receipt: receipt.ok(),
                base_balance_eth: base_balance_eth.ok(),
                block_receipts: block_receipts.unwrap_or_default(),
            })
        }
    }))
    .buffer_unordered(request.sale_metric_max_concurrency.max(1));

    let mut rows = BTreeMap::<String, ValueFlowEdgePayload>::new();
    while let Some(result) = fetched.next().await {
        let inputs = result?;
        let lookup = inputs.lookup;
        let eth_transfers = inputs.transfers;
        let wallet_snapshot = mint_payment_wallet_snapshot(
            &lookup.minter_address,
            inputs.base_balance_eth,
            inputs.receipt.as_ref(),
            &eth_transfers,
            &inputs.block_receipts,
        );
        for transfer in eth_transfers {
            if transfer.tx_hash != lookup.tx_hash
                || (transfer.value_eth <= 0.0 && transfer.value_usd.unwrap_or(0.0) <= 0.0)
            {
                continue;
            }
            let Some((channel, from_role, to_role, evidence_type, evidence_flags)) =
                classify_mint_value_flow_transfer(
                    &transfer,
                    &lookup,
                    contract_address,
                    contract_metadata,
                )
            else {
                continue;
            };
            let edge_id = format!(
                "value:{}:{}:{}:{}",
                channel, transfer.tx_hash, transfer.from_address, transfer.to_address
            );
            rows.entry(edge_id.clone()).or_insert(ValueFlowEdgePayload {
                value_with_gas_eth: value_with_gas_eth(&transfer, inputs.receipt.as_ref()),
                value_with_gas_usd: value_with_gas_usd(&transfer, inputs.receipt.as_ref()),
                from_before_eth_balance: wallet_snapshot.before_eth_balance,
                from_before_usd_balance: wallet_snapshot.before_usd_balance,
                edge_id,
                contract_address: contract_address.to_string(),
                from_address: transfer.from_address,
                to_address: transfer.to_address,
                tx_hash: lookup.tx_hash.clone(),
                block_number: lookup.block_number,
                block_time: lookup.block_time,
                token_id: lookup.token_ids.join(","),
                value_eth: (transfer.value_eth > 0.0).then_some(transfer.value_eth),
                value_usd: transfer.value_usd.filter(|value| *value > 0.0),
                payment_token_symbol: if transfer.payment_token_symbol.is_empty() {
                    "ETH".into()
                } else {
                    transfer.payment_token_symbol
                },
                payment_token_address: if transfer.payment_token_address.is_empty() {
                    ZERO_ADDRESS.into()
                } else {
                    transfer.payment_token_address
                },
                channel,
                marketplace: String::new(),
                evidence_type,
                from_role,
                to_role,
                recipient_known: true,
                evidence_flags,
            });
        }
    }

    Ok(rows.into_values().collect())
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
    if transfer
        .to_address
        .eq_ignore_ascii_case(&lookup.minter_address)
        && !transfer
            .from_address
            .eq_ignore_ascii_case(&lookup.minter_address)
        && !transfer.from_address.eq_ignore_ascii_case(ZERO_ADDRESS)
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
    if transfer.from_address.eq_ignore_ascii_case(contract_address)
        && !transfer
            .to_address
            .eq_ignore_ascii_case(&lookup.minter_address)
        && !transfer.to_address.eq_ignore_ascii_case(ZERO_ADDRESS)
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
            let sign = if transfer.to_address.eq_ignore_ascii_case(minter_address) {
                1.0
            } else if transfer.from_address.eq_ignore_ascii_case(minter_address) {
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
        .filter(|receipt| {
            receipt
                .from_address
                .eq_ignore_ascii_case(&transfer.from_address)
        })
        .map(gas_eth_from_receipt)
        .unwrap_or_default();
    Some(value_eth + gas_eth)
}

pub(super) fn value_with_gas_usd(
    transfer: &EthTransferRecord,
    receipt: Option<&TransactionReceiptRecord>,
) -> Option<f64> {
    let value_usd = transfer.value_usd?;
    let gas_usd = receipt
        .filter(|receipt| {
            receipt
                .from_address
                .eq_ignore_ascii_case(&transfer.from_address)
        })
        .and_then(|receipt| {
            infer_eth_usd_rate_from_transfer(transfer)
                .map(|rate| gas_eth_from_receipt(receipt) * rate)
        })
        .unwrap_or_default();
    Some(value_usd + gas_usd)
}

pub(super) fn gas_eth_from_receipt(receipt: &TransactionReceiptRecord) -> f64 {
    (receipt.gas_used as f64 * receipt.effective_gas_price_wei as f64)
        / 1_000_000_000_000_000_000_f64
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
    if address.eq_ignore_ascii_case(contract_address) {
        return Some("mint_contract");
    }
    let metadata = metadata?;
    if !metadata.contract_deployer.is_empty()
        && address.eq_ignore_ascii_case(&metadata.contract_deployer)
    {
        return Some("contract_deployer");
    }
    if !metadata.owner_address.is_empty() && address.eq_ignore_ascii_case(&metadata.owner_address) {
        return Some("contract_owner");
    }
    if !metadata.admin_address.is_empty() && address.eq_ignore_ascii_case(&metadata.admin_address) {
        return Some("contract_admin");
    }
    if !metadata.proxy_admin_address.is_empty()
        && address.eq_ignore_ascii_case(&metadata.proxy_admin_address)
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
        && transfer
            .from_address
            .eq_ignore_ascii_case(&lookup.minter_address)
        && (transfer.to_address.eq_ignore_ascii_case(contract_address)
            || (!contract_deployer.is_empty()
                && transfer.to_address.eq_ignore_ascii_case(contract_deployer))
            || contract_metadata
                .map(|metadata| {
                    (!metadata.owner_address.is_empty()
                        && transfer
                            .to_address
                            .eq_ignore_ascii_case(&metadata.owner_address))
                        || (!metadata.admin_address.is_empty()
                            && transfer
                                .to_address
                                .eq_ignore_ascii_case(&metadata.admin_address))
                        || (!metadata.proxy_admin_address.is_empty()
                            && transfer
                                .to_address
                                .eq_ignore_ascii_case(&metadata.proxy_admin_address))
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
