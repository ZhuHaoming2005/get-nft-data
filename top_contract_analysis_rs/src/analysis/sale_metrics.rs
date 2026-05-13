use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::Semaphore;

use crate::error::AppError;
use crate::models::{EthTransferRecord, NftSaleRecord, TransactionReceiptRecord};

use super::{acquire_optional_limit, address_records, AnalysisDeps, AnalyzeRequest, RuntimeLimits};

pub(super) async fn compute_sale_metrics_for_contract(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    sales: &[NftSaleRecord],
    runtime_limits: &RuntimeLimits,
) -> Result<BTreeMap<String, address_records::SaleMetricRecord>, AppError> {
    let sale_metric_limit = runtime_limits.sale_metric_limit.clone().or_else(|| {
        Some(Arc::new(Semaphore::new(
            request.sale_metric_max_concurrency.max(1),
        )))
    });
    let mut latest_sale_by_buyer = BTreeMap::<String, &NftSaleRecord>::new();
    for sale in sales {
        if sale.buyer_address.is_empty() {
            continue;
        }
        latest_sale_by_buyer
            .entry(sale.buyer_address.clone())
            .and_modify(|existing| {
                if sale_sort_key_for_metrics(sale) >= sale_sort_key_for_metrics(existing) {
                    *existing = sale;
                }
            })
            .or_insert(sale);
    }

    let mut unique_sales_by_purchase = BTreeMap::new();
    for sale in latest_sale_by_buyer.into_values() {
        unique_sales_by_purchase
            .entry(address_records::sale_metric_key(
                &sale.tx_hash,
                &sale.buyer_address,
            ))
            .or_insert(sale);
    }

    let mut prefetches = FuturesUnordered::new();
    for sale in unique_sales_by_purchase.into_values() {
        let sale_metric_limit = sale_metric_limit.clone();
        prefetches.push(async move {
            let _permit = acquire_optional_limit(&sale_metric_limit).await?;
            Ok::<_, AppError>(prefetch_sale_metric_inputs(request, deps, sale).await)
        });
    }

    let mut prefetched_by_purchase = BTreeMap::new();
    let mut queued_blocks = BTreeSet::new();
    let mut block_receipts = FuturesUnordered::new();
    let mut receipts_by_block = BTreeMap::new();
    loop {
        tokio::select! {
            Some(row) = prefetches.next(), if !prefetches.is_empty() => {
                let row = row?;
                if !row.same_block_transfers.is_empty() && queued_blocks.insert(row.block_number) {
                    let sale_metric_limit = sale_metric_limit.clone();
                    let block_number = row.block_number;
                    block_receipts.push(async move {
                        let _permit = match acquire_optional_limit(&sale_metric_limit).await {
                            Ok(permit) => permit,
                            Err(_) => return (block_number, BTreeMap::new()),
                        };
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
                    });
                }
                prefetched_by_purchase.insert(row.metric_key.clone(), row);
            }
            Some((block_number, receipts)) = block_receipts.next(), if !block_receipts.is_empty() => {
                receipts_by_block.insert(block_number, receipts);
            }
            else => break,
        }
    }

    let mut rows = BTreeMap::new();
    for sale in sales {
        let metric_key = address_records::sale_metric_key(&sale.tx_hash, &sale.buyer_address);
        if rows.contains_key(&metric_key) {
            continue;
        }
        let unavailable;
        let prefetched = if let Some(prefetched) = prefetched_by_purchase.get(&metric_key) {
            prefetched
        } else {
            unavailable = SaleMetricPrefetch::unavailable(sale);
            &unavailable
        };
        rows.insert(
            metric_key,
            compute_sale_metrics_for_sale(sale, prefetched, &receipts_by_block),
        );
    }
    Ok(rows)
}

pub(super) fn sale_sort_key_for_metrics(sale: &NftSaleRecord) -> (i64, i64, i64, &str) {
    (
        sale.block_number,
        sale.log_index,
        sale.bundle_index,
        sale.tx_hash.as_str(),
    )
}

pub(super) struct SaleMetricPrefetch {
    metric_key: String,
    block_number: i64,
    purchase_receipt: Option<TransactionReceiptRecord>,
    base_balance_eth: Option<f64>,
    same_block_transfers: Vec<EthTransferRecord>,
}

impl SaleMetricPrefetch {
    fn unavailable(sale: &NftSaleRecord) -> Self {
        Self {
            metric_key: address_records::sale_metric_key(&sale.tx_hash, &sale.buyer_address),
            block_number: sale.block_number,
            purchase_receipt: None,
            base_balance_eth: None,
            same_block_transfers: vec![],
        }
    }
}

pub(super) async fn prefetch_sale_metric_inputs(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    sale: &NftSaleRecord,
) -> SaleMetricPrefetch {
    if !sale.is_native_eth || sale.price_eth.is_none() {
        return SaleMetricPrefetch::unavailable(sale);
    }

    let (purchase_receipt, base_balance_eth, same_block_transfers) = tokio::join!(
        deps.api.fetch_transaction_receipt_on_chain(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &sale.tx_hash,
        ),
        deps.api.fetch_eth_balance_on_chain(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &sale.buyer_address,
            sale.block_number - 1,
        ),
        deps.api
            .fetch_same_block_eth_transfers_for_address_on_chain(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                sale.block_number,
                &sale.buyer_address,
            )
    );
    let purchase_receipt = match purchase_receipt {
        Ok(row) => row,
        Err(_) => return SaleMetricPrefetch::unavailable(sale),
    };
    let base_balance_eth = match base_balance_eth {
        Ok(value) => value,
        Err(_) => return SaleMetricPrefetch::unavailable(sale),
    };
    let same_block_transfers = match same_block_transfers {
        Ok(rows) => rows,
        Err(_) => return SaleMetricPrefetch::unavailable(sale),
    };

    SaleMetricPrefetch {
        metric_key: address_records::sale_metric_key(&sale.tx_hash, &sale.buyer_address),
        block_number: sale.block_number,
        purchase_receipt: Some(purchase_receipt),
        base_balance_eth: Some(base_balance_eth),
        same_block_transfers,
    }
}

pub(super) fn compute_sale_metrics_for_sale(
    sale: &NftSaleRecord,
    prefetched: &SaleMetricPrefetch,
    receipts_by_block: &BTreeMap<i64, BTreeMap<String, TransactionReceiptRecord>>,
) -> address_records::SaleMetricRecord {
    let Some(purchase_receipt) = prefetched.purchase_receipt.as_ref() else {
        return unavailable_sale_metrics();
    };
    let Some(base_balance_eth) = prefetched.base_balance_eth else {
        return unavailable_sale_metrics();
    };
    let empty_receipts = BTreeMap::new();
    let block_receipts = receipts_by_block
        .get(&prefetched.block_number)
        .unwrap_or(&empty_receipts);

    calculate_sale_eth_metrics(
        sale,
        purchase_receipt,
        base_balance_eth,
        &prefetched.same_block_transfers,
        block_receipts,
    )
}

pub(super) fn calculate_sale_eth_metrics(
    sale: &NftSaleRecord,
    purchase_receipt: &TransactionReceiptRecord,
    base_balance_eth: f64,
    same_block_transfers: &[EthTransferRecord],
    receipts_by_hash: &BTreeMap<String, TransactionReceiptRecord>,
) -> address_records::SaleMetricRecord {
    if !sale.is_native_eth || sale.price_eth.is_none() {
        return unavailable_sale_metrics();
    }
    let mut same_block_delta = 0.0;
    for transfer in same_block_transfers {
        let Some(receipt) = receipts_by_hash.get(&transfer.tx_hash) else {
            return unavailable_sale_metrics();
        };
        if receipt.transaction_index >= purchase_receipt.transaction_index {
            continue;
        }
        if transfer.to_address == sale.buyer_address {
            same_block_delta += transfer.value_eth;
        }
        if transfer.from_address == sale.buyer_address {
            same_block_delta -= transfer.value_eth;
        }
    }
    let buy_before_eth_balance = base_balance_eth + same_block_delta;
    let mut buy_total_eth_out = sale.price_eth.unwrap_or(0.0);
    let eth_usd_rate = sale.price_eth.and_then(|price_eth| {
        sale.price_usd
            .filter(|price_usd| price_eth > 0.0 && *price_usd > 0.0)
            .map(|price_usd| price_usd / price_eth)
    });
    let buy_before_usd_balance = eth_usd_rate.map(|rate| buy_before_eth_balance * rate);
    let mut buy_total_usd_out = sale.price_usd;
    if purchase_receipt.from_address == sale.buyer_address {
        let gas_eth = (purchase_receipt.gas_used as f64
            * purchase_receipt.effective_gas_price_wei as f64)
            / 1_000_000_000_000_000_000_f64;
        buy_total_eth_out += gas_eth;
        if let (Some(total_usd), Some(rate)) = (buy_total_usd_out, eth_usd_rate) {
            buy_total_usd_out = Some(total_usd + gas_eth * rate);
        }
    }
    let (ratio_denominator, ratio_numerator, ratio_with_gas_numerator) =
        if let (Some(before_usd), Some(price_usd), Some(total_usd)) =
            (buy_before_usd_balance, sale.price_usd, buy_total_usd_out)
        {
            (before_usd, price_usd, total_usd)
        } else {
            (
                buy_before_eth_balance,
                sale.price_eth.unwrap_or(0.0),
                buy_total_eth_out,
            )
        };
    address_records::SaleMetricRecord {
        buy_before_eth_balance: Some(buy_before_eth_balance),
        buy_before_usd_balance,
        buy_asset_ratio: (ratio_denominator > 0.0).then(|| ratio_numerator / ratio_denominator),
        buy_asset_ratio_with_gas: (ratio_denominator > 0.0)
            .then(|| ratio_with_gas_numerator / ratio_denominator),
        ratio_status: if ratio_denominator > 0.0 {
            "ok".into()
        } else {
            "unavailable".into()
        },
    }
}

pub(super) fn unavailable_sale_metrics() -> address_records::SaleMetricRecord {
    address_records::SaleMetricRecord {
        ratio_status: "unavailable".into(),
        ..address_records::SaleMetricRecord::default()
    }
}
