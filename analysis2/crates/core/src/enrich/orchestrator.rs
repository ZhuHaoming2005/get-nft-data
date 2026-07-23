//! Enrichment orchestrator: one fetch per unique candidate contract.

use std::sync::Arc;

use ahash::AHashMap;
use tokio::sync::Semaphore;

use crate::dedup::candidates::CandidateRegistry;
use crate::entity::{ContractId, ResidentStore};
use crate::error::Analysis2Error;
use crate::progress::ProgressObserver;

use super::alchemy::{self, sales_need_opensea_fallback, FetchOutcome};
use super::controllers;
use super::etherscan;
use super::helius::{self, holders_from_assets};
use super::http::HttpClient;
use super::legit_detect;
use super::mint_payment;
use super::opensea;
use super::types::{
    day_bucket, finalize_legit_signals, ApiKeys, EvidenceBundle, EvidenceStatus, HttpLimits,
    SaleEvent, TransferEvent,
};
use super::value_flow;

/// Enrich each unique candidate once; missing keys → `not_requested`, continue.
pub async fn enrich_candidates(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error> {
    progress.set_stage("enrich");
    let candidates = registry.candidate_contracts();
    progress.begin_phase("enrich_candidates", Some(candidates.len() as u64));

    let client = HttpClient::with_retries(limits.concurrency.max(1), limits.retries)?;
    let semaphore = Arc::new(Semaphore::new(limits.concurrency.max(1)));
    let mut handles = Vec::with_capacity(candidates.len());

    for &contract_id in candidates {
        progress.check_cancelled()?;
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Analysis2Error::http("enrich concurrency pool closed"))?;
        let client = client.clone();
        let keys = keys.clone();
        let limits = limits.clone();
        let chain = store
            .chain_name(store.contracts[contract_id as usize].chain_id)
            .to_owned();
        let address = store.contracts[contract_id as usize].address.clone();
        let is_evm = store.is_evm_chain(&chain);

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let bundle = if is_evm {
                enrich_evm(contract_id, &chain, &address, &client, &keys, &limits).await
            } else {
                enrich_solana(contract_id, &chain, &address, &client, &keys, &limits).await
            };
            (contract_id, bundle)
        }));
    }

    let mut out = AHashMap::with_capacity(candidates.len());
    for handle in handles {
        progress.check_cancelled()?;
        match handle.await {
            Ok((contract_id, bundle)) => {
                out.insert(contract_id, bundle);
                progress.add_completed(1);
            }
            Err(e) => {
                return Err(Analysis2Error::http(format!("enrich task join failed: {e}")));
            }
        }
    }

    // Relation-level legit after all candidate bundles exist (needs seed caches).
    progress.set_stage("enrich_legit");
    legit_detect::attach_relation_legit(&mut out, registry, store, &client, keys, limits).await;

    progress.finish();
    Ok(out)
}

async fn enrich_evm(
    contract_id: ContractId,
    chain: &str,
    address: &str,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
) -> EvidenceBundle {
    let mut bundle = EvidenceBundle::empty(contract_id, chain, address);
    bundle.quality.gas = EvidenceStatus::NotRequested;
    bundle.quality.value_flows = EvidenceStatus::NotRequested;

    let mut transfers = alchemy::fetch_transfers(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        address,
        limits.max_transfer_pages,
    )
    .await;

    if matches!(
        transfers.status,
        EvidenceStatus::Failed | EvidenceStatus::NotRequested
    ) {
        let fallback = etherscan::fetch_transfers(
            client,
            &limits.endpoints.etherscan,
            keys.etherscan(),
            chain,
            address,
            limits.max_transfer_pages,
        )
        .await;
        if !matches!(fallback.status, EvidenceStatus::NotRequested) {
            if matches!(transfers.status, EvidenceStatus::Failed) {
                if let Some(failure) = transfers.failure.take() {
                    bundle.quality.failures.push(failure);
                }
            }
            transfers = fallback;
        }
    }

    let holders = alchemy::fetch_holders(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        address,
        limits.max_holder_pages,
    )
    .await;

    let mut sales = alchemy::fetch_sales(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        address,
        limits.max_sale_pages,
    )
    .await;

    if sales_need_opensea_fallback(&sales.value, sales.status) {
        let os = opensea::fetch_contract_sales(
            client,
            &limits.endpoints.opensea,
            keys.opensea(),
            chain,
            address,
        )
        .await;
        if matches!(os.status, EvidenceStatus::Complete) {
            if matches!(sales.status, EvidenceStatus::Failed) {
                if let Some(failure) = sales.failure.take() {
                    bundle.quality.failures.push(failure);
                }
            }
            if sales.value.is_empty() {
                sales = os;
            } else {
                fill_missing_sale_amounts(&mut sales.value, &os.value);
                if let Some(obs) = os.observation {
                    bundle.provenance.push(obs);
                }
            }
        } else if let Some(obs) = os.observation {
            bundle.provenance.push(obs);
            if let Some(failure) = os.failure {
                bundle.quality.failures.push(failure);
            }
        }
    }

    let day_buckets = collect_day_buckets(&transfers.value, &sales.value);
    let prices = alchemy::fetch_prices(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        &day_buckets,
    )
    .await;
    apply_prices_to_sales(&mut sales.value, &prices.value, chain);

    apply_outcome(
        &mut bundle.quality.transfers,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &transfers,
    );
    bundle.transfers = transfers.value;

    apply_outcome(
        &mut bundle.quality.holders,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &holders,
    );
    bundle.holders = holders.value;

    apply_outcome(
        &mut bundle.quality.sales,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &sales,
    );
    bundle.sales = sales.value;

    apply_outcome(
        &mut bundle.quality.prices,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &prices,
    );
    bundle.prices = prices.value;

    let tx_hashes = alchemy::collect_unique_tx_hashes(&bundle.transfers, &bundle.sales);
    let gas = alchemy::fetch_receipt_gas(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        &tx_hashes,
        limits.concurrency,
    )
    .await;
    alchemy::attach_receipt_gas(&mut bundle.transfers, &gas.value);
    apply_outcome(
        &mut bundle.quality.gas,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &gas,
    );

    // Controllers before value-flow so operator seeds include on-chain owners.
    let controllers_out = controllers::fetch_evm_controllers(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        address,
    )
    .await;
    if let Some(obs) = controllers_out.observation {
        bundle.provenance.push(obs);
    }
    if let Some(failure) = controllers_out.failure {
        bundle.quality.failures.push(failure);
    }
    bundle.controllers = controllers_out.value;

    // After gas attach so mint fee_payers are available as operator seeds.
    let value_flows = value_flow::fetch_evm_value_flows(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        &bundle.controllers,
        &bundle.transfers,
        &bundle.sales,
        limits.concurrency,
    )
    .await;
    apply_outcome(
        &mut bundle.quality.value_flows,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &value_flows,
    );
    bundle.value_flows = value_flows.value;

    let mint_extras =
        collect_evm_mint_payment_extras(client, keys, limits, chain, &bundle.transfers).await;
    mint_payment::attach_mint_payments(
        &mut bundle.transfers,
        &bundle.value_flows,
        &bundle.prices,
        chain,
        &mint_extras,
    );

    bundle.quality.assets = EvidenceStatus::NotRequested;
    bundle.quality.histories = EvidenceStatus::NotRequested;
    finalize_legit_signals(&mut bundle);
    bundle
}

async fn enrich_solana(
    contract_id: ContractId,
    chain: &str,
    address: &str,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
) -> EvidenceBundle {
    let mut bundle = EvidenceBundle::empty(contract_id, chain, address);
    bundle.quality.gas = EvidenceStatus::NotRequested;
    bundle.quality.value_flows = EvidenceStatus::NotRequested;

    let snapshot = helius::fetch_collection_assets(
        client,
        &limits.endpoints.helius,
        keys.helius(),
        address,
        limits.max_solana_assets,
    )
    .await;

    let holders = holders_from_assets(&snapshot.value.assets);
    let holder_status = match snapshot.status {
        EvidenceStatus::NotRequested => EvidenceStatus::NotRequested,
        EvidenceStatus::Failed => EvidenceStatus::Failed,
        other => {
            let truncated = snapshot.truncated;
            if truncated {
                EvidenceStatus::Truncated
            } else if holders.is_empty() {
                EvidenceStatus::Empty
            } else {
                other
            }
        }
    };

    // Controllers from collection metadata before decode/value-flow.
    bundle.controllers = snapshot.value.authority.clone();
    if !bundle.controllers.is_empty() {
        bundle.provenance.push(super::types::EvidenceObservation {
            source: "helius".into(),
            request_key: "contract_controllers".into(),
            observed_at: super::types::now_unix(),
            status: EvidenceStatus::Complete,
        });
    }

    let history = helius::fetch_asset_histories(
        client,
        &limits.endpoints.helius,
        keys.helius(),
        &snapshot.value.assets,
        limits.max_history_assets,
        limits.max_signatures_per_asset,
    )
    .await;

    let (mut transfers, mut sales) = history.value;

    let (gas, value_flows, decode_stats) = helius::decode_and_attach_transactions(
        client,
        &limits.endpoints.helius,
        keys.helius(),
        &mut transfers,
        &mut sales,
        &bundle.controllers,
        limits.concurrency,
    )
    .await;

    let day_buckets = collect_day_buckets(&transfers, &sales);
    let prices = alchemy::fetch_prices(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        &day_buckets,
    )
    .await;
    apply_prices_to_sales(&mut sales, &prices.value, chain);

    apply_outcome(
        &mut bundle.quality.assets,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &snapshot,
    );
    bundle.quality.holders = holder_status;
    if let Some(obs) = snapshot.observation.clone() {
        let mut holder_obs = obs;
        holder_obs.request_key = "helius_holders".into();
        holder_obs.status = holder_status;
        bundle.provenance.push(holder_obs);
    }
    bundle.holders = holders;

    match history.status {
        EvidenceStatus::NotRequested => {
            bundle.quality.histories = EvidenceStatus::NotRequested;
            bundle.quality.transfers = EvidenceStatus::NotRequested;
            bundle.quality.sales = EvidenceStatus::NotRequested;
        }
        EvidenceStatus::Failed => {
            bundle.quality.histories = EvidenceStatus::Failed;
            bundle.quality.transfers = EvidenceStatus::Failed;
            bundle.quality.sales = EvidenceStatus::Failed;
            if let Some(failure) = history.failure {
                bundle.quality.failures.push(failure);
            }
        }
        _ => {
            // Asset/signature page caps, or decode incomplete → Truncated.
            // Signature-only stubs (no successful getTransaction) never Complete.
            // Asset-list Truncated must also force histories Truncated.
            let page_trunc = history.truncated
                || snapshot.truncated
                || snapshot.value.assets.len() > limits.max_history_assets;
            bundle.quality.transfers = helius::field_status_after_decode(
                transfers.is_empty(),
                page_trunc,
                decode_stats.transfers_all_complete(),
                &decode_stats,
            );
            bundle.quality.sales = helius::field_status_after_decode(
                sales.is_empty(),
                page_trunc,
                decode_stats.sales_all_complete(),
                &decode_stats,
            );
            bundle.quality.histories = helius::histories_status_after_decode(
                transfers.is_empty(),
                sales.is_empty(),
                page_trunc,
                &decode_stats,
            );
        }
    }
    if let Some(mut obs) = history.observation {
        // Discovery observation must match final quality after decode (P3).
        if !matches!(
            history.status,
            EvidenceStatus::NotRequested | EvidenceStatus::Failed
        ) {
            obs.status = bundle.quality.histories;
        }
        bundle.provenance.push(obs);
    }
    bundle.transfers = std::mem::take(&mut transfers);
    bundle.sales = std::mem::take(&mut sales);

    apply_outcome(
        &mut bundle.quality.gas,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &gas,
    );
    apply_outcome(
        &mut bundle.quality.value_flows,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &value_flows,
    );
    bundle.value_flows = value_flows.value;

    apply_outcome(
        &mut bundle.quality.prices,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &prices,
    );
    bundle.prices = prices.value;

    mint_payment::attach_mint_payments(
        &mut bundle.transfers,
        &bundle.value_flows,
        &bundle.prices,
        chain,
        &ahash::AHashMap::new(),
    );

    finalize_legit_signals(&mut bundle);
    bundle
}

fn apply_outcome<T>(
    field: &mut EvidenceStatus,
    provenance: &mut Vec<super::types::EvidenceObservation>,
    failures: &mut Vec<String>,
    outcome: &FetchOutcome<T>,
) {
    *field = outcome.status;
    if let Some(obs) = outcome.observation.clone() {
        provenance.push(obs);
    }
    if let Some(failure) = outcome.failure.clone() {
        failures.push(failure);
    }
}

/// Cap mint-recipient EXTERNAL probes for paid-mint attachment.
const MAX_MINT_PAYER_PROBES: usize = 8;

async fn collect_evm_mint_payment_extras(
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    chain: &str,
    transfers: &[TransferEvent],
) -> ahash::AHashMap<String, f64> {
    use ahash::{AHashMap, AHashSet};
    use super::value_flow::activity_block_window;

    let mut out = AHashMap::new();
    let mint_txs: AHashSet<String> = transfers
        .iter()
        .filter(|t| t.is_mint)
        .map(|t| t.tx_hash.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    if mint_txs.is_empty() || keys.alchemy().is_none() {
        return out;
    }
    let window = activity_block_window(transfers, &[]);
    let (from_block, to_block) = window.unwrap_or((0, u64::MAX));

    let mut payers = AHashSet::new();
    for t in transfers.iter().filter(|t| t.is_mint) {
        let addr = t.to.trim().to_ascii_lowercase();
        if !addr.is_empty() && addr != "0x0000000000000000000000000000000000000000" {
            payers.insert(addr);
        }
    }
    let mut payers: Vec<String> = payers.into_iter().collect();
    payers.sort();
    payers.truncate(MAX_MINT_PAYER_PROBES);

    for (idx, payer) in payers.into_iter().enumerate() {
        let outcome = alchemy::fetch_external_transfers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            &payer,
            "from",
            from_block,
            to_block,
            idx,
        )
        .await;
        for row in outcome.value {
            let tx = row.tx_hash.trim().to_ascii_lowercase();
            if !mint_txs.contains(&tx) {
                continue;
            }
            let amt = row.value_native.unwrap_or(0.0);
            if amt <= 0.0 {
                continue;
            }
            let entry = out.entry(tx).or_insert(0.0);
            *entry += amt;
        }
    }
    out
}

fn collect_day_buckets(transfers: &[TransferEvent], sales: &[SaleEvent]) -> Vec<i64> {
    let mut days = std::collections::BTreeSet::new();
    for event in transfers {
        if let Some(ts) = event.timestamp {
            days.insert(day_bucket(ts));
        }
    }
    for event in sales {
        if let Some(ts) = event.timestamp {
            days.insert(day_bucket(ts));
        }
    }
    days.into_iter().collect()
}

fn apply_prices_to_sales(
    sales: &mut [SaleEvent],
    prices: &[super::types::PriceBucket],
    chain: &str,
) {
    if prices.is_empty() {
        return;
    }
    let by_day: AHashMap<i64, f64> = prices
        .iter()
        .filter(|p| p.chain == chain)
        .map(|p| (p.day_utc, p.usd_per_native))
        .collect();
    for sale in sales {
        if sale.usd_amount.is_some() {
            continue;
        }
        let (Some(ts), Some(native)) = (sale.timestamp, sale.native_amount) else {
            continue;
        };
        if let Some(rate) = by_day.get(&day_bucket(ts)) {
            sale.usd_amount = Some(native * rate);
        }
    }
}

fn fill_missing_sale_amounts(preferred: &mut [SaleEvent], fallback: &[SaleEvent]) {
    for sale in preferred.iter_mut() {
        if sale.native_amount.is_some() || sale.usd_amount.is_some() {
            continue;
        }
        if let Some(src) = fallback.iter().find(|f| {
            (!sale.tx_hash.is_empty() && f.tx_hash == sale.tx_hash)
                || (f.token_id == sale.token_id
                    && !sale.token_id.is_empty()
                    && f.timestamp == sale.timestamp)
        }) {
            if sale.native_amount.is_none() {
                sale.native_amount = src.native_amount;
            }
            if sale.usd_amount.is_none() {
                sale.usd_amount = src.usd_amount;
            }
            if sale.currency_symbol.is_none() {
                sale.currency_symbol = src.currency_symbol.clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashSet;
    use httpmock::prelude::*;
    use serde_json::json;

    use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
    use crate::enrich::types::ProviderEndpoints;
    use crate::enrich::ValueFlowKind;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::progress::NoopProgress;

    fn identity(chain: &str, address: &str, token: &str, row: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.into(),
            contract_address: address.into(),
            token_id: token.into(),
            name_norm: "n".into(),
            token_uri_norm: format!("uri://{token}"),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: row,
            },
        }
    }

    fn store_with_candidate(chain: &str, address: &str) -> (ResidentStore, u32, u32) {
        let evm = ["ethereum", "base", "polygon"]
            .into_iter()
            .map(str::to_owned)
            .collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(2, &evm);
        store
            .ingest_identity_row(identity(chain, "0xseed", "1", 1))
            .unwrap();
        store
            .ingest_identity_row(identity(chain, address, "1", 2))
            .unwrap();
        let seed = cid(&store, chain, "0xseed");
        let cand = cid(&store, chain, address);
        (store, seed, cand)
    }

    fn cid(store: &ResidentStore, chain: &str, address: &str) -> u32 {
        let chain_id = store.chain_ids[chain];
        store.contract_index[&(chain_id, address.to_owned())]
    }

    fn registry_one(seed: u32, cand: u32) -> CandidateRegistry {
        let mut g = HitGraph::new();
        g.push(HitEdge {
            seed_contract: seed,
            candidate_contract: cand,
            candidate_nft: Some(1),
            dimension: Dimension::TokenUri,
            score: 1.0,
            primary_chain: 0,
            secondary_chain: 0,
        });
        let mut nfts = AHashMap::new();
        nfts.insert(cand, vec![1]);
        CandidateRegistry::from_hit_graph(&g, &nfts)
    }

    fn mock_endpoints(server: &MockServer) -> ProviderEndpoints {
        let base = server.base_url();
        let mut alchemy_networks = AHashMap::new();
        alchemy_networks.insert("ethereum".into(), "eth-mainnet".into());
        ProviderEndpoints {
            alchemy_rpc_template: format!("{base}/rpc/{{network}}/{{key}}"),
            alchemy_nft_template: format!("{base}/nft/{{network}}/{{key}}/{{method}}"),
            alchemy_prices: format!("{base}/prices/v1"),
            etherscan: format!("{base}/etherscan"),
            helius: format!("{base}/helius"),
            opensea: format!("{base}/opensea"),
            alchemy_networks,
        }
    }

    #[tokio::test]
    async fn missing_keys_mark_not_requested_and_continue() {
        let (store, seed, cand) = store_with_candidate("ethereum", "0xabc");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            ..HttpLimits::default()
        };
        let keys = ApiKeys::default();
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.transfers, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.sales, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.holders, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.prices, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.gas, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::NotRequested);
        assert!(bundle.quality.failures.is_empty());
    }

    #[tokio::test]
    async fn alchemy_empty_transfers_are_empty_not_failed() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0x1111111111111111111111111111111111111111");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            opensea: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Empty);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Empty);
        assert_eq!(bundle.quality.sales, EvidenceStatus::Empty);
        assert_eq!(bundle.quality.gas, EvidenceStatus::Empty);
        // No operator seeds without mint fee_payers / controllers.
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Empty);
        assert_ne!(bundle.quality.transfers, EvidenceStatus::Failed);
        assert!(bundle.controllers.is_empty());
    }

    #[tokio::test]
    async fn alchemy_controllers_filled_from_metadata_and_onchain() {
        let server = MockServer::start_async().await;
        let _meta = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getContractMetadata");
                then.status(200).json_body(json!({
                    "contractMetadata": {
                        "contractDeployer": "0xDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDd",
                        "ownerAddress": "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
                    }
                }));
            })
            .await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!([
                    {
                        "jsonrpc": "2.0",
                        "id": "owner",
                        "result": "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": "owner-fallback",
                        "result": "0x0"
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": "admin",
                        "result": "0x0"
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": "eip1967-admin",
                        "result": "0x000000000000000000000000cccccccccccccccccccccccccccccccccccccccc"
                    }
                ]));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0x1111111111111111111111111111111111111111");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert!(
            bundle
                .controllers
                .iter()
                .any(|c| c == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "controllers={:?}",
            bundle.controllers
        );
        assert!(bundle
            .controllers
            .iter()
            .any(|c| c == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        assert!(bundle
            .controllers
            .iter()
            .any(|c| c == "0xcccccccccccccccccccccccccccccccccccccccc"));
        assert!(bundle
            .provenance
            .iter()
            .any(|o| o.request_key == "contract_controllers"));
    }

    #[tokio::test]
    async fn alchemy_transfer_failure_falls_back_to_etherscan() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(500).body("boom");
            })
            .await;
        let _etherscan = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/etherscan")
                    .query_param("action", "tokennfttx");
                then.status(200).json_body(json!({
                    "status": "1",
                    "message": "OK",
                    "result": [{
                        "hash": "0xdead",
                        "from": "0x0000000000000000000000000000000000000000",
                        "to": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "tokenID": "7",
                        "timeStamp": "1700000000",
                        "blockNumber": "100"
                    }]
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({
                    "owners": [{
                        "ownerAddress": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "tokenBalances": [{"tokenId": "7", "balance": "1"}]
                    }]
                }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            etherscan: Some("esk".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_eq!(bundle.transfers.len(), 1);
        assert_eq!(bundle.transfers[0].token_id, "7");
        assert!(bundle.transfers[0].is_mint);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Complete);
    }

    #[tokio::test]
    async fn http_500_marks_failed_quality() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(500).body("nope");
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(500).body("nope");
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(500).body("nope");
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xcccccccccccccccccccccccccccccccccccccccc");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            etherscan: None,
            opensea: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.sales, EvidenceStatus::Failed);
        assert!(!bundle.quality.failures.is_empty());
    }

    #[tokio::test]
    async fn prices_complete_when_day_buckets_returned() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xabc",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({
                    "nftSales": [{
                        "transactionHash": "0xsale",
                        "tokenId": "1",
                        "sellerAddress": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "buyerAddress": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "blockTimestamp": "2024-01-01T12:00:00Z",
                        "sellerFee": { "amount": "1000000000000000000", "decimals": 18, "symbol": "ETH" },
                        "protocolFee": { "amount": "0", "decimals": 18, "symbol": "ETH" },
                        "royaltyFee": { "amount": "0", "decimals": 18, "symbol": "ETH" }
                    }]
                }));
            })
            .await;
        let _prices = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/tokens/historical");
                then.status(200).json_body(json!({
                    "data": [{
                        "timestamp": "2024-01-01T00:00:00Z",
                        "value": "2500.5"
                    }]
                }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            opensea: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.prices, EvidenceStatus::Complete);
        assert_eq!(bundle.prices.len(), 1);
        assert!(bundle.sales[0].usd_amount.is_some());
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_eq!(bundle.quality.sales, EvidenceStatus::Complete);
    }

    #[tokio::test]
    async fn solana_missing_helius_is_not_requested() {
        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(2, &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed111111111111111111111111111111111",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand111111111111111111111111111111111",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed111111111111111111111111111111111");
        let cand = cid(&store, "solana", "ColCand111111111111111111111111111111111");
        let registry = registry_one(seed, cand);
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            helius: None,
            ..ApiKeys::default()
        };
        let map =
            enrich_candidates(&registry, &store, &keys, &HttpLimits::default(), &NoopProgress)
                .await
                .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.assets, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.histories, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.holders, EvidenceStatus::NotRequested);
    }

    #[tokio::test]
    async fn alchemy_empty_sales_do_not_call_opensea() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;
        let opensea = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/events/nft");
                then.status(200).json_body(json!({ "asset_events": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0x2222222222222222222222222222222222222222");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            opensea: Some("osk".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.sales, EvidenceStatus::Empty);
        assert_eq!(opensea.hits(), 0);
        assert!(!bundle
            .provenance
            .iter()
            .any(|o| o.source == "opensea" || o.request_key == "opensea_sales"));
    }

    #[tokio::test]
    async fn solana_signature_stubs_are_not_complete() {
        let server = MockServer::start_async().await;
        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": "MintStub111111111111111111111111111111111",
                            "ownership": {"owner": "OwnerStub1111111111111111111111111111111"},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "items": [
                            ["SigTransfer111111111111111111111111111111", "transfer"],
                            ["SigSale11111111111111111111111111111111111", "sale"]
                        ]
                    }
                }));
            })
            .await;
        // Null getTransaction results leave signature stubs → Truncated (not Complete).
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": null
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(2, &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed222222222222222222222222222222222",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand222222222222222222222222222222222",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed222222222222222222222222222222222");
        let cand = cid(&store, "solana", "ColCand222222222222222222222222222222222");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert!(!bundle.transfers.is_empty() || !bundle.sales.is_empty());
        assert_ne!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_ne!(bundle.quality.sales, EvidenceStatus::Complete);
        assert_ne!(bundle.quality.histories, EvidenceStatus::Complete);
        if !bundle.transfers.is_empty() {
            assert_eq!(bundle.quality.transfers, EvidenceStatus::Truncated);
        }
        if !bundle.sales.is_empty() {
            assert_eq!(bundle.quality.sales, EvidenceStatus::Truncated);
        }
        assert_eq!(bundle.quality.histories, EvidenceStatus::Truncated);
        // Stubs lack from/fee — gas must not be Complete.
        assert_ne!(bundle.quality.gas, EvidenceStatus::Complete);
        // P3: discovery provenance must not stay Complete when decode leaves Truncated.
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Truncated);
    }

    #[tokio::test]
    async fn solana_get_transaction_decode_can_complete() {
        let server = MockServer::start_async().await;
        let mint = "MintComplete11111111111111111111111111111";
        let seller = "SellerComp1111111111111111111111111111111";
        let buyer = "BuyerComp11111111111111111111111111111111";
        let fee_payer = "FeePayerComp111111111111111111111111111";
        let funder = "FunderComp11111111111111111111111111111";
        let sig = "SigComplete111111111111111111111111111111";

        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": mint,
                            "ownership": {"owner": buyer},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "items": [[sig, "transfer"]]
                    }
                }));
            })
            .await;
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": {
                        "slot": 99,
                        "blockTime": 1_700_000_100i64,
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {"pubkey": fee_payer, "signer": true},
                                    {"pubkey": "TokenAcc11111111111111111111111111111111", "signer": false}
                                ],
                                "instructions": [{
                                    "program": "system",
                                    "parsed": {
                                        "type": "transfer",
                                        "info": {
                                            "source": funder,
                                            "destination": fee_payer,
                                            "lamports": 1_500_000_000u64
                                        }
                                    }
                                }]
                            }
                        },
                        "meta": {
                            "err": null,
                            "fee": 5000,
                            "preTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": seller,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }],
                            "postTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": buyer,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }]
                        }
                    }
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(2, &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed333333333333333333333333333333333",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand333333333333333333333333333333333",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed333333333333333333333333333333333");
        let cand = cid(&store, "solana", "ColCand333333333333333333333333333333333");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.transfers.len(), 1);
        let t = &bundle.transfers[0];
        assert_eq!(t.from, seller);
        assert_eq!(t.to, buyer);
        assert_eq!(t.timestamp, Some(1_700_000_100));
        assert!(t.gas_native.is_some());
        assert_eq!(t.fee_payer.as_deref(), Some(fee_payer));
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_eq!(bundle.quality.histories, EvidenceStatus::Complete);
        assert_eq!(bundle.quality.gas, EvidenceStatus::Complete);
        assert!(
            bundle
                .value_flows
                .iter()
                .any(|e| e.from == funder && e.to == fee_payer && e.kind == ValueFlowKind::Funding),
            "expected Funding edge into fee payer, got {:?}",
            bundle.value_flows
        );
        assert_ne!(bundle.quality.value_flows, EvidenceStatus::Failed);
        assert_ne!(bundle.quality.value_flows, EvidenceStatus::NotRequested);
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Complete);
    }

    #[tokio::test]
    async fn solana_mint_without_token_balances_stays_truncated() {
        // Mint stub + getTransaction fee/timestamp but no pre/postTokenBalances
        // (Bubblegum/compressed) → never Complete.
        let server = MockServer::start_async().await;
        let mint = "MintNoBalInt111111111111111111111111111";
        let owner = "OwnerNoBalInt1111111111111111111111111";
        let fee_payer = "FeePayerNoBal111111111111111111111111";
        let sig = "SigMintNoBalInt111111111111111111111111";

        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": mint,
                            "ownership": {"owner": owner},
                            "compression": {"compressed": true}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "items": [[sig, "mint"]]
                    }
                }));
            })
            .await;
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": {
                        "slot": 3,
                        "blockTime": 1_700_000_300i64,
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {"pubkey": fee_payer, "signer": true}
                                ],
                                "instructions": []
                            }
                        },
                        "meta": {
                            "err": null,
                            "fee": 5000
                        }
                    }
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(2, &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed555555555555555555555555555555555",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand555555555555555555555555555555555",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed555555555555555555555555555555555");
        let cand = cid(&store, "solana", "ColCand555555555555555555555555555555555");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.transfers.len(), 1);
        assert!(bundle.transfers[0].is_mint);
        assert!(bundle.transfers[0].gas_native.is_some());
        assert!(bundle.transfers[0].timestamp.is_some());
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Truncated);
        assert_eq!(bundle.quality.histories, EvidenceStatus::Truncated);
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Truncated);
    }

    #[tokio::test]
    async fn solana_asset_page_truncation_keeps_histories_truncated() {
        // Even when the single returned asset fully decodes, collection total > fetched
        // assets → snapshot.truncated → histories must stay Truncated.
        let server = MockServer::start_async().await;
        let mint = "MintTruncPage111111111111111111111111111";
        let seller = "SellerTrunc11111111111111111111111111111";
        let buyer = "BuyerTrunc111111111111111111111111111111";
        let fee_payer = "FeePayerTrunc1111111111111111111111111";
        let sig = "SigTruncPage11111111111111111111111111111";

        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 50,
                        "items": [{
                            "id": mint,
                            "ownership": {"owner": buyer},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "items": [[sig, "transfer"]]
                    }
                }));
            })
            .await;
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": {
                        "slot": 11,
                        "blockTime": 1_700_000_200i64,
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {"pubkey": fee_payer, "signer": true},
                                    {"pubkey": "TokenAccTrunc1111111111111111111111111", "signer": false}
                                ],
                                "instructions": []
                            }
                        },
                        "meta": {
                            "err": null,
                            "fee": 5000,
                            "preTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": seller,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }],
                            "postTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": buyer,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }]
                        }
                    }
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(2, &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed444444444444444444444444444444444",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand444444444444444444444444444444444",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed444444444444444444444444444444444");
        let cand = cid(&store, "solana", "ColCand444444444444444444444444444444444");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.assets, EvidenceStatus::Truncated);
        assert_eq!(bundle.transfers.len(), 1);
        assert_eq!(bundle.transfers[0].from, seller);
        assert_eq!(bundle.transfers[0].to, buyer);
        // Decoded transfer fields are complete, but asset-list truncation forbids
        // Complete histories.
        assert_eq!(bundle.quality.histories, EvidenceStatus::Truncated);
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Truncated);
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Truncated);
    }

    #[tokio::test]
    async fn alchemy_receipt_gas_complete_fills_transfer() {
        let server = MockServer::start_async().await;
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xabc123",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _external = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "transactionHash": "0xabc123",
                        "from": "0xFeePayer1111111111111111111111111111111111",
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.gas, EvidenceStatus::Complete);
        assert_eq!(bundle.transfers.len(), 1);
        // 21000 * 1e9 wei = 2.1e13 → 0.000021 ETH
        let gas = bundle.transfers[0].gas_native.unwrap();
        assert!((gas - 0.000021).abs() < 1e-12);
        assert_eq!(
            bundle.transfers[0].fee_payer.as_deref(),
            Some("0xfeepayer1111111111111111111111111111111111")
        );
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Empty);
    }

    #[tokio::test]
    async fn alchemy_receipt_gas_truncated_when_partial() {
        let server = MockServer::start_async().await;
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [
                            {
                                "hash": "0xgood",
                                "from": "0x0000000000000000000000000000000000000000",
                                "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                                "erc721TokenId": "0x1",
                                "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                                "blockNum": "0x10"
                            },
                            {
                                "hash": "0xbad",
                                "from": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "to": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "erc721TokenId": "0x2",
                                "metadata": { "blockTimestamp": "2024-01-01T01:00:00Z" },
                                "blockNum": "0x11"
                            }
                        ]
                    }
                }));
            })
            .await;
        let _external = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _receipt_ok = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xgood");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-ok",
                    "result": {
                        "from": "0xcccccccccccccccccccccccccccccccccccccccc",
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _receipt_bad = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xbad");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-bad",
                    "result": null
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.gas, EvidenceStatus::Truncated);
        let good = bundle
            .transfers
            .iter()
            .find(|t| t.tx_hash.eq_ignore_ascii_case("0xgood"))
            .unwrap();
        assert!(good.gas_native.is_some());
        let bad = bundle
            .transfers
            .iter()
            .find(|t| t.tx_hash.eq_ignore_ascii_case("0xbad"))
            .unwrap();
        assert!(bad.gas_native.is_none());
    }

    #[tokio::test]
    async fn alchemy_value_flows_complete_funding_and_withdrawal() {
        let server = MockServer::start_async().await;
        let operator = "0xcccccccccccccccccccccccccccccccccccccccc";
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xmint",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "from": operator,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _external_to = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("toAddress");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-to",
                    "result": {
                        "transfers": [{
                            "hash": "0xfund",
                            "from": "0x1111111111111111111111111111111111111111",
                            "to": operator,
                            "category": "external",
                            "value": 2.0,
                            "blockNum": "0x10",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" }
                        }]
                    }
                }));
            })
            .await;
        let _external_from = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("fromAddress");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-from",
                    "result": {
                        "transfers": [{
                            "hash": "0xwithdraw",
                            "from": operator,
                            "to": "0x2222222222222222222222222222222222222222",
                            "category": "external",
                            "value": 0.5,
                            "blockNum": "0x10",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:01Z" }
                        }]
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Complete);
        assert_eq!(bundle.value_flows.len(), 2);
        let funding = bundle
            .value_flows
            .iter()
            .find(|e| e.kind == ValueFlowKind::Funding)
            .unwrap();
        assert!((funding.native_amount.unwrap() - 2.0).abs() < 1e-12);
        let withdrawal = bundle
            .value_flows
            .iter()
            .find(|e| e.kind == ValueFlowKind::Withdrawal)
            .unwrap();
        assert!((withdrawal.native_amount.unwrap() - 0.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn alchemy_value_flows_unbounded_window_truncated_without_failure() {
        let server = MockServer::start_async().await;
        let operator = "0xcccccccccccccccccccccccccccccccccccccccc";
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xmint",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" }
                        }]
                    }
                }));
            })
            .await;
        let _receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "from": operator,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _external = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external",
                    "result": {
                        "transfers": [{
                            "hash": "0xfund",
                            "from": "0x1111111111111111111111111111111111111111",
                            "to": operator,
                            "category": "external",
                            "value": 1.0,
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" }
                        }]
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Truncated);
        assert!(!bundle.value_flows.is_empty());
        assert!(
            !bundle
                .quality
                .failures
                .iter()
                .any(|f| f.contains("activity block window")),
            "unbounded window note must not enter quality.failures: {:?}",
            bundle.quality.failures
        );
        assert!(
            bundle
                .provenance
                .iter()
                .any(|o| o.request_key.contains("activity block window unknown")),
            "expected provenance note, got {:?}",
            bundle.provenance
        );
    }

    #[tokio::test]
    async fn alchemy_value_flows_ignores_non_mint_fee_payer_seed() {
        let server = MockServer::start_async().await;
        let mint_op = "0xcccccccccccccccccccccccccccccccccccccccc";
        let secondary_fee = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [
                            {
                                "hash": "0xmint",
                                "from": "0x0000000000000000000000000000000000000000",
                                "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                                "erc721TokenId": "0x1",
                                "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                                "blockNum": "0x10"
                            },
                            {
                                "hash": "0xsec",
                                "from": "0xdddddddddddddddddddddddddddddddddddddddd",
                                "to": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                                "erc721TokenId": "0x1",
                                "metadata": { "blockTimestamp": "2024-01-01T00:00:01Z" },
                                "blockNum": "0x11"
                            }
                        ]
                    }
                }));
            })
            .await;
        let _receipt_mint = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xmint");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "from": mint_op,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _receipt_sec = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xsec");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-1",
                    "result": {
                        "from": secondary_fee,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        // Only mint operator should be queried; secondary fee_payer must not appear.
        let external_mint = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains(mint_op);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-mint",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let external_secondary = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains(secondary_fee);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-secondary",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getNFTSales");
                then.status(200).json_body(json!({ "nftSales": [] }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Empty);
        assert!(external_mint.hits() > 0);
        assert_eq!(
            external_secondary.hits(),
            0,
            "non-mint fee_payer must not be queried as operator seed"
        );
    }
}
