use crate::api::http::ProviderHttpClient;
use crate::api::{price::PriceOracle, ProviderApiKeys};
use crate::config::ProviderEndpoints;
use crate::error::{AnalysisError, Result};
use crate::model::{
    ChainId, ContractKey, EventKind, EvidenceBundle, EvidenceObservation, EvidenceQuality,
    EvidenceStatus, NftKey, NftSelection, NormalizedEvent, RelationVerification,
    SeedCandidateRelation, ValueChannel,
};
use futures_util::{stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
type OpenSeaContractCell = Arc<tokio::sync::OnceCell<Value>>;
type OpenSeaContractCache = Arc<tokio::sync::Mutex<HashMap<ContractKey, OpenSeaContractCell>>>;
type CashoutLookup = (Arc<str>, u64, Option<i128>, Option<i128>);

#[derive(Clone, Copy)]
enum FlowDirection {
    Incoming,
    Outgoing,
    Cashout,
}

#[derive(Default)]
struct ReceiptCoverage {
    fetched: u64,
    provider_reported: u64,
    failed: u64,
}

struct ReceiptData {
    from: Option<Arc<str>>,
    gas_native: Option<i128>,
}

impl FlowDirection {
    const fn address_field(self) -> &'static str {
        match self {
            Self::Incoming => "toAddress",
            Self::Outgoing | Self::Cashout => "fromAddress",
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Incoming => "incoming",
            Self::Outgoing => "outgoing",
            Self::Cashout => "cashout",
        }
    }
}

#[derive(Clone)]
pub struct EvmClient {
    alchemy: ProviderHttpClient,
    other: ProviderHttpClient,
    keys: ProviderApiKeys,
    endpoints: ProviderEndpoints,
    page_limits: Arc<BTreeMap<String, usize>>,
    prices: PriceOracle,
    observed_at: i64,
    controller_cache:
        Arc<tokio::sync::Mutex<HashMap<ContractKey, Arc<tokio::sync::OnceCell<Metadata>>>>>,
    opensea_contract_cache: OpenSeaContractCache,
}

#[derive(Clone)]
struct Metadata {
    deployment_timestamp: Option<i64>,
    deployment_block: Option<u64>,
    deployment_tx: Option<Arc<str>>,
    deployer: Option<Arc<str>>,
    controllers: Vec<Arc<str>>,
    authority_status: EvidenceStatus,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            deployment_timestamp: None,
            deployment_block: None,
            deployment_tx: None,
            deployer: None,
            controllers: Vec::new(),
            authority_status: EvidenceStatus::NotRequested,
        }
    }
}

struct Outcome<T> {
    value: T,
    status: EvidenceStatus,
    source: &'static str,
    request_key: &'static str,
    failure: Option<String>,
}

impl<T: Default> Outcome<T> {
    fn skipped(request_key: &'static str) -> Self {
        Self {
            value: T::default(),
            status: EvidenceStatus::NotRequested,
            source: "none",
            request_key,
            failure: None,
        }
    }

    fn failed(source: &'static str, request_key: &'static str, error: AnalysisError) -> Self {
        Self {
            value: T::default(),
            status: EvidenceStatus::Failed,
            source,
            request_key,
            failure: Some(format!("{request_key}: {error}")),
        }
    }
}

impl<T> Outcome<T> {
    fn complete(
        value: T,
        empty: bool,
        truncated: bool,
        source: &'static str,
        request_key: &'static str,
    ) -> Self {
        Self {
            value,
            status: if truncated {
                EvidenceStatus::Truncated
            } else if empty {
                EvidenceStatus::Empty
            } else {
                EvidenceStatus::Complete
            },
            source,
            request_key,
            failure: None,
        }
    }

    fn observation(&self, observed_at: i64) -> EvidenceObservation {
        EvidenceObservation {
            source: self.source.into(),
            request_key: self.request_key.into(),
            observed_at,
            status: self.status,
        }
    }

    fn with_failure(mut self, error: AnalysisError) -> Self {
        self.failure = Some(format!("{}: {error}", self.request_key));
        self
    }
}

impl EvmClient {
    pub fn new(
        alchemy: ProviderHttpClient,
        other: ProviderHttpClient,
        keys: ProviderApiKeys,
        endpoints: ProviderEndpoints,
        page_limits: Arc<BTreeMap<String, usize>>,
        prices: PriceOracle,
        observed_at: i64,
    ) -> Self {
        Self {
            alchemy,
            other,
            keys,
            endpoints,
            page_limits,
            prices,
            observed_at,
            controller_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            opensea_contract_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn fetch(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
        relations: &[SeedCandidateRelation],
    ) -> Result<EvidenceBundle> {
        if !candidate.chain.is_evm() {
            return Err(AnalysisError::Provider(
                "EVM client received a non-EVM candidate".into(),
            ));
        }
        let (metadata, transfers, holders, sales, value_flows) = tokio::join!(
            self.fetch_metadata(candidate),
            self.fetch_transfers(candidate, selection),
            self.fetch_holders(candidate, selection),
            self.fetch_sales(candidate, selection),
            self.fetch_value_flows(candidate),
        );
        let relation_verifications = self
            .relation_verifications(
                relations,
                &metadata.value.controllers,
                matches!(
                    metadata.value.authority_status,
                    EvidenceStatus::Complete | EvidenceStatus::Empty
                ),
            )
            .await;
        let observed_at = self.observed_at;
        let metadata_observation = metadata.observation(observed_at);
        let transfers_observation = transfers.observation(observed_at);
        let holders_observation = holders.observation(observed_at);
        let sales_observation = sales.observation(observed_at);
        let value_flows_observation = value_flows.observation(observed_at);
        let assets_status = holders.status;
        let histories_status = transfers.status;
        let authority_status = metadata.value.authority_status;
        let mut events = transfers.value;
        events.extend(sales.value);
        events.extend(value_flows.value);
        if let Some(deployer) = metadata.value.deployer.clone() {
            events.push(deployment_event(candidate, &metadata.value, deployer));
        }
        assign_event_indices(&mut events);
        let receipt_coverage = self.enrich_receipts(candidate, &mut events).await;
        let receipt_observation = receipt_coverage.observation(observed_at);
        let transactions_status =
            combined_status(&[sales.status, value_flows.status, receipt_coverage.status]);
        let price_failure = self
            .prices
            .enrich_events(&mut events)
            .await
            .err()
            .map(|error| format!("historical_prices: {error}"));
        let sale_prices_total = events
            .iter()
            .filter(|event| event.kind == EventKind::Sale)
            .count() as u64;
        let sale_prices_parsed = events
            .iter()
            .filter(|event| event.kind == EventKind::Sale && event.usd_micros.is_some())
            .count() as u64;
        let priced_values_total = events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    EventKind::Sale
                        | EventKind::Funding
                        | EventKind::Withdrawal
                        | EventKind::Cashout
                )
            })
            .count();
        let priced_values_parsed = events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    EventKind::Sale
                        | EventKind::Funding
                        | EventKind::Withdrawal
                        | EventKind::Cashout
                ) && event.usd_micros.is_some()
            })
            .count();
        let prices = if [sales.status, value_flows.status]
            .iter()
            .all(|status| *status == EvidenceStatus::NotRequested)
        {
            EvidenceStatus::NotRequested
        } else if sales.status == EvidenceStatus::Failed && priced_values_total == 0 {
            EvidenceStatus::Failed
        } else if priced_values_total == 0 {
            EvidenceStatus::Empty
        } else if price_failure.is_none() && priced_values_total == priced_values_parsed {
            EvidenceStatus::Complete
        } else {
            EvidenceStatus::Truncated
        };
        let mut failures = [
            metadata.failure.as_ref(),
            transfers.failure.as_ref(),
            holders.failure.as_ref(),
            sales.failure.as_ref(),
            value_flows.failure.as_ref(),
            receipt_coverage.failure.as_ref(),
            price_failure.as_ref(),
        ]
        .into_iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
        let relation_failure_count = relation_verifications
            .iter()
            .map(|verification| verification.failures.len() as u64)
            .sum::<u64>();
        failures.extend(relation_verifications.iter().flat_map(|verification| {
            verification.failures.iter().map(move |failure| {
                format!(
                    "seed_{} relation_verification: {failure}",
                    verification.seed_id.0
                )
            })
        }));
        failures.sort();
        failures.dedup();
        let provenance = vec![
            metadata_observation,
            transfers_observation,
            holders_observation,
            sales_observation,
            value_flows_observation,
            receipt_observation,
            EvidenceObservation {
                source: if self.keys.alchemy.is_empty() {
                    "none"
                } else {
                    "alchemy"
                }
                .into(),
                request_key: "historical_prices".into(),
                observed_at,
                status: prices,
            },
        ];
        let candidate_assets_analyzed = holders
            .value
            .iter()
            .map(|(nft, _)| nft)
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let selected_assets = selection.declared_count();
        let history_assets_requested =
            u64::from(transfers.status != EvidenceStatus::NotRequested) * selected_assets;
        let history_assets_succeeded = u64::from(matches!(
            transfers.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty | EvidenceStatus::Truncated
        )) * selected_assets;
        let history_assets_complete = u64::from(matches!(
            transfers.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        )) * selected_assets;
        let history_assets_failed =
            u64::from(transfers.status == EvidenceStatus::Failed) * selected_assets;
        let history_assets_not_requested =
            u64::from(transfers.status == EvidenceStatus::NotRequested) * selected_assets;
        let history_assets_truncated =
            u64::from(transfers.status == EvidenceStatus::Truncated) * selected_assets;
        Ok(EvidenceBundle {
            candidate: candidate.clone(),
            deployment_timestamp: metadata.value.deployment_timestamp,
            duplicate_content_timestamp: None,
            events,
            holders: holders.value,
            controllers: metadata.value.controllers,
            relation_verifications,
            provenance,
            quality: EvidenceQuality {
                assets: Some(assets_status),
                histories: Some(histories_status),
                transactions: Some(transactions_status),
                prices: Some(prices),
                authority: Some(authority_status),
                sale_prices_parsed,
                sale_prices_total,
                candidate_assets_analyzed,
                candidate_assets_total: selected_assets,
                history_assets_requested,
                history_assets_succeeded,
                history_assets_complete,
                history_assets_failed,
                history_assets_not_requested,
                history_assets_truncated,
                transactions_fetched: receipt_coverage.value.fetched,
                transactions_provider_reported: receipt_coverage.value.provider_reported,
                transactions_failed: receipt_coverage.value.failed,
                supplemental_query_failures: receipt_coverage.value.failed
                    + u64::from(price_failure.is_some())
                    + u64::from(value_flows.failure.is_some())
                    + u64::from(metadata.value.authority_status == EvidenceStatus::Truncated)
                    + relation_failure_count,
                failures,
                ..EvidenceQuality::default()
            },
        })
    }

    pub async fn verify_relations(
        &self,
        relations: &[SeedCandidateRelation],
        controllers: &[Arc<str>],
        authority_complete: bool,
    ) -> Vec<RelationVerification> {
        self.relation_verifications(relations, controllers, authority_complete)
            .await
    }

    async fn fetch_metadata(&self, candidate: &ContractKey) -> Outcome<Metadata> {
        if self.keys.alchemy.is_empty() && self.keys.opensea.is_empty() {
            return Outcome::skipped("contract_metadata");
        }
        match self.metadata_with_fallback(candidate).await {
            Ok((metadata, source, primary_failure)) => {
                let truncated = metadata.authority_status == EvidenceStatus::Truncated;
                let outcome =
                    Outcome::complete(metadata, false, truncated, source, "contract_metadata");
                if let Some(error) = primary_failure {
                    outcome.with_failure(error)
                } else {
                    outcome
                }
            }
            Err(error) => Outcome::failed("alchemy+opensea", "contract_metadata", error),
        }
    }

    async fn metadata_with_fallback(
        &self,
        contract: &ContractKey,
    ) -> Result<(Metadata, &'static str, Option<AnalysisError>)> {
        if !self.keys.alchemy.is_empty() {
            match self.alchemy_metadata(contract).await {
                Ok(metadata) => return Ok((metadata, "alchemy", None)),
                Err(error) if self.keys.opensea.is_empty() => return Err(error),
                Err(primary_error) => {
                    return self
                        .opensea_metadata(contract)
                        .await
                        .map(|metadata| (metadata, "opensea", Some(primary_error)));
                }
            }
        }
        if self.keys.opensea.is_empty() {
            return Err(AnalysisError::Config(
                "EVM metadata requires an Alchemy or OpenSea API key".into(),
            ));
        }
        self.opensea_metadata(contract)
            .await
            .map(|metadata| (metadata, "opensea", None))
    }

    async fn alchemy_metadata(&self, contract: &ContractKey) -> Result<Metadata> {
        let mut url = self.alchemy_nft_url(contract.chain, "getContractMetadata")?;
        url.query_pairs_mut()
            .append_pair("contractAddress", &contract.contract_address);
        let payload = self.alchemy.get(url, HeaderMap::new()).await?;
        if let Some(error) = payload.get("error") {
            return Err(AnalysisError::Provider(format!(
                "Alchemy contract metadata request failed: {error}"
            )));
        }
        let metadata = payload.get("contractMetadata").unwrap_or(&payload);
        if !metadata.is_object() {
            return Err(AnalysisError::Provider(
                "Alchemy contract metadata response was not an object".into(),
            ));
        }
        let block_number = integer(metadata.get("deployedBlockNumber"));
        let (deployment_timestamp, deployment_tx, mut controllers, supplemental_failed) =
            if block_number > 0 {
                let (deployment, controllers) = tokio::join!(
                    self.deployment_evidence(contract, block_number),
                    self.onchain_controllers(contract)
                );
                let supplemental_failed = deployment.is_err() || controllers.is_err();
                let (timestamp, tx_id) = deployment.unwrap_or((0, None));
                (
                    positive(timestamp),
                    tx_id,
                    controllers.unwrap_or_default(),
                    supplemental_failed,
                )
            } else {
                let controllers = self.onchain_controllers(contract).await;
                let supplemental_failed = controllers.is_err();
                (
                    None,
                    None,
                    controllers.unwrap_or_default(),
                    supplemental_failed,
                )
            };
        for field in [
            "contractDeployer",
            "ownerAddress",
            "owner",
            "adminAddress",
            "proxyAdminAddress",
        ] {
            push_address(
                &mut controllers,
                metadata
                    .get(field)
                    .or_else(|| payload.get(field))
                    .and_then(Value::as_str),
            );
        }
        controllers.sort();
        controllers.dedup();
        let deployer = [
            "contractDeployer",
            "deployerAddress",
            "deployer",
            "creatorAddress",
        ]
        .into_iter()
        .find_map(|field| {
            metadata
                .get(field)
                .or_else(|| payload.get(field))
                .and_then(Value::as_str)
                .and_then(normalize_evm_address)
                .map(Arc::from)
        });
        Ok(Metadata {
            deployment_timestamp,
            deployment_block: positive(block_number).map(|value| value as u64),
            deployment_tx,
            deployer,
            authority_status: if supplemental_failed {
                EvidenceStatus::Truncated
            } else if controllers.is_empty() {
                EvidenceStatus::Empty
            } else {
                EvidenceStatus::Complete
            },
            controllers,
        })
    }

    async fn onchain_controllers(&self, contract: &ContractKey) -> Result<Vec<Arc<str>>> {
        const EIP1967_ADMIN_SLOT: &str =
            "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";
        let batch = json!([
            {
                "jsonrpc": "2.0",
                "id": "owner",
                "method": "eth_call",
                "params": [{"to": contract.contract_address.as_ref(), "data": "0x8da5cb5b"}, "latest"]
            },
            {
                "jsonrpc": "2.0",
                "id": "owner-fallback",
                "method": "eth_call",
                "params": [{"to": contract.contract_address.as_ref(), "data": "0x893d20e8"}, "latest"]
            },
            {
                "jsonrpc": "2.0",
                "id": "admin",
                "method": "eth_call",
                "params": [{"to": contract.contract_address.as_ref(), "data": "0xf851a440"}, "latest"]
            },
            {
                "jsonrpc": "2.0",
                "id": "eip1967-admin",
                "method": "eth_getStorageAt",
                "params": [contract.contract_address.as_ref(), EIP1967_ADMIN_SLOT, "latest"]
            }
        ]);
        let payload = self
            .alchemy
            .post(
                self.alchemy_rpc_url(contract.chain)?,
                HeaderMap::new(),
                &batch,
            )
            .await?;
        let rows = payload.as_array().ok_or_else(|| {
            AnalysisError::Provider("Alchemy controller batch response was not an array".into())
        })?;
        let storage_complete = rows.iter().any(|row| {
            row.get("id").and_then(Value::as_str) == Some("eip1967-admin")
                && row.get("result").and_then(Value::as_str).is_some()
        });
        if !storage_complete {
            return Err(AnalysisError::Provider(
                "Alchemy controller batch omitted the EIP-1967 storage result".into(),
            ));
        }
        let mut controllers = Vec::new();
        let mut owner = None;
        let mut owner_fallback = None;
        for row in rows {
            let id = row.get("id").and_then(Value::as_str).unwrap_or_default();
            let Some(address) = abi_address(row.get("result").and_then(Value::as_str)) else {
                continue;
            };
            match id {
                // Prefer Ownable `owner()` (`0x8da5cb5b`); fall back to `getOwner()`
                // (`0x893d20e8`) only when the primary selector is empty.
                "owner" => owner = Some(Arc::from(address)),
                "owner-fallback" => owner_fallback = Some(Arc::from(address)),
                _ => controllers.push(Arc::from(address)),
            }
        }
        if let Some(address) = owner.or(owner_fallback) {
            controllers.push(address);
        }
        controllers.sort();
        controllers.dedup();
        Ok(controllers)
    }

    async fn opensea_metadata(&self, contract: &ContractKey) -> Result<Metadata> {
        let payload = self.cached_opensea_contract(contract).await?;
        let mut controllers = Vec::new();
        for field in [
            "contract_deployer",
            "contractDeployer",
            "deployer",
            "owner_address",
            "ownerAddress",
            "owner",
            "admin_address",
            "adminAddress",
            "proxy_admin_address",
            "proxyAdminAddress",
        ] {
            push_address(&mut controllers, payload.get(field).and_then(Value::as_str));
        }
        controllers.sort();
        controllers.dedup();
        let deployer = ["contract_deployer", "contractDeployer", "deployer"]
            .into_iter()
            .find_map(|field| {
                payload
                    .get(field)
                    .and_then(Value::as_str)
                    .and_then(normalize_evm_address)
                    .map(Arc::from)
            });
        Ok(Metadata {
            deployment_timestamp: positive(integer(
                payload
                    .get("deployed_block_time")
                    .or_else(|| payload.get("deployedBlockTime")),
            )),
            deployment_block: positive(integer(
                payload
                    .get("deployed_block_number")
                    .or_else(|| payload.get("deployedBlockNumber")),
            ))
            .map(|value| value as u64),
            deployment_tx: None,
            deployer,
            authority_status: EvidenceStatus::Truncated,
            controllers,
        })
    }

    async fn cached_opensea_contract(&self, contract: &ContractKey) -> Result<Value> {
        let cell = {
            let mut cache = self.opensea_contract_cache.lock().await;
            if cache.len() >= 1_024 {
                cache.clear();
            }
            cache
                .entry(contract.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        cell.get_or_try_init(|| async {
            let url = reqwest::Url::parse(&format!(
                "{}/api/v2/chain/{}/contract/{}",
                self.endpoints.opensea.trim_end_matches('/'),
                opensea_chain(contract.chain),
                contract.contract_address
            ))
            .map_err(|error| AnalysisError::Provider(error.to_string()))?;
            self.other
                .get(url, opensea_headers(&self.keys.opensea)?)
                .await
        })
        .await
        .cloned()
    }

    async fn deployment_evidence(
        &self,
        contract: &ContractKey,
        block: i64,
    ) -> Result<(i64, Option<Arc<str>>)> {
        let block_tag = format!("0x{block:x}");
        let rpc_url = self.alchemy_rpc_url(contract.chain)?;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [block_tag, false]
        });
        let mut value = self
            .alchemy
            .post(rpc_url.clone(), HeaderMap::new(), &payload)
            .await?;
        let result = value.get_mut("result").ok_or_else(|| {
            AnalysisError::Provider("Alchemy block response omitted result".into())
        })?;
        let timestamp = integer(result.get("timestamp"));
        if timestamp <= 0 {
            return Err(AnalysisError::Provider(
                "Alchemy block response omitted a positive timestamp".into(),
            ));
        }
        let hashes = result
            .get("transactions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|hash| is_transaction_hash(hash))
            .take(self.limit("transactions"))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if hashes.is_empty() {
            return Ok((timestamp, None));
        }
        for method in ["alchemy_getTransactionReceipts", "eth_getBlockReceipts"] {
            // Alchemy's getTransactionReceipts expects `{blockNumber}`; eth_getBlockReceipts
            // takes the block tag string directly.
            let params = if method == "alchemy_getTransactionReceipts" {
                json!([{ "blockNumber": block_tag }])
            } else {
                json!([block_tag])
            };
            let request = json!({
                "jsonrpc": "2.0",
                "id": format!("deployment-{method}"),
                "method": method,
                "params": params
            });
            let Ok(mut payload) = self
                .alchemy
                .post(rpc_url.clone(), HeaderMap::new(), &request)
                .await
            else {
                continue;
            };
            if payload.get("error").is_some() {
                continue;
            }
            let receipts = if method == "alchemy_getTransactionReceipts" {
                payload
                    .get_mut("result")
                    .and_then(|result| result.get_mut("receipts"))
                    .map(Value::take)
            } else {
                payload.get_mut("result").map(Value::take)
            };
            let Some(Value::Array(receipts)) = receipts else {
                continue;
            };
            let target = contract.contract_address.clone();
            let deployment_tx = self
                .alchemy
                .normalize_response(move || Ok(find_deployment_receipt(&receipts, &target)))
                .await?;
            return Ok((timestamp, deployment_tx));
        }
        for chunk in hashes.chunks(100) {
            let request = Value::Array(
                chunk
                    .iter()
                    .map(|hash| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": hash,
                            "method": "eth_getTransactionReceipt",
                            "params": [hash]
                        })
                    })
                    .collect(),
            );
            let Ok(payload) = self
                .alchemy
                .post(rpc_url.clone(), HeaderMap::new(), &request)
                .await
            else {
                continue;
            };
            let receipts = match payload {
                Value::Array(receipts) => receipts,
                payload => vec![payload],
            };
            let target = contract.contract_address.clone();
            let deployment_tx = self
                .alchemy
                .normalize_response(move || Ok(find_deployment_receipt(&receipts, &target)))
                .await?;
            if deployment_tx.is_some() {
                return Ok((timestamp, deployment_tx));
            }
        }
        Ok((timestamp, None))
    }

    async fn fetch_transfers(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Outcome<Vec<NormalizedEvent>> {
        if self.keys.alchemy.is_empty() && self.keys.etherscan.is_empty() {
            return Outcome::skipped("nft_transfers");
        }
        if !self.keys.alchemy.is_empty() {
            match self.alchemy_transfers(candidate, selection).await {
                Ok((events, truncated)) => {
                    let empty = events.is_empty();
                    return Outcome::complete(events, empty, truncated, "alchemy", "nft_transfers");
                }
                Err(error) if self.keys.etherscan.is_empty() => {
                    return Outcome::failed("alchemy", "nft_transfers", error);
                }
                Err(primary_error) => {
                    return match self.etherscan_transfers(candidate, selection).await {
                        Ok((events, truncated)) => {
                            let empty = events.is_empty();
                            Outcome::complete(
                                events,
                                empty,
                                truncated,
                                "etherscan",
                                "nft_transfers",
                            )
                            .with_failure(primary_error)
                        }
                        Err(fallback_error) => Outcome::failed(
                            "alchemy+etherscan",
                            "nft_transfers",
                            AnalysisError::Provider(format!(
                                "Alchemy failed: {primary_error}; Etherscan fallback failed: {fallback_error}"
                            )),
                        ),
                    };
                }
            }
        }
        match self.etherscan_transfers(candidate, selection).await {
            Ok((events, truncated)) => {
                let empty = events.is_empty();
                Outcome::complete(events, empty, truncated, "etherscan", "nft_transfers")
            }
            Err(error) => Outcome::failed("etherscan", "nft_transfers", error),
        }
    }

    async fn alchemy_transfers(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let cap = self.limit("history");
        let mut page_key: Option<String> = None;
        let mut seen = BTreeSet::new();
        let mut events = Vec::new();
        loop {
            let mut params = json!({
                "fromBlock": "0x0",
                "toBlock": "latest",
                "category": ["erc721", "erc1155"],
                "contractAddresses": [candidate.contract_address.as_ref()],
                "withMetadata": true,
                "excludeZeroValue": false,
                "maxCount": "0x3e8",
                "order": "asc"
            });
            if let Some(cursor) = &page_key {
                params["pageKey"] = Value::String(cursor.clone());
            }
            let payload = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "alchemy_getAssetTransfers",
                "params": [params]
            });
            let mut body = self
                .alchemy
                .post(
                    self.alchemy_rpc_url(candidate.chain)?,
                    HeaderMap::new(),
                    &payload,
                )
                .await?;
            if let Some(error) = body.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Alchemy transfer request failed: {error}"
                )));
            }
            let next = text(body.get("result").and_then(|result| result.get("pageKey"))).to_owned();
            let owned_transfers = match body
                .get_mut("result")
                .and_then(|result| result.get_mut("transfers"))
                .map(Value::take)
            {
                Some(Value::Array(transfers)) => transfers,
                _ => {
                    return Err(AnalysisError::Provider(
                        "Alchemy transfer response omitted result.transfers".into(),
                    ));
                }
            };
            let owned_candidate = candidate.clone();
            let owned_selection = selection.clone();
            let remaining = cap.saturating_sub(events.len());
            let probe_limit = remaining.saturating_add(1);
            let normalized = self
                .alchemy
                .normalize_response(move || {
                    Ok(normalize_transfer_page(
                        &owned_candidate,
                        &owned_selection,
                        &owned_transfers,
                        probe_limit,
                        "alchemy",
                    ))
                })
                .await?;
            let page_overflow = normalized.len() > remaining;
            events.extend(normalized.into_iter().take(remaining));
            if events.len() >= cap {
                return Ok((events, page_overflow || !next.is_empty()));
            }
            if next.is_empty() {
                return Ok((events, false));
            }
            if !seen.insert(next.clone()) {
                return Err(AnalysisError::Provider(
                    "Alchemy transfer pagination repeated pageKey".into(),
                ));
            }
            page_key = Some(next);
        }
    }

    async fn etherscan_transfers(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let cap = self.limit("history");
        let results = tokio::join!(
            self.etherscan_transfer_action(candidate, selection, "tokennfttx", cap),
            self.etherscan_transfer_action(candidate, selection, "token1155tx", cap),
        );
        let mut events = Vec::new();
        let mut truncated = false;
        for result in [results.0, results.1] {
            let (mut action_events, action_truncated) = result?;
            events.append(&mut action_events);
            truncated |= action_truncated;
        }
        events.sort_by(|left, right| {
            (
                &left.tx_id,
                left.event_index,
                &left.nft,
                &left.from,
                &left.to,
            )
                .cmp(&(
                    &right.tx_id,
                    right.event_index,
                    &right.nft,
                    &right.from,
                    &right.to,
                ))
        });
        events.dedup();
        if events.len() > cap {
            events.truncate(cap);
            truncated = true;
        }
        Ok((events, truncated))
    }

    async fn etherscan_transfer_action(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
        action: &'static str,
        cap: usize,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let mut events = Vec::new();
        let mut page = 1_usize;
        loop {
            let mut url = reqwest::Url::parse(&self.endpoints.etherscan)
                .map_err(|error| AnalysisError::Provider(error.to_string()))?;
            url.query_pairs_mut()
                .append_pair("chainid", chain_id(candidate.chain))
                .append_pair("module", "account")
                .append_pair("action", action)
                .append_pair("contractaddress", &candidate.contract_address)
                .append_pair("page", &page.to_string())
                .append_pair("offset", "1000")
                .append_pair("startblock", "0")
                .append_pair("endblock", "9999999999")
                .append_pair("sort", "asc")
                .append_pair("apikey", &self.keys.etherscan);
            let body = self.other.get(url, HeaderMap::new()).await?;
            let items = match body.get("result").and_then(Value::as_array) {
                Some(items) => items,
                None if text(body.get("message"))
                    .to_ascii_lowercase()
                    .contains("no transactions") =>
                {
                    return Ok((events, false));
                }
                None => {
                    return Err(AnalysisError::Provider(format!(
                        "Etherscan {action} response failed: {}",
                        text(body.get("message"))
                    )));
                }
            };
            let mut page_overflow = false;
            for item in items {
                let token_id = normalize_token_id(
                    item.get("tokenID")
                        .or_else(|| item.get("tokenId"))
                        .unwrap_or(&Value::Null),
                );
                if !selected(selection, &token_id) {
                    continue;
                }
                if events.len() < cap {
                    events.push(transfer_event(candidate, item, token_id, "etherscan"));
                } else {
                    page_overflow = true;
                }
            }
            if events.len() >= cap {
                // Etherscan has no next-page token. A full 1,000-row page may
                // have a successor, while a short page is definitively final.
                return Ok((events, page_overflow || items.len() == 1000));
            }
            if items.len() < 1000 {
                return Ok((events, false));
            }
            page += 1;
        }
    }

    async fn enrich_receipts(
        &self,
        candidate: &ContractKey,
        events: &mut [NormalizedEvent],
    ) -> Outcome<ReceiptCoverage> {
        if self.keys.alchemy.is_empty() {
            return Outcome::skipped("transaction_receipts");
        }
        let cap = self.limit("transactions");
        let all_hashes = events
            .iter()
            .filter(|event| event.kind != EventKind::Listing)
            .map(|event| event.tx_id.as_ref())
            .filter(|hash| is_transaction_hash(hash))
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        if all_hashes.is_empty() {
            let requires_receipt = events.iter().any(|event| {
                matches!(
                    event.kind,
                    EventKind::Deploy
                        | EventKind::Mint
                        | EventKind::Sale
                        | EventKind::Funding
                        | EventKind::Withdrawal
                        | EventKind::Cashout
                )
            });
            return Outcome::complete(
                ReceiptCoverage::default(),
                !requires_receipt,
                requires_receipt,
                "alchemy",
                "transaction_receipts",
            );
        }
        let truncated = all_hashes.len() > cap;
        let hashes = all_hashes.into_iter().take(cap).collect::<Vec<_>>();
        let mut receipts = HashMap::<String, ReceiptData>::new();
        let mut failed = 0_u64;
        let rpc_url = match self.alchemy_rpc_url(candidate.chain) {
            Ok(url) => url,
            Err(error) => {
                return Outcome::failed("alchemy", "transaction_receipts", error);
            }
        };
        for chunk in hashes.chunks(100) {
            let body = Value::Array(
                chunk
                    .iter()
                    .map(|hash| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": hash,
                            "method": "eth_getTransactionReceipt",
                            "params": [hash]
                        })
                    })
                    .collect(),
            );
            let payload = match self
                .alchemy
                .post(rpc_url.clone(), HeaderMap::new(), &body)
                .await
            {
                Ok(payload) => payload,
                Err(error) => {
                    return Outcome::failed(
                        "alchemy",
                        "transaction_receipts",
                        AnalysisError::Provider(format!(
                            "receipt batch failed after {} successful receipts: {error}",
                            receipts.len()
                        )),
                    );
                }
            };
            let rows = match payload {
                Value::Array(rows) => rows,
                payload => vec![payload],
            };
            let by_id = rows
                .iter()
                .filter_map(|row| row.get("id").and_then(Value::as_str).map(|id| (id, row)))
                .collect::<HashMap<_, _>>();
            for hash in chunk {
                let Some(row) = by_id.get(hash.as_str()) else {
                    failed = failed.saturating_add(1);
                    continue;
                };
                let Some(result) = row.get("result").filter(|result| !result.is_null()) else {
                    failed = failed.saturating_add(1);
                    continue;
                };
                let gas_used = result
                    .get("gasUsed")
                    .and_then(Value::as_str)
                    .and_then(parse_hex_i128);
                let gas_price = result
                    .get("effectiveGasPrice")
                    .or_else(|| result.get("gasPrice"))
                    .and_then(Value::as_str)
                    .and_then(parse_hex_i128);
                receipts.insert(
                    hash.clone(),
                    ReceiptData {
                        from: address(result, &["from"]),
                        gas_native: gas_used
                            .zip(gas_price)
                            .and_then(|(used, price)| used.checked_mul(price)),
                    },
                );
            }
        }
        for event in events {
            let Some(receipt) = receipts.get(event.tx_id.as_ref()) else {
                continue;
            };
            if event.fee_payer.is_none() {
                event.fee_payer.clone_from(&receipt.from);
            }
            event.gas_native = receipt.gas_native;
        }
        let coverage = ReceiptCoverage {
            fetched: receipts.len() as u64,
            provider_reported: hashes.len() as u64,
            failed,
        };
        Outcome::complete(
            coverage,
            false,
            truncated || failed > 0,
            "alchemy",
            "transaction_receipts",
        )
    }

    async fn fetch_value_flows(&self, candidate: &ContractKey) -> Outcome<Vec<NormalizedEvent>> {
        if self.keys.alchemy.is_empty() {
            return Outcome::skipped("contract_value_flows");
        }
        let (incoming, outgoing) = tokio::join!(
            self.alchemy_value_transfers(candidate, FlowDirection::Incoming),
            self.alchemy_value_transfers(candidate, FlowDirection::Outgoing),
        );
        let mut events = Vec::new();
        let mut truncated = false;
        let mut failures = Vec::new();
        for result in [incoming, outgoing] {
            match result {
                Ok((mut flow_events, flow_truncated)) => {
                    events.append(&mut flow_events);
                    truncated |= flow_truncated;
                }
                Err(error) => failures.push(error),
            }
        }
        if !events.is_empty() {
            match self.alchemy_cashout_transfers(candidate, &events).await {
                Ok((mut cashout_events, cashout_truncated)) => {
                    events.append(&mut cashout_events);
                    truncated |= cashout_truncated;
                }
                Err(error) => {
                    truncated = true;
                    failures.push(error);
                }
            }
        }
        events.sort_by(|left, right| {
            (
                &left.tx_id,
                left.kind.as_str(),
                &left.from,
                &left.to,
                left.native_amount,
                left.usd_micros,
            )
                .cmp(&(
                    &right.tx_id,
                    right.kind.as_str(),
                    &right.from,
                    &right.to,
                    right.native_amount,
                    right.usd_micros,
                ))
        });
        events.dedup();
        if events.is_empty() && failures.len() >= 2 {
            return Outcome::failed(
                "alchemy",
                "contract_value_flows",
                AnalysisError::Provider(
                    failures
                        .into_iter()
                        .map(|error| error.to_string())
                        .collect::<Vec<_>>()
                        .join("; "),
                ),
            );
        }
        let empty = events.is_empty();
        let outcome = Outcome::complete(
            events,
            empty,
            truncated || !failures.is_empty(),
            "alchemy",
            "contract_value_flows",
        );
        if let Some(error) = failures.into_iter().next() {
            outcome.with_failure(error)
        } else {
            outcome
        }
    }

    async fn alchemy_cashout_transfers(
        &self,
        candidate: &ContractKey,
        events: &[NormalizedEvent],
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let cap = self.limit("transactions");
        let withdrawals = events
            .iter()
            .filter(|event| event.kind == EventKind::Withdrawal)
            .filter_map(|event| {
                Some((
                    event.to.clone()?,
                    event.block_number?,
                    event.native_amount,
                    event.usd_micros,
                ))
            })
            .collect::<BTreeSet<_>>();
        if withdrawals.is_empty() {
            return Ok((Vec::new(), false));
        }
        let mut output = Vec::new();
        let mut truncated = withdrawals.len() > cap;
        let lookups = withdrawals.into_iter().take(cap).collect::<Vec<_>>();
        for (batch_index, chunk) in lookups.chunks(100).enumerate() {
            let body = Value::Array(
                chunk
                    .iter()
                    .enumerate()
                    .map(|(index, (address, block, _, _))| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": format!("cashout-{}", batch_index * 100 + index),
                            "method": "alchemy_getAssetTransfers",
                            "params": [{
                                "fromBlock": format!("0x{block:x}"),
                                "toBlock": format!("0x{block:x}"),
                                "fromAddress": address.as_ref(),
                                "category": ["external", "internal", "erc20"],
                                "withMetadata": true,
                                "excludeZeroValue": true,
                                "maxCount": "0x3e8",
                                "order": "asc"
                            }]
                        })
                    })
                    .collect(),
            );
            let payload = self
                .alchemy
                .post(
                    self.alchemy_rpc_url(candidate.chain)?,
                    HeaderMap::new(),
                    &body,
                )
                .await?;
            let rows = match payload {
                Value::Array(rows) => rows,
                payload => vec![payload],
            };
            let owned_candidate = candidate.clone();
            let owned_chunk = chunk.to_vec();
            let remaining = cap.saturating_sub(output.len());
            let (normalized, batch_truncated) = self
                .alchemy
                .normalize_response(move || {
                    Ok(normalize_cashout_batch(
                        &owned_candidate,
                        &owned_chunk,
                        &rows,
                        batch_index,
                        remaining,
                    ))
                })
                .await?;
            output.extend(normalized);
            truncated |= batch_truncated;
            if output.len() >= cap {
                return Ok((output, true));
            }
        }
        Ok((output, truncated))
    }

    async fn alchemy_value_transfers(
        &self,
        candidate: &ContractKey,
        direction: FlowDirection,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let cap = self.limit("transactions");
        let mut page_key: Option<String> = None;
        let mut seen = BTreeSet::new();
        let mut events = Vec::new();
        loop {
            let mut params = json!({
                "fromBlock": "0x0",
                "toBlock": "latest",
                "category": ["external", "internal", "erc20"],
                "withMetadata": true,
                "excludeZeroValue": true,
                "maxCount": "0x3e8",
                "order": "asc"
            });
            params[direction.address_field()] =
                Value::String(candidate.contract_address.to_string());
            if let Some(cursor) = &page_key {
                params["pageKey"] = Value::String(cursor.clone());
            }
            let payload = json!({
                "jsonrpc": "2.0",
                "id": format!("value-{}", direction.as_str()),
                "method": "alchemy_getAssetTransfers",
                "params": [params]
            });
            let mut body = self
                .alchemy
                .post(
                    self.alchemy_rpc_url(candidate.chain)?,
                    HeaderMap::new(),
                    &payload,
                )
                .await?;
            if let Some(error) = body.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Alchemy {} value-flow request failed: {error}",
                    direction.as_str()
                )));
            }
            let next = text(body.get("result").and_then(|result| result.get("pageKey"))).to_owned();
            let owned_transfers = match body
                .get_mut("result")
                .and_then(|result| result.get_mut("transfers"))
                .map(Value::take)
            {
                Some(Value::Array(transfers)) => transfers,
                _ => {
                    return Err(AnalysisError::Provider(
                        "Alchemy value-flow response omitted result.transfers".into(),
                    ));
                }
            };
            let owned_candidate = candidate.clone();
            let remaining = cap.saturating_sub(events.len());
            let normalized = self
                .alchemy
                .normalize_response(move || {
                    Ok(normalize_value_flow_page(
                        &owned_candidate,
                        &owned_transfers,
                        direction,
                        remaining,
                    ))
                })
                .await?;
            events.extend(normalized);
            if events.len() >= cap {
                return Ok((events, true));
            }
            if next.is_empty() {
                return Ok((events, false));
            }
            if !seen.insert(next.clone()) {
                return Err(AnalysisError::Provider(
                    "Alchemy value-flow pagination repeated pageKey".into(),
                ));
            }
            page_key = Some(next);
        }
    }

    async fn fetch_holders(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Outcome<Vec<(NftKey, Arc<str>)>> {
        if self.keys.alchemy.is_empty() {
            return Outcome::skipped("current_holders");
        }
        match self.alchemy_holders(candidate, selection).await {
            Ok((holders, truncated)) => {
                let empty = holders.is_empty();
                Outcome::complete(holders, empty, truncated, "alchemy", "current_holders")
            }
            Err(error) => Outcome::failed("alchemy", "current_holders", error),
        }
    }

    async fn alchemy_holders(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Result<(Vec<(NftKey, Arc<str>)>, bool)> {
        let cap = self.limit("assets");
        let mut url = self.alchemy_nft_url(candidate.chain, "getOwnersForContract")?;
        url.query_pairs_mut()
            .append_pair("contractAddress", &candidate.contract_address)
            .append_pair("withTokenBalances", "true");
        let mut page_key: Option<String> = None;
        let mut seen = BTreeSet::new();
        let mut holders = Vec::new();
        loop {
            let mut request_url = url.clone();
            if let Some(cursor) = &page_key {
                request_url.query_pairs_mut().append_pair("pageKey", cursor);
            }
            let mut body = self.alchemy.get(request_url, HeaderMap::new()).await?;
            if let Some(error) = body.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Alchemy owner request failed: {error}"
                )));
            }
            let next = text(body.get("pageKey")).to_owned();
            let owned_owners = match body.get_mut("owners").map(Value::take) {
                Some(Value::Array(owners)) => owners,
                _ => {
                    return Err(AnalysisError::Provider(
                        "Alchemy owner response omitted owners".into(),
                    ));
                }
            };
            let owned_candidate = candidate.clone();
            let owned_selection = selection.clone();
            let remaining = cap.saturating_sub(holders.len());
            let probe_limit = remaining.saturating_add(1);
            let normalized = self
                .alchemy
                .normalize_response(move || {
                    Ok(normalize_holder_page(
                        &owned_candidate,
                        &owned_selection,
                        &owned_owners,
                        probe_limit,
                    ))
                })
                .await?;
            let page_overflow = normalized.len() > remaining;
            holders.extend(normalized.into_iter().take(remaining));
            if holders.len() >= cap {
                return Ok((holders, page_overflow || !next.is_empty()));
            }
            if next.is_empty() {
                return Ok((holders, false));
            }
            if !seen.insert(next.clone()) {
                return Err(AnalysisError::Provider(
                    "Alchemy owner pagination repeated pageKey".into(),
                ));
            }
            page_key = Some(next);
        }
    }

    async fn fetch_sales(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Outcome<Vec<NormalizedEvent>> {
        if self.keys.opensea.is_empty() {
            return Outcome::skipped("nft_sales");
        }
        match self.opensea_sales(candidate, selection).await {
            Ok((events, truncated)) => {
                let empty = events.is_empty();
                Outcome::complete(events, empty, truncated, "opensea", "nft_sales")
            }
            Err(error) => Outcome::failed("opensea", "nft_sales", error),
        }
    }

    async fn opensea_sales(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        self.opensea_market_events(candidate, selection).await
    }

    async fn opensea_market_events(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let headers = opensea_headers(&self.keys.opensea)?;
        let metadata = self.cached_opensea_contract(candidate).await?;
        let slug = opensea_collection_slug(&metadata);
        if slug.is_empty() {
            return Ok((Vec::new(), false));
        }
        let cap = self.limit("transactions");
        let mut base = reqwest::Url::parse(&format!(
            "{}/api/v2/events/collection/{slug}",
            self.endpoints.opensea.trim_end_matches('/')
        ))
        .map_err(|error| AnalysisError::Provider(error.to_string()))?;
        base.query_pairs_mut()
            .append_pair("event_type", "sale")
            .append_pair("limit", "200");
        let mut cursor: Option<String> = None;
        let mut seen = BTreeSet::new();
        let mut events = Vec::new();
        loop {
            let mut url = base.clone();
            if let Some(next) = &cursor {
                url.query_pairs_mut().append_pair("next", next);
            }
            let body = self.other.get(url, headers.clone()).await?;
            let mut page_overflow = false;
            for market_event in body
                .get("asset_events")
                .or_else(|| body.get("events"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let event_type = text(
                    market_event
                        .get("event_type")
                        .or_else(|| market_event.get("eventType")),
                );
                if !event_type.is_empty() && !event_type.eq_ignore_ascii_case("sale") {
                    continue;
                }
                let nft = market_event
                    .get("nft")
                    .or_else(|| market_event.get("asset"))
                    .unwrap_or(&Value::Null);
                let event_contract = nft
                    .get("contract")
                    .or_else(|| nft.get("contract_address"))
                    .or_else(|| nft.get("asset_contract"))
                    .or_else(|| market_event.get("asset_contract_address"))
                    .and_then(address_like);
                if event_contract.is_some_and(|address| {
                    !address
                        .trim()
                        .eq_ignore_ascii_case(&candidate.contract_address)
                }) {
                    continue;
                }
                let token_id = normalize_token_id(
                    nft.get("identifier")
                        .or_else(|| nft.get("token_id"))
                        .unwrap_or(&Value::Null),
                );
                if token_id.is_empty() || !selected(selection, &token_id) {
                    continue;
                }
                if events.len() < cap {
                    events.push(sale_event(candidate, market_event, token_id));
                } else {
                    page_overflow = true;
                }
            }
            let next = text(body.get("next"));
            if events.len() >= cap {
                return Ok((events, page_overflow || !next.is_empty()));
            }
            if next.is_empty() {
                return Ok((events, false));
            }
            if !seen.insert(next.to_owned()) {
                return Err(AnalysisError::Provider(
                    "OpenSea sales pagination repeated cursor".into(),
                ));
            }
            cursor = Some(next.to_owned());
        }
    }

    async fn relation_verifications(
        &self,
        relations: &[SeedCandidateRelation],
        candidate_controllers: &[Arc<str>],
        candidate_authority_complete: bool,
    ) -> Vec<RelationVerification> {
        stream::iter(relations.iter().cloned())
            .map(|relation| async move {
                let seed = relation_seed_contract(&relation);
                let lookup = match seed {
                    Some(seed) => self.cached_metadata(seed).await,
                    None => Err(AnalysisError::Provider(
                        "relation does not expose a seed contract identity".into(),
                    )),
                };
                match lookup {
                    Ok(metadata) => {
                        let continuity = metadata.controllers.iter().any(|seed_controller| {
                            candidate_controllers
                                .iter()
                                .any(|candidate| candidate == seed_controller)
                        });
                        RelationVerification {
                            seed_id: relation.seed_id,
                            official_controller_continuity: continuity,
                            authorized_reissue: false,
                            verified_migration: false,
                            official_collection_relation: false,
                            complete: candidate_authority_complete
                                && !candidate_controllers.is_empty()
                                && !metadata.controllers.is_empty()
                                && matches!(
                                    metadata.authority_status,
                                    EvidenceStatus::Complete | EvidenceStatus::Empty
                                ),
                            evidence_keys: if continuity {
                                vec![Arc::from("controller_continuity")]
                            } else {
                                Vec::new()
                            },
                            failures: Vec::new(),
                        }
                    }
                    Err(error) => RelationVerification {
                        seed_id: relation.seed_id,
                        official_controller_continuity: false,
                        authorized_reissue: false,
                        verified_migration: false,
                        official_collection_relation: false,
                        complete: false,
                        evidence_keys: Vec::new(),
                        failures: vec![format!("seed_authority_lookup: {error}")],
                    },
                }
            })
            .buffer_unordered(8)
            .collect()
            .await
    }

    async fn cached_metadata(&self, contract: ContractKey) -> Result<Metadata> {
        let cell = {
            let mut cache = self.controller_cache.lock().await;
            cache
                .entry(contract.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        cell.get_or_try_init(|| async {
            self.metadata_with_fallback(&contract)
                .await
                .map(|(metadata, _, _)| metadata)
        })
        .await
        .cloned()
    }

    fn alchemy_nft_url(&self, chain: ChainId, method: &str) -> Result<reqwest::Url> {
        let network = self.network(chain)?;
        reqwest::Url::parse(&format!(
            "https://{network}.g.alchemy.com/nft/v3/{}/{method}",
            self.keys.alchemy
        ))
        .map_err(|_| AnalysisError::Config("invalid Alchemy network or API key".into()))
    }

    fn alchemy_rpc_url(&self, chain: ChainId) -> Result<reqwest::Url> {
        let network = self.network(chain)?;
        reqwest::Url::parse(&format!(
            "https://{network}.g.alchemy.com/v2/{}",
            self.keys.alchemy
        ))
        .map_err(|_| AnalysisError::Config("invalid Alchemy network or API key".into()))
    }

    fn network(&self, chain: ChainId) -> Result<&str> {
        self.endpoints
            .alchemy_networks
            .get(chain.as_str())
            .map(String::as_str)
            .ok_or_else(|| {
                AnalysisError::Config(format!(
                    "provider_endpoints.alchemy_networks is missing {}",
                    chain.as_str()
                ))
            })
    }

    fn limit(&self, key: &str) -> usize {
        self.page_limits.get(key).copied().unwrap_or(1_000).max(1)
    }
}

fn normalize_value_flow_page(
    candidate: &ContractKey,
    items: &[Value],
    direction: FlowDirection,
    limit: usize,
) -> Vec<NormalizedEvent> {
    if limit == 0 {
        return Vec::new();
    }
    items
        .iter()
        .filter_map(|item| value_flow_event(candidate, item, direction))
        .take(limit)
        .collect()
}

fn find_deployment_receipt(receipts: &[Value], target: &str) -> Option<Arc<str>> {
    receipts.iter().find_map(|row| {
        let receipt = row.get("result").unwrap_or(row);
        let address = receipt.get("contractAddress").and_then(Value::as_str)?;
        if !address.eq_ignore_ascii_case(target) {
            return None;
        }
        receipt
            .get("transactionHash")
            .and_then(Value::as_str)
            .or_else(|| row.get("id").and_then(Value::as_str))
            .filter(|hash| is_transaction_hash(hash))
            .map(Arc::from)
    })
}

fn normalize_holder_page(
    candidate: &ContractKey,
    selection: &NftSelection,
    owners: &[Value],
    limit: usize,
) -> Vec<(NftKey, Arc<str>)> {
    if limit == 0 {
        return Vec::new();
    }
    let mut holders = Vec::new();
    for owner in owners {
        let address = text(owner.get("ownerAddress"));
        if address.is_empty() || address.eq_ignore_ascii_case(ZERO_ADDRESS) {
            continue;
        }
        for balance in owner
            .get("tokenBalances")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if integer(balance.get("balance")) <= 0 {
                continue;
            }
            let token_id = normalize_token_id(balance.get("tokenId").unwrap_or(&Value::Null));
            if token_id.is_empty() || !selected(selection, &token_id) {
                continue;
            }
            holders.push((
                NftKey {
                    chain: candidate.chain,
                    contract_address: candidate.contract_address.clone(),
                    token_id: Arc::from(token_id),
                },
                Arc::from(address.to_ascii_lowercase()),
            ));
            if holders.len() >= limit {
                return holders;
            }
        }
    }
    holders
}

fn normalize_cashout_batch(
    candidate: &ContractKey,
    lookups: &[CashoutLookup],
    rows: &[Value],
    batch_index: usize,
    limit: usize,
) -> (Vec<NormalizedEvent>, bool) {
    if limit == 0 {
        return (Vec::new(), true);
    }
    let by_id = rows
        .iter()
        .filter_map(|row| row.get("id").and_then(Value::as_str).map(|id| (id, row)))
        .collect::<HashMap<_, _>>();
    let mut output = Vec::new();
    let mut truncated = false;
    for (index, (_, _, withdrawn_native, withdrawn_usd)) in lookups.iter().enumerate() {
        let id = format!("cashout-{}", batch_index * 100 + index);
        let Some(result) = by_id.get(id.as_str()).and_then(|row| row.get("result")) else {
            truncated = true;
            continue;
        };
        let Some(transfers) = result.get("transfers").and_then(Value::as_array) else {
            truncated = true;
            continue;
        };
        truncated |= !text(result.get("pageKey")).is_empty();
        for item in transfers {
            let Some(event) = value_flow_event(candidate, item, FlowDirection::Cashout) else {
                continue;
            };
            if flow_amount_compatible(
                *withdrawn_native,
                *withdrawn_usd,
                event.native_amount,
                event.usd_micros,
            ) {
                output.push(event);
                if output.len() >= limit {
                    return (output, true);
                }
            }
        }
    }
    (output, truncated)
}

fn value_flow_event(
    candidate: &ContractKey,
    item: &Value,
    direction: FlowDirection,
) -> Option<NormalizedEvent> {
    let from = address(item, &["from", "from_address"])?;
    let to = address(item, &["to", "to_address"])?;
    if from == to {
        return None;
    }
    let symbol = text(item.get("asset")).to_ascii_uppercase();
    let category = text(item.get("category")).to_ascii_lowercase();
    let raw = item
        .get("rawContract")
        .and_then(|value| value.get("value"))
        .and_then(Value::as_str)
        .and_then(parse_hex_i128);
    let (native_amount, usd_micros) = if matches!(category.as_str(), "external" | "internal")
        || is_native_symbol(candidate.chain, &symbol)
    {
        (
            raw.or_else(|| decimal_to_units(item.get("value")?, 18)),
            None,
        )
    } else if is_stablecoin(&symbol) {
        (None, decimal_to_units(item.get("value")?, 6))
    } else {
        return None;
    };
    if native_amount == Some(0) || usd_micros == Some(0) {
        return None;
    }
    let tx_id = transaction_id(item.get("hash").or_else(|| item.get("transactionHash")));
    if tx_id.is_empty() {
        return None;
    }
    let (kind, channel) = match direction {
        FlowDirection::Incoming => (EventKind::Funding, ValueChannel::Funding),
        FlowDirection::Outgoing => (EventKind::Withdrawal, ValueChannel::Withdrawal),
        FlowDirection::Cashout => (EventKind::Cashout, ValueChannel::CashoutHop),
    };
    Some(NormalizedEvent {
        chain: candidate.chain,
        tx_id: Arc::from(tx_id),
        event_index: integer(item.get("uniqueId").or_else(|| item.get("logIndex"))).max(0) as u32,
        timestamp: timestamp(
            item.get("metadata")
                .and_then(|metadata| metadata.get("blockTimestamp"))
                .or_else(|| item.get("timeStamp")),
        ),
        block_number: positive(integer(
            item.get("blockNum").or_else(|| item.get("blockNumber")),
        ))
        .map(|value| value as u64),
        kind,
        channel: Some(channel),
        from: Some(from),
        to: Some(to),
        fee_payer: None,
        payment_payer: None,
        payment_recipient: None,
        nft: None,
        native_amount,
        usd_micros,
        gas_native: None,
        gas_usd_micros: None,
        marketplace_fee_native: None,
        marketplace_fee_usd_micros: None,
    })
}

fn flow_amount_compatible(
    withdrawn_native: Option<i128>,
    withdrawn_usd: Option<i128>,
    cashout_native: Option<i128>,
    cashout_usd: Option<i128>,
) -> bool {
    fn within(source: i128, next: i128) -> bool {
        source > 0
            && next > 0
            && next
                .checked_mul(100)
                .zip(source.checked_mul(110))
                .is_some_and(|(next, source)| next <= source)
    }
    withdrawn_native
        .zip(cashout_native)
        .is_some_and(|(source, next)| within(source, next))
        || withdrawn_usd
            .zip(cashout_usd)
            .is_some_and(|(source, next)| within(source, next))
}

fn deployment_event(
    candidate: &ContractKey,
    metadata: &Metadata,
    deployer: Arc<str>,
) -> NormalizedEvent {
    NormalizedEvent {
        chain: candidate.chain,
        tx_id: metadata.deployment_tx.clone().unwrap_or_else(|| {
            Arc::from(format!(
                "deployment:{}:{}",
                candidate.contract_address,
                metadata.deployment_block.unwrap_or_default()
            ))
        }),
        event_index: 0,
        timestamp: metadata.deployment_timestamp,
        block_number: metadata.deployment_block,
        kind: EventKind::Deploy,
        channel: Some(ValueChannel::Deployment),
        from: Some(deployer.clone()),
        to: Some(candidate.contract_address.clone()),
        fee_payer: Some(deployer),
        payment_payer: None,
        payment_recipient: None,
        nft: None,
        native_amount: None,
        usd_micros: None,
        gas_native: None,
        gas_usd_micros: None,
        marketplace_fee_native: None,
        marketplace_fee_usd_micros: None,
    }
}

fn normalize_transfer_page(
    candidate: &ContractKey,
    selection: &NftSelection,
    items: &[Value],
    limit: usize,
    source: &str,
) -> Vec<NormalizedEvent> {
    if limit == 0 {
        return Vec::new();
    }
    let mut events = Vec::new();
    for item in items {
        for token_id in transfer_token_ids(item) {
            if !selected(selection, &token_id) {
                continue;
            }
            events.push(transfer_event(candidate, item, token_id, source));
            if events.len() >= limit {
                return events;
            }
        }
    }
    events
}

fn transfer_event(
    candidate: &ContractKey,
    item: &Value,
    token_id: String,
    source: &str,
) -> NormalizedEvent {
    let from = address(item, &["from", "from_address"]);
    let to = address(item, &["to", "to_address"]);
    let mint = from
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case(ZERO_ADDRESS));
    let tx_id = transaction_id(
        item.get("hash")
            .or_else(|| item.get("tx_hash"))
            .or_else(|| item.get("transactionHash")),
    );
    NormalizedEvent {
        chain: candidate.chain,
        tx_id: Arc::from(if tx_id.is_empty() {
            format!(
                "{source}:transfer:{}:{}:{token_id}:{}:{}",
                integer(item.get("blockNum").or_else(|| item.get("blockNumber"))),
                integer(
                    item.get("logIndex")
                        .or_else(|| item.get("transactionIndex"))
                ),
                from.as_deref().unwrap_or(""),
                to.as_deref().unwrap_or("")
            )
        } else {
            tx_id
        }),
        event_index: integer(
            item.get("logIndex")
                .or_else(|| item.get("transactionIndex")),
        )
        .max(0) as u32,
        timestamp: timestamp(
            item.get("timeStamp")
                .or_else(|| item.get("block_time"))
                .or_else(|| {
                    item.get("metadata")
                        .and_then(|metadata| metadata.get("blockTimestamp"))
                }),
        ),
        block_number: positive(integer(
            item.get("blockNum").or_else(|| item.get("blockNumber")),
        ))
        .map(|value| value as u64),
        kind: if mint {
            EventKind::Mint
        } else {
            EventKind::Transfer
        },
        channel: None,
        from,
        to,
        fee_payer: None,
        payment_payer: None,
        payment_recipient: None,
        nft: Some(NftKey {
            chain: candidate.chain,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from(token_id),
        }),
        native_amount: None,
        usd_micros: None,
        gas_native: None,
        gas_usd_micros: None,
        marketplace_fee_native: None,
        marketplace_fee_usd_micros: None,
    }
}

fn sale_event(candidate: &ContractKey, item: &Value, token_id: String) -> NormalizedEvent {
    let seller_fee = item.get("sellerFee");
    let seller_payment = item
        .get("payment")
        .or_else(|| item.get("payment_token"))
        .unwrap_or(&Value::Null);
    let (native_amount, usd_micros) = seller_fee
        .map(|fee| payment_amount(candidate.chain, fee))
        .unwrap_or_else(|| marketplace_payment_amount(candidate.chain, item, seller_payment));
    let (marketplace_fee_native, marketplace_fee_usd_micros) = item
        .get("protocolFee")
        .map(|fee| payment_amount(candidate.chain, fee))
        .unwrap_or((None, None));
    let tx_id = transaction_id(
        item.get("transactionHash")
            .or_else(|| item.get("transaction_hash"))
            .or_else(|| item.get("order_hash"))
            .or_else(|| item.get("transaction")),
    );
    let buyer = address(
        item,
        &[
            "buyerAddress",
            "buyer_address",
            "buyer",
            "to_account",
            "winner_account",
        ],
    );
    let seller = address(
        item,
        &["sellerAddress", "seller_address", "seller", "from_account"],
    );
    NormalizedEvent {
        chain: candidate.chain,
        tx_id: Arc::from(if tx_id.is_empty() {
            format!(
                "opensea:sale:{token_id}:{}:{}:{}:{}",
                integer(item.get("blockNumber")),
                timestamp(
                    item.get("event_timestamp")
                        .or_else(|| item.get("timestamp"))
                )
                .unwrap_or_default(),
                buyer.as_deref().unwrap_or(""),
                seller.as_deref().unwrap_or("")
            )
        } else {
            tx_id
        }),
        event_index: 0,
        timestamp: timestamp(
            item.get("event_timestamp")
                .or_else(|| item.get("timestamp")),
        ),
        block_number: positive(integer(
            item.get("blockNumber").or_else(|| item.get("block_number")),
        ))
        .map(|value| value as u64),
        kind: EventKind::Sale,
        channel: Some(ValueChannel::SalePayment),
        from: seller.clone(),
        to: buyer.clone(),
        fee_payer: buyer.clone(),
        payment_payer: buyer,
        payment_recipient: seller,
        nft: Some(NftKey {
            chain: candidate.chain,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from(token_id),
        }),
        native_amount,
        usd_micros,
        gas_native: None,
        gas_usd_micros: None,
        marketplace_fee_native,
        marketplace_fee_usd_micros,
    }
}

fn payment_amount(chain: ChainId, value: &Value) -> (Option<i128>, Option<i128>) {
    let raw = value
        .get("amount")
        .or_else(|| value.get("quantity"))
        .or_else(|| value.get("value"))
        .unwrap_or(value);
    let symbol = text(value.get("symbol")).to_ascii_uppercase();
    normalize_payment_amount(chain, raw, &symbol, payment_decimals(value, &symbol))
}

fn marketplace_payment_amount(
    chain: ChainId,
    item: &Value,
    payment: &Value,
) -> (Option<i128>, Option<i128>) {
    let raw = item
        .get("payment_quantity")
        .or_else(|| payment.get("quantity"))
        .or_else(|| item.get("price"))
        .or_else(|| item.get("total_price"))
        .unwrap_or(&Value::Null);
    let symbol = text(
        payment
            .get("symbol")
            .or_else(|| item.get("payment_token_symbol")),
    )
    .to_ascii_uppercase();
    normalize_payment_amount(chain, raw, &symbol, payment_decimals(payment, &symbol))
}

fn normalize_payment_amount(
    chain: ChainId,
    raw: &Value,
    symbol: &str,
    decimals: u32,
) -> (Option<i128>, Option<i128>) {
    if is_stablecoin(symbol) {
        (None, scale_amount(raw, decimals, 6))
    } else if is_native_symbol(chain, symbol) {
        (scale_amount(raw, decimals, 18), None)
    } else {
        (None, None)
    }
}

fn payment_decimals(value: &Value, symbol: &str) -> u32 {
    value
        .get("decimals")
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(match symbol {
            "USDC" | "USDT" => 6,
            _ => 18,
        })
        .min(30)
}

fn scale_amount(value: &Value, source_decimals: u32, target_decimals: u32) -> Option<i128> {
    let integer = value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .or_else(|| value.as_str().and_then(|value| value.parse::<i128>().ok()))
        .or_else(|| {
            value
                .is_number()
                .then(|| value.to_string().parse::<i128>().ok())
                .flatten()
        });
    if let Some(integer) = integer {
        return if source_decimals <= target_decimals {
            integer.checked_mul(10_i128.checked_pow(target_decimals - source_decimals)?)
        } else {
            Some(integer / 10_i128.checked_pow(source_decimals - target_decimals)?)
        };
    }
    let amount = number(value)?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    Some((amount * 10_f64.powi(target_decimals as i32 - source_decimals as i32)).round() as i128)
}

fn is_native_symbol(chain: ChainId, symbol: &str) -> bool {
    match chain {
        ChainId::Ethereum | ChainId::Base => matches!(symbol, "ETH" | "WETH"),
        ChainId::Polygon => matches!(symbol, "POL" | "WPOL" | "MATIC" | "WMATIC"),
        ChainId::Solana => false,
    }
}

fn is_stablecoin(symbol: &str) -> bool {
    matches!(
        symbol,
        "USDC"
            | "USDT"
            | "DAI"
            | "USDS"
            | "USDE"
            | "FDUSD"
            | "TUSD"
            | "PYUSD"
            | "GUSD"
            | "USDP"
            | "LUSD"
            | "SUSD"
            | "FRAX"
    )
}

fn assign_event_indices(events: &mut [NormalizedEvent]) {
    events.sort_by(|left, right| {
        (&left.tx_id, left.event_index, left.kind.as_str(), &left.nft).cmp(&(
            &right.tx_id,
            right.event_index,
            right.kind.as_str(),
            &right.nft,
        ))
    });
    let mut previous = "";
    let mut index = 0_u32;
    for event in events {
        if event.tx_id.as_ref() != previous {
            previous = event.tx_id.as_ref();
            index = 0;
        }
        event.event_index = index;
        index = index.saturating_add(1);
    }
}

fn transfer_token_ids(item: &Value) -> Vec<String> {
    if let Some(values) = item.get("erc1155Metadata").and_then(Value::as_array) {
        return values
            .iter()
            .map(|value| normalize_token_id(value.get("tokenId").unwrap_or(&Value::Null)))
            .filter(|value| !value.is_empty())
            .collect();
    }
    let token = normalize_token_id(
        item.get("tokenId")
            .or_else(|| item.get("erc721TokenId"))
            .or_else(|| item.get("tokenID"))
            .unwrap_or(&Value::Null),
    );
    (!token.is_empty()).then_some(token).into_iter().collect()
}

fn selected(selection: &NftSelection, token_id: &str) -> bool {
    match selection {
        NftSelection::AllInContract { .. } => true,
        NftSelection::Explicit { nfts } => nfts.iter().any(|nft| nft.token_id.as_ref() == token_id),
    }
}

fn relation_seed_contract(relation: &SeedCandidateRelation) -> Option<ContractKey> {
    Some(relation.seed.clone())
}

fn opensea_headers(api_key: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(api_key)
            .map_err(|error| AnalysisError::Config(format!("invalid OpenSea API key: {error}")))?,
    );
    Ok(headers)
}

fn opensea_chain(chain: ChainId) -> &'static str {
    chain.as_str()
}

fn chain_id(chain: ChainId) -> &'static str {
    match chain {
        ChainId::Ethereum => "1",
        ChainId::Base => "8453",
        ChainId::Polygon => "137",
        ChainId::Solana => "",
    }
}

fn normalize_token_id(value: &Value) -> String {
    let raw = value
        .as_str()
        .map(std::borrow::Cow::Borrowed)
        .or_else(|| {
            value
                .is_number()
                .then(|| std::borrow::Cow::Owned(value.to_string()))
        })
        .unwrap_or_default();
    let raw = raw.trim();
    if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        hex_to_decimal(hex).unwrap_or_else(|| raw.to_owned())
    } else {
        raw.trim_start_matches('0').to_owned().if_empty("0")
    }
}

fn hex_to_decimal(hex: &str) -> Option<String> {
    if hex.is_empty() {
        return Some("0".into());
    }
    let mut digits = vec![0_u8];
    for byte in hex.bytes() {
        let mut carry = char::from(byte).to_digit(16)? as u16;
        for digit in &mut digits {
            let value = u16::from(*digit) * 16 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    while digits.len() > 1 && digits.last() == Some(&0) {
        digits.pop();
    }
    Some(
        digits
            .into_iter()
            .rev()
            .map(|digit| char::from(b'0' + digit))
            .collect(),
    )
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> Self;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> Self {
        if self.is_empty() {
            fallback.to_owned()
        } else {
            self
        }
    }
}

fn text(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or("")
}

fn transaction_id(value: Option<&Value>) -> String {
    value
        .and_then(|value| {
            value.as_str().or_else(|| {
                value
                    .get("hash")
                    .or_else(|| value.get("transaction_hash"))
                    .and_then(Value::as_str)
            })
        })
        .unwrap_or("")
        .to_owned()
}

fn is_transaction_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn address_like(value: &Value) -> Option<&str> {
    value.as_str().or_else(|| {
        ["address", "hash", "token", "contract_address"]
            .into_iter()
            .find_map(|field| value.get(field).and_then(address_like))
    })
}

fn opensea_collection_slug(value: &Value) -> &str {
    value
        .get("collection")
        .and_then(|collection| {
            collection
                .as_str()
                .or_else(|| collection.get("slug").and_then(Value::as_str))
        })
        .unwrap_or("")
}

fn integer(value: Option<&Value>) -> i64 {
    let Some(value) = value else {
        return 0;
    };
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| {
            value.as_str().and_then(|raw| {
                raw.strip_prefix("0x")
                    .or_else(|| raw.strip_prefix("0X"))
                    .and_then(|hex| i64::from_str_radix(hex, 16).ok())
                    .or_else(|| raw.parse().ok())
            })
        })
        .unwrap_or(0)
}

fn parse_hex_i128(raw: &str) -> Option<i128> {
    let hex = raw
        .trim()
        .strip_prefix("0x")
        .or_else(|| raw.trim().strip_prefix("0X"))?;
    i128::from_str_radix(hex, 16).ok()
}

fn decimal_to_units(value: &Value, decimals: u32) -> Option<i128> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.is_number().then(|| value.to_string()))?;
    let raw = raw.trim();
    if raw.starts_with('-') {
        return None;
    }
    let mut parts = raw.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let scale = 10_i128.checked_pow(decimals)?;
    let whole = if whole.is_empty() {
        0
    } else {
        whole.parse::<i128>().ok()?
    };
    let decimals = decimals as usize;
    let kept = &fraction[..fraction.len().min(decimals)];
    let mut fraction_units = if kept.is_empty() {
        0
    } else {
        kept.parse::<i128>().ok()?
    };
    fraction_units = fraction_units.checked_mul(
        10_i128.checked_pow(u32::try_from(decimals.saturating_sub(kept.len())).ok()?)?,
    )?;
    whole.checked_mul(scale)?.checked_add(fraction_units)
}

fn combined_status(statuses: &[EvidenceStatus]) -> EvidenceStatus {
    if statuses.contains(&EvidenceStatus::Failed) {
        EvidenceStatus::Failed
    } else if statuses.iter().any(|status| {
        matches!(
            status,
            EvidenceStatus::Truncated | EvidenceStatus::NotRequested | EvidenceStatus::Requested
        )
    }) {
        EvidenceStatus::Truncated
    } else if statuses
        .iter()
        .all(|status| *status == EvidenceStatus::Empty)
    {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    }
}

fn number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn positive(value: i64) -> Option<i64> {
    (value > 0).then_some(value)
}

fn timestamp(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    let parsed = integer(Some(value));
    if parsed > 0 {
        return Some(parsed);
    }
    value
        .as_str()
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp())
}

fn abi_address(value: Option<&str>) -> Option<String> {
    let raw = value?.trim();
    let hex = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X"))?;
    if hex.len() < 40 {
        return None;
    }
    let address = &hex[hex.len() - 40..];
    if address.bytes().all(|byte| byte == b'0')
        || !address.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(format!("0x{}", address.to_ascii_lowercase()))
}

fn address(item: &Value, fields: &[&str]) -> Option<Arc<str>> {
    fields
        .iter()
        .find_map(|field| {
            item.get(*field).and_then(|value| {
                value
                    .as_str()
                    .or_else(|| value.get("address").and_then(Value::as_str))
            })
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| Arc::from(value.to_ascii_lowercase()))
}

fn push_address(values: &mut Vec<Arc<str>>, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    let Some(value) = normalize_evm_address(value) else {
        return;
    };
    values.push(Arc::from(value));
}

fn normalize_evm_address(value: &str) -> Option<String> {
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    if hex.len() != 40
        || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        || hex.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some(format!("0x{}", hex.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_keys_skip_providers_and_token_ids_normalize() {
        assert_eq!(normalize_token_id(&Value::String("0x0f".into())), "15");
        assert_eq!(normalize_token_id(&Value::String("0007".into())), "7");
        assert_eq!(normalize_token_id(&json!(42)), "42");
        assert_eq!(
            normalize_token_id(&Value::String(format!("0x{}", "f".repeat(64)))),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        assert_eq!(
            abi_address(Some(
                "0x0000000000000000000000001111111111111111111111111111111111111111"
            ))
            .as_deref(),
            Some("0x1111111111111111111111111111111111111111")
        );
        assert!(abi_address(Some("0x0")).is_none());
        assert!(ProviderApiKeys::default().alchemy.is_empty());
        assert_eq!(opensea_chain(ChainId::Polygon), "polygon");
    }

    #[test]
    fn event_indices_are_unique_within_a_transaction() {
        let candidate = ContractKey::new(ChainId::Ethereum, "0x1");
        let mut events = vec![
            sale_event(&candidate, &json!({"transactionHash":"0xa"}), "1".into()),
            transfer_event(
                &candidate,
                &json!({"hash":"0xa","from":ZERO_ADDRESS,"to":"0x2"}),
                "1".into(),
                "alchemy",
            ),
        ];
        assign_event_indices(&mut events);
        assert_eq!(events[0].event_index, 0);
        assert_eq!(events[1].event_index, 1);
    }

    #[test]
    fn sale_parser_accepts_nested_transaction_and_rfc3339_time() {
        let candidate = ContractKey::new(ChainId::Ethereum, "0x1");
        let event = sale_event(
            &candidate,
            &json!({
                "transaction": {"hash": "0xabc"},
                "event_timestamp": "2025-01-02T03:04:05Z",
                "payment_quantity": "1250000",
                "payment": {"symbol": "USDC", "decimals": 6}
            }),
            "1".into(),
        );
        assert_eq!(event.tx_id.as_ref(), "0xabc");
        assert_eq!(event.timestamp, Some(1_735_787_045));
        assert_eq!(event.usd_micros, Some(1_250_000));
    }

    #[test]
    fn opensea_contract_collection_accepts_current_and_legacy_shapes() {
        assert_eq!(
            opensea_collection_slug(&json!({"collection":"current-slug"})),
            "current-slug"
        );
        assert_eq!(
            opensea_collection_slug(&json!({"collection":{"slug":"legacy-slug"}})),
            "legacy-slug"
        );
    }

    #[test]
    fn deployment_receipt_accepts_enhanced_and_json_rpc_rows() {
        let target = "0x1111111111111111111111111111111111111111";
        let first_hash = format!("0x{}", "a".repeat(64));
        let second_hash = format!("0x{}", "b".repeat(64));
        assert_eq!(
            find_deployment_receipt(
                &[json!({
                    "contractAddress": target,
                    "transactionHash": first_hash
                })],
                target
            )
            .as_deref(),
            Some(first_hash.as_str())
        );
        assert_eq!(
            find_deployment_receipt(
                &[json!({
                    "id": second_hash,
                    "result": {"contractAddress": target}
                })],
                target
            )
            .as_deref(),
            Some(second_hash.as_str())
        );
    }

    #[test]
    fn payment_parser_keeps_base_units_and_rejects_unknown_tokens() {
        assert_eq!(
            payment_amount(
                ChainId::Ethereum,
                &json!({"amount":"1000000000000000000","symbol":"ETH"})
            ),
            (Some(1_000_000_000_000_000_000), None)
        );
        assert_eq!(
            payment_amount(
                ChainId::Ethereum,
                &json!({"amount":"1250000","symbol":"USDC","decimals":6})
            ),
            (None, Some(1_250_000))
        );
        assert_eq!(
            payment_amount(
                ChainId::Ethereum,
                &json!({"amount":"1000000000000000000","symbol":"OTHER"})
            ),
            (None, None)
        );
    }

    #[test]
    fn value_flow_parser_preserves_base_units_and_conservative_cashout_bounds() {
        let candidate = ContractKey::new(
            ChainId::Ethereum,
            "0x1111111111111111111111111111111111111111",
        );
        let event = value_flow_event(
            &candidate,
            &json!({
                "hash": format!("0x{}", "a".repeat(64)),
                "from": "0x2222222222222222222222222222222222222222",
                "to": candidate.contract_address.as_ref(),
                "category": "external",
                "asset": "ETH",
                "value": "1",
                "rawContract": {"value": "0xde0b6b3a7640000"},
                "metadata": {"blockTimestamp": "2024-01-01T00:00:00Z"},
                "blockNum": "0x10"
            }),
            FlowDirection::Incoming,
        )
        .unwrap();
        assert_eq!(event.kind, EventKind::Funding);
        assert_eq!(event.native_amount, Some(1_000_000_000_000_000_000));
        assert_eq!(decimal_to_units(&json!("1.25"), 6), Some(1_250_000));
        assert!(flow_amount_compatible(Some(1_000), None, Some(1_050), None));
        assert!(!flow_amount_compatible(
            Some(1_000),
            None,
            Some(1_101),
            None
        ));
    }
}
