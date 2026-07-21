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

type AuthorityCell = Arc<tokio::sync::OnceCell<Vec<Arc<str>>>>;
type AuthorityCache = Arc<tokio::sync::Mutex<HashMap<ContractKey, AuthorityCell>>>;

#[derive(Clone)]
pub struct SolanaClient {
    helius: ProviderHttpClient,
    other: ProviderHttpClient,
    keys: ProviderApiKeys,
    endpoints: ProviderEndpoints,
    page_limits: Arc<BTreeMap<String, usize>>,
    authority_cache: AuthorityCache,
    prices: PriceOracle,
    observed_at: i64,
}

#[derive(Clone, Default)]
struct AssetSnapshot {
    assets: Vec<Asset>,
    total: usize,
    authority: Vec<Arc<str>>,
    truncated: bool,
}

#[derive(Clone)]
struct Asset {
    mint: Arc<str>,
    owner: Option<Arc<str>>,
    compressed: bool,
}

#[derive(Clone)]
struct SignatureRef {
    signature: String,
    mint: Arc<str>,
    event_type: String,
}

struct HeliusHistory {
    events: Vec<NormalizedEvent>,
    truncated: bool,
    transactions_fetched: u64,
    transactions_provider_reported: u64,
    signature_discovery_failures: u64,
    transaction_detail_failures: u64,
    unattributed_transactions: u64,
}

struct HistoryBatch {
    events: Vec<NormalizedEvent>,
    transactions_fetched: u64,
    transaction_detail_failures: u64,
    unattributed_transactions: u64,
    truncated: bool,
}

struct Outcome<T> {
    value: T,
    status: EvidenceStatus,
    source: &'static str,
    request_key: &'static str,
    failure: Option<String>,
    transactions_fetched: u64,
    transactions_provider_reported: u64,
    transactions_failed: u64,
    signature_discovery_failures: u64,
    transaction_detail_failures: u64,
    unattributed_transactions: u64,
}

impl<T: Default> Outcome<T> {
    fn skipped(request_key: &'static str) -> Self {
        Self {
            value: T::default(),
            status: EvidenceStatus::NotRequested,
            source: "none",
            request_key,
            failure: None,
            transactions_fetched: 0,
            transactions_provider_reported: 0,
            transactions_failed: 0,
            signature_discovery_failures: 0,
            transaction_detail_failures: 0,
            unattributed_transactions: 0,
        }
    }

    fn failed(source: &'static str, request_key: &'static str, error: AnalysisError) -> Self {
        Self {
            value: T::default(),
            status: EvidenceStatus::Failed,
            source,
            request_key,
            failure: Some(format!("{request_key}: {error}")),
            transactions_fetched: 0,
            transactions_provider_reported: 0,
            transactions_failed: 0,
            signature_discovery_failures: 0,
            transaction_detail_failures: 0,
            unattributed_transactions: 0,
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
            transactions_fetched: 0,
            transactions_provider_reported: 0,
            transactions_failed: 0,
            signature_discovery_failures: 0,
            transaction_detail_failures: 0,
            unattributed_transactions: 0,
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
}

impl SolanaClient {
    pub fn new(
        helius: ProviderHttpClient,
        other: ProviderHttpClient,
        keys: ProviderApiKeys,
        endpoints: ProviderEndpoints,
        page_limits: Arc<BTreeMap<String, usize>>,
        prices: PriceOracle,
        observed_at: i64,
    ) -> Self {
        Self {
            helius,
            other,
            keys,
            endpoints,
            page_limits,
            authority_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            prices,
            observed_at,
        }
    }

    pub async fn fetch(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
        relations: &[SeedCandidateRelation],
    ) -> Result<EvidenceBundle> {
        if candidate.chain != ChainId::Solana {
            return Err(AnalysisError::Provider(
                "Solana client received a non-Solana candidate".into(),
            ));
        }
        let snapshot = self.fetch_snapshot(candidate, selection).await;
        let history_assets = selected_assets(selection, &snapshot.value.assets);
        let (mut history, mut market) = tokio::join!(
            self.fetch_history(candidate, &history_assets, &snapshot.value.authority),
            self.fetch_market_events(candidate, selection),
        );
        normalize_helius_chain_history(&mut history.value);
        let mut events = std::mem::take(&mut history.value);
        events.append(&mut market.value);
        assign_event_indices(&mut events);
        let observed_at = self.observed_at;
        let price_failure = self
            .prices
            .enrich_events(&mut events)
            .await
            .err()
            .map(|error| format!("historical_prices: {error}"));
        let mut failures = Vec::new();
        for failure in [
            snapshot.failure.as_ref(),
            history.failure.as_ref(),
            market.failure.as_ref(),
            price_failure.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            failures.push(failure.clone());
        }
        let holders = snapshot
            .value
            .assets
            .iter()
            .filter_map(|asset| {
                asset.owner.as_ref().map(|owner| {
                    (
                        NftKey {
                            chain: ChainId::Solana,
                            contract_address: candidate.contract_address.clone(),
                            token_id: asset.mint.clone(),
                        },
                        owner.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();
        let missing_authority = snapshot.value.authority.is_empty();
        let authority_status = match snapshot.status {
            EvidenceStatus::Failed | EvidenceStatus::NotRequested => snapshot.status,
            _ if missing_authority => EvidenceStatus::Empty,
            _ => EvidenceStatus::Complete,
        };
        let relation_verifications = self
            .relation_verifications(
                relations,
                &snapshot.value.authority,
                matches!(
                    authority_status,
                    EvidenceStatus::Complete | EvidenceStatus::Empty
                ),
            )
            .await;
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
        let mut provenance = vec![
            snapshot.observation(observed_at),
            history.observation(observed_at),
            market.observation(observed_at),
        ];
        let assets_status = snapshot.status;
        let history_status = history.status;
        let transactions_status = combined_status(&[history.status, market.status]);
        let transaction_count = events
            .iter()
            .map(|event| &event.tx_id)
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let resolved_mints = events
            .iter()
            .filter(|event| event.kind == EventKind::Mint)
            .filter_map(|event| event.nft.as_ref().map(|nft| nft.token_id.clone()))
            .collect::<BTreeSet<_>>();
        let unresolved_compressed_mints = snapshot
            .value
            .assets
            .iter()
            .filter(|asset| asset.compressed && !resolved_mints.contains(&asset.mint))
            .count() as u64;
        let sale_count = events
            .iter()
            .filter(|event| event.kind == EventKind::Sale)
            .count() as u64;
        let price_count = events
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
        let price_status =
            if market.status == EvidenceStatus::NotRequested && priced_values_total == 0 {
                EvidenceStatus::NotRequested
            } else if market.status == EvidenceStatus::Failed && priced_values_total == 0 {
                EvidenceStatus::Failed
            } else if priced_values_total == 0 {
                EvidenceStatus::Empty
            } else if price_failure.is_none() && priced_values_total == priced_values_parsed {
                EvidenceStatus::Complete
            } else {
                EvidenceStatus::Truncated
            };
        provenance.push(EvidenceObservation {
            source: if self.keys.alchemy.is_empty() {
                "none"
            } else {
                "alchemy"
            }
            .into(),
            request_key: "historical_prices".into(),
            observed_at,
            status: price_status,
        });
        Ok(EvidenceBundle {
            candidate: candidate.clone(),
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events,
            holders,
            controllers: snapshot.value.authority,
            relation_verifications,
            provenance,
            quality: EvidenceQuality {
                assets: Some(assets_status),
                histories: Some(history_status),
                transactions: Some(transactions_status),
                prices: Some(price_status),
                authority: Some(authority_status),
                sale_prices_parsed: price_count,
                sale_prices_total: sale_count,
                candidate_assets_analyzed: snapshot.value.assets.len() as u64,
                candidate_assets_total: snapshot.value.total as u64,
                history_assets_requested: if history_status != EvidenceStatus::NotRequested {
                    history_assets.len() as u64
                } else {
                    0
                },
                history_assets_succeeded: if matches!(
                    history_status,
                    EvidenceStatus::Complete | EvidenceStatus::Empty | EvidenceStatus::Truncated
                ) {
                    history_assets.len() as u64
                } else {
                    0
                },
                history_assets_complete: if matches!(
                    history_status,
                    EvidenceStatus::Complete | EvidenceStatus::Empty
                ) {
                    history_assets.len() as u64
                } else {
                    0
                },
                history_assets_failed: if history_status == EvidenceStatus::Failed {
                    history_assets.len() as u64
                } else {
                    0
                },
                history_assets_not_requested: if history_status == EvidenceStatus::NotRequested {
                    history_assets.len() as u64
                } else {
                    0
                },
                history_assets_truncated: if history_status == EvidenceStatus::Truncated {
                    history_assets.len() as u64
                } else {
                    0
                },
                transactions_fetched: history.transactions_fetched.max(transaction_count),
                transactions_provider_reported: history
                    .transactions_provider_reported
                    .max(transaction_count),
                transactions_failed: history.transactions_failed,
                signature_discovery_failures: history.signature_discovery_failures,
                transaction_detail_failures: history.transaction_detail_failures,
                unattributed_solana_transactions: history.unattributed_transactions,
                unresolved_compressed_mints,
                missing_collection_authorities: u64::from(missing_authority),
                supplemental_query_failures: u64::from(price_failure.is_some())
                    + relation_failure_count,
                failures,
                ..EvidenceQuality::default()
            },
        })
    }

    pub async fn verify_relations(
        &self,
        relations: &[SeedCandidateRelation],
        authorities: &[Arc<str>],
        authority_complete: bool,
    ) -> Vec<RelationVerification> {
        self.relation_verifications(relations, authorities, authority_complete)
            .await
    }

    async fn fetch_snapshot(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Outcome<AssetSnapshot> {
        if self.keys.helius.is_empty() {
            return Outcome::skipped("collection_assets");
        }
        match self.helius_snapshot(candidate, selection).await {
            Ok(snapshot) => {
                let empty = snapshot.assets.is_empty();
                let truncated = snapshot.truncated;
                Outcome::complete(snapshot, empty, truncated, "helius", "collection_assets")
            }
            Err(error) => Outcome::failed("helius", "collection_assets", error),
        }
    }

    async fn helius_snapshot(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Result<AssetSnapshot> {
        if let NftSelection::Explicit { nfts } = selection {
            return self.helius_explicit_assets(candidate, nfts).await;
        }
        let cap = self.limit("assets");
        let mut snapshot = AssetSnapshot::default();
        let mut page = 1_usize;
        let mut seen = BTreeSet::new();
        let mut total_known = false;
        loop {
            let request_limit = 1_000_usize.min(cap.saturating_sub(snapshot.assets.len()).max(1));
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("assets-{page}"),
                "method": "getAssetsByGroup",
                "params": {
                    "groupKey": "collection",
                    "groupValue": candidate.contract_address.as_ref(),
                    "page": page,
                    "limit": request_limit,
                    "options": {
                        "showUnverifiedCollections": false,
                        "showCollectionMetadata": true,
                        "showGrandTotal": true
                    }
                }
            });
            let mut payload = self
                .helius
                .post(self.helius_url()?, HeaderMap::new(), &body)
                .await?;
            if let Some(error) = payload.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Helius getAssetsByGroup failed: {error}"
                )));
            }
            let (reported_total, authority) = {
                let result = payload.get("result").unwrap_or(&Value::Null);
                let items = result
                    .get("items")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        AnalysisError::Provider(
                            "Helius getAssetsByGroup response omitted result.items".into(),
                        )
                    })?;
                (
                    result
                        .get("total")
                        .and_then(Value::as_u64)
                        .map(|value| value as usize),
                    items.first().map(|item| {
                        collection_authorities(item, result, &candidate.contract_address)
                    }),
                )
            };
            if let Some(reported_total) = reported_total {
                snapshot.total = reported_total;
                total_known = true;
            }
            if snapshot.authority.is_empty() {
                snapshot.authority = authority.unwrap_or_default();
            }
            let owned_items = match payload
                .get_mut("result")
                .and_then(|result| result.get_mut("items"))
                .map(Value::take)
            {
                Some(Value::Array(items)) => items,
                _ => {
                    return Err(AnalysisError::Provider(
                        "Helius getAssetsByGroup response omitted result.items".into(),
                    ));
                }
            };
            let item_count = owned_items.len();
            let assets = self
                .helius
                .normalize_response(move || Ok(normalize_asset_items(&owned_items)))
                .await?;
            for asset in assets {
                if !seen.insert(asset.mint.clone()) {
                    continue;
                }
                snapshot.assets.push(asset);
                if snapshot.assets.len() >= cap {
                    snapshot.truncated = !total_known || snapshot.assets.len() < snapshot.total;
                    if !total_known {
                        snapshot.total = snapshot.assets.len();
                    }
                    return Ok(snapshot);
                }
            }
            if item_count == 0
                || item_count < request_limit
                || (total_known && snapshot.assets.len() >= snapshot.total)
            {
                snapshot.truncated = total_known && snapshot.assets.len() < snapshot.total;
                if !total_known {
                    snapshot.total = snapshot.assets.len();
                }
                return Ok(snapshot);
            }
            if !total_known {
                snapshot.total = snapshot.assets.len();
            }
            page += 1;
        }
    }

    async fn helius_explicit_assets(
        &self,
        candidate: &ContractKey,
        nfts: &[NftKey],
    ) -> Result<AssetSnapshot> {
        let ids = nfts
            .iter()
            .take(self.limit("assets"))
            .map(|nft| nft.token_id.as_ref())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(AssetSnapshot::default());
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": "asset-batch",
            "method": "getAssetBatch",
            "params": {"ids": ids}
        });
        let mut payload = self
            .helius
            .post(self.helius_url()?, HeaderMap::new(), &body)
            .await?;
        if let Some(error) = payload.get("error") {
            return Err(AnalysisError::Provider(format!(
                "Helius getAssetBatch failed: {error}"
            )));
        }
        let mut snapshot = AssetSnapshot {
            total: nfts.len(),
            truncated: nfts.len() > ids.len(),
            ..AssetSnapshot::default()
        };
        snapshot.authority = payload
            .get("result")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .map(|item| collection_authorities(item, &Value::Null, &candidate.contract_address))
            .unwrap_or_default();
        let owned_items = match payload.get_mut("result").map(Value::take) {
            Some(Value::Array(items)) => items,
            _ => {
                return Err(AnalysisError::Provider(
                    "Helius getAssetBatch response omitted result array".into(),
                ));
            }
        };
        snapshot.assets = self
            .helius
            .normalize_response(move || Ok(normalize_asset_items(&owned_items)))
            .await?;
        if snapshot.assets.is_empty() && !ids.is_empty() {
            return Err(AnalysisError::Provider(format!(
                "Helius returned no explicit assets for {}",
                candidate.contract_address
            )));
        }
        snapshot.truncated |= snapshot.assets.len() < ids.len();
        Ok(snapshot)
    }

    async fn fetch_history(
        &self,
        candidate: &ContractKey,
        assets: &[Asset],
        controllers: &[Arc<str>],
    ) -> Outcome<Vec<NormalizedEvent>> {
        if self.keys.helius.is_empty() || assets.is_empty() {
            return Outcome::skipped("asset_history");
        }
        match self.helius_history(candidate, assets, controllers).await {
            Ok(result) => {
                let empty = result.events.is_empty();
                let mut outcome = Outcome::complete(
                    result.events,
                    empty,
                    result.truncated,
                    "helius",
                    "asset_history",
                );
                outcome.transactions_fetched = result.transactions_fetched;
                outcome.transactions_provider_reported = result.transactions_provider_reported;
                outcome.transactions_failed = result.transaction_detail_failures;
                outcome.signature_discovery_failures = result.signature_discovery_failures;
                outcome.transaction_detail_failures = result.transaction_detail_failures;
                outcome.unattributed_transactions = result.unattributed_transactions;
                outcome
            }
            Err(error) => Outcome::failed("helius", "asset_history", error),
        }
    }

    async fn helius_history(
        &self,
        candidate: &ContractKey,
        assets: &[Asset],
        controllers: &[Arc<str>],
    ) -> Result<HeliusHistory> {
        let cap = self.limit("transactions");
        let per_asset = self.limit("history").min(1_000);
        let mut by_signature = BTreeMap::<String, Vec<SignatureRef>>::new();
        let mut truncated = false;
        let mut successful_lookups = 0_usize;
        let mut signature_discovery_failures = 0_u64;
        let mut last_lookup_error = None;
        for (chunk_index, asset_chunk) in assets.chunks(16).enumerate() {
            if by_signature.len() >= cap {
                truncated = true;
                break;
            }
            let remaining = cap - by_signature.len();
            let remaining_assets = assets
                .len()
                .saturating_sub(chunk_index.saturating_mul(16))
                .max(1);
            let request_limit = per_asset.min(remaining.div_ceil(remaining_assets).max(1));
            let lookups = stream::iter(asset_chunk.iter().cloned())
                .map(|asset| {
                    let client = self.clone();
                    async move { client.fetch_asset_signatures(&asset, request_limit).await }
                })
                .buffer_unordered(16)
                .collect::<Vec<_>>()
                .await;
            for lookup in lookups {
                let (refs, asset_truncated) = match lookup {
                    Ok(value) => {
                        successful_lookups += 1;
                        value
                    }
                    Err(error) => {
                        truncated = true;
                        signature_discovery_failures =
                            signature_discovery_failures.saturating_add(1);
                        last_lookup_error = Some(error);
                        continue;
                    }
                };
                truncated |= asset_truncated;
                for reference in refs {
                    let references = by_signature.entry(reference.signature.clone()).or_default();
                    if !references
                        .iter()
                        .any(|existing| existing.mint == reference.mint)
                    {
                        references.push(reference);
                    }
                }
            }
            if by_signature.len() >= cap {
                truncated |= (chunk_index + 1) * 16 < assets.len();
                break;
            }
        }
        if successful_lookups == 0 {
            return Err(last_lookup_error.unwrap_or_else(|| {
                AnalysisError::Provider("Helius asset history lookup produced no results".into())
            }));
        }
        if by_signature.len() > cap {
            by_signature = by_signature.into_iter().take(cap).collect();
            truncated = true;
        }
        let refs = by_signature.into_iter().collect::<Vec<_>>();
        let transactions_provider_reported = refs.len() as u64;
        let mut events = Vec::new();
        let mut transactions_fetched = 0_u64;
        let mut transaction_detail_failures = 0_u64;
        let mut unattributed_transactions = 0_u64;
        for chunk in refs.chunks(100) {
            let batch = Value::Array(
                chunk
                    .iter()
                    .map(|(signature, _)| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": signature,
                            "method": "getTransaction",
                            "params": [
                                signature,
                                {
                                    "encoding": "jsonParsed",
                                    "commitment": "finalized",
                                    "maxSupportedTransactionVersion": 0
                                }
                            ]
                        })
                    })
                    .collect(),
            );
            let payload = self
                .helius
                .post(self.helius_url()?, HeaderMap::new(), &batch)
                .await?;
            let rows = match payload {
                Value::Array(rows) => rows,
                payload => vec![payload],
            };
            let owned_chunk = chunk.to_vec();
            let owned_candidate = candidate.clone();
            let owned_controllers = controllers.to_vec();
            let normalized = self
                .helius
                .normalize_response(move || {
                    Ok(normalize_history_batch(
                        owned_candidate,
                        owned_chunk,
                        rows,
                        owned_controllers,
                    ))
                })
                .await?;
            events.extend(normalized.events);
            transactions_fetched =
                transactions_fetched.saturating_add(normalized.transactions_fetched);
            transaction_detail_failures =
                transaction_detail_failures.saturating_add(normalized.transaction_detail_failures);
            unattributed_transactions =
                unattributed_transactions.saturating_add(normalized.unattributed_transactions);
            truncated |= normalized.truncated;
        }
        assign_event_indices(&mut events);
        Ok(HeliusHistory {
            events,
            truncated,
            transactions_fetched,
            transactions_provider_reported,
            signature_discovery_failures,
            transaction_detail_failures,
            unattributed_transactions,
        })
    }

    async fn fetch_market_events(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
    ) -> Outcome<Vec<NormalizedEvent>> {
        if self.keys.opensea.is_empty() {
            return Outcome::skipped("nft_market");
        }
        let slug = match self.opensea_collection_slug(candidate).await {
            Ok(slug) => slug,
            Err(error) => return Outcome::failed("opensea", "nft_market", error),
        };
        if slug.is_empty() {
            return Outcome::complete(Vec::new(), true, false, "opensea", "nft_market");
        }
        match self
            .opensea_market_events_inner(candidate, selection, &slug)
            .await
        {
            Ok((mut sales, sales_truncated)) => {
                assign_event_indices(&mut sales);
                let cap = self.limit("transactions");
                let truncated = sales_truncated || sales.len() > cap;
                sales.truncate(cap);
                let empty = sales.is_empty();
                Outcome::complete(sales, empty, truncated, "opensea", "nft_market")
            }
            Err(error) => Outcome::failed("opensea", "nft_market", error),
        }
    }

    async fn opensea_market_events_inner(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
        slug: &str,
    ) -> Result<(Vec<NormalizedEvent>, bool)> {
        let headers = opensea_headers(&self.keys.opensea)?;
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
            let payload = self.other.get(url, headers.clone()).await?;
            let mut page_overflow = false;
            for market_event in payload
                .get("asset_events")
                .or_else(|| payload.get("events"))
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
                if event_contract
                    .is_some_and(|address| address.trim() != candidate.contract_address.as_ref())
                {
                    continue;
                }
                let mint = text(nft.get("identifier").or_else(|| nft.get("token_id")));
                if mint.is_empty() || !selected(selection, mint) {
                    continue;
                }
                if events.len() < cap {
                    events.push(opensea_market_event(
                        candidate,
                        market_event,
                        mint,
                        EventKind::Sale,
                    ));
                } else {
                    page_overflow = true;
                }
            }
            let next = text(payload.get("next"));
            if events.len() >= cap {
                assign_event_indices(&mut events);
                return Ok((events, page_overflow || !next.is_empty()));
            }
            if next.is_empty() {
                assign_event_indices(&mut events);
                return Ok((events, false));
            }
            if !seen.insert(next.to_owned()) {
                return Err(AnalysisError::Provider(
                    "OpenSea sale pagination repeated cursor".into(),
                ));
            }
            cursor = Some(next.to_owned());
        }
    }

    async fn opensea_collection_slug(&self, candidate: &ContractKey) -> Result<String> {
        let contract_url = reqwest::Url::parse(&format!(
            "{}/api/v2/chain/solana/contract/{}",
            self.endpoints.opensea.trim_end_matches('/'),
            candidate.contract_address
        ))
        .map_err(|error| AnalysisError::Provider(error.to_string()))?;
        let metadata = self
            .other
            .get(contract_url, opensea_headers(&self.keys.opensea)?)
            .await?;
        Ok(opensea_collection_slug(&metadata).to_owned())
    }

    async fn relation_verifications(
        &self,
        relations: &[SeedCandidateRelation],
        candidate_authority: &[Arc<str>],
        candidate_authority_complete: bool,
    ) -> Vec<RelationVerification> {
        stream::iter(relations.iter().cloned())
            .map(|relation| async move {
                let seed = relation_seed_contract(&relation);
                let authority = match seed {
                    Some(seed) => self.cached_authority(seed).await,
                    None => Err(AnalysisError::Provider(
                        "relation has no seed NFT identity".into(),
                    )),
                };
                match authority {
                    Ok(authority) => {
                        let continuity = authority.iter().any(|seed| {
                            candidate_authority
                                .iter()
                                .any(|candidate| candidate == seed)
                        });
                        RelationVerification {
                            seed_id: relation.seed_id,
                            official_controller_continuity: continuity,
                            authorized_reissue: false,
                            verified_migration: false,
                            official_collection_relation: continuity,
                            complete: candidate_authority_complete
                                && !candidate_authority.is_empty()
                                && !authority.is_empty(),
                            evidence_keys: if continuity {
                                vec![Arc::from("collection_authority")]
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

    async fn cached_authority(&self, contract: ContractKey) -> Result<Vec<Arc<str>>> {
        let cell = {
            let mut cache = self.authority_cache.lock().await;
            cache
                .entry(contract.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        cell.get_or_try_init(|| async {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("authority-{}", contract.contract_address),
                "method": "getAssetsByGroup",
                "params": {
                    "groupKey": "collection",
                    "groupValue": contract.contract_address.as_ref(),
                    "page": 1,
                    "limit": 1,
                    "options": {
                        "showUnverifiedCollections": false,
                        "showCollectionMetadata": true
                    }
                }
            });
            let payload = self
                .helius
                .post(self.helius_url()?, HeaderMap::new(), &body)
                .await?;
            if let Some(error) = payload.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Helius authority lookup failed: {error}"
                )));
            }
            let result = payload.get("result").ok_or_else(|| {
                AnalysisError::Provider("Helius authority response omitted result".into())
            })?;
            let items = result
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AnalysisError::Provider("Helius authority response omitted result.items".into())
                })?;
            let item = items.first().unwrap_or(&Value::Null);
            Ok(collection_authorities(
                item,
                result,
                &contract.contract_address,
            ))
        })
        .await
        .cloned()
    }

    /// Paginate `getSignaturesForAsset` with the official `before` cursor.
    /// See https://www.helius.dev/docs/api-reference/das/getsignaturesforasset
    async fn fetch_asset_signatures(
        &self,
        asset: &Asset,
        max_records: usize,
    ) -> Result<(Vec<SignatureRef>, bool)> {
        let max_records = max_records.max(1);
        let mut collected = Vec::new();
        let mut seen_signatures = BTreeSet::new();
        let mut seen_cursors = BTreeSet::new();
        let mut before = None::<String>;
        let mut reported_total = None::<usize>;
        let mut truncated = false;
        while collected.len() < max_records {
            let request_limit = max_records.saturating_sub(collected.len()).clamp(1, 1_000);
            let mut params = json!({
                "id": asset.mint.as_ref(),
                "page": 1,
                "limit": request_limit,
            });
            if let Some(cursor) = before.as_deref().filter(|value| !value.is_empty()) {
                params["before"] = Value::String(cursor.to_owned());
            }
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("signatures-{}", asset.mint),
                "method": "getSignaturesForAsset",
                "params": params
            });
            let mut payload = self
                .helius
                .post(self.helius_url()?, HeaderMap::new(), &body)
                .await?;
            if let Some(error) = payload.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Helius getSignaturesForAsset failed for {}: {error}",
                    asset.mint
                )));
            }
            let result = payload.get_mut("result").ok_or_else(|| {
                AnalysisError::Provider(
                    "Helius getSignaturesForAsset response omitted result".into(),
                )
            })?;
            if let Some(total) = result.get("total").and_then(Value::as_u64) {
                let total = usize::try_from(total).unwrap_or(usize::MAX);
                reported_total = Some(reported_total.unwrap_or_default().max(total));
            }
            let owned_items = if result.is_array() {
                result.take()
            } else {
                result
                    .get_mut("items")
                    .map(Value::take)
                    .unwrap_or(Value::Null)
            };
            let items = match owned_items {
                Value::Array(items) => items,
                _ => {
                    return Err(AnalysisError::Provider(
                        "Helius getSignaturesForAsset result omitted items".into(),
                    ));
                }
            };
            let page_len = items.len();
            let mint = asset.mint.clone();
            let refs = self
                .helius
                .normalize_response(move || Ok(normalize_signature_items(&items, mint)))
                .await?;
            let next_cursor = refs.last().map(|reference| reference.signature.clone());
            let repeated_cursor = next_cursor
                .as_ref()
                .is_some_and(|cursor| seen_cursors.contains(cursor));
            if let Some(cursor) = next_cursor.as_ref() {
                seen_cursors.insert(cursor.clone());
            }
            let previous_count = collected.len();
            for reference in refs {
                if seen_signatures.insert(reference.signature.clone()) {
                    collected.push(reference);
                }
            }
            let pagination_stalled =
                repeated_cursor || (page_len >= request_limit && collected.len() == previous_count);
            let cursor_advanced = next_cursor.is_some() && next_cursor != before;
            before = next_cursor;
            let reached_reported_total =
                reported_total.is_some_and(|total| collected.len() >= total);
            let short_page = page_len < request_limit;
            if collected.len() >= max_records {
                truncated = !reached_reported_total
                    && (reported_total.is_some_and(|total| collected.len() < total)
                        || page_len >= request_limit);
                break;
            }
            if pagination_stalled
                || short_page
                || reached_reported_total
                || !cursor_advanced
                || before.is_none()
            {
                // A full page that stopped without exhausting reported_total is truncated.
                truncated = page_len >= request_limit
                    && !reached_reported_total
                    && (reported_total.is_some_and(|total| collected.len() < total)
                        || pagination_stalled);
                break;
            }
        }
        Ok((collected, truncated))
    }

    fn helius_url(&self) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&self.endpoints.helius)
            .map_err(|error| AnalysisError::Provider(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("api-key", &self.keys.helius);
        Ok(url)
    }

    fn limit(&self, key: &str) -> usize {
        self.page_limits.get(key).copied().unwrap_or(1_000).max(1)
    }
}

fn normalize_asset_items(items: &[Value]) -> Vec<Asset> {
    items
        .iter()
        .filter_map(|item| {
            let mint = nonempty(item.get("id").and_then(Value::as_str))?;
            Some(Asset {
                mint: Arc::from(mint),
                owner: nonempty(
                    item.get("ownership")
                        .and_then(|ownership| ownership.get("owner"))
                        .and_then(Value::as_str),
                )
                .map(Arc::from),
                compressed: item
                    .get("compression")
                    .and_then(|compression| compression.get("compressed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect()
}

fn normalize_signature_items(items: &[Value], mint: Arc<str>) -> Vec<SignatureRef> {
    items
        .iter()
        .filter_map(|item| {
            let parts = item.as_array();
            let signature = parts
                .and_then(|parts| parts.first())
                .and_then(Value::as_str)
                .or_else(|| item.get("signature").and_then(Value::as_str))
                .or_else(|| item.get("id").and_then(Value::as_str));
            nonempty(signature).map(|signature| SignatureRef {
                signature: signature.to_owned(),
                mint: mint.clone(),
                event_type: parts
                    .and_then(|parts| parts.get(1))
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| text(item.get("type").or_else(|| item.get("eventType"))))
                    .to_owned(),
            })
        })
        .collect()
}

fn normalize_history_batch(
    candidate: ContractKey,
    chunk: Vec<(String, Vec<SignatureRef>)>,
    rows: Vec<Value>,
    controllers: Vec<Arc<str>>,
) -> HistoryBatch {
    let by_id = rows
        .iter()
        .filter_map(|row| row.get("id").and_then(Value::as_str).map(|id| (id, row)))
        .collect::<HashMap<_, _>>();
    let mut output = HistoryBatch {
        events: Vec::new(),
        transactions_fetched: 0,
        transaction_detail_failures: 0,
        unattributed_transactions: 0,
        truncated: false,
    };
    for (signature, references) in chunk {
        let Some(row) = by_id.get(signature.as_str()) else {
            output.truncated = true;
            output.transaction_detail_failures =
                output.transaction_detail_failures.saturating_add(1);
            continue;
        };
        if row.get("error").is_some() {
            output.truncated = true;
            output.transaction_detail_failures =
                output.transaction_detail_failures.saturating_add(1);
            continue;
        }
        let Some(result) = row.get("result").filter(|result| !result.is_null()) else {
            output.truncated = true;
            output.transaction_detail_failures =
                output.transaction_detail_failures.saturating_add(1);
            continue;
        };
        output.transactions_fetched = output.transactions_fetched.saturating_add(1);
        let before = output.events.len();
        for reference in &references {
            if let Some(event) = solana_event(&candidate, reference, result) {
                output.events.push(event);
            } else {
                output.truncated = true;
            }
        }
        let primary_end = output.events.len();
        if primary_end == before {
            output.unattributed_transactions = output.unattributed_transactions.saturating_add(1);
        }
        let sale_payments = output.events[before..primary_end]
            .iter()
            .filter(|event| event.kind == EventKind::Sale)
            .filter_map(|event| {
                Some((
                    event.payment_payer.clone()?,
                    event.payment_recipient.clone()?,
                    event.native_amount?,
                ))
            })
            .collect::<BTreeSet<_>>();
        output.events.extend(solana_value_flow_events(
            &candidate,
            &signature,
            result,
            &controllers,
            &sale_payments,
        ));
    }
    output
}

fn selected_assets(selection: &NftSelection, snapshot: &[Asset]) -> Vec<Asset> {
    match selection {
        NftSelection::AllInContract { .. } => snapshot.to_vec(),
        NftSelection::Explicit { nfts } => {
            let existing = snapshot
                .iter()
                .map(|asset| (asset.mint.as_ref(), asset))
                .collect::<HashMap<_, _>>();
            nfts.iter()
                .map(|nft| {
                    existing
                        .get(nft.token_id.as_ref())
                        .map(|asset| (*asset).clone())
                        .unwrap_or_else(|| Asset {
                            mint: nft.token_id.clone(),
                            owner: None,
                            compressed: false,
                        })
                })
                .collect()
        }
    }
}

fn normalize_helius_chain_history(events: &mut Vec<NormalizedEvent>) {
    events.retain_mut(|event| match event.kind {
        EventKind::Listing => false,
        EventKind::Sale => {
            event.kind = EventKind::Transfer;
            event.channel = None;
            event.payment_payer = None;
            event.payment_recipient = None;
            event.native_amount = None;
            event.usd_micros = None;
            event.marketplace_fee_native = None;
            event.marketplace_fee_usd_micros = None;
            true
        }
        _ => true,
    });
}

fn solana_event(
    candidate: &ContractKey,
    reference: &SignatureRef,
    result: &Value,
) -> Option<NormalizedEvent> {
    let meta = result.get("meta")?;
    if meta.get("err").is_some_and(|error| !error.is_null()) {
        return None;
    }
    let keys = account_keys(result);
    let before = token_owner(meta.get("preTokenBalances"), &reference.mint);
    let after = token_owner(meta.get("postTokenBalances"), &reference.mint);
    let (from, to, bubblegum_mint) = match (before, after) {
        (from, to) if from.is_some() || to.is_some() => {
            let is_mint = from.is_none() && to.is_some();
            (from, to, is_mint)
        }
        _ => compressed_nft_owner_change(result, &reference.event_type, &keys)
            .map(|(from, to, is_mint)| (from, Some(to), is_mint))
            .unwrap_or((None, None, false)),
    };
    let event_type = reference.event_type.to_ascii_lowercase();
    let is_mint =
        bubblegum_mint || event_type.starts_with("mint") || (from.is_none() && to.is_some());
    let provider_reports_listing =
        event_type.contains("listing") || event_type == "nft_list" || event_type.ends_with("_list");
    let provider_reports_sale = event_type.contains("sale");
    let from = from.map(Arc::from);
    let to = to.map(Arc::from);
    let payment = native_payment(result, to.as_deref(), from.as_deref());
    let kind = if is_mint {
        EventKind::Mint
    } else if provider_reports_listing {
        EventKind::Listing
    } else if provider_reports_sale || payment.is_some() {
        EventKind::Sale
    } else {
        EventKind::Transfer
    };
    Some(NormalizedEvent {
        chain: ChainId::Solana,
        tx_id: Arc::from(reference.signature.as_str()),
        event_index: 0,
        timestamp: result.get("blockTime").and_then(Value::as_i64),
        block_number: result.get("slot").and_then(Value::as_u64),
        kind,
        channel: match kind {
            EventKind::Sale => Some(ValueChannel::SalePayment),
            EventKind::Listing => Some(ValueChannel::Listing),
            _ => None,
        },
        from: from.clone(),
        to: to.clone(),
        fee_payer: keys.first().cloned().map(Arc::from),
        payment_payer: (kind == EventKind::Sale).then_some(to.clone()).flatten(),
        payment_recipient: (kind == EventKind::Sale).then_some(from.clone()).flatten(),
        nft: Some(NftKey {
            chain: ChainId::Solana,
            contract_address: candidate.contract_address.clone(),
            token_id: reference.mint.clone(),
        }),
        native_amount: payment,
        usd_micros: None,
        gas_native: meta.get("fee").and_then(Value::as_i64).map(i128::from),
        gas_usd_micros: None,
        marketplace_fee_native: None,
        marketplace_fee_usd_micros: None,
    })
}

const BUBBLEGUM_PROGRAM_ID: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
const BUBBLEGUM_TRANSFER_V1_DISCRIMINATOR: [u8; 8] = [163, 52, 200, 231, 140, 3, 69, 186];
const BUBBLEGUM_TRANSFER_V2_DISCRIMINATOR: [u8; 8] = [119, 40, 6, 235, 234, 221, 248, 49];
const BUBBLEGUM_MINT_V1_DISCRIMINATOR: [u8; 8] = [145, 98, 192, 118, 184, 147, 118, 104];
const BUBBLEGUM_MINT_TO_COLLECTION_V1_DISCRIMINATOR: [u8; 8] = [153, 18, 178, 47, 197, 158, 86, 15];

fn compressed_nft_owner_change(
    result: &Value,
    asset_event_type: &str,
    account_keys: &[String],
) -> Option<(Option<String>, String, bool)> {
    let is_transfer = asset_event_type.eq_ignore_ascii_case("transfer");
    let is_mint = asset_event_type
        .trim()
        .to_ascii_lowercase()
        .starts_with("mint");
    if !is_transfer && !is_mint {
        return None;
    }
    let message = result.get("transaction")?.get("message")?;
    let mut instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for group in result
        .get("meta")
        .and_then(|meta| meta.get("innerInstructions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        instructions.extend(
            group
                .get("instructions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        );
    }
    for instruction in instructions {
        let program_id = instruction
            .get("programId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                instruction
                    .get("programIdIndex")
                    .and_then(Value::as_u64)
                    .and_then(|index| account_keys.get(index as usize).cloned())
            });
        if program_id.as_deref() != Some(BUBBLEGUM_PROGRAM_ID) {
            continue;
        }
        if is_mint {
            let discriminator = instruction
                .get("data")
                .and_then(Value::as_str)
                .and_then(decode_base58)
                .and_then(|bytes| bytes.get(..8).map(|slice| slice.to_vec()));
            let parsed_is_mint = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("type"))
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.to_ascii_lowercase().starts_with("mint"));
            let supported_discriminator = matches!(
                discriminator.as_deref(),
                Some(bytes)
                    if bytes == BUBBLEGUM_MINT_V1_DISCRIMINATOR
                        || bytes == BUBBLEGUM_MINT_TO_COLLECTION_V1_DISCRIMINATOR
            );
            if !parsed_is_mint && !supported_discriminator {
                continue;
            }
            let parsed_owner = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("info"))
                .and_then(|info| {
                    info.get("leafOwner")
                        .or_else(|| info.get("leaf_owner"))
                        .or_else(|| info.get("owner"))
                })
                .and_then(Value::as_str)
                .map(str::to_string);
            let account_owner = instruction
                .get("accounts")
                .and_then(Value::as_array)
                .and_then(|accounts| accounts.get(1))
                .and_then(|value| instruction_account(value, account_keys));
            let Some(owner) = parsed_owner.or(account_owner) else {
                continue;
            };
            if !owner.is_empty() {
                return Some((None, owner, true));
            }
            continue;
        }
        let discriminator = instruction
            .get("data")
            .and_then(Value::as_str)
            .and_then(decode_base58)
            .and_then(|bytes| bytes.get(..8).map(|slice| slice.to_vec()));
        let (from_index, to_index) = match discriminator.as_deref() {
            Some(bytes) if bytes == BUBBLEGUM_TRANSFER_V1_DISCRIMINATOR => (1, 3),
            Some(bytes) if bytes == BUBBLEGUM_TRANSFER_V2_DISCRIMINATOR => (3, 5),
            _ => continue,
        };
        let accounts = instruction.get("accounts")?.as_array()?;
        let from = instruction_account(accounts.get(from_index)?, account_keys)?;
        let to = instruction_account(accounts.get(to_index)?, account_keys)?;
        if from.is_empty() || to.is_empty() || from == to {
            continue;
        }
        return Some((Some(from), to, false));
    }
    None
}

fn instruction_account(value: &Value, account_keys: &[String]) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| {
            value
                .get("pubkey")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| {
            value
                .as_u64()
                .and_then(|index| account_keys.get(index as usize).cloned())
        })
}

fn decode_base58(value: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|candidate| *candidate == byte)? as u32;
        let mut carry = digit;
        for item in decoded.iter_mut().rev() {
            let next = u32::from(*item) * 58 + carry;
            *item = (next & 0xff) as u8;
            carry = next >> 8;
        }
        while carry > 0 {
            decoded.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let leading_zeroes = value.bytes().take_while(|byte| *byte == b'1').count();
    let first_nonzero = decoded
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(decoded.len());
    let mut result = vec![0_u8; leading_zeroes];
    result.extend_from_slice(&decoded[first_nonzero..]);
    Some(result)
}

fn token_owner(values: Option<&Value>, mint: &str) -> Option<String> {
    values
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|balance| {
            text(balance.get("mint")) == mint
                && balance
                    .get("uiTokenAmount")
                    .and_then(|amount| amount.get("amount"))
                    .and_then(Value::as_str)
                    .and_then(|amount| amount.parse::<u64>().ok())
                    .unwrap_or(0)
                    > 0
        })
        .and_then(|balance| balance.get("owner"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn native_payment(result: &Value, buyer: Option<&str>, seller: Option<&str>) -> Option<i128> {
    let (Some(buyer), Some(seller)) = (buyer, seller) else {
        return None;
    };
    let message = result.get("transaction")?.get("message")?;
    if let Some(payment) = payment_from_instructions(
        message
            .get("instructions")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default(),
        buyer,
        seller,
    ) {
        return Some(payment);
    }
    result
        .get("meta")
        .and_then(|meta| meta.get("innerInstructions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|group| {
            payment_from_instructions(
                group
                    .get("instructions")
                    .and_then(Value::as_array)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                buyer,
                seller,
            )
        })
}

fn payment_from_instructions(instructions: &[Value], buyer: &str, seller: &str) -> Option<i128> {
    instructions.iter().find_map(|instruction| {
        let parsed = instruction.get("parsed")?;
        let kind = text(parsed.get("type"));
        if !matches!(kind, "transfer" | "transferWithSeed") {
            return None;
        }
        let info = parsed.get("info")?;
        let source = text(info.get("source").or_else(|| info.get("from")));
        let destination = text(info.get("destination").or_else(|| info.get("to")));
        if source != buyer || destination != seller {
            return None;
        }
        info.get("lamports").and_then(Value::as_i64).map(i128::from)
    })
}

fn solana_value_flow_events(
    candidate: &ContractKey,
    signature: &str,
    result: &Value,
    controllers: &[Arc<str>],
    sale_payments: &BTreeSet<(Arc<str>, Arc<str>, i128)>,
) -> Vec<NormalizedEvent> {
    let mut controlled = controllers.iter().map(Arc::as_ref).collect::<BTreeSet<_>>();
    controlled.insert(candidate.contract_address.as_ref());
    let transfers = parsed_native_transfers(result);
    let fee_payer = account_keys(result).first().cloned().map(Arc::from);
    let gas_native = result
        .get("meta")
        .and_then(|meta| meta.get("fee"))
        .and_then(|value| {
            value
                .as_i64()
                .map(i128::from)
                .or_else(|| value.as_u64().map(i128::from))
        });
    let timestamp = result.get("blockTime").and_then(Value::as_i64);
    let block_number = result.get("slot").and_then(Value::as_u64);
    let mut events = Vec::new();
    let mut withdrawals = Vec::<(Arc<str>, i128)>::new();
    for (from, to, amount) in &transfers {
        if sale_payments.contains(&(from.clone(), to.clone(), *amount)) {
            continue;
        }
        let from_controlled = controlled.contains(from.as_ref());
        let to_controlled = controlled.contains(to.as_ref());
        let (kind, channel) = match (from_controlled, to_controlled) {
            (false, true) => (EventKind::Funding, ValueChannel::Funding),
            (true, false) => {
                withdrawals.push((to.clone(), *amount));
                (EventKind::Withdrawal, ValueChannel::Withdrawal)
            }
            _ => continue,
        };
        events.push(solana_flow_event(
            signature,
            timestamp,
            block_number,
            kind,
            channel,
            from.clone(),
            to.clone(),
            *amount,
            fee_payer.clone(),
            gas_native,
        ));
    }
    for (intermediate, withdrawn) in withdrawals {
        for (from, to, amount) in &transfers {
            if from != &intermediate
                || controlled.contains(to.as_ref())
                || !flow_amount_compatible(withdrawn, *amount)
            {
                continue;
            }
            events.push(solana_flow_event(
                signature,
                timestamp,
                block_number,
                EventKind::Cashout,
                ValueChannel::CashoutHop,
                from.clone(),
                to.clone(),
                *amount,
                fee_payer.clone(),
                gas_native,
            ));
        }
    }
    events.sort_by(|left, right| {
        (left.kind.as_str(), &left.from, &left.to, left.native_amount).cmp(&(
            right.kind.as_str(),
            &right.from,
            &right.to,
            right.native_amount,
        ))
    });
    events.dedup();
    events
}

fn parsed_native_transfers(result: &Value) -> Vec<(Arc<str>, Arc<str>, i128)> {
    let outer = result
        .get("transaction")
        .and_then(|transaction| transaction.get("message"))
        .and_then(|message| message.get("instructions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    let inner = result
        .get("meta")
        .and_then(|meta| meta.get("innerInstructions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|group| {
            group
                .get("instructions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        });
    outer
        .chain(inner)
        .filter_map(|instruction| {
            let parsed = instruction.get("parsed")?;
            if !matches!(text(parsed.get("type")), "transfer" | "transferWithSeed") {
                return None;
            }
            let info = parsed.get("info")?;
            let from = text(info.get("source").or_else(|| info.get("from")));
            let to = text(info.get("destination").or_else(|| info.get("to")));
            let amount = integer(info.get("lamports"));
            (!from.is_empty() && !to.is_empty() && amount > 0).then(|| {
                (
                    Arc::from(from.to_owned()),
                    Arc::from(to.to_owned()),
                    i128::from(amount),
                )
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn solana_flow_event(
    signature: &str,
    timestamp: Option<i64>,
    block_number: Option<u64>,
    kind: EventKind,
    channel: ValueChannel,
    from: Arc<str>,
    to: Arc<str>,
    native_amount: i128,
    fee_payer: Option<Arc<str>>,
    gas_native: Option<i128>,
) -> NormalizedEvent {
    NormalizedEvent {
        chain: ChainId::Solana,
        tx_id: Arc::from(signature),
        event_index: 0,
        timestamp,
        block_number,
        kind,
        channel: Some(channel),
        from: Some(from),
        to: Some(to),
        fee_payer,
        payment_payer: None,
        payment_recipient: None,
        nft: None,
        native_amount: Some(native_amount),
        usd_micros: None,
        gas_native,
        gas_usd_micros: None,
        marketplace_fee_native: None,
        marketplace_fee_usd_micros: None,
    }
}

fn flow_amount_compatible(source: i128, next: i128) -> bool {
    source > 0
        && next > 0
        && next
            .checked_mul(100)
            .zip(source.checked_mul(110))
            .is_some_and(|(next, source)| next <= source)
}

fn account_keys(result: &Value) -> Vec<String> {
    result
        .get("transaction")
        .and_then(|transaction| transaction.get("message"))
        .and_then(|message| message.get("accountKeys"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            value
                .as_str()
                .or_else(|| value.get("pubkey").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .collect()
}

fn opensea_market_event(
    candidate: &ContractKey,
    item: &Value,
    mint: &str,
    kind: EventKind,
) -> NormalizedEvent {
    let payment = item
        .get("payment")
        .or_else(|| item.get("payment_token"))
        .unwrap_or(&Value::Null);
    let quantity = item
        .get("payment_quantity")
        .or_else(|| payment.get("quantity"))
        .or_else(|| item.get("price"))
        .unwrap_or(&Value::Null);
    let symbol = text(
        payment
            .get("symbol")
            .or_else(|| item.get("payment_token_symbol")),
    )
    .to_ascii_uppercase();
    let decimals = payment
        .get("decimals")
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or({
            if matches!(symbol.as_str(), "USDC" | "USDT") {
                6
            } else {
                9
            }
        })
        .min(30);
    let (lamports, usd_micros) = if matches!(symbol.as_str(), "SOL" | "WSOL") {
        (scale_amount(quantity, decimals, 9), None)
    } else if is_stablecoin(&symbol) {
        (None, scale_amount(quantity, decimals, 6))
    } else {
        (None, None)
    };
    let buyer = (kind == EventKind::Sale)
        .then(|| {
            address(
                item,
                &["to_account", "winner_account", "buyer", "buyer_address"],
            )
        })
        .flatten();
    let seller = address(item, &["from_account", "seller", "seller_address"]);
    NormalizedEvent {
        chain: ChainId::Solana,
        tx_id: Arc::from(
            nonempty(
                item.get("transaction")
                    .or_else(|| item.get("transaction_hash"))
                    .or_else(|| item.get("order_hash"))
                    .and_then(|value| {
                        value
                            .as_str()
                            .or_else(|| value.get("hash").and_then(Value::as_str))
                    }),
            )
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "opensea:{}:{mint}:{}:{}:{}",
                    kind.as_str(),
                    timestamp(
                        item.get("event_timestamp")
                            .or_else(|| item.get("timestamp"))
                    )
                    .unwrap_or_default(),
                    buyer.as_deref().unwrap_or(""),
                    seller.as_deref().unwrap_or("")
                )
            }),
        ),
        event_index: 0,
        timestamp: timestamp(
            item.get("event_timestamp")
                .or_else(|| item.get("timestamp")),
        ),
        block_number: None,
        kind,
        channel: match kind {
            EventKind::Sale => Some(ValueChannel::SalePayment),
            EventKind::Listing => Some(ValueChannel::Listing),
            _ => None,
        },
        from: seller.clone(),
        to: buyer.clone(),
        fee_payer: None,
        payment_payer: (kind == EventKind::Sale).then_some(buyer).flatten(),
        payment_recipient: (kind == EventKind::Sale).then_some(seller).flatten(),
        nft: Some(NftKey {
            chain: ChainId::Solana,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from(mint),
        }),
        native_amount: lamports,
        usd_micros,
        gas_native: None,
        gas_usd_micros: None,
        marketplace_fee_native: None,
        marketplace_fee_usd_micros: None,
    }
}

fn collection_authorities(item: &Value, result: &Value, collection_address: &str) -> Vec<Arc<str>> {
    let metadata = item
        .get("grouping")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|group| {
            text(group.get("group_key")) == "collection"
                && group
                    .get("group_value")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value == collection_address)
        })
        .and_then(|group| {
            group
                .get("collection_metadata")
                .or_else(|| group.get("collectionMetadata"))
        })
        .or_else(|| result.get("collection_metadata"))
        .or_else(|| result.get("collectionMetadata"))
        .unwrap_or(&Value::Null);
    [
        metadata.get("update_authority"),
        metadata.get("updateAuthority"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    .filter_map(|value| nonempty(Some(value)))
    .map(Arc::from)
    .collect()
}

fn relation_seed_contract(relation: &SeedCandidateRelation) -> Option<ContractKey> {
    Some(relation.seed.clone())
}

fn selected(selection: &NftSelection, token_id: &str) -> bool {
    match selection {
        NftSelection::AllInContract { .. } => true,
        NftSelection::Explicit { nfts } => nfts.iter().any(|nft| nft.token_id.as_ref() == token_id),
    }
}

fn assign_event_indices(events: &mut [NormalizedEvent]) {
    events.sort_by(|left, right| {
        (&left.tx_id, &left.nft, left.kind.as_str()).cmp(&(
            &right.tx_id,
            &right.nft,
            right.kind.as_str(),
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
        .and_then(|value| nonempty(Some(value)))
        .map(Arc::from)
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

fn text(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or("")
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn integer(value: Option<&Value>) -> i64 {
    value
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .unwrap_or(0)
}

fn number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_solana_owner_change_becomes_transfer() {
        let candidate = ContractKey::new(ChainId::Solana, "collection");
        let reference = SignatureRef {
            signature: "sig".into(),
            mint: Arc::from("mint"),
            event_type: "TRANSFER".into(),
        };
        let payload = json!({
            "slot": 7,
            "blockTime": 8,
            "transaction": {"message":{"accountKeys":[],"instructions":[]}},
            "meta": {
                "err": null,
                "preTokenBalances":[{"mint":"mint","owner":"from","uiTokenAmount":{"amount":"1"}}],
                "postTokenBalances":[{"mint":"mint","owner":"to","uiTokenAmount":{"amount":"1"}}]
            }
        });
        let event = solana_event(&candidate, &reference, &payload).unwrap();
        assert_eq!(event.kind, EventKind::Transfer);
        assert_eq!(event.from.as_deref(), Some("from"));
        assert_eq!(event.to.as_deref(), Some("to"));
    }

    #[test]
    fn helius_sale_is_retained_as_chain_transfer_not_market_evidence() {
        let candidate = ContractKey::new(ChainId::Solana, "collection");
        let reference = SignatureRef {
            signature: "sig".into(),
            mint: Arc::from("mint"),
            event_type: "NFT_SALE".into(),
        };
        let payload = json!({
            "slot": 7,
            "blockTime": 8,
            "transaction": {"message":{"accountKeys":[],"instructions":[]}},
            "meta": {
                "err": null,
                "preTokenBalances":[{"mint":"mint","owner":"seller","uiTokenAmount":{"amount":"1"}}],
                "postTokenBalances":[{"mint":"mint","owner":"buyer","uiTokenAmount":{"amount":"1"}}]
            }
        });
        let mut events = vec![solana_event(&candidate, &reference, &payload).unwrap()];
        assert_eq!(events[0].kind, EventKind::Sale);
        normalize_helius_chain_history(&mut events);
        assert_eq!(events[0].kind, EventKind::Transfer);
        assert!(events[0].native_amount.is_none());
        assert!(events[0].payment_payer.is_none());
        assert!(events[0].payment_recipient.is_none());
    }

    #[test]
    fn helius_listing_is_not_emitted_as_market_evidence() {
        let candidate = ContractKey::new(ChainId::Solana, "collection");
        let reference = SignatureRef {
            signature: "sig".into(),
            mint: Arc::from("mint"),
            event_type: "NFT_LISTING".into(),
        };
        let payload = json!({
            "slot": 7,
            "blockTime": 8,
            "transaction": {"message":{"accountKeys":[],"instructions":[]}},
            "meta": {
                "err": null,
                "preTokenBalances":[{"mint":"mint","owner":"seller","uiTokenAmount":{"amount":"1"}}],
                "postTokenBalances":[{"mint":"mint","owner":"seller","uiTokenAmount":{"amount":"1"}}]
            }
        });
        let mut events = vec![solana_event(&candidate, &reference, &payload).unwrap()];
        normalize_helius_chain_history(&mut events);
        assert!(events.is_empty());
    }

    #[test]
    fn opensea_solana_sale_distinguishes_sol_and_stablecoin_units() {
        let candidate = ContractKey::new(ChainId::Solana, "collection");
        let sol = opensea_market_event(
            &candidate,
            &json!({
                "payment_quantity":"1500000000",
                "payment":{"symbol":"SOL","decimals":9}
            }),
            "mint",
            EventKind::Sale,
        );
        assert_eq!(sol.native_amount, Some(1_500_000_000));
        assert_eq!(sol.usd_micros, None);

        let usdc = opensea_market_event(
            &candidate,
            &json!({
                "payment_quantity":"2500000",
                "payment":{"symbol":"USDC","decimals":6}
            }),
            "mint",
            EventKind::Sale,
        );
        assert_eq!(usdc.native_amount, None);
        assert_eq!(usdc.usd_micros, Some(2_500_000));
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
    fn authority_native_transfers_create_funding_withdrawal_and_cashout_events() {
        let candidate = ContractKey::new(ChainId::Solana, "collection");
        let payload = json!({
            "slot": 7,
            "blockTime": 8,
            "transaction": {"message":{
                "accountKeys":["fee-payer"],
                "instructions":[
                    {"parsed":{"type":"transfer","info":{
                        "source":"funder","destination":"authority","lamports":1000
                    }}},
                    {"parsed":{"type":"transfer","info":{
                        "source":"authority","destination":"intermediate","lamports":500
                    }}},
                    {"parsed":{"type":"transfer","info":{
                        "source":"intermediate","destination":"exit","lamports":400
                    }}}
                ]
            }},
            "meta": {"fee": 5, "innerInstructions":[]}
        });
        let events = solana_value_flow_events(
            &candidate,
            "sig",
            &payload,
            &[Arc::from("authority")],
            &BTreeSet::new(),
        );
        assert_eq!(events.len(), 3);
        assert!(events.iter().any(|event| event.kind == EventKind::Funding));
        assert!(events
            .iter()
            .any(|event| event.kind == EventKind::Withdrawal));
        assert!(events.iter().any(|event| event.kind == EventKind::Cashout));
        assert!(events.iter().all(|event| event.gas_native == Some(5)));
    }
}
