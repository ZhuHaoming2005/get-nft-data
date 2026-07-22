use crate::config::RunConfig;
use crate::error::{AnalysisError, Result};
use crate::seed::{select_exact, RankedSeed, SeedSource};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "analysis")]
#[command(about = "四链头部 NFT 查重与深入分析")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    SelectSeeds {
        #[arg(long)]
        config: PathBuf,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        seeds: Option<PathBuf>,
    },
}

impl Cli {
    pub fn runtime_worker_threads(&self) -> Result<usize> {
        let path = match &self.command {
            Command::SelectSeeds { config } | Command::Run { config, .. } => config,
        };
        let workers = RunConfig::from_path_unvalidated(path)?.tokio_worker_threads;
        if workers == 0 {
            return Err(AnalysisError::Config(
                "tokio_worker_threads must be positive".into(),
            ));
        }
        Ok(workers)
    }

    pub async fn execute(self) -> Result<()> {
        match self.command {
            Command::SelectSeeds { config } => select_seeds(&config).await,
            Command::Run { config, seeds } => {
                crate::pipeline::orchestrator::run(&config, seeds.as_deref()).await
            }
        }
    }
}

async fn select_seeds(config_path: &Path) -> Result<()> {
    let config = RunConfig::from_path_unvalidated(config_path)?;
    config.validate_seed_selection()?;
    if config.api_keys.opensea.is_empty() {
        return Err(AnalysisError::Config(
            "api_keys.opensea is required for select-seeds".into(),
        ));
    }
    if config.api_keys.helius.is_empty() {
        return Err(AnalysisError::Config(
            "api_keys.helius is required to resolve Magic Eden Solana collection addresses".into(),
        ));
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(config.provider_timeout_ms))
        .build()?;
    let request_permits = Arc::new(tokio::sync::Semaphore::new(
        config.provider_concurrency.other,
    ));
    let opensea = OpenSeaSeedSource {
        client: client.clone(),
        endpoint: format!(
            "{}/api/v2/collections/top",
            config.provider_endpoints.opensea.trim_end_matches('/')
        ),
        opensea_api_key: config.api_keys.opensea.trim().to_owned(),
        retries: config.provider_retry_count,
        request_permits: request_permits.clone(),
    };
    let magic_eden = MagicEdenSeedSource {
        client,
        base_endpoint: config.provider_endpoints.magic_eden.clone(),
        helius_endpoint: config.provider_endpoints.helius.clone(),
        helius_api_key: config.api_keys.helius.trim().to_owned(),
        retries: config.provider_retry_count,
        request_permits,
    };
    let source = RoutedSeedSource {
        opensea: &opensea,
        magic_eden: &magic_eden,
    };
    let manifest = select_exact(&source, config.seed_top).await?;
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    if let Some(parent) = config.seed_manifest.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::reporting::contract_writer::atomic_write(&config.seed_manifest, &bytes)
}

struct RoutedSeedSource<'a> {
    opensea: &'a OpenSeaSeedSource,
    magic_eden: &'a MagicEdenSeedSource,
}

#[async_trait]
impl SeedSource for RoutedSeedSource<'_> {
    async fn ranked(&self, chain: crate::model::ChainId, limit: usize) -> Result<Vec<RankedSeed>> {
        if chain == crate::model::ChainId::Solana {
            self.magic_eden.ranked(chain, limit).await
        } else {
            self.opensea.ranked(chain, limit).await
        }
    }
}

struct OpenSeaSeedSource {
    client: reqwest::Client,
    endpoint: String,
    opensea_api_key: String,
    retries: usize,
    request_permits: Arc<tokio::sync::Semaphore>,
}

struct MagicEdenSeedSource {
    client: reqwest::Client,
    base_endpoint: String,
    helius_endpoint: String,
    helius_api_key: String,
    retries: usize,
    request_permits: Arc<tokio::sync::Semaphore>,
}

impl OpenSeaSeedSource {
    fn ranked_request(
        &self,
        chain: crate::model::ChainId,
        cursor: Option<&str>,
    ) -> Result<reqwest::Request> {
        let mut url = reqwest::Url::parse(&self.endpoint)
            .map_err(|error| AnalysisError::Config(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("chains", chain.as_str())
            .append_pair("limit", "50")
            .append_pair("sort_by", "thirty_days_volume");
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        Ok(self
            .client
            .get(url)
            .header("accept", "application/json")
            .header("x-api-key", &self.opensea_api_key)
            .build()?)
    }

    fn stats_request(&self, slug: &str) -> Result<reqwest::Request> {
        let mut url = reqwest::Url::parse(&self.endpoint)
            .map_err(|error| AnalysisError::Config(error.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| AnalysisError::Config("OpenSea endpoint cannot be a base URL".into()))?
            .clear()
            .extend(["api", "v2", "collections", slug, "stats"]);
        url.set_query(None);
        Ok(self
            .client
            .get(url)
            .header("accept", "application/json")
            .header("x-api-key", &self.opensea_api_key)
            .build()?)
    }

    async fn fetch_request(
        &self,
        build: impl Fn() -> Result<reqwest::Request>,
        context: &str,
    ) -> Result<serde_json::Value> {
        let mut last_error = None;
        for attempt in 0..=self.retries {
            let _permit = self
                .request_permits
                .acquire()
                .await
                .map_err(|_| AnalysisError::State("OpenSea seed permit pool closed".into()))?;
            let result = match self.client.execute(build()?).await {
                Ok(response) => crate::api::response_bytes(response, 16 * 1024 * 1024)
                    .await
                    .and_then(|bytes| serde_json::from_slice(&bytes).map_err(Into::into)),
                Err(error) => Err(AnalysisError::Http(error.without_url())),
            };
            drop(_permit);
            match result {
                Ok(payload) => return Ok(payload),
                Err(error) => {
                    let retryable = error.retryable();
                    let retry_after_ms = error.retry_after_ms();
                    last_error = Some(error);
                    if attempt >= self.retries || !retryable {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(
                        retry_after_ms
                            .unwrap_or_else(|| 100_u64.saturating_mul(1_u64 << attempt.min(8))),
                    ))
                    .await;
                }
            }
        }
        Err(AnalysisError::Seed(format!(
            "{context}: {}",
            last_error.expect("seed request performs at least one attempt")
        )))
    }

    async fn fetch_page(
        &self,
        chain: crate::model::ChainId,
        cursor: Option<&str>,
    ) -> Result<serde_json::Value> {
        self.fetch_request(
            || self.ranked_request(chain, cursor),
            &format!("OpenSea top collection request failed for {chain}"),
        )
        .await
    }

    async fn thirty_day_volume(&self, slug: &str) -> Result<f64> {
        let payload = self
            .fetch_request(
                || self.stats_request(slug),
                &format!("OpenSea collection stats request failed for {slug}"),
            )
            .await?;
        opensea_thirty_day_volume(&payload).ok_or_else(|| {
            AnalysisError::Seed(format!(
                "OpenSea collection stats omitted a valid thirty_day volume for {slug}"
            ))
        })
    }
}

#[async_trait]
impl SeedSource for OpenSeaSeedSource {
    async fn ranked(&self, chain: crate::model::ChainId, limit: usize) -> Result<Vec<RankedSeed>> {
        let mut ranked = Vec::with_capacity(limit);
        let mut seen_addresses = BTreeSet::new();
        let mut seen_cursors = BTreeSet::new();
        let mut cursor = None;
        while ranked.len() < limit {
            let payload = self.fetch_page(chain, cursor.as_deref()).await?;
            let collections = ["collections", "top_collections", "data", "results"]
                .into_iter()
                .find_map(|field| payload.get(field).and_then(serde_json::Value::as_array))
                .ok_or_else(|| {
                    AnalysisError::Seed("OpenSea top response omitted collections".into())
                })?;
            for collection in collections {
                let collection_name = collection
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let slug = collection
                    .get("collection")
                    .or_else(|| collection.get("slug"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                if slug.is_empty() {
                    continue;
                }
                for contract in collection_contracts(collection) {
                    let raw_chain = contract
                        .get("chain")
                        .or_else(|| contract.get("chain_identifier"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if !opensea_chain_matches(chain, raw_chain) {
                        continue;
                    }
                    let Some(address) = ["address", "contract_address", "contractAddress"]
                        .into_iter()
                        .find_map(|field| {
                            contract
                                .get(field)
                                .and_then(serde_json::Value::as_str)
                                .map(str::trim)
                                .filter(|value| !value.is_empty())
                        })
                    else {
                        continue;
                    };
                    if !valid_contract_address(chain, address) {
                        continue;
                    }
                    let address = if chain.is_evm() {
                        address.to_ascii_lowercase()
                    } else {
                        address.to_owned()
                    };
                    if !seen_addresses.insert(address.clone()) {
                        continue;
                    }
                    ranked.push(RankedSeed {
                        chain,
                        contract_address: address.clone(),
                        collection_name: if collection_name.is_empty() {
                            slug.to_owned()
                        } else {
                            collection_name.to_owned()
                        },
                        stable_identifier: slug.to_owned(),
                        ranking_metric: "thirty_days_volume".into(),
                        ranking_value: 0.0,
                        ranking_window: "30d".into(),
                        source: "opensea".into(),
                    });
                    if ranked.len() == limit {
                        break;
                    }
                }
                if ranked.len() == limit {
                    break;
                }
            }
            if ranked.len() == limit {
                break;
            }
            let next = ["cursor", "next", "next_cursor"]
                .into_iter()
                .find_map(|field| payload.get(field).and_then(serde_json::Value::as_str))
                .filter(|value| !value.is_empty());
            let Some(next) = next else {
                break;
            };
            if !seen_cursors.insert(next.to_owned()) {
                return Err(AnalysisError::Seed(format!(
                    "OpenSea top pagination repeated cursor for {chain}"
                )));
            }
            cursor = Some(next.to_owned());
        }
        let slugs = ranked
            .iter()
            .map(|seed| seed.stable_identifier.clone())
            .collect::<BTreeSet<_>>();
        let volumes =
            futures_util::future::try_join_all(slugs.into_iter().map(|slug| async move {
                Ok::<_, AnalysisError>((slug.clone(), self.thirty_day_volume(&slug).await?))
            }))
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for seed in &mut ranked {
            seed.ranking_value = *volumes.get(&seed.stable_identifier).ok_or_else(|| {
                AnalysisError::Seed(format!(
                    "OpenSea volume lookup omitted {}",
                    seed.stable_identifier
                ))
            })?;
        }
        Ok(ranked)
    }
}

impl MagicEdenSeedSource {
    fn endpoint_url(&self, segments: &[&str]) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&self.base_endpoint)
            .map_err(|error| AnalysisError::Config(error.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| AnalysisError::Config("Magic Eden endpoint cannot be a base URL".into()))?
            .pop_if_empty()
            .extend(segments);
        url.set_query(None);
        Ok(url)
    }

    fn popular_request(&self) -> Result<reqwest::Request> {
        let mut url = self.endpoint_url(&["marketplace", "popular_collections"])?;
        url.query_pairs_mut().append_pair("timeRange", "30d");
        Ok(self
            .client
            .get(url)
            .header("accept", "application/json")
            .build()?)
    }

    fn collection_listings_request(&self, symbol: &str) -> Result<reqwest::Request> {
        let mut url = self.endpoint_url(&["collections", symbol, "listings"])?;
        url.query_pairs_mut()
            .append_pair("offset", "0")
            .append_pair("limit", "1");
        Ok(self
            .client
            .get(url)
            .header("accept", "application/json")
            .build()?)
    }

    fn helius_asset_batch_request(&self, mints: &[String]) -> Result<reqwest::Request> {
        let mut url = reqwest::Url::parse(&self.helius_endpoint)
            .map_err(|error| AnalysisError::Config(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("api-key", &self.helius_api_key);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "seed-collection-addresses",
            "method": "getAssetBatch",
            "params": {"ids": mints}
        });
        Ok(self
            .client
            .post(url)
            .header("accept", "application/json")
            .json(&body)
            .build()?)
    }

    async fn fetch_request(
        &self,
        build: impl Fn() -> Result<reqwest::Request>,
        context: &str,
    ) -> Result<serde_json::Value> {
        let mut last_error = None;
        for attempt in 0..=self.retries {
            let _permit =
                self.request_permits.acquire().await.map_err(|_| {
                    AnalysisError::State("Magic Eden seed permit pool closed".into())
                })?;
            let result = match self.client.execute(build()?).await {
                Ok(response) => crate::api::response_bytes(response, 16 * 1024 * 1024)
                    .await
                    .and_then(|bytes| serde_json::from_slice(&bytes).map_err(Into::into)),
                Err(error) => Err(AnalysisError::Http(error.without_url())),
            };
            drop(_permit);
            match result {
                Ok(payload) => return Ok(payload),
                Err(error) => {
                    let retryable = error.retryable();
                    let retry_after_ms = error.retry_after_ms();
                    last_error = Some(error);
                    if attempt >= self.retries || !retryable {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(
                        retry_after_ms
                            .unwrap_or_else(|| 100_u64.saturating_mul(1_u64 << attempt.min(8))),
                    ))
                    .await;
                }
            }
        }
        Err(AnalysisError::Seed(format!(
            "{context}: {}",
            last_error.expect("seed request performs at least one attempt")
        )))
    }

    async fn fetch_popular(&self) -> Result<serde_json::Value> {
        self.fetch_request(
            || self.popular_request(),
            "Magic Eden popular collection request failed",
        )
        .await
    }

    async fn collection_detail(
        &self,
        collection: MagicEdenCollection,
    ) -> Result<Option<MagicEdenCollectionMint>> {
        let symbol = collection.symbol.clone();
        let listing_context = format!("Magic Eden collection listing request failed for {symbol}");
        let listing = self
            .fetch_request(
                || self.collection_listings_request(&symbol),
                &listing_context,
            )
            .await?;
        Ok(magic_eden_listing_mint(&listing)
            .map(|mint| MagicEdenCollectionMint { collection, mint }))
    }

    async fn collection_addresses(&self, mints: &[String]) -> Result<BTreeMap<String, String>> {
        let mut addresses = BTreeMap::new();
        for chunk in mints.chunks(1_000) {
            let payload = self
                .fetch_request(
                    || self.helius_asset_batch_request(chunk),
                    "Helius collection address resolution failed",
                )
                .await?;
            if let Some(error) = payload.get("error") {
                return Err(AnalysisError::Provider(format!(
                    "Helius getAssetBatch failed: {error}"
                )));
            }
            let items = payload
                .get("result")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    AnalysisError::Provider(
                        "Helius getAssetBatch response omitted result array".into(),
                    )
                })?;
            for item in items {
                let Some(mint) = item
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    continue;
                };
                let Some(address) = helius_collection_address(item) else {
                    continue;
                };
                if valid_contract_address(crate::model::ChainId::Solana, &address) {
                    addresses.insert(mint.to_owned(), address);
                }
            }
        }
        Ok(addresses)
    }
}

#[async_trait]
impl SeedSource for MagicEdenSeedSource {
    async fn ranked(&self, chain: crate::model::ChainId, limit: usize) -> Result<Vec<RankedSeed>> {
        if chain != crate::model::ChainId::Solana {
            return Err(AnalysisError::Seed(format!(
                "Magic Eden seed source does not support {chain}"
            )));
        }
        let payload = self.fetch_popular().await?;
        let collections = magic_eden_popular_collections(&payload);
        if collections.is_empty() {
            return Err(AnalysisError::Seed(
                "Magic Eden popular response omitted usable collection symbols".into(),
            ));
        }
        let details = futures_util::future::try_join_all(
            collections
                .into_iter()
                .map(|collection| self.collection_detail(collection)),
        )
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let detail_count = details.len();
        let mints = details
            .iter()
            .map(|detail| detail.mint.clone())
            .collect::<Vec<_>>();
        let addresses = self.collection_addresses(&mints).await?;
        let address_count = addresses.len();
        let mut seen_addresses = BTreeSet::new();
        let mut ranked = details
            .into_iter()
            .filter_map(|detail| {
                let address = addresses.get(&detail.mint)?.clone();
                seen_addresses
                    .insert(address.clone())
                    .then_some(RankedSeed {
                        chain: crate::model::ChainId::Solana,
                        contract_address: address,
                        collection_name: detail.collection.name,
                        stable_identifier: detail.collection.symbol,
                        ranking_metric: "popularity_rank".into(),
                        ranking_value: f64::from(detail.collection.rank),
                        ranking_window: "30d".into(),
                        source: "magic_eden".into(),
                    })
            })
            .collect::<Vec<_>>();
        ranked.truncate(limit);
        if ranked.is_empty() {
            return Err(AnalysisError::Seed(format!(
                "Magic Eden returned no usable Solana collection seeds (listing mints={detail_count}, Helius collection addresses={address_count})"
            )));
        }
        Ok(ranked)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MagicEdenCollection {
    symbol: String,
    name: String,
    rank: u32,
}

struct MagicEdenCollectionMint {
    collection: MagicEdenCollection,
    mint: String,
}

fn magic_eden_popular_collections(payload: &serde_json::Value) -> Vec<MagicEdenCollection> {
    let Some(collections) = payload.as_array().or_else(|| {
        ["collections", "data", "results"]
            .into_iter()
            .find_map(|field| payload.get(field).and_then(serde_json::Value::as_array))
    }) else {
        return Vec::new();
    };
    let mut seen_symbols = BTreeSet::new();
    collections
        .iter()
        .enumerate()
        .filter_map(|(index, collection)| {
            let symbol = collection
                .get("symbol")
                .and_then(serde_json::Value::as_str)?
                .trim();
            if symbol.is_empty() || !seen_symbols.insert(symbol.to_owned()) {
                return None;
            }
            let name = collection
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(symbol);
            Some(MagicEdenCollection {
                symbol: symbol.to_owned(),
                name: name.to_owned(),
                rank: u32::try_from(index + 1).ok()?,
            })
        })
        .collect()
}

fn magic_eden_listing_mint(payload: &serde_json::Value) -> Option<String> {
    let listing = payload
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| payload.get("results")?.as_array()?.first())?;
    listing
        .get("tokenMint")
        .or_else(|| listing.get("token_mint"))
        .or_else(|| {
            listing
                .get("token")
                .and_then(|token| token.get("mintAddress"))
        })
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| valid_contract_address(crate::model::ChainId::Solana, value))
        .map(str::to_owned)
}

fn helius_collection_address(item: &serde_json::Value) -> Option<String> {
    let group = item
        .get("grouping")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .find(|group| {
            group
                .get("group_key")
                .or_else(|| group.get("groupKey"))
                .and_then(serde_json::Value::as_str)
                == Some("collection")
        })?;
    group
        .get("group_value")
        .or_else(|| group.get("groupValue"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn collection_contracts(collection: &serde_json::Value) -> Vec<&serde_json::Value> {
    let mut contracts = Vec::new();
    for field in ["contracts", "primary_asset_contracts", "asset_contracts"] {
        contracts.extend(
            collection
                .get(field)
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten(),
        );
    }
    if contracts.is_empty() {
        contracts.push(collection);
    }
    contracts
}

fn opensea_chain_matches(chain: crate::model::ChainId, raw: &str) -> bool {
    let raw = raw.trim();
    match chain {
        crate::model::ChainId::Polygon => {
            raw.eq_ignore_ascii_case("polygon") || raw.eq_ignore_ascii_case("matic")
        }
        _ => raw.eq_ignore_ascii_case(chain.as_str()),
    }
}

fn json_number(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn opensea_thirty_day_volume(payload: &serde_json::Value) -> Option<f64> {
    payload
        .get("intervals")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .find(|interval| {
            interval.get("interval").and_then(serde_json::Value::as_str) == Some("thirty_day")
        })
        .and_then(|interval| interval.get("volume"))
        .and_then(json_number)
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn valid_contract_address(chain: crate::model::ChainId, value: &str) -> bool {
    let value = value.trim();
    if chain.is_evm() {
        return value
            .strip_prefix("0x")
            .or_else(|| value.strip_prefix("0X"))
            .is_some_and(|hex| {
                hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            });
    }
    base58_decoded_len(value) == Some(32)
}

fn base58_decoded_len(value: &str) -> Option<usize> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let leading_zeroes = value.bytes().take_while(|byte| *byte == b'1').count();
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|candidate| *candidate == byte)? as u16;
        let mut carry = digit;
        for part in &mut decoded {
            let value = u16::from(*part) * 58 + carry;
            *part = value as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            decoded.push(carry as u8);
            carry >>= 8;
        }
    }
    while decoded.last() == Some(&0) && decoded.len() > 1 {
        decoded.pop();
    }
    let body_len = if decoded == [0] { 0 } else { decoded.len() };
    Some(leading_zeroes + body_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ChainId;

    #[test]
    fn seed_request_uses_opensea_top_collections_contract() {
        let source = |key: &str| OpenSeaSeedSource {
            client: reqwest::Client::new(),
            endpoint: "https://example.com/api/v2/collections/top".into(),
            opensea_api_key: key.into(),
            retries: 0,
            request_permits: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        let with_key = source("secret")
            .ranked_request(ChainId::Ethereum, None)
            .unwrap();
        assert_eq!(with_key.headers()["x-api-key"], "secret");
        assert_eq!(
            with_key.url().as_str(),
            "https://example.com/api/v2/collections/top?chains=ethereum&limit=50&sort_by=thirty_days_volume"
        );
        let polygon = source("secret")
            .ranked_request(ChainId::Polygon, None)
            .unwrap();
        assert!(polygon
            .url()
            .query()
            .is_some_and(|query| { query.split('&').any(|part| part == "chains=polygon") }));
        assert_eq!(
            source("secret")
                .stats_request("collection slug")
                .unwrap()
                .url()
                .as_str(),
            "https://example.com/api/v2/collections/collection%20slug/stats"
        );
        assert_eq!(
            opensea_thirty_day_volume(&serde_json::json!({
                "intervals":[
                    {"interval":"one_day","volume":1},
                    {"interval":"thirty_day","volume":12.5}
                ]
            })),
            Some(12.5)
        );
    }

    #[test]
    fn solana_seed_request_and_parsing_use_magic_eden_thirty_day_rank() {
        let source = MagicEdenSeedSource {
            client: reqwest::Client::new(),
            base_endpoint: "https://example.com/v2".into(),
            helius_endpoint: "https://helius.example.com/".into(),
            helius_api_key: "secret".into(),
            retries: 0,
            request_permits: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        assert_eq!(
            source.popular_request().unwrap().url().as_str(),
            "https://example.com/v2/marketplace/popular_collections?timeRange=30d"
        );
        assert_eq!(
            source
                .collection_listings_request("collection symbol")
                .unwrap()
                .url()
                .as_str(),
            "https://example.com/v2/collections/collection%20symbol/listings?offset=0&limit=1"
        );
        assert_eq!(
            magic_eden_popular_collections(&serde_json::json!([
                {"symbol":"popular", "name":"Popular"},
                {"symbol":"popular", "name":"Duplicate"}
            ])),
            vec![MagicEdenCollection {
                symbol: "popular".into(),
                name: "Popular".into(),
                rank: 1,
            }]
        );
        assert_eq!(
            magic_eden_listing_mint(&serde_json::json!([{
                "tokenMint":"So11111111111111111111111111111111111111112"
            }])),
            Some("So11111111111111111111111111111111111111112".into())
        );
        assert_eq!(
            helius_collection_address(&serde_json::json!({
                "grouping":[{
                    "group_key":"collection",
                    "group_value":"11111111111111111111111111111111"
                }]
            })),
            Some("11111111111111111111111111111111".into())
        );
    }

    #[test]
    fn seed_addresses_are_validated_per_chain() {
        assert!(valid_contract_address(
            ChainId::Ethereum,
            "0x1111111111111111111111111111111111111111"
        ));
        assert!(!valid_contract_address(ChainId::Ethereum, "0x1"));
        assert!(valid_contract_address(
            ChainId::Solana,
            "So11111111111111111111111111111111111111112"
        ));
        assert!(!valid_contract_address(ChainId::Solana, "not-base58-0"));
    }
}
