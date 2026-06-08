use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::error::AppError;
use crate::models::{ContractMetadata, SeedNft};

pub mod alchemy;
pub mod etherscan;
pub mod opensea;

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_API_RETRIES: usize = 5;
pub const DEFAULT_API_RETRY_DELAY_MS: u64 = 500;
pub const DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS: u64 = 300;
pub const DEFAULT_OTHER_API_RATE_LIMIT_BURST: usize = 4;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct ApiEndpoints {
    pub alchemy_nft_v2_base: String,
    pub alchemy_nft_v3_base: String,
    pub alchemy_rpc_base: String,
    pub etherscan_base: String,
    pub opensea_base: String,
}

impl ApiEndpoints {
    pub fn for_alchemy(network: &str, api_key: &str) -> Self {
        Self {
            alchemy_nft_v2_base: format!("https://{network}.g.alchemy.com/nft/v2/{api_key}"),
            alchemy_nft_v3_base: format!("https://{network}.g.alchemy.com/nft/v3/{api_key}"),
            alchemy_rpc_base: format!("https://{network}.g.alchemy.com/v2/{api_key}"),
            etherscan_base: "https://api.etherscan.io/v2/api".to_string(),
            opensea_base: "https://api.opensea.io".to_string(),
        }
    }
}

#[derive(Clone, Copy)]
enum RequestLimitMode {
    InFlight,
    Rate,
}

#[derive(Clone)]
pub struct AsyncApiClient {
    pub http: reqwest::Client,
    pub request_limit: Arc<Semaphore>,
    limit_mode: RequestLimitMode,
    retries: usize,
    retry_delay: Duration,
}

impl AsyncApiClient {
    pub fn new(timeout_seconds: u64, max_concurrency: usize) -> Result<Self, AppError> {
        Self::new_with_retry_policy(
            timeout_seconds,
            max_concurrency,
            DEFAULT_API_RETRIES,
            Duration::from_millis(DEFAULT_API_RETRY_DELAY_MS),
        )
    }

    pub fn new_with_retry_policy(
        timeout_seconds: u64,
        max_concurrency: usize,
        retries: usize,
        retry_delay: Duration,
    ) -> Result<Self, AppError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()?;
        Ok(Self {
            http,
            request_limit: Arc::new(Semaphore::new(max_concurrency.max(1))),
            limit_mode: RequestLimitMode::InFlight,
            retries: retries.max(1),
            retry_delay,
        })
    }

    pub fn new_rate_limited(timeout_seconds: u64, max_burst: usize) -> Result<Self, AppError> {
        Self::new_rate_limited_with_retry_policy(
            timeout_seconds,
            max_burst,
            Duration::from_millis(DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS),
            DEFAULT_API_RETRIES,
            Duration::from_millis(DEFAULT_API_RETRY_DELAY_MS),
        )
    }

    pub fn new_rate_limited_with_retry_policy(
        timeout_seconds: u64,
        max_burst: usize,
        refill_interval: Duration,
        retries: usize,
        retry_delay: Duration,
    ) -> Result<Self, AppError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()?;
        let max_burst = max_burst.max(1);
        let request_limit = Arc::new(Semaphore::new(1));
        spawn_rate_limiter_refill(request_limit.clone(), max_burst, refill_interval);
        Ok(Self {
            http,
            request_limit,
            limit_mode: RequestLimitMode::Rate,
            retries: retries.max(1),
            retry_delay,
        })
    }

    async fn request_json<T, B>(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&B>,
        headers: Option<HeaderMap>,
    ) -> Result<T, AppError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let mut last_error: Option<String> = None;
        for attempt in 1..=self.retries {
            let (attempt_error, retry_after) = {
                let _permit = match self.limit_mode {
                    RequestLimitMode::InFlight => Some(
                        self.request_limit
                            .clone()
                            .acquire_owned()
                            .await
                            .map_err(|err| AppError::Http(err.to_string()))?,
                    ),
                    RequestLimitMode::Rate => {
                        let permit = self
                            .request_limit
                            .clone()
                            .acquire_owned()
                            .await
                            .map_err(|err| AppError::Http(err.to_string()))?;
                        permit.forget();
                        None
                    }
                };
                let mut builder = self.http.request(method.clone(), url);
                if let Some(headers) = headers.clone() {
                    builder = builder.headers(headers);
                }
                if let Some(payload) = body {
                    builder = builder.json(payload);
                }
                match builder.send().await {
                    Ok(response) => {
                        let status = response.status();
                        if status.is_success() {
                            return Ok(response.json::<T>().await?);
                        }
                        let retry_after = retry_after_delay(response.headers());
                        let body = response.text().await.unwrap_or_else(|err| {
                            format!("<failed to read error response body: {err}>")
                        });
                        let status_kind = if status.is_client_error() {
                            "client error"
                        } else if status.is_server_error() {
                            "server error"
                        } else {
                            "unexpected status"
                        };
                        let message = format!(
                            "HTTP status {status_kind} ({status}) for url ({url}); response body: {}",
                            response_body_excerpt(&body)
                        );
                        if should_retry_status(status) {
                            (message, retry_after)
                        } else {
                            return Err(AppError::Http(message));
                        }
                    }
                    Err(err) => (err.to_string(), None),
                }
            };
            last_error = Some(attempt_error);
            if attempt < self.retries {
                let delay = retry_delay_for_attempt(attempt, self.retry_delay, retry_after, url);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Err(AppError::Http(
            last_error.unwrap_or_else(|| "request failed".to_string()),
        ))
    }

    pub async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, AppError> {
        self.request_json::<T, serde_json::Value>(reqwest::Method::GET, url, None, None)
            .await
    }

    pub async fn get_json_with_headers<T: DeserializeOwned>(
        &self,
        url: &str,
        headers: HeaderMap,
    ) -> Result<T, AppError> {
        self.request_json::<T, serde_json::Value>(reqwest::Method::GET, url, None, Some(headers))
            .await
    }

    pub async fn post_json<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, AppError> {
        self.request_json(reqwest::Method::POST, url, Some(body), None)
            .await
    }
}

fn spawn_rate_limiter_refill(
    request_limit: Arc<Semaphore>,
    max_burst: usize,
    refill_interval: Duration,
) {
    let weak_limit = Arc::downgrade(&request_limit);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(refill_interval).await;
            let Some(limit) = weak_limit.upgrade() else {
                break;
            };
            if limit.available_permits() < max_burst {
                limit.add_permits(1);
            }
        }
    });
}

fn should_retry_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after)
        .map(|delay| delay.min(MAX_RETRY_DELAY))
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = chrono::DateTime::parse_from_rfc2822(trimmed)
        .ok()?
        .with_timezone(&chrono::Utc);
    let now = chrono::Utc::now();
    if retry_at <= now {
        Some(Duration::ZERO)
    } else {
        (retry_at - now).to_std().ok()
    }
}

fn retry_delay_for_attempt(
    attempt: usize,
    base_delay: Duration,
    retry_after: Option<Duration>,
    url: &str,
) -> Duration {
    if let Some(delay) = retry_after {
        return delay.min(MAX_RETRY_DELAY);
    }
    if base_delay.is_zero() {
        return Duration::ZERO;
    }

    let exponent = attempt.saturating_sub(1).min(6) as u32;
    let multiplier = 1_u128 << exponent;
    let base_ms = base_delay.as_millis();
    let exponential_ms = base_ms.saturating_mul(multiplier);
    let jitter_window_ms = (base_ms / 4).max(1);
    let url_hash = url.bytes().fold(0_u128, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(byte as u128)
    });
    let jitter_ms =
        (url_hash.wrapping_add((attempt as u128).wrapping_mul(17))) % (jitter_window_ms + 1);
    let delay_ms = exponential_ms
        .saturating_add(jitter_ms)
        .min(MAX_RETRY_DELAY.as_millis())
        .min(u64::MAX as u128);
    Duration::from_millis(delay_ms as u64)
}

fn response_body_excerpt(body: &str) -> String {
    const MAX_RESPONSE_BODY_CHARS: usize = 1024;
    let compact = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_RESPONSE_BODY_CHARS {
        compact
    } else {
        let truncated = compact
            .chars()
            .take(MAX_RESPONSE_BODY_CHARS)
            .collect::<String>();
        format!("{truncated}...")
    }
}

pub use alchemy::{
    fetch_alchemy_contract_collection_slug, fetch_contract_metadata, fetch_contract_owners,
    fetch_contract_total_supply, fetch_contract_transfers,
    fetch_contract_transfers_with_etherscan_fallback, fetch_eth_balance,
    fetch_is_holder_of_contract, fetch_license_sample, fetch_same_block_eth_transfers_for_address,
    fetch_same_block_value_transfers_for_address, fetch_same_block_value_transfers_to_address,
    fetch_seed_contract_nfts, fetch_transaction_receipt, fetch_transaction_receipts_for_block,
    is_open_license_payload,
};
pub use etherscan::fetch_etherscan_contract_transfers;
pub use opensea::{
    fetch_contract_sales, fetch_contract_sales_with_clients,
    fetch_opensea_account_holds_contract_nft, fetch_opensea_contract_collection_slug,
    fetch_opensea_contract_market_events, fetch_opensea_contract_metadata,
    fetch_opensea_contract_nfts,
};

pub async fn fetch_contract_metadata_with_opensea_fallback(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
) -> Result<ContractMetadata, AppError> {
    fetch_contract_metadata_with_opensea_fallback_clients(
        client,
        client,
        endpoints,
        chain,
        contract_address,
        opensea_api_key,
    )
    .await
}

pub async fn fetch_contract_metadata_with_opensea_fallback_clients(
    alchemy_client: &AsyncApiClient,
    other_client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
) -> Result<ContractMetadata, AppError> {
    match fetch_contract_metadata(alchemy_client, endpoints, chain, contract_address).await {
        Ok(metadata) => Ok(metadata),
        Err(err) if opensea_api_key.trim().is_empty() => Err(err),
        Err(err) => {
            eprintln!(
                "warning: Alchemy contract info failed for {contract_address}: {err}; falling back to OpenSea"
            );
            fetch_opensea_contract_metadata(
                other_client,
                &endpoints.opensea_base,
                chain,
                contract_address,
                opensea_api_key,
            )
            .await
        }
    }
}

pub async fn fetch_contract_nfts_with_fallback_clients(
    alchemy_client: &AsyncApiClient,
    other_client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    etherscan_api_key: &str,
    opensea_api_key: &str,
) -> Result<Vec<SeedNft>, AppError> {
    match fetch_seed_contract_nfts(alchemy_client, endpoints, chain, contract_address).await {
        Ok(rows) if !rows.is_empty() => Ok(rows),
        Ok(rows) => {
            if opensea_api_key.trim().is_empty() {
                return Ok(rows);
            }
            match fetch_opensea_contract_nfts(
                other_client,
                &endpoints.opensea_base,
                chain,
                contract_address,
                opensea_api_key,
            )
            .await
            {
                Ok(opensea_rows) if !opensea_rows.is_empty() => Ok(opensea_rows),
                Ok(_) => Ok(rows),
                Err(err) => {
                    eprintln!(
                        "warning: OpenSea NFT expansion failed for {contract_address}: {err}; using Alchemy NFT result"
                    );
                    Ok(rows)
                }
            }
        }
        Err(alchemy_err) => {
            if !opensea_api_key.trim().is_empty() {
                eprintln!(
                    "warning: Alchemy NFT expansion failed for {contract_address}: {alchemy_err}; falling back to OpenSea"
                );
                match fetch_opensea_contract_nfts(
                    other_client,
                    &endpoints.opensea_base,
                    chain,
                    contract_address,
                    opensea_api_key,
                )
                .await
                {
                    Ok(rows) if !rows.is_empty() => return Ok(rows),
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "warning: OpenSea NFT expansion failed for {contract_address}: {err}; falling back to Etherscan transfers"
                        );
                    }
                }
            }

            if etherscan_api_key.trim().is_empty() {
                return Err(alchemy_err);
            }
            eprintln!(
                "warning: Alchemy NFT expansion failed for {contract_address}: {alchemy_err}; falling back to Etherscan transfers"
            );
            let transfers = fetch_etherscan_contract_transfers(
                other_client,
                &endpoints.etherscan_base,
                etherscan_api_key,
                chain,
                contract_address,
                "ERC721",
            )
            .await?;
            let mut seen = BTreeSet::new();
            let mut rows = Vec::new();
            for transfer in transfers {
                if transfer.token_id.is_empty() || !seen.insert(transfer.token_id.clone()) {
                    continue;
                }
                rows.push(SeedNft {
                    chain: chain.to_string(),
                    contract_address: contract_address.to_lowercase(),
                    token_id: transfer.token_id,
                    ..SeedNft::default()
                });
            }
            Ok(rows)
        }
    }
}

pub async fn fetch_contract_collection_slug_alchemy_first(
    alchemy_client: &AsyncApiClient,
    other_client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
) -> Result<Option<String>, AppError> {
    match fetch_alchemy_contract_collection_slug(alchemy_client, endpoints, contract_address).await
    {
        Ok(Some(slug)) => Ok(Some(slug)),
        Ok(None) if opensea_api_key.trim().is_empty() => Ok(None),
        Ok(None) => {
            fetch_opensea_contract_collection_slug(
                other_client,
                &endpoints.opensea_base,
                chain,
                contract_address,
                opensea_api_key,
            )
            .await
        }
        Err(err) if opensea_api_key.trim().is_empty() => {
            eprintln!(
                "warning: Alchemy collection slug lookup failed for {contract_address}: {err}; continuing without collection slug"
            );
            Ok(None)
        }
        Err(err) => {
            eprintln!(
                "warning: Alchemy collection slug lookup failed for {contract_address}: {err}; falling back to OpenSea"
            );
            fetch_opensea_contract_collection_slug(
                other_client,
                &endpoints.opensea_base,
                chain,
                contract_address,
                opensea_api_key,
            )
            .await
        }
    }
}

#[derive(Clone, Copy)]
pub struct OpenSeaAccountFallback<'a> {
    pub api_key: &'a str,
    pub collection_slug: Option<&'a str>,
}

pub async fn fetch_account_holds_contract_alchemy_first(
    alchemy_client: &AsyncApiClient,
    other_client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    account_address: &str,
    contract_address: &str,
    opensea_fallback: OpenSeaAccountFallback<'_>,
) -> Result<bool, AppError> {
    match fetch_is_holder_of_contract(alchemy_client, endpoints, account_address, contract_address)
        .await
    {
        Ok(holds_contract_nft) => Ok(holds_contract_nft),
        Err(alchemy_err) => {
            let Some(collection_slug) = opensea_fallback.collection_slug else {
                return Err(alchemy_err);
            };
            if opensea_fallback.api_key.trim().is_empty() {
                return Err(alchemy_err);
            }
            eprintln!(
                "warning: Alchemy isHolderOfContract failed for {account_address}: {alchemy_err}; falling back to OpenSea account NFT lookup"
            );
            fetch_opensea_account_holds_contract_nft(
                other_client,
                &endpoints.opensea_base,
                chain,
                account_address,
                contract_address,
                opensea_fallback.api_key,
                Some(collection_slug),
            )
            .await
        }
    }
}
