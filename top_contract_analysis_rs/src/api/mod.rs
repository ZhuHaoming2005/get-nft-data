use std::sync::Arc;
use std::time::Duration;

use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::error::AppError;

pub mod alchemy;
pub mod etherscan;
pub mod opensea;

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_ALCHEMY_RETRIES: usize = 3;

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

#[derive(Clone)]
pub struct AsyncApiClient {
    pub http: reqwest::Client,
    pub request_limit: Arc<Semaphore>,
    pub contract_limit: Arc<Semaphore>,
    pub sale_metric_limit: Arc<Semaphore>,
    retries: usize,
}

impl AsyncApiClient {
    pub fn new(
        timeout_seconds: u64,
        max_concurrency: usize,
        contract_max_concurrency: usize,
        sale_metric_max_concurrency: usize,
    ) -> Result<Self, AppError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()?;
        Ok(Self {
            http,
            request_limit: Arc::new(Semaphore::new(max_concurrency.max(1))),
            contract_limit: Arc::new(Semaphore::new(contract_max_concurrency.max(1))),
            sale_metric_limit: Arc::new(Semaphore::new(sale_metric_max_concurrency.max(1))),
            retries: DEFAULT_ALCHEMY_RETRIES,
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
        let mut last_error: Option<reqwest::Error> = None;
        for _ in 0..self.retries {
            let _permit = self
                .request_limit
                .clone()
                .acquire_owned()
                .await
                .map_err(|err| AppError::Http(err.to_string()))?;
            let mut builder = self.http.request(method.clone(), url);
            if let Some(headers) = headers.clone() {
                builder = builder.headers(headers);
            }
            if let Some(payload) = body {
                builder = builder.json(payload);
            }
            match builder.send().await {
                Ok(response) => match response.error_for_status() {
                    Ok(ok) => return Ok(ok.json::<T>().await?),
                    Err(err) => last_error = Some(err),
                },
                Err(err) => last_error = Some(err),
            }
        }
        Err(AppError::Http(
            last_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "request failed".to_string()),
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

pub use alchemy::{
    fetch_contract_metadata, fetch_contract_owners, fetch_contract_transfers, fetch_eth_balance,
    fetch_license_sample, fetch_same_block_eth_transfers_for_address, fetch_seed_contract_nfts,
    fetch_transaction_receipt, fetch_transaction_receipts_for_block, is_open_license_payload,
};
pub use opensea::{
    fetch_contract_sales, fetch_opensea_contract_metadata, fetch_opensea_contract_nfts,
};
