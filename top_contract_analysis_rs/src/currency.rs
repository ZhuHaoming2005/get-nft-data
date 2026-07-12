use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
use serde_json::Value;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::api::AsyncApiClient;
use crate::error::AppError;

pub const ETH_USD_PRICE_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd";
pub const COINBASE_ETH_USD_SPOT_URL: &str = "https://api.coinbase.com/v2/prices/ETH-USD/spot";
pub const ALCHEMY_ETH_USD_PRICE_URL_PREFIX: &str = "https://api.g.alchemy.com/prices/v1";
pub const FALLBACK_ETH_USD_RATE: f64 = 2200.0;
const COINGECKO_SIMPLE_PRICE_URL: &str = "https://api.coingecko.com/api/v3/simple/price";
const DEFAULT_ETH_USD_RATE_ATTEMPTS: usize = 2;
const DEFAULT_ALCHEMY_ETH_USD_RATE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_PUBLIC_ETH_USD_RATE_TIMEOUT: Duration = Duration::from_secs(5);

const ETH_LIKE_SYMBOLS: &[&str] = &["ETH", "WETH"];
const STABLECOIN_SYMBOLS: &[&str] = &[
    "USDC", "USDT", "DAI", "USDS", "USDE", "FDUSD", "TUSD", "PYUSD", "GUSD", "USDP", "LUSD",
    "SUSD", "FRAX",
];

pub fn is_eth_like_symbol(symbol: &str) -> bool {
    ETH_LIKE_SYMBOLS
        .iter()
        .any(|candidate| symbol.trim().eq_ignore_ascii_case(candidate))
}

pub fn is_stablecoin_symbol(symbol: &str) -> bool {
    STABLECOIN_SYMBOLS
        .iter()
        .any(|candidate| symbol.trim().eq_ignore_ascii_case(candidate))
}

pub fn is_supported_priced_symbol(symbol: &str) -> bool {
    is_eth_like_symbol(symbol) || is_stablecoin_symbol(symbol)
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NormalizedCurrencyAmount {
    pub eth: Option<f64>,
    pub usd: Option<f64>,
}

pub fn to_normalized_amount(
    amount: f64,
    symbol: &str,
    eth_usd_rate: Option<f64>,
) -> NormalizedCurrencyAmount {
    if !amount.is_finite() || amount < 0.0 {
        return NormalizedCurrencyAmount::default();
    }
    if is_eth_like_symbol(symbol) {
        return NormalizedCurrencyAmount {
            eth: Some(amount),
            usd: eth_usd_rate
                .filter(|rate| rate.is_finite() && *rate > 0.0)
                .map(|rate| amount * rate),
        };
    }
    if is_stablecoin_symbol(symbol) {
        return NormalizedCurrencyAmount {
            eth: eth_usd_rate
                .filter(|rate| rate.is_finite() && *rate > 0.0)
                .map(|rate| amount / rate),
            usd: Some(amount),
        };
    }
    NormalizedCurrencyAmount::default()
}

pub fn is_chain_native_symbol(chain: &str, symbol: &str) -> bool {
    let symbol = symbol.trim();
    let candidates: &[&str] = match chain.trim().to_ascii_lowercase().as_str() {
        "ethereum" | "base" => &["ETH", "WETH"],
        // MATIC/WMATIC remain accepted for historical marketplace records.
        "polygon" => &["POL", "WPOL", "MATIC", "WMATIC"],
        "solana" => &["SOL", "WSOL"],
        _ => &[],
    };
    candidates
        .iter()
        .any(|candidate| !candidate.is_empty() && symbol.eq_ignore_ascii_case(candidate))
}

pub fn to_chain_normalized_amount(
    chain: &str,
    amount: f64,
    symbol: &str,
    native_usd_rate: Option<f64>,
) -> NormalizedCurrencyAmount {
    if !amount.is_finite() || amount < 0.0 {
        return NormalizedCurrencyAmount::default();
    }
    if is_chain_native_symbol(chain, symbol) {
        return NormalizedCurrencyAmount {
            eth: Some(amount),
            usd: native_usd_rate
                .filter(|rate| rate.is_finite() && *rate > 0.0)
                .map(|rate| amount * rate),
        };
    }
    if is_stablecoin_symbol(symbol) {
        return NormalizedCurrencyAmount {
            eth: native_usd_rate
                .filter(|rate| rate.is_finite() && *rate > 0.0)
                .map(|rate| amount / rate),
            usd: Some(amount),
        };
    }
    NormalizedCurrencyAmount::default()
}

pub fn to_eth_equivalent(amount: f64, symbol: &str, eth_usd_rate: Option<f64>) -> Option<f64> {
    to_normalized_amount(amount, symbol, eth_usd_rate).eth
}

#[derive(Default)]
pub(crate) struct EthUsdRateCache {
    value: Mutex<Option<Result<f64, String>>>,
    fetch_in_progress: AtomicBool,
}

struct EthUsdRateFetchGuard<'a> {
    fetch_in_progress: &'a AtomicBool,
}

impl Drop for EthUsdRateFetchGuard<'_> {
    fn drop(&mut self) {
        self.fetch_in_progress.store(false, AtomicOrdering::Release);
    }
}

impl EthUsdRateCache {
    fn value(&self) -> Result<MutexGuard<'_, Option<Result<f64, String>>>, AppError> {
        self.value.lock().map_err(|err| {
            AppError::InvalidData(format!("ETH/USD rate cache lock poisoned: {err}"))
        })
    }

    fn cached_value(&self) -> Result<Option<Result<f64, AppError>>, AppError> {
        Ok(self
            .value()?
            .as_ref()
            .map(|result| result.clone().map_err(AppError::InvalidData)))
    }

    #[cfg(test)]
    pub(crate) async fn get_or_try_init<F, Fut>(&self, fetch: F) -> Result<f64, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<f64, AppError>>,
    {
        if let Some(result) = self.cached_value()? {
            return result;
        }

        if self
            .fetch_in_progress
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            if let Some(result) = self.cached_value()? {
                return result;
            }
            return Err(AppError::InvalidData(
                "ETH/USD rate fetch already in progress".to_string(),
            ));
        }
        let _fetch_guard = EthUsdRateFetchGuard {
            fetch_in_progress: &self.fetch_in_progress,
        };

        let rate = fetch().await?;
        *self.value()? = Some(Ok(rate));
        Ok(rate)
    }

    pub(crate) async fn get_or_try_init_or_fallback<F, Fut>(
        &self,
        fetch: F,
        fallback_rate: f64,
    ) -> Result<f64, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<f64, AppError>>,
    {
        if let Some(result) = self.cached_value()? {
            return match result {
                Ok(rate) => Ok(rate),
                Err(_) => Ok(fallback_rate),
            };
        }

        if self
            .fetch_in_progress
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            if let Some(result) = self.cached_value()? {
                return match result {
                    Ok(rate) => Ok(rate),
                    Err(_) => Ok(fallback_rate),
                };
            }
            return Ok(fallback_rate);
        }
        let _fetch_guard = EthUsdRateFetchGuard {
            fetch_in_progress: &self.fetch_in_progress,
        };

        let rate = match fetch().await {
            Ok(rate) if rate.is_finite() && rate > 0.0 => rate,
            Ok(_) | Err(_) => fallback_rate,
        };
        *self.value()? = Some(Ok(rate));
        Ok(rate)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum EthUsdPriceParser {
    AlchemyBySymbol,
    CoinGecko,
    CoinbaseSpot,
}

pub(crate) struct EthUsdPriceSource {
    url: String,
    parser: EthUsdPriceParser,
}

impl EthUsdPriceSource {
    pub(crate) fn alchemy_by_symbol(url: &str) -> Self {
        Self {
            url: url.to_string(),
            parser: EthUsdPriceParser::AlchemyBySymbol,
        }
    }

    pub(crate) fn coin_gecko(url: &str) -> Self {
        Self {
            url: url.to_string(),
            parser: EthUsdPriceParser::CoinGecko,
        }
    }

    pub(crate) fn coinbase_spot(url: &str) -> Self {
        Self {
            url: url.to_string(),
            parser: EthUsdPriceParser::CoinbaseSpot,
        }
    }
}

fn parse_alchemy_eth_usd(payload: &Value) -> Option<f64> {
    payload
        .get("data")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| {
            item.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.eq_ignore_ascii_case("ETH"))
        })?
        .get("prices")
        .and_then(Value::as_array)?
        .iter()
        .find(|price| {
            price
                .get("currency")
                .and_then(Value::as_str)
                .is_some_and(|currency| currency.eq_ignore_ascii_case("usd"))
        })?
        .get("value")
        .and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
        })
        .filter(|rate| rate.is_finite() && *rate > 0.0)
}

fn parse_coingecko_eth_usd(payload: &Value) -> Option<f64> {
    payload
        .get("ethereum")
        .and_then(|ethereum| ethereum.get("usd"))
        .and_then(Value::as_f64)
        .filter(|rate| rate.is_finite() && *rate > 0.0)
}

fn parse_coinbase_spot_eth_usd(payload: &Value) -> Option<f64> {
    payload
        .get("data")
        .and_then(|data| data.get("amount"))
        .and_then(|amount| {
            amount
                .as_f64()
                .or_else(|| amount.as_str().and_then(|text| text.parse::<f64>().ok()))
        })
        .filter(|rate| rate.is_finite() && *rate > 0.0)
}

fn price_request_headers() -> Result<HeaderMap, AppError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .map_err(|err| AppError::Http(err.to_string()))?,
    );
    Ok(headers)
}

pub(crate) async fn fetch_current_eth_usd_rate_from_urls(
    client: &AsyncApiClient,
    sources: &[EthUsdPriceSource],
) -> Result<f64, AppError> {
    fetch_current_eth_usd_rate_from_urls_with_retries(
        client,
        sources,
        DEFAULT_ETH_USD_RATE_ATTEMPTS,
    )
    .await
}

pub(crate) async fn fetch_current_eth_usd_rate_from_urls_with_retries(
    client: &AsyncApiClient,
    sources: &[EthUsdPriceSource],
    attempts: usize,
) -> Result<f64, AppError> {
    let mut errors = Vec::new();
    let attempts = attempts.max(1);
    for source in sources {
        for attempt in 1..=attempts {
            let payload = match client
                .get_json_with_headers::<Value>(&source.url, price_request_headers()?)
                .await
            {
                Ok(payload) => payload,
                Err(err) => {
                    errors.push(format!(
                        "{} attempt {attempt}/{attempts}: {err}",
                        source.url
                    ));
                    continue;
                }
            };
            let rate = match source.parser {
                EthUsdPriceParser::AlchemyBySymbol => parse_alchemy_eth_usd(&payload),
                EthUsdPriceParser::CoinGecko => parse_coingecko_eth_usd(&payload),
                EthUsdPriceParser::CoinbaseSpot => parse_coinbase_spot_eth_usd(&payload),
            };
            if let Some(rate) = rate {
                return Ok(rate);
            }
            errors.push(format!(
                "{} attempt {attempt}/{attempts}: invalid ETH/USD price response: {payload}",
                source.url
            ));
        }
    }
    Err(AppError::InvalidData(format!(
        "all ETH/USD price sources failed: {}",
        errors.join("; ")
    )))
}

pub async fn fetch_current_eth_usd_rate(client: &AsyncApiClient) -> Result<f64, AppError> {
    let sources = public_eth_usd_price_sources();
    fetch_current_eth_usd_rate_from_urls(client, &sources).await
}

pub(crate) async fn fetch_native_usd_rate_from_url(
    client: &AsyncApiClient,
    url: &str,
    coin_id: &str,
) -> Result<f64, AppError> {
    let payload: Value = client
        .get_json_with_headers(url, price_request_headers()?)
        .await?;
    payload
        .get(coin_id)
        .and_then(|coin| coin.get("usd"))
        .and_then(Value::as_f64)
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .ok_or_else(|| {
            AppError::InvalidData(format!("invalid {coin_id}/USD price response: {payload}"))
        })
}

pub(crate) async fn fetch_current_native_usd_rate(
    client: &AsyncApiClient,
    chain: &str,
) -> Result<f64, AppError> {
    let coin_id = match chain.trim().to_ascii_lowercase().as_str() {
        "ethereum" | "base" => "ethereum",
        "polygon" => "polygon-ecosystem-token",
        "solana" => "solana",
        other => {
            return Err(AppError::InvalidData(format!(
                "unsupported chain for native/USD rate: {other}"
            )))
        }
    };
    let url = format!("{COINGECKO_SIMPLE_PRICE_URL}?ids={coin_id}&vs_currencies=usd");
    fetch_native_usd_rate_from_url(client, &url, coin_id).await
}

fn public_eth_usd_price_sources() -> [EthUsdPriceSource; 2] {
    [
        EthUsdPriceSource::coin_gecko(ETH_USD_PRICE_URL),
        EthUsdPriceSource::coinbase_spot(COINBASE_ETH_USD_SPOT_URL),
    ]
}

pub(crate) async fn fetch_current_eth_usd_rate_alchemy_first_from_sources_with_timeouts(
    alchemy_client: &AsyncApiClient,
    fallback_client: &AsyncApiClient,
    alchemy_sources: &[EthUsdPriceSource],
    fallback_sources: &[EthUsdPriceSource],
    alchemy_timeout: Duration,
    fallback_timeout: Duration,
) -> Result<f64, AppError> {
    if !alchemy_sources.is_empty() {
        if let Ok(Ok(rate)) = tokio::time::timeout(
            alchemy_timeout,
            fetch_current_eth_usd_rate_from_urls(alchemy_client, alchemy_sources),
        )
        .await
        {
            return Ok(rate);
        }
    }

    match tokio::time::timeout(
        fallback_timeout,
        fetch_current_eth_usd_rate_from_urls(fallback_client, fallback_sources),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(AppError::InvalidData(
            "ETH/USD public fallback rate fetch timed out".to_string(),
        )),
    }
}

pub async fn fetch_current_eth_usd_rate_with_timeout(
    client: &AsyncApiClient,
    timeout: Duration,
) -> Result<f64, AppError> {
    let sources = public_eth_usd_price_sources();
    match tokio::time::timeout(
        timeout,
        fetch_current_eth_usd_rate_from_urls(client, &sources),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(AppError::InvalidData(
            "ETH/USD rate fetch timed out".to_string(),
        )),
    }
}

pub async fn fetch_current_eth_usd_rate_alchemy_first(
    alchemy_client: &AsyncApiClient,
    fallback_client: &AsyncApiClient,
    alchemy_api_key: &str,
) -> Result<f64, AppError> {
    let alchemy_api_key = alchemy_api_key.trim();
    let alchemy_sources = if alchemy_api_key.is_empty() {
        Vec::new()
    } else {
        vec![EthUsdPriceSource::alchemy_by_symbol(
            &alchemy_eth_usd_price_url(alchemy_api_key),
        )]
    };
    let fallback_sources = public_eth_usd_price_sources();
    fetch_current_eth_usd_rate_alchemy_first_from_sources_with_timeouts(
        alchemy_client,
        fallback_client,
        &alchemy_sources,
        &fallback_sources,
        DEFAULT_ALCHEMY_ETH_USD_RATE_TIMEOUT,
        DEFAULT_PUBLIC_ETH_USD_RATE_TIMEOUT,
    )
    .await
}

fn alchemy_eth_usd_price_url(api_key: &str) -> String {
    format!(
        "{}/{}/tokens/by-symbol?symbols=ETH",
        ALCHEMY_ETH_USD_PRICE_URL_PREFIX,
        api_key.trim()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn converts_supported_symbols_to_eth_equivalent() {
        assert_eq!(to_eth_equivalent(1.0, "ETH", None), Some(1.0));
        assert_eq!(to_eth_equivalent(2.0, "WETH", None), Some(2.0));
        assert_eq!(to_eth_equivalent(150.0, "USDC", Some(3000.0)), Some(0.05));
        assert!(is_supported_priced_symbol("DAI"));
    }

    #[test]
    fn stablecoin_conversion_requires_current_eth_usd_rate() {
        assert_eq!(to_eth_equivalent(150.0, "USDT", None), None);
        assert_eq!(to_eth_equivalent(150.0, "USDT", Some(0.0)), None);
    }

    #[test]
    fn converts_supported_symbols_to_usd_primary_amount() {
        assert_eq!(
            to_normalized_amount(1.5, "ETH", Some(3000.0)),
            NormalizedCurrencyAmount {
                eth: Some(1.5),
                usd: Some(4500.0)
            }
        );
        assert_eq!(
            to_normalized_amount(150.0, "USDC", Some(3000.0)),
            NormalizedCurrencyAmount {
                eth: Some(0.05),
                usd: Some(150.0)
            }
        );
        assert_eq!(to_normalized_amount(150.0, "USDT", None).usd, Some(150.0));
    }

    #[test]
    fn converts_solana_and_polygon_native_symbols() {
        assert_eq!(
            to_chain_normalized_amount("solana", 2.0, "SOL", Some(100.0)),
            NormalizedCurrencyAmount {
                eth: Some(2.0),
                usd: Some(200.0),
            }
        );
        assert_eq!(
            to_chain_normalized_amount("polygon", 3.0, "WPOL", Some(0.5)),
            NormalizedCurrencyAmount {
                eth: Some(3.0),
                usd: Some(1.5),
            }
        );
        assert_eq!(
            to_chain_normalized_amount("polygon", 3.0, "WETH", Some(0.5)),
            NormalizedCurrencyAmount::default()
        );
        assert_eq!(
            to_chain_normalized_amount("solana", 2.0, "SOL", Some(100.0)).usd,
            Some(200.0)
        );
    }

    #[tokio::test]
    async fn price_fetch_falls_back_to_coinbase_when_coingecko_is_forbidden() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/coingecko");
                then.status(403);
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/coinbase");
                then.status(200).json_body_obj(&serde_json::json!({
                    "data": {
                        "amount": "3123.45",
                        "currency": "USD"
                    }
                }));
            })
            .await;

        let client = AsyncApiClient::new(5, 4).unwrap();
        let rate = fetch_current_eth_usd_rate_from_urls(
            &client,
            &[
                EthUsdPriceSource::coin_gecko(&format!("{}/coingecko", server.base_url())),
                EthUsdPriceSource::coinbase_spot(&format!("{}/coinbase", server.base_url())),
            ],
        )
        .await
        .unwrap();

        assert_eq!(rate, 3123.45);
    }

    #[tokio::test]
    async fn fetches_chain_native_usd_rate_from_coingecko_shape() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/native");
                then.status(200).json_body_obj(&serde_json::json!({
                    "solana": {"usd": 150.25}
                }));
            })
            .await;
        let client = AsyncApiClient::new(5, 2).unwrap();

        let rate = fetch_native_usd_rate_from_url(
            &client,
            &format!("{}/native", server.base_url()),
            "solana",
        )
        .await
        .unwrap();

        assert_eq!(rate, 150.25);
    }

    #[tokio::test]
    async fn price_fetch_reads_alchemy_symbol_price_before_public_fallbacks() {
        let server = MockServer::start_async().await;
        let alchemy_price = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/alchemy")
                    .query_param("symbols", "ETH");
                then.status(200).json_body_obj(&serde_json::json!({
                    "data": [{
                        "symbol": "ETH",
                        "prices": [{
                            "currency": "usd",
                            "value": "3333.21",
                            "lastUpdatedAt": "2026-06-08T00:00:00Z"
                        }]
                    }]
                }));
            })
            .await;
        let public_fallback = server
            .mock_async(|when, then| {
                when.method(GET).path("/coingecko");
                then.status(500).json_body_obj(&serde_json::json!({
                    "error": "public fallback should not be called"
                }));
            })
            .await;

        let client = AsyncApiClient::new(5, 4).unwrap();
        let rate = fetch_current_eth_usd_rate_from_urls(
            &client,
            &[
                EthUsdPriceSource::alchemy_by_symbol(&format!(
                    "{}/alchemy?symbols=ETH",
                    server.base_url()
                )),
                EthUsdPriceSource::coin_gecko(&format!("{}/coingecko", server.base_url())),
            ],
        )
        .await
        .unwrap();

        assert_eq!(rate, 3333.21);
        assert_eq!(alchemy_price.hits_async().await, 1);
        assert_eq!(public_fallback.hits_async().await, 0);
    }

    #[tokio::test]
    async fn alchemy_first_price_fetch_times_out_slow_alchemy_and_uses_public_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let alchemy_url = format!("http://{}?symbols=ETH", listener.local_addr().unwrap());
        let slow_alchemy = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 4096];
            let _ = stream.read(&mut buffer).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        });

        let public_server = MockServer::start_async().await;
        let public_price = public_server
            .mock_async(|when, then| {
                when.method(GET).path("/coingecko");
                then.status(200).json_body_obj(&serde_json::json!({
                    "ethereum": {"usd": 3456.78}
                }));
            })
            .await;

        let alchemy_client = AsyncApiClient::new(5, 4).unwrap();
        let fallback_client = AsyncApiClient::new(5, 4).unwrap();
        let rate = fetch_current_eth_usd_rate_alchemy_first_from_sources_with_timeouts(
            &alchemy_client,
            &fallback_client,
            &[EthUsdPriceSource::alchemy_by_symbol(&alchemy_url)],
            &[EthUsdPriceSource::coin_gecko(&format!(
                "{}/coingecko",
                public_server.base_url()
            ))],
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert_eq!(rate, 3456.78);
        assert_eq!(public_price.hits_async().await, 1);
        slow_alchemy.abort();
    }

    async fn spawn_sequential_status_server(
        responses: Vec<(u16, serde_json::Value)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0_u8; 4096];
                let _ = stream.read(&mut buffer).await.unwrap();
                let payload = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    payload.len(),
                    payload
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        (format!("http://{address}"), handle)
    }

    #[tokio::test]
    async fn price_fetch_retries_a_source_before_returning_rate() {
        let mut responses = vec![];
        for _ in 0..3 {
            responses.push((500, serde_json::json!({"error": "temporarily unavailable"})));
        }
        responses.push((200, serde_json::json!({"ethereum": {"usd": 3456.0}})));
        let (base_url, handle) = spawn_sequential_status_server(responses).await;

        let client = AsyncApiClient::new(5, 4).unwrap();
        let rate = fetch_current_eth_usd_rate_from_urls_with_retries(
            &client,
            &[EthUsdPriceSource::coin_gecko(&base_url)],
            2,
        )
        .await
        .unwrap();
        handle.await.unwrap();

        assert_eq!(rate, 3456.0);
    }

    #[tokio::test]
    async fn eth_usd_rate_cache_initializes_only_once() {
        let cache = EthUsdRateCache::default();
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_try_init({
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(3000.0)
                }
            })
            .await
            .unwrap();
        let second = cache
            .get_or_try_init({
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(4000.0)
                }
            })
            .await
            .unwrap();

        assert_eq!(first, 3000.0);
        assert_eq!(second, 3000.0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn eth_usd_rate_cache_retries_after_failed_initialization() {
        let cache = EthUsdRateCache::default();
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_try_init({
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(AppError::InvalidData("temporary price outage".into()))
                }
            })
            .await;
        assert!(first.is_err());

        let second = cache
            .get_or_try_init({
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(3100.0)
                }
            })
            .await
            .unwrap();

        assert_eq!(second, 3100.0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn eth_usd_rate_cache_uses_fallback_after_failed_initialization() {
        let cache = EthUsdRateCache::default();
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_try_init_or_fallback(
                {
                    let calls = Arc::clone(&calls);
                    move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err(AppError::InvalidData("temporary price outage".into()))
                    }
                },
                2200.0,
            )
            .await
            .unwrap();
        let second = cache
            .get_or_try_init_or_fallback(
                {
                    let calls = Arc::clone(&calls);
                    move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(3100.0)
                    }
                },
                2200.0,
            )
            .await
            .unwrap();

        assert_eq!(first, 2200.0);
        assert_eq!(second, 2200.0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn eth_usd_rate_cache_does_not_block_callers_while_initializing() {
        let cache = Arc::new(EthUsdRateCache::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (release_fetch, wait_for_release) = futures::channel::oneshot::channel::<()>();

        let first = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                cache
                    .get_or_try_init(move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        wait_for_release.await.unwrap();
                        Ok(3000.0)
                    })
                    .await
            })
        };

        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let second = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            cache.get_or_try_init({
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(4000.0)
                }
            }),
        )
        .await
        .expect("caller should not wait behind the in-flight price fetch");

        let err = second.expect_err("in-flight fetch should be reported as a cache miss");
        assert!(err.to_string().contains("already in progress"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        release_fetch.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 3000.0);

        let third = cache
            .get_or_try_init({
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(4000.0)
                }
            })
            .await
            .unwrap();
        assert_eq!(third, 3000.0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
