use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
use serde_json::Value;
use std::future::Future;

use crate::api::AsyncApiClient;
use crate::error::AppError;

pub const ETH_USD_PRICE_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd";
pub const COINBASE_ETH_USD_SPOT_URL: &str = "https://api.coinbase.com/v2/prices/ETH-USD/spot";
const DEFAULT_ETH_USD_RATE_ATTEMPTS: usize = 2;

const ETH_LIKE_SYMBOLS: &[&str] = &["ETH", "WETH"];
const STABLECOIN_SYMBOLS: &[&str] = &[
    "USDC", "USDT", "DAI", "USDS", "USDE", "FDUSD", "TUSD", "PYUSD", "GUSD", "USDP", "LUSD",
    "SUSD", "FRAX",
];

pub fn is_native_eth_symbol(symbol: &str) -> bool {
    symbol.trim().eq_ignore_ascii_case("ETH")
}

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

pub fn to_eth_equivalent(amount: f64, symbol: &str, eth_usd_rate: Option<f64>) -> Option<f64> {
    to_normalized_amount(amount, symbol, eth_usd_rate).eth
}

#[derive(Default)]
pub(crate) struct EthUsdRateCache {
    value: futures::lock::Mutex<Option<Result<f64, String>>>,
}

impl EthUsdRateCache {
    pub(crate) async fn get_or_try_init<F, Fut>(&self, fetch: F) -> Result<f64, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<f64, AppError>>,
    {
        let mut value = self.value.lock().await;
        if let Some(result) = value.as_ref() {
            return result
                .clone()
                .map_err(|err| AppError::InvalidData(err.to_string()));
        }

        let result = fetch().await.map_err(|err| err.to_string());
        let output = result
            .clone()
            .map_err(|err| AppError::InvalidData(err.to_string()));
        *value = Some(result);
        output
    }
}

#[derive(Clone, Copy)]
pub(crate) enum EthUsdPriceParser {
    CoinGecko,
    CoinbaseSpot,
}

pub(crate) struct EthUsdPriceSource {
    url: String,
    parser: EthUsdPriceParser,
}

impl EthUsdPriceSource {
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
    fetch_current_eth_usd_rate_from_urls(
        client,
        &[
            EthUsdPriceSource::coin_gecko(ETH_USD_PRICE_URL),
            EthUsdPriceSource::coinbase_spot(COINBASE_ETH_USD_SPOT_URL),
        ],
    )
    .await
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
}
