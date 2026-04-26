use serde_json::Value;

use crate::api::AsyncApiClient;
use crate::error::AppError;

pub const ETH_USD_PRICE_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd";

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

pub async fn fetch_current_eth_usd_rate(client: &AsyncApiClient) -> Result<f64, AppError> {
    let payload: Value = client.get_json(ETH_USD_PRICE_URL).await?;
    let rate = payload
        .get("ethereum")
        .and_then(|ethereum| ethereum.get("usd"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    if rate.is_finite() && rate > 0.0 {
        Ok(rate)
    } else {
        Err(AppError::InvalidData(format!(
            "invalid ETH/USD price response: {payload}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
