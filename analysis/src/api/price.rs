use crate::api::http::ProviderHttpClient;
use crate::error::{AnalysisError, Result};
use crate::model::{ChainId, NormalizedEvent};
use chrono::{SecondsFormat, TimeZone, Utc};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PriceKey {
    pub chain: ChainId,
    pub time_bucket: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceObservation {
    pub key: PriceKey,
    pub usd_micros_per_native: i128,
    pub source: String,
}

#[derive(Clone)]
pub struct PriceOracle {
    alchemy: ProviderHttpClient,
    api_key: Arc<str>,
    endpoint: Arc<str>,
    cache: Arc<tokio::sync::RwLock<BTreeMap<PriceKey, i128>>>,
    chain_locks: Arc<BTreeMap<ChainId, Arc<tokio::sync::Mutex<()>>>>,
}

impl PriceOracle {
    pub fn new(alchemy: ProviderHttpClient, api_key: &str, endpoint: &str) -> Self {
        Self {
            alchemy,
            api_key: Arc::from(api_key.trim()),
            endpoint: Arc::from(endpoint.trim_end_matches('/')),
            cache: Arc::new(tokio::sync::RwLock::new(BTreeMap::new())),
            chain_locks: Arc::new(
                [
                    ChainId::Ethereum,
                    ChainId::Base,
                    ChainId::Polygon,
                    ChainId::Solana,
                ]
                .into_iter()
                .map(|chain| (chain, Arc::new(tokio::sync::Mutex::new(()))))
                .collect(),
            ),
        }
    }

    pub async fn enrich_events(&self, events: &mut [NormalizedEvent]) -> Result<()> {
        let required = events
            .iter()
            .filter(|event| event_needs_native_price(event))
            .filter_map(|event| {
                event.timestamp.map(|timestamp| PriceKey {
                    chain: price_cache_chain(event.chain),
                    time_bucket: day_bucket(timestamp),
                })
            })
            .collect::<BTreeSet<_>>();
        if required.is_empty() {
            return Ok(());
        }
        if self.api_key.is_empty() {
            return Err(AnalysisError::Config(
                "same-day native/USD pricing requires the configured Alchemy API key".into(),
            ));
        }
        for chain in [ChainId::Ethereum, ChainId::Polygon, ChainId::Solana] {
            let days = required
                .iter()
                .filter(|key| key.chain == chain)
                .map(|key| key.time_bucket)
                .collect::<BTreeSet<_>>();
            if !days.is_empty() {
                self.ensure_days(chain, &days).await?;
            }
        }
        let cache = self.cache.read().await;
        for event in events {
            let Some(timestamp) = event.timestamp else {
                continue;
            };
            let Some(rate) = cache.get(&PriceKey {
                chain: price_cache_chain(event.chain),
                time_bucket: day_bucket(timestamp),
            }) else {
                continue;
            };
            let scale = native_scale(event.chain);
            if event.usd_micros.is_none() {
                event.usd_micros = event
                    .native_amount
                    .and_then(|amount| convert(amount, *rate, scale));
            }
            if event.gas_usd_micros.is_none() {
                event.gas_usd_micros = event
                    .gas_native
                    .and_then(|amount| convert(amount, *rate, scale));
            }
            if event.marketplace_fee_usd_micros.is_none() {
                event.marketplace_fee_usd_micros = event
                    .marketplace_fee_native
                    .and_then(|amount| convert(amount, *rate, scale));
            }
        }
        Ok(())
    }

    async fn ensure_days(&self, chain: ChainId, days: &BTreeSet<i64>) -> Result<()> {
        let lock = self
            .chain_locks
            .get(&chain)
            .expect("all supported chains have a price lock")
            .clone();
        let _guard = lock.lock().await;
        let missing = {
            let cache = self.cache.read().await;
            days.iter()
                .copied()
                .filter(|day| {
                    !cache.contains_key(&PriceKey {
                        chain,
                        time_bucket: *day,
                    })
                })
                .collect::<Vec<_>>()
        };
        if missing.is_empty() {
            return Ok(());
        }
        for (range_start, range_end) in bounded_day_windows(&missing) {
            let mut start = range_start;
            while start <= range_end {
                let end = range_end.min(start.saturating_add(364 * 86_400));
                let observations = self.fetch_range(chain, start, end).await?;
                let mut cache = self.cache.write().await;
                for observation in observations {
                    cache.insert(observation.key, observation.usd_micros_per_native);
                }
                start = end.saturating_add(86_400);
            }
        }
        let unresolved = {
            let cache = self.cache.read().await;
            days.iter()
                .filter(|day| {
                    !cache.contains_key(&PriceKey {
                        chain,
                        time_bucket: **day,
                    })
                })
                .count()
        };
        if unresolved > 0 {
            return Err(AnalysisError::Provider(format!(
                "Alchemy historical price response omitted {unresolved} required {} day buckets",
                native_symbol(chain)
            )));
        }
        Ok(())
    }

    async fn fetch_range(
        &self,
        chain: ChainId,
        start: i64,
        end: i64,
    ) -> Result<Vec<PriceObservation>> {
        if chain != ChainId::Polygon {
            return self
                .fetch_symbol_range(chain, native_symbol(chain), start, end)
                .await;
        }
        let mut observations = match self
            .fetch_symbol_range(chain, native_symbol(chain), start, end)
            .await
        {
            Ok(observations) => observations,
            Err(primary_error) => {
                return self
                    .fetch_symbol_range(chain, "MATIC", start, end)
                    .await
                    .map_err(|fallback_error| {
                        AnalysisError::Provider(format!(
                            "Alchemy Polygon price lookup failed for POL ({primary_error}) and MATIC ({fallback_error})"
                        ))
                    });
            }
        };
        let covered = observations
            .iter()
            .map(|observation| observation.key.time_bucket)
            .collect::<BTreeSet<_>>();
        let expected = (start..=end).step_by(86_400).collect::<BTreeSet<_>>();
        if expected.is_subset(&covered) {
            return Ok(observations);
        }
        if let Ok(fallback) = self.fetch_symbol_range(chain, "MATIC", start, end).await {
            observations.extend(
                fallback
                    .into_iter()
                    .filter(|observation| !covered.contains(&observation.key.time_bucket)),
            );
        }
        Ok(observations)
    }

    async fn fetch_symbol_range(
        &self,
        chain: ChainId,
        symbol: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<PriceObservation>> {
        let url = reqwest::Url::parse(&format!(
            "{}/{}/tokens/historical",
            self.endpoint, self.api_key
        ))
        .map_err(|_| AnalysisError::Config("invalid Alchemy price endpoint or API key".into()))?;
        let body = json!({
            "symbol": symbol,
            "startTime": rfc3339(start)?,
            "endTime": rfc3339(end.saturating_add(86_399))?,
            "interval": "1d",
            "withMarketData": false
        });
        let payload = self.alchemy.post(url, HeaderMap::new(), &body).await?;
        if let Some(error) = payload.get("error") {
            return Err(AnalysisError::Provider(format!(
                "Alchemy historical price request failed: {error}"
            )));
        }
        let data = payload
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                AnalysisError::Provider("Alchemy historical price response omitted data".into())
            })?;
        Ok(data
            .iter()
            .filter_map(|row| {
                let timestamp = row
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())?
                    .timestamp();
                let usd_micros_per_native = decimal_usd_micros(row.get("value")?)?;
                Some(PriceObservation {
                    key: PriceKey {
                        chain,
                        time_bucket: day_bucket(timestamp),
                    },
                    usd_micros_per_native,
                    source: "alchemy".into(),
                })
            })
            .collect())
    }
}

fn event_needs_native_price(event: &NormalizedEvent) -> bool {
    (event.native_amount.is_some() && event.usd_micros.is_none())
        || (event.gas_native.is_some() && event.gas_usd_micros.is_none())
        || (event.marketplace_fee_native.is_some() && event.marketplace_fee_usd_micros.is_none())
}

fn rfc3339(timestamp: i64) -> Result<String> {
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
        .ok_or_else(|| AnalysisError::Provider(format!("invalid price timestamp {timestamp}")))
}

const fn native_symbol(chain: ChainId) -> &'static str {
    match chain {
        ChainId::Ethereum | ChainId::Base => "ETH",
        ChainId::Polygon => "POL",
        ChainId::Solana => "SOL",
    }
}

const fn price_cache_chain(chain: ChainId) -> ChainId {
    match chain {
        ChainId::Base => ChainId::Ethereum,
        other => other,
    }
}

fn bounded_day_windows(days: &[i64]) -> Vec<(i64, i64)> {
    let Some(&first) = days.first() else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let mut start = first;
    let mut end = first;
    for &day in &days[1..] {
        if day.saturating_sub(start) <= 364 * 86_400 {
            end = day;
        } else {
            ranges.push((start, end));
            start = day;
            end = day;
        }
    }
    ranges.push((start, end));
    ranges
}

const fn native_scale(chain: ChainId) -> i128 {
    match chain {
        ChainId::Solana => 1_000_000_000,
        ChainId::Ethereum | ChainId::Base | ChainId::Polygon => 1_000_000_000_000_000_000,
    }
}

fn day_bucket(timestamp: i64) -> i64 {
    timestamp.div_euclid(86_400) * 86_400
}

fn convert(amount: i128, usd_micros_per_native: i128, scale: i128) -> Option<i128> {
    amount
        .checked_mul(usd_micros_per_native)?
        .checked_div(scale)
}

fn decimal_usd_micros(value: &Value) -> Option<i128> {
    let owned;
    let raw = if let Some(raw) = value.as_str() {
        raw.trim()
    } else {
        owned = value.to_string();
        owned.as_str()
    };
    let (mantissa, exponent) = match raw.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (raw, 0_i32),
    };
    let mantissa = mantissa.strip_prefix('+').unwrap_or(mantissa);
    if mantissa.starts_with('-') {
        return None;
    }
    let (integer, fraction) = mantissa
        .split_once('.')
        .map_or((mantissa, ""), |parts| parts);
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{integer}{fraction}").parse::<i128>().ok()?;
    let scale = exponent
        .checked_sub(i32::try_from(fraction.len()).ok()?)?
        .checked_add(6)?;
    if scale >= 0 {
        digits.checked_mul(10_i128.checked_pow(scale as u32)?)
    } else {
        let divisor = 10_i128.checked_pow(scale.unsigned_abs())?;
        let quotient = digits / divisor;
        let remainder = digits % divisor;
        quotient.checked_add(i128::from(remainder >= (divisor + 1) / 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventKind, ValueChannel};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn native_base_units_convert_to_usd_micros_without_float_amounts() {
        assert_eq!(
            convert(
                2_000_000_000_000_000_000,
                3_500_000_000,
                native_scale(ChainId::Ethereum)
            ),
            Some(7_000_000_000)
        );
        assert_eq!(
            convert(500_000_000, 200_000_000, native_scale(ChainId::Solana)),
            Some(100_000_000)
        );
    }

    #[test]
    fn negative_timestamps_use_euclidean_utc_day_buckets() {
        assert_eq!(day_bucket(-1), -86_400);
        assert_eq!(day_bucket(86_401), 86_400);
    }

    #[test]
    fn sparse_price_days_are_not_expanded_into_multi_year_ranges() {
        assert_eq!(
            bounded_day_windows(&[0, 86_400, 400 * 86_400]),
            vec![(0, 86_400), (400 * 86_400, 400 * 86_400)]
        );
        assert_eq!(price_cache_chain(ChainId::Base), ChainId::Ethereum);
    }

    #[test]
    fn historical_usd_prices_use_exact_half_up_micro_rounding() {
        assert_eq!(
            decimal_usd_micros(&Value::String("2000.1234564".into())),
            Some(2_000_123_456)
        );
        assert_eq!(
            decimal_usd_micros(&Value::String("2000.1234565".into())),
            Some(2_000_123_457)
        );
        assert_eq!(decimal_usd_micros(&json!(2.5e3)), Some(2_500_000_000));
        assert_eq!(decimal_usd_micros(&Value::String("-1".into())), None);
    }

    #[tokio::test]
    async fn historical_price_days_are_single_flight_cached_and_enrich_gas() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let server_hits = hits.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let size = stream.read(&mut request).unwrap();
            assert!(
                String::from_utf8_lossy(&request[..size]).contains("/test-key/tokens/historical")
            );
            server_hits.fetch_add(1, Ordering::SeqCst);
            let body = json!({
                "symbol": "ETH",
                "currency": "usd",
                "data": [{
                    "value": "2000.00",
                    "timestamp": "2024-01-01T00:00:00Z"
                }]
            })
            .to_string();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let executor = Arc::new(crate::pipeline::CpuExecutor::new(1).unwrap());
        let client = ProviderHttpClient::concurrent(
            "alchemy",
            std::time::Duration::from_secs(2),
            2,
            0,
            1024 * 1024,
            executor,
        )
        .unwrap();
        let oracle = PriceOracle::new(client, "test-key", &format!("http://{address}"));
        let mut events = vec![NormalizedEvent {
            chain: ChainId::Ethereum,
            tx_id: Arc::from("tx"),
            event_index: 0,
            timestamp: Some(1_704_067_200),
            block_number: None,
            kind: EventKind::Sale,
            channel: Some(ValueChannel::SalePayment),
            from: None,
            to: None,
            fee_payer: None,
            payment_payer: None,
            payment_recipient: None,
            nft: None,
            native_amount: Some(1_000_000_000_000_000_000),
            usd_micros: None,
            gas_native: Some(21_000_000_000_000),
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        }];
        let mut base_event = events[0].clone();
        base_event.chain = ChainId::Base;
        base_event.tx_id = Arc::from("base-tx");
        events.push(base_event);
        oracle.enrich_events(&mut events).await.unwrap();
        oracle.enrich_events(&mut events).await.unwrap();
        assert_eq!(events[0].usd_micros, Some(2_000_000_000));
        assert_eq!(events[0].gas_usd_micros, Some(42_000));
        assert_eq!(events[1].usd_micros, Some(2_000_000_000));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn polygon_legacy_days_fall_back_from_pol_to_matic() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for (symbol, data) in [
                ("POL", json!([])),
                (
                    "MATIC",
                    json!([{
                        "value": "0.75",
                        "timestamp": "2023-01-01T00:00:00Z"
                    }]),
                ),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let size = stream.read(&mut request).unwrap();
                assert!(String::from_utf8_lossy(&request[..size])
                    .contains(&format!("\"symbol\":\"{symbol}\"")));
                let body = json!({"data": data}).to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let client = ProviderHttpClient::concurrent(
            "alchemy",
            std::time::Duration::from_secs(2),
            1,
            0,
            1024 * 1024,
            Arc::new(crate::pipeline::CpuExecutor::new(1).unwrap()),
        )
        .unwrap();
        let oracle = PriceOracle::new(client, "test-key", &format!("http://{address}"));
        let mut events = vec![NormalizedEvent {
            chain: ChainId::Polygon,
            tx_id: Arc::from("tx"),
            event_index: 0,
            timestamp: Some(1_672_531_200),
            block_number: None,
            kind: EventKind::Sale,
            channel: Some(ValueChannel::SalePayment),
            from: None,
            to: None,
            fee_payer: None,
            payment_payer: None,
            payment_recipient: None,
            nft: None,
            native_amount: Some(2_000_000_000_000_000_000),
            usd_micros: None,
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        }];
        oracle.enrich_events(&mut events).await.unwrap();
        assert_eq!(events[0].usd_micros, Some(1_500_000));
        server.join().unwrap();
    }
}
