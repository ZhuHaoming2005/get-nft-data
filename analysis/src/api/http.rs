use crate::error::{AnalysisError, Result};
use crate::pipeline::{CpuExecutor, CpuTaskKind};
use reqwest::header::HeaderMap;
use reqwest::Method;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ProviderHttpClient {
    provider: &'static str,
    http: reqwest::Client,
    in_flight: Arc<tokio::sync::Semaphore>,
    rate: Option<Arc<RateGate>>,
    retries: usize,
    response_limit: u64,
    executor: Arc<CpuExecutor>,
}

struct RateGate {
    tokens: Arc<tokio::sync::Semaphore>,
    burst: usize,
}

impl RateGate {
    fn new(burst: usize, refill: Duration) -> Arc<Self> {
        let gate = Arc::new(Self {
            tokens: Arc::new(tokio::sync::Semaphore::new(burst.max(1))),
            burst: burst.max(1),
        });
        let tokens = gate.tokens.clone();
        let burst = gate.burst;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(refill.max(Duration::from_millis(1)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                interval.tick().await;
                if tokens.available_permits() < burst {
                    tokens.add_permits(1);
                }
            }
        });
        gate
    }

    async fn acquire(&self) -> Result<()> {
        self.tokens
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AnalysisError::State("provider rate limiter closed".into()))?
            .forget();
        Ok(())
    }
}

impl ProviderHttpClient {
    pub fn concurrent(
        provider: &'static str,
        timeout: Duration,
        concurrency: usize,
        retries: usize,
        response_limit: u64,
        executor: Arc<CpuExecutor>,
    ) -> Result<Self> {
        Self::new(
            provider,
            timeout,
            concurrency,
            retries,
            response_limit,
            executor,
            None,
        )
    }

    pub fn rate_limited(
        provider: &'static str,
        timeout: Duration,
        concurrency: usize,
        refill: Duration,
        retries: usize,
        response_limit: u64,
        executor: Arc<CpuExecutor>,
    ) -> Result<Self> {
        Self::new(
            provider,
            timeout,
            concurrency,
            retries,
            response_limit,
            executor,
            Some(refill),
        )
    }

    fn new(
        provider: &'static str,
        timeout: Duration,
        concurrency: usize,
        retries: usize,
        response_limit: u64,
        executor: Arc<CpuExecutor>,
        refill: Option<Duration>,
    ) -> Result<Self> {
        Ok(Self {
            provider,
            http: reqwest::Client::builder()
                .timeout(timeout)
                .pool_idle_timeout(Duration::from_secs(90))
                .tcp_keepalive(Duration::from_secs(60))
                .build()?,
            in_flight: Arc::new(tokio::sync::Semaphore::new(concurrency.max(1))),
            rate: refill.map(|interval| RateGate::new(concurrency, interval)),
            retries,
            response_limit,
            executor,
        })
    }

    pub async fn get(&self, url: reqwest::Url, headers: HeaderMap) -> Result<Value> {
        self.request(Method::GET, url, headers, None).await
    }

    pub async fn post(&self, url: reqwest::Url, headers: HeaderMap, body: &Value) -> Result<Value> {
        self.request(Method::POST, url, headers, Some(body)).await
    }

    pub async fn normalize_response<T, F>(&self, task: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        self.executor
            .execute_async_kind(CpuTaskKind::ResponseDecode, task)
            .await
    }

    async fn request(
        &self,
        method: Method,
        url: reqwest::Url,
        headers: HeaderMap,
        body: Option<&Value>,
    ) -> Result<Value> {
        let mut last_error = None;
        for attempt in 0..=self.retries {
            if let Some(rate) = &self.rate {
                rate.acquire().await?;
            }
            let _permit =
                self.in_flight.acquire().await.map_err(|_| {
                    AnalysisError::State("provider concurrency limiter closed".into())
                })?;
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .headers(headers.clone());
            if let Some(body) = body {
                request = request.json(body);
            }
            let result = match request.send().await {
                Ok(response) => crate::api::response_bytes(response, self.response_limit).await,
                Err(error) => Err(AnalysisError::ApiRequest {
                    message: format!(
                        "{} transport failure: {}",
                        self.provider,
                        error.without_url()
                    ),
                    retryable: true,
                    retry_after_ms: None,
                }),
            };
            drop(_permit);
            match result {
                Ok(bytes) => {
                    match self
                        .executor
                        .execute_async_kind(CpuTaskKind::ResponseDecode, move || {
                            serde_json::from_slice(&bytes).map_err(Into::into)
                        })
                        .await
                    {
                        Ok(value) => {
                            if let Some(error) = retryable_application_error(self.provider, &value)
                            {
                                last_error = Some(error);
                                if attempt >= self.retries {
                                    break;
                                }
                            } else {
                                return Ok(value);
                            }
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
                Err(error) => {
                    let retryable = error.retryable();
                    last_error = Some(error);
                    if attempt >= self.retries || !retryable {
                        break;
                    }
                }
            }
            if attempt < self.retries {
                tokio::time::sleep(Duration::from_millis(
                    last_error
                        .as_ref()
                        .and_then(AnalysisError::retry_after_ms)
                        .unwrap_or_else(|| 100_u64.saturating_mul(1_u64 << attempt.min(8))),
                ))
                .await;
            }
        }
        Err(last_error.expect("provider request performs at least one attempt"))
    }
}

fn retryable_application_error(provider: &str, value: &Value) -> Option<AnalysisError> {
    if let Some(error) = value.as_object().and_then(|object| object.get("error")) {
        return classify_application_error(provider, error);
    }
    let rows = value.as_array()?;
    if rows.is_empty() {
        return None;
    }
    let errors = rows
        .iter()
        .map(|row| {
            row.as_object()
                .and_then(|object| object.get("error"))
                .and_then(|error| classify_application_error(provider, error))
        })
        .collect::<Option<Vec<_>>>()?;
    errors.into_iter().next()
}

fn classify_application_error(provider: &str, error: &Value) -> Option<AnalysisError> {
    let code = error
        .get("code")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .unwrap_or_default();
    let message = error
        .as_str()
        .or_else(|| error.get("message").and_then(Value::as_str))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let retryable = matches!(code, 429 | -32000 | -32005 | -32603)
        || [
            "rate limit",
            "too many request",
            "temporar",
            "timeout",
            "timed out",
            "try again",
            "unavailable",
            "overloaded",
        ]
        .iter()
        .any(|needle| message.contains(needle));
    retryable.then(|| AnalysisError::ApiRequest {
        message: format!("{provider} application failure: {error}"),
        retryable: true,
        retry_after_ms: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    #[test]
    fn retryable_json_rpc_errors_are_classified_before_provider_parsing() {
        let error = retryable_application_error(
            "helius",
            &json!({"jsonrpc": "2.0", "error": {"code": -32005, "message": "rate limit"}}),
        )
        .expect("temporary JSON-RPC failure should retry");
        assert!(error.retryable());
        assert!(retryable_application_error(
            "alchemy",
            &json!({"error": {"code": -32602, "message": "invalid params"}})
        )
        .is_none());
        assert!(retryable_application_error("alchemy", &json!({"result": []})).is_none());
        assert!(retryable_application_error(
            "alchemy",
            &json!([
                {"id":"a","error":{"code":-32005,"message":"rate limit"}},
                {"id":"b","error":{"code":-32005,"message":"rate limit"}}
            ])
        )
        .is_some());
        assert!(retryable_application_error(
            "alchemy",
            &json!([
                {"id":"a","result":{}},
                {"id":"b","error":{"code":-32005,"message":"rate limit"}}
            ])
        )
        .is_none());
    }

    #[tokio::test]
    async fn http_200_json_rpc_rate_limit_is_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for payload in [
                json!({"jsonrpc":"2.0","error":{"code":-32005,"message":"rate limit"}}),
                json!({"jsonrpc":"2.0","result":{"ok":true}}),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).unwrap();
                let body = payload.to_string();
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
            Duration::from_secs(2),
            1,
            1,
            1024 * 1024,
            Arc::new(crate::pipeline::CpuExecutor::new(1).unwrap()),
        )
        .unwrap();
        let value = client
            .get(
                reqwest::Url::parse(&format!("http://{address}/rpc")).unwrap(),
                HeaderMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(value["result"]["ok"], true);
        server.join().unwrap();
    }
}
