//! Shared HTTP client scaffolding for seed selection and enrichment.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::error::Analysis2Error;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RETRIES: usize = 2;
const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

/// Concurrent HTTP helper with finite retries.
#[derive(Clone)]
pub struct HttpClient {
    http: reqwest::Client,
    in_flight: Arc<Semaphore>,
    retries: usize,
}

impl HttpClient {
    pub fn new(concurrency: usize) -> Result<Self, Analysis2Error> {
        Self::with_retries(concurrency, DEFAULT_RETRIES)
    }

    pub fn with_retries(concurrency: usize, retries: usize) -> Result<Self, Analysis2Error> {
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .map_err(|e| Analysis2Error::http(e.to_string()))?;
        Ok(Self {
            http,
            in_flight: Arc::new(Semaphore::new(concurrency.max(1))),
            retries,
        })
    }

    pub async fn get_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, Analysis2Error> {
        self.request(reqwest::Method::GET, url, headers, None).await
    }

    pub async fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, Analysis2Error> {
        self.request(reqwest::Method::POST, url, headers, Some(body))
            .await
    }

    async fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Value, Analysis2Error> {
        let header_map = build_headers(headers)?;
        let mut last_error = None;
        for attempt in 0..=self.retries {
            let _permit = self
                .in_flight
                .acquire()
                .await
                .map_err(|_| Analysis2Error::http("HTTP concurrency pool closed"))?;
            let mut builder = self.http.request(method.clone(), url).headers(header_map.clone());
            if let Some(body) = body {
                builder = builder.json(body);
            }
            let result = match builder.send().await {
                Ok(response) => read_json_response(response).await,
                Err(error) => Err(Analysis2Error::http(error.without_url().to_string())),
            };
            drop(_permit);
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let retryable = is_retryable(&error);
                    last_error = Some(error);
                    if attempt >= self.retries || !retryable {
                        break;
                    }
                    let backoff_ms = 100u64.saturating_mul(1u64 << attempt.min(8));
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Analysis2Error::http("HTTP request failed")))
    }
}

fn build_headers(headers: &[(&str, &str)]) -> Result<HeaderMap, Analysis2Error> {
    let mut map = HeaderMap::new();
    map.insert(
        reqwest::header::ACCEPT,
        HeaderValue::from_static("application/json"),
    );
    map.insert(
        reqwest::header::USER_AGENT,
        HeaderValue::from_static("analysis2-select-seeds/0.1"),
    );
    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Analysis2Error::http(format!("invalid header name {name}: {e}")))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|e| Analysis2Error::http(format!("invalid header value: {e}")))?;
        map.insert(header_name, header_value);
    }
    Ok(map)
}

async fn read_json_response(response: reqwest::Response) -> Result<Value, Analysis2Error> {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| Analysis2Error::http(e.to_string()))?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(Analysis2Error::http(format!(
            "response exceeds {MAX_RESPONSE_BYTES} bytes"
        )));
    }
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        let snippet: String = body.chars().take(200).collect();
        return Err(Analysis2Error::http(format!(
            "HTTP {status}: {snippet}"
        )));
    }
    serde_json::from_slice(&bytes).map_err(|e| Analysis2Error::http(format!("invalid JSON: {e}")))
}

fn is_retryable(error: &Analysis2Error) -> bool {
    match error {
        Analysis2Error::Http(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("timeout")
                || lower.contains("timed out")
                || lower.contains("connection")
                || lower.contains("http 429")
                || lower.contains("http 500")
                || lower.contains("http 502")
                || lower.contains("http 503")
                || lower.contains("http 504")
        }
        _ => false,
    }
}
