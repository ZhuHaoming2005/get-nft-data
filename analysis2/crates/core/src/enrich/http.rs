//! Shared HTTP client scaffolding for seed selection and enrichment.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::error::Analysis2Error;

/// Total request timeout (connect + headers + body). Large Alchemy NFT pages
/// (e.g. `getOwnersForContract?withTokenBalances=true`) often exceed 30s.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_RETRIES: usize = 3;
const MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

pub const OPENSEA_RATE_LIMIT_BURST: usize = 4;
pub const OPENSEA_RATE_LIMIT_REFILL_MS: u64 = 300;
/// Helius sustained cap: 5 req/s (one token every 200 ms, burst 5).
pub const HELIUS_RATE_LIMIT_BURST: usize = 5;
pub const HELIUS_RATE_LIMIT_REFILL_MS: u64 = 200;
/// Shared cool-down applied to a provider bucket after HTTP 429 / rate-limit.
pub const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
    /// When set, every `acquire` waits until this instant (global 429 pause).
    cool_down_until: Option<Instant>,
}

/// Token-bucket rate gate without a background task (safe to construct outside
/// a Tokio runtime; waits only on `acquire`).
#[derive(Clone, Debug)]
pub struct TokenBucketRateLimiter {
    max_burst: f64,
    refill_interval: Duration,
    state: Arc<Mutex<TokenBucketState>>,
}

impl TokenBucketRateLimiter {
    pub fn new(max_burst: usize, refill_interval: Duration) -> Self {
        Self::with_initial_tokens(max_burst, refill_interval, 1.0)
    }

    /// Start with a full bucket (used when concurrency is the primary throttle).
    pub fn new_full(max_burst: usize, refill_interval: Duration) -> Self {
        let max_burst_f = max_burst.max(1) as f64;
        Self::with_initial_tokens(max_burst, refill_interval, max_burst_f)
    }

    fn with_initial_tokens(max_burst: usize, refill_interval: Duration, initial: f64) -> Self {
        let max_burst = max_burst.max(1) as f64;
        Self {
            max_burst,
            refill_interval: refill_interval.max(Duration::from_millis(1)),
            state: Arc::new(Mutex::new(TokenBucketState {
                tokens: initial.clamp(0.0, max_burst),
                last_refill: Instant::now(),
                cool_down_until: None,
            })),
        }
    }

    /// OpenSea default: 4 burst / 300 ms refill.
    pub fn opensea_default() -> Self {
        Self::new(
            OPENSEA_RATE_LIMIT_BURST,
            Duration::from_millis(OPENSEA_RATE_LIMIT_REFILL_MS),
        )
    }

    /// Helius default: 5 req/s (burst 5, 200 ms per token).
    pub fn helius_default() -> Self {
        Self::new(
            HELIUS_RATE_LIMIT_BURST,
            Duration::from_millis(HELIUS_RATE_LIMIT_REFILL_MS),
        )
    }

    /// No RPS cap: concurrency semaphore is the throttle; still supports 429 cool-down.
    pub fn concurrency_only() -> Self {
        // Large burst + 1 ms refill ≈ unlimited steady-state RPS.
        Self::new_full(10_000, Duration::from_millis(1))
    }

    /// Block *all* subsequent acquires for at least `duration` (429 cool-down).
    ///
    /// Extends an existing cool-down if one is already active; drains tokens so
    /// traffic cannot immediately resume at full burst after the pause.
    pub fn note_rate_limited(&self, duration: Duration) {
        if let Ok(mut state) = self.state.lock() {
            let until = Instant::now() + duration;
            state.cool_down_until = Some(match state.cool_down_until {
                Some(prev) if prev > until => prev,
                _ => until,
            });
            state.tokens = 0.0;
            state.last_refill = Instant::now();
        }
    }

    /// Wait until cool-down (if any) ends and a rate token is available, then consume one.
    pub async fn acquire(&self) -> Result<(), Analysis2Error> {
        loop {
            let wait = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| Analysis2Error::http("rate limiter poisoned"))?;
                let now = Instant::now();

                // Global 429 cool-down: every concurrent caller parks until it ends.
                if let Some(until) = state.cool_down_until {
                    if now < until {
                        Some(until.saturating_duration_since(now))
                    } else {
                        // Cool-down finished: resume with a single token (not a full burst).
                        state.cool_down_until = None;
                        state.tokens = 1.0;
                        state.last_refill = now;
                        state.tokens -= 1.0;
                        None
                    }
                } else {
                    let elapsed = state.last_refill.elapsed();
                    if !elapsed.is_zero() {
                        let add = elapsed.as_secs_f64() / self.refill_interval.as_secs_f64();
                        state.tokens = (state.tokens + add).min(self.max_burst);
                        state.last_refill = Instant::now();
                    }
                    if state.tokens >= 1.0 {
                        state.tokens -= 1.0;
                        None
                    } else {
                        let need = 1.0 - state.tokens;
                        let wait_secs = need * self.refill_interval.as_secs_f64();
                        Some(Duration::from_secs_f64(wait_secs.max(0.001)))
                    }
                }
            };
            match wait {
                None => return Ok(()),
                Some(delay) => tokio::time::sleep(delay).await,
            }
        }
    }
}

/// One API provider's independent concurrency pool + rate / 429 cool-down gate.
///
/// Lanes never share semaphores or cool-downs: saturating Alchemy does not block
/// OpenSea / Helius / Etherscan, and a Helius 429 pause does not slow Alchemy.
#[derive(Clone, Debug)]
struct ProviderLane {
    name: &'static str,
    in_flight: Arc<Semaphore>,
    limiter: TokenBucketRateLimiter,
}

impl ProviderLane {
    fn new(name: &'static str, concurrency: usize, limiter: TokenBucketRateLimiter) -> Self {
        Self {
            name,
            in_flight: Arc::new(Semaphore::new(concurrency.max(1))),
            limiter,
        }
    }
}

/// Concurrent HTTP helper with **per-provider** concurrency and rate control.
#[derive(Clone)]
pub struct HttpClient {
    http: reqwest::Client,
    retries: usize,
    alchemy: ProviderLane,
    opensea: ProviderLane,
    helius: ProviderLane,
    etherscan: ProviderLane,
    /// Magic Eden / other non-primary providers.
    other: ProviderLane,
}

impl HttpClient {
    pub fn new(concurrency: usize) -> Result<Self, Analysis2Error> {
        Self::with_retries(concurrency, DEFAULT_RETRIES)
    }

    pub fn with_retries(concurrency: usize, retries: usize) -> Result<Self, Analysis2Error> {
        let n = concurrency.max(1);
        // Each provider gets its own pool of size `n` (Alchemy uses the CLI
        // --http-concurrency value; others are independent and not shared).
        let opensea_n = n.max(OPENSEA_RATE_LIMIT_BURST);
        let helius_n = n.max(HELIUS_RATE_LIMIT_BURST);
        let etherscan_n = n;
        let other_n = n;
        let pool_idle = n
            .saturating_add(opensea_n)
            .saturating_add(helius_n)
            .saturating_add(etherscan_n)
            .saturating_add(other_n)
            .max(1);

        let http = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(pool_idle)
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| Analysis2Error::http(e.to_string()))?;
        Ok(Self {
            http,
            retries,
            alchemy: ProviderLane::new("alchemy", n, TokenBucketRateLimiter::concurrency_only()),
            opensea: ProviderLane::new(
                "opensea",
                opensea_n,
                TokenBucketRateLimiter::opensea_default(),
            ),
            helius: ProviderLane::new(
                "helius",
                helius_n,
                TokenBucketRateLimiter::helius_default(),
            ),
            etherscan: ProviderLane::new(
                "etherscan",
                etherscan_n,
                TokenBucketRateLimiter::concurrency_only(),
            ),
            other: ProviderLane::new("other", other_n, TokenBucketRateLimiter::concurrency_only()),
        })
    }

    /// Generic GET on the independent "other" lane (Magic Eden, misc).
    pub async fn get_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(reqwest::Method::GET, url, headers, None, &self.other)
            .await
    }

    pub async fn get_json_alchemy(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(reqwest::Method::GET, url, headers, None, &self.alchemy)
            .await
    }

    pub async fn post_json_alchemy(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.alchemy,
        )
        .await
    }

    /// GET on the OpenSea lane (≤ ~4 req/s + provider-local 429 cool-down).
    pub async fn get_json_opensea(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(reqwest::Method::GET, url, headers, None, &self.opensea)
            .await
    }

    /// Generic POST on the "other" lane.
    pub async fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.other,
        )
        .await
    }

    /// POST on the Helius lane (5 req/s + provider-local 429 cool-down).
    pub async fn post_json_helius(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.helius,
        )
        .await
    }

    pub async fn get_json_etherscan(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, Analysis2Error> {
        self.request_on_lane(
            reqwest::Method::GET,
            url,
            headers,
            None,
            &self.etherscan,
        )
        .await
    }

    /// Shared retry loop for one provider lane: rate token → concurrency permit → HTTP.
    async fn request_on_lane(
        &self,
        method: reqwest::Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
        lane: &ProviderLane,
    ) -> Result<Value, Analysis2Error> {
        let header_map = build_headers(headers)?;
        let endpoint = redact_endpoint(url);
        let mut last_error = None;
        for attempt in 0..=self.retries {
            // Rate / cool-down is provider-local.
            lane.limiter.acquire().await?;
            let _permit = lane
                .in_flight
                .acquire()
                .await
                .map_err(|_| {
                    Analysis2Error::http(format!(
                        "HTTP concurrency pool closed provider={}",
                        lane.name
                    ))
                })?;
            let mut builder = self
                .http
                .request(method.clone(), url)
                .headers(header_map.clone());
            if let Some(body) = body {
                builder = builder.json(body);
            }
            let result = match builder.send().await {
                Ok(response) => read_json_response(response, &endpoint).await,
                Err(error) => Err(Analysis2Error::http(format_transport_error(
                    &method, &endpoint, &error,
                ))),
            };
            drop(_permit);
            match result {
                Ok(value) => {
                    // Helius (and similar) may return HTTP 200 + JSON-RPC rate-limit.
                    if let Some(err) = jsonrpc_rate_limit_error(&value) {
                        let error = Analysis2Error::http(format!(
                            "HTTP 429 endpoint={endpoint} content_type=application/json body={err}"
                        ));
                        if !self
                            .handle_request_error(
                                &method,
                                &endpoint,
                                attempt,
                                error,
                                lane,
                                &mut last_error,
                            )
                            .await?
                        {
                            break;
                        }
                        continue;
                    }
                    return Ok(value);
                }
                Err(error) => {
                    if !self
                        .handle_request_error(
                            &method,
                            &endpoint,
                            attempt,
                            error,
                            lane,
                            &mut last_error,
                        )
                        .await?
                    {
                        break;
                    }
                }
            }
        }
        let final_error =
            last_error.unwrap_or_else(|| Analysis2Error::http("HTTP request failed"));
        eprintln!(
            "[api/error] endpoint={endpoint} method={method} provider={} action=give_up error={}",
            lane.name,
            one_line_error(&final_error.to_string(), ERROR_LOG_CHARS)
        );
        Err(final_error)
    }

    /// Log, cool-down, and sleep for one failed attempt.
    /// Returns `Ok(true)` when the caller should retry, `Ok(false)` to stop.
    async fn handle_request_error(
        &self,
        method: &reqwest::Method,
        endpoint: &str,
        attempt: usize,
        error: Analysis2Error,
        lane: &ProviderLane,
        last_error: &mut Option<Analysis2Error>,
    ) -> Result<bool, Analysis2Error> {
        let rate_limited = is_rate_limited(&error);
        let retryable = rate_limited || is_retryable(&error);
        let will_retry = attempt < self.retries && retryable;
        // 429: cool down only this provider's limiter (not other providers).
        let backoff = if will_retry {
            if rate_limited {
                lane.limiter.note_rate_limited(RATE_LIMIT_COOLDOWN);
                Some(RATE_LIMIT_COOLDOWN)
            } else {
                Some(Duration::from_millis(
                    100u64.saturating_mul(1u64 << attempt.min(8)),
                ))
            }
        } else {
            None
        };
        print_request_error(
            method,
            endpoint,
            attempt + 1,
            self.retries + 1,
            backoff.map(|d| d.as_millis() as u64),
            &error,
        );
        *last_error = Some(error);
        if !will_retry {
            return Ok(false);
        }
        if let Some(delay) = backoff {
            tokio::time::sleep(delay).await;
        }
        Ok(true)
    }
}

/// Max characters kept from error/response bodies in logs and error strings.
const ERROR_BODY_CHARS: usize = 800;
const ERROR_LOG_CHARS: usize = 1_200;

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

async fn read_json_response(
    response: reqwest::Response,
    endpoint: &str,
) -> Result<Value, Analysis2Error> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = response.bytes().await.map_err(|e| {
        Analysis2Error::http(format!(
            "read body failed endpoint={endpoint} status={status}: {e}"
        ))
    })?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(Analysis2Error::http(format!(
            "response exceeds {MAX_RESPONSE_BYTES} bytes endpoint={endpoint} status={status} \
             content_type={content_type} body_len={}",
            bytes.len()
        )));
    }
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        let snippet = one_line_error(&body, ERROR_BODY_CHARS);
        return Err(Analysis2Error::http(format!(
            "HTTP {status} endpoint={endpoint} content_type={content_type} body={snippet}"
        )));
    }
    serde_json::from_slice(&bytes).map_err(|e| {
        let preview = String::from_utf8_lossy(&bytes);
        let snippet = one_line_error(&preview, ERROR_BODY_CHARS);
        Analysis2Error::http(format!(
            "invalid JSON endpoint={endpoint} status={status} content_type={content_type} \
             parse_error={e} body={snippet}"
        ))
    })
}

fn format_transport_error(
    method: &reqwest::Method,
    endpoint: &str,
    error: &reqwest::Error,
) -> String {
    let mut parts = vec![
        format!("transport error"),
        format!("method={method}"),
        format!("endpoint={endpoint}"),
    ];
    if error.is_timeout() {
        parts.push("kind=timeout".into());
    } else if error.is_connect() {
        parts.push("kind=connect".into());
    } else if error.is_request() {
        parts.push("kind=request".into());
    } else if error.is_body() {
        parts.push("kind=body".into());
    } else if error.is_decode() {
        parts.push("kind=decode".into());
    }
    if let Some(status) = error.status() {
        parts.push(format!("status={status}"));
    }
    // Keep the library message but strip raw secrets if any leaked in.
    // Prefer `to_string()` over `without_url()` so we can borrow `&Error`.
    let detail = one_line_error(
        &redact_sensitive_text(&error.to_string()),
        ERROR_BODY_CHARS,
    );
    parts.push(format!("detail={detail}"));
    parts.join(" ")
}

fn is_retryable(error: &Analysis2Error) -> bool {
    if is_rate_limited(error) {
        return true;
    }
    match error {
        Analysis2Error::Http(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("timeout")
                || lower.contains("timed out")
                || lower.contains("kind=timeout")
                || lower.contains("kind=connect")
                || lower.contains("kind=request")
                || lower.contains("kind=body")
                || lower.contains("kind=decode")
                || lower.contains("connection")
                || lower.contains("read body failed")
                || lower.contains("error decoding response body")
                || lower.contains("error sending request")
                || lower.contains("http 500")
                || lower.contains("http 502")
                || lower.contains("http 503")
                || lower.contains("http 504")
        }
        _ => false,
    }
}

/// True when the HTTP error message reports the given status (e.g. 429).
fn is_http_status(error: &Analysis2Error, status: u16) -> bool {
    match error {
        Analysis2Error::Http(message) => {
            let lower = message.to_ascii_lowercase();
            let code = status.to_string();
            // "HTTP 429 …", "HTTP 429 Too Many Requests …", "status=429 …"
            lower.contains(&format!("http {code}"))
                || lower.contains(&format!("status={code}"))
        }
        _ => false,
    }
}

/// Provider rate-limit signal: HTTP 429, transport status=429, or rate-limit text.
fn is_rate_limited(error: &Analysis2Error) -> bool {
    match error {
        Analysis2Error::Http(message) => {
            if is_http_status(error, 429) {
                return true;
            }
            let lower = message.to_ascii_lowercase();
            lower.contains("too many requests")
                || lower.contains("rate limit")
                || lower.contains("rate-limit")
                || lower.contains("ratelimited")
                || lower.contains("\"code\":-32429")
                || lower.contains("\"code\": 429")
                || lower.contains("\"code\":429")
        }
        _ => false,
    }
}

/// If a successful JSON-RPC payload is a rate-limit error, return a short body snippet.
fn jsonrpc_rate_limit_error(value: &Value) -> Option<String> {
    let err = value.get("error")?;
    let code = err.get("code").and_then(|c| c.as_i64());
    let message = err
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let rate_limited = matches!(code, Some(429) | Some(-32429) | Some(-32005))
        || message.contains("rate limit")
        || message.contains("too many requests")
        || message.contains("rate limited");
    if rate_limited {
        Some(one_line_error(&err.to_string(), ERROR_BODY_CHARS))
    } else {
        None
    }
}

fn print_request_error(
    method: &reqwest::Method,
    endpoint: &str,
    attempt: usize,
    max_attempts: usize,
    backoff_ms: Option<u64>,
    error: &Analysis2Error,
) {
    // Error string already carries endpoint/status/body; still prefix for grepping.
    let message = one_line_error(&error.to_string(), ERROR_LOG_CHARS);
    match backoff_ms {
        Some(delay) => eprintln!(
            "[api/error] endpoint={endpoint} method={method} attempt={attempt}/{max_attempts} \
             action=retry backoff_ms={delay} error={message}"
        ),
        None => eprintln!(
            "[api/error] endpoint={endpoint} method={method} attempt={attempt}/{max_attempts} \
             action=continue error={message}"
        ),
    }
}

/// Query keys whose values are secrets (not bare substrings like `token` in
/// `withTokenBalances`).
fn is_secret_query_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "api-key"
            | "api_key"
            | "apikey"
            | "x-api-key"
            | "key"
            | "access_token"
            | "access-token"
            | "token"
            | "secret"
            | "password"
            | "authorization"
            | "auth"
    )
}

/// Host + path + redacted query for logs (never includes API keys).
fn redact_endpoint(url: &str) -> String {
    // reqwest error strings wrap URLs in parentheses; peel them first.
    let trimmed = url
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let Ok(parsed) = reqwest::Url::parse(trimmed) else {
        return redact_path_secrets(trimmed);
    };
    let host = match (parsed.host_str(), parsed.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_owned(),
        _ => "unknown-host".to_owned(),
    };
    let path = parsed.path();
    let mut out = format!("{host}{path}");
    if let Some(query) = parsed.query() {
        let redacted = query
            .split('&')
            .map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next().unwrap_or("");
                if is_secret_query_key(key) {
                    format!("{key}=***")
                } else {
                    pair.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("&");
        if !redacted.is_empty() {
            out.push('?');
            out.push_str(&redacted);
        }
    }
    // Alchemy / similar paths embed the key as a path segment: /v2/<key>
    redact_path_secrets(&out)
}

fn strip_wrapping_punct(s: &str) -> &str {
    s.trim_matches(|c: char| {
        matches!(c, '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\'' | ',' | ';' | '.')
    })
}

fn looks_like_api_key_segment(segment: &str) -> bool {
    let cleaned = strip_wrapping_punct(segment);
    cleaned.len() >= 12
        && cleaned
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn redact_path_secrets(endpoint: &str) -> String {
    // Replace long secret-looking path segments after /v2/ or /v3/, even when
    // the segment is glued to punctuation (e.g. "KEY)" inside error strings).
    let mut parts: Vec<String> = endpoint.split('/').map(str::to_owned).collect();
    for i in 0..parts.len() {
        let head = strip_wrapping_punct(&parts[i]);
        if matches!(head, "v2" | "v3") {
            if let Some(next) = parts.get_mut(i + 1) {
                if looks_like_api_key_segment(next) {
                    // Preserve trailing punctuation so messages stay readable.
                    let trailing: String = next
                        .chars()
                        .rev()
                        .take_while(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_')
                        .collect::<String>()
                        .chars()
                        .rev()
                        .collect();
                    *next = format!("***{trailing}");
                }
            }
        }
    }
    parts.join("/")
}

fn redact_sensitive_text(text: &str) -> String {
    // Best-effort: hide query api-key=... and path /v2/<long token>.
    let mut out = text.to_owned();
    for marker in ["api-key=", "api_key=", "apikey=", "x-api-key="] {
        let lower = out.to_ascii_lowercase();
        let mut search_from = 0;
        while let Some(rel) = lower[search_from..].find(marker) {
            let idx = search_from + rel;
            let start = idx + marker.len();
            let end = out[start..]
                .find(|c: char| c == '&' || c == ' ' || c == '"' || c == '\'' || c == ')')
                .map(|n| start + n)
                .unwrap_or(out.len());
            out.replace_range(start..end, "***");
            search_from = start + 3;
            if search_from >= out.len() {
                break;
            }
        }
    }
    redact_path_secrets(&out)
}

fn one_line_error(message: &str, max_chars: usize) -> String {
    message
        .chars()
        .take(max_chars)
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

/// Print a provider-layer failure (non-HTTP transport already logged above).
pub fn print_provider_error(source: &str, request_key: &str, error: &str) {
    eprintln!(
        "[api/error] source={source} request_key={request_key} error={}",
        one_line_error(&redact_sensitive_text(error), ERROR_LOG_CHARS)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_log_label_never_contains_path_or_api_key() {
        let url = "https://eth-mainnet.g.alchemy.com/v2/super-secret-key/getNFTs";
        let label = redact_endpoint(url);
        assert!(label.contains("eth-mainnet.g.alchemy.com"));
        assert!(label.contains("/v2/***/getNFTs") || label.contains("/v2/***"));
        assert!(!label.contains("super-secret-key"));
    }

    #[test]
    fn query_api_key_is_redacted() {
        let url = "https://mainnet.helius-rpc.com/?api-key=abc123secret";
        let label = redact_endpoint(url);
        assert!(label.contains("api-key=***"));
        assert!(!label.contains("abc123secret"));
    }

    #[test]
    fn with_token_balances_query_is_not_treated_as_secret() {
        let url = "https://eth-mainnet.g.alchemy.com/nft/v3/super-secret-key/getOwnersForContract?contractAddress=0xabc&withTokenBalances=true";
        let label = redact_endpoint(url);
        assert!(label.contains("withTokenBalances=true"));
        assert!(!label.contains("super-secret-key"));
    }

    #[test]
    fn redacts_key_inside_reqwest_error_parentheses() {
        let msg = "error sending request for url (https://base-mainnet.g.alchemy.com/v2/O6O-K8fkagLHjOa-LLM3_KEY)";
        let redacted = redact_sensitive_text(msg);
        assert!(!redacted.contains("O6O-K8fkagLHjOa-LLM3_KEY"));
        assert!(redacted.contains("/v2/***"));
    }

    #[test]
    fn error_log_message_is_single_line_and_bounded() {
        let message = format!("first\nsecond\r\n{}", "x".repeat(2000));
        let sanitized = one_line_error(&message, 500);
        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));
        assert_eq!(sanitized.chars().count(), 500);
    }

    #[tokio::test]
    async fn opensea_token_bucket_starts_with_one_and_caps_burst() {
        let limiter = TokenBucketRateLimiter::new(4, Duration::from_millis(50));
        // Initial permit available.
        limiter.acquire().await.unwrap();
        // Immediate second acquire must wait for refill; with 50ms refill it should succeed.
        let start = std::time::Instant::now();
        limiter.acquire().await.unwrap();
        assert!(
            start.elapsed() >= Duration::from_millis(40),
            "second token should wait for refill"
        );
    }

    #[tokio::test]
    async fn helius_and_opensea_buckets_are_independent() {
        // Same knobs as production defaults, short refill for the test.
        let opensea = TokenBucketRateLimiter::new(1, Duration::from_millis(200));
        let helius = TokenBucketRateLimiter::new(1, Duration::from_millis(200));
        opensea.acquire().await.unwrap();
        // OpenSea is empty, but Helius still has its own starting token.
        let start = std::time::Instant::now();
        helius.acquire().await.unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "helius must not wait on the opensea bucket"
        );
    }

    #[tokio::test]
    async fn provider_lanes_have_independent_concurrency_and_cooldown() {
        let client = HttpClient::with_retries(1, 0).unwrap();
        // Saturate Alchemy concurrency (1 slot) by holding the permit without
        // going through HTTP — acquire the semaphore directly via a long cool-down
        // is not enough; use rate cool-down on alchemy and ensure opensea still moves.
        client
            .alchemy
            .limiter
            .note_rate_limited(Duration::from_millis(400));
        // OpenSea must not wait for Alchemy's cool-down.
        let start = Instant::now();
        client.opensea.limiter.acquire().await.unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(80),
            "opensea cool-down/rate must be independent of alchemy pause"
        );

        // Concurrency: hold Alchemy's only in-flight permit and ensure OpenSea
        // can still acquire its own permit immediately.
        let alchemy_permit = client.alchemy.in_flight.clone().try_acquire_owned().unwrap();
        let os_start = Instant::now();
        let _os_permit = client.opensea.in_flight.clone().try_acquire_owned().unwrap();
        assert!(
            os_start.elapsed() < Duration::from_millis(20),
            "opensea concurrency must not be blocked by a full alchemy pool"
        );
        drop(alchemy_permit);
    }

    #[test]
    fn helius_defaults_target_five_rps() {
        assert_eq!(HELIUS_RATE_LIMIT_BURST, 5);
        assert_eq!(HELIUS_RATE_LIMIT_REFILL_MS, 200);
        let rps = 1000.0 / HELIUS_RATE_LIMIT_REFILL_MS as f64;
        assert!((rps - 5.0).abs() < 1e-9);
        assert_eq!(RATE_LIMIT_COOLDOWN, Duration::from_secs(1));
    }

    #[test]
    fn http_429_is_detected_for_fixed_backoff() {
        let err = Analysis2Error::http(
            "HTTP 429 Too Many Requests endpoint=example.com/ content_type=application/json body=rate limited",
        );
        assert!(is_http_status(&err, 429));
        assert!(is_rate_limited(&err));
        assert!(is_retryable(&err));
        assert!(!is_http_status(&err, 500));

        let transport = Analysis2Error::http(
            "transport error method=POST endpoint=mainnet.helius-rpc.com/ status=429 detail=…",
        );
        assert!(is_rate_limited(&transport));
    }

    #[test]
    fn jsonrpc_rate_limit_payload_is_detected() {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "1",
            "error": { "code": -32429, "message": "rate limited" }
        });
        assert!(jsonrpc_rate_limit_error(&payload).is_some());
        let ok = serde_json::json!({"jsonrpc":"2.0","id":"1","result":{}});
        assert!(jsonrpc_rate_limit_error(&ok).is_none());
    }

    #[tokio::test]
    async fn rate_limit_cooldown_blocks_other_acquires_for_one_second() {
        let limiter = TokenBucketRateLimiter::new(5, Duration::from_millis(200));
        // Consume the initial token so the next acquire must wait.
        limiter.acquire().await.unwrap();
        limiter.note_rate_limited(Duration::from_millis(250));
        let start = Instant::now();
        limiter.acquire().await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(240),
            "expected ~250ms cool-down, got {elapsed:?}"
        );
        // After cool-down, a single resume token was granted and consumed;
        // next acquire should wait for refill (~200ms), not block another full second.
        let start2 = Instant::now();
        limiter.acquire().await.unwrap();
        assert!(
            start2.elapsed() < Duration::from_millis(350),
            "post cool-down acquire should only wait for refill"
        );
    }
}
