pub mod evm;
pub mod http;
pub mod price;
pub mod solana;

use crate::error::Result;
use crate::model::{
    ContractKey, EvidenceBundle, NftSelection, RelationVerification, SeedCandidateRelation,
};
use async_trait::async_trait;
use futures_util::StreamExt;

#[derive(Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderApiKeys {
    pub alchemy: String,
    pub etherscan: String,
    pub opensea: String,
    pub helius: String,
}

impl std::fmt::Debug for ProviderApiKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderApiKeys")
            .field("alchemy", &redacted(&self.alchemy))
            .field("etherscan", &redacted(&self.etherscan))
            .field("opensea", &redacted(&self.opensea))
            .field("helius", &redacted(&self.helius))
            .finish()
    }
}

impl ProviderApiKeys {
    pub fn new(alchemy: String, etherscan: String, opensea: String, helius: String) -> Self {
        Self {
            alchemy: alchemy.trim().to_owned(),
            etherscan: etherscan.trim().to_owned(),
            opensea: opensea.trim().to_owned(),
            helius: helius.trim().to_owned(),
        }
    }

    pub fn normalize(&mut self) {
        self.alchemy = self.alchemy.trim().to_owned();
        self.etherscan = self.etherscan.trim().to_owned();
        self.opensea = self.opensea.trim().to_owned();
        self.helius = self.helius.trim().to_owned();
    }
}

fn redacted(value: &str) -> &'static str {
    if value.trim().is_empty() {
        "<unset>"
    } else {
        "<redacted>"
    }
}

#[async_trait]
pub trait EvidenceProvider: Send + Sync {
    async fn fetch_candidate(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
        relations: &[SeedCandidateRelation],
    ) -> Result<EvidenceBundle>;

    async fn fetch_relation_verifications(
        &self,
        evidence: &EvidenceBundle,
        relations: &[SeedCandidateRelation],
    ) -> Result<Vec<RelationVerification>> {
        let _ = (evidence, relations);
        Err(crate::error::AnalysisError::Provider(
            "incremental relation verification is not supported by this evidence provider".into(),
        ))
    }
}

pub async fn response_bytes(response: reqwest::Response, limit: u64) -> Result<bytes::Bytes> {
    let status = response.status();
    if status.is_success() {
        if response
            .content_length()
            .is_some_and(|length| length > limit)
        {
            return Err(crate::error::AnalysisError::ApiRequest {
                message: format!(
                    "provider response Content-Length exceeds {limit} byte memory boundary"
                ),
                retryable: false,
                retry_after_ms: None,
            });
        }
        let mut body = bytes::BytesMut::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| crate::error::AnalysisError::ApiRequest {
                message: format!("provider response body failure: {}", error.without_url()),
                retryable: true,
                retry_after_ms: None,
            })?;
            let next = body.len() as u64 + chunk.len() as u64;
            if next > limit {
                return Err(crate::error::AnalysisError::ApiRequest {
                    message: format!("provider response exceeds {limit} byte memory boundary"),
                    retryable: false,
                    retry_after_ms: None,
                });
            }
            body.extend_from_slice(&chunk);
        }
        return Ok(body.freeze());
    }
    let retry_after_ms = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after_ms);
    let retryable = status.as_u16() == 429 || status.is_server_error();
    let mut error_bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while error_bytes.len() < 512 {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let Ok(chunk) = chunk else {
            break;
        };
        let remaining = 512 - error_bytes.len();
        error_bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    let error_detail = String::from_utf8_lossy(&error_bytes);
    let error_detail = error_detail.trim();
    Err(crate::error::AnalysisError::ApiRequest {
        message: if error_detail.is_empty() {
            format!("HTTP {status}")
        } else {
            format!("HTTP {status}: {error_detail}")
        },
        retryable,
        retry_after_ms,
    })
}

fn parse_retry_after_ms(value: &str) -> Option<u64> {
    const MAX_RETRY_AFTER_MS: u64 = 60_000;
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(seconds.saturating_mul(1000).min(MAX_RETRY_AFTER_MS));
    }
    let retry_at = chrono::DateTime::parse_from_rfc2822(value.trim())
        .ok()?
        .with_timezone(&chrono::Utc);
    let delay = (retry_at - chrono::Utc::now()).num_milliseconds().max(0) as u64;
    Some(delay.min(MAX_RETRY_AFTER_MS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_supports_seconds_and_http_dates_with_a_cap() {
        assert_eq!(parse_retry_after_ms("2"), Some(2_000));
        assert_eq!(parse_retry_after_ms("999"), Some(60_000));
        assert_eq!(
            parse_retry_after_ms("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(0)
        );
    }
}
