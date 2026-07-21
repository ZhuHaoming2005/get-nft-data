use crate::api::{
    evm::EvmClient, http::ProviderHttpClient, price::PriceOracle, solana::SolanaClient,
    EvidenceProvider,
};
use crate::error::{AnalysisError, Result};
use crate::model::{
    ContractKey, EvidenceBundle, EvidenceStatus, NftSelection, RelationVerification,
    SeedCandidateRelation,
};
use async_trait::async_trait;
use std::time::Duration;

pub struct ApiClients {
    evm: EvmClient,
    solana: SolanaClient,
    evm_permits: tokio::sync::Semaphore,
    solana_permits: tokio::sync::Semaphore,
}

impl ApiClients {
    pub fn new(
        timeout: Duration,
        config: &crate::config::RunConfig,
        response_limit: u64,
        executor: std::sync::Arc<crate::pipeline::CpuExecutor>,
    ) -> Result<Self> {
        let alchemy = ProviderHttpClient::concurrent(
            "alchemy",
            timeout,
            config.provider_concurrency.alchemy,
            config.provider_retry_count,
            response_limit,
            executor.clone(),
        )?;
        let helius = ProviderHttpClient::rate_limited(
            "helius",
            timeout,
            config.provider_concurrency.helius,
            Duration::from_millis(100),
            config.provider_retry_count,
            response_limit,
            executor.clone(),
        )?;
        let other = ProviderHttpClient::rate_limited(
            "other",
            timeout,
            config.provider_concurrency.other,
            Duration::from_millis(300),
            config.provider_retry_count,
            response_limit,
            executor,
        )?;
        let page_limits = std::sync::Arc::new(config.provider_page_limits.clone());
        let prices = PriceOracle::new(
            alchemy.clone(),
            &config.api_keys.alchemy,
            &config.provider_endpoints.alchemy_prices,
        );
        Ok(Self {
            evm: EvmClient::new(
                alchemy,
                other.clone(),
                config.api_keys.clone(),
                config.provider_endpoints.clone(),
                page_limits.clone(),
                prices.clone(),
                config.analysis_timestamp.timestamp(),
            ),
            solana: SolanaClient::new(
                helius,
                other,
                config.api_keys.clone(),
                config.provider_endpoints.clone(),
                page_limits,
                prices,
                config.analysis_timestamp.timestamp(),
            ),
            evm_permits: tokio::sync::Semaphore::new(config.provider_concurrency.alchemy),
            solana_permits: tokio::sync::Semaphore::new(config.provider_concurrency.helius),
        })
    }
}

#[async_trait]
impl EvidenceProvider for ApiClients {
    async fn fetch_candidate(
        &self,
        candidate: &ContractKey,
        selection: &NftSelection,
        relations: &[SeedCandidateRelation],
    ) -> Result<EvidenceBundle> {
        if candidate.chain.is_evm() {
            let _permit = self
                .evm_permits
                .acquire()
                .await
                .map_err(|_| AnalysisError::State("EVM permit pool closed".into()))?;
            let bundle = self.evm.fetch(candidate, selection, relations).await?;
            validate_evidence_bundle(&bundle)?;
            Ok(bundle)
        } else {
            let _permit = self
                .solana_permits
                .acquire()
                .await
                .map_err(|_| AnalysisError::State("Solana permit pool closed".into()))?;
            let bundle = self.solana.fetch(candidate, selection, relations).await?;
            validate_evidence_bundle(&bundle)?;
            Ok(bundle)
        }
    }

    async fn fetch_relation_verifications(
        &self,
        evidence: &EvidenceBundle,
        relations: &[SeedCandidateRelation],
    ) -> Result<Vec<RelationVerification>> {
        let complete = matches!(
            evidence.quality.authority,
            Some(EvidenceStatus::Complete | EvidenceStatus::Empty)
        );
        if evidence.candidate.chain.is_evm() {
            Ok(self
                .evm
                .verify_relations(relations, &evidence.controllers, complete)
                .await)
        } else {
            Ok(self
                .solana
                .verify_relations(relations, &evidence.controllers, complete)
                .await)
        }
    }
}

fn validate_evidence_bundle(bundle: &EvidenceBundle) -> Result<()> {
    for event in &bundle.events {
        if event.chain != bundle.candidate.chain {
            return Err(AnalysisError::Provider(
                "normalized event chain differs from candidate chain".into(),
            ));
        }
        if matches!(
            event.kind,
            crate::model::EventKind::Mint
                | crate::model::EventKind::Transfer
                | crate::model::EventKind::Sale
                | crate::model::EventKind::Listing
        ) && event.nft.is_none()
        {
            return Err(AnalysisError::Provider(format!(
                "normalized {} event omitted its NFT identity",
                event.kind.as_str()
            )));
        }
        if event
            .channel
            .is_some_and(|channel| !channel.compatible_with(event.kind))
        {
            return Err(AnalysisError::Provider(format!(
                "normalized {} event has an incompatible value channel",
                event.kind.as_str()
            )));
        }
        if event.nft.as_ref().is_some_and(|nft| {
            nft.chain != bundle.candidate.chain
                || nft.contract_address != bundle.candidate.contract_address
        }) {
            return Err(AnalysisError::Provider(
                "normalized event NFT differs from candidate identity".into(),
            ));
        }
    }
    if bundle.holders.iter().any(|(nft, _)| {
        nft.chain != bundle.candidate.chain
            || nft.contract_address != bundle.candidate.contract_address
    }) {
        return Err(AnalysisError::Provider(
            "normalized holder NFT differs from candidate identity".into(),
        ));
    }
    for (name, status) in [
        ("assets", bundle.quality.assets),
        ("histories", bundle.quality.histories),
        ("transactions", bundle.quality.transactions),
        ("prices", bundle.quality.prices),
        ("authority", bundle.quality.authority),
    ] {
        let status = status.ok_or_else(|| {
            AnalysisError::Provider(format!(
                "normalized evidence omitted the `{name}` quality status"
            ))
        })?;
        if status == crate::model::EvidenceStatus::Requested {
            return Err(AnalysisError::Provider(format!(
                "normalized evidence left `{name}` in nonterminal Requested state"
            )));
        }
    }
    if bundle.provenance.is_empty() {
        return Err(AnalysisError::Provider(
            "normalized evidence omitted source provenance".into(),
        ));
    }
    for observation in &bundle.provenance {
        if observation.source.trim().is_empty()
            || observation.request_key.trim().is_empty()
            || observation.observed_at <= 0
            || observation.status == crate::model::EvidenceStatus::Requested
        {
            return Err(AnalysisError::Provider(
                "normalized evidence contains invalid or nonterminal provenance".into(),
            ));
        }
    }
    Ok(())
}

pub async fn fetch_with_retry(
    provider: &dyn EvidenceProvider,
    candidate: &ContractKey,
    selection: &NftSelection,
    relations: &[SeedCandidateRelation],
    retries: usize,
) -> Result<EvidenceBundle> {
    let mut last_error = None;
    for attempt in 0..=retries {
        match provider
            .fetch_candidate(candidate, selection, relations)
            .await
        {
            Ok(bundle) => return Ok(bundle),
            Err(error) => {
                let retryable = error.retryable();
                let retry_after_ms = error.retry_after_ms();
                last_error = Some(error);
                if attempt < retries && retryable {
                    tokio::time::sleep(Duration::from_millis(
                        retry_after_ms
                            .unwrap_or_else(|| 100_u64.saturating_mul(1_u64 << attempt.min(8))),
                    ))
                    .await;
                } else {
                    break;
                }
            }
        }
    }
    Err(last_error.expect("at least one fetch attempt"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainId, EvidenceObservation, EvidenceQuality, EvidenceStatus};

    fn bundle() -> EvidenceBundle {
        EvidenceBundle {
            candidate: ContractKey::new(ChainId::Ethereum, "0x1"),
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: Vec::new(),
            holders: Vec::new(),
            controllers: Vec::new(),
            relation_verifications: Vec::new(),
            provenance: vec![EvidenceObservation {
                source: "fixture".to_owned(),
                request_key: "request-1".to_owned(),
                observed_at: 1,
                status: EvidenceStatus::Empty,
            }],
            quality: EvidenceQuality {
                assets: Some(EvidenceStatus::Empty),
                histories: Some(EvidenceStatus::NotRequested),
                transactions: Some(EvidenceStatus::Empty),
                prices: Some(EvidenceStatus::NotRequested),
                authority: Some(EvidenceStatus::Empty),
                ..Default::default()
            },
        }
    }

    #[test]
    fn normalized_evidence_requires_terminal_statuses_and_provenance() {
        assert!(validate_evidence_bundle(&bundle()).is_ok());
        let mut missing = bundle();
        missing.quality.assets = None;
        assert!(validate_evidence_bundle(&missing).is_err());
        let mut requested = bundle();
        requested.provenance[0].status = EvidenceStatus::Requested;
        assert!(validate_evidence_bundle(&requested).is_err());
    }
}
