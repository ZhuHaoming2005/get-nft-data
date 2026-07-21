use crate::error::{AnalysisError, Result};
use crate::model::ChainId;
use crate::seed::{SeedDefinition, SeedManifest};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankedSeed {
    pub chain: ChainId,
    pub contract_address: String,
    pub collection_name: String,
    pub stable_identifier: String,
    pub ranking_metric: String,
    pub ranking_value: f64,
    pub ranking_window: String,
    pub source: String,
}

#[async_trait]
pub trait SeedSource: Send + Sync {
    async fn ranked(&self, chain: ChainId, limit: usize) -> Result<Vec<RankedSeed>>;
}

pub async fn select_exact(source: &dyn SeedSource) -> Result<SeedManifest> {
    let generated_at = Utc::now();
    let mut seeds = Vec::with_capacity(100);
    let ranked_by_chain =
        futures_util::future::try_join_all(ChainId::ALL.into_iter().map(|chain| async move {
            Ok::<_, AnalysisError>((chain, source.ranked(chain, 25).await?))
        }))
        .await?;
    for (chain, ranked) in ranked_by_chain {
        if ranked.len() < 25 {
            return Err(AnalysisError::Seed(format!(
                "source returned only {} seeds for {chain}",
                ranked.len()
            )));
        }
        seeds.extend(
            ranked
                .into_iter()
                .take(25)
                .enumerate()
                .map(|(index, seed)| {
                    if seed.chain != chain {
                        return Err(AnalysisError::Seed(format!(
                            "source returned a {} seed for {chain}",
                            seed.chain
                        )));
                    }
                    Ok(SeedDefinition {
                        id: crate::model::SeedId(0),
                        chain: seed.chain,
                        contract_address: seed.contract_address,
                        rank: (index + 1) as u8,
                        collection_name: seed.collection_name,
                        stable_identifier: seed.stable_identifier,
                        ranking_metric: seed.ranking_metric,
                        ranking_value: seed.ranking_value,
                        ranking_window: seed.ranking_window,
                        source: seed.source,
                        collected_at: generated_at,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }
    SeedManifest {
        generated_at,
        seeds,
    }
    .validate_exact()
}
