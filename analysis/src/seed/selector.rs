use crate::error::{AnalysisError, Result};
use crate::model::ChainId;
use crate::seed::{SeedDefinition, SeedManifest, SeedTopCounts};
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

pub async fn select_exact(
    source: &dyn SeedSource,
    seed_top: SeedTopCounts,
) -> Result<SeedManifest> {
    seed_top.validate()?;
    let generated_at = Utc::now();
    let mut seeds = Vec::with_capacity(seed_top.total());
    let ranked_by_chain =
        futures_util::future::try_join_all(ChainId::ALL.into_iter().map(|chain| async move {
            let expected = usize::from(seed_top.count(chain));
            Ok::<_, AnalysisError>((chain, expected, source.ranked(chain, expected).await?))
        }))
        .await?;
    for (chain, expected, ranked) in ranked_by_chain {
        if ranked.len() < expected {
            return Err(AnalysisError::Seed(format!(
                "source returned only {} of {expected} requested seeds for {chain}",
                ranked.len(),
            )));
        }
        seeds.extend(
            ranked
                .into_iter()
                .take(expected)
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
    .validate_exact(seed_top)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSource {
        requested: Mutex<BTreeMap<ChainId, usize>>,
    }

    #[async_trait]
    impl SeedSource for RecordingSource {
        async fn ranked(&self, chain: ChainId, limit: usize) -> Result<Vec<RankedSeed>> {
            self.requested.lock().unwrap().insert(chain, limit);
            Ok((0..limit)
                .map(|index| RankedSeed {
                    chain,
                    contract_address: format!("{chain}-contract-{index}"),
                    collection_name: format!("{chain} collection {index}"),
                    stable_identifier: format!("{chain}-collection-{index}"),
                    ranking_metric: "fixture".into(),
                    ranking_value: (limit - index) as f64,
                    ranking_window: "30d".into(),
                    source: "fixture".into(),
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn selects_and_validates_each_configured_chain_count() {
        let source = RecordingSource::default();
        let counts = SeedTopCounts {
            base: 1,
            ethereum: 2,
            polygon: 3,
            solana: 4,
        };
        let manifest = select_exact(&source, counts).await.unwrap();

        assert_eq!(manifest.seeds.len(), 10);
        let requested = source.requested.lock().unwrap();
        for chain in ChainId::ALL {
            let expected = usize::from(counts.count(chain));
            assert_eq!(requested.get(&chain), Some(&expected));
            let selected = manifest
                .seeds
                .iter()
                .filter(|seed| seed.chain == chain)
                .collect::<Vec<_>>();
            assert_eq!(selected.len(), expected);
            assert_eq!(
                selected.iter().map(|seed| seed.rank).collect::<Vec<_>>(),
                (1..=counts.count(chain)).collect::<Vec<_>>()
            );
        }
    }
}
