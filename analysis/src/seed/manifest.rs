use crate::error::{AnalysisError, Result};
use crate::model::{ChainId, ContractKey, SeedId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SeedTopCounts {
    pub base: u8,
    pub ethereum: u8,
    pub polygon: u8,
    pub solana: u8,
}

impl Default for SeedTopCounts {
    fn default() -> Self {
        Self {
            base: 25,
            ethereum: 25,
            polygon: 25,
            solana: 25,
        }
    }
}

impl SeedTopCounts {
    pub const fn count(self, chain: ChainId) -> u8 {
        match chain {
            ChainId::Base => self.base,
            ChainId::Ethereum => self.ethereum,
            ChainId::Polygon => self.polygon,
            ChainId::Solana => self.solana,
        }
    }

    pub fn total(self) -> usize {
        ChainId::ALL
            .into_iter()
            .map(|chain| usize::from(self.count(chain)))
            .sum()
    }

    pub fn validate(self) -> Result<()> {
        for chain in ChainId::ALL {
            if self.count(chain) == 0 {
                return Err(AnalysisError::Config(format!(
                    "seed_top.{chain} must be positive"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedManifest {
    pub generated_at: DateTime<Utc>,
    pub seeds: Vec<SeedDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedDefinition {
    #[serde(default, skip_deserializing)]
    pub id: SeedId,
    pub chain: ChainId,
    pub contract_address: String,
    pub rank: u8,
    pub collection_name: String,
    pub stable_identifier: String,
    pub ranking_metric: String,
    pub ranking_value: f64,
    pub ranking_window: String,
    pub source: String,
    pub collected_at: DateTime<Utc>,
}

impl SeedDefinition {
    pub fn contract_key(&self) -> ContractKey {
        ContractKey::new(self.chain, self.contract_address.clone())
    }
}

impl SeedManifest {
    pub fn from_path(path: &Path, seed_top: SeedTopCounts) -> Result<Self> {
        let raw = fs::read(path)?;
        let manifest: Self = serde_json::from_slice(&raw)?;
        manifest.validate_exact(seed_top)
    }

    pub fn validate_exact(mut self, seed_top: SeedTopCounts) -> Result<Self> {
        seed_top.validate()?;
        self.seeds.sort_by(|left, right| {
            (left.chain, left.rank, &left.stable_identifier).cmp(&(
                right.chain,
                right.rank,
                &right.stable_identifier,
            ))
        });
        let expected_total = seed_top.total();
        if self.seeds.len() != expected_total {
            return Err(AnalysisError::Seed(format!(
                "expected exactly {expected_total} configured seeds, found {}",
                self.seeds.len()
            )));
        }
        let mut counts = BTreeMap::new();
        let mut keys = BTreeSet::new();
        let mut ranks = BTreeMap::<ChainId, BTreeSet<u8>>::new();
        for (index, seed) in self.seeds.iter_mut().enumerate() {
            seed.contract_address = if seed.chain.is_evm() {
                seed.contract_address.trim().to_ascii_lowercase()
            } else {
                seed.contract_address.trim().to_owned()
            };
            if seed.contract_address.trim().is_empty()
                || seed.collection_name.trim().is_empty()
                || seed.stable_identifier.trim().is_empty()
                || seed.ranking_metric.trim().is_empty()
                || seed.ranking_window.trim().is_empty()
                || seed.source.trim().is_empty()
            {
                return Err(AnalysisError::Seed(format!(
                    "seed {} has an empty required field",
                    seed.stable_identifier
                )));
            }
            let expected = seed_top.count(seed.chain);
            if seed.rank == 0 || seed.rank > expected {
                return Err(AnalysisError::Seed(format!(
                    "seed {} rank must be in 1..={expected} for {}",
                    seed.stable_identifier, seed.chain
                )));
            }
            if !ranks.entry(seed.chain).or_default().insert(seed.rank) {
                return Err(AnalysisError::Seed(format!(
                    "chain {} contains duplicate rank {}",
                    seed.chain, seed.rank
                )));
            }
            if !seed.ranking_value.is_finite() {
                return Err(AnalysisError::Seed(format!(
                    "seed {} ranking_value is not finite",
                    seed.stable_identifier
                )));
            }
            let key = (seed.chain, seed.contract_address.clone());
            if !keys.insert(key) {
                return Err(AnalysisError::Seed(format!(
                    "duplicate seed contract {}:{}",
                    seed.chain, seed.contract_address
                )));
            }
            *counts.entry(seed.chain).or_insert(0usize) += 1;
            seed.id = SeedId(index as u16);
        }
        for chain in ChainId::ALL {
            let expected = usize::from(seed_top.count(chain));
            let count = counts.get(&chain).copied().unwrap_or_default();
            if count != expected {
                return Err(AnalysisError::Seed(format!(
                    "chain {chain} must have exactly {expected} seeds, found {count}"
                )));
            }
            if ranks.get(&chain).map(BTreeSet::len) != Some(expected) {
                return Err(AnalysisError::Seed(format!(
                    "chain {chain} must contain each rank in 1..={expected} exactly once"
                )));
            }
        }
        Ok(self)
    }
}
