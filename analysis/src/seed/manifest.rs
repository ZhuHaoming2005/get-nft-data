use crate::error::{AnalysisError, Result};
use crate::model::{ChainId, ContractKey, SeedId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

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
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = fs::read(path)?;
        let manifest: Self = serde_json::from_slice(&raw)?;
        manifest.validate_exact()
    }

    pub fn validate_exact(mut self) -> Result<Self> {
        self.seeds.sort_by(|left, right| {
            (left.chain, left.rank, &left.stable_identifier).cmp(&(
                right.chain,
                right.rank,
                &right.stable_identifier,
            ))
        });
        if self.seeds.len() != 100 {
            return Err(AnalysisError::Seed(format!(
                "expected exactly 100 seeds, found {}",
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
            if !(1..=25).contains(&seed.rank) {
                return Err(AnalysisError::Seed(format!(
                    "seed {} rank must be in 1..=25",
                    seed.stable_identifier
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
            let count = counts.get(&chain).copied().unwrap_or_default();
            if count != 25 {
                return Err(AnalysisError::Seed(format!(
                    "chain {chain} must have exactly 25 seeds, found {count}"
                )));
            }
            if ranks.get(&chain).map(BTreeSet::len) != Some(25) {
                return Err(AnalysisError::Seed(format!(
                    "chain {chain} must contain each rank in 1..=25 exactly once"
                )));
            }
        }
        Ok(self)
    }
}
