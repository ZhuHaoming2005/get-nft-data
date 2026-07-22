use crate::error::{AnalysisError, Result};
use crate::seed::SeedTopCounts;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub snapshot_files: Vec<PathBuf>,
    pub seed_manifest: PathBuf,
    pub output_dir: PathBuf,
    #[serde(deserialize_with = "deserialize_bytes")]
    pub memory_limit: u64,
    pub cpu_workers: usize,
    pub index_shards: usize,
    pub seed_batch_size: usize,
    #[serde(default)]
    pub seed_top: SeedTopCounts,
    pub numa_mode: NumaMode,
    pub tokio_worker_threads: usize,
    pub cpu_queue_capacity: usize,
    pub network_queue_capacity: usize,
    pub analysis_queue_capacity: usize,
    pub compression_concurrency: usize,
    pub writer_threads: usize,
    #[serde(deserialize_with = "deserialize_bytes")]
    pub writer_queue_bytes: u64,
    pub next_dimension_overlap: bool,
    pub provider_timeout_ms: u64,
    #[serde(default, skip_serializing)]
    pub api_keys: crate::api::ProviderApiKeys,
    #[serde(default)]
    pub provider_endpoints: ProviderEndpoints,
    pub provider_concurrency: ProviderConcurrency,
    pub provider_page_limits: BTreeMap<String, usize>,
    pub provider_retry_count: usize,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchor_count: usize,
    pub analysis_timestamp: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumaMode {
    Auto,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConcurrency {
    pub alchemy: usize,
    pub helius: usize,
    pub other: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderEndpoints {
    pub opensea: String,
    pub etherscan: String,
    pub helius: String,
    pub alchemy_prices: String,
    pub alchemy_networks: BTreeMap<String, String>,
}

impl Default for ProviderEndpoints {
    fn default() -> Self {
        Self {
            opensea: "https://api.opensea.io".into(),
            etherscan: "https://api.etherscan.io/v2/api".into(),
            helius: "https://mainnet.helius-rpc.com/".into(),
            alchemy_prices: "https://api.g.alchemy.com/prices/v1".into(),
            alchemy_networks: BTreeMap::from([
                ("ethereum".into(), "eth-mainnet".into()),
                ("base".into(), "base-mainnet".into()),
                ("polygon".into(), "polygon-mainnet".into()),
            ]),
        }
    }
}

impl RunConfig {
    pub fn from_path(path: &Path) -> Result<Self> {
        let config = Self::from_path_unvalidated(path)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_path_unvalidated(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&raw)?;
        config.api_keys.normalize();
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        for snapshot in &mut config.snapshot_files {
            make_absolute(base, snapshot);
        }
        make_absolute(base, &mut config.seed_manifest);
        make_absolute(base, &mut config.output_dir);
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.seed_top.validate()?;
        if self.snapshot_files.is_empty() {
            return Err(AnalysisError::Config(
                "snapshot_files must contain at least one explicitly ordered file".into(),
            ));
        }
        if self.snapshot_files.len() > u16::MAX as usize {
            return Err(AnalysisError::Config(
                "snapshot_files exceeds SourceOrder file ordinal capacity".into(),
            ));
        }
        if self.cpu_workers != 128 {
            return Err(AnalysisError::Config(
                "production cpu_workers must equal 128".into(),
            ));
        }
        if self.index_shards != 128 || !self.index_shards.is_power_of_two() {
            return Err(AnalysisError::Config(
                "production index_shards must equal 128".into(),
            ));
        }
        if self.seed_batch_size == 0 {
            return Err(AnalysisError::Config(
                "seed_batch_size must be positive".into(),
            ));
        }
        for (name, value) in [
            ("tokio_worker_threads", self.tokio_worker_threads),
            ("cpu_queue_capacity", self.cpu_queue_capacity),
            ("network_queue_capacity", self.network_queue_capacity),
            ("analysis_queue_capacity", self.analysis_queue_capacity),
            ("compression_concurrency", self.compression_concurrency),
            ("writer_threads", self.writer_threads),
        ] {
            if value == 0 {
                return Err(AnalysisError::Config(format!("{name} must be positive")));
            }
        }
        if self.compression_concurrency > self.cpu_workers {
            return Err(AnalysisError::Config(
                "compression_concurrency cannot exceed cpu_workers".into(),
            ));
        }
        if self.provider_concurrency.alchemy == 0
            || self.provider_concurrency.helius == 0
            || self.provider_concurrency.other == 0
        {
            return Err(AnalysisError::Config(
                "all provider concurrency values must be positive".into(),
            ));
        }
        if self.writer_queue_bytes == 0 {
            return Err(AnalysisError::Config(
                "writer_queue_bytes must be positive".into(),
            ));
        }
        if self.provider_timeout_ms == 0 {
            return Err(AnalysisError::Config(
                "provider_timeout_ms must be positive".into(),
            ));
        }
        for (name, endpoint) in [
            (
                "provider_endpoints.opensea",
                &self.provider_endpoints.opensea,
            ),
            (
                "provider_endpoints.etherscan",
                &self.provider_endpoints.etherscan,
            ),
            ("provider_endpoints.helius", &self.provider_endpoints.helius),
            (
                "provider_endpoints.alchemy_prices",
                &self.provider_endpoints.alchemy_prices,
            ),
        ] {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|error| AnalysisError::Config(format!("{name} is invalid: {error}")))?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(AnalysisError::Config(format!(
                    "{name} must use http or https"
                )));
            }
        }
        for chain in ["ethereum", "base", "polygon"] {
            if self
                .provider_endpoints
                .alchemy_networks
                .get(chain)
                .is_none_or(|network| network.trim().is_empty())
            {
                return Err(AnalysisError::Config(format!(
                    "provider_endpoints.alchemy_networks.{chain} is required"
                )));
            }
        }
        if self
            .provider_page_limits
            .iter()
            .any(|(name, limit)| name.trim().is_empty() || *limit == 0)
        {
            return Err(AnalysisError::Config(
                "provider_page_limits keys and values must be positive".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.name_threshold) || self.name_threshold.is_nan() {
            return Err(AnalysisError::Config(
                "name_threshold must be in [0,1]".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.metadata_threshold) || self.metadata_threshold.is_nan() {
            return Err(AnalysisError::Config(
                "metadata_threshold must be in [0,1]".into(),
            ));
        }
        if self.metadata_anchor_count != 8 {
            return Err(AnalysisError::Config(
                "metadata_anchor_count must equal the fixed business value 8".into(),
            ));
        }
        if self.memory_limit < 464 * GIB {
            return Err(AnalysisError::Config(
                "memory_limit must be at least 464GiB for a production run".into(),
            ));
        }
        for snapshot in &self.snapshot_files {
            if !snapshot.is_file() {
                return Err(AnalysisError::Config(format!(
                    "snapshot file does not exist: {}",
                    snapshot.display()
                )));
            }
        }
        Ok(())
    }

    pub fn validate_seed_selection(&self) -> Result<()> {
        self.seed_top.validate()?;
        if self.tokio_worker_threads == 0 {
            return Err(AnalysisError::Config(
                "tokio_worker_threads must be positive".into(),
            ));
        }
        if self.provider_concurrency.other == 0 {
            return Err(AnalysisError::Config(
                "provider_concurrency.other must be positive for select-seeds".into(),
            ));
        }
        if self.provider_timeout_ms == 0 {
            return Err(AnalysisError::Config(
                "provider_timeout_ms must be positive".into(),
            ));
        }
        let url = reqwest::Url::parse(&self.provider_endpoints.opensea).map_err(|error| {
            AnalysisError::Config(format!("provider_endpoints.opensea is invalid: {error}"))
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(AnalysisError::Config(
                "provider_endpoints.opensea must use http or https".into(),
            ));
        }
        Ok(())
    }
}

fn make_absolute(base: &Path, value: &mut PathBuf) {
    if value.is_relative() {
        *value = base.join(&*value);
    }
}

fn deserialize_bytes<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct BytesVisitor;

    impl serde::de::Visitor<'_> for BytesVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a byte count or a string such as 464GiB")
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            parse_bytes(value).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(BytesVisitor)
}

pub fn parse_bytes(raw: &str) -> std::result::Result<u64, String> {
    let raw = raw.trim();
    let split = raw
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(raw.len());
    let value = raw[..split]
        .parse::<u64>()
        .map_err(|_| format!("invalid byte count `{raw}`"))?;
    let multiplier = match raw[split..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => GIB,
        "tib" => 1024 * GIB,
        suffix => return Err(format!("unsupported byte suffix `{suffix}`")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte count overflows u64: `{raw}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binary_units() {
        assert_eq!(parse_bytes("464GiB").unwrap(), 464 * GIB);
        assert_eq!(parse_bytes("2 GiB").unwrap(), 2 * GIB);
    }

    #[test]
    fn api_keys_are_out_of_serialized_manifests() {
        let mut config =
            RunConfig::from_path_unvalidated(Path::new("config/default.toml")).unwrap();
        config.api_keys = crate::api::ProviderApiKeys {
            alchemy: "alchemy-secret-sentinel".to_owned(),
            etherscan: "etherscan-secret-sentinel".to_owned(),
            opensea: "opensea-secret-sentinel".to_owned(),
            helius: "helius-secret-sentinel".to_owned(),
        };
        let serialized = serde_json::to_string(&config).unwrap();
        assert!(!serialized.contains("api_keys"));
        assert!(!serialized.contains("secret-sentinel"));
    }

    #[test]
    fn seed_selection_rejects_a_zero_request_permit_pool() {
        let mut config =
            RunConfig::from_path_unvalidated(Path::new("config/default.toml")).unwrap();
        assert!(config.validate_seed_selection().is_ok());
        config.provider_concurrency.other = 0;
        assert!(config.validate_seed_selection().is_err());
    }

    #[test]
    fn seed_top_defaults_to_twenty_five_and_accepts_per_chain_overrides() {
        let default = SeedTopCounts::default();
        assert_eq!(default.total(), 100);

        let parsed: SeedTopCounts = toml::from_str("ethereum = 7").unwrap();
        assert_eq!(parsed.base, 25);
        assert_eq!(parsed.ethereum, 7);
        assert_eq!(parsed.polygon, 25);
        assert_eq!(parsed.solana, 25);
        assert_eq!(parsed.total(), 82);
    }

    #[test]
    fn seed_top_rejects_zero_for_any_chain() {
        let counts = SeedTopCounts {
            solana: 0,
            ..SeedTopCounts::default()
        };
        assert!(counts.validate().is_err());
    }
}
