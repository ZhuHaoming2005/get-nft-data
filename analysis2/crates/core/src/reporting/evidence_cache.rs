//! Durable enrich evidence checkpoint for `run` restarts.
//!
//! After candidate enrichment finishes, the pipeline writes portable JSON under
//! the output directory. A later `run --reuse-evidence` rematerializes bundles
//! (remapping process-local `contract_id`) and only HTTP-fetches candidates
//! missing from the cache. Pagination bounds and provider-key presence must
//! match so completeness is not silently overstated.

use std::fs;
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::enrich::types::{ApiKeys, EvidenceBundle, HttpLimits};
use crate::entity::{ContractId, ResidentStore};
use crate::error::Analysis2Error;
use crate::reporting::json::SeedRecord;

pub const EVIDENCE_CACHE_VERSION: u32 = 1;
pub const DEFAULT_EVIDENCE_CACHE_FILE: &str = "evidence_cache.json";

/// Parameters that must match between the producing and reusing runs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvidenceCacheParams {
    /// Seeds embedded for exact list comparison on reuse (relation_legit is seed-scoped).
    pub seeds: Vec<SeedRecord>,
    pub seeds_path: String,
    pub max_transfer_pages: usize,
    pub max_holder_pages: usize,
    pub max_sale_pages: usize,
    pub max_solana_assets: usize,
    pub max_history_assets: usize,
    pub max_signatures_per_asset: usize,
    /// Whether each provider key was present when the cache was built (not the secret).
    pub had_alchemy: bool,
    pub had_etherscan: bool,
    pub had_helius: bool,
    pub had_opensea: bool,
}

/// On-disk enrich checkpoint. Bundles use stable chain/address identity;
/// `contract_id` is rewritten on rematerialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceCacheFile {
    pub version: u32,
    pub params: EvidenceCacheParams,
    pub bundles: Vec<EvidenceBundle>,
}

/// Default cache path: `{output_dir}/evidence_cache.json`.
pub fn default_evidence_cache_path(output_dir: &Path) -> PathBuf {
    output_dir.join(DEFAULT_EVIDENCE_CACHE_FILE)
}

/// Build params from the current run knobs (no secrets).
pub fn evidence_cache_params(
    seeds: &[SeedRecord],
    seeds_path: &str,
    keys: &ApiKeys,
    limits: &HttpLimits,
) -> EvidenceCacheParams {
    EvidenceCacheParams {
        seeds: seeds.to_vec(),
        seeds_path: seeds_path.to_owned(),
        max_transfer_pages: limits.max_transfer_pages,
        max_holder_pages: limits.max_holder_pages,
        max_sale_pages: limits.max_sale_pages,
        max_solana_assets: limits.max_solana_assets,
        max_history_assets: limits.max_history_assets,
        max_signatures_per_asset: limits.max_signatures_per_asset,
        had_alchemy: keys.alchemy().is_some(),
        had_etherscan: keys.etherscan().is_some(),
        had_helius: keys.helius().is_some(),
        had_opensea: keys.opensea().is_some(),
    }
}

/// Build a portable cache from in-memory evidence (stable chain/address keys).
pub fn build_evidence_cache(
    params: EvidenceCacheParams,
    evidence: &AHashMap<ContractId, EvidenceBundle>,
) -> EvidenceCacheFile {
    let mut bundles: Vec<EvidenceBundle> = evidence.values().cloned().collect();
    // Deterministic order for stable diffs / golden tests.
    bundles.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then_with(|| a.address.cmp(&b.address))
    });
    // Zero process-local ids so the file is portable across remaps.
    for bundle in &mut bundles {
        bundle.contract_id = 0;
    }
    EvidenceCacheFile {
        version: EVIDENCE_CACHE_VERSION,
        params,
        bundles,
    }
}

/// Write cache JSON (pretty) atomically via temp file + rename when possible.
pub fn write_evidence_cache(path: &Path, cache: &EvidenceCacheFile) -> Result<(), Analysis2Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_string_pretty(cache)
        .map_err(|e| Analysis2Error::invalid(format!("serialize evidence cache: {e}")))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, body.as_bytes())?;
    if let Err(error) = fs::rename(&tmp, path) {
        fs::write(path, body.as_bytes()).map_err(|e| {
            Analysis2Error::invalid(format!(
                "write evidence cache {} (rename failed: {error}): {e}",
                path.display()
            ))
        })?;
        let _ = fs::remove_file(&tmp);
    }
    Ok(())
}

/// Load and parse an evidence cache file.
pub fn load_evidence_cache(path: &Path) -> Result<EvidenceCacheFile, Analysis2Error> {
    let text = fs::read_to_string(path).map_err(|e| {
        Analysis2Error::invalid(format!("read evidence cache {}: {e}", path.display()))
    })?;
    let cache: EvidenceCacheFile = serde_json::from_str(&text).map_err(|e| {
        Analysis2Error::invalid(format!("parse evidence cache {}: {e}", path.display()))
    })?;
    if cache.version != EVIDENCE_CACHE_VERSION {
        return Err(Analysis2Error::invalid(format!(
            "evidence cache version {} unsupported (expected {EVIDENCE_CACHE_VERSION})",
            cache.version
        )));
    }
    Ok(cache)
}

/// Ensure the cache was produced with equivalent enrich knobs / seeds.
pub fn validate_evidence_cache(
    cache: &EvidenceCacheFile,
    expected: &EvidenceCacheParams,
) -> Result<(), Analysis2Error> {
    let got = &cache.params;
    if got.seeds != expected.seeds {
        return Err(Analysis2Error::invalid(
            "evidence cache seeds list does not match current --seeds file; re-run without --reuse-evidence",
        ));
    }
    if got.max_transfer_pages != expected.max_transfer_pages
        || got.max_holder_pages != expected.max_holder_pages
        || got.max_sale_pages != expected.max_sale_pages
        || got.max_solana_assets != expected.max_solana_assets
        || got.max_history_assets != expected.max_history_assets
        || got.max_signatures_per_asset != expected.max_signatures_per_asset
    {
        return Err(Analysis2Error::invalid(
            "evidence cache pagination limits do not match current HttpLimits; re-run without --reuse-evidence",
        ));
    }
    if got.had_alchemy != expected.had_alchemy
        || got.had_etherscan != expected.had_etherscan
        || got.had_helius != expected.had_helius
        || got.had_opensea != expected.had_opensea
    {
        return Err(Analysis2Error::invalid(
            "evidence cache provider key presence does not match current API keys; re-run without --reuse-evidence",
        ));
    }
    Ok(())
}

/// Rematerialize evidence keyed by process-local contract ids.
///
/// Bundles whose chain/address are absent from the snapshot are skipped with a
/// warning so partial snapshot loads do not abort the whole run.
pub fn rematerialize_evidence(
    store: &ResidentStore,
    cache: &EvidenceCacheFile,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error> {
    let mut out = AHashMap::with_capacity(cache.bundles.len());
    let mut skipped = 0_usize;
    for entry in &cache.bundles {
        let Some(contract_id) = store.contract_id(&entry.chain, &entry.address) else {
            skipped += 1;
            continue;
        };
        let mut bundle = entry.clone();
        bundle.contract_id = contract_id;
        out.insert(contract_id, bundle);
    }
    if skipped > 0 {
        eprintln!(
            "evidence cache: skipped {skipped} bundles not present in current snapshot identity"
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::reporting::json::SeedRecord;
    use ahash::AHashSet;

    fn prepared() -> ResidentStore {
        let evm = ["ethereum"].into_iter().map(str::to_owned).collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(8, &evm);
        store
            .ingest_identity_row(IdentityRow {
                chain: "ethereum".into(),
                contract_address: "0xabc".into(),
                token_id: "1".into(),
                name_norm: "n".into(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 0,
                },
            })
            .unwrap();
        store
    }

    #[test]
    fn round_trip_remaps_contract_id() {
        let store = prepared();
        let cid = store.contract_id("ethereum", "0xabc").unwrap();
        let mut bundle = EvidenceBundle::empty(cid, "ethereum", "0xabc");
        bundle.controllers.push("0xop".into());
        let mut map = AHashMap::new();
        map.insert(cid, bundle);

        let params = evidence_cache_params(
            &[SeedRecord {
                chain: "ethereum".into(),
                address: "0xseed".into(),
                rank: Some(1),
            }],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        );
        let cache = build_evidence_cache(params.clone(), &map);
        assert_eq!(cache.bundles[0].contract_id, 0);

        let dir = std::env::temp_dir().join(format!(
            "analysis2_evidence_cache_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        write_evidence_cache(&path, &cache).unwrap();
        let loaded = load_evidence_cache(&path).unwrap();
        validate_evidence_cache(&loaded, &params).unwrap();
        let remapped = rematerialize_evidence(&store, &loaded).unwrap();
        assert_eq!(remapped.len(), 1);
        let got = remapped.get(&cid).unwrap();
        assert_eq!(got.contract_id, cid);
        assert_eq!(got.controllers, vec!["0xop".to_owned()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_rejects_key_presence_mismatch() {
        let params = evidence_cache_params(
            &[],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        );
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: params.clone(),
            bundles: Vec::new(),
        };
        let mut other = params;
        other.had_alchemy = true;
        assert!(validate_evidence_cache(&cache, &other).is_err());
    }
}
