//! Durable enrich evidence checkpoint for `run` restarts.
//!
//! Network evidence is written **incrementally** while enrich runs:
//! - `evidence_cache.meta.json` — version + params (once)
//! - `evidence_cache.jsonl` — one compact bundle per line (append in batches)
//! - `evidence_cache.json` — full snapshot rewritten periodically and at finish
//!
//! After an interrupt, the next run loads meta+jsonl (and/or the JSON snapshot),
//! rematerializes bundles, and only HTTP-fetches candidates still missing.
//! Pagination bounds must match. Seed membership and provider-key presence do
//! not invalidate candidate-scoped HTTP evidence.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::enrich::types::{ApiKeys, EvidenceBundle, HttpLimits};
use crate::entity::{ContractId, ResidentStore};
use crate::error::Analysis2Error;
use crate::reporting::json::SeedRecord;

pub const EVIDENCE_CACHE_VERSION: u32 = 1;
pub const DEFAULT_EVIDENCE_CACHE_FILE: &str = "evidence_cache.json";
/// How many finished candidates to buffer before an append + snapshot flush.
pub const DEFAULT_EVIDENCE_CACHE_BATCH: usize = 16;

/// Parameters that must match between the producing and reusing runs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvidenceCacheParams {
    /// Seeds recorded for provenance only. Candidate HTTP evidence is keyed by
    /// chain/address and remains reusable when the run's seed set changes.
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

/// Default cache path: `{output_dir}/intermediate/evidence_cache.json`.
pub fn default_evidence_cache_path(output_dir: &Path) -> PathBuf {
    super::layout::intermediate_path(output_dir, DEFAULT_EVIDENCE_CACHE_FILE)
}

fn companion_jsonl(path: &Path) -> PathBuf {
    path.with_extension("jsonl")
}

fn companion_meta(path: &Path) -> PathBuf {
    // evidence_cache.json → evidence_cache.meta.json
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("evidence_cache");
    path.with_file_name(format!("{stem}.meta.json"))
}

/// True when a full snapshot and/or incremental jsonl+meta exist.
pub fn evidence_cache_artifacts_present(path: &Path) -> bool {
    path.is_file() || (companion_meta(path).is_file() && companion_jsonl(path).is_file())
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

fn portable_bundle(bundle: &EvidenceBundle) -> EvidenceBundle {
    let mut b = bundle.clone();
    b.contract_id = 0;
    b
}

fn bundle_key(bundle: &EvidenceBundle) -> (String, String) {
    (
        bundle.chain.to_ascii_lowercase(),
        bundle.address.to_ascii_lowercase(),
    )
}

/// Build a portable cache from in-memory evidence (stable chain/address keys).
pub fn build_evidence_cache(
    params: EvidenceCacheParams,
    evidence: &AHashMap<ContractId, EvidenceBundle>,
) -> EvidenceCacheFile {
    let mut by_key: AHashMap<(String, String), EvidenceBundle> = AHashMap::new();
    for bundle in evidence.values() {
        let portable = portable_bundle(bundle);
        by_key.insert(bundle_key(&portable), portable);
    }
    let mut bundles: Vec<EvidenceBundle> = by_key.into_values().collect();
    bundles.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then_with(|| a.address.cmp(&b.address))
    });
    EvidenceCacheFile {
        version: EVIDENCE_CACHE_VERSION,
        params,
        bundles,
    }
}

/// Write cache JSON (compact, non-pretty) atomically via temp file + rename.
pub fn write_evidence_cache(path: &Path, cache: &EvidenceCacheFile) -> Result<(), Analysis2Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec(cache)
        .map_err(|e| Analysis2Error::invalid(format!("serialize evidence cache: {e}")))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &body)?;
    if let Err(error) = fs::rename(&tmp, path) {
        fs::write(path, &body).map_err(|e| {
            Analysis2Error::invalid(format!(
                "write evidence cache {} (rename failed: {error}): {e}",
                path.display()
            ))
        })?;
        let _ = fs::remove_file(&tmp);
    }
    Ok(())
}

/// Load and parse a full `evidence_cache.json` file.
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

/// Load from JSON snapshot and/or incremental JSONL.
///
/// **Resume performance:** do **not** parse both full snapshot and full jsonl
/// (that doubles multi‑GB load time). Prefer:
/// 1. jsonl + meta when present (covers mid-run flushes; source of truth)
/// 2. else full JSON snapshot
///
/// Looks for:
/// - `{path}` full snapshot
/// - `{stem}.meta.json` + `{stem}.jsonl` incremental stream
pub fn load_evidence_cache_resumable(path: &Path) -> Result<EvidenceCacheFile, Analysis2Error> {
    let meta_path = companion_meta(path);
    let jsonl_path = companion_jsonl(path);
    let has_json = path.is_file();
    let has_jsonl = meta_path.is_file() && jsonl_path.is_file();

    if !has_json && !has_jsonl {
        return Err(Analysis2Error::invalid(format!(
            "evidence cache not found at {} (or {}.jsonl + meta)",
            path.display(),
            path.with_extension("").display()
        )));
    }

    // Prefer jsonl-only when available — avoids double-reading a multi-GB snapshot.
    if has_jsonl {
        let meta_text = fs::read_to_string(&meta_path).map_err(|e| {
            Analysis2Error::invalid(format!("read evidence meta {}: {e}", meta_path.display()))
        })?;
        #[derive(Deserialize)]
        struct Meta {
            version: u32,
            params: EvidenceCacheParams,
        }
        let meta: Meta = serde_json::from_str(&meta_text).map_err(|e| {
            Analysis2Error::invalid(format!("parse evidence meta {}: {e}", meta_path.display()))
        })?;
        if meta.version != EVIDENCE_CACHE_VERSION {
            return Err(Analysis2Error::invalid(format!(
                "evidence cache version {} unsupported (expected {EVIDENCE_CACHE_VERSION})",
                meta.version
            )));
        }

        let mut by_key: AHashMap<(String, String), EvidenceBundle> = AHashMap::new();
        let file = File::open(&jsonl_path).map_err(|e| {
            Analysis2Error::invalid(format!("read evidence jsonl {}: {e}", jsonl_path.display()))
        })?;
        let mut line_count = 0_usize;
        for (line_no, line) in BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|e| {
                Analysis2Error::invalid(format!(
                    "read evidence jsonl {}: line {}: {e}",
                    jsonl_path.display(),
                    line_no + 1
                ))
            })?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let bundle: EvidenceBundle = serde_json::from_str(line).map_err(|e| {
                Analysis2Error::invalid(format!(
                    "parse evidence jsonl {}: line {}: {e}",
                    jsonl_path.display(),
                    line_no + 1
                ))
            })?;
            by_key.insert(bundle_key(&bundle), bundle);
            line_count += 1;
        }
        eprintln!(
            "evidence cache: loaded {} unique bundles from jsonl ({} lines) at {}",
            by_key.len(),
            line_count,
            jsonl_path.display()
        );
        let mut bundles: Vec<EvidenceBundle> = by_key.into_values().collect();
        bundles.sort_by(|a, b| {
            a.chain
                .cmp(&b.chain)
                .then_with(|| a.address.cmp(&b.address))
        });
        return Ok(EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: meta.params,
            bundles,
        });
    }

    // Snapshot-only path (no jsonl/meta pair).
    let cache = load_evidence_cache(path)?;
    eprintln!(
        "evidence cache: loaded {} bundles from snapshot {}",
        cache.bundles.len(),
        path.display()
    );
    Ok(cache)
}

/// Ensure the cache was produced with equivalent evidence completeness bounds.
///
/// Seed membership and API-key presence are deliberately excluded: cached
/// provider responses are candidate-scoped and remain useful across those run
/// configuration changes.
pub fn validate_evidence_cache(
    cache: &EvidenceCacheFile,
    expected: &EvidenceCacheParams,
) -> Result<(), Analysis2Error> {
    let got = &cache.params;
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
    Ok(())
}

/// Rematerialize evidence keyed by process-local contract ids.
///
/// Address match is **case-insensitive** so checksummed vs lowercased EVM
/// addresses still hit. Bundles absent from the snapshot are skipped.
pub fn rematerialize_evidence(
    store: &ResidentStore,
    cache: &EvidenceCacheFile,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error> {
    // Build a one-shot lowercased index so rematerialize does not depend on
    // interned address string exact match (common re-run miss → full HTTP).
    let mut by_lower: AHashMap<(String, String), ContractId> =
        AHashMap::with_capacity(store.contracts.len());
    for c in &store.contracts {
        let chain = store.chain_name(c.chain_id).to_ascii_lowercase();
        let addr = c.address.to_ascii_lowercase();
        by_lower.insert((chain, addr), c.id);
    }

    let mut out = AHashMap::with_capacity(cache.bundles.len());
    let mut skipped = 0_usize;
    for entry in &cache.bundles {
        let key = (
            entry.chain.to_ascii_lowercase(),
            entry.address.to_ascii_lowercase(),
        );
        let Some(&contract_id) = by_lower.get(&key) else {
            skipped += 1;
            continue;
        };
        let mut bundle = entry.clone();
        bundle.contract_id = contract_id;
        out.insert(contract_id, bundle);
    }
    if skipped > 0 {
        eprintln!(
            "evidence cache: skipped {skipped}/{} bundles not present in current snapshot identity",
            cache.bundles.len()
        );
    } else {
        eprintln!(
            "evidence cache: rematerialized {}/{} bundles into resident contract ids",
            out.len(),
            cache.bundles.len()
        );
    }
    Ok(out)
}

/// Incremental writer: append JSONL in batches and periodically rewrite the
/// full JSON snapshot so interrupts leave a reusable cache on disk.
pub struct EvidenceCacheSink {
    path: PathBuf,
    jsonl_path: PathBuf,
    params: EvidenceCacheParams,
    batch_size: usize,
    pending: Vec<EvidenceBundle>,
    /// All portable bundles known so far (for snapshot rewrites), keyed by chain+address.
    all: AHashMap<(String, String), EvidenceBundle>,
}

impl EvidenceCacheSink {
    /// Create or resume a sink. Writes meta if missing / overwrites meta with current params.
    pub fn create(
        path: &Path,
        params: EvidenceCacheParams,
        batch_size: usize,
    ) -> Result<Self, Analysis2Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let jsonl_path = companion_jsonl(path);
        let meta_path = companion_meta(path);

        // Seed in-memory index from any existing cache (resume mid-run).
        let mut all = AHashMap::new();
        if path.is_file() || (meta_path.is_file() && jsonl_path.is_file()) {
            if let Ok(existing) = load_evidence_cache_resumable(path) {
                if validate_evidence_cache(&existing, &params).is_ok() {
                    for b in existing.bundles {
                        all.insert(bundle_key(&b), b);
                    }
                } else {
                    // Params changed: start a fresh jsonl to avoid mixing.
                    eprintln!(
                        "evidence cache: params changed; truncating incremental jsonl at {}",
                        jsonl_path.display()
                    );
                    let _ = fs::remove_file(&jsonl_path);
                    all.clear();
                }
            }
        }

        let meta_body = serde_json::to_vec(&serde_json::json!({
            "version": EVIDENCE_CACHE_VERSION,
            "params": params,
        }))
        .map_err(|e| Analysis2Error::invalid(format!("serialize evidence meta: {e}")))?;
        fs::write(&meta_path, &meta_body)?;

        // If we seeded from snapshot only (no jsonl), rewrite jsonl from `all`
        // so incremental append stays consistent.
        if !jsonl_path.is_file() && !all.is_empty() {
            let mut f = File::create(&jsonl_path).map_err(|e| {
                Analysis2Error::invalid(format!("create evidence jsonl: {e}"))
            })?;
            let mut rows: Vec<_> = all.values().collect();
            rows.sort_by(|a, b| {
                a.chain
                    .cmp(&b.chain)
                    .then_with(|| a.address.cmp(&b.address))
            });
            for b in rows {
                let line = serde_json::to_string(b).map_err(|e| {
                    Analysis2Error::invalid(format!("serialize evidence jsonl: {e}"))
                })?;
                writeln!(f, "{line}").map_err(|e| {
                    Analysis2Error::invalid(format!("write evidence jsonl: {e}"))
                })?;
            }
            f.flush()
                .map_err(|e| Analysis2Error::invalid(format!("flush evidence jsonl: {e}")))?;
        }

        Ok(Self {
            path: path.to_path_buf(),
            jsonl_path,
            params,
            batch_size: batch_size.max(1),
            pending: Vec::new(),
            all,
        })
    }

    pub fn cached_count(&self) -> usize {
        self.all.len()
    }

    /// Index a bundle already known on disk (or from a prior resume load).
    /// Does **not** append to JSONL again.
    pub fn note_cached(&mut self, bundle: &EvidenceBundle) {
        let portable = portable_bundle(bundle);
        self.all.insert(bundle_key(&portable), portable);
    }

    /// Buffer one newly finished candidate; flush when the batch is full.
    pub fn push(&mut self, bundle: &EvidenceBundle) -> Result<(), Analysis2Error> {
        let portable = portable_bundle(bundle);
        let key = bundle_key(&portable);
        let is_new = !self.all.contains_key(&key);
        self.all.insert(key, portable.clone());
        // Always re-append when newly enriched so a restarted run that re-fetches
        // a key still records the latest bytes; note_cached avoids dup on seed.
        if is_new || !self.pending.iter().any(|b| bundle_key(b) == bundle_key(&portable)) {
            self.pending.push(portable);
        }
        if self.pending.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Append pending lines and rewrite the full JSON snapshot.
    pub fn flush(&mut self) -> Result<(), Analysis2Error> {
        if !self.pending.is_empty() {
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.jsonl_path)
                .map_err(|e| {
                    Analysis2Error::invalid(format!(
                        "open evidence jsonl {}: {e}",
                        self.jsonl_path.display()
                    ))
                })?;
            for b in self.pending.drain(..) {
                let line = serde_json::to_string(&b).map_err(|e| {
                    Analysis2Error::invalid(format!("serialize evidence jsonl: {e}"))
                })?;
                writeln!(f, "{line}").map_err(|e| {
                    Analysis2Error::invalid(format!(
                        "append evidence jsonl {}: {e}",
                        self.jsonl_path.display()
                    ))
                })?;
            }
            f.flush().map_err(|e| {
                Analysis2Error::invalid(format!(
                    "flush evidence jsonl {}: {e}",
                    self.jsonl_path.display()
                ))
            })?;
        }
        // Full snapshot for easy single-file reuse / tools.
        let mut bundles: Vec<EvidenceBundle> = self.all.values().cloned().collect();
        bundles.sort_by(|a, b| {
            a.chain
                .cmp(&b.chain)
                .then_with(|| a.address.cmp(&b.address))
        });
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: self.params.clone(),
            bundles,
        };
        write_evidence_cache(&self.path, &cache)?;
        Ok(())
    }

    /// Flush remaining and return final cache.
    pub fn finish(mut self) -> Result<EvidenceCacheFile, Analysis2Error> {
        self.flush()?;
        let mut bundles: Vec<EvidenceBundle> = self.all.into_values().collect();
        bundles.sort_by(|a, b| {
            a.chain
                .cmp(&b.chain)
                .then_with(|| a.address.cmp(&b.address))
        });
        Ok(EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: self.params,
            bundles,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::reporting::json::SeedRecord;
    use ahash::AHashSet;

    fn prepared() -> ResidentStore {
        let evm = ["ethereum"]
            .into_iter()
            .map(str::to_owned)
            .collect::<AHashSet<_>>();
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
            .ingest_identity_row(IdentityRow {
                chain: "ethereum".into(),
                contract_address: "0xdef".into(),
                token_id: "1".into(),
                name_norm: "n".into(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 1,
                },
            })
            .unwrap();
        store
    }

    fn params() -> EvidenceCacheParams {
        evidence_cache_params(
            &[SeedRecord {
                chain: "ethereum".into(),
                address: "0xseed".into(),
                rank: Some(1),
            }],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        )
    }

    #[test]
    fn round_trip_remaps_contract_id() {
        let store = prepared();
        let cid = store.contract_id("ethereum", "0xabc").unwrap();
        let mut bundle = EvidenceBundle::empty(cid, "ethereum", "0xabc");
        bundle.controllers.push("0xop".into());
        let mut map = AHashMap::new();
        map.insert(cid, bundle);

        let p = params();
        let cache = build_evidence_cache(p.clone(), &map);
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
        validate_evidence_cache(&loaded, &p).unwrap();
        let remapped = rematerialize_evidence(&store, &loaded).unwrap();
        assert_eq!(remapped.len(), 1);
        let got = remapped.get(&cid).unwrap();
        assert_eq!(got.contract_id, cid);
        assert_eq!(got.controllers, vec!["0xop".to_owned()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_sink_survives_without_finish() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let d = store.contract_id("ethereum", "0xdef").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis2_evidence_sink_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let p = params();

        {
            let mut sink = EvidenceCacheSink::create(&path, p.clone(), 2).unwrap();
            sink.push(&EvidenceBundle::empty(a, "ethereum", "0xabc"))
                .unwrap();
            // batch not full — force flush as if batch completed mid-run
            sink.push(&EvidenceBundle::empty(d, "ethereum", "0xdef"))
                .unwrap();
            // drop without finish — flush already ran at batch_size=2
        }

        assert!(path.is_file());
        assert!(companion_jsonl(&path).is_file());
        let loaded = load_evidence_cache_resumable(&path).unwrap();
        validate_evidence_cache(&loaded, &p).unwrap();
        assert_eq!(loaded.bundles.len(), 2);
        let map = rematerialize_evidence(&store, &loaded).unwrap();
        assert!(map.contains_key(&a));
        assert!(map.contains_key(&d));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_sink_keeps_bundles_when_seed_and_key_presence_change() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis2_evidence_sink_compat_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let cached_params = params();

        {
            let mut sink = EvidenceCacheSink::create(&path, cached_params.clone(), 1).unwrap();
            sink.push(&EvidenceBundle::empty(a, "ethereum", "0xabc"))
                .unwrap();
            sink.finish().unwrap();
        }

        let mut current_params = cached_params;
        current_params.seeds = vec![SeedRecord {
            chain: "base".into(),
            address: "0xnew-seed".into(),
            rank: None,
        }];
        current_params.had_alchemy = true;
        let sink = EvidenceCacheSink::create(&path, current_params, 1).unwrap();
        assert_eq!(sink.cached_count(), 1);
        sink.finish().unwrap();

        let loaded = load_evidence_cache_resumable(&path).unwrap();
        assert_eq!(loaded.bundles.len(), 1);
        assert_eq!(loaded.bundles[0].address, "0xabc");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_accepts_seed_and_key_presence_changes() {
        let cached =
            evidence_cache_params(&[], "seeds.json", &ApiKeys::default(), &HttpLimits::default());
        let mut current = cached.clone();
        current.seeds = vec![SeedRecord {
            chain: "polygon".into(),
            address: "0xother-seed".into(),
            rank: Some(99),
        }];
        current.seeds_path = "different-seeds.json".into();
        current.had_alchemy = true;
        current.had_etherscan = true;
        current.had_helius = true;
        current.had_opensea = true;
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: cached,
            bundles: Vec::new(),
        };

        validate_evidence_cache(&cache, &current)
            .expect("candidate evidence must survive seed and key-presence changes");
    }

    #[test]
    fn validate_still_rejects_pagination_changes() {
        let cached =
            evidence_cache_params(&[], "seeds.json", &ApiKeys::default(), &HttpLimits::default());
        let mut current = cached.clone();
        current.max_transfer_pages += 1;
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: cached,
            bundles: Vec::new(),
        };

        assert!(validate_evidence_cache(&cache, &current).is_err());
    }
}
