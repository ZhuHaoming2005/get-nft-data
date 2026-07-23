//! `select_seeds` orchestration and `seeds.json` / `seeds.audit.json` writers.

use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::enrich::http::HttpClient;
use crate::enrich::opensea::{self, OpenSeaRankedItem};
use crate::error::Analysis2Error;

use super::address::is_evm_chain;
use super::magic_eden::{self, MagicEdenCollection};

/// Selected seed row written to `seeds.json` (compatible with run-dedup reader).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeedRecord {
    pub chain: String,
    pub address: String,
    pub rank: u32,
    pub name: String,
    pub metric: String,
    pub window: String,
    pub source: String,
    pub collected_at: DateTime<Utc>,
}

/// Options for per-chain top-N seed selection.
#[derive(Clone, Debug)]
pub struct SelectSeedsOptions {
    pub chains: Vec<String>,
    pub seeds_per_chain: usize,
    pub opensea_api_key: Option<String>,
    pub helius_api_key: Option<String>,
    pub http_concurrency: usize,
    /// Override OpenSea base URL (tests / mirrors).
    pub opensea_base_url: Option<String>,
    /// Override Magic Eden base URL.
    pub magic_eden_base_url: Option<String>,
    /// Override Helius RPC URL.
    pub helius_base_url: Option<String>,
}

impl Default for SelectSeedsOptions {
    fn default() -> Self {
        Self {
            chains: vec![
                "ethereum".into(),
                "base".into(),
                "polygon".into(),
                "solana".into(),
            ],
            seeds_per_chain: 25,
            opensea_api_key: None,
            helius_api_key: None,
            http_concurrency: 32,
            opensea_base_url: None,
            magic_eden_base_url: None,
            helius_base_url: None,
        }
    }
}

/// Select top-N seeds per chain. Incomplete chains are recorded in audit and not
/// backfilled from other chains.
///
/// Creates a dedicated Tokio runtime. From an existing async context, call
/// [`select_seeds_async`] instead.
pub fn select_seeds(
    opts: &SelectSeedsOptions,
) -> Result<(Vec<SeedRecord>, Value), Analysis2Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Analysis2Error::invalid(
            "select_seeds cannot run inside an async runtime; use select_seeds_async",
        ));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Analysis2Error::http(format!("tokio runtime: {e}")))?;
    runtime.block_on(select_seeds_async(opts))
}

/// Async entry for seed selection (preferred inside existing Tokio runtimes / tests).
pub async fn select_seeds_async(
    opts: &SelectSeedsOptions,
) -> Result<(Vec<SeedRecord>, Value), Analysis2Error> {
    if opts.seeds_per_chain == 0 {
        return Err(Analysis2Error::invalid(
            "seeds_per_chain must be positive",
        ));
    }
    let chains = normalize_chains(&opts.chains)?;
    if chains.is_empty() {
        return Err(Analysis2Error::invalid("at least one chain is required"));
    }

    let client = HttpClient::new(opts.http_concurrency)?;
    let collected_at = Utc::now();
    let mut seeds = Vec::new();
    let mut chain_audit = serde_json::Map::new();

    for chain in &chains {
        let (chain_seeds, status) = select_chain(&client, opts, chain, collected_at).await?;
        seeds.extend(chain_seeds);
        chain_audit.insert(chain.clone(), status);
    }

    let audit = json!({
        "generated_at": collected_at,
        "seeds_per_chain": opts.seeds_per_chain,
        "chains": Value::Object(chain_audit),
    });
    Ok((seeds, audit))
}

async fn select_chain(
    client: &HttpClient,
    opts: &SelectSeedsOptions,
    chain: &str,
    collected_at: DateTime<Utc>,
) -> Result<(Vec<SeedRecord>, Value), Analysis2Error> {
    let requested = opts.seeds_per_chain;
    if is_evm_chain(chain) {
        select_evm_chain(client, opts, chain, requested, collected_at).await
    } else if chain == "solana" {
        select_solana_chain(client, opts, requested, collected_at).await
    } else {
        Ok((
            Vec::new(),
            incomplete_status(requested, 0, format!("unsupported chain: {chain}")),
        ))
    }
}

async fn select_evm_chain(
    client: &HttpClient,
    opts: &SelectSeedsOptions,
    chain: &str,
    requested: usize,
    collected_at: DateTime<Utc>,
) -> Result<(Vec<SeedRecord>, Value), Analysis2Error> {
    let Some(api_key) = opts
        .opensea_api_key
        .as_deref()
        .map(str::trim)
        .filter(|k| !k.is_empty())
    else {
        return Ok((
            Vec::new(),
            incomplete_status(requested, 0, "missing_opensea_api_key"),
        ));
    };
    let base = opts
        .opensea_base_url
        .as_deref()
        .unwrap_or(opensea::default_base_url());
    let ranked = match opensea::fetch_top_contracts(client, base, api_key, chain, requested).await
    {
        Ok(items) => items,
        Err(error) => {
            crate::enrich::print_provider_error("opensea", "select_seeds_top_collections", &error.to_string());
            return Ok((
                Vec::new(),
                incomplete_status(requested, 0, format!("opensea_error: {error}")),
            ));
        }
    };
    let seeds = ranked
        .into_iter()
        .enumerate()
        .map(|(index, item)| evm_seed(item, (index + 1) as u32, collected_at))
        .collect::<Vec<_>>();
    let collected = seeds.len();
    let status = if collected >= requested {
        complete_status(requested, collected)
    } else {
        incomplete_status(
            requested,
            collected,
            format!("collected {collected} of {requested}"),
        )
    };
    Ok((seeds, status))
}

async fn select_solana_chain(
    client: &HttpClient,
    opts: &SelectSeedsOptions,
    requested: usize,
    collected_at: DateTime<Utc>,
) -> Result<(Vec<SeedRecord>, Value), Analysis2Error> {
    let me_base = opts
        .magic_eden_base_url
        .as_deref()
        .unwrap_or(magic_eden::default_base_url());
    let helius_rpc = opts
        .helius_base_url
        .as_deref()
        .unwrap_or(crate::enrich::helius::default_rpc_url());
    let helius_key = opts.helius_api_key.as_deref();

    let collections = match magic_eden::fetch_popular_collections(client, me_base).await {
        Ok(items) => items,
        Err(error) => {
            crate::enrich::print_provider_error(
                "magic_eden",
                "select_seeds_popular_collections",
                &error.to_string(),
            );
            return Ok((
                Vec::new(),
                incomplete_status(requested, 0, format!("magic_eden_error: {error}")),
            ));
        }
    };

    let mut seeds = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut missing_helius_api_key = false;
    let mut resolve_errors: Vec<String> = Vec::new();
    for collection in collections {
        if seeds.len() >= requested {
            break;
        }
        let address = match magic_eden::resolve_collection_address(
            client,
            me_base,
            helius_rpc,
            helius_key,
            &collection,
        )
        .await
        {
            Ok(Some(address)) => address,
            Ok(None) => continue,
            Err(error) => {
                match &error {
                    Analysis2Error::Invalid(message) if message == "missing_helius_api_key" => {
                        missing_helius_api_key = true;
                    }
                    _ if resolve_errors.len() < 5 => {
                        resolve_errors.push(error.to_string());
                    }
                    _ => {}
                }
                continue;
            }
        };
        if !seen.insert(address.clone()) {
            continue;
        }
        seeds.push(solana_seed(
            &collection,
            &address,
            seeds.len() as u32 + 1,
            collected_at,
        ));
    }

    let collected = seeds.len();
    let status = if collected >= requested {
        complete_status(requested, collected)
    } else {
        let reason = solana_incomplete_reason(
            requested,
            collected,
            missing_helius_api_key,
            &resolve_errors,
        );
        incomplete_status(requested, collected, reason)
    };
    Ok((seeds, status))
}

fn solana_incomplete_reason(
    requested: usize,
    collected: usize,
    missing_helius_api_key: bool,
    resolve_errors: &[String],
) -> String {
    if missing_helius_api_key {
        return "missing_helius_api_key".into();
    }
    if !resolve_errors.is_empty() {
        return format!(
            "collected {collected} of {requested}; {}",
            resolve_errors.join("; ")
        );
    }
    format!("collected {collected} of {requested}")
}

fn evm_seed(item: OpenSeaRankedItem, rank: u32, collected_at: DateTime<Utc>) -> SeedRecord {
    SeedRecord {
        chain: item.chain,
        address: item.address,
        rank,
        name: item.name,
        metric: "thirty_days_volume".into(),
        window: "30d".into(),
        source: "opensea".into(),
        collected_at,
    }
}

fn solana_seed(
    collection: &MagicEdenCollection,
    address: &str,
    rank: u32,
    collected_at: DateTime<Utc>,
) -> SeedRecord {
    SeedRecord {
        chain: "solana".into(),
        address: address.to_owned(),
        rank,
        name: collection.name.clone(),
        metric: "magic_eden_30d_popularity".into(),
        window: "30d".into(),
        source: "magic_eden".into(),
        collected_at,
    }
}

fn complete_status(requested: usize, collected: usize) -> Value {
    json!({
        "requested": requested,
        "collected": collected,
        "complete": true,
    })
}

fn incomplete_status(requested: usize, collected: usize, reason: impl Into<String>) -> Value {
    json!({
        "requested": requested,
        "collected": collected,
        "complete": false,
        "reason": reason.into(),
    })
}

fn normalize_chains(chains: &[String]) -> Result<Vec<String>, Analysis2Error> {
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for chain in chains {
        let normalized = chain.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if normalized == "matic" {
            if seen.insert("polygon".into()) {
                out.push("polygon".into());
            }
            continue;
        }
        if !seen.insert(normalized.clone()) {
            continue;
        }
        out.push(normalized);
    }
    Ok(out)
}

/// Write `seeds.json` + `seeds.audit.json` under `output_dir`.
pub fn write_seed_outputs(
    output_dir: &Path,
    seeds: &[SeedRecord],
    audit: &Value,
) -> Result<(), Analysis2Error> {
    fs::create_dir_all(output_dir)?;
    let seeds_path = output_dir.join("seeds.json");
    let audit_path = output_dir.join("seeds.audit.json");
    let seeds_json = serde_json::to_vec_pretty(seeds)
        .map_err(|e| Analysis2Error::invalid(format!("serialize seeds.json: {e}")))?;
    let audit_json = serde_json::to_vec_pretty(audit)
        .map_err(|e| Analysis2Error::invalid(format!("serialize seeds.audit.json: {e}")))?;
    fs::write(&seeds_path, seeds_json)?;
    fs::write(&audit_path, audit_json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporting::load_seeds_json;
    use httpmock::prelude::*;
    use serde_json::json;

    #[test]
    fn missing_opensea_key_records_incomplete_without_backfill() {
        let (seeds, audit) = select_seeds(&SelectSeedsOptions {
            chains: vec!["ethereum".into(), "base".into()],
            seeds_per_chain: 2,
            opensea_api_key: None,
            ..SelectSeedsOptions::default()
        })
        .unwrap();
        assert!(seeds.is_empty());
        assert_eq!(audit["chains"]["ethereum"]["complete"], false);
        assert_eq!(audit["chains"]["base"]["complete"], false);
        assert_eq!(
            audit["chains"]["ethereum"]["reason"],
            "missing_opensea_api_key"
        );
        // No backfill: ethereum shortfall does not inflate base.
        assert_eq!(audit["chains"]["ethereum"]["collected"], 0);
        assert_eq!(audit["chains"]["base"]["collected"], 0);
    }

    #[tokio::test]
    async fn missing_helius_key_when_resolve_needed_records_audit_reason() {
        let server = MockServer::start_async().await;
        let _me_popular = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/marketplace/popular_collections")
                    .query_param("timeRange", "30d");
                then.status(200).json_body(json!([{
                    "symbol": "needs-resolve",
                    "name": "Needs Resolve"
                }]));
            })
            .await;

        let (seeds, audit) = select_seeds_async(&SelectSeedsOptions {
            chains: vec!["solana".into()],
            seeds_per_chain: 1,
            helius_api_key: None,
            magic_eden_base_url: Some(server.base_url()),
            ..SelectSeedsOptions::default()
        })
        .await
        .unwrap();

        assert!(seeds.is_empty());
        assert_eq!(audit["chains"]["solana"]["complete"], false);
        assert_eq!(
            audit["chains"]["solana"]["reason"],
            "missing_helius_api_key"
        );
    }

    #[tokio::test]
    async fn solana_resolve_http_error_surfaces_in_audit_reason() {
        let server = MockServer::start_async().await;
        let _me_popular = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/marketplace/popular_collections")
                    .query_param("timeRange", "30d");
                then.status(200).json_body(json!([{
                    "symbol": "needs-resolve",
                    "name": "Needs Resolve"
                }]));
            })
            .await;
        let _me_listings = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/collections/needs-resolve/listings");
                then.status(500).body("boom");
            })
            .await;
        let _me_activities = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/collections/needs-resolve/activities");
                then.status(500).body("boom");
            })
            .await;

        let (_, audit) = select_seeds_async(&SelectSeedsOptions {
            chains: vec!["solana".into()],
            seeds_per_chain: 1,
            helius_api_key: Some("helius-key".into()),
            magic_eden_base_url: Some(server.base_url()),
            helius_base_url: Some(format!("{}/", server.base_url())),
            ..SelectSeedsOptions::default()
        })
        .await
        .unwrap();

        assert_eq!(audit["chains"]["solana"]["complete"], false);
        let reason = audit["chains"]["solana"]["reason"].as_str().unwrap();
        assert!(
            reason.contains("magic_eden_listing_error") || reason.contains("http"),
            "expected resolve/HTTP error in audit reason, got {reason}"
        );
    }

    #[tokio::test]
    async fn http_mock_opensea_ranking_and_solana_resolve() {
        let server = MockServer::start_async().await;

        let _opensea = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/v2/collections/top")
                    .query_param("chains", "ethereum")
                    .query_param("sort_by", "thirty_days_volume");
                then.status(200).json_body(json!({
                    "collections": [{
                        "name": "Alpha",
                        "collection": "alpha",
                        "thirty_days_volume": 100,
                        "contracts": [{
                            "chain": "ethereum",
                            "address": "0x1111111111111111111111111111111111111111"
                        }]
                    }]
                }));
            })
            .await;

        let _opensea_polygon = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/v2/collections/top")
                    .query_param("chains", "polygon")
                    .query_param("sort_by", "thirty_days_volume");
                then.status(200).json_body(json!({
                    "collections": [{
                        "name": "Poly",
                        "collection": "poly",
                        "thirty_days_volume": 50,
                        "contracts": [{
                            "chain": "matic",
                            "address": "0x2222222222222222222222222222222222222222"
                        }]
                    }]
                }));
            })
            .await;

        let _me_popular = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/marketplace/popular_collections")
                    .query_param("timeRange", "30d");
                then.status(200).json_body(json!([
                    {
                        "symbol": "needs-resolve",
                        "name": "Needs Resolve"
                    },
                    {
                        "symbol": "direct",
                        "name": "Direct",
                        "onChainCollectionAddress": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    }
                ]));
            })
            .await;

        let _me_listings = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/collections/needs-resolve/listings");
                then.status(200).json_body(json!([{
                    "mintAddress": "So11111111111111111111111111111111111111112"
                }]));
            })
            .await;

        let _helius = server
            .mock_async(|when, then| {
                when.method(POST).path("/");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "seed-collection-So11111111111111111111111111111111111111112",
                    "result": {
                        "id": "So11111111111111111111111111111111111111112",
                        "grouping": [{
                            "group_key": "collection",
                            "group_value": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
                        }]
                    }
                }));
            })
            .await;

        let base = server.base_url();
        let (seeds, audit) = select_seeds_async(&SelectSeedsOptions {
            chains: vec!["ethereum".into(), "polygon".into(), "solana".into()],
            seeds_per_chain: 2,
            opensea_api_key: Some("os-key".into()),
            helius_api_key: Some("helius-key".into()),
            http_concurrency: 4,
            opensea_base_url: Some(base.clone()),
            magic_eden_base_url: Some(base.clone()),
            helius_base_url: Some(format!("{base}/")),
            ..SelectSeedsOptions::default()
        })
        .await
        .unwrap();

        let eth: Vec<_> = seeds.iter().filter(|s| s.chain == "ethereum").collect();
        let poly: Vec<_> = seeds.iter().filter(|s| s.chain == "polygon").collect();
        let sol: Vec<_> = seeds.iter().filter(|s| s.chain == "solana").collect();
        assert_eq!(eth.len(), 1);
        assert_eq!(eth[0].address, "0x1111111111111111111111111111111111111111");
        assert_eq!(eth[0].source, "opensea");
        assert_eq!(eth[0].metric, "thirty_days_volume");
        assert_eq!(audit["chains"]["ethereum"]["complete"], false);
        assert_eq!(audit["chains"]["ethereum"]["collected"], 1);

        assert_eq!(poly.len(), 1);
        assert_eq!(poly[0].address, "0x2222222222222222222222222222222222222222");
        assert_eq!(audit["chains"]["polygon"]["collected"], 1);

        assert_eq!(sol.len(), 2);
        assert_eq!(sol[0].address, "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263");
        assert_eq!(sol[0].source, "magic_eden");
        assert_eq!(sol[1].address, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(audit["chains"]["solana"]["complete"], true);

        let dir = std::env::temp_dir().join(format!(
            "analysis2-seed-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        write_seed_outputs(&dir, &seeds, &audit).unwrap();
        let loaded = load_seeds_json(&dir.join("seeds.json")).unwrap();
        assert_eq!(loaded.len(), seeds.len());
        assert_eq!(loaded[0].chain, seeds[0].chain);
        assert_eq!(loaded[0].address, seeds[0].address);
        assert_eq!(loaded[0].rank, Some(seeds[0].rank));
        assert!(dir.join("seeds.audit.json").is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
