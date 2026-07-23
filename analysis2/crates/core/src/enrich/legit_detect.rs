//! Relation-level legit detection: controller continuity, OpenSea slug, seed NFT interaction.

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::AHashMap;
use tokio::sync::Semaphore;

use crate::dedup::candidates::CandidateRegistry;
use crate::entity::{ContractId, ResidentStore};

use super::alchemy;
use super::controllers::{self, normalize_evm_address};
use super::helius;
use super::http::HttpClient;
use super::opensea;
use super::types::{
    finalize_legit_signals, ApiKeys, EvidenceBundle, HttpLimits, LegitSignals,
};

/// Seed-side cache reused across all candidates for that seed.
#[derive(Clone, Debug, Default)]
struct SeedCache {
    controllers: Vec<String>,
    collection_slug: Option<String>,
    /// Normalized addresses that appear as from/to on seed NFT transfers.
    transfer_counterparties: BTreeSet<String>,
    /// Current owners of seed NFTs (Solana) — for holds check.
    current_owners: BTreeSet<String>,
    controllers_probed: bool,
}

fn seed_key(chain: &str, address: &str) -> String {
    format!("{chain}:{address}")
}

fn normalize_addr(chain: &str, address: &str) -> String {
    if chain.eq_ignore_ascii_case("solana") {
        address.trim().to_owned()
    } else {
        normalize_evm_address(address)
            .unwrap_or_else(|| address.trim().to_ascii_lowercase())
    }
}

fn controller_set(addrs: &[String], chain: &str) -> BTreeSet<String> {
    addrs
        .iter()
        .map(|a| normalize_addr(chain, a))
        .filter(|a| !a.is_empty())
        .collect()
}

async fn resolve_collection_slug(
    client: &HttpClient,
    limits: &HttpLimits,
    keys: &ApiKeys,
    chain: &str,
    address: &str,
) -> Option<String> {
    if chain.eq_ignore_ascii_case("solana") {
        // OpenSea Solana contract slug when key present.
        return opensea::fetch_contract_collection_slug(
            client,
            &limits.endpoints.opensea,
            keys.opensea(),
            chain,
            address,
        )
        .await
        .map(|s| s.to_ascii_lowercase());
    }
    if let Some(slug) = alchemy::fetch_collection_slug(
        client,
        &limits.endpoints,
        keys.alchemy(),
        chain,
        address,
    )
    .await
    {
        return Some(slug.to_ascii_lowercase());
    }
    opensea::fetch_contract_collection_slug(
        client,
        &limits.endpoints.opensea,
        keys.opensea(),
        chain,
        address,
    )
    .await
    .map(|s| s.to_ascii_lowercase())
}

async fn build_seed_cache(
    client: &HttpClient,
    store: &ResidentStore,
    evidence: &AHashMap<ContractId, EvidenceBundle>,
    keys: &ApiKeys,
    limits: &HttpLimits,
    seed_id: ContractId,
) -> SeedCache {
    let contract = &store.contracts[seed_id as usize];
    let chain = store.chain_name(contract.chain_id).to_owned();
    let address = contract.address.clone();
    let is_evm = store.is_evm_chain(&chain);

    let mut cache = SeedCache::default();

    // Controllers: reuse candidate enrich if seed was also a candidate.
    if let Some(bundle) = evidence.get(&seed_id) {
        cache.controllers = bundle.controllers.clone();
        cache.controllers_probed = true;
    } else if is_evm {
        let outcome = controllers::fetch_evm_controllers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
        )
        .await;
        cache.controllers = outcome.value;
        cache.controllers_probed = !matches!(
            outcome.status,
            super::types::EvidenceStatus::NotRequested
        );
    } else {
        let snapshot = helius::fetch_collection_assets(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            &address,
            limits.max_solana_assets.min(50).max(1),
        )
        .await;
        cache.controllers = snapshot.value.authority.clone();
        for asset in &snapshot.value.assets {
            if let Some(owner) = &asset.owner {
                cache
                    .current_owners
                    .insert(normalize_addr(&chain, owner));
            }
        }
        cache.controllers_probed = !matches!(
            snapshot.status,
            super::types::EvidenceStatus::NotRequested
        );
    }

    cache.collection_slug =
        resolve_collection_slug(client, limits, keys, &chain, &address).await;

    if is_evm {
        let transfers = alchemy::fetch_transfers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
            limits.max_transfer_pages.min(3).max(1),
        )
        .await;
        for t in &transfers.value {
            if !t.from.is_empty() {
                cache
                    .transfer_counterparties
                    .insert(normalize_addr(&chain, &t.from));
            }
            if !t.to.is_empty() {
                cache
                    .transfer_counterparties
                    .insert(normalize_addr(&chain, &t.to));
            }
        }
    } else if cache.current_owners.is_empty() {
        // Seed not in evidence: already loaded owners above when fetching assets.
        // If controllers came from evidence reuse, still need owners for holds.
        if evidence.contains_key(&seed_id) {
            let snapshot = helius::fetch_collection_assets(
                client,
                &limits.endpoints.helius,
                keys.helius(),
                &address,
                limits.max_solana_assets.min(50).max(1),
            )
            .await;
            for asset in &snapshot.value.assets {
                if let Some(owner) = &asset.owner {
                    cache
                        .current_owners
                        .insert(normalize_addr(&chain, owner));
                }
            }
        }
    }

    cache
}

fn continuity_signals(
    seed_controllers: &[String],
    cand_controllers: &[String],
    chain: &str,
    probed_both: bool,
) -> LegitSignals {
    let mut signals = LegitSignals {
        verification_complete: probed_both,
        ..LegitSignals::default()
    };
    let seed_set = controller_set(seed_controllers, chain);
    let cand_set = controller_set(cand_controllers, chain);
    for addr in seed_set.intersection(&cand_set) {
        signals.official_controller_continuity = true;
        signals
            .evidence_keys
            .push(format!("controller_continuity:{addr}"));
    }
    signals
}

async fn probe_relation(
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    chain: &str,
    is_evm: bool,
    seed_address: &str,
    candidate_address: &str,
    cand_controllers: &[String],
    cand_slug: &mut Option<String>,
    seed: &SeedCache,
) -> LegitSignals {
    let mut signals = continuity_signals(
        &seed.controllers,
        cand_controllers,
        chain,
        seed.controllers_probed && !cand_controllers.is_empty(),
    );
    // Controllers probed on candidate side even if empty when enrich ran with key.
    if seed.controllers_probed {
        signals.verification_complete = true;
    }

    // OpenSea / Alchemy collection slug match.
    if cand_slug.is_none() {
        *cand_slug = resolve_collection_slug(client, limits, keys, chain, candidate_address).await;
    }
    if let (Some(seed_slug), Some(cand_slug)) = (&seed.collection_slug, cand_slug.as_ref()) {
        if !seed_slug.is_empty() && seed_slug == cand_slug {
            signals.official_collection_relation = true;
            signals
                .evidence_keys
                .push(format!("opensea_collection:{seed_slug}"));
        }
    }

    let cand_norm = normalize_addr(chain, candidate_address);

    // Current holds seed NFT.
    if is_evm {
        match alchemy::is_holder_of_contract(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            candidate_address,
            seed_address,
        )
        .await
        {
            Ok(Some(true)) => {
                signals.seed_nft_interaction = true;
                signals.evidence_keys.push("holds_seed_nft".into());
            }
            Ok(Some(false)) | Ok(None) => {}
            Err(_) => {}
        }
    } else if seed.current_owners.contains(&cand_norm) {
        signals.seed_nft_interaction = true;
        signals.evidence_keys.push("holds_seed_nft".into());
    }

    // Historical transfer counterparty.
    if seed.transfer_counterparties.contains(&cand_norm) {
        signals.seed_nft_interaction = true;
        if !signals
            .evidence_keys
            .iter()
            .any(|k| k == "seed_transfer_counterparty")
        {
            signals
                .evidence_keys
                .push("seed_transfer_counterparty".into());
        }
    }

    signals
}

/// After candidate enrich: attach per-seed relation legit signals onto each bundle.
pub async fn attach_relation_legit(
    evidence: &mut AHashMap<ContractId, EvidenceBundle>,
    registry: &CandidateRegistry,
    store: &ResidentStore,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
) {
    let seed_ids: Vec<ContractId> = {
        let mut set = BTreeSet::new();
        for rel in registry.relations() {
            set.insert(rel.seed_contract);
        }
        set.into_iter().collect()
    };
    if seed_ids.is_empty() {
        for bundle in evidence.values_mut() {
            finalize_legit_signals(bundle);
        }
        return;
    }

    let semaphore = Arc::new(Semaphore::new(limits.concurrency.max(1)));
    let mut seed_caches: AHashMap<ContractId, SeedCache> = AHashMap::new();

    // Build seed caches (sequential is fine; seeds << candidates). Parallel optional.
    for seed_id in &seed_ids {
        let _permit = semaphore.acquire().await.ok();
        let cache =
            build_seed_cache(client, store, evidence, keys, limits, *seed_id).await;
        seed_caches.insert(*seed_id, cache);
    }

    // Candidate slug cache to avoid repeat lookups.
    let mut cand_slugs: AHashMap<ContractId, Option<String>> = AHashMap::new();

    for rel in registry.relations() {
        let Some(bundle) = evidence.get_mut(&rel.candidate_contract) else {
            continue;
        };
        let seed_row = &store.contracts[rel.seed_contract as usize];
        let seed_chain = store.chain_name(seed_row.chain_id).to_owned();
        let seed_address = seed_row.address.clone();
        let is_evm = store.is_evm_chain(&seed_chain);
        let seed_cache = seed_caches
            .get(&rel.seed_contract)
            .cloned()
            .unwrap_or_default();
        let cand_controllers = bundle.controllers.clone();
        let cand_address = bundle.address.clone();
        let cand_id = rel.candidate_contract;

        let slug_entry = cand_slugs.entry(cand_id).or_insert(None);
        // probe needs &mut Option — clone out, probe, write back
        let mut slug = slug_entry.clone();
        let signals = probe_relation(
            client,
            keys,
            limits,
            &seed_chain,
            is_evm,
            &seed_address,
            &cand_address,
            &cand_controllers,
            &mut slug,
            &seed_cache,
        )
        .await;
        *slug_entry = slug;

        let key = seed_key(&seed_chain, &seed_address);
        bundle.relation_legit.insert(key, signals);
    }

    for bundle in evidence.values_mut() {
        finalize_legit_signals(bundle);
    }
}

/// Pure helper for unit tests: continuity + slug + interaction without HTTP.
pub fn classify_relation_offline(
    seed_controllers: &[String],
    cand_controllers: &[String],
    chain: &str,
    seed_slug: Option<&str>,
    cand_slug: Option<&str>,
    holds_seed: bool,
    transfer_counterparty: bool,
) -> LegitSignals {
    let mut signals = continuity_signals(seed_controllers, cand_controllers, chain, true);
    if let (Some(a), Some(b)) = (seed_slug, cand_slug) {
        if !a.is_empty() && a.eq_ignore_ascii_case(b) {
            signals.official_collection_relation = true;
            signals
                .evidence_keys
                .push(format!("opensea_collection:{}", a.to_ascii_lowercase()));
        }
    }
    if holds_seed {
        signals.seed_nft_interaction = true;
        signals.evidence_keys.push("holds_seed_nft".into());
    }
    if transfer_counterparty {
        signals.seed_nft_interaction = true;
        signals
            .evidence_keys
            .push("seed_transfer_counterparty".into());
    }
    signals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_controller_marks_continuity() {
        let s = classify_relation_offline(
            &["0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa".into()],
            &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()],
            "ethereum",
            None,
            None,
            false,
            false,
        );
        assert!(s.official_controller_continuity);
        assert!(s.is_legit_duplicate());
    }

    #[test]
    fn same_slug_marks_collection_relation() {
        let s = classify_relation_offline(
            &[],
            &[],
            "ethereum",
            Some("boredapeyachtclub"),
            Some("BoredApeYachtClub"),
            false,
            false,
        );
        assert!(s.official_collection_relation);
        assert!(s.is_legit_duplicate());
    }

    #[test]
    fn holds_or_transfer_marks_interaction() {
        let hold = classify_relation_offline(&[], &[], "ethereum", None, None, true, false);
        assert!(hold.seed_nft_interaction);
        let xfer = classify_relation_offline(&[], &[], "ethereum", None, None, false, true);
        assert!(xfer.seed_nft_interaction);
    }
}
