//! Relation-level legit detection: controller continuity, OpenSea slug, seed NFT interaction.

use std::collections::BTreeSet;

use ahash::{AHashMap, AHashSet};
use futures_util::{StreamExt, stream};

use crate::dedup::candidates::CandidateRegistry;
use crate::entity::{ContractId, ResidentStore};
use crate::error::Analysis2Error;
use crate::progress::{NoopProgress, ProgressObserver};

use super::alchemy;
use super::controllers::{self, normalize_evm_address};
use super::helius;
use super::http::HttpClient;
use super::opensea;
use super::types::{
    ApiKeys, EvidenceBundle, EvidenceStatus, HttpLimits, LegitSignals, finalize_legit_signals,
};

/// Seed-side cache reused across all candidates for that seed.
#[derive(Clone, Debug, Default)]
struct SeedCache {
    controllers: Vec<String>,
    collection_slug: Option<String>,
    /// Normalized addresses that appear as from/to on seed NFT transfers.
    transfer_counterparties: BTreeSet<String>,
    /// Current owners of seed NFTs used for in-memory holds checks.
    current_owners: BTreeSet<String>,
    /// A missing owner is conclusive only when every EVM holder page was read.
    current_owners_complete: bool,
    controllers_probed: bool,
}

#[derive(Clone, Debug)]
struct CandidateProbe {
    chain: String,
    address: String,
    controllers: Vec<String>,
    collection_slug: Option<String>,
}

pub(super) struct LegitPreflight {
    pub evidence: AHashMap<ContractId, EvidenceBundle>,
    pub candidates_to_enrich: Vec<ContractId>,
}

fn seed_key(chain: &str, address: &str) -> String {
    format!("{chain}:{address}")
}

fn normalize_addr(chain: &str, address: &str) -> String {
    if chain.eq_ignore_ascii_case("solana") {
        address.trim().to_owned()
    } else {
        normalize_evm_address(address).unwrap_or_else(|| address.trim().to_ascii_lowercase())
    }
}

fn controller_set(addrs: &[String], chain: &str) -> BTreeSet<String> {
    addrs
        .iter()
        .map(|a| normalize_addr(chain, a))
        .filter(|a| !a.is_empty())
        .collect()
}

/// Resolve a collection identity string for legit "same collection" matching.
///
/// Preference order (OpenSea only as last resort, and never for Solana):
/// - Solana: Helius DAS metadata symbol/name for the collection address
/// - EVM: Alchemy NFT collection slug → OpenSea contract slug fallback
async fn resolve_collection_slug(
    client: &HttpClient,
    limits: &HttpLimits,
    keys: &ApiKeys,
    chain: &str,
    address: &str,
) -> Option<String> {
    if chain.eq_ignore_ascii_case("solana") {
        // Prefer Helius; do not spend OpenSea quota for Solana legit slug.
        return helius::fetch_collection_identity(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            address,
        )
        .await
        .map(|s| s.to_ascii_lowercase());
    }
    if let Some(slug) =
        alchemy::fetch_collection_slug(client, &limits.endpoints, keys.alchemy(), chain, address)
            .await
    {
        return Some(slug.to_ascii_lowercase());
    }
    // Last resort only when Alchemy could not supply a slug.
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

    if is_evm {
        // These seed-side probes are independent. Holder snapshots replace
        // thousands of relation-local `isHolderOfContract` requests whenever
        // the bounded snapshot is complete.
        let controller_probe = async {
            if let Some(bundle) = evidence.get(&seed_id) {
                return (bundle.controllers.clone(), true);
            }
            let outcome = controllers::fetch_evm_controllers(
                client,
                &limits.endpoints,
                keys.alchemy(),
                &chain,
                &address,
            )
            .await;
            let probed = !matches!(outcome.status, EvidenceStatus::NotRequested);
            (outcome.value, probed)
        };
        let slug_probe = resolve_collection_slug(client, limits, keys, &chain, &address);
        let transfer_probe = alchemy::fetch_transfers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
            limits.max_transfer_pages.clamp(1, 3),
        );
        let holder_probe = alchemy::fetch_holders(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
            limits.max_holder_pages,
        );
        let ((controllers, controllers_probed), slug, transfers, holders) =
            tokio::join!(controller_probe, slug_probe, transfer_probe, holder_probe);

        cache.controllers = controllers;
        cache.controllers_probed = controllers_probed;
        cache.collection_slug = slug;
        cache.current_owners_complete = matches!(
            holders.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        );
        for holder in holders.value {
            cache
                .current_owners
                .insert(normalize_addr(&chain, &holder.owner));
        }
        for transfer in transfers.value {
            if !transfer.from.is_empty() {
                cache
                    .transfer_counterparties
                    .insert(normalize_addr(&chain, &transfer.from));
            }
            if !transfer.to.is_empty() {
                cache
                    .transfer_counterparties
                    .insert(normalize_addr(&chain, &transfer.to));
            }
        }
    } else {
        let slug_probe = resolve_collection_slug(client, limits, keys, &chain, &address);
        let asset_probe = helius::fetch_collection_assets(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            &address,
            limits.max_solana_assets.clamp(1, 50),
        );
        let (slug, snapshot) = tokio::join!(slug_probe, asset_probe);

        cache.collection_slug = slug;
        // Controllers: reuse candidate enrich if seed was also a candidate.
        if let Some(bundle) = evidence.get(&seed_id) {
            cache.controllers = bundle.controllers.clone();
            cache.controllers_probed = true;
        } else {
            cache.controllers = snapshot.value.authority.clone();
            cache.controllers_probed = !matches!(snapshot.status, EvidenceStatus::NotRequested);
        }
        for asset in &snapshot.value.assets {
            if let Some(owner) = &asset.owner {
                cache.current_owners.insert(normalize_addr(&chain, owner));
            }
        }
    }

    cache
}

fn relation_needs_holder_request(seed: &SeedCache, chain: &str, candidate_address: &str) -> bool {
    !seed.current_owners_complete
        && !seed
            .current_owners
            .contains(&normalize_addr(chain, candidate_address))
}

fn apply_cached_holder_signal(
    signals: &mut LegitSignals,
    seed: &SeedCache,
    chain: &str,
    candidate_address: &str,
) {
    if seed
        .current_owners
        .contains(&normalize_addr(chain, candidate_address))
    {
        signals.seed_nft_interaction = true;
        signals.evidence_keys.push("holds_seed_nft".into());
    }
}

async fn build_candidate_probe(
    client: &HttpClient,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    candidate_id: ContractId,
    resolve_slug: bool,
) -> CandidateProbe {
    let contract = &store.contracts[candidate_id as usize];
    let chain = store.chain_name(contract.chain_id).to_owned();
    let address = contract.address.clone();

    if store.is_evm_chain(&chain) {
        let controller_probe = controllers::fetch_evm_controllers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
        );
        let slug_probe = async {
            if resolve_slug {
                resolve_collection_slug(client, limits, keys, &chain, &address).await
            } else {
                None
            }
        };
        let (controllers, collection_slug) = tokio::join!(controller_probe, slug_probe);
        CandidateProbe {
            chain,
            address,
            controllers: controllers.value,
            collection_slug,
        }
    } else {
        let asset_probe = helius::fetch_collection_assets(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            &address,
            limits.max_solana_assets.clamp(1, 50),
        );
        let slug_probe = async {
            if resolve_slug {
                resolve_collection_slug(client, limits, keys, &chain, &address).await
            } else {
                None
            }
        };
        let (snapshot, collection_slug) = tokio::join!(asset_probe, slug_probe);
        CandidateProbe {
            chain,
            address,
            controllers: snapshot.value.authority,
            collection_slug,
        }
    }
}

fn all_relations_legit(bundle: &EvidenceBundle, expected_relations: usize) -> bool {
    expected_relations > 0
        && bundle.relation_legit.len() == expected_relations
        && bundle
            .relation_legit
            .values()
            .all(LegitSignals::is_legit_duplicate)
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
    cand_slug: Option<&str>,
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

    // Candidate slugs are fetched once per candidate by the orchestrator.
    if let (Some(seed_slug), Some(cand_slug)) = (&seed.collection_slug, cand_slug)
        && !seed_slug.is_empty()
        && seed_slug == cand_slug
    {
        signals.official_collection_relation = true;
        signals
            .evidence_keys
            .push(format!("collection_relation:{seed_slug}"));
    }

    let cand_norm = normalize_addr(chain, candidate_address);

    // Current holds seed NFT. Prefer one seed-wide owner snapshot; fall back to
    // relation-local lookup only when that snapshot was truncated or failed.
    apply_cached_holder_signal(&mut signals, seed, chain, candidate_address);
    if is_evm
        && normalize_evm_address(candidate_address).is_some()
        && relation_needs_holder_request(seed, chain, candidate_address)
    {
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

/// Lightweight relation gate run before full candidate enrichment.
///
/// A candidate is excluded from full enrichment only when every seed relation
/// has a positive official/interaction signal.
pub(super) async fn prefilter_candidates(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<LegitPreflight, Analysis2Error> {
    let seed_ids: Vec<ContractId> = {
        let mut set = BTreeSet::new();
        for rel in registry.relations() {
            set.insert(rel.seed_contract);
        }
        set.into_iter().collect()
    };
    if seed_ids.is_empty() {
        let mut evidence = AHashMap::with_capacity(registry.candidate_contracts().len());
        for &candidate_id in registry.candidate_contracts() {
            let contract = &store.contracts[candidate_id as usize];
            let chain = store.chain_name(contract.chain_id).to_owned();
            evidence.insert(
                candidate_id,
                EvidenceBundle::empty(candidate_id, chain, contract.address.clone()),
            );
        }
        return Ok(LegitPreflight {
            evidence,
            candidates_to_enrich: registry.candidate_contracts().to_vec(),
        });
    }

    let concurrency = limits.concurrency.max(1);
    let empty_evidence = AHashMap::new();

    progress.begin_phase("seed_caches", Some(seed_ids.len() as u64));
    let empty_evidence_ref = &empty_evidence;
    let mut seed_results = stream::iter(seed_ids.iter().copied().map(|seed_id| async move {
        (
            seed_id,
            build_seed_cache(client, store, empty_evidence_ref, keys, limits, seed_id).await,
        )
    }))
    .buffer_unordered(concurrency);
    let mut seed_caches = AHashMap::with_capacity(seed_ids.len());
    while let Some((seed_id, cache)) = seed_results.next().await {
        progress.check_cancelled()?;
        seed_caches.insert(seed_id, cache);
        progress.add_completed(1);
    }

    // Candidate slugs are useful only for relations whose seed has a slug.
    let slug_candidates: AHashSet<ContractId> = registry
        .relations()
        .iter()
        .filter(|rel| {
            seed_caches
                .get(&rel.seed_contract)
                .and_then(|seed| seed.collection_slug.as_ref())
                .is_some()
        })
        .map(|rel| rel.candidate_contract)
        .collect();

    progress.begin_phase(
        "candidate_identity",
        Some(registry.candidate_contracts().len() as u64),
    );
    let slug_candidates_ref = &slug_candidates;
    let mut candidate_results = stream::iter(registry.candidate_contracts().iter().copied().map(
        |candidate_id| async move {
            (
                candidate_id,
                build_candidate_probe(
                    client,
                    store,
                    keys,
                    limits,
                    candidate_id,
                    slug_candidates_ref.contains(&candidate_id),
                )
                .await,
            )
        },
    ))
    .buffer_unordered(concurrency);
    let mut candidate_probes = AHashMap::with_capacity(registry.candidate_contracts().len());
    while let Some((candidate_id, probe)) = candidate_results.next().await {
        progress.check_cancelled()?;
        candidate_probes.insert(candidate_id, probe);
        progress.add_completed(1);
    }

    progress.begin_phase("relations", Some(registry.relations().len() as u64));
    let mut relation_results = stream::iter(registry.relations().iter().filter_map(|rel| {
        let candidate = candidate_probes.get(&rel.candidate_contract)?;
        let seed_cache = seed_caches.get(&rel.seed_contract)?;
        let seed_row = &store.contracts[rel.seed_contract as usize];
        let seed_chain = store.chain_name(seed_row.chain_id);
        let seed_address = seed_row.address.as_str();
        let is_evm = store.is_evm_chain(seed_chain);
        let candidate_id = rel.candidate_contract;
        Some(async move {
            let signals = probe_relation(
                client,
                keys,
                limits,
                seed_chain,
                is_evm,
                seed_address,
                &candidate.address,
                &candidate.controllers,
                candidate.collection_slug.as_deref(),
                seed_cache,
            )
            .await;
            (candidate_id, seed_key(seed_chain, seed_address), signals)
        })
    }))
    .buffer_unordered(concurrency);

    let mut relation_legit: AHashMap<ContractId, Vec<(String, LegitSignals)>> = AHashMap::new();
    while let Some((candidate_id, key, signals)) = relation_results.next().await {
        progress.check_cancelled()?;
        relation_legit
            .entry(candidate_id)
            .or_default()
            .push((key, signals));
        progress.add_completed(1);
    }
    drop(relation_results);

    let mut expected_relations: AHashMap<ContractId, usize> = AHashMap::new();
    for relation in registry.relations() {
        *expected_relations
            .entry(relation.candidate_contract)
            .or_default() += 1;
    }

    let mut evidence = AHashMap::with_capacity(candidate_probes.len());
    let mut candidates_to_enrich = Vec::new();
    for (candidate_id, probe) in candidate_probes {
        let mut bundle = EvidenceBundle::empty(candidate_id, probe.chain, probe.address);
        bundle.controllers = probe.controllers;
        if let Some(rows) = relation_legit.remove(&candidate_id) {
            for (key, signals) in rows {
                bundle.relation_legit.insert(key, signals);
            }
        }
        finalize_legit_signals(&mut bundle);
        let expected = expected_relations.get(&candidate_id).copied().unwrap_or(0);
        let fully_legit = all_relations_legit(&bundle, expected);
        if !fully_legit {
            candidates_to_enrich.push(candidate_id);
        }
        evidence.insert(candidate_id, bundle);
    }
    candidates_to_enrich.sort_unstable();

    Ok(LegitPreflight {
        evidence,
        candidates_to_enrich,
    })
}

/// Compatibility entry point for callers that already hold enriched bundles.
///
/// New pipeline code should use the pre-enrichment gate in the orchestrator.
pub async fn attach_relation_legit(
    evidence: &mut AHashMap<ContractId, EvidenceBundle>,
    registry: &CandidateRegistry,
    store: &ResidentStore,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
) {
    let Ok(mut preflight) =
        prefilter_candidates(registry, store, client, keys, limits, &NoopProgress).await
    else {
        return;
    };
    for (&candidate_id, bundle) in evidence.iter_mut() {
        let Some(mut relation_bundle) = preflight.evidence.remove(&candidate_id) else {
            continue;
        };
        bundle.relation_legit = std::mem::take(&mut relation_bundle.relation_legit);
        bundle.legit = std::mem::take(&mut relation_bundle.legit);
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
    if let (Some(a), Some(b)) = (seed_slug, cand_slug)
        && !a.is_empty()
        && a.eq_ignore_ascii_case(b)
    {
        signals.official_collection_relation = true;
        signals
            .evidence_keys
            .push(format!("collection_relation:{}", a.to_ascii_lowercase()));
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

    #[test]
    fn full_enrich_is_skipped_only_when_every_relation_is_legit() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        bundle.relation_legit.insert(
            "ethereum:0xseed-a".into(),
            LegitSignals {
                official_controller_continuity: true,
                ..LegitSignals::default()
            },
        );
        assert!(all_relations_legit(&bundle, 1));

        bundle
            .relation_legit
            .insert("ethereum:0xseed-b".into(), LegitSignals::default());
        assert!(
            !all_relations_legit(&bundle, 2),
            "one unresolved/suspicious seed relation must retain the candidate"
        );
    }

    #[test]
    fn missing_relation_result_never_excludes_candidate() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        bundle.relation_legit.insert(
            "ethereum:0xseed-a".into(),
            LegitSignals {
                official_collection_relation: true,
                ..LegitSignals::default()
            },
        );
        assert!(!all_relations_legit(&bundle, 2));
    }
}
