//! Metadata query-to-index engine (descending anchors + BM25).
//!
//! Anchors are selected at load (Task 4). Finalize prepares BM25 documents and a
//! term→document inverted index for lossless rare-prefix candidate probes.
//! Query aligns one document pair per seed↔candidate (largest shared / max each
//! side), then exact canonical match or BM25 cosine (default threshold 0.6).
//! No template / MinHash / LSH / quotas.

mod align;
mod bm25;

pub use align::{select_documents, AnchorRef};
pub use bm25::{cosine_similarity, similarity_at_least, PreparedDocument, ThresholdDecision};

use ahash::{AHashMap, AHashSet};

use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
use crate::entity::{ChainId, ContractId, CsrIndex, ResidentStore};
use crate::error::Analysis2Error;
use crate::progress::{NoopProgress, ProgressObserver};

use self::align::select_documents as align_pair;
use self::bm25::lossless_prefix_len;

/// Default BM25 cosine threshold.
pub const DEFAULT_METADATA_THRESHOLD: f64 = 0.6;

/// Prepared BM25 documents + inverted term postings + per-contract anchor refs.
#[derive(Clone, Debug, Default)]
pub struct MetadataIndex {
    documents: Vec<PreparedDocument>,
    terms: Vec<(u32, u32)>,
    /// `document_id` → contracts that hold this canonical document as an anchor.
    doc_contracts: Vec<Vec<ContractId>>,
    /// Parallel to `ResidentStore::contracts`.
    pub(crate) contract_anchors: Vec<Vec<AnchorRef>>,
    contract_is_evm: Vec<bool>,
    /// Global document frequency per term id.
    document_frequency: Vec<u32>,
    /// term_id → sorted unique document ids (full inverted index).
    term_postings: CsrIndex,
}

impl MetadataIndex {
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    fn document_terms(&self, document_id: u32) -> &[(u32, u32)] {
        self.documents[document_id as usize].terms(&self.terms)
    }

    #[cfg(test)]
    fn cosine_between(&self, left_doc: u32, right_doc: u32) -> f64 {
        cosine_similarity(
            &self.documents[left_doc as usize],
            self.document_terms(left_doc),
            &self.documents[right_doc as usize],
            self.document_terms(right_doc),
        )
    }
}

/// Build BM25 prepared documents and lossless-capable term postings from anchors.
pub fn finalize_metadata_index(store: &mut ResidentStore) -> Result<(), Analysis2Error> {
    finalize_metadata_index_with_progress(store, &NoopProgress)
}

/// Progress-aware metadata finalize used by the full load pipeline.
pub fn finalize_metadata_index_with_progress(
    store: &mut ResidentStore,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    const PROGRESS_BATCH: usize = 1 << 10;

    let n_contracts = store.contracts.len();
    let anchor_count: usize = store
        .contracts
        .iter()
        .map(|contract| contract.metadata_by_token.len())
        .sum();
    // Keys borrow the already-resident anchor strings. The previous owned maps
    // duplicated every unique canonical document and token id during finalize.
    let mut canonical_to_doc: AHashMap<&str, u32> = AHashMap::with_capacity(anchor_count);
    let mut documents: Vec<PreparedDocument> = Vec::with_capacity(anchor_count);
    let mut terms: Vec<(u32, u32)> = Vec::new();
    let mut doc_contracts: Vec<Vec<ContractId>> = Vec::with_capacity(anchor_count);
    let mut term_ids: AHashMap<String, u32> = AHashMap::new();
    let mut token_keys: AHashMap<&str, u32> = AHashMap::with_capacity(anchor_count);
    let mut scratch: Vec<u32> = Vec::new();
    let mut term_scratch: Vec<(u32, u32)> = Vec::new();
    let mut document_frequency: Vec<u32> = Vec::new();

    let mut contract_anchors: Vec<Vec<AnchorRef>> = vec![Vec::new(); n_contracts];
    let mut contract_is_evm: Vec<bool> = vec![false; n_contracts];

    progress.begin_phase("metadata_documents", Some(n_contracts as u64));
    let mut pending_progress = 0_u64;
    for (contract_index, contract) in store.contracts.iter().enumerate() {
        if contract_index % PROGRESS_BATCH == 0 {
            progress.check_cancelled()?;
        }
        let chain = store.chain_name(contract.chain_id);
        let is_evm = store.is_evm_chain(chain);
        contract_is_evm[contract.id as usize] = is_evm;
        let mut anchors = Vec::with_capacity(contract.metadata_by_token.len());
        for record in &contract.metadata_by_token {
            let canonical = record.canonical_json.as_str();
            let document_id = if let Some(&id) = canonical_to_doc.get(canonical) {
                id
            } else {
                let id = u32::try_from(documents.len())
                    .map_err(|_| Analysis2Error::invalid("too many metadata documents for u32"))?;
                let mut intern = |term: &str| -> Result<u32, Analysis2Error> {
                    if let Some(&existing) = term_ids.get(term) {
                        return Ok(existing);
                    }
                    let next = u32::try_from(term_ids.len())
                        .map_err(|_| Analysis2Error::invalid("too many BM25 terms for u32"))?;
                    term_ids.insert(term.to_owned(), next);
                    Ok(next)
                };
                term_scratch.clear();
                let mut document = PreparedDocument::try_new_into(
                    &record.canonical_json,
                    &mut intern,
                    &mut scratch,
                    &mut term_scratch,
                )?;
                let term_start = u32::try_from(terms.len())
                    .map_err(|_| Analysis2Error::invalid("too many metadata terms for u32"))?;
                document.set_term_start(term_start);
                terms.extend_from_slice(&term_scratch);
                if document_frequency.len() < term_ids.len() {
                    document_frequency.resize(term_ids.len(), 0);
                }
                for &(term, _) in &term_scratch {
                    document_frequency[term as usize] =
                        document_frequency[term as usize].saturating_add(1);
                }
                documents.push(document);
                doc_contracts.push(Vec::new());
                canonical_to_doc.insert(canonical, id);
                id
            };
            let contracts = &mut doc_contracts[document_id as usize];
            if contracts.last().copied() != Some(contract.id) {
                // Contracts are visited in id order, so this also keeps every
                // document's posting sorted without a later sort/dedup pass.
                contracts.push(contract.id);
            }

            let token = if is_evm {
                // Decimal token ids only need canonical equality here. Borrowing
                // the zero-trimmed slice avoids BigUint parse + String allocation.
                normalized_evm_token_slice(&record.token_id)
            } else {
                record.token_id.as_str()
            };
            let token_key = if let Some(&key) = token_keys.get(token) {
                key
            } else {
                let key = u32::try_from(token_keys.len())
                    .map_err(|_| Analysis2Error::invalid("too many metadata token keys for u32"))?;
                token_keys.insert(token, key);
                key
            };
            anchors.push(AnchorRef {
                token_key,
                document_id,
            });
        }
        contract_anchors[contract.id as usize] = anchors;
        pending_progress += 1;
        if pending_progress as usize == PROGRESS_BATCH {
            progress.add_completed(pending_progress);
            pending_progress = 0;
        }
    }
    if pending_progress > 0 {
        progress.add_completed(pending_progress);
    }

    // Release maps that borrow store fields before assigning the finished index.
    drop(canonical_to_doc);
    drop(token_keys);
    drop(term_ids);

    progress.begin_phase("metadata_postings", Some(documents.len() as u64));
    let term_postings = build_term_postings(
        &documents,
        &terms,
        &document_frequency,
        progress,
        PROGRESS_BATCH,
    )?;

    store.metadata_index = MetadataIndex {
        documents,
        terms,
        doc_contracts,
        contract_anchors,
        contract_is_evm,
        document_frequency,
        term_postings,
    };
    Ok(())
}

fn normalized_evm_token_slice(token: &str) -> &str {
    let trimmed = token.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return token;
    }
    let without_zeroes = trimmed.trim_start_matches('0');
    if without_zeroes.is_empty() {
        &trimmed[trimmed.len() - 1..]
    } else {
        without_zeroes
    }
}

fn build_term_postings(
    documents: &[PreparedDocument],
    terms: &[(u32, u32)],
    document_frequency: &[u32],
    progress: &dyn ProgressObserver,
    progress_batch: usize,
) -> Result<CsrIndex, Analysis2Error> {
    let term_count = u32::try_from(document_frequency.len())
        .map_err(|_| Analysis2Error::invalid("too many BM25 terms for u32"))?;
    let mut offsets = Vec::with_capacity(document_frequency.len() + 1);
    offsets.push(0_u32);
    let mut total = 0_u64;
    for &frequency in document_frequency {
        total = total.saturating_add(u64::from(frequency));
        offsets.push(
            u32::try_from(total)
                .map_err(|_| Analysis2Error::invalid("too many metadata postings for u32"))?,
        );
    }

    let mut values = vec![0_u32; total as usize];
    let mut cursors = offsets[..document_frequency.len()].to_vec();
    let mut pending_progress = 0_u64;
    for (document_id, document) in documents.iter().enumerate() {
        if document_id % progress_batch == 0 {
            progress.check_cancelled()?;
        }
        let document_id = u32::try_from(document_id)
            .map_err(|_| Analysis2Error::invalid("too many metadata documents for u32"))?;
        for &(term, _) in document.terms(terms) {
            let cursor = &mut cursors[term as usize];
            values[*cursor as usize] = document_id;
            *cursor += 1;
        }
        pending_progress += 1;
        if pending_progress as usize == progress_batch {
            progress.add_completed(pending_progress);
            pending_progress = 0;
        }
    }
    if pending_progress > 0 {
        progress.add_completed(pending_progress);
    }

    Ok(CsrIndex {
        keys: (0..term_count).collect(),
        offsets,
        values,
    })
}

/// Query Metadata for `seed` against the finalized index; emit whole-contract edges.
///
/// Hits use `candidate_nft: None` so scope helpers expand all candidate NFTs.
pub fn query_metadata_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    progress.set_stage("metadata");
    progress.check_cancelled()?;

    let seed_usize = seed as usize;
    if seed_usize >= store.contracts.len() {
        return Err(Analysis2Error::invalid(format!(
            "unknown seed contract id {seed}"
        )));
    }
    let index = &store.metadata_index;
    if index.is_empty() {
        return Ok(());
    }
    let seed_anchors = index
        .contract_anchors
        .get(seed_usize)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if seed_anchors.is_empty() {
        return Ok(());
    }

    let seed_chain = store.contracts[seed_usize].chain_id;
    let seed_is_evm = index.contract_is_evm[seed_usize];

    let candidates = collect_candidates(index, seed, seed_anchors, threshold);
    progress.begin_phase("metadata_query", Some(candidates.len() as u64));

    let mut seen: AHashSet<(ContractId, ChainId)> = AHashSet::new();
    for cand in candidates {
        progress.check_cancelled()?;
        if cand == seed {
            progress.add_completed(1);
            continue;
        }
        let cand_usize = cand as usize;
        let Some(cand_anchors) = index.contract_anchors.get(cand_usize) else {
            progress.add_completed(1);
            continue;
        };
        if cand_anchors.is_empty() {
            progress.add_completed(1);
            continue;
        }
        let cand_is_evm = index.contract_is_evm[cand_usize];
        let Some((left_doc, right_doc)) =
            align_pair(seed_is_evm, seed_anchors, cand_is_evm, cand_anchors)
        else {
            progress.add_completed(1);
            continue;
        };

        let score = if left_doc == right_doc {
            Some(1.0)
        } else {
            let left = &index.documents[left_doc as usize];
            let right = &index.documents[right_doc as usize];
            let left_terms = index.document_terms(left_doc);
            let right_terms = index.document_terms(right_doc);
            let decision = similarity_at_least(left, left_terms, right, right_terms, threshold);
            if decision.matched {
                Some(cosine_similarity(left, left_terms, right, right_terms))
            } else {
                None
            }
        };

        if let Some(score) = score {
            let secondary = store.contracts[cand_usize].chain_id;
            if seen.insert((cand, secondary)) {
                graph.push(HitEdge {
                    seed_contract: seed,
                    candidate_contract: cand,
                    candidate_nft: None, // whole-contract Metadata hit
                    dimension: Dimension::Metadata,
                    score,
                    primary_chain: seed_chain,
                    secondary_chain: secondary,
                });
            }
        }
        progress.add_completed(1);
    }
    Ok(())
}

fn collect_candidates(
    index: &MetadataIndex,
    seed: ContractId,
    seed_anchors: &[AnchorRef],
    threshold: f64,
) -> Vec<ContractId> {
    let mut candidates: AHashSet<ContractId> = AHashSet::new();

    // Exact document reuse is always a candidate (byte-identical canonical JSON).
    for anchor in seed_anchors {
        if let Some(contracts) = index.doc_contracts.get(anchor.document_id as usize) {
            for &contract_id in contracts {
                if contract_id != seed {
                    candidates.insert(contract_id);
                }
            }
        }
    }

    if threshold.is_nan() || threshold > 1.0 {
        let mut out: Vec<_> = candidates.into_iter().collect();
        out.sort_unstable();
        return out;
    }

    if threshold <= 0.0 {
        // Every other contract with anchors can match.
        for (contract_id, anchors) in index.contract_anchors.iter().enumerate() {
            if contract_id as ContractId != seed && !anchors.is_empty() {
                candidates.insert(contract_id as ContractId);
            }
        }
        let mut out: Vec<_> = candidates.into_iter().collect();
        out.sort_unstable();
        return out;
    }

    // Lossless rare-prefix probe: any BM25≥threshold pair shares a seed prefix term.
    for anchor in seed_anchors {
        let doc_terms = index.document_terms(anchor.document_id);
        if doc_terms.is_empty() {
            continue;
        }
        let mut ordered: Vec<(u32, u32, u32)> = doc_terms
            .iter()
            .map(|&(term, frequency)| {
                let df = index
                    .document_frequency
                    .get(term as usize)
                    .copied()
                    .unwrap_or(0);
                (df, term, frequency)
            })
            .collect();
        ordered.sort_unstable();
        let frequencies: Vec<u32> = ordered.iter().map(|(_, _, frequency)| *frequency).collect();
        let prefix_len = lossless_prefix_len(&frequencies, threshold);
        for &(_, term, _) in ordered.iter().take(prefix_len) {
            if let Some(docs) = index.term_postings.values_for(term) {
                for &document_id in docs {
                    if let Some(contracts) = index.doc_contracts.get(document_id as usize) {
                        for &contract_id in contracts {
                            if contract_id != seed {
                                candidates.insert(contract_id);
                            }
                        }
                    }
                }
            }
        }
    }

    let mut out: Vec<_> = candidates.into_iter().collect();
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::ScopeKind;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::progress::NoopProgress;
    use crate::reporting::count_scope_nfts;
    use ahash::{AHashMap, AHashSet};

    fn row(chain: &str, contract: &str, token: &str, n: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: String::new(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: n,
            },
        }
    }

    fn anchor(
        store: &mut ResidentStore,
        chain: &str,
        contract: &str,
        token: &str,
        canonical: &str,
        n: u64,
    ) {
        store
            .ingest_metadata_anchor(
                chain,
                contract,
                token,
                canonical.to_owned(),
                canonical.to_owned(),
                SourceOrder {
                    file_ordinal: 0,
                    file_row_number: n,
                },
            )
            .unwrap();
    }

    fn prepared(
        evm: &[&str],
        k: usize,
        rows: impl IntoIterator<Item = IdentityRow>,
        anchors: impl IntoIterator<Item = (&'static str, &'static str, &'static str, &'static str, u64)>,
    ) -> ResidentStore {
        let evm_set = evm.iter().map(|c| (*c).to_owned()).collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(k, &evm_set);
        for r in rows {
            store.ingest_identity_row(r).unwrap();
        }
        for (chain, contract, token, canonical, n) in anchors {
            anchor(&mut store, chain, contract, token, canonical, n);
        }
        finalize_metadata_index(&mut store).unwrap();
        store
    }

    fn cid(store: &ResidentStore, chain: &str, address: &str) -> ContractId {
        let chain_id = store.chain_ids[chain];
        store.contract_index[&(chain_id, address.to_owned())]
    }

    fn nft_map(store: &ResidentStore) -> AHashMap<ContractId, Vec<crate::entity::NftId>> {
        let mut map: AHashMap<ContractId, Vec<_>> = AHashMap::new();
        for nft in &store.nfts {
            map.entry(nft.contract_id).or_default().push(nft.id);
        }
        map
    }

    fn prepare_parts(texts: &[&str]) -> Vec<(PreparedDocument, Vec<(u32, u32)>)> {
        let mut term_ids: AHashMap<String, u32> = AHashMap::new();
        texts
            .iter()
            .map(|text| {
                let parts = PreparedDocument::try_new(text, |term| {
                    if let Some(&id) = term_ids.get(term) {
                        return Ok::<_, std::convert::Infallible>(id);
                    }
                    let id = term_ids.len() as u32;
                    term_ids.insert(term.to_owned(), id);
                    Ok(id)
                })
                .unwrap();
                (parts.document, parts.terms)
            })
            .collect()
    }

    #[test]
    fn descending_anchors_from_load_order_are_largest_first() {
        // Mirrors Task 4: tokens 1,2,10 with k=2 → descending [10, 2].
        let store = prepared(
            &["ethereum"],
            2,
            [
                row("ethereum", "0xa", "1", 1),
                row("ethereum", "0xa", "2", 2),
                row("ethereum", "0xa", "10", 3),
            ],
            [
                ("ethereum", "0xa", "1", r#"{"name":"t1"}"#, 1),
                ("ethereum", "0xa", "2", r#"{"name":"t2"}"#, 2),
                ("ethereum", "0xa", "10", r#"{"name":"t10"}"#, 3),
            ],
        );
        let tokens: Vec<&str> = store.contracts[0]
            .metadata_by_token
            .iter()
            .map(|r| r.token_id.as_str())
            .collect();
        assert_eq!(tokens, ["10", "2"]);
        let refs = &store.metadata_index.contract_anchors[0];
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn borrowed_evm_token_normalization_matches_owned_contract() {
        assert_eq!(normalized_evm_token_slice("010"), "10");
        assert_eq!(normalized_evm_token_slice("000"), "0");
        assert_eq!(
            normalized_evm_token_slice("000123456789012345678901234567890"),
            "123456789012345678901234567890"
        );
        assert_eq!(normalized_evm_token_slice(" token "), " token ");
        assert_eq!(normalized_evm_token_slice("   "), "   ");
    }

    #[test]
    fn bm25_threshold_match_and_mismatch_oracle() {
        let docs = prepare_parts(&[
            "alpha beta gamma delta epsilon zeta eta theta",
            "alpha beta gamma delta epsilon zeta eta theta",
            "alpha beta gamma delta epsilon zeta eta theta iota",
            "completely unrelated vocabulary one two three four",
        ]);
        assert!(
            similarity_at_least(
                &docs[0].0,
                &docs[0].1,
                &docs[1].0,
                &docs[1].1,
                DEFAULT_METADATA_THRESHOLD,
            )
            .matched
        );
        assert!(cosine_similarity(&docs[0].0, &docs[0].1, &docs[1].0, &docs[1].1) > 0.99);

        let near_score = cosine_similarity(&docs[0].0, &docs[0].1, &docs[2].0, &docs[2].1);
        assert!(near_score > 0.0 && near_score < 1.0);
        assert!(
            similarity_at_least(
                &docs[0].0,
                &docs[0].1,
                &docs[2].0,
                &docs[2].1,
                near_score - 1e-9,
            )
            .matched
        );
        assert!(
            !similarity_at_least(
                &docs[0].0,
                &docs[0].1,
                &docs[2].0,
                &docs[2].1,
                near_score + 0.05,
            )
            .matched
        );

        let far = similarity_at_least(
            &docs[0].0,
            &docs[0].1,
            &docs[3].0,
            &docs[3].1,
            DEFAULT_METADATA_THRESHOLD,
        );
        assert!(!far.matched);
        assert!(far.zero_overlap_pruned || far.upper_bound_pruned);
    }

    #[test]
    fn exact_canonical_hit_expands_whole_candidate_contract() {
        let shared = r#"{"name":"CoolCats","desc":"shared metadata body"}"#;
        let store = prepared(
            &["ethereum"],
            8,
            [
                row("ethereum", "0xa", "10", 1),
                row("ethereum", "0xa", "2", 2),
                row("ethereum", "0xb", "10", 3),
                row("ethereum", "0xb", "9", 4),
                row("ethereum", "0xb", "8", 5),
            ],
            [
                ("ethereum", "0xa", "10", shared, 1),
                ("ethereum", "0xa", "2", r#"{"name":"other-a"}"#, 2),
                ("ethereum", "0xb", "10", shared, 3),
                ("ethereum", "0xb", "9", r#"{"name":"other-b1"}"#, 4),
                ("ethereum", "0xb", "8", r#"{"name":"other-b2"}"#, 5),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "ethereum", "0xb");
        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();

        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand)
            .expect("metadata edge");
        assert_eq!(edge.candidate_nft, None);
        assert_eq!(edge.dimension, Dimension::Metadata);
        assert_eq!(edge.score, 1.0);

        let eth = store.chain_ids["ethereum"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            eth,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.metadata, 3, "whole candidate NFT expansion");
    }

    #[test]
    fn alignment_uses_largest_shared_not_smallest() {
        // Shared tokens 1 and 10; largest shared is 10. Docs at 10 match; docs at 1 do not.
        let store = prepared(
            &["ethereum"],
            8,
            [
                row("ethereum", "0xa", "1", 1),
                row("ethereum", "0xa", "10", 2),
                row("ethereum", "0xb", "1", 3),
                row("ethereum", "0xb", "10", 4),
            ],
            [
                ("ethereum", "0xa", "1", r#"{"name":"placeholder a"}"#, 1),
                (
                    "ethereum",
                    "0xa",
                    "10",
                    r#"{"name":"real collection shared body"}"#,
                    2,
                ),
                (
                    "ethereum",
                    "0xb",
                    "1",
                    r#"{"name":"placeholder b totally different"}"#,
                    3,
                ),
                (
                    "ethereum",
                    "0xb",
                    "10",
                    r#"{"name":"real collection shared body"}"#,
                    4,
                ),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "ethereum", "0xb");
        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        assert!(
            graph
                .edges()
                .iter()
                .any(|e| e.candidate_contract == cand && e.score == 1.0),
            "largest shared token 10 should exact-match"
        );
    }

    #[test]
    fn evm_leading_zero_token_ids_share_alignment() {
        // Without bigint normalize, "10" vs "010" would not share; max-each-side
        // would compare unrelated docs at 20 vs 30 and miss the shared body.
        let shared = r#"{"name":"aligned shared metadata body"}"#;
        let store = prepared(
            &["ethereum"],
            8,
            [
                row("ethereum", "0xa", "20", 1),
                row("ethereum", "0xa", "10", 2),
                row("ethereum", "0xb", "30", 3),
                row("ethereum", "0xb", "010", 4),
            ],
            [
                ("ethereum", "0xa", "20", r#"{"name":"max-a unrelated"}"#, 1),
                ("ethereum", "0xa", "10", shared, 2),
                ("ethereum", "0xb", "30", r#"{"name":"max-b different"}"#, 3),
                ("ethereum", "0xb", "010", shared, 4),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "ethereum", "0xb");
        let seed_anchors = &store.metadata_index.contract_anchors[seed as usize];
        let cand_anchors = &store.metadata_index.contract_anchors[cand as usize];
        assert_eq!(seed_anchors.len(), 2);
        assert_eq!(cand_anchors.len(), 2);
        // Descending: [20, 10] and [30, 010]; shared key is the second entry.
        assert_eq!(seed_anchors[1].token_key, cand_anchors[1].token_key);
        assert_ne!(seed_anchors[0].token_key, cand_anchors[0].token_key);

        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand && e.candidate_nft.is_none())
            .expect("shared-token exact hit");
        assert_eq!(edge.dimension, Dimension::Metadata);
        assert_eq!(edge.score, 1.0);
    }

    #[test]
    fn bm25_near_match_emits_whole_contract_edge() {
        // Non-identical high-overlap JSON; query at a threshold below the true score.
        let store = prepared(
            &["ethereum", "base"],
            8,
            [
                row("ethereum", "0xa", "1", 1),
                row("base", "0xb", "1", 2),
                row("base", "0xb", "2", 3),
            ],
            [
                (
                    "ethereum",
                    "0xa",
                    "1",
                    r#"{"description":"alpha beta gamma delta epsilon zeta eta theta","name":"CoolCats"}"#,
                    1,
                ),
                (
                    "base",
                    "0xb",
                    "1",
                    r#"{"description":"alpha beta gamma delta epsilon zeta eta theta iota","name":"CoolCats"}"#,
                    2,
                ),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "base", "0xb");
        assert_eq!(store.metadata_index.document_count(), 2);

        let left_doc = store.metadata_index.contract_anchors[seed as usize][0].document_id;
        let right_doc = store.metadata_index.contract_anchors[cand as usize][0].document_id;
        assert_ne!(left_doc, right_doc);
        let score = store.metadata_index.cosine_between(left_doc, right_doc);
        assert!(
            score > 0.0 && score < 1.0,
            "expected non-exact BM25 score, got {score}"
        );
        let threshold = score * 0.9;

        let mut graph = HitGraph::new();
        query_metadata_for_seed(&store, seed, threshold, &mut graph, &NoopProgress).unwrap();
        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand && e.candidate_nft.is_none())
            .expect("BM25 whole-contract edge");
        assert_eq!(edge.dimension, Dimension::Metadata);
        assert!((edge.score - score).abs() < 1e-9);

        let eth = store.chain_ids["ethereum"];
        let base = store.chain_ids["base"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::ChainMatrix,
            eth,
            Some(base),
            &nft_map(&store),
        );
        assert_eq!(counts.metadata, 2);
    }

    #[test]
    fn self_hit_excluded() {
        let store = prepared(
            &["ethereum"],
            8,
            [row("ethereum", "0xa", "1", 1)],
            [("ethereum", "0xa", "1", r#"{"name":"solo"}"#, 1)],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        assert!(graph.is_empty());
    }
}
