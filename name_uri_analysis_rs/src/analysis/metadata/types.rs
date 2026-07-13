use std::path::Path;
use std::sync::Arc;

use super::*;

pub(super) const METADATA_THRESHOLD: f64 = 0.6;
pub(super) const METADATA_MATCH_MODE: &str = "template_recall_hybrid_verify";
#[cfg(test)]
pub(super) const METADATA_PAIR_LEFT_CHUNK_SIZE: usize = 256;
pub(super) const METADATA_CONTENT_PARALLEL_MIN_RECORDS: usize = 64;
pub(super) const METADATA_CONTENT_SCORE_BATCH_PAIRS: usize = 16 * 1024;
pub(super) const METADATA_REUSE_CACHE_BUDGET_DIVISOR: usize = 16;
// Debug builds retain substantially larger Rayon/JSON/scoring frames than
// optimized builds. The explicit worker stack prevents data-dependent aborts;
// Linux commits these stack pages on demand rather than eagerly.
pub(super) const METADATA_ANALYSIS_WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;
pub(super) type MetadataDocKey = String;
pub(super) type MetadataContractIndex = u32;
pub(super) type MetadataDocIndex = u32;
#[cfg(test)]
pub(super) type MetadataDocPair = (MetadataDocIndex, MetadataDocIndex);

#[derive(Debug)]
pub(crate) enum MetadataTemplateDocument {
    Owned(MetadataBm25Document),
    Shared(Arc<MetadataBm25Document>),
}

impl MetadataTemplateDocument {
    pub(super) fn owned_payload_bytes(&self) -> usize {
        match self {
            Self::Owned(document) => document.memory_bytes(),
            Self::Shared(_) => 0,
        }
    }

    #[cfg(test)]
    pub(super) fn is_owned(&self) -> bool {
        matches!(self, Self::Owned(_))
    }

    #[cfg(test)]
    pub(super) fn is_shared(&self) -> bool {
        matches!(self, Self::Shared(_))
    }

    #[cfg(test)]
    pub(super) fn shares_allocation_with(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Shared(left), Self::Shared(right)) if Arc::ptr_eq(left, right))
    }
}

impl std::ops::Deref for MetadataTemplateDocument {
    type Target = MetadataBm25Document;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(document) => document,
            Self::Shared(document) => document,
        }
    }
}

impl From<MetadataBm25Document> for MetadataTemplateDocument {
    fn from(document: MetadataBm25Document) -> Self {
        Self::Owned(document)
    }
}

impl From<Arc<MetadataBm25Document>> for MetadataTemplateDocument {
    fn from(document: Arc<MetadataBm25Document>) -> Self {
        Self::Shared(document)
    }
}

#[derive(Clone, Debug)]
pub(super) struct MetadataContract {
    pub(super) chain_index: usize,
    pub(super) nft_count: i64,
    pub(super) content_doc: Option<Arc<MetadataBm25Document>>,
    pub(super) template_doc_index: MetadataDocIndex,
    pub(super) uses_declared_metadata_source: bool,
}

#[derive(Debug)]
pub(super) struct SourceMetadataDocEntry {
    pub(super) doc: MetadataTemplateDocument,
    pub(super) contracts: Vec<MetadataContractIndex>,
}

#[derive(Debug)]
pub(super) struct MetadataData {
    pub(super) contracts: Vec<MetadataContract>,
    pub(super) contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    pub(super) compact_contract_indexes_by_source: Vec<Option<MetadataContractIndex>>,
    pub(super) metadata_index: InternedMetadataIndex,
    pub(super) reused_documents: ReusedMetadataDocuments,
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct MetadataTemplateMatches {
    pub(super) compatible_docs: CompactMetadataPostings,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct MetadataAlgorithmMetrics {
    pub(crate) recall_mode: MetadataRecallMode,
    pub(crate) eligible_rows: u64,
    pub(crate) selected_sources: u64,
    pub(crate) reused_raw_json_cache_entries: u64,
    pub(crate) singleton_tokens_removed: u64,
    pub(crate) retained_shared_tokens: u64,
    pub(crate) template_documents: u64,
    pub(crate) content_documents: u64,
    pub(crate) template_candidate_pairs: u64,
    pub(crate) template_scored_pairs: u64,
    pub(crate) template_matched_pairs: u64,
    pub(crate) content_atoms: u64,
    pub(crate) content_raw_candidate_pairs: u64,
    pub(crate) content_dimension_rejected_pairs: u64,
    pub(crate) content_candidate_pairs: u64,
    pub(crate) content_already_connected_pairs: u64,
    pub(crate) content_scored_pairs: u64,
    pub(crate) template_rejected_pairs: u64,
    pub(crate) template_cache_hits: u64,
    pub(crate) template_cache_misses: u64,
    pub(crate) template_batch_unique_pairs: u64,
    pub(crate) template_batch_reused_pairs: u64,
    pub(crate) conservative_groups: u64,
    pub(crate) exact_fallback_groups: u64,
    pub(crate) recall_sampled_left_atoms: u64,
    pub(crate) recall_exact_candidate_pairs: u64,
    pub(crate) recall_conservative_candidate_pairs: u64,
    pub(crate) recall_exact_matched_pairs: u64,
    pub(crate) recall_missed_matched_pairs: u64,
    pub(crate) recall_exact_duplicate_contract_members: u64,
    pub(crate) recall_missed_duplicate_contract_members: u64,
    pub(crate) recall_exact_component_members: u64,
    pub(crate) recall_shifted_component_members: u64,
    pub(crate) mmap_bytes: u64,
    pub(crate) dsu_bytes: u64,
}

pub(crate) struct MetadataAnalysisResult {
    pub(crate) rows: Vec<SummaryRow>,
    pub(crate) metrics: MetadataAlgorithmMetrics,
}

pub(crate) struct MetadataAnalysisSpec<'a> {
    pub(crate) threads: usize,
    pub(crate) recall_mode: MetadataRecallMode,
    pub(crate) memory_limit: &'a str,
    pub(crate) artifact_directory: Option<&'a Path>,
}

#[cfg(test)]
impl MetadataTemplateMatches {
    pub(super) fn from_pairs(doc_count: usize, mut pairs: Vec<MetadataDocPair>) -> Self {
        for (left, right) in &mut pairs {
            if *left > *right {
                std::mem::swap(left, right);
            }
        }
        pairs.sort_unstable();
        pairs.dedup();
        Self {
            compatible_docs: CompactMetadataPostings::from_symmetric_pairs(doc_count, &pairs),
        }
    }

    pub(super) fn matches(&self, left: usize, right: usize) -> bool {
        left == right
            || self
                .compatible_docs(metadata_doc_index_from_usize(left))
                .binary_search(&metadata_doc_index_from_usize(right))
                .is_ok()
    }

    pub(super) fn compatible_docs(&self, doc: MetadataDocIndex) -> &[MetadataDocIndex] {
        let doc = metadata_doc_index_to_usize(doc);
        if doc >= self.compatible_docs.len() {
            &[]
        } else {
            self.compatible_docs.posting(doc)
        }
    }

    pub(super) fn owned_memory_bytes(&self) -> usize {
        self.compatible_docs.owned_memory_bytes()
    }

    pub(super) fn remap_if_over_budget(
        &mut self,
        directory: &Path,
        maximum_owned_bytes: usize,
    ) -> std::io::Result<bool> {
        if self.owned_memory_bytes() <= maximum_owned_bytes {
            return Ok(false);
        }
        self.remap(directory)?;
        Ok(true)
    }

    pub(super) fn remap(&mut self, directory: &Path) -> std::io::Result<()> {
        let compatible_docs = std::mem::replace(
            &mut self.compatible_docs,
            CompactMetadataPostings::from_nested(Vec::new()),
        );
        self.compatible_docs = compatible_docs.persist_and_remap_named(
            directory,
            "template_match_offsets.bin",
            "template_matches.bin",
        )?;
        Ok(())
    }
}

#[cfg(test)]
impl Default for MetadataTemplateMatches {
    fn default() -> Self {
        Self {
            compatible_docs: CompactMetadataPostings::from_nested(Vec::new()),
        }
    }
}

impl MetadataData {
    pub(super) fn compact_contract_index_for_source(
        &self,
        source_contract_index: usize,
    ) -> Option<MetadataContractIndex> {
        self.compact_contract_indexes_by_source
            .get(source_contract_index)
            .copied()
            .flatten()
    }
}

pub(super) fn metadata_contract_index_from_usize(index: usize) -> MetadataContractIndex {
    MetadataContractIndex::try_from(index)
        .expect("metadata contract count must fit in compact u32 membership indexes")
}

pub(super) fn metadata_contract_index_to_usize(index: MetadataContractIndex) -> usize {
    index as usize
}

pub(super) fn metadata_doc_index_from_usize(index: usize) -> MetadataDocIndex {
    MetadataDocIndex::try_from(index)
        .expect("metadata document count must fit in compact u32 postings")
}

pub(super) fn metadata_doc_index_to_usize(index: MetadataDocIndex) -> usize {
    index as usize
}
