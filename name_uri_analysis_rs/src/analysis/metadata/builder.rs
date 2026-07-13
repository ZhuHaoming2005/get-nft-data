use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::*;

pub(super) struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    source_contract_indexes: Vec<u32>,
    docs: Vec<SourceMetadataDocEntry>,
    doc_index_by_key: HashMap<MetadataDocKey, usize>,
    pub(super) document_payload_bytes: usize,
    doc_contract_bytes: usize,
    doc_key_bytes: usize,
    pub(super) content_doc_bytes: usize,
    seen_content_docs: HashSet<usize>,
    template_unique_terms: usize,
}

impl MetadataDataBuilder {
    pub(super) fn new(chain_count: usize) -> Self {
        Self {
            contracts: Vec::new(),
            contracts_by_chain: vec![Vec::new(); chain_count],
            source_contract_indexes: Vec::new(),
            docs: Vec::new(),
            doc_index_by_key: HashMap::new(),
            document_payload_bytes: 0,
            doc_contract_bytes: 0,
            doc_key_bytes: 0,
            content_doc_bytes: 0,
            seen_content_docs: HashSet::new(),
            template_unique_terms: 0,
        }
    }

    pub(super) fn merge_indexed_rows(
        &mut self,
        indexed_rows: Vec<(u32, IndexedMetadataRow)>,
        uses_declared_metadata_source: bool,
    ) {
        for (source_contract_index, row) in indexed_rows {
            self.merge_source_indexed_row(
                source_contract_index,
                row,
                uses_declared_metadata_source,
            );
        }
    }

    pub(super) fn memory_bytes(&self) -> usize {
        let contracts = self
            .contracts
            .capacity()
            .saturating_mul(std::mem::size_of::<MetadataContract>());
        let chains = self
            .contracts_by_chain
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<MetadataContractIndex>>())
            .saturating_add(
                self.contracts_by_chain
                    .iter()
                    .map(|contracts| {
                        contracts
                            .capacity()
                            .saturating_mul(std::mem::size_of::<MetadataContractIndex>())
                    })
                    .fold(0usize, usize::saturating_add),
            );
        let sources = self
            .source_contract_indexes
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>());
        let docs = self
            .docs
            .capacity()
            .saturating_mul(std::mem::size_of::<SourceMetadataDocEntry>())
            .saturating_add(self.document_payload_bytes)
            .saturating_add(self.doc_contract_bytes)
            .saturating_add(self.content_doc_bytes);
        let lookup = hash_table_allocation_bytes(
            self.doc_index_by_key.capacity(),
            std::mem::size_of::<(MetadataDocKey, usize)>(),
        )
        .saturating_add(self.doc_key_bytes)
        .saturating_add(hash_table_allocation_bytes(
            self.seen_content_docs.capacity(),
            std::mem::size_of::<usize>(),
        ));
        contracts
            .saturating_add(chains)
            .saturating_add(sources)
            .saturating_add(docs)
            .saturating_add(lookup)
    }

    fn lookup_memory_bytes(&self) -> usize {
        hash_table_allocation_bytes(
            self.doc_index_by_key.capacity(),
            std::mem::size_of::<(MetadataDocKey, usize)>(),
        )
        .saturating_add(self.doc_key_bytes)
        .saturating_add(hash_table_allocation_bytes(
            self.seen_content_docs.capacity(),
            std::mem::size_of::<usize>(),
        ))
    }

    /// Conservative peak for converting the loaded string documents into the
    /// compact BM25 arrays. The lookup maps are dropped before this conversion;
    /// the estimate covers the overlapping lexical dictionary, source docs,
    /// prepared queries/docs and final flat arrays, plus 25% allocator and
    /// HashMap slack. No global template-pair graph is materialized; compact
    /// per-document safe-prefix tokens are included in the flat arrays.
    pub(super) fn estimated_finish_peak_memory_bytes(&self) -> usize {
        let document_count = self.docs.len();
        let unique_terms = self.template_unique_terms;

        let lexical_dictionary = unique_terms.saturating_mul(
            std::mem::size_of::<&str>()
                .saturating_add(std::mem::size_of::<(&str, usize)>())
                .saturating_add(1),
        );
        let source_documents = document_count
            .saturating_mul(std::mem::size_of::<InternedMetadataSourceDoc>())
            .saturating_add(unique_terms.saturating_mul(std::mem::size_of::<(u32, u32)>()));
        let prepared_and_queries = document_count
            .saturating_mul(
                std::mem::size_of::<usize>()
                    .saturating_add(std::mem::size_of::<PreparedInternedMetadataDoc>())
                    .saturating_add(std::mem::size_of::<PreparedInternedMetadataQuery>()),
            )
            .saturating_add(
                unique_terms.saturating_mul(
                    4usize
                        .saturating_mul(std::mem::size_of::<usize>())
                        .saturating_add(2 * std::mem::size_of::<f64>()),
                ),
            );
        let compact_scoring = document_count
            .saturating_add(1)
            .saturating_mul(4 * std::mem::size_of::<u64>())
            .saturating_add(document_count.saturating_mul(std::mem::size_of::<f64>()))
            .saturating_add(
                unique_terms.saturating_mul(
                    3usize
                        .saturating_mul(std::mem::size_of::<u32>())
                        .saturating_add(std::mem::size_of::<f64>()),
                ),
            );
        let corpus = unique_terms
            .saturating_mul(2 * std::mem::size_of::<usize>())
            .saturating_add(std::mem::size_of::<InternedMetadataCorpus>());
        let conversion_working_bytes = lexical_dictionary
            .saturating_add(source_documents)
            .saturating_add(prepared_and_queries)
            .saturating_add(compact_scoring)
            .saturating_add(corpus);
        let conversion_with_slack =
            conversion_working_bytes.saturating_add(conversion_working_bytes.saturating_div(4));
        let post_lookup_builder = self
            .memory_bytes()
            .saturating_sub(self.lookup_memory_bytes());
        self.memory_bytes()
            .max(post_lookup_builder.saturating_add(conversion_with_slack))
    }

    pub(super) fn ensure_within_memory_budget(
        &self,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        let resident_bytes = self.estimated_finish_peak_memory_bytes();
        if resident_bytes > maximum_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "projected metadata index build peak is about {}, exceeding analysis budget {}",
                format_byte_size(resident_bytes),
                format_byte_size(maximum_bytes)
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn merge_indexed_row(&mut self, row: IndexedMetadataRow) {
        let source_contract_index = u32::try_from(self.source_contract_indexes.len())
            .expect("metadata source contract index exceeds u32 indexes");
        self.merge_source_indexed_row(source_contract_index, row, true);
    }

    pub(super) fn merge_source_indexed_row(
        &mut self,
        source_contract_index: u32,
        row: IndexedMetadataRow,
        uses_declared_metadata_source: bool,
    ) {
        if let Some(content_doc) = row.content_doc.as_ref() {
            let pointer = Arc::as_ptr(content_doc) as usize;
            if self.seen_content_docs.insert(pointer) && Arc::strong_count(content_doc) == 1 {
                self.content_doc_bytes = self
                    .content_doc_bytes
                    .saturating_add(metadata_content_arc_memory_bytes(content_doc));
            }
        }
        let doc_index = match self.doc_index_by_key.get(&row.doc_key).copied() {
            Some(index) => index,
            None => {
                let index = self.docs.len();
                self.template_unique_terms = self
                    .template_unique_terms
                    .saturating_add(row.doc.unique_len());
                self.document_payload_bytes = self
                    .document_payload_bytes
                    .saturating_add(row.doc.owned_payload_bytes());
                self.doc_key_bytes = self.doc_key_bytes.saturating_add(row.doc_key.capacity());
                self.doc_index_by_key.insert(row.doc_key, index);
                self.docs.push(SourceMetadataDocEntry {
                    doc: row.doc,
                    contracts: Vec::new(),
                });
                index
            }
        };
        let compact_doc_index = metadata_doc_index_from_usize(doc_index);
        let contract_index = self.contracts.len();
        self.contracts.push(MetadataContract {
            chain_index: row.chain_index,
            nft_count: row.nft_count,
            content_doc: row.content_doc,
            template_doc_index: compact_doc_index,
            uses_declared_metadata_source,
        });
        self.contracts_by_chain[row.chain_index]
            .push(metadata_contract_index_from_usize(contract_index));
        self.source_contract_indexes.push(source_contract_index);
        let compact_contract_index = metadata_contract_index_from_usize(contract_index);
        let previous_contract_capacity = self.docs[doc_index].contracts.capacity();
        self.docs[doc_index].contracts.push(compact_contract_index);
        self.doc_contract_bytes = self.doc_contract_bytes.saturating_add(
            self.docs[doc_index]
                .contracts
                .capacity()
                .saturating_sub(previous_contract_capacity)
                .saturating_mul(std::mem::size_of::<MetadataContractIndex>()),
        );
    }

    #[cfg(test)]
    pub(super) fn finish(self) -> MetadataData {
        self.finish_with_reused_documents(ReusedMetadataDocuments::new())
    }

    pub(super) fn finish_with_reused_documents(
        self,
        reused_documents: ReusedMetadataDocuments,
    ) -> MetadataData {
        let Self {
            contracts,
            contracts_by_chain,
            source_contract_indexes,
            docs,
            doc_index_by_key,
            seen_content_docs,
            ..
        } = self;
        // These two hash tables are load-only state. Releasing them before the
        // BM25 conversion removes a full copy of every document key from the
        // actual peak.
        drop(doc_index_by_key);
        drop(seen_content_docs);
        let mut compact_contract_indexes_by_source = source_contract_indexes
            .iter()
            .copied()
            .max()
            .map_or_else(Vec::new, |max_source_index| {
                vec![None; max_source_index as usize + 1]
            });
        for (compact_contract_index, source_contract_index) in
            source_contract_indexes.into_iter().enumerate()
        {
            compact_contract_indexes_by_source[source_contract_index as usize] =
                Some(metadata_contract_index_from_usize(compact_contract_index));
        }
        let metadata_index = InternedMetadataIndex::from_source_doc_entries(docs);
        MetadataData {
            contracts,
            contracts_by_chain,
            compact_contract_indexes_by_source,
            metadata_index,
            reused_documents,
        }
    }

    pub(super) fn missing_source_indexes(&self, source_count: usize) -> Vec<u32> {
        let mut present = vec![false; source_count];
        for &source_index in &self.source_contract_indexes {
            if let Some(slot) = present.get_mut(source_index as usize) {
                *slot = true;
            }
        }
        present
            .into_iter()
            .enumerate()
            .filter(|(_, present)| !present)
            .map(|(source_index, _)| {
                u32::try_from(source_index)
                    .expect("metadata source contract index exceeds u32 indexes")
            })
            .collect()
    }
}
