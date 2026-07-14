use super::*;

pub(super) fn metadata_runtime_reserve_bytes(
    contract_count: usize,
    retained_contract_token_rows: usize,
    chain_count: usize,
) -> usize {
    let contract_tokens =
        metadata_contract_token_resident_bytes(contract_count, retained_contract_token_rows);
    let source_mapping =
        contract_count.saturating_mul(std::mem::size_of::<Option<MetadataContractIndex>>());
    let dense_dsu = contract_count
        .saturating_mul(std::mem::size_of::<usize>().saturating_add(std::mem::size_of::<u8>()));
    let sparse_state_count = metadata_sparse_membership_factor(chain_count);
    let sparse_dsu = contract_count
        .saturating_mul(SPARSE_UNION_NODE_BYTES)
        .saturating_mul(sparse_state_count);
    let reserve = contract_tokens
        .saturating_add(source_mapping)
        .saturating_add(dense_dsu)
        .saturating_add(sparse_dsu);
    reserve.saturating_add(reserve.saturating_div(8))
}

pub(super) fn metadata_worker_stack_reserve_bytes(threads: usize) -> usize {
    threads
        .max(1)
        .saturating_mul(METADATA_ANALYSIS_WORKER_STACK_BYTES)
}

pub(super) fn metadata_structure_memory_budget_bytes(
    total_analysis_memory_bytes: usize,
    threads: usize,
) -> Result<usize, AnalysisError> {
    let worker_stack_reserve_bytes = metadata_worker_stack_reserve_bytes(threads);
    if worker_stack_reserve_bytes >= total_analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata worker stacks need about {}, exceeding analysis budget {}",
            format_byte_size(worker_stack_reserve_bytes),
            format_byte_size(total_analysis_memory_bytes)
        )));
    }
    Ok(total_analysis_memory_bytes - worker_stack_reserve_bytes)
}

pub(super) fn metadata_contract_token_resident_bytes(
    contract_count: usize,
    retained_contract_token_rows: usize,
) -> usize {
    contract_count
        .saturating_add(1)
        .saturating_mul(std::mem::size_of::<u64>())
        .saturating_add(retained_contract_token_rows.saturating_mul(std::mem::size_of::<u32>()))
}

pub(super) fn metadata_contract_token_reserve_bytes(
    contract_count: usize,
    retained_contract_token_rows: usize,
) -> usize {
    let bytes =
        metadata_contract_token_resident_bytes(contract_count, retained_contract_token_rows)
            .saturating_add(contract_count.saturating_mul(std::mem::size_of::<u32>()));
    bytes.saturating_add(bytes.saturating_div(8))
}

pub(super) fn metadata_pre_token_resident_budget_bytes(
    analysis_memory_bytes: usize,
    contract_token_reserve_bytes: usize,
) -> Result<usize, AnalysisError> {
    if contract_token_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata contract-token load needs about {}, exceeding analysis budget {}",
            format_byte_size(contract_token_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    Ok(analysis_memory_bytes - contract_token_reserve_bytes)
}

pub(super) fn metadata_sparse_membership_factor(chain_count: usize) -> usize {
    if chain_count > 1 {
        // Every contract can appear once in the global cross-chain DSU and
        // once in each of the (k - 1) pair matrices involving its own chain.
        chain_count
    } else {
        0
    }
}

pub(super) fn metadata_build_overlap_reserve_bytes(contract_count: usize) -> usize {
    let mapping =
        contract_count.saturating_mul(std::mem::size_of::<Option<MetadataContractIndex>>());
    mapping.saturating_add(mapping.saturating_div(8))
}

pub(super) fn metadata_builder_peak_budget_bytes(
    analysis_memory_bytes: usize,
    build_overlap_reserve_bytes: usize,
    reused_cache_bytes: usize,
    load_transient_reserve_bytes: usize,
) -> Result<usize, AnalysisError> {
    let concurrent_reserve_bytes = build_overlap_reserve_bytes
        .saturating_add(reused_cache_bytes)
        .saturating_add(load_transient_reserve_bytes);
    if concurrent_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata cache/mapping/load transient state needs about {}, exceeding analysis budget {}",
            format_byte_size(concurrent_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    Ok(analysis_memory_bytes - concurrent_reserve_bytes)
}

pub(super) fn remap_metadata_index_for_resident_budget(
    data: &mut MetadataData,
    resident_bytes: usize,
    maximum_resident_bytes: usize,
    artifact_directory: Option<&Path>,
) -> Result<usize, AnalysisError> {
    if resident_bytes <= maximum_resident_bytes {
        return Ok(resident_bytes);
    }
    let Some(directory) = artifact_directory else {
        return Ok(resident_bytes);
    };
    let logical_index_bytes = data.metadata_index.logical_memory_bytes();
    let non_index_bytes = resident_bytes.saturating_sub(logical_index_bytes);
    let maximum_owned_index_bytes = maximum_resident_bytes.saturating_sub(non_index_bytes);
    data.metadata_index
        .remap_if_over_budget(directory, maximum_owned_index_bytes)?;
    Ok(non_index_bytes.saturating_add(data.metadata_index.logical_memory_bytes()))
}

pub(super) fn metadata_resident_memory_bytes(
    data: &MetadataData,
    contract_tokens: Option<&CompactContractTokens>,
    chain_count: usize,
) -> usize {
    let content_doc_bytes = data
        .contracts
        .iter()
        .filter_map(|contract| contract.content_doc.as_ref())
        .chain(
            data.reused_documents
                .values()
                .filter_map(|document| document.content.as_ref()),
        )
        .map(|document| {
            metadata_content_arc_memory_bytes(document).div_ceil(Arc::strong_count(document).max(1))
        })
        .fold(0usize, usize::saturating_add);
    let contract_bytes = data
        .contracts
        .capacity()
        .saturating_mul(std::mem::size_of::<MetadataContract>())
        .saturating_add(content_doc_bytes);
    let chain_bytes = data
        .contracts_by_chain
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<MetadataContractIndex>>())
        .saturating_add(
            data.contracts_by_chain
                .iter()
                .map(|contracts| {
                    contracts
                        .capacity()
                        .saturating_mul(std::mem::size_of::<MetadataContractIndex>())
                })
                .fold(0usize, usize::saturating_add),
        );
    let mapping_bytes = data
        .compact_contract_indexes_by_source
        .capacity()
        .saturating_mul(std::mem::size_of::<Option<MetadataContractIndex>>());
    let contract_token_bytes = contract_tokens.map_or(0, |tokens| {
        debug_assert_eq!(tokens.len(), data.contracts.len());
        debug_assert_eq!(tokens.is_empty(), data.contracts.is_empty());
        tokens.memory_bytes()
    });
    let reused_bytes = reused_metadata_documents_non_content_memory_bytes(&data.reused_documents);
    let contract_count = data.contracts.len();
    let dense_dsu_bytes = contract_count
        .saturating_mul(std::mem::size_of::<usize>().saturating_add(std::mem::size_of::<u8>()))
        .saturating_add(UnionFind::connected_cache_capacity_bytes());
    let sparse_state_count = metadata_sparse_membership_factor(chain_count);
    let sparse_dsu_bytes = contract_count
        .saturating_mul(SPARSE_UNION_NODE_BYTES)
        .saturating_mul(sparse_state_count);
    contract_bytes
        .saturating_add(chain_bytes)
        .saturating_add(mapping_bytes)
        .saturating_add(contract_token_bytes)
        .saturating_add(data.metadata_index.logical_memory_bytes())
        .saturating_add(reused_bytes)
        .saturating_add(dense_dsu_bytes)
        .saturating_add(sparse_dsu_bytes)
}
