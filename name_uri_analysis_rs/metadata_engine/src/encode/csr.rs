//! Bidirectional contract↔token CSR builders.

use thiserror::Error;

use crate::format::{self, ArrayKind, FormatError};
use rayon::prelude::*;
use std::borrow::Cow;
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Error)]
pub enum CsrError {
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error("CSR cardinality exceeds the addressable memory space")]
    CardinalityOverflow,
}

/// One source membership used to build both CSR directions.
#[derive(Debug, Clone)]
pub struct CsrSourceMembership {
    pub source_doc_id: u32,
    pub contract_id: u32,
    pub retained_token_ids: Vec<u32>,
}

/// `contract -> retained token IDs` and `token -> (contract, source)` CSR arrays.
#[derive(Debug, Clone, Default)]
pub struct BidirectionalCsr {
    pub contract_token_offsets: Vec<u64>,
    pub contract_tokens: Vec<u32>,
    pub token_member_offsets: Vec<u64>,
    pub token_member_contracts: Vec<u32>,
    pub token_member_sources: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
pub struct BidirectionalCsrView<'a> {
    pub contract_token_offsets: &'a [u64],
    pub contract_tokens: &'a [u32],
    pub token_member_offsets: &'a [u64],
    pub token_member_contracts: &'a [u32],
    pub token_member_sources: &'a [u32],
}

impl BidirectionalCsr {
    pub fn as_view(&self) -> BidirectionalCsrView<'_> {
        BidirectionalCsrView {
            contract_token_offsets: &self.contract_token_offsets,
            contract_tokens: &self.contract_tokens,
            token_member_offsets: &self.token_member_offsets,
            token_member_contracts: &self.token_member_contracts,
            token_member_sources: &self.token_member_sources,
        }
    }
}

/// Build sorted bidirectional CSR from source memberships.
pub fn build_bidirectional_csr(
    memberships: &[CsrSourceMembership],
) -> Result<BidirectionalCsr, CsrError> {
    build_bidirectional_csr_from_iter(memberships.iter().map(|membership| {
        (
            membership.source_doc_id,
            membership.contract_id,
            membership.retained_token_ids.as_slice(),
        )
    }))
}

/// Build both CSR directions from borrowed membership slices, avoiding a
/// second source-sized object graph and token-vector clone in Encode.
pub fn build_bidirectional_csr_from_iter<'a>(
    memberships: impl IntoIterator<Item = (u32, u32, &'a [u32])>,
) -> Result<BidirectionalCsr, CsrError> {
    build_bidirectional_csr_from_iter_with_lanes(memberships, rayon::current_num_threads())
}

fn build_bidirectional_csr_from_iter_with_lanes<'a>(
    memberships: impl IntoIterator<Item = (u32, u32, &'a [u32])>,
    lanes: usize,
) -> Result<BidirectionalCsr, CsrError> {
    struct Membership<'a> {
        source: u32,
        contract: u32,
        tokens: Cow<'a, [u32]>,
    }

    // Counting buckets trade predictable resident arrays for speed: every
    // occurrence is placed in O(1), then only its contract/token-local slice
    // is sorted. This avoids two global O(M log M) tuple sorts.
    let mut normalized = Vec::<Membership<'a>>::new();
    let mut max_contract = None;
    let mut max_token = None;
    let mut occurrence_count = 0usize;

    for (source_doc_id, contract_id, retained_token_ids) in memberships {
        max_contract =
            Some(max_contract.map_or(contract_id, |current: u32| current.max(contract_id)));
        let tokens = if retained_token_ids.windows(2).all(|pair| pair[0] < pair[1]) {
            Cow::Borrowed(retained_token_ids)
        } else {
            let mut owned = retained_token_ids.to_vec();
            owned.sort_unstable();
            owned.dedup();
            Cow::Owned(owned)
        };
        if let Some(&token) = tokens.last() {
            max_token = Some(max_token.map_or(token, |current: u32| current.max(token)));
        }
        occurrence_count = occurrence_count
            .checked_add(tokens.len())
            .ok_or(CsrError::CardinalityOverflow)?;
        normalized.push(Membership {
            source: source_doc_id,
            contract: contract_id,
            tokens,
        });
    }

    let contract_count = count_from_max(max_contract)?;
    let token_count = count_from_max(max_token)?;
    // Encode invokes this builder from its already configured persistence
    // pool. Reuse that registry instead of creating another full 128-thread
    // pool (and another set of worker stacks) inside it. Standalone callers
    // still receive the requested bounded pool.
    let pool = if rayon::current_thread_index().is_some() {
        None
    } else {
        Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(lanes.max(1))
                .build()
                .map_err(|_| CsrError::CardinalityOverflow)?,
        )
    };
    let contract_counts = (0..contract_count)
        .map(|_| AtomicUsize::new(0))
        .collect::<Vec<_>>();
    let token_counts = (0..token_count)
        .map(|_| AtomicUsize::new(0))
        .collect::<Vec<_>>();
    install_in_pool(pool.as_ref(), || {
        normalized.par_iter().for_each(|membership| {
            contract_counts[membership.contract as usize]
                .fetch_add(membership.tokens.len(), Ordering::Relaxed);
            for &token in membership.tokens.iter() {
                token_counts[token as usize].fetch_add(1, Ordering::Relaxed);
            }
        });
    });
    let contract_counts = contract_counts
        .into_iter()
        .map(AtomicUsize::into_inner)
        .collect::<Vec<_>>();
    let token_counts = token_counts
        .into_iter()
        .map(AtomicUsize::into_inner)
        .collect::<Vec<_>>();
    let contract_bucket_offsets = bucket_offsets(&contract_counts)?;
    let token_bucket_offsets = bucket_offsets(&token_counts)?;
    debug_assert_eq!(
        contract_bucket_offsets.last().copied().unwrap_or(0),
        occurrence_count
    );
    debug_assert_eq!(
        token_bucket_offsets.last().copied().unwrap_or(0),
        occurrence_count
    );

    let contract_buckets = (0..occurrence_count)
        .map(|_| AtomicU32::new(0))
        .collect::<Vec<_>>();
    let token_buckets = (0..occurrence_count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    let contract_cursors = contract_bucket_offsets[..contract_count]
        .iter()
        .copied()
        .map(AtomicUsize::new)
        .collect::<Vec<_>>();
    let token_cursors = token_bucket_offsets[..token_count]
        .iter()
        .copied()
        .map(AtomicUsize::new)
        .collect::<Vec<_>>();
    install_in_pool(pool.as_ref(), || {
        normalized.par_iter().for_each(|membership| {
            let packed = (u64::from(membership.contract) << 32) | u64::from(membership.source);
            for &token in membership.tokens.iter() {
                let contract_position =
                    contract_cursors[membership.contract as usize].fetch_add(1, Ordering::Relaxed);
                contract_buckets[contract_position].store(token, Ordering::Relaxed);
                let token_position = token_cursors[token as usize].fetch_add(1, Ordering::Relaxed);
                token_buckets[token_position].store(packed, Ordering::Relaxed);
            }
        });
    });
    let mut contract_buckets = install_in_pool(pool.as_ref(), || {
        contract_buckets
            .into_par_iter()
            .map(AtomicU32::into_inner)
            .collect::<Vec<_>>()
    });
    let mut token_buckets = install_in_pool(pool.as_ref(), || {
        token_buckets
            .into_par_iter()
            .map(AtomicU64::into_inner)
            .collect::<Vec<_>>()
    });
    drop(normalized);

    install_in_pool(pool.as_ref(), || {
        rayon::join(
            || parallel_sort_buckets(&mut contract_buckets, &contract_bucket_offsets),
            || parallel_sort_buckets(&mut token_buckets, &token_bucket_offsets),
        );
    });

    let (contract_token_offsets, contract_tokens) = install_in_pool(pool.as_ref(), || {
        compact_sorted_buckets(contract_buckets, &contract_bucket_offsets)
    })?;
    let (token_member_offsets, token_members) = install_in_pool(pool.as_ref(), || {
        compact_sorted_buckets(token_buckets, &token_bucket_offsets)
    })?;
    let mut token_member_contracts = Vec::with_capacity(token_members.len());
    let mut token_member_sources = Vec::with_capacity(token_members.len());
    for packed in token_members {
        token_member_contracts.push((packed >> 32) as u32);
        token_member_sources.push(packed as u32);
    }

    Ok(BidirectionalCsr {
        contract_token_offsets,
        contract_tokens,
        token_member_offsets,
        token_member_contracts,
        token_member_sources,
    })
}

fn install_in_pool<R: Send>(
    pool: Option<&rayon::ThreadPool>,
    operation: impl FnOnce() -> R + Send,
) -> R {
    match pool {
        Some(pool) => pool.install(operation),
        None => operation(),
    }
}

fn count_from_max(maximum: Option<u32>) -> Result<usize, CsrError> {
    maximum.map_or(Ok(0), |value| {
        usize::try_from(value)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(CsrError::CardinalityOverflow)
    })
}

fn bucket_offsets(counts: &[usize]) -> Result<Vec<usize>, CsrError> {
    let mut offsets = Vec::with_capacity(counts.len() + 1);
    offsets.push(0usize);
    for &count in counts {
        offsets.push(
            offsets
                .last()
                .copied()
                .unwrap_or(0)
                .checked_add(count)
                .ok_or(CsrError::CardinalityOverflow)?,
        );
    }
    Ok(offsets)
}

fn compact_sorted_buckets<T>(
    values: Vec<T>,
    source_offsets: &[usize],
) -> Result<(Vec<u64>, Vec<T>), CsrError>
where
    T: Copy + Eq + Send + Sync,
{
    let row_count = source_offsets.len().saturating_sub(1);
    let unique_counts = (0..row_count)
        .into_par_iter()
        .map(|row| sorted_unique_count(&values[source_offsets[row]..source_offsets[row + 1]]))
        .collect::<Vec<_>>();
    let target_offsets = bucket_offsets(&unique_counts)?;
    let target_offsets_u64 = target_offsets.iter().map(|&offset| offset as u64).collect();
    let target_len = target_offsets.last().copied().unwrap_or(0);
    let mut compacted = Vec::<MaybeUninit<T>>::with_capacity(target_len);
    // Every target slot belongs to exactly one row. The recursive copier writes
    // each slot once before `assume_init_vec` converts the allocation to Vec<T>.
    unsafe {
        compacted.set_len(target_len);
    }
    copy_sorted_unique_buckets(
        &values,
        source_offsets,
        &mut compacted,
        &target_offsets,
        0,
        row_count,
        0,
        0,
    );
    Ok((target_offsets_u64, assume_init_vec(compacted)))
}

fn sorted_unique_count<T: Eq + Sync>(values: &[T]) -> usize {
    if values.is_empty() {
        return 0;
    }
    const PARALLEL_THRESHOLD: usize = 65_536;
    let changes = if values.len() >= PARALLEL_THRESHOLD {
        values
            .par_windows(2)
            .filter(|pair| pair[0] != pair[1])
            .count()
    } else {
        values.windows(2).filter(|pair| pair[0] != pair[1]).count()
    };
    changes + 1
}

#[allow(clippy::too_many_arguments)]
fn copy_sorted_unique_buckets<T>(
    source: &[T],
    source_offsets: &[usize],
    target: &mut [MaybeUninit<T>],
    target_offsets: &[usize],
    first_row: usize,
    end_row: usize,
    source_base: usize,
    target_base: usize,
) where
    T: Copy + Eq + Send + Sync,
{
    const MIN_PARALLEL_VALUES: usize = 16_384;
    if first_row >= end_row {
        return;
    }
    if source.len() < MIN_PARALLEL_VALUES || end_row - first_row <= 1 {
        for row in first_row..end_row {
            let source_start = source_offsets[row] - source_base;
            let source_end = source_offsets[row + 1] - source_base;
            let target_start = target_offsets[row] - target_base;
            let target_end = target_offsets[row + 1] - target_base;
            write_sorted_unique(
                &source[source_start..source_end],
                &mut target[target_start..target_end],
            );
        }
        return;
    }
    let middle = first_row + (end_row - first_row) / 2;
    let source_split = source_offsets[middle] - source_base;
    let target_split = target_offsets[middle] - target_base;
    let (source_left, source_right) = source.split_at(source_split);
    let (target_left, target_right) = target.split_at_mut(target_split);
    rayon::join(
        || {
            copy_sorted_unique_buckets(
                source_left,
                source_offsets,
                target_left,
                target_offsets,
                first_row,
                middle,
                source_base,
                target_base,
            )
        },
        || {
            copy_sorted_unique_buckets(
                source_right,
                source_offsets,
                target_right,
                target_offsets,
                middle,
                end_row,
                source_offsets[middle],
                target_offsets[middle],
            )
        },
    );
}

fn write_sorted_unique<T: Copy + Eq>(source: &[T], target: &mut [MaybeUninit<T>]) {
    if source.is_empty() {
        debug_assert!(target.is_empty());
        return;
    }
    let mut written = 0usize;
    let mut previous = source[0];
    target[written].write(previous);
    written += 1;
    for &value in &source[1..] {
        if value != previous {
            target[written].write(value);
            written += 1;
            previous = value;
        }
    }
    debug_assert_eq!(written, target.len());
}

fn assume_init_vec<T>(mut values: Vec<MaybeUninit<T>>) -> Vec<T> {
    let pointer = values.as_mut_ptr().cast::<T>();
    let length = values.len();
    let capacity = values.capacity();
    std::mem::forget(values);
    // SAFETY: `compact_sorted_buckets` sizes each target row from the exact
    // unique count, and `write_sorted_unique` initializes every slot once.
    unsafe { Vec::from_raw_parts(pointer, length, capacity) }
}

fn parallel_sort_buckets<T: Ord + Send>(values: &mut [T], offsets: &[usize]) {
    fn recurse<T: Ord + Send>(
        values: &mut [T],
        offsets: &[usize],
        first_row: usize,
        end_row: usize,
        base: usize,
    ) {
        const MIN_PARALLEL_VALUES: usize = 16_384;
        if first_row >= end_row {
            return;
        }
        if end_row - first_row == 1 {
            values.par_sort_unstable();
            return;
        }
        if values.len() < MIN_PARALLEL_VALUES {
            for row in first_row..end_row {
                let start = offsets[row] - base;
                let end = offsets[row + 1] - base;
                values[start..end].sort_unstable();
            }
            return;
        }
        let middle = first_row + (end_row - first_row) / 2;
        let split = offsets[middle] - base;
        let (left, right) = values.split_at_mut(split);
        rayon::join(
            || recurse(left, offsets, first_row, middle, base),
            || recurse(right, offsets, middle, end_row, offsets[middle]),
        );
    }

    recurse(values, offsets, 0, offsets.len().saturating_sub(1), 0);
}

/// Persist the in-memory CSR while reporting cumulative payload bytes.
/// Headers and checksum footers are deliberately excluded so the total is
/// stable and can be computed before any file is opened.
pub fn write_csr_files_with_progress(
    bundle_dir: &Path,
    csr: &BidirectionalCsr,
    on_payload_bytes: impl FnMut(u64),
) -> Result<(), CsrError> {
    write_csr_view_files_with_progress(bundle_dir, csr.as_view(), on_payload_bytes)
}

pub fn write_csr_view_files_with_progress(
    bundle_dir: &Path,
    csr: BidirectionalCsrView<'_>,
    mut on_payload_bytes: impl FnMut(u64),
) -> Result<(), CsrError> {
    let mut completed = 0u64;
    format::write_u64_iter_with_progress(
        &bundle_dir.join("contract_token_offsets.u64"),
        ArrayKind::U64,
        csr.contract_token_offsets.len() as u64,
        csr.contract_token_offsets.iter().copied(),
        |local| on_payload_bytes(completed.saturating_add(local)),
    )?;
    completed = completed.saturating_add(csr.contract_token_offsets.len() as u64 * 8);
    format::write_u32_iter_with_progress(
        &bundle_dir.join("contract_tokens.u32"),
        ArrayKind::U32,
        csr.contract_tokens.len() as u64,
        csr.contract_tokens.iter().copied(),
        |local| on_payload_bytes(completed.saturating_add(local)),
    )?;
    completed = completed.saturating_add(csr.contract_tokens.len() as u64 * 4);
    format::write_u64_iter_with_progress(
        &bundle_dir.join("token_member_offsets.u64"),
        ArrayKind::U64,
        csr.token_member_offsets.len() as u64,
        csr.token_member_offsets.iter().copied(),
        |local| on_payload_bytes(completed.saturating_add(local)),
    )?;
    completed = completed.saturating_add(csr.token_member_offsets.len() as u64 * 8);
    format::write_u32_iter_with_progress(
        &bundle_dir.join("token_member_contracts.u32"),
        ArrayKind::U32,
        csr.token_member_contracts.len() as u64,
        csr.token_member_contracts.iter().copied(),
        |local| on_payload_bytes(completed.saturating_add(local)),
    )?;
    completed = completed.saturating_add(csr.token_member_contracts.len() as u64 * 4);
    format::write_u32_iter_with_progress(
        &bundle_dir.join("token_member_sources.u32"),
        ArrayKind::U32,
        csr.token_member_sources.len() as u64,
        csr.token_member_sources.iter().copied(),
        |local| on_payload_bytes(completed.saturating_add(local)),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_bucket_fill_matches_canonical_bidirectional_csr() {
        let memberships = [
            (0, 0, [1, 3].as_slice()),
            (1, 0, [2, 3].as_slice()),
            (2, 1, [1, 2].as_slice()),
        ];

        let csr = build_bidirectional_csr_from_iter_with_lanes(memberships, 4).unwrap();

        assert_eq!(csr.contract_token_offsets, vec![0, 3, 5]);
        assert_eq!(csr.contract_tokens, vec![1, 2, 3, 1, 2]);
        assert_eq!(csr.token_member_offsets, vec![0, 0, 2, 4, 6]);
        assert_eq!(csr.token_member_contracts, vec![0, 1, 0, 1, 0, 0]);
        assert_eq!(csr.token_member_sources, vec![0, 2, 1, 2, 0, 1]);
    }

    #[test]
    fn parallel_bucket_sort_never_moves_values_across_csr_rows() {
        let mut values = vec![3, 1, 2, 9, 7, 8];
        parallel_sort_buckets(&mut values, &[0, 3, 6]);
        assert_eq!(values, vec![1, 2, 3, 7, 8, 9]);
    }
}
