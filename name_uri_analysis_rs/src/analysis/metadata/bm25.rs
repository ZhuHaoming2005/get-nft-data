use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
#[cfg(test)]
use std::sync::Arc;

use super::parse::metadata_bm25_tokens;
#[cfg(test)]
use super::MetadataContractIndex;
use super::{MetadataDocIndex, METADATA_THRESHOLD};
use crate::atomic_file::replace_file_atomically;
use memmap2::{Mmap, MmapOptions};

pub(super) const METADATA_BM25_K1: f64 = 1.2;
pub(super) const METADATA_BM25_B: f64 = 0.75;

#[derive(Debug, Clone)]
pub(crate) struct MetadataBm25Document {
    pub(super) tokens: Vec<String>,
    pub(super) unique_tokens: Vec<String>,
    pub(super) term_freqs: HashMap<String, usize>,
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct MetadataContentRecord {
    pub(super) contract_index: MetadataContractIndex,
    pub(super) doc: Arc<MetadataBm25Document>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CompactMetadataContentDocument {
    pub(super) len: usize,
    pub(super) terms: Vec<(u32, usize)>,
}

#[cfg(test)]
pub(super) struct CompactMetadataContentSet {
    pub(super) docs: Vec<CompactMetadataContentDocument>,
}

#[derive(Debug)]
pub(super) struct InternedMetadataSourceDoc {
    pub(super) tokens: Vec<usize>,
    pub(super) term_freqs: HashMap<usize, usize>,
    pub(super) unique_tokens: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct InternedMetadataCorpus {
    pub(super) total_docs: usize,
    pub(super) avg_doc_len: f64,
    pub(super) doc_freqs: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct PreparedInternedMetadataQuery {
    pub(super) terms: Vec<(usize, usize)>,
    pub(super) denominator: f64,
    pub(super) candidate_tokens: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct PreparedInternedMetadataDoc {
    pub(super) token_weights: Vec<(usize, f64)>,
}

#[derive(Debug)]
pub(super) struct CompactMetadataPostings {
    offsets: MetadataPostingOffsets,
    values: MetadataPostingValues,
}

#[derive(Debug)]
enum MetadataPostingOffsets {
    Owned(Vec<u64>),
    Mapped(Mmap),
}

#[derive(Debug)]
enum MetadataPostingValues {
    Owned(Vec<MetadataDocIndex>),
    Mapped(Mmap),
}

#[derive(Debug)]
enum F64Storage {
    Owned(Vec<f64>),
    Mapped(Mmap),
}

#[derive(Debug)]
struct CompactF64Lists {
    offsets: MetadataPostingOffsets,
    values: F64Storage,
}

#[derive(Debug)]
pub(super) struct CompactMetadataScoring {
    query_tokens: CompactMetadataPostings,
    query_frequencies: CompactMetadataPostings,
    query_denominators: F64Storage,
    candidate_tokens: CompactMetadataPostings,
    prepared_tokens: CompactMetadataPostings,
    prepared_weights: CompactF64Lists,
}

impl CompactF64Lists {
    fn from_flat(offsets: Vec<u64>, values: Vec<f64>) -> Self {
        Self {
            offsets: MetadataPostingOffsets::Owned(offsets),
            values: F64Storage::Owned(values),
        }
    }

    fn posting(&self, index: usize) -> &[f64] {
        let offsets = posting_offsets_slice(&self.offsets);
        &f64_slice(&self.values)[offsets[index] as usize..offsets[index + 1] as usize]
    }

    fn owned_memory_bytes(&self) -> usize {
        owned_offset_bytes(&self.offsets).saturating_add(owned_f64_bytes(&self.values))
    }

    fn persist_and_remap(
        self,
        directory: &Path,
        offsets_name: &str,
        values_name: &str,
    ) -> io::Result<Self> {
        let offsets_path = directory.join(offsets_name);
        let values_path = directory.join(values_name);
        write_u64_artifact(&offsets_path, posting_offsets_slice(&self.offsets))?;
        write_f64_artifact(&values_path, f64_slice(&self.values))?;
        Ok(Self {
            offsets: map_u64_storage(offsets_path)?,
            values: map_f64_storage(values_path)?,
        })
    }

    #[cfg(test)]
    fn is_mapped(&self) -> bool {
        matches!(self.offsets, MetadataPostingOffsets::Mapped(_))
            && matches!(self.values, F64Storage::Mapped(_))
    }
}

impl CompactMetadataScoring {
    pub(super) fn from_nested(
        queries: Vec<PreparedInternedMetadataQuery>,
        prepared_docs: Vec<PreparedInternedMetadataDoc>,
    ) -> Self {
        let query_term_count = queries.iter().map(|query| query.terms.len()).sum();
        let candidate_token_count = queries
            .iter()
            .map(|query| query.candidate_tokens.len())
            .sum();
        let prepared_term_count = prepared_docs
            .iter()
            .map(|doc| doc.token_weights.len())
            .sum();

        let mut query_token_offsets = Vec::with_capacity(queries.len() + 1);
        let mut query_token_values = Vec::with_capacity(query_term_count);
        let mut query_frequency_offsets = Vec::with_capacity(queries.len() + 1);
        let mut query_frequency_values = Vec::with_capacity(query_term_count);
        let mut query_denominators = Vec::with_capacity(queries.len());
        let mut candidate_token_offsets = Vec::with_capacity(queries.len() + 1);
        let mut candidate_token_values = Vec::with_capacity(candidate_token_count);
        query_token_offsets.push(0);
        query_frequency_offsets.push(0);
        candidate_token_offsets.push(0);
        for query in queries {
            for (token, frequency) in query.terms {
                query_token_values.push(u32::try_from(token).expect("metadata token exceeds u32"));
                query_frequency_values
                    .push(u32::try_from(frequency).expect("metadata term frequency exceeds u32"));
            }
            query_token_offsets.push(query_token_values.len() as u64);
            query_frequency_offsets.push(query_frequency_values.len() as u64);
            query_denominators.push(query.denominator);
            candidate_token_values.extend(
                query
                    .candidate_tokens
                    .into_iter()
                    .map(|token| u32::try_from(token).expect("metadata token exceeds u32")),
            );
            candidate_token_offsets.push(candidate_token_values.len() as u64);
        }

        let mut prepared_token_offsets = Vec::with_capacity(prepared_docs.len() + 1);
        let mut prepared_token_values = Vec::with_capacity(prepared_term_count);
        let mut prepared_weight_offsets = Vec::with_capacity(prepared_docs.len() + 1);
        let mut prepared_weight_values = Vec::with_capacity(prepared_term_count);
        prepared_token_offsets.push(0);
        prepared_weight_offsets.push(0);
        for doc in prepared_docs {
            for (token, weight) in doc.token_weights {
                prepared_token_values
                    .push(u32::try_from(token).expect("metadata token exceeds u32"));
                prepared_weight_values.push(weight);
            }
            prepared_token_offsets.push(prepared_token_values.len() as u64);
            prepared_weight_offsets.push(prepared_weight_values.len() as u64);
        }

        Self {
            query_tokens: CompactMetadataPostings::from_flat(
                query_token_offsets,
                query_token_values,
            ),
            query_frequencies: CompactMetadataPostings::from_flat(
                query_frequency_offsets,
                query_frequency_values,
            ),
            query_denominators: F64Storage::Owned(query_denominators),
            candidate_tokens: CompactMetadataPostings::from_flat(
                candidate_token_offsets,
                candidate_token_values,
            ),
            prepared_tokens: CompactMetadataPostings::from_flat(
                prepared_token_offsets,
                prepared_token_values,
            ),
            prepared_weights: CompactF64Lists::from_flat(
                prepared_weight_offsets,
                prepared_weight_values,
            ),
        }
    }

    pub(super) fn candidate_tokens(&self, index: usize) -> &[u32] {
        self.candidate_tokens.posting(index)
    }

    pub(super) fn owned_memory_bytes(&self) -> usize {
        self.query_tokens
            .owned_memory_bytes()
            .saturating_add(self.query_frequencies.owned_memory_bytes())
            .saturating_add(owned_f64_bytes(&self.query_denominators))
            .saturating_add(self.candidate_tokens.owned_memory_bytes())
            .saturating_add(self.prepared_tokens.owned_memory_bytes())
            .saturating_add(self.prepared_weights.owned_memory_bytes())
    }

    #[cfg(test)]
    pub(super) fn query_terms_len(&self, index: usize) -> usize {
        self.query_tokens.posting(index).len()
    }

    #[cfg(test)]
    pub(super) fn query_term_frequency(&self, index: usize, token: u32) -> Option<u32> {
        self.query_tokens
            .posting(index)
            .binary_search(&token)
            .ok()
            .map(|position| self.query_frequencies.posting(index)[position])
    }

    pub(super) fn score(&self, query: usize, right: usize) -> f64 {
        let query_tokens = self.query_tokens.posting(query);
        let query_frequencies = self.query_frequencies.posting(query);
        let right_tokens = self.prepared_tokens.posting(right);
        let right_weights = self.prepared_weights.posting(right);
        if query_tokens.is_empty() || right_tokens.is_empty() {
            return 0.0;
        }
        let mut score = 0.0;
        let mut query_index = 0;
        let mut right_index = 0;
        while query_index < query_tokens.len() && right_index < right_tokens.len() {
            match query_tokens[query_index].cmp(&right_tokens[right_index]) {
                std::cmp::Ordering::Equal => {
                    score += query_frequencies[query_index] as f64 * right_weights[right_index];
                    query_index += 1;
                    right_index += 1;
                }
                std::cmp::Ordering::Less => query_index += 1,
                std::cmp::Ordering::Greater => right_index += 1,
            }
        }
        (score / f64_slice(&self.query_denominators)[query]).clamp(0.0, 1.0)
    }

    pub(super) fn persist_and_remap(self, directory: &Path) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let query_denominators_path = directory.join("query_denominators.bin");
        write_f64_artifact(
            &query_denominators_path,
            f64_slice(&self.query_denominators),
        )?;
        Ok(Self {
            query_tokens: self.query_tokens.persist_and_remap_named(
                directory,
                "query_term_offsets.bin",
                "query_term_tokens.bin",
            )?,
            query_frequencies: self.query_frequencies.persist_and_remap_named(
                directory,
                "query_frequency_offsets.bin",
                "query_frequencies.bin",
            )?,
            query_denominators: map_f64_storage(query_denominators_path)?,
            candidate_tokens: self.candidate_tokens.persist_and_remap_named(
                directory,
                "candidate_token_offsets.bin",
                "candidate_tokens.bin",
            )?,
            prepared_tokens: self.prepared_tokens.persist_and_remap_named(
                directory,
                "prepared_token_offsets.bin",
                "prepared_tokens.bin",
            )?,
            prepared_weights: self.prepared_weights.persist_and_remap(
                directory,
                "prepared_weight_offsets.bin",
                "prepared_weights.bin",
            )?,
        })
    }

    #[cfg(test)]
    pub(super) fn is_mapped(&self) -> bool {
        matches!(self.query_denominators, F64Storage::Mapped(_))
            && self.query_tokens.is_mapped()
            && self.query_frequencies.is_mapped()
            && self.candidate_tokens.is_mapped()
            && self.prepared_tokens.is_mapped()
            && self.prepared_weights.is_mapped()
    }
}

impl CompactMetadataPostings {
    fn from_flat(offsets: Vec<u64>, values: Vec<MetadataDocIndex>) -> Self {
        Self {
            offsets: MetadataPostingOffsets::Owned(offsets),
            values: MetadataPostingValues::Owned(values),
        }
    }

    pub(super) fn from_nested(postings: Vec<Vec<MetadataDocIndex>>) -> Self {
        let total_values = postings.iter().map(Vec::len).sum();
        let mut offsets = Vec::with_capacity(postings.len() + 1);
        let mut values = Vec::with_capacity(total_values);
        offsets.push(0);
        for posting in postings {
            values.extend(posting);
            offsets.push(values.len() as u64);
        }
        Self {
            offsets: MetadataPostingOffsets::Owned(offsets),
            values: MetadataPostingValues::Owned(values),
        }
    }

    pub(super) fn from_symmetric_pairs(
        doc_count: usize,
        pairs: &[(MetadataDocIndex, MetadataDocIndex)],
    ) -> Self {
        if pairs.is_empty() {
            return Self::from_nested(Vec::new());
        }
        let mut offsets = vec![0u64; doc_count.saturating_add(1)];
        for &(left, right) in pairs {
            if left == right {
                continue;
            }
            offsets[super::metadata_doc_index_to_usize(left) + 1] += 1;
            offsets[super::metadata_doc_index_to_usize(right) + 1] += 1;
        }
        for index in 1..offsets.len() {
            offsets[index] = offsets[index].saturating_add(offsets[index - 1]);
        }
        let value_count = usize::try_from(*offsets.last().unwrap_or(&0)).unwrap_or(usize::MAX);
        let mut values = vec![0u32; value_count];
        let mut cursors = offsets[..doc_count].to_vec();
        for &(left, right) in pairs {
            if left == right {
                continue;
            }
            let left_index = super::metadata_doc_index_to_usize(left);
            let right_index = super::metadata_doc_index_to_usize(right);
            let left_position = cursors[left_index] as usize;
            values[left_position] = right;
            cursors[left_index] += 1;
            let right_position = cursors[right_index] as usize;
            values[right_position] = left;
            cursors[right_index] += 1;
        }
        debug_assert!((0..doc_count).all(|doc| {
            let start = offsets[doc] as usize;
            let end = offsets[doc + 1] as usize;
            values[start..end].is_sorted()
        }));
        Self {
            offsets: MetadataPostingOffsets::Owned(offsets),
            values: MetadataPostingValues::Owned(values),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.offsets().len().saturating_sub(1)
    }

    pub(super) fn owned_memory_bytes(&self) -> usize {
        owned_offset_bytes(&self.offsets).saturating_add(match &self.values {
            MetadataPostingValues::Owned(values) => values
                .capacity()
                .saturating_mul(std::mem::size_of::<MetadataDocIndex>()),
            MetadataPostingValues::Mapped(_) => 0,
        })
    }

    pub(super) fn posting(&self, token: usize) -> &[MetadataDocIndex] {
        let start = self.offsets()[token] as usize;
        let end = self.offsets()[token + 1] as usize;
        &self.values()[start..end]
    }

    fn values(&self) -> &[MetadataDocIndex] {
        match &self.values {
            MetadataPostingValues::Owned(values) => values,
            MetadataPostingValues::Mapped(mapping) => {
                debug_assert_eq!(mapping.len() % size_of::<MetadataDocIndex>(), 0);
                // SAFETY: file mappings start at page-aligned addresses, the
                // artifact contains only native-endian u32 values written by
                // this same binary, and its byte length is validated below.
                unsafe {
                    std::slice::from_raw_parts(
                        mapping.as_ptr().cast::<MetadataDocIndex>(),
                        mapping.len() / size_of::<MetadataDocIndex>(),
                    )
                }
            }
        }
    }

    fn offsets(&self) -> &[u64] {
        match &self.offsets {
            MetadataPostingOffsets::Owned(offsets) => offsets,
            MetadataPostingOffsets::Mapped(mapping) => {
                debug_assert_eq!(mapping.len() % size_of::<u64>(), 0);
                // SAFETY: see `values`; this artifact contains aligned u64s.
                unsafe {
                    std::slice::from_raw_parts(
                        mapping.as_ptr().cast::<u64>(),
                        mapping.len() / size_of::<u64>(),
                    )
                }
            }
        }
    }

    pub(super) fn persist_and_remap(self, directory: &Path) -> io::Result<Self> {
        self.persist_and_remap_named(directory, "posting_offsets.bin", "postings.bin")
    }

    pub(super) fn persist_and_remap_named(
        self,
        directory: &Path,
        offsets_name: &str,
        values_name: &str,
    ) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let offsets_path = directory.join(offsets_name);
        let values_path = directory.join(values_name);
        write_u64_artifact(&offsets_path, self.offsets())?;
        let offsets = match self.offsets {
            MetadataPostingOffsets::Owned(offsets) => offsets,
            MetadataPostingOffsets::Mapped(mapping) => {
                return Ok(Self {
                    offsets: MetadataPostingOffsets::Mapped(mapping),
                    values: self.values,
                });
            }
        };
        let values = match self.values {
            MetadataPostingValues::Owned(values) => values,
            MetadataPostingValues::Mapped(mapping) => {
                return Ok(Self {
                    offsets: MetadataPostingOffsets::Owned(offsets),
                    values: MetadataPostingValues::Mapped(mapping),
                });
            }
        };
        write_u32_artifact(&values_path, &values)?;
        if values.is_empty() {
            return Ok(Self {
                offsets: MetadataPostingOffsets::Owned(offsets),
                values: MetadataPostingValues::Owned(values),
            });
        }
        let file = fs::File::open(values_path)?;
        // SAFETY: the file is immutable for the remaining metadata phase and
        // was fully flushed before it was opened for mapping.
        let mapping = unsafe { MmapOptions::new().map(&file)? };
        if mapping.len() % size_of::<MetadataDocIndex>() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metadata postings artifact length is not u32-aligned",
            ));
        }
        let offsets_file = fs::File::open(offsets_path)?;
        // SAFETY: same invariant as the postings values mapping.
        let offsets_mapping = unsafe { MmapOptions::new().map(&offsets_file)? };
        if offsets_mapping.len() % size_of::<u64>() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metadata posting offsets artifact length is not u64-aligned",
            ));
        }
        Ok(Self {
            offsets: MetadataPostingOffsets::Mapped(offsets_mapping),
            values: MetadataPostingValues::Mapped(mapping),
        })
    }

    #[cfg(test)]
    pub(super) fn is_mapped(&self) -> bool {
        matches!(self.offsets, MetadataPostingOffsets::Mapped(_))
            && matches!(self.values, MetadataPostingValues::Mapped(_))
    }
}

fn owned_offset_bytes(storage: &MetadataPostingOffsets) -> usize {
    match storage {
        MetadataPostingOffsets::Owned(values) => {
            values.capacity().saturating_mul(std::mem::size_of::<u64>())
        }
        MetadataPostingOffsets::Mapped(_) => 0,
    }
}

fn owned_f64_bytes(storage: &F64Storage) -> usize {
    match storage {
        F64Storage::Owned(values) => values.capacity().saturating_mul(std::mem::size_of::<f64>()),
        F64Storage::Mapped(_) => 0,
    }
}

fn write_u64_artifact(path: &Path, values: &[u64]) -> io::Result<()> {
    let partial = path.with_extension("bin.partial");
    let mut file = fs::File::create(&partial)?;
    write_u64_values(&mut file, values)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_artifact(partial, path)
}

fn write_u32_artifact(path: &Path, values: &[u32]) -> io::Result<()> {
    let partial = path.with_extension("bin.partial");
    let mut file = fs::File::create(&partial)?;
    write_u32_values(&mut file, values)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_artifact(partial, path)
}

fn write_f64_artifact(path: &Path, values: &[f64]) -> io::Result<()> {
    let partial = path.with_extension("bin.partial");
    let mut file = fs::File::create(&partial)?;
    write_f64_values(&mut file, values)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_artifact(partial, path)
}

const ARTIFACT_CHUNK_VALUES: usize = 1 << 20;

pub(super) fn write_u32_values(writer: &mut impl Write, values: &[u32]) -> io::Result<()> {
    write_scalar_chunks(writer, values, u32::to_le_bytes)
}

pub(super) fn write_u64_values(writer: &mut impl Write, values: &[u64]) -> io::Result<()> {
    write_scalar_chunks(writer, values, u64::to_le_bytes)
}

fn write_f64_values(writer: &mut impl Write, values: &[f64]) -> io::Result<()> {
    write_scalar_chunks(writer, values, f64::to_le_bytes)
}

fn write_scalar_chunks<T, const WIDTH: usize>(
    writer: &mut impl Write,
    values: &[T],
    encode: impl Fn(T) -> [u8; WIDTH] + Copy,
) -> io::Result<()>
where
    T: Copy,
{
    let mut bytes = Vec::with_capacity(ARTIFACT_CHUNK_VALUES.saturating_mul(WIDTH));
    for chunk in values.chunks(ARTIFACT_CHUNK_VALUES) {
        bytes.clear();
        for &value in chunk {
            bytes.extend_from_slice(&encode(value));
        }
        writer.write_all(&bytes)?;
    }
    Ok(())
}

fn posting_offsets_slice(storage: &MetadataPostingOffsets) -> &[u64] {
    match storage {
        MetadataPostingOffsets::Owned(values) => values,
        MetadataPostingOffsets::Mapped(mapping) => unsafe {
            std::slice::from_raw_parts(mapping.as_ptr().cast(), mapping.len() / size_of::<u64>())
        },
    }
}

fn f64_slice(storage: &F64Storage) -> &[f64] {
    match storage {
        F64Storage::Owned(values) => values,
        F64Storage::Mapped(mapping) => unsafe {
            std::slice::from_raw_parts(mapping.as_ptr().cast(), mapping.len() / size_of::<f64>())
        },
    }
}

fn map_u64_storage(path: std::path::PathBuf) -> io::Result<MetadataPostingOffsets> {
    if fs::metadata(&path)?.len() == 0 {
        return Ok(MetadataPostingOffsets::Owned(Vec::new()));
    }
    let file = fs::File::open(path)?;
    let mapping = unsafe { MmapOptions::new().map(&file)? };
    if mapping.len() % size_of::<u64>() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unaligned u64 artifact",
        ));
    }
    Ok(MetadataPostingOffsets::Mapped(mapping))
}

fn map_f64_storage(path: std::path::PathBuf) -> io::Result<F64Storage> {
    if fs::metadata(&path)?.len() == 0 {
        return Ok(F64Storage::Owned(Vec::new()));
    }
    let file = fs::File::open(path)?;
    let mapping = unsafe { MmapOptions::new().map(&file)? };
    if mapping.len() % size_of::<f64>() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unaligned f64 artifact",
        ));
    }
    Ok(F64Storage::Mapped(mapping))
}

fn replace_artifact(partial: std::path::PathBuf, destination: &Path) -> io::Result<()> {
    replace_file_atomically(&partial, destination)
}

/// Shared Okapi BM25 term contribution used by interned and compact scorers.
#[inline]
pub(super) fn bm25_term_score(query_tf: f64, tf: f64, idf: f64, norm: f64, k1: f64) -> f64 {
    if tf == 0.0 {
        return 0.0;
    }
    query_tf * idf * (tf * (k1 + 1.0)) / (tf + norm)
}

impl MetadataBm25Document {
    pub(super) fn len(&self) -> usize {
        self.tokens.len()
    }

    pub(super) fn unique_len(&self) -> usize {
        self.unique_tokens.len()
    }

    pub(super) fn memory_bytes(&self) -> usize {
        let string_bytes = |values: &Vec<String>| {
            values
                .iter()
                .map(|value| value.capacity())
                .fold(0usize, usize::saturating_add)
                .saturating_add(
                    values
                        .capacity()
                        .saturating_mul(std::mem::size_of::<String>()),
                )
        };
        string_bytes(&self.tokens)
            .saturating_add(string_bytes(&self.unique_tokens))
            .saturating_add(super::load::hash_table_allocation_bytes(
                self.term_freqs.capacity(),
                std::mem::size_of::<(String, usize)>(),
            ))
            .saturating_add(
                self.term_freqs
                    .keys()
                    .map(|value| value.capacity())
                    .fold(0usize, usize::saturating_add),
            )
    }

    pub(super) fn from_text(document: &str) -> Option<Self> {
        let mut tokens = metadata_bm25_tokens(document);
        if tokens.is_empty() {
            return None;
        }
        tokens.sort_unstable();
        let mut term_freqs = HashMap::new();
        for token in &tokens {
            *term_freqs.entry(token.clone()).or_insert(0usize) += 1;
        }
        let mut unique_tokens = tokens.clone();
        unique_tokens.sort_unstable();
        unique_tokens.dedup();
        Some(Self {
            tokens,
            unique_tokens,
            term_freqs,
        })
    }
}

pub(super) fn metadata_token_id(token: &str, token_ids: &HashMap<&str, usize>) -> usize {
    *token_ids
        .get(token)
        .expect("metadata token must be present in the lexical token id map")
}

impl InternedMetadataSourceDoc {
    pub(super) fn from_metadata_doc(
        doc: &MetadataBm25Document,
        token_ids: &HashMap<&str, usize>,
    ) -> Self {
        let mut tokens = Vec::with_capacity(doc.tokens.len());
        let mut term_freqs = HashMap::new();
        for token in &doc.tokens {
            let token_id = metadata_token_id(token, token_ids);
            tokens.push(token_id);
            *term_freqs.entry(token_id).or_insert(0usize) += 1;
        }
        let mut unique_tokens = term_freqs.keys().copied().collect::<Vec<_>>();
        unique_tokens.sort_unstable();
        Self {
            tokens,
            term_freqs,
            unique_tokens,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.tokens.len()
    }

    pub(super) fn term_frequency(&self, token: usize) -> usize {
        *self.term_freqs.get(&token).unwrap_or(&0)
    }

    pub(super) fn unique_tokens(&self) -> &[usize] {
        &self.unique_tokens
    }
}

impl InternedMetadataCorpus {
    pub(super) fn from_doc_weights(
        doc_weights: &[usize],
        docs: &[InternedMetadataSourceDoc],
        token_count: usize,
    ) -> Self {
        let mut total_docs = 0usize;
        let mut total_terms = 0usize;
        let mut doc_freqs = vec![0; token_count];
        for (&weight, doc) in doc_weights.iter().zip(docs) {
            if weight == 0 {
                continue;
            }
            total_docs += weight;
            total_terms += doc.len() * weight;
            for &token in doc.unique_tokens() {
                doc_freqs[token] += weight;
            }
        }
        let avg_doc_len = if total_docs == 0 {
            0.0
        } else {
            total_terms as f64 / total_docs as f64
        };
        Self {
            total_docs,
            avg_doc_len,
            doc_freqs,
        }
    }
}

impl PreparedInternedMetadataQuery {
    pub(super) fn new(
        query: &InternedMetadataSourceDoc,
        corpus: &InternedMetadataCorpus,
        max_token_weights: &[f64],
        postings: &CompactMetadataPostings,
    ) -> Self {
        let terms = query_terms_from_token_ids(&query.tokens);
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        let candidate_tokens = metadata_bm25_candidate_prefix(
            &terms,
            denominator,
            max_token_weights,
            postings,
            METADATA_THRESHOLD,
        );
        Self {
            terms,
            denominator,
            candidate_tokens,
        }
    }
}

pub(super) fn metadata_bm25_candidate_prefix(
    terms: &[(usize, usize)],
    denominator: f64,
    max_token_weights: &[f64],
    postings: &CompactMetadataPostings,
    threshold: f64,
) -> Vec<usize> {
    let mut candidates = terms
        .iter()
        .filter_map(|&(token, query_tf)| {
            let max_weight = max_token_weights.get(token).copied().unwrap_or(0.0);
            let upper_bound = query_tf as f64 * max_weight;
            (upper_bound > 0.0).then_some((token, upper_bound, postings.posting(token).len()))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Vec::new();
    }

    candidates.sort_unstable_by(|left, right| {
        let left_cost = left.2 as f64 / left.1;
        let right_cost = right.2 as f64 / right.1;
        left_cost
            .total_cmp(&right_cost)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut remaining_upper_bound = candidates
        .iter()
        .map(|(_, upper_bound, _)| upper_bound)
        .sum::<f64>();
    let required_score = threshold * denominator;
    let tolerance = f64::EPSILON
        * (remaining_upper_bound.abs() + required_score.abs() + 1.0)
        * candidates.len() as f64
        * 8.0;
    let mut prefix = Vec::new();
    for (token, upper_bound, _) in candidates {
        prefix.push(token);
        remaining_upper_bound = (remaining_upper_bound - upper_bound).max(0.0);
        if remaining_upper_bound + tolerance < required_score {
            break;
        }
    }
    prefix
}

impl PreparedInternedMetadataDoc {
    pub(super) fn new(doc: &InternedMetadataSourceDoc, corpus: &InternedMetadataCorpus) -> Self {
        if doc.len() == 0 || corpus.total_docs == 0 || corpus.avg_doc_len <= 0.0 {
            return Self {
                token_weights: Vec::new(),
            };
        }

        let doc_len = doc.len() as f64;
        let norm = METADATA_BM25_K1
            * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);
        let token_weights = doc
            .unique_tokens()
            .iter()
            .filter_map(|&token| {
                let tf = doc.term_frequency(token) as f64;
                if tf == 0.0 {
                    return None;
                }
                let df = corpus.doc_freqs.get(token).copied().unwrap_or(0) as f64;
                let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
                let weight = bm25_term_score(1.0, tf, idf, norm, METADATA_BM25_K1);
                Some((token, weight))
            })
            .collect();
        Self { token_weights }
    }
}

pub(super) fn query_terms_from_token_ids(query_tokens: &[usize]) -> Vec<(usize, usize)> {
    if query_tokens.is_empty() {
        return Vec::new();
    }
    let mut tokens = query_tokens.to_vec();
    tokens.sort_unstable();
    let mut terms = Vec::new();
    let mut iter = tokens.into_iter();
    let Some(mut current) = iter.next() else {
        return Vec::new();
    };
    let mut count = 1usize;
    for token in iter {
        if token == current {
            count += 1;
        } else {
            terms.push((current, count));
            current = token;
            count = 1;
        }
    }
    terms.push((current, count));
    terms
}

pub(super) fn bm25_score_terms(
    query_terms: &[(usize, usize)],
    doc: &InternedMetadataSourceDoc,
    corpus: &InternedMetadataCorpus,
) -> f64 {
    if query_terms.is_empty()
        || doc.len() == 0
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }

    let doc_len = doc.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);

    query_terms
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_frequency(*token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = corpus.doc_freqs.get(*token).copied().unwrap_or(0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            bm25_term_score(*query_tf as f64, tf, idf, norm, METADATA_BM25_K1)
        })
        .sum()
}

#[cfg(test)]
impl CompactMetadataContentSet {
    pub(super) fn from_records(records: &[MetadataContentRecord]) -> Self {
        let mut token_ids = HashMap::<&str, u32>::new();
        for record in records {
            for token in &record.doc.unique_tokens {
                if token_ids.contains_key(token.as_str()) {
                    continue;
                }
                let token_id = u32::try_from(token_ids.len())
                    .expect("metadata content token dictionary exceeds u32 indexes");
                token_ids.insert(token, token_id);
            }
        }
        let docs = records
            .iter()
            .map(|record| {
                let mut terms = record
                    .doc
                    .term_freqs
                    .iter()
                    .map(|(token, term_frequency)| (token_ids[token.as_str()], *term_frequency))
                    .collect::<Vec<_>>();
                terms.sort_unstable_by_key(|(token_id, _)| *token_id);
                CompactMetadataContentDocument {
                    len: record.doc.tokens.len(),
                    terms,
                }
            })
            .collect();
        Self { docs }
    }
}

pub(super) fn compact_metadata_content_pair_score(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
) -> f64 {
    compact_metadata_single_document_score(left, right)
        .max(compact_metadata_single_document_score(right, left))
}

pub(super) fn compact_metadata_single_document_score(
    query: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
) -> f64 {
    if !compact_metadata_content_docs_share_token(query, right) {
        return 0.0;
    }
    let numerator = compact_metadata_single_corpus_bm25_score(query, right, right);
    let denominator = compact_metadata_single_corpus_bm25_score(query, query, right);
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

pub(super) fn compact_metadata_single_corpus_bm25_score(
    query: &CompactMetadataContentDocument,
    document: &CompactMetadataContentDocument,
    corpus_document: &CompactMetadataContentDocument,
) -> f64 {
    if query.len == 0 || document.len == 0 || corpus_document.len == 0 {
        return 0.0;
    }
    let doc_len = document.len as f64;
    let avg_doc_len = corpus_document.len as f64;
    let norm = METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / avg_doc_len);
    query
        .terms
        .iter()
        .map(|(token_id, query_tf)| {
            let tf = compact_metadata_content_term_frequency(document, *token_id) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let doc_freq =
                f64::from(compact_metadata_content_term_frequency(corpus_document, *token_id) > 0);
            let idf = ((1.0 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
            bm25_term_score(*query_tf as f64, tf, idf, norm, METADATA_BM25_K1)
        })
        .sum()
}

pub(super) fn compact_metadata_content_term_frequency(
    document: &CompactMetadataContentDocument,
    token_id: u32,
) -> usize {
    document
        .terms
        .binary_search_by_key(&token_id, |(document_token_id, _)| *document_token_id)
        .ok()
        .map_or(0, |index| document.terms[index].1)
}

pub(super) fn compact_metadata_content_docs_share_token(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.terms.len() && right_index < right.terms.len() {
        match left.terms[left_index].0.cmp(&right.terms[right_index].0) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

#[cfg(test)]
pub(super) fn metadata_content_pair_score(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    metadata_single_document_score(left, right).max(metadata_single_document_score(right, left))
}

#[cfg(test)]
pub(super) fn metadata_single_document_score(
    query: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    if !metadata_string_docs_share_token(query, right) {
        return 0.0;
    }
    let numerator = metadata_single_corpus_bm25_score(query, right, right);
    let denominator = metadata_single_corpus_bm25_score(query, query, right);
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
pub(super) fn metadata_single_corpus_bm25_score(
    query: &MetadataBm25Document,
    doc: &MetadataBm25Document,
    corpus_doc: &MetadataBm25Document,
) -> f64 {
    if query.tokens.is_empty() || doc.tokens.is_empty() || corpus_doc.tokens.is_empty() {
        return 0.0;
    }
    let doc_len = doc.tokens.len() as f64;
    let avg_doc_len = corpus_doc.tokens.len() as f64;
    let norm = METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / avg_doc_len);
    query
        .term_freqs
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_freqs.get(token).copied().unwrap_or(0) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let doc_freq = f64::from(corpus_doc.term_freqs.contains_key(token));
            let idf = ((1.0 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
            bm25_term_score(*query_tf as f64, tf, idf, norm, METADATA_BM25_K1)
        })
        .sum()
}

#[cfg(test)]
pub(super) fn metadata_string_docs_share_token(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.unique_tokens.len() && right_index < right.unique_tokens.len() {
        match left.unique_tokens[left_index].cmp(&right.unique_tokens[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

#[cfg(test)]
mod artifact_write_tests {
    use super::*;

    #[derive(Default)]
    struct CountingWriter {
        calls: usize,
        bytes: Vec<u8>,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            self.bytes.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn scalar_artifacts_are_serialized_in_large_chunks() {
        let values = (0..1_000_000u32).collect::<Vec<_>>();
        let mut writer = CountingWriter::default();

        write_u32_values(&mut writer, &values).unwrap();

        assert!(writer.calls <= 4, "write calls={}", writer.calls);
        assert_eq!(
            writer.bytes.len(),
            values.len() * std::mem::size_of::<u32>()
        );
        assert_eq!(&writer.bytes[..4], &0u32.to_le_bytes());
        assert_eq!(
            &writer.bytes[writer.bytes.len() - 4..],
            &(999_999u32).to_le_bytes()
        );
    }
}
