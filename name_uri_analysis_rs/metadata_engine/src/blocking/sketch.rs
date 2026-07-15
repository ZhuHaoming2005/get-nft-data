//! HEAD-equivalent Conservative sketches built from the complete atom universe.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use rayon::prelude::*;

use super::{AtomSketch, ANCHOR_COUNT};
use crate::encode::PayloadTermSoA;

const HIGH_FREQUENCY_MIN_DOCS: usize = 32;
const HIGH_FREQUENCY_DIVISOR: usize = 5;

/// One fallback atom's template/content term-frequency rows.
///
/// Conservative sketches intentionally use only term presence. The stored
/// frequency remains available to exact BM25 scoring but must not weight the
/// BaseEquivalent SimHash.
#[derive(Debug, Clone, Copy)]
pub struct BaseEquivalentAtomInput<'a> {
    pub template_terms: &'a [(u32, u32)],
    pub content_terms: &'a [(u32, u32)],
}

#[derive(Debug)]
struct DimensionSketch {
    simhash: u64,
    anchors: Vec<u32>,
    has_terms: bool,
}

enum DocumentFrequencies {
    Dense(Vec<usize>),
    Sparse(BTreeMap<u32, usize>),
    SparseSorted { terms: Vec<u32>, counts: Vec<usize> },
}

impl DocumentFrequencies {
    fn get(&self, term: u32) -> usize {
        match self {
            Self::Dense(frequencies) => frequencies[term as usize],
            Self::Sparse(frequencies) => frequencies[&term],
            Self::SparseSorted { terms, counts } => {
                counts[terms.binary_search(&term).expect("term was indexed")]
            }
        }
    }
}

/// Build global DF/IDF and high-frequency-anchor sketches over all supplied atoms.
pub fn build_base_equivalent_atom_sketches(
    atoms: &[BaseEquivalentAtomInput<'_>],
) -> Vec<AtomSketch> {
    build_sketches_sequential(atoms)
}

pub fn build_base_equivalent_atom_sketches_parallel(
    atoms: &[BaseEquivalentAtomInput<'_>],
    lanes: usize,
) -> Vec<AtomSketch> {
    if lanes <= 1 {
        return build_sketches_sequential(atoms);
    }
    let Ok(pool) = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|index| format!("metadata-sketch-{index}"))
        .build()
    else {
        return build_sketches_sequential(atoms);
    };
    pool.install(|| {
        let (template, content) = rayon::join(
            || build_dimension_parallel(atoms, |atom| atom.template_terms),
            || build_dimension_parallel(atoms, |atom| atom.content_terms),
        );
        combine_dimensions(template, content)
    })
}

/// Build sketches straight from payload SoA term-id slices. Frequencies are
/// intentionally not materialized because BaseEquivalent uses presence only.
pub fn build_base_equivalent_atom_sketches_from_soa_parallel(
    payloads: &PayloadTermSoA,
    atom_payloads: &[u32],
    lanes: usize,
) -> Vec<AtomSketch> {
    let atoms = atom_payloads
        .iter()
        .map(|&payload_id| {
            let payload = payload_id as usize;
            SoaAtomInput {
                template_terms: payloads.template_term_ids(payload),
                content_terms: payloads.content_term_ids(payload),
            }
        })
        .collect::<Vec<_>>();
    if lanes <= 1 {
        return build_id_sketches_sequential(&atoms);
    }
    let Ok(pool) = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|index| format!("metadata-sketch-{index}"))
        .build()
    else {
        return build_id_sketches_sequential(&atoms);
    };
    pool.install(|| {
        let (template, content) = rayon::join(
            || build_id_dimension_parallel(&atoms, |atom| atom.template_terms),
            || build_id_dimension_parallel(&atoms, |atom| atom.content_terms),
        );
        combine_dimensions(template, content)
    })
}

#[derive(Clone, Copy)]
struct SoaAtomInput<'a> {
    template_terms: &'a [u32],
    content_terms: &'a [u32],
}

fn build_id_sketches_sequential(atoms: &[SoaAtomInput<'_>]) -> Vec<AtomSketch> {
    let template = build_id_dimension(atoms, |atom| atom.template_terms);
    let content = build_id_dimension(atoms, |atom| atom.content_terms);
    combine_dimensions(template, content)
}

fn build_id_dimension<'a>(
    atoms: &'a [SoaAtomInput<'a>],
    terms_of: impl Fn(&SoaAtomInput<'a>) -> &'a [u32],
) -> Vec<DimensionSketch> {
    let mut document_frequencies = BTreeMap::<u32, usize>::new();
    for atom in atoms {
        visit_unique_term_ids(terms_of(atom), |term| {
            *document_frequencies.entry(term).or_default() += 1;
        });
    }
    let document_frequencies = DocumentFrequencies::Sparse(document_frequencies);
    atoms
        .iter()
        .map(|atom| dimension_sketch_ids(atoms.len(), terms_of(atom), &document_frequencies))
        .collect()
}

fn build_id_dimension_parallel<'a>(
    atoms: &'a [SoaAtomInput<'a>],
    terms_of: impl Fn(&SoaAtomInput<'a>) -> &'a [u32] + Sync,
) -> Vec<DimensionSketch> {
    let document_frequencies = build_id_document_frequencies_parallel(atoms, &terms_of);
    atoms
        .par_iter()
        .map(|atom| dimension_sketch_ids(atoms.len(), terms_of(atom), &document_frequencies))
        .collect()
}

fn build_id_document_frequencies_parallel<'a>(
    atoms: &'a [SoaAtomInput<'a>],
    terms_of: impl Fn(&SoaAtomInput<'a>) -> &'a [u32] + Sync,
) -> DocumentFrequencies {
    let mut maximum = None::<u32>;
    let mut occurrences = 0usize;
    for atom in atoms {
        let terms = terms_of(atom);
        occurrences = occurrences.saturating_add(terms.len());
        if let Some(&term) = terms.iter().max() {
            maximum = Some(maximum.map_or(term, |current| current.max(term)));
        }
    }
    let dense_len = maximum
        .and_then(|term| usize::try_from(term).ok())
        .and_then(|term| term.checked_add(1))
        .unwrap_or(0);
    if dense_len <= occurrences.saturating_mul(4).max(1) {
        let frequencies = (0..dense_len)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>();
        atoms.par_iter().for_each(|atom| {
            visit_unique_term_ids(terms_of(atom), |term| {
                frequencies[term as usize].fetch_add(1, Ordering::Relaxed);
            });
        });
        return DocumentFrequencies::Dense(
            frequencies
                .into_iter()
                .map(AtomicUsize::into_inner)
                .collect(),
        );
    }
    let mut occurrences = atoms
        .par_iter()
        .fold(Vec::<u32>::new, |mut terms, atom| {
            visit_unique_term_ids(terms_of(atom), |term| terms.push(term));
            terms
        })
        .reduce(Vec::<u32>::new, |mut left, mut right| {
            left.append(&mut right);
            left
        });
    occurrences.par_sort_unstable();
    let mut terms = Vec::new();
    let mut counts = Vec::new();
    for term in occurrences {
        if terms.last().copied() == Some(term) {
            *counts.last_mut().expect("count accompanies sparse term") += 1;
        } else {
            terms.push(term);
            counts.push(1usize);
        }
    }
    DocumentFrequencies::SparseSorted { terms, counts }
}

fn build_sketches_sequential(atoms: &[BaseEquivalentAtomInput<'_>]) -> Vec<AtomSketch> {
    let template = build_dimension(atoms, |atom| atom.template_terms);
    let content = build_dimension(atoms, |atom| atom.content_terms);
    combine_dimensions(template, content)
}

fn combine_dimensions(
    template: Vec<DimensionSketch>,
    content: Vec<DimensionSketch>,
) -> Vec<AtomSketch> {
    template
        .into_iter()
        .zip(content)
        .map(|(template, content)| AtomSketch {
            template_simhash: template.simhash,
            content_simhash: content.simhash,
            template_anchors: template.anchors,
            content_anchors: content.anchors,
            has_template_terms: template.has_terms,
            has_content_terms: content.has_terms,
        })
        .collect()
}

fn build_dimension<'a>(
    atoms: &'a [BaseEquivalentAtomInput<'a>],
    terms_of: impl Fn(&BaseEquivalentAtomInput<'a>) -> &'a [(u32, u32)],
) -> Vec<DimensionSketch> {
    let mut document_frequencies = BTreeMap::<u32, usize>::new();
    for atom in atoms {
        visit_unique_terms(terms_of(atom), |term| {
            *document_frequencies.entry(term).or_default() += 1;
        });
    }

    let document_frequencies = DocumentFrequencies::Sparse(document_frequencies);
    atoms
        .iter()
        .map(|atom| dimension_sketch(atoms.len(), terms_of(atom), &document_frequencies))
        .collect()
}

fn build_dimension_parallel<'a>(
    atoms: &'a [BaseEquivalentAtomInput<'a>],
    terms_of: impl Fn(&BaseEquivalentAtomInput<'a>) -> &'a [(u32, u32)] + Sync,
) -> Vec<DimensionSketch> {
    let document_frequencies = build_document_frequencies_parallel(atoms, &terms_of);
    atoms
        .par_iter()
        .map(|atom| dimension_sketch(atoms.len(), terms_of(atom), &document_frequencies))
        .collect()
}

fn build_document_frequencies_parallel<'a>(
    atoms: &'a [BaseEquivalentAtomInput<'a>],
    terms_of: impl Fn(&BaseEquivalentAtomInput<'a>) -> &'a [(u32, u32)] + Sync,
) -> DocumentFrequencies {
    let mut maximum = None::<u32>;
    let mut occurrences = 0usize;
    for atom in atoms {
        let terms = terms_of(atom);
        occurrences = occurrences.saturating_add(terms.len());
        if let Some(&(term, _)) = terms.iter().max_by_key(|(term, _)| *term) {
            maximum = Some(maximum.map_or(term, |current| current.max(term)));
        }
    }
    let dense_len = maximum
        .and_then(|term| usize::try_from(term).ok())
        .and_then(|term| term.checked_add(1))
        .unwrap_or(0);
    if dense_len <= occurrences.saturating_mul(4).max(1) {
        let frequencies = (0..dense_len)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>();
        atoms.par_iter().for_each(|atom| {
            visit_unique_terms(terms_of(atom), |term| {
                frequencies[term as usize].fetch_add(1, Ordering::Relaxed);
            });
        });
        return DocumentFrequencies::Dense(
            frequencies
                .into_iter()
                .map(AtomicUsize::into_inner)
                .collect(),
        );
    }
    let mut occurrences = atoms
        .par_iter()
        .fold(Vec::<u32>::new, |mut terms, atom| {
            visit_unique_terms(terms_of(atom), |term| terms.push(term));
            terms
        })
        .reduce(Vec::<u32>::new, |mut left, mut right| {
            left.append(&mut right);
            left
        });
    occurrences.par_sort_unstable();
    let mut terms = Vec::new();
    let mut counts = Vec::new();
    for term in occurrences {
        if terms.last().copied() == Some(term) {
            *counts.last_mut().expect("count accompanies sparse term") += 1;
        } else {
            terms.push(term);
            counts.push(1usize);
        }
    }
    DocumentFrequencies::SparseSorted { terms, counts }
}

fn dimension_sketch(
    atom_count: usize,
    terms: &[(u32, u32)],
    document_frequencies: &DocumentFrequencies,
) -> DimensionSketch {
    let total_documents = atom_count.max(1) as f64;
    let mut weights = [0.0f64; 64];
    let mut ranked_anchors = Vec::new();
    let mut has_terms = false;
    visit_unique_terms(terms, |term| {
        has_terms = true;
        let document_frequency = document_frequencies.get(term);
        let idf = ((total_documents + 1.0) / (document_frequency as f64 + 0.5)).ln();
        let hash = stable_token_hash(term);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if (hash >> bit) & 1 == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        let high_frequency = atom_count >= HIGH_FREQUENCY_MIN_DOCS
            && document_frequency.saturating_mul(HIGH_FREQUENCY_DIVISOR) > atom_count;
        if !high_frequency {
            ranked_anchors.push((document_frequency, term));
        }
    });
    ranked_anchors.sort_unstable();
    ranked_anchors.truncate(ANCHOR_COUNT);
    let mut anchors: Vec<u32> = ranked_anchors.into_iter().map(|(_, term)| term).collect();
    anchors.sort_unstable();
    let simhash = weights
        .into_iter()
        .enumerate()
        .fold(0u64, |hash, (bit, weight)| {
            hash | (u64::from(weight >= 0.0) << bit)
        });
    DimensionSketch {
        simhash,
        anchors,
        has_terms,
    }
}

fn dimension_sketch_ids(
    atom_count: usize,
    terms: &[u32],
    document_frequencies: &DocumentFrequencies,
) -> DimensionSketch {
    let total_documents = atom_count.max(1) as f64;
    let mut weights = [0.0f64; 64];
    let mut ranked_anchors = Vec::new();
    let mut has_terms = false;
    visit_unique_term_ids(terms, |term| {
        has_terms = true;
        let document_frequency = document_frequencies.get(term);
        let idf = ((total_documents + 1.0) / (document_frequency as f64 + 0.5)).ln();
        let hash = stable_token_hash(term);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if (hash >> bit) & 1 == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        let high_frequency = atom_count >= HIGH_FREQUENCY_MIN_DOCS
            && document_frequency.saturating_mul(HIGH_FREQUENCY_DIVISOR) > atom_count;
        if !high_frequency {
            ranked_anchors.push((document_frequency, term));
        }
    });
    ranked_anchors.sort_unstable();
    ranked_anchors.truncate(ANCHOR_COUNT);
    let mut anchors: Vec<u32> = ranked_anchors.into_iter().map(|(_, term)| term).collect();
    anchors.sort_unstable();
    let simhash = weights
        .into_iter()
        .enumerate()
        .fold(0u64, |hash, (bit, weight)| {
            hash | (u64::from(weight >= 0.0) << bit)
        });
    DimensionSketch {
        simhash,
        anchors,
        has_terms,
    }
}

fn visit_unique_terms(terms: &[(u32, u32)], mut visit: impl FnMut(u32)) {
    if terms.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        for &(term, _) in terms {
            visit(term);
        }
        return;
    }
    let mut unique = terms.iter().map(|(term, _)| *term).collect::<Vec<_>>();
    unique.sort_unstable();
    unique.dedup();
    for term in unique {
        visit(term);
    }
}

fn visit_unique_term_ids(terms: &[u32], mut visit: impl FnMut(u32)) {
    if terms.windows(2).all(|pair| pair[0] < pair[1]) {
        for &term in terms {
            visit(term);
        }
        return;
    }
    let mut unique = terms.to_vec();
    unique.sort_unstable();
    unique.dedup();
    for term in unique {
        visit(term);
    }
}

fn stable_token_hash(term: u32) -> u64 {
    let mut value = u64::from(term).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_df_uses_one_dense_table_for_dense_term_ids() {
        let first = vec![(0, 1), (2, 1)];
        let second = vec![(1, 1), (2, 1)];
        let atoms = vec![
            BaseEquivalentAtomInput {
                template_terms: &first,
                content_terms: &[],
            },
            BaseEquivalentAtomInput {
                template_terms: &second,
                content_terms: &[],
            },
        ];

        let frequencies = build_document_frequencies_parallel(&atoms, |atom| atom.template_terms);

        assert!(matches!(frequencies, DocumentFrequencies::Dense(_)));
        assert_eq!(frequencies.get(0), 1);
        assert_eq!(frequencies.get(1), 1);
        assert_eq!(frequencies.get(2), 2);
    }

    #[test]
    fn parallel_df_uses_one_sorted_occurrence_table_for_sparse_term_ids() {
        let first = vec![(1, 1), (1_000_000, 1)];
        let second = vec![(1_000_000, 1), (4_000_000, 1)];
        let atoms = vec![
            BaseEquivalentAtomInput {
                template_terms: &first,
                content_terms: &[],
            },
            BaseEquivalentAtomInput {
                template_terms: &second,
                content_terms: &[],
            },
        ];

        let frequencies = build_document_frequencies_parallel(&atoms, |atom| atom.template_terms);

        assert!(matches!(
            frequencies,
            DocumentFrequencies::SparseSorted { .. }
        ));
        assert_eq!(frequencies.get(1), 1);
        assert_eq!(frequencies.get(1_000_000), 2);
        assert_eq!(frequencies.get(4_000_000), 1);
    }

    #[test]
    fn soa_sketch_builder_matches_pair_materializing_builder() {
        let payloads = PayloadTermSoA::from_term_lists_owned(vec![
            (vec![(0, 2), (2, 1)], vec![(7, 3)]),
            (vec![(1, 1), (2, 9)], vec![(8, 1), (9, 2)]),
            (vec![(4, 1)], vec![]),
        ])
        .unwrap();
        let atom_payloads = [2u32, 0, 1];
        let materialized = atom_payloads
            .iter()
            .map(|&payload| {
                (
                    payloads.materialize_template_pairs(payload as usize),
                    payloads.materialize_content_pairs(payload as usize),
                )
            })
            .collect::<Vec<_>>();
        let inputs = materialized
            .iter()
            .map(|(template_terms, content_terms)| BaseEquivalentAtomInput {
                template_terms,
                content_terms,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            build_base_equivalent_atom_sketches_parallel(&inputs, 2),
            build_base_equivalent_atom_sketches_from_soa_parallel(&payloads, &atom_payloads, 2)
        );
    }
}
