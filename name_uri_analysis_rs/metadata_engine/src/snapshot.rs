//! The only Match-facing entry point for Encode and Blocking artifacts.
//!
//! Opening a snapshot validates ready revisions, checksummed typed arrays and
//! cross-array invariants. Raw payload packs are deliberately absent.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::Deserialize;
use thiserror::Error;

use crate::blocking::BLOCKING_REVISION;
use crate::encode::{EncodeBundle, FeatureSoaError, FeatureView, ENCODE_SCHEMA_REVISION};
use crate::format::{self, FormatError, MappedU32Array, MappedU64Array, MappedU8Array};

#[derive(Debug, Deserialize)]
struct FeatureReady {
    schema_revision: u32,
    source_count: usize,
    payload_count: usize,
    #[serde(default)]
    artifact_run_id: Option<String>,
    chains: Vec<String>,
    chain_totals: Vec<ChainTotal>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct ChainTotal {
    pub name: String,
    pub contracts: i64,
    pub nfts: i64,
}

#[derive(Debug, Deserialize)]
struct BlockingReady {
    blocking_revision: u32,
    atom_count: usize,
    #[serde(default)]
    artifact_run_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("missing ready marker: {0}")]
    MissingReady(PathBuf),
    #[error("invalid ready marker {path}: {source}")]
    InvalidReady {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("{artifact} revision {got} != expected {expected}")]
    Revision {
        artifact: &'static str,
        got: u32,
        expected: u32,
    },
    #[error(transparent)]
    Feature(#[from] FeatureSoaError),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("snapshot invariant failed: {0}")]
    Invariant(String),
}

/// Checksummed blocking mmap view. No index or payload data is constructed.
pub struct BlockingView {
    pub primary_storage_shards: MappedU32Array,
    pub template_simhashes: MappedU64Array,
    pub content_simhashes: MappedU64Array,
    pub routing_statuses: MappedU8Array,
    pub atom_block_offsets: MappedU64Array,
    pub atom_block_ids: MappedU32Array,
    pub block_atom_offsets: MappedU64Array,
    pub block_atoms: MappedU32Array,
    pub block_kinds: MappedU32Array,
    pub block_keys: MappedU64Array,
}

const BLOCKING_ARRAY_FILES: &[&str] = &[
    "atom_primary_storage_shard.u32",
    "atom_template_simhash.u64",
    "atom_content_simhash.u64",
    "atom_routing_status.u8",
    "atom_block_offsets.u64",
    "atom_block_ids.u32",
    "block_atom_offsets.u64",
    "block_atoms.u32",
    "block_kinds.u32",
    "block_keys.u64",
];

impl BlockingView {
    fn open_with_progress(
        dir: &Path,
        atom_count: usize,
        progress: &mut impl FnMut(u64),
    ) -> Result<Self, SnapshotError> {
        macro_rules! map {
            ($function:ident, $name:literal) => {{
                let path = dir.join($name);
                let mapped = format::$function(&path)?;
                progress(std::fs::metadata(path)?.len());
                mapped
            }};
        }
        let this = Self {
            primary_storage_shards: map!(map_u32_array, "atom_primary_storage_shard.u32"),
            template_simhashes: map!(map_u64_array, "atom_template_simhash.u64"),
            content_simhashes: map!(map_u64_array, "atom_content_simhash.u64"),
            routing_statuses: map!(map_u8_array, "atom_routing_status.u8"),
            atom_block_offsets: map!(map_u64_array, "atom_block_offsets.u64"),
            atom_block_ids: map!(map_u32_array, "atom_block_ids.u32"),
            block_atom_offsets: map!(map_u64_array, "block_atom_offsets.u64"),
            block_atoms: map!(map_u32_array, "block_atoms.u32"),
            block_kinds: map!(map_u32_array, "block_kinds.u32"),
            block_keys: map!(map_u64_array, "block_keys.u64"),
        };
        this.validate(atom_count)?;
        Ok(this)
    }

    fn validate(&self, atom_count: usize) -> Result<(), SnapshotError> {
        for (name, len) in [
            ("primary_storage_shards", self.primary_storage_shards.len()),
            ("template_simhashes", self.template_simhashes.len()),
            ("content_simhashes", self.content_simhashes.len()),
            ("routing_statuses", self.routing_statuses.len()),
        ] {
            if len != atom_count {
                return Err(SnapshotError::Invariant(format!(
                    "{name} length {len} != atom_count {atom_count}"
                )));
            }
        }
        validate_csr(
            "atom_blocks",
            &self.atom_block_offsets,
            self.atom_block_ids.len(),
        )?;
        validate_csr(
            "block_atoms",
            &self.block_atom_offsets,
            self.block_atoms.len(),
        )?;
        if self.atom_block_offsets.len() != atom_count + 1 {
            return Err(SnapshotError::Invariant("atom_block_offsets length".into()));
        }
        let blocks = self.block_atom_offsets.len().saturating_sub(1);
        if self.block_kinds.len() != blocks || self.block_keys.len() != blocks {
            return Err(SnapshotError::Invariant("block descriptor count".into()));
        }
        if self.atom_block_ids.iter().any(|&id| id as usize >= blocks) {
            return Err(SnapshotError::Invariant(
                "atom references missing block".into(),
            ));
        }
        if self.block_atoms.iter().any(|&id| id as usize >= atom_count) {
            return Err(SnapshotError::Invariant(
                "block references missing atom".into(),
            ));
        }
        if self.routing_statuses.iter().any(|&status| status > 2) {
            return Err(SnapshotError::Invariant("unknown routing status".into()));
        }
        if self.block_kinds.iter().any(|&kind| kind > 2) {
            return Err(SnapshotError::Invariant("unknown block kind".into()));
        }
        for atom in 0..atom_count {
            let memberships = csr_row(&self.atom_block_offsets, &self.atom_block_ids, atom);
            if !strictly_increasing(memberships) {
                return Err(SnapshotError::Invariant(
                    "atom block ids not strictly sorted".into(),
                ));
            }
            if self.routing_statuses[atom] != 2 && memberships.is_empty() {
                return Err(SnapshotError::Invariant(format!(
                    "routable atom {atom} has no block membership"
                )));
            }
        }
        for block in 0..blocks {
            let members = csr_row(&self.block_atom_offsets, &self.block_atoms, block);
            if !strictly_increasing(members) {
                return Err(SnapshotError::Invariant(
                    "block atoms not strictly sorted".into(),
                ));
            }
        }
        let mut atom_membership_cursors = self.atom_block_offsets[..atom_count].to_vec();
        for block in 0..blocks {
            for &atom in csr_row(&self.block_atom_offsets, &self.block_atoms, block) {
                let atom = atom as usize;
                let cursor = atom_membership_cursors[atom] as usize;
                let end = self.atom_block_offsets[atom + 1] as usize;
                if cursor >= end || self.atom_block_ids[cursor] != block as u32 {
                    return Err(SnapshotError::Invariant(
                        "block membership directions disagree".into(),
                    ));
                }
                atom_membership_cursors[atom] += 1;
            }
        }
        if atom_membership_cursors
            .iter()
            .enumerate()
            .any(|(atom, &cursor)| cursor != self.atom_block_offsets[atom + 1])
        {
            return Err(SnapshotError::Invariant(
                "block membership directions disagree".into(),
            ));
        }
        Ok(())
    }
}

/// Validated immutable input for Index, RecallPlan, Match and Reduce.
pub struct MetadataSnapshot {
    encode: EncodeBundle,
    blocking: BlockingView,
    atom_count: usize,
    chain_names: Vec<String>,
    chain_totals: Vec<ChainTotal>,
    fingerprint: OnceLock<String>,
}

impl MetadataSnapshot {
    pub fn open(features_dir: &Path, blocking_dir: &Path) -> Result<Self, SnapshotError> {
        Self::open_with_progress(features_dir, blocking_dir, |_| {})
    }

    pub fn verification_bytes(
        features_dir: &Path,
        blocking_dir: &Path,
    ) -> Result<u64, SnapshotError> {
        crate::encode::feature_soa::FEATURE_ARRAY_FILES
            .iter()
            .map(|name| std::fs::metadata(features_dir.join(name)).map(|m| m.len()))
            .chain(
                BLOCKING_ARRAY_FILES
                    .iter()
                    .map(|name| std::fs::metadata(blocking_dir.join(name)).map(|m| m.len())),
            )
            .try_fold(0u64, |total, bytes| {
                total
                    .checked_add(bytes?)
                    .ok_or_else(|| std::io::Error::other("snapshot verification byte overflow"))
            })
            .map_err(SnapshotError::from)
    }

    pub fn open_with_progress(
        features_dir: &Path,
        blocking_dir: &Path,
        mut progress: impl FnMut(u64),
    ) -> Result<Self, SnapshotError> {
        let feature_ready: FeatureReady = read_ready(&features_dir.join("features.ready"))?;
        let blocking_ready: BlockingReady = read_ready(&blocking_dir.join("blocking.ready"))?;
        check_revision(
            "encode",
            feature_ready.schema_revision,
            ENCODE_SCHEMA_REVISION,
        )?;
        check_revision(
            "blocking",
            blocking_ready.blocking_revision,
            BLOCKING_REVISION,
        )?;
        if feature_ready.artifact_run_id != blocking_ready.artifact_run_id {
            return Err(SnapshotError::Invariant(
                "encode and blocking publish generation mismatch".into(),
            ));
        }

        let encode = EncodeBundle::open_with_progress(features_dir, &mut progress)?;
        let features = encode.feature_view();
        validate_feature_view(features, &feature_ready)?;
        let blocking = BlockingView::open_with_progress(
            blocking_dir,
            blocking_ready.atom_count,
            &mut progress,
        )?;
        if features.fallback_atom_offsets.len() != blocking_ready.atom_count + 1 {
            return Err(SnapshotError::Invariant(format!(
                "fallback atom count {} != blocking atom count {}",
                features.fallback_atom_offsets.len().saturating_sub(1),
                blocking_ready.atom_count
            )));
        }
        if features
            .fallback_atom_contracts
            .iter()
            .any(|&contract| contract as usize >= features.contract_source.len())
        {
            return Err(SnapshotError::Invariant(
                "fallback atom references missing contract".into(),
            ));
        }
        let mut seen_contracts = vec![false; features.contract_source.len()];
        for atom_id in 0..blocking_ready.atom_count {
            let begin = features.fallback_atom_offsets[atom_id] as usize;
            let end = features.fallback_atom_offsets[atom_id + 1] as usize;
            let members = &features.fallback_atom_contracts[begin..end];
            let Some((&representative, rest)) = members.split_first() else {
                return Err(SnapshotError::Invariant(format!(
                    "fallback atom {atom_id} has no contracts"
                )));
            };
            if !strictly_increasing(members) {
                return Err(SnapshotError::Invariant(format!(
                    "fallback atom {atom_id} contracts not strictly sorted"
                )));
            }
            let representative_payload = features.contract_payload[representative as usize];
            let representative_chain = features.contract_chain[representative as usize];
            for &contract in members {
                if std::mem::replace(&mut seen_contracts[contract as usize], true) {
                    return Err(SnapshotError::Invariant(format!(
                        "contract {contract} belongs to multiple fallback atoms"
                    )));
                }
            }
            for &contract in rest {
                if features.contract_chain[contract as usize] != representative_chain
                    || !payload_features_equal(
                        features,
                        features.contract_payload[contract as usize],
                        representative_payload,
                    )
                {
                    return Err(SnapshotError::Invariant(format!(
                        "fallback atom {atom_id} mixes chain or scoring-feature identity"
                    )));
                }
            }
        }
        if let Some(contract) = seen_contracts.iter().position(|seen| !seen) {
            return Err(SnapshotError::Invariant(format!(
                "contract {contract} is missing from fallback atoms"
            )));
        }
        Ok(Self {
            encode,
            blocking,
            atom_count: blocking_ready.atom_count,
            chain_names: feature_ready.chains,
            chain_totals: feature_ready.chain_totals,
            fingerprint: OnceLock::new(),
        })
    }

    pub fn features(&self) -> &FeatureView {
        self.encode.feature_view()
    }
    pub fn blocking(&self) -> &BlockingView {
        &self.blocking
    }
    pub fn atom_count(&self) -> usize {
        self.atom_count
    }
    pub fn contract_count(&self) -> usize {
        self.features().contract_source.len()
    }
    pub fn chain_names(&self) -> &[String] {
        &self.chain_names
    }
    pub fn chain_totals(&self) -> &[ChainTotal] {
        &self.chain_totals
    }
    pub(crate) fn cached_fingerprint(&self, compute: impl FnOnce() -> String) -> &str {
        self.fingerprint.get_or_init(compute)
    }
}

fn payload_features_equal(features: &crate::encode::FeatureView, left: u32, right: u32) -> bool {
    fn u32_slice<'a>(offsets: &[u64], values: &'a [u32], id: u32) -> &'a [u32] {
        &values[offsets[id as usize] as usize..offsets[id as usize + 1] as usize]
    }
    fn f64_slice<'a>(offsets: &[u64], values: &'a [f64], id: u32) -> &'a [f64] {
        &values[offsets[id as usize] as usize..offsets[id as usize + 1] as usize]
    }
    let left_index = left as usize;
    let right_index = right as usize;
    u32_slice(
        &features.payload_template_offsets,
        &features.payload_template_terms,
        left,
    ) == u32_slice(
        &features.payload_template_offsets,
        &features.payload_template_terms,
        right,
    ) && u32_slice(
        &features.payload_template_offsets,
        &features.payload_template_freqs,
        left,
    ) == u32_slice(
        &features.payload_template_offsets,
        &features.payload_template_freqs,
        right,
    ) && u32_slice(
        &features.payload_content_offsets,
        &features.payload_content_terms,
        left,
    ) == u32_slice(
        &features.payload_content_offsets,
        &features.payload_content_terms,
        right,
    ) && u32_slice(
        &features.payload_content_offsets,
        &features.payload_content_freqs,
        left,
    ) == u32_slice(
        &features.payload_content_offsets,
        &features.payload_content_freqs,
        right,
    ) && features.payload_lengths[left_index] == features.payload_lengths[right_index]
        && features.query_denominators[left_index].to_bits()
            == features.query_denominators[right_index].to_bits()
        && f64_slice(
            &features.prepared_weight_offsets,
            &features.prepared_weights,
            left,
        )
        .iter()
        .zip(f64_slice(
            &features.prepared_weight_offsets,
            &features.prepared_weights,
            right,
        ))
        .all(|(left, right)| left.to_bits() == right.to_bits())
        && (features.prepared_weight_offsets[left_index + 1]
            - features.prepared_weight_offsets[left_index])
            == (features.prepared_weight_offsets[right_index + 1]
                - features.prepared_weight_offsets[right_index])
}

fn read_ready<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, SnapshotError> {
    if !path.is_file() {
        return Err(SnapshotError::MissingReady(path.to_path_buf()));
    }
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|source| SnapshotError::InvalidReady {
        path: path.to_path_buf(),
        source,
    })
}

fn check_revision(artifact: &'static str, got: u32, expected: u32) -> Result<(), SnapshotError> {
    if got == expected {
        Ok(())
    } else {
        Err(SnapshotError::Revision {
            artifact,
            got,
            expected,
        })
    }
}

fn validate_csr(name: &str, offsets: &[u64], values_len: usize) -> Result<(), SnapshotError> {
    if offsets.first().copied() != Some(0) || offsets.last().copied() != Some(values_len as u64) {
        return Err(SnapshotError::Invariant(format!("{name} endpoints")));
    }
    if offsets.windows(2).any(|w| w[0] > w[1]) {
        return Err(SnapshotError::Invariant(format!(
            "{name} offsets not monotone"
        )));
    }
    Ok(())
}

fn csr_row<'a>(offsets: &[u64], values: &'a [u32], row: usize) -> &'a [u32] {
    &values[offsets[row] as usize..offsets[row + 1] as usize]
}

fn strictly_increasing(values: &[u32]) -> bool {
    values.windows(2).all(|window| window[0] < window[1])
}

fn validate_feature_view(view: &FeatureView, ready: &FeatureReady) -> Result<(), SnapshotError> {
    if view.source_to_payload.len() != ready.source_count {
        return Err(SnapshotError::Invariant("source_count".into()));
    }
    let payload_count = view.payload_template_offsets.len().saturating_sub(1);
    if payload_count != ready.payload_count
        || view.payload_content_offsets.len() != payload_count + 1
    {
        return Err(SnapshotError::Invariant("payload_count".into()));
    }
    validate_csr(
        "template",
        &view.payload_template_offsets,
        view.payload_template_terms.len(),
    )?;
    validate_csr(
        "content",
        &view.payload_content_offsets,
        view.payload_content_terms.len(),
    )?;
    validate_csr(
        "contract_tokens",
        &view.contract_token_offsets,
        view.contract_tokens.len(),
    )?;
    validate_csr(
        "token_members",
        &view.token_member_offsets,
        view.token_member_contracts.len(),
    )?;
    validate_csr(
        "fallback_atoms",
        &view.fallback_atom_offsets,
        view.fallback_atom_contracts.len(),
    )?;
    let contract_count = view.contract_source.len();
    if view.payload_template_terms.len() != view.payload_template_freqs.len()
        || view.payload_content_terms.len() != view.payload_content_freqs.len()
        || view.token_member_contracts.len() != view.token_member_sources.len()
        || view.payload_lengths.len() != payload_count
        || view.query_denominators.len() != payload_count
        || view.prepared_weight_offsets.len() != payload_count + 1
        || view.prepared_weight_offsets.first().copied() != Some(0)
        || view.prepared_weight_offsets.last().copied() != Some(view.prepared_weights.len() as u64)
        || view.prepared_weight_offsets.windows(2).any(|w| w[0] > w[1])
        || view.prepared_weights.len() != view.payload_template_terms.len()
        || view.payload_template_sigs.len()
            != payload_count * crate::cascade::PAYLOAD_TERM_SIG_BYTES
        || view.payload_content_sigs.len() != payload_count * crate::cascade::PAYLOAD_TERM_SIG_BYTES
        || view.contract_source.len() != view.contract_payload.len()
        || view.contract_source.len() != view.contract_chain.len()
        || view.contract_source.len() != view.contract_weight.len()
    {
        return Err(SnapshotError::Invariant("parallel column length".into()));
    }
    if view.contract_token_offsets.len() != contract_count + 1 {
        return Err(SnapshotError::Invariant(format!(
            "contract_token_offsets length {} != contract_count + 1 {}",
            view.contract_token_offsets.len(),
            contract_count + 1
        )));
    }
    if view
        .source_to_payload
        .iter()
        .any(|&payload| payload as usize >= payload_count)
        || view
            .contract_payload
            .iter()
            .any(|&payload| payload as usize >= payload_count)
        || view
            .contract_source
            .iter()
            .any(|&source| source as usize >= ready.source_count)
    {
        return Err(SnapshotError::Invariant(
            "source/contract identity references missing row".into(),
        ));
    }
    for contract in 0..contract_count {
        let source = view.contract_source[contract] as usize;
        if view.source_to_payload[source] != view.contract_payload[contract] {
            return Err(SnapshotError::Invariant(
                "contract source/payload identity mismatch".into(),
            ));
        }
    }

    for payload in 0..payload_count {
        let template_terms = csr_row(
            &view.payload_template_offsets,
            &view.payload_template_terms,
            payload,
        );
        let template_freqs = csr_row(
            &view.payload_template_offsets,
            &view.payload_template_freqs,
            payload,
        );
        let content_terms = csr_row(
            &view.payload_content_offsets,
            &view.payload_content_terms,
            payload,
        );
        let content_freqs = csr_row(
            &view.payload_content_offsets,
            &view.payload_content_freqs,
            payload,
        );
        if !strictly_increasing(template_terms) {
            return Err(SnapshotError::Invariant(
                "payload template terms not strictly sorted".into(),
            ));
        }
        if !strictly_increasing(content_terms) {
            return Err(SnapshotError::Invariant(
                "payload content terms not strictly sorted".into(),
            ));
        }
        if template_freqs
            .iter()
            .chain(content_freqs)
            .any(|&freq| freq == 0)
        {
            return Err(SnapshotError::Invariant(
                "payload term frequency must be positive".into(),
            ));
        }
        let content_length = content_freqs
            .iter()
            .try_fold(0u32, |total, &frequency| total.checked_add(frequency));
        if content_length != Some(view.payload_lengths[payload]) {
            return Err(SnapshotError::Invariant(
                "payload content length mismatch".into(),
            ));
        }
        let prepared_begin = view.prepared_weight_offsets[payload] as usize;
        let prepared_end = view.prepared_weight_offsets[payload + 1] as usize;
        let prepared = &view.prepared_weights[prepared_begin..prepared_end];
        if prepared.len() != template_terms.len() {
            return Err(SnapshotError::Invariant(
                "prepared template weight cardinality mismatch".into(),
            ));
        }
        if prepared
            .iter()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err(SnapshotError::Invariant(
                "invalid prepared template weight".into(),
            ));
        }
        let denominator = view.query_denominators[payload];
        if !denominator.is_finite() || denominator <= 0.0 {
            return Err(SnapshotError::Invariant("invalid query denominator".into()));
        }
    }

    let token_count = view.token_member_offsets.len().saturating_sub(1);
    if view
        .token_member_contracts
        .iter()
        .any(|&contract| contract as usize >= contract_count)
    {
        return Err(SnapshotError::Invariant(
            "token member references missing contract".into(),
        ));
    }
    if view
        .token_member_sources
        .iter()
        .any(|&source| source as usize >= ready.source_count)
    {
        return Err(SnapshotError::Invariant(
            "token member references missing source".into(),
        ));
    }
    let mut source_contracts = vec![u32::MAX; ready.source_count];
    for (&contract, &source) in view
        .token_member_contracts
        .iter()
        .zip(view.token_member_sources.iter())
    {
        let owner = &mut source_contracts[source as usize];
        if *owner == u32::MAX {
            *owner = contract;
        } else if *owner != contract {
            return Err(SnapshotError::Invariant(
                "source identity belongs to multiple contracts".into(),
            ));
        }
    }
    for (contract, &source) in view.contract_source.iter().enumerate() {
        let owner = source_contracts[source as usize];
        if owner != u32::MAX && owner != contract as u32 {
            return Err(SnapshotError::Invariant(
                "contract representative source identity mismatch".into(),
            ));
        }
    }
    drop(source_contracts);
    for contract in 0..contract_count {
        let tokens = csr_row(
            &view.contract_token_offsets,
            &view.contract_tokens,
            contract,
        );
        if !strictly_increasing(tokens) {
            return Err(SnapshotError::Invariant(
                "contract tokens not strictly sorted".into(),
            ));
        }
    }
    for token in 0..token_count {
        let begin = view.token_member_offsets[token] as usize;
        let end = view.token_member_offsets[token + 1] as usize;
        if (begin + 1..end).any(|member| {
            (
                view.token_member_contracts[member - 1],
                view.token_member_sources[member - 1],
            ) >= (
                view.token_member_contracts[member],
                view.token_member_sources[member],
            )
        }) {
            return Err(SnapshotError::Invariant(
                "token members not strictly sorted".into(),
            ));
        }
    }
    let mut contract_token_cursors = view.contract_token_offsets[..contract_count].to_vec();
    for token in 0..token_count {
        let mut previous_contract = None;
        for &contract in csr_row(
            &view.token_member_offsets,
            &view.token_member_contracts,
            token,
        ) {
            if previous_contract == Some(contract) {
                continue;
            }
            previous_contract = Some(contract);
            let contract = contract as usize;
            let cursor = contract_token_cursors[contract] as usize;
            let end = view.contract_token_offsets[contract + 1] as usize;
            if cursor >= end || view.contract_tokens[cursor] != token as u32 {
                return Err(SnapshotError::Invariant(
                    "token membership directions disagree".into(),
                ));
            }
            contract_token_cursors[contract] += 1;
        }
    }
    if contract_token_cursors
        .iter()
        .enumerate()
        .any(|(contract, &cursor)| cursor != view.contract_token_offsets[contract + 1])
    {
        return Err(SnapshotError::Invariant(
            "token membership directions disagree".into(),
        ));
    }

    let unique_chain_count = ready
        .chains
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if ready.chains.iter().any(|chain| chain.is_empty()) || unique_chain_count != ready.chains.len()
    {
        return Err(SnapshotError::Invariant(
            "chain identities are empty or duplicated".into(),
        ));
    }
    if ready.chain_totals.len() != ready.chains.len()
        || ready
            .chain_totals
            .iter()
            .zip(&ready.chains)
            .any(|(total, name)| total.name != *name)
    {
        return Err(SnapshotError::Invariant(
            "chain totals do not align with chain ids".into(),
        ));
    }
    if ready
        .chain_totals
        .iter()
        .any(|total| total.contracts < 0 || total.nfts < 0)
    {
        return Err(SnapshotError::Invariant(
            "chain totals must be non-negative".into(),
        ));
    }
    if view
        .contract_chain
        .iter()
        .any(|&chain| chain as usize >= ready.chains.len())
    {
        return Err(SnapshotError::Invariant(
            "contract references missing chain".into(),
        ));
    }
    Ok(())
}
