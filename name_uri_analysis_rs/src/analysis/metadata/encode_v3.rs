//! Bounded external stores used by the Encode v3 adapter.
//!
//! Files are transient and never constitute an Encode ready checkpoint. All
//! indexes are dense fixed-width files so cardinality increases disk usage,
//! not Rust heap usage.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const IO_BUFFER_BYTES: usize = 1024 * 1024;
const MEMBERSHIP_RECORD_BYTES: u64 = 8;
const SOURCE_RECORD_HEADER_BYTES: u64 = 16;

pub(super) const TOKEN_SOURCE_STORE_BUFFER_BYTES: u64 = 4 * IO_BUFFER_BYTES as u64;
pub(super) const TOKEN_SOURCE_STORE_IDENTITY_MAX_BYTES: u64 = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TokenSourceStorePlan {
    pub owner_identity: String,
    pub contract_count: u32,
    pub source_count: u32,
    pub membership_count: u64,
}

pub(super) fn planned_token_source_store_peak(
    contract_count: u32,
    source_count: u32,
    membership_count: u64,
    source_json_bytes: u64,
) -> std::io::Result<u64> {
    u64::from(source_count)
        .checked_mul(SOURCE_RECORD_HEADER_BYTES + 8)
        .and_then(|bytes| bytes.checked_add(source_json_bytes))
        .and_then(|bytes| {
            membership_count
                .checked_mul(MEMBERSHIP_RECORD_BYTES)?
                .checked_add(bytes)
        })
        .and_then(|bytes| {
            u64::from(contract_count)
                .checked_add(1)?
                .checked_mul(8)?
                .checked_add(bytes)
        })
        .and_then(|bytes| bytes.checked_add(TOKEN_SOURCE_STORE_BUFFER_BYTES))
        .and_then(|bytes| bytes.checked_add(TOKEN_SOURCE_STORE_IDENTITY_MAX_BYTES))
        .ok_or_else(|| invalid_data("token source store plan overflow"))
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenSourceStoreIdentity {
    revision: u32,
    plan: TokenSourceStorePlan,
    logical_bytes: u64,
    source_data_sha256: String,
    source_offsets_sha256: String,
    memberships_sha256: String,
    contract_offsets_sha256: String,
}

const TOKEN_SOURCE_STORE_REVISION: u32 = 1;
pub(super) const ENCODE_EXTERNAL_PLAN_REVISION: u32 = 1;

#[derive(Debug)]
pub(super) struct SourceDictionaryRow {
    pub source_id: u32,
    pub source_file: u32,
    pub source_row_number: u64,
    pub metadata_json: String,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TokenMembershipRow {
    pub contract_index: u32,
    pub token_id: u32,
    pub source_id: u32,
}

#[derive(Debug)]
pub(super) struct ExternalTokenSource {
    pub token_ids: Vec<u32>,
    pub source_file: u32,
    pub source_row_number: u64,
    pub metadata_json: String,
}

pub(super) struct ExternalTokenSourceStore {
    source_data: BufReader<File>,
    source_offsets: BufReader<File>,
    memberships: BufReader<File>,
    contract_offsets: BufReader<File>,
    contract_count: u32,
    logical_bytes: u64,
    paths: StorePaths,
}

impl ExternalTokenSourceStore {
    pub fn read_contract(
        &mut self,
        contract_index: u32,
    ) -> std::io::Result<Vec<ExternalTokenSource>> {
        if contract_index >= self.contract_count {
            return Ok(Vec::new());
        }
        self.contract_offsets
            .seek(SeekFrom::Start(u64::from(contract_index) * 8))?;
        let start = read_u64(&mut self.contract_offsets)?;
        let end = read_u64(&mut self.contract_offsets)?;
        if end < start || !(end - start).is_multiple_of(MEMBERSHIP_RECORD_BYTES) {
            return Err(invalid_data("corrupt token membership offsets"));
        }
        self.memberships.seek(SeekFrom::Start(start))?;
        let rows = (end - start) / MEMBERSHIP_RECORD_BYTES;
        let capacity = usize::try_from(rows)
            .map_err(|_| invalid_data("token membership count exceeds usize"))?;
        let mut output = Vec::new();
        let mut current_source_id = None;
        let mut token_ids = Vec::new();
        for _ in 0..rows {
            let token_id = read_u32(&mut self.memberships)?;
            let source_id = read_u32(&mut self.memberships)?;
            if current_source_id != Some(source_id) {
                if let Some(previous_source_id) = current_source_id {
                    output.push(
                        self.read_source(previous_source_id, std::mem::take(&mut token_ids))?,
                    );
                }
                current_source_id = Some(source_id);
            }
            token_ids.push(token_id);
        }
        if let Some(source_id) = current_source_id {
            output.push(self.read_source(source_id, token_ids)?);
        }
        debug_assert!(output.len() <= capacity);
        Ok(output)
    }

    fn read_source(
        &mut self,
        source_id: u32,
        token_ids: Vec<u32>,
    ) -> std::io::Result<ExternalTokenSource> {
        self.source_offsets
            .seek(SeekFrom::Start(u64::from(source_id) * 8))?;
        let offset = read_u64(&mut self.source_offsets)?;
        self.source_data.seek(SeekFrom::Start(offset))?;
        let source_file = read_u32(&mut self.source_data)?;
        let source_row_number = read_u64(&mut self.source_data)?;
        let json_len = read_u32(&mut self.source_data)? as usize;
        let mut json = vec![0u8; json_len];
        self.source_data.read_exact(&mut json)?;
        let metadata_json = String::from_utf8(json)
            .map_err(|_| invalid_data("token source dictionary contains invalid UTF-8"))?;
        Ok(ExternalTokenSource {
            token_ids,
            source_file,
            source_row_number,
            metadata_json,
        })
    }

    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    pub fn remove(self) -> std::io::Result<()> {
        let paths = self.paths.clone();
        drop(self);
        for path in paths.all() {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct StorePaths {
    source_data: PathBuf,
    source_offsets: PathBuf,
    memberships: PathBuf,
    contract_offsets: PathBuf,
    identity: PathBuf,
}

impl StorePaths {
    fn new(base: &Path) -> Self {
        let parent = base.parent().unwrap_or_else(|| Path::new("."));
        let stem = base
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("metadata-token-sources");
        Self {
            source_data: base.to_path_buf(),
            source_offsets: parent.join(format!("{stem}.source-offsets.u64")),
            memberships: parent.join(format!("{stem}.memberships.bin")),
            contract_offsets: parent.join(format!("{stem}.contract-offsets.u64")),
            identity: parent.join(format!("{stem}.identity.json")),
        }
    }

    fn data(&self) -> [&Path; 4] {
        [
            &self.source_data,
            &self.source_offsets,
            &self.memberships,
            &self.contract_offsets,
        ]
    }

    fn all(&self) -> [&Path; 5] {
        [
            &self.source_data,
            &self.source_offsets,
            &self.memberships,
            &self.contract_offsets,
            &self.identity,
        ]
    }

    fn partial(&self) -> Self {
        Self {
            source_data: partial_path(&self.source_data),
            source_offsets: partial_path(&self.source_offsets),
            memberships: partial_path(&self.memberships),
            contract_offsets: partial_path(&self.contract_offsets),
            identity: partial_path(&self.identity),
        }
    }

    fn cleanup(&self) {
        for path in self.all() {
            let _ = fs::remove_file(path);
        }
    }
}

struct GenerationCleanup {
    final_paths: StorePaths,
    partial_paths: StorePaths,
    active: bool,
}

impl GenerationCleanup {
    fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for GenerationCleanup {
    fn drop(&mut self) {
        if self.active {
            self.partial_paths.cleanup();
            self.final_paths.cleanup();
        }
    }
}

pub(super) fn write_external_token_source_store(
    base: &Path,
    plan: &TokenSourceStorePlan,
    sources: impl IntoIterator<Item = std::io::Result<SourceDictionaryRow>>,
    memberships: impl IntoIterator<Item = std::io::Result<TokenMembershipRow>>,
) -> std::io::Result<ExternalTokenSourceStore> {
    if plan.owner_identity.len() > 256 {
        return Err(invalid_data(
            "token source store owner identity is too long",
        ));
    }
    if let Some(parent) = base.parent() {
        fs::create_dir_all(parent)?;
    }
    let final_paths = StorePaths::new(base);
    if let Some(existing) = open_existing_store(&final_paths, plan)? {
        return Ok(existing);
    }
    let paths = final_paths.partial();
    final_paths.cleanup();
    paths.cleanup();
    let cleanup = GenerationCleanup {
        final_paths: final_paths.clone(),
        partial_paths: paths.clone(),
        active: true,
    };
    let mut source_data =
        BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&paths.source_data)?);
    let mut source_offsets =
        BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&paths.source_offsets)?);
    let mut expected_source_id = 0u32;
    let mut source_bytes = 0u64;
    for source in sources {
        let source = source?;
        if source.source_id != expected_source_id {
            return Err(invalid_data(
                "source dictionary IDs are not dense and ordered",
            ));
        }
        let json_len = u32::try_from(source.metadata_json.len())
            .map_err(|_| invalid_data("source dictionary JSON exceeds u32"))?;
        source_offsets.write_all(&source_bytes.to_le_bytes())?;
        source_data.write_all(&source.source_file.to_le_bytes())?;
        source_data.write_all(&source.source_row_number.to_le_bytes())?;
        source_data.write_all(&json_len.to_le_bytes())?;
        source_data.write_all(source.metadata_json.as_bytes())?;
        source_bytes = source_bytes
            .checked_add(SOURCE_RECORD_HEADER_BYTES)
            .and_then(|bytes| bytes.checked_add(u64::from(json_len)))
            .ok_or_else(|| invalid_data("source dictionary size overflow"))?;
        expected_source_id = expected_source_id
            .checked_add(1)
            .ok_or_else(|| invalid_data("source dictionary exceeds u32 IDs"))?;
    }
    source_data.flush()?;
    source_offsets.flush()?;
    source_data.get_ref().sync_all()?;
    source_offsets.get_ref().sync_all()?;
    drop(source_data);
    drop(source_offsets);
    if expected_source_id != plan.source_count {
        return Err(invalid_data("source dictionary count differs from plan"));
    }

    let mut membership_writer =
        BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&paths.memberships)?);
    let mut contract_offset_writer =
        BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&paths.contract_offsets)?);
    let mut next_contract_offset = 0u32;
    let mut previous_key = None;
    let mut membership_bytes = 0u64;
    let mut membership_count = 0u64;
    for membership in memberships {
        let membership = membership?;
        if membership.contract_index >= plan.contract_count {
            return Err(invalid_data("token membership contract is out of range"));
        }
        if membership.source_id >= expected_source_id {
            return Err(invalid_data("token membership source is out of range"));
        }
        let key = (
            membership.contract_index,
            membership.source_id,
            membership.token_id,
        );
        if previous_key.is_some_and(|previous| key <= previous) {
            return Err(invalid_data(
                "token memberships are not ordered by contract, source and token",
            ));
        }
        while next_contract_offset <= membership.contract_index {
            contract_offset_writer.write_all(&membership_bytes.to_le_bytes())?;
            next_contract_offset += 1;
        }
        membership_writer.write_all(&membership.token_id.to_le_bytes())?;
        membership_writer.write_all(&membership.source_id.to_le_bytes())?;
        membership_bytes = membership_bytes
            .checked_add(MEMBERSHIP_RECORD_BYTES)
            .ok_or_else(|| invalid_data("token membership size overflow"))?;
        membership_count = membership_count
            .checked_add(1)
            .ok_or_else(|| invalid_data("token membership count overflow"))?;
        previous_key = Some(key);
    }
    while next_contract_offset <= plan.contract_count {
        contract_offset_writer.write_all(&membership_bytes.to_le_bytes())?;
        next_contract_offset += 1;
    }
    membership_writer.flush()?;
    contract_offset_writer.flush()?;
    membership_writer.get_ref().sync_all()?;
    contract_offset_writer.get_ref().sync_all()?;
    drop(membership_writer);
    drop(contract_offset_writer);
    if membership_count != plan.membership_count {
        return Err(invalid_data("token membership count differs from plan"));
    }

    let logical_bytes = paths.data().into_iter().try_fold(0u64, |total, path| {
        total
            .checked_add(fs::metadata(path)?.len())
            .ok_or_else(|| invalid_data("token source store size overflow"))
    })?;
    let identity = TokenSourceStoreIdentity {
        revision: TOKEN_SOURCE_STORE_REVISION,
        plan: plan.clone(),
        logical_bytes,
        source_data_sha256: sha256_file(&paths.source_data)?,
        source_offsets_sha256: sha256_file(&paths.source_offsets)?,
        memberships_sha256: sha256_file(&paths.memberships)?,
        contract_offsets_sha256: sha256_file(&paths.contract_offsets)?,
    };
    let identity_bytes = serde_json::to_vec_pretty(&identity).map_err(std::io::Error::other)?;
    if identity_bytes.len() as u64 > TOKEN_SOURCE_STORE_IDENTITY_MAX_BYTES {
        return Err(invalid_data(
            "token source store identity exceeds reserved bytes",
        ));
    }
    {
        let mut identity_file = File::create(&paths.identity)?;
        identity_file.write_all(&identity_bytes)?;
        identity_file.sync_all()?;
    }
    for (partial, final_path) in paths.data().into_iter().zip(final_paths.data()) {
        fs::rename(partial, final_path)?;
    }
    fs::rename(&paths.identity, &final_paths.identity)?;
    let store = open_store(&final_paths, plan.contract_count, logical_bytes)?;
    cleanup.disarm();
    Ok(store)
}

fn open_existing_store(
    paths: &StorePaths,
    expected: &TokenSourceStorePlan,
) -> std::io::Result<Option<ExternalTokenSourceStore>> {
    if !paths.identity.is_file() {
        return Ok(None);
    }
    let identity: TokenSourceStoreIdentity =
        match serde_json::from_slice(&fs::read(&paths.identity)?) {
            Ok(identity) => identity,
            Err(_) => return Ok(None),
        };
    if identity.revision != TOKEN_SOURCE_STORE_REVISION || identity.plan != *expected {
        return Ok(None);
    }
    let actual_logical_bytes = match paths.data().into_iter().try_fold(0u64, |total, path| {
        total
            .checked_add(fs::metadata(path)?.len())
            .ok_or_else(|| invalid_data("existing token source store logical byte count overflow"))
    }) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    if actual_logical_bytes != identity.logical_bytes {
        return Ok(None);
    }
    let expected_source_offsets = u64::from(expected.source_count).saturating_mul(8);
    let expected_memberships = expected
        .membership_count
        .saturating_mul(MEMBERSHIP_RECORD_BYTES);
    let expected_contract_offsets = u64::from(expected.contract_count)
        .saturating_add(1)
        .saturating_mul(8);
    if fs::metadata(&paths.source_offsets).map(|m| m.len()).ok() != Some(expected_source_offsets)
        || fs::metadata(&paths.memberships).map(|m| m.len()).ok() != Some(expected_memberships)
        || fs::metadata(&paths.contract_offsets).map(|m| m.len()).ok()
            != Some(expected_contract_offsets)
        || sha256_file(&paths.source_data).ok().as_deref()
            != Some(identity.source_data_sha256.as_str())
        || sha256_file(&paths.source_offsets).ok().as_deref()
            != Some(identity.source_offsets_sha256.as_str())
        || sha256_file(&paths.memberships).ok().as_deref()
            != Some(identity.memberships_sha256.as_str())
        || sha256_file(&paths.contract_offsets).ok().as_deref()
            != Some(identity.contract_offsets_sha256.as_str())
    {
        return Ok(None);
    }
    Ok(Some(open_store(
        paths,
        expected.contract_count,
        identity.logical_bytes,
    )?))
}

fn open_store(
    paths: &StorePaths,
    contract_count: u32,
    logical_bytes: u64,
) -> std::io::Result<ExternalTokenSourceStore> {
    Ok(ExternalTokenSourceStore {
        source_data: BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&paths.source_data)?),
        source_offsets: BufReader::with_capacity(
            IO_BUFFER_BYTES,
            File::open(&paths.source_offsets)?,
        ),
        memberships: BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&paths.memberships)?),
        contract_offsets: BufReader::with_capacity(
            IO_BUFFER_BYTES,
            File::open(&paths.contract_offsets)?,
        ),
        contract_count,
        logical_bytes,
        paths: paths.clone(),
    })
}

fn partial_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".partial");
    PathBuf::from(name)
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut buffer = [0u8; 64 * 1024];
    let mut hasher = Sha256::new();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn read_u32(reader: &mut impl Read) -> std::io::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn invalid_data(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_row_plan(owner: &str) -> TokenSourceStorePlan {
        TokenSourceStorePlan {
            owner_identity: owner.into(),
            contract_count: 1,
            source_count: 1,
            membership_count: 1,
        }
    }

    fn one_source(json: &str) -> [std::io::Result<SourceDictionaryRow>; 1] {
        [Ok(SourceDictionaryRow {
            source_id: 0,
            source_file: 1,
            source_row_number: 2,
            metadata_json: json.to_owned(),
        })]
    }

    fn one_membership() -> [std::io::Result<TokenMembershipRow>; 1] {
        [Ok(TokenMembershipRow {
            contract_index: 0,
            token_id: 3,
            source_id: 0,
        })]
    }

    #[test]
    fn failed_store_build_never_leaves_final_generation_files() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("token-sources.bin");
        let sources = [
            Ok(SourceDictionaryRow {
                source_id: 0,
                source_file: 1,
                source_row_number: 2,
                metadata_json: "{}".to_owned(),
            }),
            Err(std::io::Error::other("injected source failure")),
        ];

        let error = match write_external_token_source_store(
            &base,
            &TokenSourceStorePlan {
                owner_identity: "failure-test".into(),
                contract_count: 1,
                source_count: 2,
                membership_count: 0,
            },
            sources,
            std::iter::empty::<std::io::Result<TokenMembershipRow>>(),
        ) {
            Ok(_) => panic!("injected failure unexpectedly published a store"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("injected source failure"));
        assert!(
            StorePaths::new(&base)
                .all()
                .into_iter()
                .all(|path| !path.exists()),
            "failed generations must remain unpublished"
        );
    }

    #[test]
    fn matching_identity_reuses_only_a_fully_verified_generation() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("token-sources.bin");
        let plan = one_row_plan("same-owner");
        drop(
            write_external_token_source_store(
                &base,
                &plan,
                one_source("{\"name\":\"first\"}"),
                one_membership(),
            )
            .unwrap(),
        );

        let failing_sources = [Err(std::io::Error::other("must not consume on reuse"))];
        let mut reopened = write_external_token_source_store(
            &base,
            &plan,
            failing_sources,
            std::iter::empty::<std::io::Result<TokenMembershipRow>>(),
        )
        .unwrap();

        assert_eq!(
            reopened.read_contract(0).unwrap()[0].metadata_json,
            "{\"name\":\"first\"}"
        );
    }

    #[test]
    fn mismatched_identity_rebuilds_instead_of_reusing_stale_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("token-sources.bin");
        drop(
            write_external_token_source_store(
                &base,
                &one_row_plan("old-owner"),
                one_source("{\"name\":\"old\"}"),
                one_membership(),
            )
            .unwrap(),
        );

        let mut rebuilt = write_external_token_source_store(
            &base,
            &one_row_plan("new-owner"),
            one_source("{\"name\":\"new\"}"),
            one_membership(),
        )
        .unwrap();

        assert_eq!(
            rebuilt.read_contract(0).unwrap()[0].metadata_json,
            "{\"name\":\"new\"}"
        );
    }

    #[test]
    fn duplicate_membership_key_is_rejected_without_publication() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("token-sources.bin");
        let membership = TokenMembershipRow {
            contract_index: 0,
            token_id: 3,
            source_id: 0,
        };
        let memberships = [Ok(membership), Ok(membership)];
        let mut plan = one_row_plan("duplicate-owner");
        plan.membership_count = 2;

        assert!(
            write_external_token_source_store(&base, &plan, one_source("{}"), memberships,)
                .is_err()
        );
        assert!(!StorePaths::new(&base).identity.exists());
    }

    #[test]
    fn exact_store_plan_counts_distinct_json_once() {
        let peak = planned_token_source_store_peak(1_000_000, 1, 1_000_000, 64 * 1024).unwrap();

        assert_eq!(
            peak,
            24 + 64 * 1024
                + 8 * 1_000_000
                + 8 * 1_000_001
                + TOKEN_SOURCE_STORE_BUFFER_BYTES
                + TOKEN_SOURCE_STORE_IDENTITY_MAX_BYTES
        );
    }
}
