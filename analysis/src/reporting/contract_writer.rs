use crate::error::{AnalysisError, Result};
use crate::model::{CandidateId, ContractKey};
use crate::reporting::{ArtifactRef, ContractArtifact};
use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct ContractWriter {
    run_dir: PathBuf,
    budget: Arc<ByteBudget>,
    next_staging_id: AtomicU64,
}

pub struct PreparedPayload {
    temporary: Option<PathBuf>,
    checksum: String,
    compressed_bytes: u64,
}

pub struct ReservedPayload {
    prepared: PreparedPayload,
    _permit: BytePermit,
}

pub enum PayloadAdmission {
    Admitted(ReservedPayload),
    Pending(PreparedPayload),
}

impl PreparedPayload {
    pub const fn compressed_bytes(&self) -> u64 {
        self.compressed_bytes
    }

    fn into_parts(mut self) -> (PathBuf, String, u64) {
        (
            self.temporary
                .take()
                .expect("prepared payload owns its staging file"),
            std::mem::take(&mut self.checksum),
            self.compressed_bytes,
        )
    }
}

impl Drop for PreparedPayload {
    fn drop(&mut self) {
        if let Some(path) = self.temporary.take() {
            let _ = fs::remove_file(path);
        }
    }
}

impl ContractWriter {
    pub fn create(output_dir: &Path, run_id: &str, queue_limit: u64) -> Result<Self> {
        fs::create_dir_all(output_dir)?;
        let run_dir = output_dir.join(run_id);
        fs::create_dir(&run_dir).map_err(|error| AnalysisError::Artifact {
            path: run_dir.clone(),
            message: format!("exclusive run directory creation failed: {error}"),
        })?;
        fs::create_dir(run_dir.join("contracts"))?;
        fs::create_dir(run_dir.join("seeds"))?;
        fs::create_dir(run_dir.join(".staging"))?;
        Ok(Self {
            run_dir,
            budget: Arc::new(ByteBudget::new(queue_limit)),
            next_staging_id: AtomicU64::new(0),
        })
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn try_reserve(&self, reserved_bytes: u64) -> Result<Option<BytePermit>> {
        if reserved_bytes > self.budget.limit {
            return Err(AnalysisError::MemoryBudget {
                required: reserved_bytes,
                limit: self.budget.limit,
            });
        }
        Ok(self.budget.try_acquire(reserved_bytes))
    }

    /// Serialize and compress exactly once into disk-backed staging. The
    /// bounded compression worker pool limits open staging files; queued bytes
    /// enter the writer budget only after their exact compressed size is known.
    pub fn serialize_contract(&self, artifact: &ContractArtifact<'_>) -> Result<PreparedPayload> {
        self.serialize_to_staging(artifact)
    }

    fn serialize_to_staging<T: Serialize + ?Sized>(&self, value: &T) -> Result<PreparedPayload> {
        let (temporary, file) = self.create_staging_file()?;
        let result = (|| {
            let mut encoder = zstd::Encoder::new(HashingFileWriter::new(file), 1)?;
            serde_json::to_writer(&mut encoder, value)?;
            let (compressed_bytes, checksum) = encoder.finish()?.finish()?;
            Ok(PreparedPayload {
                temporary: Some(temporary.clone()),
                checksum,
                compressed_bytes,
            })
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn create_staging_file(&self) -> Result<(PathBuf, std::fs::File)> {
        loop {
            let id = self.next_staging_id.fetch_add(1, Ordering::Relaxed);
            let path = self.run_dir.join(".staging").join(format!(
                "payload.{}.{}.json.zst.tmp",
                std::process::id(),
                id
            ));
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => return Ok((path, file)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub fn try_admit(&self, payload: PreparedPayload) -> Result<PayloadAdmission> {
        let compressed_bytes = payload.compressed_bytes;
        if compressed_bytes > self.budget.limit {
            return Err(AnalysisError::MemoryBudget {
                required: compressed_bytes,
                limit: self.budget.limit,
            });
        }
        Ok(match self.budget.try_acquire(compressed_bytes) {
            Some(permit) => PayloadAdmission::Admitted(ReservedPayload {
                prepared: payload,
                _permit: permit,
            }),
            None => PayloadAdmission::Pending(payload),
        })
    }

    pub fn write(
        &self,
        candidate_id: CandidateId,
        contract: ContractKey,
        payload: ReservedPayload,
        analysis_status: &str,
        lightweight_summary: serde_json::Value,
    ) -> Result<ArtifactRef> {
        let ReservedPayload { prepared, _permit } = payload;
        let (temporary, checksum, compressed_bytes) = prepared.into_parts();
        let mut last_error = None;
        for attempt in 0..3_u8 {
            match self.write_attempt(&contract, &temporary) {
                Ok(artifact_path) => {
                    return Ok(ArtifactRef {
                        candidate_id,
                        contract,
                        artifact_path,
                        checksum,
                        compressed_bytes,
                        analysis_status: analysis_status.to_owned(),
                        lightweight_summary,
                    });
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 2 {
                        std::thread::sleep(std::time::Duration::from_millis(25_u64 << attempt));
                    }
                }
            }
        }
        let _ = fs::remove_file(&temporary);
        Err(last_error.expect("artifact write must execute at least once"))
    }

    fn write_attempt(&self, contract: &ContractKey, temporary: &Path) -> Result<PathBuf> {
        let prefix = address_prefix(&contract.contract_address);
        let directory = self
            .run_dir
            .join("contracts")
            .join(contract.chain.as_str())
            .join(prefix);
        fs::create_dir_all(&directory)?;
        let filename = safe_filename(&contract.contract_address);
        let path = directory.join(format!("{filename}.json.zst"));
        fs::rename(temporary, &path)?;
        Ok(path
            .strip_prefix(&self.run_dir)
            .unwrap_or(&path)
            .to_path_buf())
    }

    pub fn finish_success(&self) -> Result<()> {
        // An empty staging directory proves every prepared payload reached its
        // final atomic rename. Keep this implementation detail out of the
        // completed public run layout.
        fs::remove_dir(self.run_dir.join(".staging"))?;
        sync_run_data(&self.run_dir)?;
        atomic_write(&self.run_dir.join("_SUCCESS"), b"")?;
        Ok(())
    }
}

struct HashingFileWriter {
    file: std::fs::File,
    digest: Sha256,
    bytes: u64,
}

impl HashingFileWriter {
    fn new(file: std::fs::File) -> Self {
        Self {
            file,
            digest: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(mut self) -> Result<(u64, String)> {
        self.file.flush()?;
        Ok((self.bytes, hex_digest(self.digest.finalize())))
    }
}

impl Write for HashingFileWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buffer)?;
        self.digest.update(&buffer[..written]);
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("compressed artifact size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

struct ByteBudget {
    limit: u64,
    state: Mutex<BudgetState>,
}

#[derive(Default)]
struct BudgetState {
    used: u64,
}

impl ByteBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            state: Mutex::new(BudgetState::default()),
        }
    }

    fn try_acquire(self: &Arc<Self>, bytes: u64) -> Option<BytePermit> {
        if bytes > self.limit {
            return None;
        }
        let mut state = self.state.lock();
        if state.used.saturating_add(bytes) > self.limit {
            return None;
        }
        state.used = state.used.saturating_add(bytes);
        Some(BytePermit {
            budget: self.clone(),
            bytes,
        })
    }
}

pub struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: u64,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.used -= self.bytes;
    }
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_stream(path, |file| {
        file.write_all(bytes)?;
        Ok(())
    })
}

/// Atomically replaces a run artifact without forcing it to stable storage.
/// `ContractWriter::finish_success` performs the single run-level durability
/// barrier before publishing `_SUCCESS`.
pub fn atomic_write_deferred(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_stream_deferred(path, |file| {
        file.write_all(bytes)?;
        Ok(())
    })
}

pub fn atomic_write_stream(
    path: &Path,
    operation: impl FnOnce(&mut std::fs::File) -> Result<()>,
) -> Result<()> {
    atomic_write_stream_inner(path, operation, true)
}

pub fn atomic_write_stream_deferred(
    path: &Path,
    operation: impl FnOnce(&mut std::fs::File) -> Result<()>,
) -> Result<()> {
    atomic_write_stream_inner(path, operation, false)
}

fn atomic_write_stream_inner(
    path: &Path,
    operation: impl FnOnce(&mut std::fs::File) -> Result<()>,
    durable: bool,
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| AnalysisError::Artifact {
        path: path.to_path_buf(),
        message: "artifact has no parent directory".into(),
    })?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("artifact"),
        std::process::id()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        operation(&mut file)?;
        if durable {
            file.sync_all()?;
        } else {
            file.flush()?;
        }
        drop(file);
        fs::rename(&temporary, path)?;
        if durable {
            sync_directory(parent)
        } else {
            Ok(())
        }
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn sync_run_data(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let directory = std::fs::File::open(path)?;
        let result = unsafe { libc::syncfs(directory.as_raw_fd()) };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        sync_tree(path)
    }
}

#[cfg(not(unix))]
fn sync_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            sync_tree(&entry.path())?;
        } else if file_type.is_file() {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(entry.path())?
                .sync_all()?;
        }
    }
    sync_directory(path)
}

fn address_prefix(address: &str) -> String {
    let digest = Sha256::digest(address.as_bytes());
    format!("{:02x}", digest[0])
}

fn safe_filename(address: &str) -> String {
    let mut value = address
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(48)
        .collect::<String>();
    if value.is_empty() {
        value.push('_');
    }
    let digest = Sha256::digest(address.as_bytes());
    value.push('-');
    for byte in &digest[..8] {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = digest.as_ref();
    let mut output = String::with_capacity(digest.len() * 2);
    for &byte in digest {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serializer;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn run_directory_is_exclusive_and_parent_is_created() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("nested").join("result");
        let writer = ContractWriter::create(&output, "run-1", 1024).unwrap();
        assert!(writer.run_dir().join("contracts").is_dir());
        assert!(ContractWriter::create(&output, "run-1", 1024).is_err());
    }

    #[test]
    fn byte_budget_backpressures_and_rejects_oversized_payloads() {
        let budget = Arc::new(ByteBudget::new(8));
        let first = budget.try_acquire(8).unwrap();
        assert!(budget.try_acquire(1).is_none());
        drop(first);
        assert!(budget.try_acquire(1).is_some());
        assert!(budget.try_acquire(16).is_none());

        let root = tempfile::tempdir().unwrap();
        let writer = ContractWriter::create(&root.path().join("result"), "run-1", 8).unwrap();
        assert!(matches!(
            writer.try_reserve(9),
            Err(AnalysisError::MemoryBudget {
                required: 9,
                limit: 8
            })
        ));
    }

    #[test]
    fn staging_serializes_once_and_cleans_up_unadmitted_payloads() {
        struct Counted<'a>(&'a AtomicUsize);

        impl Serialize for Counted<'_> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                serializer.serialize_str("payload")
            }
        }

        let root = tempfile::tempdir().unwrap();
        let writer = ContractWriter::create(&root.path().join("result"), "run-1", 1024).unwrap();
        let calls = AtomicUsize::new(0);
        let prepared = writer.serialize_to_staging(&Counted(&calls)).unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(prepared.compressed_bytes() > 0);
        let temporary = prepared.temporary.clone().unwrap();
        assert!(temporary.is_file());
        drop(prepared);
        assert!(!temporary.exists());
    }

    #[test]
    fn admitted_staging_payload_is_atomically_moved_and_budget_is_released() {
        let root = tempfile::tempdir().unwrap();
        let writer = ContractWriter::create(&root.path().join("result"), "run-1", 1024).unwrap();
        let prepared = writer.serialize_to_staging(&"payload").unwrap();
        let compressed_bytes = prepared.compressed_bytes();
        let payload = match writer.try_admit(prepared).unwrap() {
            PayloadAdmission::Admitted(payload) => payload,
            PayloadAdmission::Pending(_) => panic!("empty writer queue must admit payload"),
        };
        let artifact = writer
            .write(
                CandidateId(7),
                ContractKey::new(crate::model::ChainId::Base, "contract"),
                payload,
                "complete",
                serde_json::json!({}),
            )
            .unwrap();
        assert_eq!(artifact.compressed_bytes, compressed_bytes);
        assert!(writer.run_dir().join(&artifact.artifact_path).is_file());
        assert!(writer.try_reserve(1024).unwrap().is_some());
        writer.finish_success().unwrap();
        assert!(writer.run_dir().join("_SUCCESS").is_file());
    }

    #[test]
    fn hashed_paths_disperse_evm_addresses_and_prevent_sanitization_collisions() {
        let prefixes = (0..64)
            .map(|index| address_prefix(&format!("0x{index:040x}")))
            .collect::<std::collections::BTreeSet<_>>();
        assert!(prefixes.len() > 16);
        assert_ne!(safe_filename("a/b"), safe_filename("a_b"));
    }
}
