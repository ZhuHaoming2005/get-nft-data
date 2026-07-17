//! StorageBroker: pin / rebuild / evict ledger and reservation accounting.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::storage::ledger::{ArtifactRecord, LedgerFile, ReservationRecord, StorageLedgerError};
use crate::storage::STORAGE_LEDGER_SCHEMA_REVISION;

/// Artifact category tracked by the storage ledger.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactClass {
    Feature,
    PayloadCas,
    Blocking,
    Index,
    ExactEvidence,
    RecallPlan,
    ConnectivityRun,
    ComponentSnapshot,
    Summary,
}

#[derive(Debug, Clone)]
pub struct ArtifactRegistration {
    pub path: PathBuf,
    pub class: ArtifactClass,
    pub logical_bytes: u64,
    pub partial_peak_bytes: u64,
    pub dependencies: Vec<String>,
}

impl ArtifactRegistration {
    pub fn new(
        path: PathBuf,
        class: ArtifactClass,
        logical_bytes: u64,
        partial_peak_bytes: u64,
        dependencies: Vec<String>,
    ) -> Self {
        Self {
            path,
            class,
            logical_bytes,
            partial_peak_bytes,
            dependencies,
        }
    }
}

/// Point-in-time view of committed storage accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageSnapshot {
    pub physical_free_bytes: u64,
    pub committed_bytes: u64,
    pub committed_partial_peak_bytes: u64,
    pub reclaimable_bytes: u64,
    pub pinned_bytes: u64,
    pub safety_reserve_bytes: u64,
}

/// Planned eviction set (not yet committed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionPlan {
    pub paths: Vec<PathBuf>,
}

const EVICTION_DIRECTORY: &str = ".storage-evictions";
const EVICTION_JOURNAL_REVISION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct EvictionJournal {
    revision: u32,
    ledger_key: String,
    original: PathBuf,
    tombstone: PathBuf,
    logical_bytes: u64,
}

struct BrokerInner {
    work_dir: PathBuf,
    ledger: LedgerFile,
    /// Optional deterministic accounting baseline used by storage-ledger tests.
    /// Production never probes filesystem free space.
    physical_free_override: Option<u64>,
    safety_reserve_bytes: u64,
}

/// Controller-scoped ledger for work-directory artifact lifecycle.
pub struct StorageBroker {
    inner: Arc<Mutex<BrokerInner>>,
}

/// RAII lease for a pin or a disk reservation. Releases on drop.
pub struct StorageLease {
    inner: Arc<Mutex<BrokerInner>>,
    kind: Option<LeaseKind>,
}

enum LeaseKind {
    Reserve { id: u64 },
    Pin { path: String, checkpoint: String },
}

impl StorageBroker {
    pub fn open(work_dir: &Path) -> Result<Self, StorageLedgerError> {
        Self::open_inner(work_dir, None)
    }

    /// Open with a fixed physical-free value for deterministic accounting tests.
    pub fn open_with_physical_free(
        work_dir: &Path,
        physical_free_bytes: u64,
    ) -> Result<Self, StorageLedgerError> {
        Self::open_inner(work_dir, Some(physical_free_bytes))
    }

    fn open_inner(
        work_dir: &Path,
        physical_free_override: Option<u64>,
    ) -> Result<Self, StorageLedgerError> {
        std::fs::create_dir_all(work_dir)?;
        let work_dir = work_dir.canonicalize()?;
        let mut ledger = LedgerFile::load_or_default(&work_dir, STORAGE_LEDGER_SCHEMA_REVISION)?;
        // Reservations are process-local admission leases. The controller never
        // overlaps heavy children, so any reservation found at broker open is
        // necessarily from a terminated process and must not poison recovery.
        let cleared_stale_reservations = !ledger.reservations.is_empty();
        ledger.reservations.clear();
        // The ledger tracks lifetimes, not speculative capacity. Actual writes
        // and fsyncs are the only disk-space authority.
        let safety_reserve_bytes = 0;
        let mut inner = BrokerInner {
            work_dir,
            ledger,
            physical_free_override,
            safety_reserve_bytes,
        };
        let recovered_evictions = recover_pending_evictions(&mut inner)?;
        if cleared_stale_reservations || recovered_evictions {
            persist(&mut inner)?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn register(
        &mut self,
        path: &Path,
        class: ArtifactClass,
        logical_bytes: u64,
        partial_peak_bytes: u64,
        dependencies: &[&str],
    ) -> Result<(), StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        // Validate at the trust boundary as well as immediately before
        // deletion.  Keep the caller's stable path spelling as the ledger key
        // so existing pins/dependencies remain addressable on Windows.
        contained_eviction_target(&guard.work_dir, path)?;
        let key = path_key(path);
        if let Some(existing) = guard.ledger.artifacts.get_mut(&key) {
            let dependencies: Vec<String> = dependencies.iter().map(|s| (*s).to_string()).collect();
            if existing.class != class || existing.dependencies != dependencies {
                return Err(StorageLedgerError::AlreadyRegistered(key));
            }
            // Recovery may replay registration after the artifact was durably
            // written but before its owner checkpoint was promoted. Refresh
            // measured bytes without losing pins/rebuild/eviction state.
            existing.logical_bytes = logical_bytes;
            existing.partial_peak_bytes = partial_peak_bytes;
            persist(&mut guard)?;
            return Ok(());
        }
        guard.ledger.artifacts.insert(
            key,
            ArtifactRecord {
                class,
                logical_bytes,
                partial_peak_bytes,
                dependencies: dependencies.iter().map(|s| (*s).to_string()).collect(),
                pins: Vec::new(),
                rebuildable: false,
                rebuild_from_checkpoint: None,
                evictable: false,
                evict_reason: None,
            },
        );
        persist(&mut guard)?;
        Ok(())
    }

    pub fn register_batch(
        &mut self,
        registrations: impl IntoIterator<Item = ArtifactRegistration>,
    ) -> Result<(), StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let previous = guard.ledger.clone();
        for registration in registrations {
            if let Err(error) = contained_eviction_target(&guard.work_dir, &registration.path) {
                guard.ledger = previous;
                return Err(error);
            }
            if let Err(error) = apply_registration(&mut guard.ledger, registration) {
                guard.ledger = previous;
                return Err(error);
            }
        }
        if let Err(error) = persist(&mut guard) {
            guard.ledger = previous;
            return Err(error);
        }
        Ok(())
    }

    pub fn pin(
        &mut self,
        path: &Path,
        owner_checkpoint: &str,
    ) -> Result<StorageLease, StorageLedgerError> {
        let key = path_key(path);
        {
            let mut guard = self.inner.lock().expect("storage broker lock");
            let artifact = guard
                .ledger
                .artifacts
                .get_mut(&key)
                .ok_or_else(|| StorageLedgerError::NotRegistered(key.clone()))?;
            if !artifact.pins.iter().any(|p| p == owner_checkpoint) {
                artifact.pins.push(owner_checkpoint.to_string());
            }
            persist(&mut guard)?;
        }
        Ok(StorageLease {
            inner: Arc::clone(&self.inner),
            kind: Some(LeaseKind::Pin {
                path: key,
                checkpoint: owner_checkpoint.to_string(),
            }),
        })
    }

    pub fn pin_batch(
        &mut self,
        paths: &[PathBuf],
        owner_checkpoint: &str,
    ) -> Result<Vec<StorageLease>, StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let previous = guard.ledger.clone();
        let mut keys = Vec::with_capacity(paths.len());
        for path in paths {
            let key = path_key(path);
            let artifact = match guard.ledger.artifacts.get_mut(&key) {
                Some(artifact) => artifact,
                None => {
                    guard.ledger = previous;
                    return Err(StorageLedgerError::NotRegistered(key));
                }
            };
            if !artifact.pins.iter().any(|pin| pin == owner_checkpoint) {
                artifact.pins.push(owner_checkpoint.to_string());
            }
            keys.push(key);
        }
        if let Err(error) = persist(&mut guard) {
            guard.ledger = previous;
            return Err(error);
        }
        drop(guard);
        Ok(keys
            .into_iter()
            .map(|path| StorageLease {
                inner: Arc::clone(&self.inner),
                kind: Some(LeaseKind::Pin {
                    path,
                    checkpoint: owner_checkpoint.to_string(),
                }),
            })
            .collect())
    }

    /// Release a durable checkpoint pin after its consumer checkpoint commits.
    pub fn release_pin(
        &mut self,
        path: &Path,
        owner_checkpoint: &str,
    ) -> Result<(), StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let key = path_key(path);
        let artifact = guard
            .ledger
            .artifacts
            .get_mut(&key)
            .ok_or_else(|| StorageLedgerError::NotRegistered(key.clone()))?;
        artifact.pins.retain(|pin| pin != owner_checkpoint);
        persist(&mut guard)
    }

    /// Release every artifact pin owned by an invalidated checkpoint.
    pub fn release_checkpoint_pins(
        &mut self,
        owner_checkpoint: &str,
    ) -> Result<usize, StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let mut released = 0usize;
        for artifact in guard.ledger.artifacts.values_mut() {
            let before = artifact.pins.len();
            artifact.pins.retain(|pin| pin != owner_checkpoint);
            released += before - artifact.pins.len();
        }
        persist(&mut guard)?;
        Ok(released)
    }

    /// Invalidate a rebuildable checkpoint and make its products
    /// reclaimable once no registered dependent remains.
    pub fn retire_checkpoint_artifacts(
        &mut self,
        owner_checkpoint: &str,
        reason: &str,
    ) -> Result<usize, StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let mut retired = 0usize;
        for artifact in guard.ledger.artifacts.values_mut() {
            if !artifact.pins.iter().any(|pin| pin == owner_checkpoint) {
                continue;
            }
            artifact.pins.retain(|pin| pin != owner_checkpoint);
            if matches!(
                artifact.class,
                ArtifactClass::Index
                    | ArtifactClass::ExactEvidence
                    | ArtifactClass::RecallPlan
                    | ArtifactClass::ConnectivityRun
                    | ArtifactClass::ComponentSnapshot
                    | ArtifactClass::Summary
            ) {
                artifact.evictable = true;
                artifact.evict_reason = Some(reason.to_string());
            }
            retired += 1;
        }
        persist(&mut guard)?;
        Ok(retired)
    }

    pub fn mark_evictable(&mut self, path: &Path, reason: &str) -> Result<(), StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let key = path_key(path);
        let artifact = guard
            .ledger
            .artifacts
            .get_mut(&key)
            .ok_or(StorageLedgerError::NotRegistered(key))?;
        artifact.evictable = true;
        artifact.evict_reason = Some(reason.to_string());
        persist(&mut guard)?;
        Ok(())
    }

    /// Payload CAS may only be reclaimed after Match declares independence.
    pub fn declare_match_independence(&mut self, path: &Path) -> Result<(), StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        let key = path_key(path);
        if !guard.ledger.artifacts.contains_key(&key) {
            return Err(StorageLedgerError::NotRegistered(key));
        }
        if !guard.ledger.match_independent.iter().any(|p| p == &key) {
            guard.ledger.match_independent.push(key);
        }
        persist(&mut guard)?;
        Ok(())
    }

    pub fn plan_evict(&self, required_bytes: u64) -> Result<EvictionPlan, StorageLedgerError> {
        if required_bytes == 0 {
            return Ok(EvictionPlan { paths: Vec::new() });
        }
        let guard = self.inner.lock().expect("storage broker lock");
        let depended_on = registered_dependencies(&guard.ledger);
        let mut candidates: Vec<(PathBuf, u64)> = Vec::new();
        for (path, artifact) in &guard.ledger.artifacts {
            if !artifact.evictable || !artifact.pins.is_empty() || depended_on.contains(path) {
                continue;
            }
            if !LedgerFile::class_allows_evict(
                artifact.class,
                path,
                &guard.ledger.match_independent,
            ) {
                continue;
            }
            candidates.push((PathBuf::from(path), artifact.logical_bytes));
        }
        candidates.sort_by(|a, b| a.0.cmp(&b.0));

        let mut paths = Vec::new();
        let mut gathered = 0u64;
        for (path, bytes) in candidates {
            if gathered >= required_bytes {
                break;
            }
            gathered = gathered.saturating_add(bytes);
            paths.push(path);
        }
        Ok(EvictionPlan { paths })
    }

    pub fn commit_evict(&mut self, plan: &EvictionPlan) -> Result<u64, StorageLedgerError> {
        let mut guard = self.inner.lock().expect("storage broker lock");
        if recover_pending_evictions(&mut guard)? {
            persist(&mut guard)?;
        }
        let mut reclaimed_logical_bytes = 0u64;
        for path in &plan.paths {
            let key = path_key(path);
            if registered_dependencies(&guard.ledger).contains(&key) {
                continue;
            }
            let Some(artifact) = guard.ledger.artifacts.get(&key) else {
                continue;
            };
            if !artifact.evictable || !artifact.pins.is_empty() {
                continue;
            }
            if !LedgerFile::class_allows_evict(
                artifact.class,
                &key,
                &guard.ledger.match_independent,
            ) {
                // Never delete CAS while Match may still depend on it.
                continue;
            }
            let target = contained_eviction_target(&guard.work_dir, path)?;
            let journal_path = prepare_eviction_journal(
                &guard.work_dir,
                EvictionJournal {
                    revision: EVICTION_JOURNAL_REVISION,
                    ledger_key: key.clone(),
                    original: target.clone(),
                    tombstone: eviction_tombstone_path(&guard.work_dir, &key),
                    logical_bytes: artifact.logical_bytes,
                },
            )?;
            let journal = read_eviction_journal(&journal_path)?;
            if target.exists() {
                std::fs::rename(&target, &journal.tombstone)?;
            }
            remove_path_if_present(&journal.tombstone)?;
            reclaimed_logical_bytes =
                reclaimed_logical_bytes.saturating_add(artifact.logical_bytes);
            guard.ledger.artifacts.remove(&key);
            guard.ledger.match_independent.retain(|p| p != &key);
            persist(&mut guard)?;
            std::fs::remove_file(journal_path)?;
        }
        if let Some(physical_free) = guard.physical_free_override.as_mut() {
            // Deterministic tests model the filesystem reading with this value;
            // committed eviction must therefore model the corresponding free.
            *physical_free = physical_free.saturating_add(reclaimed_logical_bytes);
        }
        Ok(available_bytes_locked(&guard))
    }

    /// Available bytes in the optional deterministic accounting model.
    ///
    /// Production brokers return zero because actual writes, not a filesystem
    /// capacity probe, determine whether space is available.
    pub fn available_after_evict(&self) -> u64 {
        let guard = self.inner.lock().expect("storage broker lock");
        available_bytes_locked(&guard)
    }

    pub fn reserve(
        &mut self,
        class: ArtifactClass,
        final_bytes: u64,
        partial_peak_bytes: u64,
    ) -> Result<StorageLease, StorageLedgerError> {
        final_bytes
            .checked_add(partial_peak_bytes)
            .ok_or(StorageLedgerError::ReservationOverflow)?;
        // Reservations coordinate artifact lifetimes only. Filesystem free
        // space can be unavailable or misleading for networked, quota-backed,
        // containerized, and Windows volumes. Do not preflight or evict from an
        // estimate; actual create/write/fsync operations are authoritative.
        let id = {
            let mut guard = self.inner.lock().expect("storage broker lock");
            let id = guard.ledger.next_reservation_id;
            guard.ledger.next_reservation_id = id.saturating_add(1);
            guard.ledger.reservations.insert(
                id,
                ReservationRecord {
                    class,
                    final_bytes,
                    partial_peak_bytes,
                },
            );
            persist(&mut guard)?;
            id
        };
        Ok(StorageLease {
            inner: Arc::clone(&self.inner),
            kind: Some(LeaseKind::Reserve { id }),
        })
    }

    pub fn snapshot(&self) -> StorageSnapshot {
        let guard = self.inner.lock().expect("storage broker lock");
        StorageSnapshot {
            physical_free_bytes: physical_free_locked(&guard),
            committed_bytes: guard.ledger.committed_bytes(),
            committed_partial_peak_bytes: guard.ledger.committed_partial_peak_bytes(),
            reclaimable_bytes: guard
                .ledger
                .reclaimable_bytes(&guard.ledger.match_independent),
            pinned_bytes: guard.ledger.pinned_bytes(),
            safety_reserve_bytes: guard.safety_reserve_bytes,
        }
    }
}

fn registered_dependencies(ledger: &LedgerFile) -> std::collections::BTreeSet<String> {
    ledger
        .artifacts
        .values()
        .flat_map(|artifact| artifact.dependencies.iter().cloned())
        .collect()
}

impl Drop for StorageLease {
    fn drop(&mut self) {
        let Some(kind) = self.kind.take() else {
            return;
        };
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        match kind {
            LeaseKind::Reserve { id } => {
                guard.ledger.reservations.remove(&id);
            }
            LeaseKind::Pin { path, checkpoint } => {
                if let Some(artifact) = guard.ledger.artifacts.get_mut(&path) {
                    artifact.pins.retain(|p| p != &checkpoint);
                }
            }
        }
        let _ = persist(&mut guard);
    }
}

impl StorageLease {
    /// Convert an RAII checkpoint pin into a durable ledger pin.
    ///
    /// Reservations cannot be persisted because they describe live allocation
    /// overlap. Durable pins are later released with [`StorageBroker::release_pin`].
    pub fn persist(mut self) -> Result<(), StorageLedgerError> {
        match self.kind {
            Some(LeaseKind::Pin { .. }) => {
                self.kind = None;
                Ok(())
            }
            Some(LeaseKind::Reserve { id }) => {
                Err(StorageLedgerError::CannotPersistReservation(id))
            }
            None => Ok(()),
        }
    }
}

fn persist(guard: &mut BrokerInner) -> Result<(), StorageLedgerError> {
    guard.ledger.save(&guard.work_dir)
}

fn apply_registration(
    ledger: &mut LedgerFile,
    registration: ArtifactRegistration,
) -> Result<(), StorageLedgerError> {
    let key = path_key(&registration.path);
    if let Some(existing) = ledger.artifacts.get_mut(&key) {
        if existing.class != registration.class
            || existing.dependencies != registration.dependencies
        {
            return Err(StorageLedgerError::AlreadyRegistered(key));
        }
        existing.logical_bytes = registration.logical_bytes;
        existing.partial_peak_bytes = registration.partial_peak_bytes;
        return Ok(());
    }
    ledger.artifacts.insert(
        key,
        ArtifactRecord {
            class: registration.class,
            logical_bytes: registration.logical_bytes,
            partial_peak_bytes: registration.partial_peak_bytes,
            dependencies: registration.dependencies,
            pins: Vec::new(),
            rebuildable: false,
            rebuild_from_checkpoint: None,
            evictable: false,
            evict_reason: None,
        },
    );
    Ok(())
}

fn path_key(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn eviction_directory(work_dir: &Path) -> PathBuf {
    work_dir.join(EVICTION_DIRECTORY)
}

fn eviction_tombstone_path(work_dir: &Path, ledger_key: &str) -> PathBuf {
    let digest = Sha256::digest(ledger_key.as_bytes());
    let name = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    eviction_directory(work_dir).join(format!("{name}.tombstone"))
}

fn prepare_eviction_journal(
    work_dir: &Path,
    journal: EvictionJournal,
) -> Result<PathBuf, StorageLedgerError> {
    let directory = eviction_directory(work_dir);
    std::fs::create_dir_all(&directory)?;
    let tombstone_name = journal.tombstone.file_name().ok_or_else(|| {
        StorageLedgerError::InvalidEvictionJournal("missing tombstone name".into())
    })?;
    let path = directory.join(tombstone_name).with_extension("json");
    let bytes = serde_json::to_vec_pretty(&journal)?;
    crate::format::atomic::write_atomic(&path, &bytes).map_err(|error| match error {
        crate::format::FormatError::Io(error) => StorageLedgerError::Io(error),
        other => StorageLedgerError::Io(std::io::Error::other(other.to_string())),
    })?;
    Ok(path)
}

fn read_eviction_journal(path: &Path) -> Result<EvictionJournal, StorageLedgerError> {
    let journal: EvictionJournal = serde_json::from_slice(&std::fs::read(path)?)?;
    if journal.revision != EVICTION_JOURNAL_REVISION {
        return Err(StorageLedgerError::InvalidEvictionJournal(format!(
            "unsupported revision {} in {}",
            journal.revision,
            path.display()
        )));
    }
    Ok(journal)
}

fn recover_pending_evictions(inner: &mut BrokerInner) -> Result<bool, StorageLedgerError> {
    let directory = eviction_directory(&inner.work_dir);
    if !directory.exists() {
        return Ok(false);
    }
    let mut journals = std::fs::read_dir(&directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    journals.sort();
    let mut ledger_changed = false;
    for journal_path in journals {
        let journal = read_eviction_journal(&journal_path)?;
        validate_recovery_path(&inner.work_dir, &journal.original)?;
        validate_recovery_path(&inner.work_dir, &journal.tombstone)?;
        if path_key(&journal.original) != journal.ledger_key
            || journal
                .tombstone
                .parent()
                .and_then(|parent| parent.canonicalize().ok())
                .as_deref()
                != Some(directory.as_path())
        {
            return Err(StorageLedgerError::InvalidEvictionJournal(format!(
                "journal paths do not match transaction root: {}",
                journal.tombstone.display()
            )));
        }
        let original_exists = journal.original.exists();
        let tombstone_exists = journal.tombstone.exists();
        match (original_exists, tombstone_exists) {
            (true, true) => {
                return Err(StorageLedgerError::AmbiguousEvictionRecovery(
                    journal.ledger_key,
                ));
            }
            (true, false) => {
                // The journal committed but the atomic rename did not. Preserve
                // the registered artifact and discard the unstarted transaction.
            }
            (false, true) => {
                remove_path_if_present(&journal.tombstone)?;
                inner.ledger.artifacts.remove(&journal.ledger_key);
                inner
                    .ledger
                    .match_independent
                    .retain(|path| path != &journal.ledger_key);
                ledger_changed = true;
                if let Some(physical_free) = inner.physical_free_override.as_mut() {
                    *physical_free = physical_free.saturating_add(journal.logical_bytes);
                }
            }
            (false, false) => {
                // Deletion committed before the ledger save. Complete the
                // transaction by making the ledger reflect durable disk state.
                inner.ledger.artifacts.remove(&journal.ledger_key);
                inner
                    .ledger
                    .match_independent
                    .retain(|path| path != &journal.ledger_key);
                ledger_changed = true;
            }
        }
        if ledger_changed {
            persist(inner)?;
            ledger_changed = false;
        }
        std::fs::remove_file(journal_path)?;
    }
    Ok(ledger_changed)
}

fn contained_eviction_target(
    work_dir: &Path,
    requested: &Path,
) -> Result<PathBuf, StorageLedgerError> {
    let resolved = if requested.exists() {
        requested.canonicalize()?
    } else {
        let parent = requested
            .parent()
            .ok_or_else(|| StorageLedgerError::OutsideWorkRoot {
                path: requested.display().to_string(),
                root: work_dir.display().to_string(),
            })?;
        let file_name =
            requested
                .file_name()
                .ok_or_else(|| StorageLedgerError::OutsideWorkRoot {
                    path: requested.display().to_string(),
                    root: work_dir.display().to_string(),
                })?;
        parent.canonicalize()?.join(file_name)
    };
    if resolved == work_dir
        || !resolved.starts_with(work_dir)
        || eviction_directory(work_dir).starts_with(&resolved)
    {
        return Err(StorageLedgerError::OutsideWorkRoot {
            path: resolved.display().to_string(),
            root: work_dir.display().to_string(),
        });
    }
    Ok(resolved)
}

fn validate_recovery_path(work_dir: &Path, path: &Path) -> Result<(), StorageLedgerError> {
    let resolved = if path.exists() {
        path.canonicalize()?
    } else {
        let parent = path
            .parent()
            .ok_or_else(|| StorageLedgerError::OutsideWorkRoot {
                path: path.display().to_string(),
                root: work_dir.display().to_string(),
            })?;
        let file_name = path
            .file_name()
            .ok_or_else(|| StorageLedgerError::OutsideWorkRoot {
                path: path.display().to_string(),
                root: work_dir.display().to_string(),
            })?;
        parent.canonicalize()?.join(file_name)
    };
    if resolved == work_dir || !resolved.starts_with(work_dir) {
        return Err(StorageLedgerError::OutsideWorkRoot {
            path: resolved.display().to_string(),
            root: work_dir.display().to_string(),
        });
    }
    Ok(())
}

fn remove_path_if_present(path: &Path) -> Result<(), StorageLedgerError> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

fn physical_free_locked(guard: &BrokerInner) -> u64 {
    guard.physical_free_override.unwrap_or(0)
}

fn available_bytes_locked(guard: &BrokerInner) -> u64 {
    let physical = physical_free_locked(guard);
    let reserved_final = guard.ledger.reserved_final_bytes();
    let partial = guard.ledger.committed_partial_peak_bytes();
    physical
        .saturating_sub(reserved_final)
        .saturating_sub(guard.safety_reserve_bytes)
        .saturating_sub(partial)
}
