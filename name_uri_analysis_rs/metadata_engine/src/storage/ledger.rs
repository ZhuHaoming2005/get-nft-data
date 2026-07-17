//! Persistent storage ledger schema and disk IO.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::format::atomic::write_atomic;
use crate::storage::ArtifactClass;

pub const LEDGER_FILE_NAME: &str = "storage-ledger.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerFile {
    pub schema_revision: u32,
    pub artifacts: BTreeMap<String, ArtifactRecord>,
    pub reservations: BTreeMap<u64, ReservationRecord>,
    pub next_reservation_id: u64,
    pub match_independent: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub class: ArtifactClass,
    pub logical_bytes: u64,
    pub partial_peak_bytes: u64,
    pub dependencies: Vec<String>,
    pub pins: Vec<String>,
    pub rebuildable: bool,
    pub rebuild_from_checkpoint: Option<String>,
    pub evictable: bool,
    pub evict_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReservationRecord {
    pub class: ArtifactClass,
    pub final_bytes: u64,
    pub partial_peak_bytes: u64,
}

impl LedgerFile {
    pub fn new(schema_revision: u32) -> Self {
        Self {
            schema_revision,
            artifacts: BTreeMap::new(),
            reservations: BTreeMap::new(),
            next_reservation_id: 1,
            match_independent: Vec::new(),
        }
    }

    pub fn path(work_dir: &Path) -> PathBuf {
        work_dir.join(LEDGER_FILE_NAME)
    }

    pub fn load_or_default(
        work_dir: &Path,
        schema_revision: u32,
    ) -> Result<Self, StorageLedgerError> {
        let path = Self::path(work_dir);
        if !path.exists() {
            return Ok(Self::new(schema_revision));
        }
        let bytes = std::fs::read(&path)?;
        let file: Self = serde_json::from_slice(&bytes)?;
        if file.schema_revision != schema_revision {
            return Err(StorageLedgerError::UnsupportedRevision {
                got: file.schema_revision,
                expected: schema_revision,
            });
        }
        Ok(file)
    }

    pub fn save(&self, work_dir: &Path) -> Result<(), StorageLedgerError> {
        let path = Self::path(work_dir);
        let bytes = serde_json::to_vec_pretty(self)?;
        write_atomic(&path, &bytes).map_err(|e| match e {
            crate::format::FormatError::Io(io) => StorageLedgerError::Io(io),
            other => StorageLedgerError::Io(std::io::Error::other(other.to_string())),
        })
    }

    pub fn committed_partial_peak_bytes(&self) -> u64 {
        // Sum concurrent reservation `.partial` peaks as a conservative upper
        // bound on worst-case overlap (all peaks held at once).
        self.reservations
            .values()
            .map(|r| r.partial_peak_bytes)
            .sum()
    }

    pub fn committed_bytes(&self) -> u64 {
        let artifacts: u64 = self.artifacts.values().map(|a| a.logical_bytes).sum();
        artifacts.saturating_add(self.reserved_final_bytes())
    }

    pub fn reserved_final_bytes(&self) -> u64 {
        self.reservations.values().map(|r| r.final_bytes).sum()
    }

    pub fn pinned_bytes(&self) -> u64 {
        self.artifacts
            .values()
            .filter(|a| !a.pins.is_empty())
            .map(|a| a.logical_bytes)
            .sum()
    }

    pub fn reclaimable_bytes(&self, match_independent: &[String]) -> u64 {
        let depended_on = self
            .artifacts
            .values()
            .flat_map(|artifact| artifact.dependencies.iter())
            .collect::<std::collections::BTreeSet<_>>();
        self.artifacts
            .iter()
            .filter(|(path, a)| {
                a.evictable
                    && a.pins.is_empty()
                    && !depended_on.contains(path)
                    && Self::class_allows_evict(a.class, path, match_independent)
            })
            .map(|(_, a)| a.logical_bytes)
            .sum()
    }

    pub fn class_allows_evict(
        class: ArtifactClass,
        path: &str,
        match_independent: &[String],
    ) -> bool {
        match class {
            ArtifactClass::PayloadCas => match_independent.iter().any(|p| p == path),
            ArtifactClass::Feature
            | ArtifactClass::Blocking
            | ArtifactClass::Index
            | ArtifactClass::ExactEvidence
            | ArtifactClass::RecallPlan
            | ArtifactClass::ConnectivityRun
            | ArtifactClass::ComponentSnapshot
            | ArtifactClass::Summary => true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageLedgerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported ledger schema revision {got} (expected {expected})")]
    UnsupportedRevision { got: u32, expected: u32 },
    #[error("artifact not registered: {0}")]
    NotRegistered(String),
    #[error("artifact already registered: {0}")]
    AlreadyRegistered(String),
    #[error("reservation not found: {0}")]
    ReservationNotFound(u64),
    #[error("storage reservation size overflow")]
    ReservationOverflow,
    #[error("live reservation {0} cannot be converted to a durable checkpoint pin")]
    CannotPersistReservation(u64),
    #[error("eviction path {path} is outside storage work root {root}")]
    OutsideWorkRoot { path: String, root: String },
    #[error("invalid eviction journal: {0}")]
    InvalidEvictionJournal(String),
    #[error("ambiguous eviction recovery: both original and tombstone exist for {0}")]
    AmbiguousEvictionRecovery(String),
}
