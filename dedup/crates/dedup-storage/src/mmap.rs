#![allow(unsafe_code)]

use crate::{MemoryBudget, MemoryLease};
use dedup_model::{DedupError, ErrorContext};
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::ops::Range;
use std::path::Path;

/// Expected access pattern for a read-only mapped segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessPattern {
    Sequential,
    Random,
    WillNeed,
    NoLongerNeeded,
}

/// Bounds-checked read-only mapping whose estimated residency owns a budget lease.
#[derive(Debug)]
pub struct ReadOnlySegment {
    mapping: Option<Mmap>,
    _residency: MemoryLease,
    length: u64,
}

impl ReadOnlySegment {
    pub fn open(path: impl AsRef<Path>, budget: &MemoryBudget) -> Result<Self, DedupError> {
        Self::open_with_residency(path, budget, u64::MAX)
    }

    pub fn open_with_residency(
        path: impl AsRef<Path>,
        budget: &MemoryBudget,
        residency_bytes: u64,
    ) -> Result<Self, DedupError> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let residency = budget.require_lease(length.min(residency_bytes))?;
        let mapping = if length == 0 {
            None
        } else {
            // SAFETY: the file is opened read-only, its descriptor stays alive for the mapping
            // operation, and this wrapper exposes only immutable, bounds-checked byte slices.
            Some(unsafe { MmapOptions::new().map(&file)? })
        };
        Ok(Self {
            mapping,
            _residency: residency,
            length,
        })
    }

    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn bytes(&self, range: Range<u64>) -> Result<&[u8], DedupError> {
        if range.start > range.end || range.end > self.length {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("mmap"),
                message: format!(
                    "mapped range {}..{} exceeds segment length {}",
                    range.start, range.end, self.length
                ),
            });
        }
        let start = usize::try_from(range.start).map_err(|_| mapped_offset_error())?;
        let end = usize::try_from(range.end).map_err(|_| mapped_offset_error())?;
        Ok(&self.as_slice()[start..end])
    }

    pub fn advise(&self, pattern: AccessPattern) -> Result<(), DedupError> {
        #[cfg(unix)]
        if let Some(mapping) = &self.mapping {
            let advice = match pattern {
                AccessPattern::Sequential => memmap2::Advice::Sequential,
                AccessPattern::Random => memmap2::Advice::Random,
                AccessPattern::WillNeed => memmap2::Advice::WillNeed,
                AccessPattern::NoLongerNeeded => memmap2::Advice::DontNeed,
            };
            mapping.advise(advice)?;
        }
        #[cfg(not(unix))]
        let _ = pattern;
        Ok(())
    }

    fn as_slice(&self) -> &[u8] {
        self.mapping.as_deref().unwrap_or_default()
    }
}

fn mapped_offset_error() -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext::stage("mmap"),
        message: "mapped offset does not fit the platform address space".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_is_read_only_bounded_and_budgeted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.bin");
        std::fs::write(&path, b"abcdef").unwrap();
        let budget = MemoryBudget::new(100, 100);
        {
            let segment = ReadOnlySegment::open(&path, &budget).unwrap();
            assert_eq!(budget.used(), 6);
            assert_eq!(segment.bytes(1..4).unwrap(), b"bcd");
            assert!(segment.bytes(0..7).is_err());
            segment.advise(AccessPattern::Sequential).unwrap();
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn empty_mapping_has_no_residency() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.bin");
        std::fs::write(&path, []).unwrap();
        let budget = MemoryBudget::new(100, 100);
        let segment = ReadOnlySegment::open(&path, &budget).unwrap();
        assert!(segment.is_empty());
        assert_eq!(segment.bytes(0..0).unwrap(), b"");
    }
}
