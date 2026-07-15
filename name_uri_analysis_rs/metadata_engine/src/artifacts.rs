//! Revisioned metadata artifact paths generated from one source of truth.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MATCH_ARTIFACT_REVISION: u32 = 1;

#[derive(Debug, Clone)]
pub struct MetadataArtifactLayout {
    metadata_root: PathBuf,
}

impl MetadataArtifactLayout {
    pub fn new(work_directory: &Path) -> Self {
        Self {
            metadata_root: work_directory.join("artifacts/metadata"),
        }
    }

    pub fn encode_dir(&self) -> PathBuf {
        self.metadata_root
            .join(format!("encode-{}", crate::encode::ENCODE_SCHEMA_REVISION))
    }

    pub fn blocking_dir(&self) -> PathBuf {
        self.metadata_root
            .join(format!("blocking-{}", crate::blocking::BLOCKING_REVISION))
    }

    pub fn match_dir(&self) -> PathBuf {
        self.metadata_root
            .join(format!("match-{MATCH_ARTIFACT_REVISION}"))
    }

    /// Per-run staging directory for Encode feature arrays (never the published path).
    pub fn encode_run_staging_dir(&self, run_id: &str) -> PathBuf {
        self.metadata_root.join(format!(
            ".staging-encode-{}-{}",
            crate::encode::ENCODE_SCHEMA_REVISION,
            run_id
        ))
    }

    /// Per-run staging directory for Blocking arrays.
    pub fn blocking_run_staging_dir(&self, run_id: &str) -> PathBuf {
        self.metadata_root.join(format!(
            ".staging-blocking-{}-{}",
            crate::blocking::BLOCKING_REVISION,
            run_id
        ))
    }

    /// Remove abandoned per-run staging directories and repair interrupted
    /// directory swaps before a new Encode publish attempt starts.
    pub fn cleanup_stale_staging(&self) -> io::Result<()> {
        recover_interrupted_publish(&self.encode_dir(), "features.ready")?;
        recover_interrupted_publish(&self.blocking_dir(), "blocking.ready")?;
        if !self.metadata_root.is_dir() {
            return Ok(());
        }
        for entry in fs::read_dir(&self.metadata_root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if entry.file_type()?.is_dir()
                && (name.starts_with(".staging-encode-") || name.starts_with(".staging-blocking-"))
            {
                fs::remove_dir_all(entry.path())?;
            }
        }
        Ok(())
    }
}

/// Stable-enough unique id for one Encode publish attempt.
pub fn new_artifact_run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{nanos}", std::process::id())
}

/// Replace `final_dir` with `staging_dir`, retaining the previous directory
/// until the caller commits its ready marker and finalizes the transaction.
pub fn publish_staged_bundle(staging_dir: &Path, final_dir: &Path) -> io::Result<PublishedBundle> {
    if !staging_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("staging bundle does not exist: {}", staging_dir.display()),
        ));
    }
    if let Some(parent) = final_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    let backup_dir = backup_dir(final_dir)?;
    if backup_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "previous publish transaction requires recovery: {}",
                backup_dir.display()
            ),
        ));
    }
    if final_dir.exists() {
        fs::rename(final_dir, &backup_dir)?;
    }
    if let Err(error) = fs::rename(staging_dir, final_dir) {
        if backup_dir.exists() {
            let _ = fs::rename(&backup_dir, final_dir);
        }
        return Err(error);
    }
    Ok(PublishedBundle {
        final_dir: final_dir.to_path_buf(),
        backup_dir,
        finalized: false,
    })
}

fn backup_dir(final_dir: &Path) -> io::Result<PathBuf> {
    let name = final_dir.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("bundle path has no file name: {}", final_dir.display()),
        )
    })?;
    Ok(final_dir.with_file_name(format!(".previous-{}", name.to_string_lossy())))
}

fn recover_interrupted_publish(final_dir: &Path, ready_marker: &str) -> io::Result<()> {
    let backup_dir = backup_dir(final_dir)?;
    if !backup_dir.exists() {
        return Ok(());
    }
    if final_dir.join(ready_marker).is_file() {
        fs::remove_dir_all(backup_dir)?;
        return Ok(());
    }
    if final_dir.exists() {
        fs::remove_dir_all(final_dir)?;
    }
    fs::rename(backup_dir, final_dir)
}

/// In-process rollback guard for one published directory swap.
pub struct PublishedBundle {
    final_dir: PathBuf,
    backup_dir: PathBuf,
    finalized: bool,
}

impl PublishedBundle {
    pub fn finalize(mut self) -> io::Result<()> {
        self.finalized = true;
        if self.backup_dir.exists() {
            fs::remove_dir_all(&self.backup_dir)?;
        }
        Ok(())
    }
}

impl Drop for PublishedBundle {
    fn drop(&mut self) {
        if self.finalized || !self.backup_dir.exists() {
            return;
        }
        if self.final_dir.exists() {
            let _ = fs::remove_dir_all(&self.final_dir);
        }
        let _ = fs::rename(&self.backup_dir, &self.final_dir);
    }
}

/// Removes the current run's unpublished staging directories on normal error
/// unwinding. Crash leftovers are handled by `cleanup_stale_staging`.
pub struct StagingCleanupGuard {
    paths: Vec<PathBuf>,
}

impl StagingCleanupGuard {
    pub fn new(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            paths: paths.into_iter().collect(),
        }
    }
}

impl Drop for StagingCleanupGuard {
    fn drop(&mut self) {
        for path in &self.paths {
            if path.is_dir() {
                let _ = fs::remove_dir_all(path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_track_engine_revisions() {
        let root = Path::new("work");
        let layout = MetadataArtifactLayout::new(root);

        assert_eq!(
            layout.encode_dir(),
            root.join(format!(
                "artifacts/metadata/encode-{}",
                crate::encode::ENCODE_SCHEMA_REVISION
            ))
        );
        assert_eq!(
            layout.blocking_dir(),
            root.join(format!(
                "artifacts/metadata/blocking-{}",
                crate::blocking::BLOCKING_REVISION
            ))
        );
        assert_eq!(layout.match_dir(), root.join("artifacts/metadata/match-1"));
        assert_eq!(
            layout.encode_run_staging_dir("abc"),
            root.join(format!(
                "artifacts/metadata/.staging-encode-{}-abc",
                crate::encode::ENCODE_SCHEMA_REVISION
            ))
        );
    }

    #[test]
    fn publish_replaces_final_with_staging() {
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("staging");
        let final_dir = root.path().join("final");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("payload.bin"), b"ok").unwrap();
        fs::create_dir_all(&final_dir).unwrap();
        fs::write(final_dir.join("stale.bin"), b"old").unwrap();

        publish_staged_bundle(&staging, &final_dir)
            .unwrap()
            .finalize()
            .unwrap();

        assert!(!staging.exists());
        assert!(final_dir.join("payload.bin").is_file());
        assert!(!final_dir.join("stale.bin").exists());
    }

    #[test]
    fn failed_publish_preserves_the_previous_bundle() {
        let root = tempfile::tempdir().unwrap();
        let missing_staging = root.path().join("missing-staging");
        let final_dir = root.path().join("final");
        fs::create_dir_all(&final_dir).unwrap();
        fs::write(final_dir.join("durable.bin"), b"old").unwrap();

        assert!(publish_staged_bundle(&missing_staging, &final_dir).is_err());

        assert_eq!(fs::read(final_dir.join("durable.bin")).unwrap(), b"old");
    }

    #[test]
    fn stale_staging_cleanup_preserves_published_bundles() {
        let root = tempfile::tempdir().unwrap();
        let layout = MetadataArtifactLayout::new(root.path());
        let encode_staging = layout.encode_run_staging_dir("stale");
        let blocking_staging = layout.blocking_run_staging_dir("stale");
        let final_dir = layout.encode_dir();
        fs::create_dir_all(&encode_staging).unwrap();
        fs::create_dir_all(&blocking_staging).unwrap();
        fs::create_dir_all(&final_dir).unwrap();

        layout.cleanup_stale_staging().unwrap();

        assert!(!encode_staging.exists());
        assert!(!blocking_staging.exists());
        assert!(final_dir.exists());
    }

    #[test]
    fn staging_guard_removes_unpublished_paths_on_drop() {
        let root = tempfile::tempdir().unwrap();
        let encode_staging = root.path().join("encode-staging");
        let blocking_staging = root.path().join("blocking-staging");
        fs::create_dir_all(&encode_staging).unwrap();
        fs::create_dir_all(&blocking_staging).unwrap();

        {
            let _guard =
                StagingCleanupGuard::new([encode_staging.clone(), blocking_staging.clone()]);
        }

        assert!(!encode_staging.exists());
        assert!(!blocking_staging.exists());
    }
}
