use dedup_model::{DedupError, ErrorContext};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitState {
    Writing,
    Flushed,
    ManifestWritten,
    DirectorySynced,
    Renamed,
    SuccessMarked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub schema_version: u32,
    pub stage: String,
    pub logical_input_digest: String,
    pub configuration_digest: String,
    pub upstream_checksums: BTreeMap<String, String>,
    pub data_checksums: BTreeMap<String, String>,
}

pub trait FailureInjector {
    fn after(&self, state: CommitState) -> Result<(), DedupError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoFailure;

impl FailureInjector for NoFailure {
    fn after(&self, _state: CommitState) -> Result<(), DedupError> {
        Ok(())
    }
}

pub struct ArtifactWriter<I = NoFailure> {
    final_dir: PathBuf,
    staging_dir: PathBuf,
    manifest: ArtifactManifest,
    injector: I,
    files: BTreeMap<String, File>,
}

impl ArtifactWriter<NoFailure> {
    pub fn new(
        final_dir: impl AsRef<Path>,
        manifest: ArtifactManifest,
    ) -> Result<Self, DedupError> {
        Self::with_injector(final_dir, manifest, NoFailure)
    }
}

impl<I: FailureInjector> ArtifactWriter<I> {
    pub fn with_injector(
        final_dir: impl AsRef<Path>,
        manifest: ArtifactManifest,
        injector: I,
    ) -> Result<Self, DedupError> {
        let final_dir = final_dir.as_ref().to_path_buf();
        let name = final_dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| DedupError::InvalidInput {
                context: ErrorContext::stage("artifact"),
                message: "artifact directory must have a UTF-8 file name".to_owned(),
            })?;
        let staging_dir = final_dir.with_file_name(format!(".{name}.staging"));
        if staging_dir.exists() {
            fs::remove_dir_all(&staging_dir)?;
        }
        fs::create_dir_all(&staging_dir)?;
        injector.after(CommitState::Writing)?;
        Ok(Self {
            final_dir,
            staging_dir,
            manifest,
            injector,
            files: BTreeMap::new(),
        })
    }

    pub fn create_data_file(&mut self, name: &str) -> Result<&mut File, DedupError> {
        if name.contains('/') || name.contains('\\') || name.starts_with('.') {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("artifact"),
                message: format!("invalid data file name {name:?}"),
            });
        }
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.staging_dir.join(name))?;
        self.files.insert(name.to_owned(), file);
        Ok(self.files.get_mut(name).expect("inserted file must exist"))
    }

    pub fn commit(mut self) -> Result<ArtifactManifest, DedupError> {
        for file in self.files.values_mut() {
            file.flush()?;
            file.sync_all()?;
        }
        self.injector.after(CommitState::Flushed)?;

        for name in self.files.keys() {
            self.manifest
                .data_checksums
                .insert(name.clone(), checksum_file(&self.staging_dir.join(name))?);
        }
        self.files.clear();
        let manifest_bytes = serde_json::to_vec_pretty(&self.manifest).map_err(|error| {
            DedupError::InvariantViolation {
                context: ErrorContext::stage("artifact"),
                message: error.to_string(),
            }
        })?;
        let manifest_path = self.staging_dir.join("artifact_manifest.json");
        let mut manifest_file = File::create(&manifest_path)?;
        manifest_file.write_all(&manifest_bytes)?;
        manifest_file.sync_all()?;
        drop(manifest_file);
        self.injector.after(CommitState::ManifestWritten)?;

        sync_directory(&self.staging_dir)?;
        self.injector.after(CommitState::DirectorySynced)?;
        if self.final_dir.exists() {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("artifact"),
                message: "official artifact directory already exists".to_owned(),
            });
        }
        fs::rename(&self.staging_dir, &self.final_dir).map_err(|error| {
            DedupError::ArtifactMismatch {
                context: ErrorContext::stage("artifact"),
                message: format!("same-filesystem atomic rename failed: {error}"),
            }
        })?;
        if let Some(parent) = self.final_dir.parent() {
            sync_directory(parent)?;
        }
        self.injector.after(CommitState::Renamed)?;

        let mut success = File::create(self.final_dir.join("_SUCCESS"))?;
        success.write_all(b"ok\n")?;
        success.sync_all()?;
        sync_directory(&self.final_dir)?;
        self.injector.after(CommitState::SuccessMarked)?;
        Ok(self.manifest)
    }
}

pub fn validate_artifact(path: impl AsRef<Path>) -> Result<ArtifactManifest, DedupError> {
    let path = path.as_ref();
    if !path.join("_SUCCESS").is_file() {
        return Err(DedupError::ArtifactMismatch {
            context: ErrorContext::stage("artifact"),
            message: "_SUCCESS is missing".to_owned(),
        });
    }
    let bytes = fs::read(path.join("artifact_manifest.json"))?;
    let manifest: ArtifactManifest =
        serde_json::from_slice(&bytes).map_err(|error| DedupError::ArtifactMismatch {
            context: ErrorContext::stage("artifact"),
            message: error.to_string(),
        })?;
    for (name, expected) in &manifest.data_checksums {
        let actual = checksum_file(&path.join(name))?;
        if &actual != expected {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("artifact"),
                message: format!("checksum mismatch for {name}"),
            });
        }
    }
    Ok(manifest)
}

pub fn recover_incomplete_artifact(path: impl AsRef<Path>) -> Result<(), DedupError> {
    let path = path.as_ref();
    if path.join("_SUCCESS").is_file() {
        validate_artifact(path)?;
        return Ok(());
    }
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DedupError::InvalidInput {
            context: ErrorContext::stage("artifact_recovery"),
            message: "artifact directory must have a UTF-8 name".to_owned(),
        })?;
    let staging = path.with_file_name(format!(".{name}.staging"));
    if staging.exists() {
        fs::remove_dir_all(staging)?;
    }
    Ok(())
}

fn checksum_file(path: &Path) -> Result<String, DedupError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn sync_directory(path: &Path) -> Result<(), DedupError> {
    #[cfg(not(windows))]
    File::open(path)?.sync_all()?;
    #[cfg(windows)]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> ArtifactManifest {
        ArtifactManifest {
            schema_version: 1,
            stage: "test".to_owned(),
            logical_input_digest: "input".to_owned(),
            configuration_digest: "config".to_owned(),
            upstream_checksums: BTreeMap::new(),
            data_checksums: BTreeMap::new(),
        }
    }

    #[test]
    fn success_is_written_last_and_checksum_is_verified() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact");
        let mut writer = ArtifactWriter::new(&path, manifest()).unwrap();
        writer
            .create_data_file("data.bin")
            .unwrap()
            .write_all(b"abc")
            .unwrap();
        writer.commit().unwrap();
        assert!(validate_artifact(&path).is_ok());
        fs::write(path.join("data.bin"), b"changed").unwrap();
        assert!(matches!(
            validate_artifact(&path),
            Err(DedupError::ArtifactMismatch { .. })
        ));
    }

    #[derive(Clone, Copy)]
    struct FailAt(CommitState);

    impl FailureInjector for FailAt {
        fn after(&self, state: CommitState) -> Result<(), DedupError> {
            if state == self.0 {
                return Err(DedupError::InvariantViolation {
                    context: ErrorContext::stage("failure_injection"),
                    message: format!("{state:?}"),
                });
            }
            Ok(())
        }
    }

    #[test]
    fn every_pre_success_failure_is_unusable_and_recoverable() {
        for state in [
            CommitState::Writing,
            CommitState::Flushed,
            CommitState::ManifestWritten,
            CommitState::DirectorySynced,
            CommitState::Renamed,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("artifact");
            let result = ArtifactWriter::with_injector(&path, manifest(), FailAt(state)).and_then(
                |mut writer| {
                    writer.create_data_file("data.bin")?.write_all(b"abc")?;
                    writer.commit().map(|_| ())
                },
            );
            assert!(result.is_err(), "{state:?} did not fail");
            assert!(validate_artifact(&path).is_err());
            assert!(!path.join("_SUCCESS").exists());
            recover_incomplete_artifact(&path).unwrap();
            assert!(!path.exists());
        }
    }

    #[test]
    fn success_mark_is_the_only_terminal_state() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact");
        let mut writer =
            ArtifactWriter::with_injector(&path, manifest(), FailAt(CommitState::SuccessMarked))
                .unwrap();
        writer
            .create_data_file("data.bin")
            .unwrap()
            .write_all(b"abc")
            .unwrap();
        assert!(writer.commit().is_err());
        assert!(validate_artifact(path).is_ok());
    }

    #[test]
    fn renamed_incomplete_artifact_can_be_recovered_and_recommitted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact");
        let mut failed =
            ArtifactWriter::with_injector(&path, manifest(), FailAt(CommitState::Renamed)).unwrap();
        failed
            .create_data_file("data.bin")
            .unwrap()
            .write_all(b"old")
            .unwrap();
        assert!(failed.commit().is_err());

        recover_incomplete_artifact(&path).unwrap();
        let mut replacement = ArtifactWriter::new(&path, manifest()).unwrap();
        replacement
            .create_data_file("data.bin")
            .unwrap()
            .write_all(b"new")
            .unwrap();
        replacement.commit().unwrap();

        assert_eq!(fs::read(path.join("data.bin")).unwrap(), b"new");
        assert!(validate_artifact(path).is_ok());
    }
}
