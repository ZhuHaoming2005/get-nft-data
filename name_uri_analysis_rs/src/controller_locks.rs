use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::UNIX_EPOCH;

use name_uri_analysis_rs::replace_file_atomically;

use crate::controller_constants::{PARENT_LIVENESS_ENV, PHASE_GENERATION_ENV};

#[derive(Debug)]
pub(crate) struct ControllerLock {
    _lock: ExclusiveFileLock,
}

impl ControllerLock {
    pub(crate) fn acquire(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let parent = work_directory
            .parent()
            .ok_or("--work-directory must not be a filesystem root")?;
        let name = work_directory
            .file_name()
            .ok_or("--work-directory must have a final path component")?
            .to_string_lossy();
        fs::create_dir_all(parent)?;
        let path = parent.join(format!(".{name}.name-uri-analysis.lock"));
        let owner = format!(
            "{} {}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        match ExclusiveFileLock::try_acquire(&path, &owner)? {
            Some(lock) => Ok(Self { _lock: lock }),
            None => {
                let current_owner = fs::read_to_string(&path).unwrap_or_default();
                let owner_description = current_owner
                    .split_whitespace()
                    .next()
                    .and_then(|value| value.parse::<u32>().ok())
                    .map_or_else(
                        || "another process".to_string(),
                        |pid| format!("process {pid}"),
                    );
                Err(format!(
                    "work directory {} is already controlled by {owner_description}",
                    work_directory.display()
                )
                .into())
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PhaseLock {
    _lock: ExclusiveFileLock,
}

impl PhaseLock {
    pub(crate) fn acquire(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let (path, owner) = Self::target(work_directory)?;
        match ExclusiveFileLock::try_acquire(&path, &owner)? {
            Some(lock) => Ok(Self { _lock: lock }),
            None => Err(format!(
                "analysis phase is still active in another process for work directory {}",
                work_directory.display()
            )
            .into()),
        }
    }

    pub(crate) fn acquire_blocking(
        work_directory: &Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (path, owner) = Self::target(work_directory)?;
        Ok(Self {
            _lock: ExclusiveFileLock::acquire(&path, &owner)?,
        })
    }

    fn target(work_directory: &Path) -> Result<(PathBuf, String), Box<dyn std::error::Error>> {
        let parent = work_directory
            .parent()
            .ok_or("--work-directory must not be a filesystem root")?;
        let name = work_directory
            .file_name()
            .ok_or("--work-directory must have a final path component")?
            .to_string_lossy();
        fs::create_dir_all(parent)?;
        let path = parent.join(format!(".{name}.name-uri-analysis.phase.lock"));
        let owner = format!(
            "{} {}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        Ok((path, owner))
    }
}

#[derive(Debug)]
pub(crate) struct ControllerPhaseLease {
    work_directory: PathBuf,
    generation: String,
    lock: Option<PhaseLock>,
}

impl ControllerPhaseLease {
    pub(crate) fn acquire(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let lock = PhaseLock::acquire(work_directory)?;
        let generation = new_process_generation();
        write_phase_generation_atomically(work_directory, &generation)?;
        Ok(Self {
            work_directory: work_directory.to_path_buf(),
            generation,
            lock: Some(lock),
        })
    }

    pub(crate) fn generation(&self) -> &str {
        &self.generation
    }

    pub(crate) fn release_for_child(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let lock = self
            .lock
            .take()
            .ok_or("controller phase lease is already released")?;
        drop(lock);
        Ok(())
    }

    pub(crate) fn reclaim_after_child(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.lock.is_some() {
            return Err("controller phase lease was not released to the child".into());
        }
        self.lock = Some(PhaseLock::acquire_blocking(&self.work_directory)?);
        validate_phase_generation(&self.work_directory, &self.generation)?;
        Ok(())
    }
}

pub(crate) fn new_process_generation() -> String {
    format!(
        "{} {}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

pub(crate) fn phase_generation_path(
    work_directory: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let parent = work_directory
        .parent()
        .ok_or("--work-directory must not be a filesystem root")?;
    let name = work_directory
        .file_name()
        .ok_or("--work-directory must have a final path component")?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.name-uri-analysis.phase.generation")))
}

pub(crate) fn write_phase_generation_atomically(
    work_directory: &Path,
    generation: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = phase_generation_path(work_directory)?;
    let partial = path.with_extension("generation.partial");
    let mut file = fs::File::create(&partial)?;
    file.write_all(generation.as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, &path)?;
    Ok(())
}

pub(crate) fn validate_phase_generation(
    work_directory: &Path,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = phase_generation_path(work_directory)?;
    let actual = fs::read_to_string(&path)?;
    if actual != expected {
        return Err(format!(
            "stale internal phase generation rejected for work directory {}",
            work_directory.display()
        )
        .into());
    }
    Ok(())
}

pub(crate) fn validate_phase_generation_from_env(
    work_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = std::env::var(PHASE_GENERATION_ENV)
        .map_err(|_| "internal phase generation is missing or invalid")?;
    validate_phase_generation(work_directory, &expected)
}

#[derive(Debug)]
pub(crate) struct ExclusiveFileLock {
    file: fs::File,
}

impl ExclusiveFileLock {
    pub(crate) fn acquire(path: &Path, owner: &str) -> std::io::Result<Self> {
        let mut file = Self::open(path)?;
        file.lock()?;
        Self::write_owner(&mut file, owner)?;
        Ok(Self { file })
    }

    pub(crate) fn try_acquire(path: &Path, owner: &str) -> std::io::Result<Option<Self>> {
        let mut file = Self::open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Ok(None),
            Err(fs::TryLockError::Error(error)) if lock_error_is_contention(&error) => {
                return Ok(None);
            }
            Err(fs::TryLockError::Error(error)) => return Err(error),
        }
        Self::write_owner(&mut file, owner)?;
        Ok(Some(Self { file }))
    }

    fn open(path: &Path) -> std::io::Result<fs::File> {
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
    }

    fn write_owner(file: &mut fs::File, owner: &str) -> std::io::Result<()> {
        file.set_len(0)?;
        file.write_all(owner.as_bytes())?;
        file.sync_all()
    }
}

pub(crate) fn lock_error_is_contention(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(33) {
        // LockFileEx reports ERROR_LOCK_VIOLATION for a conflicting range.
        return true;
    }
    false
}

impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(crate) fn start_parent_liveness_watchdog() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os(PARENT_LIVENESS_ENV).as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Err("internal phases must be launched by the pipeline controller".into());
    }
    thread::Builder::new()
        .name("controller-liveness".to_string())
        .spawn(|| {
            let stdin = std::io::stdin();
            watch_parent_liveness(stdin.lock(), || {
                eprintln!("pipeline controller disconnected; terminating internal phase");
                std::process::exit(1);
            });
        })?;
    Ok(())
}

pub(crate) fn watch_parent_liveness<R: Read>(mut reader: R, on_disconnect: impl FnOnce()) {
    let mut buffer = [0u8; 64];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => {
                on_disconnect();
                return;
            }
            Ok(_) => {}
        }
    }
}
