//! Atomic write helpers (`.partial` then rename).

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::format::FormatError;

/// Path used for an in-progress write of `final_path`.
pub fn partial_path(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".partial");
    PathBuf::from(s)
}

/// Write `bytes` to `final_path` via `*.partial` then rename.
///
/// The last durable destination is never unlinked before replacement.
pub fn write_atomic(final_path: &Path, bytes: &[u8]) -> Result<(), FormatError> {
    write_atomic_file(final_path, |file| {
        file.write_all(bytes)?;
        Ok(())
    })
}

/// Stream an atomic file body into `*.partial`, sync it, then publish it.
///
/// A body-write failure leaves the durable destination untouched. The
/// incomplete partial is never renamed and can be overwritten by a retry.
pub(crate) fn write_atomic_file(
    final_path: &Path,
    write_body: impl FnOnce(&mut File) -> Result<(), FormatError>,
) -> Result<(), FormatError> {
    if let Some(parent) = final_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let partial = partial_path(final_path);
    {
        let mut file = File::create(&partial)?;
        write_body(&mut file)?;
        file.sync_all()?;
    }

    replace_file_atomically(&partial, final_path)?;
    Ok(())
}

/// Atomically publish a ready marker containing `manifest_json` under `bundle_dir`.
pub fn commit_ready(
    bundle_dir: &Path,
    ready_name: &str,
    manifest_json: &str,
) -> Result<(), FormatError> {
    // Ready markers always carry a JSON manifest object/value.
    let _: serde_json::Value = serde_json::from_str(manifest_json)
        .map_err(|e| FormatError::InvalidManifest(e.to_string()))?;

    let ready_path = bundle_dir.join(ready_name);
    write_atomic(&ready_path, manifest_json.as_bytes())
}

pub(super) fn replace_file_atomically(from: &Path, to: &Path) -> Result<(), FormatError> {
    #[cfg(windows)]
    replace_file_windows(from, to)?;
    #[cfg(not(windows))]
    fs::rename(from, to)?;

    sync_parent_directory(to)?;
    Ok(())
}

#[cfg(windows)]
fn replace_file_windows(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, ReplaceFileW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        REPLACEFILE_WRITE_THROUGH,
    };

    let destination_exists = to.exists();
    let destination = to
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let partial = from
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let replaced = unsafe {
        if destination_exists {
            ReplaceFileW(
                destination.as_ptr(),
                partial.as_ptr(),
                std::ptr::null(),
                REPLACEFILE_WRITE_THROUGH,
                std::ptr::null(),
                std::ptr::null(),
            )
        } else {
            MoveFileExW(
                partial.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        }
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent_directory(destination: &Path) -> std::io::Result<()> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_destination: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_body_failure_never_replaces_the_durable_destination() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("values.u64");
        fs::write(&destination, b"durable").unwrap();

        let result = write_atomic_file(&destination, |file| {
            file.write_all(b"incomplete")?;
            Err(std::io::Error::other("injected body failure").into())
        });

        assert!(result.is_err());
        assert_eq!(fs::read(&destination).unwrap(), b"durable");
        assert_eq!(fs::read(partial_path(&destination)).unwrap(), b"incomplete");
    }
}
