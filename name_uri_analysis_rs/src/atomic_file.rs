use std::io::{self, Write};
use std::path::Path;

use serde::Serialize;

/// Pretty-print `value` to `destination` via a `.json.partial` sibling, flushing
/// and syncing before an atomic replace.
pub fn write_json_atomically<T: Serialize>(value: &T, destination: &Path) -> io::Result<()> {
    let partial = destination.with_extension("json.partial");
    let mut file = std::fs::File::create(&partial)?;
    serde_json::to_writer_pretty(&mut file, value).map_err(io::Error::other)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, destination)
}

/// Replace `destination` with the already-flushed `partial` file without first
/// unlinking the last durable copy. Both paths must be on the same filesystem.
pub fn replace_file_atomically(partial: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(windows)]
    replace_file_windows(partial, destination)?;
    #[cfg(not(windows))]
    std::fs::rename(partial, destination)?;

    sync_parent_directory(destination)
}

#[cfg(windows)]
fn replace_file_windows(partial: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, ReplaceFileW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        REPLACEFILE_WRITE_THROUGH,
    };
    let destination_exists = destination.exists();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let partial = partial
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
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent_directory(destination: &Path) -> io::Result<()> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_destination: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_an_existing_file_and_consumes_partial() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("manifest.json");
        let partial = temp.path().join("manifest.json.partial");
        std::fs::write(&destination, b"old").unwrap();
        std::fs::write(&partial, b"new").unwrap();

        replace_file_atomically(&partial, &destination).unwrap();

        assert_eq!(std::fs::read(&destination).unwrap(), b"new");
        assert!(!partial.exists());
    }

    #[test]
    fn publishes_a_new_file_and_consumes_partial() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("ready.json");
        let partial = temp.path().join("ready.json.partial");
        std::fs::write(&partial, b"ready").unwrap();

        replace_file_atomically(&partial, &destination).unwrap();

        assert_eq!(std::fs::read(&destination).unwrap(), b"ready");
        assert!(!partial.exists());
    }

    #[test]
    fn windows_first_publish_uses_a_write_through_move() {
        let source = include_str!("atomic_file.rs");
        let production = source.split("#[cfg(test)]").next().unwrap();

        assert!(production.contains("MoveFileExW"));
        assert!(production.contains("MOVEFILE_WRITE_THROUGH"));
    }
}
