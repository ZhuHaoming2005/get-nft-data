use crate::PlatformError;
use std::path::{Path, PathBuf};

/// Reads the native Linux mount table.
///
/// # Errors
///
/// Returns a platform error outside Linux or when `/proc/self/mountinfo`
/// cannot be read.
pub fn read_native_mountinfo() -> Result<String, PlatformError> {
    read_native_mountinfo_impl()
}

#[cfg(target_os = "linux")]
fn read_native_mountinfo_impl() -> Result<String, PlatformError> {
    std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|error| PlatformError::Io(error.to_string()))
}

#[cfg(not(target_os = "linux"))]
fn read_native_mountinfo_impl() -> Result<String, PlatformError> {
    Err(PlatformError::Missing("Linux mountinfo"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountInfo {
    pub mount_point: PathBuf,
    pub file_system: String,
}

pub fn parse_mountinfo(input: &str) -> Result<Vec<MountInfo>, PlatformError> {
    input
        .lines()
        .map(|line| {
            let (left, right) =
                line.split_once(" - ")
                    .ok_or_else(|| PlatformError::InvalidData {
                        field: "mountinfo",
                        value: line.to_owned(),
                    })?;
            let mount_point =
                left.split_whitespace()
                    .nth(4)
                    .ok_or_else(|| PlatformError::InvalidData {
                        field: "mountinfo",
                        value: line.to_owned(),
                    })?;
            let file_system =
                right
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| PlatformError::InvalidData {
                        field: "mountinfo",
                        value: line.to_owned(),
                    })?;
            Ok(MountInfo {
                mount_point: PathBuf::from(unescape_mount_field(mount_point)?),
                file_system: file_system.to_owned(),
            })
        })
        .collect()
}

pub fn inspect_local_filesystem<'a>(
    path: &Path,
    mounts: &'a [MountInfo],
) -> Result<&'a MountInfo, PlatformError> {
    let mount = mounts
        .iter()
        .filter(|mount| path.starts_with(&mount.mount_point))
        .max_by_key(|mount| mount.mount_point.as_os_str().len())
        .ok_or(PlatformError::Missing("mount point"))?;
    if matches!(
        mount.file_system.as_str(),
        "nfs" | "nfs4" | "cifs" | "smb3" | "fuse.sshfs"
    ) {
        return Err(PlatformError::InvalidData {
            field: "local_filesystem",
            value: mount.file_system.clone(),
        });
    }
    Ok(mount)
}

fn unescape_mount_field(value: &str) -> Result<String, PlatformError> {
    let mut output = String::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied().peekable();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            output.push(char::from(byte));
            continue;
        }
        let digits: String = (0..3)
            .map(|_| bytes.next())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| PlatformError::InvalidData {
                field: "mountinfo_escape",
                value: value.to_owned(),
            })?
            .into_iter()
            .map(char::from)
            .collect();
        let decoded = u8::from_str_radix(&digits, 8).map_err(|_| PlatformError::InvalidData {
            field: "mountinfo_escape",
            value: value.to_owned(),
        })?;
        output.push(char::from(decoded));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_mount_wins_and_network_storage_is_rejected() {
        let mounts = parse_mountinfo(
            "1 0 8:1 / / rw - ext4 /dev/a rw\n2 1 0:2 / /data rw - nfs server:/data rw",
        )
        .unwrap();
        assert!(inspect_local_filesystem(Path::new("/tmp/x"), &mounts).is_ok());
        assert!(inspect_local_filesystem(Path::new("/data/x"), &mounts).is_err());
    }
}
