use std::fs;
use std::io;
use std::path::Path;

/// Parses the resident-set size from Linux `/proc/<pid>/status` content.
#[must_use]
pub fn parse_resident_memory_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_ascii_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

/// Returns this process's resident-set size when the host exposes it.
#[must_use]
pub fn process_resident_memory_bytes() -> Option<u64> {
    process_resident_memory_bytes_impl()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageFaults {
    pub minor: u64,
    pub major: u64,
}

/// Parses cumulative minor and major faults from Linux `/proc/<pid>/stat`.
#[must_use]
pub fn parse_page_faults(stat: &str) -> Option<PageFaults> {
    let fields = stat.get(stat.rfind(')')?.saturating_add(1)..)?;
    let mut fields = fields.split_ascii_whitespace();
    let minor = fields.nth(7)?.parse().ok()?;
    let major = fields.nth(1)?.parse().ok()?;
    Some(PageFaults { minor, major })
}

/// Returns cumulative process page faults when `/proc` exposes them.
#[must_use]
pub fn process_page_faults() -> Option<PageFaults> {
    process_page_faults_impl()
}

#[cfg(target_os = "linux")]
fn process_resident_memory_bytes_impl() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    parse_resident_memory_bytes(&status)
}

#[cfg(not(target_os = "linux"))]
fn process_resident_memory_bytes_impl() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn process_page_faults_impl() -> Option<PageFaults> {
    parse_page_faults(&fs::read_to_string("/proc/self/stat").ok()?)
}

#[cfg(not(target_os = "linux"))]
fn process_page_faults_impl() -> Option<PageFaults> {
    None
}

/// Replaces `destination` with `source` using the strongest operation offered
/// by the current standard-library platform implementation.
///
/// # Errors
///
/// Returns an I/O error if an existing Windows destination cannot be removed
/// or if the final rename fails.
pub fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    replace_file_impl(source, destination)
}

#[cfg(target_os = "windows")]
fn replace_file_impl(source: &Path, destination: &Path) -> io::Result<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)
}

#[cfg(not(target_os = "windows"))]
fn replace_file_impl(source: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_resident_memory() {
        assert_eq!(
            parse_resident_memory_bytes("Name:\tdedup\nVmRSS:\t   1234 kB\n"),
            Some(1_263_616)
        );
        assert_eq!(parse_resident_memory_bytes("VmRSS:\tunknown kB\n"), None);
    }

    #[test]
    fn parses_linux_page_faults_even_when_command_contains_spaces() {
        let stat = "123 (dedup worker) S 1 2 3 4 5 6 77 8 9 11 12";
        assert_eq!(
            parse_page_faults(stat),
            Some(PageFaults {
                minor: 77,
                major: 9
            })
        );
    }
}
