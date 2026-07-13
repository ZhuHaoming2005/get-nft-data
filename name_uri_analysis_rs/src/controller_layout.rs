use std::path::{Path, PathBuf};

pub(crate) fn resolve_directory_layout(
    work_directory: &Path,
    output_directory: &Path,
) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let work_directory = normalize_layout_path(work_directory)?;
    let output_directory = normalize_layout_path(output_directory)?;
    if path_is_same_or_descendant(&output_directory, &work_directory) {
        return Err(format!(
            "--output-dir {} cannot be inside --work-directory {}; successful cleanup would delete the outputs",
            output_directory.display(),
            work_directory.display()
        )
        .into());
    }
    Ok((work_directory, output_directory))
}

/// Resolve every existing component (including directory symlinks/junctions)
/// while retaining a normalized suffix for paths that have not been created
/// yet. Resolving incrementally preserves filesystem semantics for `link/..`.
pub(crate) fn normalize_layout_path(path: &Path) -> std::io::Result<PathBuf> {
    use std::path::Component;

    let absolute = std::path::absolute(path)?;
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(part) => {
                resolved.push(part);
                if resolved.exists() {
                    resolved = resolved.canonicalize()?;
                }
            }
        }
    }
    Ok(resolved)
}

pub(crate) fn path_is_same_or_descendant(path: &Path, ancestor: &Path) -> bool {
    let mut path_components = path.components();
    for ancestor_component in ancestor.components() {
        let Some(path_component) = path_components.next() else {
            return false;
        };
        if !path_components_equal(path_component.as_os_str(), ancestor_component.as_os_str()) {
            return false;
        }
    }
    true
}

#[cfg(windows)]
pub(crate) fn path_components_equal(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left.to_string_lossy()
        .eq_ignore_ascii_case(&right.to_string_lossy())
}

#[cfg(not(windows))]
pub(crate) fn path_components_equal(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left == right
}
