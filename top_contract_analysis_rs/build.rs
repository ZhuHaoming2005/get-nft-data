use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = std::fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", root.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("failed to enumerate {}: {error}", root.display()));
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn main() {
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=Rstrtmgr");
    }

    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"),
    );
    let mut files = vec![
        manifest_dir.join("Cargo.toml"),
        manifest_dir.join("build.rs"),
    ];
    let workspace_lock = manifest_dir
        .parent()
        .map(|parent| parent.join("Cargo.lock"))
        .filter(|path| path.is_file());
    if let Some(path) = &workspace_lock {
        files.push(path.clone());
    }
    collect_files(&manifest_dir.join("src"), &mut files);
    files.sort();

    let mut hasher = Sha256::new();
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        let relative = if workspace_lock.as_ref() == Some(&path) {
            "../Cargo.lock".into()
        } else {
            path.strip_prefix(&manifest_dir)
                .unwrap_or(&path)
                .to_string_lossy()
        };
        let contents = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        hasher.update((relative.len() as u64).to_le_bytes());
        hasher.update(relative.as_bytes());
        hasher.update((contents.len() as u64).to_le_bytes());
        hasher.update(contents);
    }
    let fingerprint = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!("cargo:rustc-env=TCA_BUILD_FINGERPRINT={fingerprint}");
}
