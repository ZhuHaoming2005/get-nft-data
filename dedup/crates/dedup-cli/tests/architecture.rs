use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn local_dependency_direction_is_acyclic_and_layered() {
    let root = workspace_root();
    let allowed = BTreeMap::from([
        ("dedup-model", BTreeSet::new()),
        ("dedup-linux", BTreeSet::new()),
        ("dedup-storage", BTreeSet::from(["dedup-model"])),
        (
            "dedup-index",
            BTreeSet::from(["dedup-model", "dedup-storage"]),
        ),
        (
            "dedup-engine",
            BTreeSet::from(["dedup-index", "dedup-model"]),
        ),
        ("dedup-report", BTreeSet::from(["dedup-model"])),
        (
            "dedup-cli",
            BTreeSet::from([
                "dedup-engine",
                "dedup-index",
                "dedup-linux",
                "dedup-model",
                "dedup-report",
                "dedup-storage",
            ]),
        ),
    ]);
    for (crate_name, expected) in allowed {
        let manifest = root.join("crates").join(crate_name).join("Cargo.toml");
        let content = fs::read_to_string(manifest).unwrap();
        let parsed: toml::Value = toml::from_str(&content).unwrap();
        let actual: BTreeSet<_> = parsed
            .get("dependencies")
            .and_then(toml::Value::as_table)
            .into_iter()
            .flat_map(|dependencies| dependencies.keys())
            .filter(|name| name.starts_with("dedup-"))
            .map(String::as_str)
            .collect();
        assert_eq!(
            actual, expected,
            "invalid local dependencies for {crate_name}"
        );
    }
}

#[test]
fn platform_cfg_and_unsafe_stay_in_tested_wrappers() {
    let root = workspace_root();
    for crate_name in [
        "dedup-model",
        "dedup-storage",
        "dedup-index",
        "dedup-engine",
        "dedup-report",
        "dedup-cli",
    ] {
        for source in rust_sources(&root.join("crates").join(crate_name).join("src")) {
            let content = fs::read_to_string(&source).unwrap();
            assert!(
                !content.contains("cfg(target_os = \"linux\")"),
                "business crate contains Linux cfg: {}",
                source.display()
            );
            if content.contains("unsafe {") {
                assert!(
                    source.ends_with("mmap.rs"),
                    "unsafe appears outside mmap/platform wrapper: {}",
                    source.display()
                );
            }
        }
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_owned()
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_owned()];
    let mut output = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                output.push(path);
            }
        }
    }
    output
}
