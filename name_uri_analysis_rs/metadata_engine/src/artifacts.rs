//! Revisioned metadata artifact paths generated from one source of truth.

use std::path::{Path, PathBuf};

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
    }
}
