pub(crate) const PIPELINE_SCHEMA_VERSION: u32 = 5;
// Any semantic change to a resumable stage must bump that stage's revision;
// the controller invalidates only the affected checkpoint and its dependents.
pub(crate) const PREPARE_STAGE_REVISION: u32 = 2;
pub(crate) const NAME_STAGE_REVISION: u32 = 1;
pub(crate) const METADATA_ENCODE_STAGE_REVISION: u32 = 5;
pub(crate) const METADATA_MATCH_STAGE_REVISION: u32 = 16;
pub(crate) const FINALIZER_STAGE_REVISION: u32 = 1;
pub(crate) const PARENT_LIVENESS_ENV: &str = "NAME_URI_ANALYSIS_PARENT_LIVENESS_PIPE";
pub(crate) const PHASE_GENERATION_ENV: &str = "NAME_URI_ANALYSIS_PHASE_GENERATION";
