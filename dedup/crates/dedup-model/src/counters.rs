use crate::DedupError;
use serde::{Deserialize, Serialize};

macro_rules! stage_counters {
    ($($name:ident),+ $(,)?) => {
        #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        pub struct StageCounters {
            $(pub $name: u64,)+
        }

        impl StageCounters {
            $(
                pub fn $name(&mut self, amount: u64) -> Result<(), DedupError> {
                    self.$name = self.$name.checked_add(amount).ok_or(
                        DedupError::CounterOverflow { counter: stringify!($name) }
                    )?;
                    Ok(())
                }
            )+

            pub fn merge(&mut self, other: &Self) -> Result<(), DedupError> {
                $(self.$name(other.$name)?;)+
                Ok(())
            }
        }
    };
}

stage_counters!(
    rows_scanned,
    entity_digest_bucket_max,
    entity_radix_handle_touches,
    uri_spilled_members,
    uri_radix_handle_touches,
    uri_member_accesses,
    uri_bitmap_word_operations,
    name_atoms,
    name_canonical_values,
    name_posting_entries,
    name_posting_touches,
    name_scored_candidates,
    name_matched_pairs,
    metadata_anchor_documents,
    metadata_template_features,
    metadata_low_information_contracts,
    metadata_prefilter_probes,
    metadata_prefilter_candidates,
    metadata_radix_handle_touches,
    metadata_verify_pairs,
    token_id_comparisons,
    bm25_term_comparisons,
    hit_events,
    spill_bytes,
);
