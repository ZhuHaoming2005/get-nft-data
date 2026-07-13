use super::*;

#[test]
fn shared_token_group_progress_combines_prior_and_live_counters() {
    let base = ProgressCounters {
        groups: 2,
        candidates: 100,
        scored: 50,
        matched: 4,
    };
    let live = MetadataContentUnionStats {
        atom_count: 10,
        candidate_pairs: 25,
        scored_pairs: 8,
        matched_pairs: 2,
        template_candidate_pairs: 0,
        template_scored_pairs: 0,
        template_matched_pairs: 0,
        ..MetadataContentUnionStats::default()
    };

    assert_eq!(
        metadata_shared_token_group_progress_counters(3, base, &live),
        ProgressCounters {
            groups: 3,
            candidates: 125,
            scored: 58,
            matched: 6,
        }
    );
}

#[test]
fn metadata_pair_progress_message_shows_throughput_and_eta() {
    assert_eq!(
        metadata_pair_progress_message(333, 2, 6, 7, std::time::Duration::from_secs(2)),
        "metadata candidate pairs scored 333; left docs 2/6; estimated remaining 666; throughput 166.5 pairs/s; ETA 4s; matched doc pairs 7"
    );
}

#[test]
fn metadata_pair_progress_message_uses_unknown_eta_before_first_scored_pair() {
    assert_eq!(
        metadata_pair_progress_message(0, 0, 6, 0, std::time::Duration::from_secs(0)),
        "metadata candidate pairs scored 0; left docs 0/6; estimated remaining 0; throughput n/a; ETA n/a; matched doc pairs 0"
    );
}

#[test]
fn metadata_scoring_progress_units_track_left_docs_not_candidate_pairs() {
    assert_eq!(metadata_scoring_progress_units(10), 10);
    assert_eq!(metadata_scoring_batch_progress_units(2, 7), 5);
}
