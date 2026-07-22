//! Golden-ish coverage for the `AggregateState` -> markdown/json report
//! surface: asserts that the pricing-policy documentation and the
//! stuck/honest-loss/economics USD fields keep a stable shape (field
//! presence and key wording), so accidental renames or removals of these
//! business-critical fields are caught even without comparing full byte
//! output against a fixture file.

use analysis::model::{
    AggregateDelta, BehaviorFacts, CandidateId, ChainId, ContractKey, EconomicFacts,
    EvidenceQuality, NftSelection, RelationDelta, SeedId,
};
use analysis::reporting::{json, markdown, AggregateState};

fn economics_delta() -> EconomicFacts {
    EconomicFacts {
        gross_revenue_usd_micros: 5_000_000,
        operator_output_usd_micros: 4_500_000,
        secondary_sale_loss_usd_micros: 1_200_000,
        paid_mint_loss_usd_micros: 300_000,
        honest_loss_native: 42,
        honest_loss_usd_micros: 1_500_000,
        stuck_nft_count: 3,
        stuck_time_numerator_seconds: 600,
        stuck_time_denominator_seconds: 3,
        ..EconomicFacts::default()
    }
}

fn populated_state() -> AggregateState {
    let mut state = AggregateState::default();
    let candidate = ContractKey::new(ChainId::Ethereum, "0xgolden");
    let economics = economics_delta();
    let relation = RelationDelta {
        seed_id: SeedId(0),
        seed_chain: ChainId::Ethereum,
        candidate_id: CandidateId(1),
        candidate: candidate.clone(),
        selection: NftSelection::Explicit { nfts: Vec::new() },
        suspected: true,
        economics: economics.clone(),
        behaviors: BehaviorFacts::default(),
    };
    state
        .merge_once(AggregateDelta {
            candidate_id: CandidateId(1),
            analysis_complete: true,
            relation_deltas: vec![relation],
            suspected_economics: economics,
            suspected_behaviors: BehaviorFacts::default(),
            behavior_entities: Vec::new(),
            matrix_suspected: std::collections::BTreeMap::new(),
            candidate_quality: EvidenceQuality::default(),
            global_address_roles: Vec::new(),
            global_nft_ids: Vec::new(),
            global_transaction_ids: Vec::new(),
        })
        .unwrap();
    state
}

#[test]
fn aggregate_snapshot_carries_stuck_and_honest_loss_usd_fields_through_json() {
    let snapshot = populated_state().snapshot();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all_chains.json");
    json::write_json(&path, &snapshot).unwrap();
    let payload: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();

    // Stable JSON shape: these are the exact paths the frontend/analysts key
    // off of. A rename here must be a deliberate, reviewed change.
    for pointer in [
        "/economics/honest_loss_usd_micros",
        "/economics/honest_loss_native",
        "/economics/secondary_sale_loss_usd_micros",
        "/economics/paid_mint_loss_usd_micros",
        "/economics/stuck_nft_count",
        "/economics/stuck_time_numerator_seconds",
        "/economics/stuck_time_denominator_seconds",
        "/economics_derived/attacker_gas_usd_micros",
        "/economics_derived/stuck_nft_ratio",
        "/economics_derived/stuck_time_ratio",
        "/data_quality/failure_records",
    ] {
        assert!(
            payload.pointer(pointer).is_some(),
            "expected stable field at {pointer} in aggregate snapshot JSON, got {payload}"
        );
    }

    assert_eq!(
        payload
            .pointer("/economics/honest_loss_usd_micros")
            .unwrap(),
        &serde_json::json!(1_500_000_i128)
    );
    assert_eq!(
        payload.pointer("/economics/stuck_nft_count").unwrap(),
        &serde_json::json!(3)
    );
    // stuck_time_ratio = numerator / denominator = 600 / 3 = 200.0
    assert_eq!(
        payload
            .pointer("/economics_derived/stuck_time_ratio")
            .unwrap()
            .as_f64()
            .unwrap(),
        200.0
    );
}

#[test]
fn markdown_summary_documents_pricing_policy_and_economics_terms() {
    let snapshot = populated_state().snapshot();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all_chains.md");
    markdown::write_all_chains(&path, &snapshot).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();

    // The markdown summary embeds the pricing-policy documentation path
    // inline (same-UTC-day Alchemy historical price, USD-only cross-chain
    // aggregation) alongside the stuck/honest-loss economics fields; both
    // must keep appearing so operators do not lose this context silently.
    for expected in [
        "计价口径",
        "Alchemy",
        "CopyMint",
        "诚实损失（USD micros）",
        "套牢 NFT",
        "套牢 NFT 比例",
        "套牢时间比例",
        "1500000",
    ] {
        assert!(
            text.contains(expected),
            "expected markdown summary to contain {expected:?}, got:\n{text}"
        );
    }
}
