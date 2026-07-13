use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use rayon::prelude::*;

use crate::currency::FALLBACK_ETH_USD_RATE;
use crate::models::{
    normalize_chain_identity, DuplicateCandidate, DuplicateContractPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftPropagationEdgePayload, NftPropagationPathPayload,
    PaperAddressClassificationPayload, PaperAttackerCostDetailPayload, PaperAttackerCostPayload,
    PaperBehaviorSummaryRowPayload, PaperContractBehaviorStatsPayload,
    PaperContractWashCycleSizePayload, PaperDataQualityPayload, PaperDuplicateScaleRowPayload,
    PaperHonestBuyerRowPayload, PaperHonestLossPayload, PaperInventoryConcentrationRowPayload,
    PaperLayeredTransferRowPayload, PaperOutputInputRatioRowPayload,
    PaperOutputInputSummaryPayload, PaperPumpExitRowPayload, PaperStarBehaviorRowPayload,
    PaperStatsPayload, PaperWashCycleSizeRowPayload, PaperWashTradingRowPayload,
    SeedCollectionStatsPayload, ValueFlowEdgePayload, VictimAcquisitionAddressPayload,
    ZERO_ADDRESS,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PaperStatsConfig {
    pub min_cycle_size: usize,
    pub min_path_length: usize,
    pub center_fanout_threshold: usize,
    pub concentration_top_pct: f64,
    pub analysis_timestamp: i64,
}

impl Default for PaperStatsConfig {
    fn default() -> Self {
        Self {
            min_cycle_size: 2,
            min_path_length: 3,
            center_fanout_threshold: 3,
            concentration_top_pct: 0.1,
            analysis_timestamp: 0,
        }
    }
}

impl PaperStatsConfig {
    fn min_cycle_size(self) -> usize {
        self.min_cycle_size.max(2)
    }

    fn min_path_length(self) -> usize {
        self.min_path_length.max(2)
    }

    fn fanout_threshold(self) -> usize {
        self.center_fanout_threshold.max(1)
    }

    fn top_contract_count(self, total_contract_count: usize) -> usize {
        if total_contract_count == 0 {
            return 0;
        }
        let pct = if self.concentration_top_pct.is_finite() {
            self.concentration_top_pct.clamp(0.0, 1.0)
        } else {
            PaperStatsConfig::default().concentration_top_pct
        };
        ((total_contract_count as f64 * pct).ceil() as usize)
            .max(1)
            .min(total_contract_count)
    }
}

pub struct PaperStatsInput<'a> {
    pub config: PaperStatsConfig,
    pub seed_collection_stats: &'a SeedCollectionStatsPayload,
    pub duplicate_candidates: &'a [DuplicateCandidate],
    pub duplicate_contracts: &'a [DuplicateContractPayload],
    pub legit_duplicates: &'a [DuplicateContractPayload],
    pub infringing_tokens: &'a [InfringingTokenRecord],
    pub malicious_addresses: &'a [MaliciousAddressPayload],
    pub victim_acquisition_addresses: &'a [VictimAcquisitionAddressPayload],
    pub value_flow_edges: &'a [ValueFlowEdgePayload],
    pub nft_propagation_paths: &'a BTreeMap<String, NftPropagationPathPayload>,
}

mod attacker_cost;
mod behavior;
mod build;
mod duplicate_scale;
mod honest_loss;
mod merge;
mod types;

use attacker_cost::*;
use behavior::*;
use duplicate_scale::*;
use honest_loss::*;
use types::*;

pub use build::build_paper_stats;
pub use merge::merge_paper_stats;
