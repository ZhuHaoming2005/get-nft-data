use dedup_model::ExecutionMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceEstimate {
    pub fixed_bytes: u64,
    pub variable_bytes: u64,
    pub hottest_group_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourcePlan {
    pub mode: ExecutionMode,
    pub predicted_peak_bytes: u64,
    pub hot_group_spill: bool,
}

impl ResourcePlan {
    pub fn choose(
        requested: ExecutionMode,
        estimate: ResourceEstimate,
        admission_limit: u64,
        stage_limit: u64,
    ) -> Self {
        let predicted_peak_bytes = estimate.fixed_bytes.saturating_add(estimate.variable_bytes);
        let automatic = if predicted_peak_bytes <= admission_limit {
            ExecutionMode::InMemory
        } else if estimate.fixed_bytes <= stage_limit {
            ExecutionMode::Hybrid
        } else {
            ExecutionMode::External
        };
        Self {
            mode: match requested {
                ExecutionMode::Auto => automatic,
                explicit => explicit,
            },
            predicted_peak_bytes,
            hot_group_spill: estimate.hottest_group_bytes > admission_limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_capacities_select_all_modes() {
        let select = |fixed, variable| {
            ResourcePlan::choose(
                ExecutionMode::Auto,
                ResourceEstimate {
                    fixed_bytes: fixed,
                    variable_bytes: variable,
                    hottest_group_bytes: 0,
                },
                500,
                1_000,
            )
            .mode
        };
        assert_eq!(select(100, 100), ExecutionMode::InMemory);
        assert_eq!(select(400, 400), ExecutionMode::Hybrid);
        assert_eq!(select(1_100, 0), ExecutionMode::External);
    }
}
