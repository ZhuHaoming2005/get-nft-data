use super::*;

pub(crate) fn run_uri_analysis(
    conn: &Connection,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    let mut rows = Vec::new();
    let stage_steps = if chains.len() > 1 { 3 } else { 2 };
    progress.start_stage("analyzing URI duplicates", stage_steps);
    let total_for = |chain: &str| {
        totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        })
    };
    let include_cross_chain = chains.len() > 1;
    progress.start_task("loading URI contract counts", None, "rows");
    let contract_counts = load_uri_contract_counts(conn, include_cross_chain)?;
    progress.advance_task(contract_counts.len() as u64, ProgressCounters::default());
    progress.finish_task("URI contract counts loaded");
    progress.step_stage("loaded URI contract counts");

    let summary_units = chains.len() + if include_cross_chain { chains.len() } else { 0 };
    progress.start_task(
        "building URI chain summaries",
        Some(summary_units as u64),
        "summaries",
    );

    for chain in chains {
        let counts = contract_counts
            .get(chain)
            .copied()
            .unwrap_or_default()
            .intra_chain;
        push_uri_rows(
            &mut rows,
            "intra_chain",
            chain,
            "",
            "norm_cross",
            total_for(chain),
            counts,
        );
        progress.advance_task(1, ProgressCounters::default());
    }

    if include_cross_chain {
        for chain in chains {
            let counts = contract_counts
                .get(chain)
                .copied()
                .unwrap_or_default()
                .cross_chain;
            push_uri_rows(
                &mut rows,
                "cross_chain_summary",
                chain,
                "",
                "norm_cross",
                total_for(chain),
                counts,
            );
            progress.advance_task(1, ProgressCounters::default());
        }

        progress.finish_task("URI chain summaries ready");
        progress.step_stage("built URI chain summaries");

        progress.start_task(
            "building URI directed chain matrix",
            Some((chains.len() * (chains.len() - 1)) as u64),
            "pairs",
        );
        let pair_counts = load_uri_chain_pair_counts(conn)?;
        for primary in chains {
            for secondary in chains {
                if primary == secondary {
                    continue;
                }
                let counts = pair_counts
                    .get(primary)
                    .and_then(|secondary_counts| secondary_counts.get(secondary))
                    .copied()
                    .unwrap_or_default();
                push_uri_rows(
                    &mut rows,
                    "chain_matrix",
                    primary,
                    secondary,
                    "norm_cross",
                    total_for(primary),
                    counts,
                );
                progress.advance_task(1, ProgressCounters::default());
            }
        }
        progress.finish_task("URI directed chain matrix ready");
        progress.step_stage("built URI directed chain matrix");
    } else {
        progress.finish_task("URI chain summaries ready");
        progress.step_stage("built URI chain summaries");
    }

    debug_assert_eq!(
        {
            let cross_chain_steps = if chains.len() > 1 {
                chains.len() + chains.len() * (chains.len() - 1)
            } else {
                0
            };
            chains.len() + cross_chain_steps
        },
        summary_units
            + if include_cross_chain {
                chains.len() * (chains.len() - 1)
            } else {
                0
            }
    );
    progress.finish_stage("URI analysis complete");
    Ok(rows)
}

pub(crate) fn push_uri_rows(
    rows: &mut Vec<SummaryRow>,
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    match_mode: &str,
    total: NameTotals,
    counts: UriCounts,
) {
    for (metric, duplicate_nfts, duplicate_contracts) in [
        ("v1", counts.v1_nfts, counts.v1_contracts),
        ("v2", counts.v2_nfts, counts.v2_contracts),
        ("v3", counts.v3_nfts, counts.v3_contracts),
    ] {
        rows.push(summary_row(
            SummarySpec {
                field_name: "uri",
                scope,
                primary_chain,
                secondary_chain,
                threshold: None,
                match_mode,
                metric,
                total_contracts: total.contracts,
                total_nfts: total.nfts,
            },
            GroupSummary {
                duplicate_contract_count: duplicate_contracts,
                duplicate_nft_count: duplicate_nfts,
                ..GroupSummary::default()
            },
        ));
    }
}
