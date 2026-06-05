fn run_uri_analysis(
    conn: &Connection,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    let mut rows = Vec::new();
    let uri_steps = chains.len();
    progress.start_phase("analyzing URI duplicates", uri_steps as u64);
    for chain in chains {
        let counts = uri_counts_from_contract_flags(conn, chain, "norm_contract")?;
        let total = totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        push_uri_rows(
            &mut rows,
            "intra_chain",
            chain,
            "",
            "norm_cross",
            total,
            counts,
        );
        progress.step(format!("URI intra {chain} norm_cross"));
    }
    progress.finish_phase("URI analysis complete");
    Ok(rows)
}

fn uri_counts_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<UriCounts> {
    Ok(UriCounts {
        v1_nfts: row.get(0)?,
        v1_contracts: row.get(1)?,
        v2_nfts: row.get(2)?,
        v2_contracts: row.get(3)?,
        v3_nfts: row.get(4)?,
        v3_contracts: row.get(5)?,
    })
}

fn push_uri_rows(
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
