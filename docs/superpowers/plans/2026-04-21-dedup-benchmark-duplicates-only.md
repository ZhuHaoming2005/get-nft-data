# Dedup Benchmark Duplicates-Only Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace top-k candidate reporting with duplicates-only reporting, driven by per-algorithm duplicate decision rules stored in a dedicated module.

**Architecture:** Keep the existing benchmark execution and timing flow, but replace score aggregation with rule-based duplicate filtering. Introduce a dedicated decision-rules module, thread it through ordinary and reference algorithms, then rename the report model and CLI/docs to remove `top_k` semantics entirely.

**Tech Stack:** Rust 2021, Rayon, Clap, Serde, DuckDB, assert_cmd

---

### File Map

**Create:**
- `dedup_bench_rs/src/decision_rules.rs`
- `docs/superpowers/specs/2026-04-21-dedup-benchmark-duplicates-only-design.md` (already written)

**Modify:**
- `dedup_bench_rs/src/algorithms.rs`
- `dedup_bench_rs/src/benchmark.rs`
- `dedup_bench_rs/src/report.rs`
- `dedup_bench_rs/src/lib.rs`
- `dedup_bench_rs/src/main.rs`
- `dedup_bench_rs/tests/cli.rs`
- `dedup_bench_rs/README.md`

**Primary test targets:**
- `cargo test algorithms::tests::duplicates`
- `cargo test benchmark::tests::repeat_changes_only_timing_not_candidate_sets`
- `cargo test cli_writes_json_and_markdown_outputs`
- `cargo test`

### Task 1: Add Rule Definitions Module

**Files:**
- Create: `dedup_bench_rs/src/decision_rules.rs`
- Modify: `dedup_bench_rs/src/lib.rs`
- Test: `dedup_bench_rs/src/decision_rules.rs`

- [ ] **Step 1: Write the failing rule-lookup tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_algorithms_have_explicit_duplicate_rules() {
        let rule = duplicate_score_rule("name_jaro_winkler").unwrap();
        assert_eq!(rule.algorithm_id, "name_jaro_winkler");
        assert_eq!(rule.threshold, 97.0);
    }

    #[test]
    fn missing_algorithm_rule_is_reported() {
        let err = duplicate_score_rule("missing_algorithm").unwrap_err();
        assert!(err.contains("missing duplicate rule"));
    }

    #[test]
    fn reference_rule_exposes_named_thresholds() {
        let rule = reference_duplicate_rule();
        assert_eq!(rule.name_threshold, 95.0);
        assert_eq!(rule.metadata_threshold, 0.55);
    }
}
```

- [ ] **Step 2: Run the rule test to verify it fails**

Run: `cargo test ordinary_algorithms_have_explicit_duplicate_rules`
Expected: FAIL with missing module / missing functions

- [ ] **Step 3: Write the minimal rules module**

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DuplicateScoreRule {
    pub algorithm_id: &'static str,
    pub threshold: f64,
    pub description: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReferenceDuplicateRule {
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub description: &'static str,
}

const ORDINARY_RULES: &[DuplicateScoreRule] = &[
    DuplicateScoreRule { algorithm_id: "name_exact_normalized", threshold: 100.0, description: "score >= 100.0" },
    DuplicateScoreRule { algorithm_id: "name_jaro_winkler", threshold: 97.0, description: "score >= 97.0" },
    DuplicateScoreRule { algorithm_id: "name_normalized_levenshtein", threshold: 96.0, description: "score >= 96.0" },
    DuplicateScoreRule { algorithm_id: "name_trigram_jaccard", threshold: 90.0, description: "score >= 90.0" },
    DuplicateScoreRule { algorithm_id: "name_current_hybrid", threshold: 95.0, description: "score >= 95.0" },
    DuplicateScoreRule { algorithm_id: "metadata_token_jaccard", threshold: 0.80, description: "score >= 0.80" },
    DuplicateScoreRule { algorithm_id: "metadata_jaro_winkler_doc", threshold: 0.92, description: "score >= 0.92" },
    DuplicateScoreRule { algorithm_id: "metadata_trigram_jaccard_doc", threshold: 0.75, description: "score >= 0.75" },
    DuplicateScoreRule { algorithm_id: "metadata_token_cosine", threshold: 0.85, description: "score >= 0.85" },
    DuplicateScoreRule { algorithm_id: "metadata_current_hybrid", threshold: 0.55, description: "score >= 0.55" },
];

const REFERENCE_RULE: ReferenceDuplicateRule = ReferenceDuplicateRule {
    name_threshold: 95.0,
    metadata_threshold: 0.55,
    description: "name_score >= 95.0 OR metadata_score >= 0.55",
};

pub fn duplicate_score_rule(algorithm_id: &str) -> Result<DuplicateScoreRule, String> {
    ORDINARY_RULES
        .iter()
        .copied()
        .find(|rule| rule.algorithm_id == algorithm_id)
        .ok_or_else(|| format!("missing duplicate rule for algorithm_id={algorithm_id}"))
}

pub fn reference_duplicate_rule() -> ReferenceDuplicateRule {
    REFERENCE_RULE
}
```

- [ ] **Step 4: Export the new module**

```rust
pub mod decision_rules;
```

- [ ] **Step 5: Run the rule tests to verify they pass**

Run: `cargo test decision_rules`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add dedup_bench_rs/src/decision_rules.rs dedup_bench_rs/src/lib.rs
git commit -m "feat: add duplicate decision rules module"
```

### Task 2: Replace Ordinary Algorithm Top-K Aggregation with Duplicates Filtering

**Files:**
- Modify: `dedup_bench_rs/src/algorithms.rs`
- Test: `dedup_bench_rs/src/algorithms.rs`

- [ ] **Step 1: Write failing ordinary-duplicate tests**

```rust
#[test]
fn duplicate_candidates_apply_per_algorithm_thresholds() {
    let rows = vec![row_raw(), second_row_raw()];
    let scores = vec![98.0, 88.0];

    let (_, duplicates) =
        build_duplicate_candidates_raw("name_jaro_winkler", &rows, &scores).unwrap();

    assert_eq!(duplicates.len(), 1);
    assert_eq!(duplicates[0].contract_address, "0xdup");
}

#[test]
fn duplicate_candidates_are_not_top_k_truncated() {
    let rows = vec![row_raw(), second_row_raw()];
    let scores = vec![0.90, 0.85];

    let (_, duplicates) =
        build_duplicate_candidates_raw("metadata_token_jaccard", &rows, &scores).unwrap();

    assert_eq!(duplicates.len(), 2);
}
```

- [ ] **Step 2: Run one new ordinary-duplicate test to verify it fails**

Run: `cargo test duplicate_candidates_apply_per_algorithm_thresholds`
Expected: FAIL with missing duplicate aggregation helper

- [ ] **Step 3: Introduce duplicates-only aggregation helpers**

```rust
pub fn build_duplicate_candidates_raw(
    algorithm_id: &str,
    rows: &[FeatureRow],
    scored: &[f64],
) -> Result<(usize, Vec<CandidateScore>), String> {
    let rule = duplicate_score_rule(algorithm_id)?;
    let mut duplicates: Vec<(usize, f64)> = scored
        .iter()
        .enumerate()
        .filter_map(|(index, score)| (*score >= rule.threshold).then_some((index, *score)))
        .collect();

    duplicates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| rows[left.0].contract_address.cmp(&rows[right.0].contract_address))
            .then_with(|| rows[left.0].token_id.cmp(&rows[right.0].token_id))
    });

    let duplicate_count = duplicates.len();
    let duplicates = duplicates
        .into_iter()
        .enumerate()
        .map(|(index, (row_index, score))| CandidateScore {
            rank: index + 1,
            contract_address: rows[row_index].contract_address.clone(),
            token_id: rows[row_index].token_id.clone(),
            name: rows[row_index].name.clone(),
            score,
        })
        .collect();

    Ok((duplicate_count, duplicates))
}
```

- [ ] **Step 4: Rename ordinary report semantics in `algorithms.rs`**

```rust
pub struct AlgorithmReport {
    pub algorithm_id: String,
    pub field: AlgorithmField,
    pub decision_rule: String,
    pub repeat: usize,
    pub runs_ms: Vec<f64>,
    pub avg_ms: f64,
    pub min_ms: f64,
    pub duplicate_count: usize,
    pub duplicates: Vec<CandidateScore>,
}
```

- [ ] **Step 5: Run targeted algorithm tests**

Run: `cargo test duplicate_candidates`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add dedup_bench_rs/src/algorithms.rs
git commit -m "feat: use duplicate filtering for ordinary algorithms"
```

### Task 3: Replace Reference Top-K Aggregation with Rule-Driven Duplicates

**Files:**
- Modify: `dedup_bench_rs/src/algorithms.rs`
- Test: `dedup_bench_rs/src/algorithms.rs`

- [ ] **Step 1: Write failing reference-duplicate tests**

```rust
#[test]
fn raw_reference_duplicates_use_rule_module_thresholds() {
    let sample = sample_raw();
    let rows = vec![row_raw(), second_row_raw()];
    let name_scores = vec![96.0, 70.0];
    let metadata_scores = vec![0.20, 0.60];

    let (_, duplicates) =
        build_reference_duplicates_raw_from_scores(&rows, &name_scores, &metadata_scores);

    assert_eq!(duplicates.len(), 2);
}

#[test]
fn raw_reference_duplicates_exclude_non_matches() {
    let rows = vec![row_raw()];
    let name_scores = vec![20.0];
    let metadata_scores = vec![0.10];

    let (_, duplicates) =
        build_reference_duplicates_raw_from_scores(&rows, &name_scores, &metadata_scores);

    assert!(duplicates.is_empty());
}
```

- [ ] **Step 2: Run one new reference test to verify it fails**

Run: `cargo test raw_reference_duplicates_use_rule_module_thresholds`
Expected: FAIL with missing duplicates helper / wrong report semantics

- [ ] **Step 3: Replace reference candidate builders with duplicates builders**

```rust
pub fn build_reference_duplicates_raw_from_scores(
    rows: &[FeatureRow],
    name_scores: &[f64],
    metadata_scores: &[f64],
) -> (usize, Vec<ReferenceCandidateScore>) {
    let rule = reference_duplicate_rule();
    let mut duplicates = rows
        .iter()
        .zip(name_scores.iter().copied())
        .zip(metadata_scores.iter().copied())
        .filter_map(|((row, name_score), metadata_score)| {
            let mut reasons = Vec::new();
            if name_score >= rule.name_threshold {
                reasons.push("name_match".to_string());
            }
            if metadata_score >= rule.metadata_threshold {
                reasons.push("metadata_match".to_string());
            }
            if reasons.is_empty() {
                return None;
            }
            Some((row, (name_score / 100.0).max(metadata_score), name_score, metadata_score, reasons))
        })
        .collect::<Vec<_>>();

    duplicates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.0.contract_address.cmp(&right.0.contract_address))
            .then_with(|| left.0.token_id.cmp(&right.0.token_id))
    });

    let duplicate_count = duplicates.len();
    let duplicates = duplicates
        .into_iter()
        .enumerate()
        .map(|(index, (row, combined_score, name_score, metadata_score, reasons))| ReferenceCandidateScore {
            rank: index + 1,
            contract_address: row.contract_address.clone(),
            token_id: row.token_id.clone(),
            name: row.name.clone(),
            combined_score,
            name_score,
            metadata_score,
            match_reasons: reasons,
        })
        .collect();

    (duplicate_count, duplicates)
}
```

- [ ] **Step 4: Rename reference report semantics**

```rust
pub struct ReferenceReport {
    pub algorithm_id: String,
    pub field: AlgorithmField,
    pub decision_rule: String,
    pub repeat: usize,
    pub runs_ms: Vec<f64>,
    pub avg_ms: f64,
    pub min_ms: f64,
    pub duplicate_count: usize,
    pub duplicates: Vec<ReferenceCandidateScore>,
}
```

- [ ] **Step 5: Run targeted reference tests**

Run: `cargo test reference_duplicates`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add dedup_bench_rs/src/algorithms.rs
git commit -m "feat: use duplicate filtering for reference output"
```

### Task 4: Thread Duplicates Semantics Through Benchmark and Report Output

**Files:**
- Modify: `dedup_bench_rs/src/benchmark.rs`
- Modify: `dedup_bench_rs/src/report.rs`
- Test: `dedup_bench_rs/src/benchmark.rs`

- [ ] **Step 1: Write failing benchmark/report tests**

```rust
#[test]
fn repeat_changes_only_timing_not_duplicate_sets() {
    let single = run_benchmark(&BenchmarkConfig { repeat: 1, ..config() }).unwrap();
    let repeated = run_benchmark(&BenchmarkConfig { repeat: 3, ..config() }).unwrap();

    let single_reference: Vec<(String, String)> = single.reference.duplicates
        .iter()
        .map(|candidate| (candidate.contract_address.clone(), candidate.token_id.clone()))
        .collect();
    let repeated_reference: Vec<(String, String)> = repeated.reference.duplicates
        .iter()
        .map(|candidate| (candidate.contract_address.clone(), candidate.token_id.clone()))
        .collect();

    assert_eq!(single_reference, repeated_reference);
}

#[test]
fn markdown_report_mentions_duplicate_counts() {
    let report = run_benchmark(&config()).unwrap();
    let markdown = report.to_markdown();
    assert!(markdown.contains("duplicate_count"));
    assert!(!markdown.contains("top_candidates"));
}
```

- [ ] **Step 2: Run one benchmark/report test to verify it fails**

Run: `cargo test markdown_report_mentions_duplicate_counts`
Expected: FAIL because report still uses `candidate_count` / `top_candidates`

- [ ] **Step 3: Update `run_benchmark()` to use duplicates builders and new fields**

```rust
let result = algorithm_thread_pool.install(|| {
    let scores = score_rows_parallel_raw(&sample, &recall_rows, algorithm.scorer);
    build_duplicate_candidates_raw(algorithm.id, &recall_rows, &scores)
        .map_err(BenchError::InvalidData)
})?;

AlgorithmReport {
    algorithm_id: algorithm.id.to_string(),
    field: algorithm.field,
    decision_rule: duplicate_score_rule(algorithm.id)
        .map_err(BenchError::InvalidData)?
        .description
        .to_string(),
    repeat,
    runs_ms: runs_ms.clone(),
    avg_ms: runs_ms.iter().sum::<f64>() / runs_ms.len() as f64,
    min_ms: runs_ms.iter().copied().fold(f64::INFINITY, f64::min),
    duplicate_count,
    duplicates,
}
```

- [ ] **Step 4: Update Markdown rendering to duplicates-only language**

```rust
out.push_str(&format!(
    "- decision_rule: `{}`\n- duplicate_count: `{}`\n- avg_ms: `{:.3}`\n- min_ms: `{:.3}`\n- runs_ms: `{:?}`\n\n",
    algorithm.decision_rule,
    algorithm.duplicate_count,
    algorithm.avg_ms,
    algorithm.min_ms,
    algorithm.runs_ms
));

for duplicate in &algorithm.duplicates {
    out.push_str(&format!(
        "1. `{}` / `{}` score=`{:.4}` name=`{}`\n",
        duplicate.contract_address, duplicate.token_id, duplicate.score, duplicate.name
    ));
}
```

- [ ] **Step 5: Run benchmark/report tests**

Run: `cargo test repeat_changes_only_timing_not_duplicate_sets`
Expected: PASS

Run: `cargo test markdown_report_mentions_duplicate_counts`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add dedup_bench_rs/src/benchmark.rs dedup_bench_rs/src/report.rs
git commit -m "feat: emit duplicates-only benchmark reports"
```

### Task 5: Remove `top_k` from Public Configuration and CLI

**Files:**
- Modify: `dedup_bench_rs/src/main.rs`
- Modify: `dedup_bench_rs/src/benchmark.rs`
- Modify: `dedup_bench_rs/tests/cli.rs`

- [ ] **Step 1: Write the failing CLI test update**

```rust
Command::cargo_bin("dedup_bench_rs")
    .unwrap()
    .args([
        "run",
        "--chain", "ethereum",
        "--contract-address", "0xseed",
        "--token-id", "1",
        "--name", "Azuki #1",
        "--metadata-file", &metadata_path.to_string_lossy(),
        "--feature-db", &db_path.to_string_lossy(),
        "--output", &output_path.to_string_lossy(),
        "--repeat", "1",
    ])
    .assert()
    .success();
```

- [ ] **Step 2: Run the CLI test to verify it fails**

Run: `cargo test cli_writes_json_and_markdown_outputs`
Expected: FAIL while code still requires `--top-k`

- [ ] **Step 3: Remove `top_k` from config and CLI**

```rust
pub struct BenchmarkConfig {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub metadata_file: PathBuf,
    pub feature_db: PathBuf,
    pub feature_parquet: Option<PathBuf>,
    pub output: PathBuf,
    pub repeat: usize,
    pub algorithm_threads: usize,
}
```

```rust
Command::Run {
    #[arg(long)]
    chain: String,
    // ...
    #[arg(long, default_value_t = 5)]
    repeat: usize,
    #[arg(long, default_value_t = 30)]
    algorithm_threads: usize,
}
```

- [ ] **Step 4: Run CLI and crate tests**

Run: `cargo test cli_writes_json_and_markdown_outputs`
Expected: PASS

Run: `cargo test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dedup_bench_rs/src/main.rs dedup_bench_rs/src/benchmark.rs dedup_bench_rs/tests/cli.rs
git commit -m "refactor: remove top-k benchmark configuration"
```

### Task 6: Update README and Final Verification

**Files:**
- Modify: `dedup_bench_rs/README.md`

- [ ] **Step 1: Update README examples and terminology**

```md
- 输出：
  - 一份 JSON 详细报告
  - 一份 Markdown 摘要报告
- 正式报告语义：
  - 每个算法只输出被判定为重复的 `duplicates`
  - 每个算法的判重标准定义在 `src/decision_rules.rs`
  - 不再使用 `top_k`
```

- [ ] **Step 2: Remove stale CLI example flags**

```bash
cargo run -- run \
  --chain ethereum \
  --contract-address 0xseed \
  --token-id 1 \
  --name "Azuki #1" \
  --metadata-file ./examples/metadata.json \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --output ./result/result.json \
  --repeat 5 \
  --algorithm-threads 30
```

- [ ] **Step 3: Run final verification**

Run: `cargo test`
Expected: PASS

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add dedup_bench_rs/README.md
git commit -m "docs: describe duplicates-only benchmark output"
```

## Self-Review

- Spec coverage:
  - rules module: covered by Task 1
  - ordinary duplicates-only output: covered by Task 2
  - reference duplicates-only output: covered by Task 3
  - report semantic rename: covered by Task 4
  - remove `top_k`: covered by Task 5
  - docs update: covered by Task 6
- Placeholder scan:
  - no `TODO` / `TBD` placeholders remain
- Type consistency:
  - plan consistently uses `duplicate_count` / `duplicates`
  - rules module is consistently named `decision_rules.rs`

