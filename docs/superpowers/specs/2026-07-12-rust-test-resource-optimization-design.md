# Rust Test Resource Optimization Design

## Goal

Reduce Rust test compilation time, peak resource use, and disk growth while preserving an explicit full-regression path for all four Rust crates.

## Scope

The change covers the active crates `name_uri_analysis_rs` and `top_contract_analysis_rs`. The obsolete `dedup_bench_rs` and `name_metadata_change_samples` crates are removed separately. The optimization does not change production algorithms or test assertions in the retained crates.

## Build Configuration

Add a repository-level `.cargo/config.toml` with `target-dir = "target"`. Cargo commands launched from any Rust crate will therefore reuse one repository target directory when dependency versions and feature sets match.

Define a root Cargo workspace containing both active crates and keep one root `Cargo.lock`. Set `[profile.test] debug = 0` once at the workspace root. This removes test debug information and the large Windows PDB cost while centralizing the profile contract.

## Test Classification

Tests use responsibility-based features: `db-tests`, `api-tests`, and `cli-tests`. The aggregate `expensive-tests` feature enables all applicable groups. Snapshot export remains independent because it adds Arrow, Parquet, and PostgreSQL dependencies.

The default tier retains pure unit tests, parsing and validation tests, deterministic algorithm tests, and a small number of lightweight database smoke tests needed to protect essential integration boundaries.

The expensive tier contains integration targets dominated by DuckDB or Parquet setup, CLI subprocess execution, HTTP mock servers, API workflows, multichain batch analysis, snapshot export, or other repeated external-resource setup. Entire integration targets are gated where their contents have the same cost class. Mixed targets are split or gated at module level so that useful fast coverage remains in the default tier.

No test is deleted, weakened, or converted into a benchmark. Snapshot tests require both `db-tests` and `export-snapshot`.

Each Cargo integration-test target remains a small root harness. Its implementation lives in a matching `*_cases/` directory, allowing future domain splits without creating more linked test binaries. Each crate's `tests/README.md` owns the target-to-tier map.

## Commands

Each crate must support the fast development command represented by this loop:

```powershell
$crates = @("name_uri_analysis_rs", "top_contract_analysis_rs")
foreach ($crate in $crates) { cargo test --manifest-path "$crate/Cargo.toml" }
```

Each crate must support the complete regression tier represented by this loop:

```powershell
foreach ($crate in $crates) { cargo test --manifest-path "$crate/Cargo.toml" --features expensive-tests }
```

Feature-specific suites use combined features where required:

```powershell
cargo test --manifest-path top_contract_analysis_rs/Cargo.toml --features "expensive-tests export-snapshot"
```

Repository documentation will list the fast and full commands and explain the classification boundary.

## Verification

Verification first checks manifest metadata and test discovery so feature gates cannot silently remove targets. It then runs the default suite for all crates and the expensive tier for affected test targets. Formatting and `git diff --check` complete the source-level verification.

Because DuckDB bundled builds are expensive, targeted commands are used during implementation. A fresh full command is required before completion claims.

## Cleanup

Only after verification succeeds, remove the legacy `target` directories inside the retained crate directories. The obsolete crate directory, including its old target, is removed as part of retiring that crate. Confirm that Cargo resolves `target_directory` to the repository-level `target`, then report the reclaimed space. The new shared repository target remains intact as the reusable development cache.

## Compatibility and Risks

The principal behavior change is that `cargo test` no longer runs every high-cost integration test. The full suite remains explicit and documented. CI or contributor workflows that require complete coverage must use `--features expensive-tests` and, when applicable, `export-snapshot`.

Sharing a target directory does not merge lockfiles or dependency versions. Cargo will reuse only compatible artifacts, so dependency version alignment remains a possible later optimization rather than part of this change.
