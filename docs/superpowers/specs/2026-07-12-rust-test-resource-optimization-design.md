# Rust Test Resource Optimization Design

## Goal

Reduce Rust test compilation time, peak resource use, and disk growth while preserving an explicit full-regression path for all four Rust crates.

## Scope

The change covers `dedup_bench_rs`, `name_metadata_change_samples`, `name_uri_analysis_rs`, and `top_contract_analysis_rs`. It does not change production algorithms or test assertions.

## Build Configuration

Add a repository-level `.cargo/config.toml` with `target-dir = "target"`. Cargo commands launched from any Rust crate will therefore reuse one repository target directory when dependency versions and feature sets match.

Set `[profile.test] debug = 0` in every crate manifest. This removes test debug information and the large Windows PDB cost. Other test-profile settings remain unchanged.

## Test Classification

Every crate declares an empty `expensive-tests` feature. Tests are divided into two tiers.

The default tier retains pure unit tests, parsing and validation tests, deterministic algorithm tests, and a small number of lightweight database smoke tests needed to protect essential integration boundaries.

The expensive tier contains integration targets dominated by DuckDB or Parquet setup, CLI subprocess execution, HTTP mock servers, API workflows, multichain batch analysis, snapshot export, or other repeated external-resource setup. Entire integration targets are gated where their contents have the same cost class. Mixed targets are split or gated at module level so that useful fast coverage remains in the default tier.

No test is deleted, weakened, or converted into a benchmark. Tests that require existing features such as `export-snapshot` retain those feature requirements in addition to `expensive-tests`.

## Commands

Each crate must support the fast development command represented by this loop:

```powershell
$crates = @("dedup_bench_rs", "name_metadata_change_samples", "name_uri_analysis_rs", "top_contract_analysis_rs")
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

Only after verification succeeds, remove the legacy `target` directories inside the four crate directories. Confirm that Cargo resolves `target_directory` to the repository-level `target`, then report the reclaimed space. The new shared repository target remains intact as the reusable development cache.

## Compatibility and Risks

The principal behavior change is that `cargo test` no longer runs every high-cost integration test. The full suite remains explicit and documented. CI or contributor workflows that require complete coverage must use `--features expensive-tests` and, when applicable, `export-snapshot`.

Sharing a target directory does not merge lockfiles or dependency versions. Cargo will reuse only compatible artifacts, so dependency version alignment remains a possible later optimization rather than part of this change.
