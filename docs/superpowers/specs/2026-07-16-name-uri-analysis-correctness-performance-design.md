# name_uri_analysis_rs Correctness and Performance Design

## Scope

Fix the MetadataEncode fallback regression and remove the reviewed hot-path
inefficiencies without changing duplicate-matching semantics, stable source
selection, candidate-pair accounting, output rows, or fail-closed resource
admission.

## Design

1. Replace the unsupported DuckDB `LIST` parameter with a bounded temporary
   table populated through the Appender API. The table contains only fallback
   contract IDs and is dropped after the ordered Arrow query.
2. Partition each ordered Arrow batch into contiguous contract/token groups.
   Validate every row, evaluate groups in parallel, and short-circuit within a
   group after its first usable JSON. Carry the first/last group state across
   Arrow batch boundaries so stable source ordering is unchanged.
3. Run every Rayon operation under the configured worker pool. Existing
   specialized sub-pools remain bounded by the same configured ceiling.
4. Parallelize fallback pair enumeration by left-member ranges while retaining
   the complete unordered-pair visit count, token-overlap rule, accepted-edge
   count, and deterministic connectivity.
5. Reduce independent component scopes in parallel. Progress is aggregated as
   deltas on the coordinator thread, and the existing admitted component peak
   remains the concurrency bound.
6. Correct the cross-scope forest capacity hint so it uses the candidate count
   rather than the length of a scratch vector that has just been cleared.

## Verification

- Reproduce the two DuckDB fallback failures before the fix and require both to
  pass afterward.
- Add fail-first tests for production group short-circuiting across batch
  boundaries, configured worker-pool execution, fallback parallel equivalence,
  parallel scope reduction/progress, and forest preallocation.
- Run targeted tests after each change, then both crate suites, all-feature DB
  and CLI integration tests, formatting, strict Clippy, and `git diff --check`.
