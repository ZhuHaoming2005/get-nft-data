# Metadata Engine v2 Production Cutover Design

## 1. Outcome and scope

This change finishes the `name_uri_analysis_rs` metadata-engine redesign as a production implementation rather than an opt-in shadow. The final codebase has one metadata data plane: `metadata_engine` owns Encode, blocking, scheduling, exact evidence, scoring, edge collection, component reduction, summaries, recovery artifacts, progress, and production-evidence validation. The controller in `name_uri_analysis_rs` owns process isolation, checkpoints, CLI progress rendering, and final report assembly.

The existing legacy metadata matcher remains available only while it is needed to generate same-snapshot differential evidence. After v2 passes the semantic cutover gate and the controller uses v2 unconditionally, the legacy matcher, shadow adapters, shadow environment switches, compatibility-only metrics, and duplicate tests are deleted in the same refactor. Test-only independent oracles and fail-closed evidence validators are not legacy code and remain.

The target-host performance gate cannot be inferred from unit tests or old logs. Production code may be complete locally while deployment selection remains fail-closed until a current-HEAD evidence bundle from the intended 128-vCPU/512-GiB host is attached.

## 2. Chosen approach

The implementation uses an evidence-gated in-place cutover.

1. Make v2 observable and deterministic before changing production selection.
2. Add a same-snapshot legacy/v2 comparison runner that produces a sealed evidence bundle.
3. Move the production metadata phase to v2 and make missing or stale evidence an explicit deployment-gate failure rather than a silent legacy fallback.
4. Delete the legacy data plane and all transition-only routing once the differential harness has served its purpose.

Rejected alternatives:

- A permanent dual engine would double maintenance and allow silent semantic drift.
- An immediate deletion before differential evidence would remove the only practical same-snapshot oracle.
- Cosmetic stage spinners would remain misleading because the expensive work is pair enumeration and reduction, not the number of named stages.

## 3. Final architecture and ownership

The physical controller sequence remains:

`Prepare -> MetadataEncode -> Name -> MetadataMatch(v2) -> Finalize`

`MetadataEncode` writes immutable revisioned feature, payload-CAS, atom, token-membership, and blocking artifacts. `MetadataMatch` opens only those snapshots and performs catalog creation, exact evidence, frozen recall planning, candidate scoring, scoped edge collection, component reduction, and metadata summary generation. It must not read raw JSON or DuckDB.

The final public runtime entry is a production-named pipeline function. Shadow naming and `NAME_URI_METADATA_V2_SHADOW*` switches disappear. The result distinguishes:

- algorithm completion: every planned work unit and required scope completed;
- semantic readiness: current snapshot/revision differential and exact-evidence gates pass;
- deployment readiness: semantic readiness plus target-host resource and performance evidence passes.

No incomplete or budget-exhausted run may emit a production summary or ready checkpoint.

## 4. Progress model

### 4.1 Engine-neutral events

`metadata_engine` exposes a small callback-based progress API with no dependency on terminal rendering. Events identify a stable subphase, completed work, total work, work unit, and monotonic counters. The no-callback entry remains a zero-cost wrapper for tests and library callers. `name_uri_analysis_rs` adapts these events to `ProgressTracker`.

Callbacks are emitted at bounded intervals or natural batch boundaries. Inner pair loops do not render directly. Completed work is monotonic, never exceeds total, and is advanced for attempted work rather than matches, retained candidates, or emitted edges.

### 4.2 Work units and exact totals

Each ETA covers one homogeneous subphase:

| Subphase | Unit | Total |
|---|---|---|
| Encode representative rows | rows | eligible prepared contract rows |
| Encode retained-token sources | rows | retained-token source rows |
| Payload/atom finalization | items | payloads plus contracts plus atoms |
| Blocking compile | memberships or pair visits | exact compiler input count |
| Pair ExactIsland | pairs | sampled lefts times full atom universe, excluding self |
| Shared-token ExactIsland | pairs | sum of `choose(group_members, 2)` over frozen sampled groups |
| Fallback atom expansion | pairs | sum of `choose(atom_contracts, 2)` |
| Catalog candidate traversal | block-pair visits | sum of `choose(block_members, 2)` over scheduled blocks |
| Shared-token production traversal | pair visits | sum of the actual selected group traversal relation |
| Edge dispatch | edges | frozen raw-edge count |
| Scope reduction | nodes plus run edges | exact work for each frozen scope |
| Artifact commit | bytes or files | frozen manifest inputs |

`WorkCatalog::estimated_work` is computed as the sum of per-block work in a job. It must never use `choose(sum(block_members), 2)` for a MicroBatch because cross-block pairs are not visited. Budget admission, progress totals, metrics, and scheduling use the same checked work-count helpers to prevent divergent arithmetic.

When a total is genuinely unknowable before execution, the UI uses an indeterminate task and reports throughput only. It must not manufacture an ETA.

### 4.3 ETA

ETA uses processed work divided by a smoothed rate within the current homogeneous subphase. It remains `n/a` during warm-up, when no work has completed, when elapsed time is too short, or when the total is unknown. The estimator uses recent samples through an EWMA so startup I/O and later steady-state pair work do not permanently bias the estimate. It resets between work-unit types and ignores zero-delta refreshes.

The renderer reports elapsed time, completed/total, rate, ETA, and diagnostic counters separately. Candidate, scored, matched, and component counters are diagnostics only and never drive the progress position.

## 5. Recovery, budgets, and failure semantics

All ready markers are committed only after their files are durable and validated. On restart, revision and snapshot fingerprints decide reuse; partial directories are rebuilt or retired through `StorageBroker`. The production Match phase reuses completed Encode, Catalog, RecallPlan, exact-evidence, connectivity-run, and component-snapshot products only when their dependency fingerprints match.

Memory, storage, exact-evidence, candidate-work, and edge budgets use checked arithmetic. Exhausting any semantic work budget is a hard error. Storage eviction is allowed only for unpinned, reproducible artifacts and becomes available to later reservations only after deletion succeeds.

Progress failure leaves the last real position visible and marks the active task failed. It never advances a failed task to 100 percent.

## 6. Semantic and production gates

The same-snapshot differential runner compares the production-relevant contract:

- eligible universes and source-context isolation;
- BaseEquivalent candidate ownership and frozen RecallPlan actions;
- pair decisions for commonly scored pairs;
- exact calibration and independent holdout misses/rescue actions;
- intra-chain, cross-chain, and chain-pair component membership with canonical roots;
- final metadata summary rows.

The sealed evidence envelope includes input fingerprints, algorithm/schema revisions, command/configuration, gate results, resource peaks, work metrics, subphase wall times, and target-host identity. A stale fingerprint or revision invalidates it. `production_ready` is derived by the validator and cannot be set directly by the pipeline.

BaseEquivalent may pass without Calibrated-profile closed-component proof only when the full HEAD differential contract passes. Any later candidate-relation change requires the C1/C2/adjudication evidence defined in the long-term redesign specification.

Target-host acceptance additionally requires current-HEAD 1%, 10%, and full evidence, bounded RSS/storage, no budget degradation, and end-to-end wall improvement. Claims of 5x or 10x improvement must be backed by the specified dominant work metrics; they cannot be inferred from shard counts or microbenchmarks.

## 7. Cleanup rules

Cleanup occurs after the production entry and evidence tests are green:

- delete the legacy metadata matching/index/union/fallback implementation and its compatibility progress calls;
- delete shadow environment switches, shadow-only result adapters, and shadow naming;
- remove duplicate old parsers/scorers only after all ownership has moved into `metadata_engine`;
- remove metrics fields that can no longer be populated accurately, including transition-only zero counters;
- merge or delete tests that assert implementation details of removed paths;
- retain exhaustive/property oracles, scoring differential tests, corruption/recovery tests, and evidence fail-closed tests;
- retain one migration note describing artifact incompatibility and required rebuild behavior.

An item is not deleted merely because production does not call it today if it enforces an independent correctness or deployment gate.

## 8. Test strategy

Implementation follows red-green-refactor cycles. Required tests include:

- exact per-block catalog work totals, overflow handling, and no cross-block overcount;
- progress monotonicity, upper bounds, homogeneous-unit reset, unknown totals, warm-up, EWMA behavior, and failure below 100 percent;
- real callback events from Encode, ExactIsland, candidate traversal, edge dispatch, and Reduce;
- deterministic outputs with progress enabled or disabled;
- budget failure prevents summary/ready publication;
- artifact corruption, stale revisions, interrupted commits, and restart reuse;
- exhaustive small-universe and property differential tests for scoring, candidates, rescue, components, roots, and summaries;
- controller integration proving v2 is the sole production metadata path and no shadow environment switch changes semantics;
- a source/reachability check that legacy production modules and transition identifiers are absent after cleanup.

Final verification runs formatting, strict Clippy, all metadata-engine tests, all `name_uri_analysis_rs` tests, controller integration tests, and the architecture/source checks. An independent reviewer audits production semantics, scale behavior, progress accounting, recovery, and deletion safety.

## 9. Completion criteria

The refactor is code-complete only when:

1. v2 is the sole production metadata data plane.
2. Every long-running subphase exposes real work-unit progress and a truthful ETA or explicit `n/a`.
3. Semantic/budget failure cannot publish production output.
4. Same-snapshot differential and evidence validation are executable and fail closed.
5. Legacy and transition-only code has been deleted without removing independent oracles.
6. The full local verification suite and independent review have no unresolved Critical or Important findings.
7. The architecture review maps every M1-M5 requirement to implemented evidence or an explicit deployment-only target-host gate.

Deployment-ready status additionally requires fresh target-host evidence. If that environment or full snapshot is unavailable, the final report must say so and must not claim that performance acceptance passed.
