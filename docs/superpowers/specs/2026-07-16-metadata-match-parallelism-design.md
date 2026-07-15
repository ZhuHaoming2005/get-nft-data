# MetadataMatch Parallelism Design

## Goal

Improve `name_uri_analysis_rs` MetadataMatch utilization on a 128 vCPU,
512 GiB host without changing metadata matching semantics, source selection,
candidate accounting, component membership, summary rows, recovery behavior,
or fail-closed resource admission.

This change focuses on three reviewed code-path bottlenecks:

1. catalog and shared-token scorers feed one serial forest-admission receiver;
2. one large shared-token group runs as one Rayon task;
3. rescue contract-product expansion is serial.

Snapshot checksum verification and MetadataEncode are outside this change.

## Required Invariants

- Normalized duplicate groups and metadata summary rows must be identical across
  thread counts.
- Each unordered candidate pair is scored at most once by its frozen routing
  owner.
- Pair-visit, scored, matched, accepted-edge, and progress counters preserve
  their current meanings.
- The configured `--threads` value is a process-wide ceiling for active Match
  workers, excluding the controller/progress coordinator.
- Snapshot, edge, component, catalog, exact-evidence, and per-lane memory remain
  cumulatively admitted through `MemoryBroker`.
- Budget exhaustion, worker failure, channel failure, integer overflow, and
  incomplete work cancel the phase and fail closed.
- Edge arrival order and forest edge identity may vary; final connectivity may
  not vary.

## Architecture

### 1. Scope-sharded forest admission

Replace the single `ScopeEdgeCollectors` receiver hot path with a
`ScopeCollectorBroker` that owns the existing collectors without duplicating
their dense degree arrays.

The broker creates one logical sink for each existing scope:

- one intra-chain collector;
- one all-cross-chain collector;
- one collector for each unordered chain pair.

Active sink workers are bounded by the smaller of the scope count, available
Match threads, and admitted sink scratch. The remaining threads form the scorer
pool. For example, a four-chain run has eight scopes and can use eight sinks plus
120 scorers under a 128-thread ceiling.

Scorer lanes compact accepted edges into scope-specific forest batches before
submission. A cross-chain edge is submitted to the all-cross scope and its one
chain-pair scope. Sink workers exclusively own their `EdgeCollector`, so no
collector lock is required. Completion joins every sink, propagates the first
error, validates accepted-edge accounting, and returns collectors in the
existing scope order.

The broker tracks aggregate retained bytes with checked atomic deltas. When the
global retained limit is approached, the owning sink compacts its collector and
reports the new retained size. If aggregate retained bytes still exceed the
admitted limit after compaction, all producers are cancelled and Match fails.

Fallback, catalog, shared-token, and rescue paths use the same broker. This
removes separate serial edge-admission implementations.

### 2. Hot shared-token routing tiles

Split local BaseEquivalent routing into two layers:

1. `LocalRoutingPlan` builds immutable local blocks, atom memberships, and the
   existing deterministic owner relation once for a token group.
2. `LocalRoutingTile` describes a bounded upper-triangle member range within one
   block.

Groups below the existing small-group threshold retain the simple exhaustive
loop. Large groups create routing tiles and submit those tiles to the configured
scorer pool. A tile tests the same owner predicate before scoring, so replicated
blocking membership cannot duplicate pair visits.

Each routed visit reserves the existing shared pair-visit budget before scoring.
Reservation failure sets a shared cancellation flag; no later tile may continue
or publish partial success. Work and group progress are aggregated by the
coordinator and remain monotonic.

Memory admission covers the immutable plan once per active group plus bounded
per-tile edge scratch. Lane selection uses the sum of concurrently scheduled
plan sizes rather than multiplying the largest group size by every lane.

### 3. Parallel rescue expansion

Keep the frozen `RescueExecutionPlan` and its scoring decisions unchanged.
Convert each matched atom pair's contract Cartesian product into deterministic
two-dimensional tiles. Tiles run in the configured scorer pool, apply the
existing retained-token exclusion, compact edges by scope, and submit them to
`ScopeCollectorBroker`.

Matched shared rescue edges are batched through the same broker. The exact
planned expansion count remains the progress total; completed work advances for
every visited contract product, not only accepted edges.

## Thread Allocation

At the start of connectivity execution:

1. calculate active scope sinks;
2. reserve bounded per-sink queue and batch scratch;
3. assign at least one scorer lane;
4. create one configured Rayon scorer pool;
5. reuse it for fallback, catalog, shared-token, rescue, collector finalization,
   and component reduction where phases do not overlap.

If the requested thread count is one, the broker uses an inline serial sink and
preserves current single-thread behavior. Thread counts below the scope count
use fewer sink workers, each owning multiple scopes in a stable round-robin
assignment.

## Error Handling and Cancellation

- Every producer checks one shared cancellation flag before starting a task and
  at bounded work intervals.
- The first producer or sink error is retained; later channel disconnects do not
  replace it with a less useful error.
- Sink queues are bounded. Backpressure remains valid but is distributed across
  scopes instead of blocking all producers behind one receiver.
- All sink workers are joined on success and failure.
- Logical accepted-edge totals are checked separately from scope submissions;
  the latter deliberately counts one intra submission or two cross-chain
  submissions before forest finalization.
- No ready marker, component result, or metadata summary is published after a
  cancelled or partially joined run.

## Test Strategy

Implementation follows red-green-refactor, one subsystem at a time.

### Scope broker tests

- fail first unless at least two independent scopes can be consumed
  concurrently;
- prove serial and sharded brokers produce identical component roots;
- prove cross edges enter exactly the cross scope and their chain-pair scope;
- prove retained-byte overflow and sink failure cancel all producers;
- prove active scorer plus sink workers never exceed the configured ceiling.

### Shared-token tests

- construct one hot group whose routing plan creates more tiles than lanes;
- prove more than one worker executes its tiles;
- compare routed pair set, visit count, accepted edges, and components with the
  existing serial local router;
- prove budget exhaustion stops remaining tiles and fails closed;
- cover empty, small, highly replicated, and anchor-only routing groups.

### Rescue tests

- compare tiled and serial contract-product expansion exactly;
- verify retained-token exclusions and self-edge removal;
- verify progress counts every product visit when no edge matches;
- verify cancellation prevents partial forest publication.

### End-to-end gates

- MetadataMatch semantic oracle at threads 1, 4, and the visible maximum;
- all `metadata_engine` tests;
- all-feature `name_uri_analysis_rs` tests;
- strict Clippy for both crates, formatting, and `git diff --check`;
- target-host benchmark matrix at 1/16/32/64/96/128 threads for balanced
  catalog, dense accepted-edge, and one-hot-token workloads.

The repository test suite gates correctness. Target-host performance results are
reported separately and do not weaken any semantic or admission assertion.

## Delivery Order

1. introduce the scope broker behind tests and route catalog through it;
2. migrate fallback and shared-token edge submission;
3. add local routing plans and hot-group tiles;
4. parallelize rescue expansion through the broker;
5. reuse the bounded pool for collector finalization and component reduction;
6. run the complete verification and narrow performance re-audit.
