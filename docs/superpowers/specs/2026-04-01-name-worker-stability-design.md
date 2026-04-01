# Name Worker Stability And Scale Design

## Goal

Upgrade the C++ name worker so it can process large atom blocks without leaking database resources, stranding work items in `running`, or holding the full edge set in memory before insertion.

## Scope

- Keep the existing PostgreSQL schema and worker entrypoint.
- Preserve current similarity semantics unless a bug prevents the intended behavior.
- Improve candidate pruning so large blocks produce fewer unnecessary Levenshtein comparisons.
- Make worker progress recoverable after crashes or forced termination.

## Design

### Resource lifetime

- Replace raw `PGconn*` and `PGresult*` ownership with RAII wrappers.
- Centralize result-status checks so all failure paths free libpq objects before throwing.
- Keep one process-level connection, but guarantee it is closed on every exit path.

### Task lifecycle

- Add a configurable lease timeout for work items.
- `claimTask` will reclaim stale `running` tasks whose `started_at` is older than the lease window, then assign them to the current worker and clear prior error state.
- Task completion and task failure stay explicit updates.
- Failure reporting must surface if the status update itself fails.

### Edge generation

- Split pure candidate generation from persistence so the worker can stream edges in bounded batches.
- Replace `computeEdges -> vector<Edge>` with a callback- or sink-based API that emits accepted edges incrementally.
- Keep the minimum-threshold short-circuit, but reduce candidate volume with:
  - length-window precheck before expensive scoring
  - inverted-index capacity reservation
  - direct substring handling for short names that would otherwise never enter the trigram candidate set

### Database writes

- Replace large SQL string concatenation with batched parameterized inserts.
- Flush fixed-size edge batches during computation and maintain an inserted-edge counter separately from in-memory batch size.
- Keep `ON CONFLICT DO NOTHING` semantics.

### Test strategy

- Extract the pure worker logic needed for deterministic C++ unit-style tests without requiring a live database.
- Add regression coverage for:
  - substring candidate recovery for short names
  - streaming edge emission not requiring a full `vector<Edge>`
  - stale-task claim SQL choosing `pending` or expired `running`

## Risks

- Reclaiming stale tasks can duplicate work if the lease window is too short; default must be conservative.
- Streaming inserts increase database round-trips; batch size must remain configurable.
- Any helper extraction from `main.cpp` must not alter CLI behavior.
