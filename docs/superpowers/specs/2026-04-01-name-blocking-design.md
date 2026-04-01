# Name Blocking Redesign

## Goal

Replace the current `name_block_key` strategy with an adaptive, recall-driven blocking scheme that:

- keeps separator-only variants such as `andy duboc` and `andyduboc` in the same block
- materially reduces the number of tiny blocks and one-atom work items
- prevents hot prefixes from turning into unbounded large blocks
- preserves the existing worker contract so heavy pairwise matching remains in C++

## Non-Goals

- Do not solve token-order permutations such as `andy duboc` versus `duboc andy` in this iteration.
- Do not move candidate generation or edit-distance scoring into Python.
- Do not redesign the work-item lease model or edge schema as part of this change.
- Do not remove the existing `signature_prefix` split path; it remains the final safeguard for oversized blocks.

## Current Problem

The current block rule is implemented in [normalize.py](/D:/code/solidity/get-nft-data/name_symbol_stats_v2/normalize.py#L75):

- `name_block_key = first_token | name_length_bucket(len(name_norm))`

This has two failure modes:

- It splits obvious separator variants into different blocks because the first token changes with spacing or punctuation normalization.
- It creates far too many tiny blocks, which then become tiny work items. In the observed `apr01` run, most work items had one or two atoms and therefore paid scheduling and database overhead without producing useful comparisons.

The C++ worker already performs candidate recall and exact scoring inside a block. The main issue is not the inner matching loop alone; it is that the current block rule discards recall and explodes orchestration cost before the worker even starts.

## Design Overview

The new design uses adaptive blocking with three layers:

1. Build a separator-insensitive canonical form called `name_collapsed`.
2. Use `name_collapsed` prefixes plus coarse collapsed-length buckets to build a recall-friendly canopy.
3. Choose the final `name_block_key` from several candidate keys based on observed canopy size, so small canopies stay merged and large canopies are selectively refined.

Each atom still ends up with exactly one final `name_block_key`. The worker protocol remains `name_block_key + signature_prefix`, and all heavy candidate generation and final similarity scoring stay in C++.

## Data Model

### New derived fields

Add these derived values during atom preparation:

- `name_collapsed`
  - Input: normalized name from `normalize_name()`
  - Rule: remove all separators and keep only `[0-9a-z]`
  - Examples:
    - `andy duboc` -> `andyduboc`
    - `andy-duboc` -> `andyduboc`
    - `andyduboc` -> `andyduboc`
- `name_collapsed_len`
  - `len(name_collapsed)`

These values may be persisted on `nsv2_name_atoms` or recomputed inside the task-preparation query. Persisting them is preferred because they are reused during every run and simplify later diagnostics.

### Existing fields retained

Keep these fields and their purpose:

- `name_signature_hash`
  - remains the fallback split key for oversized final blocks
- `name_block_key`
  - remains the worker-visible final block assignment

The semantic change is that `name_block_key` is no longer a purely local normalization result. It becomes the output of the adaptive task-preparation phase.

## Blocking Algorithm

### Candidate key parts

For each atom, derive:

- `p3 = left(name_collapsed, 3)`
- `p4 = left(name_collapsed, 4)`
- `p5 = left(name_collapsed, 5)`
- `len6 = floor(name_collapsed_len / 6) * 6`

If `name_collapsed` is shorter than the requested prefix length, use the full string.

### Adaptive key selection

The final `name_block_key` is chosen by canopy size:

- If `count(p3) < 64`, use `p3`
- If `64 <= count(p3) <= 8000`, use `p3|len6`
- If `count(p3) > 8000`, use `p4|len6`
- If `count(p4|len6) > 30000`, use `p5|len6`
- If the resulting block still exceeds `max_atoms_per_task`, keep the current `signature_prefix` split logic in [work_items.py](/D:/code/solidity/get-nft-data/name_symbol_stats_v2/work_items.py#L85)

These thresholds are initial operating defaults, not immutable constants. They must be configurable from Python so future runs can tune them without touching C++.

### Why this is recall-driven but controlled

Recall-driven:

- separator-only variants map to the same `name_collapsed`
- small canopies are allowed to stay merged instead of being fragmented by an early length bucket
- the primary grouping signal is character-form similarity, not first-token identity

Controlled:

- each atom still has one final block key
- only hot canopies are refined further
- the last-resort `signature_prefix` split still prevents pathological block sizes

## Pipeline Changes

### Python responsibilities

Python remains responsible for:

- reading and writing PostgreSQL rows
- computing or querying canopy statistics
- assigning the final `name_block_key`
- generating work items from that final key

Python must not become responsible for large-scale string pair scoring.

### C++ responsibilities

C++ remains responsible for all compute-heavy matching:

- trigram posting-index candidate recall inside a block
- substring-based recovery for short names
- exact final scoring, currently `levenshteinRatio`
- future upgrades such as thresholded banded Levenshtein

This preserves the current shape of [main.cpp](/D:/code/solidity/get-nft-data/name_symbol_stats_v2/cpp/src/main.cpp) and [worker_core.cpp](/D:/code/solidity/get-nft-data/name_symbol_stats_v2/cpp/src/worker_core.cpp).

### Task preparation flow

The new task-preparation flow is:

1. Prepare `nsv2_name_atoms` with `name_norm`, `name_collapsed`, `name_collapsed_len`, `name_signature_hash`
2. Compute canopy counts for `p3`, `p3|len6`, and `p4|len6`
3. Write the final adaptive `name_block_key` back to `nsv2_name_atoms`
4. Build work items by grouping on final `name_block_key`
5. For any block larger than `max_atoms_per_task`, apply the existing `signature_prefix` split path

This keeps the worker query contract unchanged while replacing only the blocking strategy.

## Compatibility Strategy

Introduce a blocking-strategy switch in Python:

- `legacy`
- `adaptive_v1`

`legacy` preserves the current first-token rule. `adaptive_v1` enables the new blocking algorithm.

This switch provides:

- safe rollout
- direct A/B comparison on the same dataset
- easy fallback if recall or runtime regress unexpectedly

## Error Handling And Edge Cases

- Empty `name_collapsed` yields an empty `name_block_key` and should be excluded from task generation, matching the current empty-key behavior.
- Extremely short collapsed names still follow the same adaptive rules; they are expected to land in small canopies and remain merged.
- Very hot prefixes such as `the`, `spa`, or similar remain bounded by the `p4|len6`, `p5|len6`, and `signature_prefix` fallback layers.
- Token-order permutations are deliberately out of scope for this version. They may still meet later inside the C++ candidate-recall layer only if they share the same adaptive block by chance; the blocker itself does not guarantee this.

## Testing Strategy

### Python tests

Add focused tests for:

- `name_collapsed` derivation
- adaptive key selection around each threshold boundary
- separator variants such as `andy duboc` and `andyduboc` producing the same final block under `adaptive_v1`
- hot-canopy refinement choosing `p4|len6` or `p5|len6`
- fallback to `signature_prefix` for oversized adaptive blocks

### Integration checks

For one representative run label, compare `legacy` and `adaptive_v1` on:

- total work-item count
- `atom_count` distribution (`p50`, `p90`, `p99`, `max`)
- count of one-atom and two-atom work items
- total emitted edge count
- end-to-end runtime

### C++ regression expectations

No worker protocol change is expected. Existing C++ tests should continue to pass unchanged. A later optimization pass may change only the edit-distance implementation, not the blocking contract.

## Rollout Plan

1. Add `name_collapsed` helpers in Python normalization.
2. Add adaptive blocking selection and feature-flag support in task preparation.
3. Rebuild work items for a local run using both `legacy` and `adaptive_v1`.
4. Compare task counts, edge counts, and runtime.
5. Promote `adaptive_v1` to the default only after the metrics show lower orchestration cost without a suspicious edge-count collapse.

## Acceptance Criteria

The redesign is accepted when all of the following are true:

- separator variants such as `andy duboc` and `andyduboc` land in the same final block under `adaptive_v1`
- total work-item count drops materially relative to `legacy`
- one-atom work items drop materially relative to `legacy`
- oversized hot blocks remain bounded by fallback splitting
- end-to-end runtime improves without an unexplained large drop in emitted edges
