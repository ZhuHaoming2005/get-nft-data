# Metadata Flat Scoring and Cross-Edge Compaction Design

## Goal

Reduce Encode and Match CPU/memory traffic without adding a performance-profile
phase and without changing the persisted feature/blocking formats, checkpoint
protocol, final business-key groups, summary rows, or matching semantics.

This iteration implements two independent optimizations together:

1. Build `prepared_weights` directly in its final flat array without cloning
   each payload's template term and frequency slices.
2. Stop duplicating every raw cross-chain edge into both the global cross scope
   and a chain-pair scope before compaction.

Internal edge order, forest representatives, arbitrary IDs, and artifact bytes
may change. Final intra-chain, cross-chain, and chain-pair connected components
and their summaries must remain semantically equivalent.

## Non-goals

- No performance profiling, benchmark harness, or claimed speedup ratio.
- No persisted schema or stage-revision change.
- No changes to candidate generation, pair scoring, evidence gates, Reduce
  snapshots, or recovery behavior.
- No unsafe parallel writes and no new unbudgeted per-worker full-corpus arrays.

## Encode: flat prepared-weight construction

### Current behavior

`prepare_template_scoring_soa` iterates payloads in parallel. For every payload,
it clones both the template term IDs and frequencies before calculating its
prepared weights. The clones exist only to produce an owned iterator accepted
by Rayon, so the hot path repeatedly allocates and copies data already present
in `PayloadTermSoA`.

### Selected design

The implementation will:

1. Compute one normalization value per payload from the existing payload
   lengths and corpus average.
2. Reuse that normalization vector for query denominators and prepared weights.
3. Allocate the final `Vec<f64>` at exactly
   `payloads.template_terms.len()` elements.
4. Split the final output into fixed-size flat-term chunks using safe
   `par_chunks_mut`.
5. For each chunk, locate its first payload with one binary search over
   `template_offsets`, then walk subsequent payload boundaries monotonically.
6. Read term IDs and frequencies directly from the flat SoA arrays and write
   each calculated value into the chunk's disjoint mutable output slice.

The work remains parallel. Complexity is linear in the number of template
terms plus one logarithmic payload lookup per output chunk. No per-payload term
or frequency vectors are created.

### Invariants and errors

- Template offsets, terms, frequencies, and output lengths must agree.
- Empty payload rows are skipped without changing later offsets.
- Every output element is written exactly once using safe disjoint slices.
- Existing checked length/identity validation remains authoritative; an
  inconsistent SoA fails closed rather than publishing artifacts.

## Match: compact pair scopes before global cross composition

### Current behavior

`compact_catalog_scope_batch` copies every raw cross-chain edge into a global
`cross` vector and into one `chain_pairs[pair]` vector. Both copies are then
compacted independently. High-recall batches therefore duplicate their largest
uncompacted edge population.

### Selected design

The implementation will:

1. Partition raw edges into `intra` and chain-pair vectors only. A raw global
   cross vector is not created.
2. Compact the intra vector with the existing reusable adaptive scratch.
3. Compact every non-empty chain-pair vector independently.
4. Concatenate copies of only the compacted pair forests into one temporary
   cross candidate vector.
5. Compact that candidate vector once to produce the global cross forest.
6. Send the compacted intra, cross, and pair forests through the existing
   `CompactedCatalogEdges` and `ScopeEdgeCollectors` interfaces.

For each chain-pair subgraph, replacing its edge set with a spanning forest
preserves that subgraph's connected components. The union of component-
preserving replacements preserves the connected components of the global
cross graph, so a second global compaction yields the same business grouping.

This design still copies compacted pair edges once when composing the global
cross candidate. It intentionally avoids a deeper collector/recovery redesign;
the copied population is bounded by pair forests rather than raw candidate
edges.

### Invariants and errors

- `accepted_edges` continues to count accepted raw edges, not retained forest
  edges, so progress and metrics semantics do not change.
- Every cross edge is assigned to exactly one valid unordered chain-pair index.
- Invalid pair indices and existing collector budget failures remain fail
  closed.
- Worker-local compaction scratch is reused sequentially for intra, pair, and
  cross scopes and stays within the existing dense/sparse budget selection.

## Testing

Implementation follows failing-first TDD.

1. Add a prepared-weight differential covering empty payloads, uneven term
   rows, repeated terms with frequencies, zero-weight payloads, and multiple
   worker counts. Compare every denominator and prepared weight against the
   existing reference computation.
2. Add a scope-compaction differential with at least three chains, duplicate
   edges, cycles within a pair, and paths that connect through multiple chain
   pairs. Compare intra, global cross, and every pair's connected components
   with the pre-optimization reference graph.
3. Run the existing Prepare -> Encode -> Match semantic golden across one and
   multiple threads, comparing business-key groups and summary rows.
4. Run `cargo test --workspace`, strict workspace Clippy, format check, and
   `git diff --check`.

## Acceptance criteria

- No per-payload `to_vec` calls remain in prepared-weight construction.
- No raw cross edge is stored simultaneously in global-cross and pair batch
  vectors.
- Persisted formats and recovery interfaces are unchanged.
- All new differentials and existing semantic/full-workspace verification pass.
