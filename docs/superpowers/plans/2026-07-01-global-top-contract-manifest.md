# Global Top Contract Manifest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace four independent OpenSea rankings with one four-chain Top collection ranking and export deterministic `(chain,address)` pairs without connecting them to an analyzer.

**Architecture:** The Python fetcher sends one `/api/v2/collections/top` request sequence with a comma-separated four-chain filter and `sort_by=thirty_days_volume`. It expands ranked collections into canonical contract pairs, writes an exact two-column CSV plus an audit JSON, and fails instead of falling back to per-chain rankings. `name_uri_analysis_rs` remains unchanged and continues full-snapshot analysis.

**Tech Stack:** Python 3 standard library, `unittest`, AST syntax validation.

---

## File Structure

- Modify `name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py`
  - Own the global request, cursor validation, pair canonicalization, and
    atomic CSV/JSON output.
- Modify `name_metadata_change_samples/tests/test_fetch_opensea_top_seeds.py`
  - Keep focused address, default-chain, and JSON parsing regression tests.
- Create `name_metadata_change_samples/tests/test_fetch_opensea_global_top.py`
  - Test global ranking semantics and the new output contracts.
- Modify `name_metadata_change_samples/README.md`
  - Document the shared ranking and pair outputs.
- Modify `docs/superpowers/specs/2026-07-01-global-top-contract-manifest-design.md`
  - Record that analyzer integration is explicitly deferred.

### Task 1: Fetch one globally ordered four-chain population

**Files:**
- Modify: `name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py`
- Test: `name_metadata_change_samples/tests/test_fetch_opensea_global_top.py`

- [ ] **Step 1: Add failing URL, pagination, and extraction tests**

Test that the URL query parses to:

```python
{
    "chains": ["ethereum,base,polygon,solana"],
    "limit": ["50"],
    "sort_by": ["thirty_days_volume"],
}
```

Use two in-memory JSON responses to verify one cursor sequence, a global
collection limit, skipped invalid collections, and collection expansion across
different chains. Add separate assertions that repeated cursors and short
result sets raise `ValueError`.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_global_top -v
```

Expected: failures report missing `build_top_collections_url`,
`collect_ranked_collections`, `ContractPair`, and `RankedCollection`.

- [ ] **Step 3: Implement the request and extraction model**

Add immutable `ContractPair` and `RankedCollection` records. Build requests
with:

```python
query = {
    "chains": ",".join(chains),
    "limit": page_size,
    "sort_by": "thirty_days_volume",
}
```

For every contract object, require an explicit supported chain and valid
address. Lowercase EVM addresses, preserve validated 32-byte Solana Base58
addresses, and deduplicate by `(canonical chain, canonical address)`.

- [ ] **Step 4: Implement safe global pagination**

Maintain one cursor and one `seen_cursors` set. Count only collections that
yield at least one valid selected-chain pair. Raise if the response lacks a
collection list, repeats a cursor, or ends before the requested collection
limit.

- [ ] **Step 5: Run focused and legacy tests**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_global_top -v
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_top_seeds -v
```

Expected: all tests pass.

### Task 2: Export pair CSV and ranking audit JSON

**Files:**
- Modify: `name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py`
- Test: `name_metadata_change_samples/tests/test_fetch_opensea_global_top.py`

- [ ] **Step 1: Add failing serialization tests**

Assert that two identical address strings on different chains remain separate,
that a duplicate pair in a later collection retains the earlier ordering, and
that rendered CSV parses exactly as:

```csv
chain,address
solana,So11111111111111111111111111111111111111112
```

Assert that the JSON audit retains global rank, slug, name, ranking criterion,
ranking value, canonical pair, and a raw chain label only when it differs.

- [ ] **Step 2: Run the serializer test and verify RED**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_global_top.FetchOpenSeaGlobalTopTest.test_rank_output_serializers_emit_exact_pair_csv_and_audit_json -v
```

Expected: failure reports missing `render_manifest_csv`.

- [ ] **Step 3: Implement deterministic serialization and atomic replacement**

Build both serialized values from the same `ranked` list. Write sibling
temporary files, then replace `top_contracts.csv` and
`top_collections.json` only after both serializations succeed. Always attempt
to remove temporary siblings in `finally`.

- [ ] **Step 4: Replace main-path per-chain output**

Default to all four chains and these output paths:

```text
../seeds/top_contracts.csv
../seeds/top_collections.json
```

Call `collect_ranked_collections` once and `write_rank_outputs` once. Do not
invoke a Rust analyzer.

- [ ] **Step 5: Run the complete Python verification**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_global_top -v
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_top_seeds -v
python -c "import ast, pathlib; ast.parse(pathlib.Path(r'name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py').read_text(encoding='utf-8'))"
```

Expected: 13 tests pass and AST parsing succeeds.

### Task 3: Document and verify the boundary

**Files:**
- Modify: `name_metadata_change_samples/README.md`
- Modify: `docs/superpowers/specs/2026-07-01-global-top-contract-manifest-design.md`

- [ ] **Step 1: Document the global ranking command**

Document:

```powershell
python .\scripts\fetch_opensea_top_seeds.py `
  --chains ethereum base polygon solana `
  --limit 100 `
  --output-dir .\seeds
```

Explain that the limit applies globally to ranked collections and may expand
to more CSV rows when collections contain multiple contracts.

- [ ] **Step 2: Document the deferred analyzer integration**

State explicitly that `name_uri_analysis_rs` continues full-snapshot
deduplication and does not read `top_contracts.csv`. Leave future
Top-contract-specific cross-chain analysis outside this change.

- [ ] **Step 3: Run final checks**

Run:

```powershell
rg -n "collections/top|thirty_days_volume|top_contracts.csv|top_collections.json" name_metadata_change_samples/README.md name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py
git diff --check
git status --short
```

Expected: documentation and implementation use the same endpoint, ranking
criterion, and output names; no `name_uri_analysis_rs` files are modified.

### Task 4: Preserve the default API key and reject colliding outputs

**Files:**
- Modify: `name_metadata_change_samples/tests/test_fetch_opensea_top_seeds.py`
- Modify: `name_metadata_change_samples/tests/test_fetch_opensea_global_top.py`
- Modify: `name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py`

- [ ] **Step 1: Correct the default-key assertion**

Assert that `parse_args([]).api_key` equals the existing built-in default
instead of asserting that it is `None`.

- [ ] **Step 2: Add a failing output-collision test**

Parse arguments that point `--contracts-output` and `--audit-output` at the
same file. Assert that parsing exits with an error whose stderr contains
`must identify different files`.

- [ ] **Step 3: Run the collision test and verify RED**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_global_top.FetchOpenSeaGlobalTopTest.test_parse_args_rejects_colliding_output_paths -v
```

Expected: failure because the parser currently accepts the colliding paths.

- [ ] **Step 4: Implement normalized path validation**

After resolving default output paths, compare normalized absolute paths with
`os.path.abspath` and `os.path.normcase`. Call `parser.error(...)` when they
are equal. Keep the existing `--api-key` default unchanged.

- [ ] **Step 5: Run the complete Python verification**

Run:

```powershell
python -m unittest discover -s name_metadata_change_samples\tests -p "test_fetch_opensea*.py" -v
python -c "import ast, pathlib; ast.parse(pathlib.Path(r'name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py').read_text(encoding='utf-8'))"
```

Expected: 14 tests pass and AST parsing succeeds.
