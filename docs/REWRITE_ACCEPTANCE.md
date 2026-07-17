# name_uri_analysis_rs code acceptance standard

This document is the standalone code-acceptance document for
[`REWRITE_ARCHITECTURE.md`](./REWRITE_ARCHITECTURE.md). It defines only the conditions that the source
code, modules, algorithms, complexity, resource management, error model and automated tests must meet.

It does not accept the production run process, does not accept real-data results, and does not
prescribe target-machine throughput or result-publication conditions.

## 1. Definition of code completion

Code is complete only when it simultaneously satisfies:

1. the standalone workspace builds;
2. module boundaries and dependency direction match the architecture;
3. data protocols have strong typing and validation;
4. Name, URI, Metadata and statistics algorithms are validated against a reference implementation;
5. the metadata approximate pre-filter has a testable audit module;
6. each stage meets its complexity contract;
7. memory, scratch, queues and spill are constrained by a unified resource interface;
8. determinism, artifact commit and recovery logic are automatically testable;
9. the error types are complete with no silent degradation;
10. all automated code gates pass.

## 2. Workspace and build acceptance

### 2.1 Independence

- All new code lives in `dedup/`;
- no path dependency points outside `dedup/`;
- no import of the original `name_uri_analysis_rs` modules;
- no reading of the original program's intermediate products;
- no reliance on the current repository's implicit working directory;
- default configuration, schema and test fixtures all live inside the new workspace.

CI must copy `dedup/` into an empty directory and run:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release
```

### 2.2 Dependency direction

Code dependencies must match:

```text
dedup-cli
  ├── dedup-linux
  ├── dedup-engine ── dedup-index ── dedup-storage ── dedup-model
  └── dedup-report ────────────────────────────────── dedup-model
```

Acceptance conditions:

- `dedup-model` performs no I/O;
- `dedup-storage` contains no dedup rules;
- the Name, URI and Metadata engines do not call each other;
- engines interact only through entity artifacts and `HitSink`;
- `dedup-cli` implements no similarity or grouping algorithm;
- business engines do not call Linux system interfaces directly;
- `unsafe` appears only in separately tested platform or mmap wrappers.

### 2.3 Code quality

- public types and artifact schemas are documented;
- configuration fields have no undocumented defaults;
- thresholds and resource budgets are not scattered magic constants;
- all integer conversions use checked conversion;
- file offsets, byte counts and work counters use `u64` or wider;
- a digest is never used directly as an equality conclusion;
- no unbounded channels, unbounded task spawning or recursive retries.

## 3. Linux platform code acceptance

`dedup-linux` must wrap:

```text
cgroup v2 resource reader
cpuset reader
CPU and NUMA topology reader
CPU affinity controller
NUMA allocation policy
mount and local-filesystem inspection
signal handling
```

Code conditions:

- resource values follow cgroup/cpuset;
- `/proc`, `/sys` and cgroup file parsing use standalone pure functions;
- platform read interfaces can be injected with fixtures;
- affinity and NUMA setup failures return structured errors;
- `SIGTERM`, `SIGINT`, `SIGHUP` use an explicit state machine;
- non-Linux builds can run unit tests with a mock backend;
- business crates contain no `cfg(target_os = "linux")` branches.

## 4. Data protocol code acceptance

### 4.1 Parquet input

An explicit schema adapter for the seven fields must be defined:

```text
chain
contract_address
token_id
name_norm
token_uri_norm
image_uri_norm
metadata_json
```

Code conditions:

- schema checks precede business row decoding;
- the file ordinal comes from the configured order;
- the file row number is unaffected by parallel row-group completion order;
- only the seven fields are projected;
- the logical digest shares the same input pass as official decoding;
- UTF-8 conversion failure, unknown chains and empty logical primary keys return typed errors.

### 4.2 Logical objects

Strong types are required:

```text
ChainId
ContractId
NftId
StringId
MetadataDocId
SourceOrder
```

Entity merge code must implement:

- the contract key `(chain, contract_address)`;
- the NFT key `(chain, contract_address, token_id)`;
- merging an empty value with a unique non-empty value;
- returning a snapshot conflict for multiple distinct non-empty Name/URI values;
- returning a snapshot conflict for multiple non-empty names in one contract;
- selecting the first valid metadata source;
- identical semantics for the `u32` and `wide_ids` paths.

### 4.3 String dictionary

The digest-keyed map must:

1. use the digest to locate a collision bucket;
2. compare real bytes;
3. reuse a StringId for identical bytes;
4. keep distinct StringIds for distinct bytes;
5. spill only the current hot spot when a bucket exceeds its bound.

Tests must be able to inject a fixed digest function that forces distinct strings to collide.

## 5. Functional algorithm code acceptance

### 5.1 Name

The code must satisfy:

- at most one non-empty Name per contract;
- byte-identical names grouped by StringId;
- large same-name groups do not materialize all in-group contract pairs;
- fuzzy candidates use `CandidateBounds`;
- the final decision uses exact Jaro-Winkler;
- threshold comparison does not depend on borderline floating-point rounding;
- no Union-Find;
- no transitive closure;
- the exact stage returns `BudgetExhausted` at the budget instead of truncating.

### 5.2 URI

The code must satisfy:

- token URI and image URI share one entity scan;
- both have independent group state and bitmaps;
- URI equality is decided only by StringId / real bytes;
- intra-chain classification checks the distinct-contract count;
- cross-chain and matrix classification use chain presence;
- a single very large group supports partial reduce;
- image hits use one per-scope AND-NOT to remove token hits;
- the URI engine has no approximate branch.

### 5.3 Metadata parse and canonical JSON

The code must reject:

- empty content;
- content over 64 KiB;
- content not starting with an object/array;
- invalid JSON;
- duplicate object keys;
- post-normalization key conflicts.

Canonical JSON must:

- normalize and sort object keys;
- apply NFKC, lowercasing and whitespace folding to strings;
- standardize numeric representation;
- keep plain array order;
- align `attributes`;
- keep all real content values;
- produce identical bytes for semantically identical input;
- produce different bytes for real value changes.

Only the `k` anchors per contract are canonicalized. The parse tree is released after single-document
processing; temporary nodes use a resettable arena.

### 5.4 Metadata anchors and template fingerprint

- documents are processed contiguously by ContractId;
- anchors are the first `k` valid tokens by ascending token id (EVM: arbitrary-precision integer
  order; Solana: account-address lexicographic), `k` from configuration;
- each anchor produces canonical bytes, a `canonical_digest` and a content vector;
- the compact template fingerprint is aggregated from the anchors and keeps structural features plus
  discriminative collection-level stable values;
- the fingerprint drops per-token-variable content;
- a contract with no discriminative stable value (structure only, or all anchors identical) is marked
  `low_information`;
- variable-value paths and collection-level value tables are versioned;
- fingerprint bytes, `template_digest` and the MinHash feature vector are independently verifiable.

### 5.5 Metadata pre-filter

Exact template-digest bucket:

- groups by byte-identical `template_digest`;
- excludes `low_information` contracts from bucket-driven candidates;
- does not enumerate all in-bucket pairs; candidate emission obeys the reducer ordering and the
  bucket-size cap;
- candidate count does not grow quadratically with bucket size.

MinHash/LSH:

- runs only on contracts not resolved by an identical digest;
- excludes `low_information` contracts as probe source and as neighbor target, so a
  `low_information` contract generates no LSH probe and no LSH candidate;
- MinHash estimates template-feature Jaccard, so `lsh_bands` (`b`) and `lsh_rows_per_band` (`r`)
  are derived from a `template_jaccard_threshold` (`t_tmpl`) and a target candidate recall using
  `1 - (1 - s^r)^b`, not from the content BM25 threshold;
- the expected recall recorded before generation is scoped to template Jaccard `>= t_tmpl`, and the
  code must not present it as an end-to-end content-BM25 recall guarantee;
- `t_tmpl`, `(b, r)` and the predicted candidate recall are written to the run manifest;
- the probe count is computed before generation;
- candidate quotas apply per source contract and per target chain;
- all truncation information is written to the audit data structure.

The end-to-end recall of the content BM25 decision is provided only by the recall audit
(section 6), never by the pre-filter.

The candidate reducer must stably aggregate digest and LSH evidence and use ContractId as the final
tie-breaker.

### 5.6 Shared-token content verification

- the EVM anchor token-id lists are ordered as integers and intersected;
- verification compares exactly one shared token, the largest shared anchor token id;
- with no shared anchor token id, or when either side is Solana, verification compares each side's
  largest anchor token (max-token fallback);
- Solana builds no token-intersection index beyond the `k`-sized anchor lists;
- byte-identical content first compares canonical bytes;
- other content uses the deterministic BM25 weight cosine;
- verification stops at the first matched token, because counting is contract-level;
- a metadata hit submits a contract-level `HitSink` event.

### 5.7 Scope statistics

A strongly typed `ScopeId` is required:

```text
Intra(chain)
CrossSummary(primary_chain)
Matrix(primary_chain, secondary_chain)
```

Code conditions:

- a cross-chain pair is verified once;
- both directions submit to `HitSink` separately;
- HitSink bitmaps de-duplicate by entity ID;
- the Name NFT count and the Metadata NFT count are summed from the `nft_count` of hit contracts;
- URI marks only the actually matched NFTs;
- engines do not compute the denominator themselves.

## 6. Metadata audit module code acceptance

The audit module must be decoupled from the candidate engine and provide:

```text
StratifiedSampler
ExhaustiveSharedTokenOracle
RecallBreakdown
QualityDecision
```

Code conditions:

- stratified sampling is decided by a fixed seed and stable IDs;
- the oracle computes true shared-token BM25 matches without using pre-filter candidates;
- recall is broken down by digest-bucket cap, LSH bands, candidate quotas and the low-information
  guard;
- the audit distinguishes misses caused by each of those sources;
- insufficient positives return `InsufficientPositives`;
- recall below the configured gate returns `QualityGateFailed`;
- the audit module modifies neither candidates nor `HitSink`;
- every decision can be tested exactly with small fixtures.

This section accepts only the audit code capability, not any real-run recall rate.

## 7. Complexity code acceptance

### 7.1 Scale and counters

| Symbol | Meaning |
|---|---|
| `N` | source rows |
| `M` | logical NFTs |
| `C` | logical contracts |
| `U` | unique strings |
| `B` | projected field byte count |
| `K` | logical-key and string-key byte count |
| `C_meta` | contracts with valid metadata |
| `k` | anchors per contract |
| `B_anchor` | canonicalized anchor byte count (≤ `k · C_meta` documents) |
| `F_tmpl` | template fingerprint feature count |
| `S_name` | total Name characters |
| `A_name` | Name posting updates |
| `P_name` | Jaro-Winkler candidates |
| `E_probe` | LSH probe count |
| `P_prefilter` | metadata candidate pairs |
| `Z_vector` | BM25 term comparisons |
| `H` | HitSink events |

The corresponding counters must be maintained directly by stage code, not inferred from elapsed time.

### 7.2 Input and entity layer

```text
T_scan = O(B + N)
S_scan = O(row_group_batch + writer_buffers)

Expected T_entity_in_memory = O(N + K)

T_entity_external =
  O(N + K + radix_handle_touches)
```

Code conditions:

- there is no second input-digest scan;
- a digest bucket over its bound spills only the hot spot;
- the external radix implementation records handle touches;
- the `ResourcePlan` can select all three modes under a mock budget;
- the three modes produce identical entity artifacts.

### 7.3 URI

```text
Expected T_uri = O(M + H)

T_uri_spill =
  O(spilled_members + radix_handle_touches + H)
```

Tests must directly assert member accesses, bitmap word operations and spill handle touches.

### 7.4 Name safety bound

Let name lengths be `a` and `b` and the threshold be `0.95`. Any real hit must satisfy:

```text
min(a,b) / max(a,b) >= 3/4
```

The global safe character-multiset overlap lower bound:

```text
ceil(7ab / (4(a+b)))
```

When the actual common prefix is below 4 a tighter bound must be used. The `5%` length-difference
formula from review suggestions does not apply to the current scoring definition.

```text
T_name_exact = O(C)

T_name_fuzzy =
  O(S_name + A_name +
    Σ(candidate a,b)(len(a) + len(b)))

Worst T_name_fuzzy = O(C^2 * L_max)
```

The position-queue verifier must satisfy:

```text
character_position_visits <= k_const * (len(a) + len(b))
```

`P_name = O(C * polylog(C))` is not accepted as an unconditional code guarantee. Candidate explosion
is handled by explicit budgets and error returns.

### 7.5 Metadata anchors and template

```text
Expected T_meta_anchor = O(B_anchor)

Expected T_meta_template = O(B_anchor + F_tmpl)
```

Because only `k` anchors per contract are canonicalized, the metadata stage is bounded by contract
count, not by `M`. Tests must assert that anchor canonicalization scales with `k · C_meta` and not
with total NFTs, and that a single large contract does not enlarge other workers' scratch.

### 7.6 Metadata pre-filter

```text
E_probe = lsh_bands * C_effective
C_effective = C_meta - low_information_contracts
```

`E_probe` scales with the number of pre-filter-eligible contracts, because `low_information`
contracts produce no probe.

```text
T_prefilter =
  O(C_meta + E_probe + probe_radix_touches +
    candidate_reduction)
```

Tests use a very large exact template-digest bucket and assert that the emitted candidate count does
not grow with the square of the bucket size, and that `low_information` contracts emit no bucket
candidates and no LSH probe. The probe count must be asserted with no implicit expansion.

### 7.7 Metadata verification

```text
T_meta_verify =
  O(Σ(candidate a,b)
    (k + terms(A[t]) + terms(B[t])))
```

Verification compares exactly one shared token per candidate pair. The EVM anchor intersection uses
merge or galloping by the `k`-sized lists; the Solana path must not trigger a token-intersection
counter beyond the anchors. Tests must assert that verification stops at the first matched token.

### 7.8 Report layer

```text
T_report = O(H + bitmap_words)
S_report = O(R * (C + M) / 8)
```

Tests must prove that a cross-chain pair is verified once and that the two-direction update does not
re-run similarity.

## 8. Resource management code acceptance

### 8.1 MemoryBudget

The code must implement:

```text
available_memory =
  min(physical_memory, cgroup_memory_limit)

stage_memory_limit =
  0.75 * available_memory

in_memory_admission_limit =
  0.50 * stage_memory_limit
```

Acceptance conditions:

- leases use RAII;
- the sum of node-local sub-budgets does not exceed the central budget;
- scratch is taken from and returned to a pool;
- mmap residency estimates are counted in the budget;
- an insufficient budget returns wait, spill or a typed error;
- there is no unbounded retry after an allocation failure;
- a change in worker count does not multiply the global index or bitmaps.

### 8.2 ResourcePlan

Tested with mock capacities:

```text
in_memory
hybrid
external
```

All three modes must produce identical logical entities, StringId grouping and URI bitmaps. A hot
group may spill alone; normal groups must not be forced external with it.

### 8.3 Bounded structures

The following must require a capacity in their type or constructor:

- worker queue;
- writer queue;
- candidate buffer;
- pair reducer buffer;
- HitSink channel;
- per-worker arena;
- LSH probe accumulator;
- spill file set.

Public production constructors without a capacity parameter are forbidden.

## 9. Determinism and recovery code acceptance

### 9.1 Determinism

Using the same fixture, the artifact checksum must stay identical after changing:

- worker count;
- row-group completion order;
- task scheduling order;
- the NUMA mock topology;
- in-memory vs hybrid mode;
- the interruption point.

Changing the explicit input file order may change the metadata first-valid source and anchor
selection.

### 9.2 Artifact commit

The artifact writer must run a state machine:

```text
Writing
Flushed
ManifestWritten
DirectorySynced
Renamed
SuccessMarked
```

Tests must inject a failure at each state and assert:

- an unfinished directory is not recognized as a valid artifact;
- `_SUCCESS` appears only last;
- a checksum mismatch refuses reuse;
- a cross-file-system rename is rejected;
- recovery does not resubmit HitSink events.

### 9.3 Signal state machine

Verified with a mock signal source:

- `SIGTERM` stops new task intake;
- the current atomic task can finish;
- `SIGINT` enters controlled shutdown;
- a second `SIGINT` terminates immediately;
- `SIGHUP` only changes the log level.

## 10. Error model and counter interface acceptance

A structured error enum is required, containing at least:

```text
InvalidInput
SchemaMismatch
SnapshotConflict
InvalidMetadata
ResourceBudgetExceeded
BudgetExhausted
ArtifactMismatch
QualityGateFailed
InsufficientPositives
PlatformCapabilityMissing
InvariantViolation
```

Code conditions:

- the exact stage cannot turn `BudgetExhausted` into success;
- approximate-stage truncation must produce an audit record;
- errors keep the stage, partition and stable object ID;
- log text is not a control-flow interface;
- counters use strongly typed names and checked increments;
- progress advances by input-object or candidate work, not by hit count.

The following stage counters must be provided:

```text
rows_scanned
entity_digest_bucket_max
entity_radix_handle_touches
uri_spilled_members
name_posting_touches
name_unique_candidates
name_character_position_visits
metadata_anchor_documents
metadata_template_features
metadata_low_information_contracts
metadata_prefilter_probes
metadata_prefilter_candidates
metadata_verify_pairs
token_id_comparisons
bm25_term_comparisons
hit_events
spill_bytes
```

## 11. Automated test acceptance

### 11.1 Oracle

- the small-data oracle is implemented independently and does not call the production candidate
  generator;
- Name, URI, Metadata and all ScopeIds output object bitmaps;
- the production bitmap is compared bit-by-bit with the oracle;
- the oracle fixture is fixed and readable.

### 11.2 Property tests

- Name CandidateBounds cover the exhaustive hits;
- the queue Jaro-Winkler equals a simple reference implementation;
- URI grouping equals the set definition;
- canonical JSON is stable to representational differences and sensitive to real value differences;
- a digest collision does not merge distinct bytes;
- the chain matrix's two directions are independent;
- bitmap OR / AND-NOT match set operations;
- the metadata template fingerprint carries collection-level stable values, and structure-only
  fingerprints are flagged `low_information`.

### 11.3 Golden cases

- byte-identical and fuzzy Name;
- large same-name group;
- token URI over image URI priority;
- shared-token identical metadata (contract-level hit, all NFTs counted);
- EVM no shared token (max-token fallback);
- Solana max-token;
- EVM token ids ordered as integers (e.g. `"10"` sorts after `"2"`), asserting anchor and shared-token
  selection use numeric order;
- large exact template-digest bucket;
- LSH near-identical template candidates;
- low-information placeholder contract (no cross-contract grouping);
- duplicate source and snapshot conflict;
- every artifact failure injection point.

### 11.4 Growth and adversarial tests

Independently scale the following and assert work counters:

- source rows;
- unique URIs;
- single URI group;
- Name postings;
- Name candidates;
- anchors per contract;
- template feature DF;
- metadata candidate pairs.

At least:

- all URIs identical;
- all Names identical;
- high-frequency-character Name;
- different permutations of the same character multiset;
- a single very large contract;
- all template structures identical (structure-only, must be flagged low-information);
- one discriminative stable value shared by all templates;
- LSH reaching the probe budget;
- many single-token Solana contracts;
- the 64 KiB boundary and many invalid JSON documents.

## 12. Final code gate

- [ ] the standalone workspace builds;
- [ ] fmt, Clippy and all tests pass;
- [ ] the dependency-direction check passes;
- [ ] Linux platform interfaces can be mocked;
- [ ] data protocol and conflict-handling tests pass;
- [ ] the Name lossless filter matches the reference implementation;
- [ ] URI exact tests pass;
- [ ] metadata anchor, template, pre-filter, verification and audit tests pass;
- [ ] complexity counter tests pass;
- [ ] resource plan and bounded-structure tests pass;
- [ ] artifact failure-injection and recovery tests pass;
- [ ] the error model and stage counters are complete.

Only when all are checked is the code implementation considered to meet the acceptance standard.
