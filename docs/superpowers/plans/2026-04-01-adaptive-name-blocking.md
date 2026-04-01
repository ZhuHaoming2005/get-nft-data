# Adaptive Name Blocking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current first-token name blocking rule with an adaptive separator-insensitive blocking flow that reduces tiny tasks while keeping heavy matching work in C++.

**Architecture:** Python will own blocking-feature preparation, adaptive `name_block_key` assignment, and task generation. PostgreSQL will do the large grouping/update work for canopy statistics. The existing C++ worker contract stays unchanged and continues to own candidate recall and final similarity scoring inside each block.

**Tech Stack:** Python 3, PostgreSQL, psycopg2, unittest/pytest, existing C++ worker

---

## File Structure

- Create: `name_symbol_stats_v2/blocking.py`
  - Pure adaptive blocking helpers, thresholds, and key-selection logic.
- Modify: `name_symbol_stats_v2/normalize.py`
  - Add `collapse_name_for_blocking()` and freeze the old rule behind a legacy helper.
- Modify: `name_symbol_stats_v2/sql/01_schema.sql`
  - Add persisted collapsed-name columns and supporting lookup index on `nsv2_name_atoms`.
- Modify: `name_symbol_stats_v2/identity.py`
  - Populate `name_collapsed` and `name_collapsed_len` when rebuilding `nsv2_name_atoms`.
- Modify: `name_symbol_stats_v2/work_items.py`
  - Add blocking-strategy options and adaptive `name_block_key` assignment before work-item aggregation.
- Modify: `name_symbol_stats_v2/main.py`
  - Expose `--blocking-strategy` for `prepare-name-tasks`.
- Modify: `name_symbol_stats_v2/README.md`
  - Document the adaptive prepare command.
- Modify: `tests/name_symbol_stats_v2/test_normalize.py`
  - Cover separator-insensitive collapse behavior.
- Create: `tests/name_symbol_stats_v2/test_blocking.py`
  - Pure tests for adaptive canopy selection.
- Modify: `tests/name_symbol_stats_v2/test_identity.py`
  - Verify the atom rebuild SQL now writes collapsed columns.
- Create: `tests/name_symbol_stats_v2/test_work_items.py`
  - Verify adaptive block-key assignment and oversize fallback behavior with fake cursors.
- Create: `tests/name_symbol_stats_v2/test_main.py`
  - Verify CLI parsing of the new blocking-strategy flag.

### Task 1: Add Pure Blocking Primitives

**Files:**
- Create: `name_symbol_stats_v2/blocking.py`
- Modify: `name_symbol_stats_v2/normalize.py`
- Modify: `tests/name_symbol_stats_v2/test_normalize.py`
- Create: `tests/name_symbol_stats_v2/test_blocking.py`

- [ ] **Step 1: Write the failing normalize and adaptive-blocking tests**

```python
# tests/name_symbol_stats_v2/test_normalize.py
import unittest

from name_symbol_stats_v2.normalize import collapse_name_for_blocking, normalize_name


class NormalizeTests(unittest.TestCase):
    def test_collapse_name_for_blocking_removes_separators(self):
        self.assertEqual(collapse_name_for_blocking(normalize_name('Andy Duboc')), 'andyduboc')
        self.assertEqual(collapse_name_for_blocking(normalize_name('andy-duboc')), 'andyduboc')
        self.assertEqual(collapse_name_for_blocking(normalize_name('andy_duboc')), 'andyduboc')
```

```python
# tests/name_symbol_stats_v2/test_blocking.py
import unittest

from name_symbol_stats_v2.blocking import AdaptiveBlockingConfig, AdaptiveCanopyCounts, choose_adaptive_block_key


class AdaptiveBlockingTests(unittest.TestCase):
    def test_choose_adaptive_block_key_keeps_small_canopies_merged(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=12, p3_len_count=12, p4_len_count=12)
        self.assertEqual(choose_adaptive_block_key('andyduboc', 10, counts, config), 'and')

    def test_choose_adaptive_block_key_uses_length_bucket_for_medium_canopies(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=8000, p3_len_count=2400, p4_len_count=2400)
        self.assertEqual(choose_adaptive_block_key('andyduboc', 10, counts, config), 'and|6')

    def test_choose_adaptive_block_key_refines_hot_canopies(self):
        config = AdaptiveBlockingConfig()
        counts = AdaptiveCanopyCounts(p3_count=12000, p3_len_count=45000, p4_len_count=32000)
        self.assertEqual(choose_adaptive_block_key('spaceape', 8, counts, config), 'space|6')
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/name_symbol_stats_v2/test_normalize.py tests/name_symbol_stats_v2/test_blocking.py -q`
Expected: FAIL with import errors because `collapse_name_for_blocking`, `AdaptiveBlockingConfig`, `AdaptiveCanopyCounts`, and `choose_adaptive_block_key` do not exist yet.

- [ ] **Step 3: Write the minimal normalization and blocking implementation**

```python
# name_symbol_stats_v2/normalize.py
import re

_TOKEN_RE = re.compile(r"[0-9a-z]+")


def collapse_name_for_blocking(name_norm: str) -> str:
    if not name_norm:
        return ""
    return "".join(_TOKEN_RE.findall(name_norm))


def build_legacy_name_block_key(name_norm: str) -> str:
    tokens = tokenize_name(name_norm)
    if not tokens:
        return ""
    return f"{tokens[0]}|{name_length_bucket(len(name_norm))}"


def build_name_block_key(name_norm: str) -> str:
    return build_legacy_name_block_key(name_norm)
```

```python
# name_symbol_stats_v2/blocking.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdaptiveBlockingConfig:
    small_canopy_max: int = 64
    medium_canopy_max: int = 8000
    large_canopy_max: int = 30000
    length_bucket_size: int = 6


@dataclass(frozen=True)
class AdaptiveCanopyCounts:
    p3_count: int
    p3_len_count: int
    p4_len_count: int


def collapsed_length_bucket(length: int, *, bucket_size: int = 6) -> int:
    if length <= 0:
        return 0
    return (length // bucket_size) * bucket_size


def collapsed_prefix(value: str, length: int) -> str:
    return value[:length] if value else ""


def choose_adaptive_block_key(name_collapsed: str, name_collapsed_len: int, counts: AdaptiveCanopyCounts, config: AdaptiveBlockingConfig) -> str:
    p3 = collapsed_prefix(name_collapsed, 3)
    p4 = collapsed_prefix(name_collapsed, 4)
    p5 = collapsed_prefix(name_collapsed, 5)
    len6 = collapsed_length_bucket(name_collapsed_len, bucket_size=config.length_bucket_size)
    if counts.p3_count < config.small_canopy_max:
        return p3
    if counts.p3_count <= config.medium_canopy_max:
        return f"{p3}|{len6}"
    if counts.p4_len_count > config.large_canopy_max:
        return f"{p5}|{len6}"
    return f"{p4}|{len6}"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/name_symbol_stats_v2/test_normalize.py tests/name_symbol_stats_v2/test_blocking.py -q`
Expected: PASS with both test modules green.

- [ ] **Step 5: Commit**

```bash
git add tests/name_symbol_stats_v2/test_normalize.py tests/name_symbol_stats_v2/test_blocking.py name_symbol_stats_v2/normalize.py name_symbol_stats_v2/blocking.py
git commit -m "feat: add adaptive blocking primitives"
```

### Task 2: Persist Collapsed Blocking Features On Name Atoms

**Files:**
- Modify: `name_symbol_stats_v2/sql/01_schema.sql`
- Modify: `name_symbol_stats_v2/identity.py`
- Modify: `tests/name_symbol_stats_v2/test_identity.py`

- [ ] **Step 1: Write the failing identity test for collapsed atom columns**

```python
# tests/name_symbol_stats_v2/test_identity.py
import unittest

from name_symbol_stats_v2.identity import _rebuild_name_atoms


class IdentityTests(unittest.TestCase):
    def test_rebuild_name_atoms_inserts_collapsed_columns(self):
        conn = _FakeConnection()

        _rebuild_name_atoms(conn, 'apr01', ['ethereum'])

        insert_sql, insert_params = conn.cursor_obj.statements[1]
        self.assertIn('name_collapsed', insert_sql)
        self.assertIn('name_collapsed_len', insert_sql)
        self.assertEqual(insert_params, ('apr01', ['ethereum']))
```

- [ ] **Step 2: Run the identity test to verify it fails**

Run: `python -m pytest tests/name_symbol_stats_v2/test_identity.py -q`
Expected: FAIL because the current rebuild SQL does not mention `name_collapsed` or `name_collapsed_len`.

- [ ] **Step 3: Add the schema columns and rebuild SQL**

```sql
-- name_symbol_stats_v2/sql/01_schema.sql
ALTER TABLE nsv2_name_atoms
    ADD COLUMN IF NOT EXISTS name_collapsed TEXT NOT NULL DEFAULT '';

ALTER TABLE nsv2_name_atoms
    ADD COLUMN IF NOT EXISTS name_collapsed_len INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_nsv2_atoms_run_collapsed
    ON nsv2_name_atoms (run_label, name_collapsed, name_collapsed_len);
```

```python
# name_symbol_stats_v2/identity.py
cur.execute(
    """
    INSERT INTO nsv2_name_atoms (
        run_label, chain, name_norm, sample_contract_address, contract_count, nft_count,
        name_len, name_len_bucket, name_collapsed, name_collapsed_len,
        name_block_key, name_signature, name_signature_hash
    )
    SELECT run_label,
           chain,
           name_norm,
           min(contract_address) AS sample_contract_address,
           count(*)::bigint AS contract_count,
           coalesce(sum(nft_count), 0)::bigint AS nft_count,
           min(name_len)::int AS name_len,
           min(name_len_bucket)::int AS name_len_bucket,
           regexp_replace(min(name_norm), '[^0-9a-z]+', '', 'g') AS name_collapsed,
           char_length(regexp_replace(min(name_norm), '[^0-9a-z]+', '', 'g'))::int AS name_collapsed_len,
           min(name_block_key) AS name_block_key,
           min(name_signature) AS name_signature,
           min(name_signature_hash) AS name_signature_hash
    FROM nsv2_contract_identity
    WHERE run_label = %s
      AND chain = ANY(%s)
      AND name_norm <> ''
      AND name_block_key <> ''
    GROUP BY run_label, chain, name_norm
    """,
    (run_label, list(chains)),
)
```

- [ ] **Step 4: Run the identity tests to verify they pass**

Run: `python -m pytest tests/name_symbol_stats_v2/test_identity.py -q`
Expected: PASS with the SQL assertion updated.

- [ ] **Step 5: Commit**

```bash
git add tests/name_symbol_stats_v2/test_identity.py name_symbol_stats_v2/sql/01_schema.sql name_symbol_stats_v2/identity.py
git commit -m "feat: persist collapsed name atom fields"
```

### Task 3: Implement Adaptive Block Assignment During Task Preparation

**Files:**
- Modify: `name_symbol_stats_v2/work_items.py`
- Modify: `name_symbol_stats_v2/main.py`
- Create: `tests/name_symbol_stats_v2/test_work_items.py`
- Create: `tests/name_symbol_stats_v2/test_main.py`

- [ ] **Step 1: Write the failing work-item and CLI tests**

```python
# tests/name_symbol_stats_v2/test_work_items.py
import unittest

from name_symbol_stats_v2.work_items import WorkItem, build_work_items


class _Cursor:
    def __init__(self, block_rows, split_rows=()):
        self.block_rows = list(block_rows)
        self.split_rows = list(split_rows)
        self.statements = []
        self._rows = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if 'SELECT name_block_key, count(*)::int AS atom_count' in sql:
            self._rows = list(self.block_rows)
        elif 'SELECT left(name_signature_hash' in sql:
            self._rows = list(self.split_rows)
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self, block_rows, split_rows=()):
        self.cursor_obj = _Cursor(block_rows, split_rows)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class WorkItemTests(unittest.TestCase):
    def test_build_work_items_applies_adaptive_block_assignment_before_grouping(self):
        conn = _Connection([('and', 12)])

        items = build_work_items(
            conn,
            'apr01',
            ['ethereum', 'base'],
            blocking_strategy='adaptive_v1',
            max_atoms_per_task=30000,
        )

        executed_sql = '\n'.join(sql for sql, _ in conn.cursor_obj.statements)
        self.assertIn('UPDATE nsv2_name_atoms', executed_sql)
        self.assertIn('name_collapsed', executed_sql)
        self.assertEqual(items[0], WorkItem('apr01', 'ethereum,base', 'and', '', 12))

    def test_build_work_items_keeps_signature_split_for_oversized_blocks(self):
        conn = _Connection([('space|6', 240)], [('abcd', 120), ('abef', 120)])

        items = build_work_items(conn, 'apr01', ['ethereum'], blocking_strategy='adaptive_v1', max_atoms_per_task=100)

        self.assertEqual([item.signature_prefix for item in items], ['abcd', 'abef'])
```

```python
# tests/name_symbol_stats_v2/test_main.py
import unittest

from name_symbol_stats_v2.main import build_parser


class MainParserTests(unittest.TestCase):
    def test_prepare_name_tasks_accepts_blocking_strategy(self):
        parser = build_parser()
        args = parser.parse_args([
            'prepare-name-tasks',
            '--run-label', 'apr01',
            '--chains', 'ethereum',
            '--blocking-strategy', 'adaptive_v1',
        ])
        self.assertEqual(args.blocking_strategy, 'adaptive_v1')
```

- [ ] **Step 2: Run the work-item and parser tests to verify they fail**

Run: `python -m pytest tests/name_symbol_stats_v2/test_work_items.py tests/name_symbol_stats_v2/test_main.py -q`
Expected: FAIL because `build_work_items()` does not accept `blocking_strategy`, there is no adaptive update SQL, and the parser has no `--blocking-strategy` flag.

- [ ] **Step 3: Implement adaptive block-key assignment and CLI wiring**

```python
# name_symbol_stats_v2/work_items.py
from dataclasses import dataclass

from .blocking import AdaptiveBlockingConfig


@dataclass(frozen=True)
class PrepareNameTaskOptions:
    blocking_strategy: str = 'legacy'
    adaptive: AdaptiveBlockingConfig = AdaptiveBlockingConfig()
    max_atoms_per_task: int = 50000
    max_prefix_len: int = 10


def _assign_name_block_keys(conn, run_label: str, chains: Sequence[str], options: PrepareNameTaskOptions) -> None:
    if options.blocking_strategy == 'legacy':
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH base AS (
                SELECT atom_id,
                       name_collapsed,
                       name_collapsed_len,
                       left(name_collapsed, 3) AS p3,
                       left(name_collapsed, 4) AS p4,
                       left(name_collapsed, 5) AS p5,
                       (name_collapsed_len / %s) * %s AS len6
                FROM nsv2_name_atoms
                WHERE run_label = %s
                  AND chain = ANY(%s)
                  AND name_collapsed <> ''
            ),
            p3_counts AS (
                SELECT p3, count(*)::int AS p3_count
                FROM base
                GROUP BY p3
            ),
            p3_len_counts AS (
                SELECT p3, len6, count(*)::int AS p3_len_count
                FROM base
                GROUP BY p3, len6
            ),
            p4_len_counts AS (
                SELECT p4, len6, count(*)::int AS p4_len_count
                FROM base
                GROUP BY p4, len6
            ),
            assigned AS (
                SELECT base.atom_id,
                       CASE
                           WHEN p3_counts.p3_count < %s THEN base.p3
                           WHEN p3_counts.p3_count <= %s THEN base.p3 || '|' || base.len6::text
                           WHEN p4_len_counts.p4_len_count > %s THEN base.p5 || '|' || base.len6::text
                           ELSE base.p4 || '|' || base.len6::text
                       END AS final_block_key
                FROM base
                JOIN p3_counts ON p3_counts.p3 = base.p3
                JOIN p3_len_counts ON p3_len_counts.p3 = base.p3 AND p3_len_counts.len6 = base.len6
                JOIN p4_len_counts ON p4_len_counts.p4 = base.p4 AND p4_len_counts.len6 = base.len6
            )
            UPDATE nsv2_name_atoms atoms
            SET name_block_key = assigned.final_block_key
            FROM assigned
            WHERE atoms.atom_id = assigned.atom_id
            """,
            (
                options.adaptive.length_bucket_size,
                options.adaptive.length_bucket_size,
                run_label,
                list(chains),
                options.adaptive.small_canopy_max,
                options.adaptive.medium_canopy_max,
                options.adaptive.large_canopy_max,
            ),
        )
    conn.commit()


def build_work_items(
    conn,
    run_label: str,
    chains: Sequence[str],
    *,
    blocking_strategy: str = 'legacy',
    max_atoms_per_task: int = 50000,
    max_prefix_len: int = 10,
) -> list[WorkItem]:
    _delete_outputs(conn, run_label, chains)
    options = PrepareNameTaskOptions(
        blocking_strategy=blocking_strategy,
        max_atoms_per_task=max_atoms_per_task,
        max_prefix_len=max_prefix_len,
    )
    _assign_name_block_keys(conn, run_label, chains, options)
    chains_csv = ','.join(chains)
    work_items: list[WorkItem] = []
    block_sizes = _fetch_block_sizes(conn, run_label, chains)
    for block_key, atom_count in block_sizes:
        if atom_count <= max_atoms_per_task:
            work_items.append(WorkItem(run_label, chains_csv, block_key, '', atom_count))
            continue
        splits = _fetch_signature_splits(conn, run_label, chains, block_key, 4)
        for signature_prefix, split_count in splits:
            work_items.append(WorkItem(run_label, chains_csv, block_key, signature_prefix, split_count))
    return work_items
```

```python
# name_symbol_stats_v2/main.py
work_parser.add_argument('--blocking-strategy', choices=['legacy', 'adaptive_v1'], default='legacy')

work_items = build_work_items(
    conn,
    run_label,
    chains,
    blocking_strategy=args.blocking_strategy,
    max_atoms_per_task=args.max_atoms_per_task,
)
```

- [ ] **Step 4: Run the work-item and parser tests to verify they pass**

Run: `python -m pytest tests/name_symbol_stats_v2/test_work_items.py tests/name_symbol_stats_v2/test_main.py -q`
Expected: PASS with adaptive SQL assignment and parser coverage.

- [ ] **Step 5: Commit**

```bash
git add tests/name_symbol_stats_v2/test_work_items.py tests/name_symbol_stats_v2/test_main.py name_symbol_stats_v2/work_items.py name_symbol_stats_v2/main.py
git commit -m "feat: add adaptive name task blocking"
```

### Task 4: Document And Verify The Rollout Path

**Files:**
- Modify: `name_symbol_stats_v2/README.md`

- [ ] **Step 1: Record the new prepare command in the README example section**

```markdown
# name_symbol_stats_v2/README.md
python -m name_symbol_stats_v2.main prepare-name-tasks --run-label apr01 --chains ethereum base polygon solana --blocking-strategy adaptive_v1 --max-atoms-per-task 30000
```

- [ ] **Step 2: Run the focused regression suite before finalizing the docs**

Run: `python -m pytest tests/name_symbol_stats_v2/test_normalize.py tests/name_symbol_stats_v2/test_blocking.py tests/name_symbol_stats_v2/test_identity.py tests/name_symbol_stats_v2/test_work_items.py tests/name_symbol_stats_v2/test_main.py -q`
Expected: PASS. If any test fails, fix code before changing docs.

- [ ] **Step 3: Update the README command examples**

```markdown
# name_symbol_stats_v2/README.md
python -m name_symbol_stats_v2.main build-contract-identity --run-label apr01 --chains ethereum base polygon solana
python -m name_symbol_stats_v2.main prepare-name-tasks --run-label apr01 --chains ethereum base polygon solana --blocking-strategy adaptive_v1 --max-atoms-per-task 30000
cmake -S name_symbol_stats_v2/cpp -B name_symbol_stats_v2/cpp/build
cmake --build name_symbol_stats_v2/cpp/build --config Release
python -m name_symbol_stats_v2.main run-name-worker --run-label apr01 --worker-exe name_symbol_stats_v2/cpp/build/Release/name_worker.exe --thresholds 85 90 95 --parallel-workers 12
```

- [ ] **Step 4: Run the end-to-end comparison commands**

Run: `python -m name_symbol_stats_v2.main prepare-name-tasks --run-label apr01 --chains ethereum base polygon solana --blocking-strategy legacy --max-atoms-per-task 30000`
Expected: command completes and writes the baseline work-item set.

Run: `python -m name_symbol_stats_v2.main prepare-name-tasks --run-label apr01 --chains ethereum base polygon solana --blocking-strategy adaptive_v1 --max-atoms-per-task 30000`
Expected: command completes and writes the adaptive work-item set.

Run: `python -m pytest tests/name_symbol_stats_v2/test_normalize.py tests/name_symbol_stats_v2/test_blocking.py tests/name_symbol_stats_v2/test_identity.py tests/name_symbol_stats_v2/test_work_items.py tests/name_symbol_stats_v2/test_main.py -q`
Expected: PASS after the README update as well.

- [ ] **Step 5: Commit**

```bash
git add name_symbol_stats_v2/README.md
git commit -m "docs: document adaptive name blocking rollout"
```

## Spec Coverage Check

- Separator-insensitive grouping is implemented in Task 1 and persisted for task prep in Task 2.
- Adaptive canopy selection is implemented in Task 3.
- `legacy` versus `adaptive_v1` compatibility is implemented in Task 3 and exercised in Task 4.
- Oversize fallback via `signature_prefix` remains in Task 3 because the existing split path is preserved after adaptive assignment.
- Rollout metrics and commands are covered in Task 4.
