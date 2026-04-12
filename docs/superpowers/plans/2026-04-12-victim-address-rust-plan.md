# Victim Address Rust Offload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `build_victim_address_records` hot-path computation from Python into Rust while preserving the current JSON output shape and semantics.

**Architecture:** Keep the Python function signature and returned row schema unchanged. Add a Rust helper that receives flattened sale, transfer, owner, and metric payloads, computes buyer-level victim rows with the same ordering and stuck-holder logic, and returns plain Python dictionaries through the bridge.

**Tech Stack:** Python 3.12, PyO3, Rust, pytest

---

### Task 1: Lock In Victim Row Semantics With Tests

**Files:**
- Modify: `D:\code\solidity\get-nft-data\tests\test_top_contract_analysis.py`
- Test: `D:\code\solidity\get-nft-data\tests\test_top_contract_analysis.py`

- [ ] **Step 1: Write the failing test**

```python
def test_build_victim_address_records_marks_sale_unstuck_after_later_transfer():
    sales = [
        mod.NFTSaleRecord(
            contract_address='0xdup',
            token_id='1',
            tx_hash='0xbuy',
            block_number=10,
            log_index=1,
            bundle_index=0,
            buyer_address='0xbuyer',
            seller_address='0xseller',
            marketplace='test',
            taker='buyer',
            payment_token_symbol='ETH',
            price_eth=1.0,
            is_native_eth=True,
        )
    ]
    transfers = [
        mod.TransferRecord(
            contract_address='0xdup',
            token_id='1',
            tx_hash='0xbuy',
            log_index=1,
            block_number=10,
            block_time=100,
            from_address='0xseller',
            to_address='0xbuyer',
            event_type='erc721',
            source='alchemy',
        ),
        mod.TransferRecord(
            contract_address='0xdup',
            token_id='1',
            tx_hash='0xlater',
            log_index=0,
            block_number=11,
            block_time=200,
            from_address='0xbuyer',
            to_address='0xnext',
            event_type='erc721',
            source='alchemy',
        ),
    ]
    owners = [mod.OwnerBalance(owner_address='0xbuyer', token_balances={'1': 1})]

    rows = analysis_mod.build_victim_address_records(
        contract_address='0xdup',
        sales=sales,
        transfers=transfers,
        owners=owners,
        sale_metrics_by_tx={'0xbuy': {'ratio_status': 'ok'}},
    )

    assert rows[0]['is_stuck'] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests\test_top_contract_analysis.py -k "build_victim_address_records_marks_sale_unstuck_after_later_transfer" -v`
Expected: FAIL because the regression case is not covered yet or the new Rust bridge path is not implemented.

- [ ] **Step 3: Write minimal implementation**

```python
def build_victim_address_records(...):
    packed_sales = [...]
    packed_transfers = [...]
    packed_owners = [...]
    packed_metrics = [...]
    return list(_rust_build_victim_address_records(...))
```

```rust
#[pyfunction]
fn build_victim_address_records(...) -> PyResult<Vec<PyObject>> {
    // Build owner token map, latest outgoing transfer index, and buyer aggregates.
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests\test_top_contract_analysis.py tests\test_top_contract_analysis_accelerated.py -k "victim_address_records or top_contract_analysis" -v`
Expected: PASS for the new regression test and existing victim-address coverage.

- [ ] **Step 5: Commit**

```bash
git add tests/test_top_contract_analysis.py tests/test_top_contract_analysis_accelerated.py top_contract_analysis/rust_bridge.py top_contract_analysis/analysis.py rust_ext/top_contract_analysis_rust/src/lib.rs
git commit -m "feat: offload victim address analysis to rust"
```

### Task 2: Rebuild Extension And Verify End-To-End

**Files:**
- Modify: `D:\code\solidity\get-nft-data\rust_ext\top_contract_analysis_rust\src\lib.rs`
- Modify: `D:\code\solidity\get-nft-data\top_contract_analysis\rust_bridge.py`
- Modify: `D:\code\solidity\get-nft-data\top_contract_analysis\analysis.py`
- Test: `D:\code\solidity\get-nft-data\tests\test_top_contract_analysis.py`
- Test: `D:\code\solidity\get-nft-data\tests\test_top_contract_analysis_accelerated.py`

- [ ] **Step 1: Rebuild the Rust wheel into the local runtime directory**

```powershell
C:\Users\z1766\.conda\envs\codex\python.exe -m top_contract_analysis.build_rust_ext --interpreter C:\Users\z1766\.conda\envs\codex\python.exe
```

- [ ] **Step 2: Run the focused accelerated test file**

Run: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests\test_top_contract_analysis_accelerated.py -v`
Expected: PASS, aside from any known unrelated pre-existing failures that also fail on `main`.

- [ ] **Step 3: Run the main top_contract_analysis test file**

Run: `C:\Users\z1766\.conda\envs\codex\python.exe -m pytest tests\test_top_contract_analysis.py -v`
Expected: PASS.

- [ ] **Step 4: Inspect output and record residual risks**

```text
Confirm victim row fields remain:
- address
- buy_tx_hashes
- buy_amount_eth
- last_buy_amount_eth
- buy_before_eth_balance
- buy_asset_ratio
- buy_asset_ratio_with_gas
- is_stuck
- last_buy_tx_hash
- ratio_status
```

- [ ] **Step 5: Commit**

```bash
git add rust_ext/top_contract_analysis_rust/src/lib.rs top_contract_analysis/rust_bridge.py top_contract_analysis/analysis.py tests/test_top_contract_analysis.py tests/test_top_contract_analysis_accelerated.py
git commit -m "feat: offload victim address analysis to rust"
```
