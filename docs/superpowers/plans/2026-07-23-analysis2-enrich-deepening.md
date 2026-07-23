# analysis2 Enrich Evidence Deepening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deepen `analysis2` enrichment so EVM gas/receipts + value-flow edges and Solana full transaction decode are populated; Task 12 economics can compute Setup/Lure/Exit and funding/withdrawal honestly.

**Architecture:** Extend existing `enrich/` (Alchemy + Helius) and `EvidenceBundle`; wire orchestrator; extend `economics`/`lifecycle` consumers. No DuckDB, no spill, OpenSea still minimized.

**Tech Stack:** Existing analysis2_core (reqwest, tokio, httpmock, serde_json).

**Branch:** `analysis2-experimental`

## Global Constraints

- Spec base: `docs/superpowers/specs/2026-07-23-analysis2-experimental-design.md` + phase-E design (gas/value-flow/Solana decode)
- Standalone `analysis2/`; no deps on `analysis/`, `dedup/`, `top_contract_analysis_rs` (reference only)
- Missing keys → `not_requested`; Empty/Failed/Truncated/Complete remain distinct
- Bounded concurrency + finite retries
- Do not commit unless the user explicitly asks — **exception for SDD:** commit per task as before on this branch
- Model: `cursor-grok-4.5-high-fast`

---

### Task E1: ValueFlowEdge types + bundle fields

**Files:**
- Modify: `analysis2/crates/core/src/enrich/types.rs`
- Modify: `analysis2/crates/core/src/analysis/lifecycle.rs` / `economics.rs` as needed for consuming edges
- Test: unit tests in types or economics

**Produces:**

```rust
pub enum ValueFlowKind { Funding, Withdrawal, Cashout, RevenueBackflow }
pub struct ValueFlowEdge {
    pub tx_hash: String,
    pub from: String,
    pub to: String,
    pub kind: ValueFlowKind,
    pub native_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    pub timestamp: Option<i64>,
}
// EvidenceBundle.value_flows: Vec<ValueFlowEdge>
// TransferEvent.fee_payer: Option<String> (optional but preferred)
```

Economics: when `quality.value_flows` Complete/Truncated, aggregate funding/withdrawal into notes or EconomicFacts fields if already present; at minimum Exit gas from cashout/withdrawal txs when `gas_native` known; mark quality honestly.

- [ ] Add types + serde
- [ ] Extend economics to consume edges (Exit stage + funding/withdrawal aggregates or notes)
- [ ] Unit tests with synthetic edges
- [ ] Commit

---

### Task E2: EVM Alchemy receipts → gas

**Files:**
- Modify: `analysis2/crates/core/src/enrich/alchemy.rs`
- Modify: `analysis2/crates/core/src/enrich/orchestrator.rs`
- Test: httpmock in orchestrator/alchemy tests

**Behavior:**
- After transfers/sales collected, unique `tx_hash` set
- Fetch receipts (Alchemy/ETH RPC `eth_getTransactionReceipt`); parse `gasUsed * effectiveGasPrice` → ETH
- Fill matching `TransferEvent.gas_native` (+ fee_payer if available)
- `quality.gas`: Complete if all requested ok; Truncated if partial; Empty if no txs; Failed if all fail; NotRequested if no alchemy key

- [ ] Implement receipt fetch + attach
- [ ] httpmock: gas Complete path
- [ ] Commit

---

### Task E3: EVM value-flow edges from native transfers

**Files:**
- Modify: `analysis2/crates/core/src/enrich/alchemy.rs` and/or new `value_flow.rs`
- Modify: orchestrator

**Behavior:**
- For txs involving candidate controllers/operators (from `controllers` + mint `from`/`to` heuristics), fetch or use alchemy `alchemy_getAssetTransfers` native transfers (EXTERNAL) related to those addresses in candidate activity window if cheap; else derive from receipt/tx value fields when available
- Prefer minimal viable path: for each unique NFT tx, also request native asset transfers in same block/tx via Alchemy where possible; classify Funding (into operator), Withdrawal/Cashout (out of operator), RevenueBackflow
- Set `quality.value_flows` honestly

- [ ] Implement edge extraction
- [ ] httpmock / unit parse tests
- [ ] Commit

---

### Task E4: Solana getTransaction decode

**Files:**
- Modify: `analysis2/crates/core/src/enrich/helius.rs`
- Modify: orchestrator Solana path
- Reference (rewrite): `top_contract_analysis_rs/src/api/helius/transaction.rs`

**Behavior:**
- Keep signature discovery
- Dedupe signatures → `getTransaction` jsonParsed
- Fill TransferEvent/SaleEvent with from/to/timestamp/fee; extract ValueFlowEdge for SOL movements involving authority/fee payer
- Non-empty stubs without decode → Truncated; successful decode with required fields → can upgrade transfers/sales/histories toward Complete; partial → Truncated

- [ ] Implement decode + attach
- [ ] httpmock: Truncated stubs vs Complete decode
- [ ] Commit

---

### Task E5: Wire + README + regression through analyze_candidate

**Files:**
- Modify: orchestrator integration; README notes on evidence depth
- Test: end-to-end fixture enrich override OR httpmock showing economics Setup/Lure non-zero when gas Complete

- [ ] Ensure `analyze_candidate` economics picks up gas Complete + Exit when withdrawal edges exist
- [ ] `cargo test --manifest-path analysis2/Cargo.toml` PASS
- [ ] README short note on gas/value-flow/Solana decode
- [ ] Commit

---

## Spec coverage

| Item | Task |
|---|---|
| ValueFlowEdge + bundle | E1 |
| EVM gas receipts | E2 |
| EVM value flows | E3 |
| Solana full tx decode | E4 |
| Economics consumption + docs | E5 |
