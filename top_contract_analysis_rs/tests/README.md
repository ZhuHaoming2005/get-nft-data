# Integration test layout

Each root `*.rs` file is a Cargo integration-test target. Its implementation lives in the matching `*_cases/` directory so a domain can be split into focused modules without creating additional linked test binaries.

| Target | Tier | Responsibility |
| --- | --- | --- |
| `algorithms` | default | Public deterministic algorithms |
| `config` | default | Configuration contract smoke test |
| `analyze` | `api-tests` | End-to-end analysis orchestration |
| `api` | `api-tests` | Provider clients, pagination, retry, and security |
| `helius` | `api-tests` | Solana/Helius response handling |
| `multichain` | `api-tests` | Cross-chain identity and orchestration |
| `multichain_batch` | `api-tests` | Batch scheduling and concurrency |
| `cli_smoke` | `cli-tests` | CLI parsing and subprocess behavior |
| `paper_stats` | `db-tests` | Research-statistics projections |
| `store` | `db-tests` | DuckDB, Parquet, and snapshot persistence |

Reusable helpers should stay inside the narrowest matching `*_cases/` directory. Do not add a global test prelude that couples unrelated domains.
