# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the Open Code Protocol (OCP) server monolith, a Go-based gRPC/web service system that powers next-generation currency launchpad and payment systems. It implements the first L2 solution on Solana using an intent-based system backed by a sequencer. [Flipcash](https://flipcash.com) is the consumer wallet app built on this protocol.

The repo is primarily a library: applications import the packages here, wire up integrations, and run them via the `grpc/app` framework. There is no server `main`. The `cmd/` directory holds one-shot operational CLIs only (see below).

## Commands

### Testing
```bash
# Run all tests with coverage (same as CI; CI uses Go 1.27.x)
make test

# Run tests for a specific package
go test ./ocp/rpc/transaction/...

# Run a single test
go test -run TestName ./path/to/package

# Run tests with verbose output
go test -v ./...
```

- Tests use `testify` (`assert`/`require`).
- Postgres store tests spin up a `postgres:14` container via `ory/dockertest` (`database/postgres/test`), so Docker must be running for the full suite. Memory store tests need nothing.
- Some stores (currency exchange/reserve/holder, messaging) are DynamoDB-backed; their tests use `database/dynamodb/test`.

### Building
```bash
go build ./...
go vet ./...
```

### Operational CLIs (`cmd/`)
One-shot operational tools, each with usage documented in its `main.go` file header.

## Architecture

### Package map

Everything protocol-specific lives under `ocp/`. All other top-level packages are generic libraries.

**`ocp/` core**
- `ocp/rpc/`: gRPC service implementations: `transaction`, `account`, `balance`, `currency`, `messaging`. Entry points for client requests.
- `ocp/worker/`: Background workers. All implement `Runtime` (`Start(ctx, interval)`) from `ocp/worker/runtime.go`.
  - `sequencer/`: intent, action, and fulfillment handlers plus the scheduler that decides when fulfillments may hit the chain
  - `nonce/`: nonce allocation and pool management
  - `swap/`: stateful swap processing with auto-recovery
  - `account/`: account sync and gift card auto-return
  - `currency/{exchangerate,feeburner,holder,launcher,reserve}/`: exchange rate polling, fee burning, holder counts, currency launch state machine, reserve state
  - `geyser/`: real-time Solana account streaming (external deposits, currency reserves, timelock state) plus backup workers
  - `task/`: guaranteed task execution sweeper
- `ocp/transaction/`: transaction building and `LocalNoncePool`
- `ocp/data/`: data layer. `Provider` composes `BlockchainData` (Solana RPC), `DatabaseData` (stores), and `WebData` (external exchange rate APIs).
- `ocp/integration/`: pluggable app hooks: `SubmitIntent`, `Swap`, `Geyser`, `Moderation`, `Antispam`, `TaskExecutor`. Default implementations exist for most (allow-everything or no-op).
- `ocp/antispam/`, `ocp/aml/`: guards applied to intents that move user funds. Antispam delegates to the integration; AML enforces daily USD limits from the data layer.
- `ocp/balance/`: cached balance ledger (see below)
- `ocp/history/`: builds per-owner transaction history records from intents, deposits, swaps, and gift cards
- `ocp/task/`: task scheduler (see below)
- `ocp/currency/`: currency utilities: `MintDataProvider` (cached, pre-signed live mint data), exchange rate and USD market value helpers, fees, limits, market cap
- `ocp/config/`: hardcoded core mint constants (USDF mint, subsidizer, VM, fees account, ALT, asset URLs). Marked todo to move to env.
- `ocp/common/`: `Account` abstraction, core mint helpers, subsidizer, VM helpers
- `ocp/auth/`: RPC signature verification
- `ocp/vm/`: virtual account utilities for the Code VM

**Generic packages**
- `grpc/app/`: app lifecycle framework (below)
- `grpc/{client,headers,metrics,protobuf}/`: interceptors, header propagation, client metadata (device, IP, user agent, version)
- `database/postgres/`: tx helpers; `database/query/`: cursor/limit/ordering options; `database/dynamodb/`: test helpers
- `solana/`: low-level primitives and program bindings: `token`, `system`, `timelock/v1`, `vm`, `currencycreator` (launchpad bonding curve), `coinbasestableswapper`, `usdfswap`, `addresslookuptable`, `computebudget`, `memo`
- `coinbase/`: Coinbase Developer Platform Onramp API client
- `currency/`: exchange rate client interface with `coingecko`, `fixer`, `exchangerateapi` implementations
- `usdc/`, `usdf/`: mint constants. USDF is the core mint (6 decimals, 1e6 quarks per unit). USDC is the external on/off-ramp currency.
- `cache/`, `retry/`, `sync/`, `metrics/`, `config/`, `pointer/`, `protoutil/`, `netutil/`, `osutil/`, `testutil/`

### Data layer

- Each entity has `ocp/data/{entity}/store.go` (interface + sentinel errors), `memory/`, a `postgres/` or `dynamodb/` implementation, and `tests/tests.go` (shared conformance suite run by every implementation).
- `ocp/data/internal.go` defines `DatabaseData` and `DatabaseProvider`. Most stores are exposed through the provider. **Currency exchange, reserve, and holder stores, and the messaging store, are not part of the provider.** They are constructed separately and passed explicitly to the workers and RPC servers that need them.
- **Table DDL and migrations are external to this repository.** Each postgres store test embeds a `tableCreate` string used only for tests. When changing a schema, update the test DDL here and coordinate the real migration separately.
- Postgres tables are named `ocp__core_{entity}`.
- `NewTestDataProvider()` wires all-memory stores plus a testnet blockchain provider.

### Database transactions

- `database/postgres/db.go`: `ExecuteTxWithinCtx` opens a tx and passes it through the context; store implementations call `ExecuteInTx`, which reuses the context tx if present. No explicit tx parameters.
- `Provider.ExecuteInTx` is the entry point for multi-store transactions. Store support for context transactions was added late and is not universal. Verify every store method called inside the closure supports it.
- The memory provider runs the closure directly with no transaction semantics.
- Default isolation is ReadCommitted. `ExecuteRetryable` retries on serialization failure.

### Intent flow (SubmitIntent)

1. Client submits an intent via `ocp/rpc/transaction/server.go`. Per-type logic lives in `CreateIntentHandler` implementations in `intent_handler.go`.
2. Validation: signature auth, antispam and AML guards, the `SubmitIntent` integration's `AllowCreation`, and local simulation of client-signed transactions.
3. Inside a single DB transaction: intent, actions, and fulfillments are saved, transaction history records are built and saved, balance ledger deltas are applied, and app tasks from `GetTasksToSchedule` are enqueued. Any failure rolls back everything.
4. After commit: best-effort fast-path task execution and the `OnSuccess` integration callback.
5. The sequencer worker processes fulfillments by state, using the contextual scheduler to decide when a fulfillment can be submitted. Transactions are built from local nonce pools.

Race resistance comes from the balance ledger's predicates inside the intent transaction, not from locking. `GetAccountsToLock` only takes in-process locks for expected mass races such as a gift card claimed from many devices.

### Balance ledger (`ocp/balance`, `ocp/data/balance`)

- One row per timelock token account holding quarks, USD cost basis, open/closed, locked/unlocked, and backfilled flags. This is the source of truth for balances of accounts OCP manages.
- Deltas are applied in the same DB transaction as the records that produce them (`ApplyDeltasInTx`), and records are created when the account is opened (`CreateRecordInTx`).
- Constraints are predicates on the UPDATE, not a version column: debits require sufficient balance, drains and closes require an exact match. Errors: `ErrInsufficientBalance`, `ErrBalanceChanged`, `ErrAccountClosed`, `ErrAccountUnlocked`.
- Once a vault unlocks, funds can move on chain without an intent, so the record stops being maintained. Unlocked records are excluded from reads and reject non-credit deltas.
- `CalculateFromCache` for managed accounts, returning `ErrNotManagedByCode` when no locked record exists. `CalculateFromBlockchain` for external accounts.
- USD values are stored as int64 micro-USD (6 decimals, matching USDF). Never do float USD arithmetic in the ledger.
- Delta rules (`delta.go`) only cover supported intent shapes and refuse anything else with `ErrUnsupportedBalanceChange`. A new money-moving intent type needs a delta rule before it can be committed.

### Task system (`ocp/task`, `ocp/worker/task`, `ocp/data/task`)

- Durable app-defined work with at-least-once execution.
- Apps return tasks from the `GetTasksToSchedule` hook. The scheduler enqueues them in the intent's DB transaction, so scheduling is atomic with the intent.
- Best-effort fast path after commit; the worker sweeps pending tasks whose `NextAttemptAt` has elapsed with exponential backoff, dead-lettering to `StateFailed` after a max attempt count (default 10).
- **`TaskExecutor` implementations must be idempotent.** Tasks may run concurrently and more than once. The task ID is the dedup key. The default executor fails every task so orphaned tasks surface loudly.
- `Type`/`Data` are opaque to the base system.

### Swap and deposit subsystem

- External USDC deposits are detected by the Geyser worker (`ocp/worker/geyser/external_deposit.go`), tracked in `ocp/data/deposit`, and swapped into USDF.
- Stateful swaps are processed by `ocp/worker/swap/`; stateless swaps are handled directly in `ocp/rpc/transaction/stateless_swap.go`.
- Launchpad currencies use a bonding curve (`solana/currencycreator`). New-currency first buys can be funded from the core mint or from another currency via a server-held treasury account (`SWAP_TREASURY_OWNER_PUBLIC_KEY`, loaded from the vault store at startup).
- Currency launch is a state machine on `currency.MetadataState` driven by the launcher worker. The enum is append-only.

### Geyser worker

- Handlers receive account updates that are not guaranteed to be in order. **Handlers must be idempotent and must not trust the passed account data.** Always re-read finalized state from a Solana RPC.
- Backup workers in `backup.go` assume Geyser may deliver nothing. Geyser exists for real-time latency only; correctness must not depend on it.

### Worker pattern

- Per-state goroutines: a worker typically spawns one loop per record state, each paging with `query.WithLimit`/`query.WithCursor` and processing a batch in parallel.
- Loops use `retry.Loop` with `retry.NonRetriableErrors(context.Canceled)`.
- Tracing: pull the metrics provider from `ctx.Value(metrics.ProviderContextKey)`, start a trace, and wrap the context with `metrics.NewContext`.
- Each worker package has `config.go` and `metrics.go`.

### gRPC application framework (`grpc/app/`)

- Lifecycle: `Init` -> `RegisterWithGRPC` -> serve -> `Drain` -> `GracefulStop` -> `Stop`.
- `Drain` runs after health flips to NOT_SERVING and before `GracefulStop`. Use it to end long-lived streams and retract cross-instance registrations; `GracefulStop` blocks on open streams otherwise. Cleanup that must outlive in-flight RPCs belongs in `Stop`.
- Built-ins: New Relic, TLS, health checks, keepalive, panic recovery, pprof/expvar, cron.
- Interceptor chain: panic recovery -> headers -> metrics -> validation -> min client version (per user agent name).
- Config via Viper (YAML or env).

### Messaging service

- Streams are keyed by rendezvous key. Cross-instance routing uses the `rendezvous` store (short-lived records refreshed while a stream is open) so any instance can forward a message to the instance holding the stream.

## Configuration

- Package-level pattern: `config.go` declares `conf` with typed `config.*` values, a `ConfigProvider func() *conf`, and `WithEnvConfigs()`. Env vars use a per-service prefix (for example `TRANSACTION_SERVICE_`).
- Tests use `withManualTestOverrides` style providers backed by `config/memory`.
- **Money amounts in config are `uint64` quarks, never USD floats.** Derive USD only via a single division by `CoreMintQuarksPerUnit` so server and client float values match exactly.
- `ocp/data/config.go` holds external API keys (`FIXER_API_KEY`, `EXCHANGE_RATE_API_KEY`).

## Code conventions

- Optional struct fields are pointers (`*string`), not empty-value sentinels.
- Do not add doc comments to record fields unless they carry non-obvious meaning. Keep records clean.
- Prefer deriving a value from an existing mechanism over storing a new column when a worker already enforces it.
- Errors: sentinel errors in store interfaces; `github.com/pkg/errors` for wrapping; map to gRPC codes in RPC handlers. Internal failures return `codes.Internal` with an empty message and a logged warning.
- Logging: `zap`, with `client.InjectLoggingMetadata` in RPC handlers.
- Enums persisted to the DB (states, types) are append-only.

## Development guidelines

**Data stores**
- Check the existing Store interface before adding methods.
- Implement for memory and for postgres (or dynamodb, matching the entity's existing backend).
- Add the case to the shared `tests/tests.go` suite. Update the test DDL in the postgres `store_test.go` if the schema changes.
- Add the method to `DatabaseData` and `DatabaseProvider` in `ocp/data/internal.go` if the store is provider-exposed.
- Use context for cancellation, not timeouts.

**RPC methods**
- Follow `ocp/rpc/transaction/server.go` patterns.
- Add metrics for the endpoint.
- Authenticate via `ocp/auth`.
- Add antispam/AML checks if handling user funds.

**Solana transactions**
- Build with `ocp/transaction/` utilities and a `LocalNoncePool`.
- Verify signatures. Simulate locally before submission.
- Compute unit limits matter for high-frequency transactions; check existing budgets before adding instructions.

**Workers**
- Implement `Runtime`, follow the per-state loop pattern above, handle context cancellation, and add metrics.

## API contracts

Protobuf definitions live in [ocp-protobuf-api](https://github.com/code-payments/ocp-protobuf-api); this repo imports the generated Go code. The Geyser client protos are vendored under `ocp/worker/geyser/api` and regenerated with its Makefile.
