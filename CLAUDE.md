# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the Open Code Protocol (OCP) server monolith, a Go-based gRPC/web service system that powers next-generation currency launchpad and payment systems. It implements the first L2 solution on Solana using an intent-based system backed by a sequencer.

## Commands

### Testing
```bash
# Run all tests with coverage
make test

# Run tests for a specific package
go test ./ocp/rpc/transaction/...

# Run a single test
go test -run TestName ./path/to/package

# Run tests with verbose output
go test -v ./...
```

### Building
This is a library project without a main entry point. Integration is done by importing packages into applications.

## Architecture

### Core System Components

**Intent System & Sequencer** (`ocp/` package)
- The heart of the L2 solution lives in the `ocp/` package
- Intent-based transaction system: clients submit intents, sequencer processes them
- Key components:
  - `ocp/rpc/`: gRPC service implementations (entry points for client requests)
  - `ocp/worker/`: Background workers (nonce management, swap processing, sequencer, Geyser integration)
  - `ocp/transaction/`: Transaction building and local nonce pool management
  - `ocp/data/`: Data layer with Store interfaces for all domain entities

**Data Layer Architecture**
- Pattern: Interface-based stores with multiple implementations (Postgres, in-memory)
- Each domain entity (account, intent, fulfillment, action, etc.) has:
  - `ocp/data/{entity}/store.go`: Store interface definition
  - `ocp/data/{entity}/postgres/`: Postgres implementation
  - `ocp/data/{entity}/memory/`: In-memory implementation for tests
  - `ocp/data/{entity}/tests/`: Shared test suite for all implementations
- `ocp/data/provider.go`: Aggregates all stores into a single Provider interface
- Provider composes three data sources: BlockchainData, DatabaseData, WebData

**gRPC Application Framework** (`grpc/app/`)
- Standardized app lifecycle: Init -> RegisterWithGRPC -> Run -> Stop
- Apps implement the `App` interface with initialization, service registration, and shutdown
- Built-in features: New Relic integration, TLS support, health checks, pprof/expvar
- Interceptor chain: headers -> metrics -> validation -> min version checking
- Configuration via Viper (YAML files or environment variables)

**Transaction & Intent Flow**
1. Client submits intent via `ocp/rpc/transaction/server.go`
2. Intent validated through antispam/AML guards
3. Sequencer worker (`ocp/worker/sequencer/`) processes intent
4. Actions scheduled and executed (intent_handler, action_handler, fulfillment_handler)
5. Transactions built using local nonce pools to avoid Solana RPC contention
6. Fulfillments committed and monitored

**Solana Integration**
- `solana/` package: Low-level Solana primitives (accounts, transactions, programs)
- `solana/token/`: SPL Token program interface
- VM indexer client for tracking on-chain state changes
- Geyser worker (`ocp/worker/geyser/`) for real-time blockchain event streaming

**Workers**
- All workers implement `Runtime` interface (Start method with interval)
- `ocp/worker/sequencer/`: Core sequencer logic (intent, action, fulfillment handlers)
- `ocp/worker/nonce/`: Nonce allocation and pool management
- `ocp/worker/swap/`: Token swap processing
- `ocp/worker/account/`: Account state synchronization
- `ocp/worker/geyser/`: Blockchain event streaming via Geyser

### Key Patterns

**Database Transactions**
- Use `database/postgres/db.go` utilities: `ExecuteTxWithinCtx` for transactions
- Automatic retry on serialization failures via `ExecuteRetryable`
- Context-based transaction passing (no explicit tx parameters)
- Default isolation: ReadCommitted

**Testing**
- Shared test suites in `{package}/tests/tests.go` for interface conformance
- Postgres tests use `ory/dockertest` for containerized database setup
- `testutil/` provides shared test utilities (server mocking, VM utilities, wait helpers)
- In-memory implementations available for fast unit tests

**Configuration**
- `config/` package: Interface-based config with env and memory providers
- App-level config in `grpc/app/config.go` (listen addresses, TLS, logging, New Relic)
- Service-specific config in respective packages (e.g., `ocp/rpc/transaction/config.go`)

**Metrics & Observability**
- New Relic integration throughout (traces, logs, custom events)
- `metrics/` package: Constants, event tracking, tracing utilities
- Custom metrics in service packages (e.g., `ocp/rpc/transaction/metrics.go`)

**Error Handling**
- Sentinel errors defined in store interfaces (e.g., `ErrAccountInfoNotFound`)
- Use `github.com/pkg/errors` for error wrapping with context
- gRPC error codes mapped appropriately in RPC handlers

## API Contracts

Protobuf definitions are in the separate [ocp-protobuf-api](https://github.com/code-payments/ocp-protobuf-api) repository. This project imports generated Go code from that repo.

## Development Guidelines

**When working with data stores:**
- Always check existing Store interface before adding new methods
- Implement for both Postgres and memory stores
- Add to shared test suite to ensure consistency
- Use context for cancellation, not timeouts

**When adding new RPC methods:**
- Follow existing patterns in `ocp/rpc/transaction/server.go`
- Add metrics collection for the new endpoint
- Implement appropriate authentication via `ocp/auth/signature.go`
- Add antispam/AML checks if handling user funds

**When working with Solana transactions:**
- Use `ocp/transaction/` package utilities for building transactions
- Leverage `LocalNoncePool` to avoid Solana nonce contention
- Always include proper signature verification
- Test with local simulation before blockchain submission

**Worker development:**
- Implement `Runtime` interface from `ocp/worker/runtime.go`
- Use appropriate intervals based on task criticality
- Include proper context cancellation handling
- Add metrics for monitoring worker health
