# Open Code Protocol Server

[![Release](https://img.shields.io/github/v/release/code-payments/ocp-server.svg)](https://github.com/code-payments/ocp-server/releases/latest)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/code-payments/ocp-server)](https://pkg.go.dev/github.com/code-payments/ocp-server)
[![Tests](https://github.com/code-payments/ocp-server/actions/workflows/test.yml/badge.svg)](https://github.com/code-payments/ocp-server/actions/workflows/test.yml)
[![GitHub License](https://img.shields.io/badge/license-MIT-lightgrey.svg?style=flat)](https://github.com/code-payments/ocp-server/blob/main/LICENSE.md)

Open Code Protocol server monolith containing the gRPC/web services and workers that power next-generation currency launchpad and payment systems. The project contains the first L2 solution on top of Solana, utilizing an intent-based system backed by a sequencer to handle transactions.

## What is Flipcash?

[Flipcash](https://flipcash.com) is a self-custodial mobile wallet app leveraging the Open Code Protocol to provide a seamless multi-currency payments experience.

## Quick Start

1. Install Go. See the [official documentation](https://go.dev/doc/install).

2. Download the source code.

```bash
git clone git@github.com:code-payments/ocp-server.git
```

3. Run the test suite:

```bash
make test
```

## Project Structure

The implementations powering the Open Code Protocol (Intent System, Sequencer, etc) can be found int the `ocp/` package. All other packages are generic libraries and utilities.

To begin diving into core systems, we recommend starting with the following packages:
- `ocp/rpc/`: gRPC and web service implementations
- `ocp/worker/`: Backend workers that perform tasks outside of RPC and web calls

## APIs

The gRPC APIs provided by the Open Code Protocol server can be found in the [ocp-protobuf-api](https://github.com/code-payments/ocp-protobuf-api) project.

## Contributing

Anyone is welcome to make code contributions through a PR.

This will evolve as we continue to build out the platform and open up more ways to contribute.

## Getting Help

If you have any additional questions or need help integrating Flipcash into your website or application, please reach out to us on [Twitter](https://twitter.com/flipcash).
