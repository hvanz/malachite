# Deterministic Simulation Testing (DST)

A testing framework that replaces Malachite's real network and WAL with simulated
versions, allowing consensus to be tested under controlled, reproducible conditions.

## Overview

DST intercepts all inter-node communication and WAL operations, routing them through
a `SimulationController` that operates on a logical tick-based clock. This makes it
possible to:

- Run consensus tests without real networking (no TCP/libp2p)
- Inject faults (partitions, message delays, crashes, WAL failures)
- Reproduce failures deterministically via a fixed seed
- Execute tests fast (a 3-node, 2-height test runs in ~0.35s)

## Architecture

```
┌─────────────────────────────────────────────┐
│              TestBuilder / run_test         │
│         (from malachitebft-test-framework)  │
└──────────────────┬──────────────────────────┘
                   │ spawns nodes via
                   ▼
┌─────────────────────────────────────────────┐
│           SimulatedNodeRunner               │
│  Implements NodeRunner<TestContext>         │
│  Wires each node with simulated components  │
└───┬──────────────┬──────────────────┬───────┘
    │              │                  │
    ▼              ▼                  ▼
┌───────────┐  ┌──────────────┐  ┌────────────┐
│ Simulated │  │  Simulated   │  │   Engine   │
│    WAL    │  │   Network    │  │  (real)    │
│ (in-mem)  │  │  (actor)     │  │            │
└───────────┘  └──────┬───────┘  └────────────┘
                      │
                      ▼
              ┌───────────────┐
              │  Simulation   │
              │  Controller   │
              │  (tick-based) │
              └───────────────┘
```

### Modules

| Module | Description |
|--------|-------------|
| `runner` | `SimulatedNodeRunner` — builds and spawns nodes with simulated I/O |
| `network` | `SimulatedNetwork` — ractor actor that intercepts outbound messages and routes them through the controller |
| `controller` | `SimulationController` — tick-based message scheduler with fault injection |
| `wal` | `SimulatedWal` — in-memory WAL actor backed by `BTreeMap` |
| `fault` | `FaultScenario` enum and `SimConfig` builder |

## Usage

```rust
use std::time::Duration;
use malachitebft_test::TestContext;
use informalsystems_malachitebft_test_dst::fault::SimConfig;
use informalsystems_malachitebft_test_dst::runner::SimulatedNodeRunner;
use malachitebft_test_framework::{TestBuilder, TestParams};

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_reach_consensus() {
    let mut test = TestBuilder::<TestContext, ()>::new();

    test.add_node().start().wait_until(2).success();
    test.add_node().start().wait_until(2).success();
    test.add_node().start().wait_until(2).success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::default(),
    )
    .await;
}
```

## Fault Injection

Faults are defined in `FaultScenario` and configured via `SimConfig`:

| Fault | Effect |
|-------|--------|
| `Partition` | Nodes in group A cannot communicate with nodes in group B |
| `Delay` | Adds extra delivery delay (in ticks) to a node's outbound messages |
| `MessageLoss` | Randomly drops a node's messages with a given probability |
| `CrashAt` | Crashes a node at a specific tick, with optional WAL corruption and restart |
| `WalFailure` | Makes WAL writes fail for a node during a time window |
| `Duplicate` | Duplicates a node's outbound messages with a given probability |

All faults are scoped to a time window (`start_tick` / `duration_ticks`) so they
can be composed to create complex scenarios.

```rust
use informalsystems_malachitebft_test_dst::fault::{FaultScenario, SimConfig};

let config = SimConfig::new()
    .with_seed(42)
    .with_fault(FaultScenario::Partition {
        group_a: vec![1],
        group_b: vec![2, 3],
        start_tick: 5,
        duration_ticks: 20,
    })
    .with_fault(FaultScenario::Delay {
        node: 2,
        delay_ticks: 3,
        start_tick: 0,
        duration_ticks: 100,
    });
```

Pass a `SimConfig` to inject faults into a test via `run_test` or `Test::run_with_runner_config`:

```rust
use informalsystems_malachitebft_test_dst::fault::{FaultScenario, SimConfig};

let config = SimConfig::new()
    .with_seed(42)
    .with_fault(FaultScenario::Partition {
        group_a: vec![1],
        group_b: vec![2, 3],
        start_tick: 5,
        duration_ticks: 20,
    });

malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
    test.build(),
    Duration::from_secs(60),
    TestParams::default(),
    config,
)
.await;
```

## Running Tests

```sh
# Run all DST tests (unit + integration)
cargo test -p informalsystems-malachitebft-test-dst

# Run just the integration test with logs
RUST_LOG=info cargo test --test basic -p informalsystems-malachitebft-test-dst -- --nocapture
```
