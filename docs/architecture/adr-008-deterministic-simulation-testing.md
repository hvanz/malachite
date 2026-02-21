# ADR 008: Deterministic Simulation Testing

## Changelog

* 2026-02-21: Initial version

## Context

Malachite currently has multiple layers of testing infrastructure, each
targeting different aspects of correctness:

- **Model-Based Testing (MBT)**, using the [malachitebft-test-mbt][mbt-crate]
  crate, validates the pure consensus state machine against formal ITF traces.
  These tests are fully deterministic but only exercise the core state machine
  in isolation, without the engine, network, or WAL layers.

- **Integration tests**, using the [malachitebft-test-framework][framework-crate]
  crate, run multiple nodes with real libp2p networking and disk-backed WAL.
  These tests exercise the full stack but are non-deterministic: real network
  timing, Tokio's multi-threaded task scheduler, and OS-level I/O introduce
  variance across runs.
  A failing test may not reproduce on the next attempt.

- **Testnet orchestration (Quake)**, using the [quake][quake-crate] tool,
  deploys full Docker-based testnets with configurable perturbations (kill,
  pause, disconnect).
  These tests are the closest to production conditions but are the slowest and
  least reproducible.

There is a gap between model-based testing and integration testing.
MBT tests prove properties of the consensus algorithm itself,
but they cannot catch bugs in the integration between consensus, the engine's
actor wiring, the WAL replay logic, or the value synchronization protocol.
Integration tests can catch those bugs, but when they do, the failures are
often difficult to reproduce because the conditions that triggered them
(message ordering, timing, crash points) are not recorded or controllable.

**Deterministic Simulation Testing** (DST), a technique pioneered by
[FoundationDB][fdb-testing] and adopted by systems like
[TigerBeetle][tigerbeetle], addresses this gap.
In DST, all sources of non-determinism -- network, disk I/O, time, randomness
-- are replaced with simulated, controllable substitutes.
The entire system runs under a controlled scheduler, making every execution
reproducible from a single seed.
This enables:

1. **Reproducibility**: any failure can be deterministically reproduced by
   re-running with the same seed;
2. **Adversarial exploration**: the simulation can inject faults (partitions,
   message loss, crashes, WAL corruption) systematically;
3. **Speed**: simulated time means tests run as fast as the CPU allows, with no
   real wall-clock delays;
4. **Coverage**: running the same test with thousands of different seeds
   explores a much larger space of execution schedules than a single
   integration test run.

Malachite's architecture is well-suited for DST.
The consensus core is a pure, deterministic state machine that communicates
with the host through an explicit [effect system](./adr-004-coroutine-effect-system.md).
The engine layer uses an [actor model](./adr-002-node-actor.md) with typed
message passing.
Most critically, the `EngineBuilder` already supports injecting custom
implementations of the Network and WAL actors via `with_custom_network()` and
`with_custom_wal()` -- a pattern already proven in production by the
[ByzantineNetworkProxy][byz-proxy] in `engine-byzantine`.

## Decision

Introduce a new crate, `malachitebft-test-dst`, that provides deterministic
simulation testing for Malachite.
The crate sits on top of the existing engine and test infrastructure without
modifying any existing code.

### Simulation boundary

The simulation replaces the **infrastructure layer** (network and WAL) while
keeping the **application layer** (engine actors, consensus core, test app)
running as real code:

```
┌──────────────────────────────────────────────────────┐
│                   REAL CODE (under test)              │
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │  Consensus   │  │   Sync       │  │  Host /    │  │
│  │  Actor       │  │   Actor      │  │  Connector │  │
│  │  (engine)    │  │   (engine)   │  │  (app-ch)  │  │
│  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘  │
│         │                 │                │         │
│  ┌──────┴───────┐         │          ┌─────┴──────┐  │
│  │  Consensus   │         │          │  Test App  │  │
│  │  Core (SM)   │         │          │            │  │
│  └──────────────┘         │          └────────────┘  │
│                           │                          │
├───────────────────────────┼──────────────────────────┤
│               SIMULATED   │  (test/dst crate)        │
│                           │                          │
│  ┌────────────────┐       │     ┌─────────────────┐  │
│  │ SimulatedNetwork│◄──────┘     │ SimulatedWal    │  │
│  │ (per node)     │             │ (per node)      │  │
│  └───────┬────────┘             └───────┬─────────┘  │
│          │                              │            │
│          ▼                              ▼            │
│  ┌─────────────────────────────────────────────────┐ │
│  │           SimulationController                  │ │
│  │  - message queue & delivery scheduling          │ │
│  │  - fault injection (partitions, delays, drops)  │ │
│  │  - time control (hybrid tick/event-driven)      │ │
│  │  - seed-based deterministic RNG                 │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

The Sync actor runs as real code.
A Byzantine variant of the Sync actor can be added later following the same
proxy pattern used by `ByzantineNetworkProxy`.

### Components

#### SimulatedNetwork

A `ractor` actor implementing `Actor<Msg = NetworkMsg<Ctx>>`, the same message
type as the real [Network actor][network-actor].
It is injected into the engine via
`EngineBuilder::with_custom_network(network_ref, tx_network)`.

When the local Consensus or Sync actor sends a message (e.g.,
`PublishConsensusMsg`, `BroadcastStatus`), the `SimulatedNetwork` does not
transmit it over the wire.
Instead, it enqueues the message in the `SimulationController`, which decides
when and to whom the message is delivered based on the current fault schedule.

When the controller delivers a message to this node, the `SimulatedNetwork`
converts it into the appropriate `NetworkEvent` variant and forwards it to its
local subscribers (the Consensus and Sync actors), exactly as the real network
would.

#### SimulatedWal

A `ractor` actor implementing `Actor<Msg = WalMsg<Ctx>>`, the same message
type as the real [WAL actor][wal-actor].
It is injected via `EngineBuilder::with_custom_wal(wal_ref)`.

All entries are stored in memory in a `HashMap<Height, Vec<(WalEntry, bool)>>`,
where the boolean tracks whether an entry has been flushed.
This enables crash simulation: when the controller triggers a crash,
unflushed entries are discarded, simulating data loss.
The controller can also inject write failures by setting a flag that causes
`Append` and `Flush` operations to return errors.

#### SimulationController

The central coordinator that manages message routing, fault injection, and
simulated time.
All simulated actors hold an `Arc<Mutex<SimulationController>>` reference.

**Message routing:**
When a node's `SimulatedNetwork` enqueues a message, the controller assigns it
a delivery tick based on the current time plus any delay faults.
On each tick, the controller drains messages scheduled for that tick, checks
them against active fault scenarios (partitions, message loss), and delivers
surviving messages to destination nodes.

**Time control (hybrid model):**
The controller drives time via `tokio::time::advance()`.
During active periods (messages pending), it advances one tick at a time for
fine-grained interleaving.
During idle periods, it jumps directly to the next scheduled event (message
delivery or timeout expiry).
This hybrid model avoids wasting cycles on empty ticks while maintaining
precise control during critical periods.

**Fault injection:**
The controller holds a list of `FaultScenario` values that describe faults
active during specific tick windows:

- **Partition**: nodes in group A cannot communicate with nodes in group B
- **Delay**: messages to/from a node are delayed by N ticks
- **MessageLoss**: messages are dropped with a given probability
- **CrashAt**: kill a node at a specific tick, optionally corrupt WAL
- **WalFailure**: WAL writes fail for a duration
- **Duplicate**: messages are duplicated with a given probability

All probabilistic decisions use a seeded `StdRng`, ensuring reproducibility.

#### SimulatedNodeRunner

Implements the existing `NodeRunner<TestContext>` trait from
[malachitebft-test-framework][framework-crate].
This is the integration point with the existing test infrastructure: existing
tests can switch from real infrastructure to simulation by changing the runner
type parameter.

In `new()`, the runner pauses Tokio time (`tokio::time::pause()`) and creates
the `SimulationController` with a seed derived from the test ID.
In `spawn()`, it creates `SimulatedNetwork` and `SimulatedWal` actors for each
node, then builds the engine via `EngineBuilder` with the simulated actors
injected via `with_custom_network` and `with_custom_wal`.
The Sync and Consensus actors are real, instantiated with `with_default_sync`
and `with_default_consensus`.

### Tokio runtime

All DST tests run on Tokio's `current_thread` runtime with paused time:

```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
```

This provides:
- **Single-threaded execution**: tasks run on one thread, making scheduling
  order highly reproducible (though not formally guaranteed by Tokio's API)
- **Controlled time**: `tokio::time::pause()` freezes the clock;
  `tokio::time::advance()` advances it in discrete steps, causing all
  `TimerScheduler` timeouts in the Consensus actor to fire deterministically
- **Speed**: no real wall-clock delays; tests run as fast as the CPU allows

### Integration with existing test framework

The test framework's `Test::run::<R>()` method is parameterized by the runner
type `R`.
Existing integration tests use `TestRunner` (real network, real WAL):

```rust
test.build().run::<TestRunner>(Duration::from_secs(60)).await;
```

The same tests can run in simulation by switching the runner:

```rust
test.build().run::<SimulatedNodeRunner>(Duration::from_secs(60)).await;
```

For tests that require fault injection, a `SimConfig` is passed via a
thread-local before running:

```rust
set_sim_config(SimConfig::new()
    .with_seed(42)
    .with_fault(FaultScenario::Partition {
        group_a: vec![1.into()],
        group_b: vec![2.into(), 3.into()],
        start_tick: 100,
        duration_ticks: 200,
    }));

test.build().run::<SimulatedNodeRunner>(Duration::from_secs(60)).await;
```

### Determinism guarantees

**Deterministic given the same seed:**
- Message delivery order and timing
- Fault injection timing and targets
- RNG-driven decisions (drops, reordering, duplications)
- Tokio timer expirations

**Near-deterministic but not formally guaranteed:**
- Tokio task scheduling order within a single time tick (in practice,
  `current_thread` runtime produces highly reproducible ordering, but this is
  not part of Tokio's API contract)

**Mitigation:**
When a test fails, the seed is logged.
Re-running with the same seed reproduces the failure in the vast majority of
cases.
For the rare scheduling-dependent edge case, running the same seed multiple
times will surface it.

### Crate structure

```
crates/test/dst/
├── Cargo.toml
└── src/
    ├── lib.rs              # Public API: SimConfig, SimulatedNodeRunner
    ├── controller.rs       # SimulationController
    ├── network.rs          # SimulatedNetwork actor
    ├── wal.rs              # SimulatedWal actor
    ├── fault.rs            # FaultScenario types
    ├── runner.rs           # SimulatedNodeRunner
    └── invariants.rs       # Safety/liveness invariant checkers
```

The crate depends on `engine`, `app-channel`, `test`, `test-app`,
`test-framework`, `ractor`, `tokio`, and `rand`.
It does **not** depend on `network` (libp2p).

### Phased rollout

**Phase 1: Basic simulation.**
`SimulatedNetwork` delivers all messages without faults.
`SimulatedWal` stores everything in memory.
Existing integration tests are verified to pass with `SimulatedNodeRunner`.

**Phase 2: Fault injection.**
Network partitions, delays, message loss.
Node crashes with WAL replay.
WAL corruption scenarios.

**Phase 3: Seed-based exploration.**
Random fault generation from a seed.
`FaultProfile` presets (Mild, Moderate, Severe, Byzantine).
Integration with `engine-byzantine` for Byzantine nodes.

**Phase 4: Invariant checking.**
Safety: no two honest nodes decide different values at the same height.
Liveness: if fewer than 1/3 are faulty and partitions heal, nodes eventually
decide.
WAL correctness: after crash and replay, node resumes correctly.

## Status

Proposed

## Consequences

### Positive

* Fills the testing gap between MBT (pure consensus) and integration tests
  (real infrastructure), enabling reproducible multi-node testing
* No modifications to existing crates: the simulation layer is purely additive,
  using injection points (`with_custom_network`, `with_custom_wal`) already
  present in `EngineBuilder`
* Existing integration tests can run in simulated mode by changing one type
  parameter, providing immediate value without writing new tests
* Seed-based exploration enables testing thousands of execution schedules that
  would be impractical with real networking
* Failures are reproducible: a seed fully determines the execution, making
  debugging tractable
* Simulated time means tests run orders of magnitude faster than integration
  tests with real network delays

### Negative

* Task scheduling within a Tokio tick is not formally deterministic, so rare
  edge cases may not reproduce from a seed alone
* The `SimulationController`'s `Arc<Mutex<_>>` introduces contention between
  the simulated actors and the tick loop, though this is mitigated by the
  single-threaded runtime
* The thread-local mechanism for passing `SimConfig` to `SimulatedNodeRunner`
  is not elegant; a more structured approach may be needed if the API grows
* Maintaining the simulated actors requires keeping them in sync with changes
  to `NetworkMsg` and `WalMsg` enums in the engine crate

### Neutral

* The DST crate adds a new workspace member but does not affect the build of
  existing crates
* DST tests complement but do not replace existing integration tests; both
  should continue to run in CI
* The simulation does not cover the libp2p networking layer itself (gossipsub,
  peer discovery, transport protocols); those remain tested by integration
  tests and Quake

## References

* [FoundationDB: Testing Distributed Systems w/ Deterministic Simulation][fdb-testing]
* [TigerBeetle: Simulation Testing][tigerbeetle]
* [Jepsen: Distributed Systems Safety Research](https://jepsen.io/)
* [ADR 001: High Level Architecture](./adr-001-architecture.md)
* [ADR 002: Node Architecture using the Actor Model](./adr-002-node-actor.md)
* [ADR 004: Coroutine-Based Effect System for Consensus](./adr-004-coroutine-effect-system.md)
* [ADR 007: Consensus Write-Ahead Log (WAL)](./adr-007-write-ahead-log.md)
* [Design document: Deterministic Simulation Testing](../plans/2026-02-21-deterministic-simulation-testing-design.md)

[mbt-crate]: https://github.com/circlefin/malachite/tree/main/code/crates/test/mbt
[framework-crate]: https://github.com/circlefin/malachite/tree/main/code/crates/test/framework
[quake-crate]: https://github.com/circlefin/malachite/tree/main/code/crates/quake
[network-actor]: https://github.com/circlefin/malachite/tree/main/code/crates/engine/src/network.rs
[wal-actor]: https://github.com/circlefin/malachite/tree/main/code/crates/engine/src/wal.rs
[byz-proxy]: https://github.com/circlefin/malachite/tree/main/code/crates/engine-byzantine/src/proxy.rs
[fdb-testing]: https://www.youtube.com/watch?v=4fFDFbi3toc
[tigerbeetle]: https://tigerbeetle.com/blog/2023-07-11-a-]database-without-dynamic-memory-allocation
