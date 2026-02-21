# Deterministic Simulation Testing (DST) for Malachite

**Date:** 2026-02-21
**Status:** Draft

## Summary

Add deterministic simulation testing to Malachite by creating a new `test/dst` crate that provides simulated Network and WAL actors, a central SimulationController, and a `SimulatedNodeRunner` that integrates with the existing test framework. No changes to the engine, consensus core, or application code.

## Goals

- Reproduce any multi-node consensus failure from a single `u64` seed
- Catch consensus protocol bugs (message reordering, partitions, timing-dependent liveness/safety failures)
- Catch crash recovery bugs (WAL replay correctness, state corruption after arbitrary crashes)
- Support Byzantine fault injection (via existing `engine-byzantine` crate)
- Integrate with the existing `TestBuilder`/`TestNode`/`Step` API so existing tests can run in simulated mode

## Non-goals

- Replacing Tokio's task scheduler with a fully deterministic one (we accept near-determinism via `current_thread` + `time::pause()`)
- Replacing ractor with a custom actor framework
- Modifying any existing crate's public API

## Architecture

### Simulation boundary

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
│  │  Core        │         │          │            │  │
│  │  (pure SM)   │         │          │            │  │
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

The engine's `EngineBuilder` already supports injecting custom actors:
- `with_custom_network(NetworkRef<Ctx>, Sender<NetworkMsg<Ctx>>)` — replaces libp2p
- `with_custom_wal(WalRef<Ctx>)` — replaces redb-backed WAL
- `with_custom_sync(SyncRef<Ctx>)` or `with_default_sync(...)` — keep real Sync

This pattern is already proven by `ByzantineNetworkProxy` in `engine-byzantine`.

### Crate structure

```
crates/test/dst/
├── Cargo.toml
└── src/
    ├── lib.rs              # Public API: SimConfig, run_simulated()
    ├── controller.rs       # SimulationController — central scheduler
    ├── network.rs          # SimulatedNetwork ractor actor
    ├── wal.rs              # SimulatedWal ractor actor
    ├── fault.rs            # FaultScenario types
    └── runner.rs           # SimulatedNodeRunner (impl NodeRunner<TestContext>)
```

**Dependencies:** `engine`, `app-channel`, `test`, `test/app`, `test/framework`, `engine-byzantine`, `ractor`, `tokio`, `rand`.

Does **not** depend on `network` (libp2p).

## Components

### SimulationController

Central coordinator that manages message routing, fault injection, and time.

```rust
pub struct SimulationController<Ctx: Context> {
    seed: u64,
    rng: StdRng,

    // Node registry — maps NodeId to its simulated network actor
    nodes: HashMap<NodeId, SimNodeHandle<Ctx>>,

    // Message queue ordered by scheduled delivery tick
    message_queue: BinaryHeap<ScheduledMessage<Ctx>>,

    // Active fault scenarios
    faults: Vec<FaultScenario>,

    // Clock
    current_tick: u64,
    tick_duration: Duration,

    // History for assertions and debugging
    delivered: Vec<DeliveredMessage<Ctx>>,
    dropped: Vec<DroppedMessage<Ctx>>,
}
```

**Hybrid time advancement:**
1. If messages are pending for the current or next tick, advance one tick at a time (fine-grained interleaving).
2. If no messages are pending, jump to the next scheduled event (message delivery or timeout expiry).
3. Time is advanced via `tokio::time::advance(duration)`, which triggers any pending `TimerScheduler` timeouts in the real Consensus actor.

**Message routing flow:**
1. Node A's `SimulatedNetwork` receives a `PublishConsensusMsg` from its local Consensus actor.
2. It enqueues the message in the controller with metadata (source node, message type).
3. The controller checks active faults to decide: deliver, delay, drop, or duplicate.
4. On delivery tick, the controller calls the destination node's `SimulatedNetwork` to emit the corresponding `NetworkEvent` to its subscribers.

### SimulatedNetwork

A ractor actor with `type Msg = NetworkMsg<Ctx>` — same as the real Network actor.

```rust
pub struct SimulatedNetwork<Ctx: Context> {
    node_id: NodeId,
    peer_id: PeerId,
    controller: Arc<Mutex<SimulationController<Ctx>>>,
}

pub struct SimulatedNetworkState<Ctx: Context> {
    subscribers: Vec<Box<dyn Subscriber<NetworkEvent<Ctx>>>>,
}
```

**Outbound messages** (from Consensus/Sync to network):
- `PublishConsensusMsg(msg)` → enqueue in controller for delivery to all other nodes
- `PublishLivenessMsg(msg)` → enqueue in controller
- `PublishProposalPart(msg)` → enqueue in controller
- `BroadcastStatus(status)` → enqueue in controller
- `OutgoingRequest(peer, req, reply)` → route to specific peer via controller
- `OutgoingResponse(req_id, resp)` → route response back to requester via controller

**Inbound messages** (from controller to local subscribers):
- `NewEvent(event)` → the controller calls this to deliver a network event
- `Subscribe(subscriber)` → register local subscriber

**Transparent pass-through:**
- `DumpState(reply)` → return simulated state
- `UpdateValidatorSet(vs)` → store locally
- `UpdatePersistentPeers(op, reply)` → no-op or store locally

### SimulatedWal

An in-memory ractor actor with `type Msg = WalMsg<Ctx>`.

```rust
pub struct SimulatedWal<Ctx: Context> {
    node_id: NodeId,
    controller: Arc<Mutex<SimulationController<Ctx>>>,
}

pub struct SimulatedWalState<Ctx: Context> {
    // Entries per height, each with a "flushed" flag
    entries: HashMap<Ctx::Height, Vec<(WalEntry<Ctx>, bool)>>,
}
```

**Message handling:**
- `StartedHeight(height, reply)` → return entries for height (only flushed entries if simulating crash recovery)
- `Append(height, entry, reply)` → store entry as unflushed; may fail if WAL fault is active
- `Flush(reply)` → mark all entries as flushed; may fail if WAL fault is active
- `Reset(height, reply)` → clear entries for height
- `Dump` → log state

**Crash simulation:**
When the controller triggers a crash for this node:
1. Kill the node's actors (via ractor)
2. Discard unflushed WAL entries (simulates data loss on crash)
3. Optionally corrupt specific flushed entries (simulate disk corruption)
4. On restart, the `StartedHeight` call returns only surviving entries, triggering WAL replay in the Consensus actor

### Fault injection

```rust
pub enum FaultScenario {
    /// Nodes in group_a cannot communicate with nodes in group_b
    Partition {
        group_a: Vec<NodeId>,
        group_b: Vec<NodeId>,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Messages to/from a node are delayed by a fixed number of ticks
    Delay {
        node: NodeId,
        delay_ticks: u64,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Messages to/from a node are dropped with given probability
    MessageLoss {
        node: NodeId,
        drop_probability: f64,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Crash a node at a specific tick
    CrashAt {
        node: NodeId,
        tick: u64,
        corrupt_wal: bool,
        restart_after_ticks: Option<u64>,
    },

    /// WAL write failures for a node
    WalFailure {
        node: NodeId,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Duplicate messages from a node (simulates network duplication)
    Duplicate {
        node: NodeId,
        duplication_probability: f64,
        start_tick: u64,
        duration_ticks: u64,
    },
}
```

### SimulatedNodeRunner

Implements `NodeRunner<TestContext>` so existing tests can switch to simulated mode.

```rust
pub struct SimulatedNodeRunner {
    controller: Arc<Mutex<SimulationController<TestContext>>>,
    nodes_info: HashMap<NodeId, NodeInfo>,
    private_keys: HashMap<NodeId, PrivateKey>,
    validator_set: ValidatorSet,
    params: TestParams,
}

#[async_trait]
impl NodeRunner<TestContext> for SimulatedNodeRunner {
    type NodeHandle = SimulatedNodeHandle;

    fn new<S>(id: usize, nodes: &[TestNode<TestContext, S>], params: TestParams) -> Self {
        // Pause tokio time for determinism
        // Create SimulationController with seed derived from test id
        // Register all nodes in controller
    }

    async fn spawn(&self, id: NodeId) -> Result<SimulatedNodeHandle> {
        // 1. Create SimulatedNetwork actor for this node
        // 2. Create SimulatedWal actor for this node
        // 3. Build engine via EngineBuilder:
        //      .with_custom_network(sim_net_ref, tx_network)
        //      .with_custom_wal(sim_wal_ref)
        //      .with_default_sync(SyncContext::new(JsonCodec))
        //      .with_default_consensus(ConsensusContext::new(...))
        //      .with_default_request(RequestContext::new(100))
        //      .build()
        // 4. Start controller tick loop (if first node)
        // 5. Return handle
    }

    async fn reset_db(&self, id: NodeId) -> Result<()> {
        // Clear SimulatedWal entries for this node
    }
}
```

## Integration with existing test framework

### Running existing tests in simulated mode

```rust
// Current: runs with real network and WAL
test.build()
    .run::<TestRunner>(Duration::from_secs(60))
    .await;

// New: runs with simulated network and WAL
test.build()
    .run::<SimulatedNodeRunner>(Duration::from_secs(60))
    .await;
```

### Running with fault injection

```rust
let sim_config = SimConfig::new()
    .with_seed(42)
    .with_tick_duration(Duration::from_millis(10))
    .with_fault(FaultScenario::Partition {
        group_a: vec![0.into(), 1.into()],
        group_b: vec![2.into(), 3.into()],
        start_tick: 100,
        duration_ticks: 200,
    })
    .with_fault(FaultScenario::CrashAt {
        node: 1.into(),
        tick: 150,
        corrupt_wal: false,
        restart_after_ticks: Some(50),
    });

test.build()
    .run_simulated(Duration::from_secs(60), sim_config)
    .await;
```

### Seed-based exploration

```rust
// Run the same test with 1000 different seeds to explore schedules
for seed in 0..1000 {
    let sim_config = SimConfig::new()
        .with_seed(seed)
        .with_random_faults(FaultProfile::Moderate); // auto-generate faults from seed

    test.build()
        .run_simulated(Duration::from_secs(60), sim_config)
        .await;
}
```

## Tokio runtime configuration

All DST tests run on:
- `tokio::runtime::Builder::new_current_thread()` — single-threaded, deterministic task ordering
- `tokio::time::pause()` — time frozen at start, advanced by controller
- `tokio::time::advance(duration)` — controller advances time in discrete steps

This means:
- All `TimerScheduler` timeouts in the Consensus actor fire at controlled times
- No real wall-clock time passes during tests (tests run as fast as possible)
- Actors process messages in a predictable order within each tick

## Testing strategy

### Phase 1: Basic simulation
- SimulatedNetwork delivers all messages (no faults)
- SimulatedWal stores everything in memory (no failures)
- Verify existing integration tests pass with `SimulatedNodeRunner`

### Phase 2: Fault injection
- Network partitions, delays, message loss
- Node crashes with WAL replay
- WAL corruption scenarios

### Phase 3: Seed-based exploration
- Random fault generation from seed
- `FaultProfile` presets (Mild, Moderate, Severe, Byzantine)
- Integration with `engine-byzantine` for Byzantine nodes

### Phase 4: Invariant checking
- Safety: no two honest nodes decide different values at the same height
- Liveness: if fewer than 1/3 are faulty and partitions heal, nodes eventually decide
- WAL correctness: after crash + replay, node resumes correctly

## Determinism guarantees

**What is deterministic given the same seed:**
- Message delivery order
- Fault injection timing and targets
- RNG-driven decisions (which messages to drop, reorder, etc.)
- Tokio timer expirations

**What is near-deterministic but not guaranteed:**
- Tokio task scheduling order within a single time tick (in practice, `current_thread` runtime is highly reproducible, but not guaranteed by Tokio's API)

**Mitigation:** If a test fails, the seed is logged. Re-running with the same seed reproduces the failure in the vast majority of cases. For the rare scheduling-dependent edge case, running the same seed multiple times will catch it.

## Relationship to existing testing infrastructure

| Layer | Tool | What it tests | Deterministic? |
|-------|------|---------------|---------------|
| Consensus logic | MBT (test/mbt) | Pure state machine | Yes (100%) |
| Single engine | Test framework | Engine + app integration | No (real Tokio) |
| Multi-node cluster | Test framework + TestRunner | Full consensus with real network | No (real network + Tokio) |
| Multi-node cluster | **DST (test/dst)** | **Full consensus with simulated infra** | **Near-deterministic (seed-based)** |
| Full testnet | Quake | Production-like environment | No (Docker, real OS) |

DST fills the gap between unit-level MBT and the full testnet (Quake), providing fast, reproducible multi-node testing.
