# Deterministic Simulation Testing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a `test/dst` crate that provides deterministic, seed-reproducible multi-node consensus testing by replacing the network and WAL layers with simulated actors.

**Architecture:** A `SimulatedNodeRunner` implements the existing `NodeRunner<TestContext>` trait, wiring each node's engine to `SimulatedNetwork` and `SimulatedWal` actors instead of real libp2p and redb. A central `SimulationController` manages message routing, fault injection, and time advancement. Existing tests can switch to simulated mode by changing the runner type.

**Tech Stack:** Rust, ractor (actor framework), tokio (with `time::pause()`), rand (`StdRng` for seed-based determinism)

**Key Reference Files:**
- Engine builder: `code/crates/app-channel/src/builder.rs` (EngineBuilder, `with_custom_network` L591, `with_custom_wal` L533)
- Byzantine proxy pattern: `code/crates/engine-byzantine/src/proxy.rs` (ByzantineNetworkProxy, Actor impl L92)
- Network messages: `code/crates/engine/src/network.rs` (Msg enum L153, NetworkEvent L105, Subscriber trait L38)
- WAL messages: `code/crates/engine/src/wal.rs` (Msg enum L55, WalRef L22)
- Sync messages: `code/crates/engine/src/sync.rs` (Msg enum L83, SyncRef L63)
- Test framework: `code/crates/test/framework/src/lib.rs` (NodeRunner trait L188, run_test L156, HasTestRunner L83)
- Existing TestRunner: `code/crates/test/tests/it/main.rs` (TestRunner L48, spawn L111)
- Test app node: `code/crates/test/app/src/node.rs` (App L62, start() L133, Handle L41)
- Events: `code/crates/engine/src/util/events.rs` (Event enum L47, TxEvent L19)
- Design doc: `docs/plans/2026-02-21-deterministic-simulation-testing-design.md`

---

### Task 1: Create the crate skeleton

**Files:**
- Create: `code/crates/test/dst/Cargo.toml`
- Create: `code/crates/test/dst/src/lib.rs`
- Create: `code/crates/test/dst/src/fault.rs`
- Create: `code/crates/test/dst/src/controller.rs`
- Create: `code/crates/test/dst/src/network.rs`
- Create: `code/crates/test/dst/src/wal.rs`
- Create: `code/crates/test/dst/src/runner.rs`
- Modify: `code/Cargo.toml` (workspace members and dependencies)

**Step 1: Create Cargo.toml**

```toml
[package]
name = "informalsystems-malachitebft-test-dst"
description = "Deterministic Simulation Testing for Malachite BFT"
publish = false

version.workspace = true
edition.workspace = true
repository.workspace = true
license.workspace = true
rust-version.workspace = true

[lints]
workspace = true

[dependencies]
malachitebft-app-channel.workspace = true
malachitebft-engine.workspace = true
malachitebft-core-types.workspace = true
malachitebft-core-consensus.workspace = true
malachitebft-config.workspace = true
malachitebft-metrics.workspace = true
malachitebft-sync.workspace = true
malachitebft-peer.workspace = true
malachitebft-test.workspace = true
malachitebft-test-app.workspace = true
malachitebft-test-framework.workspace = true

async-trait.workspace = true
eyre.workspace = true
ractor.workspace = true
rand.workspace = true
tokio.workspace = true
tracing.workspace = true
```

**Step 2: Create stub source files**

`src/lib.rs`:
```rust
pub mod controller;
pub mod fault;
pub mod network;
pub mod runner;
pub mod wal;
```

`src/fault.rs`, `src/controller.rs`, `src/network.rs`, `src/wal.rs`, `src/runner.rs`: empty files for now.

**Step 3: Add to workspace**

In `code/Cargo.toml`, add to members array (after `"crates/test/framework"`):
```toml
  "crates/test/dst",
```

And add to workspace dependencies:
```toml
malachitebft-test-dst              = { version = "0.7.0-pre", package = "informalsystems-malachitebft-test-dst", path = "crates/test/dst" }
```

**Step 4: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-dst`
Expected: Compiles with no errors (possibly warnings about unused imports)

**Step 5: Commit**

```bash
git add code/crates/test/dst/ code/Cargo.toml
git commit -m "feat(test-dst): add crate skeleton for deterministic simulation testing"
```

---

### Task 2: Implement FaultScenario types

**Files:**
- Modify: `code/crates/test/dst/src/fault.rs`

**Step 1: Write tests for FaultScenario**

Add to `src/fault.rs`:

```rust
use malachitebft_test_framework::NodeId;

/// A fault scenario to inject during simulation.
#[derive(Debug, Clone)]
pub enum FaultScenario {
    /// Nodes in group_a cannot communicate with nodes in group_b.
    Partition {
        group_a: Vec<NodeId>,
        group_b: Vec<NodeId>,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Messages to/from a node are delayed by a fixed number of ticks.
    Delay {
        node: NodeId,
        delay_ticks: u64,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Messages to/from a node are dropped with given probability.
    MessageLoss {
        node: NodeId,
        drop_probability: f64,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Crash a node at a specific tick.
    CrashAt {
        node: NodeId,
        tick: u64,
        corrupt_wal: bool,
        restart_after_ticks: Option<u64>,
    },

    /// WAL write failures for a node.
    WalFailure {
        node: NodeId,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Duplicate messages from a node.
    Duplicate {
        node: NodeId,
        duplication_probability: f64,
        start_tick: u64,
        duration_ticks: u64,
    },
}

impl FaultScenario {
    /// Check if this fault is active at the given tick.
    pub fn is_active_at(&self, tick: u64) -> bool {
        match self {
            Self::Partition { start_tick, duration_ticks, .. }
            | Self::Delay { start_tick, duration_ticks, .. }
            | Self::MessageLoss { start_tick, duration_ticks, .. }
            | Self::WalFailure { start_tick, duration_ticks, .. }
            | Self::Duplicate { start_tick, duration_ticks, .. } => {
                tick >= *start_tick && tick < start_tick + duration_ticks
            }
            Self::CrashAt { tick: crash_tick, .. } => tick == *crash_tick,
        }
    }

    /// Check if a message between source and destination is blocked by this fault.
    pub fn blocks_message(&self, tick: u64, source: NodeId, dest: NodeId) -> bool {
        if !self.is_active_at(tick) {
            return false;
        }
        match self {
            Self::Partition { group_a, group_b, .. } => {
                (group_a.contains(&source) && group_b.contains(&dest))
                    || (group_b.contains(&source) && group_a.contains(&dest))
            }
            _ => false,
        }
    }
}

/// Configuration for a simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub seed: u64,
    pub tick_duration: std::time::Duration,
    pub faults: Vec<FaultScenario>,
}

impl SimConfig {
    pub fn new() -> Self {
        Self {
            seed: 0,
            tick_duration: std::time::Duration::from_millis(10),
            faults: Vec::new(),
        }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_tick_duration(mut self, duration: std::time::Duration) -> Self {
        self.tick_duration = duration;
        self
    }

    pub fn with_fault(mut self, fault: FaultScenario) -> Self {
        self.faults.push(fault);
        self
    }
}

impl Default for SimConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_is_active_within_window() {
        let fault = FaultScenario::Partition {
            group_a: vec![1.into()],
            group_b: vec![2.into()],
            start_tick: 10,
            duration_ticks: 5,
        };
        assert!(!fault.is_active_at(9));
        assert!(fault.is_active_at(10));
        assert!(fault.is_active_at(14));
        assert!(!fault.is_active_at(15));
    }

    #[test]
    fn partition_blocks_cross_group_messages() {
        let fault = FaultScenario::Partition {
            group_a: vec![1.into()],
            group_b: vec![2.into()],
            start_tick: 0,
            duration_ticks: 100,
        };
        assert!(fault.blocks_message(5, 1.into(), 2.into()));
        assert!(fault.blocks_message(5, 2.into(), 1.into()));
        assert!(!fault.blocks_message(5, 1.into(), 3.into()));
    }

    #[test]
    fn sim_config_builder() {
        let config = SimConfig::new()
            .with_seed(42)
            .with_tick_duration(std::time::Duration::from_millis(5))
            .with_fault(FaultScenario::Delay {
                node: 1.into(),
                delay_ticks: 3,
                start_tick: 0,
                duration_ticks: 100,
            });
        assert_eq!(config.seed, 42);
        assert_eq!(config.faults.len(), 1);
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All 3 tests pass

**Step 3: Commit**

```bash
git add code/crates/test/dst/src/fault.rs
git commit -m "feat(test-dst): implement FaultScenario types and SimConfig builder"
```

---

### Task 3: Implement SimulatedWal actor

The SimulatedWal is simpler than SimulatedNetwork, so we build it first.

**Files:**
- Modify: `code/crates/test/dst/src/wal.rs`

**Reference:** The real WAL actor message type is at `code/crates/engine/src/wal.rs:55-61`. The ByzantineNetworkProxy actor pattern is at `code/crates/engine-byzantine/src/proxy.rs:92`.

**Step 1: Implement the SimulatedWal actor**

```rust
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tracing::{debug, warn};

use malachitebft_core_consensus::WalEntry;
use malachitebft_core_types::Context;
use malachitebft_engine::wal::{Msg as WalMsg, WalRef};
use malachitebft_test_framework::NodeId;

use crate::fault::FaultScenario;

/// Shared state for the simulated WAL, accessible by the controller for crash simulation.
#[derive(Debug)]
pub struct SimulatedWalStore<Ctx: Context> {
    /// Entries per height. Each entry has a flag indicating whether it's been flushed.
    entries: HashMap<Ctx::Height, Vec<(WalEntry<Ctx>, bool)>>,
    /// Whether WAL writes should currently fail (set by controller for fault injection).
    pub fail_writes: bool,
}

impl<Ctx: Context> SimulatedWalStore<Ctx> {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            fail_writes: false,
        }
    }

    /// Discard all unflushed entries (simulates crash before flush).
    pub fn discard_unflushed(&mut self) {
        for entries in self.entries.values_mut() {
            entries.retain(|(_, flushed)| *flushed);
        }
    }

    /// Clear all entries (simulates full DB reset).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

pub struct SimulatedWal<Ctx: Context> {
    node_id: NodeId,
    store: Arc<Mutex<SimulatedWalStore<Ctx>>>,
}

impl<Ctx: Context> SimulatedWal<Ctx> {
    pub async fn spawn(
        node_id: NodeId,
        store: Arc<Mutex<SimulatedWalStore<Ctx>>>,
    ) -> Result<WalRef<Ctx>, SpawnErr> {
        let actor = Self {
            node_id,
            store,
        };
        let (actor_ref, _) = Actor::spawn(
            Some(format!("sim-wal-{node_id}")),
            actor,
            (),
        )
        .await?;
        Ok(actor_ref)
    }
}

#[async_trait]
impl<Ctx: Context> Actor for SimulatedWal<Ctx> {
    type Msg = WalMsg<Ctx>;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            WalMsg::StartedHeight(height, reply) => {
                let store = self.store.lock().unwrap();
                let entries: Vec<io::Result<WalEntry<Ctx>>> = store
                    .entries
                    .get(&height)
                    .map(|es| es.iter().map(|(e, _)| Ok(e.clone())).collect())
                    .unwrap_or_default();
                debug!(node = %self.node_id, %height, count = entries.len(), "WAL replay");
                let _ = reply.send(Ok(entries));
            }

            WalMsg::Append(height, entry, reply) => {
                let mut store = self.store.lock().unwrap();
                if store.fail_writes {
                    warn!(node = %self.node_id, %height, "WAL append failed (fault injected)");
                    let _ = reply.send(Err(eyre::eyre!("simulated WAL write failure")));
                } else {
                    store
                        .entries
                        .entry(height)
                        .or_default()
                        .push((entry, false));
                    let _ = reply.send(Ok(()));
                }
            }

            WalMsg::Flush(reply) => {
                let mut store = self.store.lock().unwrap();
                if store.fail_writes {
                    warn!(node = %self.node_id, "WAL flush failed (fault injected)");
                    let _ = reply.send(Err(eyre::eyre!("simulated WAL flush failure")));
                } else {
                    for entries in store.entries.values_mut() {
                        for (_, flushed) in entries.iter_mut() {
                            *flushed = true;
                        }
                    }
                    let _ = reply.send(Ok(()));
                }
            }

            WalMsg::Reset(height, reply) => {
                let mut store = self.store.lock().unwrap();
                store.entries.remove(&height);
                let _ = reply.send(Ok(()));
            }

            WalMsg::Dump => {
                let store = self.store.lock().unwrap();
                debug!(node = %self.node_id, heights = store.entries.len(), "WAL dump");
            }
        }
        Ok(())
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-dst`
Expected: Compiles (the actor matches the WalMsg type exactly)

**Step 3: Commit**

```bash
git add code/crates/test/dst/src/wal.rs
git commit -m "feat(test-dst): implement SimulatedWal in-memory actor"
```

---

### Task 4: Implement SimulationController (core scheduling)

This is the central piece. Start with basic message routing (no faults yet).

**Files:**
- Modify: `code/crates/test/dst/src/controller.rs`

**Step 1: Implement the controller core**

The controller needs to:
1. Track registered nodes and their simulated network actor refs
2. Accept messages from simulated networks and schedule them for delivery
3. Advance time and deliver messages

```rust
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use ractor::ActorRef;
use tracing::{debug, info, trace, warn};

use malachitebft_core_types::Context;
use malachitebft_engine::network::{Msg as NetworkMsg, NetworkRef};
use malachitebft_test_framework::NodeId;

use crate::fault::FaultScenario;

/// The type of simulated message being routed.
#[derive(Debug, Clone)]
pub enum SimMessage<Ctx: Context> {
    /// A network message to broadcast to all other nodes.
    Broadcast {
        source: NodeId,
        msg: NetworkMsg<Ctx>,
    },
    /// A network message directed at a specific node.
    Directed {
        source: NodeId,
        dest: NodeId,
        msg: NetworkMsg<Ctx>,
    },
}

/// A message scheduled for delivery at a specific tick.
#[derive(Debug)]
struct ScheduledMessage<Ctx: Context> {
    delivery_tick: u64,
    sequence: u64, // for stable ordering within same tick
    message: SimMessage<Ctx>,
}

impl<Ctx: Context> PartialEq for ScheduledMessage<Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.delivery_tick == other.delivery_tick && self.sequence == other.sequence
    }
}

impl<Ctx: Context> Eq for ScheduledMessage<Ctx> {}

impl<Ctx: Context> PartialOrd for ScheduledMessage<Ctx> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Ctx: Context> Ord for ScheduledMessage<Ctx> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so we reverse for min-heap behavior
        other
            .delivery_tick
            .cmp(&self.delivery_tick)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// Handle to a registered node's simulated network actor.
pub struct SimNodeEntry<Ctx: Context> {
    pub network_ref: NetworkRef<Ctx>,
}

pub struct SimulationController<Ctx: Context> {
    pub seed: u64,
    pub rng: StdRng,
    pub nodes: HashMap<NodeId, SimNodeEntry<Ctx>>,
    message_queue: BinaryHeap<ScheduledMessage<Ctx>>,
    pub faults: Vec<FaultScenario>,
    pub current_tick: u64,
    pub tick_duration: Duration,
    sequence_counter: u64,
}

impl<Ctx: Context> SimulationController<Ctx> {
    pub fn new(seed: u64, tick_duration: Duration) -> Self {
        Self {
            seed,
            rng: StdRng::seed_from_u64(seed),
            nodes: HashMap::new(),
            message_queue: BinaryHeap::new(),
            faults: Vec::new(),
            current_tick: 0,
            tick_duration,
            sequence_counter: 0,
        }
    }

    pub fn register_node(&mut self, id: NodeId, network_ref: NetworkRef<Ctx>) {
        self.nodes.insert(id, SimNodeEntry { network_ref });
    }

    /// Enqueue a broadcast message from a source node to all other nodes.
    pub fn enqueue_broadcast(&mut self, source: NodeId, msg: NetworkMsg<Ctx>) {
        let delivery_tick = self.compute_delivery_tick(source);
        let sequence = self.next_sequence();

        trace!(
            %source,
            tick = delivery_tick,
            "Enqueuing broadcast message"
        );

        self.message_queue.push(ScheduledMessage {
            delivery_tick,
            sequence,
            message: SimMessage::Broadcast { source, msg },
        });
    }

    /// Enqueue a directed message from source to a specific destination.
    pub fn enqueue_directed(&mut self, source: NodeId, dest: NodeId, msg: NetworkMsg<Ctx>) {
        let delivery_tick = self.compute_delivery_tick(source);
        let sequence = self.next_sequence();

        trace!(
            %source,
            %dest,
            tick = delivery_tick,
            "Enqueuing directed message"
        );

        self.message_queue.push(ScheduledMessage {
            delivery_tick,
            sequence,
            message: SimMessage::Directed { source, dest, msg },
        });
    }

    /// Compute the tick at which a message from source should be delivered.
    /// Accounts for delay faults.
    fn compute_delivery_tick(&self, source: NodeId) -> u64 {
        let mut delay = 1; // minimum 1-tick delivery delay

        for fault in &self.faults {
            if let FaultScenario::Delay {
                node,
                delay_ticks,
                start_tick,
                duration_ticks,
            } = fault
            {
                if *node == source
                    && self.current_tick >= *start_tick
                    && self.current_tick < start_tick + duration_ticks
                {
                    delay = delay.max(*delay_ticks);
                }
            }
        }

        self.current_tick + delay
    }

    /// Check if a message from source to dest is blocked by any active fault.
    fn is_blocked(&mut self, source: NodeId, dest: NodeId) -> bool {
        for fault in &self.faults {
            if fault.blocks_message(self.current_tick, source, dest) {
                return true;
            }

            if let FaultScenario::MessageLoss {
                node,
                drop_probability,
                start_tick,
                duration_ticks,
            } = fault
            {
                if *node == source
                    && self.current_tick >= *start_tick
                    && self.current_tick < start_tick + duration_ticks
                {
                    if self.rng.gen_bool(*drop_probability) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Get the tick of the next pending message, if any.
    pub fn next_message_tick(&self) -> Option<u64> {
        self.message_queue.peek().map(|m| m.delivery_tick)
    }

    /// Drain all messages scheduled for the current tick and return them.
    /// Messages blocked by faults are dropped.
    pub fn drain_current_tick(&mut self) -> Vec<(NodeId, NetworkMsg<Ctx>)> {
        let mut deliveries = Vec::new();

        while let Some(scheduled) = self.message_queue.peek() {
            if scheduled.delivery_tick > self.current_tick {
                break;
            }

            let scheduled = self.message_queue.pop().unwrap();

            match scheduled.message {
                SimMessage::Broadcast { source, msg } => {
                    let dest_ids: Vec<NodeId> = self
                        .nodes
                        .keys()
                        .filter(|id| **id != source)
                        .copied()
                        .collect();

                    for dest in dest_ids {
                        if !self.is_blocked(source, dest) {
                            deliveries.push((dest, msg.clone()));
                        } else {
                            debug!(%source, %dest, "Dropped message (fault)");
                        }
                    }
                }
                SimMessage::Directed { source, dest, msg } => {
                    if !self.is_blocked(source, dest) {
                        deliveries.push((dest, msg));
                    } else {
                        debug!(%source, %dest, "Dropped directed message (fault)");
                    }
                }
            }
        }

        deliveries
    }

    /// Advance the controller's tick counter by one.
    pub fn advance_tick(&mut self) {
        self.current_tick += 1;
    }

    /// Check if there are any pending messages.
    pub fn has_pending_messages(&self) -> bool {
        !self.message_queue.is_empty()
    }

    fn next_sequence(&mut self) -> u64 {
        let seq = self.sequence_counter;
        self.sequence_counter += 1;
        seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use malachitebft_test::TestContext;

    #[test]
    fn controller_creation() {
        let ctrl = SimulationController::<TestContext>::new(42, Duration::from_millis(10));
        assert_eq!(ctrl.seed, 42);
        assert_eq!(ctrl.current_tick, 0);
        assert!(!ctrl.has_pending_messages());
    }

    #[test]
    fn next_message_tick_empty() {
        let ctrl = SimulationController::<TestContext>::new(0, Duration::from_millis(10));
        assert_eq!(ctrl.next_message_tick(), None);
    }
}
```

**Step 2: Verify it compiles and tests pass**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All tests pass

**Step 3: Commit**

```bash
git add code/crates/test/dst/src/controller.rs
git commit -m "feat(test-dst): implement SimulationController with message scheduling and fault checking"
```

---

### Task 5: Implement SimulatedNetwork actor

This is the most complex actor — it intercepts all network messages and routes them through the controller.

**Files:**
- Modify: `code/crates/test/dst/src/network.rs`

**Reference:** Follow the pattern from `ByzantineNetworkProxy` at `code/crates/engine-byzantine/src/proxy.rs:92`. The actor must handle all variants of `NetworkMsg<Ctx>` (defined at `code/crates/engine/src/network.rs:153-190`).

**Step 1: Implement the SimulatedNetwork actor**

The key challenge is that `NetworkMsg` contains variants like `Subscribe(Box<dyn Subscriber<NetworkEvent<Ctx>>>)` and `NewEvent(Event)`. The `Subscribe` variant registers local subscribers (Consensus and Sync actors). The `NewEvent` variant is how the controller delivers incoming events to the local subscribers.

```rust
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tracing::{debug, trace, warn};

use malachitebft_core_types::Context;
use malachitebft_engine::network::{
    Msg as NetworkMsg, NetworkEvent, NetworkRef, NetworkStateDump, Subscriber,
};
use malachitebft_network::{Event, PeerId};
use malachitebft_sync::{InboundRequestId, OutboundRequestId};
use malachitebft_test_framework::NodeId;

use crate::controller::SimulationController;

pub struct SimulatedNetwork<Ctx: Context> {
    node_id: NodeId,
    peer_id: PeerId,
    controller: Arc<Mutex<SimulationController<Ctx>>>,
}

pub struct SimulatedNetworkState<Ctx: Context> {
    subscribers: Vec<Box<dyn Subscriber<NetworkEvent<Ctx>>>>,
}

impl<Ctx: Context> SimulatedNetwork<Ctx> {
    pub async fn spawn(
        node_id: NodeId,
        peer_id: PeerId,
        controller: Arc<Mutex<SimulationController<Ctx>>>,
    ) -> Result<(NetworkRef<Ctx>, tokio::sync::mpsc::Sender<NetworkMsg<Ctx>>), SpawnErr> {
        let actor = Self {
            node_id,
            peer_id,
            controller,
        };

        let (actor_ref, _) = Actor::spawn(
            Some(format!("sim-net-{node_id}")),
            actor,
            (),
        )
        .await?;

        // Create a channel for the EngineBuilder's tx_network parameter.
        // The EngineBuilder requires a Sender<NetworkMsg> for the host to send
        // network messages (like PublishProposalPart from the app).
        let (tx, mut rx) = tokio::sync::mpsc::channel::<NetworkMsg<Ctx>>(256);
        let ref_clone = actor_ref.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let _ = ref_clone.cast(msg);
            }
        });

        Ok((actor_ref, tx))
    }

    /// Deliver a network event to all local subscribers.
    fn deliver_to_subscribers(
        state: &SimulatedNetworkState<Ctx>,
        event: NetworkEvent<Ctx>,
    ) {
        for sub in &state.subscribers {
            sub.send(event.clone());
        }
    }
}

#[async_trait]
impl<Ctx: Context> Actor for SimulatedNetwork<Ctx> {
    type Msg = NetworkMsg<Ctx>;
    type State = SimulatedNetworkState<Ctx>;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(SimulatedNetworkState {
            subscribers: Vec::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            // Local subscriber registration
            NetworkMsg::Subscribe(subscriber) => {
                trace!(node = %self.node_id, "Registering network subscriber");
                state.subscribers.push(subscriber);
            }

            // Outbound: consensus message to broadcast
            NetworkMsg::PublishConsensusMsg(consensus_msg) => {
                trace!(node = %self.node_id, "Publishing consensus message via simulation");
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(
                    self.node_id,
                    NetworkMsg::PublishConsensusMsg(consensus_msg),
                );
            }

            // Outbound: liveness message to broadcast
            NetworkMsg::PublishLivenessMsg(liveness_msg) => {
                trace!(node = %self.node_id, "Publishing liveness message via simulation");
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(
                    self.node_id,
                    NetworkMsg::PublishLivenessMsg(liveness_msg),
                );
            }

            // Outbound: proposal part to broadcast
            NetworkMsg::PublishProposalPart(part) => {
                trace!(node = %self.node_id, "Publishing proposal part via simulation");
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(
                    self.node_id,
                    NetworkMsg::PublishProposalPart(part),
                );
            }

            // Outbound: status broadcast
            NetworkMsg::BroadcastStatus(status) => {
                trace!(node = %self.node_id, "Broadcasting status via simulation");
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(
                    self.node_id,
                    NetworkMsg::BroadcastStatus(status),
                );
            }

            // Outbound: sync request to specific peer
            NetworkMsg::OutgoingRequest(peer_id, request, reply) => {
                trace!(node = %self.node_id, %peer_id, "Outgoing sync request via simulation");
                let request_id = OutboundRequestId::new(format!(
                    "sim-{}-{}",
                    self.node_id,
                    rand::random::<u32>()
                ));
                let _ = reply.send(request_id.clone());

                // Route to the destination node via controller
                // The destination is identified by peer_id — the controller maps peer_id to NodeId
                let mut ctrl = self.controller.lock().unwrap();
                // For now, we need a way to map PeerId -> NodeId.
                // This will be set up during runner initialization.
                // We store the request as a directed message.
                ctrl.enqueue_broadcast(
                    self.node_id,
                    NetworkMsg::OutgoingRequest(peer_id, request, reply),
                );
                // TODO: Implement proper peer_id -> node_id mapping and directed routing
            }

            // Outbound: sync response
            NetworkMsg::OutgoingResponse(request_id, response) => {
                trace!(node = %self.node_id, "Outgoing sync response via simulation");
                // Route back to the requesting node
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(
                    self.node_id,
                    NetworkMsg::OutgoingResponse(request_id, response),
                );
                // TODO: Implement proper routing back to requester
            }

            // Inbound: event delivered by the controller
            NetworkMsg::NewEvent(event) => {
                // Convert raw network Event to typed NetworkEvent and deliver to subscribers
                // This is where the controller pushes received messages into the local node
                trace!(node = %self.node_id, "Received simulated network event");
                // The event will be decoded and delivered by the controller's delivery mechanism
                // For now, this is a pass-through
            }

            // Local operations — no simulation needed
            NetworkMsg::DumpState(reply) => {
                let _ = reply.send(None);
            }

            NetworkMsg::UpdatePersistentPeers(_, reply) => {
                let _ = reply.send(Ok(()));
            }

            NetworkMsg::UpdateValidatorSet(_vs) => {
                // Store locally if needed
            }
        }

        Ok(())
    }
}
```

**Important note:** The sync request/response routing (OutgoingRequest, OutgoingResponse) needs a PeerId-to-NodeId mapping. This will be refined in Task 6 when we build the runner. For now, the basic broadcast messages (PublishConsensusMsg, PublishLivenessMsg, PublishProposalPart, BroadcastStatus) are the critical path.

**Step 2: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-dst`
Expected: Compiles (possibly with warnings about unused variables/TODO items)

**Step 3: Commit**

```bash
git add code/crates/test/dst/src/network.rs
git commit -m "feat(test-dst): implement SimulatedNetwork actor with message routing"
```

---

### Task 6: Implement SimulatedNodeRunner

This wires everything together. The runner creates the controller, spawns simulated actors, and builds each node's engine.

**Files:**
- Modify: `code/crates/test/dst/src/runner.rs`
- Modify: `code/crates/test/dst/src/lib.rs`

**Reference:** The existing `TestRunner` at `code/crates/test/tests/it/main.rs:48-197` is the template. The key difference is using `with_custom_network` and `with_custom_wal` instead of defaults.

**Step 1: Implement the runner**

Model it closely on the existing `TestRunner` but replace network and WAL with simulated versions:

```rust
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tempfile::TempDir;
use tokio::task::JoinHandle;

use malachitebft_app_channel::app::events::TxEvent;
use malachitebft_app_channel::{
    ConsensusContext, EngineBuilder, EngineHandle, RequestContext, SyncContext, WalContext,
};
use malachitebft_test::codec::json::JsonCodec;
use malachitebft_test::codec::proto::ProtobufCodec;
use malachitebft_test::middleware::{DefaultMiddleware, Middleware};
use malachitebft_test::node::{Node, NodeHandle};
use malachitebft_test::traits::{
    CanGeneratePrivateKey, CanMakeConfig, CanMakeGenesis, CanMakePrivateKeyFile, MakeConfigSettings,
};
use malachitebft_test::{
    Address, Ed25519Provider, Genesis, Height, PrivateKey, PublicKey, TestContext, Validator,
    ValidatorSet,
};
use malachitebft_test_app::config::Config;
use malachitebft_test_app::node::App;
use malachitebft_test_framework::{ConfigModifier, NodeId, NodeRunner, TestNode, TestParams};

use crate::controller::SimulationController;
use crate::fault::SimConfig;
use crate::network::SimulatedNetwork;
use crate::wal::{SimulatedWal, SimulatedWalStore};

/// Handle for a node running in simulation.
pub struct SimulatedNodeHandle {
    pub engine: EngineHandle,
    pub tx_event: TxEvent<TestContext>,
    pub app_handle: JoinHandle<()>,
}

#[async_trait]
impl NodeHandle<TestContext> for SimulatedNodeHandle {
    fn subscribe(&self) -> malachitebft_app_channel::app::events::RxEvent<TestContext> {
        self.tx_event.subscribe()
    }

    async fn kill(&self, _reason: Option<String>) -> eyre::Result<()> {
        self.engine.actor.kill_and_wait(None).await?;
        self.app_handle.abort();
        self.engine.handle.abort();
        Ok(())
    }
}

#[derive(Clone)]
struct SimNodeInfo {
    start_height: Height,
    home_dir: PathBuf,
    middleware: Arc<dyn Middleware>,
    config_modifier: ConfigModifier<Config>,
}

/// A NodeRunner that wires nodes through simulated network and WAL actors.
#[derive(Clone)]
pub struct SimulatedNodeRunner {
    id: usize,
    params: TestParams,
    nodes_info: HashMap<NodeId, SimNodeInfo>,
    private_keys: HashMap<NodeId, PrivateKey>,
    validator_set: ValidatorSet,
    controller: Arc<Mutex<SimulationController<TestContext>>>,
    wal_stores: Arc<Mutex<HashMap<NodeId, Arc<Mutex<SimulatedWalStore<TestContext>>>>>>,
}

#[async_trait]
impl NodeRunner<TestContext> for SimulatedNodeRunner {
    type NodeHandle = SimulatedNodeHandle;

    fn new<S>(id: usize, nodes: &[TestNode<TestContext, S>], params: TestParams) -> Self {
        // Pause tokio time for deterministic simulation
        tokio::time::pause();

        let (validators, private_keys) = make_validators(nodes, &params);
        let validator_set = ValidatorSet::new(validators);

        let nodes_info = nodes
            .iter()
            .map(|node| {
                (
                    node.id,
                    SimNodeInfo {
                        start_height: node.start_height,
                        home_dir: temp_dir(node.id),
                        middleware: Arc::clone(&node.middleware),
                        config_modifier: Arc::clone(&node.config_modifier),
                    },
                )
            })
            .collect();

        // Derive seed from test id for determinism
        let seed = id as u64;
        let controller = SimulationController::new(seed, std::time::Duration::from_millis(10));

        Self {
            id,
            params,
            nodes_info,
            private_keys,
            validator_set,
            controller: Arc::new(Mutex::new(controller)),
            wal_stores: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn spawn(&self, id: NodeId) -> eyre::Result<SimulatedNodeHandle> {
        let node_info = &self.nodes_info[&id];
        let private_key = self.private_keys[&id].clone();

        // Create the App with the same logic as the real TestRunner
        let app = App {
            config: self.generate_config(id),
            home_dir: node_info.home_dir.clone(),
            validator_set: self.validator_set.clone(),
            private_key: private_key.clone(),
            start_height: Some(node_info.start_height),
            middleware: Some(Arc::clone(&node_info.middleware)),
        };

        // Create simulated WAL
        let wal_store = Arc::new(Mutex::new(SimulatedWalStore::new()));
        {
            let mut stores = self.wal_stores.lock().unwrap();
            stores.insert(id, Arc::clone(&wal_store));
        }
        let wal_ref = SimulatedWal::spawn(id, wal_store).await?;

        // Create simulated network
        let peer_id = malachitebft_network::PeerId::random();
        let (net_ref, tx_network) =
            SimulatedNetwork::spawn(id, peer_id, Arc::clone(&self.controller)).await?;

        // Register with controller
        {
            let mut ctrl = self.controller.lock().unwrap();
            ctrl.register_node(id, net_ref.clone());
        }

        // Build engine with simulated actors
        // This is the key difference from the real TestRunner:
        // we use with_custom_network and with_custom_wal
        let ctx = TestContext::with_middleware(
            node_info.middleware.clone(),
        );
        let public_key = app.get_public_key(&private_key);
        let address = app.get_address(&public_key);

        let config = app.config.clone();
        let registry = malachitebft_metrics::SharedRegistry::global();

        let (mut channels, engine_handle) = EngineBuilder::new(ctx.clone(), config.clone())
            .with_custom_wal(wal_ref)
            .with_custom_network(net_ref, tx_network)
            .with_default_consensus(ConsensusContext::new(
                address,
                Box::new(app.get_signing_provider(private_key)),
            ))
            .with_default_sync(SyncContext::new(JsonCodec))
            .with_default_request(RequestContext::new(100))
            .build()
            .await?;

        // Spawn the app handler task (same as real App::start)
        let tx_event = channels.events.clone();
        let app_handle = tokio::spawn(async move {
            app.run(channels).await.unwrap();
        });

        Ok(SimulatedNodeHandle {
            engine: engine_handle,
            tx_event,
            app_handle,
        })
    }

    async fn reset_db(&self, id: NodeId) -> eyre::Result<()> {
        let stores = self.wal_stores.lock().unwrap();
        if let Some(store) = stores.get(&id) {
            let mut store = store.lock().unwrap();
            store.clear();
        }
        Ok(())
    }
}

impl SimulatedNodeRunner {
    fn generate_config(&self, node: NodeId) -> Config {
        let mut config = self.generate_default_config(node);
        self.params.apply_to_config(&mut config);

        let node_info = &self.nodes_info[&node];
        (node_info.config_modifier)(&mut config);

        config
    }

    fn generate_default_config(&self, node: NodeId) -> Config {
        use malachitebft_config::*;

        // For simulation, we don't need real network addresses.
        // Use dummy addresses since the real network is bypassed.
        let i = node - 1;
        let base_port = 30_000 + self.id * 1000;

        Config {
            moniker: format!("sim-node-{node}"),
            logging: LoggingConfig::default(),
            consensus: ConsensusConfig {
                enabled: true,
                value_payload: ValuePayload::ProposalAndParts,
                queue_capacity: 100,
                p2p: P2pConfig {
                    protocol: PubSubProtocol::default(),
                    discovery: DiscoveryConfig::default(),
                    listen_addr: TransportProtocol::Tcp
                        .multiaddr("127.0.0.1", base_port + i),
                    persistent_peers: Vec::new(), // No real peers in simulation
                    ..Default::default()
                },
            },
            value_sync: ValueSyncConfig {
                enabled: true,
                status_update_interval: std::time::Duration::from_secs(2),
                request_timeout: std::time::Duration::from_secs(5),
                ..Default::default()
            },
            metrics: MetricsConfig {
                enabled: false,
                listen_addr: format!("127.0.0.1:{}", base_port + 200 + i)
                    .parse()
                    .unwrap(),
            },
            runtime: RuntimeConfig::single_threaded(),
            test: TestConfig::default(),
            byzantine: None,
        }
    }
}

fn temp_dir(id: NodeId) -> PathBuf {
    TempDir::with_prefix(format!("malachitebft-sim-{id}"))
        .unwrap()
        .keep()
}

fn make_validators<S>(
    nodes: &[TestNode<TestContext, S>],
    params: &TestParams,
) -> (Vec<Validator>, HashMap<NodeId, PrivateKey>) {
    // Same logic as the real TestRunner
    let mut rng = StdRng::seed_from_u64(0x42);
    let mut validators = Vec::new();
    let mut private_keys = HashMap::new();

    let sk = PrivateKey::generate(&mut rng);
    for &nid in params.shared_key_group.iter() {
        private_keys.insert(nid, sk.clone());
    }
    let total_power: u64 = params
        .shared_key_group
        .iter()
        .filter_map(|nid| nodes.iter().find(|n| n.id == *nid))
        .map(|n| n.voting_power)
        .sum();
    if total_power > 0 {
        validators.push(Validator::new(sk.public_key(), total_power));
    }

    for node in nodes {
        if params.shared_key_group.contains(&node.id) {
            continue;
        }
        let sk = PrivateKey::generate(&mut rng);
        let val = Validator::new(sk.public_key(), node.voting_power);
        private_keys.insert(node.id, sk);
        if node.voting_power > 0 {
            validators.push(val);
        }
    }

    (validators, private_keys)
}
```

**Note:** This is an initial implementation. The `App::run()` method and exact wiring will need adjustment once we discover the exact API during compilation. The import paths and method signatures will be refined.

**Step 2: Update lib.rs with re-exports**

```rust
pub mod controller;
pub mod fault;
pub mod network;
pub mod runner;
pub mod wal;

pub use fault::{FaultScenario, SimConfig};
pub use runner::{SimulatedNodeHandle, SimulatedNodeRunner};
```

**Step 3: Attempt to compile and fix issues**

Run: `cargo check -p informalsystems-malachitebft-test-dst`
Expected: Likely compilation errors around exact import paths and API signatures. Fix iteratively.

This is the most important task — getting the runner to compile means the entire wiring works. Expect to spend time resolving:
- Exact `App` method signatures for `run()` vs `start()`
- EngineBuilder generic parameter requirements
- TestContext trait implementations
- Config type mismatches

**Step 4: Commit once compiling**

```bash
git add code/crates/test/dst/src/runner.rs code/crates/test/dst/src/lib.rs
git commit -m "feat(test-dst): implement SimulatedNodeRunner with EngineBuilder wiring"
```

---

### Task 7: Write first integration test — basic consensus without faults

Verify that 3 nodes can reach consensus at height 2 using the simulated runner.

**Files:**
- Create: `code/crates/test/dst/tests/basic.rs`

**Step 1: Write the test**

```rust
use std::time::Duration;

use malachitebft_test::TestContext;
use malachitebft_test_dst::{SimulatedNodeRunner, SimConfig};
use malachitebft_test_framework::{HasTestRunner, TestBuilder, TestParams};

impl HasTestRunner<SimulatedNodeRunner> for TestContext {
    type Runner = SimulatedNodeRunner;
}

type SimTestBuilder<S> = TestBuilder<TestContext, S>;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn three_nodes_reach_consensus() {
    const HEIGHT: u64 = 2;

    let mut test = SimTestBuilder::<()>::new();

    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();

    test.build()
        .run::<SimulatedNodeRunner>(Duration::from_secs(60))
        .await;
}
```

Note the `#[tokio::test(flavor = "current_thread", start_paused = true)]` — this ensures single-threaded runtime with paused time.

**Step 2: Run the test**

Run: `cargo test -p informalsystems-malachitebft-test-dst three_nodes_reach_consensus -- --nocapture`
Expected: This will likely fail initially because the simulated network doesn't actually deliver messages yet. The controller's `drain_current_tick` is called but nobody drives the tick loop.

**Step 3: Identify what's needed for message delivery**

The missing piece is the **tick loop** — something needs to call `controller.drain_current_tick()` and deliver messages to the destination nodes' `SimulatedNetwork` actors. This will be addressed in Task 8.

**Step 4: Commit the test (even if not yet passing)**

```bash
git add code/crates/test/dst/tests/basic.rs
git commit -m "test(test-dst): add basic 3-node consensus test (WIP)"
```

---

### Task 8: Implement the tick loop for message delivery

The controller needs a background task that advances time and delivers messages. This is the "heartbeat" of the simulation.

**Files:**
- Modify: `code/crates/test/dst/src/controller.rs`
- Modify: `code/crates/test/dst/src/runner.rs`

**Step 1: Add a tick loop task to the controller**

Add a method that spawns a tokio task to drive the simulation:

```rust
impl<Ctx: Context> SimulationController<Ctx> {
    /// Spawn the tick loop that advances time and delivers messages.
    /// Returns a JoinHandle for the loop.
    pub fn spawn_tick_loop(
        controller: Arc<Mutex<Self>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                // Get tick duration and check for messages
                let (tick_duration, has_messages, next_tick) = {
                    let ctrl = controller.lock().unwrap();
                    (
                        ctrl.tick_duration,
                        ctrl.has_pending_messages(),
                        ctrl.next_message_tick(),
                    )
                };

                // Hybrid scheduling: jump to next event if nothing pending soon
                if let Some(next) = next_tick {
                    let current = controller.lock().unwrap().current_tick;
                    if next > current + 1 {
                        // Jump forward: advance time to just before the next message
                        let ticks_to_skip = next - current - 1;
                        tokio::time::advance(tick_duration * ticks_to_skip as u32).await;
                        controller.lock().unwrap().current_tick = next - 1;
                    }
                }

                // Advance one tick
                tokio::time::advance(tick_duration).await;
                tokio::task::yield_now().await;

                // Drain and deliver messages for this tick
                let deliveries = {
                    let mut ctrl = controller.lock().unwrap();
                    ctrl.advance_tick();
                    ctrl.drain_current_tick()
                };

                for (dest_node, msg) in deliveries {
                    let dest_ref = {
                        let ctrl = controller.lock().unwrap();
                        ctrl.nodes.get(&dest_node).map(|e| e.network_ref.clone())
                    };
                    if let Some(dest_ref) = dest_ref {
                        let _ = dest_ref.cast(msg);
                    }
                }

                // Yield to let actors process delivered messages
                tokio::task::yield_now().await;
            }
        })
    }
}
```

**Step 2: Start the tick loop in SimulatedNodeRunner::spawn()**

In `runner.rs`, after the first node is spawned, start the tick loop:

```rust
// In spawn(), after building the engine:
// Start tick loop if this is the first node
{
    let ctrl = self.controller.lock().unwrap();
    if ctrl.nodes.len() == 1 {
        drop(ctrl);
        SimulationController::spawn_tick_loop(Arc::clone(&self.controller));
    }
}
```

**Step 3: Run the integration test again**

Run: `cargo test -p informalsystems-malachitebft-test-dst three_nodes_reach_consensus -- --nocapture`
Expected: This should now make progress — messages flow between nodes. May still fail due to missing message type conversion (the controller delivers `NetworkMsg` but subscribers expect `NetworkEvent`).

**Step 4: Fix message type conversion**

The critical issue: when the controller delivers a `NetworkMsg::PublishConsensusMsg(msg)` to the destination node's `SimulatedNetwork`, the destination needs to convert it into a `NetworkEvent::Vote/Proposal/etc` and deliver it to subscribers. This conversion logic is what the real network actor does internally after decoding bytes.

Add a conversion function to `network.rs`:

```rust
/// Convert an outbound NetworkMsg from a source node into the NetworkEvent
/// that destination subscribers should receive.
fn outbound_to_inbound<Ctx: Context>(
    source_peer: PeerId,
    msg: NetworkMsg<Ctx>,
) -> Option<NetworkEvent<Ctx>> {
    match msg {
        NetworkMsg::PublishConsensusMsg(signed_msg) => {
            // Determine if it's a vote or proposal
            use malachitebft_core_consensus::SignedConsensusMsg;
            match signed_msg {
                SignedConsensusMsg::Vote(vote) => {
                    Some(NetworkEvent::Vote(source_peer, vote))
                }
                SignedConsensusMsg::Proposal(proposal) => {
                    Some(NetworkEvent::Proposal(source_peer, proposal))
                }
            }
        }
        NetworkMsg::PublishProposalPart(part) => {
            Some(NetworkEvent::ProposalPart(source_peer, part))
        }
        NetworkMsg::PublishLivenessMsg(liveness) => {
            use malachitebft_core_consensus::LivenessMsg;
            match liveness {
                LivenessMsg::PolkaCertificate(cert) => {
                    Some(NetworkEvent::PolkaCertificate(source_peer, cert))
                }
                LivenessMsg::RoundCertificate(cert) => {
                    Some(NetworkEvent::RoundCertificate(source_peer, cert))
                }
            }
        }
        NetworkMsg::BroadcastStatus(status) => {
            Some(NetworkEvent::Status(source_peer, status))
        }
        _ => None,
    }
}
```

Then update `SimulatedNetwork::handle` to process delivered messages by calling `deliver_to_subscribers` when it receives a message tagged for inbound delivery.

**Step 5: Commit**

```bash
git add code/crates/test/dst/src/controller.rs code/crates/test/dst/src/network.rs code/crates/test/dst/src/runner.rs
git commit -m "feat(test-dst): implement tick loop and message type conversion for delivery"
```

---

### Task 9: Debug and stabilize basic consensus test

This task is iterative. Run the test, read errors, fix them.

**Files:**
- Modify: various files in `code/crates/test/dst/src/`

**Step 1: Run with full tracing**

Run: `RUST_LOG=debug cargo test -p informalsystems-malachitebft-test-dst three_nodes_reach_consensus -- --nocapture 2>&1 | head -200`

**Step 2: Fix issues iteratively**

Common issues to expect:
1. **PeerConnected events not fired** — the simulated network needs to emit `NetworkEvent::PeerConnected` for each peer during startup, so the Consensus actor knows about peers
2. **Missing Listening event** — emit `NetworkEvent::Listening(addr)` on startup
3. **Message delivery timing** — the tick loop may need to yield more aggressively
4. **WAL path issues** — the EngineBuilder may still try to create WAL directories even with custom WAL
5. **Sync actor issues** — the real Sync actor expects network events; ensure they're delivered

**Step 3: Add peer connection simulation**

After all nodes are spawned, fire `PeerConnected` events:

```rust
// After all nodes are registered in the controller:
for node_id in all_node_ids {
    let other_peers: Vec<PeerId> = /* peers of other nodes */;
    for peer in other_peers {
        // Deliver PeerConnected event to this node's subscribers
        sim_network.deliver_event(NetworkEvent::PeerConnected(peer));
    }
}
```

**Step 4: Run test until it passes**

Run: `cargo test -p informalsystems-malachitebft-test-dst three_nodes_reach_consensus -- --nocapture`
Expected: 3 nodes reach height 2 and test passes

**Step 5: Commit**

```bash
git add code/crates/test/dst/
git commit -m "feat(test-dst): stabilize basic 3-node consensus in simulation"
```

---

### Task 10: Add fault injection tests

Now that basic consensus works, add tests with faults.

**Files:**
- Create: `code/crates/test/dst/tests/faults.rs`

**Step 1: Write partition test**

```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn consensus_survives_minority_partition() {
    // 4 nodes, partition 1 node away. The remaining 3 (>2/3) should still make progress.
    const HEIGHT: u64 = 3;

    let mut test = SimTestBuilder::<()>::new();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();

    let config = SimConfig::new()
        .with_seed(42)
        .with_fault(FaultScenario::Partition {
            group_a: vec![4.into()],       // Node 4 isolated
            group_b: vec![1.into(), 2.into(), 3.into()],
            start_tick: 0,
            duration_ticks: 10000,
        });

    test.build()
        .run_simulated(Duration::from_secs(60), config)
        .await;
}
```

**Step 2: Write crash + recovery test**

```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn node_recovers_from_crash() {
    const HEIGHT: u64 = 5;

    let mut test = SimTestBuilder::<()>::new();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node()
        .start()
        .crash_after(Duration::from_secs(2))
        .restart_after(Duration::from_secs(1))
        .wait_until(HEIGHT)
        .success();

    test.build()
        .run::<SimulatedNodeRunner>(Duration::from_secs(60))
        .await;
}
```

**Step 3: Run tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst -- --nocapture`
Expected: Both tests pass

**Step 4: Commit**

```bash
git add code/crates/test/dst/tests/faults.rs
git commit -m "test(test-dst): add partition and crash recovery simulation tests"
```

---

### Task 11: Add `run_simulated` method to Test struct

To support the `SimConfig`-based API, add a convenience method to the test framework.

**Files:**
- Modify: `code/crates/test/dst/src/lib.rs`

**Step 1: Add a run_simulated extension**

Since we don't want to modify the existing `Test` struct, add a free function:

```rust
/// Run a test with the simulated runner and fault configuration.
pub async fn run_simulated<S>(
    test: malachitebft_test_framework::Test<TestContext, S>,
    timeout: std::time::Duration,
    config: SimConfig,
    params: TestParams,
) where
    S: Send + Sync + 'static,
{
    // The SimConfig is passed to the runner via a thread-local or similar mechanism.
    // Alternatively, we can set it on the controller after construction.
    //
    // For now, use TestParams with the SimConfig::seed
    // and inject faults after runner creation.
    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, S>(
        test, timeout, params,
    )
    .await
}
```

This needs refinement — the challenge is passing `SimConfig` to the `SimulatedNodeRunner::new()` method, which only receives `TestParams`. Options:
1. Add a `SimConfig` field to `TestParams` (modifies existing code — not desired)
2. Use a thread-local to pass SimConfig
3. Add the `SimConfig` data as a static in the DST crate

Use option 2 (thread-local) for now:

```rust
use std::cell::RefCell;

thread_local! {
    static SIM_CONFIG: RefCell<Option<SimConfig>> = RefCell::new(None);
}

pub fn set_sim_config(config: SimConfig) {
    SIM_CONFIG.with(|c| *c.borrow_mut() = Some(config));
}

pub fn take_sim_config() -> Option<SimConfig> {
    SIM_CONFIG.with(|c| c.borrow_mut().take())
}
```

Then in `SimulatedNodeRunner::new()`:
```rust
let sim_config = crate::take_sim_config().unwrap_or_default();
let controller = SimulationController::new(sim_config.seed, sim_config.tick_duration);
controller.faults = sim_config.faults;
```

**Step 2: Commit**

```bash
git add code/crates/test/dst/src/lib.rs
git commit -m "feat(test-dst): add run_simulated convenience function with SimConfig passing"
```

---

### Task 12: Seed-based exploration test

Write a test that runs the same scenario with multiple seeds to exercise different schedules.

**Files:**
- Create: `code/crates/test/dst/tests/exploration.rs`

**Step 1: Write seed exploration test**

```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn explore_schedules_with_seeds() {
    for seed in 0..10 {
        let mut test = SimTestBuilder::<()>::new();
        test.add_node().start().wait_until(2).success();
        test.add_node().start().wait_until(2).success();
        test.add_node().start().wait_until(2).success();

        set_sim_config(SimConfig::new().with_seed(seed));

        test.build()
            .run::<SimulatedNodeRunner>(Duration::from_secs(30))
            .await;
    }
}
```

**Step 2: Run and verify**

Run: `cargo test -p informalsystems-malachitebft-test-dst explore_schedules -- --nocapture`
Expected: All 10 seeds pass

**Step 3: Commit**

```bash
git add code/crates/test/dst/tests/exploration.rs
git commit -m "test(test-dst): add seed-based schedule exploration test"
```

---

### Task 13: Add invariant checking

Add safety and liveness assertions that run across all nodes after the simulation.

**Files:**
- Create: `code/crates/test/dst/src/invariants.rs`
- Modify: `code/crates/test/dst/src/lib.rs`

**Step 1: Implement safety invariant**

```rust
/// Safety: no two honest nodes decided different values at the same height.
/// This is checked by collecting all Decided events and comparing value_ids per height.
pub fn check_safety<Ctx: Context>(events: &[(NodeId, Vec<Event<Ctx>>)]) -> Result<(), String> {
    let mut decisions: HashMap<Ctx::Height, HashMap<NodeId, ValueId>> = HashMap::new();

    for (node_id, node_events) in events {
        for event in node_events {
            if let Event::Decided { commit_certificate, .. } = event {
                let height = commit_certificate.height();
                let value_id = commit_certificate.value_id();
                decisions.entry(height).or_default().insert(*node_id, value_id);
            }
        }
    }

    for (height, node_decisions) in &decisions {
        let values: HashSet<_> = node_decisions.values().collect();
        if values.len() > 1 {
            return Err(format!(
                "Safety violation at height {height}: nodes decided different values: {node_decisions:?}"
            ));
        }
    }

    Ok(())
}
```

**Step 2: Commit**

```bash
git add code/crates/test/dst/src/invariants.rs code/crates/test/dst/src/lib.rs
git commit -m "feat(test-dst): add safety invariant checker"
```

---

### Summary of tasks

| Task | Component | Description |
|------|-----------|-------------|
| 1 | Crate skeleton | Create `test/dst` crate with Cargo.toml and stub files |
| 2 | FaultScenario | Implement fault types and SimConfig builder |
| 3 | SimulatedWal | In-memory WAL actor with crash simulation |
| 4 | SimulationController | Central scheduler with message routing and fault checking |
| 5 | SimulatedNetwork | Network actor that routes through controller |
| 6 | SimulatedNodeRunner | NodeRunner impl wiring simulated actors via EngineBuilder |
| 7 | Basic test | First 3-node consensus test |
| 8 | Tick loop | Background task driving time and message delivery |
| 9 | Stabilization | Debug and fix until basic test passes |
| 10 | Fault tests | Partition and crash recovery tests |
| 11 | run_simulated | Convenience API for SimConfig-based runs |
| 12 | Seed exploration | Multi-seed schedule exploration test |
| 13 | Invariants | Safety and liveness invariant checking |

Tasks 1-6 are foundation. Task 7-9 are the critical integration milestone. Tasks 10-13 build on the working foundation.
