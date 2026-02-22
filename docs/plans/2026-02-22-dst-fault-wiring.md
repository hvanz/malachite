# DST Fault Wiring Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire fault injection from SimConfig through to the controller, implement Duplicate and WalFailure fault logic, and add a comprehensive test suite covering all fault types plus composed scenarios.

**Architecture:** The runner passes `sim_config.faults` and WAL stores to the controller at construction. The controller applies network faults (Partition, Delay, MessageLoss, Duplicate) during message delivery and WAL faults during the tick loop. Tests use `run_test` with `SimConfig` containing fault scenarios.

**Tech Stack:** Rust, tokio, ractor, the existing DST crate (`informalsystems-malachitebft-test-dst`)

---

### Task 1: Wire faults from SimConfig to Controller

**Files:**
- Modify: `code/crates/test/dst/src/runner.rs:74-77`

**Step 1: Pass faults to controller**

In `SimulatedNodeRunner::with_config()`, after creating the controller, set its `faults` field. Change lines 74-77 from:

```rust
let controller = Arc::new(Mutex::new(SimulationController::new(
    sim_config.seed,
    sim_config.tick_duration,
)));
```

to:

```rust
let mut ctrl = SimulationController::new(
    sim_config.seed,
    sim_config.tick_duration,
);
ctrl.faults = sim_config.faults;
let controller = Arc::new(Mutex::new(ctrl));
```

**Step 2: Run existing tests to verify no regression**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All 7 tests pass (5 unit + basic + determinism). No behavioral change since `SimConfig::default()` has empty faults.

**Step 3: Commit**

```
feat(test-dst): wire SimConfig faults to SimulationController
```

---

### Task 2: Implement Duplicate fault in controller

**Files:**
- Modify: `code/crates/test/dst/src/controller.rs:180-218`

**Step 1: Write a unit test for Duplicate**

Add to the `#[cfg(test)] mod tests` block in `controller.rs` (after line 291):

```rust
#[test]
fn duplicate_fault_doubles_deliveries() {
    let mut ctrl = SimulationController::<TestContext>::new(42, Duration::from_millis(10));
    ctrl.faults.push(FaultScenario::Duplicate {
        node: 1,
        duplication_probability: 1.0, // always duplicate
        start_tick: 0,
        duration_ticks: 100,
    });

    // Register two fake nodes (we only need entries in the nodes map)
    // We can't easily create full SimulatedNetworkHandle in a unit test,
    // so test the helper method directly instead.
    assert!(ctrl.should_duplicate(1)); // source=1 matches fault
    assert!(!ctrl.should_duplicate(2)); // source=2 does not match
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p informalsystems-malachitebft-test-dst controller::tests::duplicate_fault_doubles_deliveries`
Expected: FAIL — `should_duplicate` method doesn't exist yet.

**Step 3: Add `should_duplicate` method and integrate into `drain_current_tick`**

Add the helper method to `SimulationController` (after `is_blocked`, around line 172):

```rust
fn should_duplicate(&mut self, source: NodeId) -> bool {
    for fault in &self.faults {
        if let FaultScenario::Duplicate {
            node,
            duplication_probability,
            start_tick,
            duration_ticks,
        } = fault
        {
            if *node == source
                && self.current_tick >= *start_tick
                && self.current_tick < start_tick + duration_ticks
                && self.rng.gen_bool(*duplication_probability)
            {
                return true;
            }
        }
    }
    false
}
```

Then modify `drain_current_tick()`. In the broadcast arm (around line 199-205), after pushing a delivery, check for duplication:

```rust
for dest in dest_ids {
    if !self.is_blocked(scheduled.source, dest) {
        deliveries.push((dest, scheduled.event.clone()));
        if self.should_duplicate(scheduled.source) {
            debug!(source = scheduled.source, %dest, "Duplicated message (fault)");
            deliveries.push((dest, scheduled.event.clone()));
        }
    } else {
        debug!(source = scheduled.source, %dest, "Dropped message (fault)");
    }
}
```

In the directed arm (around line 207-213), same pattern:

```rust
Some(dest) => {
    if !self.is_blocked(scheduled.source, dest) {
        let dup = self.should_duplicate(scheduled.source);
        deliveries.push((dest, scheduled.event.clone()));
        if dup {
            debug!(source = scheduled.source, %dest, "Duplicated directed message (fault)");
            deliveries.push((dest, scheduled.event));
        }
    } else {
        debug!(source = scheduled.source, %dest, "Dropped directed message (fault)");
    }
}
```

Note: the directed arm previously moved `scheduled.event` into the push. Now it must clone when duplication is possible (the event is already `Clone` since `NetworkEvent` derives it).

**Step 4: Run test to verify it passes**

Run: `cargo test -p informalsystems-malachitebft-test-dst controller::tests::duplicate_fault_doubles_deliveries`
Expected: PASS

**Step 5: Run all existing tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All pass.

**Step 6: Commit**

```
feat(test-dst): implement Duplicate fault in SimulationController
```

---

### Task 3: Implement WalFailure fault in controller

**Files:**
- Modify: `code/crates/test/dst/src/controller.rs:56-79` (struct + constructor)
- Modify: `code/crates/test/dst/src/controller.rs:238-271` (tick loop)
- Modify: `code/crates/test/dst/src/runner.rs:74-93` (pass WAL stores to controller)

**Step 1: Add WAL stores map to `SimulationController`**

Add a new field to the struct (line 56-65):

```rust
pub struct SimulationController<Ctx: Context> {
    pub seed: u64,
    pub rng: StdRng,
    pub nodes: HashMap<NodeId, SimNodeEntry<Ctx>>,
    message_queue: BinaryHeap<ScheduledMessage<Ctx>>,
    pub faults: Vec<FaultScenario>,
    pub current_tick: u64,
    pub tick_duration: Duration,
    sequence_counter: u64,
    wal_stores: HashMap<NodeId, Arc<Mutex<crate::wal::SimulatedWalStore<Ctx>>>>,
}
```

Update the constructor (line 68-79) to initialize with an empty map:

```rust
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
        wal_stores: HashMap::new(),
    }
}
```

Add a setter method:

```rust
pub fn set_wal_stores(&mut self, stores: HashMap<NodeId, Arc<Mutex<crate::wal::SimulatedWalStore<Ctx>>>>) {
    self.wal_stores = stores;
}
```

**Step 2: Add `apply_wal_faults` method to controller**

Add after `advance_tick` (around line 222):

```rust
/// Toggle WAL `fail_writes` flags based on active WalFailure faults.
pub fn apply_wal_faults(&self) {
    for (node_id, store) in &self.wal_stores {
        let should_fail = self.faults.iter().any(|f| {
            matches!(f, FaultScenario::WalFailure { node, start_tick, duration_ticks }
                if *node == *node_id
                    && self.current_tick >= *start_tick
                    && self.current_tick < start_tick + duration_ticks
            )
        });
        let mut s = store.lock().unwrap();
        if s.fail_writes != should_fail {
            tracing::debug!(node = *node_id, fail = should_fail, tick = self.current_tick, "WAL fault toggle");
        }
        s.fail_writes = should_fail;
    }
}
```

**Step 3: Call `apply_wal_faults` in the tick loop**

In `spawn_tick_loop` (line 251-255), add the call after `advance_tick`:

```rust
let deliveries = {
    let mut ctrl = controller.lock().unwrap();
    ctrl.advance_tick();
    ctrl.apply_wal_faults();
    ctrl.drain_current_tick()
};
```

**Step 4: Pass WAL stores from runner to controller**

In `runner.rs::with_config()`, after building `nodes_info` and before wrapping the controller in `Arc`, collect WAL stores and pass them:

```rust
let wal_stores: HashMap<NodeId, Arc<Mutex<SimulatedWalStore<TestContext>>>> = nodes_info
    .iter()
    .map(|(id, info)| (*id, Arc::clone(&info.wal_store)))
    .collect();
ctrl.set_wal_stores(wal_stores);
let controller = Arc::new(Mutex::new(ctrl));
```

This goes right after `ctrl.faults = sim_config.faults;` (from Task 1) and before `let controller = Arc::new(...)`.

**Step 5: Run all existing tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All pass. No behavioral change because no test uses WalFailure faults yet.

**Step 6: Commit**

```
feat(test-dst): implement WalFailure fault in SimulationController
```

---

### Task 4: Add per-fault integration tests

**Files:**
- Create: `code/crates/test/dst/tests/faults.rs`

**Step 1: Create the test file with helper and all 5 per-fault tests**

Create `code/crates/test/dst/tests/faults.rs`:

```rust
use std::time::Duration;

use informalsystems_malachitebft_test_dst::fault::{FaultScenario, SimConfig};
use informalsystems_malachitebft_test_dst::runner::SimulatedNodeRunner;
use malachitebft_test::TestContext;
use malachitebft_test_framework::{TestBuilder, TestParams};

type DstTestBuilder = TestBuilder<TestContext, ()>;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();
}

fn build_3_node_test(height: u64) -> DstTestBuilder {
    let mut test = DstTestBuilder::new();
    test.add_node().start().wait_until(height).success();
    test.add_node().start().wait_until(height).success();
    test.add_node().start().wait_until(height).success();
    test
}

/// Partition node 1 from {2, 3} for 30 ticks, then heal.
/// With 3 nodes, nodes 2 and 3 form a 2/3 majority and can still decide.
/// After the partition heals, node 1 catches up.
#[tokio::test(flavor = "multi_thread")]
async fn partition_and_heal() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(1)
        .with_fault(FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![2, 3],
            start_tick: 0,
            duration_ticks: 30,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Delay node 1's messages by 5 ticks.
/// Consensus is slower but all nodes eventually decide.
#[tokio::test(flavor = "multi_thread")]
async fn delayed_messages() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(2)
        .with_fault(FaultScenario::Delay {
            node: 1,
            delay_ticks: 5,
            start_tick: 0,
            duration_ticks: 200,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Drop 30% of node 1's outbound messages.
/// Consensus tolerates partial message loss.
#[tokio::test(flavor = "multi_thread")]
async fn message_loss() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(3)
        .with_fault(FaultScenario::MessageLoss {
            node: 1,
            drop_probability: 0.3,
            start_tick: 0,
            duration_ticks: 200,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Duplicate 50% of node 1's outbound messages.
/// Consensus must handle duplicate votes/proposals idempotently.
#[tokio::test(flavor = "multi_thread")]
async fn duplicate_messages() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(4)
        .with_fault(FaultScenario::Duplicate {
            node: 1,
            duplication_probability: 0.5,
            start_tick: 0,
            duration_ticks: 200,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// WAL writes fail on node 1 for ticks 5-25.
/// Node 1 may not be able to persist state, but nodes 2 and 3
/// form a majority and drive consensus forward.
#[tokio::test(flavor = "multi_thread")]
async fn wal_failure() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(5)
        .with_fault(FaultScenario::WalFailure {
            node: 1,
            start_tick: 5,
            duration_ticks: 20,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}
```

**Step 2: Run the new tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst --test faults`
Expected: All 5 pass. The first 3 (partition, delay, message_loss) work because the controller logic was already implemented; only the wiring was missing (fixed in Task 1). `duplicate_messages` works because of Task 2. `wal_failure` works because of Task 3.

**Step 3: Commit**

```
test(test-dst): add per-fault integration tests for all 5 fault types
```

---

### Task 5: Add composed fault scenario tests

**Files:**
- Modify: `code/crates/test/dst/tests/faults.rs` (append)

**Step 1: Add 3 composed scenario tests**

Append to `faults.rs`:

```rust
/// Sequential faults: partition for ticks 0-15, then message loss on a different
/// node for ticks 15-50. Tests that consensus recovers across fault transitions.
#[tokio::test(flavor = "multi_thread")]
async fn partition_then_message_loss() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(10)
        .with_fault(FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![2, 3],
            start_tick: 0,
            duration_ticks: 15,
        })
        .with_fault(FaultScenario::MessageLoss {
            node: 2,
            drop_probability: 0.3,
            start_tick: 15,
            duration_ticks: 35,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Concurrent faults: node 1 delayed by 3 ticks while node 2 duplicates 50%
/// of its messages. Tests that the system handles mixed fault conditions.
#[tokio::test(flavor = "multi_thread")]
async fn delay_and_duplicate() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(11)
        .with_fault(FaultScenario::Delay {
            node: 1,
            delay_ticks: 3,
            start_tick: 0,
            duration_ticks: 200,
        })
        .with_fault(FaultScenario::Duplicate {
            node: 2,
            duplication_probability: 0.5,
            start_tick: 0,
            duration_ticks: 200,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Stress test: partition + delay + message loss all active simultaneously
/// on different nodes. Validates consensus under compound adversarial conditions.
#[tokio::test(flavor = "multi_thread")]
async fn all_network_faults() {
    init_tracing();

    let config = SimConfig::new()
        .with_seed(12)
        .with_fault(FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![3],
            start_tick: 0,
            duration_ticks: 10,
        })
        .with_fault(FaultScenario::Delay {
            node: 2,
            delay_ticks: 3,
            start_tick: 0,
            duration_ticks: 100,
        })
        .with_fault(FaultScenario::MessageLoss {
            node: 3,
            drop_probability: 0.2,
            start_tick: 10,
            duration_ticks: 100,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        build_3_node_test(2).build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}
```

**Step 2: Run all fault tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst --test faults`
Expected: All 8 pass.

**Step 3: Run the full DST test suite**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All tests pass (unit + basic + determinism + faults).

**Step 4: Commit**

```
test(test-dst): add composed fault scenario tests
```

---

### Task 6: Format, lint, and final verification

**Files:** None (verification only)

**Step 1: Format**

Run: `cd code && cargo fmt --all`

**Step 2: Clippy**

Run: `cargo clippy -p informalsystems-malachitebft-test-dst --all-features --all-targets -- -D warnings`
Expected: No warnings.

**Step 3: Full DST test suite**

Run: `cargo test -p informalsystems-malachitebft-test-dst`
Expected: All tests pass.

**Step 4: Commit any formatting changes**

If fmt made changes:
```
chore: format
```
