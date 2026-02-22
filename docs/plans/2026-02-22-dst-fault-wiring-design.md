# DST Fault Wiring and Testing

## Problem

The DST framework defines six fault types in `FaultScenario` and has controller logic for three of them (Partition, Delay, MessageLoss), but faults are never actually applied because `runner.rs` doesn't transfer `sim_config.faults` to the controller. Additionally, Duplicate and WalFailure have no controller logic yet, and no fault scenario tests exist.

## Decisions

- **CrashAt**: Deferred. The test framework's `Step::Crash`/`Step::Restart` already covers crash scenarios from the test script side. Controller-driven crashes require node lifecycle control that adds significant complexity.
- **Duplicate timing**: Duplicates arrive on the same tick as the original (not delayed).
- **Test depth**: One test per fault type plus 2-3 composed scenarios.

## Design

### 1. Wire faults from SimConfig to Controller

In `runner.rs::with_config()`, after creating the controller, set its `faults` field from `sim_config.faults`. The three faults with existing controller logic (Partition, Delay, MessageLoss) start working immediately.

### 2. Implement Duplicate fault

In `controller.rs::drain_current_tick()`, after deciding a message is not blocked, check active `Duplicate` faults. If the source matches and `rng.gen_bool(duplication_probability)` is true, push the event into the deliveries vec twice. Both copies arrive on the same tick.

### 3. Implement WalFailure fault

The `SimulatedWalStore` already has a `fail_writes: bool` field and the WAL actor checks it on every append/flush. The controller needs access to WAL stores to toggle this flag per tick.

Approach: the runner passes a `HashMap<NodeId, Arc<Mutex<SimulatedWalStore>>>` to the controller at construction time. On each tick (in `drain_current_tick` or a dedicated method called from the tick loop), the controller scans `WalFailure` faults and sets/clears `fail_writes` on matching stores.

### 4. Test suite

All tests in a new `tests/faults.rs` file. Each uses `SimConfig` with a fixed seed, passes through `run_test`, and verifies nodes reach a target height.

**Per-fault tests (5):**

| Test | Fault | Setup |
|------|-------|-------|
| `partition_and_heal` | Partition | 3 nodes; node 1 isolated from {2,3} for 30 ticks; all reach height 2 |
| `delayed_messages` | Delay | 3 nodes; node 1 delayed 5 ticks; all reach height 2 |
| `message_loss` | MessageLoss | 3 nodes; 30% drop on node 1; all reach height 2 |
| `duplicate_messages` | Duplicate | 3 nodes; 50% duplication on node 1; all reach height 2 |
| `wal_failure` | WalFailure | 3 nodes; WAL fails on node 1 for ticks 5-25; all reach height 2 |

**Composed scenarios (3):**

| Test | Faults | Setup |
|------|--------|-------|
| `partition_then_message_loss` | Partition + MessageLoss | Partition ticks 0-15, then 30% loss on node 2 ticks 15-50 |
| `delay_and_duplicate` | Delay + Duplicate | Node 1 delayed 3 ticks + node 2 duplicating 50%, concurrent |
| `all_network_faults` | Partition + Delay + MessageLoss | All active simultaneously on different nodes |

## Files to modify

| File | Change |
|------|--------|
| `dst/src/runner.rs` | Pass `sim_config.faults` to controller; pass WAL stores map to controller |
| `dst/src/controller.rs` | Accept WAL stores; implement Duplicate logic in `drain_current_tick`; add `apply_wal_faults` method |
| `dst/tests/faults.rs` | New file with 8 fault scenario tests |
