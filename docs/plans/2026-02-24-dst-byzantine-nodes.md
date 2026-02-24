# DST Byzantine Nodes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable running byzantine nodes in DST by wiring `ByzantineNetworkProxy` and `ByzantineMiddleware` into the simulated node runner, and exposing a `.byzantine()` builder method on `TestNode`.

**Architecture:** When a node's `Config.byzantine` is active, the DST runner inserts a `ByzantineNetworkProxy` actor between the consensus engine and the `SimulatedNetwork`, and wraps the node's middleware with `ByzantineMiddleware` for amnesia attacks. This mirrors the existing wiring in the test app's `App::start()` (`crates/test/app/src/node.rs:139-236`) but uses simulated I/O instead of real networking.

**Tech Stack:** Rust, ractor actors, `malachitebft-engine-byzantine` crate (provides `ByzantineConfig`, `ByzantineNetworkProxy`, `ByzantineMiddleware`, `Trigger`).

---

### Task 1: Add `engine-byzantine` dependency to the framework crate

**Files:**
- Modify: `code/crates/test/framework/Cargo.toml`

**Step 1: Add the dependency**

In `code/crates/test/framework/Cargo.toml`, add to the `[dependencies]` section:

```toml
malachitebft-engine-byzantine.workspace = true
```

**Step 2: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-framework`
Expected: compiles successfully

**Step 3: Commit**

```bash
git add code/crates/test/framework/Cargo.toml
git commit -m "build(test-framework): add engine-byzantine dependency"
```

---

### Task 2: Add `.byzantine()` builder method to `TestNode`

**Files:**
- Modify: `code/crates/test/framework/src/node.rs`

**Step 1: Add import and method**

At the top of `code/crates/test/framework/src/node.rs`, add:

```rust
use malachitebft_engine_byzantine::ByzantineConfig;
```

Then add a new `impl` block after the existing `impl<Ctx, State, Cfg> TestNode<Ctx, State, Cfg> where Cfg: NodeConfig` block (after line 388):

```rust
impl<Ctx, State> TestNode<Ctx, State, TestConfig>
where
    Ctx: Context,
{
    pub fn byzantine(&mut self, config: ByzantineConfig) -> &mut Self {
        self.add_config_modifier(move |cfg| {
            cfg.byzantine = Some(config.clone());
        })
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-framework`
Expected: compiles successfully

**Step 3: Commit**

```bash
git add code/crates/test/framework/src/node.rs
git commit -m "feat(test-framework): add .byzantine() builder method to TestNode"
```

---

### Task 3: Add `engine-byzantine` dependency to the DST crate

**Files:**
- Modify: `code/crates/test/dst/Cargo.toml`

**Step 1: Add the dependency**

In `code/crates/test/dst/Cargo.toml`, add to the `[dependencies]` section:

```toml
malachitebft-engine-byzantine.workspace = true
```

**Step 2: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-dst`
Expected: compiles successfully

**Step 3: Commit**

```bash
git add code/crates/test/dst/Cargo.toml
git commit -m "build(test-dst): add engine-byzantine dependency"
```

---

### Task 4: Wire byzantine behavior in `SimulatedNodeRunner::spawn()`

**Files:**
- Modify: `code/crates/test/dst/src/runner.rs`

This is the core change. The `spawn()` method currently always passes the `SimulatedNetwork` actor directly to the `EngineBuilder`. When `config.byzantine` is active, we need to:

1. Wrap the middleware with `ByzantineMiddleware` if `ignore_locks` is true
2. Spawn a `ByzantineNetworkProxy` in front of the `SimulatedNetwork`
3. Pass the proxy to `EngineBuilder` instead

**Step 1: Add imports**

At the top of `runner.rs`, add:

```rust
use malachitebft_engine_byzantine::{ByzantineMiddleware, ByzantineNetworkProxy};
```

**Step 2: Wrap middleware with ByzantineMiddleware**

In the `spawn()` method, replace the current middleware setup (lines 187-188):

```rust
let middleware: Arc<dyn Middleware> = Arc::clone(&node_info.middleware);
let ctx = TestContext::with_middleware(middleware.clone());
```

With:

```rust
let byzantine_cfg = config.byzantine.clone();

let middleware: Arc<dyn Middleware> = {
    let inner = Arc::clone(&node_info.middleware);
    if let Some(ref byz) = byzantine_cfg {
        if byz.ignore_locks {
            tracing::warn!("BYZANTINE: Amnesia attack enabled (ignoring voting locks)");
            Arc::new(ByzantineMiddleware::new(true, inner))
        } else {
            inner
        }
    } else {
        inner
    }
};
let ctx = TestContext::with_middleware(middleware.clone());
```

**Step 3: Conditionally inject ByzantineNetworkProxy**

Replace the engine builder section (lines 257-268):

```rust
// Build engine with custom network and custom WAL
let builder = EngineBuilder::new(ctx.clone(), config.clone())
    .with_custom_wal(wal_ref)
    .with_custom_network(net_handle.actor_ref.clone(), tx_app_network)
    .with_default_consensus(ConsensusContext::new(
        address,
        Ed25519Provider::new(private_key.clone()),
    ))
    .with_default_sync(SyncContext::new(JsonCodec))
    .with_default_request(RequestContext::new(100));

let (mut channels, engine_handle) = builder.build().await?;
```

With:

```rust
let is_byzantine = byzantine_cfg.as_ref().is_some_and(|c| c.is_active());

let builder = EngineBuilder::new(ctx.clone(), config.clone())
    .with_custom_wal(wal_ref);

let (mut channels, engine_handle) = if is_byzantine {
    let byz_cfg = byzantine_cfg.unwrap();

    tracing::warn!(
        ?byz_cfg,
        "BYZANTINE: Starting node with Byzantine behavior enabled"
    );

    let conflicting_value_fn: Option<
        malachitebft_engine_byzantine::ConflictingValueFn<TestContext>,
    > = Some(Box::new(|original: &malachitebft_test::Value| {
        malachitebft_test::Value::new(original.value.wrapping_add(1))
    }));

    let proxy_ref = ByzantineNetworkProxy::spawn(
        byz_cfg,
        net_handle.actor_ref.clone(),
        Box::new(Ed25519Provider::new(private_key.clone())),
        ctx.clone(),
        address,
        tracing::error_span!("byzantine-proxy", node = id),
        conflicting_value_fn,
    )
    .await?;

    builder
        .with_custom_network(proxy_ref, tx_app_network)
        .with_default_consensus(ConsensusContext::new(
            address,
            Ed25519Provider::new(private_key.clone()),
        ))
        .with_default_sync(SyncContext::new(JsonCodec))
        .with_default_request(RequestContext::new(100))
        .build()
        .await?
} else {
    builder
        .with_custom_network(net_handle.actor_ref.clone(), tx_app_network)
        .with_default_consensus(ConsensusContext::new(
            address,
            Ed25519Provider::new(private_key.clone()),
        ))
        .with_default_sync(SyncContext::new(JsonCodec))
        .with_default_request(RequestContext::new(100))
        .build()
        .await?
};
```

**Step 4: Verify it compiles**

Run: `cargo check -p informalsystems-malachitebft-test-dst`
Expected: compiles successfully

**Step 5: Commit**

```bash
git add code/crates/test/dst/src/runner.rs
git commit -m "feat(test-dst): wire ByzantineNetworkProxy and ByzantineMiddleware in SimulatedNodeRunner"
```

---

### Task 5: Write byzantine DST integration tests

**Files:**
- Create: `code/crates/test/dst/tests/byzantine.rs`

**Step 1: Write test file**

Create `code/crates/test/dst/tests/byzantine.rs`:

```rust
use std::time::Duration;

use informalsystems_malachitebft_test_dst::fault::SimConfig;
use informalsystems_malachitebft_test_dst::runner::SimulatedNodeRunner;
use malachitebft_engine_byzantine::{ByzantineConfig, Trigger};
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

/// 4 nodes: 3 honest + 1 that equivocates votes always.
/// The honest 3/4 majority can still reach consensus.
#[tokio::test(flavor = "multi_thread")]
async fn equivocate_votes_always() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::Always),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(100),
    )
    .await;
}

/// 4 nodes: 3 honest + 1 that drops all proposals.
/// The honest majority can still decide (they just won't
/// decide on the byzantine node's proposals).
#[tokio::test(flavor = "multi_thread")]
async fn drop_proposals() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            drop_proposals: Some(Trigger::Always),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(101),
    )
    .await;
}

/// 4 nodes: 3 honest + 1 that drops all votes (silence attack).
/// The honest majority proceeds without the silent node's votes.
#[tokio::test(flavor = "multi_thread")]
async fn drop_votes() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            drop_votes: Some(Trigger::Always),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(102),
    )
    .await;
}

/// 4 nodes: 3 honest + 1 byzantine with amnesia (ignores locks).
/// The honest majority can still reach consensus despite the
/// amnesia node voting inconsistently with its locks.
#[tokio::test(flavor = "multi_thread")]
async fn amnesia_attack() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            ignore_locks: true,
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(103),
    )
    .await;
}

/// Combined: network faults + byzantine behavior.
/// 4 nodes, node 4 equivocates, node 1 is partitioned for 10 ticks.
/// After partition heals, honest majority still decides.
#[tokio::test(flavor = "multi_thread")]
async fn byzantine_with_network_faults() {
    init_tracing();

    use informalsystems_malachitebft_test_dst::fault::FaultScenario;

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::Random { probability: 0.5 }),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    let config = SimConfig::new()
        .with_seed(104)
        .with_fault(FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![2, 3, 4],
            start_tick: 0,
            duration_ticks: 10,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}
```

**Step 2: Run the tests**

Run: `cargo test -p informalsystems-malachitebft-test-dst --test byzantine`
Expected: all 5 tests pass

**Step 3: Commit**

```bash
git add code/crates/test/dst/tests/byzantine.rs
git commit -m "test(test-dst): add byzantine node integration tests"
```

---

### Task 6: Update DST README

**Files:**
- Modify: `code/crates/test/dst/README.md`

**Step 1: Add Byzantine Nodes section**

After the existing "Fault Injection" section in the README, add a new section documenting byzantine node support, including a table of behaviors and a usage example.

**Step 2: Update the architecture diagram**

Update the ASCII architecture diagram to show the optional `ByzantineNetworkProxy` in the data flow.

**Step 3: Commit**

```bash
git add code/crates/test/dst/README.md
git commit -m "docs(test-dst): document byzantine node support in DST README"
```

---

### Task 7: Format and lint

**Step 1: Run fmt and clippy**

```bash
cd code && cargo fmt --all && cargo clippy --workspace --lib --examples --tests --benches --all-features -- -D warnings
```

Expected: no errors

**Step 2: Commit if any formatting changes**

```bash
git add -u && git commit -m "style: cargo fmt + clippy"
```
