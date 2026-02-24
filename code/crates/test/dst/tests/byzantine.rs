use std::time::Duration;

use informalsystems_malachitebft_test_dst::fault::{FaultScenario, SimConfig};
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

// ─── Proposal equivocation ────────────────────────────────────────

/// 4 nodes: 3 honest + 1 that sends conflicting proposals always.
/// The byzantine node proposes two different values for the same height/round.
/// The honest majority ignores the duplicate and still decides.
#[tokio::test(flavor = "multi_thread")]
async fn equivocate_proposals_always() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_proposals: Some(Trigger::Always),
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
        SimConfig::new().with_seed(105),
    )
    .await;
}

// ─── Trigger varieties ────────────────────────────────────────────

/// Byzantine behavior activated only at specific heights.
/// Node 4 equivocates votes only at heights 1 and 2, then behaves honestly.
#[tokio::test(flavor = "multi_thread")]
async fn equivocate_at_specific_heights() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(5).success();
    test.add_node().start().wait_until(5).success();
    test.add_node().start().wait_until(5).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::AtHeights {
                heights: vec![1, 2],
            }),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(5)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(106),
    )
    .await;
}

/// Byzantine behavior activated within a height range.
/// Node 4 drops proposals during heights 2-3, then stops.
#[tokio::test(flavor = "multi_thread")]
async fn drop_proposals_in_height_range() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(5).success();
    test.add_node().start().wait_until(5).success();
    test.add_node().start().wait_until(5).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            drop_proposals: Some(Trigger::HeightRange { from: 2, to: 3 }),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(5)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(107),
    )
    .await;
}

// ─── Multiple byzantine nodes ─────────────────────────────────────

/// 7 nodes: 5 honest + 2 byzantine (one equivocates, one drops votes).
/// With f < n/3, 2 out of 7 is within tolerance.
#[tokio::test(flavor = "multi_thread")]
async fn two_byzantine_nodes() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    // 5 honest nodes
    for _ in 0..5 {
        test.add_node().start().wait_until(3).success();
    }

    // Byzantine node 6: equivocates votes
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::Always),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    // Byzantine node 7: drops all votes (silent)
    test.add_node()
        .byzantine(ByzantineConfig {
            drop_votes: Some(Trigger::Always),
            seed: Some(43),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        SimConfig::new().with_seed(108),
    )
    .await;
}

// ─── Full Byzantine attack ────────────────────────────────────────

/// A node doing everything bad at once: equivocating votes and proposals,
/// plus amnesia (ignoring locks). The honest 3/4 majority still decides.
#[tokio::test(flavor = "multi_thread")]
async fn full_byzantine_attack() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::Always),
            equivocate_proposals: Some(Trigger::Always),
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
        SimConfig::new().with_seed(109),
    )
    .await;
}

// ─── Combined byzantine + network faults ──────────────────────────

/// Byzantine equivocation combined with message loss on an honest node.
/// Tests that the protocol tolerates both content-level and delivery-level faults.
#[tokio::test(flavor = "multi_thread")]
async fn byzantine_with_message_loss() {
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

    let config = SimConfig::new()
        .with_seed(110)
        .with_fault(FaultScenario::MessageLoss {
            node: 1,
            drop_probability: 0.2,
            start_tick: 0,
            duration_ticks: 200,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Byzantine node with delayed messages: equivocating votes arrive late,
/// testing that honest nodes handle out-of-order conflicting messages.
#[tokio::test(flavor = "multi_thread")]
async fn byzantine_with_delay() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node().start().wait_until(3).success();
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::Always),
            equivocate_proposals: Some(Trigger::Always),
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    let config = SimConfig::new()
        .with_seed(111)
        .with_fault(FaultScenario::Delay {
            node: 4,
            delay_ticks: 5,
            start_tick: 0,
            duration_ticks: 200,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}

/// Stress test: 2 byzantine nodes with different attack patterns,
/// plus network partition and message delay on honest nodes.
#[tokio::test(flavor = "multi_thread")]
async fn byzantine_stress_test() {
    init_tracing();

    let mut test = DstTestBuilder::new();

    // 5 honest nodes
    for _ in 0..5 {
        test.add_node().start().wait_until(3).success();
    }

    // Byzantine node 6: equivocates everything + amnesia
    test.add_node()
        .byzantine(ByzantineConfig {
            equivocate_votes: Some(Trigger::Random { probability: 0.5 }),
            equivocate_proposals: Some(Trigger::Always),
            ignore_locks: true,
            seed: Some(42),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    // Byzantine node 7: drops votes and proposals intermittently
    test.add_node()
        .byzantine(ByzantineConfig {
            drop_votes: Some(Trigger::Random { probability: 0.5 }),
            drop_proposals: Some(Trigger::Random { probability: 0.5 }),
            seed: Some(43),
            ..Default::default()
        })
        .start()
        .wait_until(3)
        .success();

    let config = SimConfig::new()
        .with_seed(112)
        .with_fault(FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![3, 4],
            start_tick: 0,
            duration_ticks: 10,
        })
        .with_fault(FaultScenario::Delay {
            node: 2,
            delay_ticks: 3,
            start_tick: 0,
            duration_ticks: 100,
        });

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
        config,
    )
    .await;
}
