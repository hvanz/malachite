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
