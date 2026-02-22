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

// ─── Per-fault tests ───────────────────────────────────────────────

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

// ─── Composed fault scenarios ──────────────────────────────────────

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
