use std::time::Duration;

use informalsystems_malachitebft_test_dst::runner::SimulatedNodeRunner;
use malachitebft_test::TestContext;
use malachitebft_test_framework::{TestBuilder, TestParams};

type DstTestBuilder<S> = TestBuilder<TestContext, S>;

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_reach_consensus() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();

    const HEIGHT: u64 = 2;

    let mut test = DstTestBuilder::<()>::new();

    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();
    test.add_node().start().wait_until(HEIGHT).success();

    malachitebft_test_framework::run_test::<SimulatedNodeRunner, TestContext, ()>(
        test.build(),
        Duration::from_secs(60),
        TestParams::default(),
    )
    .await;
}
