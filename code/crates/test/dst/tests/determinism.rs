use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{error_span, Instrument};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;

use informalsystems_malachitebft_test_dst::runner::SimulatedNodeRunner;
use malachitebft_test::TestContext;
use malachitebft_test_framework::{run_node, NodeRunner, TestBuilder, TestNode, TestParams};

type DstTestBuilder = TestBuilder<TestContext, ()>;

/// A shared buffer for capturing tracing output. Implements `MakeWriter`
/// so it can be used with `tracing_subscriber::fmt::Layer::with_writer`.
#[derive(Clone)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn into_string(self) -> String {
        let bytes = Arc::try_unwrap(self.0).unwrap().into_inner().unwrap();
        String::from_utf8(bytes).unwrap()
    }
}

/// A writer returned by `MakeWriter::make_writer` that appends to the shared buffer.
struct SharedBufferWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedBuffer {
    type Writer = SharedBufferWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedBufferWriter(Arc::clone(&self.0))
    }
}

/// Strip the leading timestamp from a tracing log line.
///
/// The default `tracing_subscriber::fmt` format produces lines like:
///   2026-02-22T12:34:56.789012Z  INFO ...
fn strip_timestamp(line: &str) -> &str {
    if let Some(z_pos) = line.find('Z') {
        let rest = &line[z_pos + 1..];
        rest.trim_start()
    } else {
        line
    }
}

/// Strip ractor actor IDs (e.g., `actor=0.15`) which are globally incrementing
/// and differ between runs within the same process.
fn strip_actor_ids(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(pos) = rest.find("actor=") {
        result.push_str(&rest[..pos]);
        result.push_str("actor=X");
        let after = &rest[pos + 6..]; // skip "actor="
        let end = after
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(after.len());
        rest = &after[end..];
    }
    result.push_str(rest);
    result
}

/// Keywords that identify consensus-relevant log messages.
/// These represent the core state transitions and decisions that must be
/// deterministic across runs with the same seed.
const CONSENSUS_KEYWORDS: &[&str] = &[
    "Starting new height",
    "Starting new round",
    "Proposing",
    "Voting",
    "Decided",
    "committing",
    "Test succeeded",
    "Test failed",
];

fn is_consensus_relevant(line: &str) -> bool {
    CONSENSUS_KEYWORDS.iter().any(|kw| line.contains(kw))
}

/// Extract and normalize consensus-relevant log lines from raw tracing output.
///
/// Filters to only lines containing consensus state transition keywords,
/// strips timestamps and ractor actor IDs, and sorts to account for
/// tokio task scheduling non-determinism.
fn extract_consensus_logs(raw: &str) -> Vec<String> {
    let mut lines: Vec<String> = raw
        .lines()
        .map(|line| {
            let line = strip_timestamp(line);
            strip_actor_ids(line)
        })
        .filter(|line| is_consensus_relevant(line))
        .collect();
    // Sort to eliminate tokio task scheduling order differences.
    lines.sort();
    lines
}

/// Build test nodes for a 3-node consensus scenario reaching height 2.
fn build_test_nodes() -> Vec<TestNode<TestContext, ()>> {
    let mut test = DstTestBuilder::new();
    test.add_node().start().wait_until(2).success();
    test.add_node().start().wait_until(2).success();
    test.add_node().start().wait_until(2).success();
    test.build().nodes
}

/// Run a 3-node consensus simulation, capturing all tracing output into a buffer.
///
/// Each call creates a fresh tokio runtime so that no state leaks between runs.
/// Returns the consensus-relevant normalized log lines.
fn run_simulation(run_id: usize) -> Vec<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let buffer = SharedBuffer::new();

    let filter = EnvFilter::builder()
        .parse(
            "trace,\
             tokio=warn,\
             ractor=warn,\
             runtime=warn,\
             hyper=warn,\
             mio=warn",
        )
        .unwrap();

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_writer(buffer.clone())
                .with_ansi(false)
                .with_thread_ids(false)
                .with_target(true),
        );

    let _guard = tracing::subscriber::set_default(subscriber);

    rt.block_on(async {
        let nodes = build_test_nodes();
        let params = TestParams::default();

        let runner = SimulatedNodeRunner::new(run_id, &nodes, params);
        let span = error_span!("test", id = run_id);

        let mut set = JoinSet::new();
        for node in nodes {
            let runner = runner.clone();
            set.spawn(
                async move {
                    let id = node.id;
                    let result = timeout(Duration::from_secs(30), run_node(runner, node)).await;
                    (id, result)
                }
                .instrument(span.clone()),
            );
        }

        let results = set.join_all().await;
        for (id, result) in &results {
            match result {
                Ok(malachitebft_test_framework::TestResult::Success(_)) => {}
                Ok(malachitebft_test_framework::TestResult::Failure(reason)) => {
                    panic!("Node {id} failed: {reason}");
                }
                Err(_) => {
                    panic!("Node {id} timed out");
                }
            }
        }
    });

    drop(_guard);
    rt.shutdown_background();

    extract_consensus_logs(&buffer.into_string())
}

/// Verify that two simulation runs with the same seed produce identical
/// consensus state transitions: same proposals, votes, and decisions
/// at each height and round.
#[test]
fn simulation_is_deterministic() {
    let run_a = run_simulation(1);
    let run_b = run_simulation(1);

    assert!(
        !run_a.is_empty(),
        "Expected non-empty consensus log output from simulation"
    );

    if run_a != run_b {
        // Find the first divergence for a helpful error message.
        let max_len = run_a.len().max(run_b.len());
        for i in 0..max_len {
            let a = run_a.get(i).map(String::as_str).unwrap_or("<missing>");
            let b = run_b.get(i).map(String::as_str).unwrap_or("<missing>");
            if a != b {
                panic!(
                    "Consensus logs diverge at line {i} (0-indexed):\n\
                     \n  run_a[{i}]: {a}\
                     \n  run_b[{i}]: {b}\
                     \n\n  Total consensus lines: run_a={}, run_b={}",
                    run_a.len(),
                    run_b.len(),
                );
            }
        }
    }
}
