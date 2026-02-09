//! Wait for nodes to reach a specific consensus height by polling Prometheus metrics.

use std::time::Duration;

use color_eyre::eyre::{bail, Result};
use tokio::time::Instant;
use tracing::debug;

/// Wait for all given nodes to reach a certain consensus height.
pub(crate) async fn wait_for_height(
    node_urls: Vec<(String, String)>, // (name, metrics_url)
    target_height: u64,
    timeout: Duration,
) -> Result<()> {
    let mut handles = Vec::new();

    let num_nodes = node_urls.len();
    debug!("Waiting for {num_nodes} nodes to reach height {target_height}...");

    for (name, url) in node_urls {
        handles.push(tokio::spawn(async move {
            wait_for_node_height(name, url, target_height, timeout).await
        }));
    }

    for handle in handles {
        handle.await??;
    }

    debug!("All {num_nodes} nodes reached height {target_height}");
    Ok(())
}

/// Wait until a node reaches the given height by polling its Prometheus metrics.
async fn wait_for_node_height(
    node: String,
    metrics_url: String,
    target_height: u64,
    timeout: Duration,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let url = format!("{metrics_url}/metrics");
    let mut last_height = 0u64;
    let mut last_progress = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        match fetch_height(&client, &url).await {
            Ok(height) => {
                if height >= target_height {
                    debug!("{node} reached height {height}");
                    return Ok(());
                }
                if height > last_height {
                    last_height = height;
                    last_progress = Instant::now();
                }
            }
            Err(e) => {
                debug!("{node}: failed to fetch metrics: {e}");
            }
        }

        if last_progress.elapsed() > timeout {
            bail!(
                "Timeout waiting for {node} to reach height {target_height} (stuck at {last_height})"
            );
        }
    }
}

/// Wait for all given nodes to be in sync (all reporting the same height and advancing).
#[allow(dead_code)]
pub(crate) async fn wait_for_sync(
    node_urls: Vec<(String, String)>,
    timeout: Duration,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let start = Instant::now();
    let mut last_progress = Instant::now();
    let mut prev_min_height = 0u64;

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut heights = Vec::new();
        for (name, metrics_url) in &node_urls {
            let url = format!("{metrics_url}/metrics");
            match fetch_height(&client, &url).await {
                Ok(h) => heights.push((name.clone(), h)),
                Err(e) => {
                    debug!("{name}: failed to fetch metrics: {e}");
                    continue;
                }
            }
        }

        if heights.len() == node_urls.len() {
            let min_height = heights.iter().map(|(_, h)| *h).min().unwrap_or(0);
            let max_height = heights.iter().map(|(_, h)| *h).max().unwrap_or(0);

            // Consider synced when all nodes are at the same height and advancing
            if min_height == max_height && min_height > 0 {
                debug!("All nodes synced at height {min_height}");
                return Ok(());
            }

            if min_height > prev_min_height {
                prev_min_height = min_height;
                last_progress = Instant::now();
            }
        }

        if start.elapsed() > timeout || last_progress.elapsed() > timeout {
            bail!("Timeout waiting for nodes to sync");
        }
    }
}

/// Fetch the current consensus height from a Prometheus metrics endpoint.
async fn fetch_height(client: &reqwest::Client, url: &str) -> Result<u64> {
    let body = client.get(url).send().await?.text().await?;

    // Parse the Prometheus text format for the height gauge.
    // Looking for a line like:
    //   malachitebft_core_consensus_height 42
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        // Match lines that contain the height metric (with or without labels)
        if line.starts_with("malachitebft_core_consensus_height") {
            // The value is the last whitespace-separated token
            if let Some(value_str) = line.rsplit_once(|c: char| c.is_whitespace()) {
                if let Ok(height) = value_str.1.parse::<f64>() {
                    return Ok(height as u64);
                }
            }
        }
    }

    bail!("Height metric not found in Prometheus output")
}
