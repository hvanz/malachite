//! Fetch and display consensus heights from Prometheus metrics endpoints.

use std::time::Duration;

use color_eyre::eyre::{self, bail, Result};

/// Fetch the current consensus height from a Prometheus metrics endpoint.
pub(crate) async fn fetch_height(client: &reqwest::Client, url: &str) -> Result<u64> {
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

/// Fetch the latest height of each node in parallel.
/// Returns a list of (name, height_string) pairs.
pub(crate) async fn latest_heights(
    node_urls: &[(String, String)],
) -> Result<Vec<(String, String)>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;

    let mut handles = Vec::new();
    for (name, metrics_url) in node_urls {
        let client = client.clone();
        let name = name.clone();
        let url = format!("{metrics_url}/metrics");
        handles.push(tokio::spawn(async move {
            let result = fetch_height(&client, &url)
                .await
                .map(|h| h.to_string())
                .unwrap_or_else(error_to_string);
            (name, result)
        }));
    }

    let mut heights = Vec::new();
    for handle in handles {
        heights.push(handle.await?);
    }
    Ok(heights)
}

/// Print node heights in a loop, polling every second.
/// If `max_rounds` is 0, loops forever (until interrupted).
pub(crate) async fn loop_print_latest_heights(
    node_urls: &[(String, String)],
    max_rounds: u32,
) -> Result<()> {
    // Print header
    for (name, _) in node_urls {
        print!("{name:>12} | ");
    }
    println!();

    let mut rounds = 0u32;
    loop {
        let heights = latest_heights(node_urls).await?;
        let highest = heights
            .iter()
            .filter_map(|(_, h)| h.parse::<u64>().ok())
            .max()
            .unwrap_or(0);

        for (_, height_str) in &heights {
            // For lagging nodes, print height in red
            let height = height_str.parse::<u64>().unwrap_or(0);
            if height == highest || height == highest - 1 {
                print!("{height_str:>12} | ");
            } else {
                print!("\x1b[31m{height_str:>12}\x1b[0m | ");
            }
        }
        println!();

        rounds += 1;
        if max_rounds > 0 && rounds >= max_rounds {
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

/// Reduce known error messages to short strings for readable tabular output.
fn error_to_string(error: eyre::Report) -> String {
    let err = error.root_cause().to_string().replace('\n', "  ");
    match err.as_str() {
        "operation timed out" => "timeout".to_string(),
        "Connection reset by peer (os error 54)" => "conn reset".to_string(),
        "Connection refused (os error 61)" => "conn refused".to_string(),
        "connection refused" => "conn refused".to_string(),
        "connection reset by peer" => "conn reset".to_string(),
        "connection timed out" => "conn timed out".to_string(),
        "connection timeout" => "conn timeout".to_string(),
        "connection reset" => "conn reset".to_string(),
        "connection closed before message completed" => "conn closed".to_string(),
        "connection was not ready" => "c. not ready".to_string(),
        _ if err.starts_with("HTTP status server error (502 Bad Gateway)") => {
            "bad gateway".to_string()
        }
        _ => err,
    }
}
