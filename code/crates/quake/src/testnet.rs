//! Core testnet orchestration.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use color_eyre::eyre::{bail, Context, Result};
use tracing::{debug, info, warn};

use crate::build;
use crate::docker;
use crate::manifest::Manifest;
use crate::node::NodesMetadata;
use crate::perturb::Perturbation;
use crate::setup;
use crate::wait;
use crate::InfoSubcommand;

const QUAKE_DIR: &str = ".quake";
const LAST_MANIFEST_FILENAME: &str = ".last_manifest";
const COMPOSE_FILENAME: &str = "compose.yaml";
const MONITORING_DIR: &str = "monitoring";

/// Main testnet orchestration struct.
pub(crate) struct Testnet {
    /// Root directory of the repository (where .quake/ lives).
    pub root_dir: PathBuf,
    /// Quake directory: .quake/
    pub quake_dir: PathBuf,
    /// Testnet data directory: .quake/<name>/
    pub dir: PathBuf,
    /// Path to the compose file.
    pub compose_path: PathBuf,
    /// Path to the monitoring directory.
    pub monitoring_dir: PathBuf,
    /// Path to the monitoring compose file.
    pub monitoring_compose_path: PathBuf,
    /// Parsed manifest.
    pub manifest: Manifest,
    /// Node metadata.
    pub nodes_metadata: NodesMetadata,
}

impl Testnet {
    /// Load a testnet from a manifest file, or from the last-used manifest.
    pub fn load(manifest_file: &Option<PathBuf>) -> Result<Self> {
        let root_dir = std::env::current_dir().context("Failed to get current directory")?;
        let quake_dir = root_dir.join(QUAKE_DIR);

        let manifest_path = if let Some(path) = manifest_file {
            path.clone()
        } else {
            let last = quake_dir.join(LAST_MANIFEST_FILENAME);
            if last.exists() {
                let p = fs::read_to_string(&last)?;
                PathBuf::from(p.trim())
            } else {
                bail!(
                    "No manifest file provided and no existing path found in {}\n\
                     Run with `--file PATH_TO_MANIFEST`",
                    last.display()
                );
            }
        };

        let manifest = Manifest::from_file(&manifest_path)?;
        let testnet_name = manifest.name.replace('_', "-");
        let dir = quake_dir.join(&testnet_name);
        let compose_path = dir.join(COMPOSE_FILENAME);
        let monitoring_dir = dir.join(MONITORING_DIR);
        let monitoring_compose_path = monitoring_dir.join(COMPOSE_FILENAME);
        let nodes_metadata = NodesMetadata::from_manifest(&manifest);

        // Remember the manifest path for next time.
        fs::create_dir_all(&quake_dir)?;
        fs::write(
            quake_dir.join(LAST_MANIFEST_FILENAME),
            manifest_path.display().to_string(),
        )?;

        info!(manifest=%manifest_path.display(), name=%testnet_name, "Loaded manifest");

        Ok(Testnet {
            root_dir,
            quake_dir,
            dir,
            compose_path,
            monitoring_dir,
            monitoring_compose_path,
            manifest,
            nodes_metadata,
        })
    }

    /// Generate all required files to run the testnet.
    pub fn setup(&self, force: bool) -> Result<()> {
        info!(dir=%self.dir.display(), "Setting up testnet files");
        setup::generate(
            &self.dir,
            &self.monitoring_dir,
            &self.manifest,
            &self.nodes_metadata,
            force,
        )?;
        info!(dir=%self.dir.display(), "✅ Testnet setup completed");
        Ok(())
    }

    /// Build the testnet Docker image.
    pub fn build(&self) -> Result<()> {
        build::build_image(
            &self.root_dir,
            &self.quake_dir,
            &self.manifest.image,
        )
    }

    /// Start the testnet or a subset of nodes.
    ///
    /// Automatically runs `setup` and `build` if needed, following the original
    /// quake convention.
    pub async fn start(&self, names: Vec<String>) -> Result<()> {
        let containers = self.nodes_metadata.expand_names(&names);
        if containers.is_empty() {
            bail!("No matching nodes found");
        }

        if names.is_empty() {
            // Start following manifest start_at heights
            self.start_from_manifest().await?;
        } else {
            info!("Starting: {}", containers.join(", "));
            docker::compose_up(&self.root_dir, &self.compose_path, &containers)?;
        }

        // Start monitoring services
        self.start_monitoring()?;

        info!(dir=%self.dir.display(), "✅ Testnet started");
        self.print_monitoring_info();
        Ok(())
    }

    /// Start nodes respecting their start_at heights from the manifest.
    async fn start_from_manifest(&self) -> Result<()> {
        use itertools::Itertools;

        // Group nodes by starting height.
        let nodes_by_height: Vec<(u64, Vec<String>)> = self
            .manifest
            .nodes
            .iter()
            .map(|(name, node)| (node.start_at.unwrap_or(0), name.clone()))
            .into_group_map_by(|(h, _)| *h)
            .into_iter()
            .sorted_by_key(|(h, _)| *h)
            .map(|(h, entries)| (h, entries.into_iter().map(|(_, n)| n).collect()))
            .collect();

        let mut started_nodes: Vec<String> = Vec::new();

        for (height, node_names) in &nodes_by_height {
            // Wait for already-started nodes to reach this height
            if !started_nodes.is_empty() && *height > 0 {
                let node_urls = self.metrics_urls(&started_nodes);
                wait::wait_for_height(node_urls, *height, Duration::from_secs(60)).await?;
            }

            info!(
                "Starting nodes at height {height}: {}",
                node_names.join(", ")
            );
            docker::compose_up(&self.root_dir, &self.compose_path, node_names)?;

            started_nodes.extend(node_names.clone());
        }

        Ok(())
    }

    /// Stop the testnet or a subset of nodes.
    pub fn stop(&self, names: Vec<String>) -> Result<()> {
        let containers = self.nodes_metadata.expand_names(&names);

        if !names.is_empty() && containers.is_empty() {
            warn!("No nodes matched: {}", names.join(", "));
            return Ok(());
        }

        // If stopping all nodes, also stop monitoring
        if names.is_empty() {
            self.stop_monitoring();
        }

        info!("Stopping: {}", containers.join(", "));
        docker::compose_stop(&self.root_dir, &self.compose_path, &containers)?;
        info!("✅ Testnet stopped");
        Ok(())
    }

    /// Stop all nodes and remove testnet-related files.
    pub fn clean(&self) -> Result<()> {
        // Stop monitoring services first
        self.stop_monitoring();

        // Docker compose down
        if self.is_setup() {
            if let Err(err) = docker::compose_down(
                &self.root_dir,
                &self.compose_path,
                &["--remove-orphans", "--volumes", "--timeout", "30"],
            ) {
                warn!(%err, "Failed to stop and remove containers");
            } else {
                info!("Testnet is down");
            }
        }

        // Monitoring compose down
        if self.monitoring_compose_path.exists() {
            let _ = docker::compose_down(
                &self.root_dir,
                &self.monitoring_compose_path,
                &["--remove-orphans", "--volumes"],
            );
        }

        // Remove testnet data
        if self.dir.exists() {
            debug!(dir=%self.dir.display(), "Removing testnet data");
            if let Err(err) = fs::remove_dir_all(&self.dir) {
                warn!(dir=%self.dir.display(), "Failed to remove testnet data: {err}");
            } else {
                info!(dir=%self.dir.display(), "Testnet data removed");
            }
        }

        info!("✅ Testnet cleaned");
        Ok(())
    }

    /// Apply a perturbation to nodes.
    pub async fn perturb(
        &self,
        action: Perturbation,
        min_time_off: Duration,
        max_time_off: Duration,
    ) -> Result<()> {
        let targets = action.target_names();
        let containers = self.nodes_metadata.expand_names(targets);
        let containers = if containers.is_empty() {
            self.nodes_metadata.names()
        } else {
            containers
        };

        info!("Applying perturbation: {action}");
        action
            .apply(
                &self.root_dir,
                &self.compose_path,
                &self.nodes_metadata,
                &containers,
                min_time_off,
                max_time_off,
            )
            .await?;
        info!("✅ Perturbation applied: {action}");
        Ok(())
    }

    /// Output container logs.
    pub fn logs(&self, names: Vec<String>, follow: bool) -> Result<()> {
        let containers = self.nodes_metadata.expand_names(&names);
        docker::compose_logs(&self.root_dir, &self.compose_path, &containers, follow)
    }

    /// Show testnet state and metadata.
    pub async fn info(&self, command: Option<InfoSubcommand>) -> Result<()> {
        let node_urls = self.all_metrics_urls();

        match command {
            None => {
                println!("Testnet: {}", self.manifest.name);
                println!("Image: {}", self.manifest.image);
                println!("Directory: {}", self.dir.display());
                println!();

                println!("Nodes:");
                print!("{}", self.nodes_metadata);
                println!();

                println!("Heights:");
                let max_name_len = self.nodes_metadata.max_name_len();
                for (name, height_str) in crate::height::latest_heights(&node_urls).await? {
                    println!("  {name:<max_name_len$} = {height_str}");
                }

                println!();
                println!("Monitoring:");
                self.print_monitoring_info();
            }
            Some(InfoSubcommand::Heights { number }) => {
                crate::height::loop_print_latest_heights(&node_urls, number).await?;
            }
        }

        Ok(())
    }

    /// Wait for nodes to reach a height.
    pub async fn wait_height(
        &self,
        height: u64,
        names: &[String],
        timeout: Duration,
    ) -> Result<()> {
        let resolved = self.nodes_metadata.expand_names(names);
        let node_urls = self.metrics_urls(&resolved);
        wait::wait_for_height(node_urls, height, timeout).await?;
        info!(nodes=%resolved.join(", "), "✅ Nodes reached height {height}");
        Ok(())
    }

    /// Wait for nodes to finish syncing.
    pub async fn wait_sync(&self, names: &[String], timeout: Duration) -> Result<()> {
        let resolved = self.nodes_metadata.expand_names(names);
        let node_urls = self.metrics_urls(&resolved);
        wait::wait_for_sync(node_urls, timeout).await?;
        info!(nodes=%resolved.join(", "), "✅ Nodes are synced");
        Ok(())
    }

    // --- helpers ---

    pub(crate) fn is_setup(&self) -> bool {
        self.compose_path.exists()
    }

    /// Check whether the Docker image for this testnet exists locally.
    pub(crate) fn image_exists(&self) -> bool {
        docker::image_exists(&self.manifest.image, &self.root_dir)
    }

    fn metrics_urls(&self, names: &[String]) -> Vec<(String, String)> {
        names
            .iter()
            .filter_map(|n| {
                self.nodes_metadata
                    .get(n)
                    .map(|m| (n.clone(), m.host_metrics_url()))
            })
            .collect()
    }

    /// Get the metrics URLs for all nodes.
    fn all_metrics_urls(&self) -> Vec<(String, String)> {
        self.nodes_metadata
            .values()
            .iter()
            .map(|m| (m.name.clone(), m.host_metrics_url()))
            .collect()
    }

    // --- Monitoring ---

    /// Start monitoring services (Prometheus + Grafana).
    fn start_monitoring(&self) -> Result<()> {
        if !self.monitoring_compose_path.exists() {
            debug!("Monitoring compose file not found, skipping");
            return Ok(());
        }
        info!("Starting monitoring services (Prometheus + Grafana)...");
        docker::compose_up(
            &self.root_dir,
            &self.monitoring_compose_path,
            &[] as &[&str],
        )
    }

    /// Stop monitoring services.
    fn stop_monitoring(&self) {
        if !self.monitoring_compose_path.exists() {
            return;
        }
        info!("Stopping monitoring services...");
        let _ = docker::compose_down(&self.root_dir, &self.monitoring_compose_path, &[]);
    }

    /// Print monitoring URLs.
    fn print_monitoring_info(&self) {
        println!("  Prometheus: http://localhost:9090");
        println!("  Grafana:    http://localhost:3000");
    }
}
