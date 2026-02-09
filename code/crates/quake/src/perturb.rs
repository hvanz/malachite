//! Perturbation actions for testnet nodes.

use std::path::Path;
use std::time::Duration;

use clap::Subcommand;
use color_eyre::eyre::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tracing::info;

use crate::docker;
use crate::node::NodesMetadata;

/// A perturbation is an action that temporarily affects node containers.
#[derive(Debug, Subcommand, Clone)]
pub(crate) enum Perturbation {
    /// Disconnect containers from the network, wait some time, then reconnect them.
    Disconnect {
        /// Node names to disconnect (all if not specified)
        targets: Vec<String>,
        /// How long the node should be offline
        #[arg(short, long, value_parser = humantime::parse_duration)]
        time_off: Option<Duration>,
    },
    /// Kill containers, wait some time, then restart them.
    Kill {
        /// Node names to kill (all if not specified)
        targets: Vec<String>,
        /// How long the node should remain killed before being restarted
        #[arg(short, long, value_parser = humantime::parse_duration)]
        time_off: Option<Duration>,
    },
    /// Pause containers, wait some time, then unpause them.
    Pause {
        /// Node names to pause (all if not specified)
        targets: Vec<String>,
        /// How long the node should be paused
        #[arg(short, long, value_parser = humantime::parse_duration)]
        time_off: Option<Duration>,
    },
    /// Restart containers immediately.
    Restart {
        /// Node names to restart (all if not specified)
        targets: Vec<String>,
    },
}

impl Perturbation {
    pub fn target_names(&self) -> &[String] {
        match self {
            Perturbation::Disconnect { targets, .. } => targets,
            Perturbation::Kill { targets, .. } => targets,
            Perturbation::Pause { targets, .. } => targets,
            Perturbation::Restart { targets } => targets,
        }
    }

    /// Apply the perturbation to the resolved containers.
    pub async fn apply(
        &self,
        root_dir: &Path,
        _compose_path: &Path,
        nodes_metadata: &NodesMetadata,
        containers: &[String],
        min_time_off: Duration,
        max_time_off: Duration,
    ) -> Result<()> {
        match self {
            Perturbation::Disconnect { time_off, .. } => {
                let time_off =
                    time_off.unwrap_or_else(|| rand_duration(min_time_off, max_time_off));
                let network = NodesMetadata::network_name();

                info!(
                    "Disconnecting from network for {time_off:?}: {}",
                    containers.join(", ")
                );
                for name in containers {
                    docker::network_disconnect(root_dir, network, name)?;
                }

                tokio::time::sleep(time_off).await;

                info!("Reconnecting: {}", containers.join(", "));
                for name in containers {
                    let ip = nodes_metadata.get(name).map(|m| m.ip.as_str());
                    docker::network_connect(root_dir, network, name, ip)?;
                }
            }

            Perturbation::Kill { time_off, .. } => {
                let time_off =
                    time_off.unwrap_or_else(|| rand_duration(min_time_off, max_time_off));

                info!(
                    "Killing and waiting {time_off:?}: {}",
                    containers.join(", ")
                );
                docker::kill(root_dir, containers)?;

                tokio::time::sleep(time_off).await;

                docker::start(root_dir, containers)?;
                info!("Restarted after {time_off:?}: {}", containers.join(", "));
            }

            Perturbation::Pause { time_off, .. } => {
                let time_off =
                    time_off.unwrap_or_else(|| rand_duration(min_time_off, max_time_off));

                info!("Pausing for {time_off:?}: {}", containers.join(", "));
                docker::pause(root_dir, containers)?;

                tokio::time::sleep(time_off).await;

                docker::unpause(root_dir, containers)?;
                info!("Unpaused after {time_off:?}: {}", containers.join(", "));
            }

            Perturbation::Restart { .. } => {
                info!("Restarting: {}", containers.join(", "));
                docker::restart(root_dir, containers)?;
                info!("Restarted: {}", containers.join(", "));
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for Perturbation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Perturbation::Disconnect { targets, .. } => {
                write!(f, "disconnect {}", targets.join(", "))
            }
            Perturbation::Kill { targets, .. } => write!(f, "kill {}", targets.join(", ")),
            Perturbation::Pause { targets, .. } => write!(f, "pause {}", targets.join(", ")),
            Perturbation::Restart { targets } => write!(f, "restart {}", targets.join(", ")),
        }
    }
}

/// Generate a random duration between min and max.
fn rand_duration(min: Duration, max: Duration) -> Duration {
    let mut rng = StdRng::from_entropy();
    let duration = rng.gen_range(min.as_millis()..=max.as_millis()) as u64;
    Duration::from_millis(duration)
}
