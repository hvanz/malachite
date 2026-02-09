use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use perturb::Perturbation;
use testnet::Testnet;

mod build;
mod docker;
mod manifest;
mod node;
mod perturb;
mod setup;
mod shell;
mod testnet;
mod wait;

#[derive(Parser)]
#[command(name = "quake", about = "Testnet Management Tool")]
struct Cli {
    /// Path to the manifest TOML file
    #[arg(short = 'f', long = "file", value_name = "MANIFEST_TOML")]
    manifest_file: Option<PathBuf>,

    /// Increase verbosity (-v for debug, -vv for trace)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate from manifest all required files to run the testnet
    Setup {
        /// Force recreation of files even if they already exist
        #[clap(long, default_value = "false")]
        force: bool,
    },
    /// Build the testnet Docker images
    Build,
    /// Start the testnet or a subset of nodes
    ///
    /// If no node names are provided, start all nodes following the starting heights in the manifest.
    /// Wildcard '*' is supported; e.g. 'val*' will match all validators.
    Start {
        /// Names of the nodes to start (all nodes if not specified)
        nodes: Vec<String>,
    },
    /// Stop the testnet or a subset of nodes
    ///
    /// If no node names are provided, stop all nodes.
    /// Wildcard '*' is supported.
    Stop {
        /// Names of the nodes to stop (all nodes if not specified)
        nodes: Vec<String>,
    },
    /// Stop all nodes and remove testnet-related files (including databases)
    Clean,
    /// Apply a perturbation (disconnect, kill, pause, or restart) to nodes
    ///
    /// Wildcard '*' is supported; e.g. 'val*' will match all validators.
    Perturb {
        #[command(subcommand)]
        action: Perturbation,
        /// Minimum time the targets will be offline before recovering
        #[arg(short = 't', long, value_parser = humantime::parse_duration, default_value = "10s")]
        min_time_off: Duration,
        /// Maximum time the targets will be offline before recovering
        #[arg(short = 'T', long, value_parser = humantime::parse_duration, default_value = "20s")]
        max_time_off: Duration,
    },
    /// Output logs of all containers or a specific container
    Logs {
        /// Names of the nodes to show logs for (all if not specified)
        names: Vec<String>,
        /// Follow the logs output
        #[clap(short = 'f', long, default_value = "false")]
        follow: bool,
    },
    /// Show the state of the testnet and metadata
    Info,
    /// Wait for nodes to reach a height or finish syncing
    Wait {
        #[clap(subcommand)]
        command: WaitSubcommand,
    },
}

#[derive(Debug, Subcommand)]
enum WaitSubcommand {
    /// Wait for nodes to reach a specific block height
    Height {
        /// Height to wait for
        height: u64,
        /// Names of the nodes to wait for (all nodes if not specified)
        nodes: Vec<String>,
        /// Timeout in seconds
        #[clap(short, long, default_value = "60")]
        timeout: u64,
    },
    /// Wait for nodes to finish syncing (all at the same height)
    Sync {
        /// Names of the nodes to wait for (all nodes if not specified)
        nodes: Vec<String>,
        /// Timeout in seconds
        #[clap(short, long, default_value = "180")]
        timeout: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    // Initialize tracing
    let level = match cli.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "{level},hyper_util=warn,reqwest=warn,handlebars=warn"
        ))
    });
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Handle clean early (no testnet required if dir doesn't exist)
    if matches!(cli.command, Commands::Clean) {
        let testnet_result = Testnet::load(&cli.manifest_file);
        match testnet_result {
            Ok(testnet) => return testnet.clean(),
            Err(e) => {
                info!("No existing testnet to clean: {e}");
                return Ok(());
            }
        }
    }

    let testnet = Testnet::load(&cli.manifest_file)?;

    match cli.command {
        Commands::Setup { force } => testnet.setup(force),
        Commands::Build => testnet.build(),
        Commands::Start { nodes } => {
            // Auto-setup if compose file doesn't exist
            if !testnet.is_setup() {
                warn!("Testnet not set up, running setup...");
                testnet.setup(false)?;
            }
            // Auto-build if Docker image doesn't exist
            if !testnet.image_exists() {
                warn!(
                    "Docker image '{}' not found, running build...",
                    testnet.manifest.image
                );
                testnet.build()?;
            }
            testnet.start(nodes).await
        }
        Commands::Stop { nodes } => testnet.stop(nodes),
        Commands::Clean => unreachable!(), // handled above
        Commands::Perturb {
            action,
            min_time_off,
            max_time_off,
        } => testnet.perturb(action, min_time_off, max_time_off).await,
        Commands::Logs { names, follow } => testnet.logs(names, follow),
        Commands::Info => testnet.info().await,
        Commands::Wait { command } => match command {
            WaitSubcommand::Height {
                height,
                nodes,
                timeout,
            } => {
                testnet
                    .wait_height(height, &nodes, Duration::from_secs(timeout))
                    .await
            }
            WaitSubcommand::Sync { nodes, timeout } => {
                testnet
                    .wait_sync(&nodes, Duration::from_secs(timeout))
                    .await
            }
        },
    }
}
