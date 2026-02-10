//! Standalone binary for the Malachite test application.
//!
//! This binary is intended for use in Docker-based testnets (via Quake).
//! It reads configuration, genesis, and private key files from the
//! `--home` directory and runs the test app consensus node.
//!
//! Usage:
//!   malachite-testapp start --home /data
//!   malachite-testapp start --home /data --start-height 10

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Context;
use tracing::info;

use malachitebft_test::node::Node;
use malachitebft_test::{Genesis, Height, PrivateKey};

// The lib crate name for this package (hyphens become underscores)
use informalsystems_malachitebft_test_app::config::{self as app_config, Config};
use informalsystems_malachitebft_test_app::node::App;

#[derive(Parser)]
#[command(name = "malachite-testapp", about = "Malachite test application node")]
struct Cli {
    /// Home directory containing config, genesis, and key files
    #[arg(long, global = true, default_value = ".")]
    home: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the consensus node
    Start {
        /// Height at which to start the node
        #[arg(long)]
        start_height: Option<u64>,
    },
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install().expect("Failed to install global error handler");

    let cli = Cli::parse();

    match cli.command {
        Commands::Start { start_height } => {
            let config_path = cli.home.join("config").join("config.toml");
            let genesis_path = cli.home.join("config").join("genesis.json");
            let private_key_path = cli.home.join("config").join("priv_validator_key.json");

            let config: Config = app_config::load_config(&config_path, Some("MALACHITE"))
                .wrap_err_with(|| {
                    format!("Failed to load config from {}", config_path.display())
                })?;

            // Init logging after config is loaded
            let _guard = malachitebft_test_cli::logging::init(
                config.logging.log_level,
                config.logging.log_format,
            );

            info!("Loading genesis from {}", genesis_path.display());
            let genesis_str = std::fs::read_to_string(&genesis_path).wrap_err_with(|| {
                format!("Failed to read genesis from {}", genesis_path.display())
            })?;
            let genesis: Genesis =
                serde_json::from_str(&genesis_str).wrap_err("Failed to parse genesis file")?;

            info!("Loading private key from {}", private_key_path.display());
            let pk_str = std::fs::read_to_string(&private_key_path).wrap_err_with(|| {
                format!(
                    "Failed to read private key from {}",
                    private_key_path.display()
                )
            })?;
            let private_key: PrivateKey =
                serde_json::from_str(&pk_str).wrap_err("Failed to parse private key file")?;

            let validator_set = genesis.validator_set.clone();

            let start_height = start_height.map(Height::new);

            if let Some(byz) = &config.byzantine {
                if byz.is_active() {
                    info!("Byzantine behavior is configured: {byz:?}");
                }
            }

            let app = App {
                home_dir: cli.home,
                config,
                validator_set,
                private_key,
                start_height,
                middleware: None, // Byzantine middleware is injected by App::start() based on config.byzantine
            };

            let metrics_config = app.config.metrics.clone();
            let rt = malachitebft_test_cli::runtime::build_runtime(app.config.runtime)?;

            rt.block_on(async {
                // Start the Prometheus metrics server
                if metrics_config.enabled {
                    info!("Starting metrics server on {}", metrics_config.listen_addr);
                    tokio::spawn(malachitebft_test_cli::metrics::serve(
                        metrics_config.listen_addr,
                    ));
                }

                info!("Node is starting...");
                app.run().await
            })
            .wrap_err("Node failed")?;

            info!("Node has stopped");
            Ok(())
        }
    }
}
