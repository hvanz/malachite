//! Testnet file generation: keys, configs, Docker Compose.

use std::fs;
use std::path::Path;

use color_eyre::eyre::{Context, Result};
use handlebars::Handlebars;
use serde::Serialize;
use tracing::debug;

use malachitebft_config::*;
use malachitebft_test::node::Node;
use malachitebft_test::traits::{CanMakeGenesis, CanMakePrivateKeyFile};
use malachitebft_test::{Height, PrivateKey, ValidatorSet};

use malachitebft_test_app::config::Config;
use malachitebft_test_app::node::App;

use crate::manifest::Manifest;
use crate::node::{NodeMetadata, NodesMetadata, METRICS_PORT, P2P_PORT};

/// Template data for rendering compose.yaml.hbs.
#[derive(Serialize)]
struct ComposeTemplateData<'a> {
    image: &'a str,
    nodes: Vec<&'a NodeMetadata>,
}

/// Generate all testnet files from the manifest.
pub(crate) fn generate(
    testnet_dir: &Path,
    monitoring_dir: &Path,
    manifest: &Manifest,
    nodes_metadata: &NodesMetadata,
    force: bool,
) -> Result<()> {
    // Create directory structure
    fs::create_dir_all(testnet_dir).with_context(|| {
        format!(
            "Failed to create testnet directory: {}",
            testnet_dir.display()
        )
    })?;

    // 1. Generate keys and genesis
    generate_keys_and_genesis(testnet_dir, manifest, nodes_metadata, force)?;

    // 2. Generate config files per node
    generate_configs(testnet_dir, manifest, nodes_metadata, force)?;

    // 3. Render Docker Compose file
    generate_compose_file(testnet_dir, manifest, nodes_metadata, force)?;

    // 4. Save node metadata
    generate_nodes_metadata_file(testnet_dir, nodes_metadata, force)?;

    // 5. Generate monitoring config (Prometheus + Grafana compose)
    fs::create_dir_all(&monitoring_dir)?;
    generate_prometheus_config(&monitoring_dir, nodes_metadata, force)?;
    generate_monitoring_compose_file(&monitoring_dir, force)?;

    Ok(())
}

/// Generate private keys and genesis file.
fn generate_keys_and_genesis(
    testnet_dir: &Path,
    manifest: &Manifest,
    _nodes_metadata: &NodesMetadata,
    force: bool,
) -> Result<()> {
    let num_nodes = manifest.num_nodes();

    // Create a dummy App instance for key/genesis generation.
    // The validator_set and private_key are placeholders; only the trait
    // methods (generate_private_key, get_public_key, make_genesis, etc.) matter.
    let dummy_pk = PrivateKey::generate(rand::thread_rng());
    let dummy_validator = malachitebft_test::Validator::new(dummy_pk.public_key(), 1);
    let dummy_node = App {
        home_dir: testnet_dir.to_path_buf(),
        config: Config::default(),
        validator_set: ValidatorSet::new(vec![dummy_validator]),
        private_key: dummy_pk,
        start_height: Some(Height::new(1)),
        middleware: None,
    };

    // Generate deterministic private keys
    let private_keys =
        malachitebft_test_cli::new::generate_private_keys(&dummy_node, num_nodes, true);

    let public_keys: Vec<_> = private_keys
        .iter()
        .map(|pk| dummy_node.get_public_key(pk))
        .collect();

    // Generate genesis with all validators
    let validator_keys: Vec<_> = manifest
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, (_, node))| node.node_type == crate::manifest::NodeType::Validator)
        .map(|(i, _)| (public_keys[i].clone(), 1u64)) // voting power = 1
        .collect();

    let genesis = dummy_node.make_genesis(validator_keys);

    // Save private keys and genesis for each node
    for (i, (name, _)) in manifest.nodes.iter().enumerate() {
        let node_dir = testnet_dir.join(name);
        let config_dir = node_dir.join("config");
        fs::create_dir_all(&config_dir)?;

        // Private key file
        let priv_key_path = config_dir.join("priv_validator_key.json");
        if force || !priv_key_path.exists() {
            let priv_key_file = dummy_node.make_private_key_file(private_keys[i].clone());
            let json = serde_json::to_string_pretty(&priv_key_file)?;
            fs::write(&priv_key_path, json)?;
            debug!("Generated private key for {name}");
        }

        // Genesis file
        let genesis_path = config_dir.join("genesis.json");
        if force || !genesis_path.exists() {
            let json = serde_json::to_string_pretty(&genesis)?;
            fs::write(&genesis_path, json)?;
            debug!("Generated genesis for {name}");
        }
    }

    Ok(())
}

/// Generate config.toml files for each node with Docker-appropriate addresses.
fn generate_configs(
    testnet_dir: &Path,
    manifest: &Manifest,
    nodes_metadata: &NodesMetadata,
    force: bool,
) -> Result<()> {
    let transport = TransportProtocol::Tcp;

    for (name, manifest_node) in &manifest.nodes {
        let config_path = testnet_dir.join(name).join("config").join("config.toml");

        if !force && config_path.exists() {
            debug!("Skipping config for {name} (already exists)");
            continue;
        }

        // Build persistent peers list
        let peer_addrs: Vec<_> = if let Some(peers) = &manifest_node.persistent_peers {
            // Use explicit peers from manifest
            peers
                .iter()
                .filter_map(|peer_name| nodes_metadata.get(peer_name))
                .map(|peer| transport.multiaddr(&peer.ip, P2P_PORT as usize))
                .collect()
        } else {
            // All other nodes
            nodes_metadata
                .values()
                .iter()
                .filter(|m| m.name != *name)
                .map(|m| transport.multiaddr(&m.ip, P2P_PORT as usize))
                .collect()
        };

        // Build base config
        let config = Config {
            moniker: name.clone(),
            consensus: ConsensusConfig {
                enabled: true,
                value_payload: ValuePayload::ProposalAndParts,
                queue_capacity: 100,
                p2p: P2pConfig {
                    protocol: PubSubProtocol::default(),
                    listen_addr: transport.multiaddr("0.0.0.0", P2P_PORT as usize),
                    persistent_peers: peer_addrs,
                    discovery: DiscoveryConfig {
                        enabled: false,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            metrics: MetricsConfig {
                enabled: true,
                listen_addr: format!("0.0.0.0:{METRICS_PORT}").parse().unwrap(),
            },
            runtime: RuntimeConfig::SingleThreaded,
            value_sync: ValueSyncConfig::default(),
            logging: LoggingConfig::default(),
            test: TestConfig::default(),
            byzantine: None,
        };

        // Ensure parent directory exists
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Apply per-node config overrides from the manifest.
        // We write the merged toml::Value directly to preserve any extra
        // sections (e.g. [byzantine]) that are not part of the base Config struct.
        if !manifest_node.config.is_empty() {
            let base_toml = toml::to_string(&config)?;
            let mut base_value: toml::Value = toml::from_str(&base_toml)?;
            let override_value = toml::Value::Table(manifest_node.config.clone());
            merge_toml_value(&mut base_value, &override_value);
            let toml_str = toml::to_string_pretty(&base_value)?;
            fs::write(&config_path, toml_str)?;
        } else {
            let toml_str = toml::to_string_pretty(&config)?;
            fs::write(&config_path, toml_str)?;
        }
        debug!("Generated config for {name}");
    }

    Ok(())
}

/// Recursively merge override TOML values into a base value.
fn merge_toml_value(base: &mut toml::Value, overrides: &toml::Value) {
    match (base, overrides) {
        (toml::Value::Table(base_table), toml::Value::Table(override_table)) => {
            for (key, override_val) in override_table {
                if let Some(base_val) = base_table.get_mut(key) {
                    merge_toml_value(base_val, override_val);
                } else {
                    base_table.insert(key.clone(), override_val.clone());
                }
            }
        }
        (base, overrides) => {
            *base = overrides.clone();
        }
    }
}

/// Render Docker Compose file from the Handlebars template.
fn generate_compose_file(
    testnet_dir: &Path,
    manifest: &Manifest,
    nodes_metadata: &NodesMetadata,
    force: bool,
) -> Result<()> {
    let compose_path = testnet_dir.join("compose.yaml");
    if !force && compose_path.exists() {
        debug!("Skipping compose file (already exists)");
        return Ok(());
    }

    let template = include_str!("../templates/compose.yaml.hbs");

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("compose", template)
        .context("Failed to register compose template")?;

    let data = ComposeTemplateData {
        image: &manifest.image,
        nodes: nodes_metadata.values(),
    };

    let content = handlebars
        .render("compose", &data)
        .context("Failed to render compose template")?;

    fs::write(&compose_path, content)?;
    debug!("Generated compose file at {}", compose_path.display());
    Ok(())
}

/// Save node metadata to a JSON file.
fn generate_nodes_metadata_file(
    testnet_dir: &Path,
    nodes_metadata: &NodesMetadata,
    force: bool,
) -> Result<()> {
    let path = testnet_dir.join("nodes.json");
    if !force && path.exists() {
        return Ok(());
    }

    let content = serde_json::to_string_pretty(&nodes_metadata.values())?;
    fs::write(&path, content)?;
    debug!("Generated nodes metadata at {}", path.display());
    Ok(())
}

// --- Monitoring ---

/// Template data for rendering prometheus.yml.hbs.
#[derive(Serialize)]
struct PrometheusTemplateData<'a> {
    nodes: Vec<&'a NodeMetadata>,
}

/// Generate Prometheus configuration from the node metadata.
fn generate_prometheus_config(
    monitoring_dir: &Path,
    nodes_metadata: &NodesMetadata,
    force: bool,
) -> Result<()> {
    let path = monitoring_dir.join("prometheus.yml");
    if !force && path.exists() {
        debug!("Skipping prometheus config (already exists)");
        return Ok(());
    }

    let template = include_str!("../templates/prometheus.yml.hbs");
    let data = PrometheusTemplateData {
        nodes: nodes_metadata.values(),
    };

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("prometheus", template)
        .context("Failed to register Prometheus template")?;

    let content = handlebars
        .render("prometheus", &data)
        .context("Failed to render Prometheus template")?;

    fs::write(&path, content)?;
    debug!("Generated Prometheus config at {}", path.display());
    Ok(())
}

/// Template data for rendering compose-monitoring.yaml.hbs.
#[derive(Serialize)]
struct MonitoringComposeData {
    grafana_provisioning_dir: String,
}

/// Generate Docker Compose file for monitoring services (Prometheus + Grafana).
fn generate_monitoring_compose_file(monitoring_dir: &Path, force: bool) -> Result<()> {
    let compose_path = monitoring_dir.join("compose.yaml");
    if !force && compose_path.exists() {
        debug!("Skipping monitoring compose file (already exists)");
        return Ok(());
    }

    let template = include_str!("../templates/compose-monitoring.yaml.hbs");

    // Resolve the absolute path to the bundled Grafana provisioning dir.
    // It lives alongside the quake binary source at monitoring/grafana/provisioning/.
    let grafana_provisioning_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|p| p.to_path_buf()))
        .map(|_| {
            // Use the path relative to the crate source for development.
            // At runtime, the provisioning files live next to compose.yaml.
            // We copy them into the monitoring dir below.
            monitoring_dir
                .join("grafana")
                .join("provisioning")
                .to_string_lossy()
                .to_string()
        })
        .unwrap_or_else(|| "grafana/provisioning".to_string());

    let data = MonitoringComposeData {
        grafana_provisioning_dir,
    };

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("monitoring", template)
        .context("Failed to register monitoring compose template")?;

    let content = handlebars
        .render("monitoring", &data)
        .context("Failed to render monitoring compose template")?;

    fs::write(&compose_path, content)?;
    debug!(
        "Generated monitoring compose file at {}",
        compose_path.display()
    );

    // Copy bundled Grafana provisioning files into the monitoring dir
    copy_grafana_provisioning(monitoring_dir)?;

    Ok(())
}

/// Copy the bundled Grafana provisioning files into the testnet monitoring directory.
fn copy_grafana_provisioning(monitoring_dir: &Path) -> Result<()> {
    let dest = monitoring_dir.join("grafana").join("provisioning");
    fs::create_dir_all(dest.join("datasources"))?;
    fs::create_dir_all(dest.join("dashboards"))?;
    fs::create_dir_all(dest.join("dashboards-data"))?;

    fs::write(
        dest.join("datasources").join("prometheus.yml"),
        include_str!("../monitoring/grafana/provisioning/datasources/prometheus.yml"),
    )?;
    fs::write(
        dest.join("dashboards").join("default.yml"),
        include_str!("../monitoring/grafana/provisioning/dashboards/default.yml"),
    )?;
    fs::write(
        dest.join("dashboards-data").join("default.json"),
        include_str!("../monitoring/grafana/provisioning/dashboards-data/default.json"),
    )?;

    debug!("Copied Grafana provisioning files to {}", dest.display());
    Ok(())
}
