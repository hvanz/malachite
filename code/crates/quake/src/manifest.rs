//! TOML manifest parsing, validation, and node type inference.

use color_eyre::eyre::{bail, Context, Result};
use indexmap::IndexMap;
use serde::Deserialize;
use std::path::Path;

/// Determines whether a node name corresponds to a validator.
/// Names starting with "val" are treated as validators.
pub fn is_validator(name: &str) -> bool {
    name.starts_with("val")
}

/// Node type inferred from the node name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType {
    Validator,
    NonValidator,
}

/// A parsed node from the manifest.
#[derive(Debug, Clone)]
pub struct Node {
    pub node_type: NodeType,
    /// Per-node malachite config overrides (merged with global config).
    pub config: toml::Table,
    /// Height at which to start this node (None = start immediately).
    pub start_at: Option<u64>,
    /// Explicit persistent peers for this node.
    pub persistent_peers: Option<Vec<String>>,
}

/// The parsed manifest.
#[derive(Debug, Clone)]
pub struct Manifest {
    pub name: String,
    pub image: String,
    #[allow(dead_code)]
    pub global_config: toml::Table,
    pub nodes: IndexMap<String, Node>,
}

// --- Raw deserialization types ---

#[derive(Deserialize)]
struct RawManifest {
    name: Option<String>,
    #[serde(default = "default_image")]
    image: String,
    #[serde(default)]
    config: toml::Table,
    #[serde(default)]
    nodes: IndexMap<String, RawNode>,
}

fn default_image() -> String {
    "malachite:latest".to_string()
}

#[derive(Deserialize, Default)]
struct RawNode {
    #[serde(default)]
    config: toml::Table,
    start_at: Option<u64>,
    persistent_peers: Option<Vec<String>>,
}

// --- Manifest construction ---

impl Manifest {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read manifest file: {}", path.display()))?;
        Self::from_string(&content, path)
    }

    fn from_string(content: &str, path: &Path) -> Result<Self> {
        let raw: RawManifest = toml::from_str(content).wrap_err("Failed to parse manifest")?;

        // Derive testnet name from manifest file stem if not specified.
        let name = raw.name.unwrap_or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("testnet")
                .to_string()
        });

        let global_config = raw.config;

        // Build nodes with merged config.
        let mut nodes = IndexMap::new();
        for (node_name, raw_node) in raw.nodes {
            let node_type = if is_validator(&node_name) {
                NodeType::Validator
            } else {
                NodeType::NonValidator
            };

            // Merge: global config + per-node overrides
            let merged_config = merge_toml_tables(&global_config, &raw_node.config);

            nodes.insert(
                node_name,
                Node {
                    node_type,
                    config: merged_config,
                    start_at: raw_node.start_at,
                    persistent_peers: raw_node.persistent_peers,
                },
            );
        }

        let manifest = Manifest {
            name,
            image: raw.image,
            global_config,
            nodes,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        if self.nodes.is_empty() {
            bail!("At least one node must be defined in the manifest");
        }

        // Check that start_at is not 0
        for (name, node) in &self.nodes {
            if node.start_at == Some(0) {
                bail!("start_at cannot be 0 for node '{name}'");
            }
        }

        // Check that persistent_peers reference existing nodes
        let node_names: Vec<&str> = self.nodes.keys().map(|s| s.as_str()).collect();
        for (name, node) in &self.nodes {
            if let Some(peers) = &node.persistent_peers {
                for peer in peers {
                    if !node_names.contains(&peer.as_str()) {
                        bail!(
                            "persistent_peers for node '{name}' references unknown node '{peer}'"
                        );
                    }
                }
            }
        }

        // Check at least one validator
        let num_validators = self.num_validators();
        if num_validators == 0 {
            bail!("At least one validator node is required (node name must start with 'val')");
        }

        Ok(())
    }

    /// Number of validator nodes.
    pub fn num_validators(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| n.node_type == NodeType::Validator)
            .count()
    }

    /// Number of total nodes.
    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Names of validator nodes in order.
    #[allow(dead_code)]
    pub fn validator_names(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.node_type == NodeType::Validator)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// Deep-merge two TOML tables: values in `overrides` take precedence.
fn merge_toml_tables(base: &toml::Table, overrides: &toml::Table) -> toml::Table {
    let mut result = base.clone();
    for (key, override_val) in overrides {
        match (result.get(key), override_val) {
            (Some(toml::Value::Table(base_table)), toml::Value::Table(override_table)) => {
                let merged = merge_toml_tables(base_table, override_table);
                result.insert(key.clone(), toml::Value::Table(merged));
            }
            _ => {
                result.insert(key.clone(), override_val.clone());
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn parse(content: &str) -> Result<Manifest> {
        Manifest::from_string(content, &PathBuf::from("test.toml"))
    }

    #[test]
    fn parse_simple_manifest() {
        let manifest = parse(
            r#"
            name = "simple"
            [nodes.validator1]
            [nodes.validator2]
            "#,
        )
        .unwrap();

        assert_eq!(manifest.name, "simple");
        assert_eq!(manifest.num_nodes(), 2);
        assert_eq!(manifest.num_validators(), 2);
    }

    #[test]
    fn parse_manifest_with_config() {
        let manifest = parse(
            r#"
            [config]
            logging.log_level = "warn"

            [nodes.validator1]
            config.logging.log_level = "debug"

            [nodes.validator2]
            "#,
        )
        .unwrap();

        assert_eq!(
            manifest.nodes["validator1"].config["logging"]["log_level"]
                .as_str()
                .unwrap(),
            "debug"
        );
        assert_eq!(
            manifest.nodes["validator2"].config["logging"]["log_level"]
                .as_str()
                .unwrap(),
            "warn"
        );
    }

    #[test]
    fn no_nodes_fails() {
        assert!(parse(r#"name = "empty""#).is_err());
    }

    #[test]
    fn no_validators_fails() {
        assert!(parse(
            r#"
            [nodes.full1]
            [nodes.full2]
            "#
        )
        .is_err());
    }

    #[test]
    fn node_type_inference() {
        let manifest = parse(
            r#"
            [nodes.validator1]
            [nodes.val2]
            [nodes.full1]
            [nodes.sentry]
            "#,
        )
        .unwrap();

        assert_eq!(manifest.nodes["validator1"].node_type, NodeType::Validator);
        assert_eq!(manifest.nodes["val2"].node_type, NodeType::Validator);
        assert_eq!(manifest.nodes["full1"].node_type, NodeType::NonValidator);
        assert_eq!(manifest.nodes["sentry"].node_type, NodeType::NonValidator);
    }

    #[test]
    fn invalid_persistent_peer() {
        assert!(parse(
            r#"
            [nodes.validator1]
            persistent_peers = ["nonexistent"]
            "#
        )
        .is_err());
    }
}
