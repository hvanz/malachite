//! Node metadata types used for Docker Compose orchestration.

use indexmap::IndexMap;
use serde::Serialize;

use crate::manifest::Manifest;

/// Fixed container ports (inside the Docker container).
pub const P2P_PORT: u16 = 27000;
pub const MEMPOOL_PORT: u16 = 28000;
pub const METRICS_PORT: u16 = 29000;

/// Docker network base IP: 172.20.0.0/16, nodes start at .2
const BASE_IP_PREFIX: &str = "172.20.0";
const BASE_IP_OFFSET: u16 = 2;

/// Metadata for a single node in the testnet.
#[derive(Debug, Clone, Serialize)]
pub struct NodeMetadata {
    /// Node name as defined in the manifest.
    pub name: String,
    /// 0-based index of the node.
    pub index: usize,
    /// Docker container IP on the bridge network.
    pub ip: String,
    /// Host-mapped P2P port.
    pub host_p2p_port: u16,
    /// Host-mapped mempool port.
    pub host_mempool_port: u16,
    /// Host-mapped metrics port.
    pub host_metrics_port: u16,
    /// Whether this is a validator node.
    pub is_validator: bool,
}

impl NodeMetadata {
    /// The Prometheus metrics URL reachable from the host.
    pub fn host_metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.host_metrics_port)
    }
}

/// Ordered collection of node metadata, keyed by node name.
#[derive(Debug, Clone)]
pub struct NodesMetadata(pub IndexMap<String, NodeMetadata>);

impl NodesMetadata {
    /// Build metadata for all nodes in the manifest.
    pub fn from_manifest(manifest: &Manifest) -> Self {
        let mut map = IndexMap::new();
        for (i, (name, node)) in manifest.nodes.iter().enumerate() {
            let ip = format!("{BASE_IP_PREFIX}.{}", BASE_IP_OFFSET + i as u16);
            let metadata = NodeMetadata {
                name: name.clone(),
                index: i,
                ip,
                host_p2p_port: P2P_PORT + i as u16,
                host_mempool_port: MEMPOOL_PORT + i as u16,
                host_metrics_port: METRICS_PORT + i as u16,
                is_validator: node.node_type == crate::manifest::NodeType::Validator,
            };
            map.insert(name.clone(), metadata);
        }
        NodesMetadata(map)
    }

    /// All node names.
    pub fn names(&self) -> Vec<String> {
        self.0.keys().cloned().collect()
    }

    /// All metadata values in order.
    pub fn values(&self) -> Vec<&NodeMetadata> {
        self.0.values().collect()
    }

    /// Get metadata by name.
    pub fn get(&self, name: &str) -> Option<&NodeMetadata> {
        self.0.get(name)
    }

    /// Expand a list of names (which may include glob wildcards like `val*` or `val*1`)
    /// into concrete node names. `*` matches any sequence of characters.
    pub fn expand_names(&self, names: &[String]) -> Vec<String> {
        if names.is_empty() {
            return self.names();
        }
        let mut result = Vec::new();
        for pattern in names {
            if pattern.contains('*') {
                for name in self.0.keys() {
                    if glob_match(pattern, name) && !result.contains(name) {
                        result.push(name.clone());
                    }
                }
            } else if self.0.contains_key(pattern) && !result.contains(pattern) {
                result.push(pattern.clone());
            }
        }
        result
    }

    /// Docker network name.
    pub fn network_name() -> &'static str {
        "malachite_quake_malachite_net"
    }
}

/// Simple glob matching where `*` matches any sequence of characters.
fn glob_match(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();

    // No wildcard — exact match
    if parts.len() == 1 {
        return pattern == text;
    }

    let mut pos = 0;

    // First part must match at the start
    if !parts[0].is_empty() {
        if !text.starts_with(parts[0]) {
            return false;
        }
        pos = parts[0].len();
    }

    // Middle parts must appear in order
    for part in &parts[1..parts.len() - 1] {
        if part.is_empty() {
            continue;
        }
        match text[pos..].find(part) {
            Some(idx) => pos += idx + part.len(),
            None => return false,
        }
    }

    // Last part must match at the end
    let last = parts[parts.len() - 1];
    if !last.is_empty() {
        return text.len() >= pos + last.len() && text[pos..].ends_with(last);
    }

    true
}

impl std::fmt::Display for NodesMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for meta in self.0.values() {
            writeln!(
                f,
                "  {} (ip={}, p2p={}, metrics={}, validator={})",
                meta.name, meta.ip, meta.host_p2p_port, meta.host_metrics_port, meta.is_validator
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        // Trailing wildcard
        assert!(glob_match("val*", "validator1"));
        assert!(glob_match("val*", "validator5"));
        assert!(!glob_match("val*", "fullnode1"));

        // Wildcard in the middle
        assert!(glob_match("val*1", "validator1"));
        assert!(!glob_match("val*1", "validator2"));
        assert!(glob_match("val*1", "val1"));

        // Leading wildcard
        assert!(glob_match("*1", "validator1"));
        assert!(glob_match("*1", "fullnode1"));
        assert!(!glob_match("*1", "validator2"));

        // Exact (no wildcard)
        assert!(glob_match("validator1", "validator1"));
        assert!(!glob_match("validator1", "validator2"));

        // Bare wildcard
        assert!(glob_match("*", "anything"));
    }
}
