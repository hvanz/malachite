# Quake

A testnet management tool for Malachite. 
Quake orchestrates local Docker-based testnets from a simple TOML manifest.

## Prerequisites

- Docker (with Compose v2)
- A `GITHUB_TOKEN` in `code/.env` (for building the Docker image)

## Quick start

```bash
cd code

# Start a 5-validator testnet (auto-runs setup + build if needed)
quake -f crates/quake/scenarios/localdev.toml start

# Check testnet state
quake info

# Follow logs
quake logs -f

# Stop everything
quake stop

# Tear down and remove all data
quake clean
```

After the first run, the manifest path is remembered in `.quake/.last_manifest`,
so subsequent commands don't need `-f`:

```bash
quake start
quake stop
```

## Manifest

A manifest is a TOML file that defines the testnet. Minimal example:

```toml
[nodes.validator1]
[nodes.validator2]
[nodes.validator3]
```

Nodes whose names start with `val` are treated as validators. All others are
non-validator (full) nodes.

### Options

| Field              | Level  | Description                                      | Default            |
|--------------------|--------|--------------------------------------------------|--------------------|
| `name`             | top    | Testnet name (derived from filename if omitted)  | manifest file stem |
| `image`            | top    | Docker image name                                | `malachite:latest` |
| `config`           | top    | Global malachite config overrides (TOML table)   | `{}`               |
| `config`           | node   | Per-node config overrides (merged with global)   | `{}`               |
| `start_at`         | node   | Start this node only after others reach height N | start immediately  |
| `persistent_peers` | node   | Explicit peer list (overrides auto-wiring)        | all other nodes    |

### Example with overrides

```toml
name = "my-testnet"
image = "malachite:custom"

[config]
consensus.timeout_propose = "3s"

[nodes.validator1]
[nodes.validator2]
[nodes.validator3]

[nodes.fullnode1]
start_at = 5
```

## Commands

| Command   | Description                                                         |
|-----------|---------------------------------------------------------------------|
| `setup`   | Generate all config files, keys, genesis, and Docker Compose files  |
| `build`   | Build the Docker image                                              |
| `start`   | Start the testnet (auto-runs setup and build if needed)             |
| `stop`    | Stop the testnet or specific nodes                                  |
| `clean`   | Stop everything and remove all generated files                      |
| `perturb` | Apply perturbations to nodes (disconnect, kill, pause, restart)     |
| `logs`    | Show container logs                                                 |
| `info`    | Display testnet state, node metadata, and current heights           |
| `wait`    | Wait for nodes to reach a specific block height                     |

### Node selection with wildcards

Commands that accept node names support glob wildcards:

```bash
quake stop validator1          # stop a single node
quake stop val*                # stop all validators
quake stop val*1               # stop validator1
quake logs *1                  # logs for all nodes ending in "1"
```

### Perturbations

```bash
# Disconnect all validators for 10-20s (random), then reconnect
quake perturb disconnect val*

# Kill validator1 for exactly 30s, then restart
quake perturb kill validator1 -t 30s

# Pause two nodes for 5-15s
quake perturb pause validator1 validator2 -t 5s -T 15s

# Restart all nodes immediately
quake perturb restart
```

### Waiting for height

```bash
# Wait for all nodes to reach height 100 (60s timeout)
quake wait 100

# Wait for specific nodes with custom timeout
quake wait 50 validator1 validator2 -t 120
```

## Monitoring

Quake automatically starts Prometheus and Grafana alongside the testnet:

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000

Grafana comes pre-configured with a Prometheus datasource and a default
dashboard showing consensus metrics (height, round, queue sizes, sync, P2P).

## File layout

After `quake setup`, the generated files live under `.quake/<testnet-name>/`:

```
.quake/
  .last_manifest                  # remembers the last manifest path
  ca-certificates.crt             # exported CA certs (for Docker build)
  localdev/
    compose.yaml                  # main Docker Compose file
    nodes.json                    # node metadata (IPs, ports)
    genesis.json                  # genesis file
    validator1/
      config/config.toml          # malachite config
      data/priv_validator_key.json
    validator2/
      ...
    monitoring/
      compose.yaml                # Prometheus + Grafana Compose file
      prometheus.yml              # Prometheus scrape config
      grafana/provisioning/       # Grafana datasources + dashboards
```

## Verbosity

```bash
quake start             # info-level logging (default)
quake -v start          # debug-level logging
quake -vv start         # trace-level logging
RUST_LOG=debug quake start  # override via environment
```
