use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tempfile::TempDir;
use tracing::Instrument;

use malachitebft_app_channel::{ConsensusContext, EngineBuilder, RequestContext, SyncContext};
use malachitebft_network::PeerId;
use malachitebft_signing_ed25519::PrivateKey;
use malachitebft_test::codec::json::JsonCodec;
use malachitebft_test::middleware::Middleware;
use malachitebft_test::{
    Address, Ed25519Provider, Genesis, Height, TestContext, Validator, ValidatorSet,
};
use malachitebft_test_app::config::Config;
use malachitebft_test_app::node::Handle;
use malachitebft_test_app::state::State;
use malachitebft_test_app::store::Store;
use malachitebft_test_framework::{ConfigModifier, NodeId, NodeRunner, TestNode, TestParams};

use crate::controller::SimulationController;
use crate::fault::SimConfig;
use crate::network::SimulatedNetwork;
use crate::wal::{SimulatedWal, SimulatedWalStore};

pub struct SimNodeInfo {
    pub start_height: Height,
    pub home_dir: PathBuf,
    pub middleware: Arc<dyn Middleware>,
    pub config_modifier: ConfigModifier<Config>,
    pub wal_store: Arc<Mutex<SimulatedWalStore<TestContext>>>,
}

/// A node runner that wires nodes through simulated network and WAL actors.
#[derive(Clone)]
pub struct SimulatedNodeRunner {
    pub id: usize,
    pub seed: u64,
    pub params: TestParams,
    pub nodes_info: HashMap<NodeId, Arc<SimNodeInfo>>,
    pub private_keys: HashMap<NodeId, PrivateKey>,
    pub validator_set: ValidatorSet,
    pub controller: Arc<Mutex<SimulationController<TestContext>>>,
    pub consensus_base_port: usize,
    pub mempool_base_port: usize,
    pub metrics_base_port: usize,
    tick_loop_started: Arc<AtomicBool>,
}

fn temp_dir(id: NodeId) -> PathBuf {
    TempDir::with_prefix(format!("malachitebft-dst-{id}"))
        .unwrap()
        .keep()
}

impl SimulatedNodeRunner {
    /// Create a runner with an explicit simulation config (seed, tick duration, faults).
    pub fn with_config<S>(
        id: usize,
        nodes: &[TestNode<TestContext, S>],
        params: TestParams,
        sim_config: SimConfig,
    ) -> Self {
        let base_port = 30_000 + id * 1000;

        let (validators, private_keys) = make_validators(nodes, &params);
        let validator_set = ValidatorSet::new(validators);

        let mut ctrl = SimulationController::new(sim_config.seed, sim_config.tick_duration);
        ctrl.faults = sim_config.faults;
        let controller = Arc::new(Mutex::new(ctrl));

        let nodes_info = nodes
            .iter()
            .map(|node| {
                (
                    node.id,
                    Arc::new(SimNodeInfo {
                        start_height: node.start_height,
                        home_dir: temp_dir(node.id),
                        middleware: Arc::clone(&node.middleware),
                        config_modifier: Arc::clone(&node.config_modifier),
                        wal_store: Arc::new(Mutex::new(SimulatedWalStore::new())),
                    }),
                )
            })
            .collect();

        Self {
            id,
            seed: sim_config.seed,
            params,
            nodes_info,
            private_keys,
            validator_set,
            controller,
            consensus_base_port: base_port,
            mempool_base_port: base_port + 100,
            metrics_base_port: base_port + 200,
            tick_loop_started: Arc::new(AtomicBool::new(false)),
        }
    }

    fn generate_config(&self, node: NodeId) -> Config {
        let mut config = self.generate_default_config(node);
        self.params.apply_to_config(&mut config);

        let node_info = &self.nodes_info[&node];
        (node_info.config_modifier)(&mut config);

        config
    }

    fn generate_default_config(&self, node: NodeId) -> Config {
        use malachitebft_config::*;

        let i = node - 1;

        Config {
            moniker: format!("sim-node-{node}"),
            logging: LoggingConfig::default(),
            consensus: ConsensusConfig {
                enabled: true,
                value_payload: ValuePayload::ProposalAndParts,
                queue_capacity: 100,
                p2p: P2pConfig {
                    protocol: PubSubProtocol::default(),
                    listen_addr: TransportProtocol::Tcp
                        .multiaddr("127.0.0.1", self.consensus_base_port + i),
                    persistent_peers: Vec::new(), // Not used in simulation
                    ..Default::default()
                },
            },
            value_sync: ValueSyncConfig {
                enabled: true,
                status_update_interval: std::time::Duration::from_secs(2),
                request_timeout: std::time::Duration::from_secs(5),
                ..Default::default()
            },
            metrics: MetricsConfig {
                enabled: false,
                listen_addr: format!("127.0.0.1:{}", self.metrics_base_port + i)
                    .parse()
                    .unwrap(),
            },
            runtime: RuntimeConfig::single_threaded(),
            test: TestConfig::default(),
            byzantine: None,
        }
    }
}

#[async_trait]
impl NodeRunner<TestContext> for SimulatedNodeRunner {
    type NodeHandle = Handle;
    type Config = SimConfig;

    fn new<S>(
        id: usize,
        nodes: &[TestNode<TestContext, S>],
        params: TestParams,
        config: SimConfig,
    ) -> Self {
        Self::with_config(id, nodes, params, config)
    }

    async fn spawn(&self, id: NodeId) -> eyre::Result<Handle> {
        let config = self.generate_config(id);
        let node_info = &self.nodes_info[&id];
        let private_key = self.private_keys[&id].clone();

        let span = tracing::error_span!("node", moniker = %config.moniker);
        let _guard = span.enter();

        let middleware: Arc<dyn Middleware> = Arc::clone(&node_info.middleware);
        let ctx = TestContext::with_middleware(middleware.clone());

        let public_key = private_key.public_key();
        let address = Address::from_public_key(&public_key);

        let genesis = {
            let validators = self
                .validator_set
                .validators
                .iter()
                .map(|v| (v.public_key, v.voting_power))
                .collect::<Vec<_>>();
            Genesis {
                validator_set: ValidatorSet::new(
                    validators
                        .into_iter()
                        .map(|(pk, vp)| Validator::new(pk, vp)),
                ),
            }
        };

        // Spawn simulated WAL
        let wal_ref = SimulatedWal::spawn(id, Arc::clone(&node_info.wal_store)).await?;

        // Generate a deterministic PeerId derived from the node ID.
        // Build an identity-multihash (code=0x00, len=0x20) with 32 zero-bytes
        // except for the node ID in the first bytes.
        let peer_id = {
            let mut buf = [0u8; 34]; // 1 byte code + 1 byte length + 32 bytes digest
            buf[0] = 0x00; // identity multihash code (varint)
            buf[1] = 0x20; // digest length = 32 (varint)
            buf[2] = id as u8;
            buf[3] = (id >> 8) as u8;
            PeerId::from_bytes(&buf).expect("valid identity multihash")
        };

        // Spawn simulated network
        let (net_handle, _tx_engine_network) =
            SimulatedNetwork::spawn(id, peer_id, Arc::clone(&self.controller)).await?;

        // Register with controller
        {
            let mut ctrl = self.controller.lock().unwrap();
            ctrl.register_node(id, net_handle.clone());
        }

        // Create the app-level network channel (for PublishProposalPart messages from the app).
        // Forward them to the engine-level network actor.
        let (tx_app_network, mut rx_app_network) =
            tokio::sync::mpsc::channel::<malachitebft_app_channel::NetworkMsg<TestContext>>(256);
        {
            let net_ref = net_handle.actor_ref.clone();
            tokio::spawn(async move {
                use malachitebft_engine::network::Msg as EngineNetworkMsg;
                while let Some(msg) = rx_app_network.recv().await {
                    match msg {
                        malachitebft_app_channel::NetworkMsg::PublishProposalPart(part) => {
                            let _ = net_ref.cast(EngineNetworkMsg::PublishProposalPart(part));
                        }
                    }
                }
            });
        }

        // Start the tick loop (once, on first spawn)
        if !self.tick_loop_started.swap(true, Ordering::SeqCst) {
            SimulationController::spawn_tick_loop(Arc::clone(&self.controller));
        }

        // Build engine with custom network and custom WAL
        let builder = EngineBuilder::new(ctx.clone(), config.clone())
            .with_custom_wal(wal_ref)
            .with_custom_network(net_handle.actor_ref.clone(), tx_app_network)
            .with_default_consensus(ConsensusContext::new(
                address,
                Ed25519Provider::new(private_key.clone()),
            ))
            .with_default_sync(SyncContext::new(JsonCodec))
            .with_default_request(RequestContext::new(100));

        let (mut channels, engine_handle) = builder.build().await?;

        drop(_guard);

        let db_path = node_info.home_dir.join("db");
        std::fs::create_dir_all(&db_path)?;

        let store = Store::open(db_path.join("store.db")).await?;
        let start_height = node_info.start_height;

        // Derive a per-node RNG seed from the simulation seed + node ID
        // so each node proposes different values but is still deterministic.
        let node_rng = StdRng::seed_from_u64(self.seed.wrapping_add(id as u64));

        let mut state = State::with_rng(
            ctx,
            config,
            genesis,
            address,
            start_height,
            store,
            Ed25519Provider::new(private_key),
            Some(middleware),
            node_rng,
        );

        let tx_event = channels.events.clone();

        let app_handle = tokio::spawn(
            async move {
                if let Err(e) = malachitebft_test_app::app::run(&mut state, &mut channels).await {
                    tracing::error!("Application has failed with an error: {e}");
                }
            }
            .instrument(span),
        );

        Ok(Handle {
            app: app_handle,
            engine: engine_handle,
            tx_event,
        })
    }

    async fn reset_db(&self, id: NodeId) -> eyre::Result<()> {
        let db_dir = self.nodes_info[&id].home_dir.join("db");
        std::fs::remove_dir_all(&db_dir)?;
        std::fs::create_dir_all(&db_dir)?;
        Ok(())
    }
}

fn make_validators<S>(
    nodes: &[TestNode<TestContext, S>],
    params: &TestParams,
) -> (Vec<Validator>, HashMap<NodeId, PrivateKey>) {
    let mut rng = StdRng::seed_from_u64(0x42);

    let mut validators = Vec::new();
    let mut private_keys = HashMap::new();

    let sk = PrivateKey::generate(&mut rng);
    for &nid in params.shared_key_group.iter() {
        private_keys.insert(nid, sk.clone());
    }
    let total_power: u64 = params
        .shared_key_group
        .iter()
        .filter_map(|nid| nodes.iter().find(|n| n.id == *nid))
        .map(|n| n.voting_power)
        .sum();
    if total_power > 0 {
        validators.push(Validator::new(sk.public_key(), total_power));
    }

    for node in nodes {
        if params.shared_key_group.contains(&node.id) {
            continue;
        }
        let sk = PrivateKey::generate(&mut rng);
        let val = Validator::new(sk.public_key(), node.voting_power);

        private_keys.insert(node.id, sk);

        if node.voting_power > 0 {
            validators.push(val);
        }
    }

    (validators, private_keys)
}
