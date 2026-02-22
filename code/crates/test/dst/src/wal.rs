use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tracing::{debug, warn};

use malachitebft_core_types::Context;
use malachitebft_engine::wal::{Msg as WalMsg, WalEntry, WalRef};
use malachitebft_test_framework::NodeId;

/// Shared state for the simulated WAL, accessible by the controller for crash simulation.
#[derive(Debug)]
pub struct SimulatedWalStore<Ctx: Context> {
    entries: BTreeMap<Ctx::Height, Vec<(WalEntry<Ctx>, bool)>>,
    /// Whether WAL writes should currently fail (set by controller for fault injection).
    pub fail_writes: bool,
}

impl<Ctx: Context> SimulatedWalStore<Ctx> {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            fail_writes: false,
        }
    }

    /// Discard all unflushed entries (simulates crash before flush).
    pub fn discard_unflushed(&mut self) {
        for entries in self.entries.values_mut() {
            entries.retain(|(_, flushed)| *flushed);
        }
    }

    /// Clear all entries (simulates full DB reset).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl<Ctx: Context> Default for SimulatedWalStore<Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SimulatedWal<Ctx: Context> {
    node_id: NodeId,
    store: Arc<Mutex<SimulatedWalStore<Ctx>>>,
}

impl<Ctx: Context> SimulatedWal<Ctx> {
    pub async fn spawn(
        node_id: NodeId,
        store: Arc<Mutex<SimulatedWalStore<Ctx>>>,
    ) -> Result<WalRef<Ctx>, SpawnErr> {
        let actor = Self { node_id, store };
        let (actor_ref, _) = Actor::spawn(None, actor, ()).await?;
        Ok(actor_ref)
    }
}

#[async_trait]
impl<Ctx: Context> Actor for SimulatedWal<Ctx> {
    type Msg = WalMsg<Ctx>;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            WalMsg::StartedHeight(height, reply) => {
                let store = self.store.lock().unwrap();
                let entries: Vec<io::Result<WalEntry<Ctx>>> = store
                    .entries
                    .get(&height)
                    .map(|es| es.iter().map(|(e, _)| Ok(e.clone())).collect())
                    .unwrap_or_default();
                debug!(node = self.node_id, %height, count = entries.len(), "WAL replay");
                let _ = reply.send(Ok(entries));
            }

            WalMsg::Append(height, entry, reply) => {
                let mut store = self.store.lock().unwrap();
                if store.fail_writes {
                    warn!(node = self.node_id, %height, "WAL append failed (fault injected)");
                    let _ = reply.send(Err(eyre::eyre!("simulated WAL write failure")));
                } else {
                    store
                        .entries
                        .entry(height)
                        .or_default()
                        .push((entry, false));
                    let _ = reply.send(Ok(()));
                }
            }

            WalMsg::Flush(reply) => {
                let mut store = self.store.lock().unwrap();
                if store.fail_writes {
                    warn!(node = self.node_id, "WAL flush failed (fault injected)");
                    let _ = reply.send(Err(eyre::eyre!("simulated WAL flush failure")));
                } else {
                    for entries in store.entries.values_mut() {
                        for (_, flushed) in entries.iter_mut() {
                            *flushed = true;
                        }
                    }
                    let _ = reply.send(Ok(()));
                }
            }

            WalMsg::Reset(height, reply) => {
                let mut store = self.store.lock().unwrap();
                store.entries.remove(&height);
                let _ = reply.send(Ok(()));
            }

            WalMsg::Dump => {
                let store = self.store.lock().unwrap();
                debug!(
                    node = self.node_id,
                    heights = store.entries.len(),
                    "WAL dump"
                );
            }
        }
        Ok(())
    }
}
