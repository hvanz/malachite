use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tracing::trace;

use malachitebft_core_consensus::{LivenessMsg, SignedConsensusMsg};
use malachitebft_core_types::Context;
use malachitebft_engine::network::{Msg as NetworkMsg, NetworkEvent, NetworkRef};
use malachitebft_engine::util::output_port::OutputPort;
use malachitebft_network::{Multiaddr, PeerId};
use malachitebft_test_framework::NodeId;

use crate::controller::SimulationController;

/// Handle for the controller to deliver events to a node's subscribers.
#[derive(Clone)]
pub struct SimulatedNetworkHandle<Ctx: Context> {
    /// The ractor ActorRef, used by EngineBuilder as the NetworkRef.
    pub actor_ref: NetworkRef<Ctx>,
    /// The output port through which we deliver events to local subscribers.
    output_port: Arc<OutputPort<NetworkEvent<Ctx>>>,
}

impl<Ctx: Context> SimulatedNetworkHandle<Ctx> {
    /// Deliver an inbound network event to all local subscribers.
    pub fn deliver_event(&self, event: NetworkEvent<Ctx>) {
        self.output_port.send(event);
    }
}

pub struct SimulatedNetwork<Ctx: Context> {
    node_id: NodeId,
    peer_id: PeerId,
    controller: Arc<Mutex<SimulationController<Ctx>>>,
    output_port: Arc<OutputPort<NetworkEvent<Ctx>>>,
}

pub struct SimulatedNetworkState {
    subscribers_count: usize,
    request_counter: u64,
}

impl<Ctx: Context> SimulatedNetwork<Ctx> {
    pub async fn spawn(
        node_id: NodeId,
        peer_id: PeerId,
        controller: Arc<Mutex<SimulationController<Ctx>>>,
    ) -> Result<
        (
            SimulatedNetworkHandle<Ctx>,
            tokio::sync::mpsc::Sender<NetworkMsg<Ctx>>,
        ),
        SpawnErr,
    > {
        let output_port = Arc::new(OutputPort::with_capacity(128));

        let actor = Self {
            node_id,
            peer_id,
            controller,
            output_port: Arc::clone(&output_port),
        };

        let (actor_ref, _) = Actor::spawn(None, actor, ()).await?;

        // Create the mpsc channel that EngineBuilder needs as tx_network.
        // Messages sent on this channel are forwarded to the actor.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<NetworkMsg<Ctx>>(256);
        let ref_clone = actor_ref.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let _ = ref_clone.cast(msg);
            }
        });

        let handle = SimulatedNetworkHandle {
            actor_ref,
            output_port,
        };

        Ok((handle, tx))
    }
}

#[async_trait]
impl<Ctx: Context> Actor for SimulatedNetwork<Ctx> {
    type Msg = NetworkMsg<Ctx>;
    type State = SimulatedNetworkState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(SimulatedNetworkState {
            subscribers_count: 0,
            request_counter: 0,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            // Local subscriber registration — when Consensus or Sync actors subscribe
            // to receive network events.
            NetworkMsg::Subscribe(subscriber) => {
                trace!(node = self.node_id, "Registering network subscriber");
                subscriber.subscribe_to_port(&self.output_port);
                state.subscribers_count += 1;

                // After the first subscriber registers (the consensus actor),
                // emit Listening to trigger ConsensusReady flow.
                if state.subscribers_count == 1 {
                    let fake_addr: Multiaddr =
                        format!("/ip4/127.0.0.1/tcp/{}", 40000 + self.node_id)
                            .parse()
                            .unwrap();

                    self.output_port.send(NetworkEvent::Listening(fake_addr));
                }
            }

            // Outbound: consensus message to broadcast
            NetworkMsg::PublishConsensusMsg(signed_msg) => {
                trace!(node = self.node_id, "Publishing consensus message");
                let event = match signed_msg {
                    SignedConsensusMsg::Vote(vote) => NetworkEvent::Vote(self.peer_id, vote),
                    SignedConsensusMsg::Proposal(proposal) => {
                        NetworkEvent::Proposal(self.peer_id, proposal)
                    }
                };
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(self.node_id, event);
            }

            // Outbound: liveness message to broadcast
            NetworkMsg::PublishLivenessMsg(liveness_msg) => {
                trace!(node = self.node_id, "Publishing liveness message");
                let event = match liveness_msg {
                    LivenessMsg::Vote(vote) => NetworkEvent::Vote(self.peer_id, vote),
                    LivenessMsg::PolkaCertificate(cert) => {
                        NetworkEvent::PolkaCertificate(self.peer_id, cert)
                    }
                    LivenessMsg::SkipRoundCertificate(cert) => {
                        NetworkEvent::RoundCertificate(self.peer_id, cert)
                    }
                };
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(self.node_id, event);
            }

            // Outbound: proposal part to broadcast
            NetworkMsg::PublishProposalPart(part) => {
                trace!(node = self.node_id, "Publishing proposal part");
                let event = NetworkEvent::ProposalPart(self.peer_id, part);
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(self.node_id, event);
            }

            // Outbound: status broadcast
            NetworkMsg::BroadcastStatus(status) => {
                trace!(node = self.node_id, "Broadcasting status");
                let event = NetworkEvent::Status(self.peer_id, status);
                let mut ctrl = self.controller.lock().unwrap();
                ctrl.enqueue_broadcast(self.node_id, event);
            }

            // Outbound: sync request to specific peer
            NetworkMsg::OutgoingRequest(_peer_id, _request, reply) => {
                state.request_counter += 1;
                let request_id = malachitebft_sync::OutboundRequestId::new(format!(
                    "sim-{}-{}",
                    self.node_id, state.request_counter,
                ));
                let _ = reply.send(request_id);
            }

            // Outbound: sync response
            NetworkMsg::OutgoingResponse(_request_id, _response) => {}

            NetworkMsg::DumpState(reply) => {
                let _ = reply.send(None);
            }

            NetworkMsg::UpdatePersistentPeers(_, reply) => {
                let _ = reply.send(Ok(()));
            }

            NetworkMsg::UpdateValidatorSet(_vs) => {}

            NetworkMsg::NewEvent(_event) => {}
        }

        Ok(())
    }
}
