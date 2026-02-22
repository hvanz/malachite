use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinHandle;
use tracing::{debug, trace};

use malachitebft_core_types::Context;
use malachitebft_engine::network::NetworkEvent;
use malachitebft_test_framework::NodeId;

use crate::fault::FaultScenario;
use crate::network::SimulatedNetworkHandle;

/// A network event scheduled for delivery at a specific tick.
struct ScheduledMessage<Ctx: Context> {
    delivery_tick: u64,
    sequence: u64,
    source: NodeId,
    dest: Option<NodeId>, // None = broadcast to all others
    event: NetworkEvent<Ctx>,
}

impl<Ctx: Context> PartialEq for ScheduledMessage<Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.delivery_tick == other.delivery_tick && self.sequence == other.sequence
    }
}

impl<Ctx: Context> Eq for ScheduledMessage<Ctx> {}

impl<Ctx: Context> PartialOrd for ScheduledMessage<Ctx> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Ctx: Context> Ord for ScheduledMessage<Ctx> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse for min-heap behaviour
        other
            .delivery_tick
            .cmp(&self.delivery_tick)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// Handle to a registered node's simulated network.
pub struct SimNodeEntry<Ctx: Context> {
    pub handle: SimulatedNetworkHandle<Ctx>,
}

pub struct SimulationController<Ctx: Context> {
    pub seed: u64,
    pub rng: StdRng,
    pub nodes: HashMap<NodeId, SimNodeEntry<Ctx>>,
    message_queue: BinaryHeap<ScheduledMessage<Ctx>>,
    pub faults: Vec<FaultScenario>,
    pub current_tick: u64,
    pub tick_duration: Duration,
    sequence_counter: u64,
}

impl<Ctx: Context> SimulationController<Ctx> {
    pub fn new(seed: u64, tick_duration: Duration) -> Self {
        Self {
            seed,
            rng: StdRng::seed_from_u64(seed),
            nodes: HashMap::new(),
            message_queue: BinaryHeap::new(),
            faults: Vec::new(),
            current_tick: 0,
            tick_duration,
            sequence_counter: 0,
        }
    }

    pub fn register_node(&mut self, id: NodeId, handle: SimulatedNetworkHandle<Ctx>) {
        self.nodes.insert(id, SimNodeEntry { handle });
    }

    /// Enqueue a broadcast event from a source node to all other nodes.
    pub fn enqueue_broadcast(&mut self, source: NodeId, event: NetworkEvent<Ctx>) {
        let delivery_tick = self.compute_delivery_tick(source);
        let sequence = self.next_sequence();

        trace!(
            %source,
            tick = delivery_tick,
            "Enqueuing broadcast"
        );

        self.message_queue.push(ScheduledMessage {
            delivery_tick,
            sequence,
            source,
            dest: None,
            event,
        });
    }

    /// Enqueue a directed event from source to a specific destination.
    pub fn enqueue_directed(&mut self, source: NodeId, dest: NodeId, event: NetworkEvent<Ctx>) {
        let delivery_tick = self.compute_delivery_tick(source);
        let sequence = self.next_sequence();

        trace!(
            %source,
            %dest,
            tick = delivery_tick,
            "Enqueuing directed message"
        );

        self.message_queue.push(ScheduledMessage {
            delivery_tick,
            sequence,
            source,
            dest: Some(dest),
            event,
        });
    }

    fn compute_delivery_tick(&self, source: NodeId) -> u64 {
        let mut delay = 1; // minimum 1-tick delivery delay

        for fault in &self.faults {
            if let FaultScenario::Delay {
                node,
                delay_ticks,
                start_tick,
                duration_ticks,
            } = fault
            {
                if *node == source
                    && self.current_tick >= *start_tick
                    && self.current_tick < start_tick + duration_ticks
                {
                    delay = delay.max(*delay_ticks);
                }
            }
        }

        self.current_tick + delay
    }

    fn is_blocked(&mut self, source: NodeId, dest: NodeId) -> bool {
        for fault in &self.faults {
            if fault.blocks_message(self.current_tick, source, dest) {
                return true;
            }

            if let FaultScenario::MessageLoss {
                node,
                drop_probability,
                start_tick,
                duration_ticks,
            } = fault
            {
                if *node == source
                    && self.current_tick >= *start_tick
                    && self.current_tick < start_tick + duration_ticks
                    && self.rng.gen_bool(*drop_probability)
                {
                    return true;
                }
            }
        }
        false
    }

    fn should_duplicate(&mut self, source: NodeId) -> bool {
        for fault in &self.faults {
            if let FaultScenario::Duplicate {
                node,
                duplication_probability,
                start_tick,
                duration_ticks,
            } = fault
            {
                if *node == source
                    && self.current_tick >= *start_tick
                    && self.current_tick < start_tick + duration_ticks
                    && self.rng.gen_bool(*duplication_probability)
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn next_message_tick(&self) -> Option<u64> {
        self.message_queue.peek().map(|m| m.delivery_tick)
    }

    /// Drain all messages scheduled for the current tick.
    /// Blocked messages are dropped.
    pub fn drain_current_tick(&mut self) -> Vec<(NodeId, NetworkEvent<Ctx>)> {
        let mut deliveries = Vec::new();

        while let Some(scheduled) = self.message_queue.peek() {
            if scheduled.delivery_tick > self.current_tick {
                break;
            }

            let scheduled = self.message_queue.pop().unwrap();

            match scheduled.dest {
                None => {
                    let dest_ids: Vec<NodeId> = self
                        .nodes
                        .keys()
                        .filter(|id| **id != scheduled.source)
                        .copied()
                        .collect();

                    for dest in dest_ids {
                        if !self.is_blocked(scheduled.source, dest) {
                            deliveries.push((dest, scheduled.event.clone()));
                            if self.should_duplicate(scheduled.source) {
                                debug!(source = scheduled.source, %dest, "Duplicated message (fault)");
                                deliveries.push((dest, scheduled.event.clone()));
                            }
                        } else {
                            debug!(source = scheduled.source, %dest, "Dropped message (fault)");
                        }
                    }
                }
                Some(dest) => {
                    if !self.is_blocked(scheduled.source, dest) {
                        let dup = self.should_duplicate(scheduled.source);
                        deliveries.push((dest, scheduled.event.clone()));
                        if dup {
                            debug!(source = scheduled.source, %dest, "Duplicated directed message (fault)");
                            deliveries.push((dest, scheduled.event));
                        }
                    } else {
                        debug!(source = scheduled.source, %dest, "Dropped directed message (fault)");
                    }
                }
            }
        }

        deliveries
    }

    pub fn advance_tick(&mut self) {
        self.current_tick += 1;
    }

    pub fn has_pending_messages(&self) -> bool {
        !self.message_queue.is_empty()
    }

    fn next_sequence(&mut self) -> u64 {
        let seq = self.sequence_counter;
        self.sequence_counter += 1;
        seq
    }

    /// Spawn the tick loop that advances time and delivers messages.
    ///
    /// Uses `tokio::time::sleep` which, with `start_paused = true`, will
    /// auto-advance the clock deterministically.
    pub fn spawn_tick_loop(controller: Arc<Mutex<Self>>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let tick_duration = {
                    let ctrl = controller.lock().unwrap();
                    ctrl.tick_duration
                };

                // Sleep one tick (auto-advances with paused time)
                tokio::time::sleep(tick_duration).await;
                tokio::task::yield_now().await;

                // Drain and deliver messages for this tick
                let deliveries = {
                    let mut ctrl = controller.lock().unwrap();
                    ctrl.advance_tick();
                    ctrl.drain_current_tick()
                };

                for (dest_node, event) in deliveries {
                    let handle = {
                        let ctrl = controller.lock().unwrap();
                        ctrl.nodes.get(&dest_node).map(|e| e.handle.clone())
                    };
                    if let Some(handle) = handle {
                        handle.deliver_event(event);
                    }
                }

                // Yield to let actors process delivered messages
                tokio::task::yield_now().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::FaultScenario;
    use malachitebft_test::TestContext;

    #[test]
    fn controller_creation() {
        let ctrl = SimulationController::<TestContext>::new(42, Duration::from_millis(10));
        assert_eq!(ctrl.seed, 42);
        assert_eq!(ctrl.current_tick, 0);
        assert!(!ctrl.has_pending_messages());
    }

    #[test]
    fn next_message_tick_empty() {
        let ctrl = SimulationController::<TestContext>::new(0, Duration::from_millis(10));
        assert_eq!(ctrl.next_message_tick(), None);
    }

    #[test]
    fn duplicate_fault_doubles_deliveries() {
        let mut ctrl = SimulationController::<TestContext>::new(42, Duration::from_millis(10));
        ctrl.faults.push(FaultScenario::Duplicate {
            node: 1,
            duplication_probability: 1.0,
            start_tick: 0,
            duration_ticks: 100,
        });

        assert!(ctrl.should_duplicate(1));
        assert!(!ctrl.should_duplicate(2));
    }
}
