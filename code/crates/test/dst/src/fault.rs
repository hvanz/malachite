use malachitebft_test_framework::NodeId;

/// A fault scenario to inject during simulation.
#[derive(Debug, Clone)]
pub enum FaultScenario {
    /// Nodes in group_a cannot communicate with nodes in group_b.
    Partition {
        group_a: Vec<NodeId>,
        group_b: Vec<NodeId>,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Messages to/from a node are delayed by a fixed number of ticks.
    Delay {
        node: NodeId,
        delay_ticks: u64,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Messages to/from a node are dropped with given probability.
    MessageLoss {
        node: NodeId,
        drop_probability: f64,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Crash a node at a specific tick.
    CrashAt {
        node: NodeId,
        tick: u64,
        corrupt_wal: bool,
        restart_after_ticks: Option<u64>,
    },

    /// WAL write failures for a node.
    WalFailure {
        node: NodeId,
        start_tick: u64,
        duration_ticks: u64,
    },

    /// Duplicate messages from a node.
    Duplicate {
        node: NodeId,
        duplication_probability: f64,
        start_tick: u64,
        duration_ticks: u64,
    },
}

impl FaultScenario {
    /// Check if this fault is active at the given tick.
    pub fn is_active_at(&self, tick: u64) -> bool {
        match self {
            Self::Partition {
                start_tick,
                duration_ticks,
                ..
            }
            | Self::Delay {
                start_tick,
                duration_ticks,
                ..
            }
            | Self::MessageLoss {
                start_tick,
                duration_ticks,
                ..
            }
            | Self::WalFailure {
                start_tick,
                duration_ticks,
                ..
            }
            | Self::Duplicate {
                start_tick,
                duration_ticks,
                ..
            } => tick >= *start_tick && tick < start_tick + duration_ticks,
            Self::CrashAt {
                tick: crash_tick, ..
            } => tick == *crash_tick,
        }
    }

    /// Check if a message between source and destination is blocked by this fault.
    pub fn blocks_message(&self, tick: u64, source: NodeId, dest: NodeId) -> bool {
        if !self.is_active_at(tick) {
            return false;
        }
        match self {
            Self::Partition {
                group_a, group_b, ..
            } => {
                (group_a.contains(&source) && group_b.contains(&dest))
                    || (group_b.contains(&source) && group_a.contains(&dest))
            }
            _ => false,
        }
    }
}

/// Configuration for a simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub seed: u64,
    pub tick_duration: std::time::Duration,
    pub faults: Vec<FaultScenario>,
}

impl SimConfig {
    pub fn new() -> Self {
        Self {
            seed: 0,
            tick_duration: std::time::Duration::from_millis(10),
            faults: Vec::new(),
        }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_tick_duration(mut self, duration: std::time::Duration) -> Self {
        self.tick_duration = duration;
        self
    }

    pub fn with_fault(mut self, fault: FaultScenario) -> Self {
        self.faults.push(fault);
        self
    }
}

impl Default for SimConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_is_active_within_window() {
        let fault = FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![2],
            start_tick: 10,
            duration_ticks: 5,
        };
        assert!(!fault.is_active_at(9));
        assert!(fault.is_active_at(10));
        assert!(fault.is_active_at(14));
        assert!(!fault.is_active_at(15));
    }

    #[test]
    fn partition_blocks_cross_group_messages() {
        let fault = FaultScenario::Partition {
            group_a: vec![1],
            group_b: vec![2],
            start_tick: 0,
            duration_ticks: 100,
        };
        assert!(fault.blocks_message(5, 1, 2));
        assert!(fault.blocks_message(5, 2, 1));
        assert!(!fault.blocks_message(5, 1, 3));
    }

    #[test]
    fn sim_config_builder() {
        let config = SimConfig::new()
            .with_seed(42)
            .with_tick_duration(std::time::Duration::from_millis(5))
            .with_fault(FaultScenario::Delay {
                node: 1,
                delay_ticks: 3,
                start_tick: 0,
                duration_ticks: 100,
            });
        assert_eq!(config.seed, 42);
        assert_eq!(config.faults.len(), 1);
    }
}
