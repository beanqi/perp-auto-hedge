use crate::pair::PairDirection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemState {
    Starting,
    Running,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairState {
    WarmingUp,
    Watching,
    Opening,
    Holding,
    Closing,
    Recovering,
    Halted,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ActiveTrade {
    pub trade_id: String,
    pub pair_id: String,
    pub direction: PairDirection,
    pub leg_a_quantity: f64,
    pub leg_b_quantity: f64,
    pub entry_price_a: f64,
    pub entry_price_b: f64,
    pub opened_at_ms: u64,
    pub entry_residual: f64,
    pub entry_z: f64,
    pub entry_percentile: f64,
    pub entry_half_life_ms: u64,
    pub entry_reversion_probability: f64,
    pub expected_holding_time_ms: u64,
    pub expected_mae: f64,
}
