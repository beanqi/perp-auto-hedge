use crate::exchange::MarketId;
use crate::state::{ActiveTrade, PairState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairDirection {
    ShortALongB,
    LongAShortB,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    pub id: String,
    pub base_asset: String,
    pub leg_a: MarketId,
    pub leg_b: MarketId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairRuntime {
    pub pair: Pair,
    pub state: PairState,
    pub active_trade: Option<ActiveTrade>,
}

impl PairRuntime {
    pub fn new(pair: Pair) -> Self {
        Self {
            pair,
            state: PairState::WarmingUp,
            active_trade: None,
        }
    }
}
