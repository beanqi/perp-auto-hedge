use crate::exchange::{Fill, MarketId, Side};
use crate::logging;

#[derive(Debug, Clone, PartialEq)]
pub struct PlanLeg {
    pub market_id: MarketId,
    pub side: Side,
    pub quantity: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenPlan {
    pub trade_id: String,
    pub pair_id: String,
    pub leg_a: PlanLeg,
    pub leg_b: PlanLeg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClosePlan {
    pub trade_id: String,
    pub pair_id: String,
    pub leg_a: PlanLeg,
    pub leg_b: PlanLeg,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionEvent {
    OpenCompleted { trade_id: String, fills: Vec<Fill> },
    OpenFailedNoFill { trade_id: String, reason: String },
    ExecutionImbalance {
        trade_id: String,
        leg_a_filled_quantity: f64,
        leg_b_filled_quantity: f64,
    },
    CloseCompleted { trade_id: String, fills: Vec<Fill> },
    CloseFailed { trade_id: String, reason: String },
}

pub fn init() {
    logging::info("execution module initialized");
}
