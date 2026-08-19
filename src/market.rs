use crate::exchange::{Funding, MarketId};
use crate::logging;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBook {
    pub market_id: MarketId,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp_ms: u64,
    pub sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    OrderBook(OrderBook),
    Funding(Funding),
}

pub fn init() {
    logging::info("market module initialized");
}
