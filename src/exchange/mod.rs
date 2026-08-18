use crate::logging;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketType {
    Spot,
    Perp,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketId {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Instrument {
    pub market_id: MarketId,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_quantity: f64,
    pub min_notional: f64,
    pub contract_multiplier: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Balance {
    pub exchange: String,
    pub market_type: MarketType,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub market_id: MarketId,
    pub quantity: f64,
    pub entry_price: Option<f64>,
    pub unrealized_pnl: f64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub market_id: MarketId,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub side: Side,
    pub price: Option<f64>,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub status: OrderStatus,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fill {
    pub market_id: MarketId,
    pub order_id: String,
    pub fill_id: String,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AccountEvent {
    Balance(Balance),
    Position(Position),
    Order(Order),
    Fill(Fill),
}

pub fn init() {
    logging::info("exchange module initialized");
}
