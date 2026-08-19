pub mod binance;
pub mod gate;

use std::error::Error;
use crate::logging;

pub type ExchangeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketType { Spot, Perp }

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

#[derive(Debug, Clone, PartialEq)]
pub struct Funding {
    pub market_id: MarketId,
    pub rate: f64,
    pub mark_price: Option<f64>,
    pub next_funding_time_ms: Option<u64>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Buy, Sell }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce { Gtc, Ioc, Fok }

#[derive(Debug, Clone, PartialEq)]
pub struct PlaceOrderRequest {
    pub market_id: MarketId,
    pub client_order_id: String,
    pub side: Side,
    pub price: Option<f64>,
    pub quantity: f64,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CancelOrderRequest {
    pub market_id: MarketId,
    pub order_id: String,
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
pub enum OrderStatus { New, PartiallyFilled, Filled, Canceled, Rejected }

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
pub struct AccountSnapshot {
    pub balances: Vec<Balance>,
    pub positions: Vec<Position>,
    pub open_orders: Vec<Order>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AccountEvent {
    Balance(Balance),
    Position(Position),
    Order(Order),
    Fill(Fill),
}

pub fn init() { logging::info("exchange module initialized"); }
