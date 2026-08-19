pub mod binance;
pub mod gate;

use std::error::Error;
use std::sync::mpsc::Sender;
use std::thread;

use crate::config::{Config, ExchangeCredentials};
use crate::event::SystemEvent;
use crate::logging;

use binance::perp::BinancePerp;
use gate::perp::GatePerp;

pub const MARKET_SYMBOLS_PER_CONNECTION: usize = 100;

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

pub fn init() {
    logging::info("exchange module initialized");
}

pub fn start(config: &Config, events: Sender<SystemEvent>) -> ExchangeResult<()> {
    start_binance(config.binance_credentials.as_ref(), events.clone())?;
    start_gate(config.gate_credentials.as_ref(), events)?;
    Ok(())
}

fn start_binance(
    credentials: Option<&ExchangeCredentials>,
    events: Sender<SystemEvent>,
) -> ExchangeResult<()> {
    let authenticated = credentials.is_some();
    let exchange = BinancePerp::new(credentials.map(api_credentials));
    let instruments = exchange.fetch_instruments()?;
    let symbols = instruments
        .iter()
        .map(|instrument| instrument.market_id.symbol.clone())
        .collect::<Vec<_>>();

    let snapshot = if authenticated {
        Some(exchange.reconcile()?)
    } else {
        None
    };

    send_snapshot(snapshot, &events)?;
    exchange.start_market_streams(&symbols, events.clone())?;

    if authenticated {
        let private = exchange.clone();
        let private_events = events.clone();
        thread::Builder::new()
            .name("binance-private".into())
            .spawn(move || private.run_private_stream(private_events))?;
    }

    logging::info(&format!(
        "binance perp started: {} instruments, {} book connections",
        symbols.len(),
        connection_count(symbols.len())
    ));
    Ok(())
}

fn start_gate(
    credentials: Option<&ExchangeCredentials>,
    events: Sender<SystemEvent>,
) -> ExchangeResult<()> {
    let authenticated = credentials.is_some();
    let exchange = GatePerp::new(credentials.map(api_credentials));
    let instruments = exchange.fetch_instruments()?;
    let symbols = instruments
        .iter()
        .map(|instrument| instrument.market_id.symbol.clone())
        .collect::<Vec<_>>();

    let snapshot = if authenticated {
        Some(exchange.reconcile()?)
    } else {
        None
    };

    send_snapshot(snapshot, &events)?;
    exchange.start_market_streams(&symbols, events.clone())?;

    if authenticated {
        let private = exchange.clone();
        let private_events = events.clone();
        thread::Builder::new()
            .name("gate-private".into())
            .spawn(move || private.run_private_stream(private_events))?;
    }

    logging::info(&format!(
        "gate perp started: {} instruments, {} book connections",
        symbols.len(),
        connection_count(symbols.len())
    ));
    Ok(())
}

fn send_snapshot(snapshot: Option<AccountSnapshot>, events: &Sender<SystemEvent>) -> ExchangeResult<()> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };

    for balance in snapshot.balances {
        events.send(SystemEvent::Account(AccountEvent::Balance(balance)))?;
    }
    for position in snapshot.positions {
        events.send(SystemEvent::Account(AccountEvent::Position(position)))?;
    }
    for order in snapshot.open_orders {
        events.send(SystemEvent::Account(AccountEvent::Order(order)))?;
    }
    Ok(())
}

fn api_credentials(credentials: &ExchangeCredentials) -> ApiCredentials {
    ApiCredentials {
        api_key: credentials.api_key.clone(),
        secret_key: credentials.secret_key.clone(),
    }
}

fn connection_count(symbols: usize) -> usize {
    (symbols + MARKET_SYMBOLS_PER_CONNECTION - 1) / MARKET_SYMBOLS_PER_CONNECTION
}
