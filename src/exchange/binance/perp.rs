use std::collections::BTreeMap;
use std::io;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::blocking::Client;
use serde_json::{Map, Value, json};
use sha2::Sha256;
use tungstenite::{Message, WebSocket, connect};
use tungstenite::stream::MaybeTlsStream;

use super::super::{
    AccountEvent, AccountSnapshot, ApiCredentials, Balance, CancelOrderRequest, ExchangeResult, Fill,
    Funding, Instrument, MARKET_SYMBOLS_PER_CONNECTION, MarketId, MarketType, Order, OrderStatus, PlaceOrderRequest, Position, Side,
    TimeInForce,
};
use crate::event::SystemEvent;
use crate::logging;
use crate::market::{MarketEvent, OrderBook, PriceLevel};

const EXCHANGE: &str = "binance";
const REST: &str = "https://fapi.binance.com";
const PUBLIC_WS: &str = "wss://fstream.binance.com/public/stream?streams=";
const MARKET_WS: &str = "wss://fstream.binance.com/market/stream?streams=";
const PRIVATE_WS: &str = "wss://fstream.binance.com/private/ws";
const TRADING_WS: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";
const RECONNECT: Duration = Duration::from_secs(1);

type Ws = WebSocket<MaybeTlsStream<std::net::TcpStream>>;

#[derive(Clone)]
pub struct BinancePerp {
    http: Client,
    credentials: Option<ApiCredentials>,
}

impl BinancePerp {
    pub fn new(credentials: Option<ApiCredentials>) -> Self {
        Self { http: Client::new(), credentials }
    }

    pub fn fetch_instruments(&self) -> ExchangeResult<Vec<Instrument>> {
        let value = self.http.get(format!("{REST}/fapi/v1/exchangeInfo"))
            .send()?.error_for_status()?.json::<Value>()?;
        let rows = value["symbols"].as_array().ok_or_else(|| err("binance symbols missing"))?;
        Ok(rows.iter().filter_map(parse_instrument).collect())
    }

    pub fn fetch_balances(&self) -> ExchangeResult<Vec<Balance>> {
        let value = self.signed_get("/fapi/v3/balance", &[])?;
        Ok(value.as_array().ok_or_else(|| err("binance balances invalid"))?.iter().filter_map(|row| {
            Some(Balance {
                exchange: EXCHANGE.into(), market_type: MarketType::Perp,
                asset: text(row, "asset")?.into(), total: number(row, "balance")?,
                available: number(row, "availableBalance")?,
                timestamp_ms: integer(row, "updateTime").unwrap_or_else(now_ms),
            })
        }).collect())
    }

    pub fn fetch_positions(&self) -> ExchangeResult<Vec<Position>> {
        let value = self.signed_get("/fapi/v3/positionRisk", &[])?;
        Ok(value.as_array().ok_or_else(|| err("binance positions invalid"))?.iter()
            .filter_map(parse_position).collect())
    }

    pub fn fetch_open_orders(&self) -> ExchangeResult<Vec<Order>> {
        let value = self.signed_get("/fapi/v1/openOrders", &[])?;
        Ok(value.as_array().ok_or_else(|| err("binance open orders invalid"))?.iter()
            .filter_map(parse_order).collect())
    }

    pub fn reconcile(&self) -> ExchangeResult<AccountSnapshot> {
        Ok(AccountSnapshot { balances: self.fetch_balances()?, positions: self.fetch_positions()?, open_orders: self.fetch_open_orders()? })
    }

    pub fn start_market_streams(&self, symbols: &[String], tx: Sender<SystemEvent>) -> ExchangeResult<()> {
        for (index, chunk) in symbols.chunks(MARKET_SYMBOLS_PER_CONNECTION).enumerate() {
            let exchange = self.clone(); let symbols = chunk.to_vec(); let tx = tx.clone();
            thread::Builder::new().name(format!("binance-book-{index}")).spawn(move || exchange.run_book_stream(&symbols, tx))?;
        }
        let exchange = self.clone(); let symbols = symbols.to_vec();
        thread::Builder::new().name("binance-funding".into()).spawn(move || exchange.run_funding_stream(&symbols, tx))?;
        Ok(())
    }

    pub fn run_book_stream(&self, symbols: &[String], tx: Sender<SystemEvent>) {
        if symbols.is_empty() { return; }
        let streams = symbols.iter().map(|s| format!("{}@bookTicker", s.to_lowercase())).collect::<Vec<_>>().join("/");
        run_stream(&format!("{PUBLIC_WS}{streams}"), move |v| {
            if let Some(event) = parse_book(data(v)) { tx.send(SystemEvent::Market(event)).map_err(|_| err("market receiver closed"))?; }
            Ok(())
        });
    }

    pub fn run_funding_stream(&self, symbols: &[String], tx: Sender<SystemEvent>) {
        if symbols.is_empty() { return; }
        let streams = symbols.iter().map(|s| format!("{}@markPrice@1s", s.to_lowercase())).collect::<Vec<_>>().join("/");
        run_stream(&format!("{MARKET_WS}{streams}"), move |v| {
            if let Some(event) = parse_funding(data(v)) { tx.send(SystemEvent::Market(event)).map_err(|_| err("market receiver closed"))?; }
            Ok(())
        });
    }

    pub fn run_private_stream(&self, tx: Sender<SystemEvent>) {
        loop {
            if let Err(e) = self.private_connection(&tx) { logging::info(&format!("binance private websocket reconnecting: {e}")); }
            thread::sleep(RECONNECT);
        }
    }

    pub fn connect_trading(&self) -> ExchangeResult<BinanceTradingWs> {
        let credentials = self.credentials()?.clone();
        let (socket, _) = connect(TRADING_WS)?;
        Ok(BinanceTradingWs { socket, credentials, next_id: now_ms() })
    }

    fn private_connection(&self, tx: &Sender<SystemEvent>) -> ExchangeResult<()> {
        let key = self.start_listen_key()?;
        let url = format!("{PRIVATE_WS}?listenKey={key}&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE");
        let (mut socket, _) = connect(url.as_str())?;
        loop {
            match socket.read()? {
                Message::Text(raw) => {
                    let value: Value = serde_json::from_str(raw.as_ref())?;
                    match text(data(&value), "e") {
                        Some("ORDER_TRADE_UPDATE") => for event in parse_order_update(data(&value)) { tx.send(SystemEvent::Account(event)).map_err(|_| err("account receiver closed"))?; },
                        Some("ACCOUNT_UPDATE") => for event in parse_account_update(data(&value)) { tx.send(SystemEvent::Account(event)).map_err(|_| err("account receiver closed"))?; },
                        _ => {}
                    }
                }
                Message::Ping(v) => socket.send(Message::Pong(v))?,
                Message::Close(_) => return Ok(()),
                _ => {}
            }
        }
    }

    fn start_listen_key(&self) -> ExchangeResult<String> {
        let c = self.credentials()?;
        let value = self.http.post(format!("{REST}/fapi/v1/listenKey"))
            .header("X-MBX-APIKEY", c.api_key.as_str()).send()?.error_for_status()?.json::<Value>()?;
        text(&value, "listenKey").map(str::to_owned).ok_or_else(|| err("binance listenKey missing"))
    }

    fn signed_get(&self, path: &str, params: &[(&str, &str)]) -> ExchangeResult<Value> {
        let c = self.credentials()?;
        let mut q = params.iter().map(|(k,v)| format!("{k}={v}")).collect::<Vec<_>>();
        q.push("recvWindow=5000".into()); q.push(format!("timestamp={}", now_ms()));
        let q = q.join("&");
        let sig = sign(&c.secret_key, &q)?;
        Ok(self.http.get(format!("{REST}{path}?{q}&signature={sig}"))
            .header("X-MBX-APIKEY", c.api_key.as_str()).send()?.error_for_status()?.json::<Value>()?)
    }

    fn credentials(&self) -> ExchangeResult<&ApiCredentials> {
        self.credentials.as_ref().ok_or_else(|| err("binance credentials required"))
    }
}

pub struct BinanceTradingWs { socket: Ws, credentials: ApiCredentials, next_id: u64 }

impl BinanceTradingWs {
    pub fn place_order(&mut self, r: &PlaceOrderRequest) -> ExchangeResult<()> {
        ensure_market(&r.market_id)?;
        let mut p = BTreeMap::from([
            ("apiKey".into(), self.credentials.api_key.clone()),
            ("newClientOrderId".into(), r.client_order_id.clone()),
            ("quantity".into(), decimal(r.quantity)),
            ("reduceOnly".into(), if r.reduce_only { "true" } else { "false" }.into()),
            ("recvWindow".into(), "5000".into()),
            ("side".into(), side(r.side).into()),
            ("symbol".into(), r.market_id.symbol.clone()),
            ("timestamp".into(), now_ms().to_string()),
        ]);
        if let Some(price) = r.price {
            p.insert("price".into(), decimal(price)); p.insert("type".into(), "LIMIT".into());
            p.insert("timeInForce".into(), tif(r.time_in_force).into());
        } else { p.insert("type".into(), "MARKET".into()); }
        self.send("order.place", p)
    }

    pub fn cancel_order(&mut self, r: &CancelOrderRequest) -> ExchangeResult<()> {
        ensure_market(&r.market_id)?;
        let mut p = BTreeMap::from([
            ("apiKey".into(), self.credentials.api_key.clone()), ("recvWindow".into(), "5000".into()),
            ("symbol".into(), r.market_id.symbol.clone()), ("timestamp".into(), now_ms().to_string()),
        ]);
        p.insert(if r.order_id.parse::<u64>().is_ok() { "orderId" } else { "origClientOrderId" }.into(), r.order_id.clone());
        self.send("order.cancel", p)
    }

    fn send(&mut self, method: &str, mut p: BTreeMap<String,String>) -> ExchangeResult<()> {
        let payload = p.iter().map(|(k,v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        p.insert("signature".into(), sign(&self.credentials.secret_key, &payload)?);
        let mut params = Map::new(); for (k,v) in p { params.insert(k, Value::String(v)); }
        self.next_id = self.next_id.wrapping_add(1); let id = self.next_id;
        self.socket.send(Message::Text(json!({"id":id,"method":method,"params":params}).to_string().into()))?;
        loop {
            match self.socket.read()? {
                Message::Text(raw) => { let v: Value = serde_json::from_str(raw.as_ref())?; if v["id"].as_u64()!=Some(id) { continue; }
                    let status=v["status"].as_u64().unwrap_or(0); if (200..300).contains(&status) { return Ok(()); }
                    return Err(err(format!("binance {method} failed: {v}"))); }
                Message::Ping(v) => self.socket.send(Message::Pong(v))?, Message::Close(_) => return Err(err("binance trading websocket closed")), _ => {}
            }
        }
    }
}

fn parse_instrument(v: &Value) -> Option<Instrument> {
    if text(v,"contractType")? != "PERPETUAL" || text(v,"status")? != "TRADING" { return None; }
    let filters=v["filters"].as_array()?; let pf=filter(filters,"PRICE_FILTER")?; let lf=filter(filters,"LOT_SIZE")?;
    Some(Instrument { market_id: market(text(v,"symbol")?), base_asset:text(v,"baseAsset")?.into(), quote_asset:text(v,"quoteAsset")?.into(),
        price_tick:number(pf,"tickSize")?, quantity_step:number(lf,"stepSize")?, min_quantity:number(lf,"minQty")?,
        min_notional:filter(filters,"MIN_NOTIONAL").and_then(|x| number(x,"notional")).unwrap_or(0.0), contract_multiplier:1.0 })
}
fn parse_position(v:&Value)->Option<Position>{ let q=number(v,"positionAmt")?; Some(Position{market_id:market(text(v,"symbol")?),quantity:q,
    entry_price:number(v,"entryPrice").filter(|p|q!=0.0&&*p>0.0),unrealized_pnl:number(v,"unRealizedProfit").or_else(||number(v,"unrealizedProfit")).unwrap_or(0.0),timestamp_ms:integer(v,"updateTime").unwrap_or_else(now_ms)}) }
fn parse_order(v:&Value)->Option<Order>{ Some(Order{market_id:market(text(v,"symbol")?),order_id:scalar(v.get("orderId")?)?,client_order_id:v.get("clientOrderId").and_then(scalar),
    side:parse_side(text(v,"side")?)?,price:number(v,"price").filter(|p|*p>0.0),quantity:number(v,"origQty")?,filled_quantity:number(v,"executedQty").unwrap_or(0.0),
    status:parse_status(text(v,"status")?)?,timestamp_ms:integer(v,"updateTime").or_else(||integer(v,"time")).unwrap_or_else(now_ms)}) }
fn parse_book(v:&Value)->Option<MarketEvent>{ Some(MarketEvent::OrderBook(OrderBook{market_id:market(text(v,"s")?),bids:vec![PriceLevel{price:number(v,"b")?,quantity:number(v,"B")?}],
    asks:vec![PriceLevel{price:number(v,"a")?,quantity:number(v,"A")?}],timestamp_ms:integer(v,"E").or_else(||integer(v,"T")).unwrap_or_else(now_ms),sequence:integer(v,"u")})) }
fn parse_funding(v:&Value)->Option<MarketEvent>{ Some(MarketEvent::Funding(Funding{market_id:market(text(v,"s")?),rate:number(v,"r")?,mark_price:number(v,"p"),next_funding_time_ms:integer(v,"T"),timestamp_ms:integer(v,"E").unwrap_or_else(now_ms)})) }
fn parse_order_update(v:&Value)->Vec<AccountEvent>{ let Some(o)=v.get("o") else{return vec![]}; let Some(symbol)=text(o,"s") else{return vec![]}; let Some(sd)=text(o,"S").and_then(parse_side) else{return vec![]}; let mut out=vec![];
    if let (Some(id),Some(q),Some(st))=(o.get("i").and_then(scalar),number(o,"q"),text(o,"X").and_then(parse_status)){out.push(AccountEvent::Order(Order{market_id:market(symbol),order_id:id,client_order_id:o.get("c").and_then(scalar),side:sd,price:number(o,"p").filter(|p|*p>0.0),quantity:q,filled_quantity:number(o,"z").unwrap_or(0.0),status:st,timestamp_ms:integer(v,"E").unwrap_or_else(now_ms)}));}
    if text(o,"x")==Some("TRADE")&&number(o,"l").unwrap_or(0.0)>0.0 { if let (Some(oid),Some(fid),Some(px))=(o.get("i").and_then(scalar),o.get("t").and_then(scalar),number(o,"L")){out.push(AccountEvent::Fill(Fill{market_id:market(symbol),order_id:oid,fill_id:fid,side:sd,price:px,quantity:number(o,"l").unwrap_or(0.0),fee:number(o,"n").unwrap_or(0.0),fee_asset:text(o,"N").unwrap_or("").into(),timestamp_ms:integer(o,"T").or_else(||integer(v,"E")).unwrap_or_else(now_ms)}));}}
    out }
fn parse_account_update(v:&Value)->Vec<AccountEvent>{ let mut out=vec![]; let Some(a)=v.get("a") else{return out};
    if let Some(rows)=a["B"].as_array(){for b in rows{if let (Some(asset),Some(total),Some(cross))=(text(b,"a"),number(b,"wb"),number(b,"cw")){out.push(AccountEvent::Balance(Balance{exchange:EXCHANGE.into(),market_type:MarketType::Perp,asset:asset.into(),total,available:cross,timestamp_ms:integer(v,"E").unwrap_or_else(now_ms)}));}}}
    if let Some(rows)=a["P"].as_array(){for p in rows{if let (Some(s),Some(q))=(text(p,"s"),number(p,"pa")){out.push(AccountEvent::Position(Position{market_id:market(s),quantity:q,entry_price:number(p,"ep").filter(|x|q!=0.0&&*x>0.0),unrealized_pnl:number(p,"up").unwrap_or(0.0),timestamp_ms:integer(v,"E").unwrap_or_else(now_ms)}));}}} out }

fn run_stream<F>(url:&str,mut f:F) where F:FnMut(&Value)->ExchangeResult<()> { loop { match connect(url){Ok((mut ws,_))=>loop{match ws.read(){Ok(Message::Text(raw))=>if let Ok(v)=serde_json::from_str::<Value>(raw.as_ref()){if f(&v).is_err(){return;}},Ok(Message::Ping(v))=>{if ws.send(Message::Pong(v)).is_err(){break;}},Ok(Message::Close(_))|Err(_)=>break,_=>{}}},Err(e)=>logging::info(&format!("binance websocket connect failed: {e}"))} thread::sleep(RECONNECT); } }
fn data(v:&Value)->&Value{v.get("data").unwrap_or(v)}
fn filter<'a>(v:&'a[Value],t:&str)->Option<&'a Value>{v.iter().find(|x|text(x,"filterType")==Some(t))}
fn market(s:&str)->MarketId{MarketId{exchange:EXCHANGE.into(),market_type:MarketType::Perp,symbol:s.into()}}
fn ensure_market(m:&MarketId)->ExchangeResult<()>{if m.exchange==EXCHANGE&&m.market_type==MarketType::Perp{Ok(())}else{Err(err("wrong binance adapter"))}}
fn parse_side(v:&str)->Option<Side>{match v{"BUY"=>Some(Side::Buy),"SELL"=>Some(Side::Sell),_=>None}}
fn side(v:Side)->&'static str{match v{Side::Buy=>"BUY",Side::Sell=>"SELL"}}
fn tif(v:TimeInForce)->&'static str{match v{TimeInForce::Gtc=>"GTC",TimeInForce::Ioc=>"IOC",TimeInForce::Fok=>"FOK"}}
fn parse_status(v:&str)->Option<OrderStatus>{match v{"NEW"=>Some(OrderStatus::New),"PARTIALLY_FILLED"=>Some(OrderStatus::PartiallyFilled),"FILLED"=>Some(OrderStatus::Filled),"CANCELED"|"EXPIRED"|"EXPIRED_IN_MATCH"=>Some(OrderStatus::Canceled),"REJECTED"=>Some(OrderStatus::Rejected),_=>None}}
fn sign(secret:&str,payload:&str)->ExchangeResult<String>{let mut mac=Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_|err("invalid binance secret"))?;mac.update(payload.as_bytes());Ok(hex(mac.finalize().into_bytes().as_slice()))}
fn hex(v:&[u8])->String{v.iter().map(|b|format!("{b:02x}")).collect()}
fn text<'a>(v:&'a Value,k:&str)->Option<&'a str>{v.get(k)?.as_str()}
fn number(v:&Value,k:&str)->Option<f64>{v.get(k).and_then(|x|match x{Value::String(s)=>s.parse().ok(),Value::Number(n)=>n.as_f64(),_=>None})}
fn integer(v:&Value,k:&str)->Option<u64>{v.get(k).and_then(|x|match x{Value::String(s)=>s.parse().ok(),Value::Number(n)=>n.as_u64(),_=>None})}
fn scalar(v:&Value)->Option<String>{match v{Value::String(s)=>Some(s.clone()),Value::Number(n)=>Some(n.to_string()),_=>None}}
fn decimal(v:f64)->String{let s=format!("{v:.16}");s.trim_end_matches('0').trim_end_matches('.').to_owned()}
fn now_ms()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64}
fn err(v:impl Into<String>)->Box<dyn std::error::Error+Send+Sync>{Box::new(io::Error::other(v.into()))}

#[cfg(test)] mod tests { use super::*; #[test] fn l1_book_is_unified(){let MarketEvent::OrderBook(b)=parse_book(&json!({"s":"BTCUSDT","b":"100","B":"2","a":"101","A":"3","E":1,"u":2})).unwrap() else{panic!()};assert_eq!(b.bids[0].quantity,2.0);assert_eq!(b.asks[0].price,101.0);} }