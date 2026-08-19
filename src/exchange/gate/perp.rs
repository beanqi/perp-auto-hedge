use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::blocking::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha512};
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::{Message, WebSocket, connect};
use tungstenite::stream::MaybeTlsStream;

use super::super::{
    AccountEvent, AccountSnapshot, ApiCredentials, Balance, CancelOrderRequest, ExchangeResult, Fill,
    Funding, Instrument, MarketId, MarketType, Order, OrderStatus, PlaceOrderRequest, Position, Side,
    TimeInForce,
};
use crate::logging;
use crate::market::{MarketEvent, OrderBook, PriceLevel};

const EXCHANGE:&str="gate";
const REST_HOST:&str="https://api.gateio.ws";
const REST_PREFIX:&str="/api/v4";
const WS_URL:&str="wss://fx-ws.gateio.ws/v4/ws/usdt";
const SBE_WS_URL:&str="wss://fx-ws.gateio.ws/v4/ws/usdt/sbe?sbe_schema_id=1";
const RECONNECT:Duration=Duration::from_secs(1);
const SBE_SCHEMA_ID:u16=1;
const SBE_BBO_TEMPLATE_ID:u16=1;
const SBE_BBO_BLOCK_LEN:usize=59;
type Ws=WebSocket<MaybeTlsStream<std::net::TcpStream>>;

#[derive(Debug,Clone,Copy)] struct ContractMeta{multiplier:f64,min_native:f64,decimal:bool}

#[derive(Clone)]
pub struct GatePerp{http:Client,credentials:Option<ApiCredentials>,contracts:Arc<RwLock<HashMap<String,ContractMeta>>>}

impl GatePerp{
    pub fn new(credentials:Option<ApiCredentials>)->Self{Self{http:Client::new(),credentials,contracts:Arc::new(RwLock::new(HashMap::new()))}}

    pub fn fetch_instruments(&self)->ExchangeResult<Vec<Instrument>>{
        let v=self.http.get(format!("{REST_HOST}{REST_PREFIX}/futures/usdt/contracts")).send()?.error_for_status()?.json::<Value>()?;
        let rows=v.as_array().ok_or_else(||err("gate contracts invalid"))?;let mut map=HashMap::new();let mut out=vec![];
        for row in rows{if let Some(i)=parse_instrument(row){map.insert(i.market_id.symbol.clone(),ContractMeta{multiplier:i.contract_multiplier,min_native:number(row,"order_size_min").unwrap_or(1.0),decimal:row["enable_decimal"].as_bool().unwrap_or(false)});out.push(i);}}
        *self.contracts.write().map_err(|_|err("gate contract lock poisoned"))?=map;Ok(out)
    }

    pub fn fetch_balances(&self)->ExchangeResult<Vec<Balance>>{let v=self.signed_get("/futures/usdt/accounts","")?;Ok(vec![parse_balance(&v).ok_or_else(||err("gate account invalid"))?])}
    pub fn fetch_positions(&self)->ExchangeResult<Vec<Position>>{self.ensure_contracts()?;let v=self.signed_get("/futures/usdt/positions","holding=true")?;let m=self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?;Ok(v.as_array().ok_or_else(||err("gate positions invalid"))?.iter().filter_map(|x|parse_position(x,&m)).collect())}
    pub fn fetch_open_orders(&self)->ExchangeResult<Vec<Order>>{self.ensure_contracts()?;let v=self.signed_get("/futures/usdt/orders","status=open")?;let m=self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?;Ok(v.as_array().ok_or_else(||err("gate orders invalid"))?.iter().filter_map(|x|parse_order(x,&m)).collect())}
    pub fn reconcile(&self)->ExchangeResult<AccountSnapshot>{Ok(AccountSnapshot{balances:self.fetch_balances()?,positions:self.fetch_positions()?,open_orders:self.fetch_open_orders()?})}

    pub fn run_market_stream(&self,symbols:&[String],tx:Sender<MarketEvent>){if symbols.is_empty(){return;}if self.ensure_contracts().is_err(){return;}let funding=self.clone();let funding_symbols=symbols.to_vec();let funding_tx=tx.clone();thread::spawn(move||funding.run_funding_stream(&funding_symbols,funding_tx));loop{if let Err(e)=self.market_connection(symbols,&tx){logging::info(&format!("gate SBE market websocket reconnecting: {e}"));}thread::sleep(RECONNECT);}}
    pub fn run_private_stream(&self,tx:Sender<AccountEvent>){if self.ensure_contracts().is_err(){return;}loop{if let Err(e)=self.private_connection(&tx){logging::info(&format!("gate private websocket reconnecting: {e}"));}thread::sleep(RECONNECT);}}

    pub fn connect_trading(&self)->ExchangeResult<GateTradingWs>{self.ensure_contracts()?;let c=self.credentials()?.clone();let mut ws=connect_ws()?;let id=format!("{}-login",now_ms());let ts=now_secs();let sig=hmac512(&c.secret_key,&format!("api\nfutures.login\n\n{ts}"))?;
        ws.send(Message::Text(json!({"time":ts,"channel":"futures.login","event":"api","payload":{"api_key":c.api_key,"signature":sig,"timestamp":ts.to_string(),"req_id":id}}).to_string().into()))?;wait_result(&mut ws,&id)?;Ok(GateTradingWs{socket:ws,contracts:Arc::clone(&self.contracts),next_id:now_ms()})}

    fn market_connection(&self,symbols:&[String],tx:&Sender<MarketEvent>)->ExchangeResult<()> {let mut ws=connect_sbe_ws()?;ws.send(Message::Text(json!({"time":now_secs(),"channel":"futures.book_ticker","event":"subscribe","payload":symbols}).to_string().into()))?;
        loop{match ws.read()?{Message::Binary(raw)=>if let Some(e)=self.parse_sbe_book(raw.as_ref())?{tx.send(e).map_err(|_|err("market receiver closed"))?;},Message::Text(raw)=>{let v:Value=serde_json::from_str(raw.as_ref())?;if let Some(e)=v.get("error").filter(|x|!x.is_null()){return Err(err(format!("gate SBE subscription failed: {e}")));}},Message::Ping(x)=>ws.send(Message::Pong(x))?,Message::Close(_)=>return Ok(()),_=>{}}}}

    fn run_funding_stream(&self,symbols:&[String],tx:Sender<MarketEvent>){loop{if let Err(e)=self.funding_connection(symbols,&tx){logging::info(&format!("gate funding websocket reconnecting: {e}"));}thread::sleep(RECONNECT);}}
    fn funding_connection(&self,symbols:&[String],tx:&Sender<MarketEvent>)->ExchangeResult<()> {let mut ws=connect_ws()?;ws.send(Message::Text(json!({"time":now_secs(),"channel":"futures.tickers","event":"subscribe","payload":symbols}).to_string().into()))?;
        loop{match ws.read()?{Message::Text(raw)=>{let v:Value=serde_json::from_str(raw.as_ref())?;if text(&v,"event")!=Some("update")||text(&v,"channel")!=Some("futures.tickers"){continue;}if let Some(rows)=v["result"].as_array(){for row in rows{if let Some(e)=parse_funding(row){tx.send(e).map_err(|_|err("market receiver closed"))?;}}}},Message::Ping(x)=>ws.send(Message::Pong(x))?,Message::Close(_)=>return Ok(()),_=>{}}}}

    fn private_connection(&self,tx:&Sender<AccountEvent>)->ExchangeResult<()> {let account=self.signed_get("/futures/usdt/accounts","")?;let uid=account.get("user").and_then(scalar).ok_or_else(||err("gate user id missing"))?;let mut ws=connect_ws()?;self.subscribe_private(&mut ws,"futures.balances",vec![uid.clone()])?;for ch in ["futures.positions","futures.orders","futures.usertrades"]{self.subscribe_private(&mut ws,ch,vec![uid.clone(),"!all".into()])?;}
        loop{match ws.read()?{Message::Text(raw)=>{let v:Value=serde_json::from_str(raw.as_ref())?;if text(&v,"event")!=Some("update"){continue;}match text(&v,"channel"){
            Some("futures.balances")=>for b in self.fetch_balances()?{tx.send(AccountEvent::Balance(b)).map_err(|_|err("account receiver closed"))?;},
            Some("futures.positions")=>for p in self.fetch_positions()?{tx.send(AccountEvent::Position(p)).map_err(|_|err("account receiver closed"))?;},
            Some("futures.orders")=>{let m=self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?;if let Some(rows)=v["result"].as_array(){for row in rows{if let Some(o)=parse_order(row,&m){tx.send(AccountEvent::Order(o)).map_err(|_|err("account receiver closed"))?;}}}},
            Some("futures.usertrades")=>{let m=self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?;if let Some(rows)=v["result"].as_array(){for row in rows{if let Some(f)=parse_fill(row,&m){tx.send(AccountEvent::Fill(f)).map_err(|_|err("account receiver closed"))?;}}}},_=>{}}},Message::Ping(x)=>ws.send(Message::Pong(x))?,Message::Close(_)=>return Ok(()),_=>{}}}}

    fn subscribe_private(&self,ws:&mut Ws,ch:&str,payload:Vec<String>)->ExchangeResult<()> {let c=self.credentials()?;let ts=now_secs();let sig=hmac512(&c.secret_key,&format!("channel={ch}&event=subscribe&time={ts}"))?;ws.send(Message::Text(json!({"time":ts,"channel":ch,"event":"subscribe","payload":payload,"auth":{"method":"api_key","KEY":c.api_key,"SIGN":sig}}).to_string().into()))?;Ok(())}
    fn parse_sbe_book(&self,raw:&[u8])->ExchangeResult<Option<MarketEvent>>{if raw.len()<8{return Err(err("gate SBE header too short"));}let block=read_u16(raw,0)? as usize;let template=read_u16(raw,2)?;let schema=read_u16(raw,4)?;if schema!=SBE_SCHEMA_ID||template!=SBE_BBO_TEMPLATE_ID{return Ok(None);}if block<SBE_BBO_BLOCK_LEN||raw.len()<8+block{return Err(err("gate SBE BBO block invalid"));}
        let o=8;let server_time=read_i64(raw,o)?;if read_i8(raw,o+8)?!=2{return Ok(None);}let engine_time=read_i64(raw,o+9)?;let update_id=read_i64(raw,o+17)?;let px_exp=read_i8(raw,o+25)?;let sz_exp=read_i8(raw,o+26)?;let ask_px=scaled(read_i64(raw,o+27)?,px_exp);let ask_sz=scaled(read_i64(raw,o+35)?,sz_exp);let bid_px=scaled(read_i64(raw,o+43)?,px_exp);let bid_sz=scaled(read_i64(raw,o+51)?,sz_exp);
        let mut cursor=8+block;let channel=read_var_string(raw,&mut cursor)?;let symbol=read_var_string(raw,&mut cursor)?;if channel!="futures.book_ticker"{return Ok(None);}let meta=self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?.get(symbol).copied().ok_or_else(||err(format!("gate contract metadata missing: {symbol}")))?;let bids=if bid_px>0.0&&bid_sz>0.0{vec![PriceLevel{price:bid_px,quantity:bid_sz*meta.multiplier}]}else{vec![]};let asks=if ask_px>0.0&&ask_sz>0.0{vec![PriceLevel{price:ask_px,quantity:ask_sz*meta.multiplier}]}else{vec![]};let t=if engine_time>0{engine_time}else{server_time};let timestamp_ms=u64::try_from(t).ok().map(|x|x/1000).unwrap_or_else(now_ms);let sequence=u64::try_from(update_id).ok();Ok(Some(MarketEvent::OrderBook(OrderBook{market_id:market(symbol),bids,asks,timestamp_ms,sequence})))}
    fn signed_get(&self,path:&str,query:&str)->ExchangeResult<Value>{let c=self.credentials()?;let ts=now_secs();let body=sha512("");let sign_text=format!("GET\n{REST_PREFIX}{path}\n{query}\n{body}\n{ts}");let sig=hmac512(&c.secret_key,&sign_text)?;let suffix=if query.is_empty(){String::new()}else{format!("?{query}")};Ok(self.http.get(format!("{REST_HOST}{REST_PREFIX}{path}{suffix}")).header("KEY",c.api_key.as_str()).header("Timestamp",ts.to_string()).header("SIGN",sig).send()?.error_for_status()?.json::<Value>()?)}
    fn ensure_contracts(&self)->ExchangeResult<()>{if self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?.is_empty(){self.fetch_instruments()?;}Ok(())}
    fn credentials(&self)->ExchangeResult<&ApiCredentials>{self.credentials.as_ref().ok_or_else(||err("gate credentials required"))}
}

pub struct GateTradingWs{socket:Ws,contracts:Arc<RwLock<HashMap<String,ContractMeta>>>,next_id:u64}
impl GateTradingWs{
    pub fn place_order(&mut self,r:&PlaceOrderRequest)->ExchangeResult<()> {ensure_market(&r.market_id)?;let meta=*self.contracts.read().map_err(|_|err("gate contract lock poisoned"))?.get(&r.market_id.symbol).ok_or_else(||err("gate contract metadata missing"))?;let mut native=r.quantity/meta.multiplier;if native.abs()+1e-12<meta.min_native{return Err(err("gate quantity below minimum"));}native=if r.side==Side::Buy{native.abs()}else{-native.abs()};if !meta.decimal&&(native-native.round()).abs()>1e-9{return Err(err("gate contract requires integer size"));}
        let (price,tif)=match r.price{Some(p)=>(decimal(p),tif(r.time_in_force)),None=>("0".into(),"ioc")};let text=client_id(&r.client_order_id)?;let id=self.next_request_id();self.socket.send(Message::Text(json!({"time":now_secs(),"channel":"futures.order_place","event":"api","payload":{"req_id":id,"req_param":{"contract":r.market_id.symbol,"size":decimal(native),"price":price,"tif":tif,"text":text,"reduce_only":r.reduce_only}}}).to_string().into()))?;wait_result(&mut self.socket,&id)}
    pub fn cancel_order(&mut self,r:&CancelOrderRequest)->ExchangeResult<()> {ensure_market(&r.market_id)?;let id=self.next_request_id();self.socket.send(Message::Text(json!({"time":now_secs(),"channel":"futures.order_cancel","event":"api","payload":{"req_id":id,"req_param":{"order_id":r.order_id}}}).to_string().into()))?;wait_result(&mut self.socket,&id)}
    fn next_request_id(&mut self)->String{self.next_id=self.next_id.wrapping_add(1);format!("{}-{}",now_ms(),self.next_id)}
}

fn wait_result(ws:&mut Ws,id:&str)->ExchangeResult<()> {loop{match ws.read()?{Message::Text(raw)=>{let v:Value=serde_json::from_str(raw.as_ref())?;if text(&v,"request_id")!=Some(id){continue;}if v["ack"].as_bool()==Some(true){continue;}let status=v["header"]["status"].as_str().and_then(|x|x.parse::<u16>().ok()).or_else(||v["header"]["status"].as_u64().map(|x|x as u16)).unwrap_or(0);if (200..300).contains(&status){return Ok(());}return Err(err(format!("gate websocket api failed: {v}")));},Message::Ping(x)=>ws.send(Message::Pong(x))?,Message::Close(_)=>return Err(err("gate trading websocket closed")),_=>{}}}}

fn parse_instrument(v:&Value)->Option<Instrument>{if text(v,"type")!="direct".into()||text(v,"status")!="trading".into(){return None;}let s=text(v,"name")?;let(base,quote)=s.rsplit_once('_')?;let mult=number(v,"quanto_multiplier")?;let min=number(v,"order_size_min")?;Some(Instrument{market_id:market(s),base_asset:base.into(),quote_asset:quote.into(),price_tick:number(v,"order_price_round")?,quantity_step:min*mult,min_quantity:min*mult,min_notional:0.0,contract_multiplier:mult})}
fn parse_balance(v:&Value)->Option<Balance>{Some(Balance{exchange:EXCHANGE.into(),market_type:MarketType::Perp,asset:text(v,"currency")?.into(),total:number(v,"total")?,available:number(v,"available")?,timestamp_ms:now_ms()})}
fn parse_position(v:&Value,m:&HashMap<String,ContractMeta>)->Option<Position>{let s=text(v,"contract")?;let meta=m.get(s)?;let q=number(v,"size")?*meta.multiplier;Some(Position{market_id:market(s),quantity:q,entry_price:number(v,"entry_price").filter(|p|q!=0.0&&*p>0.0),unrealized_pnl:number(v,"unrealised_pnl").unwrap_or(0.0),timestamp_ms:timestamp(v,"update_time").unwrap_or_else(now_ms)})}
fn parse_order(v:&Value,m:&HashMap<String,ContractMeta>)->Option<Order>{let s=text(v,"contract")?;let meta=m.get(s)?;let size=number(v,"size")?;let left=number(v,"left").unwrap_or(size.abs());let q=size.abs()*meta.multiplier;let filled=(size.abs()-left.abs()).max(0.0)*meta.multiplier;let status=match text(v,"status")?{"open" if filled>0.0=>OrderStatus::PartiallyFilled,"open"=>OrderStatus::New,"finished" if text(v,"finish_as")==Some("filled")=>OrderStatus::Filled,"finished"=>OrderStatus::Canceled,_=>return None};Some(Order{market_id:market(s),order_id:v.get("id_string").and_then(scalar).or_else(||v.get("id").and_then(scalar))?,client_order_id:v.get("text").and_then(scalar),side:if size>=0.0{Side::Buy}else{Side::Sell},price:number(v,"price").filter(|p|*p>0.0),quantity:q,filled_quantity:filled,status,timestamp_ms:timestamp(v,"update_time").unwrap_or_else(now_ms)})}
fn parse_fill(v:&Value,m:&HashMap<String,ContractMeta>)->Option<Fill>{let s=text(v,"contract")?;let meta=m.get(s)?;let size=number(v,"size")?;Some(Fill{market_id:market(s),order_id:v.get("order_id").and_then(scalar)?,fill_id:v.get("id").and_then(scalar)?,side:if size>=0.0{Side::Buy}else{Side::Sell},price:number(v,"price")?,quantity:size.abs()*meta.multiplier,fee:number(v,"fee").unwrap_or(0.0),fee_asset:"USDT".into(),timestamp_ms:timestamp(v,"create_time_ms").or_else(||timestamp(v,"create_time")).unwrap_or_else(now_ms)})}
fn parse_funding(v:&Value)->Option<MarketEvent>{Some(MarketEvent::Funding(Funding{market_id:market(text(v,"contract")?),rate:number(v,"funding_rate")?,mark_price:number(v,"mark_price"),next_funding_time_ms:None,timestamp_ms:integer(v,"t").unwrap_or_else(now_ms)}))}
fn connect_ws()->ExchangeResult<Ws>{connect_gate_ws(WS_URL)}
fn connect_sbe_ws()->ExchangeResult<Ws>{connect_gate_ws(SBE_WS_URL)}
fn connect_gate_ws(url:&str)->ExchangeResult<Ws>{let mut r=url.into_client_request()?;r.headers_mut().insert("X-Gate-Size-Decimal",HeaderValue::from_static("1"));Ok(connect(r)?.0)}
fn read_u16(v:&[u8],o:usize)->ExchangeResult<u16>{let b:[u8;2]=v.get(o..o+2).ok_or_else(||err("gate SBE u16 out of bounds"))?.try_into().map_err(|_|err("gate SBE u16 invalid"))?;Ok(u16::from_le_bytes(b))}
fn read_i64(v:&[u8],o:usize)->ExchangeResult<i64>{let b:[u8;8]=v.get(o..o+8).ok_or_else(||err("gate SBE i64 out of bounds"))?.try_into().map_err(|_|err("gate SBE i64 invalid"))?;Ok(i64::from_le_bytes(b))}
fn read_i8(v:&[u8],o:usize)->ExchangeResult<i8>{Ok(*v.get(o).ok_or_else(||err("gate SBE i8 out of bounds"))? as i8)}
fn read_var_string<'a>(v:&'a[u8],o:&mut usize)->ExchangeResult<&'a str>{let n=*v.get(*o).ok_or_else(||err("gate SBE string length missing"))? as usize;*o+=1;let end=*o+n;let s=std::str::from_utf8(v.get(*o..end).ok_or_else(||err("gate SBE string out of bounds"))?).map_err(|_|err("gate SBE string invalid utf8"))?;*o=end;Ok(s)}
fn scaled(m:i64,e:i8)->f64{m as f64*10_f64.powi(e as i32)}
fn market(s:&str)->MarketId{MarketId{exchange:EXCHANGE.into(),market_type:MarketType::Perp,symbol:s.into()}}
fn ensure_market(m:&MarketId)->ExchangeResult<()>{if m.exchange==EXCHANGE&&m.market_type==MarketType::Perp{Ok(())}else{Err(err("wrong gate adapter"))}}
fn tif(v:TimeInForce)->&'static str{match v{TimeInForce::Gtc=>"gtc",TimeInForce::Ioc=>"ioc",TimeInForce::Fok=>"fok"}}
fn client_id(v:&str)->ExchangeResult<String>{let x=v.strip_prefix("t-").unwrap_or(v);if x.is_empty()||x.len()>28||!x.bytes().all(|b|b.is_ascii_alphanumeric()||matches!(b,b'_'|b'-'|b'.')){return Err(err("invalid gate client order id"));}Ok(format!("t-{x}"))}
fn hmac512(secret:&str,payload:&str)->ExchangeResult<String>{let mut mac=Hmac::<Sha512>::new_from_slice(secret.as_bytes()).map_err(|_|err("invalid gate secret"))?;mac.update(payload.as_bytes());Ok(hex(mac.finalize().into_bytes().as_slice()))}
fn sha512(v:&str)->String{hex(Sha512::digest(v.as_bytes()).as_slice())} fn hex(v:&[u8])->String{v.iter().map(|b|format!("{b:02x}")).collect()}
fn text<'a>(v:&'a Value,k:&str)->Option<&'a str>{v.get(k)?.as_str()} fn number(v:&Value,k:&str)->Option<f64>{v.get(k).and_then(|x|match x{Value::String(s)=>s.parse().ok(),Value::Number(n)=>n.as_f64(),_=>None})} fn integer(v:&Value,k:&str)->Option<u64>{v.get(k).and_then(|x|match x{Value::String(s)=>s.parse().ok(),Value::Number(n)=>n.as_u64(),_=>None})} fn scalar(v:&Value)->Option<String>{match v{Value::String(s)=>Some(s.clone()),Value::Number(n)=>Some(n.to_string()),_=>None}}
fn timestamp(v:&Value,k:&str)->Option<u64>{integer(v,k).map(|x|if x<10_000_000_000{x*1000}else{x})} fn decimal(v:f64)->String{let s=format!("{v:.16}");s.trim_end_matches('0').trim_end_matches('.').to_owned()} fn now_secs()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()} fn now_ms()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64} fn err(v:impl Into<String>)->Box<dyn std::error::Error+Send+Sync>{Box::new(io::Error::other(v.into()))}

#[cfg(test)] mod tests{use super::*;#[test]fn sbe_bbo_is_decoded_to_base_quantity(){let a=GatePerp::new(None);a.contracts.write().unwrap().insert("BTC_USDT".into(),ContractMeta{multiplier:0.0001,min_native:1.0,decimal:true});let mut v=vec![];v.extend_from_slice(&(SBE_BBO_BLOCK_LEN as u16).to_le_bytes());v.extend_from_slice(&SBE_BBO_TEMPLATE_ID.to_le_bytes());v.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());v.extend_from_slice(&1_u16.to_le_bytes());v.extend_from_slice(&1_700_000_000_000_000_i64.to_le_bytes());v.push(2);v.extend_from_slice(&1_700_000_000_001_000_i64.to_le_bytes());v.extend_from_slice(&42_i64.to_le_bytes());v.push((-1_i8) as u8);v.push(0);v.extend_from_slice(&1010_i64.to_le_bytes());v.extend_from_slice(&20_i64.to_le_bytes());v.extend_from_slice(&1000_i64.to_le_bytes());v.extend_from_slice(&10_i64.to_le_bytes());push(&mut v,"futures.book_ticker");push(&mut v,"BTC_USDT");let MarketEvent::OrderBook(b)=a.parse_sbe_book(&v).unwrap().unwrap()else{panic!()};assert_eq!(b.bids[0].price,100.0);assert_eq!(b.asks[0].price,101.0);assert_eq!(b.bids[0].quantity,0.001);assert_eq!(b.asks[0].quantity,0.002);assert_eq!(b.timestamp_ms,1_700_000_000_001);assert_eq!(b.sequence,Some(42));}fn push(v:&mut Vec<u8>,s:&str){v.push(s.len() as u8);v.extend_from_slice(s.as_bytes());}}
