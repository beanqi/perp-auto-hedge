use crate::exchange::AccountEvent;
use crate::execution::ExecutionEvent;
use crate::market::MarketEvent;

#[derive(Debug)]
pub enum SystemEvent {
    Started,
    Market(MarketEvent),
    Account(AccountEvent),
    Execution(ExecutionEvent),
    Shutdown,
}
