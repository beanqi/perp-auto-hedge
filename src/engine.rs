use std::sync::mpsc::Receiver;

use crate::exchange::AccountEvent;
use crate::execution::ExecutionEvent;
use crate::logging;
use crate::market::MarketEvent;

#[derive(Debug)]
pub enum EngineEvent {
    Started,
    Market(MarketEvent),
    Account(AccountEvent),
    Execution(ExecutionEvent),
    Shutdown,
}

pub struct Engine {
    events: Receiver<EngineEvent>,
}

impl Engine {
    pub fn new(events: Receiver<EngineEvent>) -> Self {
        Self { events }
    }

    pub fn run(self) {
        logging::info("engine started");

        while let Ok(event) = self.events.recv() {
            match event {
                EngineEvent::Started => logging::info("engine received started event"),
                EngineEvent::Market(_) => logging::info("engine received market event"),
                EngineEvent::Account(_) => logging::info("engine received account event"),
                EngineEvent::Execution(_) => logging::info("engine received execution event"),
                EngineEvent::Shutdown => {
                    logging::info("engine shutting down");
                    break;
                }
            }
        }
    }
}
