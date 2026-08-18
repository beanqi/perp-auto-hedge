use std::sync::mpsc::Receiver;

use crate::logging;

#[derive(Debug)]
pub enum EngineEvent {
    Started,
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
                EngineEvent::Shutdown => {
                    logging::info("engine shutting down");
                    break;
                }
            }
        }
    }
}
