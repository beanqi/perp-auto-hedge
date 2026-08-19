use std::sync::mpsc::Receiver;

use crate::event::SystemEvent;
use crate::logging;

pub struct Engine {
    events: Receiver<SystemEvent>,
}

impl Engine {
    pub fn new(events: Receiver<SystemEvent>) -> Self {
        Self { events }
    }

    pub fn run(self) {
        logging::info("engine started");

        while let Ok(event) = self.events.recv() {
            match event {
                SystemEvent::Started => logging::info("engine received started event"),
                SystemEvent::Market(_) => {}
                SystemEvent::Account(_) => logging::info("engine received account event"),
                SystemEvent::Execution(_) => logging::info("engine received execution event"),
                SystemEvent::Shutdown => {
                    logging::info("engine shutting down");
                    break;
                }
            }
        }
    }
}
