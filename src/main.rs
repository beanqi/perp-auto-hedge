mod api;
mod config;
mod engine;
mod exchange;
mod execution;
mod logging;
mod market;
mod pair;
mod risk;
mod state;
mod storage;
mod strategy;

use std::error::Error;
use std::io;
use std::sync::mpsc;
use std::thread;

use config::Config;
use engine::{Engine, EngineEvent};

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load();
    logging::init(&config.log_level);
    logging::info("starting perp-auto-hedge");

    storage::init();
    exchange::init();
    market::init();
    strategy::init();
    risk::init();
    execution::init();
    api::init();

    let (event_tx, event_rx) = mpsc::channel();
    let engine_thread = thread::spawn(move || Engine::new(event_rx).run());

    event_tx.send(EngineEvent::Started)?;
    logging::info("perp-auto-hedge is running; press Enter to stop");

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    event_tx.send(EngineEvent::Shutdown)?;
    engine_thread
        .join()
        .map_err(|_| io::Error::other("engine thread panicked"))?;

    logging::info("perp-auto-hedge stopped");
    Ok(())
}
