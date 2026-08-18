use std::env;

const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Debug, Clone)]
pub struct Config {
    pub log_level: String,
}

impl Config {
    pub fn load() -> Self {
        Self {
            log_level: env::var("PERP_AUTO_HEDGE_LOG")
                .unwrap_or_else(|_| DEFAULT_LOG_LEVEL.to_string()),
        }
    }
}
