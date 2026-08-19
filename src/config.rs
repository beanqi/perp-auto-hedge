use std::env;

const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Debug, Clone)]
pub struct ExchangeCredentials {
    pub api_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub log_level: String,
    pub binance_credentials: Option<ExchangeCredentials>,
    pub gate_credentials: Option<ExchangeCredentials>,
}

impl Config {
    pub fn load() -> Self {
        Self {
            log_level: env::var("PERP_AUTO_HEDGE_LOG")
                .unwrap_or_else(|_| DEFAULT_LOG_LEVEL.to_string()),
            binance_credentials: load_credentials(
                "PERP_AUTO_HEDGE_BINANCE_API_KEY",
                "PERP_AUTO_HEDGE_BINANCE_SECRET_KEY",
            ),
            gate_credentials: load_credentials(
                "PERP_AUTO_HEDGE_GATE_API_KEY",
                "PERP_AUTO_HEDGE_GATE_SECRET_KEY",
            ),
        }
    }
}

fn load_credentials(api_key: &str, secret_key: &str) -> Option<ExchangeCredentials> {
    let api_key = env::var(api_key).ok()?;
    let secret_key = env::var(secret_key).ok()?;
    if api_key.is_empty() || secret_key.is_empty() {
        return None;
    }
    Some(ExchangeCredentials { api_key, secret_key })
}
