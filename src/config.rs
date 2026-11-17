use std::{path::PathBuf, time::Duration};

use anyhow::{Result, ensure};
use clap::Parser;

use crate::balance::MIN_CONNECTIONS;

pub const TIMEOUT: Duration = Duration::from_secs(30);
pub const SUBSCRIPTION_BATCH: usize = 200;

#[derive(Clone, Debug, Parser)]
#[command(about = "Stream Kalshi orderbooks as aligned binary SBE on stdout")]
pub struct Config {
    #[arg(
        long,
        default_value = "key.pem",
        help = "PEM file for the private API signing key"
    )]
    pub key: PathBuf,
    #[arg(long, help = "API key identifier")]
    pub key_id: String,
    #[arg(
        long,
        env = "KALSHI_REST_URL",
        default_value = "https://external-api.kalshi.com/trade-api/v2",
        help = "REST API base URL for market discovery"
    )]
    pub rest_url: String,
    #[arg(
        long,
        env = "KALSHI_WS_URL",
        default_value = "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        help = "Websocket URL for order book subscriptions"
    )]
    pub ws_url: String,
    #[arg(
        long,
        default_value_t = 192, // Kalshi's limit is 200, leaving some room for reconnections
        help = "Maximum number of websocket slots"
    )]
    pub max_connections: usize,
    #[arg(
        long,
        default_value_t = 2000,
        help = "Maximum market subscriptions per websocket slot"
    )]
    pub markets_per_socket: usize,
    #[arg(
        long,
        default_value_t = 60,
        help = "Interval between market discovery refreshes, in seconds"
    )]
    pub market_refresh_secs: u64,
    #[arg(
        long,
        default_value_t = 250,
        help = "Interval between checks for subscription changes, in milliseconds"
    )]
    pub command_interval_ms: u64,
    #[arg(
        long,
        default_value_t = 250,
        help = "Minimum interval between websocket connection attempts, in milliseconds"
    )]
    pub connect_interval_ms: u64,
    #[arg(
        long,
        default_value_t = 100,
        help = "Minimum interval between market discovery requests, in milliseconds"
    )]
    pub rest_interval_ms: u64,
    #[arg(
        long,
        default_value_t = 65536,
        help = "Maximum messages per websocket event queue and output queue"
    )]
    pub queue_capacity: usize,
    #[arg(
        long,
        default_value_t = 1,
        help = "Interval between output flushes, in seconds"
    )]
    pub output_flush_secs: u64,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_connections >= MIN_CONNECTIONS,
            "max-connections must be at least {MIN_CONNECTIONS}"
        );
        ensure!(
            self.markets_per_socket > 0,
            "markets-per-socket must be positive"
        );
        ensure!(
            self.market_refresh_secs > 0,
            "market-refresh-secs must be positive"
        );
        ensure!(
            self.command_interval_ms > 0
                && self.connect_interval_ms > 0
                && self.rest_interval_ms > 0,
            "request intervals must be positive"
        );
        ensure!(self.queue_capacity > 0, "queue-capacity must be positive");
        ensure!(
            self.output_flush_secs > 0,
            "output-flush-secs must be positive"
        );
        Ok(())
    }
}
