use clap::Parser;
use engineering_challenge::{config::Config, runtime};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    runtime::run(Config::parse()).await
}
