use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::time::{Instant, sleep_until};

use crate::auth::Credentials;

#[derive(Deserialize)]
struct MarketPage {
    markets: Vec<Market>,
    cursor: String,
}

#[derive(Deserialize)]
struct Market {
    // Market ticker for subscriptions and order book output.
    ticker: String,
}

pub struct Markets {
    client: Client,
    url: reqwest::Url,
    credentials: Credentials,
    interval: Duration,
    next_request: Instant,
}

impl Markets {
    pub fn new(
        url: &str,
        credentials: Credentials,
        timeout: Duration,
        interval: Duration,
    ) -> Result<Self> {
        let mut url = reqwest::Url::parse(&format!("{}/markets", url.trim_end_matches('/')))
            .context("invalid market URL")?;
        url.query_pairs_mut()
            .append_pair("status", "open")
            .append_pair("mve_filter", "exclude")
            .append_pair("limit", "1000");
        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .build()
                .context("failed to create HTTP client")?,
            url,
            credentials,
            interval,
            next_request: Instant::now(),
        })
    }

    pub async fn fetch(&mut self) -> Result<BTreeSet<String>> {
        let mut tickers = BTreeSet::new();
        let mut cursor = String::new();
        let mut pages = 0_usize;
        loop {
            let page = self.page(&cursor).await?;
            pages += 1;
            tickers.extend(page.markets.into_iter().map(|market| market.ticker));
            cursor = page.cursor;
            if pages.is_multiple_of(50) || cursor.is_empty() {
                eprintln!(
                    "market discovery: {pages} pages, {} open non-MVE markets",
                    tickers.len()
                );
            }
            if cursor.is_empty() {
                return Ok(tickers);
            }
        }
    }

    async fn page(&mut self, cursor: &str) -> Result<MarketPage> {
        let mut url = self.url.clone();
        if !cursor.is_empty() {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        for attempt in 0..5 {
            sleep_until(self.next_request).await;
            self.next_request = Instant::now() + self.interval;
            let mut request = self.client.get(url.clone());
            for (header, value) in self.credentials.headers("GET", url.path())? {
                request = request.header(header, value);
            }
            let response = request.send().await;
            match response {
                Ok(response) if response.status().is_success() => {
                    return response.json().await.context("invalid markets response");
                }
                Ok(response)
                    if response.status() == StatusCode::TOO_MANY_REQUESTS
                        || response.status().is_server_error() =>
                {
                    let retry_secs = response
                        .headers()
                        .get("retry-after")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(1 << attempt);
                    self.next_request = self
                        .next_request
                        .max(Instant::now() + Duration::from_secs(retry_secs));
                }
                Ok(response) => bail!("markets request failed with HTTP {}", response.status()),
                Err(error) if attempt == 4 => return Err(error).context("markets request failed"),
                Err(_) => self.next_request = Instant::now() + Duration::from_secs(1 << attempt),
            }
        }
        bail!("markets request failed after five attempts")
    }
}
