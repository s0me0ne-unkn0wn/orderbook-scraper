use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use engineering_challenge::config::Config;
use engineering_challenge::runtime;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use futures_util::{SinkExt, StreamExt};
use rand::{SeedableRng, rngs::StdRng};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Message;

const CHUNK_BYTES: usize = 64 << 20;
static SIGNAL_LOCK: Mutex<()> = Mutex::const_new(());

struct Command {
    connection: usize,
    action: String,
    tickers: Vec<String>,
}

#[derive(Default)]
struct Log {
    commands: Vec<Command>,
    active: HashMap<usize, BTreeSet<String>>,
    pongs: BTreeSet<usize>,
}

struct Fixture {
    _directory: tempfile::TempDir,
    config: Config,
    output: PathBuf,
    log: Arc<Mutex<Log>>,
    http: JoinHandle<Result<()>>,
    websocket: JoinHandle<Result<()>>,
}

impl Fixture {
    async fn start(refresh_secs: u64) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let key_path = directory.path().join("test.pem");
        let key = RsaPrivateKey::new(&mut StdRng::seed_from_u64(31), 1024)?;
        std::fs::write(&key_path, key.to_pkcs8_pem(Default::default())?.as_bytes())?;
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;
        let websocket_listener = TcpListener::bind("127.0.0.1:0").await?;
        let http_url = format!("http://{}/trade-api/v2", http_listener.local_addr()?);
        let websocket_url = format!("ws://{}/trade-api/ws/v2", websocket_listener.local_addr()?);
        let config = Config {
            max_connections: 2,
            markets_per_socket: 2,
            command_interval_ms: 5,
            connect_interval_ms: 5,
            rest_interval_ms: 5,
            queue_capacity: 1024,
            market_refresh_secs: refresh_secs,
            ..Config::try_parse_from([
                "scraper".to_owned(),
                format!("--key={}", key_path.display()),
                "--key-id=runtime-test-id".to_owned(),
                format!("--rest-url={http_url}"),
                format!("--ws-url={websocket_url}"),
            ])?
        };
        let output = directory.path().join("books.sbe");
        let log = Arc::new(Mutex::new(Log::default()));
        let http = tokio::spawn(http_server(http_listener));
        let websocket = tokio::spawn(websocket_server(websocket_listener, log.clone()));
        Ok(Self {
            _directory: directory,
            config,
            output,
            log,
            http,
            websocket,
        })
    }

    async fn run(mut self) -> Result<Self> {
        tokio::select! {
            result = runtime::run_to(self.config.clone(), &self.output) => result,
            result = &mut self.http => {
                result.context("HTTP server task failed")??;
                bail!("HTTP server stopped before runtime");
            }
            result = &mut self.websocket => {
                result.context("websocket server task failed")??;
                bail!("websocket server stopped before runtime");
            }
        }?;
        Ok(self)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.http.abort();
        self.websocket.abort();
    }
}

async fn stop_runtime(runtime: JoinHandle<Result<Fixture>>, signal: &str) -> Result<Fixture> {
    let status = std::process::Command::new("kill")
        .args([signal, &std::process::id().to_string()])
        .status()
        .context("failed to signal runtime test process")?;
    ensure!(
        status.success(),
        "failed to send {signal} to runtime test process"
    );
    tokio::time::timeout(Duration::from_secs(10), runtime)
        .await
        .context("runtime did not stop after signal")?
        .context("runtime test task failed")?
}

async fn http_server(listener: TcpListener) -> Result<()> {
    let mut rounds = 0;
    loop {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut buffer = [0_u8; 1024];
            let count = stream.read(&mut buffer).await?;
            ensure!(
                count > 0 && request.len() < 16384,
                "incomplete HTTP request"
            );
            request.extend_from_slice(&buffer[..count]);
        }
        let request = String::from_utf8(request)?;
        let target = request
            .split_whitespace()
            .nth(1)
            .context("HTTP request has no target")?;
        let url = reqwest::Url::parse(&format!("http://localhost{target}"))?;
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        ensure!(
            url.path() == "/trade-api/v2/markets",
            "unexpected REST path"
        );
        for (field, value) in [
            ("status", "open"),
            ("mve_filter", "exclude"),
            ("limit", "1000"),
        ] {
            ensure!(
                query.get(field).map(String::as_str) == Some(value),
                "market discovery has no {field}={value} filter"
            );
        }
        ensure!(
            request
                .to_ascii_lowercase()
                .contains("kalshi-access-key: runtime-test-id"),
            "REST request has no API key ID"
        );
        let cursor = query.get("cursor").map(String::as_str);
        let (current, second_page) = match cursor {
            None => {
                rounds += 1;
                (rounds > 1, false)
            }
            Some("initial-page2") => (false, true),
            Some("current-page2") => (true, true),
            Some(_) => bail!("unexpected market cursor"),
        };
        let tickers = match (current, second_page) {
            (false, false) => ["A", "B"],
            (false, true) => ["C", "D"],
            (true, false) => ["B", "C"],
            (true, true) => ["D", "E"],
        };
        let markets: Vec<_> = tickers.into_iter().map(market).collect();
        let cursor = if second_page {
            ""
        } else if current {
            "current-page2"
        } else {
            "initial-page2"
        };
        let body = json!({"markets":markets,"cursor":cursor}).to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;
    }
}

fn market(ticker: &str) -> Value {
    json!({"ticker":ticker})
}

async fn websocket_server(listener: TcpListener, log: Arc<Mutex<Log>>) -> Result<()> {
    let mut streams = JoinSet::new();
    let mut connection = 0;
    loop {
        tokio::select! {
            stream = listener.accept() => {
                let (stream, _) = stream?;
                streams.spawn(websocket_session(stream, connection, log.clone()));
                connection += 1;
            }
            result = streams.join_next(), if !streams.is_empty() => {
                result.context("websocket server task set is empty")?.context("websocket session task failed")??;
            }
        }
    }
}

#[allow(clippy::result_large_err)]
async fn websocket_session(
    stream: TcpStream,
    connection: usize,
    log: Arc<Mutex<Log>>,
) -> Result<()> {
    let mut websocket = tokio_tungstenite::accept_hdr_async(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            assert_eq!(request.uri().path(), "/trade-api/ws/v2");
            assert_eq!(request.headers()["kalshi-access-key"], "runtime-test-id");
            assert!(request.headers().contains_key("kalshi-access-signature"));
            Ok(response)
        },
    )
    .await?;
    let mut sequence = 0_u64;
    let sid = connection as u64 + 1;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        let message = tokio::select! {
            message = websocket.next() => {
                let Some(message) = message else { return Ok(()); };
                message
            }
            _ = heartbeat.tick() => {
                websocket.send(Message::Ping(Vec::new().into())).await?;
                continue;
            }
        };
        let message = match message {
            Ok(Message::Text(text)) => text,
            Ok(Message::Pong(_)) => {
                log.lock().await.pongs.insert(connection);
                continue;
            }
            Ok(Message::Close(_)) | Err(_) => return Ok(()),
            Ok(_) => continue,
        };
        let command: Value = serde_json::from_str(&message)?;
        let tickers: Vec<String> =
            serde_json::from_value(command["params"]["market_tickers"].clone())?;
        let action = if command["cmd"] == "subscribe" {
            ensure!(
                command["params"]["use_yes_price"] == true,
                "subscription lacks YES-leg price mode"
            );
            "subscribe"
        } else {
            command["params"]["action"]
                .as_str()
                .context("subscription command has no action")?
        };
        {
            let mut log = log.lock().await;
            let active = log.active.entry(connection).or_default();
            if action == "delete_markets" {
                for ticker in &tickers {
                    active.remove(ticker);
                }
            } else {
                active.extend(tickers.iter().cloned());
            }
            ensure!(active.len() <= 2, "websocket exceeds market capacity");
            log.commands.push(Command {
                connection,
                action: action.to_owned(),
                tickers: tickers.clone(),
            });
        }
        let acknowledgment = if action == "subscribe" {
            json!({"id":command["id"], "type":"subscribed", "msg":{"channel":"orderbook_delta","sid":sid}})
        } else {
            sequence += 1;
            json!({"id":command["id"], "type":"ok", "sid":sid, "seq":sequence})
        };
        websocket
            .send(Message::Text(acknowledgment.to_string().into()))
            .await?;
        for ticker in &tickers {
            if action == "delete_markets" {
                sequence += 1;
                websocket.send(Message::Text(json!({
                    "type":"orderbook_delta", "sid":sid,"seq":sequence,
                    "msg":{"market_ticker":ticker,"side":"yes","price_dollars":"0.4","delta_fp":"100.00"}
                }).to_string().into())).await?;
                sequence += 1;
                websocket.send(Message::Text(json!({
                    "type":"orderbook_snapshot", "sid":sid,"seq":sequence,
                    "msg":{"market_ticker":ticker,"yes_dollars_fp":[["0.4","100.00"]],"no_dollars_fp":[]}
                }).to_string().into())).await?;
                continue;
            }
            let mut payloads = vec![
                (
                    "orderbook_snapshot",
                    json!({"market_ticker":ticker,"yes_dollars_fp":[["0.4","2.00"]],"no_dollars_fp":[["0.6","3.50"]]}),
                ),
                (
                    "orderbook_delta",
                    json!({"market_ticker":ticker,"side":"yes","price_dollars":"0.4","delta_fp":"-0.75","ts_ms":1_700_000_000_000_i64}),
                ),
                (
                    "orderbook_delta",
                    json!({"market_ticker":ticker,"side":"no","price_dollars":"0.6","delta_fp":"0.25","ts_ms":1_700_000_000_001_i64}),
                ),
            ];
            if ticker == "C" {
                payloads.push(("orderbook_snapshot", json!({"market_ticker":ticker,"yes_dollars_fp":[["0.45","1.00"]],"no_dollars_fp":[]})));
                payloads.push(("orderbook_delta", json!({"market_ticker":ticker,"side":"yes","price_dollars":"0.45","delta_fp":"0.50","ts_ms":1_700_000_000_002_i64})));
            }
            for (kind, payload) in payloads {
                sequence += 1;
                websocket
                    .send(Message::Text(
                        json!({"type":kind,"sid":sid,"seq":sequence,"msg":payload})
                            .to_string()
                            .into(),
                    ))
                    .await?;
            }
        }
    }
}

#[derive(Default)]
struct Records {
    snapshots: Vec<SnapshotDomain>,
    updates: Vec<PriceLevelUpdateDomain>,
}

fn records(path: &Path) -> Result<Records> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut result = Records::default();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(snapshot) => result
                .snapshots
                .push(SnapshotDomain::try_from_decoder(snapshot)?),
            AnyMessage::PriceLevelUpdate(update) => result
                .updates
                .push(PriceLevelUpdateDomain::try_from_decoder(update)?),
            _ => bail!("unknown SBE output message"),
        }
    }
    Ok(result)
}

async fn wait_for_output(path: &Path, ready: impl Fn(&Records) -> bool) -> Result<Records> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut chunk_count = 0;
        loop {
            if let Ok(metadata) = std::fs::metadata(path) {
                let current_chunks = metadata.len() / CHUNK_BYTES as u64;
                if current_chunks > chunk_count {
                    chunk_count = current_chunks;
                    let output = records(path)?;
                    if ready(&output) {
                        return Ok(output);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("orderbook output did not match test condition")?
}

fn quantities(records: &Records, ticker: &str) -> Vec<i64> {
    records
        .updates
        .iter()
        .filter(|update| update.symbol == ticker)
        .map(|update| update.quantity)
        .collect()
}

#[tokio::test]
async fn market_churn_changes_balanced_subscriptions_and_sbe_excludes_obsolete_updates()
-> Result<()> {
    let _signal_guard = SIGNAL_LOCK.lock().await;
    let fixture = Fixture::start(1).await?;
    let output_path = fixture.output.clone();
    let runtime = tokio::spawn(fixture.run());
    wait_for_output(&output_path, |output| output.updates.len() >= 11).await?;
    let fixture = stop_runtime(runtime, "-TERM").await?;
    let log = fixture.log.lock().await;
    let subscriptions: Vec<_> = log
        .commands
        .iter()
        .filter(|command| command.action == "subscribe")
        .collect();
    assert_eq!(subscriptions.len(), 2);
    assert_ne!(subscriptions[0].connection, subscriptions[1].connection);
    assert!(
        subscriptions
            .iter()
            .all(|command| command.tickers.len() == 2)
    );
    let initial: BTreeSet<_> = subscriptions
        .iter()
        .flat_map(|command| &command.tickers)
        .map(String::as_str)
        .collect();
    assert_eq!(initial, BTreeSet::from(["A", "B", "C", "D"]));
    for (action, ticker) in [("delete_markets", "A"), ("add_markets", "E")] {
        assert!(
            log.commands
                .iter()
                .any(|command| command.action == action && command.tickers == [ticker])
        );
    }
    let active: BTreeSet<_> = log.active.values().flatten().map(String::as_str).collect();
    assert_eq!(active, BTreeSet::from(["B", "C", "D", "E"]));
    assert!(log.active.values().all(|markets| markets.len() == 2));
    drop(log);

    let output = records(&fixture.output)?;
    let symbols: BTreeSet<_> = output
        .snapshots
        .iter()
        .map(|snapshot| snapshot.symbol.as_str())
        .collect();
    assert_eq!(symbols, BTreeSet::from(["A", "B", "C", "D", "E"]));
    assert_eq!(output.snapshots.len(), 6);
    assert_eq!(output.updates.len(), 11);
    for ticker in ["A", "B", "D", "E"] {
        assert_eq!(quantities(&output, ticker), [1_250_000, 3_750_000]);
    }
    assert_eq!(quantities(&output, "C"), [1_250_000, 3_750_000, 1_500_000]);
    let replacements: Vec<_> = output
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.symbol == "C")
        .collect();
    assert_eq!(replacements.len(), 2);
    assert_eq!(replacements[1].bids.len(), 1);
    assert_eq!(replacements[1].bids[0].price_level, 450_000);
    assert!(replacements[1].asks.is_empty());
    Ok(())
}

#[tokio::test]
async fn minute_snapshots_contain_current_quantities_and_replace_previous_depth() -> Result<()> {
    let _signal_guard = SIGNAL_LOCK.lock().await;
    let fixture = Fixture::start(600).await?;
    let output_path = fixture.output.clone();
    let log = fixture.log.clone();
    let runtime = tokio::spawn(fixture.run());
    let initial = wait_for_output(&output_path, |output| output.updates.len() >= 9).await?;
    assert_eq!(initial.snapshots.len(), 5);

    for _ in 0..6 {
        log.lock().await.pongs.clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if log.lock().await.pongs.len() == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("runtime did not respond to websocket heartbeats")?;
    }
    wait_for_output(&output_path, |output| output.snapshots.len() >= 9).await?;
    let fixture = stop_runtime(runtime, "-INT").await?;

    let output = records(&fixture.output)?;
    assert_eq!(output.snapshots.len(), 9);
    for ticker in ["A", "B", "D"] {
        let snapshot = output
            .snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.symbol == ticker)
            .context("missing minute snapshot")?;
        assert_eq!(snapshot.bids[0].quantity, 1_250_000);
        assert_eq!(snapshot.asks[0].price_level, 600_000);
        assert_eq!(snapshot.asks[0].quantity, 3_750_000);
    }
    let snapshot = output
        .snapshots
        .iter()
        .rev()
        .find(|snapshot| snapshot.symbol == "C")
        .context("missing replacement minute snapshot")?;
    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.bids[0].price_level, 450_000);
    assert_eq!(snapshot.bids[0].quantity, 1_500_000);
    assert!(snapshot.asks.is_empty());
    Ok(())
}
