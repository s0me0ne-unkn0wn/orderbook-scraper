use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{
    sync::{Mutex, mpsc, watch},
    time::{Instant, interval, sleep, sleep_until, timeout},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};

use crate::{
    auth::Credentials,
    config::{Config, SUBSCRIPTION_BATCH, TIMEOUT},
    protocol::{BookMessage, ServerMessage},
};

pub enum SocketEvent {
    Clear {
        socket: usize,
    },
    Data {
        socket: usize,
        message: BookMessage,
        receive_timestamp: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
}

#[derive(Debug)]
struct QueueFull;

impl std::fmt::Display for QueueFull {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("websocket event queue is full; stopping to prevent data loss")
    }
}

impl std::error::Error for QueueFull {}

fn emit(events: &mpsc::Sender<SocketEvent>, event: SocketEvent) -> Result<()> {
    events.try_send(event).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => anyhow::Error::new(QueueFull),
        mpsc::error::TrySendError::Closed(_) => {
            anyhow::anyhow!("websocket event receiver is closed")
        }
    })
}

pub struct ConnectPacer {
    next: Mutex<Instant>,
    interval: Duration,
}

impl ConnectPacer {
    pub fn new(interval: Duration) -> Self {
        Self {
            next: Mutex::new(Instant::now()),
            interval,
        }
    }

    async fn wait(&self) {
        let deadline = {
            let mut next = self.next.lock().await;
            let deadline = (*next).max(Instant::now());
            *next = deadline + self.interval;
            deadline
        };
        sleep_until(deadline).await;
    }
}

pub async fn worker(
    socket: usize,
    config: Arc<Config>,
    credentials: Credentials,
    mut target: watch::Receiver<BTreeSet<String>>,
    mut reconnect: mpsc::Receiver<()>,
    events: mpsc::Sender<SocketEvent>,
    pacer: Arc<ConnectPacer>,
) -> Result<()> {
    let mut failures = 0_u32;
    loop {
        while target.borrow().is_empty() {
            if target.changed().await.is_err() {
                return Ok(());
            }
        }
        pacer.wait().await;
        let start = Instant::now();
        let result = session(
            socket,
            &config,
            &credentials,
            &mut target,
            &mut reconnect,
            &events,
        )
        .await;
        emit(&events, SocketEvent::Clear { socket })?;
        if target.has_changed().is_err() {
            return Ok(());
        }
        if let Err(error) = result {
            if error.is::<QueueFull>() || events.is_closed() {
                return Err(error);
            }
            eprintln!("websocket {socket}: {error:#}");
            failures = if start.elapsed() >= Duration::from_secs(60) {
                0
            } else {
                (failures + 1).min(6)
            };
            let delay =
                Duration::from_millis(500 * (1_u64 << failures) + rand::random::<u64>() % 500);
            sleep(delay).await;
        } else {
            failures = 0;
        }
    }
}

enum Action {
    Add,
    Delete,
}

struct Pending {
    id: u64,
    action: Action,
    markets: Vec<String>,
    deadline: Instant,
}

fn acknowledge(pending: &mut Option<Pending>, id: u64, active: &mut BTreeSet<String>) {
    if let Some(command) = pending.take_if(|command| command.id == id) {
        match command.action {
            Action::Add => active.extend(command.markets),
            Action::Delete => {
                for ticker in command.markets {
                    active.remove(&ticker);
                }
            }
        }
    }
}

#[derive(Default)]
struct Sequence {
    last: Option<u64>,
}

impl Sequence {
    fn check(&mut self, sequence: u64) -> Result<()> {
        if let Some(previous) = self.last {
            ensure!(
                previous.checked_add(1) == Some(sequence),
                "orderbook sequence gap: {previous} -> {sequence}"
            );
        }
        self.last = Some(sequence);
        Ok(())
    }
}

async fn session(
    socket: usize,
    config: &Config,
    credentials: &Credentials,
    target: &mut watch::Receiver<BTreeSet<String>>,
    reconnect: &mut mpsc::Receiver<()>,
    events: &mpsc::Sender<SocketEvent>,
) -> Result<()> {
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .context("invalid websocket URL")?;
    for (header, value) in credentials.headers("GET", request.uri().path())? {
        request.headers_mut().insert(
            HeaderName::from_bytes(header.as_bytes())?,
            HeaderValue::from_str(&value)?,
        );
    }
    let (mut stream, _) = timeout(TIMEOUT, connect_async(request))
        .await
        .context("websocket connection timeout")?
        .context("websocket connection failed")?;
    eprintln!("websocket {socket}: connected");
    let mut active = BTreeSet::new();
    let mut sid = None;
    let mut sequence = Sequence::default();
    let mut command_id = 0_u64;
    let mut pending: Option<Pending> = None;
    let mut commands = interval(Duration::from_millis(config.command_interval_ms));
    commands.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let idle = sleep(TIMEOUT);
    tokio::pin!(idle);
    loop {
        tokio::select! {
            message = stream.next() => {
                let message = message.context("websocket closed")?.context("websocket read failed")?;
                let receive_timestamp = Utc::now();
                idle.as_mut().reset(Instant::now() + TIMEOUT);
                let message: ServerMessage = match message {
                    Message::Text(text) => serde_json::from_str(&text),
                    Message::Binary(bytes) => serde_json::from_slice(&bytes),
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                    Message::Close(_) => bail!("websocket server closed connection"),
                }.context("invalid websocket JSON")?;
                if let Some(sequence_number) = message.sequence() {
                    sequence.check(sequence_number)?;
                }
                let (message, sending_ts_ms) = match message {
                    ServerMessage::Subscription {id, msg} => {
                        sid = Some(msg.sid);
                        acknowledge(&mut pending, id, &mut active);
                        continue;
                    }
                    ServerMessage::Ok {id, ..} => {
                        acknowledge(&mut pending, id, &mut active);
                        continue;
                    }
                    ServerMessage::Error {msg: error} => {
                        bail!("websocket command error {}: {}", error.code, error.msg);
                    }
                    ServerMessage::Snapshot {sending_ts_ms, msg, ..} => (BookMessage::Snapshot(msg), sending_ts_ms),
                    ServerMessage::Delta {sending_ts_ms, msg, ..} => (BookMessage::Delta(msg), sending_ts_ms),
                    ServerMessage::Other => continue,
                };
                let exchange_timestamp = message.exchange_timestamp(sending_ts_ms, receive_timestamp)?;
                emit(events, SocketEvent::Data {socket, message, receive_timestamp, exchange_timestamp})?;
            }
            _ = commands.tick() => {
                ensure!(pending.as_ref().is_none_or(|command| command.deadline > Instant::now()), "websocket command acknowledgment timeout");
                if pending.is_some() { continue; }
                let (empty, remove, add) = {
                    let desired = target.borrow();
                    (
                        desired.is_empty(),
                        active.difference(&desired).take(SUBSCRIPTION_BATCH).cloned().collect::<Vec<_>>(),
                        desired.difference(&active).take(SUBSCRIPTION_BATCH).cloned().collect::<Vec<_>>(),
                    )
                };
                if empty && active.is_empty() {
                    timeout(TIMEOUT, stream.close(None)).await.context("websocket close timeout")??;
                    return Ok(());
                }
                command_id += 1;
                let (action, markets, command) = if !remove.is_empty() {
                    (Action::Delete, remove.clone(), json!({"id":command_id,"cmd":"update_subscription","params":{"sid":sid,"market_tickers":remove,"action":"delete_markets"}}))
                } else if !add.is_empty() {
                    if let Some(subscription) = sid {
                        (Action::Add, add.clone(), json!({"id":command_id,"cmd":"update_subscription","params":{"sid":subscription,"market_tickers":add,"action":"add_markets"}}))
                    } else {
                        (Action::Add, add.clone(), json!({"id":command_id,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":add,"use_yes_price":true}}))
                    }
                } else { continue; };
                timeout(TIMEOUT, stream.send(Message::Text(command.to_string().into()))).await.context("websocket command write timeout")??;
                pending = Some(Pending {id:command_id,action,markets,deadline:Instant::now()+TIMEOUT});
            }
            change = target.changed() => {
                if change.is_err() { return Ok(()); }
            }
            Some(()) = reconnect.recv() => bail!("local book requires a new snapshot"),
            _ = &mut idle => bail!("websocket heartbeat timeout"),
        }
    }
}
