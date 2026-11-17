use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Parser;
use engineering_challenge::{
    auth::Credentials,
    config::{Config, SUBSCRIPTION_BATCH},
    socket::{ConnectPacer, SocketEvent, worker},
};
use futures_util::{SinkExt, StreamExt};
use rand::{SeedableRng, rngs::StdRng};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type ServerStream = WebSocketStream<TcpStream>;

struct Harness {
    listener: TcpListener,
    target: watch::Sender<BTreeSet<String>>,
    reconnect: mpsc::Sender<()>,
    events: mpsc::Receiver<SocketEvent>,
    worker: JoinHandle<Result<()>>,
}

impl Harness {
    async fn start(tickers: &[&str]) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/trade-api/ws/v2", listener.local_addr()?);
        let config = Arc::new(Config::parse_from([
            "socket-test",
            "--key-id",
            "socket-test-key",
            "--ws-url",
            &url,
            "--command-interval-ms",
            "1",
            "--connect-interval-ms",
            "1",
        ]));
        config.validate()?;
        let directory = tempfile::tempdir()?;
        let key = RsaPrivateKey::new(&mut StdRng::seed_from_u64(23), 1024)?;
        let key_path = directory.path().join("private.pem");
        std::fs::write(&key_path, key.to_pkcs8_pem(Default::default())?.as_bytes())?;
        let credentials = Credentials::load(&key_path, "socket-test-key")?;
        let (target, target_receiver) = watch::channel(ticker_set(tickers));
        let (reconnect, reconnect_receiver) = mpsc::channel(8);
        let (sender, events) = mpsc::channel(64);
        let worker = tokio::spawn(worker(
            3,
            config,
            credentials,
            target_receiver,
            reconnect_receiver,
            sender,
            Arc::new(ConnectPacer::new(Duration::from_millis(1))),
        ));
        Ok(Self {
            listener,
            target,
            reconnect,
            events,
            worker,
        })
    }

    async fn accept(&self) -> Result<ServerStream> {
        timeout(Duration::from_secs(5), async {
            let (socket, _) = self.listener.accept().await?;
            Ok(accept_async(socket).await?)
        })
        .await
        .context("worker did not connect to test server")?
    }

    async fn event(&mut self) -> Result<SocketEvent> {
        timeout(Duration::from_secs(5), self.events.recv())
            .await
            .context("worker did not emit a socket event")?
            .context("worker event stream closed")
    }

    async fn clear(&mut self) -> Result<()> {
        match self.event().await? {
            SocketEvent::Clear { socket: 3 } => Ok(()),
            SocketEvent::Clear { socket } => bail!("unexpected socket {socket}"),
            SocketEvent::Data { message, .. } => bail!("book data before clear: {message:?}"),
        }
    }

    async fn data(&mut self, ticker: &str) -> Result<()> {
        match self.event().await? {
            SocketEvent::Data {
                socket, message, ..
            } => {
                assert_eq!(socket, 3);
                assert_eq!(message.ticker(), ticker);
                Ok(())
            }
            SocketEvent::Clear { .. } => {
                bail!("worker reset instead of processing contiguous data")
            }
        }
    }

    async fn finish(self) -> Result<()> {
        drop(self.target);
        drop(self.reconnect);
        timeout(Duration::from_secs(5), self.worker)
            .await
            .context("worker did not stop after target sender close")???;
        Ok(())
    }
}

fn ticker_set(tickers: &[&str]) -> BTreeSet<String> {
    tickers.iter().map(|ticker| (*ticker).to_owned()).collect()
}

fn command_tickers(command: &Value) -> BTreeSet<String> {
    command["params"]["market_tickers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ticker| ticker.as_str().unwrap().to_owned())
        .collect()
}

async fn command(stream: &mut ServerStream) -> Result<Value> {
    let message = timeout(Duration::from_secs(5), stream.next())
        .await
        .context("worker did not send a websocket command")?
        .context("worker closed before websocket command")??;
    Ok(serde_json::from_slice(&message.into_data())?)
}

async fn send(stream: &mut ServerStream, value: Value) -> Result<()> {
    stream.send(Message::Text(value.to_string().into())).await?;
    Ok(())
}

async fn subscribe_ack(stream: &mut ServerStream, command: &Value, sid: u64) -> Result<()> {
    send(stream, json!({"id":command["id"], "type":"subscribed", "msg":{"channel":"orderbook_delta", "sid":sid}})).await
}

async fn update_ack(
    stream: &mut ServerStream,
    command: &Value,
    sid: u64,
    sequence: u64,
) -> Result<()> {
    send(
        stream,
        json!({"id":command["id"], "type":"ok", "sid":sid, "seq":sequence, "msg":{}}),
    )
    .await
}

async fn snapshot(stream: &mut ServerStream, ticker: &str, sid: u64, sequence: u64) -> Result<()> {
    send(
        stream,
        json!({
            "type":"orderbook_snapshot", "sid":sid, "seq":sequence,
            "msg":{"market_ticker":ticker, "yes_dollars_fp":[["0.4000", "2.00"]]}
        }),
    )
    .await
}

#[tokio::test]
async fn batched_subscription_changes_preserve_sequence_across_markets_and_control_acks()
-> Result<()> {
    let tickers: Vec<_> = (0..=SUBSCRIPTION_BATCH)
        .map(|index| format!("MARKET-{index:04}"))
        .collect();
    let ticker_refs: Vec<_> = tickers.iter().map(String::as_str).collect();
    let universe = ticker_set(&ticker_refs);
    let mut harness = Harness::start(&ticker_refs).await?;
    let mut stream = harness.accept().await?;
    let subscribe = command(&mut stream).await?;
    assert_eq!(subscribe["cmd"], "subscribe");
    assert_eq!(subscribe["params"]["channels"], json!(["orderbook_delta"]));
    assert_eq!(subscribe["params"]["use_yes_price"], true);
    let first = command_tickers(&subscribe);
    assert_eq!(first.len(), SUBSCRIPTION_BATCH);
    assert!(first.is_subset(&universe));
    subscribe_ack(&mut stream, &subscribe, 19).await?;
    let mut sequence = 0_u64;
    for ticker in &first {
        sequence += 1;
        snapshot(&mut stream, ticker, 19, sequence).await?;
        harness.data(ticker).await?;
    }
    let add = command(&mut stream).await?;
    assert_eq!(add["cmd"], "update_subscription");
    assert_eq!(add["params"]["action"], "add_markets");
    assert_eq!(add["params"]["sid"], 19);
    let remaining = universe.difference(&first).cloned().collect();
    assert_eq!(command_tickers(&add), remaining);
    stream
        .send(Message::Ping(b"heartbeat".to_vec().into()))
        .await?;
    let pong = timeout(Duration::from_secs(1), stream.next())
        .await
        .context("worker did not reply to heartbeat")?
        .context("worker closed before pong")??;
    assert_eq!(pong, Message::Pong(b"heartbeat".to_vec().into()));
    sequence += 1;
    update_ack(&mut stream, &add, 19, sequence).await?;
    let last = command_tickers(&add).into_iter().next().unwrap();
    sequence += 1;
    snapshot(&mut stream, &last, 19, sequence).await?;
    harness.data(&last).await?;

    let remove_ticker = first.first().unwrap();
    let update_ticker = first.iter().nth(1).unwrap();
    let mut desired = universe.clone();
    desired.remove(remove_ticker);
    desired.insert("NEW-MARKET".to_owned());
    harness.target.send(desired)?;
    let delete = command(&mut stream).await?;
    assert_eq!(delete["params"]["action"], "delete_markets");
    assert_eq!(command_tickers(&delete), ticker_set(&[remove_ticker]));
    sequence += 1;
    send(&mut stream, json!({"type":"orderbook_delta", "sid":19, "seq":sequence, "msg":{"market_ticker":update_ticker,"side":"yes","price_dollars":"0.4","delta_fp":"1"}})).await?;
    harness.data(update_ticker).await?;
    sequence += 1;
    update_ack(&mut stream, &delete, 19, sequence).await?;
    let add = command(&mut stream).await?;
    assert_eq!(add["params"]["action"], "add_markets");
    assert_eq!(command_tickers(&add), ticker_set(&["NEW-MARKET"]));
    sequence += 1;
    update_ack(&mut stream, &add, 19, sequence).await?;
    sequence += 1;
    snapshot(&mut stream, "NEW-MARKET", 19, sequence).await?;
    harness.data("NEW-MARKET").await?;
    harness.finish().await
}

#[tokio::test]
async fn sequence_gap_invalidates_all_socket_books_before_fresh_connection_snapshots() -> Result<()>
{
    let mut harness = Harness::start(&["A", "B"]).await?;
    let mut stream = harness.accept().await?;
    let subscribe = command(&mut stream).await?;
    subscribe_ack(&mut stream, &subscribe, 11).await?;
    snapshot(&mut stream, "A", 11, 10).await?;
    harness.data("A").await?;
    snapshot(&mut stream, "B", 11, 12).await?;
    harness.clear().await?;
    let mut recovery = harness.accept().await?;
    let subscribe = command(&mut recovery).await?;
    assert_eq!(subscribe["cmd"], "subscribe");
    assert_eq!(command_tickers(&subscribe), ticker_set(&["A", "B"]));
    subscribe_ack(&mut recovery, &subscribe, 22).await?;
    snapshot(&mut recovery, "A", 22, 1).await?;
    snapshot(&mut recovery, "B", 22, 2).await?;
    harness.data("A").await?;
    harness.data("B").await?;
    harness.finish().await
}

#[tokio::test]
async fn command_error_reconnects_with_the_complete_target_instead_of_accepting_failed_changes()
-> Result<()> {
    let mut harness = Harness::start(&["A"]).await?;
    let mut stream = harness.accept().await?;
    let subscribe = command(&mut stream).await?;
    subscribe_ack(&mut stream, &subscribe, 7).await?;
    snapshot(&mut stream, "A", 7, 1).await?;
    harness.data("A").await?;
    harness.target.send(ticker_set(&["A", "B"]))?;
    let add = command(&mut stream).await?;
    assert_eq!(add["params"]["action"], "add_markets");
    assert_eq!(command_tickers(&add), ticker_set(&["B"]));
    send(
        &mut stream,
        json!({"type":"error", "id":add["id"], "msg":{"code":27, "msg":"command rate limit"}}),
    )
    .await?;
    harness.clear().await?;
    let mut recovery = harness.accept().await?;
    let subscribe = command(&mut recovery).await?;
    assert_eq!(subscribe["cmd"], "subscribe");
    assert_eq!(command_tickers(&subscribe), ticker_set(&["A", "B"]));
    subscribe_ack(&mut recovery, &subscribe, 8).await?;
    snapshot(&mut recovery, "A", 8, 1).await?;
    snapshot(&mut recovery, "B", 8, 2).await?;
    harness.data("A").await?;
    harness.data("B").await?;
    harness.finish().await
}
