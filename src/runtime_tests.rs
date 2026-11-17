use chrono::{DateTime, Utc};
use clap::Parser;
use rand::{SeedableRng, rngs::StdRng};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use super::*;
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};

fn timestamp() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
}

fn snapshot_event(socket: usize, ticker: &str, quantity: &str) -> SocketEvent {
    SocketEvent::Data {
        socket,
        receive_timestamp: timestamp(),
        exchange_timestamp: timestamp(),
        message: BookMessage::Snapshot(
            serde_json::from_value(json!({
                "market_ticker":ticker,"yes_dollars_fp":[["0.4",quantity]]
            }))
            .expect("invalid snapshot test data"),
        ),
    }
}

fn delta_event(socket: usize, ticker: &str, quantity: &str) -> SocketEvent {
    SocketEvent::Data {
        socket,
        receive_timestamp: timestamp(),
        exchange_timestamp: timestamp(),
        message: BookMessage::Delta(
            serde_json::from_value(json!({
                "market_ticker":ticker,"side":"yes","price_dollars":"0.4","delta_fp":quantity
            }))
            .expect("invalid delta test data"),
        ),
    }
}

struct StateFixture {
    // Runtime state for processing socket events in tests.
    state: State,
    // Subscription target receivers by socket index.
    targets: Vec<watch::Receiver<BTreeSet<String>>>,
    // Reconnect request receivers by socket index.
    recovery: Vec<mpsc::Receiver<()>>,
}

fn state_with_markets(tickers: &[&str]) -> Result<StateFixture> {
    let config = Config::try_parse_from(["runtime-state-test", "--key-id", "runtime-state-test"])?;
    let mut state = State::new(&config)?;
    let desired = tickers.iter().map(|ticker| (*ticker).to_owned()).collect();
    let shards = state.assignments.reconcile(&desired);
    let mut targets = Vec::new();
    let mut recovery = Vec::new();
    for shard in shards {
        let (sender, receiver) = watch::channel(shard);
        state.targets.push(sender);
        targets.push(receiver);
        let (sender, receiver) = mpsc::channel(1);
        state.reconnect.push(sender);
        recovery.push(receiver);
    }
    Ok(StateFixture {
        state,
        targets,
        recovery,
    })
}

fn read_output(path: &Path) -> Result<(Vec<SnapshotDomain>, Vec<PriceLevelUpdateDomain>)> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut snapshots = Vec::new();
    let mut updates = Vec::new();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(snapshot) => {
                snapshots.push(SnapshotDomain::try_from_decoder(snapshot)?);
            }
            AnyMessage::PriceLevelUpdate(update) => {
                updates.push(PriceLevelUpdateDomain::try_from_decoder(update)?);
            }
            _ => anyhow::bail!("unknown orderbook output message"),
        }
    }
    Ok((snapshots, updates))
}

#[tokio::test]
async fn equal_market_refreshes_do_not_notify_subscription_processing() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let key_path = directory.path().join("test.pem");
    let key = RsaPrivateKey::new(&mut StdRng::seed_from_u64(41), 1024)?;
    std::fs::write(&key_path, key.to_pkcs8_pem(Default::default())?.as_bytes())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let markets = Markets::new(
        &format!("http://{}/trade-api/v2", listener.local_addr()?),
        Credentials::load(&key_path, "refresh-test")?,
        Duration::from_secs(5),
        Duration::ZERO,
    )?;
    let (universe_tx, mut universe_rx) = watch::channel(BTreeSet::from(["A".to_owned()]));
    let poller = tokio::spawn(refresh_markets(
        markets,
        Duration::from_millis(5),
        universe_tx,
    ));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        for (ticker, change) in [("A", false), ("B", false), ("B", true), ("B", false)] {
            let (mut stream, _) = listener.accept().await?;
            assert_eq!(universe_rx.has_changed()?, change);
            if change {
                assert_eq!(*universe_rx.borrow_and_update(), BTreeSet::from(["B".to_owned()]));
            }
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut buffer = [0_u8; 1024];
                let count = stream.read(&mut buffer).await?;
                ensure!(count > 0, "incomplete market request");
                request.extend_from_slice(&buffer[..count]);
            }
            let body = json!({"markets":[{"ticker":ticker}],"cursor":""}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await?;
            stream.shutdown().await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("market refresh test timeout");
    poller.abort();
    let _ = poller.await;
    result?
}

#[tokio::test]
async fn impossible_delta_excludes_all_socket_books_until_fresh_recovery_snapshots() -> Result<()> {
    let StateFixture {
        mut state,
        targets: _targets,
        mut recovery,
    } = state_with_markets(&["A", "B", "C", "D"])?;
    let socket = state.assignments.owner("A").unwrap();
    let same_socket = ["C", "D", "B"]
        .into_iter()
        .find(|ticker| state.assignments.owner(ticker) == Some(socket))
        .unwrap();
    let other_ticker = ["B", "C", "D"]
        .into_iter()
        .find(|ticker| state.assignments.owner(ticker) != Some(socket))
        .unwrap();
    let other_socket = state.assignments.owner(other_ticker).unwrap();
    let file = tempfile::NamedTempFile::new()?;
    let output = OutputWriter::start(file.path(), 64 << 20, 64)?;
    state.process(snapshot_event(socket, "A", "2"), &output)?;
    state.process(snapshot_event(socket, same_socket, "5"), &output)?;
    state.process(snapshot_event(other_socket, other_ticker, "7"), &output)?;
    state.process(delta_event(socket, "A", "-0.75"), &output)?;

    state.process(delta_event(socket, "A", "-2"), &output)?;
    assert_eq!(recovery[socket].try_recv(), Ok(()));
    assert!(!state.books.contains_key("A"));
    assert!(!state.books.contains_key(same_socket));
    assert!(state.books.contains_key(other_ticker));
    state.process(delta_event(socket, "A", "100"), &output)?;
    state.process(snapshot_event(socket, same_socket, "50"), &output)?;
    assert!(recovery[socket].try_recv().is_err());
    state.process(delta_event(other_socket, other_ticker, "1"), &output)?;
    for (ticker, book) in &state.books {
        output.submit(Output::Snapshot(book.snapshot(ticker, timestamp())))?;
    }

    state.process(SocketEvent::Clear { socket }, &output)?;
    state.process(snapshot_event(socket, "A", "4"), &output)?;
    state.process(delta_event(socket, "A", "-0.5"), &output)?;
    assert_eq!(
        state.books["A"].snapshot("A", timestamp()).bids[0].quantity,
        3_500_000
    );
    output.finish().await?;
    let (snapshots, updates) = read_output(file.path())?;
    assert_eq!(snapshots.len(), 5);
    assert_eq!(
        snapshots
            .iter()
            .filter(|snapshot| snapshot.symbol == "A")
            .map(|snapshot| snapshot.bids[0].quantity)
            .collect::<Vec<_>>(),
        [2_000_000, 4_000_000]
    );
    assert_eq!(
        snapshots
            .iter()
            .filter(|snapshot| snapshot.symbol == same_socket)
            .map(|snapshot| snapshot.bids[0].quantity)
            .collect::<Vec<_>>(),
        [5_000_000]
    );
    assert_eq!(
        snapshots
            .iter()
            .filter(|snapshot| snapshot.symbol == other_ticker)
            .map(|snapshot| snapshot.bids[0].quantity)
            .collect::<Vec<_>>(),
        [7_000_000, 8_000_000]
    );
    assert_eq!(
        updates
            .iter()
            .map(|update| (update.symbol.as_str(), update.quantity))
            .collect::<Vec<_>>(),
        [
            ("A", 1_250_000),
            (other_ticker, 8_000_000),
            ("A", 3_500_000)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn market_migration_requires_a_new_owner_snapshot_and_ignores_previous_owner_frames()
-> Result<()> {
    let StateFixture {
        mut state,
        targets,
        mut recovery,
    } = state_with_markets(&["A", "B", "C", "D"])?;
    let previous_socket = state.assignments.owner("B").unwrap();
    let desired: BTreeSet<_> = ["A", "B", "C", "D"]
        .into_iter()
        .filter(|ticker| state.assignments.owner(ticker) == Some(previous_socket))
        .map(str::to_owned)
        .collect();
    let file = tempfile::NamedTempFile::new()?;
    let output = OutputWriter::start(file.path(), 64 << 20, 64)?;
    for ticker in &desired {
        state.process(snapshot_event(previous_socket, ticker, "2"), &output)?;
    }
    let directory = tempfile::tempdir()?;
    let key_path = directory.path().join("test.pem");
    let key = RsaPrivateKey::new(&mut StdRng::seed_from_u64(79), 1024)?;
    std::fs::write(&key_path, key.to_pkcs8_pem(Default::default())?.as_bytes())?;
    let credentials = Credentials::load(&key_path, "migration-test")?;
    let config = Arc::new(Config::try_parse_from([
        "runtime-state-test",
        "--key-id",
        "migration-test",
    ])?);
    let (events, _receiver) = mpsc::channel(16);
    let pacer = Arc::new(ConnectPacer::new(Duration::from_millis(1)));
    let mut workers = JoinSet::new();
    state.reconcile(
        &desired,
        &config,
        &credentials,
        &events,
        &pacer,
        &mut workers,
    )?;
    assert!(workers.is_empty());
    let ticker = desired
        .iter()
        .find(|ticker| state.assignments.owner(ticker) != Some(previous_socket))
        .unwrap();
    let socket = state.assignments.owner(ticker).unwrap();
    assert!(!state.books.contains_key(ticker));
    assert!(targets[socket].borrow().contains(ticker));
    assert!(!targets[previous_socket].borrow().contains(ticker));

    state.process(delta_event(previous_socket, ticker, "100"), &output)?;
    state.process(delta_event(socket, ticker, "1"), &output)?;
    assert_eq!(recovery[socket].try_recv(), Ok(()));
    state.process(snapshot_event(socket, ticker, "99"), &output)?;
    assert!(!state.books.contains_key(ticker));
    state.process(SocketEvent::Clear { socket }, &output)?;
    state.process(snapshot_event(socket, ticker, "4"), &output)?;
    state.process(snapshot_event(previous_socket, ticker, "100"), &output)?;
    state.process(delta_event(socket, ticker, "-0.25"), &output)?;
    assert_eq!(
        state.books[ticker].snapshot(ticker, timestamp()).bids[0].quantity,
        3_750_000
    );
    output.finish().await?;
    let (snapshots, updates) = read_output(file.path())?;
    assert_eq!(snapshots.len(), 3);
    assert_eq!(
        snapshots
            .iter()
            .filter(|snapshot| snapshot.symbol == *ticker)
            .map(|snapshot| snapshot.bids[0].quantity)
            .collect::<Vec<_>>(),
        [2_000_000, 4_000_000]
    );
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].symbol, *ticker);
    assert_eq!(updates[0].quantity, 3_750_000);
    Ok(())
}
