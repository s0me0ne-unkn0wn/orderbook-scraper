use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::interval,
};

use crate::{
    auth::Credentials,
    balance::Assignments,
    book::Book,
    config::{Config, TIMEOUT},
    markets::Markets,
    output::{Output, OutputWriter},
    protocol::BookMessage,
    socket::{self, ConnectPacer, SocketEvent},
};

struct State {
    // Market ownership and subscription distribution across sockets.
    assignments: Assignments,
    // Current order books by market ticker.
    books: HashMap<String, Book>,
    // Socket indices to ignore until their workers clear the connection state.
    suspended: HashSet<usize>,
    // Channels for sending target market tickers to each socket worker.
    targets: Vec<watch::Sender<BTreeSet<String>>>,
    // Channels for requesting reconnects after local book errors.
    reconnect: Vec<mpsc::Sender<()>>,
}

impl State {
    fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            assignments: Assignments::new(config.markets_per_socket)?,
            books: HashMap::new(),
            suspended: HashSet::new(),
            targets: Vec::new(),
            reconnect: Vec::new(),
        })
    }

    fn clear(&mut self, socket: usize) {
        for ticker in self.assignments.markets(socket) {
            self.books.remove(ticker);
        }
        self.suspended.remove(&socket);
    }

    fn process(&mut self, event: SocketEvent, output: &OutputWriter) -> Result<()> {
        let (socket, message, receive_timestamp, exchange_timestamp) = match event {
            SocketEvent::Clear { socket } => {
                self.clear(socket);
                return Ok(());
            }
            SocketEvent::Data {
                socket,
                message,
                receive_timestamp,
                exchange_timestamp,
            } => (socket, message, receive_timestamp, exchange_timestamp),
        };
        let ticker = message.ticker();
        if self.suspended.contains(&socket) || self.assignments.owner(ticker) != Some(socket) {
            return Ok(());
        }
        let result = match &message {
            BookMessage::Snapshot(snapshot) => {
                let book = Book::from_snapshot(snapshot, exchange_timestamp);
                let snapshot = book.snapshot(ticker, receive_timestamp);
                self.books.insert(ticker.to_owned(), book);
                Ok(Output::Snapshot(snapshot))
            }
            BookMessage::Delta(delta) => self
                .books
                .get_mut(ticker)
                .context("orderbook delta precedes its snapshot")
                .and_then(|book| book.apply_delta(delta, receive_timestamp, exchange_timestamp))
                .map(Output::Update),
        };
        match result {
            Ok(message) => output.submit(message)?,
            Err(error) => {
                eprintln!("websocket {socket}: {error:#}; requesting fresh books");
                self.clear(socket);
                self.suspended.insert(socket);
                if let Some(reconnect) = self.reconnect.get(socket) {
                    reconnect
                        .try_send(())
                        .context("failed to request websocket recovery")?;
                }
            }
        }
        Ok(())
    }

    fn reconcile(
        &mut self,
        desired: &BTreeSet<String>,
        config: &Arc<Config>,
        credentials: &Credentials,
        events: &mpsc::Sender<SocketEvent>,
        pacer: &Arc<ConnectPacer>,
        workers: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        let previous: HashMap<_, _> = self
            .books
            .keys()
            .map(|ticker| (ticker.clone(), self.assignments.owner(ticker)))
            .collect();
        let shards = self.assignments.reconcile(desired);
        ensure!(
            shards.len() <= config.max_connections,
            "{} markets require {} websockets, exceeding max-connections {}; increase markets-per-socket or max-connections",
            desired.len(),
            shards.len(),
            config.max_connections
        );
        self.books.retain(|ticker, _| {
            previous[ticker] == self.assignments.owner(ticker)
                && self.assignments.owner(ticker).is_some()
        });
        for (socket, markets) in shards.into_iter().enumerate() {
            if socket < self.targets.len() {
                self.targets[socket].send_if_modified(|current| {
                    if *current == markets {
                        false
                    } else {
                        *current = markets;
                        true
                    }
                });
            } else {
                let (target_tx, target_rx) = watch::channel(markets);
                let (reconnect_tx, reconnect_rx) = mpsc::channel(1);
                workers.spawn(socket::worker(
                    socket,
                    config.clone(),
                    credentials.clone(),
                    target_rx,
                    reconnect_rx,
                    events.clone(),
                    pacer.clone(),
                ));
                self.targets.push(target_tx);
                self.reconnect.push(reconnect_tx);
            }
        }
        eprintln!(
            "tracking {} open non-MVE markets across {} websocket slots; {} books ready",
            desired.len(),
            self.targets.len(),
            self.books.len()
        );
        Ok(())
    }
}

async fn refresh_markets(
    mut markets: Markets,
    refresh_duration: Duration,
    universe: watch::Sender<BTreeSet<String>>,
) {
    let mut refresh = interval(refresh_duration);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    refresh.tick().await;
    loop {
        refresh.tick().await;
        match markets.fetch().await {
            Ok(markets) => {
                universe.send_if_modified(|current| {
                    if *current == markets {
                        false
                    } else {
                        *current = markets;
                        true
                    }
                });
            }
            Err(error) => {
                eprintln!("market refresh failed; subscriptions unchanged: {error:#}")
            }
        }
    }
}

pub async fn run(config: Config) -> Result<()> {
    run_to(config, Path::new("/dev/stdout")).await
}

pub async fn run_to(config: Config, path: &Path) -> Result<()> {
    config.validate()?;
    let credentials = Credentials::load(&config.key, &config.key_id)?;
    let mut markets = Markets::new(
        &config.rest_url,
        credentials.clone(),
        TIMEOUT,
        Duration::from_millis(config.rest_interval_ms),
    )?;
    let initial = markets.fetch().await?;
    let config = Arc::new(config);
    let mut output = OutputWriter::start_with_interval(
        path,
        64 << 20,
        config.queue_capacity,
        Duration::from_secs(config.output_flush_secs),
    )?;
    let (events_tx, mut events_rx) = mpsc::channel(config.queue_capacity);
    let (universe_tx, mut universe_rx) = watch::channel(initial.clone());
    let mut workers = JoinSet::new();
    let mut state = State::new(&config)?;
    let pacer = Arc::new(ConnectPacer::new(Duration::from_millis(
        config.connect_interval_ms,
    )));
    let refresh_duration = Duration::from_secs(config.market_refresh_secs);
    let poller = tokio::spawn(refresh_markets(markets, refresh_duration, universe_tx));
    let interrupt = shutdown();
    tokio::pin!(interrupt);
    let mut snapshots = interval(Duration::from_secs(60));
    snapshots.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    snapshots.tick().await;
    let mut snapshot_queue = VecDeque::new();
    let mut result: Result<()> = async {
        state.reconcile(&initial, &config, &credentials, &events_tx, &pacer, &mut workers)?;
        loop {
            tokio::select! {
                event = events_rx.recv() => {
                    state.process(event.context("websocket event channel closed")?, &output)?;
                    // TODO: Benchmark to estimate optimal chunk size
                    for _ in 0..127 {
                        let Ok(event) = events_rx.try_recv() else { break; };
                        state.process(event, &output)?;
                    }
                },
                change = universe_rx.changed() => {
                    change.context("market discovery task stopped")?;
                    let desired = universe_rx.borrow_and_update().clone();
                    state.reconcile(&desired, &config, &credentials, &events_tx, &pacer, &mut workers)?;
                }
                _ = snapshots.tick() => {
                    if snapshot_queue.is_empty() {
                        snapshot_queue = state.books.keys().cloned().collect();
                        eprintln!("snapshot: {} books", state.books.len());
                    } else {
                        eprintln!("snapshot output is delayed: {} books pending", snapshot_queue.len());
                    }
                }
                _ = std::future::ready(()), if !snapshot_queue.is_empty() => {
                    // TODO: Benchmark how much time is required to finalize a snapshot and set the optimal pace
                    for _ in 0..64 {
                        let Some(ticker) = snapshot_queue.pop_front() else { break; };
                        if let Some(book) = state.books.get(&ticker) {
                            // `.submit()` is just `.try_send()` to a writer thread, no severe blocking involved
                            output.submit(Output::Snapshot(book.snapshot(&ticker, Utc::now())))?;
                        }
                    }
                }
                result = workers.join_next(), if !workers.is_empty() => {
                    result.context("websocket task set is empty")?.context("websocket task failed")??;
                    anyhow::bail!("websocket worker stopped unexpectedly");
                }
                completion = &mut output.completion => {
                    completion.context("output thread stopped")??;
                    anyhow::bail!("output thread stopped unexpectedly");
                }
                signal = &mut interrupt => { signal?; break; }
            }
        }
        Ok(())
    }.await;
    poller.abort();
    let _ = poller.await;
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    state.reconnect.clear();
    if result.is_ok() {
        while let Ok(event) = events_rx.try_recv() {
            if let Err(error) = state.process(event, &output) {
                result = Err(error);
                break;
            }
        }
    }
    if !output.completion.is_terminated() {
        tokio::time::timeout(TIMEOUT, output.finish())
            .await
            .context("SBE output shutdown timeout")??;
    }
    result
}

async fn shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("failed to install SIGINT handler"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("failed to install SIGINT handler")
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
