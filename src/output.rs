use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use crossbeam::channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded};
use tokio::sync::oneshot;

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

pub enum Output {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

pub struct OutputWriter {
    pub sender: Sender<Output>,
    pub completion: oneshot::Receiver<Result<()>>,
}

impl OutputWriter {
    pub fn start(path: &Path, chunk_bytes: usize, capacity: usize) -> Result<Self> {
        Self::start_with_interval(path, chunk_bytes, capacity, Duration::from_secs(1))
    }

    pub fn start_with_interval(
        path: &Path,
        chunk_bytes: usize,
        capacity: usize,
        flush_interval: Duration,
    ) -> Result<Self> {
        ensure!(capacity > 0, "output queue capacity must be positive");
        ensure!(
            !flush_interval.is_zero(),
            "output flush interval must be positive"
        );
        let writer = AlignedSbeWriter::create(path, chunk_bytes)
            .with_context(|| format!("failed to create SBE output at {}", path.display()))?;
        let (sender, receiver) = bounded(capacity);
        let (completion_sender, completion) = oneshot::channel();
        std::thread::Builder::new()
            .name("sbe-output".to_owned())
            .spawn(move || {
                let result = write_output(writer, receiver, flush_interval);
                let _ = completion_sender.send(result);
            })
            .context("failed to start SBE output thread")?;
        Ok(Self { sender, completion })
    }

    pub fn submit(&self, message: Output) -> Result<()> {
        match self.sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                bail!("output queue is full; output cannot process orderbook messages")
            }
            Err(TrySendError::Disconnected(_)) => bail!("SBE output thread has stopped"),
        }
    }

    pub async fn finish(self) -> Result<()> {
        drop(self.sender);
        self.completion
            .await
            .context("SBE output thread has no completion result")?
    }
}

fn write_output(
    mut writer: AlignedSbeWriter,
    receiver: Receiver<Output>,
    flush_interval: Duration,
) -> Result<()> {
    let mut flush_deadline = Instant::now() + flush_interval;
    loop {
        match receiver.recv_timeout(flush_deadline.saturating_duration_since(Instant::now())) {
            Ok(Output::Snapshot(snapshot)) => {
                writer
                    .write_msg(&snapshot)
                    .context("failed to write SBE snapshot")?;
            }
            Ok(Output::Update(update)) => {
                writer
                    .write_msg(&update)
                    .context("failed to write SBE update")?;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return writer.flush().context("failed to flush final SBE chunk");
            }
        }
        let now = Instant::now();
        if now >= flush_deadline {
            writer.flush().context("failed to flush SBE output")?;
            flush_deadline = now + flush_interval;
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::Side;
    use crate::util::dtos::challenge_orderbook::{AnyMessage, SnapshotBidsEntryDomain};
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    fn timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    fn update(quantity: i64) -> PriceLevelUpdateDomain {
        PriceLevelUpdateDomain {
            backtest_timestamp: timestamp(),
            exchange_timestamp: timestamp(),
            price_level: 400_000,
            quantity,
            side: Side::Buy,
            symbol: "TEST-MARKET".to_owned(),
        }
    }

    #[tokio::test]
    async fn shutdown_drains_ordered_messages_and_flushes_the_final_chunk() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let writer = OutputWriter::start(file.path(), 64 << 20, 1024)?;
        let snapshot = SnapshotDomain {
            received_at: timestamp(),
            exchange_timestamp: timestamp(),
            bids: vec![SnapshotBidsEntryDomain {
                price_level: 400_000,
                quantity: 2_000_000,
            }],
            asks: Vec::new(),
            symbol: "TEST-MARKET".to_owned(),
        };
        writer.submit(Output::Snapshot(snapshot.clone()))?;
        for quantity in [1_250_000, 0, 500_000] {
            writer.submit(Output::Update(update(quantity)))?;
        }
        writer.finish().await?;

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;
        let Some(AnyMessage::Snapshot(decoder)) = reader.read_msg()? else {
            bail!("missing snapshot before updates");
        };
        assert_eq!(SnapshotDomain::try_from_decoder(decoder)?, snapshot);
        for quantity in [1_250_000, 0, 500_000] {
            let Some(AnyMessage::PriceLevelUpdate(decoder)) = reader.read_msg()? else {
                bail!("missing output update");
            };
            assert_eq!(
                PriceLevelUpdateDomain::try_from_decoder(decoder)?,
                update(quantity)
            );
        }
        assert!(reader.read_msg()?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn active_stream_flushes_before_shutdown_or_chunk_capacity() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let chunk_bytes = 1 << 20;
        let writer = OutputWriter::start(file.path(), chunk_bytes, 1024)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut quantity = 0;
        while tokio::time::Instant::now() < deadline && file.as_file().metadata()?.len() == 0 {
            writer.submit(Output::Update(update(quantity)))?;
            quantity += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(file.as_file().metadata()?.len(), chunk_bytes as u64);
        writer.finish().await?;
        Ok(())
    }

    #[tokio::test]
    async fn write_failure_propagates_without_further_input() -> Result<()> {
        let mut writer = OutputWriter::start(Path::new("/dev/full"), 4096, 16)?;
        writer.submit(Output::Update(update(1)))?;
        let result = tokio::time::timeout(Duration::from_secs(3), &mut writer.completion)
            .await
            .context("output failure has no completion result")?
            .context("output completion channel has stopped")?;
        assert!(result.is_err());
        assert!(writer.submit(Output::Update(update(2))).is_err());
        Ok(())
    }
}
