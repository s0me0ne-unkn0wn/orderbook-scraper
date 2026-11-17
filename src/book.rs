use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};

use crate::Side;
use crate::protocol::{Delta, FixedPoint, OrderbookSide, Snapshot};
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    exchange_timestamp: DateTime<Utc>,
}

impl Book {
    pub fn from_snapshot(message: &Snapshot, exchange_timestamp: DateTime<Utc>) -> Self {
        Self {
            bids: snapshot_levels(message.yes_dollars_fp.as_deref()),
            asks: snapshot_levels(message.no_dollars_fp.as_deref()),
            exchange_timestamp,
        }
    }

    pub fn apply_delta(
        &mut self,
        message: &Delta,
        receive_timestamp: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> Result<PriceLevelUpdateDomain> {
        let price_level = message.price_dollars.0;
        let (side, levels) = match message.side {
            OrderbookSide::Yes => (Side::Buy, &mut self.bids),
            OrderbookSide::No => (Side::Sell, &mut self.asks),
        };
        let quantity = levels
            .get(&price_level)
            .copied()
            .unwrap_or_default()
            .checked_add(message.delta_fp.0)
            .context("orderbook quantity exceeds int64")?;
        ensure!(
            quantity >= 0,
            "orderbook delta produces a negative quantity"
        );
        if quantity == 0 {
            levels.remove(&price_level);
        } else {
            levels.insert(price_level, quantity);
        }
        self.exchange_timestamp = exchange_timestamp;
        Ok(PriceLevelUpdateDomain {
            backtest_timestamp: receive_timestamp,
            exchange_timestamp,
            price_level,
            quantity,
            side,
            symbol: message.market_ticker.clone(),
        })
    }

    pub fn snapshot(&self, symbol: &str, receive_timestamp: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at: receive_timestamp,
            exchange_timestamp: self.exchange_timestamp,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

fn snapshot_levels(levels: Option<&[(FixedPoint, FixedPoint)]>) -> BTreeMap<i64, i64> {
    levels
        .unwrap_or_default()
        .iter()
        .map(|(price, quantity)| (price.0, quantity.0))
        .filter(|(_, quantity)| *quantity != 0)
        .collect()
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use serde_json::{Value, json};

    use super::*;
    use crate::util::dtos::challenge_orderbook::AnyMessage;
    use crate::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

    fn timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    fn snapshot(mut value: Value) -> Snapshot {
        value["market_ticker"] = json!("TEST-MARKET");
        serde_json::from_value(value).expect("invalid snapshot test data")
    }

    fn delta(side: &str, price: &str, quantity: &str) -> Delta {
        serde_json::from_value(json!({
            "market_ticker": "TEST-MARKET",
            "side": side,
            "price_dollars": price,
            "delta_fp": quantity
        }))
        .expect("invalid delta test data")
    }

    #[test]
    fn yes_price_mode_preserves_ask_prices_and_subcent_precision() {
        let book = Book::from_snapshot(
            &snapshot(json!({
                "yes_dollars_fp": [["0.0800", "300.25"], ["0.220001", "333.01"]],
                "no_dollars_fp": [["0.5400", "20.00"], ["0.560001", "146.00"]]
            })),
            timestamp(),
        );
        let snapshot = book.snapshot("TEST-MARKET", timestamp());
        assert_eq!(
            snapshot
                .bids
                .iter()
                .map(|level| level.price_level)
                .collect::<Vec<_>>(),
            [220_001, 80_000]
        );
        assert_eq!(snapshot.bids[0].quantity, 333_010_000);
        assert_eq!(
            snapshot
                .asks
                .iter()
                .map(|level| level.price_level)
                .collect::<Vec<_>>(),
            [540_000, 560_001]
        );
        assert_eq!(snapshot.asks[0].quantity, 20_000_000);
    }

    #[test]
    fn replay_outputs_absolute_quantities_and_removes_empty_levels_in_sbe() -> Result<()> {
        let mut book = Book::from_snapshot(
            &snapshot(json!({
                "yes_dollars_fp": [["0.4000", "2.00"]],
                "no_dollars_fp": [["0.7000", "3.50"]]
            })),
            timestamp(),
        );
        let file = tempfile::NamedTempFile::new()?;
        let mut writer = AlignedSbeWriter::create(file.path(), 64 << 20)?;
        writer.write_msg(&book.snapshot("TEST-MARKET", timestamp()))?;
        for message in [
            delta("yes", "0.4000", "-0.75"),
            delta("no", "0.7000", "0.25"),
            delta("yes", "0.4000", "-1.25"),
            delta("yes", "0.4500", "0.01"),
        ] {
            writer.write_msg(&book.apply_delta(&message, timestamp(), timestamp())?)?;
        }
        writer.write_msg(&book.snapshot("TEST-MARKET", timestamp()))?;
        writer.flush()?;

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;
        let Some(AnyMessage::Snapshot(initial)) = reader.read_msg()? else {
            bail!("missing initial snapshot");
        };
        assert_eq!(
            SnapshotDomain::try_from_decoder(initial)?.bids[0].quantity,
            2_000_000
        );
        let mut updates = Vec::new();
        for _ in 0..4 {
            let Some(AnyMessage::PriceLevelUpdate(update)) = reader.read_msg()? else {
                bail!("missing replay update");
            };
            updates.push(PriceLevelUpdateDomain::try_from_decoder(update)?);
        }
        assert_eq!(
            updates
                .iter()
                .map(|update| update.quantity)
                .collect::<Vec<_>>(),
            [1_250_000, 3_750_000, 0, 10_000]
        );
        assert_eq!(updates[1].side, Side::Sell);
        assert_eq!(updates[1].price_level, 700_000);
        let Some(AnyMessage::Snapshot(final_snapshot)) = reader.read_msg()? else {
            bail!("missing final snapshot");
        };
        let final_snapshot = SnapshotDomain::try_from_decoder(final_snapshot)?;
        assert_eq!(final_snapshot.bids.len(), 1);
        assert_eq!(final_snapshot.bids[0].price_level, 450_000);
        assert_eq!(final_snapshot.asks[0].quantity, 3_750_000);
        assert!(reader.read_msg()?.is_none());
        Ok(())
    }

    #[test]
    fn invalid_deltas_leave_the_book_and_exchange_timestamp_unchanged() -> Result<()> {
        let mut book = Book::from_snapshot(
            &snapshot(json!({"yes_dollars_fp": [["0.40", "1.00"]]})),
            timestamp(),
        );
        let before = book.snapshot("TEST-MARKET", timestamp());
        let next_timestamp = timestamp() + chrono::Duration::seconds(1);
        for message in [
            delta("yes", "0.40", "-1.01"),
            delta("no", "0.40", "-0.01"),
            delta("yes", "0.40", "9223372036854.775807"),
        ] {
            assert!(
                book.apply_delta(&message, timestamp(), next_timestamp)
                    .is_err(),
                "invalid delta: {message:?}"
            );
            assert_eq!(book.snapshot("TEST-MARKET", timestamp()), before);
        }
        Ok(())
    }

    #[test]
    fn empty_snapshot_replaces_every_previous_level() -> Result<()> {
        let mut book = Book::from_snapshot(
            &snapshot(json!({"yes_dollars_fp": [["0.4", "2"]], "no_dollars_fp": [["0.3", "3"]]})),
            timestamp(),
        );
        assert!(!book.snapshot("TEST-MARKET", timestamp()).bids.is_empty());
        book = Book::from_snapshot(&snapshot(json!({"yes_dollars_fp": null})), timestamp());
        let snapshot = book.snapshot("TEST-MARKET", timestamp());
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        let update = book.apply_delta(&delta("yes", "0.4", "1"), timestamp(), timestamp())?;
        assert_eq!(update.quantity, 1_000_000);
        Ok(())
    }
}
