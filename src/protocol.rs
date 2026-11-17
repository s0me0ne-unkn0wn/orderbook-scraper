use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde::Deserialize;

const SCALE: i64 = 1_000_000;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct FixedPoint(pub i64);

impl TryFrom<String> for FixedPoint {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        decimal(&value).map(Self)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderbookSide {
    Yes,
    No,
}

#[derive(Debug, Deserialize)]
pub struct Snapshot {
    pub market_ticker: String,
    pub yes_dollars_fp: Option<Vec<(FixedPoint, FixedPoint)>>,
    pub no_dollars_fp: Option<Vec<(FixedPoint, FixedPoint)>>,
    #[serde(flatten)]
    timestamp: Timestamp,
}

#[derive(Debug, Deserialize)]
pub struct Delta {
    pub market_ticker: String,
    pub price_dollars: FixedPoint,
    pub delta_fp: FixedPoint,
    pub side: OrderbookSide,
    #[serde(flatten)]
    timestamp: Timestamp,
}

#[derive(Debug, Deserialize)]
struct Timestamp {
    ts_ms: Option<i64>,
    ts: Option<String>,
}

impl Timestamp {
    fn resolve(
        &self,
        sending_ts_ms: Option<i64>,
        receive_timestamp: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        if let Some(timestamp) = self.ts_ms {
            return DateTime::from_timestamp_millis(timestamp)
                .context("exchange timestamp is out of range");
        }
        if let Some(timestamp) = &self.ts {
            return DateTime::parse_from_rfc3339(timestamp)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .context("invalid exchange timestamp");
        }
        if let Some(timestamp) = sending_ts_ms {
            return DateTime::from_timestamp_millis(timestamp)
                .context("server timestamp is out of range");
        }
        Ok(receive_timestamp)
    }
}

#[derive(Debug)]
pub enum BookMessage {
    Snapshot(Snapshot),
    Delta(Delta),
}

impl BookMessage {
    pub fn ticker(&self) -> &str {
        match self {
            Self::Snapshot(snapshot) => &snapshot.market_ticker,
            Self::Delta(delta) => &delta.market_ticker,
        }
    }

    pub fn exchange_timestamp(
        &self,
        sending_ts_ms: Option<i64>,
        receive_timestamp: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        let timestamp = match self {
            Self::Snapshot(snapshot) => &snapshot.timestamp,
            Self::Delta(delta) => &delta.timestamp,
        };
        timestamp.resolve(sending_ts_ms, receive_timestamp)
    }
}

#[derive(Debug, Deserialize)]
pub struct Subscription {
    pub sid: u64,
}

#[derive(Debug, Deserialize)]
pub struct CommandError {
    pub code: i64,
    pub msg: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "subscribed")]
    Subscription { id: u64, msg: Subscription },
    #[serde(rename = "ok")]
    Ok { id: u64, seq: u64 },
    #[serde(rename = "error")]
    Error { msg: CommandError },
    #[serde(rename = "orderbook_snapshot")]
    Snapshot {
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: Snapshot,
    },
    #[serde(rename = "orderbook_delta")]
    Delta {
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: Delta,
    },
    #[serde(other)]
    Other,
}

impl ServerMessage {
    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Ok { seq, .. } | Self::Snapshot { seq, .. } | Self::Delta { seq, .. } => {
                Some(*seq)
            }
            _ => None,
        }
    }
}

fn decimal(value: &str) -> Result<i64> {
    let (sign, digits) = value
        .strip_prefix('-')
        .map_or((1, value), |digits| (-1, digits));
    let (whole, fraction) = match digits.split_once('.') {
        Some((whole, fraction)) => {
            ensure!(
                !fraction.is_empty(),
                "fixed point number has an empty fraction"
            );
            (whole, fraction)
        }
        None => (digits, ""),
    };
    ensure!(
        !whole.is_empty()
            && whole.bytes().all(|digit| digit.is_ascii_digit())
            && fraction.bytes().all(|digit| digit.is_ascii_digit())
            && fraction.len() <= 6,
        "invalid fixed point number"
    );
    let whole: i64 = whole.parse().context("fixed point integer exceeds int64")?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i64>()
            .context("invalid fixed point fraction")?
            * 10_i64.pow(6 - fraction.len() as u32)
    };
    whole
        .checked_mul(sign * SCALE)
        .and_then(|whole| whole.checked_add(sign * fraction))
        .context("fixed point number exceeds int64")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn fixed_point_json_preserves_int64_bounds_and_rejects_precision_loss() {
        for (text, quantity) in [
            ("9223372036854.775807", i64::MAX),
            ("-9223372036854.775808", i64::MIN),
            ("0", 0),
            ("-0.000000", 0),
            ("1", 1_000_000),
            ("-1", -1_000_000),
            ("1.234567", 1_234_567),
            ("-1.234567", -1_234_567),
            ("0.1", 100_000),
            ("-0.1", -100_000),
            ("0.000001", 1),
            ("-0.000001", -1),
        ] {
            assert_eq!(
                serde_json::from_value::<FixedPoint>(json!(text)).unwrap(),
                FixedPoint(quantity)
            );
        }
        for text in [
            "9223372036854.775808",
            "-9223372036854.775809",
            "9223372036855",
            "-9223372036855",
            "9223372036854775808",
            "-9223372036854775808",
            "0.0000001",
            "1e2",
            "NaN",
            ".5",
            "1.",
            " 1",
            "+1",
            "",
            "--1",
        ] {
            assert!(
                serde_json::from_value::<FixedPoint>(json!(text)).is_err(),
                "invalid fixed point value: {text}"
            );
        }
    }

    #[test]
    fn book_timestamps_prefer_exchange_time_and_use_send_or_receive_time_as_fallback() -> Result<()>
    {
        let receive_timestamp = DateTime::from_timestamp_millis(1_700_000_000_003).unwrap();
        for (fields, sending_ts_ms, milliseconds) in [
            (
                json!({"ts_ms":1_700_000_000_000_i64,"ts":"2020-01-01T00:00:00Z"}),
                Some(1_700_000_000_002),
                1_700_000_000_000,
            ),
            (
                json!({"ts":"2020-01-01T00:00:00.001Z"}),
                Some(1_700_000_000_002),
                1_577_836_800_001,
            ),
            (json!({}), Some(1_700_000_000_002), 1_700_000_000_002),
            (json!({}), None, 1_700_000_000_003),
        ] {
            let mut value = fields;
            value["market_ticker"] = json!("TEST-MARKET");
            let snapshot = serde_json::from_value::<Snapshot>(value)?;
            let message = BookMessage::Snapshot(snapshot);
            assert_eq!(
                message
                    .exchange_timestamp(sending_ts_ms, receive_timestamp)?
                    .timestamp_millis(),
                milliseconds
            );
        }
        Ok(())
    }
}
