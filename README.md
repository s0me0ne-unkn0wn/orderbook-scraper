Kalshi orderbook scraper
=======================

The scraper subscribes to every open market except multivariate events and writes binary SBE messages to stdout. Logs go to stderr.

Run
---

Place the private key in `key.pem` (filename overriden by `--key`) and pass its API key ID with `--key-id`. Ed25519 PKCS#8 and RSA PKCS#1/PKCS#8 keys are supported.

```sh
cargo run -r -- --key-id YOUR_API_KEY_ID > orderbooks.sbe
```

To stop, press Ctrl-C.

Subscriptions and recovery
-------------------------

Market list is refreshed every 60 seconds. By default, markets are ditstributed by 2,000 per connection. Connections are properly load-balanced when new markets are added and old ones are removed.

Balancing uses subscription counts. It does not estimate individual market message rates. Set a lower market cap or a higher minimum connection count when busy markets exceed a connection's capacity. The program fails explicitly if the configured pool cannot cover the full universe.

Each connection checks the subscription sequence, including sequenced control acknowledgments. A gap, message decoding error, negative resulting quantity, command error, idle timeout or disconnect discards the affected local books and reconnects with exponential backoff and jitter.

Output
------

An exchange snapshot replaces the local book and immediately emits a `Snapshot`. Each delta emits `PriceLevelUpdate` with the resulting **absolute** quantity; zero deletes a level. Every 60 seconds the scraper also emits a full snapshot of each valid local book, interleaving the work with incoming updates. A disconnected or recovering market has no valid local book and does not emit periodic snapshots until recovery.

The writer uses 64 MiB zero-padded chunks, as expected by the reader, and flushes at least once per second when data is present. Full chunks flush immediately.
