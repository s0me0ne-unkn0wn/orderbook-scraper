use std::{collections::BTreeSet, time::Duration};

use anyhow::{Result, ensure};
use engineering_challenge::{auth::Credentials, markets::Markets};
use rand::{SeedableRng, rngs::StdRng};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

struct Response {
    status: &'static str,
    body: String,
    retry_after: Option<u64>,
}

impl Response {
    fn page(body: Value) -> Self {
        Self {
            status: "200 OK",
            body: body.to_string(),
            retry_after: None,
        }
    }
}

async fn server(responses: Vec<Response>) -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let length = socket.read(&mut buffer).await?;
                ensure!(length > 0, "HTTP client closed before request headers");
                request.extend_from_slice(&buffer[..length]);
                ensure!(
                    request.len() < 65536,
                    "HTTP request headers exceed test limit"
                );
            }
            requests.push(String::from_utf8(request)?);
            let mut headers = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                response.status,
                response.body.len()
            );
            if let Some(seconds) = response.retry_after {
                headers.push_str(&format!("Retry-After: {seconds}\r\n"));
            }
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(b"\r\n").await?;
            socket.write_all(response.body.as_bytes()).await?;
            socket.shutdown().await?;
        }
        Ok(requests)
    });
    Ok((format!("http://{address}/trade-api/v2"), task))
}

fn client(url: &str) -> Result<Markets> {
    let directory = tempfile::tempdir()?;
    let key = RsaPrivateKey::new(&mut StdRng::seed_from_u64(13), 1024)?;
    let key_path = directory.path().join("private.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem(Default::default())?.as_bytes())?;
    Markets::new(
        url,
        Credentials::load(&key_path, "market-test-key")?,
        Duration::from_secs(2),
        Duration::ZERO,
    )
}

fn request_cursor(request: &str) -> Result<Option<String>> {
    let target = request.split_whitespace().nth(1).unwrap();
    let url = reqwest::Url::parse(&format!("http://localhost{target}"))?;
    assert_eq!(url.path(), "/trade-api/v2/markets");
    let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
    assert_eq!(
        query.get("status").map(|value| value.as_ref()),
        Some("open")
    );
    assert_eq!(
        query.get("mve_filter").map(|value| value.as_ref()),
        Some("exclude")
    );
    assert_eq!(query.get("limit").map(|value| value.as_ref()), Some("1000"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("kalshi-access-key: market-test-key\r\n")
    );
    Ok(query.get("cursor").map(|value| value.to_string()))
}

#[tokio::test]
async fn pagination_covers_all_active_non_mve_markets_without_duplicate_subscriptions() -> Result<()>
{
    let cursor = "page+two/=?& value";
    let (url, task) = server(vec![
        Response::page(json!({
            "markets": [
                {"ticker":"FIRST"},
                {"ticker":"DUPLICATE"}
            ],
            "cursor": cursor
        })),
        Response::page(json!({
            "markets": [
                {"ticker":"DUPLICATE"},
                {"ticker":"LAST"}
            ],
            "cursor":""
        })),
    ])
    .await?;
    let tickers = client(&url)?.fetch().await?;
    assert_eq!(
        tickers,
        BTreeSet::from(["FIRST".into(), "DUPLICATE".into(), "LAST".into()])
    );
    let requests = task.await??;
    assert_eq!(request_cursor(&requests[0])?, None);
    assert_eq!(request_cursor(&requests[1])?.as_deref(), Some(cursor));
    Ok(())
}

#[tokio::test]
async fn failed_page_cannot_replace_the_market_universe_with_partial_results() -> Result<()> {
    let (url, task) = server(vec![
        Response::page(
            json!({"markets":[{"ticker":"PARTIAL", "status":"active"}], "cursor":"next"}),
        ),
        Response {
            status: "403 Forbidden",
            body: "{}".into(),
            retry_after: None,
        },
        Response::page(json!({"markets":[{"ticker":"RECOVERY", "status":"active"}], "cursor":""})),
    ])
    .await?;
    let mut markets = client(&url)?;
    let error = markets.fetch().await.unwrap_err();
    assert!(error.to_string().contains("403"));
    assert_eq!(markets.fetch().await?, BTreeSet::from(["RECOVERY".into()]));
    let requests = task.await??;
    assert_eq!(request_cursor(&requests[0])?, None);
    assert_eq!(request_cursor(&requests[1])?.as_deref(), Some("next"));
    assert_eq!(request_cursor(&requests[2])?, None);
    Ok(())
}

#[tokio::test]
async fn rate_limit_retries_the_same_page_so_discovery_does_not_skip_markets() -> Result<()> {
    let (url, task) = server(vec![
        Response::page(json!({"markets":[], "cursor":"retry-page"})),
        Response {
            status: "429 Too Many Requests",
            body: "{}".into(),
            retry_after: Some(0),
        },
        Response::page(json!({"markets":[{"ticker":"RETRY", "status":"active"}], "cursor":""})),
    ])
    .await?;
    assert_eq!(
        client(&url)?.fetch().await?,
        BTreeSet::from(["RETRY".into()])
    );
    let requests = task.await??;
    assert_eq!(request_cursor(&requests[1])?.as_deref(), Some("retry-page"));
    assert_eq!(request_cursor(&requests[2])?.as_deref(), Some("retry-page"));
    Ok(())
}

#[tokio::test]
async fn malformed_page_cannot_be_treated_as_an_empty_market_universe() -> Result<()> {
    for body in [
        "{\"markets\":".to_owned(),
        json!({"markets":[]}).to_string(),
        json!({"markets":[],"cursor":null}).to_string(),
    ] {
        let (url, task) = server(vec![Response {
            status: "200 OK",
            body,
            retry_after: None,
        }])
        .await?;
        assert!(client(&url)?.fetch().await.is_err());
        assert_eq!(task.await??.len(), 1);
    }
    Ok(())
}
