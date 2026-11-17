use clap::{Parser, error::ErrorKind};
use engineering_challenge::config::Config;

#[test]
fn cli_requires_an_api_key_identifier() {
    let error = Config::try_parse_from(["scraper"]).expect_err("missing API key ID has no error");
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    let config = Config::try_parse_from(["scraper", "--key-id", "test-key-id"])
        .expect("literal API key ID fails to parse");
    assert_eq!(config.key_id, "test-key-id");
}

#[test]
fn cli_rejects_internal_runtime_controls() {
    for argument in ["--connections", "--timeout-secs", "--subscription-batch"] {
        let error = Config::try_parse_from(["scraper", "--key-id", "test-key-id", argument, "2"])
            .expect_err("internal runtime option has no error");
        assert_eq!(error.kind(), ErrorKind::UnknownArgument);
    }
}
