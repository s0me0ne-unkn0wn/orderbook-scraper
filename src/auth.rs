use std::{fs, path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use rsa::{Pss, RsaPrivateKey, pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Credentials {
    key: Arc<PrivateKey>,
    id: String,
}

enum PrivateKey {
    Rsa(RsaPrivateKey),
    Ed25519(SigningKey),
}

impl Credentials {
    pub fn load(key_path: &Path, id: &str) -> Result<Self> {
        let pem = fs::read_to_string(key_path)
            .with_context(|| format!("failed to read private key {}", key_path.display()))?;
        let key = if let Ok(key) = SigningKey::from_pkcs8_pem(&pem) {
            PrivateKey::Ed25519(key)
        } else {
            PrivateKey::Rsa(
                RsaPrivateKey::from_pkcs8_pem(&pem)
                    .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
                    .with_context(|| {
                        format!(
                            "failed to parse RSA or Ed25519 private key {}",
                            key_path.display()
                        )
                    })?,
            )
        };
        ensure!(
            !id.is_empty() && id.bytes().all(|byte| (0x21..=0x7e).contains(&byte)),
            "invalid API key ID"
        );
        Ok(Self {
            key: Arc::new(key),
            id: id.to_owned(),
        })
    }

    pub fn headers(&self, method: &str, path: &str) -> Result<Vec<(&'static str, String)>> {
        self.headers_at(method, path, Utc::now().timestamp_millis())
    }

    fn headers_at(
        &self,
        method: &str,
        path: &str,
        timestamp: i64,
    ) -> Result<Vec<(&'static str, String)>> {
        let path = path.split('?').next().unwrap_or(path);
        let timestamp = timestamp.to_string();
        let message = format!("{timestamp}{method}{path}");
        let signature = match self.key.as_ref() {
            PrivateKey::Rsa(key) => key
                .sign_with_rng(
                    &mut OsRng,
                    Pss::new_with_salt::<Sha256>(32),
                    &Sha256::digest(message.as_bytes()),
                )
                .context("failed to sign Kalshi request")?,
            PrivateKey::Ed25519(key) => key.sign(message.as_bytes()).to_bytes().to_vec(),
        };
        Ok(vec![
            ("KALSHI-ACCESS-KEY", self.id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP", timestamp),
            ("KALSHI-ACCESS-SIGNATURE", STANDARD.encode(signature)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::{SeedableRng, rngs::StdRng};
    use rsa::{RsaPublicKey, pkcs1::EncodeRsaPrivateKey, pkcs8::EncodePrivateKey};

    use super::*;

    #[test]
    fn ed25519_signs_queryless_request_bytes_without_a_preliminary_hash() -> Result<()> {
        let key = SigningKey::from_bytes(&[42; 32]);
        let public_key = key.verifying_key();
        let directory = tempfile::tempdir()?;
        let key_path = directory.path().join("private.pem");
        fs::write(&key_path, key.to_pkcs8_pem(Default::default())?.as_bytes())?;
        let credentials = Credentials::load(&key_path, "ed25519-test-key")?;
        let headers: HashMap<_, _> = credentials
            .headers_at("GET", "/trade-api/ws/v2?unused=true", 123456789)?
            .into_iter()
            .collect();
        assert_eq!(headers["KALSHI-ACCESS-KEY"], "ed25519-test-key");
        let bytes = STANDARD.decode(&headers["KALSHI-ACCESS-SIGNATURE"])?;
        let signature = ed25519_dalek::Signature::from_slice(&bytes)?;
        let message = b"123456789GET/trade-api/ws/v2";
        public_key.verify_strict(message, &signature)?;
        assert!(
            public_key
                .verify_strict(&Sha256::digest(message), &signature)
                .is_err()
        );
        assert!(
            public_key
                .verify_strict(b"123456789GET/trade-api/ws/v2?unused=true", &signature)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn both_pem_formats_sign_the_exact_queryless_kalshi_request() -> Result<()> {
        let key = RsaPrivateKey::new(&mut StdRng::seed_from_u64(7), 2048)?;
        let public_key = RsaPublicKey::from(&key);
        let directory = tempfile::tempdir()?;
        let key_path = directory.path().join("private.pem");
        let pems = [
            key.to_pkcs1_pem(Default::default())?,
            key.to_pkcs8_pem(Default::default())?,
        ];
        for pem in pems {
            fs::write(&key_path, pem.as_bytes())?;
            let credentials = Credentials::load(&key_path, "test-key-id")?;
            let headers: HashMap<_, _> = credentials
                .headers_at(
                    "GET",
                    "/trade-api/v2/markets?cursor=abc&limit=1000",
                    123456789,
                )?
                .into_iter()
                .collect();
            assert_eq!(headers["KALSHI-ACCESS-KEY"], "test-key-id");
            assert_eq!(headers["KALSHI-ACCESS-TIMESTAMP"], "123456789");
            let signature = STANDARD.decode(&headers["KALSHI-ACCESS-SIGNATURE"])?;
            public_key.verify(
                Pss::new_with_salt::<Sha256>(32),
                &Sha256::digest(b"123456789GET/trade-api/v2/markets"),
                &signature,
            )?;
            assert!(
                public_key
                    .verify(
                        Pss::new_with_salt::<Sha256>(32),
                        &Sha256::digest(b"123456789GET/trade-api/v2/markets?cursor=abc&limit=1000"),
                        &signature,
                    )
                    .is_err()
            );
        }
        Ok(())
    }
}
