//! Minimal S3-compatible GET/PUT for an authoring gate's content store
//! (Cloudflare R2, or MinIO in tests). Hand-rolled AWS SigV4 over the existing
//! reqwest client + rustls — deliberately NOT `aws-sdk-s3`, whose `aws-lc-sys`
//! crypto needs a C toolchain the `rust:slim` build image doesn't carry
//! (`sha2`/`hmac` are pure Rust; TLS rides the already-built `ring` in rustls).
//! Path-style addressing (`{endpoint}/{bucket}/{key}`) so the same code works
//! against R2 and MinIO. Only what content authoring needs: get + put one object.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Credentials + addressing for an S3-compatible bucket (R2/MinIO).
pub struct R2Store {
    /// Base endpoint, no trailing slash (e.g. `https://acct.r2.cloudflarestorage.com`).
    endpoint: String,
    /// The endpoint host (incl. port) — the SigV4 `host` header.
    host: String,
    bucket: String,
    /// SigV4 region: `auto` for R2, the bucket's region for MinIO (`us-east-1`).
    region: String,
    key_id: String,
    secret: String,
}

impl R2Store {
    /// The **content** store from env. All four core vars required, else `None`
    /// (no R2 writes — the gate falls back to disk persistence): `R2_S3_ENDPOINT`,
    /// `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`. `R2_REGION`
    /// optional (default `auto`, which R2 accepts).
    pub fn from_env() -> Option<Self> {
        Self::build(
            envv("R2_S3_ENDPOINT")?,
            envv("R2_BUCKET")?,
            envv("R2_ACCESS_KEY_ID")?,
            envv("R2_SECRET_ACCESS_KEY")?,
            envv("R2_REGION"),
        )
    }

    /// The **texture** store from env — a SEPARATE bucket (masters live in a
    /// different R2 bucket than DSL content, e.g. `resonantdust-assets`). Reuses
    /// the content account's endpoint/creds/region by default, overridable per
    /// field. `TEXTURE_R2_BUCKET` is required (else `None` → texture authoring
    /// unconfigured): `TEXTURE_R2_BUCKET`, plus `TEXTURE_R2_ENDPOINT` /
    /// `TEXTURE_R2_ACCESS_KEY_ID` / `TEXTURE_R2_SECRET_ACCESS_KEY` /
    /// `TEXTURE_R2_REGION` falling back to the `R2_*` equivalents.
    pub fn textures_from_env() -> Option<Self> {
        let bucket = envv("TEXTURE_R2_BUCKET")?;
        let endpoint = envv("TEXTURE_R2_ENDPOINT").or_else(|| envv("R2_S3_ENDPOINT"))?;
        let key_id = envv("TEXTURE_R2_ACCESS_KEY_ID").or_else(|| envv("R2_ACCESS_KEY_ID"))?;
        let secret = envv("TEXTURE_R2_SECRET_ACCESS_KEY").or_else(|| envv("R2_SECRET_ACCESS_KEY"))?;
        let region = envv("TEXTURE_R2_REGION").or_else(|| envv("R2_REGION"));
        Self::build(endpoint, bucket, key_id, secret, region)
    }

    fn build(
        endpoint: String,
        bucket: String,
        key_id: String,
        secret: String,
        region: Option<String>,
    ) -> Option<Self> {
        let region = region.unwrap_or_else(|| "auto".to_string());
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let host = endpoint
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(&endpoint)
            .split('/')
            .next()
            .unwrap_or("")
            .to_string();
        Some(Self { endpoint, host, bucket, region, key_id, secret })
    }

    /// GET an object's body as UTF-8 text.
    pub async fn get(&self, key: &str) -> Result<String, String> {
        let resp = self.signed(reqwest::Method::GET, key, &[]).await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("S3 GET {key}: HTTP {status}"));
        }
        resp.text().await.map_err(|e| format!("S3 GET {key}: read body: {e}"))
    }

    /// PUT `body` at `key` (overwriting).
    pub async fn put(&self, key: &str, body: &[u8]) -> Result<(), String> {
        let resp = self.signed(reqwest::Method::PUT, key, body).await?;
        let status = resp.status();
        if !status.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(format!("S3 PUT {key}: HTTP {status}: {detail}"));
        }
        Ok(())
    }

    /// Sign + send one path-style request. Signs `host` / `x-amz-date` /
    /// `x-amz-content-sha256` (other headers reqwest adds stay unsigned, which
    /// SigV4 allows). `x-amz-content-sha256` is the real payload hash, which both
    /// R2 and MinIO accept.
    async fn signed(
        &self,
        method: reqwest::Method,
        key: &str,
        body: &[u8],
    ) -> Result<reqwest::Response, String> {
        let canonical_uri = format!("/{}/{}", self.bucket, uri_encode_path(key));
        let url = format!("{}{}", self.endpoint, canonical_uri);

        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let payload_hash = hex::encode(Sha256::digest(body));
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            self.host, payload_hash, amz_date
        );
        // method \n uri \n query \n headers \n signed_headers \n payload_hash
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        let scope = format!("{date_stamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = self.signature(&date_stamp, &string_to_sign);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.key_id
        );

        let client = crate::connections::http_client();
        let mut req = client
            .request(method.clone(), &url)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &payload_hash)
            .header(reqwest::header::AUTHORIZATION, authorization);
        if method == reqwest::Method::PUT {
            req = req.body(body.to_vec());
        }
        req.send().await.map_err(|e| format!("S3 {method} {key}: send: {e}"))
    }

    /// Derive the SigV4 signing key (date→region→service→`aws4_request`) and sign.
    fn signature(&self, date_stamp: &str, string_to_sign: &str) -> String {
        let k_date = hmac(format!("AWS4{}", self.secret).as_bytes(), date_stamp.as_bytes());
        let k_region = hmac(&k_date, self.region.as_bytes());
        let k_service = hmac(&k_region, b"s3");
        let k_signing = hmac(&k_service, b"aws4_request");
        hex::encode(hmac(&k_signing, string_to_sign.as_bytes()))
    }
}

/// A non-empty env var, or `None`.
fn envv(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Percent-encode a key per RFC3986 unreserved, preserving `/` (our keys are all
/// safe chars, but be correct for arbitrary names).
fn uri_encode_path(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for &b in key.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
