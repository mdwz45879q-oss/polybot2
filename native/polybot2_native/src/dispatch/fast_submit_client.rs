use super::*;
use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use memchr::memchr;
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::{Client as ReqwestClient, Method, StatusCode};
use serde::de::DeserializeOwned;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

const POLY_ADDRESS: &str = "POLY_ADDRESS";
const POLY_API_KEY: &str = "POLY_API_KEY";
const POLY_PASSPHRASE: &str = "POLY_PASSPHRASE";
const POLY_SIGNATURE: &str = "POLY_SIGNATURE";
const POLY_TIMESTAMP: &str = "POLY_TIMESTAMP";
const HMAC_B64_LEN: usize = 44;

pub(crate) struct FastClobSubmitClient {
    http: ReqwestClient,
    order_url: String,
    orders_url: String,
    poly_address: HeaderValue,
    poly_api_key: HeaderValue,
    poly_passphrase: HeaderValue,
    decoded_secret: Vec<u8>,
}

impl FastClobSubmitClient {
    pub(crate) fn new(
        cfg: &DispatchConfig,
        signer_address_checksum: String,
    ) -> Result<Self, String> {
        let host = normalize_host(cfg.clob_host.as_str());
        let order_url = format!("{}order", host);
        let orders_url = format!("{}orders", host);
        let poly_address = HeaderValue::from_str(signer_address_checksum.as_str())
            .map_err(|e| format!("submitter_invalid_poly_address:{}", e))?;
        let poly_api_key = HeaderValue::from_str(cfg.api_key.trim())
            .map_err(|e| format!("submitter_invalid_api_key_header:{}", e))?;
        let poly_passphrase = HeaderValue::from_str(cfg.api_passphrase.as_str())
            .map_err(|e| format!("submitter_invalid_passphrase_header:{}", e))?;
        let decoded_secret = URL_SAFE
            .decode(cfg.api_secret.trim())
            .map_err(|e| format!("submitter_invalid_api_secret_base64:{}", e))?;
        let http = ReqwestClient::builder()
            .build()
            .map_err(|e| format!("submitter_reqwest_client_new:{}", e))?;
        Ok(Self {
            http,
            order_url,
            orders_url,
            poly_address,
            poly_api_key,
            poly_passphrase,
            decoded_secret,
        })
    }

    pub(crate) async fn post_order_bytes_single(
        &self,
        body: Vec<u8>,
    ) -> Result<PostOrderResponse, String> {
        let timestamp = timestamp_now_seconds();
        let mut ts_buf = itoa::Buffer::new();
        let ts_text = ts_buf.format(timestamp);
        let timestamp_header = HeaderValue::from_str(ts_text)
            .map_err(|e| format!("submitter_invalid_timestamp_header:{}", e))?;

        let (sig_bytes, sig_len) = self.hmac_signature_b64_into_stack_trusted_json(
            timestamp,
            Method::POST.as_str(),
            "/order",
            body.as_slice(),
        )?;
        let signature_header = HeaderValue::from_bytes(&sig_bytes[..sig_len])
            .map_err(|e| format!("submitter_invalid_signature_header:{}", e))?;

        let response = self
            .http
            .request(Method::POST, self.order_url.as_str())
            .header(POLY_ADDRESS, self.poly_address.clone())
            .header(POLY_API_KEY, self.poly_api_key.clone())
            .header(POLY_PASSPHRASE, self.poly_passphrase.clone())
            .header(POLY_SIGNATURE, signature_header)
            .header(POLY_TIMESTAMP, timestamp_header)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = response.status();
        let raw = response.bytes().await.map_err(|e| e.to_string())?;
        parse_json_response(status, raw.as_ref())
    }

    pub(crate) async fn post_orders_bytes(
        &self,
        body: Vec<u8>,
    ) -> Result<Vec<PostOrderResponse>, String> {
        self.send_json(Method::POST, self.orders_url.as_str(), "/orders", body)
            .await
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        method: Method,
        url: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<T, String> {
        let timestamp = timestamp_now_seconds();
        let signature =
            self.signature_for_parts(timestamp, method.as_str(), path, body.as_slice())?;
        let headers = self.headers(timestamp, signature.as_str())?;

        let response = self
            .http
            .request(method, url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = response.status();
        let raw = response.bytes().await.map_err(|e| e.to_string())?;
        parse_json_response(status, raw.as_ref())
    }

    fn headers(&self, timestamp: i64, signature: &str) -> Result<HeaderMap, String> {
        let mut headers = HeaderMap::with_capacity(6);
        headers.insert(POLY_ADDRESS, self.poly_address.clone());
        headers.insert(POLY_API_KEY, self.poly_api_key.clone());
        headers.insert(POLY_PASSPHRASE, self.poly_passphrase.clone());
        headers.insert(
            POLY_SIGNATURE,
            HeaderValue::from_str(signature)
                .map_err(|e| format!("submitter_invalid_signature_header:{}", e))?,
        );
        headers.insert(
            POLY_TIMESTAMP,
            HeaderValue::from_str(timestamp.to_string().as_str())
                .map_err(|e| format!("submitter_invalid_timestamp_header:{}", e))?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    pub(crate) fn signature_for_parts(
        &self,
        timestamp: i64,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<String, String> {
        let (sig_bytes, sig_len) =
            self.hmac_signature_b64_into_stack(timestamp, method, path, body)?;
        let sig_str = std::str::from_utf8(&sig_bytes[..sig_len])
            .map_err(|e| format!("submitter_invalid_signature_utf8:{}", e))?;
        Ok(sig_str.to_owned())
    }

    fn hmac_signature_b64_into_stack(
        &self,
        timestamp: i64,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<([u8; HMAC_B64_LEN], usize), String> {
        self.hmac_signature_b64_impl(timestamp, method, path, body, true)
    }

    fn hmac_signature_b64_into_stack_trusted_json(
        &self,
        timestamp: i64,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<([u8; HMAC_B64_LEN], usize), String> {
        self.hmac_signature_b64_impl(timestamp, method, path, body, false)
    }

    fn hmac_signature_b64_impl(
        &self,
        timestamp: i64,
        method: &str,
        path: &str,
        body: &[u8],
        normalize_apostrophes: bool,
    ) -> Result<([u8; HMAC_B64_LEN], usize), String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.decoded_secret.as_slice())
            .map_err(|e| e.to_string())?;

        let mut ts_buf = itoa::Buffer::new();
        let ts_text = ts_buf.format(timestamp);
        mac.update(ts_text.as_bytes());
        mac.update(method.as_bytes());
        mac.update(path.as_bytes());
        if normalize_apostrophes {
            append_body_with_quote_normalization_to_hmac(&mut mac, body);
        } else {
            mac.update(body);
        }

        let digest = mac.finalize().into_bytes();
        let digest_bytes: &[u8] = &digest;
        let mut out = [0u8; HMAC_B64_LEN];
        let written = URL_SAFE
            .encode_slice(digest_bytes, &mut out)
            .map_err(|e| format!("submitter_signature_encode:{}", e))?;
        Ok((out, written))
    }

    #[cfg(test)]
    pub(crate) fn single_order_auth_headers_for_test(
        &self,
        timestamp: i64,
        body: &[u8],
    ) -> Result<(HeaderValue, HeaderValue), String> {
        let (sig_bytes, sig_len) =
            self.hmac_signature_b64_into_stack(timestamp, Method::POST.as_str(), "/order", body)?;
        let sig = HeaderValue::from_bytes(&sig_bytes[..sig_len])
            .map_err(|e| format!("submitter_invalid_signature_header:{}", e))?;
        let mut ts_buf = itoa::Buffer::new();
        let ts = HeaderValue::from_str(ts_buf.format(timestamp))
            .map_err(|e| format!("submitter_invalid_timestamp_header:{}", e))?;
        Ok((sig, ts))
    }
}

pub(crate) fn build_orders_body_from_slices(order_jsons: &[&[u8]]) -> Vec<u8> {
    if order_jsons.is_empty() {
        return b"[]".to_vec();
    }
    let mut capacity = 2usize;
    for body in order_jsons {
        capacity += body.len();
    }
    capacity += order_jsons.len().saturating_sub(1);

    let mut out = Vec::with_capacity(capacity);
    out.push(b'[');
    for (idx, body) in order_jsons.iter().enumerate() {
        if idx > 0 {
            out.push(b',');
        }
        out.extend_from_slice(body);
    }
    out.push(b']');
    out
}

fn normalize_host(host: &str) -> String {
    let trimmed = host.trim();
    if trimmed.ends_with('/') {
        trimmed.to_string()
    } else {
        format!("{}/", trimmed)
    }
}

fn timestamp_now_seconds() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => 0,
    }
}

fn append_body_with_quote_normalization_to_hmac(mac: &mut Hmac<Sha256>, body: &[u8]) {
    if memchr(b'\'', body).is_none() {
        mac.update(body);
        return;
    }

    let mut start = 0usize;
    while let Some(pos) = memchr(b'\'', &body[start..]) {
        let idx = start + pos;
        if idx > start {
            mac.update(&body[start..idx]);
        }
        mac.update(b"\"");
        start = idx + 1;
    }
    if start < body.len() {
        mac.update(&body[start..]);
    }
}

fn parse_json_response<T: DeserializeOwned>(status: StatusCode, raw: &[u8]) -> Result<T, String> {
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(raw);
        return Err(format!(
            "status={} body={}",
            status.as_u16(),
            truncate_err(body_text.as_ref(), 512)
        ));
    }

    serde_json::from_slice::<T>(raw).map_err(|e| {
        let body_text = String::from_utf8_lossy(raw);
        format!(
            "response_parse:{} body={}",
            e,
            truncate_err(body_text.as_ref(), 512)
        )
    })
}

fn truncate_err(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        &s[..max_len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn fixture_client() -> FastClobSubmitClient {
        let cfg = DispatchConfig {
            clob_host: "http://localhost".to_string(),
            api_key: "00000000-0000-0000-0000-000000000000".to_string(),
            api_secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            api_passphrase: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            ..DispatchConfig::default()
        };
        FastClobSubmitClient::new(
            &cfg,
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_string(),
        )
        .expect("fixture client")
    }

    fn legacy_signature_for_parts(
        client: &FastClobSubmitClient,
        timestamp: i64,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> String {
        let text = String::from_utf8_lossy(body);
        let body_text = if text.contains('\'') {
            text.replace('\'', "\"")
        } else {
            text.to_string()
        };
        let message = format!("{}{}{}{}", timestamp, method, path, body_text);
        let mut mac =
            Hmac::<Sha256>::new_from_slice(client.decoded_secret.as_slice()).expect("hmac key");
        mac.update(message.as_bytes());
        URL_SAFE.encode(mac.finalize().into_bytes())
    }

    #[test]
    fn signature_matches_sdk_auth_fixture() {
        let client = fixture_client();
        let signature = client
            .signature_for_parts(1_000_000, "test-sign", "/orders", br#"{"hash":"0x123"}"#)
            .expect("signature");
        assert_eq!(signature, "4gJVbox-R6XlDK4nlaicig0_ANVL1qdcahiL8CXfXLM=");
    }

    #[test]
    fn signature_matches_sdk_l2_header_fixture() {
        let client = fixture_client();
        let signature = client
            .signature_for_parts(1, "GET", "/", b"")
            .expect("signature");
        assert_eq!(signature, "eHaylCwqRSOa2LFD77Nt_SaTpbsxzN8eTEI3LryhEj4=");
    }

    #[test]
    fn signature_quote_normalization_parity_with_legacy() {
        let client = fixture_client();
        let body = br#"{'hash':'0xabc','maker':'0xdef'}"#;
        let new_sig = client
            .signature_for_parts(987_654_321, "POST", "/order", body)
            .expect("new signature");
        let legacy_sig = legacy_signature_for_parts(&client, 987_654_321, "POST", "/order", body);
        assert_eq!(new_sig, legacy_sig);
    }

    #[test]
    fn single_order_header_construction_and_response_parse() {
        let client = fixture_client();
        let (sig, ts) = client
            .single_order_auth_headers_for_test(12345, br#"{"foo":"bar"}"#)
            .expect("headers");
        assert_eq!(ts.to_str().expect("timestamp utf8"), "12345");
        assert!(!sig.to_str().expect("sig utf8").is_empty());

        let sample = br#"{"errorMsg":null,"makingAmount":"1","takingAmount":"1","orderID":"oid123","status":"LIVE","success":true,"transactionsHashes":[],"trade_ids":[]}"#;
        let parsed = parse_json_response::<PostOrderResponse>(StatusCode::OK, sample)
            .expect("post order response parse");
        assert!(parsed.success);
        assert_eq!(parsed.order_id, "oid123");
    }

    #[test]
    fn build_orders_body_joins_json_without_reserialization() {
        let one = br#"{"a":1}"#.as_slice();
        let two = br#"{"b":2}"#.as_slice();
        let out = build_orders_body_from_slices(&[one, two]);
        assert_eq!(out, br#"[{"a":1},{"b":2}]"#);
    }

    #[test]
    #[ignore]
    fn single_intent_auth_prep_faster_than_legacy_synthetic() {
        let client = fixture_client();
        let body = br#"{"salt":"123456789012345678901234567890","maker":"0x1111222233334444555566667777888899990000","signer":"0xaabbccddeeff0011223344556677889900aabbcc","tokenId":"123456789","makerAmount":"1000000","takerAmount":"990000","expiration":"1712345678","side":"BUY"}"#.to_vec();
        let iters = 180_000usize;

        let legacy_start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(legacy_signature_for_parts(
                &client,
                1_700_000_123,
                "POST",
                "/order",
                body.as_slice(),
            ));
        }
        let legacy_elapsed = legacy_start.elapsed();

        let new_start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(
                client
                    .hmac_signature_b64_into_stack_trusted_json(
                        1_700_000_123,
                        "POST",
                        "/order",
                        body.as_slice(),
                    )
                    .expect("new signature"),
            );
        }
        let new_elapsed = new_start.elapsed();

        println!(
            "[single_intent_auth_prep] legacy={:?} new={:?} speedup={:.2}x",
            legacy_elapsed,
            new_elapsed,
            legacy_elapsed.as_secs_f64() / new_elapsed.as_secs_f64().max(1e-9)
        );
        assert!(
            new_elapsed < legacy_elapsed,
            "expected new stack-based auth prep to beat legacy alloc path"
        );
    }
}
