use super::*;
use polymarket_client_sdk_v2::auth::state::Authenticated as SdkAuthenticatedState;
use polymarket_client_sdk_v2::auth::Normal as SdkAuthNormal;
use polymarket_client_sdk_v2::auth::{
    Credentials as SdkCredentials, LocalSigner as SdkLocalSigner, Signer as _, Uuid,
};
pub(super) type CachedSigner = alloy::signers::local::PrivateKeySigner;
use polymarket_client_sdk_v2::clob::types::{
    Amount as SdkAmount, OrderType as SdkOrderType, Side as SdkSide,
    SignatureType as SdkSignatureType, SignedOrder as SdkSignedOrder,
};
use polymarket_client_sdk_v2::clob::{Client as SdkClient, Config as SdkConfig};
use polymarket_client_sdk_v2::types::{Address as SdkAddress, Decimal as SdkDecimal, U256 as SdkU256};
use std::str::FromStr;

mod flow;
mod presign_pool;
pub(crate) mod sdk_exec;
mod submitter;
mod types;

pub(crate) use presign_pool::warm_presign_startup_into;
pub(crate) use submitter::run_submitter_async;
pub(crate) use types::{DispatchHandle, OrderSubmitter, PresignTemplateData, SubmitBatch, SubmitWork};
pub(crate) use types::OrderRequestData;
pub(super) use types::PolymarketSdkRuntime;

pub(crate) fn build_dispatch_config(
    exec_cfg: ExecStartConfig,
    amount_usdc: f64,
    size_shares: f64,
    limit_price: f64,
    time_in_force: String,
) -> Result<DispatchConfig, String> {
    let mode_text = exec_cfg
        .dispatch_mode
        .unwrap_or_else(|| "noop".to_string())
        .trim()
        .to_lowercase();
    let mode = match mode_text.as_str() {
        "http" => DispatchMode::Http,
        "noop" | "" => DispatchMode::Noop,
        other => return Err(format!("unsupported_dispatch_mode:{}", other)),
    };

    let funder = exec_cfg
        .funder
        .clone()
        .unwrap_or_else(|| std::env::var("POLY_EXEC_FUNDER").unwrap_or_default());
    let signature_type = exec_cfg.signature_type.unwrap_or_else(|| {
        std::env::var("POLY_EXEC_SIGNATURE_TYPE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or_else(|| if funder.trim().is_empty() { 0 } else { 1 })
    });
    Ok(DispatchConfig {
        mode,
        clob_host: exec_cfg
            .clob_host
            .unwrap_or_else(|| "https://clob.polymarket.com".to_string()),
        api_key: exec_cfg.api_key.unwrap_or_default(),
        api_secret: exec_cfg.api_secret.unwrap_or_default(),
        api_passphrase: exec_cfg.api_passphrase.unwrap_or_default(),
        funder,
        signature_type,
        chain_id: exec_cfg.chain_id.unwrap_or(137),
        presign_enabled: exec_cfg.presign_enabled.unwrap_or(false),
        presign_private_key: exec_cfg.presign_private_key.unwrap_or_else(|| {
            std::env::var("POLY_EXEC_PRESIGN_PRIVATE_KEY")
                .ok()
                .or_else(|| std::env::var("POLY_EXEC_PRIVATE_KEY").ok())
                .or_else(|| std::env::var("PRIVATE_KEY").ok())
                .unwrap_or_default()
        }),
        presign_startup_warm_timeout_seconds: exec_cfg
            .presign_startup_warm_timeout_seconds
            .unwrap_or(5.0)
            .max(0.1),
        amount_usdc,
        size_shares,
        limit_price,
        time_in_force: parse_time_in_force(time_in_force.as_str())?,
    })
}

/// Wall-clock nanoseconds since UNIX epoch. Retained for future L2 auth header
/// timestamps and ad-hoc diagnostics; not used on the live path (the WS worker
/// uses an `Instant`-based monotonic clock for dedup/cooldown deltas).
#[allow(dead_code)]
pub(crate) fn now_unix_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(_) => 0,
    }
}

pub(crate) fn now_unix_s() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => 0,
    }
}

pub(super) fn normalize_side(side: &str) -> String {
    let raw = side.trim().to_lowercase();
    if raw == "yes" || raw == "buy_yes" {
        "buy_yes".to_string()
    } else if raw == "no" || raw == "buy_no" {
        "buy_no".to_string()
    } else {
        raw
    }
}

pub(super) fn parse_time_in_force(tif: &str) -> Result<OrderTimeInForce, String> {
    let raw = tif.trim().to_uppercase();
    let raw = if raw.is_empty() { "FAK".to_string() } else { raw };
    match raw.as_str() {
        "FAK" => Ok(OrderTimeInForce::FAK),
        "FOK" => Ok(OrderTimeInForce::FOK),
        "GTC" => Ok(OrderTimeInForce::GTC),
        other => Err(format!("unsupported_time_in_force:{}", other)),
    }
}

pub(super) fn map_sdk_signature_type(signature_type: i64) -> Result<SdkSignatureType, String> {
    match signature_type {
        0 => Ok(SdkSignatureType::Eoa),
        1 => Ok(SdkSignatureType::Proxy),
        2 => Ok(SdkSignatureType::GnosisSafe),
        other => Err(format!("unsupported_signature_type:{}", other)),
    }
}

pub(crate) fn map_sdk_order_type(tif: OrderTimeInForce) -> SdkOrderType {
    match tif {
        OrderTimeInForce::FAK => SdkOrderType::FAK,
        OrderTimeInForce::FOK => SdkOrderType::FOK,
        OrderTimeInForce::GTC => SdkOrderType::GTC,
    }
}

pub(super) fn parse_decimal_from_f64(
    value: f64,
    precision: usize,
    label: &str,
) -> Result<SdkDecimal, String> {
    let normalized = value.max(0.0);
    let text = format!("{:.*}", precision, normalized);
    SdkDecimal::from_str(text.as_str()).map_err(|e| format!("invalid_decimal_{}:{}", label, e))
}

pub(super) fn parse_sdk_token_id(token_id: &str) -> Result<SdkU256, String> {
    SdkU256::from_str(token_id.trim()).map_err(|e| format!("invalid_token_id:{}:{}", token_id, e))
}

pub(super) fn map_sdk_side(side: &str) -> Result<SdkSide, String> {
    let normalized = normalize_side(side);
    match normalized.as_str() {
        "buy_yes" | "buy_no" => Ok(SdkSide::Buy),
        "sell_yes" | "sell_no" => Err(format!(
            "dispatch_side_unsupported:{}:sell_requires_share_amount",
            normalized
        )),
        _ => Err(format!("dispatch_side_unsupported:{}", normalized)),
    }
}

pub(super) fn redact_token_id(token_id: &str) -> String {
    let t = token_id.trim();
    if t.len() <= 12 {
        return t.to_string();
    }
    let head = &t[..6];
    let tail = &t[t.len().saturating_sub(6)..];
    format!("{}...{}", head, tail)
}

#[cfg(test)]
mod tests;
