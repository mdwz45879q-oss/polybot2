use super::*;

fn env_enabled(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_or_default(name: &str, default: &str) -> String {
    let val = std::env::var(name).unwrap_or_default();
    let trimmed = val.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
    }
}

fn contains_min_notional_rejection(err: &str) -> bool {
    let lowered = err.to_ascii_lowercase();
    lowered.contains("market buys must be greater than $1")
        || (lowered.contains("marketable buy order") && lowered.contains("min size: $1"))
}

#[test]
fn presign_key_is_token_only() {
    let key_a = PreSignKey { token_id: "t".to_string() };
    let key_b = PreSignKey { token_id: "t".to_string() };
    assert_eq!(key_a, key_b);
    let key_c = PreSignKey { token_id: "other".to_string() };
    assert_ne!(key_a, key_c);
}

#[test]
fn submit_presigned_miss_is_fail_closed() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        presign_pool_target_per_key: 2,
        ..DispatchConfig::default()
    };
    let mut rt = DispatchRuntime::new(cfg);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.dispatch_order("t"))
        .expect_err("empty presign pool must fail closed");
    assert!(err.contains("submit_presigned_miss"));
}

#[test]
fn startup_warm_fails_when_templates_missing() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        presign_pool_target_per_key: 1,
        presign_startup_warm_timeout_seconds: 0.01,
        ..DispatchConfig::default()
    };
    let mut rt = DispatchRuntime::new(cfg);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.warm_presign_startup_async())
        .expect_err("missing templates should fail startup warm");
    assert!(err.contains("presign_startup_warm_no_templates"));
}

#[test]
fn noop_dispatch_succeeds() {
    let mut rt = DispatchRuntime::new(DispatchConfig::default());
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt
        .block_on(rt.dispatch_order("t1"))
        .expect("noop dispatch should succeed");
}

#[test]
fn map_sdk_order_type_all_variants() {
    assert_eq!(map_sdk_order_type(OrderTimeInForce::FAK), SdkOrderType::FAK);
    assert_eq!(map_sdk_order_type(OrderTimeInForce::FOK), SdkOrderType::FOK);
    assert_eq!(map_sdk_order_type(OrderTimeInForce::GTC), SdkOrderType::GTC);
}

#[test]
fn parse_time_in_force_all_variants() {
    assert_eq!(parse_time_in_force("FAK").unwrap(), OrderTimeInForce::FAK);
    assert_eq!(parse_time_in_force("fak").unwrap(), OrderTimeInForce::FAK);
    assert_eq!(parse_time_in_force("FOK").unwrap(), OrderTimeInForce::FOK);
    assert_eq!(parse_time_in_force("GTC").unwrap(), OrderTimeInForce::GTC);
    assert_eq!(parse_time_in_force("").unwrap(), OrderTimeInForce::FAK);
    assert!(parse_time_in_force("GTD").is_err());
    assert!(parse_time_in_force("BOGUS").is_err());
}

#[test]
fn sdk_side_mapping_accepts_buy_and_rejects_sell_notional() {
    assert!(matches!(map_sdk_side("buy_yes"), Ok(SdkSide::Buy)));
    assert!(matches!(map_sdk_side("buy_no"), Ok(SdkSide::Buy)));
    let err =
        map_sdk_side("sell_yes").expect_err("sell should be rejected for usdc notional flow");
    assert!(err.contains("sell_requires_share_amount"));
}

fn build_live_dispatch_config() -> Option<DispatchConfig> {
    if !env_enabled("POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST") {
        return None;
    }
    let token_id = env_or_default("POLYBOT2_LIVE_EXEC_TOKEN_ID", "");
    if token_id.trim().is_empty() {
        return None;
    }
    let mut cfg = DispatchConfig {
        mode: DispatchMode::Http,
        clob_host: env_or_default("POLY_EXEC_CLOB_HOST", "https://clob.polymarket.com"),
        api_key: env_or_default("POLY_EXEC_API_KEY", ""),
        api_secret: env_or_default("POLY_EXEC_API_SECRET", ""),
        api_passphrase: env_or_default("POLY_EXEC_API_PASSPHRASE", ""),
        funder: env_or_default("POLY_EXEC_FUNDER", ""),
        signature_type: env_or_default("POLY_EXEC_SIGNATURE_TYPE", "1")
            .parse::<i64>()
            .unwrap_or(1),
        presign_private_key: env_or_default("POLY_EXEC_PRESIGN_PRIVATE_KEY", ""),
        ..DispatchConfig::default()
    };
    cfg.presign_enabled = false;
    if cfg.api_key.trim().is_empty()
        || cfg.api_secret.trim().is_empty()
        || cfg.api_passphrase.trim().is_empty()
        || cfg.presign_private_key.trim().is_empty()
    {
        return None;
    }
    Some(cfg)
}

fn contains_min_size_rejection(err: &str) -> bool {
    let lowered = err.to_ascii_lowercase();
    (lowered.contains("minimum") && lowered.contains("shares"))
        || lowered.contains("min size")
        || lowered.contains("minimum order size")
        || (lowered.contains("lower than the minimum") && lowered.contains("size"))
}

#[test]
fn live_rust_submit_min_notional_rejection() {
    if !env_enabled("POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST") {
        eprintln!(
            "skipping live rust execution test; set POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1"
        );
        return;
    }

    let token_id = env_or_default("POLYBOT2_LIVE_EXEC_TOKEN_ID", "");
    assert!(
        !token_id.trim().is_empty(),
        "POLYBOT2_LIVE_EXEC_TOKEN_ID is required when POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1"
    );

    let mut cfg = DispatchConfig {
        mode: DispatchMode::Http,
        clob_host: env_or_default("POLY_EXEC_CLOB_HOST", "https://clob.polymarket.com"),
        api_key: env_or_default("POLY_EXEC_API_KEY", ""),
        api_secret: env_or_default("POLY_EXEC_API_SECRET", ""),
        api_passphrase: env_or_default("POLY_EXEC_API_PASSPHRASE", ""),
        funder: env_or_default("POLY_EXEC_FUNDER", ""),
        signature_type: env_or_default("POLY_EXEC_SIGNATURE_TYPE", "1")
            .parse::<i64>()
            .unwrap_or(1),
        presign_private_key: env_or_default("POLY_EXEC_PRESIGN_PRIVATE_KEY", ""),
        amount_usdc: 0.5,
        limit_price: 0.5,
        size_shares: 1.0,
        time_in_force: OrderTimeInForce::FAK,
        ..DispatchConfig::default()
    };
    cfg.presign_enabled = false;

    let mut rt = DispatchRuntime::new(cfg);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.dispatch_order(token_id.as_str()))
        .expect_err("sub-$1 notional should be rejected");
    assert!(
        contains_min_notional_rejection(err.as_str()),
        "unexpected live rejection: {}",
        err
    );
}

#[test]
fn live_rust_submit_fok_min_notional_rejection() {
    let Some(mut cfg) = build_live_dispatch_config() else {
        eprintln!("skipping live FOK test; set POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1");
        return;
    };
    let token_id = env_or_default("POLYBOT2_LIVE_EXEC_TOKEN_ID", "");
    cfg.amount_usdc = 0.5;
    cfg.limit_price = 0.5;
    cfg.size_shares = 1.0;
    cfg.time_in_force = OrderTimeInForce::FOK;
    let mut rt = DispatchRuntime::new(cfg);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.dispatch_order(token_id.as_str()))
        .expect_err("FOK with sub-$1 notional should be rejected by exchange");
    assert!(
        contains_min_notional_rejection(err.as_str()),
        "unexpected FOK live rejection: {}",
        err
    );
}

#[test]
fn live_rust_submit_gtc_min_size_rejection() {
    let Some(mut cfg) = build_live_dispatch_config() else {
        eprintln!("skipping live GTC test; set POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1");
        return;
    };
    let token_id = env_or_default("POLYBOT2_LIVE_EXEC_TOKEN_ID", "");
    cfg.amount_usdc = 1.0;
    cfg.limit_price = 0.5;
    cfg.size_shares = 2.0;
    cfg.time_in_force = OrderTimeInForce::GTC;
    let mut rt = DispatchRuntime::new(cfg);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.dispatch_order(token_id.as_str()))
        .expect_err("GTC with size < 5 shares should be rejected by exchange");
    assert!(
        contains_min_size_rejection(err.as_str()),
        "unexpected GTC live rejection: {}",
        err
    );
}

