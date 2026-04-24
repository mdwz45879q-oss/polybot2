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

fn env_parse_f64(name: &str, default: f64) -> f64 {
    let raw = std::env::var(name).unwrap_or_default();
    raw.trim().parse::<f64>().unwrap_or(default)
}

fn test_intent(strategy_key: &str) -> Intent {
    Intent {
        strategy_key: strategy_key.to_string(),
        token_id: "t1".to_string(),
        side: "buy_yes".to_string(),
        amount_usdc: 5.0,
        size_shares: 5.0,
        limit_price: 0.52,
        time_in_force: "FAK".to_string(),
        condition_id: "c1".to_string(),
        source_universal_id: "u1".to_string(),
        chain_id: "u1:1".to_string(),
        reason: "test".to_string(),
        market_type: "totals".to_string(),
        outcome_semantic: "over".to_string(),
    }
}

fn contains_min_notional_rejection(err: &str) -> bool {
    let lowered = err.to_ascii_lowercase();
    lowered.contains("market buys must be greater than $1")
        || (lowered.contains("marketable buy order") && lowered.contains("min size: $1"))
}

#[test]
fn presign_key_is_token_only() {
    let cfg = DispatchConfig {
        presign_enabled: true,
        presign_pool_target_per_key: 2,
        ..DispatchConfig::default()
    };
    let rt = DispatchRuntime::new(cfg, None);
    let req_a = OrderRequestData {
        token_id: "t".to_string(),
        side: "buy_yes".to_string(),
        amount_usdc: 6.2,
        limit_price: 0.531,
        time_in_force: "FAK".to_string(),
        client_order_id: "x1".to_string(),
        size_shares: 6.2 / 0.531,
        expiration_ts: None,
    };
    let req_b = OrderRequestData {
        token_id: "t".to_string(),
        side: "buy_no".to_string(),
        amount_usdc: 50.0,
        limit_price: 0.11,
        time_in_force: "FOK".to_string(),
        client_order_id: "x2".to_string(),
        size_shares: 50.0 / 0.11,
        expiration_ts: None,
    };
    let key_a = rt.build_presign_key(&req_a);
    let key_b = rt.build_presign_key(&req_b);
    assert_eq!(key_a, key_b);
}

#[test]
fn submit_presigned_miss_is_fail_closed() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        presign_pool_target_per_key: 2,
        ..DispatchConfig::default()
    };
    let mut rt = DispatchRuntime::new(cfg, None);
    let req = OrderRequestData {
        token_id: "t".to_string(),
        side: "buy_yes".to_string(),
        amount_usdc: 5.0,
        limit_price: 0.5,
        time_in_force: "FAK".to_string(),
        client_order_id: "cid".to_string(),
        size_shares: 10.0,
        expiration_ts: None,
    };
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.submit_with_policy_async(&req))
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
    let mut rt = DispatchRuntime::new(cfg, None);
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
fn invalid_tif_intents_are_rejected() {
    let mut rt = DispatchRuntime::new(DispatchConfig::default(), None);
    let mut intent = test_intent("s1");
    intent.time_in_force = "BOGUS".to_string();
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let err = tokio_rt
        .block_on(rt.dispatch_intent_async(&intent))
        .expect_err("invalid TIF should be rejected");
    assert!(err.contains("dispatch_tif_invalid"));
}

#[test]
fn map_sdk_order_type_all_variants() {
    assert_eq!(map_sdk_order_type("FAK").unwrap(), SdkOrderType::FAK);
    assert_eq!(map_sdk_order_type("FOK").unwrap(), SdkOrderType::FOK);
    assert_eq!(map_sdk_order_type("GTC").unwrap(), SdkOrderType::GTC);
    assert_eq!(map_sdk_order_type("GTD").unwrap(), SdkOrderType::GTD);
    assert_eq!(map_sdk_order_type("fak").unwrap(), SdkOrderType::FAK);
    assert_eq!(map_sdk_order_type("").unwrap(), SdkOrderType::FAK);
    assert!(map_sdk_order_type("BOGUS").is_err());
}

#[test]
fn fok_intent_accepted_in_noop() {
    let mut rt = DispatchRuntime::new(DispatchConfig::default(), None);
    let mut intent = test_intent("s1");
    intent.time_in_force = "FOK".to_string();
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt
        .block_on(rt.dispatch_intent_async(&intent))
        .expect("FOK intent should be accepted in noop mode");
}

#[test]
fn gtc_intent_accepted_in_noop() {
    let mut rt = DispatchRuntime::new(DispatchConfig::default(), None);
    let mut intent = test_intent("s1");
    intent.time_in_force = "GTC".to_string();
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt
        .block_on(rt.dispatch_intent_async(&intent))
        .expect("GTC intent should be accepted in noop mode");
}

#[test]
fn stale_active_order_evicted_after_ttl() {
    let mut rt = DispatchRuntime::new(DispatchConfig::default(), None);
    let stale_ns = now_unix_ns() - 120_000_000_000; // 120s ago
    rt.active_orders_by_strategy.insert(
        "sk_stale".to_string(),
        ActiveOrderRef {
            client_order_id: "cid1".to_string(),
            exchange_order_id: "eid1".to_string(),
            status: "submitted".to_string(),
            source_universal_id: "g1".to_string(),
            chain_id: "g1:1".to_string(),
            inserted_ns: stale_ns,
        },
    );
    assert!(rt.active_orders_by_strategy.contains_key("sk_stale"));
    rt.evict_stale_active_orders();
    assert!(
        !rt.active_orders_by_strategy.contains_key("sk_stale"),
        "stale entry should be evicted after 60s TTL"
    );
}

#[test]
fn fresh_active_order_not_evicted() {
    let mut rt = DispatchRuntime::new(DispatchConfig::default(), None);
    rt.active_orders_by_strategy.insert(
        "sk_fresh".to_string(),
        ActiveOrderRef {
            client_order_id: "cid1".to_string(),
            exchange_order_id: "eid1".to_string(),
            status: "submitted".to_string(),
            source_universal_id: "g1".to_string(),
            chain_id: "g1:1".to_string(),
            inserted_ns: now_unix_ns(),
        },
    );
    rt.evict_stale_active_orders();
    assert!(
        rt.active_orders_by_strategy.contains_key("sk_fresh"),
        "fresh entry should survive eviction"
    );
}

#[test]
fn sdk_side_mapping_accepts_buy_and_rejects_sell_notional() {
    assert!(matches!(map_sdk_side("buy_yes"), Ok(SdkSide::Buy)));
    assert!(matches!(map_sdk_side("buy_no"), Ok(SdkSide::Buy)));
    let err =
        map_sdk_side("sell_yes").expect_err("sell should be rejected for usdc notional flow");
    assert!(err.contains("sell_requires_share_amount"));
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
        ..DispatchConfig::default()
    };
    cfg.presign_enabled = false;

    assert!(
        !cfg.api_key.trim().is_empty()
            && !cfg.api_secret.trim().is_empty()
            && !cfg.api_passphrase.trim().is_empty()
            && !cfg.funder.trim().is_empty()
            && cfg.signature_type == 1
            && !cfg.presign_private_key.trim().is_empty(),
        "POLY_EXEC_API_KEY/POLY_EXEC_API_SECRET/POLY_EXEC_API_PASSPHRASE, POLY_EXEC_FUNDER, POLY_EXEC_SIGNATURE_TYPE=1 and POLY_EXEC_PRESIGN_PRIVATE_KEY are required"
    );

    let mut rt = DispatchRuntime::new(cfg, None);
    let notional = env_parse_f64("POLYBOT2_LIVE_EXEC_NOTIONAL_USDC", 0.5);
    let price = env_parse_f64("POLYBOT2_LIVE_EXEC_LIMIT_PRICE", 0.5);
    let request = OrderRequestData {
        token_id,
        side: "buy_yes".to_string(),
        amount_usdc: notional,
        limit_price: price,
        time_in_force: "FAK".to_string(),
        client_order_id: format!("rust_live_exec_{}", now_unix_s()),
        size_shares: notional / price.max(0.001),
        expiration_ts: None,
    };

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = match tokio_rt.block_on(rt.submit_with_policy_async(&request)) {
        Ok(state) => panic!(
            "expected min-notional rejection, got success status={} exchange_order_id={}",
            state.status, state.exchange_order_id
        ),
        Err(e) => e,
    };
    assert!(
        contains_min_notional_rejection(err.as_str()),
        "unexpected live rejection: {}",
        err
    );
    assert!(
        !err.to_ascii_lowercase().contains("invalid order payload"),
        "payload contract still invalid: {}",
        err
    );
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
    lowered.contains("minimum") && lowered.contains("shares")
        || lowered.contains("min size")
        || lowered.contains("minimum order size")
}

#[test]
fn live_rust_submit_fok_min_notional_rejection() {
    let Some(cfg) = build_live_dispatch_config() else {
        eprintln!("skipping live FOK test; set POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1");
        return;
    };
    let token_id = env_or_default("POLYBOT2_LIVE_EXEC_TOKEN_ID", "");
    let mut rt = DispatchRuntime::new(cfg, None);
    let request = OrderRequestData {
        token_id,
        side: "buy_yes".to_string(),
        amount_usdc: 0.5,
        limit_price: 0.5,
        time_in_force: "FOK".to_string(),
        client_order_id: format!("rust_live_fok_{}", now_unix_s()),
        size_shares: 1.0,
        expiration_ts: None,
    };

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.submit_with_policy_async(&request))
        .expect_err("FOK with sub-$1 notional should be rejected by exchange");
    assert!(
        contains_min_notional_rejection(err.as_str()),
        "unexpected FOK live rejection: {}",
        err
    );
}

#[test]
fn live_rust_submit_gtc_min_size_rejection() {
    let Some(cfg) = build_live_dispatch_config() else {
        eprintln!("skipping live GTC test; set POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1");
        return;
    };
    let token_id = env_or_default("POLYBOT2_LIVE_EXEC_TOKEN_ID", "");
    let mut rt = DispatchRuntime::new(cfg, None);
    let request = OrderRequestData {
        token_id,
        side: "buy_yes".to_string(),
        amount_usdc: 1.0,
        limit_price: 0.5,
        time_in_force: "GTC".to_string(),
        client_order_id: format!("rust_live_gtc_{}", now_unix_s()),
        size_shares: 2.0,
        expiration_ts: None,
    };

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let err = tokio_rt
        .block_on(rt.submit_with_policy_async(&request))
        .expect_err("GTC with size < 5 shares should be rejected by exchange");
    assert!(
        contains_min_size_rejection(err.as_str()),
        "unexpected GTC live rejection: {}",
        err
    );
}
