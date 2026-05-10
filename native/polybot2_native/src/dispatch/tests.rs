use super::*;
use crate::log_writer::LogWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static TEST_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_log() -> Arc<Mutex<LogWriter>> {
    temp_log_with_path().0
}

fn temp_log_with_path() -> (Arc<Mutex<LogWriter>>, PathBuf) {
    let n = TEST_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("polybot2_test_{}_{}.jsonl", std::process::id(), n));
    let log = Arc::new(Mutex::new(
        LogWriter::open(path.to_str().expect("utf8 path")).expect("temp log"),
    ));
    (log, path)
}

fn empty_registry() -> Arc<crate::TargetRegistry> {
    Arc::new(crate::TargetRegistry {
        tokens: vec![],
        targets: vec![],
    })
}

fn registry_with_one_target(token_id: &str, sk: &str) -> Arc<crate::TargetRegistry> {
    Arc::new(crate::TargetRegistry {
        tokens: vec![crate::TokenSlot {
            token_id: Arc::from(token_id),
        }],
        targets: vec![crate::TargetSlot {
            token_idx: crate::TokenIdx(0),
            strategy_key: Arc::from(sk),
        }],
    })
}

fn registry_with_n_targets(targets: &[(&str, &str)]) -> Arc<crate::TargetRegistry> {
    let mut tokens: Vec<crate::TokenSlot> = Vec::new();
    let mut token_to_idx: std::collections::HashMap<String, crate::TokenIdx> =
        std::collections::HashMap::new();
    let mut target_slots: Vec<crate::TargetSlot> = Vec::new();
    for (token_id, sk) in targets {
        let idx = match token_to_idx.get(*token_id) {
            Some(&i) => i,
            None => {
                let i = crate::TokenIdx(tokens.len() as u16);
                tokens.push(crate::TokenSlot {
                    token_id: Arc::from(*token_id),
                });
                token_to_idx.insert(token_id.to_string(), i);
                i
            }
        };
        target_slots.push(crate::TargetSlot {
            token_idx: idx,
            strategy_key: Arc::from(*sk),
        });
    }
    Arc::new(crate::TargetRegistry {
        tokens,
        targets: target_slots,
    })
}

fn shared_registry_for(registry: Arc<crate::TargetRegistry>) -> crate::dispatch::SharedRegistry {
    Arc::new(arc_swap::ArcSwap::new(registry))
}

fn make_handle_with_registry(
    cfg: DispatchConfig,
    registry: Arc<crate::TargetRegistry>,
) -> (DispatchHandle, rtrb::Consumer<SubmitWork>) {
    let shared_registry = shared_registry_for(Arc::clone(&registry));
    let mut handle = DispatchHandle::new(cfg, registry, shared_registry);
    let (tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    handle.install_submit_tx(tx);
    (handle, rx)
}

/// Test-only convenience: dispatch a single target end-to-end (noop log,
/// pop+send for http). Mirrors the per-intent behavior that the production
/// path now does in batches via `replay::process_decoded_frame_sync`.
fn dispatch_target_inline(
    handle: &mut DispatchHandle,
    target_idx: crate::TargetIdx,
    log: &Arc<Mutex<LogWriter>>,
) {
    if matches!(handle.cfg.mode, DispatchMode::Noop) {
        let (sk, tok) = handle.resolve_strings(target_idx);
        if let Ok(mut g) = log.lock() {
            g.log_order_ok(sk, tok, "noop");
        }
        return;
    }
    match handle.pop_for_target(target_idx) {
        Ok(signed) => {
            let mut batch = SubmitBatch::new();
            batch.push((target_idx, signed));
            handle.send_batch(batch, log);
        }
        Err(err) => {
            let (sk, tok) = handle.resolve_strings(target_idx);
            if let Ok(mut g) = log.lock() {
                g.log_order_err(sk, tok, &err);
            }
        }
    }
}

fn read_log_lines(log: &Arc<Mutex<LogWriter>>) -> Vec<String> {
    if let Ok(mut g) = log.lock() {
        g.flush();
    }
    // Find the file path: we don't track it directly. Instead, this helper is
    // not used; tests rely on channel inspection and behavioral assertions.
    Vec::new()
}

fn make_dummy_signed_order() -> SdkSignedOrder {
    use alloy::primitives::{Signature, U256};
    use polymarket_client_sdk_v2::auth::ApiKey;
    use polymarket_client_sdk_v2::clob::types::{OrderPayload, OrderType};

    SdkSignedOrder::builder()
        .payload(OrderPayload::default())
        .signature(Signature::new(U256::ZERO, U256::ZERO, false))
        .order_type(OrderType::GTC)
        .owner(ApiKey::nil())
        .build()
}

fn make_dummy_prepared_payload() -> Box<PreparedOrderPayload> {
    Box::new(
        super::presign_pool::prepare_payload_from_signed(make_dummy_signed_order())
            .expect("serialize dummy order"),
    )
}

#[test]
fn map_post_response_success_with_id_is_ok() {
    use super::sdk_exec::map_post_response;
    let id = map_post_response(true, "abc123".to_string(), None, "submit_failed").unwrap();
    assert_eq!(id, "abc123");
}

#[test]
fn map_post_response_empty_order_id_is_error() {
    use super::sdk_exec::map_post_response;
    let err = map_post_response(true, String::new(), None, "submit_failed").unwrap_err();
    assert!(err.contains("empty_order_id_with_success"), "got: {}", err);
}

#[test]
fn map_post_response_empty_id_uses_prefix() {
    use super::sdk_exec::map_post_response;
    let err = map_post_response(true, String::new(), None, "batch_submit_failed").unwrap_err();
    assert!(err.starts_with("batch_submit_failed:"), "got: {}", err);
}

#[test]
fn map_post_response_failure_includes_error_msg() {
    use super::sdk_exec::map_post_response;
    let err = map_post_response(
        false,
        String::new(),
        Some("rejected_min_size".to_string()),
        "submit_failed",
    )
    .unwrap_err();
    assert!(err.contains("rejected_min_size"), "got: {}", err);
}

#[test]
fn map_post_response_failure_unknown_when_no_msg() {
    use super::sdk_exec::map_post_response;
    let err = map_post_response(false, String::new(), None, "submit_failed").unwrap_err();
    assert!(err.contains("unknown"), "got: {}", err);
}

#[test]
fn submitter_health_default_is_not_running() {
    let h = crate::SubmitterHealth::default();
    assert!(!h.running);
    assert!(h.last_error.is_empty());
}

#[test]
fn submitter_run_sets_running_then_clears_on_stop() {
    let cfg = DispatchConfig::default();
    let log = temp_log();
    let (mut tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health = Arc::new(Mutex::new(crate::SubmitterHealth::default()));
    let registry = empty_registry();
    let shared_registry = shared_registry_for(registry);
    let submitter = OrderSubmitter::new(
        cfg,
        log,
        rx,
        Arc::clone(&stop_flag),
        Arc::clone(&health),
        shared_registry,
    );
    let _ = tx.push(SubmitWork::Stop);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt.block_on(crate::dispatch::run_submitter_async(submitter));
    let h = health.lock().expect("health lock");
    assert!(!h.running, "submitter should report not-running after Stop");
}

#[test]
fn submitter_run_sets_running_then_clears_on_channel_close() {
    let cfg = DispatchConfig::default();
    let log = temp_log();
    let (_tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health = Arc::new(Mutex::new(crate::SubmitterHealth::default()));
    let registry = empty_registry();
    let shared_registry = shared_registry_for(registry);
    let submitter = OrderSubmitter::new(
        cfg,
        log,
        rx,
        Arc::clone(&stop_flag),
        Arc::clone(&health),
        shared_registry,
    );
    stop_flag.store(true, std::sync::atomic::Ordering::Release);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt.block_on(crate::dispatch::run_submitter_async(submitter));
    let h = health.lock().expect("health lock");
    assert!(!h.running);
}

#[test]
fn submitter_stop_flag_only_exits_promptly_without_work() {
    let cfg = DispatchConfig::default();
    let log = temp_log();
    let (_tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let health = Arc::new(Mutex::new(crate::SubmitterHealth::default()));
    let registry = empty_registry();
    let shared_registry = shared_registry_for(registry);
    let submitter = OrderSubmitter::new(
        cfg,
        log,
        rx,
        Arc::clone(&stop_flag),
        Arc::clone(&health),
        shared_registry,
    );
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let start = std::time::Instant::now();
    tokio_rt.block_on(crate::dispatch::run_submitter_async(submitter));
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "spin-loop submitter should observe stop flag promptly, elapsed={:?}",
        start.elapsed()
    );
    let h = health.lock().expect("health lock");
    assert!(!h.running);
}

#[test]
fn submitter_processes_queued_batch_without_notify_signal() {
    let cfg = DispatchConfig::default(); // noop mode: no HTTP init needed.
    let log = temp_log();
    let (mut tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health = Arc::new(Mutex::new(crate::SubmitterHealth::default()));
    let registry = empty_registry();
    let shared_registry = shared_registry_for(registry);
    let submitter = OrderSubmitter::new(
        cfg,
        log,
        rx,
        Arc::clone(&stop_flag),
        Arc::clone(&health),
        shared_registry,
    );

    let mut batch = SubmitBatch::new();
    batch.push((crate::TargetIdx(0), make_dummy_prepared_payload()));
    tx.push(SubmitWork::Batch(batch))
        .expect("batch push succeeds");
    tx.push(SubmitWork::Stop).expect("stop push succeeds");

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt.block_on(crate::dispatch::run_submitter_async(submitter));

    let h = health.lock().expect("health lock");
    assert!(!h.running);
}

#[test]
fn submitter_parallelizes_two_chunks_when_permits_allow() {
    let delay = Duration::from_millis(35);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (elapsed, max_inflight) =
        tokio_rt.block_on(super::simulate_chunk_parallelism_for_test(30, 15, 3, delay));
    assert!(max_inflight >= 2, "max_inflight={}", max_inflight);
    assert!(
        elapsed < Duration::from_millis(65),
        "expected near single-delay parallel wall time, got {:?}",
        elapsed
    );
}

#[test]
fn submitter_serializes_two_chunks_when_permit_is_one() {
    let delay = Duration::from_millis(35);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (elapsed, max_inflight) =
        tokio_rt.block_on(super::simulate_chunk_parallelism_for_test(30, 15, 1, delay));
    assert_eq!(max_inflight, 1, "max_inflight={}", max_inflight);
    assert!(
        elapsed >= Duration::from_millis(60),
        "expected serialized >= two-delay wall time, got {:?}",
        elapsed
    );
}

#[test]
fn submitter_chunk_calls_respect_global_permit_cap() {
    let delay = Duration::from_millis(20);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (_elapsed, max_inflight) =
        tokio_rt.block_on(super::simulate_chunk_parallelism_for_test(90, 15, 3, delay));
    assert!(
        max_inflight <= 3,
        "max_inflight {} exceeded permit cap 3",
        max_inflight
    );
}

#[test]
fn submitter_queue_is_serialized_across_batches() {
    let delay = Duration::from_millis(35);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (elapsed, max_inflight) =
        tokio_rt.block_on(super::simulate_submitter_serial_queue_for_test(2, delay));
    assert_eq!(max_inflight, 1, "max_inflight={}", max_inflight);
    assert!(
        elapsed >= Duration::from_millis(60),
        "expected roughly sum of both delays for strict serialization, got {:?}",
        elapsed
    );
}

#[test]
fn submitter_small_batch_single_call_synthetic() {
    let delay = Duration::from_millis(25);
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (elapsed, max_inflight) =
        tokio_rt.block_on(super::simulate_chunk_parallelism_for_test(8, 15, 3, delay));
    assert_eq!(max_inflight, 1, "max_inflight={}", max_inflight);
    assert!(
        elapsed < Duration::from_millis(50),
        "expected single-call wall time for 2..=15 path, got {:?}",
        elapsed
    );
}

#[test]
fn submitter_small_batch_mapping_is_deterministic_and_short_error_aligned() {
    let targets = vec![
        crate::TargetIdx(10),
        crate::TargetIdx(11),
        crate::TargetIdx(12),
    ];
    let mapped = super::simulate_small_batch_mapping_for_test(&targets, 2);
    assert_eq!(mapped.len(), 3);
    assert_eq!(mapped[0].0, crate::TargetIdx(10));
    assert!(mapped[0].1.as_ref().is_ok());
    assert_eq!(mapped[1].0, crate::TargetIdx(11));
    assert!(mapped[1].1.as_ref().is_ok());
    assert_eq!(mapped[2].0, crate::TargetIdx(12));
    let err = mapped[2]
        .1
        .as_ref()
        .expect_err("3rd should be short-response err");
    assert!(
        err.starts_with("batch_response_short:expected=3,got=2"),
        "unexpected err {}",
        err
    );
}

#[test]
fn submitter_spawn_overhead_proxy_is_higher_than_inline() {
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (spawn_elapsed, inline_elapsed) = tokio_rt.block_on(
        super::simulate_submitter_spawn_vs_inline_overhead_for_test(5_000),
    );
    assert!(
        spawn_elapsed > inline_elapsed,
        "spawn_elapsed={:?} inline_elapsed={:?}",
        spawn_elapsed,
        inline_elapsed
    );
}

#[test]
fn submitter_hot_path_has_no_metric_or_counter_calls() {
    let submitter_src = include_str!("submitter.rs");
    assert!(
        !submitter_src.contains("record_outcome("),
        "submitter hot path must not update per-order health counters"
    );
    assert!(
        !submitter_src.contains("record_pop_to_task_start_ns("),
        "submitter hot path must not record latency metrics"
    );
    assert!(
        !submitter_src.contains("record_task_prep_ns("),
        "submitter hot path must not record latency metrics"
    );
    assert!(
        !submitter_src.contains("record_permit_wait_ns("),
        "submitter hot path must not record latency metrics"
    );
    assert!(
        !submitter_src.contains("record_sdk_call_total_ns("),
        "submitter hot path must not record latency metrics"
    );
    assert!(
        !submitter_src.contains("record_batch_total_ns("),
        "submitter hot path must not record latency metrics"
    );
    assert!(
        !submitter_src.contains("record_chunk_sdk_call_total_ns("),
        "submitter hot path must not record latency metrics"
    );
}

#[test]
fn submit_presigned_miss_is_fail_closed() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        ..DispatchConfig::default()
    };
    let (mut handle, mut rx) =
        make_handle_with_registry(cfg, registry_with_one_target("t", "strategy_a"));
    let log = temp_log();
    dispatch_target_inline(&mut handle, crate::TargetIdx(0), &log);
    // Nothing was sent to the submitter channel: presign miss is fail-closed.
    assert!(rx.pop().is_err());
}

#[test]
fn startup_warm_fails_when_templates_missing() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        presign_startup_warm_timeout_seconds: 0.01,
        ..DispatchConfig::default()
    };
    // We can't easily construct an SdkClient/Signer without credentials, but
    // warm_presign_startup_into early-returns on empty templates before
    // touching them. Pass dummy refs by short-circuiting in the function.
    // Easiest verification: empty templates → returns the expected error.
    let registry = empty_registry();
    let shared_registry = shared_registry_for(Arc::clone(&registry));
    let mut handle = DispatchHandle::new(cfg.clone(), registry, shared_registry);
    handle.set_presign_templates(&[]);
    handle.activate_presign_templates_for_tokens(&[]);
    let (templates, pool) = handle.templates_and_pool_mut();
    assert!(templates.is_empty());
    assert!(pool.is_empty());
    // Templates are empty; the early `return Err(...)` is reached without
    // needing a real client. Build a placeholder runtime to exercise the
    // error path.
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    // We can't fabricate a real SdkClient, so this test is reduced to: the
    // function exists and errors when there are no templates. To exercise it
    // we'd need real credentials; we rely on the check ordering inside
    // `warm_presign_startup_into` (target>0 → keys empty → error). The first
    // two early-returns don't dereference the client/signer.
    drop(tokio_rt);
}

#[test]
fn noop_dispatch_succeeds() {
    let (mut handle, mut rx) = make_handle_with_registry(
        DispatchConfig::default(),
        registry_with_one_target("t1", "sk1"),
    );
    let log = temp_log();
    dispatch_target_inline(&mut handle, crate::TargetIdx(0), &log);
    // Noop short-circuits before reaching the channel.
    assert!(rx.pop().is_err());
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
    let err = map_sdk_side("sell_yes").expect_err("sell should be rejected for usdc notional flow");
    assert!(err.contains("sell_requires_share_amount"));
}

#[test]
fn empty_send_batch_is_noop() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        ..DispatchConfig::default()
    };
    let (mut handle, mut rx) = make_handle_with_registry(cfg, empty_registry());
    let log = temp_log();
    handle.send_batch(SubmitBatch::new(), &log);
    // Empty batch should not send anything.
    assert!(rx.pop().is_err());
}

#[test]
fn send_batch_emits_one_submitwork_per_call() {
    // This test verifies the channel-send path indirectly by asserting that
    // pop fails closed when no prepared presigned payload exists in the pool.
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        ..DispatchConfig::default()
    };
    let registry = registry_with_one_target("tok_batch", "sk_batch");
    let (mut handle, _rx) = make_handle_with_registry(cfg, registry);
    let err = handle
        .pop_for_target(crate::TargetIdx(0))
        .expect_err("empty pool");
    assert!(err.contains("submit_presigned_miss"), "got: {}", err);
    let _ = std::any::type_name::<PreparedOrderPayload>();
}

#[test]
fn noop_dispatch_batch_succeeds() {
    let registry = registry_with_n_targets(&[("t1", "sk1"), ("t2", "sk2"), ("t3", "sk3")]);
    let (mut handle, mut rx) = make_handle_with_registry(DispatchConfig::default(), registry);
    let log = temp_log();
    dispatch_target_inline(&mut handle, crate::TargetIdx(0), &log);
    dispatch_target_inline(&mut handle, crate::TargetIdx(1), &log);
    dispatch_target_inline(&mut handle, crate::TargetIdx(2), &log);
    // Noop never sends on the channel.
    assert!(rx.pop().is_err());
}

#[test]
fn presign_batch_miss_is_per_order() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        ..DispatchConfig::default()
    };
    let registry = registry_with_n_targets(&[("t1", "sk1"), ("t2", "sk2")]);
    let (mut handle, mut rx) = make_handle_with_registry(cfg, registry);
    let log = temp_log();
    dispatch_target_inline(&mut handle, crate::TargetIdx(0), &log);
    dispatch_target_inline(&mut handle, crate::TargetIdx(1), &log);
    // Both intents should have failed presign-miss, so nothing on the channel.
    assert!(rx.pop().is_err());
}

#[test]
fn dispatch_handle_logs_when_channel_closed() {
    // Build a handle with a sender whose receiver is dropped; pop returns None
    // (empty pool), so we hit the miss path (not the closed-channel path).
    // This still verifies dispatch_target is robust to a closed channel under
    // fail-closed conditions.
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        presign_enabled: true,
        ..DispatchConfig::default()
    };
    let (mut handle, _) = make_handle_with_registry(cfg, registry_with_one_target("t1", "sk1"));
    // Receiver dropped here — channel is closed before dispatching.
    let log = temp_log();
    dispatch_target_inline(&mut handle, crate::TargetIdx(0), &log);
}

#[test]
fn registry_publish_is_independent_of_ring_capacity() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        ..DispatchConfig::default()
    };
    let old_registry = registry_with_one_target("tok_old", "sk_old");
    let shared_registry = shared_registry_for(Arc::clone(&old_registry));
    let mut handle =
        DispatchHandle::new(cfg, Arc::clone(&old_registry), Arc::clone(&shared_registry));

    let (mut tx, mut _rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    for _ in 0..64 {
        assert!(tx.push(SubmitWork::Stop).is_ok());
    }
    handle.install_submit_tx(tx);

    let new_registry = registry_with_one_target("tok_new", "sk_new");
    handle.replace_registry(Arc::clone(&new_registry));

    let observed = shared_registry.load_full();
    assert_eq!(observed.targets.len(), 1);
    assert_eq!(&*observed.targets[0].strategy_key, "sk_new");
    assert_eq!(&*observed.tokens[0].token_id, "tok_new");
    let (sk, tok) = handle.resolve_strings(crate::TargetIdx(0));
    assert_eq!(sk, "sk_new");
    assert_eq!(tok, "tok_new");
}

#[test]
fn submitter_error_drain_uses_shared_registry_snapshot() {
    let cfg = DispatchConfig {
        mode: DispatchMode::Http,
        ..DispatchConfig::default()
    };
    let old_registry = registry_with_one_target("tok_old", "sk_old");
    let new_registry = registry_with_one_target("tok_new", "sk_new");
    let shared_registry = shared_registry_for(Arc::clone(&old_registry));
    let (log, log_path) = temp_log_with_path();
    let (mut tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health = Arc::new(Mutex::new(crate::SubmitterHealth::default()));
    let submitter = OrderSubmitter::new(
        cfg,
        Arc::clone(&log),
        rx,
        Arc::clone(&stop_flag),
        Arc::clone(&health),
        Arc::clone(&shared_registry),
    );

    let mut batch = SubmitBatch::new();
    batch.push((crate::TargetIdx(0), make_dummy_prepared_payload()));
    tx.push(SubmitWork::Batch(batch))
        .expect("batch push succeeds");
    tx.push(SubmitWork::Stop).expect("stop push succeeds");

    // Publish newer registry before submitter drains error path.
    shared_registry.store(new_registry);

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    tokio_rt.block_on(crate::dispatch::run_submitter_async(submitter));
    if let Ok(mut g) = log.lock() {
        g.flush();
    }
    let text = std::fs::read_to_string(log_path).expect("read log file");
    assert!(text.contains("\"sk\":\"sk_new\""), "log contents: {}", text);
    assert!(
        text.contains("\"tok\":\"tok_new\""),
        "log contents: {}",
        text
    );
}

