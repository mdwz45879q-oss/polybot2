use super::sdk_exec::map_post_response;
use super::*;
use crate::log_writer::LogWriter;
use futures_util::future::join_all;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_CLOB_BATCH: usize = 15;
/// Interval between keepalive pings to the CLOB during quiet periods.
/// Keeps the TCP + TLS + HTTP/2 connection warm so no order ever pays
/// the ~9ms cold-start handshake. Must be well under Cloudflare's
/// ~300s idle timeout.
const CLOB_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(120);

struct ChunkScratch {
    body_buf: Vec<u8>,
    idxs_buf: Vec<crate::TargetIdx>,
    tifs_buf: Vec<OrderTimeInForce>,
}

fn with_submitter_health<F>(health: &Arc<Mutex<crate::SubmitterHealth>>, mut f: F)
where
    F: FnMut(&mut crate::SubmitterHealth),
{
    if let Ok(mut h) = health.lock() {
        f(&mut h);
    }
}

fn set_running(health: &Arc<Mutex<crate::SubmitterHealth>>, running: bool) {
    with_submitter_health(health, |h| h.running = running);
}

fn set_init_error(health: &Arc<Mutex<crate::SubmitterHealth>>, err: &str) {
    with_submitter_health(health, |h| {
        h.running = false;
        h.last_error = err.to_string();
    });
}

/// Submitter loop: receives frame batches from the WS thread and processes
/// them inline (strict serialized queue). Three dispatch tiers:
/// - len==1: single POST /order (bare await)
/// - 2..=15: single POST /orders (batch, 1 HMAC, 1 HTTP request)
/// - >15: concurrent POST /orders chunks via join_all
pub(crate) async fn run_submitter_async(mut sub: OrderSubmitter) {
    set_running(&sub.health, true);

    let mut fast_client: Option<Arc<FastClobSubmitClient>> = None;
    if matches!(sub.cfg.mode, DispatchMode::Http) {
        if let Err(err) = sub.ensure_sdk_runtime_async().await {
            set_init_error(&sub.health, &err);
            if let Ok(mut g) = sub.log.lock() {
                g.log_order_err("_init_", "_", &format!("submitter_init_failed:{}", err), "");
            }
            drain_channel_with_error(&mut sub, &err).await;
            return;
        }
        let signer_address = match sub.signer_ref() {
            Ok(signer) => signer.address().to_checksum(None),
            Err(err) => {
                set_init_error(&sub.health, &err);
                if let Ok(mut g) = sub.log.lock() {
                    g.log_order_err("_init_", "_", &format!("submitter_init_failed:{}", err), "");
                }
                drain_channel_with_error(&mut sub, &err).await;
                return;
            }
        };
        match FastClobSubmitClient::new(&sub.cfg, signer_address) {
            Ok(client) => {
                let arc_client = Arc::new(client);
                // Pre-establish TCP + TLS + HTTP/2 connection so the first
                // real order doesn't pay the cold-start handshake.
                arc_client.warmup_connection().await;
                fast_client = Some(arc_client);
            }
            Err(err) => {
                set_init_error(&sub.health, &err);
                if let Ok(mut g) = sub.log.lock() {
                    g.log_order_err("_init_", "_", &format!("submitter_init_failed:{}", err), "");
                }
                drain_channel_with_error(&mut sub, &err).await;
                return;
            }
        }
    }

    let submit_client = fast_client;
    let log = sub.log;
    let health = sub.health.clone();
    let mut submit_rx = sub.submit_rx;
    let shared_registry = sub.shared_registry;
    let stop_flag = sub.stop_flag;
    let mut scratch = ChunkScratch { body_buf: Vec::new(), idxs_buf: Vec::new(), tifs_buf: Vec::new() };
    let mut last_clob_activity = Instant::now();

    'outer: loop {
        let mut had_work = false;
        while let Ok(work) = submit_rx.pop() {
            had_work = true;
            last_clob_activity = Instant::now();
            match work {
                SubmitWork::Stop => break 'outer,
                SubmitWork::Batch(batch) => {
                    let reg_guard = shared_registry.load();
                    submit_batch_task(batch, &submit_client, &*reg_guard, &log, &mut scratch).await;
                }
            }
        }
        if stop_flag.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
        if !had_work {
            // Keep CLOB connection warm during quiet periods so no order
            // ever pays the cold-start TLS handshake.
            if last_clob_activity.elapsed() >= CLOB_KEEPALIVE_INTERVAL {
                if let Some(ref client) = submit_client {
                    client.warmup_connection().await;
                }
                last_clob_activity = Instant::now();
            }
            std::hint::spin_loop();
        }
    }
    set_running(&health, false);
}

async fn drain_channel_with_error(sub: &mut OrderSubmitter, err: &str) {
    loop {
        let mut had_work = false;
        while let Ok(work) = sub.submit_rx.pop() {
            had_work = true;
            match work {
                SubmitWork::Stop => return,
                SubmitWork::Batch(b) => {
                    let reg_guard = sub.shared_registry.load();
                    for (target_idx, _) in b {
                        log_outcome_idx(&sub.log, &*reg_guard, target_idx, &Err(err.to_string()), OrderTimeInForce::FAK);
                    }
                }
            }
        }
        if sub.stop_flag.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        if !had_work {
            std::hint::spin_loop();
        }
    }
}

async fn submit_batch_task(
    batch: SubmitBatch,
    client: &Option<Arc<FastClobSubmitClient>>,
    registry: &crate::TargetRegistry,
    log: &Arc<Mutex<LogWriter>>,
    scratch: &mut ChunkScratch,
) {
    let batch_len = batch.len();
    if batch_len == 0 {
        return;
    }
    let Some(client_ref) = client.as_ref() else {
        return;
    };

    // Tier 1: single order — bare await, no overhead.
    if batch_len == 1 {
        let (target_idx, prepared) = batch.into_iter().next().unwrap();
        let tif = prepared.time_in_force;
        let outcome = match client_ref.post_order_bytes_single(prepared.order_json).await {
            Ok(resp) => {
                map_post_response(resp.success, resp.order_id, resp.error_msg, "submit_failed")
            }
            Err(e) => Err(format!("submit_failed:{}", e)),
        };
        log_outcome_idx(log, registry, target_idx, &outcome, tif);
        return;
    }

    // Tier 2: 2..=15 — one POST /orders call (1 HMAC, 1 HTTP request).
    // All orders hit the wire in a single TCP segment.
    if batch_len <= MAX_CLOB_BATCH {
        let meta: smallvec::SmallVec<[(crate::TargetIdx, OrderTimeInForce); 32]> =
            batch.iter().map(|(idx, p)| (*idx, p.time_in_force)).collect();
        scratch.body_buf.clear();
        scratch.body_buf.push(b'[');
        for (i, (_, prepared)) in batch.into_iter().enumerate() {
            if i > 0 {
                scratch.body_buf.push(b',');
            }
            scratch.body_buf.extend_from_slice(&prepared.order_json);
        }
        scratch.body_buf.push(b']');
        let chunk_body = std::mem::take(&mut scratch.body_buf);

        let outcomes: Vec<(crate::TargetIdx, OrderTimeInForce, Result<String, String>)> =
            match client_ref.post_orders_bytes(chunk_body).await {
                Ok(responses) => {
                    let resp_len = responses.len();
                    let mut out = Vec::with_capacity(meta.len());
                    for ((tidx, tif), resp) in meta.iter().zip(&responses) {
                        out.push((
                            *tidx,
                            *tif,
                            map_post_response(
                                resp.success,
                                resp.order_id.clone(),
                                resp.error_msg.clone(),
                                "batch_submit_failed",
                            ),
                        ));
                    }
                    if resp_len < meta.len() {
                        let err = format!(
                            "batch_response_short:expected={},got={}",
                            meta.len(),
                            resp_len
                        );
                        for (tidx, tif) in &meta[resp_len..] {
                            out.push((*tidx, *tif, Err(err.clone())));
                        }
                    }
                    out
                }
                Err(e) => {
                    let err = format!("batch_submit_failed:{}", e);
                    meta.iter()
                        .map(|(tidx, tif)| (*tidx, *tif, Err(err.clone())))
                        .collect()
                }
            };
        for (target_idx, tif, outcome) in outcomes {
            log_outcome_idx(log, registry, target_idx, &outcome, tif);
        }
        return;
    }

    // Tier 3: >15 — chunk into groups of MAX_CLOB_BATCH, fire concurrently.
    let num_chunks = (batch_len + MAX_CLOB_BATCH - 1) / MAX_CLOB_BATCH;
    let mut chunk_jobs: Vec<(Vec<(crate::TargetIdx, OrderTimeInForce)>, Vec<u8>)> = Vec::with_capacity(num_chunks);
    let mut items = batch.into_iter();
    let mut offset = 0usize;
    while offset < batch_len {
        let chunk_len = (batch_len - offset).min(MAX_CLOB_BATCH);
        scratch.idxs_buf.clear();
        scratch.tifs_buf.clear();
        scratch.body_buf.clear();
        scratch.body_buf.push(b'[');
        let mut chunk_meta: Vec<(crate::TargetIdx, OrderTimeInForce)> = Vec::with_capacity(chunk_len);
        for i in 0..chunk_len {
            let (tidx, prepared) = items.next().unwrap();
            chunk_meta.push((tidx, prepared.time_in_force));
            if i > 0 {
                scratch.body_buf.push(b',');
            }
            scratch.body_buf.extend_from_slice(&prepared.order_json);
        }
        scratch.body_buf.push(b']');
        chunk_jobs.push((chunk_meta, scratch.body_buf.clone()));
        offset += chunk_len;
    }

    let futures = chunk_jobs.into_iter().map(|(chunk_meta, chunk_body)| {
        let client = Arc::clone(client_ref);
        async move {
            match client.post_orders_bytes(chunk_body).await {
                Ok(responses) => {
                    let resp_len = responses.len();
                    let mut out = Vec::with_capacity(chunk_meta.len());
                    for ((tidx, tif), resp) in chunk_meta.iter().zip(&responses) {
                        out.push((
                            *tidx,
                            *tif,
                            map_post_response(
                                resp.success,
                                resp.order_id.clone(),
                                resp.error_msg.clone(),
                                "batch_submit_failed",
                            ),
                        ));
                    }
                    if resp_len < chunk_meta.len() {
                        let err = format!(
                            "batch_response_short:expected={},got={}",
                            chunk_meta.len(),
                            resp_len
                        );
                        for (tidx, tif) in &chunk_meta[resp_len..] {
                            out.push((*tidx, *tif, Err(err.clone())));
                        }
                    }
                    out
                }
                Err(e) => {
                    let err = format!("batch_submit_failed:{}", e);
                    chunk_meta
                        .iter()
                        .map(|(tidx, tif)| (*tidx, *tif, Err(err.clone())))
                        .collect()
                }
            }
        }
    });

    let chunk_results = join_all(futures).await;
    for outcomes in chunk_results {
        for (target_idx, tif, outcome) in outcomes {
            log_outcome_idx(log, registry, target_idx, &outcome, tif);
        }
    }
}

#[cfg(any(test, feature = "bench-support"))]
pub(crate) async fn simulate_chunk_parallelism_for_test(
    total_orders: usize,
    max_batch: usize,
    permit_count: usize,
    delay: std::time::Duration,
) -> (std::time::Duration, usize) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let semaphore = Arc::new(tokio::sync::Semaphore::new(permit_count.max(1)));
    let inflight = Arc::new(AtomicUsize::new(0));
    let max_inflight = Arc::new(AtomicUsize::new(0));
    let mut remaining = total_orders;
    let mut chunk_sizes: Vec<usize> = Vec::new();
    while remaining > 0 {
        let size = remaining.min(max_batch.max(1));
        chunk_sizes.push(size);
        remaining -= size;
    }

    let start = Instant::now();
    let futures = chunk_sizes.into_iter().map(|_chunk_size| {
        let sem = Arc::clone(&semaphore);
        let in_flight = Arc::clone(&inflight);
        let max_seen = Arc::clone(&max_inflight);
        async move {
            let permit = sem.acquire_owned().await.expect("permit");
            let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let mut prev = max_seen.load(Ordering::SeqCst);
            while current > prev {
                match max_seen.compare_exchange(prev, current, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => break,
                    Err(v) => prev = v,
                }
            }
            tokio::time::sleep(delay).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            drop(permit);
        }
    });
    join_all(futures).await;
    (
        start.elapsed(),
        max_inflight.load(std::sync::atomic::Ordering::SeqCst),
    )
}

#[cfg(any(test, feature = "bench-support"))]
pub(crate) async fn simulate_submitter_serial_queue_for_test(
    batch_count: usize,
    delay: std::time::Duration,
) -> (std::time::Duration, usize) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let inflight = Arc::new(AtomicUsize::new(0));
    let max_inflight = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    for _ in 0..batch_count {
        let current = inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let mut prev = max_inflight.load(Ordering::SeqCst);
        while current > prev {
            match max_inflight.compare_exchange(prev, current, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(v) => prev = v,
            }
        }
        tokio::time::sleep(delay).await;
        inflight.fetch_sub(1, Ordering::SeqCst);
    }
    (start.elapsed(), max_inflight.load(Ordering::SeqCst))
}

#[cfg(any(test, feature = "bench-support"))]
pub(crate) async fn simulate_submitter_spawn_vs_inline_overhead_for_test(
    iterations: usize,
) -> (std::time::Duration, std::time::Duration) {
    let inline_start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(());
    }
    let inline_elapsed = inline_start.elapsed();

    let spawn_start = Instant::now();
    for _ in 0..iterations {
        let handle = tokio::spawn(async {
            std::hint::black_box(());
        });
        let _ = handle.await;
    }
    let spawn_elapsed = spawn_start.elapsed();
    (spawn_elapsed, inline_elapsed)
}

#[cfg(any(test, feature = "bench-support"))]
pub(crate) fn simulate_small_batch_mapping_for_test(
    target_idxs: &[crate::TargetIdx],
    response_count: usize,
) -> Vec<(crate::TargetIdx, Result<String, String>)> {
    let mut outcomes = Vec::with_capacity(target_idxs.len());
    let mapped = response_count.min(target_idxs.len());
    for (i, target_idx) in target_idxs.iter().take(mapped).enumerate() {
        outcomes.push((*target_idx, Ok(format!("ok_{}", i))));
    }
    if mapped < target_idxs.len() {
        let err = format!(
            "batch_response_short:expected={},got={}",
            target_idxs.len(),
            mapped
        );
        for target_idx in &target_idxs[mapped..] {
            outcomes.push((*target_idx, Err(err.clone())));
        }
    }
    outcomes
}

fn tif_str(tif: OrderTimeInForce) -> &'static str {
    match tif {
        OrderTimeInForce::FAK => "FAK",
        OrderTimeInForce::FOK => "FOK",
        OrderTimeInForce::GTC => "GTC",
    }
}

fn log_outcome_idx(
    log: &Arc<Mutex<LogWriter>>,
    registry: &crate::TargetRegistry,
    target_idx: crate::TargetIdx,
    outcome: &Result<String, String>,
    tif: OrderTimeInForce,
) {
    let (sk, tok): (&str, &str) = match registry.targets.get(target_idx.0 as usize) {
        Some(target) => match registry.tokens.get(target.token_idx.0 as usize) {
            Some(token) => (&target.strategy_key, &token.token_id),
            None => (&target.strategy_key, "_"),
        },
        None => ("_", "_"),
    };
    let tif_s = tif_str(tif);
    if let Ok(mut g) = log.lock() {
        match outcome {
            Ok(eid) => g.log_order_ok(sk, tok, eid, tif_s),
            Err(err) => g.log_order_err(sk, tok, err, tif_s),
        }
    }
}
