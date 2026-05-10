use super::*;
use super::sdk_exec::map_post_response;
use crate::log_writer::LogWriter;
use futures_util::future::join_all;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_CONCURRENT_SUBMITS: usize = 3;
const MAX_CLOB_BATCH: usize = 15;

fn duration_ns_u64(d: std::time::Duration) -> u64 {
    d.as_nanos().min(u64::MAX as u128) as u64
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

fn record_outcome(
    health: &Arc<Mutex<crate::SubmitterHealth>>,
    outcome: &Result<String, String>,
) {
    with_submitter_health(health, |h| {
        match outcome {
            Ok(_) => h.posted_ok += 1,
            Err(e) => {
                h.posted_err += 1;
                h.last_error = e.clone();
            }
        }
    });
}

/// Submitter loop: receives frame batches from the WS thread and processes
/// them inline (strict serialized queue). Global HTTP backpressure is still
/// enforced with a semaphore, and oversized batches still parallelize chunk
/// calls inside a single batch task.
pub(crate) async fn run_submitter_async(mut sub: OrderSubmitter) {
    set_running(&sub.health, true);

    if matches!(sub.cfg.mode, DispatchMode::Http) {
        if let Err(err) = sub.ensure_sdk_runtime_async().await {
            set_init_error(&sub.health, &err);
            if let Ok(mut g) = sub.log.lock() {
                g.log_order_err("_init_", "_", &format!("submitter_init_failed:{}", err));
            }
            drain_channel_with_error(&mut sub, &err).await;
            return;
        }
    }

    let sdk_client = sub.sdk_runtime.take().map(|r| r.client);
    let log = sub.log;
    let health = sub.health.clone();
    let mut submit_rx = sub.submit_rx;
    let shared_registry = sub.shared_registry;
    let submit_notify = sub.submit_notify;
    let stop_flag = sub.stop_flag;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_SUBMITS));

    'outer: loop {
        submit_notify.notified().await;
        while let Ok(work) = submit_rx.pop() {
            match work {
                SubmitWork::Stop => break 'outer,
                SubmitWork::Batch(batch) => {
                    let popped_at = Instant::now();
                    let client = sdk_client.clone();
                    let reg = shared_registry.load_full();
                    submit_batch_task(
                        batch,
                        client,
                        reg,
                        &log,
                        &health,
                        Arc::clone(&semaphore),
                        popped_at,
                    )
                    .await;
                }
            }
        }
        if stop_flag.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
    }
    set_running(&health, false);
}

async fn drain_channel_with_error(sub: &mut OrderSubmitter, err: &str) {
    loop {
        sub.submit_notify.notified().await;
        while let Ok(work) = sub.submit_rx.pop() {
            match work {
                SubmitWork::Stop => return,
                SubmitWork::Batch(b) => {
                    let registry = sub.shared_registry.load_full();
                    for (target_idx, _) in b {
                        log_outcome_idx(&sub.log, &registry, target_idx, &Err(err.to_string()));
                    }
                }
            }
        }
        if sub.stop_flag.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
    }
}

async fn submit_batch_task(
    batch: SubmitBatch,
    client: Option<SdkClient<SdkAuthenticatedState<SdkAuthNormal>>>,
    registry: Arc<crate::TargetRegistry>,
    log: &Arc<Mutex<LogWriter>>,
    health: &Arc<Mutex<crate::SubmitterHealth>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    popped_at: Instant,
) {
    let batch_len = batch.len();
    if batch_len == 0 {
        return;
    }
    let task_start = Instant::now();
    with_submitter_health(health, |h| {
        h.record_pop_to_task_start_ns(
            batch_len,
            duration_ns_u64(task_start.saturating_duration_since(popped_at)),
        )
    });

    let Some(ref client_ref) = client else {
        return;
    };

    let batch_start = Instant::now();
    let prep_start = Instant::now();

    if batch_len == 1 {
        let (target_idx, signed) = batch.into_iter().next().expect("len==1");
        with_submitter_health(health, |h| {
            h.record_task_prep_ns(batch_len, duration_ns_u64(prep_start.elapsed()))
        });

        let permit_wait_start = Instant::now();
        let permit = match Arc::clone(&semaphore).acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                with_submitter_health(health, |h| {
                    h.record_permit_wait_ns(batch_len, duration_ns_u64(permit_wait_start.elapsed()));
                    h.record_batch_total_ns(batch_len, duration_ns_u64(batch_start.elapsed()));
                });
                return;
            }
        };
        with_submitter_health(health, |h| {
            h.record_permit_wait_ns(batch_len, duration_ns_u64(permit_wait_start.elapsed()))
        });

        let sdk_start = Instant::now();
        let outcome = match client_ref.post_order(*signed).await {
            Ok(resp) => {
                map_post_response(resp.success, resp.order_id, resp.error_msg, "submit_failed")
            }
            Err(e) => Err(format!("submit_failed:{}", e)),
        };
        let sdk_call_total_ns = duration_ns_u64(sdk_start.elapsed());
        drop(permit);
        with_submitter_health(health, |h| {
            h.record_sdk_call_total_ns(batch_len, sdk_call_total_ns);
            h.record_chunk_sdk_call_total_ns(1, sdk_call_total_ns);
        });
        record_outcome(health, &outcome);
        log_outcome_idx(log, &registry, target_idx, &outcome);
        with_submitter_health(health, |h| {
            h.record_batch_total_ns(batch_len, duration_ns_u64(batch_start.elapsed()))
        });
        return;
    }

    if batch_len <= MAX_CLOB_BATCH {
        // 2..=15 path: one post_orders call (no chunk fan-out).
        let target_idxs: smallvec::SmallVec<[crate::TargetIdx; 4]> =
            batch.iter().map(|(idx, _)| *idx).collect();
        let chunk_orders: Vec<SdkSignedOrder> = batch.into_iter().map(|(_, s)| *s).collect();
        with_submitter_health(health, |h| {
            h.record_task_prep_ns(batch_len, duration_ns_u64(prep_start.elapsed()))
        });

        let permit_wait_start = Instant::now();
        let permit = match Arc::clone(&semaphore).acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                with_submitter_health(health, |h| {
                    h.record_permit_wait_ns(batch_len, duration_ns_u64(permit_wait_start.elapsed()));
                    h.record_batch_total_ns(batch_len, duration_ns_u64(batch_start.elapsed()));
                });
                return;
            }
        };
        with_submitter_health(health, |h| {
            h.record_permit_wait_ns(batch_len, duration_ns_u64(permit_wait_start.elapsed()))
        });

        let sdk_start = Instant::now();
        let outcomes: Vec<(crate::TargetIdx, Result<String, String>)> =
            match client_ref.post_orders(chunk_orders).await {
                Ok(responses) => {
                    let resp_len = responses.len();
                    let mut out = Vec::with_capacity(target_idxs.len());
                    for (tidx, resp) in target_idxs.iter().zip(&responses) {
                        out.push((
                            *tidx,
                            map_post_response(
                                resp.success,
                                resp.order_id.clone(),
                                resp.error_msg.clone(),
                                "batch_submit_failed",
                            ),
                        ));
                    }
                    if resp_len < target_idxs.len() {
                        let err = format!(
                            "batch_response_short:expected={},got={}",
                            target_idxs.len(),
                            resp_len
                        );
                        for tidx in &target_idxs[resp_len..] {
                            out.push((*tidx, Err(err.clone())));
                        }
                    }
                    out
                }
                Err(e) => {
                    let err = format!("batch_submit_failed:{}", e);
                    target_idxs
                        .iter()
                        .map(|tidx| (*tidx, Err(err.clone())))
                        .collect()
                }
            };
        let sdk_call_total_ns = duration_ns_u64(sdk_start.elapsed());
        drop(permit);
        with_submitter_health(health, |h| {
            h.record_sdk_call_total_ns(batch_len, sdk_call_total_ns);
            h.record_chunk_sdk_call_total_ns(batch_len, sdk_call_total_ns);
        });
        for (target_idx, outcome) in outcomes {
            record_outcome(health, &outcome);
            log_outcome_idx(log, &registry, target_idx, &outcome);
        }
        with_submitter_health(health, |h| {
            h.record_batch_total_ns(batch_len, duration_ns_u64(batch_start.elapsed()))
        });
        return;
    }

    // Large multi-order: post_orders with chunking at CLOB limit, posted concurrently.
    let target_idxs: smallvec::SmallVec<[crate::TargetIdx; 4]> =
        batch.iter().map(|(idx, _)| *idx).collect();
    let mut signed: Vec<SdkSignedOrder> = batch.into_iter().map(|(_, s)| *s).collect();
    let mut chunk_jobs: Vec<(Vec<crate::TargetIdx>, Vec<SdkSignedOrder>)> = Vec::new();

    let mut offset = 0usize;
    while !signed.is_empty() {
        let chunk_len = signed.len().min(MAX_CLOB_BATCH);
        let chunk_orders: Vec<SdkSignedOrder> = signed.drain(..chunk_len).collect();
        let chunk_idxs = target_idxs[offset..offset + chunk_len].to_vec();
        chunk_jobs.push((chunk_idxs, chunk_orders));
        offset += chunk_len;
    }
    with_submitter_health(health, |h| {
        h.record_task_prep_ns(batch_len, duration_ns_u64(prep_start.elapsed()))
    });

    let client_owned = client_ref.clone();
    let futures = chunk_jobs.into_iter().map(|(chunk_idxs, chunk_orders)| {
        let chunk_len = chunk_orders.len();
        let sem = Arc::clone(&semaphore);
        let client = client_owned.clone();
        async move {
            let permit_wait_start = Instant::now();
            let permit = sem.acquire_owned().await;
            let permit_wait_ns = duration_ns_u64(permit_wait_start.elapsed());

            let mut outcomes: Vec<(crate::TargetIdx, Result<String, String>)> =
                Vec::with_capacity(chunk_len);
            let mut sdk_call_total_ns = 0u64;

            match permit {
                Ok(permit) => {
                    let sdk_start = Instant::now();
                    match client.post_orders(chunk_orders).await {
                        Ok(responses) => {
                            sdk_call_total_ns = duration_ns_u64(sdk_start.elapsed());
                            let resp_len = responses.len();
                            for (tidx, resp) in chunk_idxs.iter().zip(&responses) {
                                outcomes.push((
                                    *tidx,
                                    map_post_response(
                                        resp.success,
                                        resp.order_id.clone(),
                                        resp.error_msg.clone(),
                                        "batch_submit_failed",
                                    ),
                                ));
                            }
                            if resp_len < chunk_idxs.len() {
                                let err = format!(
                                    "batch_response_short:expected={},got={}",
                                    chunk_idxs.len(),
                                    resp_len
                                );
                                for tidx in &chunk_idxs[resp_len..] {
                                    outcomes.push((*tidx, Err(err.clone())));
                                }
                            }
                        }
                        Err(e) => {
                            sdk_call_total_ns = duration_ns_u64(sdk_start.elapsed());
                            let err = format!("batch_submit_failed:{}", e);
                            for tidx in &chunk_idxs {
                                outcomes.push((*tidx, Err(err.clone())));
                            }
                        }
                    }
                    drop(permit);
                }
                Err(_) => {
                    let err = "submitter_semaphore_closed".to_string();
                    for tidx in &chunk_idxs {
                        outcomes.push((*tidx, Err(err.clone())));
                    }
                }
            }

            (chunk_len, permit_wait_ns, sdk_call_total_ns, outcomes)
        }
    });

    let chunk_results = join_all(futures).await;
    for (chunk_len, permit_wait_ns, sdk_call_total_ns, outcomes) in chunk_results {
        with_submitter_health(health, |h| {
            h.record_permit_wait_ns(batch_len, permit_wait_ns);
            h.record_sdk_call_total_ns(batch_len, sdk_call_total_ns);
            h.record_chunk_sdk_call_total_ns(chunk_len, sdk_call_total_ns);
        });
        for (target_idx, outcome) in outcomes {
            record_outcome(health, &outcome);
            log_outcome_idx(log, &registry, target_idx, &outcome);
        }
    }
    with_submitter_health(health, |h| {
        h.record_batch_total_ns(batch_len, duration_ns_u64(batch_start.elapsed()))
    });
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
                match max_seen.compare_exchange(
                    prev,
                    current,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
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
    (start.elapsed(), max_inflight.load(std::sync::atomic::Ordering::SeqCst))
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
            match max_inflight.compare_exchange(
                prev,
                current,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
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

fn log_outcome_idx(
    log: &Arc<Mutex<LogWriter>>,
    registry: &crate::TargetRegistry,
    target_idx: crate::TargetIdx,
    outcome: &Result<String, String>,
) {
    let (sk, tok): (&str, &str) = match registry.targets.get(target_idx.0 as usize) {
        Some(target) => match registry.tokens.get(target.token_idx.0 as usize) {
            Some(token) => (&target.strategy_key, &token.token_id),
            None => (&target.strategy_key, "_"),
        },
        None => ("_", "_"),
    };
    if let Ok(mut g) = log.lock() {
        match outcome {
            Ok(eid) => g.log_order_ok(sk, tok, eid),
            Err(err) => g.log_order_err(sk, tok, err),
        }
    }
}
