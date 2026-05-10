use super::*;
use super::sdk_exec::map_post_response;
use crate::log_writer::LogWriter;
use std::sync::{Arc, Mutex};

const MAX_CONCURRENT_SUBMITS: usize = 3;
const MAX_CLOB_BATCH: usize = 15;

fn set_running(health: &Arc<Mutex<crate::SubmitterHealth>>, running: bool) {
    if let Ok(mut h) = health.lock() {
        h.running = running;
    }
}

fn set_init_error(health: &Arc<Mutex<crate::SubmitterHealth>>, err: &str) {
    if let Ok(mut h) = health.lock() {
        h.running = false;
        h.last_error = err.to_string();
    }
}

fn record_outcome(
    health: &Arc<Mutex<crate::SubmitterHealth>>,
    outcome: &Result<String, String>,
) {
    if let Ok(mut h) = health.lock() {
        match outcome {
            Ok(_) => h.posted_ok += 1,
            Err(e) => {
                h.posted_err += 1;
                h.last_error = e.clone();
            }
        }
    }
}

/// Dispatcher loop: receives frame batches from the WS thread and spawns
/// concurrent submit tasks bounded by a semaphore. Each spawned task does
/// its own HTTP POST and logging — frame 2's orders no longer wait behind
/// frame 1's CLOB round-trip.
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
                    let permit = match Arc::clone(&semaphore).acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break 'outer,
                    };
                    let client = sdk_client.clone();
                    let reg = shared_registry.load_full();
                    let lg = Arc::clone(&log);
                    let hl = health.clone();
                    tokio::spawn(async move {
                        submit_batch_task(batch, client, reg, &lg, &hl).await;
                        drop(permit);
                    });
                }
            }
        }
        if stop_flag.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
    }

    let _ = semaphore
        .acquire_many(MAX_CONCURRENT_SUBMITS as u32)
        .await;
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
) {
    let Some(ref client) = client else {
        return;
    };
    if batch.is_empty() {
        return;
    }

    if batch.len() == 1 {
        let (target_idx, signed) = batch.into_iter().next().expect("len==1");
        let outcome = match client.post_order(*signed).await {
            Ok(resp) => {
                map_post_response(resp.success, resp.order_id, resp.error_msg, "submit_failed")
            }
            Err(e) => Err(format!("submit_failed:{}", e)),
        };
        record_outcome(health, &outcome);
        log_outcome_idx(log, &registry, target_idx, &outcome);
        return;
    }

    // Multi-order: post_orders with chunking at CLOB limit.
    let target_idxs: smallvec::SmallVec<[crate::TargetIdx; 4]> =
        batch.iter().map(|(idx, _)| *idx).collect();
    let mut signed: Vec<SdkSignedOrder> = batch.into_iter().map(|(_, s)| *s).collect();

    let mut offset = 0;
    while !signed.is_empty() {
        let chunk_len = signed.len().min(MAX_CLOB_BATCH);
        let chunk: Vec<SdkSignedOrder> = signed.drain(..chunk_len).collect();
        let chunk_idxs = &target_idxs[offset..offset + chunk_len];

        match client.post_orders(chunk).await {
            Ok(responses) => {
                let resp_len = responses.len();
                for (tidx, resp) in chunk_idxs.iter().zip(&responses) {
                    let outcome = map_post_response(
                        resp.success,
                        resp.order_id.clone(),
                        resp.error_msg.clone(),
                        "batch_submit_failed",
                    );
                    record_outcome(health, &outcome);
                    log_outcome_idx(log, &registry, *tidx, &outcome);
                }
                if resp_len < chunk_idxs.len() {
                    let err = format!(
                        "batch_response_short:expected={},got={}",
                        chunk_idxs.len(),
                        resp_len
                    );
                    for tidx in &chunk_idxs[resp_len..] {
                        record_outcome(health, &Err(err.clone()));
                        log_outcome_idx(log, &registry, *tidx, &Err(err.clone()));
                    }
                }
            }
            Err(e) => {
                let err = format!("batch_submit_failed:{}", e);
                for tidx in chunk_idxs {
                    record_outcome(health, &Err(err.clone()));
                    log_outcome_idx(log, &registry, *tidx, &Err(err.clone()));
                }
            }
        }
        offset += chunk_len;
    }
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
