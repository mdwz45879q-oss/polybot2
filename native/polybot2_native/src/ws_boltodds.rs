//! BoltOdds WebSocket worker.
//! Mirrors the structure of `ws.rs` (V1 Kalstrop) but with BoltOdds-specific
//! connection handshake, subscribe protocol, and frame dispatch.

use crate::boltodds_frame_pipeline::{process_boltodds_frame_sync, BoltOddsPendingLog};
use crate::dispatch::DispatchHandle;
use crate::log_writer::LogWriter;
use crate::ws::{apply_pending_patches, with_health};
use crate::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::Message;

pub(crate) struct BoltOddsWorkerConfig {
    pub ws_url: String,
    pub api_key: String,
}

/// Build the subscribe payload for BoltOdds WS.
fn boltodds_subscribe_payload(game_labels: &[String]) -> String {
    let msg = json!({
        "action": "subscribe",
        "filters": {
            "games": game_labels
        }
    });
    msg.to_string()
}

/// Main BoltOdds WS worker loop. Runs on a dedicated thread.
///
/// Takes `&mut SportEngine` (must be `SportEngine::Soccer` variant). This
/// allows reusing `apply_pending_patches` from `ws.rs` directly.
pub(crate) async fn run_boltodds_worker_async(
    engine: &mut SportEngine,
    cfg: BoltOddsWorkerConfig,
    mut dispatch_handle: DispatchHandle,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    initial_game_labels: Vec<String>,
    command_rx: flume::Receiver<LiveWorkerCommand>,
    patch_rx: flume::Receiver<PatchPayload>,
    log: Arc<Mutex<LogWriter>>,
) {
    let worker_clock_origin = Instant::now();
    let mut game_labels = initial_game_labels;
    let mut running = true;
    let mut reconnect_count: u32 = 0;

    // Store labels in subscriptions Arc for health reporting.
    if let Ok(mut lock) = subscriptions.write() {
        *lock = game_labels.clone();
    }

    while running {
        // --- Check commands before connecting ---
        loop {
            match command_rx.try_recv() {
                Ok(LiveWorkerCommand::Stop) => {
                    with_health(&health, |h| h.running = false);
                    return;
                }
                Ok(LiveWorkerCommand::SetCandidateSubscriptions(labels)) => {
                    game_labels = labels;
                    if let Ok(mut lock) = subscriptions.write() {
                        *lock = game_labels.clone();
                    }
                }
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    with_health(&health, |h| h.running = false);
                    return;
                }
            }
        }
        apply_pending_patches(engine, &mut dispatch_handle, &patch_rx, &health, &log);

        if game_labels.is_empty() {
            with_health(&health, |h| {
                h.running = true;
                h.last_error.clear();
            });
            tokio_sleep(Duration::from_millis(500)).await;
            continue;
        }

        // --- Connect ---
        let url = format!("{}?key={}", cfg.ws_url, cfg.api_key);
        let (mut ws, _) = match connect_async_tls_with_config(url.as_str(), None, true, None).await
        {
            Ok(v) => v,
            Err(e) => {
                with_health(&health, |h| {
                    h.reconnects += 1;
                    h.last_error = format!("boltodds_ws_connect:{}", e);
                });
                let backoff_ms = std::cmp::min(2000u64 * (1u64 << reconnect_count.min(5)), 60_000);
                tokio_sleep(Duration::from_millis(backoff_ms)).await;
                reconnect_count += 1;
                continue;
            }
        };

        // --- Handshake: wait for socket_connected ack ---
        let handshake_ok = match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                // Expect the first frame to contain "socket_connected"
                text.contains("socket_connected")
            }
            Ok(Some(Ok(Message::Binary(bytes)))) => std::str::from_utf8(bytes.as_ref())
                .map(|s| s.contains("socket_connected"))
                .unwrap_or(false),
            _ => false,
        };
        if !handshake_ok {
            with_health(&health, |h| {
                h.reconnects += 1;
                h.last_error = "boltodds_handshake_failed".to_string();
            });
            let _ = ws.close(None).await;
            let backoff_ms = std::cmp::min(2000u64 * (1u64 << reconnect_count.min(5)), 60_000);
            tokio_sleep(Duration::from_millis(backoff_ms)).await;
            reconnect_count += 1;
            continue;
        }

        // --- Subscribe ---
        let sub_msg = boltodds_subscribe_payload(&game_labels);
        if let Err(e) = ws.send(Message::Text(sub_msg.into())).await {
            with_health(&health, |h| {
                h.reconnects += 1;
                h.last_error = format!("boltodds_subscribe_send:{}", e);
            });
            let backoff_ms = std::cmp::min(2000u64 * (1u64 << reconnect_count.min(5)), 60_000);
            tokio_sleep(Duration::from_millis(backoff_ms)).await;
            reconnect_count += 1;
            continue;
        }

        // Successful handshake + subscribe — reset backoff.
        reconnect_count = 0;

        with_health(&health, |h| {
            h.running = true;
            h.last_error.clear();
        });
        if let Ok(mut g) = log.lock() {
            g.log_ws_connect(&game_labels);
        }

        // --- Event loop ---
        let mut reconn_reason = String::new();
        'event_loop: loop {
            // Drain commands
            loop {
                match command_rx.try_recv() {
                    Ok(LiveWorkerCommand::Stop) => {
                        running = false;
                        break 'event_loop;
                    }
                    Ok(LiveWorkerCommand::SetCandidateSubscriptions(labels)) => {
                        // Re-subscribe with new labels
                        game_labels = labels.clone();
                        if let Ok(mut lock) = subscriptions.write() {
                            *lock = game_labels.clone();
                        }
                        let msg = boltodds_subscribe_payload(&game_labels);
                        let _ = ws.send(Message::Text(msg.into())).await;
                    }
                    Err(flume::TryRecvError::Empty) => break,
                    Err(flume::TryRecvError::Disconnected) => {
                        running = false;
                        break 'event_loop;
                    }
                }
            }

            // Apply patches
            apply_pending_patches(engine, &mut dispatch_handle, &patch_rx, &health, &log);

            // Frame drain loop — collect pending logs, flush after drain.
            let mut pending_logs = smallvec::SmallVec::<[BoltOddsPendingLog; 4]>::new();
            let mut first_read = true;
            loop {
                let wait = if first_read {
                    Duration::from_millis(100)
                } else {
                    Duration::ZERO
                };
                let next = tokio::time::timeout(wait, ws.next()).await;
                let msg = match next {
                    Err(_) => break, // timeout — go to housekeeping
                    Ok(None) => {
                        reconn_reason = "boltodds_stream_closed".to_string();
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = reconn_reason.clone();
                        });
                        break 'event_loop;
                    }
                    Ok(Some(Err(e))) => {
                        reconn_reason = format!("boltodds_ws_read:{}", e);
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = reconn_reason.clone();
                        });
                        break 'event_loop;
                    }
                    Ok(Some(Ok(v))) => v,
                };
                first_read = false;

                let source_recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                match &msg {
                    Message::Text(text) => {
                        if let SportEngine::Soccer(ref mut e) = engine {
                            if let Some(tl) = process_boltodds_frame_sync(
                                e,
                                text.as_ref(),
                                source_recv_ns,
                                &mut dispatch_handle,
                                &log,
                            ) {
                                pending_logs.push(tl);
                            }
                        }
                    }
                    Message::Binary(bytes) => {
                        if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                            if let SportEngine::Soccer(ref mut e) = engine {
                                if let Some(tl) = process_boltodds_frame_sync(
                                    e,
                                    text,
                                    source_recv_ns,
                                    &mut dispatch_handle,
                                    &log,
                                ) {
                                    pending_logs.push(tl);
                                }
                            }
                        }
                    }
                    Message::Ping(p) => {
                        let _ = ws.send(Message::Pong(p.clone())).await;
                    }
                    Message::Close(_) => {
                        reconn_reason = "boltodds_close_received".to_string();
                        break 'event_loop;
                    }
                    _ => {}
                }
            }

            // Flush deferred tick logs after drain (off the hot path).
            if !pending_logs.is_empty() {
                if let SportEngine::Soccer(ref e) = engine {
                    if let Ok(mut g) = log.lock() {
                        for tl in &pending_logs {
                            let gid = e
                                .game_ids
                                .get(tl.game_idx.0 as usize)
                                .map(|s| s.as_str())
                                .unwrap_or("_");
                            g.log_tick(
                                gid,
                                tl.state.home,
                                tl.state.away,
                                None,
                                tl.half,
                                tl.game_state,
                                tl.state.total_corners,
                            );
                        }
                    }
                }
                pending_logs.clear();
            }

            // Flush log buffer
            if let Ok(mut g) = log.lock() {
                g.flush();
            }
        }

        // Disconnected — log and reconnect
        let reconnects = if let Ok(h) = health.lock() {
            h.reconnects
        } else {
            0
        };
        if let Ok(mut g) = log.lock() {
            g.log_ws_disconnect(&reconn_reason, reconnects);
        }

        if running {
            let backoff_ms = std::cmp::min(2000u64 * (1u64 << reconnect_count.min(5)), 60_000);
            tokio_sleep(Duration::from_millis(backoff_ms)).await;
            reconnect_count += 1;
        }
    }

    with_health(&health, |h| h.running = false);
}
