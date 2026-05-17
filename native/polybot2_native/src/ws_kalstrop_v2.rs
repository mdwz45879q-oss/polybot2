//! Kalstrop V2 Socket.IO worker.
//! Connects to the BetGenius live stats endpoint via Socket.IO,
//! subscribes to fixture rooms, and processes genius_update frames
//! through the soccer engine.

use crate::kalstrop_v2_frame_pipeline::{process_v2_frame_sync, V2PendingLog};
use crate::kalstrop_v2_sio::{self, SioFrame};
use crate::dispatch::DispatchHandle;
use crate::log_writer::LogWriter;
use crate::ws::{apply_pending_patches, with_health};
use crate::*;
use futures_util::StreamExt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

pub(crate) struct KalstropV2WorkerConfig {
    pub base_url: String,
    pub sio_path: String,
    pub client_id: String,
    pub shared_secret_raw: String,
}

pub(crate) async fn run_kalstrop_v2_worker_async(
    engine: &mut SportEngine,
    cfg: KalstropV2WorkerConfig,
    mut dispatch_handle: DispatchHandle,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    command_rx: flume::Receiver<LiveWorkerCommand>,
    patch_rx: flume::Receiver<PatchPayload>,
    log: Arc<Mutex<LogWriter>>,
) {
    let worker_clock_origin = Instant::now();
    let mut fixture_ids: Vec<String> = Vec::new();
    let mut running = true;
    let mut reconnect_count: u32 = 0;

    // Initialize fixture_ids from subscriptions.
    if let Ok(lock) = subscriptions.read() {
        fixture_ids = lock.clone();
    }

    while running {
        // --- Check commands before connecting ---
        loop {
            match command_rx.try_recv() {
                Ok(LiveWorkerCommand::Stop) => {
                    with_health(&health, |h| h.running = false);
                    return;
                }
                Ok(LiveWorkerCommand::SetCandidateSubscriptions(next)) => {
                    fixture_ids = next;
                    if let Ok(mut lock) = subscriptions.write() {
                        *lock = fixture_ids.clone();
                    }
                }
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    with_health(&health, |h| h.running = false);
                    return;
                }
            }
        }
        let _ = apply_pending_patches(engine, &mut dispatch_handle, &patch_rx, &health, &log);

        if fixture_ids.is_empty() {
            with_health(&health, |h| {
                h.running = true;
                h.last_error.clear();
            });
            tokio_sleep(Duration::from_millis(500)).await;
            continue;
        }

        // --- Connect via Socket.IO ---
        let mut conn = match kalstrop_v2_sio::connect(&cfg.base_url, &cfg.sio_path, &cfg.client_id, &cfg.shared_secret_raw).await {
            Ok(c) => c,
            Err(e) => {
                with_health(&health, |h| {
                    h.reconnects += 1;
                    h.last_error = e.clone();
                });
                let backoff_ms = std::cmp::min(2000u64 * (1u64 << reconnect_count.min(5)), 60_000);
                tokio_sleep(Duration::from_millis(backoff_ms)).await;
                reconnect_count += 1;
                continue;
            }
        };

        // --- Subscribe to all fixtures ---
        let mut subscribe_ok = true;
        for fid in &fixture_ids {
            if let Err(e) = kalstrop_v2_sio::subscribe(&mut conn, fid, "", "10").await {
                with_health(&health, |h| {
                    h.reconnects += 1;
                    h.last_error = e.clone();
                });
                subscribe_ok = false;
                break;
            }
        }
        if !subscribe_ok {
            let backoff_ms = std::cmp::min(2000u64 * (1u64 << reconnect_count.min(5)), 60_000);
            tokio_sleep(Duration::from_millis(backoff_ms)).await;
            reconnect_count += 1;
            continue;
        }

        reconnect_count = 0;
        with_health(&health, |h| {
            h.running = true;
            h.last_error.clear();
        });
        if let Ok(mut g) = log.lock() {
            g.log_ws_connect(&fixture_ids);
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
                    Ok(LiveWorkerCommand::SetCandidateSubscriptions(next)) => {
                        let new_ids: Vec<String> = next.iter()
                            .filter(|id| !fixture_ids.contains(id))
                            .cloned()
                            .collect();
                        fixture_ids = next;
                        if let Ok(mut lock) = subscriptions.write() {
                            *lock = fixture_ids.clone();
                        }
                        for fid in &new_ids {
                            let _ = kalstrop_v2_sio::subscribe(&mut conn, fid, "", "10").await;
                        }
                    }
                    Err(flume::TryRecvError::Empty) => break,
                    Err(flume::TryRecvError::Disconnected) => {
                        running = false;
                        break 'event_loop;
                    }
                }
            }

            let _ = apply_pending_patches(engine, &mut dispatch_handle, &patch_rx, &health, &log);

            // Frame drain loop
            let mut pending_logs = smallvec::SmallVec::<[V2PendingLog; 4]>::new();
            let mut first_read = true;
            loop {
                let next = if first_read {
                    match tokio::time::timeout(Duration::from_millis(100), conn.ws.next()).await {
                        Ok(v) => v,
                        Err(_) => break,
                    }
                } else {
                    match futures_util::FutureExt::now_or_never(conn.ws.next()) {
                        Some(v) => v,
                        None => break,
                    }
                };
                let msg = match next {
                    None => {
                        reconn_reason = "v2_sio_stream_closed".to_string();
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = reconn_reason.clone();
                        });
                        break 'event_loop;
                    }
                    Some(Err(e)) => {
                        reconn_reason = format!("v2_sio_ws_read:{}", e);
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = reconn_reason.clone();
                        });
                        break 'event_loop;
                    }
                    Some(Ok(v)) => v,
                };
                first_read = false;

                let text = match &msg {
                    Message::Text(t) => t.as_str(),
                    Message::Binary(b) => match std::str::from_utf8(b.as_ref()) {
                        Ok(s) => s,
                        Err(_) => continue,
                    },
                    Message::Close(_) => {
                        reconn_reason = "v2_sio_close_received".to_string();
                        break 'event_loop;
                    }
                    _ => continue,
                };

                match kalstrop_v2_sio::classify_frame(text) {
                    SioFrame::GeniusUpdate(payload) => {
                        let source_recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                        if let SportEngine::Soccer(ref mut e) = engine {
                            if let Some(tl) = process_v2_frame_sync(
                                e, payload, source_recv_ns, &mut dispatch_handle, &log,
                            ) {
                                pending_logs.push(tl);
                            }
                        }
                    }
                    SioFrame::Ping => {
                        if let Err(e) = kalstrop_v2_sio::send_pong(&mut conn).await {
                            reconn_reason = e;
                            break 'event_loop;
                        }
                    }
                    SioFrame::Subscribed(_payload) => {
                        // Initial state delivered on subscribe — logged but not processed
                        // (engine will process subsequent genius_update frames)
                    }
                    SioFrame::ConnectAck | SioFrame::Other => {}
                }
            }

            // Flush deferred tick logs after drain
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

            if let Ok(mut g) = log.lock() {
                g.flush();
            }
        }

        // Disconnected — log and reconnect
        let reconnects = if let Ok(h) = health.lock() { h.reconnects } else { 0 };
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
