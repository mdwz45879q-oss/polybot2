//! Multiplexed WS worker: manages multiple provider connections in a single
//! event loop via `tokio::select!`. "Fastest wins" — whichever provider
//! delivers a score change first triggers evaluation and dispatch.

use crate::boltodds_baseball_frame_pipeline::{
    process_boltodds_baseball_frame_sync, BoltOddsBaseballPendingLog,
};
use crate::boltodds_frame_pipeline::{process_boltodds_frame_sync, BoltOddsPendingLog};
use crate::dispatch::DispatchHandle;
use crate::kalstrop_v2_frame_pipeline::{process_v2_frame_sync, V2PendingLog};
use crate::kalstrop_v2_sio::{self, SioFrame};
use crate::log_writer::LogWriter;
use crate::ws::{apply_pending_patches, with_health};
use crate::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

/// Provider-specific connection configuration.
pub(crate) enum ProviderConfig {
    KalstropV1 {
        ws_url: String,
        client_id: String,
        shared_secret_raw: String,
    },
    KalstropV2(crate::ws_kalstrop_v2::KalstropV2WorkerConfig),
    BoltOdds {
        cfg: crate::ws_boltodds::BoltOddsWorkerConfig,
        game_labels: Vec<String>,
    },
}

type V1WsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;


/// Try to connect V1 Kalstrop WS and subscribe.
async fn try_connect_v1(
    ws_url: &str,
    client_id: &str,
    shared_secret_raw: &str,
    fixture_ids: &[String],
) -> Result<V1WsStream, String> {
    if ws_url.is_empty() || client_id.is_empty() || shared_secret_raw.is_empty() {
        return Err("v1_missing_credentials".to_string());
    }
    let ts = crate::dispatch::now_unix_s().to_string();
    let sig = crate::ws::kalstrop_signature(client_id, shared_secret_raw, &ts);
    let q = format!(
        "X-Client-ID={}&X-Timestamp={}&Authorization={}",
        urlencoding::encode(client_id),
        urlencoding::encode(&ts),
        urlencoding::encode(&format!("Bearer {}", sig)),
    );
    let sep = if ws_url.contains('?') { "&" } else { "?" };
    let uri = format!("{}{}{}", ws_url, sep, q);
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        uri.as_str(), None, true, None,
    )
    .await
    .map_err(|e| format!("v1_ws_connect:{}", e))?;

    crate::ws::send_kalstrop_subscribe_async(&mut ws, fixture_ids)
        .await
        .map_err(|e| format!("v1_subscribe:{}", e))?;

    Ok(ws)
}

/// Try to connect V2 Socket.IO and subscribe to all fixture IDs.
async fn try_connect_v2(
    cfg: &crate::ws_kalstrop_v2::KalstropV2WorkerConfig,
    fixture_ids: &[String],
) -> Result<kalstrop_v2_sio::SioConnection, String> {
    let mut conn = kalstrop_v2_sio::connect(
        &cfg.base_url, &cfg.sio_path, &cfg.client_id, &cfg.shared_secret_raw,
    ).await?;

    for fid in fixture_ids {
        kalstrop_v2_sio::subscribe(&mut conn, fid, "", "10").await?;
    }
    Ok(conn)
}

/// Try to connect BoltOdds WS, wait for handshake, and subscribe.
async fn try_connect_boltodds(
    cfg: &crate::ws_boltodds::BoltOddsWorkerConfig,
    game_labels: &[String],
) -> Result<V1WsStream, String> {
    let url = format!("{}?key={}", cfg.ws_url, cfg.api_key);
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        url.as_str(), None, true, None,
    )
    .await
    .map_err(|e| format!("boltodds_connect:{}", e))?;

    // Wait for socket_connected handshake
    let ack = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "boltodds_handshake_timeout".to_string())?
        .ok_or_else(|| "boltodds_handshake_closed".to_string())?
        .map_err(|e| format!("boltodds_handshake_read:{}", e))?;
    let ack_ok = match &ack {
        Message::Text(t) => t.contains("socket_connected"),
        Message::Binary(b) => std::str::from_utf8(b.as_ref())
            .map(|s| s.contains("socket_connected"))
            .unwrap_or(false),
        _ => false,
    };
    if !ack_ok {
        return Err("boltodds_handshake_not_ack".to_string());
    }

    // Subscribe
    let sub = serde_json::json!({"action": "subscribe", "filters": {"games": game_labels}});
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .map_err(|e| format!("boltodds_subscribe:{}", e))?;

    Ok(ws)
}

/// Run a multiplexed worker managing V1 + V2 + BoltOdds connections via `select!`.
pub(crate) async fn run_multiplexed_worker_async(
    engine: &mut SportEngine,
    providers: Vec<ProviderConfig>,
    mut dispatch_handle: DispatchHandle,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    command_rx: flume::Receiver<LiveWorkerCommand>,
    patch_rx: flume::Receiver<PatchPayload>,
    log: Arc<Mutex<LogWriter>>,
) {
    // Extract configs for each provider type.
    let mut v1_cfg: Option<(String, String, String)> = None;
    let mut v2_cfg: Option<crate::ws_kalstrop_v2::KalstropV2WorkerConfig> = None;
    let mut bo_cfg: Option<crate::ws_boltodds::BoltOddsWorkerConfig> = None;
    let mut bo_game_labels: Vec<String> = Vec::new();

    for p in providers {
        match p {
            ProviderConfig::KalstropV1 { ws_url, client_id, shared_secret_raw, .. } => {
                v1_cfg = Some((ws_url, client_id, shared_secret_raw));
            }
            ProviderConfig::KalstropV2(cfg) => {
                v2_cfg = Some(cfg);
            }
            ProviderConfig::BoltOdds { cfg, game_labels } => {
                bo_cfg = Some(cfg);
                bo_game_labels = game_labels;
            }
        }
    }

    let worker_clock_origin = Instant::now();
    let mut running = true;
    let mut v1_ws: Option<V1WsStream> = None;
    let mut v2_conn: Option<kalstrop_v2_sio::SioConnection> = None;
    let mut bo_ws: Option<V1WsStream> = None;
    let mut v1_reconnect_count: u32 = 0;
    let mut v2_reconnect_count: u32 = 0;
    let mut bo_reconnect_count: u32 = 0;
    // Per-provider reconnection timers. After a failed reconnection attempt,
    // the timer is set with exponential backoff. The event loop checks these
    // during housekeeping and breaks to retry when the timer expires.
    // Without this, a dead connection that fails to reconnect would never
    // get another attempt — the event loop would run indefinitely draining
    // only the live connections.
    let mut v1_reconnect_at: Option<Instant> = None;
    let mut v2_reconnect_at: Option<Instant> = None;
    let mut bo_reconnect_at: Option<Instant> = None;
    let mut candidate_subs: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    let mut v1_active_subs: Vec<String> = Vec::new();
    let mut v2_active_subs: Vec<String> = Vec::new();

    while running {
        // --- Drain commands ---
        loop {
            match command_rx.try_recv() {
                Ok(LiveWorkerCommand::Stop) => {
                    with_health(&health, |h| h.running = false);
                    return;
                }
                Ok(LiveWorkerCommand::SetCandidateSubscriptions(next)) => {
                    candidate_subs = next;
                    if let Ok(mut lock) = subscriptions.write() {
                        *lock = candidate_subs.values().flatten().cloned().collect();
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

        // --- Extract per-provider subscriptions ---
        let v1_subs = candidate_subs.get("kalstrop_v1").or_else(|| candidate_subs.get("kalstrop")).cloned().unwrap_or_default();
        let v2_subs = candidate_subs.get("kalstrop_v2").cloned().unwrap_or_default();
        let bo_subs = candidate_subs.get("boltodds").cloned().unwrap_or_default();
        if !bo_subs.is_empty() {
            bo_game_labels = bo_subs;
        }

        // --- Connect/reconnect dead connections ---
        if v1_ws.is_none() {
            if let Some((ref ws_url, ref client_id, ref secret)) = v1_cfg {
                if !v1_subs.is_empty() {
                    match try_connect_v1(ws_url, client_id, secret, &v1_subs).await {
                        Ok(ws) => {
                            v1_ws = Some(ws);
                            v1_active_subs = v1_subs.clone();
                            v1_reconnect_count = 0;
                            v1_reconnect_at = None;
                            eprintln!("[mux] V1 connected, {} subscriptions", v1_subs.len());
                        }
                        Err(e) => {
                            let backoff_ms = 2000u64 * (1u64 << v1_reconnect_count.min(5));
                            let backoff_ms = backoff_ms.min(30_000);
                            v1_reconnect_at = Some(Instant::now() + Duration::from_millis(backoff_ms));
                            eprintln!("[mux] V1 reconnect failed (attempt {}): {} — retry in {}ms", v1_reconnect_count + 1, e, backoff_ms);
                            with_health(&health, |h| {
                                h.reconnects += 1;
                                h.last_error = format!("v1:{}", e);
                            });
                            v1_reconnect_count += 1;
                        }
                    }
                }
            }
        }
        if v2_conn.is_none() {
            if let Some(ref cfg) = v2_cfg {
                if !v2_subs.is_empty() {
                    match try_connect_v2(cfg, &v2_subs).await {
                        Ok(conn) => {
                            v2_conn = Some(conn);
                            v2_active_subs = v2_subs.clone();
                            v2_reconnect_count = 0;
                            v2_reconnect_at = None;
                            eprintln!("[mux] V2 connected, {} subscriptions", v2_subs.len());
                        }
                        Err(e) => {
                            let backoff_ms = 2000u64 * (1u64 << v2_reconnect_count.min(5));
                            let backoff_ms = backoff_ms.min(30_000);
                            v2_reconnect_at = Some(Instant::now() + Duration::from_millis(backoff_ms));
                            eprintln!("[mux] V2 reconnect failed (attempt {}): {} — retry in {}ms", v2_reconnect_count + 1, e, backoff_ms);
                            with_health(&health, |h| {
                                h.reconnects += 1;
                                h.last_error = format!("v2:{}", e);
                            });
                            v2_reconnect_count += 1;
                        }
                    }
                }
            }
        }
        if bo_ws.is_none() {
            if let Some(ref cfg) = bo_cfg {
                if !bo_game_labels.is_empty() {
                    match try_connect_boltodds(cfg, &bo_game_labels).await {
                        Ok(ws) => {
                            bo_ws = Some(ws);
                            bo_reconnect_count = 0;
                            bo_reconnect_at = None;
                            eprintln!("[mux] BoltOdds connected, {} games", bo_game_labels.len());
                        }
                        Err(e) => {
                            let backoff_ms = 2000u64 * (1u64 << bo_reconnect_count.min(5));
                            let backoff_ms = backoff_ms.min(30_000);
                            bo_reconnect_at = Some(Instant::now() + Duration::from_millis(backoff_ms));
                            eprintln!("[mux] BoltOdds reconnect failed (attempt {}): {} — retry in {}ms", bo_reconnect_count + 1, e, backoff_ms);
                            with_health(&health, |h| {
                                h.reconnects += 1;
                                h.last_error = format!("bo:{}", e);
                            });
                            bo_reconnect_count += 1;
                        }
                    }
                }
            }
        }

        // All dead — backoff
        if v1_ws.is_none() && v2_conn.is_none() && bo_ws.is_none() {
            let max_count = v1_reconnect_count.max(v2_reconnect_count).max(bo_reconnect_count).min(5);
            let backoff_ms = 2000u64 * (1u64 << max_count);
            tokio::time::sleep(Duration::from_millis(backoff_ms.min(30_000))).await;
            continue;
        }

        with_health(&health, |h| {
            h.running = true;
            h.last_error.clear();
        });

        // Log connection state
        eprintln!("[mux] entering event loop: v1={}, v2={}, bo={}",
            if v1_ws.is_some() { "up" } else { "down" },
            if v2_conn.is_some() { "up" } else { "down" },
            if bo_ws.is_some() { "up" } else { "down" });
        {
            let all_subs: Vec<String> = candidate_subs.values().flatten().cloned().collect();
            if let Ok(mut g) = log.lock() {
                g.log_ws_connect("multiplexed", &all_subs);
            }
        }

        // --- Event loop: select! across all connections ---
        let mut reconn_v1 = false;
        let mut reconn_v2 = false;
        let mut reconn_bo = false;
        let mut bo_last_ping = Instant::now();

        'event_loop: loop {
            // Drain commands
            loop {
                match command_rx.try_recv() {
                    Ok(LiveWorkerCommand::Stop) => {
                        running = false;
                        break 'event_loop;
                    }
                    Ok(LiveWorkerCommand::SetCandidateSubscriptions(next)) => {
                        candidate_subs = next;
                        if let Ok(mut lock) = subscriptions.write() {
                            *lock = candidate_subs.values().flatten().cloned().collect();
                        }
                        let new_v1 = candidate_subs.get("kalstrop_v1").or_else(|| candidate_subs.get("kalstrop")).cloned().unwrap_or_default();
                        let new_v2 = candidate_subs.get("kalstrop_v2").cloned().unwrap_or_default();

                        // V1: resubscribe if the set changed
                        if new_v1 != v1_active_subs {
                            if let Some(ref mut ws) = v1_ws {
                                if let Err(e) = crate::ws::send_kalstrop_resubscribe_async(ws, &new_v1).await {
                                    eprintln!("[mux] V1 resubscribe error: {}", e);
                                    v1_ws = None;
                                    v1_active_subs.clear();
                                    reconn_v1 = true;
                                } else {
                                    v1_active_subs = new_v1;
                                }
                            }
                        }

                        // V2: subscribe only new fixture IDs (only mark successful ones as active)
                        let v2_new_ids: Vec<String> = new_v2.iter()
                            .filter(|id| !v2_active_subs.contains(id))
                            .cloned()
                            .collect();
                        if !v2_new_ids.is_empty() {
                            if let Some(ref mut conn) = v2_conn {
                                for fid in &v2_new_ids {
                                    match kalstrop_v2_sio::subscribe(conn, fid, "", "10").await {
                                        Ok(()) => {
                                            v2_active_subs.push(fid.clone());
                                        }
                                        Err(e) => {
                                            eprintln!("[mux] V2 subscribe {} error: {}", fid, e);
                                        }
                                    }
                                }
                            }
                        }

                        // BoltOdds: game labels are static (from plan), no resubscribe needed
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
            let mut pending_v2_logs = smallvec::SmallVec::<[V2PendingLog; 4]>::new();
            let mut pending_bo_logs = smallvec::SmallVec::<[BoltOddsPendingLog; 4]>::new();
            let mut pending_baseball_bo_logs =
                smallvec::SmallVec::<[BoltOddsBaseballPendingLog; 4]>::new();
            let mut got_frame = false;

            loop {
                // Two-phase drain: if we already got a frame, poll without
                // creating a timer future (saves ~50-100ns per burst frame).
                // Otherwise, wait up to 100ms for the first frame.
                if got_frame {
                    // Burst drain: poll each stream once without blocking
                    // (now_or_never returns None if no data buffered).
                    // Priority order: V2 > V1 > BoltOdds.
                    use futures_util::FutureExt;
                    let mut any_ready = false;

                    if let Some(ref mut conn) = v2_conn {
                        if let Some(msg) = conn.ws.next().now_or_never() {
                            any_ready = true;
                            match msg {
                                Some(Ok(Message::Text(ref text))) => {
                                    match kalstrop_v2_sio::classify_frame(text) {
                                        SioFrame::GeniusUpdate(payload) => {
                                            let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                            if let SportEngine::Soccer(ref mut e) = engine {
                                                if let Some(tl) = process_v2_frame_sync(
                                                    e, payload, recv_ns, &mut dispatch_handle, &log,
                                                ) {
                                                    pending_v2_logs.push(tl);
                                                }
                                            }
                                        }
                                        SioFrame::Ping => {
                                            if let Some(ref mut conn) = v2_conn {
                                                if let Err(_e) = kalstrop_v2_sio::send_pong(conn).await {
                                                    v2_conn = None;
                                                    v2_active_subs.clear();
                                                    reconn_v2 = true;
                                                }
                                            }
                                        }
                                        SioFrame::Subscribed(_) | SioFrame::ConnectAck | SioFrame::Other => {}
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    v2_conn = None;
                                    v2_active_subs.clear();
                                    reconn_v2 = true;
                                }
                                Some(Err(_)) => {
                                    v2_conn = None;
                                    v2_active_subs.clear();
                                    reconn_v2 = true;
                                }
                                _ => {}
                            }
                        }
                    }
                    if let Some(ref mut ws) = v1_ws {
                        if let Some(msg) = ws.next().now_or_never() {
                            any_ready = true;
                            match msg {
                                Some(Ok(Message::Text(ref text))) => {
                                    let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                    match engine {
                                        SportEngine::Soccer(ref mut e) => {
                                            crate::soccer::frame_pipeline::process_decoded_frame_sync(
                                                e, text, recv_ns, &mut dispatch_handle, &log,
                                            );
                                        }
                                        SportEngine::Baseball(ref mut e) => {
                                            crate::baseball::frame_pipeline::process_decoded_frame_sync(
                                                e, text, recv_ns, &mut dispatch_handle, &log,
                                            );
                                        }
                                        _ => {}
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    v1_ws = None;
                                    v1_active_subs.clear();
                                    reconn_v1 = true;
                                }
                                Some(Err(_)) => {
                                    v1_ws = None;
                                    v1_active_subs.clear();
                                    reconn_v1 = true;
                                }
                                _ => {}
                            }
                        }
                    }
                    if let Some(ref mut ws) = bo_ws {
                        if let Some(msg) = ws.next().now_or_never() {
                            any_ready = true;
                            match msg {
                                Some(Ok(Message::Text(ref text))) => {
                                    let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                    match engine {
                                        SportEngine::Soccer(ref mut e) => {
                                            if let Some(tl) = process_boltodds_frame_sync(
                                                e, text, recv_ns, &mut dispatch_handle, &log,
                                            ) {
                                                pending_bo_logs.push(tl);
                                            }
                                        }
                                        SportEngine::Baseball(ref mut e) => {
                                            if let Some(tl) = process_boltodds_baseball_frame_sync(
                                                e, text, recv_ns, &mut dispatch_handle, &log,
                                            ) {
                                                pending_baseball_bo_logs.push(tl);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                Some(Ok(Message::Binary(ref bytes))) => {
                                    if let Ok(text) = std::str::from_utf8(bytes) {
                                        let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                        match engine {
                                            SportEngine::Soccer(ref mut e) => {
                                                if let Some(tl) = process_boltodds_frame_sync(
                                                    e, text, recv_ns, &mut dispatch_handle, &log,
                                                ) {
                                                    pending_bo_logs.push(tl);
                                                }
                                            }
                                            SportEngine::Baseball(ref mut e) => {
                                                if let Some(tl) = process_boltodds_baseball_frame_sync(
                                                    e, text, recv_ns, &mut dispatch_handle, &log,
                                                ) {
                                                    pending_baseball_bo_logs.push(tl);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    bo_ws = None;
                                    reconn_bo = true;
                                }
                                Some(Err(_)) => {
                                    bo_ws = None;
                                    reconn_bo = true;
                                }
                                _ => {}
                            }
                        }
                    }
                    if !any_ready { break; }
                    if reconn_v1 || reconn_v2 || reconn_bo { break; }
                    continue;
                }

                let timeout_sleep = tokio::time::sleep(Duration::from_millis(100));
                tokio::pin!(timeout_sleep);

                tokio::select! {
                    biased;
                    // V2 first: fastest for goals (~1.5s ahead of V1)
                    msg = async {
                        match v2_conn.as_mut() {
                            Some(conn) => conn.ws.next().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        match msg {
                            Some(Ok(Message::Text(ref text))) => {
                                got_frame = true;
                                match kalstrop_v2_sio::classify_frame(text) {
                                    SioFrame::GeniusUpdate(payload) => {
                                        let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                        if let SportEngine::Soccer(ref mut e) = engine {
                                            if let Some(tl) = process_v2_frame_sync(
                                                e, payload, recv_ns, &mut dispatch_handle, &log,
                                            ) {
                                                pending_v2_logs.push(tl);
                                            }
                                        }
                                    }
                                    SioFrame::Ping => {
                                        if let Some(ref mut conn) = v2_conn {
                                            if let Err(e) = kalstrop_v2_sio::send_pong(conn).await {
                                                eprintln!("[mux] V2 pong error: {}", e);
                                                v2_conn = None;
                                                v2_active_subs.clear();
                                                reconn_v2 = true;
                                            }
                                        }
                                    }
                                    SioFrame::Subscribed(_) | SioFrame::ConnectAck | SioFrame::Other => {}
                                }
                            }
                            Some(Ok(Message::Close(_))) | None => {
                                eprintln!("[mux] V2 connection closed (v1={}, bo={})",
                                    if v1_ws.is_some() { "up" } else { "down" },
                                    if bo_ws.is_some() { "up" } else { "down" });
                                v2_conn = None;
                                v2_active_subs.clear();
                                reconn_v2 = true;
                                if v1_ws.is_none() && bo_ws.is_none() { break 'event_loop; }
                            }
                            Some(Err(e)) => {
                                eprintln!("[mux] V2 error: {} (v1={}, bo={})", e,
                                    if v1_ws.is_some() { "up" } else { "down" },
                                    if bo_ws.is_some() { "up" } else { "down" });
                                v2_conn = None;
                                v2_active_subs.clear();
                                reconn_v2 = true;
                                if v1_ws.is_none() && bo_ws.is_none() { break 'event_loop; }
                            }
                            _ => {}
                        }
                    }

                    // V1 branch
                    msg = async {
                        match v1_ws.as_mut() {
                            Some(ws) => ws.next().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        match msg {
                            Some(Ok(Message::Text(ref text))) => {
                                got_frame = true;
                                let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                match engine {
                                    SportEngine::Soccer(ref mut e) => {
                                        crate::soccer::frame_pipeline::process_decoded_frame_sync(
                                            e, text, recv_ns, &mut dispatch_handle, &log,
                                        );
                                    }
                                    SportEngine::Baseball(ref mut e) => {
                                        crate::baseball::frame_pipeline::process_decoded_frame_sync(
                                            e, text, recv_ns, &mut dispatch_handle, &log,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            Some(Ok(Message::Ping(p))) => {
                                if let Some(ref mut ws) = v1_ws {
                                    let _ = ws.send(Message::Pong(p)).await;
                                }
                            }
                            Some(Ok(Message::Close(_))) | None => {
                                eprintln!("[mux] V1 connection closed (v2={}, bo={})",
                                    if v2_conn.is_some() { "up" } else { "down" },
                                    if bo_ws.is_some() { "up" } else { "down" });
                                v1_ws = None;
                                v1_active_subs.clear();
                                reconn_v1 = true;
                                if v2_conn.is_none() && bo_ws.is_none() { break 'event_loop; }
                            }
                            Some(Err(e)) => {
                                eprintln!("[mux] V1 error: {} (v2={}, bo={})", e,
                                    if v2_conn.is_some() { "up" } else { "down" },
                                    if bo_ws.is_some() { "up" } else { "down" });
                                v1_ws = None;
                                v1_active_subs.clear();
                                reconn_v1 = true;
                                if v2_conn.is_none() && bo_ws.is_none() { break 'event_loop; }
                            }
                            _ => {}
                        }
                    }

                    // BoltOdds branch
                    msg = async {
                        match bo_ws.as_mut() {
                            Some(ws) => ws.next().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        match msg {
                            Some(Ok(Message::Text(ref text))) => {
                                got_frame = true;
                                let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                match engine {
                                    SportEngine::Soccer(ref mut e) => {
                                        if let Some(tl) = process_boltodds_frame_sync(
                                            e, text, recv_ns, &mut dispatch_handle, &log,
                                        ) {
                                            pending_bo_logs.push(tl);
                                        }
                                    }
                                    SportEngine::Baseball(ref mut e) => {
                                        if let Some(tl) = process_boltodds_baseball_frame_sync(
                                            e, text, recv_ns, &mut dispatch_handle, &log,
                                        ) {
                                            pending_baseball_bo_logs.push(tl);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Some(Ok(Message::Binary(ref bytes))) => {
                                if let Ok(text) = std::str::from_utf8(bytes) {
                                    got_frame = true;
                                    let recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
                                    match engine {
                                        SportEngine::Soccer(ref mut e) => {
                                            if let Some(tl) = process_boltodds_frame_sync(
                                                e, text, recv_ns, &mut dispatch_handle, &log,
                                            ) {
                                                pending_bo_logs.push(tl);
                                            }
                                        }
                                        SportEngine::Baseball(ref mut e) => {
                                            if let Some(tl) = process_boltodds_baseball_frame_sync(
                                                e, text, recv_ns, &mut dispatch_handle, &log,
                                            ) {
                                                pending_baseball_bo_logs.push(tl);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(p))) => {
                                if let Some(ref mut ws) = bo_ws {
                                    let _ = ws.send(Message::Pong(p)).await;
                                }
                            }
                            Some(Ok(Message::Close(_))) | None => {
                                eprintln!("[mux] BoltOdds connection closed (v1={}, v2={})",
                                    if v1_ws.is_some() { "up" } else { "down" },
                                    if v2_conn.is_some() { "up" } else { "down" });
                                bo_ws = None;
                                reconn_bo = true;
                                if v1_ws.is_none() && v2_conn.is_none() { break 'event_loop; }
                            }
                            Some(Err(e)) => {
                                eprintln!("[mux] BoltOdds error: {} (v1={}, v2={})", e,
                                    if v1_ws.is_some() { "up" } else { "down" },
                                    if v2_conn.is_some() { "up" } else { "down" });
                                bo_ws = None;
                                reconn_bo = true;
                                if v1_ws.is_none() && v2_conn.is_none() { break 'event_loop; }
                            }
                            _ => {}
                        }
                    }

                    // Timeout — break to housekeeping
                    _ = &mut timeout_sleep => {
                        break;
                    }
                }
            }

            // Flush deferred tick logs
            if !pending_v2_logs.is_empty() || !pending_bo_logs.is_empty() {
                if let SportEngine::Soccer(ref e) = engine {
                    if let Ok(mut g) = log.lock() {
                        for tl in &pending_v2_logs {
                            let gid = e.game_ids.get(tl.game_idx.0 as usize)
                                .map(|s| s.as_str()).unwrap_or("_");
                            let lg = e.game_leagues.get(tl.game_idx.0 as usize)
                                .map(|s| s.as_ref()).unwrap_or("");
                            g.log_tick(gid, &crate::log_writer::TickPayload::Soccer {
                                lg,
                                goals_home: tl.state.home.unwrap_or(0),
                                goals_away: tl.state.away.unwrap_or(0),
                                half: tl.half,
                                corners_home: tl.state.corners_home,
                                corners_away: tl.state.corners_away,
                                gs: tl.game_state,
                                src: "kalstrop_v2",
                                var_action_type: tl.var_action_type.as_str(),
                                var_action_subtype: tl.var_action_subtype.as_str(),
                            });
                        }
                        for tl in &pending_bo_logs {
                            let gid = e.game_ids.get(tl.game_idx.0 as usize)
                                .map(|s| s.as_str()).unwrap_or("_");
                            let lg = e.game_leagues.get(tl.game_idx.0 as usize)
                                .map(|s| s.as_ref()).unwrap_or("");
                            g.log_tick(gid, &crate::log_writer::TickPayload::Soccer {
                                lg,
                                goals_home: tl.state.home.unwrap_or(0),
                                goals_away: tl.state.away.unwrap_or(0),
                                half: tl.half,
                                corners_home: tl.state.corners_home,
                                corners_away: tl.state.corners_away,
                                gs: tl.game_state,
                                src: "boltodds",
                                var_action_type: "",
                                var_action_subtype: "",
                            });
                        }
                    }
                }
            }
            if !pending_baseball_bo_logs.is_empty() {
                if let SportEngine::Baseball(ref e) = engine {
                    if let Ok(mut g) = log.lock() {
                        for tl in &pending_baseball_bo_logs {
                            let gid = e.game_ids.get(tl.game_idx.0 as usize)
                                .map(|s| s.as_str()).unwrap_or("_");
                            let lg = e.game_leagues.get(tl.game_idx.0 as usize)
                                .map(|s| s.as_ref()).unwrap_or("");
                            g.log_tick(gid, &crate::log_writer::TickPayload::Baseball {
                                lg,
                                runs_home: tl.state.home.unwrap_or(0),
                                runs_away: tl.state.away.unwrap_or(0),
                                inn: tl.state.inning_number,
                                inn_half: tl.state.inning_half,
                                gs: tl.state.game_state,
                                src: "boltodds",
                                outs: tl.state.outs,
                                strikes: tl.state.strikes,
                                base1: tl.state.base1,
                                base2: tl.state.base2,
                                base3: tl.state.base3,
                                period_raw: tl.period_raw.as_str(),
                            });
                        }
                    }
                }
                pending_baseball_bo_logs.clear();
            }

            if let Ok(mut g) = log.lock() {
                g.flush();
            }

            // BoltOdds keepalive: send a WS ping every ~60s to detect dead
            // connections. BoltOdds has no protocol-level keepalive (unlike
            // V1 GraphQL pings and V2 Engine.IO pings). If the server
            // silently drops the TCP connection, ws.next() blocks forever
            // without this.
            if let Some(ref mut ws) = bo_ws {
                if bo_last_ping.elapsed() >= Duration::from_secs(60) {
                    if let Err(e) = ws.send(Message::Ping(vec![].into())).await {
                        eprintln!("[mux] BoltOdds ping failed: {} — marking dead", e);
                        bo_ws = None;
                        reconn_bo = true;
                    }
                    bo_last_ping = Instant::now();
                }
            }

            // Check reconnection timers: if a dead connection's backoff has
            // expired, schedule a reconnection by setting its flag. Without
            // this, a connection that fails to reconnect would stay dead
            // forever — the event loop drains only live connections and
            // nothing sets the reconn flag for an already-None connection.
            let now = Instant::now();
            if v1_ws.is_none() {
                if let Some(t) = v1_reconnect_at {
                    if now >= t {
                        reconn_v1 = true;
                        v1_reconnect_at = None;
                        eprintln!("[mux] V1 reconnect timer expired, will retry");
                    }
                }
            }
            if v2_conn.is_none() {
                if let Some(t) = v2_reconnect_at {
                    if now >= t {
                        reconn_v2 = true;
                        v2_reconnect_at = None;
                        eprintln!("[mux] V2 reconnect timer expired, will retry");
                    }
                }
            }
            if bo_ws.is_none() {
                if let Some(t) = bo_reconnect_at {
                    if now >= t {
                        reconn_bo = true;
                        bo_reconnect_at = None;
                        eprintln!("[mux] BoltOdds reconnect timer expired, will retry");
                    }
                }
            }

            // If any connection died or needs reconnection, break to outer loop
            if reconn_v1 || reconn_v2 || reconn_bo {
                let mut reasons = Vec::new();
                if reconn_v1 { reasons.push("v1"); }
                if reconn_v2 { reasons.push("v2"); }
                if reconn_bo { reasons.push("bo"); }
                let reason = format!("disconnected:{} (v1={}, v2={}, bo={})",
                    reasons.join("+"),
                    if v1_ws.is_some() { "up" } else { "down" },
                    if v2_conn.is_some() { "up" } else { "down" },
                    if bo_ws.is_some() { "up" } else { "down" },
                );
                eprintln!("[mux] {}", reason);
                with_health(&health, |h| {
                    h.reconnects += 1;
                    h.last_error = reason.clone();
                });
                if let Ok(mut g) = log.lock() {
                    g.log_ws_disconnect("multiplexed", &reason, 0);
                }
                break 'event_loop;
            }
        }

        // Clean shutdown (Stop command or channel closed)
        if !reconn_v1 && !reconn_v2 && !reconn_bo {
            if let Ok(mut g) = log.lock() {
                g.log_ws_disconnect("multiplexed", "stopped", 0);
            }
        }
    }

    with_health(&health, |h| h.running = false);
}
