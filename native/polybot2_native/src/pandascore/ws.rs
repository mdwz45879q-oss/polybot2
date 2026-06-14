use crate::dispatch::DispatchHandle;
use crate::log_writer::{LogWriter, TickPayload};
use crate::pandascore::cs2_pipeline::{
    process_pandascore_cs2_frame, PandaScoreCs2PendingLog, PandaScoreMatchState,
};
use crate::ws::{apply_pending_patches, with_health};
use crate::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::Message;

const BASE_URL: &str = "wss://live.pandascore.co/matches";

pub(crate) struct PandaScoreWorkerConfig {
    pub api_token: String,
}

#[derive(serde::Deserialize, Clone)]
pub(crate) struct PandaScoreMatchInit {
    pub match_id: i64,
    pub maps_to_win: i64,
    #[serde(default)]
    pub maps_home: i64,
    #[serde(default)]
    pub maps_away: i64,
    #[serde(default)]
    pub home_team_id: i64,
    #[serde(default)]
    pub away_team_id: i64,
}

type PsWsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

struct PandaScoreConn {
    match_id: i64,
    match_state: PandaScoreMatchState,
    ws: Option<PsWsStream>,
    reconnect_at: Option<Instant>,
    reconnect_count: u32,
    pending_connect: Option<tokio::sync::oneshot::Receiver<Result<PsWsStream, String>>>,
}

impl PandaScoreConn {
    fn is_connected(&self) -> bool {
        self.ws.is_some()
    }

    fn mark_disconnected(&mut self, reason: &str) {
        self.ws = None;
        if !self.match_state.match_completed {
            let backoff_ms =
                std::cmp::min(2000u64 * (1u64 << self.reconnect_count.min(5)), 60_000);
            self.reconnect_at = Some(Instant::now() + Duration::from_millis(backoff_ms));
            self.reconnect_count += 1;
            eprintln!(
                "[pandascore] match {} disconnected: {} (reconnect in {}ms)",
                self.match_id, reason, backoff_ms
            );
        }
    }
}

async fn try_connect(
    match_id: i64,
    token: &str,
) -> Result<PsWsStream, String> {
    let url = format!("{}/{}/low_latency_feed?token={}", BASE_URL, match_id, token);
    let (mut ws, _) = connect_async_tls_with_config(url.as_str(), None, true, None)
        .await
        .map_err(|e| format!("pandascore_connect:{}", e))?;

    // Wait for hello frame
    let hello = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .map_err(|_| "pandascore_hello_timeout".to_string())?;

    match hello {
        Some(Ok(Message::Text(text))) => {
            if let Ok(frame) =
                serde_json::from_str::<crate::pandascore::types::PandaScoreLLFrame>(text.as_ref())
            {
                if frame.is_hello() {
                    return Ok(ws);
                }
            }
            Ok(ws)
        }
        Some(Ok(_)) => Ok(ws),
        Some(Err(e)) => Err(format!("pandascore_hello_read:{}", e)),
        None => Err("pandascore_hello_stream_closed".to_string()),
    }
}

/// Process a single WS message for a connection. Handles Text, Binary,
/// Ping, and Close. Returns true if a data frame was processed.
fn handle_ws_message(
    msg: &Message,
    conn: &mut PandaScoreConn,
    engine: &mut SportEngine,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
    clock_origin: &Instant,
    pending_logs: &mut smallvec::SmallVec<[PandaScoreCs2PendingLog; 4]>,
) {
    let text = match msg {
        Message::Text(t) => Some(t.as_ref()),
        Message::Binary(b) => std::str::from_utf8(b.as_ref()).ok(),
        Message::Ping(p) => {
            let pong = p.clone();
            if let Some(ws) = conn.ws.as_mut() {
                let _ = futures_util::FutureExt::now_or_never(
                    ws.send(Message::Pong(pong)),
                );
            }
            None
        }
        Message::Close(_) => {
            conn.mark_disconnected("close_received");
            None
        }
        _ => None,
    };

    if let Some(text) = text {
        let recv_ns = clock_origin.elapsed().as_nanos() as i64;
        if let SportEngine::Cs2(ref mut e) = engine {
            if let Some(tl) = process_pandascore_cs2_frame(
                e,
                &mut conn.match_state,
                text,
                recv_ns,
                dispatch_handle,
                log,
            ) {
                pending_logs.push(tl);
            }
        }
    }
}

pub(crate) async fn run_pandascore_worker_async(
    engine: &mut SportEngine,
    cfg: PandaScoreWorkerConfig,
    match_inits: Vec<PandaScoreMatchInit>,
    mut dispatch_handle: DispatchHandle,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    command_rx: flume::Receiver<LiveWorkerCommand>,
    patch_rx: flume::Receiver<PatchPayload>,
    log: Arc<Mutex<LogWriter>>,
) {
    let worker_clock_origin = Instant::now();

    // Build per-match connections
    let mut connections: Vec<PandaScoreConn> = match_inits
        .into_iter()
        .map(|init| PandaScoreConn {
            match_id: init.match_id,
            match_state: PandaScoreMatchState::new(
                init.match_id,
                init.maps_to_win,
                init.maps_home,
                init.maps_away,
                init.home_team_id,
                init.away_team_id,
            ),
            ws: None,
            reconnect_at: Some(Instant::now()), // connect immediately
            reconnect_count: 0,
            pending_connect: None,
        })
        .collect();

    // Store match IDs in subscriptions for health reporting
    {
        let ids: Vec<String> = connections.iter().map(|c| c.match_id.to_string()).collect();
        if let Ok(mut lock) = subscriptions.write() {
            *lock = ids;
        }
    }

    let mut running = true;
    let mut pending_logs = smallvec::SmallVec::<[PandaScoreCs2PendingLog; 4]>::new();

    while running {
        // --- Check commands ---
        loop {
            match command_rx.try_recv() {
                Ok(LiveWorkerCommand::Stop) => {
                    running = false;
                    break;
                }
                Ok(LiveWorkerCommand::SetCandidateSubscriptions(_)) => {}
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    running = false;
                    break;
                }
            }
        }
        if !running {
            break;
        }

        // --- Apply patches + derive new connections ---
        let new_games_added =
            apply_pending_patches(engine, &mut dispatch_handle, &patch_rx, &health, &log);
        if new_games_added {
            if let SportEngine::Cs2(ref e) = engine {
                for (i, game_id_str) in e.game_ids.iter().enumerate() {
                    let mid: i64 = game_id_str.parse().unwrap_or(0);
                    if mid == 0 {
                        continue;
                    }
                    if connections.iter().any(|c| c.match_id == mid) {
                        continue;
                    }
                    let mtw = e.maps_to_win[i];
                    connections.push(PandaScoreConn {
                        match_id: mid,
                        match_state: PandaScoreMatchState::new(mid, mtw, 0, 0, 0, 0),
                        ws: None,
                        reconnect_at: Some(Instant::now()),
                        reconnect_count: 0,
                        pending_connect: None,
                    });
                    if let Ok(mut g) = log.lock() {
                        g.log_ws_connect("pandascore_patch", &[mid.to_string()]);
                    }
                }
                let ids: Vec<String> =
                    connections.iter().map(|c| c.match_id.to_string()).collect();
                if let Ok(mut lock) = subscriptions.write() {
                    *lock = ids;
                }
            }
        }

        // --- Collect completed reconnections (non-blocking) ---
        for conn in &mut connections {
            let resolved = match conn.pending_connect.as_mut() {
                Some(rx) => rx.try_recv().ok(),
                None => None,
            };
            if let Some(result) = resolved {
                conn.pending_connect = None;
                match result {
                    Ok(ws) => {
                        conn.ws = Some(ws);
                        conn.reconnect_count = 0;
                        if let Ok(mut g) = log.lock() {
                            g.log_ws_connect("pandascore", &[conn.match_id.to_string()]);
                        }
                        with_health(&health, |h| {
                            h.running = true;
                            h.last_error.clear();
                        });
                    }
                    Err(e) => {
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = e.clone();
                        });
                        conn.mark_disconnected(&e);
                    }
                }
            }
        }

        // --- Spawn reconnections for due connections (non-blocking) ---
        let now = Instant::now();
        for conn in &mut connections {
            if conn.is_connected()
                || conn.match_state.match_completed
                || conn.pending_connect.is_some()
            {
                continue;
            }
            if !conn.reconnect_at.map_or(false, |at| now >= at) {
                continue;
            }
            conn.reconnect_at = None;
            let mid = conn.match_id;
            let token = cfg.api_token.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            conn.pending_connect = Some(rx);
            tokio::spawn(async move {
                let _ = tx.send(try_connect(mid, &token).await);
            });
        }

        // --- Check if all matches are done ---
        let all_done =
            !connections.is_empty() && connections.iter().all(|c| c.match_state.match_completed);
        if all_done {
            with_health(&health, |h| h.running = false);
            break;
        }

        // --- Burst drain: poll all connections non-blocking ---
        loop {
            let mut any_ready = false;

            for conn in &mut connections {
                let ws = match conn.ws.as_mut() {
                    Some(ws) => ws,
                    None => continue,
                };

                match futures_util::FutureExt::now_or_never(ws.next()) {
                    Some(Some(Ok(ref msg))) => {
                        any_ready = true;
                        handle_ws_message(
                            msg, conn, engine, &mut dispatch_handle,
                            &log, &worker_clock_origin, &mut pending_logs,
                        );
                    }
                    Some(Some(Err(e))) => {
                        any_ready = true;
                        let reason = format!("ws_read:{}", e);
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = reason.clone();
                        });
                        let reconnects = health.lock().map(|h| h.reconnects).unwrap_or(0);
                        if let Ok(mut g) = log.lock() {
                            g.log_ws_disconnect("pandascore", &reason, reconnects);
                        }
                        conn.mark_disconnected(&reason);
                    }
                    Some(None) => {
                        any_ready = true;
                        let reason = "stream_closed";
                        let reconnects = health.lock().map(|h| h.reconnects).unwrap_or(0);
                        if let Ok(mut g) = log.lock() {
                            g.log_ws_disconnect("pandascore", reason, reconnects);
                        }
                        conn.mark_disconnected(reason);
                    }
                    None => {} // No data ready
                }
            }

            if !any_ready {
                break;
            }
        }

        // --- Flush deferred tick logs ---
        if !pending_logs.is_empty() {
            if let SportEngine::Cs2(ref e) = engine {
                if let Ok(mut g) = log.lock() {
                    for tl in &pending_logs {
                        let gid = e
                            .game_ids
                            .get(tl.game_idx.0 as usize)
                            .map(|s| s.as_str())
                            .unwrap_or("_");
                        let lg = e
                            .game_leagues
                            .get(tl.game_idx.0 as usize)
                            .map(|s| s.as_ref())
                            .unwrap_or("");
                        g.log_tick(
                            gid,
                            &TickPayload::Cs2 {
                                lg,
                                maps_home: tl.maps_home,
                                maps_away: tl.maps_away,
                                rounds_home: tl.rounds_home,
                                rounds_away: tl.rounds_away,
                                current_map: tl.current_map,
                                gs: tl.game_state,
                                src: "pandascore",
                            },
                        );
                    }
                }
            }
            pending_logs.clear();
        }

        // --- Flush log buffer ---
        if let Ok(mut g) = log.lock() {
            g.flush();
        }

        // --- Blocking wait: frame-aware select! ---
        // Poll the first connected WS stream so we wake IMMEDIATELY on
        // frame arrival instead of sleeping through it. The subsequent
        // burst drain catches frames from all other connections.
        let first_connected = connections.iter().position(|c| c.ws.is_some());

        if let Some(idx) = first_connected {
            let timeout = tokio::time::sleep(Duration::from_millis(100));
            tokio::pin!(timeout);

            let ws_ref = connections[idx].ws.as_mut().unwrap();

            tokio::select! {
                biased;
                msg = ws_ref.next() => {
                    match msg {
                        Some(Ok(ref m)) => {
                            handle_ws_message(
                                m, &mut connections[idx], engine,
                                &mut dispatch_handle, &log,
                                &worker_clock_origin, &mut pending_logs,
                            );
                        }
                        Some(Err(e)) => {
                            let reason = format!("ws_read:{}", e);
                            with_health(&health, |h| {
                                h.reconnects += 1;
                                h.last_error = reason.clone();
                            });
                            connections[idx].mark_disconnected(&reason);
                        }
                        None => {
                            connections[idx].mark_disconnected("stream_closed");
                        }
                    }
                }
                cmd = command_rx.recv_async() => {
                    match cmd {
                        Ok(LiveWorkerCommand::Stop) => { running = false; }
                        Ok(LiveWorkerCommand::SetCandidateSubscriptions(_)) => {}
                        Err(_) => { running = false; }
                    }
                }
                _ = &mut timeout => {}
            }
        } else if connections.iter().any(|c| c.reconnect_at.is_some()) {
            // No connections active, but reconnects pending — short sleep
            tokio_sleep(Duration::from_millis(100)).await;
        } else {
            // Nothing connected, nothing pending — sleep longer
            tokio_sleep(Duration::from_millis(500)).await;
        }
    }

    // --- Cleanup: close all connections ---
    for conn in &mut connections {
        if let Some(ref mut ws) = conn.ws {
            let _ = ws.close(None).await;
        }
    }

    with_health(&health, |h| h.running = false);
}
