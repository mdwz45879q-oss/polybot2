use super::*;
use crate::dispatch::DispatchHandle;
use crate::log_writer::LogWriter;
use tokio_tungstenite::connect_async_tls_with_config;

fn kalstrop_signature(client_id: &str, shared_secret_raw: &str, timestamp: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(shared_secret_raw.as_bytes());
    let hashed_secret = hex::encode(hasher.finalize());
    let payload = format!("{}:{}", client_id, timestamp);
    let mut mac = Hmac::<Sha256>::new_from_slice(hashed_secret.as_bytes())
        .unwrap_or_else(|_| Hmac::<Sha256>::new_from_slice(b"fallback").expect("hmac init"));
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn build_kalstrop_ws_uri(cfg: &RuntimeStartConfig) -> Option<String> {
    let ws_url = cfg
        .kalstrop_ws_url
        .clone()
        .unwrap_or_else(|| "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws".to_string());
    let client_id = cfg.kalstrop_client_id.clone().unwrap_or_default();
    let secret = cfg.kalstrop_shared_secret_raw.clone().unwrap_or_default();
    if ws_url.trim().is_empty() || client_id.trim().is_empty() || secret.trim().is_empty() {
        return None;
    }
    let ts = crate::dispatch::now_unix_s().to_string();
    let signature = kalstrop_signature(&client_id, &secret, &ts);
    let q = format!(
        "X-Client-ID={}&X-Timestamp={}&Authorization={}",
        urlencoding::encode(&client_id),
        urlencoding::encode(&ts),
        urlencoding::encode(&format!("Bearer {}", signature)),
    );
    let sep = if ws_url.contains('?') { "&" } else { "?" };
    Some(format!("{}{}{}", ws_url, sep, q))
}

fn kalstrop_subscribe_payload(subscriptions: &[String]) -> Value {
    json!({
        "id":"kal_scores_sub",
        "type":"subscribe",
        "payload":{
            "operationName":"sportsMatchStateUpdatedV2",
            "query":"subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!) { sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }",
            "variables":{"fixtureIds": subscriptions}
        }
    })
}

async fn send_kalstrop_subscribe_async<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    subscriptions: &[String],
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let init_payload = json!({"type":"connection_init","payload":{}});
    let init_text = serde_json::to_string(&init_payload)
        .map_err(|e| format!("connection_init_encode: {}", e))?;
    ws.send(Message::Text(init_text.into()))
        .await
        .map_err(|e| format!("connection_init_send: {}", e))?;
    if subscriptions.is_empty() {
        return Ok(());
    }
    let sub_payload = kalstrop_subscribe_payload(subscriptions);
    let sub_text =
        serde_json::to_string(&sub_payload).map_err(|e| format!("subscribe_encode: {}", e))?;
    ws.send(Message::Text(sub_text.into()))
        .await
        .map_err(|e| format!("subscribe_send: {}", e))?;
    Ok(())
}

async fn send_kalstrop_resubscribe_async<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    subscriptions: &[String],
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let complete_payload = json!({
        "id":"kal_scores_sub",
        "type":"complete",
    });
    let complete_text =
        serde_json::to_string(&complete_payload).map_err(|e| format!("complete_encode: {}", e))?;
    ws.send(Message::Text(complete_text.into()))
        .await
        .map_err(|e| format!("complete_send: {}", e))?;
    if subscriptions.is_empty() {
        return Ok(());
    }
    let sub_payload = kalstrop_subscribe_payload(subscriptions);
    let sub_text =
        serde_json::to_string(&sub_payload).map_err(|e| format!("subscribe_encode: {}", e))?;
    ws.send(Message::Text(sub_text.into()))
        .await
        .map_err(|e| format!("subscribe_send: {}", e))?;
    Ok(())
}

fn normalize_subscriptions(values: Vec<String>) -> Vec<String> {
    let mut out = values
        .into_iter()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out
}

fn apply_worker_command(cmd: LiveWorkerCommand, candidate_subs: &mut Vec<String>) -> (bool, bool) {
    match cmd {
        LiveWorkerCommand::Stop => (true, false),
        LiveWorkerCommand::SetCandidateSubscriptions(next) => {
            let normalized = normalize_subscriptions(next);
            let changed = *candidate_subs != normalized;
            *candidate_subs = normalized;
            (false, changed)
        }
    }
}

async fn sleep_with_command_poll(
    reconnect_sleep_s: f64,
    command_rx: &mut tokio_mpsc::UnboundedReceiver<LiveWorkerCommand>,
    candidate_subs: &mut Vec<String>,
) -> (bool, bool) {
    let duration = Duration::from_secs_f64(reconnect_sleep_s.max(0.01));
    let started = Instant::now();
    let mut changed = false;
    while started.elapsed() < duration {
        match command_rx.try_recv() {
            Ok(cmd) => {
                let (stop, candidate_changed) = apply_worker_command(cmd, candidate_subs);
                if stop {
                    return (true, changed);
                }
                changed |= candidate_changed;
            }
            Err(tokio_mpsc::error::TryRecvError::Empty) => {}
            Err(tokio_mpsc::error::TryRecvError::Disconnected) => return (true, changed),
        }
        tokio_sleep(Duration::from_millis(20)).await;
    }
    (false, changed)
}

async fn refresh_active_subscriptions(
    engine: &NativeMlbEngine,
    cfg: &RuntimeStartConfig,
    subscriptions: &Arc<RwLock<Vec<String>>>,
    candidate_subs: &[String],
    active_subs: &mut Vec<String>,
) -> Result<bool, String> {
    let subscribe_lead_minutes = cfg.subscribe_lead_minutes.unwrap_or(90).max(0);
    let next_active = engine.active_subscriptions_for_candidates(
        candidate_subs,
        crate::dispatch::now_unix_s(),
        subscribe_lead_minutes,
    );
    if *active_subs == next_active {
        return Ok(false);
    }
    *active_subs = next_active;
    if let Ok(mut lock) = subscriptions.write() {
        *lock = active_subs.clone();
    }
    Ok(true)
}

pub(crate) fn with_health<F>(health: &Arc<Mutex<RuntimeHealth>>, mut f: F)
where
    F: FnMut(&mut RuntimeHealth),
{
    if let Ok(mut lock) = health.lock() {
        f(&mut lock);
    }
}

pub(crate) async fn run_live_worker_async(
    engine: &mut NativeMlbEngine,
    cfg: RuntimeStartConfig,
    mut dispatch_handle: DispatchHandle,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    initial_candidate_subs: Vec<String>,
    initial_active_subs: Vec<String>,
    mut command_rx: tokio_mpsc::UnboundedReceiver<LiveWorkerCommand>,
    log: Arc<Mutex<LogWriter>>,
) {
    let reconnect_sleep_s = cfg.reconnect_sleep_seconds.unwrap_or(0.2).max(0.01);
    let subscription_refresh_interval =
        Duration::from_secs_f64(cfg.subscription_refresh_seconds.unwrap_or(120.0).max(1.0));
    let mut next_subscription_refresh = Instant::now();
    // Monotonic worker clock for dedup/cooldown deltas. Wall-clock (`now_unix_ns`)
    // can jump backward under chrony/NTP slewing; the engine's TTL/cooldown math
    // would go negative. The replay path (`process_score_event`) keeps using the
    // Python-supplied wall timestamp — replay is offline, so monotonicity isn't
    // required there.
    let worker_clock_origin = Instant::now();
    let mut candidate_subs = normalize_subscriptions(initial_candidate_subs);
    let mut active_subs = normalize_subscriptions(initial_active_subs);
    if let Ok(mut lock) = subscriptions.write() {
        *lock = active_subs.clone();
    }

    loop {
        let mut candidate_changed = false;
        while let Ok(cmd) = command_rx.try_recv() {
            let (stop, changed) = apply_worker_command(cmd, &mut candidate_subs);
            if stop {
                with_health(&health, |h| h.running = false);
                return;
            }
            candidate_changed |= changed;
        }
        if candidate_changed || Instant::now() >= next_subscription_refresh {
            next_subscription_refresh = Instant::now() + subscription_refresh_interval;
            if let Err(e) = refresh_active_subscriptions(
                engine,
                &cfg,
                &subscriptions,
                candidate_subs.as_slice(),
                &mut active_subs,
            )
            .await
            {
                with_health(&health, |h| {
                    h.running = false;
                    h.last_error = format!("subscription_refresh: {}", e);
                });
                return;
            }
        }

        if active_subs.is_empty() {
            with_health(&health, |h| {
                h.running = true;
                h.last_error.clear();
            });
            let (stop, changed) =
                sleep_with_command_poll(reconnect_sleep_s, &mut command_rx, &mut candidate_subs)
                    .await;
            if stop {
                with_health(&health, |h| h.running = false);
                return;
            }
            if changed {
                next_subscription_refresh = Instant::now();
            }
            continue;
        }

        let uri = match build_kalstrop_ws_uri(&cfg) {
            Some(v) => v,
            None => {
                with_health(&health, |h| {
                    h.last_error = "missing_kalstrop_ws_credentials".to_string();
                });
                let (stop, changed) = sleep_with_command_poll(
                    reconnect_sleep_s,
                    &mut command_rx,
                    &mut candidate_subs,
                )
                .await;
                if stop {
                    with_health(&health, |h| h.running = false);
                    return;
                }
                if changed {
                    next_subscription_refresh = Instant::now();
                }
                continue;
            }
        };
        let (mut ws, _) = match connect_async_tls_with_config(uri.as_str(), None, true, None).await
        {
            Ok(v) => v,
            Err(e) => {
                with_health(&health, |h| {
                    h.reconnects += 1;
                    h.last_error = format!("ws_connect: {}", e);
                });
                let (stop, changed) = sleep_with_command_poll(
                    reconnect_sleep_s,
                    &mut command_rx,
                    &mut candidate_subs,
                )
                .await;
                if stop {
                    with_health(&health, |h| h.running = false);
                    return;
                }
                if changed {
                    next_subscription_refresh = Instant::now();
                }
                continue;
            }
        };
        if let Err(e) = send_kalstrop_subscribe_async(&mut ws, &active_subs).await {
            with_health(&health, |h| {
                h.reconnects += 1;
                h.last_error = format!("ws_subscribe: {}", e);
            });
            let (stop, changed) =
                sleep_with_command_poll(reconnect_sleep_s, &mut command_rx, &mut candidate_subs)
                    .await;
            if stop {
                with_health(&health, |h| h.running = false);
                return;
            }
            if changed {
                next_subscription_refresh = Instant::now();
            }
            continue;
        }

        with_health(&health, |h| {
            h.running = true;
            h.last_error.clear();
        });
        if let Ok(mut g) = log.lock() {
            g.log_ws_connect(&active_subs);
        }

        let mut reconn_reason = String::new();
        'event_loop: loop {
            let mut candidate_changed_inner = false;
            loop {
                match command_rx.try_recv() {
                    Ok(cmd) => {
                        let (stop, changed) = apply_worker_command(cmd, &mut candidate_subs);
                        if stop {
                            with_health(&health, |h| h.running = false);
                            return;
                        }
                        candidate_changed_inner |= changed;
                    }
                    Err(tokio_mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                        with_health(&health, |h| h.running = false);
                        return;
                    }
                }
            }
            let mut active_changed = false;
            if candidate_changed_inner || Instant::now() >= next_subscription_refresh {
                next_subscription_refresh = Instant::now() + subscription_refresh_interval;
                match refresh_active_subscriptions(
                    engine,
                    &cfg,
                    &subscriptions,
                    candidate_subs.as_slice(),
                    &mut active_subs,
                )
                .await
                {
                    Ok(changed) => {
                        active_changed = changed;
                    }
                    Err(e) => {
                        with_health(&health, |h| {
                            h.running = false;
                            h.last_error = format!("subscription_refresh: {}", e);
                        });
                        return;
                    }
                }
            }

            if active_changed {
                if let Err(e) = send_kalstrop_resubscribe_async(&mut ws, &active_subs).await {
                    with_health(&health, |h| {
                        h.reconnects += 1;
                        h.last_error = format!("ws_resubscribe: {}", e);
                    });
                    break;
                }
                if active_subs.is_empty() {
                    break;
                }
            }

            // Frame drain loop — process all pending frames before housekeeping
            let mut first_read = true;
            loop {
                let wait = if first_read {
                    Duration::from_millis(100)
                } else {
                    Duration::ZERO
                };
                let next = tokio::time::timeout(wait, ws.next()).await;
                let msg = match next {
                    Err(_) => break,
                    Ok(None) => {
                        with_health(&health, |h| {
                            h.reconnects += 1;
                            h.last_error = "ws_stream_closed".to_string();
                        });
                        reconn_reason = "ws_stream_closed".to_string();
                        break 'event_loop;
                    }
                    Ok(Some(Err(e))) => {
                        reconn_reason = format!("ws_read: {}", e);
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
                let frame_text: &str = match &msg {
                    Message::Text(text) => text.as_ref(),
                    Message::Binary(bytes) => match std::str::from_utf8(bytes.as_ref()) {
                        Ok(s) => s,
                        Err(_) => continue,
                    },
                    Message::Ping(p) => {
                        let _ = ws.send(Message::Pong(p.clone())).await;
                        continue;
                    }
                    Message::Close(_) => {
                        reconn_reason = "ws_close_received".to_string();
                        break 'event_loop;
                    }
                    _ => continue,
                };

                crate::replay::process_decoded_frame_sync(
                    engine,
                    frame_text,
                    source_recv_ns,
                    &mut dispatch_handle,
                    &log,
                );
            }
            if let Ok(mut g) = log.lock() {
                g.flush();
            }
        }
        let reconnects = if let Ok(h) = health.lock() { h.reconnects } else { 0 };
        if let Ok(mut g) = log.lock() {
            g.log_ws_disconnect(&reconn_reason, reconnects);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_worker_command_stop() {
        let mut candidates = Vec::<String>::new();
        let (stop, changed) = apply_worker_command(LiveWorkerCommand::Stop, &mut candidates);
        assert!(stop);
        assert!(!changed);
    }

    #[test]
    fn apply_worker_command_set_subscriptions_normalizes() {
        let mut candidates = vec!["old".to_string()];
        let (stop, changed) = apply_worker_command(
            LiveWorkerCommand::SetCandidateSubscriptions(vec![
                " z ".to_string(),
                "a".to_string(),
                "a".to_string(),
                "".to_string(),
            ]),
            &mut candidates,
        );
        assert!(!stop);
        assert!(changed);
        assert_eq!(candidates, vec!["a".to_string(), "z".to_string()]);
    }

    fn register_game(engine: &mut NativeMlbEngine, id: &str, kickoff: Option<i64>) {
        let gidx = GameIdx(engine.game_ids.len() as u16);
        engine.game_id_to_idx.insert(id.to_string(), gidx);
        engine.game_ids.push(id.to_string());
        engine.game_targets.push(GameTargets::default());
        engine.kickoff_ts.push(kickoff);
        engine.token_ids_by_game.push(vec![]);
        engine.has_totals.push(false);
        engine.has_nrfi.push(false);
        engine.has_final.push(false);
        engine.rows.push(None);
        engine.game_states.push(GameState::default());
        engine.totals_final_under_emitted.push(false);
        engine.nrfi_resolved_games.push(false);
        engine.nrfi_first_inning_observed.push(false);
        engine.final_resolved_games.push(false);
    }

    #[test]
    fn scheduler_activation_respects_lead_and_completion() {
        let mut engine = NativeMlbEngine::new(2.0, 0.5, 0.1);
        let now = crate::dispatch::now_unix_s();
        register_game(&mut engine, "g_past", Some(now - 30));
        register_game(&mut engine, "g_future", Some(now + 600));
        register_game(&mut engine, "g_done", None);
        let gi_done = engine.game_id_to_idx["g_done"].0 as usize;
        engine.game_states[gi_done] = GameState {
            match_completed: Some(true),
            ..Default::default()
        };

        let out = engine.active_subscriptions_for_candidates(
            &[
                "g_done".to_string(),
                "g_future".to_string(),
                "g_past".to_string(),
                "g_no_kickoff".to_string(),
            ],
            now,
            5,
        );
        assert_eq!(out, vec!["g_no_kickoff".to_string(), "g_past".to_string()]);
    }

    #[test]
    fn refresh_active_subscriptions_changes_only_on_transition() {
        let mut engine = NativeMlbEngine::new(2.0, 0.5, 0.1);
        let now = crate::dispatch::now_unix_s();
        register_game(&mut engine, "g1", Some(now - 60));
        let gi = engine.game_id_to_idx["g1"].0 as usize;
        engine.token_ids_by_game[gi] = vec!["tok_1".to_string(), "tok_1".to_string()];
        let cfg = RuntimeStartConfig {
            subscribe_lead_minutes: Some(5),
            ..RuntimeStartConfig::default()
        };
        let subscriptions = Arc::new(RwLock::new(Vec::<String>::new()));
        let mut active_subs = Vec::<String>::new();
        let candidates = vec!["g1".to_string()];
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let changed = rt
            .block_on(refresh_active_subscriptions(
                &engine,
                &cfg,
                &subscriptions,
                candidates.as_slice(),
                &mut active_subs,
            ))
            .expect("refresh should succeed");
        assert!(changed);
        assert_eq!(active_subs, vec!["g1".to_string()]);
        let unchanged = rt
            .block_on(refresh_active_subscriptions(
                &engine,
                &cfg,
                &subscriptions,
                candidates.as_slice(),
                &mut active_subs,
            ))
            .expect("refresh should succeed");
        assert!(!unchanged);
    }
}
