use super::*;
use crate::dispatch::DispatchRuntime;
use crate::replay::TelemetryRuntimeState;
use crate::telemetry::TelemetryEmitter;

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
    dispatch_runtime: &mut DispatchRuntime,
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
    let active_tokens = engine.active_token_ids_for_games(active_subs.as_slice());
    dispatch_runtime.activate_presign_templates_for_tokens(active_tokens.as_slice());
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
    mut dispatch_runtime: DispatchRuntime,
    telemetry: Option<TelemetryEmitter>,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    initial_candidate_subs: Vec<String>,
    initial_active_subs: Vec<String>,
    mut command_rx: tokio_mpsc::UnboundedReceiver<LiveWorkerCommand>,
) {
    let reconnect_sleep_s = cfg.reconnect_sleep_seconds.unwrap_or(0.2).max(0.01);
    let subscription_refresh_interval =
        Duration::from_secs_f64(cfg.subscription_refresh_seconds.unwrap_or(120.0).max(1.0));
    let mut next_subscription_refresh = Instant::now();
    let mut telemetry_state = TelemetryRuntimeState::default();
    let refresh_interval_s = dispatch_runtime.active_order_refresh_interval_seconds();
    let refresh_interval = if refresh_interval_s > 0.0 {
        Some(Duration::from_secs_f64(refresh_interval_s.max(0.01)))
    } else {
        None
    };
    let mut last_active_refresh = Instant::now();
    let mut candidate_subs = normalize_subscriptions(initial_candidate_subs);
    let mut active_subs = normalize_subscriptions(initial_active_subs);
    if let Ok(mut lock) = subscriptions.write() {
        *lock = active_subs.clone();
    }
    let heartbeat_interval = Duration::from_secs(30);
    let mut last_heartbeat = Instant::now();

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
                &mut dispatch_runtime,
                &subscriptions,
                candidate_subs.as_slice(),
                &mut active_subs,
            )
            .await
            {
                if let Some(emitter) = telemetry.as_ref() {
                    emitter.emit(
                        "ws_disconnected",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "subscription_refresh_error",
                        json!({"error": e.clone()}),
                    );
                }
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
                if let Some(emitter) = telemetry.as_ref() {
                    emitter.emit(
                        "ws_disconnected",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "missing_kalstrop_ws_credentials",
                        json!({}),
                    );
                }
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
        let (mut ws, _) = match connect_async(uri.as_str()).await {
            Ok(v) => v,
            Err(e) => {
                if let Some(emitter) = telemetry.as_ref() {
                    emitter.emit(
                        "ws_disconnected",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "ws_connect_error",
                        json!({"error": e.to_string()}),
                    );
                }
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
            if let Some(emitter) = telemetry.as_ref() {
                emitter.emit(
                    "ws_disconnected",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "ws_subscribe_error",
                    json!({"error": e.clone()}),
                );
            }
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
        if let Some(emitter) = telemetry.as_ref() {
            emitter.emit(
                "ws_connected",
                "",
                "",
                "",
                "",
                "",
                "",
                json!({
                    "subscriptions": active_subs.clone(),
                }),
            );
            emitter.emit(
                "exec_connected",
                "",
                "",
                "",
                "",
                "",
                "",
                json!({
                    "dispatch_mode": dispatch_runtime.dispatch_mode_label(),
                }),
            );
        }

        loop {
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
                    &mut dispatch_runtime,
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
                        if let Some(emitter) = telemetry.as_ref() {
                            emitter.emit(
                                "ws_disconnected",
                                "",
                                "",
                                "",
                                "",
                                "",
                                "subscription_refresh_error",
                                json!({"error": e.clone()}),
                            );
                        }
                        with_health(&health, |h| {
                            h.running = false;
                            h.last_error = format!("subscription_refresh: {}", e);
                        });
                        return;
                    }
                }
            }

            if active_changed {
                if let Some(emitter) = telemetry.as_ref() {
                    emitter.emit(
                        "subscriptions_changed",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        json!({
                            "active_count": active_subs.len(),
                            "active_subscriptions": active_subs.clone(),
                        }),
                    );
                }
                if let Err(e) = send_kalstrop_resubscribe_async(&mut ws, &active_subs).await {
                    if let Some(emitter) = telemetry.as_ref() {
                        emitter.emit(
                            "ws_reconnected",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "ws_resubscribe_error",
                            json!({"error": e.clone()}),
                        );
                    }
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
            let now = Instant::now();
            if let Some(interval) = refresh_interval {
                if now.duration_since(last_active_refresh) >= interval {
                    dispatch_runtime
                        .refresh_active_state_from_broker_async()
                        .await;
                    last_active_refresh = now;
                }
            }
            if now.duration_since(last_heartbeat) >= heartbeat_interval {
                if let Some(emitter) = telemetry.as_ref() {
                    emitter.emit(
                        "runtime_heartbeat",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        json!({
                            "games": engine.dump_game_states_for_heartbeat(active_subs.as_slice()),
                            "teams": engine.dump_team_names(),
                            "dm": dispatch_runtime.dispatch_mode_label(),
                        }),
                    );
                }
                last_heartbeat = now;
            }
            dispatch_runtime.refill_presign_tick_async().await;
            if active_subs.is_empty() {
                break;
            }

            let next = tokio::time::timeout(Duration::from_millis(100), ws.next()).await;
            let msg = match next {
                Err(_) => continue,
                Ok(None) => {
                    with_health(&health, |h| {
                        h.reconnects += 1;
                        h.last_error = "ws_stream_closed".to_string();
                    });
                    break;
                }
                Ok(Some(Err(e))) => {
                    with_health(&health, |h| {
                        h.reconnects += 1;
                        h.last_error = format!("ws_read: {}", e);
                    });
                    break;
                }
                Ok(Some(Ok(v))) => v,
            };

            let source_recv_ns = crate::dispatch::now_unix_ns();
            let frame: Value = match msg {
                Message::Text(text) => match parse_json_text(text.as_ref()) {
                    Ok(v) => v,
                    Err(_) => {
                        if let Some(emitter) = telemetry.as_ref() {
                            emitter.emit_empty(
                                "provider_decode_error",
                                "",
                                "",
                                "",
                                "",
                                "",
                                "json_text_decode_error",
                            );
                        }
                        continue;
                    }
                },
                Message::Binary(bytes) => match parse_json_bytes(bytes.as_ref()) {
                    Ok(v) => v,
                    Err(_) => {
                        if let Some(emitter) = telemetry.as_ref() {
                            emitter.emit_empty(
                                "provider_decode_error",
                                "",
                                "",
                                "",
                                "",
                                "",
                                "json_binary_decode_error",
                            );
                        }
                        continue;
                    }
                },
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Message::Close(_) => break,
                _ => continue,
            };

            let mut dispatch_error: Option<String> = None;
            crate::replay::process_decoded_frame_async(
                engine,
                &frame,
                source_recv_ns,
                source_recv_ns,
                &mut dispatch_runtime,
                telemetry.as_ref(),
                &mut telemetry_state,
                |err| {
                    if dispatch_error.is_none() {
                        dispatch_error = Some(err);
                    }
                },
            )
            .await;
            if let Some(err) = dispatch_error {
                if let Some(emitter) = telemetry.as_ref() {
                    emitter.emit_empty("exec_error", "", "", "", "", "", err.as_str());
                }
                with_health(&health, |h| h.last_error = format!("dispatch: {}", err));
            }
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

    #[test]
    fn scheduler_activation_respects_lead_and_completion() {
        let mut engine = NativeMlbEngine::new(2.0, 0.5, 0.1, 5.0, 0.52, "FAK".to_string());
        let now = crate::dispatch::now_unix_s();
        engine
            .kickoff_ts_by_game
            .insert("g_past".to_string(), now - 30);
        engine
            .kickoff_ts_by_game
            .insert("g_future".to_string(), now + 600);
        engine.game_states.insert(
            "g_done".to_string(),
            GameState {
                match_completed: Some(true),
                ..Default::default()
            },
        );

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
        let mut engine = NativeMlbEngine::new(2.0, 0.5, 0.1, 5.0, 0.52, "FAK".to_string());
        let now = crate::dispatch::now_unix_s();
        engine.kickoff_ts_by_game.insert("g1".to_string(), now - 60);
        engine.token_ids_by_game.insert(
            "g1".to_string(),
            vec!["tok_1".to_string(), "tok_1".to_string()],
        );
        let cfg = RuntimeStartConfig {
            subscribe_lead_minutes: Some(5),
            ..RuntimeStartConfig::default()
        };
        let mut dispatch = DispatchRuntime::new(DispatchConfig::default(), None);
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
                &mut dispatch,
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
                &mut dispatch,
                &subscriptions,
                candidates.as_slice(),
                &mut active_subs,
            ))
            .expect("refresh should succeed");
        assert!(!unchanged);
    }
}
