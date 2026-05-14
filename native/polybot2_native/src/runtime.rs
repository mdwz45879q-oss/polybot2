use super::*;
use crate::dispatch::{
    run_submitter_async, warm_presign_startup_into, DispatchHandle, OrderSubmitter,
};
use crate::log_writer::LogWriter;
use std::thread;

fn install_rustls_provider_once() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Determine the sport league from the compiled plan JSON.
fn detect_league_from_plan(plan_json: &str) -> &'static str {
    // Quick scan for "league" key to avoid full parse.
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(plan_json) {
        if let Some(league) = val.get("league").and_then(|v| v.as_str()) {
            return match league {
                "epl" | "ucl" | "bundesliga" | "laliga" | "la_liga" | "ligue1" | "serie_a" => "soccer",
                _ => "baseball",
            };
        }
    }
    "baseball"
}

#[cfg(feature = "python-extension")]
#[pymethods]
impl NativeHotPathRuntime {
    #[new]
    fn new() -> Self {
        Self {
            engine: None,
            running: false,
            subscriptions: Vec::new(),
            runtime_cfg: RuntimeStartConfig::default(),
            dispatch_cfg: DispatchConfig::default(),
            presign_templates: Vec::new(),
            live_worker: None,
            submitter: None,
            cached_sdk_client: None,
            cached_signer: None,
        }
    }

    fn start(
        &mut self,
        py: Python<'_>,
        config_json: &str,
        compiled_plan_json: &str,
        exec_config_json: &str,
    ) -> PyResult<()> {
        self.stop();
        install_rustls_provider_once();
        let cfg: RuntimeStartConfig = serde_json::from_str(config_json)
            .map_err(|e| PyValueError::new_err(format!("invalid_runtime_config_json:{}", e)))?;
        let exec_cfg: ExecStartConfig = serde_json::from_str(exec_config_json)
            .map_err(|e| PyValueError::new_err(format!("invalid_exec_config_json:{}", e)))?;
        let worker_cfg = cfg.clone();
        self.runtime_cfg = cfg.clone();
        let runtime_amount = cfg.amount_usdc.unwrap_or(5.0);
        let runtime_size = cfg.size_shares.unwrap_or(5.0);
        let runtime_price = cfg.limit_price.unwrap_or(0.52);
        let runtime_tif = cfg
            .time_in_force
            .clone()
            .unwrap_or_else(|| "FAK".to_string());
        let dispatch_cfg = crate::dispatch::build_dispatch_config(
            exec_cfg,
            runtime_amount,
            runtime_size,
            runtime_price,
            runtime_tif.clone(),
        )
        .map_err(PyValueError::new_err)?;
        self.dispatch_cfg = dispatch_cfg.clone();

        // Determine sport from plan JSON and provider from config.
        let sport = detect_league_from_plan(compiled_plan_json);
        let provider = cfg.provider.clone().unwrap_or_default();

        // Create engine based on sport type.
        let engine: SportEngine = match sport {
            "soccer" => {
                let mut e = NativeSoccerEngine::new();
                e.load_plan_from_json(compiled_plan_json)
                    .map_err(|err| PyValueError::new_err(format!("soccer_load_plan:{}", err)))?;
                e.reset_runtime_state();
                SportEngine::Soccer(e)
            }
            _ => {
                let mut e = NativeMlbEngine::new();
                e.load_plan_from_json(compiled_plan_json)
                    .map_err(|err| PyValueError::new_err(format!("baseball_load_plan:{}", err)))?;
                e.reset_runtime_state();
                SportEngine::Baseball(e)
            }
        };
        self.engine = Some(engine);

        if cfg.live_enabled.unwrap_or(false) {
            let subscribe_lead_minutes = cfg.subscribe_lead_minutes.unwrap_or(90).max(0);
            let initial_candidates = self.subscriptions.clone();
            let initial_active_subscriptions = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .active_subscriptions_for_candidates(
                    initial_candidates.as_slice(),
                    crate::dispatch::now_unix_s(),
                    subscribe_lead_minutes,
                );
            let all_plan_tokens = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .all_token_ids();
            let subs = Arc::new(RwLock::new(initial_active_subscriptions.clone()));
            let health = Arc::new(Mutex::new(RuntimeHealth {
                running: true,
                reconnects: 0,
                last_error: String::new(),
            }));
            let (command_tx, command_rx) = flume::unbounded::<LiveWorkerCommand>();
            let (patch_tx, patch_rx) = flume::unbounded::<PatchPayload>();

            // Extract registry from engine. Both DispatchHandle and OrderSubmitter
            // share an Arc clone for read-only target/token resolution.
            let registry = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .clone_registry()
                .ok_or_else(|| PyValueError::new_err("registry_not_built_after_load_plan"))?;
            let shared_registry: crate::dispatch::SharedRegistry =
                Arc::new(arc_swap::ArcSwap::new(Arc::clone(&registry)));

            // Build the WS-thread half: presign pool, no submit_tx yet.
            let mut dispatch_handle = DispatchHandle::new(
                dispatch_cfg.clone(),
                Arc::clone(&registry),
                Arc::clone(&shared_registry),
            );
            dispatch_handle.set_presign_templates(self.presign_templates.as_slice());
            dispatch_handle.activate_presign_templates_for_tokens(all_plan_tokens.as_slice());

            // Create the structured log file and wrap it for cross-thread sharing.
            let run_id = cfg.run_id.unwrap_or(0);
            let log_dir = cfg.log_dir.clone().unwrap_or_else(|| ".".to_string());
            let log_ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
            let log_path = format!("{}/hotpath_{}_{}.jsonl", log_dir, run_id, log_ts);
            let log_writer = LogWriter::open(&log_path)
                .map_err(|e| PyValueError::new_err(format!("log_writer_open_failed:{}", e)))?;
            let log_arc = Arc::new(Mutex::new(log_writer));
            let dispatch_mode_label = dispatch_handle.mode_label();
            let games_count = self
                .engine
                .as_ref()
                .map(|e| e.token_ids_by_game_len())
                .unwrap_or(0);
            if let Ok(mut g) = log_arc.lock() {
                g.log_startup(
                    run_id,
                    games_count,
                    all_plan_tokens.len(),
                    dispatch_mode_label,
                );
            }

            // Build the submitter-thread half. In Http mode, initialize the SDK
            // runtime and run presign warmup using its client/signer references.
            let (submit_producer, submit_consumer) =
                rtrb::RingBuffer::<crate::dispatch::SubmitWork>::new(64);
            let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let submitter_log_arc = Arc::clone(&log_arc);
            let submitter_health = Arc::new(Mutex::new(SubmitterHealth::default()));
            let mut submitter = OrderSubmitter::new(
                dispatch_cfg.clone(),
                submitter_log_arc,
                submit_consumer,
                Arc::clone(&stop_flag),
                Arc::clone(&submitter_health),
                Arc::clone(&shared_registry),
            );

            let submitter_handle: Option<SubmitterHandle> =
                if matches!(dispatch_cfg.mode, DispatchMode::Http) {
                    if dispatch_cfg.presign_enabled && !all_plan_tokens.is_empty() {
                        let warm_result =
                            py.allow_threads(|| {
                                match TokioBuilder::new_multi_thread().enable_all().build() {
                                    Ok(rt) => rt.block_on(async {
                                        submitter.ensure_sdk_runtime_async().await?;
                                        let client = submitter.sdk_client_ref()?.clone();
                                        let signer = submitter.signer_ref()?.clone();
                                        let (templates, pool) =
                                            dispatch_handle.templates_and_pool_mut();
                                        warm_presign_startup_into(
                                            &dispatch_cfg,
                                            &client,
                                            &signer,
                                            templates,
                                            pool,
                                        )
                                        .await
                                    }),
                                    Err(e) => Err(format!("tokio_runtime_create_failed:{}", e)),
                                }
                            });
                        if let Err(err) = warm_result {
                            return Err(PyValueError::new_err(format!(
                                "presign_startup_warm_failed:{}",
                                err
                            )));
                        }
                        self.cached_sdk_client = submitter.sdk_client_ref().ok().map(|c| c.clone());
                        self.cached_signer = submitter.signer_ref().ok().map(|s| s.clone());
                    }

                    dispatch_handle.install_submit_tx(submit_producer);

                    let submit_join = thread::spawn(move || {
                        if let Ok(rt) = TokioBuilder::new_current_thread().enable_all().build() {
                            rt.block_on(run_submitter_async(submitter));
                        }
                    });

                    Some(SubmitterHandle {
                        stop_flag,
                        join: Some(submit_join),
                        health: submitter_health,
                    })
                } else {
                    // Noop mode: no submitter spawned. The send-channel sender is
                    // dropped; DispatchHandle::dispatch_intent short-circuits to
                    // log "noop" inline and never reaches the channel.
                    drop(submitter);
                    drop(submit_producer);
                    drop(submitter_health);
                    None
                };

            // Clone the engine for the worker thread.
            let worker_engine = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?;
            let mut worker_engine = match worker_engine {
                SportEngine::Baseball(e) => SportEngine::Baseball(e.clone()),
                SportEngine::Soccer(e) => SportEngine::Soccer(e.clone()),
            };
            let worker_dispatch_handle = dispatch_handle;
            let subs_clone = Arc::clone(&subs);
            let health_clone = Arc::clone(&health);
            let worker_log_arc = Arc::clone(&log_arc);

            // Spawn the appropriate WS worker based on provider.
            let join = match provider.as_str() {
                "boltodds" => {
                    let api_key = cfg.boltodds_api_key.clone().unwrap_or_default();
                    if api_key.is_empty() {
                        return Err(PyValueError::new_err(
                            "boltodds_api_key_required_for_boltodds_provider",
                        ));
                    }
                    let ws_url = cfg
                        .boltodds_ws_url
                        .clone()
                        .unwrap_or_else(|| "wss://spro.agency/api/livescores".to_string());
                    let bo_cfg = crate::ws_boltodds::BoltOddsWorkerConfig { ws_url, api_key };
                    // For BoltOdds, game_labels come from the engine's game_ids
                    // (which are universal_ids set at plan load time).
                    let game_labels: Vec<String> = worker_engine.game_ids().to_vec();

                    thread::spawn(move || {
                        if let Ok(runtime) = TokioBuilder::new_current_thread().enable_all().build()
                        {
                            runtime.block_on(crate::ws_boltodds::run_boltodds_worker_async(
                                &mut worker_engine,
                                bo_cfg,
                                worker_dispatch_handle,
                                subs_clone,
                                health_clone,
                                game_labels,
                                command_rx,
                                patch_rx,
                                worker_log_arc,
                            ));
                        } else {
                            crate::ws::with_health(&health_clone, |h| {
                                h.running = false;
                                h.last_error = "tokio_runtime_create_failed".to_string();
                            });
                        }
                    })
                }
                "kalstrop_v2" => {
                    let base_url = cfg.kalstrop_v2_base_url.clone()
                        .unwrap_or_else(|| "https://stats.kalstropservice.com".to_string());
                    let sio_path = cfg.kalstrop_v2_sio_path.clone()
                        .unwrap_or_else(|| "/socket.io".to_string());
                    let v2_client_id = cfg.kalstrop_client_id.clone().unwrap_or_default();
                    let v2_secret = cfg.kalstrop_shared_secret_raw.clone().unwrap_or_default();
                    let v2_cfg = crate::ws_kalstrop_v2::KalstropV2WorkerConfig {
                        base_url,
                        sio_path,
                        client_id: v2_client_id,
                        shared_secret_raw: v2_secret,
                    };
                    thread::spawn(move || {
                        if let Ok(runtime) = TokioBuilder::new_current_thread().enable_all().build()
                        {
                            runtime.block_on(crate::ws_kalstrop_v2::run_kalstrop_v2_worker_async(
                                &mut worker_engine,
                                v2_cfg,
                                worker_dispatch_handle,
                                subs_clone,
                                health_clone,
                                command_rx,
                                patch_rx,
                                worker_log_arc,
                            ));
                        } else {
                            crate::ws::with_health(&health_clone, |h| {
                                h.running = false;
                                h.last_error = "tokio_runtime_create_failed".to_string();
                            });
                        }
                    })
                }
                _ => {
                    // Default: Kalstrop V1 worker
                    thread::spawn(move || {
                        if let Ok(runtime) = TokioBuilder::new_current_thread().enable_all().build()
                        {
                            runtime.block_on(crate::ws::run_live_worker_async(
                                &mut worker_engine,
                                worker_cfg,
                                worker_dispatch_handle,
                                subs_clone,
                                health_clone,
                                initial_candidates,
                                initial_active_subscriptions,
                                command_rx,
                                patch_rx,
                                worker_log_arc,
                            ));
                        } else {
                            crate::ws::with_health(&health_clone, |h| {
                                h.running = false;
                                h.last_error = "tokio_runtime_create_failed".to_string();
                            });
                        }
                    })
                }
            };

            self.live_worker = Some(LiveWorkerHandle {
                command_tx,
                patch_tx,
                join: Some(join),
                subscriptions: subs,
                health,
            });
            self.submitter = submitter_handle;
        }

        self.running = true;
        Ok(())
    }

    fn stop(&mut self) {
        self.running = false;
        if let Some(worker) = self.live_worker.as_mut() {
            let _ = worker.command_tx.send(LiveWorkerCommand::Stop);
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
        self.live_worker = None;
        if let Some(sub) = self.submitter.as_mut() {
            sub.stop_flag
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(join) = sub.join.take() {
                let _ = join.join();
            }
        }
        self.submitter = None;
    }

    fn set_subscriptions(&mut self, subscriptions: Vec<String>) {
        self.subscriptions = subscriptions
            .into_iter()
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect();
        self.subscriptions.sort();
        self.subscriptions.dedup();
        if let Some(worker) = self.live_worker.as_ref() {
            let _ = worker
                .command_tx
                .send(LiveWorkerCommand::SetCandidateSubscriptions(
                    self.subscriptions.clone(),
                ));
        }
    }

    fn prewarm_presign(&mut self, template_orders_json: &str) -> PyResult<usize> {
        let templates: Vec<crate::dispatch::PresignTemplateData> =
            serde_json::from_str(template_orders_json)
                .map_err(|e| PyValueError::new_err(format!("invalid_prewarm_payload:{}", e)))?;
        self.presign_templates = templates;
        Ok(self.presign_templates.len())
    }

    fn patch_plan(
        &self,
        py: Python<'_>,
        plan_json: String,
        templates_json: String,
    ) -> PyResult<usize> {
        let worker = self
            .live_worker
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("patch_plan_requires_running_live_worker"))?;
        let patch_tx = worker.patch_tx.clone();
        let client = self
            .cached_sdk_client
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("patch_plan_requires_cached_sdk_client"))?
            .clone();
        let signer = self
            .cached_signer
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("patch_plan_requires_cached_signer"))?
            .clone();
        let dispatch_cfg = self.dispatch_cfg.clone();

        let templates: Vec<crate::dispatch::PresignTemplateData> =
            serde_json::from_str(&templates_json).map_err(|e| {
                PyValueError::new_err(format!("patch_plan_invalid_templates:{}", e))
            })?;
        let mut template_map: std::collections::HashMap<String, smallvec::SmallVec<[crate::dispatch::OrderRequestData; 2]>> =
            std::collections::HashMap::new();
        for tpl in &templates {
            if let Some(req) = crate::dispatch::DispatchHandle::parse_template_request(tpl) {
                template_map.entry(req.token_id.clone())
                    .or_insert_with(smallvec::SmallVec::new)
                    .push(req);
            }
        }

        let plan_json_owned = plan_json;
        let template_map_clone = template_map.clone();

        let sign_result = py
            .allow_threads(
                move || -> Result<std::collections::HashMap<String, smallvec::SmallVec<[SdkSignedOrder; 2]>>, String> {
                    if !dispatch_cfg.presign_enabled || template_map_clone.is_empty() {
                        return Ok(std::collections::HashMap::new());
                    }
                    let rt = TokioBuilder::new_multi_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| format!("patch_tokio_rt:{}", e))?;
                    rt.block_on(async {
                        // Flatten: sign each template independently (primary + optional secondary).
                        let mut work_items: Vec<(String, crate::dispatch::OrderRequestData)> = Vec::new();
                        for (token_id, requests) in template_map_clone {
                            for req in requests {
                                work_items.push((token_id.clone(), req));
                            }
                        }
                        let handles: Vec<_> = work_items
                            .into_iter()
                            .map(|(token_id, request)| {
                                let c = client.clone();
                                let s = signer.clone();
                                tokio::spawn(async move {
                                    let result = crate::dispatch::sdk_exec::sign_order_batch(
                                        &c, &s, &request, 1,
                                    )
                                    .await;
                                    (token_id, result)
                                })
                            })
                            .collect();
                        let mut signed: std::collections::HashMap<String, smallvec::SmallVec<[SdkSignedOrder; 2]>> =
                            std::collections::HashMap::new();
                        for handle in handles {
                            let (token_id, result) =
                                handle.await.map_err(|e| format!("patch_sign_task:{}", e))?;
                            let orders =
                                result.map_err(|e| format!("patch_sign:{}:{}", token_id, e))?;
                            if let Some(order) = orders.into_iter().next() {
                                signed.entry(token_id)
                                    .or_insert_with(smallvec::SmallVec::new)
                                    .push(order);
                            }
                        }
                        Ok(signed)
                    })
                },
            )
            .map_err(|e| PyValueError::new_err(format!("patch_plan_sign_error:{}", e)))?;

        let new_count = sign_result.len();
        let payload = PatchPayload {
            plan_json: plan_json_owned,
            new_presigned: sign_result,
            new_templates: template_map,
        };
        patch_tx
            .send(payload)
            .map_err(|_| PyValueError::new_err("patch_plan_channel_closed"))?;

        Ok(new_count)
    }

    fn health_snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let mut running = self.running;
        let mut reconnects = 0i64;
        let mut last_error = String::new();
        let mut subscriptions = self.subscriptions.clone();
        if let Some(worker) = self.live_worker.as_ref() {
            if let Ok(health) = worker.health.lock() {
                running = health.running;
                reconnects = health.reconnects;
                last_error = health.last_error.clone();
            }
            if let Ok(subs) = worker.subscriptions.read() {
                subscriptions = subs.clone();
            }
        }
        let mut submitter_present = false;
        let mut submitter_running = false;
        let mut submitter_last_error = String::new();
        if let Some(sub) = self.submitter.as_ref() {
            submitter_present = true;
            if let Ok(h) = sub.health.lock() {
                submitter_running = h.running;
                submitter_last_error = h.last_error.clone();
            }
        }
        let payload = json!({
            "running": running,
            "subscriptions": subscriptions,
            "reconnects": reconnects,
            "last_error": last_error,
            "submitter": {
                "present": submitter_present,
                "running": submitter_running,
                "last_error": submitter_last_error,
            },
        });
        crate::baseball::engine::serde_value_to_py(py, &payload)
    }
}
