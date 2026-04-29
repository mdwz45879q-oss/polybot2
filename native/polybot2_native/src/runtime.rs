use super::*;
use crate::dispatch::{
    run_submitter_async, warm_presign_startup_into, DispatchHandle, OrderSubmitter,
};
use crate::log_writer::LogWriter;
use std::thread;

fn install_rustls_provider_once() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

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

        let mut engine = NativeMlbEngine::new(
            cfg.dedup_ttl_seconds.unwrap_or(2.0),
            cfg.decision_cooldown_seconds.unwrap_or(0.5),
            cfg.decision_debounce_seconds.unwrap_or(0.1),
        );
        let json_mod = py.import_bound("json")?;
        let plan_any = json_mod.call_method1("loads", (compiled_plan_json,))?;
        let plan_dict = plan_any.downcast::<PyDict>()?;
        engine.load_plan(plan_dict)?;
        engine.reset_runtime_state();
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
            let (command_tx, command_rx) = tokio_mpsc::unbounded_channel::<LiveWorkerCommand>();

            // Extract registry from engine. Both DispatchHandle and OrderSubmitter
            // share an Arc clone for read-only target/token resolution.
            let registry = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .clone_registry()
                .ok_or_else(|| {
                    PyValueError::new_err("registry_not_built_after_load_plan")
                })?;

            // Build the WS-thread half: presign pool, no submit_tx yet.
            let mut dispatch_handle =
                DispatchHandle::new(dispatch_cfg.clone(), Arc::clone(&registry));
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
                .map(|e| e.token_ids_by_game.len())
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
            let (submit_tx, submit_rx) =
                tokio_mpsc::unbounded_channel::<crate::dispatch::SubmitWork>();
            let submitter_log_arc = Arc::clone(&log_arc);
            let submitter_health = Arc::new(Mutex::new(SubmitterHealth::default()));
            let mut submitter = OrderSubmitter::new(
                dispatch_cfg.clone(),
                submitter_log_arc,
                submit_rx,
                Arc::clone(&submitter_health),
                Arc::clone(&registry),
            );

            let submitter_handle: Option<SubmitterHandle> = if matches!(
                dispatch_cfg.mode,
                DispatchMode::Http
            ) {
                if dispatch_cfg.presign_enabled && !all_plan_tokens.is_empty() {
                    let warm_result = py.allow_threads(|| {
                        match TokioBuilder::new_multi_thread().enable_all().build() {
                            Ok(rt) => rt.block_on(async {
                                submitter.ensure_sdk_runtime_async().await?;
                                let client = submitter.sdk_client_ref()?.clone();
                                let signer = submitter.signer_ref()?.clone();
                                let (templates, pool) = dispatch_handle.templates_and_pool_mut();
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
                }

                dispatch_handle.install_submit_tx(submit_tx.clone());

                let submit_join = thread::spawn(move || {
                    if let Ok(rt) = TokioBuilder::new_current_thread().enable_all().build() {
                        rt.block_on(run_submitter_async(submitter));
                    }
                });

                Some(SubmitterHandle {
                    submit_tx,
                    join: Some(submit_join),
                    health: submitter_health,
                })
            } else {
                // Noop mode: no submitter spawned. The send-channel sender is
                // dropped; DispatchHandle::dispatch_intent short-circuits to
                // log "noop" inline and never reaches the channel.
                drop(submitter);
                drop(submit_tx);
                drop(submitter_health);
                None
            };

            let mut worker_engine = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .clone();
            let worker_dispatch_handle = dispatch_handle;
            let subs_clone = Arc::clone(&subs);
            let health_clone = Arc::clone(&health);
            let worker_log_arc = Arc::clone(&log_arc);

            let join = thread::spawn(move || {
                if let Ok(runtime) = TokioBuilder::new_current_thread().enable_all().build() {
                    runtime.block_on(crate::ws::run_live_worker_async(
                        &mut worker_engine,
                        worker_cfg,
                        worker_dispatch_handle,
                        subs_clone,
                        health_clone,
                        initial_candidates,
                        initial_active_subscriptions,
                        command_rx,
                        worker_log_arc,
                    ));
                } else {
                    crate::ws::with_health(&health_clone, |h| {
                        h.running = false;
                        h.last_error = "tokio_runtime_create_failed".to_string();
                    });
                }
            });

            self.live_worker = Some(LiveWorkerHandle {
                command_tx,
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
            let _ = sub.submit_tx.send(crate::dispatch::SubmitWork::Stop);
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
        let mut posted_ok = 0i64;
        let mut posted_err = 0i64;
        if let Some(sub) = self.submitter.as_ref() {
            submitter_present = true;
            if let Ok(h) = sub.health.lock() {
                submitter_running = h.running;
                submitter_last_error = h.last_error.clone();
                posted_ok = h.posted_ok;
                posted_err = h.posted_err;
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
                "posted_ok": posted_ok,
                "posted_err": posted_err,
            },
        });
        crate::engine::serde_value_to_py(py, &payload)
    }
}
