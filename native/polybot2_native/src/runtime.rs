use super::*;
use crate::dispatch::DispatchRuntime;
use crate::telemetry::build_telemetry;
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
        let dispatch_cfg =
            crate::dispatch::build_dispatch_config(exec_cfg).map_err(PyValueError::new_err)?;
        self.dispatch_cfg = dispatch_cfg.clone();
        let runtime_tif = cfg
            .time_in_force
            .clone()
            .unwrap_or_else(|| "FAK".to_string());
        crate::dispatch::map_sdk_order_type(runtime_tif.trim())
            .map_err(|e| PyValueError::new_err(format!("runtime_time_in_force_invalid:{}", e)))?;

        let mut engine = NativeMlbEngine::new(
            cfg.dedup_ttl_seconds.unwrap_or(2.0),
            cfg.decision_cooldown_seconds.unwrap_or(0.5),
            cfg.decision_debounce_seconds.unwrap_or(0.1),
            cfg.amount_usdc.unwrap_or(5.0),
            cfg.size_shares.unwrap_or(5.0),
            cfg.limit_price.unwrap_or(0.52),
            runtime_tif,
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
            let initial_active_tokens = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .active_token_ids_for_games(initial_active_subscriptions.as_slice());
            let subs = Arc::new(RwLock::new(initial_active_subscriptions.clone()));
            let health = Arc::new(Mutex::new(RuntimeHealth {
                running: true,
                reconnects: 0,
                last_error: String::new(),
            }));
            let (command_tx, command_rx) = tokio_mpsc::unbounded_channel::<LiveWorkerCommand>();
            let provider = cfg
                .provider
                .clone()
                .unwrap_or_else(|| "kalstrop".to_string());
            let league = cfg.league.clone().unwrap_or_else(|| "mlb".to_string());
            let (telemetry_emitter, telemetry_worker) =
                build_telemetry(provider.as_str(), league.as_str());
            let mut dispatch_runtime =
                DispatchRuntime::new(dispatch_cfg.clone(), telemetry_emitter.clone());
            dispatch_runtime.set_presign_templates(self.presign_templates.as_slice());
            dispatch_runtime
                .activate_presign_templates_for_tokens(initial_active_tokens.as_slice());
            if dispatch_cfg.presign_enabled && !initial_active_subscriptions.is_empty() {
                let warm_result = match TokioBuilder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt.block_on(dispatch_runtime.warm_presign_startup_async()),
                    Err(e) => Err(format!("tokio_runtime_create_failed:{}", e)),
                };
                if let Err(err) = warm_result {
                    return Err(PyValueError::new_err(format!(
                        "presign_startup_warm_failed:{}",
                        err
                    )));
                }
            }

            let mut worker_engine = self
                .engine
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("engine unavailable"))?
                .clone();
            let worker_dispatch_runtime = dispatch_runtime;
            let subs_clone = Arc::clone(&subs);
            let health_clone = Arc::clone(&health);
            let join = thread::spawn(move || {
                if let Ok(runtime) = TokioBuilder::new_current_thread().enable_all().build() {
                    runtime.block_on(crate::ws::run_live_worker_async(
                        &mut worker_engine,
                        worker_cfg,
                        worker_dispatch_runtime,
                        telemetry_emitter,
                        subs_clone,
                        health_clone,
                        initial_candidates,
                        initial_active_subscriptions,
                        command_rx,
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
                telemetry_worker,
            });
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
            if let Some(t) = worker.telemetry_worker.as_mut() {
                t.shutdown();
            }
        }
        self.live_worker = None;
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
        let mut telemetry_emitted = 0u64;
        let mut telemetry_dropped = 0u64;
        if let Some(worker) = self.live_worker.as_ref() {
            if let Ok(health) = worker.health.lock() {
                running = health.running;
                reconnects = health.reconnects;
                last_error = health.last_error.clone();
            }
            if let Ok(subs) = worker.subscriptions.read() {
                subscriptions = subs.clone();
            }
            if let Some(t) = worker.telemetry_worker.as_ref() {
                telemetry_emitted = t.emitted();
                telemetry_dropped = t.dropped();
            }
        }
        let payload = json!({
            "running": running,
            "subscriptions": subscriptions,
            "reconnects": reconnects,
            "last_error": last_error,
            "telemetry_emitted": telemetry_emitted as i64,
            "telemetry_dropped": telemetry_dropped as i64,
        });
        crate::engine::serde_value_to_py(py, &payload)
    }
}
