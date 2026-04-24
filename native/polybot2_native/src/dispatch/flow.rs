use super::*;

impl DispatchRuntime {
    pub(crate) fn dispatch_mode_label(&self) -> &'static str {
        if matches!(self.cfg.mode, DispatchMode::Http) {
            "http"
        } else {
            "noop"
        }
    }

    fn build_request_from_intent(&self, intent: &Intent) -> OrderRequestData {
        let tif = normalize_tif(intent.time_in_force.as_str());
        let expiration_ts = if tif == "GTD" && self.cfg.gtd_expiration_seconds > 0 {
            Some(now_unix_s() + self.cfg.gtd_expiration_seconds)
        } else {
            None
        };
        OrderRequestData {
            token_id: intent.token_id.clone(),
            side: normalize_side(intent.side.as_str()),
            amount_usdc: intent.amount_usdc.max(0.0),
            limit_price: intent.limit_price.max(0.0),
            time_in_force: tif,
            client_order_id: new_client_order_id(),
            size_shares: intent.size_shares.max(0.0),
            expiration_ts,
        }
    }

    fn mark_active_state(
        &mut self,
        strategy_key: &str,
        state: &OrderStateData,
        source_universal_id: &str,
        chain_id: &str,
    ) {
        if strategy_key.trim().is_empty() {
            return;
        }
        if state.is_terminal() {
            self.active_orders_by_strategy.remove(strategy_key);
            return;
        }
        self.active_orders_by_strategy.insert(
            strategy_key.to_string(),
            ActiveOrderRef {
                client_order_id: state.client_order_id.clone(),
                exchange_order_id: state.exchange_order_id.clone(),
                status: state.status.clone(),
                source_universal_id: source_universal_id.to_string(),
                chain_id: chain_id.to_string(),
                inserted_ns: now_unix_ns(),
            },
        );
    }

    pub(super) async fn submit_with_policy_async(
        &mut self,
        request: &OrderRequestData,
    ) -> Result<OrderStateData, String> {
        if self.cfg.presign_enabled && self.cfg.presign_pool_target_per_key > 0 {
            let key = self.build_presign_key(request);
            let presigned = self.pop_presigned_order(&key).ok_or_else(|| {
                format!(
                    "submit_presigned_miss:token_id={}",
                    redact_token_id(key.token_id.as_str())
                )
            })?;
            return self
                .post_signed_order_async(presigned.signed_order, request)
                .await;
        }
        self.submit_order_async(request).await
    }

    fn dispatch_intent_noop(&mut self, intent: &Intent) -> Result<(), String> {
        let strategy_key = intent.strategy_key.trim();
        if strategy_key.is_empty() {
            return Ok(());
        }
        let request = self.build_request_from_intent(intent);
        self.enforce_time_in_force_policy(&request)?;
        let state = OrderStateData {
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: format!("noop_{}", next_suffix()),
            side: request.side.clone(),
            requested_amount_usdc: request.amount_usdc,
            filled_amount_usdc: 0.0,
            limit_price: request.limit_price,
            time_in_force: request.time_in_force.clone(),
            status: "submitted".to_string(),
            reason: "ok".to_string(),
            error_code: String::new(),

        };
        self.emit_event_lazy(
            "order_submit_called",
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
            strategy_key,
            request.client_order_id.as_str(),
            "",
            intent.reason.as_str(),
            || {
                json!({
                    "dispatch_mode": "noop",
                    "token_id": request.token_id,
                    "side": request.side,
                    "time_in_force": request.time_in_force,
                    "limit_price": request.limit_price,
                    "amount_usdc": request.amount_usdc,
                    "market_type": intent.market_type,
                    "outcome_semantic": intent.outcome_semantic,
                    "phase": "submit",
                })
            },
        );
        self.emit_event_lazy(
            "order_submit_ok",
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
            strategy_key,
            state.client_order_id.as_str(),
            state.exchange_order_id.as_str(),
            "ok",
            || {
                json!({
                    "dispatch_mode": "noop",
                    "status": state.status,
                    "requested_amount_usdc": state.requested_amount_usdc,
                    "filled_amount_usdc": state.filled_amount_usdc,
                })
            },
        );
        self.emit_lifecycle_transition(
            strategy_key,
            "",
            &state,
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
        );
        self.mark_active_state(
            strategy_key,
            &state,
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
        );
        Ok(())
    }

    fn enforce_time_in_force_policy(&self, request: &OrderRequestData) -> Result<(), String> {
        map_sdk_order_type(request.time_in_force.as_str())
            .map_err(|e| format!("dispatch_tif_invalid:{}", e))?;
        Ok(())
    }

    async fn dispatch_intent_http_async(&mut self, intent: &Intent) -> Result<(), String> {
        let strategy_key = intent.strategy_key.trim().to_string();
        let request = self.build_request_from_intent(intent);
        self.enforce_time_in_force_policy(&request)?;

        let state = match self.submit_with_policy_async(&request).await {
            Ok(s) => s,
            Err(err) => {
                self.emit_event_lazy(
                    "order_submit_called",
                    intent.source_universal_id.as_str(),
                    intent.chain_id.as_str(),
                    strategy_key.as_str(),
                    request.client_order_id.as_str(),
                    "",
                    intent.reason.as_str(),
                    || {
                        json!({
                            "dispatch_mode": "http",
                            "token_id": request.token_id,
                            "side": request.side,
                            "time_in_force": request.time_in_force,
                            "limit_price": request.limit_price,
                            "amount_usdc": request.amount_usdc,
                            "market_type": intent.market_type,
                            "outcome_semantic": intent.outcome_semantic,
                            "phase": "submit",
                        })
                    },
                );
                self.emit_event_lazy(
                    "order_submit_failed",
                    intent.source_universal_id.as_str(),
                    intent.chain_id.as_str(),
                    strategy_key.as_str(),
                    request.client_order_id.as_str(),
                    "",
                    err.as_str(),
                    || {
                        json!({
                            "dispatch_mode": "http",
                            "token_id": request.token_id,
                            "side": request.side,
                            "time_in_force": request.time_in_force,
                            "limit_price": request.limit_price,
                            "amount_usdc": request.amount_usdc,
                            "market_type": intent.market_type,
                            "outcome_semantic": intent.outcome_semantic,
                            "phase": "submit",
                        })
                    },
                );
                return Err(err);
            }
        };
        self.emit_event_lazy(
            "order_submit_called",
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
            strategy_key.as_str(),
            request.client_order_id.as_str(),
            "",
            intent.reason.as_str(),
            || {
                json!({
                    "dispatch_mode": "http",
                    "token_id": request.token_id,
                    "side": request.side,
                    "time_in_force": request.time_in_force,
                    "limit_price": request.limit_price,
                    "amount_usdc": request.amount_usdc,
                    "market_type": intent.market_type,
                    "outcome_semantic": intent.outcome_semantic,
                    "phase": "submit",
                })
            },
        );
        self.emit_event_lazy(
            "order_submit_ok",
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
            strategy_key.as_str(),
            state.client_order_id.as_str(),
            state.exchange_order_id.as_str(),
            state.reason.as_str(),
            || {
                json!({
                    "status": state.status,
                    "requested_amount_usdc": state.requested_amount_usdc,
                    "filled_amount_usdc": state.filled_amount_usdc,
                })
            },
        );
        self.emit_lifecycle_transition(
            strategy_key.as_str(),
            "",
            &state,
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
        );
        self.mark_active_state(
            strategy_key.as_str(),
            &state,
            intent.source_universal_id.as_str(),
            intent.chain_id.as_str(),
        );
        Ok(())
    }

    pub(crate) async fn dispatch_intent_async(&mut self, intent: &Intent) -> Result<(), String> {
        if matches!(self.cfg.mode, DispatchMode::Noop) {
            return self.dispatch_intent_noop(intent);
        }
        self.dispatch_intent_http_async(intent).await
    }

    pub(crate) fn active_order_refresh_interval_seconds(&self) -> f64 {
        self.cfg.active_order_refresh_interval_seconds.max(0.0)
    }

    pub(crate) async fn refresh_active_state_from_broker_async(&mut self) {
        self.evict_stale_active_orders();
        if !matches!(self.cfg.mode, DispatchMode::Http) {
            return;
        }
        let keys = self
            .active_orders_by_strategy
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        for (strategy_key, active_ref) in keys {
            let exchange_id = active_ref.exchange_order_id.clone();
            if exchange_id.trim().is_empty() {
                continue;
            }
            match self.get_order_async(exchange_id.as_str()).await {
                Ok(state) => {
                    self.broker_failure_count.remove(&strategy_key);
                    self.emit_lifecycle_transition(
                        strategy_key.as_str(),
                        active_ref.status.as_str(),
                        &state,
                        active_ref.source_universal_id.as_str(),
                        active_ref.chain_id.as_str(),
                    );
                    self.mark_active_state(
                        strategy_key.as_str(),
                        &state,
                        active_ref.source_universal_id.as_str(),
                        active_ref.chain_id.as_str(),
                    );
                }
                Err(err) => {
                    let count = self.broker_failure_count.entry(strategy_key.clone()).or_insert(0);
                    *count += 1;
                    let failures = *count;
                    self.emit_event(
                        "exec_error",
                        active_ref.source_universal_id.as_str(),
                        active_ref.chain_id.as_str(),
                        strategy_key.as_str(),
                        active_ref.client_order_id.as_str(),
                        exchange_id.as_str(),
                        "broker_query_failed",
                        json!({
                            "error": err,
                            "consecutive_failures": failures,
                        }),
                    );
                }
            }
        }
    }

    pub(crate) fn evict_stale_active_orders(&mut self) {
        const STALE_ACTIVE_ORDER_TTL_NS: i64 = 60_000_000_000;
        let evict_cutoff = now_unix_ns().saturating_sub(STALE_ACTIVE_ORDER_TTL_NS);
        let stale: Vec<_> = self
            .active_orders_by_strategy
            .iter()
            .filter(|(_, r)| r.inserted_ns < evict_cutoff)
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        for (key, r) in &stale {
            self.active_orders_by_strategy.remove(key);
            self.emit_event(
                "order_failed",
                r.source_universal_id.as_str(),
                r.chain_id.as_str(),
                key.as_str(),
                r.client_order_id.as_str(),
                r.exchange_order_id.as_str(),
                "stale_active_order_evicted",
                json!({ "age_ns": now_unix_ns().saturating_sub(r.inserted_ns) }),
            );
        }
    }
}
