use super::*;

impl DispatchRuntime {
    pub(super) fn build_presign_key(&self, request: &OrderRequestData) -> PreSignKey {
        PreSignKey {
            token_id: request.token_id.trim().to_string(),
        }
    }

    fn presign_key_for_token(token_id: &str) -> PreSignKey {
        PreSignKey {
            token_id: token_id.trim().to_string(),
        }
    }

    fn presign_target_depth(&self) -> usize {
        self.cfg.presign_pool_target_per_key.max(0) as usize
    }

    fn presign_low_watermark(&self) -> usize {
        let target = self.presign_target_depth();
        if target <= 1 {
            0
        } else {
            (target / 2).max(1)
        }
    }

    fn presign_depth(&self, key: &PreSignKey) -> usize {
        self.presign_pool.get(key).map(|q| q.len()).unwrap_or(0)
    }

    fn parse_template_request(template: &PresignTemplateData) -> Option<OrderRequestData> {
        let token_id = template.token_id.trim().to_string();
        if token_id.is_empty() {
            return None;
        }
        let notional_usdc = template.notional_usdc.unwrap_or(0.0);
        let limit_price = template.limit_price.unwrap_or(0.0);
        if notional_usdc <= 0.0 || limit_price <= 0.0 {
            return None;
        }
        Some(OrderRequestData {
            token_id,
            side: normalize_side(template.side.as_deref().unwrap_or("buy_yes")),
            notional_usdc,
            limit_price,
            time_in_force: normalize_tif(template.time_in_force.as_deref().unwrap_or("FAK")),
            client_order_id: "hp_template".to_string(),
        })
    }

    pub(crate) fn set_presign_templates(&mut self, templates: &[PresignTemplateData]) {
        self.presign_template_catalog.clear();
        self.presign_templates.clear();
        self.presign_pool.clear();
        self.last_refill_ns_by_key.clear();
        self.pending_refill_by_key.clear();
        for template in templates {
            let Some(mut request) = Self::parse_template_request(template) else {
                continue;
            };
            request.time_in_force = "FAK".to_string();
            let key = Self::presign_key_for_token(request.token_id.as_str());
            self.presign_template_catalog.insert(key, request);
        }
    }

    pub(crate) fn activate_presign_templates_for_tokens(&mut self, token_ids: &[String]) -> usize {
        let mut next_active: HashMap<PreSignKey, OrderRequestData> = HashMap::new();
        for token_id in token_ids {
            let key = Self::presign_key_for_token(token_id.as_str());
            if let Some(request) = self.presign_template_catalog.get(&key) {
                next_active.insert(key, request.clone());
            }
        }
        self.presign_templates = next_active;
        self.presign_pool
            .retain(|k, _| self.presign_templates.contains_key(k));
        self.last_refill_ns_by_key
            .retain(|k, _| self.presign_templates.contains_key(k));
        self.pending_refill_by_key.clear();
        for key in self.presign_templates.keys() {
            self.pending_refill_by_key.insert(key.clone(), ());
        }
        self.presign_templates.len()
    }

    pub(crate) async fn warm_presign_startup_async(&mut self) -> Result<(), String> {
        if !self.cfg.presign_enabled || self.presign_target_depth() == 0 {
            return Ok(());
        }
        let keys = self.presign_templates.keys().cloned().collect::<Vec<_>>();
        if keys.is_empty() {
            return Err("presign_startup_warm_no_templates".to_string());
        }
        let target = self.presign_target_depth();

        self.ensure_sdk_runtime().await?;
        let client = self.sdk_client_ref()?.clone();
        let signer = self.signer_ref()?.clone();

        let mut key_work: Vec<(PreSignKey, OrderRequestData, usize)> = Vec::new();
        for key in &keys {
            let current = self.presign_depth(key);
            let want = target.saturating_sub(current);
            if want == 0 {
                continue;
            }
            let template = self
                .presign_templates
                .get(key)
                .ok_or_else(|| {
                    format!(
                        "presign_template_missing:{}",
                        redact_token_id(key.token_id.as_str())
                    )
                })?
                .clone();
            key_work.push((key.clone(), template, want));
        }
        if key_work.is_empty() {
            return Ok(());
        }

        let per_key_s = 1.0_f64;
        let base_s = self.cfg.presign_startup_warm_timeout_seconds.max(0.1);
        let total_timeout_s = base_s + per_key_s * key_work.len() as f64;
        let timeout = Duration::from_secs_f64(total_timeout_s);

        let handles: Vec<_> = key_work
            .into_iter()
            .map(|(key, template, want)| {
                let c = client.clone();
                let s = signer.clone();
                tokio::spawn(async move {
                    let result =
                        super::sdk_exec::sign_order_batch(&c, &s, &template, want).await;
                    (key, result)
                })
            })
            .collect();

        let results = tokio::time::timeout(timeout, futures_util::future::join_all(handles))
            .await
            .map_err(|_| {
                let detail = keys
                    .iter()
                    .map(|k| {
                        format!(
                            "{}:{}",
                            redact_token_id(k.token_id.as_str()),
                            self.presign_depth(k)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "presign_startup_warm_timeout:target_depth={} timeout_s={:.3} keys={} detail={}",
                    target, total_timeout_s, keys.len(), detail
                )
            })?;

        let now_ns = now_unix_ns();
        for result in results {
            let (key, batch_result) =
                result.map_err(|e| format!("presign_task_panicked:{}", e))?;
            let signed_orders = batch_result?;
            let q = self.presign_pool.entry(key.clone()).or_default();
            for signed in signed_orders {
                q.push_back(PreSignedOrderData {
                    signed_order: signed,
                });
            }
            self.last_refill_ns_by_key.insert(key.clone(), now_ns);
            self.pending_refill_by_key.remove(&key);
        }

        for key in &keys {
            if self.presign_depth(key) < target {
                return Err(format!(
                    "presign_startup_warm_incomplete:{}:depth={}/{}",
                    redact_token_id(key.token_id.as_str()),
                    self.presign_depth(key),
                    target
                ));
            }
        }

        Ok(())
    }

    async fn refill_presign_key_async(
        &mut self,
        key: &PreSignKey,
        force: bool,
    ) -> Result<(), String> {
        if !self.cfg.presign_enabled || self.presign_target_depth() == 0 {
            return Ok(());
        }
        let target = self.presign_target_depth();
        let current_depth = self.presign_depth(key);
        if current_depth >= target {
            self.pending_refill_by_key.remove(key);
            return Ok(());
        }
        let last_refill_ns = *self.last_refill_ns_by_key.get(key).unwrap_or(&0);
        let refill_interval_ns =
            (self.cfg.presign_refill_interval_seconds.max(0.001) * 1_000_000_000.0) as i64;
        let now_ns = now_unix_ns();
        if !force && current_depth > 0 && now_ns.saturating_sub(last_refill_ns) < refill_interval_ns
        {
            return Ok(());
        }
        let template = match self.presign_templates.get(key) {
            Some(v) => v.clone(),
            None => {
                self.pending_refill_by_key.remove(key);
                self.presign_pool.remove(key);
                self.last_refill_ns_by_key.remove(key);
                return Err(format!(
                    "presign_template_missing:{}",
                    redact_token_id(key.token_id.as_str())
                ));
            }
        };
        let batch = self.cfg.presign_refill_batch_size.max(1) as usize;
        let want = target.saturating_sub(current_depth).min(batch);
        let mut built: Vec<PreSignedOrderData> = Vec::with_capacity(want);
        for _ in 0..want {
            let signed_order = self.build_signed_order_async(&template).await?;
            built.push(PreSignedOrderData { signed_order });
        }
        let q = self.presign_pool.entry(key.clone()).or_default();
        for item in built {
            q.push_back(item);
        }
        self.last_refill_ns_by_key.insert(key.clone(), now_ns);
        if self.presign_depth(key) >= target {
            self.pending_refill_by_key.remove(key);
        } else {
            self.pending_refill_by_key.insert(key.clone(), ());
        }
        Ok(())
    }

    pub(crate) async fn refill_presign_tick_async(&mut self) {
        if !self.cfg.presign_enabled || self.presign_target_depth() == 0 {
            return;
        }
        if self.pending_refill_by_key.is_empty() {
            let low_watermark = self.presign_low_watermark();
            for key in self.presign_templates.keys() {
                if self.presign_depth(key) <= low_watermark {
                    self.pending_refill_by_key.insert(key.clone(), ());
                }
            }
        }
        if self.pending_refill_by_key.is_empty() {
            return;
        }
        let keys = self.pending_refill_by_key.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let _ = self.refill_presign_key_async(&key, false).await;
        }
    }

    pub(super) fn pop_presigned_order(&mut self, key: &PreSignKey) -> Option<PreSignedOrderData> {
        let q = self.presign_pool.get_mut(key)?;
        let out = q.pop_front();
        if q.is_empty() {
            self.presign_pool.remove(key);
        }
        out
    }

    pub(super) fn schedule_refill_if_needed(&mut self, key: &PreSignKey) {
        if self.presign_depth(key) <= self.presign_low_watermark() {
            self.pending_refill_by_key.insert(key.clone(), ());
        }
    }
}
