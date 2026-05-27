use super::*;

impl DispatchHandle {
    pub(crate) fn new(
        cfg: DispatchConfig,
        registry: Arc<crate::TargetRegistry>,
        shared_registry: SharedRegistry,
    ) -> Self {
        let n = registry.tokens.len();
        Self {
            cfg,
            registry,
            shared_registry,
            presign_template_catalog: HashMap::new(),
            presign_templates: (0..n).map(|_| smallvec::SmallVec::new()).collect(),
            presign_pool: (0..n).map(|_| smallvec::SmallVec::new()).collect(),
            submit_tx: None,
        }
    }

    pub(crate) fn install_submit_tx(&mut self, tx: rtrb::Producer<SubmitWork>) {
        self.submit_tx = Some(tx);
    }

    pub(crate) fn templates_and_pool_mut(
        &mut self,
    ) -> (
        &[smallvec::SmallVec<[OrderRequestData; 2]>],
        &mut [smallvec::SmallVec<[Box<PreparedOrderPayload>; 2]>],
    ) {
        (
            self.presign_templates.as_slice(),
            self.presign_pool.as_mut_slice(),
        )
    }

    pub(crate) fn parse_template_request(
        template: &PresignTemplateData,
    ) -> Option<OrderRequestData> {
        let token_id = template.token_id.trim().to_string();
        if token_id.is_empty() {
            return None;
        }
        let amount_usdc = template.amount_usdc.unwrap_or(0.0);
        let limit_price = template.limit_price.unwrap_or(0.0);
        if amount_usdc <= 0.0 || limit_price <= 0.0 {
            return None;
        }
        let price = limit_price.max(0.001);
        Some(OrderRequestData {
            token_id,
            side: normalize_side(template.side.as_deref().unwrap_or("buy_yes")),
            amount_usdc,
            limit_price,
            time_in_force: parse_time_in_force(template.time_in_force.as_deref().unwrap_or("FAK"))
                .unwrap_or(OrderTimeInForce::FAK),
            size_shares: template
                .size_shares
                .filter(|v| *v > 0.0)
                .unwrap_or(amount_usdc / price),
        })
    }

    pub(crate) fn set_presign_templates(&mut self, templates: &[PresignTemplateData]) {
        self.presign_template_catalog.clear();
        for slot in self.presign_templates.iter_mut() {
            slot.clear();
        }
        for slot in self.presign_pool.iter_mut() {
            slot.clear();
        }
        for template in templates {
            let Some(request) = Self::parse_template_request(template) else {
                continue;
            };
            self.presign_template_catalog
                .entry(request.token_id.clone())
                .or_insert_with(smallvec::SmallVec::new)
                .push(request);
        }
    }

    pub(crate) fn activate_presign_templates_for_tokens(&mut self, _token_ids: &[String]) -> usize {
        let mut active = 0usize;
        for (idx, slot) in self.registry.tokens.iter().enumerate() {
            let trimmed = slot.token_id.trim();
            if let Some(templates) = self.presign_template_catalog.get(trimmed) {
                self.presign_templates[idx] = templates.clone();
                active += 1;
            } else {
                self.presign_templates[idx].clear();
                self.presign_pool[idx].clear();
            }
        }
        active
    }

    pub(crate) fn extend_for_patch(
        &mut self,
        new_templates: &mut std::collections::HashMap<String, smallvec::SmallVec<[OrderRequestData; 2]>>,
        new_presigned: &mut std::collections::HashMap<String, smallvec::SmallVec<[SdkSignedOrder; 2]>>,
        registry_tokens: &[crate::TokenSlot],
    ) {
        let old_len = self.presign_templates.len();
        let new_len = registry_tokens.len();
        self.presign_templates.resize_with(new_len, smallvec::SmallVec::new);
        self.presign_pool.resize_with(new_len, smallvec::SmallVec::new);
        for idx in old_len..new_len {
            let token_id = registry_tokens[idx].token_id.trim();
            if let Some(tpls) = new_templates.remove(token_id) {
                self.presign_template_catalog
                    .insert(token_id.to_string(), tpls.clone());
                self.presign_templates[idx] = tpls;
            }
            if let Some(signed_orders) = new_presigned.remove(token_id) {
                let tifs: smallvec::SmallVec<[OrderTimeInForce; 2]> = self.presign_templates[idx]
                    .iter()
                    .map(|t| t.time_in_force)
                    .collect();
                for (i, signed) in signed_orders.into_iter().enumerate() {
                    let tif = tifs.get(i).copied().unwrap_or(OrderTimeInForce::FAK);
                    if let Ok(payload) = prepare_payload_from_signed(signed, tif) {
                        self.presign_pool[idx].push(Box::new(payload));
                    }
                }
            }
        }
    }

    pub(crate) fn replace_registry(&mut self, new_registry: std::sync::Arc<crate::TargetRegistry>) {
        self.registry = Arc::clone(&new_registry);
        self.shared_registry.store(new_registry);
    }
}

/// Signs orders for each token at startup and stores them in the pool.
/// Each token may have 1-2 templates (primary + optional secondary).
pub(crate) async fn warm_presign_startup_into(
    cfg: &DispatchConfig,
    client: &SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
    signer: &super::CachedSigner,
    templates: &[smallvec::SmallVec<[OrderRequestData; 2]>],
    pool: &mut [smallvec::SmallVec<[Box<PreparedOrderPayload>; 2]>],
) -> Result<(), String> {
    if !cfg.presign_enabled {
        return Ok(());
    }
    if templates.is_empty() || templates.iter().all(|t| t.is_empty()) {
        return Err("presign_startup_warm_no_templates".to_string());
    }
    if templates.len() != pool.len() {
        return Err(format!(
            "presign_startup_warm_size_mismatch:templates={},pool={}",
            templates.len(),
            pool.len()
        ));
    }

    // Flatten to (token_idx, template) work items — one per template, not per token.
    let mut key_work: Vec<(usize, OrderRequestData)> = Vec::new();
    for (idx, template_slot) in templates.iter().enumerate() {
        if template_slot.is_empty() || !pool[idx].is_empty() {
            continue;
        }
        for tpl in template_slot {
            key_work.push((idx, tpl.clone()));
        }
    }
    if key_work.is_empty() {
        return Ok(());
    }

    // Timeout: generous fixed budget. 308 orders at 50-concurrent finishes
    // in ~5-10s on a healthy network; 120s gives ample margin for slow CLOB
    // responses without the old per-key scaling that allowed 5+ minute hangs.
    let base_s = cfg.presign_startup_warm_timeout_seconds.max(0.1);
    let total_timeout_s = base_s.max(120.0);
    let timeout = Duration::from_secs_f64(total_timeout_s);

    // All tasks are spawned immediately but a semaphore caps concurrent
    // SDK `.build()` calls (which hit GET /tick-size on first call per
    // token). The SDK caches tick-size per token_id internally, so only
    // ~N unique tokens produce HTTP calls. 50 concurrent is safe — tested
    // at 100 concurrent with zero 429s. The old batch-of-5 + 500ms sleep
    // caused 5+ minute warmup for ~150 orders; this brings it to seconds.
    const MAX_CONCURRENT: usize = 50;
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));

    let warmup_start = std::time::Instant::now();
    let n_orders = key_work.len();

    let handles: Vec<_> = key_work
        .iter()
        .map(|(idx, template)| {
            let c = client.clone();
            let s = signer.clone();
            let tpl = template.clone();
            let i = *idx;
            let tif = tpl.time_in_force;
            let sem = semaphore.clone();
            let t0 = warmup_start;
            tokio::spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore closed");
                let task_start = t0.elapsed();
                let result = super::sdk_exec::sign_order_batch(&c, &s, &tpl, 1).await;
                let task_dur = t0.elapsed() - task_start;
                (i, tpl.token_id, tif, result, task_dur)
            })
        })
        .collect();

    let results = tokio::time::timeout(timeout, futures_util::future::join_all(handles))
        .await
        .map_err(|_| {
            format!(
                "presign_startup_warm_timeout:timeout_s={:.3},n_orders={},elapsed_s={:.3}",
                total_timeout_s,
                n_orders,
                warmup_start.elapsed().as_secs_f64(),
            )
        })?;

    let mut max_task_ms = 0u128;
    let mut sum_task_ms = 0u128;
    let mut n_ok = 0usize;
    let mut first_err: Option<String> = None;

    for result in results {
        let (idx, token_id, tif, batch_result, task_dur) =
            result.map_err(|e| format!("presign_task_panicked:{}", e))?;
        let task_ms = task_dur.as_millis();
        sum_task_ms += task_ms;
        if task_ms > max_task_ms {
            max_task_ms = task_ms;
        }
        match batch_result {
            Ok(signed_orders) => {
                if let Some(signed) = signed_orders.into_iter().next() {
                    pool[idx].push(Box::new(prepare_payload_from_signed(signed, tif)?));
                }
                n_ok += 1;
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(format!("{}:{}", redact_token_id(&token_id), e));
                }
            }
        }
    }

    let wall_ms = warmup_start.elapsed().as_millis();
    let avg_ms = if n_ok + first_err.iter().count() > 0 {
        sum_task_ms / (n_ok + first_err.iter().count()) as u128
    } else {
        0
    };
    eprintln!(
        "[presign] warmup: {} orders, {} ok, wall={:.1}s, avg_task={}ms, max_task={}ms",
        n_orders, n_ok, wall_ms as f64 / 1000.0, avg_ms, max_task_ms,
    );

    if let Some(err) = first_err {
        return Err(format!("presign_warmup_failed:{}", err));
    }

    // Verify all tokens with templates have at least one signed order
    for (idx, template_slot) in templates.iter().enumerate() {
        if template_slot.is_empty() {
            continue;
        }
        if pool[idx].is_empty() {
            let token_id = template_slot
                .first()
                .map(|t| t.token_id.as_str())
                .unwrap_or("_");
            return Err(format!(
                "presign_startup_warm_incomplete:{}",
                redact_token_id(token_id),
            ));
        }
    }

    Ok(())
}

pub(crate) fn prepare_payload_from_signed(
    signed: SdkSignedOrder,
    time_in_force: OrderTimeInForce,
) -> Result<PreparedOrderPayload, String> {
    let order_json =
        serde_json::to_vec(&signed).map_err(|e| format!("presign_serialize_failed:{}", e))?;
    Ok(PreparedOrderPayload { order_json, time_in_force })
}
