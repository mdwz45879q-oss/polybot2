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
            presign_templates: vec![None; n],
            presign_pool: (0..n).map(|_| None).collect(),
            submit_tx: None,
            submit_notify: None,
        }
    }

    pub(crate) fn install_submit_tx(&mut self, tx: rtrb::Producer<SubmitWork>, notify: Arc<tokio::sync::Notify>) {
        self.submit_tx = Some(tx);
        self.submit_notify = Some(notify);
    }

    pub(crate) fn templates_and_pool_mut(
        &mut self,
    ) -> (
        &[Option<OrderRequestData>],
        &mut [Option<Box<SdkSignedOrder>>],
    ) {
        (self.presign_templates.as_slice(), self.presign_pool.as_mut_slice())
    }

    pub(crate) fn parse_template_request(template: &PresignTemplateData) -> Option<OrderRequestData> {
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
            size_shares: template.size_shares.filter(|v| *v > 0.0).unwrap_or(amount_usdc / price),
        })
    }

    pub(crate) fn set_presign_templates(&mut self, templates: &[PresignTemplateData]) {
        self.presign_template_catalog.clear();
        for slot in self.presign_templates.iter_mut() {
            *slot = None;
        }
        for slot in self.presign_pool.iter_mut() {
            *slot = None;
        }
        for template in templates {
            let Some(request) = Self::parse_template_request(template) else {
                continue;
            };
            self.presign_template_catalog
                .insert(request.token_id.clone(), request);
        }
    }

    pub(crate) fn activate_presign_templates_for_tokens(
        &mut self,
        _token_ids: &[String],
    ) -> usize {
        let mut active = 0usize;
        for (idx, slot) in self.registry.tokens.iter().enumerate() {
            let trimmed = slot.token_id.trim();
            if let Some(template) = self.presign_template_catalog.get(trimmed) {
                self.presign_templates[idx] = Some(template.clone());
                active += 1;
            } else {
                self.presign_templates[idx] = None;
                self.presign_pool[idx] = None;
            }
        }
        active
    }

    pub(crate) fn extend_for_patch(
        &mut self,
        new_templates: &mut std::collections::HashMap<String, OrderRequestData>,
        new_presigned: &mut std::collections::HashMap<String, SdkSignedOrder>,
        registry_tokens: &[crate::TokenSlot],
    ) {
        let old_len = self.presign_templates.len();
        let new_len = registry_tokens.len();
        self.presign_templates.resize_with(new_len, || None);
        self.presign_pool.resize_with(new_len, || None);
        for idx in old_len..new_len {
            let token_id = registry_tokens[idx].token_id.trim();
            if let Some(tpl) = new_templates.remove(token_id) {
                self.presign_template_catalog
                    .insert(token_id.to_string(), tpl.clone());
                self.presign_templates[idx] = Some(tpl);
            }
            if let Some(signed) = new_presigned.remove(token_id) {
                self.presign_pool[idx] = Some(Box::new(signed));
            }
        }
    }

    pub(crate) fn replace_registry(&mut self, new_registry: std::sync::Arc<crate::TargetRegistry>) {
        self.registry = Arc::clone(&new_registry);
        self.shared_registry.store(new_registry);
    }
}

/// Signs one order per token at startup and stores them in the pool. Pool
/// depth is always 1 (one-shot intents fire each token at most once).
pub(crate) async fn warm_presign_startup_into(
    cfg: &DispatchConfig,
    client: &SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
    signer: &super::CachedSigner,
    templates: &[Option<OrderRequestData>],
    pool: &mut [Option<Box<SdkSignedOrder>>],
) -> Result<(), String> {
    if !cfg.presign_enabled {
        return Ok(());
    }
    if templates.is_empty() || templates.iter().all(|t| t.is_none()) {
        return Err("presign_startup_warm_no_templates".to_string());
    }
    if templates.len() != pool.len() {
        return Err(format!(
            "presign_startup_warm_size_mismatch:templates={},pool={}",
            templates.len(),
            pool.len()
        ));
    }

    let mut key_work: Vec<(usize, OrderRequestData)> = Vec::new();
    for (idx, template_slot) in templates.iter().enumerate() {
        let Some(template) = template_slot.as_ref() else {
            continue;
        };
        if pool[idx].is_some() {
            continue;
        }
        key_work.push((idx, template.clone()));
    }
    if key_work.is_empty() {
        return Ok(());
    }

    let per_key_s = 1.0_f64;
    let base_s = cfg.presign_startup_warm_timeout_seconds.max(0.1);
    let total_timeout_s = base_s + per_key_s * key_work.len() as f64;
    let timeout = Duration::from_secs_f64(total_timeout_s);

    let handles: Vec<_> = key_work
        .into_iter()
        .map(|(idx, template)| {
            let c = client.clone();
            let s = signer.clone();
            tokio::spawn(async move {
                let result =
                    super::sdk_exec::sign_order_batch(&c, &s, &template, 1).await;
                (idx, template.token_id, result)
            })
        })
        .collect();

    let results = tokio::time::timeout(timeout, futures_util::future::join_all(handles))
        .await
        .map_err(|_| {
            let detail = templates
                .iter()
                .enumerate()
                .filter_map(|(idx, t)| {
                    t.as_ref().map(|tpl| {
                        format!(
                            "{}:{}",
                            redact_token_id(&tpl.token_id),
                            if pool[idx].is_some() { 1 } else { 0 }
                        )
                    })
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "presign_startup_warm_timeout:timeout_s={:.3} detail={}",
                total_timeout_s, detail
            )
        })?;

    for result in results {
        let (idx, token_id, batch_result) =
            result.map_err(|e| format!("presign_task_panicked:{}", e))?;
        let signed_orders = batch_result.map_err(|e| {
            format!("presign_warmup_failed:{}:{}", redact_token_id(&token_id), e)
        })?;
        if let Some(signed) = signed_orders.into_iter().next() {
            pool[idx] = Some(Box::new(signed));
        }
    }

    for (idx, template_slot) in templates.iter().enumerate() {
        if template_slot.is_none() {
            continue;
        }
        if pool[idx].is_none() {
            let token_id = template_slot
                .as_ref()
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
