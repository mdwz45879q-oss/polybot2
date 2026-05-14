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
                for signed in signed_orders {
                    if let Ok(payload) = prepare_payload_from_signed(signed) {
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

    let per_key_s = 1.0_f64;
    let base_s = cfg.presign_startup_warm_timeout_seconds.max(0.1);
    let total_timeout_s = base_s + per_key_s * key_work.len() as f64;
    let timeout = Duration::from_secs_f64(total_timeout_s);

    const BATCH_SIZE: usize = 5;
    const BATCH_DELAY_MS: u64 = 500;

    let deadline = tokio::time::Instant::now() + timeout;

    for chunk in key_work.chunks(BATCH_SIZE) {
        let handles: Vec<_> = chunk
            .iter()
            .map(|(idx, template)| {
                let c = client.clone();
                let s = signer.clone();
                let tpl = template.clone();
                let i = *idx;
                tokio::spawn(async move {
                    let result = super::sdk_exec::sign_order_batch(&c, &s, &tpl, 1).await;
                    (i, tpl.token_id, result)
                })
            })
            .collect();

        let results = tokio::time::timeout_at(deadline, futures_util::future::join_all(handles))
            .await
            .map_err(|_| {
                format!(
                    "presign_startup_warm_timeout:timeout_s={:.3}",
                    total_timeout_s,
                )
            })?;

        for result in results {
            let (idx, token_id, batch_result) =
                result.map_err(|e| format!("presign_task_panicked:{}", e))?;
            let signed_orders = batch_result.map_err(|e| {
                format!("presign_warmup_failed:{}:{}", redact_token_id(&token_id), e)
            })?;
            if let Some(signed) = signed_orders.into_iter().next() {
                pool[idx].push(Box::new(prepare_payload_from_signed(signed)?));
            }
        }

        // Rate-limit pause between batches to avoid 429 from CLOB /tick-size
        tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
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
) -> Result<PreparedOrderPayload, String> {
    let order_json =
        serde_json::to_vec(&signed).map_err(|e| format!("presign_serialize_failed:{}", e))?;
    Ok(PreparedOrderPayload { order_json })
}
