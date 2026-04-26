use super::*;

impl DispatchRuntime {
    pub(crate) fn mode_label(&self) -> &'static str {
        if matches!(self.cfg.mode, DispatchMode::Http) {
            "http"
        } else {
            "noop"
        }
    }

    fn build_order_request(&self, token_id: &str) -> OrderRequestData {
        OrderRequestData {
            token_id: token_id.to_string(),
            side: "buy_yes".to_string(),
            amount_usdc: self.cfg.amount_usdc.max(0.0),
            limit_price: self.cfg.limit_price.max(0.0),
            time_in_force: self.cfg.time_in_force,
            size_shares: self.cfg.size_shares.max(0.0),
        }
    }

    /// Presign fast path: pop presigned order by token, POST it, return exchange_order_id.
    /// No OrderRequestData construction, no metadata building.
    async fn dispatch_presigned_async(&mut self, token_id: &str) -> Result<String, String> {
        let key = PreSignKey {
            token_id: token_id.trim().to_string(),
        };
        let presigned = self.pop_presigned_order(&key).ok_or_else(|| {
            format!(
                "submit_presigned_miss:token_id={}",
                redact_token_id(key.token_id.as_str())
            )
        })?;
        self.ensure_sdk_runtime().await?;
        let sdk = self
            .sdk_runtime
            .as_ref()
            .ok_or_else(|| "sdk_runtime_missing".to_string())?;
        let resp = sdk
            .client
            .post_order(presigned.signed_order)
            .await
            .map_err(|e| format!("submit_failed:{}", e))?;
        if !resp.success {
            return Err(format!(
                "submit_failed:errorMsg:{}",
                resp.error_msg.unwrap_or_else(|| "unknown".to_string())
            ));
        }
        Ok(resp.order_id)
    }

    /// Non-presign path: build request, sign, submit.
    async fn dispatch_sign_and_submit_async(&mut self, token_id: &str) -> Result<String, String> {
        let request = self.build_order_request(token_id);
        self.submit_order_async(&request).await
    }

    /// Dispatch an order for the given token. Returns the exchange order ID on
    /// success, or "noop" for paper mode.
    pub(crate) async fn dispatch_order(&mut self, token_id: &str) -> Result<String, String> {
        if matches!(self.cfg.mode, DispatchMode::Noop) {
            return Ok("noop".to_string());
        }
        if self.cfg.presign_enabled && self.cfg.presign_pool_target_per_key > 0 {
            return self.dispatch_presigned_async(token_id).await;
        }
        self.dispatch_sign_and_submit_async(token_id).await
    }

    pub(crate) async fn dispatch_orders_batch(
        &mut self,
        token_ids: &[&str],
    ) -> Vec<Result<String, String>> {
        let count = token_ids.len();
        if count == 0 {
            return Vec::new();
        }

        if matches!(self.cfg.mode, DispatchMode::Noop) {
            return vec![Ok("noop".to_string()); count];
        }

        let use_presign = self.cfg.presign_enabled && self.cfg.presign_pool_target_per_key > 0;

        let mut signed_orders: Vec<Option<SdkSignedOrder>> = Vec::with_capacity(count);
        let mut early_errors: Vec<Option<String>> = vec![None; count];

        for (i, &token_id) in token_ids.iter().enumerate() {
            if use_presign {
                let key = PreSignKey {
                    token_id: token_id.trim().to_string(),
                };
                match self.pop_presigned_order(&key) {
                    Some(presigned) => signed_orders.push(Some(presigned.signed_order)),
                    None => {
                        early_errors[i] = Some(format!(
                            "submit_presigned_miss:token_id={}",
                            redact_token_id(key.token_id.as_str())
                        ));
                        signed_orders.push(None);
                    }
                }
            } else {
                let request = self.build_order_request(token_id);
                match self.build_signed_order_async(&request).await {
                    Ok(signed) => signed_orders.push(Some(signed)),
                    Err(e) => {
                        early_errors[i] = Some(e);
                        signed_orders.push(None);
                    }
                }
            }
        }

        let mut batch_indices: Vec<usize> = Vec::new();
        let mut batch_orders: Vec<SdkSignedOrder> = Vec::new();
        for (i, maybe_signed) in signed_orders.into_iter().enumerate() {
            if let Some(signed) = maybe_signed {
                batch_indices.push(i);
                batch_orders.push(signed);
            }
        }

        let mut results: Vec<Result<String, String>> = early_errors
            .into_iter()
            .map(|e| match e {
                Some(err) => Err(err),
                None => Ok(String::new()),
            })
            .collect();

        const MAX_BATCH_SIZE: usize = 15;
        let mut batch_offset = 0;
        while !batch_orders.is_empty() {
            let chunk_len = batch_orders.len().min(MAX_BATCH_SIZE);
            let chunk: Vec<SdkSignedOrder> = batch_orders.drain(..chunk_len).collect();
            let chunk_count = chunk.len();
            match self.post_signed_orders_batch_async(chunk).await {
                Ok(chunk_results) => {
                    for (j, result) in chunk_results.into_iter().enumerate() {
                        results[batch_indices[batch_offset + j]] = result;
                    }
                }
                Err(transport_err) => {
                    for j in 0..chunk_count {
                        results[batch_indices[batch_offset + j]] = Err(transport_err.clone());
                    }
                }
            }
            batch_offset += chunk_count;
        }

        results
    }
}
