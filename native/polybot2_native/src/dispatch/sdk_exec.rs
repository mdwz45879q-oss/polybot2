use super::*;
use super::events::normalize_status;

impl DispatchRuntime {
    pub(crate) fn new(dispatch_cfg: DispatchConfig, telemetry: Option<TelemetryEmitter>) -> Self {
        Self {
            cfg: dispatch_cfg,
            sdk_runtime: None,
            cached_signer: None,
            active_orders_by_strategy: HashMap::new(),
            presign_template_catalog: HashMap::new(),
            presign_templates: HashMap::new(),
            presign_pool: HashMap::new(),
            last_refill_ns_by_key: HashMap::new(),
            pending_refill_by_key: HashMap::new(),
            telemetry,
        }
    }

    pub(super) async fn ensure_sdk_runtime(&mut self) -> Result<(), String> {
        if self.sdk_runtime.is_some() {
            return Ok(());
        }

        let private_key = self.cfg.presign_private_key.trim().to_string();
        if private_key.is_empty() {
            return Err(
                "missing signer private key; set POLY_EXEC_PRIVATE_KEY or POLY_EXEC_PRESIGN_PRIVATE_KEY"
                    .to_string(),
            );
        }
        if self.cfg.api_key.trim().is_empty()
            || self.cfg.api_secret.trim().is_empty()
            || self.cfg.api_passphrase.trim().is_empty()
        {
            return Err(
                "missing POLY_EXEC_API_KEY/POLY_EXEC_API_SECRET/POLY_EXEC_API_PASSPHRASE"
                    .to_string(),
            );
        }

        let chain_id = self.cfg.chain_id.max(1) as u64;
        let signer = SdkLocalSigner::from_str(private_key.as_str())
            .map_err(|e| format!("sdk_signer_private_key:{}", e))?
            .with_chain_id(Some(chain_id));

        let api_key = Uuid::parse_str(self.cfg.api_key.trim())
            .map_err(|e| format!("invalid_api_key_uuid:{}", e))?;
        let credentials = SdkCredentials::new(
            api_key,
            self.cfg.api_secret.clone(),
            self.cfg.api_passphrase.clone(),
        );
        let signature_type = map_sdk_signature_type(self.cfg.signature_type)?;
        if !self.cfg.funder.trim().is_empty() && !matches!(signature_type, SdkSignatureType::Proxy)
        {
            return Err(format!(
                "invalid_signature_type_for_funder:{}:expected_proxy(1)",
                self.cfg.signature_type
            ));
        }
        let client_cfg = SdkConfig::builder().use_server_time(true).build();
        let client = SdkClient::new(self.cfg.clob_host.as_str(), client_cfg)
            .map_err(|e| format!("sdk_client_new:{}", e))?;

        let mut auth = client
            .authentication_builder(&signer)
            .credentials(credentials)
            .signature_type(signature_type);
        if !self.cfg.funder.trim().is_empty() {
            let funder = SdkAddress::from_str(self.cfg.funder.trim())
                .map_err(|e| format!("invalid_funder_address:{}", e))?;
            auth = auth.funder(funder);
        }
        let authed = auth
            .authenticate()
            .await
            .map_err(|e| format!("sdk_authenticate:{}", e))?;
        self.sdk_runtime = Some(PolymarketSdkRuntime { client: authed });
        self.cached_signer = Some(signer);
        Ok(())
    }

    pub(super) async fn build_signed_order_async(
        &mut self,
        request: &OrderRequestData,
    ) -> Result<SdkSignedOrder, String> {
        self.ensure_sdk_runtime().await?;
        let sdk = self
            .sdk_runtime
            .as_ref()
            .ok_or_else(|| "sdk_runtime_missing".to_string())?;
        let token_id = parse_sdk_token_id(request.token_id.as_str())?;
        let side = map_sdk_side(request.side.as_str())?;
        let order_type = map_sdk_order_type(request.time_in_force.as_str())?;
        let signer = self
            .cached_signer
            .clone()
            .ok_or_else(|| "cached_signer_missing:call_ensure_sdk_runtime_first".to_string())?;

        let signable = if is_market_order_type(request.time_in_force.as_str()) {
            let amount_usdc =
                parse_decimal_from_f64(request.amount_usdc, 6, "amount_usdc")?;
            let limit_price =
                parse_decimal_from_f64(request.limit_price, 6, "limit_price")?;
            sdk.client
                .market_order()
                .token_id(token_id)
                .amount(
                    SdkAmount::usdc(amount_usdc)
                        .map_err(|e| format!("invalid_market_amount:{}", e))?,
                )
                .side(side)
                .order_type(order_type)
                .price(limit_price)
                .build()
                .await
                .map_err(|e| format!("submit_failed:{}", e))?
        } else {
            let size = parse_decimal_from_f64(request.size_shares, 2, "size_shares")?;
            let limit_price =
                parse_decimal_from_f64(request.limit_price, 6, "limit_price")?;
            let mut builder = sdk
                .client
                .limit_order()
                .token_id(token_id)
                .size(size)
                .side(side)
                .order_type(order_type)
                .price(limit_price);
            if let Some(exp_ts) = request.expiration_ts {
                if exp_ts > 0 {
                    let dt = chrono::DateTime::from_timestamp(exp_ts, 0)
                        .ok_or_else(|| format!("invalid_expiration_ts:{}", exp_ts))?;
                    builder = builder.expiration(dt);
                }
            }
            builder
                .build()
                .await
                .map_err(|e| format!("submit_failed:{}", e))?
        };

        sdk.client
            .sign(&signer, signable)
            .await
            .map_err(|e| format!("submit_failed:{}", e))
    }

    pub(super) async fn post_signed_order_async(
        &mut self,
        signed: SdkSignedOrder,
        request: &OrderRequestData,
    ) -> Result<OrderStateData, String> {
        self.ensure_sdk_runtime().await?;
        let sdk = self
            .sdk_runtime
            .as_ref()
            .ok_or_else(|| "sdk_runtime_missing".to_string())?;
        let resp = sdk
            .client
            .post_order(signed)
            .await
            .map_err(|e| format!("submit_failed:{}", e))?;
        if !resp.success {
            return Err(format!(
                "submit_failed:errorMsg:{}",
                resp.error_msg.unwrap_or_else(|| "unknown".to_string())
            ));
        }

        Ok(OrderStateData {
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: resp.order_id,
            side: request.side.clone(),
            requested_amount_usdc: request.amount_usdc.max(0.0),
            filled_amount_usdc: 0.0,
            limit_price: request.limit_price.max(0.0),
            time_in_force: request.time_in_force.clone(),
            status: normalize_status(format!("{}", resp.status).as_str()),
            reason: resp.error_msg.unwrap_or_default(),
            error_code: String::new(),
            parent_client_order_id: String::new(),
        })
    }

    pub(super) async fn submit_order_async(
        &mut self,
        request: &OrderRequestData,
    ) -> Result<OrderStateData, String> {
        let signed = self.build_signed_order_async(request).await?;
        self.post_signed_order_async(signed, request).await
    }

    pub(super) async fn cancel_order_async(
        &mut self,
        exchange_order_id: &str,
    ) -> Result<bool, String> {
        self.ensure_sdk_runtime().await?;
        let sdk = self
            .sdk_runtime
            .as_ref()
            .ok_or_else(|| "sdk_runtime_missing".to_string())?;
        let response = sdk
            .client
            .cancel_order(exchange_order_id.trim())
            .await
            .map_err(|e| format!("cancel_failed:{}", e))?;
        if response
            .canceled
            .iter()
            .any(|id| id.trim() == exchange_order_id.trim())
        {
            return Ok(true);
        }
        if response.not_canceled.contains_key(exchange_order_id.trim()) {
            return Ok(false);
        }
        Ok(!response.canceled.is_empty())
    }

    pub(super) fn sdk_client_ref(
        &self,
    ) -> Result<&SdkClient<SdkAuthenticatedState<SdkAuthNormal>>, String> {
        self.sdk_runtime
            .as_ref()
            .map(|r| &r.client)
            .ok_or_else(|| "sdk_runtime_missing".to_string())
    }

    pub(super) fn signer_ref(&self) -> Result<&super::CachedSigner, String> {
        self.cached_signer
            .as_ref()
            .ok_or_else(|| "cached_signer_missing".to_string())
    }

    pub(super) async fn get_order_async(
        &mut self,
        exchange_order_id: &str,
    ) -> Result<OrderStateData, String> {
        self.ensure_sdk_runtime().await?;
        let sdk = self
            .sdk_runtime
            .as_ref()
            .ok_or_else(|| "sdk_runtime_missing".to_string())?;
        let order = sdk
            .client
            .order(exchange_order_id.trim())
            .await
            .map_err(|e| format!("get_order_failed:{}", e))?;

        Ok(OrderStateData {
            client_order_id: String::new(),
            exchange_order_id: order.id,
            side: format!("{}", order.side).to_ascii_lowercase(),
            requested_amount_usdc: 0.0,
            filled_amount_usdc: 0.0,
            limit_price: order.price.to_string().parse::<f64>().unwrap_or(0.0),
            time_in_force: format!("{}", order.order_type),
            status: normalize_status(format!("{}", order.status).as_str()),
            reason: String::new(),
            error_code: String::new(),
            parent_client_order_id: String::new(),
        })
    }
}

pub(super) async fn sign_order_batch(
    client: &SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
    signer: &super::CachedSigner,
    template: &OrderRequestData,
    count: usize,
) -> Result<Vec<SdkSignedOrder>, String> {
    let token_id = parse_sdk_token_id(template.token_id.as_str())?;
    let side = map_sdk_side(template.side.as_str())?;
    let order_type = map_sdk_order_type(template.time_in_force.as_str())?;
    let is_market = is_market_order_type(template.time_in_force.as_str());

    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        let signable = if is_market {
            let amount_usdc =
                parse_decimal_from_f64(template.amount_usdc, 6, "amount_usdc")?;
            let limit_price =
                parse_decimal_from_f64(template.limit_price, 6, "limit_price")?;
            client
                .market_order()
                .token_id(token_id)
                .amount(
                    SdkAmount::usdc(amount_usdc)
                        .map_err(|e| format!("invalid_market_amount:{}", e))?,
                )
                .side(side)
                .order_type(order_type.clone())
                .price(limit_price)
                .build()
                .await
                .map_err(|e| format!("sign_batch_build_failed:{}", e))?
        } else {
            let size =
                parse_decimal_from_f64(template.size_shares, 2, "size_shares")?;
            let limit_price =
                parse_decimal_from_f64(template.limit_price, 6, "limit_price")?;
            let mut builder = client
                .limit_order()
                .token_id(token_id)
                .size(size)
                .side(side)
                .order_type(order_type.clone())
                .price(limit_price);
            if let Some(exp_ts) = template.expiration_ts {
                if exp_ts > 0 {
                    if let Some(dt) = chrono::DateTime::from_timestamp(exp_ts, 0) {
                        builder = builder.expiration(dt);
                    }
                }
            }
            builder
                .build()
                .await
                .map_err(|e| format!("sign_batch_build_failed:{}", e))?
        };
        let signed = client
            .sign(signer, signable)
            .await
            .map_err(|e| format!("sign_batch_sign_failed:{}", e))?;
        results.push(signed);
    }
    Ok(results)
}
