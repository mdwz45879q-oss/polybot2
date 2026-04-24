use super::*;
use crate::telemetry::TelemetryEmitter;
use std::collections::{HashMap, VecDeque};

const TERMINAL_STATUSES: [&str; 6] = [
    "filled", "canceled", "expired", "rejected", "failed", "replaced",
];

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct PreSignKey {
    pub(super) token_id: String,
}

#[derive(Clone)]
pub(crate) struct OrderRequestData {
    pub(super) token_id: String,
    pub(super) side: String,
    pub(super) notional_usdc: f64,
    pub(super) limit_price: f64,
    pub(super) time_in_force: String,
    pub(super) client_order_id: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OrderStateData {
    pub(super) client_order_id: String,
    pub(super) exchange_order_id: String,
    pub(super) side: String,
    pub(super) requested_notional_usdc: f64,
    pub(super) filled_notional_usdc: f64,
    pub(super) limit_price: f64,
    pub(super) time_in_force: String,
    pub(super) status: String,
    pub(super) reason: String,
    pub(super) error_code: String,
    pub(super) parent_client_order_id: String,
}

impl OrderStateData {
    pub(super) fn is_terminal(&self) -> bool {
        let status = self.status.trim().to_lowercase();
        TERMINAL_STATUSES.iter().any(|s| *s == status)
    }
}

#[derive(Clone)]
pub(crate) struct ActiveOrderRef {
    pub(super) client_order_id: String,
    pub(super) exchange_order_id: String,
    pub(super) status: String,
    pub(super) source_universal_id: String,
    pub(super) chain_id: String,
}

pub(crate) struct PreSignedOrderData {
    pub(super) signed_order: SdkSignedOrder,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PresignTemplateData {
    pub(super) token_id: String,
    pub(super) side: Option<String>,
    pub(super) notional_usdc: Option<f64>,
    pub(super) limit_price: Option<f64>,
    pub(super) time_in_force: Option<String>,
}

pub(crate) struct DispatchRuntime {
    pub(super) cfg: DispatchConfig,
    pub(super) sdk_runtime: Option<PolymarketSdkRuntime>,
    pub(super) cached_signer: Option<super::CachedSigner>,
    pub(super) active_orders_by_strategy: HashMap<String, ActiveOrderRef>,
    pub(super) presign_template_catalog: HashMap<PreSignKey, OrderRequestData>,
    pub(super) presign_templates: HashMap<PreSignKey, OrderRequestData>,
    pub(super) presign_pool: HashMap<PreSignKey, VecDeque<PreSignedOrderData>>,
    pub(super) last_refill_ns_by_key: HashMap<PreSignKey, i64>,
    pub(super) pending_refill_by_key: HashMap<PreSignKey, ()>,
    pub(super) telemetry: Option<TelemetryEmitter>,
}

pub(crate) struct PolymarketSdkRuntime {
    pub(super) client: SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
}
