use super::*;
use crate::telemetry::TelemetryEmitter;
use std::collections::{HashMap, VecDeque};

const TERMINAL_STATUSES: [&str; 8] = [
    "filled", "canceled", "expired", "rejected", "failed", "replaced",
    "matched", "delayed",
];

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct PreSignKey {
    pub(super) token_id: String,
}

#[derive(Clone)]
pub(crate) struct OrderRequestData {
    pub(super) token_id: String,
    pub(super) side: String,
    pub(super) amount_usdc: f64,
    pub(super) limit_price: f64,
    pub(super) time_in_force: String,
    pub(super) client_order_id: String,
    pub(super) size_shares: f64,
    pub(super) expiration_ts: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OrderStateData {
    pub(super) client_order_id: String,
    pub(super) exchange_order_id: String,
    pub(super) side: String,
    pub(super) requested_amount_usdc: f64,
    pub(super) filled_amount_usdc: f64,
    pub(super) limit_price: f64,
    pub(super) time_in_force: String,
    pub(super) status: String,
    pub(super) reason: String,
    pub(super) error_code: String,

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
    pub(super) inserted_ns: i64,
}

pub(crate) struct PreSignedOrderData {
    pub(super) signed_order: SdkSignedOrder,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PresignTemplateData {
    pub(super) token_id: String,
    pub(super) side: Option<String>,
    pub(super) amount_usdc: Option<f64>,
    pub(super) size_shares: Option<f64>,
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
    pub(super) broker_failure_count: HashMap<String, u64>,
    pub(super) telemetry: Option<TelemetryEmitter>,
}

pub(crate) struct PolymarketSdkRuntime {
    pub(super) client: SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
}
