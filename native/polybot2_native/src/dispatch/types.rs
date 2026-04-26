use super::*;
use std::collections::{HashMap, VecDeque};

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
    pub(super) time_in_force: OrderTimeInForce,
    pub(super) size_shares: f64,
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
    pub(super) presign_template_catalog: HashMap<PreSignKey, OrderRequestData>,
    pub(super) presign_templates: HashMap<PreSignKey, OrderRequestData>,
    pub(super) presign_pool: HashMap<PreSignKey, VecDeque<PreSignedOrderData>>,
}

pub(crate) struct PolymarketSdkRuntime {
    pub(super) client: SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
}
