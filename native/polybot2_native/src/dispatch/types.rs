use super::*;
use crate::log_writer::LogWriter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) type SharedRegistry = Arc<arc_swap::ArcSwap<crate::TargetRegistry>>;

#[derive(Debug)]
pub(crate) struct PreparedOrderPayload {
    pub(crate) order_json: Vec<u8>,
    pub(crate) time_in_force: OrderTimeInForce,
}

#[derive(Clone)]
pub(crate) struct OrderRequestData {
    pub(crate) token_id: String,
    pub(super) side: String,
    pub(super) amount_usdc: f64,
    pub(super) limit_price: f64,
    pub(super) time_in_force: OrderTimeInForce,
    pub(super) size_shares: f64,
    pub(crate) tick_size: f64,
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
    #[serde(default)]
    pub(super) tick_size: Option<f64>,
}

/// Inline-friendly batch of `(TargetIdx, PreparedOrderPayload)` pairs. With
/// up to 3 orders per presign slot, a frame with N intents can produce up to 3N entries.
/// Capacity 32 covers the common case without heap allocation.
pub(crate) type SubmitBatch =
    smallvec::SmallVec<[(crate::TargetIdx, Box<PreparedOrderPayload>); 32]>;

/// Channel payload from WS thread to submitter thread. One `Batch` is built
/// per material WS frame; the submitter may further coalesce subsequent
/// `Batch` arrivals up to `MAX_BATCH_SIZE` before posting.
pub(crate) enum SubmitWork {
    Batch(SubmitBatch),
    #[allow(dead_code)]
    Stop,
}

/// WS-thread half of dispatch: owns the presign pool and a channel sender to
/// the submitter thread. All operations are synchronous.
pub(crate) struct DispatchHandle {
    pub(crate) cfg: DispatchConfig,
    pub(super) registry: Arc<crate::TargetRegistry>,
    pub(super) shared_registry: SharedRegistry,
    /// Catalog of templates indexed by raw token_id string. Each token may
    /// have 1-3 templates (primary + optional secondary + optional tertiary).
    /// Set once via `prewarm_presign` from Python; survives across plan loads.
    pub(super) presign_template_catalog: HashMap<String, smallvec::SmallVec<[OrderRequestData; 3]>>,
    /// Active templates resolved against the current registry, indexed by
    /// `TokenIdx`. Empty vec means no template is active for that token.
    pub(super) presign_templates: Vec<smallvec::SmallVec<[OrderRequestData; 3]>>,
    /// Presign pool, indexed by `TokenIdx`. Each slot holds 0-3 pre-signed
    /// orders. `std::mem::take()` drains all orders for that token at once.
    pub(super) presign_pool: Vec<smallvec::SmallVec<[Box<PreparedOrderPayload>; 3]>>,
    /// Retirement presign template catalog — parallel to `presign_template_catalog`.
    /// Populated by Python with 50/50 retirement orders for tennis markets.
    pub(super) presign_template_catalog_retirement: HashMap<String, smallvec::SmallVec<[OrderRequestData; 3]>>,
    /// Retirement templates resolved against the current registry, indexed by
    /// `TokenIdx`. Parallel to `presign_templates`.
    pub(super) presign_templates_retirement: Vec<smallvec::SmallVec<[OrderRequestData; 3]>>,
    /// Retirement presign pool, indexed by `TokenIdx`. Parallel to `presign_pool`.
    /// Used for 50/50 retirement orders in tennis.
    pub(super) presign_pool_retirement: Vec<smallvec::SmallVec<[Box<PreparedOrderPayload>; 3]>>,
    pub(super) submit_tx: Option<rtrb::Producer<SubmitWork>>,
}

/// Submitter-thread half: owns the SDK client and consumes work from the
/// channel. All HTTP and serialization happen here, off the WS thread.
pub(crate) struct OrderSubmitter {
    pub(super) cfg: DispatchConfig,
    pub(super) shared_registry: SharedRegistry,
    pub(super) sdk_runtime: Option<PolymarketSdkRuntime>,
    pub(super) cached_signer: Option<super::CachedSigner>,
    pub(super) submit_rx: rtrb::Consumer<SubmitWork>,
    pub(super) stop_flag: Arc<std::sync::atomic::AtomicBool>,
    pub(super) log: Arc<Mutex<LogWriter>>,
    pub(super) health: Arc<Mutex<crate::SubmitterHealth>>,
}

pub(crate) struct PolymarketSdkRuntime {
    pub(super) client: SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
}
