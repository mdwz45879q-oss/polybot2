use super::*;
use crate::log_writer::LogWriter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) type SharedRegistry = Arc<arc_swap::ArcSwap<crate::TargetRegistry>>;

#[derive(Clone)]
pub(crate) struct OrderRequestData {
    pub(crate) token_id: String,
    pub(super) side: String,
    pub(super) amount_usdc: f64,
    pub(super) limit_price: f64,
    pub(super) time_in_force: OrderTimeInForce,
    pub(super) size_shares: f64,
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

/// Inline-friendly batch of `(TargetIdx, SdkSignedOrder)` pairs. The common
/// case is 1–4 intents per frame; `SmallVec` avoids a heap allocation on the
/// WS thread for that range.
pub(crate) type SubmitBatch = smallvec::SmallVec<[(crate::TargetIdx, Box<SdkSignedOrder>); 4]>;

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
    /// Catalog of templates indexed by raw token_id string. Set once via
    /// `prewarm_presign` from Python; survives across plan loads.
    pub(super) presign_template_catalog: HashMap<String, OrderRequestData>,
    /// Active templates resolved against the current registry, indexed by
    /// `TokenIdx`. `None` means no template is active for that token.
    pub(super) presign_templates: Vec<Option<OrderRequestData>>,
    /// Presign pool, indexed by `TokenIdx`. One slot per unique token (depth=1).
    /// `Option::take()` is the pop operation — zero overhead for a one-shot pool.
    pub(super) presign_pool: Vec<Option<Box<SdkSignedOrder>>>,
    pub(super) submit_tx: Option<rtrb::Producer<SubmitWork>>,
    pub(super) submit_notify: Option<Arc<tokio::sync::Notify>>,
}

/// Submitter-thread half: owns the SDK client and consumes work from the
/// channel. All HTTP and serialization happen here, off the WS thread.
pub(crate) struct OrderSubmitter {
    pub(super) cfg: DispatchConfig,
    pub(super) shared_registry: SharedRegistry,
    pub(super) sdk_runtime: Option<PolymarketSdkRuntime>,
    pub(super) cached_signer: Option<super::CachedSigner>,
    pub(super) submit_rx: rtrb::Consumer<SubmitWork>,
    pub(super) submit_notify: Arc<tokio::sync::Notify>,
    pub(super) stop_flag: Arc<std::sync::atomic::AtomicBool>,
    pub(super) log: Arc<Mutex<LogWriter>>,
    pub(super) health: Arc<Mutex<crate::SubmitterHealth>>,
}

pub(crate) struct PolymarketSdkRuntime {
    pub(super) client: SdkClient<SdkAuthenticatedState<SdkAuthNormal>>,
}
