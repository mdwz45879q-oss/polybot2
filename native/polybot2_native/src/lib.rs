mod baseball;
pub(crate) mod boltodds_frame_pipeline;
pub(crate) mod boltodds_types;
mod dispatch;
pub(crate) mod fast_extract;
mod kalstrop_types;
mod log_writer;
mod runtime;
mod soccer;
mod ws;
pub(crate) mod ws_boltodds;

#[cfg(feature = "bench-support")]
#[doc(hidden)]
pub mod bench_support;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use tokio::runtime::Builder as TokioBuilder;
use tokio::time::sleep as tokio_sleep;

use tokio_tungstenite::tungstenite::Message;

use polymarket_client_sdk_v2::auth::state::Authenticated as SdkAuthenticatedState;
use polymarket_client_sdk_v2::auth::Normal as SdkAuthNormal;
use polymarket_client_sdk_v2::clob::types::SignedOrder as SdkSignedOrder;
use polymarket_client_sdk_v2::clob::Client as SdkClient;
type CachedSigner = alloy::signers::local::PrivateKeySigner;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct GameIdx(u16);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct TargetIdx(pub(crate) u16);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct TokenIdx(pub(crate) u16);

#[derive(Clone)]
struct TokenSlot {
    token_id: Arc<str>,
}

/// Read-only mapping of `TargetIdx → TargetSlot` and `TokenIdx → TokenSlot`,
/// shared via `Arc` between the WS-thread `DispatchHandle` and the submitter
/// thread. Built once in `engine.load_plan` and cloned to both halves at
/// runtime startup.
struct TargetRegistry {
    tokens: Vec<TokenSlot>,
    targets: Vec<TargetSlot>,
}

#[derive(Clone)]
struct TargetSlot {
    token_idx: TokenIdx,
    strategy_key: Arc<str>,
}

// --- Inline string for zero-alloc StateRow dedup ---

/// Fixed-capacity inline string. No heap allocation.
/// Used in StateRow for dedup -- stores short raw field values.
#[derive(Clone)]
pub(crate) struct InlineStr<const N: usize> {
    buf: [u8; N],
    len: u8,
}

impl<const N: usize> InlineStr<N> {
    pub const fn new() -> Self {
        Self {
            buf: [0u8; N],
            len: 0,
        }
    }
    pub fn from_str(s: &str) -> Self {
        let bytes = s.as_bytes();
        let copy_len = bytes.len().min(N);
        let mut this = Self {
            buf: [0u8; N],
            len: copy_len as u8,
        };
        this.buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        this
    }
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len as usize]) }
    }
}

impl<const N: usize> Default for InlineStr<N> {
    fn default() -> Self {
        Self::new()
    }
}

// --- Sport-generic types shared by baseball and soccer ---

#[derive(Clone, Copy, Debug, PartialEq)]
enum SpreadSide {
    Home,
    Away,
}

#[derive(Clone, Copy)]
struct OverLine {
    half_int: u16,
    target_idx: TargetIdx,
}

#[derive(Clone, Copy)]
struct Intent {
    target_idx: TargetIdx,
}

/// Sport-specific engine variant. The WS worker matches once per frame
/// and dispatches to the right frame pipeline. One branch per frame
/// (~1ns, always predicted) — no trait vtable overhead.
enum SportEngine {
    Baseball(baseball::types::NativeMlbEngine),
    Soccer(soccer::types::NativeSoccerEngine),
}

impl SportEngine {
    fn active_subscriptions_for_candidates(
        &self,
        candidates: &[String],
        now_ts_utc: i64,
        subscribe_lead_minutes: i64,
    ) -> Vec<String> {
        match self {
            Self::Baseball(e) => e.active_subscriptions_for_candidates(
                candidates,
                now_ts_utc,
                subscribe_lead_minutes,
            ),
            Self::Soccer(e) => e.active_subscriptions_for_candidates(
                candidates,
                now_ts_utc,
                subscribe_lead_minutes,
            ),
        }
    }

    fn merge_plan(&mut self, plan_json: &str) -> Result<MergePlanResult, String> {
        match self {
            Self::Baseball(e) => e.merge_plan(plan_json),
            Self::Soccer(e) => e.merge_plan(plan_json),
        }
    }

    fn tokens(&self) -> &[TokenSlot] {
        match self {
            Self::Baseball(e) => &e.tokens,
            Self::Soccer(e) => &e.tokens,
        }
    }

    fn target_slots(&self) -> &[TargetSlot] {
        match self {
            Self::Baseball(e) => &e.target_slots,
            Self::Soccer(e) => &e.target_slots,
        }
    }

    fn set_registry(&mut self, reg: Option<Arc<TargetRegistry>>) {
        match self {
            Self::Baseball(e) => e.registry = reg,
            Self::Soccer(e) => e.registry = reg,
        }
    }

    fn game_ids(&self) -> &[String] {
        match self {
            Self::Baseball(e) => &e.game_ids,
            Self::Soccer(e) => &e.game_ids,
        }
    }

    fn all_token_ids(&self) -> Vec<String> {
        match self {
            Self::Baseball(e) => e.all_token_ids(),
            Self::Soccer(e) => e.all_token_ids(),
        }
    }

    fn clone_registry(&self) -> Option<Arc<TargetRegistry>> {
        match self {
            Self::Baseball(e) => e.clone_registry(),
            Self::Soccer(e) => e.clone_registry(),
        }
    }

    fn token_ids_by_game_len(&self) -> usize {
        match self {
            Self::Baseball(e) => e.token_ids_by_game.len(),
            Self::Soccer(e) => e.token_ids_by_game.len(),
        }
    }
}

#[derive(Default, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RuntimeStartConfig {
    subscribe_lead_minutes: Option<i64>,
    subscription_refresh_seconds: Option<f64>,
    amount_usdc: Option<f64>,
    size_shares: Option<f64>,
    limit_price: Option<f64>,
    time_in_force: Option<String>,
    live_enabled: Option<bool>,
    reconnect_sleep_seconds: Option<f64>,
    kalstrop_ws_url: Option<String>,
    kalstrop_client_id: Option<String>,
    kalstrop_shared_secret_raw: Option<String>,
    log_dir: Option<String>,
    run_id: Option<i64>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    boltodds_api_key: Option<String>,
    #[serde(default)]
    boltodds_ws_url: Option<String>,
}

#[derive(Default, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct ExecStartConfig {
    dispatch_mode: Option<String>,
    clob_host: Option<String>,
    api_key: Option<String>,
    api_secret: Option<String>,
    api_passphrase: Option<String>,
    funder: Option<String>,
    signature_type: Option<i64>,
    chain_id: Option<i64>,
    presign_enabled: Option<bool>,
    presign_private_key: Option<String>,
    #[allow(dead_code)]
    presign_pool_target_per_key: Option<i64>,
    presign_startup_warm_timeout_seconds: Option<f64>,
}

#[derive(Default)]
struct RuntimeHealth {
    running: bool,
    reconnects: i64,
    last_error: String,
}

pub(crate) struct SubmitterHealth {
    pub(crate) running: bool,
    pub(crate) last_error: String,
}

impl Default for SubmitterHealth {
    fn default() -> Self {
        Self {
            running: false,
            last_error: String::new(),
        }
    }
}

#[derive(Clone)]
enum DispatchMode {
    Noop,
    Http,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderTimeInForce {
    FAK,
    FOK,
    GTC,
}

impl OrderTimeInForce {
    fn is_market_order(self) -> bool {
        matches!(self, Self::FAK | Self::FOK)
    }
}

#[derive(Clone)]
struct DispatchConfig {
    mode: DispatchMode,
    clob_host: String,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    funder: String,
    signature_type: i64,
    chain_id: i64,
    presign_enabled: bool,
    presign_private_key: String,
    presign_startup_warm_timeout_seconds: f64,
    // Order parameters carried for the no-presign sign-and-submit path used
    // only by live tests; the WS hot path always uses presigned orders.
    #[allow(dead_code)]
    amount_usdc: f64,
    #[allow(dead_code)]
    size_shares: f64,
    #[allow(dead_code)]
    limit_price: f64,
    #[allow(dead_code)]
    time_in_force: OrderTimeInForce,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            mode: DispatchMode::Noop,
            clob_host: "https://clob.polymarket.com".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            api_passphrase: String::new(),
            funder: String::new(),
            signature_type: 0,
            chain_id: 137,
            presign_enabled: false,
            presign_private_key: String::new(),
            presign_startup_warm_timeout_seconds: 5.0,
            amount_usdc: 5.0,
            size_shares: 5.0,
            limit_price: 0.52,
            time_in_force: OrderTimeInForce::FAK,
        }
    }
}

struct LiveWorkerHandle {
    command_tx: flume::Sender<LiveWorkerCommand>,
    patch_tx: flume::Sender<PatchPayload>,
    join: Option<JoinHandle<()>>,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
}

pub(crate) struct PatchPayload {
    pub(crate) plan_json: String,
    pub(crate) new_presigned: HashMap<String, SdkSignedOrder>,
    pub(crate) new_templates: HashMap<String, crate::dispatch::OrderRequestData>,
}

struct SubmitterHandle {
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
    join: Option<JoinHandle<()>>,
    health: Arc<Mutex<SubmitterHealth>>,
}

#[derive(Clone)]
enum LiveWorkerCommand {
    Stop,
    SetCandidateSubscriptions(Vec<String>),
}

use crate::baseball::types::NativeMlbEngine;
use crate::soccer::types::NativeSoccerEngine;

#[cfg_attr(feature = "python-extension", pyclass)]
struct NativeHotPathRuntime {
    engine: Option<SportEngine>,
    running: bool,
    subscriptions: Vec<String>,
    runtime_cfg: RuntimeStartConfig,
    dispatch_cfg: DispatchConfig,
    presign_templates: Vec<crate::dispatch::PresignTemplateData>,
    live_worker: Option<LiveWorkerHandle>,
    submitter: Option<SubmitterHandle>,
    cached_sdk_client: Option<SdkClient<SdkAuthenticatedState<SdkAuthNormal>>>,
    cached_signer: Option<CachedSigner>,
}

pub(crate) struct MergePlanResult {
    pub(crate) new_tokens: usize,
    pub(crate) new_targets: usize,
}

#[cfg(feature = "python-extension")]
#[pymodule]
fn polybot2_native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeMlbEngine>()?;
    m.add_class::<NativeHotPathRuntime>()?;
    Ok(())
}
