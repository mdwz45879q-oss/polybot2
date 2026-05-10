mod baseball;
mod soccer;
mod dispatch;
pub(crate) mod boltodds_frame_pipeline;
pub(crate) mod boltodds_types;
pub(crate) mod fast_extract;
mod kalstrop_types;
mod log_writer;
mod runtime;
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
use polymarket_client_sdk_v2::clob::Client as SdkClient;
use polymarket_client_sdk_v2::clob::types::SignedOrder as SdkSignedOrder;
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
        Self { buf: [0u8; N], len: 0 }
    }
    pub fn from_str(s: &str) -> Self {
        let bytes = s.as_bytes();
        let copy_len = bytes.len().min(N);
        let mut this = Self { buf: [0u8; N], len: copy_len as u8 };
        this.buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        this
    }
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len as usize]) }
    }
}

impl<const N: usize> Default for InlineStr<N> {
    fn default() -> Self { Self::new() }
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
            Self::Baseball(e) => e.active_subscriptions_for_candidates(candidates, now_ts_utc, subscribe_lead_minutes),
            Self::Soccer(e) => e.active_subscriptions_for_candidates(candidates, now_ts_utc, subscribe_lead_minutes),
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

const SUBMITTER_LATENCY_WINDOW: usize = 2048;
const SUBMITTER_BUCKET_COUNT: usize = 3;
const SUBMITTER_CHUNK_MAX: usize = 15;
const SUBMITTER_BUCKET_N2_15_MAX: usize = 15;

pub(crate) struct LatencyRing {
    values: Box<[u64; SUBMITTER_LATENCY_WINDOW]>,
    len: usize,
    next: usize,
}

impl Default for LatencyRing {
    fn default() -> Self {
        Self {
            values: Box::new([0u64; SUBMITTER_LATENCY_WINDOW]),
            len: 0,
            next: 0,
        }
    }
}

impl LatencyRing {
    fn push(&mut self, value: u64) {
        self.values[self.next] = value;
        self.next = (self.next + 1) % SUBMITTER_LATENCY_WINDOW;
        if self.len < SUBMITTER_LATENCY_WINDOW {
            self.len += 1;
        }
    }

    fn stats_json(&self) -> Value {
        if self.len == 0 {
            return json!({
                "count": 0,
                "min": 0,
                "max": 0,
                "p50": 0,
                "p95": 0,
                "p99": 0,
            });
        }

        let mut data = if self.len < SUBMITTER_LATENCY_WINDOW {
            self.values[..self.len].to_vec()
        } else {
            let mut out = Vec::with_capacity(SUBMITTER_LATENCY_WINDOW);
            out.extend_from_slice(&self.values[self.next..]);
            out.extend_from_slice(&self.values[..self.next]);
            out
        };
        data.sort_unstable();
        let count = data.len();
        let p50 = data[count / 2];
        let p95 = data[(count * 95) / 100];
        let p99 = data[(count * 99) / 100];

        json!({
            "count": count as u64,
            "min": data[0],
            "max": data[count - 1],
            "p50": p50,
            "p95": p95,
            "p99": p99,
        })
    }
}

pub(crate) struct SubmitterLatencyMetrics {
    pop_to_task_start_ns: [LatencyRing; SUBMITTER_BUCKET_COUNT],
    task_prep_ns: [LatencyRing; SUBMITTER_BUCKET_COUNT],
    permit_wait_ns: [LatencyRing; SUBMITTER_BUCKET_COUNT],
    sdk_call_total_ns: [LatencyRing; SUBMITTER_BUCKET_COUNT],
    batch_total_ns: [LatencyRing; SUBMITTER_BUCKET_COUNT],
    chunk_sdk_call_total_ns: [LatencyRing; SUBMITTER_CHUNK_MAX],
}

impl Default for SubmitterLatencyMetrics {
    fn default() -> Self {
        Self {
            pop_to_task_start_ns: std::array::from_fn(|_| LatencyRing::default()),
            task_prep_ns: std::array::from_fn(|_| LatencyRing::default()),
            permit_wait_ns: std::array::from_fn(|_| LatencyRing::default()),
            sdk_call_total_ns: std::array::from_fn(|_| LatencyRing::default()),
            batch_total_ns: std::array::from_fn(|_| LatencyRing::default()),
            chunk_sdk_call_total_ns: std::array::from_fn(|_| LatencyRing::default()),
        }
    }
}

impl SubmitterLatencyMetrics {
    fn bucket_idx(batch_len: usize) -> usize {
        if batch_len <= 1 {
            0
        } else if batch_len <= SUBMITTER_BUCKET_N2_15_MAX {
            1
        } else {
            2
        }
    }

    fn record_pop_to_task_start(&mut self, batch_len: usize, ns: u64) {
        let idx = Self::bucket_idx(batch_len);
        self.pop_to_task_start_ns[idx].push(ns);
    }

    fn record_task_prep(&mut self, batch_len: usize, ns: u64) {
        let idx = Self::bucket_idx(batch_len);
        self.task_prep_ns[idx].push(ns);
    }

    fn record_permit_wait(&mut self, batch_len: usize, ns: u64) {
        let idx = Self::bucket_idx(batch_len);
        self.permit_wait_ns[idx].push(ns);
    }

    fn record_sdk_call_total(&mut self, batch_len: usize, ns: u64) {
        let idx = Self::bucket_idx(batch_len);
        self.sdk_call_total_ns[idx].push(ns);
    }

    fn record_batch_total(&mut self, batch_len: usize, ns: u64) {
        let idx = Self::bucket_idx(batch_len);
        self.batch_total_ns[idx].push(ns);
    }

    fn record_chunk_sdk_call_total(&mut self, chunk_len: usize, ns: u64) {
        if chunk_len == 0 || chunk_len > SUBMITTER_CHUNK_MAX {
            return;
        }
        self.chunk_sdk_call_total_ns[chunk_len - 1].push(ns);
    }

    fn snapshot_json(&self) -> Value {
        let bucket_n1 = 0usize;
        let bucket_n2_15 = 1usize;
        let bucket_n16_plus = 2usize;
        let mut chunk_map = serde_json::Map::new();
        for (i, ring) in self.chunk_sdk_call_total_ns.iter().enumerate() {
            chunk_map.insert((i + 1).to_string(), ring.stats_json());
        }

        json!({
            "window": SUBMITTER_LATENCY_WINDOW,
            "buckets": {
                "n1": {
                    "pop_to_task_start_ns": self.pop_to_task_start_ns[bucket_n1].stats_json(),
                    "task_prep_ns": self.task_prep_ns[bucket_n1].stats_json(),
                    "permit_wait_ns": self.permit_wait_ns[bucket_n1].stats_json(),
                    "sdk_call_total_ns": self.sdk_call_total_ns[bucket_n1].stats_json(),
                    "batch_total_ns": self.batch_total_ns[bucket_n1].stats_json(),
                },
                "n2_15": {
                    "pop_to_task_start_ns": self.pop_to_task_start_ns[bucket_n2_15].stats_json(),
                    "task_prep_ns": self.task_prep_ns[bucket_n2_15].stats_json(),
                    "permit_wait_ns": self.permit_wait_ns[bucket_n2_15].stats_json(),
                    "sdk_call_total_ns": self.sdk_call_total_ns[bucket_n2_15].stats_json(),
                    "batch_total_ns": self.batch_total_ns[bucket_n2_15].stats_json(),
                },
                "n16_plus": {
                    "pop_to_task_start_ns": self.pop_to_task_start_ns[bucket_n16_plus].stats_json(),
                    "task_prep_ns": self.task_prep_ns[bucket_n16_plus].stats_json(),
                    "permit_wait_ns": self.permit_wait_ns[bucket_n16_plus].stats_json(),
                    "sdk_call_total_ns": self.sdk_call_total_ns[bucket_n16_plus].stats_json(),
                    "batch_total_ns": self.batch_total_ns[bucket_n16_plus].stats_json(),
                }
            },
            "chunk_len": chunk_map,
        })
    }
}

pub(crate) struct SubmitterHealth {
    pub(crate) running: bool,
    pub(crate) last_error: String,
    pub(crate) posted_ok: i64,
    pub(crate) posted_err: i64,
    pub(crate) latency_metrics: Box<SubmitterLatencyMetrics>,
}

impl Default for SubmitterHealth {
    fn default() -> Self {
        Self {
            running: false,
            last_error: String::new(),
            posted_ok: 0,
            posted_err: 0,
            latency_metrics: Box::new(SubmitterLatencyMetrics::default()),
        }
    }
}

impl SubmitterHealth {
    fn record_pop_to_task_start_ns(&mut self, batch_len: usize, ns: u64) {
        self.latency_metrics.record_pop_to_task_start(batch_len, ns);
    }

    fn record_task_prep_ns(&mut self, batch_len: usize, ns: u64) {
        self.latency_metrics.record_task_prep(batch_len, ns);
    }

    fn record_permit_wait_ns(&mut self, batch_len: usize, ns: u64) {
        self.latency_metrics.record_permit_wait(batch_len, ns);
    }

    fn record_sdk_call_total_ns(&mut self, batch_len: usize, ns: u64) {
        self.latency_metrics.record_sdk_call_total(batch_len, ns);
    }

    fn record_batch_total_ns(&mut self, batch_len: usize, ns: u64) {
        self.latency_metrics.record_batch_total(batch_len, ns);
    }

    fn record_chunk_sdk_call_total_ns(&mut self, chunk_len: usize, ns: u64) {
        self.latency_metrics.record_chunk_sdk_call_total(chunk_len, ns);
    }

    fn latency_metrics_snapshot_json(&self) -> Value {
        self.latency_metrics.snapshot_json()
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
    submit_notify: Arc<tokio::sync::Notify>,
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
