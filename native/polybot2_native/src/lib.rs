mod dispatch;
mod engine;
mod eval;
mod kalstrop_types;
mod log_writer;
mod parse;
mod replay;
mod runtime;
mod ws;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use tokio::runtime::Builder as TokioBuilder;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::sleep as tokio_sleep;

use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct GameIdx(u16);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct TargetIdx(pub(crate) u16);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct TokenIdx(pub(crate) u16);

#[derive(Clone)]
struct TokenSlot {
    token_id: String,
}

/// Read-only mapping of `TargetIdx → TargetSlot` and `TokenIdx → TokenSlot`,
/// shared via `Arc` between the WS-thread `DispatchHandle` and the submitter
/// thread. Built once in `engine.load_plan` and cloned to both halves at
/// runtime startup.
struct TargetRegistry {
    tokens: Vec<TokenSlot>,
    targets: Vec<TargetSlot>,
}

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

#[derive(Clone, Default)]
struct GameTargets {
    over_lines: Vec<OverLine>,
    under_lines: Vec<OverLine>,
    nrfi_yes: Option<TargetIdx>,
    nrfi_no: Option<TargetIdx>,
    moneyline_home: Option<TargetIdx>,
    moneyline_away: Option<TargetIdx>,
    spreads: Vec<(SpreadSide, f64, TargetIdx)>,
}

impl GameTargets {
    #[allow(dead_code)]
    fn all_target_indices(&self) -> Vec<TargetIdx> {
        let mut out = Vec::with_capacity(
            self.over_lines.len() + self.under_lines.len() + 4 + self.spreads.len(),
        );
        for ol in &self.over_lines {
            out.push(ol.target_idx);
        }
        for ol in &self.under_lines {
            out.push(ol.target_idx);
        }
        if let Some(t) = self.nrfi_yes {
            out.push(t);
        }
        if let Some(t) = self.nrfi_no {
            out.push(t);
        }
        if let Some(t) = self.moneyline_home {
            out.push(t);
        }
        if let Some(t) = self.moneyline_away {
            out.push(t);
        }
        for &(_, _, t) in &self.spreads {
            out.push(t);
        }
        out
    }
}

#[derive(Clone)]
struct TargetSlot {
    token_idx: TokenIdx,
    strategy_key: String,
}

struct RawIntent {
    target_idx: TargetIdx,
}

#[derive(Clone, Default)]
struct Tick {
    universal_id: String,
    action: &'static str,
    recv_monotonic_ns: i64,
    goals_home: Option<i64>,
    goals_away: Option<i64>,
    inning_number: Option<i64>,
    inning_half: &'static str,
    match_completed: Option<bool>,
    game_state: &'static str,
}

#[derive(Clone, Copy, Default)]
struct DeltaEvent {
    recv_monotonic_ns: i64,
    material_change: bool,
    goal_delta_home: i64,
    goal_delta_away: i64,
}

#[derive(Clone, Default)]
struct StateRow {
    seen_monotonic_ns: i64,
    action: &'static str,
    goals_home: Option<i64>,
    goals_away: Option<i64>,
    inning_number: Option<i64>,
    inning_half: &'static str,
    match_completed: Option<bool>,
}

#[derive(Clone, Copy, Default)]
struct GameState {
    home: Option<i64>,
    away: Option<i64>,
    total: Option<i64>,
    prev_total: Option<i64>,
    inning_number: Option<i64>,
    inning_half: &'static str,
    match_completed: Option<bool>,
    game_state: &'static str,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct DecisionSig {
    token_idx: TokenIdx,
}

#[derive(Clone, Copy)]
struct Intent {
    target_idx: TargetIdx,
}

struct TickResult {
    game_id: String,
    state: GameState,
    intents: Vec<Intent>,
    material: bool,
}

#[derive(Default, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RuntimeStartConfig {
    dedup_ttl_seconds: Option<f64>,
    decision_cooldown_seconds: Option<f64>,
    decision_debounce_seconds: Option<f64>,
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

#[derive(Default)]
pub(crate) struct SubmitterHealth {
    pub(crate) running: bool,
    pub(crate) last_error: String,
    pub(crate) posted_ok: i64,
    pub(crate) posted_err: i64,
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
    command_tx: tokio_mpsc::UnboundedSender<LiveWorkerCommand>,
    join: Option<JoinHandle<()>>,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
}

struct SubmitterHandle {
    submit_tx: tokio_mpsc::UnboundedSender<crate::dispatch::SubmitWork>,
    join: Option<JoinHandle<()>>,
    health: Arc<Mutex<SubmitterHealth>>,
}

#[derive(Clone)]
enum LiveWorkerCommand {
    Stop,
    SetCandidateSubscriptions(Vec<String>),
}

#[pyclass]
#[derive(Clone)]
struct NativeMlbEngine {
    dedup_ttl_ns: i64,
    decision_cooldown_ns: i64,
    decision_debounce_ns: i64,

    game_id_to_idx: HashMap<String, GameIdx>,
    game_ids: Vec<String>,
    game_targets: Vec<GameTargets>,
    target_slots: Vec<TargetSlot>,
    tokens: Vec<TokenSlot>,
    token_id_to_idx: HashMap<String, TokenIdx>,
    /// Built once at the end of `load_plan`. Cloned via `clone_registry()`
    /// for cross-thread sharing with `DispatchHandle` and `OrderSubmitter`.
    registry: Option<Arc<TargetRegistry>>,
    kickoff_ts: Vec<Option<i64>>,
    token_ids_by_game: Vec<Vec<String>>,

    has_totals: Vec<bool>,
    has_nrfi: Vec<bool>,
    has_final: Vec<bool>,

    rows: Vec<Option<StateRow>>,
    game_states: Vec<GameState>,

    totals_final_under_emitted: Vec<bool>,
    nrfi_resolved_games: Vec<bool>,
    nrfi_first_inning_observed: Vec<bool>,
    final_resolved_games: Vec<bool>,

    attempted: Vec<bool>,
    last_emit_ns: Vec<i64>,
    last_signature: Vec<Option<DecisionSig>>,
}

#[pyclass]
struct NativeHotPathRuntime {
    engine: Option<NativeMlbEngine>,
    running: bool,
    subscriptions: Vec<String>,
    runtime_cfg: RuntimeStartConfig,
    dispatch_cfg: DispatchConfig,
    presign_templates: Vec<crate::dispatch::PresignTemplateData>,
    live_worker: Option<LiveWorkerHandle>,
    submitter: Option<SubmitterHandle>,
}

#[pymodule]
fn polybot2_native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeMlbEngine>()?;
    m.add_class::<NativeHotPathRuntime>()?;
    Ok(())
}
