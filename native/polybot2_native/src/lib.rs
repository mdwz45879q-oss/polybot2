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

#[derive(Clone)]
struct TargetEntry {
    token_id: String,
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

#[derive(Clone, Default)]
struct DeltaEvent {
    universal_id: String,
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

#[derive(Clone, Eq, PartialEq, Hash)]
struct DecisionSig {
    token_id: String,
}

#[derive(Clone)]
struct Intent {
    strategy_key: String,
    token_id: String,
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
    presign_pool_target_per_key: Option<i64>,
    presign_startup_warm_timeout_seconds: Option<f64>,
}

#[derive(Default)]
struct RuntimeHealth {
    running: bool,
    reconnects: i64,
    last_error: String,
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
    presign_pool_target_per_key: i64,
    presign_startup_warm_timeout_seconds: f64,
    amount_usdc: f64,
    size_shares: f64,
    limit_price: f64,
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
            presign_pool_target_per_key: 1,
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
    targets: HashMap<String, TargetEntry>,
    games_with_totals: HashSet<String>,
    games_with_nrfi: HashSet<String>,
    games_with_final: HashSet<String>,
    under_lines_by_game: HashMap<String, Vec<f64>>,
    spread_lines_by_game: HashMap<String, Vec<(&'static str, f64)>>,
    strategy_keys_by_game: HashMap<String, Vec<String>>,
    kickoff_ts_by_game: HashMap<String, i64>,
    token_ids_by_game: HashMap<String, Vec<String>>,
    totals_final_under_emitted: HashSet<String>,
    nrfi_resolved_games: HashSet<String>,
    nrfi_first_inning_observed: HashSet<String>,
    final_resolved_games: HashSet<String>,
    rows: HashMap<String, StateRow>,
    game_states: HashMap<String, GameState>,
    last_emit_ns: HashMap<String, i64>,
    last_signature: HashMap<String, DecisionSig>,
    attempted_strategy_keys: HashSet<String>,
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
}

#[pymodule]
fn polybot2_native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeMlbEngine>()?;
    m.add_class::<NativeHotPathRuntime>()?;
    Ok(())
}
