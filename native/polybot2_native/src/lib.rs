mod decode;
mod dispatch;
mod engine;
mod eval;
mod parse;
mod replay;
mod runtime;
mod telemetry;
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
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Builder as TokioBuilder;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::sleep as tokio_sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::decode::{parse_json_bytes, parse_json_text};

#[derive(Clone)]
struct TotalsTarget {
    line: f64,
    token_id: String,
    condition_id: String,
    strategy_key: String,
}

#[derive(Clone)]
struct NrfiTarget {
    token_id: String,
    condition_id: String,
    strategy_key: String,
}

#[derive(Clone)]
struct FinalTarget {
    token_id: String,
    condition_id: String,
    strategy_key: String,
}

#[derive(Clone)]
struct SpreadTarget {
    line: f64,
    token_id: String,
    condition_id: String,
    strategy_key: String,
}

#[derive(Clone, Default)]
struct Tick {
    universal_id: String,
    action: String,
    recv_monotonic_ns: i64,
    goals_home: Option<i64>,
    goals_away: Option<i64>,
    inning_number: Option<i64>,
    inning_half: String,
    outs: Option<i64>,
    balls: Option<i64>,
    strikes: Option<i64>,
    runner_on_first: Option<bool>,
    runner_on_second: Option<bool>,
    runner_on_third: Option<bool>,
    match_completed: Option<bool>,
    period: String,
    game_state: String,
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
    action: String,
    goals_home: Option<i64>,
    goals_away: Option<i64>,
    inning_number: Option<i64>,
    inning_half: String,
    outs: Option<i64>,
    balls: Option<i64>,
    strikes: Option<i64>,
    runner_on_first: Option<bool>,
    runner_on_second: Option<bool>,
    runner_on_third: Option<bool>,
    match_completed: Option<bool>,
    period: String,
}

#[derive(Clone, Default)]
struct GameState {
    home: Option<i64>,
    away: Option<i64>,
    total: Option<i64>,
    prev_total: Option<i64>,
    inning_number: Option<i64>,
    inning_half: String,
    outs: Option<i64>,
    balls: Option<i64>,
    strikes: Option<i64>,
    runner_on_first: Option<bool>,
    runner_on_second: Option<bool>,
    runner_on_third: Option<bool>,
    match_completed: Option<bool>,
    game_state: String,
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct DecisionSig {
    token_id: String,
    side: String,
    time_in_force: String,
}

#[derive(Clone)]
struct Intent {
    strategy_key: String,
    token_id: String,
    side: String,
    notional_usdc: f64,
    limit_price: f64,
    time_in_force: String,
    condition_id: String,
    source_universal_id: String,
    chain_id: String,
    reason: String,
    market_type: String,
    outcome_semantic: String,
}

#[derive(Clone, Default)]
struct ObserveSignal {
    event_type: String,
    game_id: String,
    payload: Value,
}

#[derive(Default)]
struct ProcessResult {
    decision: String,
    reason: String,
    intents: Vec<Intent>,
    observe_signals: Vec<ObserveSignal>,
    drops_cooldown: i64,
    drops_debounce: i64,
    drops_one_shot: i64,
    decision_non_material: i64,
    decision_no_action: i64,
}

#[derive(Default, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RuntimeStartConfig {
    provider: Option<String>,
    league: Option<String>,
    dedup_ttl_seconds: Option<f64>,
    decision_cooldown_seconds: Option<f64>,
    decision_debounce_seconds: Option<f64>,
    subscribe_lead_minutes: Option<i64>,
    subscription_refresh_seconds: Option<f64>,
    amount_usdc: Option<f64>,
    limit_price: Option<f64>,
    time_in_force: Option<String>,
    live_enabled: Option<bool>,
    reconnect_sleep_seconds: Option<f64>,
    kalstrop_ws_url: Option<String>,
    kalstrop_client_id: Option<String>,
    kalstrop_shared_secret_raw: Option<String>,
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
    address: Option<String>,
    chain_id: Option<i64>,
    presign_enabled: Option<bool>,
    presign_private_key: Option<String>,
    presign_pool_target_per_key: Option<i64>,
    presign_refill_batch_size: Option<i64>,
    presign_refill_interval_seconds: Option<f64>,
    presign_startup_warm_timeout_seconds: Option<f64>,
    active_order_refresh_interval_seconds: Option<f64>,
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
    presign_refill_batch_size: i64,
    presign_refill_interval_seconds: f64,
    presign_startup_warm_timeout_seconds: f64,
    active_order_refresh_interval_seconds: f64,
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
            presign_pool_target_per_key: 8,
            presign_refill_batch_size: 4,
            presign_refill_interval_seconds: 0.02,
            presign_startup_warm_timeout_seconds: 5.0,
            active_order_refresh_interval_seconds: 0.25,
        }
    }
}

struct LiveWorkerHandle {
    command_tx: tokio_mpsc::UnboundedSender<LiveWorkerCommand>,
    join: Option<JoinHandle<()>>,
    subscriptions: Arc<RwLock<Vec<String>>>,
    health: Arc<Mutex<RuntimeHealth>>,
    telemetry_worker: Option<crate::telemetry::TelemetryWorkerHandle>,
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
    amount_usdc: f64,
    limit_price: f64,
    time_in_force: String,
    over_targets_by_game: HashMap<String, Vec<TotalsTarget>>,
    over_lines_by_game: HashMap<String, Vec<f64>>,
    under_targets_by_game: HashMap<String, Vec<TotalsTarget>>,
    nrfi_targets_by_game: HashMap<String, HashMap<String, NrfiTarget>>,
    moneyline_by_game: HashMap<String, HashMap<String, FinalTarget>>,
    spreads_by_game: HashMap<String, HashMap<String, Vec<SpreadTarget>>>,
    unknown_by_game: HashMap<String, i64>,
    kickoff_ts_by_game: HashMap<String, i64>,
    home_team_by_game: HashMap<String, String>,
    away_team_by_game: HashMap<String, String>,
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
