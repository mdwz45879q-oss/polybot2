//! MOBA-specific types for LoL/Dota2. Shared types (GameIdx, TargetIdx,
//! TokenIdx, TargetRegistry, OverLine, SpreadSide, Intent, etc.) live in
//! the crate root (lib.rs).

use crate::{
    GameIdx, InlineStr, Intent, OverLine, SpreadSide, TargetIdx, TargetRegistry, TargetSlot,
    TokenIdx, TokenSlot,
};
use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Per-game spread slot (own type, no cross-sport dependency)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct SpreadSlot {
    pub(crate) side: SpreadSide,
    pub(crate) line: f64,
    pub(crate) covers_idx: Option<TargetIdx>,
    pub(crate) not_covers_idx: Option<TargetIdx>,
}

// ---------------------------------------------------------------------------
// Per-game target slots
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub(crate) struct MobaGameTargets {
    /// Map moneyline (child_moneyline): per-map (home, away) target indices.
    /// Index 0 = map 1, index 1 = map 2, etc.
    pub(crate) map_moneyline: Vec<(Option<TargetIdx>, Option<TargetIdx>)>,
    /// Match moneyline
    pub(crate) moneyline_home: Option<TargetIdx>,
    pub(crate) moneyline_away: Option<TargetIdx>,
    /// Totals (maps played): sorted by half_int
    pub(crate) over_lines: Vec<OverLine>,
    pub(crate) under_lines: Vec<OverLine>,
    /// Map handicap (spreads on map margin)
    pub(crate) map_handicaps: Vec<SpreadSlot>,
}

// ---------------------------------------------------------------------------
// Live game state
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
pub(crate) struct MobaGameState {
    pub(crate) maps_home: Option<i64>,
    pub(crate) maps_away: Option<i64>,
    pub(crate) prev_maps_home: Option<i64>,
    pub(crate) prev_maps_away: Option<i64>,
    pub(crate) total_maps: i64,
    pub(crate) prev_total_maps: Option<i64>,
    pub(crate) match_completed: bool,
    pub(crate) game_state: &'static str,
}

// ---------------------------------------------------------------------------
// Dedup row (maps only — no round-level data in MOBA)
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub(crate) struct MobaStateRow {
    pub(crate) maps_home_raw: InlineStr<4>,
    pub(crate) maps_away_raw: InlineStr<4>,
}

// ---------------------------------------------------------------------------
// Live tick result
// ---------------------------------------------------------------------------

pub(crate) struct MobaLiveTickResult {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: MobaGameState,
    pub(crate) intents: smallvec::SmallVec<[Intent; 32]>,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct NativeMobaEngine {
    // Game indexing
    pub(crate) game_id_to_idx: FxHashMap<String, GameIdx>,
    pub(crate) game_ids: Vec<String>,
    pub(crate) game_leagues: Vec<Arc<str>>,
    pub(crate) game_targets: Vec<MobaGameTargets>,
    pub(crate) target_slots: Vec<TargetSlot>,
    pub(crate) tokens: Vec<TokenSlot>,
    pub(crate) token_id_to_idx: FxHashMap<String, TokenIdx>,
    pub(crate) strategy_keys: HashSet<String>,
    pub(crate) registry: Option<Arc<TargetRegistry>>,
    pub(crate) kickoff_ts: Vec<Option<i64>>,
    pub(crate) token_ids_by_game: Vec<Vec<String>>,

    // Market flags (per-game)
    pub(crate) has_moneyline: Vec<bool>,
    pub(crate) has_totals: Vec<bool>,
    pub(crate) has_child_moneyline: Vec<bool>,
    pub(crate) has_map_handicap: Vec<bool>,

    // State (per-game)
    pub(crate) rows: Vec<Option<MobaStateRow>>,
    pub(crate) game_states: Vec<MobaGameState>,

    // Match format (per-game — from BO parsing, stored as sets_to_win in plan JSON)
    pub(crate) maps_to_win: Vec<i64>,

    // Resolution flags (per-game)
    pub(crate) final_resolved_games: Vec<bool>,
    pub(crate) totals_under_emitted: Vec<bool>,
    pub(crate) map_handicap_early_emitted: Vec<bool>,
    /// Per-game, per-map: whether the map winner has been resolved.
    /// Used for cold-start tombstoning and preventing double-fire.
    pub(crate) map_winner_resolved: Vec<Vec<bool>>,
}
