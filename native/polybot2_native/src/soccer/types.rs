//! Soccer-specific types. Shared types (GameIdx, TargetIdx, etc.) live in
//! the crate root (lib.rs).

use crate::{
    GameIdx, InlineStr, Intent, OverLine, SpreadSide, TargetIdx, TargetRegistry, TargetSlot,
    TokenIdx, TokenSlot,
};
use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct SoccerGameTargets {
    // Totals — identical structure to baseball
    pub(crate) over_lines: Vec<OverLine>,
    pub(crate) under_lines: Vec<OverLine>,

    // Three-way moneyline: 3 markets × Yes/No
    pub(crate) moneyline_home_yes: Option<TargetIdx>,
    pub(crate) moneyline_home_no: Option<TargetIdx>,
    pub(crate) moneyline_away_yes: Option<TargetIdx>,
    pub(crate) moneyline_away_no: Option<TargetIdx>,
    pub(crate) moneyline_draw_yes: Option<TargetIdx>,
    pub(crate) moneyline_draw_no: Option<TargetIdx>,

    // Spreads — covers + not-covers per side+line
    pub(crate) spreads: Vec<SpreadSlot>,

    // Both Teams to Score
    pub(crate) btts_yes: Option<TargetIdx>,
    pub(crate) btts_no: Option<TargetIdx>,

    // Total corners (over/under) — same OverLine pattern as goal totals
    pub(crate) corner_over_lines: Vec<OverLine>,
    pub(crate) corner_under_lines: Vec<OverLine>,

    // Halftime result (three-way, same structure as moneyline)
    pub(crate) halftime_home_yes: Option<TargetIdx>,
    pub(crate) halftime_home_no: Option<TargetIdx>,
    pub(crate) halftime_away_yes: Option<TargetIdx>,
    pub(crate) halftime_away_no: Option<TargetIdx>,
    pub(crate) halftime_draw_yes: Option<TargetIdx>,
    pub(crate) halftime_draw_no: Option<TargetIdx>,

    // Exact score — YES/NO per predicted scoreline + "any other score"
    pub(crate) exact_scores: Vec<ExactScoreSlot>,
    pub(crate) any_other_score_yes: Option<TargetIdx>,
    pub(crate) any_other_score_no: Option<TargetIdx>,
}

#[derive(Clone, Default)]
pub(crate) struct ExactScoreSlot {
    pub(crate) home_pred: i64,
    pub(crate) away_pred: i64,
    pub(crate) yes_idx: Option<TargetIdx>,
    pub(crate) no_idx: Option<TargetIdx>,
}

#[derive(Clone)]
pub(crate) struct SpreadSlot {
    pub(crate) side: SpreadSide,
    pub(crate) line: f64,
    pub(crate) covers_idx: Option<TargetIdx>,
    pub(crate) not_covers_idx: Option<TargetIdx>,
}

#[derive(Clone, Default)]
pub(crate) struct SoccerStateRow {
    pub(crate) home_score_raw: InlineStr<4>,
    pub(crate) away_score_raw: InlineStr<4>,
    pub(crate) free_text_raw: InlineStr<32>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SoccerGameState {
    pub(crate) home: Option<i64>,
    pub(crate) away: Option<i64>,
    pub(crate) total: Option<i64>,
    pub(crate) prev_total: Option<i64>,
    pub(crate) half: &'static str,
    pub(crate) match_completed: Option<bool>,
    pub(crate) game_state: &'static str,
    pub(crate) total_corners: Option<i64>,
    pub(crate) prev_total_corners: Option<i64>,
}

/// Stack-only result from the live WS tick path.
pub(crate) struct SoccerLiveTickResult {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: SoccerGameState,
    pub(crate) intents: smallvec::SmallVec<[Intent; 4]>,
}

/// Per-game BoltOdds state row for frame-level deduplication.
/// Separate from `SoccerStateRow` because BoltOdds provides integer fields
/// directly (goals, corners) plus a period string, while Kalstrop V1 uses
/// raw score strings and freeText.
#[derive(Clone, Default)]
pub(crate) struct BoltOddsSoccerStateRow {
    pub(crate) goals_a: i64,
    pub(crate) goals_b: i64,
    pub(crate) corners_a: i64,
    pub(crate) corners_b: i64,
    pub(crate) period: InlineStr<24>,
}

#[derive(Clone)]
pub(crate) struct NativeSoccerEngine {
    pub(crate) game_id_to_idx: FxHashMap<String, GameIdx>,
    pub(crate) game_ids: Vec<String>,
    pub(crate) game_targets: Vec<SoccerGameTargets>,
    pub(crate) target_slots: Vec<TargetSlot>,
    pub(crate) tokens: Vec<TokenSlot>,
    pub(crate) token_id_to_idx: FxHashMap<String, TokenIdx>,
    pub(crate) strategy_keys: HashSet<String>,
    pub(crate) registry: Option<Arc<TargetRegistry>>,
    pub(crate) kickoff_ts: Vec<Option<i64>>,
    pub(crate) token_ids_by_game: Vec<Vec<String>>,

    pub(crate) has_totals: Vec<bool>,
    pub(crate) has_moneyline: Vec<bool>,
    pub(crate) has_btts: Vec<bool>,
    pub(crate) has_corners: Vec<bool>,
    pub(crate) has_halftime: Vec<bool>,
    pub(crate) has_exact_score: Vec<bool>,

    pub(crate) rows: Vec<Option<SoccerStateRow>>,
    pub(crate) game_states: Vec<SoccerGameState>,

    pub(crate) totals_final_under_emitted: Vec<bool>,
    pub(crate) final_resolved_games: Vec<bool>,
    pub(crate) btts_resolved_games: Vec<bool>,
    pub(crate) corners_final_under_emitted: Vec<bool>,
    pub(crate) halftime_resolved: Vec<bool>,
    pub(crate) exact_score_resolved: Vec<bool>,

    /// BoltOdds dedup rows, indexed by `GameIdx`. `None` means no BoltOdds
    /// frame has been received for this game yet.
    pub(crate) boltodds_rows: Vec<Option<BoltOddsSoccerStateRow>>,
}
