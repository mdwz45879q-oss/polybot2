use std::collections::HashSet;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::{
    GameIdx, InlineStr, Intent, OverLine, SpreadSide, TargetIdx, TargetRegistry, TargetSlot,
    TokenIdx, TokenSlot,
};

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
pub(crate) struct TennisGameTargets {
    // Set totals (total sets played) — over/under
    pub(crate) set_total_over_lines: Vec<OverLine>,
    pub(crate) set_total_under_lines: Vec<OverLine>,

    // Moneyline (match winner, two-way: home/away, no draw in tennis)
    pub(crate) moneyline_home: Option<TargetIdx>,
    pub(crate) moneyline_away: Option<TargetIdx>,

    // First-set winner (two-way)
    pub(crate) first_set_winner_home: Option<TargetIdx>,
    pub(crate) first_set_winner_away: Option<TargetIdx>,

    // Set handicap (like spreads, indexed by side + line)
    pub(crate) set_handicaps: Vec<SpreadSlot>,

}

// ---------------------------------------------------------------------------
// Per-game live state
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
#[allow(dead_code)] // Fields read by log output and future observer scoreboard.
pub(crate) struct TennisGameState {
    pub(crate) sets_home: i64,
    pub(crate) sets_away: i64,
    pub(crate) games_home: i64,        // games in current set (home)
    pub(crate) games_away: i64,        // games in current set (away)
    pub(crate) total_games: i64,              // cumulative games across all sets + current
    pub(crate) prev_total_games: Option<i64>, // None on first tick (cold-start safe)
    pub(crate) first_set_games: i64,          // frozen after set 1 ends
    pub(crate) prev_first_set_games: Option<i64>,
    pub(crate) total_sets: i64,               // sets completed so far
    pub(crate) prev_total_sets: Option<i64>,
    pub(crate) current_set: i64,       // 1, 2, 3, ...
    pub(crate) match_completed: bool,
    pub(crate) first_set_completed: bool,
    pub(crate) game_state: &'static str,
}

// ---------------------------------------------------------------------------
// Dedup row (stack-only, game-level granularity)
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub(crate) struct TennisStateRow {
    pub(crate) sets_home: InlineStr<2>,
    pub(crate) sets_away: InlineStr<2>,
    pub(crate) games_home: InlineStr<2>,
    pub(crate) games_away: InlineStr<2>,
    pub(crate) free_text_raw: InlineStr<32>,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct NativeTennisEngine {
    // Game indexing
    pub(crate) game_id_to_idx: FxHashMap<String, GameIdx>,
    pub(crate) game_ids: Vec<String>,
    pub(crate) game_leagues: Vec<Arc<str>>,

    // Target routing (per-game)
    pub(crate) game_targets: Vec<TennisGameTargets>,
    pub(crate) target_slots: Vec<TargetSlot>,
    pub(crate) tokens: Vec<TokenSlot>,
    pub(crate) token_id_to_idx: FxHashMap<String, TokenIdx>,
    pub(crate) strategy_keys: HashSet<String>,
    pub(crate) registry: Option<Arc<TargetRegistry>>,

    // Scheduling metadata
    pub(crate) kickoff_ts: Vec<Option<i64>>,
    pub(crate) token_ids_by_game: Vec<Vec<String>>,

    // Market type flags (per-game)
    pub(crate) has_set_totals: Vec<bool>,
    pub(crate) has_moneyline: Vec<bool>,
    pub(crate) has_first_set_winner: Vec<bool>,
    pub(crate) has_set_handicap: Vec<bool>,

    // Live game state (per-game)
    pub(crate) rows: Vec<Option<TennisStateRow>>,
    pub(crate) game_states: Vec<TennisGameState>,

    // Match format (per-game)
    pub(crate) sets_to_win: Vec<i64>,

    // Resolution tracking (per-game)
    pub(crate) set_total_under_emitted: Vec<bool>,
    pub(crate) first_set_winner_resolved: Vec<bool>,
    pub(crate) final_resolved_games: Vec<bool>,

    // Retirement targets: per-game list of tokens eligible for 50/50 orders
    pub(crate) retirement_targets: Vec<Vec<RetirementTarget>>,
}

// ---------------------------------------------------------------------------
// Retirement target — a token that resolves 50/50 on retirement
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct RetirementTarget {
    pub(crate) target_idx: TargetIdx,
    /// True for first_set_winner and first_set_totals — only fire if
    /// retirement happens during set 1 (before set 1 completes).
    pub(crate) first_set_only: bool,
    /// True for set_totals with line 3.5 (half_int == 3) — skip in BO5
    /// set 5 where the resolution rule is ambiguous.
    pub(crate) is_set_total_3_5: bool,
}

/// Stack-only result from the live WS tick path.
pub(crate) struct TennisLiveTickResult {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: TennisGameState,
    pub(crate) intents: smallvec::SmallVec<[Intent; 32]>,
    /// Number of intents at the start that use the NORMAL presign pool.
    /// Remaining intents use the RETIREMENT pool. Equal to `intents.len()`
    /// when all intents are normal (no retirement).
    pub(crate) normal_intent_count: usize,
}

/// Retirement dispatch result — two separate intent lists for different pools.
pub(crate) struct RetirementResult {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: TennisGameState,
    /// Moneyline intent for the winner → dispatched from NORMAL presign pool.
    pub(crate) moneyline_intents: smallvec::SmallVec<[Intent; 2]>,
    /// 50/50 retirement intents → dispatched from RETIREMENT presign pool.
    pub(crate) retirement_intents: smallvec::SmallVec<[Intent; 32]>,
}
