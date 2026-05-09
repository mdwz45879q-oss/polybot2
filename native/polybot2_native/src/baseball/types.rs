//! Baseball-specific types. Shared types (GameIdx, TargetIdx, TokenIdx,
//! TargetRegistry, OverLine, SpreadSide, Intent, RawIntent,
//! etc.) live in the crate root (lib.rs).

use crate::{GameIdx, TargetIdx, TokenIdx, TokenSlot, TargetSlot, TargetRegistry,
            OverLine, SpreadSide, Intent, InlineStr};
use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct GameTargets {
    pub(crate) over_lines: Vec<OverLine>,
    pub(crate) under_lines: Vec<OverLine>,
    pub(crate) nrfi_yes: Option<TargetIdx>,
    pub(crate) nrfi_no: Option<TargetIdx>,
    pub(crate) moneyline_home: Option<TargetIdx>,
    pub(crate) moneyline_away: Option<TargetIdx>,
    pub(crate) spreads: Vec<(SpreadSide, f64, TargetIdx)>,
}

impl GameTargets {
    #[allow(dead_code)]
    pub(crate) fn all_target_indices(&self) -> Vec<TargetIdx> {
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

#[derive(Clone, Default)]
pub(crate) struct Tick {
    pub(crate) universal_id: String,
    pub(crate) goals_home: Option<i64>,
    pub(crate) goals_away: Option<i64>,
    pub(crate) inning_number: Option<i64>,
    pub(crate) inning_half: &'static str,
    pub(crate) match_completed: Option<bool>,
    pub(crate) game_state: &'static str,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct DeltaEvent {
    pub(crate) material_change: bool,
    pub(crate) goal_delta_home: i64,
    pub(crate) goal_delta_away: i64,
}

#[derive(Clone, Default)]
pub(crate) struct StateRow {
    pub(crate) home_score_raw: InlineStr<4>,
    pub(crate) away_score_raw: InlineStr<4>,
    pub(crate) free_text_raw: InlineStr<32>,
    pub(crate) goals_home: Option<i64>,
    pub(crate) goals_away: Option<i64>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct GameState {
    pub(crate) home: Option<i64>,
    pub(crate) away: Option<i64>,
    pub(crate) total: Option<i64>,
    pub(crate) prev_total: Option<i64>,
    pub(crate) inning_number: Option<i64>,
    pub(crate) inning_half: &'static str,
    pub(crate) match_completed: Option<bool>,
    pub(crate) game_state: &'static str,
}

#[allow(dead_code)]
pub(crate) struct TickResult {
    pub(crate) game_id: String,
    pub(crate) state: GameState,
    pub(crate) intents: Vec<Intent>,
    pub(crate) material: bool,
}

/// Stack-only result from the live WS tick path. No owned strings,
/// no heap-allocated vectors for the common ≤4-intent case.
pub(crate) struct LiveTickResult {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: GameState,
    pub(crate) intents: smallvec::SmallVec<[Intent; 4]>,
    pub(crate) material: bool,
}

#[cfg_attr(feature = "python-extension", pyo3::prelude::pyclass)]
#[derive(Clone)]
pub(crate) struct NativeMlbEngine {
    pub(crate) game_id_to_idx: FxHashMap<String, GameIdx>,
    pub(crate) game_ids: Vec<String>,
    pub(crate) game_targets: Vec<GameTargets>,
    pub(crate) target_slots: Vec<TargetSlot>,
    pub(crate) tokens: Vec<TokenSlot>,
    pub(crate) token_id_to_idx: FxHashMap<String, TokenIdx>,
    pub(crate) strategy_keys: HashSet<String>,
    pub(crate) registry: Option<Arc<TargetRegistry>>,
    pub(crate) kickoff_ts: Vec<Option<i64>>,
    pub(crate) token_ids_by_game: Vec<Vec<String>>,

    pub(crate) has_totals: Vec<bool>,
    pub(crate) has_nrfi: Vec<bool>,
    pub(crate) has_final: Vec<bool>,

    pub(crate) rows: Vec<Option<StateRow>>,
    pub(crate) game_states: Vec<GameState>,

    pub(crate) totals_final_under_emitted: Vec<bool>,
    pub(crate) nrfi_resolved_games: Vec<bool>,
    pub(crate) nrfi_first_inning_observed: Vec<bool>,
    pub(crate) final_resolved_games: Vec<bool>,
}
