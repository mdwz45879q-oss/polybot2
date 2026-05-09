//! Soccer evaluation strategies.

use crate::*;
use crate::soccer::types::*;

fn push_if_some(slot: Option<TargetIdx>, out: &mut smallvec::SmallVec<[RawIntent; 4]>) {
    if let Some(tidx) = slot {
        out.push(RawIntent { target_idx: tidx });
    }
}

impl NativeSoccerEngine {
    // ---------------------------------------------------------------
    // Zero-alloc _into variants (live WS path)
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &SoccerGameState,
        out: &mut smallvec::SmallVec<[RawIntent; 4]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_totals[gi] {
            return;
        }
        let Some(total_now) = state.total else {
            return;
        };

        let targets = &self.game_targets[gi];

        // Over-crossing requires prev_total (delta check).
        if let Some(prev_total) = state.prev_total {
            if total_now > prev_total {
                let prev = prev_total as u16;
                let now = total_now as u16;
                for ol in &targets.over_lines {
                    if ol.half_int >= prev && ol.half_int < now {
                        out.push(RawIntent { target_idx: ol.target_idx });
                    }
                }
            }
        }

        // Final-under only needs the current total + match_completed.
        if state.match_completed.unwrap_or(false)
            && !self.totals_final_under_emitted[gi]
        {
            self.totals_final_under_emitted[gi] = true;
            let total = total_now as u16;
            for ol in &targets.under_lines {
                if ol.half_int >= total {
                    out.push(RawIntent { target_idx: ol.target_idx });
                }
            }
        }
    }

    pub(crate) fn evaluate_moneyline_into(
        &mut self,
        gidx: GameIdx,
        state: &SoccerGameState,
        out: &mut smallvec::SmallVec<[RawIntent; 4]>,
    ) {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return;
        }
        if !self.has_moneyline[gi] {
            return;
        }
        if !state.match_completed.unwrap_or(false) {
            return;
        }
        let (home, away) = match (state.home, state.away) {
            (Some(h), Some(a)) => (h, a),
            _ => return,
        };

        let targets = &self.game_targets[gi];
        if home > away {
            push_if_some(targets.moneyline_home_yes, out);
            push_if_some(targets.moneyline_away_no, out);
            push_if_some(targets.moneyline_draw_no, out);
        } else if away > home {
            push_if_some(targets.moneyline_away_yes, out);
            push_if_some(targets.moneyline_home_no, out);
            push_if_some(targets.moneyline_draw_no, out);
        } else {
            // Draw
            push_if_some(targets.moneyline_draw_yes, out);
            push_if_some(targets.moneyline_home_no, out);
            push_if_some(targets.moneyline_away_no, out);
        }

        // Spreads evaluated at game end alongside moneyline.
        let margin_home = home - away;
        for &(side, sl, tidx) in &targets.spreads {
            let margin = if side == SpreadSide::Home {
                margin_home
            } else {
                -margin_home
            };
            if (margin as f64) + sl > 0.0 {
                out.push(RawIntent { target_idx: tidx });
            }
        }

        self.final_resolved_games[gi] = true;
    }

    pub(crate) fn evaluate_btts_into(
        &mut self,
        gidx: GameIdx,
        state: &SoccerGameState,
        out: &mut smallvec::SmallVec<[RawIntent; 4]>,
    ) {
        let gi = gidx.0 as usize;
        if self.btts_resolved_games[gi] {
            return;
        }
        if !self.has_btts[gi] {
            return;
        }
        if !state.match_completed.unwrap_or(false) {
            return;
        }
        let (home, away) = match (state.home, state.away) {
            (Some(h), Some(a)) => (h, a),
            _ => return,
        };

        let targets = &self.game_targets[gi];
        if home > 0 && away > 0 {
            push_if_some(targets.btts_yes, out);
        } else {
            push_if_some(targets.btts_no, out);
        }
        self.btts_resolved_games[gi] = true;
    }

    // ---------------------------------------------------------------
    // Total corners (over/under) — same crossing logic as goal totals
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_corners_into(
        &mut self,
        gidx: GameIdx,
        state: &SoccerGameState,
        out: &mut smallvec::SmallVec<[RawIntent; 4]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_corners[gi] {
            return;
        }
        let Some(total_now) = state.total_corners else {
            return;
        };

        let targets = &self.game_targets[gi];

        // Over-crossing requires prev_total_corners (delta check).
        if let Some(prev) = state.prev_total_corners {
            if total_now > prev {
                let p = prev as u16;
                let n = total_now as u16;
                for ol in &targets.corner_over_lines {
                    if ol.half_int >= p && ol.half_int < n {
                        out.push(RawIntent { target_idx: ol.target_idx });
                    }
                }
            }
        }

        // Final-under only needs the current total + match_completed.
        if state.match_completed.unwrap_or(false)
            && !self.corners_final_under_emitted[gi]
        {
            self.corners_final_under_emitted[gi] = true;
            let total = total_now as u16;
            for ol in &targets.corner_under_lines {
                if ol.half_int >= total {
                    out.push(RawIntent { target_idx: ol.target_idx });
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Halftime result (three-way: home/away/draw at half-time)
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_halftime_into(
        &mut self,
        gidx: GameIdx,
        state: &SoccerGameState,
        out: &mut smallvec::SmallVec<[RawIntent; 4]>,
    ) {
        let gi = gidx.0 as usize;
        if self.halftime_resolved[gi] {
            return;
        }
        if !self.has_halftime[gi] {
            return;
        }
        if state.half != "Halftime" {
            return;
        }
        let (home, away) = match (state.home, state.away) {
            (Some(h), Some(a)) => (h, a),
            _ => return,
        };

        let targets = &self.game_targets[gi];
        if home > away {
            push_if_some(targets.halftime_home_yes, out);
            push_if_some(targets.halftime_away_no, out);
            push_if_some(targets.halftime_draw_no, out);
        } else if away > home {
            push_if_some(targets.halftime_away_yes, out);
            push_if_some(targets.halftime_home_no, out);
            push_if_some(targets.halftime_draw_no, out);
        } else {
            // Draw at halftime
            push_if_some(targets.halftime_draw_yes, out);
            push_if_some(targets.halftime_home_no, out);
            push_if_some(targets.halftime_away_no, out);
        }
        self.halftime_resolved[gi] = true;
    }

    // ---------------------------------------------------------------
    // Exact score (fires YES target if final score matches prediction)
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_exact_score_into(
        &mut self,
        gidx: GameIdx,
        state: &SoccerGameState,
        out: &mut smallvec::SmallVec<[RawIntent; 4]>,
    ) {
        let gi = gidx.0 as usize;
        if self.exact_score_resolved[gi] {
            return;
        }
        if !self.has_exact_score[gi] {
            return;
        }
        if !state.match_completed.unwrap_or(false) {
            return;
        }
        let (home, away) = match (state.home, state.away) {
            (Some(h), Some(a)) => (h, a),
            _ => return,
        };

        let targets = &self.game_targets[gi];
        for &(pred_h, pred_a, tidx) in &targets.exact_scores {
            if home == pred_h && away == pred_a {
                out.push(RawIntent { target_idx: tidx });
            }
        }
        self.exact_score_resolved[gi] = true;
    }
}
