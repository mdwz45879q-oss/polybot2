use crate::baseball::types::*;
use crate::*;

fn push_if_some(slot: Option<TargetIdx>, out: &mut smallvec::SmallVec<[Intent; 32]>) {
    if let Some(tidx) = slot {
        out.push(Intent { target_idx: tidx });
    }
}

fn period_inning_number(period_detail: &str) -> Option<i64> {
    let bytes = period_detail.as_bytes();
    if bytes.len() < 8 {
        return None;
    }
    let mut n: i64 = 0;
    for &b in &bytes[7..] {
        if b.is_ascii_digit() {
            n = n * 10 + (b - b'0') as i64;
        } else {
            break;
        }
    }
    if n > 0 { Some(n) } else { None }
}

#[cfg(test)]
pub(crate) fn line_key(value: f64) -> String {
    let text = format!("{:.6}", value);
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

impl NativeMlbEngine {
    // ---------------------------------------------------------------
    // Zero-alloc _into variants (live WS path)
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_totals[gi] {
            return;
        }
        let Some(total_now) = state.total else {
            return;
        };

        let targets = &self.game_targets[gi];

        if let Some(prev_total) = state.prev_total {
            if total_now > prev_total {
                let prev = prev_total as u16;
                let now = total_now as u16;
                for ol in &targets.over_lines {
                    if ol.half_int >= now {
                        break; // sorted — no more can match
                    }
                    if ol.half_int >= prev {
                        out.push(Intent {
                            target_idx: ol.target_idx,
                        });
                    }
                }
                // Tie guarantee: a tied game must produce at least one more run,
                // so the line at half_int == total is also guaranteed over.
                if let (Some(h), Some(a)) = (state.home, state.away) {
                    if h == a {
                        let tied_half_int = total_now as u16;
                        for ol in &targets.over_lines {
                            if ol.half_int == tied_half_int {
                                out.push(Intent {
                                    target_idx: ol.target_idx,
                                });
                                break;
                            }
                        }
                    }
                }
            }
        }

        if state.match_completed.unwrap_or(false) && !self.totals_final_under_emitted[gi] {
            self.totals_final_under_emitted[gi] = true;
            let total = total_now as u16;
            for ol in &targets.under_lines {
                if ol.half_int >= total {
                    out.push(Intent {
                        target_idx: ol.target_idx,
                    });
                }
            }
        }
    }

    pub(crate) fn evaluate_nrfi_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        delta: &DeltaEvent,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_nrfi[gi] {
            return;
        }
        if self.nrfi_resolved_games[gi] {
            return;
        }
        if !self.nrfi_first_inning_observed[gi] {
            match state.inning_number {
                Some(1) => {
                    self.nrfi_first_inning_observed[gi] = true;
                }
                Some(_) => {
                    self.nrfi_resolved_games[gi] = true;
                    return;
                }
                None => {
                    return;
                }
            }
        }

        let targets = &self.game_targets[gi];
        let run_delta = delta.goal_delta_home + delta.goal_delta_away;
        if run_delta > 0 && is_first_inning(state) {
            self.nrfi_resolved_games[gi] = true;
            if let Some(tidx) = targets.nrfi_yes {
                out.push(Intent { target_idx: tidx });
            }
            return;
        }

        if has_first_inning_ended(state) {
            if state.total.unwrap_or(-1) == 0 {
                if let Some(tidx) = targets.nrfi_no {
                    self.nrfi_resolved_games[gi] = true;
                    out.push(Intent { target_idx: tidx });
                }
            }
        }
    }

    /// Walkoff detection with three-tier resolution:
    ///
    /// **Tier 1 (always, both V1 and BoltOdds):**
    /// - Moneyline: home wins.
    /// - Partial spreads: fire sides that are mathematically locked.
    ///   `home_covers` when current margin satisfies the line (margin
    ///   can only stay same or grow → still covers).
    ///   `away_not_covers` always (away margin only gets worse → stays
    ///   not covering, regardless of line sign).
    ///
    /// **Tier 2 (BoltOdds only, bases empty):**
    /// When `base1=false, base2=false, base3=false`, the walkoff hit
    /// scores exactly +1 run — no additional runners can score. The
    /// current score IS the final score. Fire all remaining spreads
    /// + all under lines. Set `final_resolved_games` to block
    /// `evaluate_final_into` / `evaluate_game_end_from_outs_into`.
    ///
    /// The presign pool's one-shot gate prevents double-firing when
    /// Tier 2 re-iterates spreads already fired by Tier 1.
    pub(crate) fn evaluate_walkoff_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        period_detail: &str,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return;
        }
        if !self.has_final[gi] {
            return;
        }
        let inning = state.inning_number.unwrap_or(0);
        if inning < 9 || state.inning_half != "bottom" {
            return;
        }

        // BoltOdds guard: reject frames where the period string doesn't
        // confirm we're genuinely in the bottom of this inning. Limbo
        // frames (state.inning advanced but period still showing the
        // previous half) masquerade as bottom-9+ and fire walkoff.
        if state.outs.is_some() {
            if !period_detail.starts_with("AT_BOT_") {
                return;
            }
            if let Some(period_inn) = period_inning_number(period_detail) {
                if period_inn != inning {
                    return;
                }
            }
        }
        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);
        if home <= away {
            return;
        }

        // --- Tier 1: Always fire (both V1 and BoltOdds) ---

        // Moneyline: home wins.
        push_if_some(self.game_targets[gi].moneyline_home, out);

        if self.has_extra_innings[gi]
            && !self.extra_innings_resolved[gi]
            && state.inning_number.is_some_and(|i| i <= 9)
            && !period_detail.contains("_EXTRA_")
        {
            push_if_some(self.game_targets[gi].extra_innings_no, out);
            self.extra_innings_resolved[gi] = true;
        }

        // Partial spreads: fire the sides that are mathematically locked.
        // On a walkoff, margin_home is ≥1 and can only stay same or grow
        // (base runners scoring). Home margin grows; away margin (= -home)
        // gets more negative.
        let margin_home = home - away;
        let targets = &self.game_targets[gi];
        for slot in &targets.spreads {
            if slot.side == SpreadSide::Home {
                if (margin_home as f64) + slot.line > 0.0 {
                    // Home covers at current margin. Margin can only grow
                    // → still covers. Safe to fire.
                    push_if_some(slot.covers_idx, out);
                }
                // Home not covering: margin could grow to cover → not safe.
            } else {
                let away_margin = -margin_home;
                if (away_margin as f64) + slot.line <= 0.0 {
                    // Away does not cover at current margin. Away margin
                    // only gets worse → stays not covering. Safe to fire.
                    push_if_some(slot.not_covers_idx, out);
                }
                // Away covering: margin gets worse → could stop covering → not safe.
            }
        }

        // --- Tier 2: Full final resolution when bases are empty (BoltOdds only) ---

        let bases_empty = state.base1 == Some(false)
            && state.base2 == Some(false)
            && state.base3 == Some(false);

        if bases_empty {
            // Current score is the final score. No additional runners can score.
            // Fire the remaining spread sides that Tier 1 left unresolved.
            // Presign pool prevents double-fire on targets already popped.
            for slot in &targets.spreads {
                let margin = if slot.side == SpreadSide::Home {
                    margin_home
                } else {
                    -margin_home
                };
                if (margin as f64) + slot.line > 0.0 {
                    push_if_some(slot.covers_idx, out);
                } else {
                    push_if_some(slot.not_covers_idx, out);
                }
            }

            // Fire under lines (same logic as evaluate_final_into / game_end_from_outs).
            if self.has_totals[gi] && !self.totals_final_under_emitted[gi] {
                let total = state.total.unwrap_or(0) as u16;
                for ol in &targets.under_lines {
                    if ol.half_int >= total {
                        out.push(Intent {
                            target_idx: ol.target_idx,
                        });
                    }
                }
                self.totals_final_under_emitted[gi] = true;
            }

            // Mark fully resolved — block evaluate_final_into / game_end_from_outs.
            self.final_resolved_games[gi] = true;
        }
        // When bases are NOT empty (or unknown/None from V1): Tier 1 partial
        // spreads already fired. Leave final_resolved_games unset — game-end
        // evaluators fire the remaining spreads + unders when the game officially ends.
        // Presign pool prevents double-fire on targets already popped by Tier 1.
    }

    pub(crate) fn evaluate_final_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return;
        }
        if !self.has_final[gi] {
            return;
        }
        if !state.match_completed.unwrap_or(false) {
            return;
        }
        if state.home.is_none() || state.away.is_none() {
            return;
        }

        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);
        let targets = &self.game_targets[gi];

        let winner_slot = if home > away {
            targets.moneyline_home
        } else if away > home {
            targets.moneyline_away
        } else {
            None
        };
        if let Some(tidx) = winner_slot {
            out.push(Intent { target_idx: tidx });
        }

        let margin_home = home - away;
        for slot in &targets.spreads {
            let margin = if slot.side == SpreadSide::Home {
                margin_home
            } else {
                -margin_home
            };
            if (margin as f64) + slot.line > 0.0 {
                push_if_some(slot.covers_idx, out);
            } else {
                push_if_some(slot.not_covers_idx, out);
            }
        }

        self.final_resolved_games[gi] = true;

        if self.has_extra_innings[gi]
            && !self.extra_innings_resolved[gi]
            && state.inning_number.is_some_and(|i| i <= 9)
        {
            push_if_some(self.game_targets[gi].extra_innings_no, out);
            self.extra_innings_resolved[gi] = true;
        }
    }

    // ---------------------------------------------------------------
    // BoltOdds-specific evaluators (outs-based, zero-alloc _into)
    // ---------------------------------------------------------------

    /// NRFI resolution via BoltOdds. Two resolution paths:
    /// 1. Early YES: fires immediately when total increases in the first
    ///    inning (run delta via prev_total, same timing as V1).
    /// 2. Outs-based NO: fires when bottom-of-1st completes with total=0
    ///    (~5s faster than V1's freeText-based inning transition).
    ///    Only fires on `outs == 3` (confirmed half-inning end).
    pub(crate) fn evaluate_nrfi_from_outs_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_nrfi[gi] {
            return;
        }
        if self.nrfi_resolved_games[gi] {
            return;
        }

        // First-inning observation gate (same logic as V1 evaluator):
        // late subscriptions with inning > 1 are permanently skipped.
        if !self.nrfi_first_inning_observed[gi] {
            match state.inning_number {
                Some(1) => {
                    self.nrfi_first_inning_observed[gi] = true;
                }
                Some(_) => {
                    self.nrfi_resolved_games[gi] = true;
                    return;
                }
                None => {
                    return;
                }
            }
        }

        // Early NRFI YES: fire immediately on run delta in the first
        // inning, before waiting for outs. Matches V1 evaluate_nrfi_into
        // behavior. prev_total is None on cold start (guard skips safely).
        if is_first_inning(state) {
            if let (Some(total), Some(prev)) = (state.total, state.prev_total) {
                if total > prev {
                    self.nrfi_resolved_games[gi] = true;
                    push_if_some(self.game_targets[gi].nrfi_yes, out);
                    return;
                }
            }
        }

        // Only fire NO when bottom of 1st is ending — outs signal means
        // the half-inning's last out is being recorded.
        if state.inning_number != Some(1) || state.inning_half != "bottom" {
            return;
        }

        // Outs signal: only out=3 (confirmed half-inning end).
        // Note: `out=2, strike=3` was previously used as a strikeout
        // pre-fire, but BoltOdds data analysis showed a 68% false positive
        // rate — strikes frequently flash to 3 then revert (dropped 3rd
        // strike, scorer corrections). Only `outs == 3` is reliable.
        if state.outs != Some(3) {
            return;
        }

        let total = state.total.unwrap_or(0);
        let targets = &self.game_targets[gi];
        if total > 0 {
            // Runs scored in 1st inning → NRFI yes (runs in first inning).
            push_if_some(targets.nrfi_yes, out);
        } else {
            // Clean 1st inning → NRFI no (no runs in first inning).
            push_if_some(targets.nrfi_no, out);
        }
        self.nrfi_resolved_games[gi] = true;
    }

    /// Game-end resolution via BoltOdds outs signal. Fires moneyline +
    /// spreads + unders when the final out is detected (~9s before V1's
    /// "Ended" frame). Only fires when the game is definitively over:
    /// the trailing team's at-bat has ended with the leading team ahead.
    /// Walkoffs are handled by `evaluate_walkoff_into` (called before this).
    pub(crate) fn evaluate_game_end_from_outs_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_final[gi] {
            return;
        }
        if self.final_resolved_games[gi] {
            return;
        }

        let inning = state.inning_number.unwrap_or(0);
        if inning < 9 {
            return;
        }

        // Outs signal: only out=3 (confirmed half-inning end).
        if state.outs != Some(3) {
            return;
        }

        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);

        // Determine if the game is definitively over:
        // - Top of inning (away batting) + home leads → away can't catch up,
        //   bottom is skipped. Home wins.
        // - Bottom of inning (home batting) + away leads → home's at-bat
        //   over, away still leads. Away wins.
        // All other cases (tied, or wrong half for the leader) → game
        // continues, do not fire.
        let home_wins;
        if state.inning_half == "top" && home > away {
            home_wins = true;
        } else if state.inning_half == "bottom" && away > home {
            home_wins = false;
        } else {
            return; // tied or game continues
        }

        let targets = &self.game_targets[gi];

        // Fire moneyline winner.
        let winner_slot = if home_wins {
            targets.moneyline_home
        } else {
            targets.moneyline_away
        };
        push_if_some(winner_slot, out);

        // Fire spreads — same margin logic as evaluate_final_into.
        let margin_home = home - away;
        for slot in &targets.spreads {
            let margin = if slot.side == SpreadSide::Home {
                margin_home
            } else {
                -margin_home
            };
            if (margin as f64) + slot.line > 0.0 {
                push_if_some(slot.covers_idx, out);
            } else {
                push_if_some(slot.not_covers_idx, out);
            }
        }

        // Fire under lines — game total is final.
        if self.has_totals[gi] && !self.totals_final_under_emitted[gi] {
            let total = state.total.unwrap_or(0) as u16;
            for ol in &targets.under_lines {
                if ol.half_int >= total {
                    out.push(Intent {
                        target_idx: ol.target_idx,
                    });
                }
            }
            self.totals_final_under_emitted[gi] = true;
        }

        self.final_resolved_games[gi] = true;

        if self.has_extra_innings[gi]
            && !self.extra_innings_resolved[gi]
            && state.inning_number.is_some_and(|i| i <= 9)
        {
            push_if_some(self.game_targets[gi].extra_innings_no, out);
            self.extra_innings_resolved[gi] = true;
        }
    }

    pub(crate) fn evaluate_extra_innings_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.extra_innings_resolved[gi] {
            return;
        }
        if !self.has_extra_innings[gi] {
            return;
        }

        let inning = state.inning_number.unwrap_or(0);
        if inning < 9 {
            return;
        }

        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);

        if inning == 9
            && state.inning_half == "bottom"
            && state.outs == Some(3)
            && home == away
        {
            push_if_some(self.game_targets[gi].extra_innings_yes, out);
            self.extra_innings_resolved[gi] = true;
            return;
        }

        if inning >= 10 {
            push_if_some(self.game_targets[gi].extra_innings_yes, out);
            self.extra_innings_resolved[gi] = true;
        }
    }

    // ---------------------------------------------------------------
    // F5 (first 5 innings) evaluators
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_f5_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_f5[gi] || self.f5_resolved[gi] {
            return;
        }

        let inning = state.inning_number.unwrap_or(0);

        if !self.f5_first_five_observed[gi] {
            if inning >= 1 && inning <= 5 {
                self.f5_first_five_observed[gi] = true;
            } else if inning > 5 {
                self.f5_resolved[gi] = true;
                return;
            } else {
                return;
            }
        }

        if inning > 5 {
            return;
        }

        let Some(total_now) = state.total else {
            return;
        };
        let Some(prev_total) = state.prev_total else {
            return;
        };
        if total_now <= prev_total {
            return;
        }

        let prev = prev_total as u16;
        let now = total_now as u16;
        let targets = &self.game_targets[gi];
        for ol in &targets.f5_over_lines {
            if ol.half_int >= now {
                break;
            }
            if ol.half_int >= prev {
                out.push(Intent {
                    target_idx: ol.target_idx,
                });
            }
        }
    }

    pub(crate) fn evaluate_f5_mid5_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        period_detail: &str,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.f5_resolved[gi] || self.f5_top5_fired[gi] {
            return;
        }
        if !self.f5_first_five_observed[gi] || !self.has_f5[gi] {
            return;
        }
        if state.home.is_none() || state.away.is_none() {
            return;
        }

        let inning = state.inning_number.unwrap_or(0);
        let top5_complete = if state.outs.is_some() {
            // BoltOdds: require outs-based primary OR confirming period.
            // The bare (inning==5 && half=="bottom") is unsafe — BoltOdds
            // advances state.inning before the period string, so AT_END_4TH
            // frames with inning=5 and top=false match it prematurely.
            (state.outs == Some(3) && inning == 5 && state.inning_half == "top")
                || period_detail == "AT_MID_5TH_INNING"
                || period_detail == "AT_BOT_5TH_INNING"
        } else {
            // V1: half=="bottom" is safe (parse_period maps breaks to "break").
            inning == 5 && state.inning_half == "bottom"
        };
        if !top5_complete {
            return;
        }

        self.f5_top5_fired[gi] = true;
        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);
        let margin_home = home - away;
        let targets = &self.game_targets[gi];

        if home > away {
            push_if_some(targets.f5_winner_home, out);
            push_if_some(targets.f5_winner_away_no, out);
            push_if_some(targets.f5_winner_draw_no, out);
        }

        for slot in &targets.f5_spreads {
            if slot.side == SpreadSide::Home {
                if (margin_home as f64) + slot.line > 0.0 {
                    push_if_some(slot.covers_idx, out);
                }
            } else {
                let away_margin = -margin_home;
                if (away_margin as f64) + slot.line <= 0.0 {
                    push_if_some(slot.not_covers_idx, out);
                }
            }
        }
    }

    pub(crate) fn evaluate_f5_completion_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        period_detail: &str,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.f5_resolved[gi] {
            return;
        }
        if !self.f5_first_five_observed[gi] || !self.has_f5[gi] {
            return;
        }
        if state.home.is_none() || state.away.is_none() {
            return;
        }

        let inning = state.inning_number.unwrap_or(0);
        let f5_complete = (state.outs == Some(3)
            && inning == 5
            && state.inning_half == "bottom")
            || inning >= 6
            || period_detail == "AT_END_5TH_INNING";
        if !f5_complete {
            return;
        }

        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);
        let targets = &self.game_targets[gi];

        if home > away {
            push_if_some(targets.f5_winner_home, out);
            push_if_some(targets.f5_winner_away_no, out);
            push_if_some(targets.f5_winner_draw_no, out);
        } else if away > home {
            push_if_some(targets.f5_winner_away, out);
            push_if_some(targets.f5_winner_home_no, out);
            push_if_some(targets.f5_winner_draw_no, out);
        } else {
            push_if_some(targets.f5_winner_draw, out);
            push_if_some(targets.f5_winner_home_no, out);
            push_if_some(targets.f5_winner_away_no, out);
        }

        let margin_home = home - away;
        for slot in &targets.f5_spreads {
            let margin = if slot.side == SpreadSide::Home {
                margin_home
            } else {
                -margin_home
            };
            if (margin as f64) + slot.line > 0.0 {
                push_if_some(slot.covers_idx, out);
            } else {
                push_if_some(slot.not_covers_idx, out);
            }
        }

        let total = (home + away) as u16;

        for ol in &targets.f5_over_lines {
            if ol.half_int >= total {
                break;
            }
            out.push(Intent {
                target_idx: ol.target_idx,
            });
        }

        if !self.f5_total_under_emitted[gi] {
            for ol in &targets.f5_under_lines {
                if ol.half_int >= total {
                    out.push(Intent {
                        target_idx: ol.target_idx,
                    });
                }
            }
            self.f5_total_under_emitted[gi] = true;
        }

        self.f5_resolved[gi] = true;
    }

    pub(crate) fn evaluate_nrfi_period_into(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        period_detail: &str,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.nrfi_resolved_games[gi] {
            return;
        }
        if !self.has_nrfi[gi] {
            return;
        }
        if !self.nrfi_first_inning_observed[gi] {
            return;
        }

        if period_detail == "AT_END_1ST_INNING" {
            let total = state.total.unwrap_or(0);
            if total == 0 {
                push_if_some(self.game_targets[gi].nrfi_no, out);
                self.nrfi_resolved_games[gi] = true;
            }
        }
    }
}

#[cfg(test)]
impl NativeMlbEngine {
    pub(crate) fn evaluate_totals(&mut self, gidx: GameIdx, state: &GameState) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_totals_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_nrfi(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        delta: &DeltaEvent,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_nrfi_into(gidx, state, delta, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_walkoff(&mut self, gidx: GameIdx, state: &GameState) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_walkoff_into(gidx, state, "", &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_final(&mut self, gidx: GameIdx, state: &GameState) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_final_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_nrfi_from_outs(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_nrfi_from_outs_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_game_end_from_outs(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_game_end_from_outs_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_extra_innings(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_extra_innings_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_f5_totals(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_f5_totals_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_f5_mid5(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_f5_mid5_into(gidx, state, "", &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_f5_completion(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_f5_completion_into(gidx, state, "", &mut out);
        out.into_vec()
    }
}

pub(crate) fn is_first_inning(state: &GameState) -> bool {
    if state.inning_number.unwrap_or(-1) != 1 {
        return false;
    }
    if !matches!(state.inning_half, "top" | "bottom" | "break" | "") {
        return false;
    }
    !state.match_completed.unwrap_or(false)
}

pub(crate) fn has_first_inning_ended(state: &GameState) -> bool {
    let inning = state.inning_number;
    if inning.is_none() {
        return false;
    }
    let inning_num = inning.unwrap_or(0);
    if inning_num > 1 {
        return true;
    }
    inning_num == 1 && state.match_completed.unwrap_or(false)
}
