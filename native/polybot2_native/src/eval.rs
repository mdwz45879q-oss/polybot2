use super::*;

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
    pub(crate) fn evaluate_totals(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<RawIntent> {
        let gi = gidx.0 as usize;
        if !self.has_totals[gi] {
            return vec![];
        }
        let (Some(total_now), Some(prev_total)) = (state.total, state.prev_total) else {
            return vec![];
        };

        let targets = &self.game_targets[gi];
        let mut intents = Vec::new();

        if total_now > prev_total {
            let prev = prev_total as u16;
            let now = total_now as u16;
            for ol in &targets.over_lines {
                if ol.half_int >= prev && ol.half_int < now {
                    intents.push(RawIntent { target_idx: ol.target_idx });
                }
            }
        }

        if state.match_completed.unwrap_or(false)
            && !self.totals_final_under_emitted[gi]
        {
            self.totals_final_under_emitted[gi] = true;
            let total = total_now as u16;
            for ol in &targets.under_lines {
                if ol.half_int >= total {
                    intents.push(RawIntent { target_idx: ol.target_idx });
                }
            }
        }

        intents
    }

    pub(crate) fn evaluate_nrfi(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
        delta: &DeltaEvent,
    ) -> Vec<RawIntent> {
        let gi = gidx.0 as usize;
        if !self.has_nrfi[gi] {
            return vec![];
        }
        if self.nrfi_resolved_games[gi] {
            return vec![];
        }
        if !self.nrfi_first_inning_observed[gi] {
            match state.inning_number {
                Some(1) => {
                    self.nrfi_first_inning_observed[gi] = true;
                }
                Some(_) => {
                    self.nrfi_resolved_games[gi] = true;
                    return vec![];
                }
                None => {
                    return vec![];
                }
            }
        }

        let targets = &self.game_targets[gi];
        // NRFI YES fires only on a positive score *delta*, not on a first-observed
        // nonzero score. A late subscription that joins mid-first-inning with 1-0
        // sees run_delta=0 (no prior row) and correctly does not fire — the engine
        // only trades score changes it witnesses in real time.
        let run_delta = delta.goal_delta_home + delta.goal_delta_away;
        if run_delta > 0 && is_first_inning(state) {
            if let Some(tidx) = targets.nrfi_yes {
                self.nrfi_resolved_games[gi] = true;
                return vec![RawIntent { target_idx: tidx }];
            }
        }

        if has_first_inning_ended(state) {
            if state.total.unwrap_or(-1) == 0 {
                if let Some(tidx) = targets.nrfi_no {
                    self.nrfi_resolved_games[gi] = true;
                    return vec![RawIntent { target_idx: tidx }];
                }
            }
        }

        vec![]
    }

    pub(crate) fn evaluate_final(
        &mut self,
        gidx: GameIdx,
        state: &GameState,
    ) -> Vec<RawIntent> {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return vec![];
        }
        if !self.has_final[gi] {
            return vec![];
        }
        if !state.match_completed.unwrap_or(false) {
            return vec![];
        }
        if state.home.is_none() || state.away.is_none() {
            return vec![];
        }

        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);
        let targets = &self.game_targets[gi];
        let mut intents = Vec::new();

        let winner_slot = if home > away {
            targets.moneyline_home
        } else if away > home {
            targets.moneyline_away
        } else {
            None
        };
        if let Some(tidx) = winner_slot {
            intents.push(RawIntent { target_idx: tidx });
        }

        let margin_home = home - away;
        for &(side, sl, tidx) in &targets.spreads {
            let margin = if side == SpreadSide::Home {
                margin_home
            } else {
                -margin_home
            };
            if (margin as f64) + sl > 0.0 {
                intents.push(RawIntent { target_idx: tidx });
            }
        }

        self.final_resolved_games[gi] = true;
        intents
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
