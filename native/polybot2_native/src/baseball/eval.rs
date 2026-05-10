use crate::baseball::types::*;
use crate::*;

fn push_if_some(slot: Option<TargetIdx>, out: &mut smallvec::SmallVec<[Intent; 32]>) {
    if let Some(tidx) = slot {
        out.push(Intent { target_idx: tidx });
    }
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

    /// Walkoff detection: in the bottom of the 9th inning or later,
    /// if the home team is leading, they are guaranteed to win.
    /// Fire moneyline_home immediately — don't wait for "Ended".
    /// The presign pool's one-shot gate prevents double-firing if
    /// evaluate_final_into later tries the same target at game end.
    pub(crate) fn evaluate_walkoff_into(
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
        let inning = state.inning_number.unwrap_or(0);
        if inning >= 9
            && state.inning_half == "bottom"
            && state.home.unwrap_or(0) > state.away.unwrap_or(0)
        {
            if let Some(tidx) = self.game_targets[gi].moneyline_home {
                out.push(Intent { target_idx: tidx });
            }
        }
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
        self.evaluate_walkoff_into(gidx, state, &mut out);
        out.into_vec()
    }

    pub(crate) fn evaluate_final(&mut self, gidx: GameIdx, state: &GameState) -> Vec<Intent> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_final_into(gidx, state, &mut out);
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
