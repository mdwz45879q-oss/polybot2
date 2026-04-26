use super::*;

/// Format a line value to match Python's `_line_key()`:
/// 6 decimal places, strip trailing zeros, strip trailing dot.
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
        delta: &DeltaEvent,
        state: &GameState,
    ) -> Vec<Intent> {
        let uid = &delta.universal_id;
        if !self.games_with_totals.contains(uid) {
            return vec![];
        }
        let (Some(total_now), Some(prev_total)) = (state.total, state.prev_total) else {
            return vec![];
        };

        let mut intents = Vec::new();

        // OVER: probe crossed half-integer lines
        if total_now > prev_total {
            for i in prev_total..total_now {
                let line = i as f64 + 0.5;
                let key = format!("{}:TOTAL:OVER:{}", uid, line_key(line));
                if let Some(target) = self.targets.get(&key) {
                    intents.push(Intent {
                        strategy_key: key,
                        token_id: target.token_id.clone(),
                    });
                }
            }
        }

        // UNDER: at game completion, check all under lines above final total
        if state.match_completed.unwrap_or(false)
            && !self.totals_final_under_emitted.contains(uid)
        {
            self.totals_final_under_emitted.insert(uid.clone());
            let empty = Vec::new();
            let under_lines = self.under_lines_by_game.get(uid).unwrap_or(&empty);
            for &ul in under_lines {
                if ul > (total_now as f64) {
                    let key = format!("{}:TOTAL:UNDER:{}", uid, line_key(ul));
                    if let Some(target) = self.targets.get(&key) {
                        intents.push(Intent {
                            strategy_key: key,
                            token_id: target.token_id.clone(),
                        });
                    }
                }
            }
        }

        intents
    }

    pub(crate) fn evaluate_nrfi(
        &mut self,
        delta: &DeltaEvent,
        state: &GameState,
    ) -> Vec<Intent> {
        let uid = &delta.universal_id;
        if !self.games_with_nrfi.contains(uid) {
            return vec![];
        }
        if self.nrfi_resolved_games.contains(uid) {
            return vec![];
        }
        if !self.nrfi_first_inning_observed.contains(uid) {
            match state.inning_number {
                Some(1) => {
                    self.nrfi_first_inning_observed.insert(uid.clone());
                }
                Some(_) => {
                    self.nrfi_resolved_games.insert(uid.clone());
                    return vec![];
                }
                None => {
                    return vec![];
                }
            }
        }

        let run_delta = delta.goal_delta_home + delta.goal_delta_away;
        if run_delta > 0 && is_first_inning(state) {
            let key = format!("{}:NRFI:YES", uid);
            if let Some(target) = self.targets.get(&key) {
                self.nrfi_resolved_games.insert(uid.clone());
                return vec![Intent {
                    strategy_key: key,
                    token_id: target.token_id.clone(),
                }];
            }
        }

        if has_first_inning_ended(state) {
            if state.total.unwrap_or(-1) == 0 {
                let key = format!("{}:NRFI:NO", uid);
                if let Some(target) = self.targets.get(&key) {
                    self.nrfi_resolved_games.insert(uid.clone());
                    return vec![Intent {
                        strategy_key: key,
                        token_id: target.token_id.clone(),
                    }];
                }
            }
        }

        vec![]
    }

    pub(crate) fn evaluate_final(
        &mut self,
        delta: &DeltaEvent,
        state: &GameState,
    ) -> Vec<Intent> {
        let uid = &delta.universal_id;
        if self.final_resolved_games.contains(uid) {
            return vec![];
        }
        if !self.games_with_final.contains(uid) {
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
        let mut intents = Vec::new();

        // Moneyline — direct key probe
        let winner = if home > away {
            "HOME"
        } else if away > home {
            "AWAY"
        } else {
            ""
        };
        if !winner.is_empty() {
            let key = format!("{}:MONEYLINE:{}", uid, winner);
            if let Some(target) = self.targets.get(&key) {
                intents.push(Intent {
                    strategy_key: key,
                    token_id: target.token_id.clone(),
                });
            }
        }

        // Spreads — iterate spread_lines index, probe each
        let margin_home = home - away;
        let empty = Vec::new();
        let spread_lines = self.spread_lines_by_game.get(uid).unwrap_or(&empty);
        for &(side, sl) in spread_lines {
            let margin = if side == "HOME" { margin_home } else { -margin_home };
            if (margin as f64) + sl > 0.0 {
                let key = format!("{}:SPREAD:{}:{}", uid, side, line_key(sl));
                if let Some(target) = self.targets.get(&key) {
                    intents.push(Intent {
                        strategy_key: key,
                        token_id: target.token_id.clone(),
                    });
                }
            }
        }

        self.final_resolved_games.insert(uid.clone());
        self.cleanup_completed_game(uid);
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
    // The first inning has two halves (top + bottom). It ends when:
    // - We've moved to inning 2+ (any half), OR
    // - The game completed during inning 1
    // A "break" at inning 1 is the mid-inning break between top and bottom —
    // the bottom of the 1st hasn't been played yet, so the first inning is NOT over.
    if inning_num > 1 {
        return true;
    }
    inning_num == 1 && state.match_completed.unwrap_or(false)
}
