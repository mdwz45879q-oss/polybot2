use super::*;

impl NativeMlbEngine {
    pub(crate) fn mk_intent(
        &self,
        target_token_id: &str,
        target_condition_id: &str,
        strategy_key: &str,
        uid: &str,
        reason: &str,
        market_type: &str,
        outcome_semantic: &str,
    ) -> Intent {
        Intent {
            strategy_key: strategy_key.to_string(),
            token_id: target_token_id.to_string(),
            side: "buy_yes".to_string(),
            notional_usdc: self.amount_usdc,
            limit_price: self.limit_price,
            time_in_force: self.time_in_force.clone(),
            condition_id: target_condition_id.to_string(),
            source_universal_id: uid.to_string(),
            chain_id: String::new(),
            reason: reason.to_string(),
            market_type: market_type.to_string(),
            outcome_semantic: outcome_semantic.to_string(),
        }
    }

    pub(crate) fn evaluate_totals(
        &mut self,
        delta: &DeltaEvent,
        state: &GameState,
        intents: &mut Vec<Intent>,
    ) -> Option<String> {
        let uid = delta.universal_id.clone();
        if !self.over_targets_by_game.contains_key(&uid)
            && !self.under_targets_by_game.contains_key(&uid)
        {
            return None;
        }
        let total_now = state.total;
        let prev_total = state.prev_total;
        if total_now.is_none() || prev_total.is_none() {
            return Some("mlb_totals_missing_state".to_string());
        }
        let total_now_int = total_now.unwrap_or(0);
        let prev_total_int = prev_total.unwrap_or(0);

        let mut over_crossed = 0i64;
        let mut under_final = 0i64;

        if total_now_int > prev_total_int {
            if let (Some(targets), Some(lines)) = (
                self.over_targets_by_game.get(&uid),
                self.over_lines_by_game.get(&uid),
            ) {
                if !targets.is_empty() && !lines.is_empty() {
                    let hi_raw = bisect_right(lines, total_now_int as f64);
                    let lo_idx = bisect_right(lines, prev_total_int as f64);
                    if hi_raw > 0 {
                        let hi_idx = hi_raw - 1;
                        if hi_idx >= lo_idx && hi_idx < targets.len() {
                            let chosen = &targets[hi_idx];
                            intents.push(self.mk_intent(
                                &chosen.token_id,
                                &chosen.condition_id,
                                &chosen.strategy_key,
                                &uid,
                                &format!("mlb_totals_over_cross:{}", chosen.line),
                                "totals",
                                "over",
                            ));
                            over_crossed = 1;
                        }
                    }
                }
            }
        }

        if state.match_completed.unwrap_or(false) && !self.totals_final_under_emitted.contains(&uid)
        {
            self.totals_final_under_emitted.insert(uid.clone());
            if let Some(targets) = self.under_targets_by_game.get(&uid) {
                for target in targets.iter() {
                    if target.line > (total_now_int as f64) {
                        intents.push(self.mk_intent(
                            &target.token_id,
                            &target.condition_id,
                            &target.strategy_key,
                            &uid,
                            &format!("mlb_totals_under_final:{}", target.line),
                            "totals",
                            "under",
                        ));
                        under_final += 1;
                    }
                }
            }
        }

        if intents.is_empty() {
            Some("mlb_totals_no_signal".to_string())
        } else {
            Some(format!(
                "mlb_totals_emit:intents={}:over_crossed={}:under_final={}",
                intents.len(),
                over_crossed,
                under_final
            ))
        }
    }

    pub(crate) fn evaluate_nrfi(
        &mut self,
        delta: &DeltaEvent,
        state: &GameState,
        intents: &mut Vec<Intent>,
    ) -> Option<String> {
        let uid = delta.universal_id.clone();
        if self.nrfi_resolved_games.contains(&uid) {
            return Some("mlb_nrfi_already_resolved".to_string());
        }
        if !self.nrfi_first_inning_observed.contains(&uid) {
            match state.inning_number {
                Some(1) => {
                    self.nrfi_first_inning_observed.insert(uid.clone());
                }
                Some(_) => {
                    self.nrfi_resolved_games.insert(uid.clone());
                    return Some("mlb_nrfi_skip_late_subscription".to_string());
                }
                None => {
                    return Some("mlb_nrfi_awaiting_inning_data".to_string());
                }
            }
        }
        let targets = if let Some(targets) = self.nrfi_targets_by_game.get(&uid) {
            targets
        } else {
            return None;
        };

        let run_delta = delta.goal_delta_home + delta.goal_delta_away;
        if run_delta > 0 && is_first_inning(state, true) {
            if let Some(target) = targets.get("yes") {
                self.nrfi_resolved_games.insert(uid.clone());
                let reason = "mlb_nrfi_yes_first_inning_run".to_string();
                intents.push(self.mk_intent(
                    &target.token_id,
                    &target.condition_id,
                    &target.strategy_key,
                    &uid,
                    &reason,
                    "nrfi",
                    "yes",
                ));
                return Some(reason);
            }
        }

        if is_first_inning_complete(state) {
            if state.total.unwrap_or(-1) == 0 {
                if let Some(target) = targets.get("no") {
                    self.nrfi_resolved_games.insert(uid.clone());
                    let reason = "mlb_nrfi_no_first_inning_complete_zero".to_string();
                    intents.push(self.mk_intent(
                        &target.token_id,
                        &target.condition_id,
                        &target.strategy_key,
                        &uid,
                        &reason,
                        "nrfi",
                        "no",
                    ));
                    return Some(reason);
                }
            }
        }

        Some("mlb_nrfi_no_signal".to_string())
    }

    pub(crate) fn evaluate_final(
        &mut self,
        delta: &DeltaEvent,
        state: &GameState,
        intents: &mut Vec<Intent>,
    ) -> (String, String) {
        let uid = delta.universal_id.clone();
        if self.final_resolved_games.contains(&uid) {
            return (
                "mlb_final_already_resolved".to_string(),
                "no_action".to_string(),
            );
        }
        if !state.match_completed.unwrap_or(false) {
            if !self.moneyline_by_game.contains_key(&uid)
                && !self.spreads_by_game.contains_key(&uid)
            {
                return (
                    "mlb_final_no_targets".to_string(),
                    "no_action".to_string(),
                );
            }
            return (
                "mlb_final_not_completed".to_string(),
                "no_action".to_string(),
            );
        }
        if state.home.is_none() || state.away.is_none() {
            return (
                "mlb_final_invalid_score_state".to_string(),
                "no_action".to_string(),
            );
        }

        let home = state.home.unwrap_or(0);
        let away = state.away.unwrap_or(0);
        let mut unresolved = *self.unknown_by_game.get(&uid).unwrap_or(&0i64);

        if let Some(ml) = self.moneyline_by_game.get(&uid) {
            let winner = if home > away {
                "home"
            } else if away > home {
                "away"
            } else {
                ""
            };
            if !winner.is_empty() {
                if let Some(target) = ml.get(winner) {
                    intents.push(self.mk_intent(
                        &target.token_id,
                        &target.condition_id,
                        &target.strategy_key,
                        &uid,
                        &format!("mlb_moneyline_final_winner:{}", winner),
                        "moneyline",
                        winner,
                    ));
                } else {
                    unresolved += 1;
                }
            } else {
                unresolved += 1;
            }
        }

        let margin_home = home - away;
        let margin_away = -margin_home;
        if let Some(spreads) = self.spreads_by_game.get(&uid) {
            if let Some(home_spreads) = spreads.get("home") {
                for target in home_spreads.iter() {
                    if (margin_home as f64) + target.line > 0.0 {
                        intents.push(self.mk_intent(
                            &target.token_id,
                            &target.condition_id,
                            &target.strategy_key,
                            &uid,
                            &format!("mlb_spread_final_cover:home:{}", target.line),
                            "spread",
                            "home",
                        ));
                    }
                }
            }
            if let Some(away_spreads) = spreads.get("away") {
                for target in away_spreads.iter() {
                    if (margin_away as f64) + target.line > 0.0 {
                        intents.push(self.mk_intent(
                            &target.token_id,
                            &target.condition_id,
                            &target.strategy_key,
                            &uid,
                            &format!("mlb_spread_final_cover:away:{}", target.line),
                            "spread",
                            "away",
                        ));
                    }
                }
            }
        }

        self.final_resolved_games.insert(uid);
        if intents.is_empty() {
            (
                format!("mlb_final_no_signal:unresolved={}", unresolved),
                "no_action".to_string(),
            )
        } else {
            (
                format!(
                    "mlb_final_emit:intents={}:unresolved={}",
                    intents.len(),
                    unresolved
                ),
                "action".to_string(),
            )
        }
    }
}

pub(crate) fn bisect_right(vals: &[f64], x: f64) -> usize {
    let mut lo = 0usize;
    let mut hi = vals.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if x < vals[mid] {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

pub(crate) fn is_first_inning(state: &GameState, include_end: bool) -> bool {
    if state.inning_number.unwrap_or(-1) != 1 {
        return false;
    }
    let half = crate::engine::norm(&state.inning_half);
    let mut allowed: HashSet<&str> = HashSet::from(["top", "bottom", ""]);
    if include_end {
        allowed.insert("end");
    }
    if !allowed.contains(half.as_str()) {
        return false;
    }
    !state.match_completed.unwrap_or(false)
}

pub(crate) fn is_first_inning_complete(state: &GameState) -> bool {
    let inning = state.inning_number;
    let half = crate::engine::norm(&state.inning_half);
    if inning.is_none() {
        return false;
    }
    let inning_num = inning.unwrap_or(0);
    if inning_num > 1 {
        return true;
    }
    if inning_num == 1 && half == "end" {
        return true;
    }
    inning_num == 1 && state.match_completed.unwrap_or(false)
}
