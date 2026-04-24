use super::*;
use crate::parse::{
    get_str, get_i64_opt, get_f64_opt,
    parse_tick_any, parse_tick_from_kalstrop_row,
    parse_tick_from_kalstrop_row_value, iter_payload_items_value,
    get_value_str,
};

fn get_dict<'py>(obj: &Bound<'py, PyDict>, key: &str) -> Option<Bound<'py, PyDict>> {
    if let Ok(Some(value)) = obj.get_item(key) {
        if let Ok(dict) = value.downcast::<PyDict>() {
            return Some(dict.clone());
        }
    }
    None
}

#[pymethods]
impl NativeMlbEngine {
    #[new]
    #[pyo3(signature = (
        dedup_ttl_seconds=2.0,
        decision_cooldown_seconds=0.5,
        decision_debounce_seconds=0.1,
        amount_usdc=5.0,
        limit_price=0.52,
        time_in_force="FAK".to_string(),
    ))]
    pub fn new(
        dedup_ttl_seconds: f64,
        decision_cooldown_seconds: f64,
        decision_debounce_seconds: f64,
        amount_usdc: f64,
        limit_price: f64,
        time_in_force: String,
    ) -> Self {
        Self {
            dedup_ttl_ns: (dedup_ttl_seconds.max(0.1) * 1_000_000_000.0) as i64,
            decision_cooldown_ns: (decision_cooldown_seconds.max(0.0) * 1_000_000_000.0) as i64,
            decision_debounce_ns: (decision_debounce_seconds.max(0.0) * 1_000_000_000.0) as i64,
            amount_usdc,
            limit_price,
            time_in_force,
            over_targets_by_game: HashMap::new(),
            over_lines_by_game: HashMap::new(),
            under_targets_by_game: HashMap::new(),
            nrfi_targets_by_game: HashMap::new(),
            moneyline_by_game: HashMap::new(),
            spreads_by_game: HashMap::new(),
            unknown_by_game: HashMap::new(),
            kickoff_ts_by_game: HashMap::new(),
            home_team_by_game: HashMap::new(),
            away_team_by_game: HashMap::new(),
            token_ids_by_game: HashMap::new(),
            totals_final_under_emitted: HashSet::new(),
            nrfi_resolved_games: HashSet::new(),
            nrfi_first_inning_observed: HashSet::new(),
            final_resolved_games: HashSet::new(),
            rows: HashMap::new(),
            game_states: HashMap::new(),
            last_emit_ns: HashMap::new(),
            last_signature: HashMap::new(),
            attempted_strategy_keys: HashSet::new(),
        }
    }

    pub fn reset_runtime_state(&mut self) {
        self.totals_final_under_emitted.clear();
        self.nrfi_resolved_games.clear();
        self.nrfi_first_inning_observed.clear();
        self.final_resolved_games.clear();
        self.rows.clear();
        self.game_states.clear();
        self.last_emit_ns.clear();
        self.last_signature.clear();
        self.attempted_strategy_keys.clear();
    }

    pub fn load_plan(&mut self, plan: &Bound<'_, PyDict>) -> PyResult<()> {
        self.over_targets_by_game.clear();
        self.over_lines_by_game.clear();
        self.under_targets_by_game.clear();
        self.nrfi_targets_by_game.clear();
        self.moneyline_by_game.clear();
        self.spreads_by_game.clear();
        self.unknown_by_game.clear();
        self.kickoff_ts_by_game.clear();
        self.home_team_by_game.clear();
        self.away_team_by_game.clear();
        self.token_ids_by_game.clear();
        self.reset_runtime_state();

        let games_obj = plan
            .get_item("games")?
            .ok_or_else(|| PyValueError::new_err("plan.games is required"))?;
        let games: &Bound<'_, PyList> = games_obj.downcast()?;

        for game_obj in games.iter() {
            let game: &Bound<'_, PyDict> = game_obj.downcast()?;
            let uid = get_str(game, "provider_game_id");
            if uid.is_empty() {
                continue;
            }
            if let Some(kickoff_ts_utc) = get_i64_opt(game, "kickoff_ts_utc") {
                self.kickoff_ts_by_game.insert(uid.clone(), kickoff_ts_utc);
            }
            let home_team = get_str(game, "canonical_home_team");
            if !home_team.trim().is_empty() {
                self.home_team_by_game.insert(uid.clone(), home_team);
            }
            let away_team = get_str(game, "canonical_away_team");
            if !away_team.trim().is_empty() {
                self.away_team_by_game.insert(uid.clone(), away_team);
            }
            let markets_obj = game
                .get_item("markets")?
                .ok_or_else(|| PyValueError::new_err("game.markets is required"))?;
            let markets: &Bound<'_, PyList> = markets_obj.downcast()?;

            let mut over_targets: Vec<TotalsTarget> = Vec::new();
            let mut under_targets: Vec<TotalsTarget> = Vec::new();
            let mut nrfi_by_side: HashMap<String, Vec<NrfiTarget>> = HashMap::new();
            let mut moneyline_by_side: HashMap<String, Vec<FinalTarget>> = HashMap::new();
            let mut spreads_by_side: HashMap<String, Vec<SpreadTarget>> = HashMap::new();
            let mut unresolved = 0i64;
            let mut token_ids: HashSet<String> = HashSet::new();

            for market_obj in markets.iter() {
                let market: &Bound<'_, PyDict> = market_obj.downcast()?;
                let sports_market_type = canonical_market_type(&get_str(market, "sports_market_type"));
                let line = get_f64_opt(market, "line");
                let targets_obj = market
                    .get_item("targets")?
                    .ok_or_else(|| PyValueError::new_err("market.targets is required"))?;
                let targets: &Bound<'_, PyList> = targets_obj.downcast()?;

                for target_obj in targets.iter() {
                    let target: &Bound<'_, PyDict> = target_obj.downcast()?;
                    let semantic = norm(&get_str(target, "outcome_semantic"));
                    let token_id = get_str(target, "token_id");
                    if token_id.is_empty() {
                        continue;
                    }
                    token_ids.insert(token_id.clone());
                    let condition_id = get_str(target, "condition_id");
                    let strategy_key = get_str(target, "strategy_key");

                    if sports_market_type == "totals" {
                        if let Some(total_line) = line {
                            let row = TotalsTarget {
                                line: total_line,
                                token_id: token_id.clone(),
                                condition_id: condition_id.clone(),
                                strategy_key: strategy_key.clone(),
                            };
                            if semantic == "over" {
                                over_targets.push(row);
                            } else if semantic == "under" {
                                under_targets.push(row);
                            }
                        }
                    }

                    if sports_market_type == "nrfi" {
                        if semantic == "yes" || semantic == "no" {
                            nrfi_by_side
                                .entry(semantic.clone())
                                .or_default()
                                .push(NrfiTarget {
                                    token_id: token_id.clone(),
                                    condition_id: condition_id.clone(),
                                    strategy_key: strategy_key.clone(),
                                });
                        }
                    }

                    if sports_market_type == "moneyline" {
                        if semantic == "home" || semantic == "away" {
                            moneyline_by_side.entry(semantic.clone()).or_default().push(
                                FinalTarget {
                                    token_id: token_id.clone(),
                                    condition_id: condition_id.clone(),
                                    strategy_key: strategy_key.clone(),
                                },
                            );
                        } else {
                            unresolved += 1;
                        }
                    } else if sports_market_type == "spread" {
                        if (semantic == "home" || semantic == "away") && line.is_some() {
                            spreads_by_side.entry(semantic.clone()).or_default().push(
                                SpreadTarget {
                                    line: line.unwrap_or(0.0),
                                    token_id: token_id.clone(),
                                    condition_id: condition_id.clone(),
                                    strategy_key: strategy_key.clone(),
                                },
                            );
                        } else {
                            unresolved += 1;
                        }
                    }
                }
            }

            over_targets.sort_by(|a, b| {
                a.line
                    .total_cmp(&b.line)
                    .then_with(|| a.strategy_key.cmp(&b.strategy_key))
            });
            if !over_targets.is_empty() {
                let lines = over_targets.iter().map(|x| x.line).collect::<Vec<_>>();
                self.over_lines_by_game.insert(uid.clone(), lines);
                self.over_targets_by_game.insert(uid.clone(), over_targets);
            }

            under_targets.sort_by(|a, b| {
                a.line
                    .total_cmp(&b.line)
                    .then_with(|| a.strategy_key.cmp(&b.strategy_key))
            });
            if !under_targets.is_empty() {
                self.under_targets_by_game
                    .insert(uid.clone(), under_targets);
            }

            if !nrfi_by_side.is_empty() {
                let mut selected: HashMap<String, NrfiTarget> = HashMap::new();
                for side in ["no", "yes"] {
                    if let Some(mut vals) = nrfi_by_side.remove(side) {
                        vals.sort_by(|a, b| a.strategy_key.cmp(&b.strategy_key));
                        if let Some(first) = vals.into_iter().next() {
                            selected.insert(side.to_string(), first);
                        }
                    }
                }
                if !selected.is_empty() {
                    self.nrfi_targets_by_game.insert(uid.clone(), selected);
                }
            }

            if !moneyline_by_side.is_empty() {
                let mut selected: HashMap<String, FinalTarget> = HashMap::new();
                for side in ["home", "away"] {
                    if let Some(mut vals) = moneyline_by_side.remove(side) {
                        vals.sort_by(|a, b| a.strategy_key.cmp(&b.strategy_key));
                        if let Some(first) = vals.into_iter().next() {
                            selected.insert(side.to_string(), first);
                        }
                    }
                }
                if !selected.is_empty() {
                    self.moneyline_by_game.insert(uid.clone(), selected);
                }
            }

            if !spreads_by_side.is_empty() {
                let mut selected: HashMap<String, Vec<SpreadTarget>> = HashMap::new();
                for side in ["home", "away"] {
                    if let Some(mut vals) = spreads_by_side.remove(side) {
                        vals.sort_by(|a, b| {
                            a.line
                                .total_cmp(&b.line)
                                .then_with(|| a.strategy_key.cmp(&b.strategy_key))
                        });
                        if !vals.is_empty() {
                            selected.insert(side.to_string(), vals);
                        }
                    }
                }
                if !selected.is_empty() {
                    self.spreads_by_game.insert(uid.clone(), selected);
                }
            }

            if unresolved > 0 {
                self.unknown_by_game.insert(uid.clone(), unresolved);
            }
            if !token_ids.is_empty() {
                let mut token_list = token_ids.into_iter().collect::<Vec<_>>();
                token_list.sort();
                token_list.dedup();
                self.token_ids_by_game.insert(uid.clone(), token_list);
            }
        }

        Ok(())
    }

    fn process_score_event(
        &mut self,
        py: Python<'_>,
        event: &Bound<'_, PyAny>,
        recv_monotonic_ns: i64,
    ) -> PyResult<PyObject> {
        let tick = parse_tick_any(event, recv_monotonic_ns);
        let out = self.process_tick(tick);
        result_to_py(py, out)
    }

    fn process_score_frame(
        &mut self,
        py: Python<'_>,
        frame: &Bound<'_, PyAny>,
        recv_monotonic_ns: i64,
        source_recv_monotonic_ns: i64,
    ) -> PyResult<PyObject> {
        let mut out = ProcessResult {
            decision: "no_action".to_string(),
            reason: "native_batch_no_action".to_string(),
            ..Default::default()
        };
        let mut events_in = 0i64;

        let mut process_msg = |msg: &Bound<'_, PyDict>| {
            let mtype = get_str(msg, "type").to_lowercase();
            if mtype == "ping" || mtype != "next" {
                return;
            }
            let payload = get_dict(msg, "payload");
            let data = payload.as_ref().and_then(|p| get_dict(p, "data"));
            let row = data
                .as_ref()
                .and_then(|d| get_dict(d, "sportsMatchStateUpdatedV2"));
            if let Some(row_dict) = row {
                let tick = parse_tick_from_kalstrop_row(
                    &row_dict,
                    recv_monotonic_ns,
                    source_recv_monotonic_ns,
                );
                events_in += 1;
                let one = self.process_tick(tick);
                out.drops_cooldown += one.drops_cooldown;
                out.drops_debounce += one.drops_debounce;
                out.drops_one_shot += one.drops_one_shot;
                out.decision_non_material += one.decision_non_material;
                out.decision_no_action += one.decision_no_action;
                out.observe_signals.extend(one.observe_signals);
                if !one.intents.is_empty() {
                    out.intents.extend(one.intents);
                    out.reason = one.reason;
                }
            }
        };

        if let Ok(items) = frame.downcast::<PyList>() {
            for item in items.iter() {
                if let Ok(msg) = item.downcast::<PyDict>() {
                    process_msg(&msg);
                }
            }
        } else if let Ok(msg) = frame.downcast::<PyDict>() {
            process_msg(msg);
        }

        if !out.intents.is_empty() {
            out.decision = "action".to_string();
        }

        let py_out = result_to_py(py, out)?;
        let bound = py_out.bind(py).downcast::<PyDict>()?;
        bound.set_item("events_in", events_in)?;
        Ok(py_out)
    }
}

impl NativeMlbEngine {
    pub(crate) fn dump_game_states_for_heartbeat(&self, candidate_subs: &[String]) -> Value {
        let resolved: HashSet<&str> = candidate_subs.iter().map(|s| s.as_str()).collect();
        let mut games = serde_json::Map::new();
        for (game_id, state) in &self.game_states {
            games.insert(
                game_id.clone(),
                json!({
                    "h": state.home,
                    "a": state.away,
                    "s": state.game_state,
                    "inn": state.inning_number,
                    "half": state.inning_half,
                    "mc": state.match_completed,
                }),
            );
        }
        let now_s = crate::dispatch::now_unix_s();
        for (game_id, kickoff_ts) in &self.kickoff_ts_by_game {
            if games.contains_key(game_id.as_str()) {
                continue;
            }
            if self.is_game_completed(game_id.as_str()) {
                games.insert(game_id.clone(), json!({"s": "FINAL"}));
            } else if !resolved.contains(game_id.as_str()) && now_s >= *kickoff_ts {
                games.insert(game_id.clone(), json!({"s": "FINAL"}));
            } else if now_s < *kickoff_ts {
                games.insert(game_id.clone(), json!({"s": "NOT STARTED"}));
            } else {
                games.insert(game_id.clone(), json!({"s": "LIVE"}));
            }
        }
        Value::Object(games)
    }

    pub(crate) fn dump_team_names(&self) -> Value {
        let mut teams = serde_json::Map::new();
        for (game_id, home) in &self.home_team_by_game {
            let away = self
                .away_team_by_game
                .get(game_id)
                .cloned()
                .unwrap_or_default();
            teams.insert(game_id.clone(), json!([home, away]));
        }
        Value::Object(teams)
    }

    pub(crate) fn active_subscriptions_for_candidates(
        &self,
        candidates: &[String],
        now_ts_utc: i64,
        subscribe_lead_minutes: i64,
    ) -> Vec<String> {
        let lead_seconds = subscribe_lead_minutes.max(0).saturating_mul(60);
        let mut out: Vec<String> = Vec::new();
        for uid in candidates {
            let id = uid.trim();
            if id.is_empty() {
                continue;
            }
            if self.is_game_completed(id) {
                continue;
            }
            if let Some(kickoff_ts_utc) = self.kickoff_ts_by_game.get(id) {
                if now_ts_utc >= kickoff_ts_utc.saturating_sub(lead_seconds) {
                    out.push(id.to_string());
                }
            } else {
                out.push(id.to_string());
            }
        }
        out.sort();
        out.dedup();
        out
    }

    pub(crate) fn active_token_ids_for_games(&self, game_ids: &[String]) -> Vec<String> {
        let mut tokens: HashSet<String> = HashSet::new();
        for uid in game_ids {
            if let Some(ids) = self.token_ids_by_game.get(uid.as_str()) {
                for token_id in ids {
                    let t = token_id.trim();
                    if !t.is_empty() {
                        tokens.insert(t.to_string());
                    }
                }
            }
        }
        let mut out = tokens.into_iter().collect::<Vec<_>>();
        out.sort();
        out
    }

    pub(crate) fn is_game_completed(&self, game_id: &str) -> bool {
        if self.final_resolved_games.contains(game_id) {
            return true;
        }
        self.game_states
            .get(game_id)
            .and_then(|s| s.match_completed)
            .unwrap_or(false)
    }

    fn process_tick(&mut self, tick: Tick) -> ProcessResult {
        let mut out = ProcessResult {
            decision: "no_action".to_string(),
            reason: "native_no_action".to_string(),
            ..Default::default()
        };

        if tick.universal_id.is_empty() {
            out.reason = "native_missing_uid".to_string();
            out.decision_no_action = 1;
            return out;
        }

        let delta = self.apply_delta(&tick);
        if !delta.material_change {
            out.reason = "native_non_material".to_string();
            out.decision_non_material = 1;
            return out;
        }

        let (prev_state, state) = self.update_game_state(&tick);
        self.build_observe_signals(
            tick.universal_id.as_str(),
            &prev_state,
            &state,
            tick.period.as_str(),
            &mut out.observe_signals,
        );
        let mut intents: Vec<Intent> = Vec::new();

        {
            let mut reasons: Vec<String> = Vec::new();
            if let Some(reason) = self.evaluate_totals(&delta, &state, &mut intents) {
                reasons.push(reason);
            }
            if let Some(reason) = self.evaluate_nrfi(&delta, &state, &mut intents) {
                reasons.push(reason);
            }
            {
                let (reason, _) = self.evaluate_final(&delta, &state, &mut intents);
                reasons.push(reason);
            }
            out.reason = reasons.join("+");
        }

        if intents.is_empty() {
            out.decision = "no_action".to_string();
            out.decision_no_action = 1;
            return out;
        }

        let mut emitted: Vec<Intent> = Vec::new();
        for intent in intents.into_iter() {
            let strategy_key = intent.strategy_key.as_str();
            if strategy_key.is_empty() {
                continue;
            }

            let last_emit = *self.last_emit_ns.get(strategy_key).unwrap_or(&0i64);
            if self.decision_cooldown_ns > 0
                && last_emit > 0
                && (delta.recv_monotonic_ns - last_emit) < self.decision_cooldown_ns
            {
                out.drops_cooldown += 1;
                continue;
            }

            let sig = DecisionSig {
                token_id: intent.token_id.clone(),
                side: intent.side.clone(),
                time_in_force: intent.time_in_force.clone(),
            };
            if self.decision_debounce_ns > 0 {
                if let Some(last_sig) = self.last_signature.get(strategy_key) {
                    if *last_sig == sig
                        && last_emit > 0
                        && (delta.recv_monotonic_ns - last_emit) < self.decision_debounce_ns
                    {
                        out.drops_debounce += 1;
                        continue;
                    }
                }
            }

            let strategy_key_owned = intent.strategy_key.clone();
            self.last_emit_ns
                .insert(strategy_key_owned.clone(), delta.recv_monotonic_ns);
            self.last_signature.insert(strategy_key_owned.clone(), sig);

            if !self.attempted_strategy_keys.insert(strategy_key_owned) {
                out.drops_one_shot += 1;
                continue;
            }
            emitted.push(intent);
        }

        if emitted.is_empty() {
            out.decision = "no_action".to_string();
            out.decision_no_action = 1;
            return out;
        }

        out.decision = "action".to_string();
        out.intents = emitted;
        out
    }

    fn apply_delta(&mut self, tick: &Tick) -> DeltaEvent {
        let uid = tick.universal_id.clone();
        let recv_ns = tick.recv_monotonic_ns;
        let prev = self.rows.get(uid.as_str());

        if let Some(row) = prev {
            if tick_matches_state_row(tick, row)
                && (recv_ns - row.seen_monotonic_ns) <= self.dedup_ttl_ns
            {
                return DeltaEvent {
                    universal_id: uid,
                    recv_monotonic_ns: recv_ns,
                    material_change: false,
                    ..Default::default()
                };
            }
        }

        let mut goal_delta_home = 0;
        let mut goal_delta_away = 0;
        if let Some(row) = prev {
            goal_delta_home = tick.goals_home.unwrap_or(0) - row.goals_home.unwrap_or(0);
            goal_delta_away = tick.goals_away.unwrap_or(0) - row.goals_away.unwrap_or(0);
        }

        let material = if let Some(row) = prev {
            !tick_matches_state_row(tick, row)
        } else {
            true
        };

        self.rows.insert(
            uid.clone(),
            StateRow {
                seen_monotonic_ns: recv_ns,
                action: tick.action.clone(),
                goals_home: tick.goals_home,
                goals_away: tick.goals_away,
                inning_number: tick.inning_number,
                inning_half: tick.inning_half.clone(),
                outs: tick.outs,
                balls: tick.balls,
                strikes: tick.strikes,
                runner_on_first: tick.runner_on_first,
                runner_on_second: tick.runner_on_second,
                runner_on_third: tick.runner_on_third,
                match_completed: tick.match_completed,
                period: tick.period.clone(),
            },
        );

        DeltaEvent {
            universal_id: uid,
            recv_monotonic_ns: recv_ns,
            material_change: material,
            goal_delta_home,
            goal_delta_away,
        }
    }

    fn update_game_state(&mut self, tick: &Tick) -> (GameState, GameState) {
        let uid = tick.universal_id.clone();
        let prev = self.game_states.get(&uid).cloned().unwrap_or_default();

        let home = tick.goals_home.or(prev.home);
        let away = tick.goals_away.or(prev.away);
        let inning_number = tick.inning_number.or(prev.inning_number);
        let inning_half = if tick.inning_half.trim().is_empty() {
            prev.inning_half.clone()
        } else {
            tick.inning_half.clone()
        };
        let outs = tick.outs.or(prev.outs);
        let balls = tick.balls.or(prev.balls);
        let strikes = tick.strikes.or(prev.strikes);
        let runner_on_first = tick.runner_on_first.or(prev.runner_on_first);
        let runner_on_second = tick.runner_on_second.or(prev.runner_on_second);
        let runner_on_third = tick.runner_on_third.or(prev.runner_on_third);

        let match_completed = if prev.match_completed.unwrap_or(false) {
            Some(true)
        } else if tick.match_completed.is_some() {
            Some(tick.match_completed.unwrap_or(false))
        } else {
            prev.match_completed
        };
        let resolved_game_state = if !tick.game_state.trim().is_empty() {
            tick.game_state.clone()
        } else if match_completed.unwrap_or(false) {
            "FINAL".to_string()
        } else if !prev.game_state.trim().is_empty() {
            prev.game_state.clone()
        } else {
            "UNKNOWN".to_string()
        };

        let mut state = GameState {
            home: prev.home,
            away: prev.away,
            total: prev.total,
            prev_total: prev.total,
            inning_number,
            inning_half,
            outs,
            balls,
            strikes,
            runner_on_first,
            runner_on_second,
            runner_on_third,
            match_completed,
            game_state: resolved_game_state,
        };

        if home.is_some() && away.is_some() {
            state.home = home;
            state.away = away;
            state.prev_total = prev.total;
            state.total = Some(home.unwrap_or(0) + away.unwrap_or(0));
        }

        self.game_states.insert(uid, state.clone());
        (prev, state)
    }

    fn build_observe_signals(
        &self,
        uid: &str,
        prev: &GameState,
        state: &GameState,
        period: &str,
        out: &mut Vec<ObserveSignal>,
    ) {
        if prev.game_state != state.game_state {
            out.push(ObserveSignal {
                event_type: "game_state_changed".to_string(),
                game_id: uid.to_string(),
                payload: json!({
                    "old_game_state": if prev.game_state.trim().is_empty() { "UNKNOWN" } else { prev.game_state.as_str() },
                    "new_game_state": if state.game_state.trim().is_empty() { "UNKNOWN" } else { state.game_state.as_str() },
                }),
            });
        }
        if prev.home != state.home || prev.away != state.away {
            out.push(ObserveSignal {
                event_type: "score_changed".to_string(),
                game_id: uid.to_string(),
                payload: json!({
                    "old_home_score": prev.home,
                    "old_away_score": prev.away,
                    "new_home_score": state.home,
                    "new_away_score": state.away,
                    "period": period,
                    "game_state": state.game_state,
                }),
            });
        }
    }
}

fn tick_matches_state_row(tick: &Tick, row: &StateRow) -> bool {
    tick.action == row.action
        && tick.goals_home == row.goals_home
        && tick.goals_away == row.goals_away
        && tick.inning_number == row.inning_number
        && tick.inning_half == row.inning_half
        && tick.outs == row.outs
        && tick.balls == row.balls
        && tick.strikes == row.strikes
        && tick.runner_on_first == row.runner_on_first
        && tick.runner_on_second == row.runner_on_second
        && tick.runner_on_third == row.runner_on_third
        && tick.match_completed == row.match_completed
        && tick.period == row.period
}

pub(crate) fn norm(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn canonical_market_type(input: &str) -> String {
    let raw = norm(input).replace('-', "_").replace(' ', "_");
    match raw.as_str() {
        "total" | "totals" | "ou" | "o_u" => "totals".to_string(),
        "nrfi" | "nfri" => "nrfi".to_string(),
        "spread" | "spreads" => "spread".to_string(),
        "moneyline" | "game" | "child_moneyline" | "first_half_moneyline" => {
            "moneyline".to_string()
        }
        _ => raw,
    }
}

pub(crate) fn process_score_frame_value(
    engine: &mut NativeMlbEngine,
    frame: &Value,
    recv_monotonic_ns: i64,
    source_recv_monotonic_ns: i64,
) -> (ProcessResult, i64) {
    let mut out = ProcessResult {
        decision: "no_action".to_string(),
        reason: "native_batch_no_action".to_string(),
        ..Default::default()
    };
    let mut events_in = 0i64;
    for msg in iter_payload_items_value(frame).iter() {
        let mtype = get_value_str(msg, "type").to_lowercase();
        if mtype == "ping" || mtype != "next" {
            continue;
        }
        let row = msg
            .get("payload")
            .and_then(|x| x.get("data"))
            .and_then(|x| x.get("sportsMatchStateUpdatedV2"));
        if let Some(row_val) = row {
            let tick = parse_tick_from_kalstrop_row_value(
                row_val,
                recv_monotonic_ns,
                source_recv_monotonic_ns,
            );
            events_in += 1;
            let one = engine.process_tick(tick);
            out.drops_cooldown += one.drops_cooldown;
            out.drops_debounce += one.drops_debounce;
            out.drops_one_shot += one.drops_one_shot;
            out.decision_non_material += one.decision_non_material;
            out.decision_no_action += one.decision_no_action;
            out.observe_signals.extend(one.observe_signals);
            if !one.intents.is_empty() {
                out.intents.extend(one.intents);
                out.reason = one.reason;
            }
        }
    }
    if !out.intents.is_empty() {
        out.decision = "action".to_string();
    }
    (out, events_in)
}

pub(crate) fn serde_value_to_py(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(v) => Ok(v.into_py(py)),
        Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                Ok(v.into_py(py))
            } else if let Some(v) = n.as_u64() {
                Ok(v.into_py(py))
            } else if let Some(v) = n.as_f64() {
                Ok(v.into_py(py))
            } else {
                Ok(py.None())
            }
        }
        Value::String(s) => Ok(s.clone().into_py(py)),
        Value::Array(arr) => {
            let out = PyList::empty_bound(py);
            for item in arr.iter() {
                out.append(serde_value_to_py(py, item)?)?;
            }
            Ok(out.into_py(py))
        }
        Value::Object(map) => {
            let out = PyDict::new_bound(py);
            for (k, v) in map.iter() {
                out.set_item(k, serde_value_to_py(py, v)?)?;
            }
            Ok(out.into_py(py))
        }
    }
}

fn result_to_py(py: Python<'_>, res: ProcessResult) -> PyResult<PyObject> {
    let out = PyDict::new_bound(py);
    out.set_item("decision", res.decision)?;
    out.set_item("reason", res.reason)?;
    out.set_item("drops_cooldown", res.drops_cooldown)?;
    out.set_item("drops_debounce", res.drops_debounce)?;
    out.set_item("drops_one_shot", res.drops_one_shot)?;
    out.set_item("decision_non_material", res.decision_non_material)?;
    out.set_item("decision_no_action", res.decision_no_action)?;

    let intents = PyList::empty_bound(py);
    for intent in res.intents.iter() {
        let row = PyDict::new_bound(py);
        row.set_item("strategy_key", intent.strategy_key.clone())?;
        row.set_item("token_id", intent.token_id.clone())?;
        row.set_item("side", intent.side.clone())?;
        row.set_item("notional_usdc", intent.notional_usdc)?;
        row.set_item("limit_price", intent.limit_price)?;
        row.set_item("time_in_force", intent.time_in_force.clone())?;
        row.set_item("condition_id", intent.condition_id.clone())?;
        row.set_item("source_universal_id", intent.source_universal_id.clone())?;
        row.set_item("chain_id", intent.chain_id.clone())?;
        row.set_item("reason", intent.reason.clone())?;
        row.set_item("market_type", intent.market_type.clone())?;
        row.set_item("outcome_semantic", intent.outcome_semantic.clone())?;
        intents.append(row)?;
    }
    out.set_item("intents", intents)?;
    Ok(out.into_py(py))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::process_score_frame_value;
    use std::collections::HashMap;

    #[test]
    fn observe_signals_propagated_in_batch() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());

        let frame1 = json!({
            "type": "next",
            "payload": {
                "data": {
                    "sportsMatchStateUpdatedV2": {
                        "fixtureId": "g1",
                        "matchSummary": {"eventState": "live", "homeScore": 0, "awayScore": 0},
                        "_hotpath_baseball": {"inning_number": 1, "inning_half": "top"}
                    }
                }
            }
        });
        let (out1, _) = process_score_frame_value(&mut engine, &frame1, 1000, 1000);
        assert!(
            !out1.observe_signals.is_empty(),
            "first tick should produce observe_signals for initial state"
        );

        let frame2 = json!({
            "type": "next",
            "payload": {
                "data": {
                    "sportsMatchStateUpdatedV2": {
                        "fixtureId": "g1",
                        "matchSummary": {"eventState": "live", "homeScore": 1, "awayScore": 0},
                        "_hotpath_baseball": {"inning_number": 1, "inning_half": "top"}
                    }
                }
            }
        });
        let (out2, events_in) = process_score_frame_value(&mut engine, &frame2, 2000, 2000);
        assert_eq!(events_in, 1);
        assert!(
            out2.observe_signals
                .iter()
                .any(|s| s.event_type == "score_changed"),
            "score change should produce score_changed observe_signal"
        );
    }

    #[test]
    fn observe_signals_accumulate_across_batch_items() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());

        let setup1 = json!({"type": "next", "payload": {"data": {"sportsMatchStateUpdatedV2": {
            "fixtureId": "g1", "matchSummary": {"eventState": "live", "homeScore": 0, "awayScore": 0}
        }}}});
        let setup2 = json!({"type": "next", "payload": {"data": {"sportsMatchStateUpdatedV2": {
            "fixtureId": "g2", "matchSummary": {"eventState": "live", "homeScore": 0, "awayScore": 0}
        }}}});
        process_score_frame_value(&mut engine, &setup1, 1000, 1000);
        process_score_frame_value(&mut engine, &setup2, 1000, 1000);

        let batch_frame = json!([
            {"type": "next", "payload": {"data": {"sportsMatchStateUpdatedV2": {
                "fixtureId": "g1", "matchSummary": {"eventState": "live", "homeScore": 1, "awayScore": 0}
            }}}},
            {"type": "next", "payload": {"data": {"sportsMatchStateUpdatedV2": {
                "fixtureId": "g2", "matchSummary": {"eventState": "live", "homeScore": 0, "awayScore": 2}
            }}}}
        ]);
        let (out, events_in) = process_score_frame_value(&mut engine, &batch_frame, 3000, 3000);
        assert_eq!(events_in, 2);
        let score_changed_count = out
            .observe_signals
            .iter()
            .filter(|s| s.event_type == "score_changed")
            .count();
        assert_eq!(
            score_changed_count, 2,
            "both game score changes should produce observe_signals"
        );
    }

    #[test]
    fn multi_market_totals_and_nrfi_both_evaluated() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());
        let game_id = "g1".to_string();

        engine.over_targets_by_game.insert(
            game_id.clone(),
            vec![TotalsTarget {
                line: 0.5,
                token_id: "tok_over".to_string(),
                condition_id: "cond_over".to_string(),
                strategy_key: "sk_over".to_string(),
            }],
        );
        engine
            .over_lines_by_game
            .insert(game_id.clone(), vec![0.5]);

        let mut nrfi_map = HashMap::new();
        nrfi_map.insert(
            "yes".to_string(),
            NrfiTarget {
                token_id: "tok_nrfi_yes".to_string(),
                condition_id: "cond_nrfi".to_string(),
                strategy_key: "sk_nrfi_yes".to_string(),
            },
        );
        engine
            .nrfi_targets_by_game
            .insert(game_id.clone(), nrfi_map);

        let tick1 = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top".to_string(),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);

        let tick2 = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 2000,
            goals_home: Some(1),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top".to_string(),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(out.decision, "action");
        let has_totals = out.intents.iter().any(|i| i.market_type == "totals");
        let has_nrfi = out.intents.iter().any(|i| i.market_type == "nrfi");
        assert!(has_totals, "totals over intent should be present");
        assert!(has_nrfi, "nrfi yes intent should be present");
        assert!(
            out.reason.contains('+'),
            "reason should join multiple evaluator reasons"
        );
    }

    #[test]
    fn evaluate_final_fires_alongside_totals_at_completion() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());
        let game_id = "g1".to_string();

        engine.under_targets_by_game.insert(
            game_id.clone(),
            vec![TotalsTarget {
                line: 8.5,
                token_id: "tok_under".to_string(),
                condition_id: "cond_under".to_string(),
                strategy_key: "sk_under".to_string(),
            }],
        );

        let mut ml_map = HashMap::new();
        ml_map.insert(
            "home".to_string(),
            FinalTarget {
                token_id: "tok_ml_home".to_string(),
                condition_id: "cond_ml".to_string(),
                strategy_key: "sk_ml_home".to_string(),
            },
        );
        engine.moneyline_by_game.insert(game_id.clone(), ml_map);

        let tick1 = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 1000,
            goals_home: Some(3),
            goals_away: Some(1),
            inning_number: Some(9),
            inning_half: "bottom".to_string(),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);

        let tick2 = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 2000,
            goals_home: Some(3),
            goals_away: Some(1),
            inning_number: Some(9),
            inning_half: "bottom".to_string(),
            match_completed: Some(true),
            game_state: "FINAL".to_string(),
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(out.decision, "action");
        let has_under = out
            .intents
            .iter()
            .any(|i| i.market_type == "totals" && i.outcome_semantic == "under");
        let has_ml = out.intents.iter().any(|i| i.market_type == "moneyline");
        assert!(has_under, "under final intent should be present");
        assert!(has_ml, "moneyline final intent should be present");
    }

    #[test]
    fn nrfi_late_subscription_past_first_inning_blocked() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());
        let game_id = "g1".to_string();

        let mut nrfi_map = HashMap::new();
        nrfi_map.insert(
            "no".to_string(),
            NrfiTarget {
                token_id: "tok_nrfi_no".to_string(),
                condition_id: "cond_nrfi".to_string(),
                strategy_key: "sk_nrfi_no".to_string(),
            },
        );
        engine
            .nrfi_targets_by_game
            .insert(game_id.clone(), nrfi_map);

        let tick = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(2),
            inning_half: "top".to_string(),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        let has_nrfi = out.intents.iter().any(|i| i.market_type == "nrfi");
        assert!(!has_nrfi, "late subscription should not produce NRFI intent");
        assert!(engine.nrfi_resolved_games.contains(&game_id));
    }

    #[test]
    fn nrfi_first_inning_subscription_allows_evaluation() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());
        let game_id = "g1".to_string();

        let mut nrfi_map = HashMap::new();
        nrfi_map.insert(
            "yes".to_string(),
            NrfiTarget {
                token_id: "tok_nrfi_yes".to_string(),
                condition_id: "cond_nrfi".to_string(),
                strategy_key: "sk_nrfi_yes".to_string(),
            },
        );
        engine
            .nrfi_targets_by_game
            .insert(game_id.clone(), nrfi_map);

        let tick1 = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top".to_string(),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);
        assert!(engine.nrfi_first_inning_observed.contains(&game_id));

        let tick2 = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 2000,
            goals_home: Some(1),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top".to_string(),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        let has_nrfi = out.intents.iter().any(|i| i.market_type == "nrfi");
        assert!(
            has_nrfi,
            "NRFI yes intent should fire for observed first inning run"
        );
    }

    #[test]
    fn nrfi_completed_game_first_tick_blocked() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());
        let game_id = "g1".to_string();

        let mut nrfi_map = HashMap::new();
        nrfi_map.insert(
            "no".to_string(),
            NrfiTarget {
                token_id: "tok_nrfi_no".to_string(),
                condition_id: "cond_nrfi".to_string(),
                strategy_key: "sk_nrfi_no".to_string(),
            },
        );
        engine
            .nrfi_targets_by_game
            .insert(game_id.clone(), nrfi_map);

        let tick = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(9),
            inning_half: "bottom".to_string(),
            match_completed: Some(true),
            game_state: "FINAL".to_string(),
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        let has_nrfi = out.intents.iter().any(|i| i.market_type == "nrfi");
        assert!(
            !has_nrfi,
            "completed game on first observation should not produce NRFI"
        );
        assert!(engine.nrfi_resolved_games.contains(&game_id));
    }

    #[test]
    fn nrfi_no_inning_data_defers_evaluation() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0, 5.0, 0.52, "FAK".to_string());
        let game_id = "g1".to_string();

        let mut nrfi_map = HashMap::new();
        nrfi_map.insert(
            "no".to_string(),
            NrfiTarget {
                token_id: "tok_nrfi_no".to_string(),
                condition_id: "cond_nrfi".to_string(),
                strategy_key: "sk_nrfi_no".to_string(),
            },
        );
        engine
            .nrfi_targets_by_game
            .insert(game_id.clone(), nrfi_map);

        let tick = Tick {
            universal_id: game_id.clone(),
            action: "update".to_string(),
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            game_state: "live".to_string(),
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        let has_nrfi = out.intents.iter().any(|i| i.market_type == "nrfi");
        assert!(!has_nrfi, "no inning data should defer NRFI evaluation");
        assert!(!engine.nrfi_first_inning_observed.contains(&game_id));
        assert!(!engine.nrfi_resolved_games.contains(&game_id));
    }
}

