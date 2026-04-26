use super::*;
use crate::kalstrop_types::KalstropFrame;
use crate::parse::{
    get_str, get_i64_opt, get_f64_opt,
    parse_tick_any,
    parse_tick_from_kalstrop_update,
};

#[pymethods]
impl NativeMlbEngine {
    #[new]
    #[pyo3(signature = (
        dedup_ttl_seconds=2.0,
        decision_cooldown_seconds=0.5,
        decision_debounce_seconds=0.1,
    ))]
    pub fn new(
        dedup_ttl_seconds: f64,
        decision_cooldown_seconds: f64,
        decision_debounce_seconds: f64,
    ) -> Self {
        Self {
            dedup_ttl_ns: (dedup_ttl_seconds.max(0.1) * 1_000_000_000.0) as i64,
            decision_cooldown_ns: (decision_cooldown_seconds.max(0.0) * 1_000_000_000.0) as i64,
            decision_debounce_ns: (decision_debounce_seconds.max(0.0) * 1_000_000_000.0) as i64,
            targets: HashMap::new(),
            games_with_totals: HashSet::new(),
            games_with_nrfi: HashSet::new(),
            games_with_final: HashSet::new(),
            under_lines_by_game: HashMap::new(),
            spread_lines_by_game: HashMap::new(),
            strategy_keys_by_game: HashMap::new(),
            kickoff_ts_by_game: HashMap::new(),
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
        self.targets.clear();
        self.games_with_totals.clear();
        self.games_with_nrfi.clear();
        self.games_with_final.clear();
        self.under_lines_by_game.clear();
        self.spread_lines_by_game.clear();
        self.strategy_keys_by_game.clear();
        self.kickoff_ts_by_game.clear();
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
            let markets_obj = game
                .get_item("markets")?
                .ok_or_else(|| PyValueError::new_err("game.markets is required"))?;
            let markets: &Bound<'_, PyList> = markets_obj.downcast()?;

            let mut token_ids: HashSet<String> = HashSet::new();

            for market_obj in markets.iter() {
                let market: &Bound<'_, PyDict> = market_obj.downcast()?;
                let sports_market_type = canonical_market_type(&get_str(market, "sports_market_type"));
                let line = get_f64_opt(market, "line");
                let targets_obj = market
                    .get_item("targets")?
                    .ok_or_else(|| PyValueError::new_err("market.targets is required"))?;
                let targets_list: &Bound<'_, PyList> = targets_obj.downcast()?;

                for target_obj in targets_list.iter() {
                    let target: &Bound<'_, PyDict> = target_obj.downcast()?;
                    let semantic = norm(&get_str(target, "outcome_semantic"));
                    let token_id = get_str(target, "token_id");
                    if token_id.is_empty() {
                        continue;
                    }
                    token_ids.insert(token_id.clone());
                    let strategy_key = get_str(target, "strategy_key");
                    if strategy_key.is_empty() {
                        continue;
                    }

                    let market_type = sports_market_type.as_str();

                    self.targets.insert(
                        strategy_key.clone(),
                        TargetEntry {
                            token_id: token_id.clone(),
                        },
                    );

                    match market_type {
                        "totals" => { self.games_with_totals.insert(uid.clone()); }
                        "nrfi" => { self.games_with_nrfi.insert(uid.clone()); }
                        "moneyline" | "spread" => { self.games_with_final.insert(uid.clone()); }
                        _ => {}
                    }

                    if market_type == "totals" && semantic.as_str() == "under" {
                        if let Some(l) = line {
                            self.under_lines_by_game.entry(uid.clone()).or_default().push(l);
                        }
                    }
                    if market_type == "spread" {
                        if let Some(l) = line {
                            let side: &'static str = match semantic.as_str() {
                                "home" => "HOME",
                                "away" => "AWAY",
                                _ => continue,
                            };
                            self.spread_lines_by_game.entry(uid.clone()).or_default().push((side, l));
                        }
                    }

                    self.strategy_keys_by_game
                        .entry(uid.clone())
                        .or_default()
                        .push(strategy_key);
                }
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

    /// Python FFI: process a single score event (used by replay.py)
    fn process_score_event(
        &mut self,
        py: Python<'_>,
        event: &Bound<'_, PyAny>,
        recv_monotonic_ns: i64,
    ) -> PyResult<PyObject> {
        let tick = parse_tick_any(event, recv_monotonic_ns);
        let result = self.process_tick(tick);
        let out = PyDict::new_bound(py);
        let decision = if result.intents.is_empty() { "no_action" } else { "action" };
        out.set_item("decision", decision)?;
        let intent_list = PyList::empty_bound(py);
        for intent in &result.intents {
            let row = PyDict::new_bound(py);
            row.set_item("strategy_key", intent.strategy_key.clone())?;
            row.set_item("token_id", intent.token_id.clone())?;
            intent_list.append(row)?;
        }
        out.set_item("intents", intent_list)?;
        Ok(out.into_py(py))
    }
}

impl NativeMlbEngine {
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

    pub(crate) fn all_token_ids(&self) -> Vec<String> {
        let mut tokens: HashSet<String> = HashSet::new();
        for ids in self.token_ids_by_game.values() {
            for token_id in ids {
                let t = token_id.trim();
                if !t.is_empty() {
                    tokens.insert(t.to_string());
                }
            }
        }
        let mut out = tokens.into_iter().collect::<Vec<_>>();
        out.sort();
        out
    }

    fn strategy_keys_for_game(&self, game_id: &str) -> Vec<String> {
        self.strategy_keys_by_game
            .get(game_id)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn cleanup_completed_game(&mut self, game_id: &str) {
        let strategy_keys = self.strategy_keys_for_game(game_id);
        self.rows.remove(game_id);
        self.game_states.remove(game_id);
        self.totals_final_under_emitted.remove(game_id);
        self.nrfi_resolved_games.remove(game_id);
        self.nrfi_first_inning_observed.remove(game_id);
        for key in &strategy_keys {
            self.attempted_strategy_keys.remove(key);
            self.last_emit_ns.remove(key);
            self.last_signature.remove(key);
        }
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

    pub(crate) fn process_tick(&mut self, tick: Tick) -> TickResult {
        let game_id = tick.universal_id.clone();

        if game_id.is_empty() {
            return TickResult {
                game_id,
                state: GameState::default(),
                intents: vec![],
                material: false,
            };
        }

        let delta = self.apply_delta(&tick);
        if !delta.material_change {
            let state = self.game_states.get(game_id.as_str()).copied().unwrap_or_default();
            return TickResult {
                game_id,
                state,
                intents: vec![],
                material: false,
            };
        }

        let (_prev_state, state) = self.update_game_state(&tick);

        let mut intents = Vec::new();
        intents.extend(self.evaluate_totals(&delta, &state));
        intents.extend(self.evaluate_nrfi(&delta, &state));
        intents.extend(self.evaluate_final(&delta, &state));

        if intents.is_empty() {
            return TickResult {
                game_id,
                state,
                intents: vec![],
                material: true,
            };
        }

        // One-shot + cooldown + debounce filter
        let recv_ns = delta.recv_monotonic_ns;
        let mut emitted: Vec<Intent> = Vec::new();
        for intent in intents {
            let strategy_key = intent.strategy_key.as_str();
            if strategy_key.is_empty() {
                continue;
            }

            let last_emit = *self.last_emit_ns.get(strategy_key).unwrap_or(&0i64);
            if self.decision_cooldown_ns > 0
                && last_emit > 0
                && (recv_ns - last_emit) < self.decision_cooldown_ns
            {
                continue;
            }

            let sig = DecisionSig {
                token_id: intent.token_id.clone(),
            };
            if self.decision_debounce_ns > 0 {
                if let Some(last_sig) = self.last_signature.get(strategy_key) {
                    if *last_sig == sig
                        && last_emit > 0
                        && (recv_ns - last_emit) < self.decision_debounce_ns
                    {
                        continue;
                    }
                }
            }

            let strategy_key_owned = intent.strategy_key.clone();
            if !self.attempted_strategy_keys.insert(strategy_key_owned.clone()) {
                continue;
            }
            self.last_emit_ns
                .insert(strategy_key_owned.clone(), recv_ns);
            self.last_signature.insert(strategy_key_owned, sig);
            emitted.push(intent);
        }

        // Defer cleanup for completed games until after intents are selected
        // but before returning (dispatch happens in the caller, after this returns).
        if state.match_completed.unwrap_or(false) && self.final_resolved_games.contains(&game_id) {
            self.cleanup_completed_game(&game_id);
        }

        TickResult {
            game_id,
            state,
            intents: emitted,
            material: true,
        }
    }

    fn apply_delta(&mut self, tick: &Tick) -> DeltaEvent {
        let uid = tick.universal_id.clone();
        let recv_ns = tick.recv_monotonic_ns;
        let prev = self.rows.get(uid.as_str());

        if let Some(row) = prev {
            if tick_matches_state_row(tick, row)
                && (recv_ns - row.seen_monotonic_ns) <= self.dedup_ttl_ns
            {
                // Refresh timestamp so identical heartbeats don't become material after TTL
                if let Some(row) = self.rows.get_mut(uid.as_str()) {
                    row.seen_monotonic_ns = recv_ns;
                }
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

        let material = true;

        self.rows.insert(
            uid.clone(),
            StateRow {
                seen_monotonic_ns: recv_ns,
                action: tick.action,
                goals_home: tick.goals_home,
                goals_away: tick.goals_away,
                inning_number: tick.inning_number,
                inning_half: tick.inning_half,
                match_completed: tick.match_completed,
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
        let prev = self.game_states.get(&uid).copied().unwrap_or_default();

        let home = tick.goals_home.or(prev.home);
        let away = tick.goals_away.or(prev.away);
        let inning_number = tick.inning_number.or(prev.inning_number);
        let inning_half = if tick.inning_half.is_empty() {
            prev.inning_half
        } else {
            tick.inning_half
        };
        let match_completed = if prev.match_completed.unwrap_or(false) {
            Some(true)
        } else if tick.match_completed.is_some() {
            Some(tick.match_completed.unwrap_or(false))
        } else {
            prev.match_completed
        };
        let resolved_game_state = if !tick.game_state.is_empty() {
            tick.game_state
        } else if match_completed.unwrap_or(false) {
            "FINAL"
        } else if !prev.game_state.is_empty() {
            prev.game_state
        } else {
            "UNKNOWN"
        };

        let mut state = GameState {
            home: prev.home,
            away: prev.away,
            total: prev.total,
            prev_total: prev.total,
            inning_number,
            inning_half,
            match_completed,
            game_state: resolved_game_state,
        };

        if home.is_some() && away.is_some() {
            state.home = home;
            state.away = away;
            state.prev_total = prev.total;
            state.total = Some(home.unwrap_or(0) + away.unwrap_or(0));
        }

        self.game_states.insert(uid, state);
        (prev, state)
    }
}

fn tick_matches_state_row(tick: &Tick, row: &StateRow) -> bool {
    tick.action == row.action
        && tick.goals_home == row.goals_home
        && tick.goals_away == row.goals_away
        && tick.inning_number == row.inning_number
        && tick.inning_half == row.inning_half
        && tick.match_completed == row.match_completed
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

fn process_single_frame(
    engine: &mut NativeMlbEngine,
    frame: &KalstropFrame<'_>,
    recv_monotonic_ns: i64,
) -> Option<TickResult> {
    if frame.msg_type != "next" {
        return None;
    }
    let update = frame
        .payload
        .as_ref()
        .and_then(|p| p.data.as_ref())
        .and_then(|d| d.update.as_ref())?;
    let tick = parse_tick_from_kalstrop_update(update, recv_monotonic_ns);
    Some(engine.process_tick(tick))
}

pub(crate) fn process_kalstrop_frame(
    engine: &mut NativeMlbEngine,
    text: &str,
    recv_monotonic_ns: i64,
) -> Vec<TickResult> {
    let mut results = Vec::new();

    let first_byte = text.as_bytes().first().copied().unwrap_or(0);
    if first_byte == b'[' {
        if let Ok(frames) = serde_json::from_str::<Vec<KalstropFrame<'_>>>(text) {
            for frame in &frames {
                if let Some(r) = process_single_frame(engine, frame, recv_monotonic_ns) {
                    results.push(r);
                }
            }
        }
    } else if let Ok(frame) = serde_json::from_str::<KalstropFrame<'_>>(text) {
        if let Some(r) = process_single_frame(engine, &frame, recv_monotonic_ns) {
            results.push(r);
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::process_kalstrop_frame;

    fn insert_target(engine: &mut NativeMlbEngine, strategy_key: &str, token_id: &str) {
        engine.targets.insert(
            strategy_key.to_string(),
            TargetEntry {
                token_id: token_id.to_string(),
            },
        );
    }

    #[test]
    fn frame_parse_produces_tick_results() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);

        let frame1 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"eventState":"live","homeScore":"0","awayScore":"0","matchStatusDisplay":[{"freeText":"1st inning top"}]}}}}}"#;
        let results1 = process_kalstrop_frame(&mut engine, frame1, 1000);
        assert_eq!(results1.len(), 1);
        assert!(results1[0].material);
        assert_eq!(results1[0].game_id, "g1");

        let frame2 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"eventState":"live","homeScore":"1","awayScore":"0","matchStatusDisplay":[{"freeText":"1st inning top"}]}}}}}"#;
        let results2 = process_kalstrop_frame(&mut engine, frame2, 2000);
        assert_eq!(results2.len(), 1);
        assert!(results2[0].material);
        assert_eq!(results2[0].state.home, Some(1));
    }

    #[test]
    fn frame_batch_parse_produces_multiple_results() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);

        let setup1 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"eventState":"live","homeScore":"0","awayScore":"0"}}}}}"#;
        let setup2 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g2","matchSummary":{"eventState":"live","homeScore":"0","awayScore":"0"}}}}}"#;
        process_kalstrop_frame(&mut engine, setup1, 1000);
        process_kalstrop_frame(&mut engine, setup2, 1000);

        let batch_frame = r#"[{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"eventState":"live","homeScore":"1","awayScore":"0"}}}}},{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g2","matchSummary":{"eventState":"live","homeScore":"0","awayScore":"2"}}}}}]"#;
        let results = process_kalstrop_frame(&mut engine, batch_frame, 3000);
        assert_eq!(results.len(), 2);
        assert!(results[0].material);
        assert!(results[1].material);
    }

    #[test]
    fn multi_market_totals_and_nrfi_both_evaluated() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let game_id = "g1".to_string();

        insert_target(&mut engine, "g1:TOTAL:OVER:0.5", "tok_over");
        engine.games_with_totals.insert(game_id.clone());

        insert_target(&mut engine, "g1:NRFI:YES", "tok_nrfi_yes");
        engine.games_with_nrfi.insert(game_id.clone());

        let tick1 = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "live",
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);

        let tick2 = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 2000,
            goals_home: Some(1),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "live",
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(out.intents.len(), 2, "should have totals over + nrfi yes intents");
    }

    #[test]
    fn evaluate_final_fires_alongside_totals_at_completion() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let game_id = "g1".to_string();

        insert_target(&mut engine, "g1:TOTAL:UNDER:8.5", "tok_under");
        engine.games_with_totals.insert(game_id.clone());
        engine.under_lines_by_game.insert(game_id.clone(), vec![8.5]);

        insert_target(&mut engine, "g1:MONEYLINE:HOME", "tok_ml_home");
        engine.games_with_final.insert(game_id.clone());

        let tick1 = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(3),
            goals_away: Some(1),
            inning_number: Some(9),
            inning_half: "bottom",
            game_state: "live",
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);

        let tick2 = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 2000,
            goals_home: Some(3),
            goals_away: Some(1),
            inning_number: Some(9),
            inning_half: "bottom",
            match_completed: Some(true),
            game_state: "FINAL",
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(out.intents.len(), 2, "should have under final + moneyline home intents");
    }

    #[test]
    fn nrfi_late_subscription_past_first_inning_blocked() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let game_id = "g1".to_string();

        insert_target(&mut engine, "g1:NRFI:NO", "tok_nrfi_no");
        engine.games_with_nrfi.insert(game_id.clone());

        let tick = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(2),
            inning_half: "top",
            game_state: "live",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(out.intents.is_empty(), "late subscription should not produce NRFI intent");
        assert!(engine.nrfi_resolved_games.contains(&game_id));
    }

    #[test]
    fn nrfi_first_inning_subscription_allows_evaluation() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let game_id = "g1".to_string();

        insert_target(&mut engine, "g1:NRFI:YES", "tok_nrfi_yes");
        engine.games_with_nrfi.insert(game_id.clone());

        let tick1 = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "live",
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);
        assert!(engine.nrfi_first_inning_observed.contains(&game_id));

        let tick2 = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 2000,
            goals_home: Some(1),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "live",
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(out.intents.len(), 1, "NRFI yes intent should fire");
    }

    #[test]
    fn nrfi_completed_game_first_tick_blocked() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let game_id = "g1".to_string();

        insert_target(&mut engine, "g1:NRFI:NO", "tok_nrfi_no");
        engine.games_with_nrfi.insert(game_id.clone());

        let tick = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(9),
            inning_half: "bottom",
            match_completed: Some(true),
            game_state: "FINAL",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(out.intents.is_empty(), "completed game on first observation should not produce NRFI");
    }

    #[test]
    fn nrfi_no_inning_data_defers_evaluation() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let game_id = "g1".to_string();

        insert_target(&mut engine, "g1:NRFI:NO", "tok_nrfi_no");
        engine.games_with_nrfi.insert(game_id.clone());

        let tick = Tick {
            universal_id: game_id.clone(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            game_state: "live",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(out.intents.is_empty(), "no inning data should defer NRFI evaluation");
        assert!(!engine.nrfi_first_inning_observed.contains(&game_id));
        assert!(!engine.nrfi_resolved_games.contains(&game_id));
    }

    fn setup_over_targets(engine: &mut NativeMlbEngine, game_id: &str, lines: &[f64]) {
        engine.games_with_totals.insert(game_id.to_string());
        for &line in lines {
            let lk = crate::eval::line_key(line);
            let key = format!("{}:TOTAL:OVER:{}", game_id, lk);
            insert_target(engine, &key, &format!("tok_over_{}", lk));
        }
    }

    fn setup_under_targets(engine: &mut NativeMlbEngine, game_id: &str, lines: &[f64]) {
        engine.games_with_totals.insert(game_id.to_string());
        for &line in lines {
            let lk = crate::eval::line_key(line);
            let key = format!("{}:TOTAL:UNDER:{}", game_id, lk);
            insert_target(engine, &key, &format!("tok_under_{}", lk));
        }
        engine.under_lines_by_game.insert(game_id.to_string(), lines.to_vec());
    }

    fn tick_with_score(game_id: &str, home: i64, away: i64, ns: i64) -> Tick {
        Tick {
            universal_id: game_id.to_string(),
            action: "update",
            recv_monotonic_ns: ns,
            goals_home: Some(home),
            goals_away: Some(away),
            inning_number: Some(3),
            inning_half: "top",
            game_state: "live",
            ..Default::default()
        }
    }

    #[test]
    fn totals_over_multi_line_crossing() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let gid = "g1";
        setup_over_targets(&mut engine, gid, &[1.5, 2.5, 3.5]);

        let _ = engine.process_tick(tick_with_score(gid, 0, 0, 1000));
        let out = engine.process_tick(tick_with_score(gid, 3, 0, 2000));

        assert_eq!(out.intents.len(), 2, "should emit intents for crossed lines 1.5 and 2.5");
    }

    #[test]
    fn totals_over_sequential_crossings() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let gid = "g1";
        setup_over_targets(&mut engine, gid, &[1.5, 2.5]);

        let _ = engine.process_tick(tick_with_score(gid, 1, 0, 1000));
        let out1 = engine.process_tick(tick_with_score(gid, 2, 0, 2000));
        assert_eq!(out1.intents.len(), 1, "first crossing fires 1.5");

        let out2 = engine.process_tick(tick_with_score(gid, 2, 1, 3000));
        assert_eq!(out2.intents.len(), 1, "second crossing fires 2.5");
    }

    #[test]
    fn totals_over_no_crossing() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let gid = "g1";
        setup_over_targets(&mut engine, gid, &[5.5, 6.5]);

        let _ = engine.process_tick(tick_with_score(gid, 0, 0, 1000));
        let out = engine.process_tick(tick_with_score(gid, 1, 1, 2000));

        assert!(out.intents.is_empty(), "total 2 is below all lines (5.5, 6.5)");
    }

    #[test]
    fn totals_over_one_shot_prevents_duplicate() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let gid = "g1";
        setup_over_targets(&mut engine, gid, &[1.5]);

        let _ = engine.process_tick(tick_with_score(gid, 0, 0, 1000));
        let out1 = engine.process_tick(tick_with_score(gid, 2, 0, 2000));
        assert_eq!(out1.intents.len(), 1, "first crossing should fire");

        let out2 = engine.process_tick(tick_with_score(gid, 3, 0, 3000));
        assert!(out2.intents.is_empty(), "one-shot should prevent duplicate");
    }

    #[test]
    fn totals_under_final_all_lines_above_total() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let gid = "g1";
        setup_under_targets(&mut engine, gid, &[5.5, 6.5, 7.5]);

        let _ = engine.process_tick(tick_with_score(gid, 3, 2, 1000));

        let mut final_tick = tick_with_score(gid, 3, 2, 2000);
        final_tick.match_completed = Some(true);
        final_tick.game_state = "FINAL";
        let out = engine.process_tick(final_tick);

        assert_eq!(out.intents.len(), 3, "all three under lines should fire");
    }

    #[test]
    fn totals_over_grand_slam_jump() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        let gid = "g1";
        setup_over_targets(&mut engine, gid, &[3.5, 4.5, 5.5, 6.5]);

        let _ = engine.process_tick(tick_with_score(gid, 2, 1, 1000));
        let out = engine.process_tick(tick_with_score(gid, 6, 1, 2000));

        assert_eq!(out.intents.len(), 4, "all four lines should be crossed");
    }
}
