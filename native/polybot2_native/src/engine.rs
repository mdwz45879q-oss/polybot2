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
            game_id_to_idx: HashMap::new(),
            game_ids: Vec::new(),
            game_targets: Vec::new(),
            target_slots: Vec::new(),
            tokens: Vec::new(),
            token_id_to_idx: HashMap::new(),
            registry: None,
            kickoff_ts: Vec::new(),
            token_ids_by_game: Vec::new(),
            has_totals: Vec::new(),
            has_nrfi: Vec::new(),
            has_final: Vec::new(),
            rows: Vec::new(),
            game_states: Vec::new(),
            totals_final_under_emitted: Vec::new(),
            nrfi_resolved_games: Vec::new(),
            nrfi_first_inning_observed: Vec::new(),
            final_resolved_games: Vec::new(),
            attempted: Vec::new(),
            last_emit_ns: Vec::new(),
            last_signature: Vec::new(),
        }
    }

    pub fn reset_runtime_state(&mut self) {
        self.rows.fill(None);
        for gs in &mut self.game_states {
            *gs = GameState::default();
        }
        self.totals_final_under_emitted.fill(false);
        self.nrfi_resolved_games.fill(false);
        self.nrfi_first_inning_observed.fill(false);
        self.final_resolved_games.fill(false);
        self.attempted.fill(false);
        self.last_emit_ns.fill(0);
        self.last_signature.fill(None);
    }

    pub fn load_plan(&mut self, plan: &Bound<'_, PyDict>) -> PyResult<()> {
        self.game_id_to_idx.clear();
        self.game_ids.clear();
        self.game_targets.clear();
        self.target_slots.clear();
        self.tokens.clear();
        self.token_id_to_idx.clear();
        self.registry = None;
        self.kickoff_ts.clear();
        self.token_ids_by_game.clear();
        self.has_totals.clear();
        self.has_nrfi.clear();
        self.has_final.clear();

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

            let gidx = GameIdx(self.game_ids.len() as u16);
            self.game_id_to_idx.insert(uid.clone(), gidx);
            self.game_ids.push(uid.clone());

            let kickoff = get_i64_opt(game, "kickoff_ts_utc");
            self.kickoff_ts.push(kickoff);

            let markets_obj = game
                .get_item("markets")?
                .ok_or_else(|| PyValueError::new_err("game.markets is required"))?;
            let markets: &Bound<'_, PyList> = markets_obj.downcast()?;

            let mut game_tgt = GameTargets::default();
            let mut game_has_totals = false;
            let mut game_has_nrfi = false;
            let mut game_has_final = false;
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

                    let token_idx = match self.token_id_to_idx.get(&token_id) {
                        Some(&idx) => idx,
                        None => {
                            let idx = TokenIdx(self.tokens.len() as u16);
                            self.tokens.push(TokenSlot { token_id: token_id.clone() });
                            self.token_id_to_idx.insert(token_id.clone(), idx);
                            idx
                        }
                    };
                    let tidx = TargetIdx(self.target_slots.len() as u16);
                    self.target_slots.push(TargetSlot {
                        token_idx,
                        strategy_key,
                    });

                    let market_type = sports_market_type.as_str();

                    match market_type {
                        "totals" => {
                            game_has_totals = true;
                            if let Some(l) = line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.over_lines.push(OverLine { half_int: half, target_idx: tidx }),
                                    "under" => game_tgt.under_lines.push(OverLine { half_int: half, target_idx: tidx }),
                                    _ => {}
                                }
                            }
                        }
                        "nrfi" => {
                            game_has_nrfi = true;
                            match semantic.as_str() {
                                "yes" => game_tgt.nrfi_yes = Some(tidx),
                                "no" => game_tgt.nrfi_no = Some(tidx),
                                _ => {}
                            }
                        }
                        "moneyline" => {
                            game_has_final = true;
                            match semantic.as_str() {
                                "home" => game_tgt.moneyline_home = Some(tidx),
                                "away" => game_tgt.moneyline_away = Some(tidx),
                                _ => {}
                            }
                        }
                        "spread" => {
                            game_has_final = true;
                            if let Some(l) = line {
                                let side = match semantic.as_str() {
                                    "home" => SpreadSide::Home,
                                    "away" => SpreadSide::Away,
                                    _ => continue,
                                };
                                game_tgt.spreads.push((side, l, tidx));
                            }
                        }
                        _ => {}
                    }
                }
            }

            game_tgt.over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.under_lines.sort_by_key(|ol| ol.half_int);

            self.game_targets.push(game_tgt);
            self.has_totals.push(game_has_totals);
            self.has_nrfi.push(game_has_nrfi);
            self.has_final.push(game_has_final);

            if !token_ids.is_empty() {
                let mut token_list = token_ids.into_iter().collect::<Vec<_>>();
                token_list.sort();
                token_list.dedup();
                self.token_ids_by_game.push(token_list);
            } else {
                self.token_ids_by_game.push(Vec::new());
            }
        }

        let num_games = self.game_ids.len();
        let num_targets = self.target_slots.len();
        self.rows = vec![None; num_games];
        self.game_states = vec![GameState::default(); num_games];
        self.totals_final_under_emitted = vec![false; num_games];
        self.nrfi_resolved_games = vec![false; num_games];
        self.nrfi_first_inning_observed = vec![false; num_games];
        self.final_resolved_games = vec![false; num_games];
        self.attempted = vec![false; num_targets];
        self.last_emit_ns = vec![0i64; num_targets];
        self.last_signature = vec![None; num_targets];

        // Freeze the registry. Cloned via `clone_registry()` for sharing with
        // `DispatchHandle` and `OrderSubmitter` at runtime startup.
        self.registry = Some(Arc::new(TargetRegistry {
            tokens: self.tokens.clone(),
            targets: self.target_slots.clone(),
        }));

        Ok(())
    }

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
        // Resolve strategy_key + token_id strings at the FFI boundary. The
        // engine itself never allocates these strings on the success path.
        for intent in &result.intents {
            let target = &self.target_slots[intent.target_idx.0 as usize];
            let token = &self.tokens[target.token_idx.0 as usize];
            let row = PyDict::new_bound(py);
            row.set_item("strategy_key", target.strategy_key.clone())?;
            row.set_item("token_id", token.token_id.clone())?;
            intent_list.append(row)?;
        }
        out.set_item("intents", intent_list)?;
        Ok(out.into_py(py))
    }
}

impl NativeMlbEngine {
    /// Returns an `Arc` clone of the read-only target registry built by
    /// `load_plan`. Returns `None` if `load_plan` has not yet been called.
    /// Used by `runtime.rs::start` to share the registry with the dispatch
    /// handle and submitter thread.
    pub(crate) fn clone_registry(&self) -> Option<Arc<TargetRegistry>> {
        self.registry.as_ref().map(Arc::clone)
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
            if let Some(&gidx) = self.game_id_to_idx.get(id) {
                if let Some(kickoff) = self.kickoff_ts[gidx.0 as usize] {
                    if now_ts_utc >= kickoff.saturating_sub(lead_seconds) {
                        out.push(id.to_string());
                    }
                } else {
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
        for ids in &self.token_ids_by_game {
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

    fn cleanup_completed_game_idx(&mut self, gidx: GameIdx) {
        let gi = gidx.0 as usize;
        self.rows[gi] = None;
        self.game_states[gi] = GameState::default();
        self.nrfi_first_inning_observed[gi] = false;
        // totals_final_under_emitted, nrfi_resolved_games, final_resolved_games,
        // attempted, last_emit_ns, last_signature are intentionally preserved as
        // tombstones — a repeated final frame must not re-emit intents.
    }

    #[allow(dead_code)]
    pub(crate) fn cleanup_completed_game(&mut self, game_id: &str) {
        if let Some(&gidx) = self.game_id_to_idx.get(game_id) {
            self.cleanup_completed_game_idx(gidx);
        }
    }

    pub(crate) fn is_game_completed(&self, game_id: &str) -> bool {
        if let Some(&gidx) = self.game_id_to_idx.get(game_id) {
            let gi = gidx.0 as usize;
            if self.final_resolved_games[gi] {
                return true;
            }
            self.game_states[gi].match_completed.unwrap_or(false)
        } else {
            false
        }
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

        let Some(&gidx) = self.game_id_to_idx.get(&game_id) else {
            return TickResult {
                game_id,
                state: GameState::default(),
                intents: vec![],
                material: false,
            };
        };

        let delta = self.apply_delta(gidx, &tick);
        if !delta.material_change {
            let state = self.game_states[gidx.0 as usize];
            return TickResult {
                game_id,
                state,
                intents: vec![],
                material: false,
            };
        }

        let (_prev_state, state) = self.update_game_state(gidx, &tick);

        let mut raw_intents = Vec::new();
        raw_intents.extend(self.evaluate_totals(gidx, &state));
        raw_intents.extend(self.evaluate_nrfi(gidx, &state, &delta));
        raw_intents.extend(self.evaluate_final(gidx, &state));

        if raw_intents.is_empty() {
            return TickResult {
                game_id,
                state,
                intents: vec![],
                material: true,
            };
        }

        let recv_ns = delta.recv_monotonic_ns;
        let mut emitted: Vec<Intent> = Vec::new();
        for raw in raw_intents {
            let ti = raw.target_idx.0 as usize;

            if self.attempted[ti] {
                continue;
            }

            let last_emit = self.last_emit_ns[ti];
            if self.decision_cooldown_ns > 0
                && last_emit > 0
                && (recv_ns - last_emit) < self.decision_cooldown_ns
            {
                continue;
            }

            let token_idx = self.target_slots[ti].token_idx;
            let sig = DecisionSig { token_idx };
            if self.decision_debounce_ns > 0 {
                if let Some(last_sig) = self.last_signature[ti] {
                    if last_sig == sig
                        && last_emit > 0
                        && (recv_ns - last_emit) < self.decision_debounce_ns
                    {
                        continue;
                    }
                }
            }

            self.attempted[ti] = true;
            self.last_emit_ns[ti] = recv_ns;
            self.last_signature[ti] = Some(sig);

            // Intent carries only TargetIdx — string fields are reconstructed
            // from the registry at the dispatch handle (for fail-closed errors)
            // and the submitter (for log output).
            emitted.push(Intent { target_idx: raw.target_idx });
        }

        let gi = gidx.0 as usize;
        if state.match_completed.unwrap_or(false) && self.final_resolved_games[gi] {
            self.cleanup_completed_game_idx(gidx);
        }

        TickResult {
            game_id,
            state,
            intents: emitted,
            material: true,
        }
    }

    fn apply_delta(&mut self, gidx: GameIdx, tick: &Tick) -> DeltaEvent {
        let gi = gidx.0 as usize;
        let recv_ns = tick.recv_monotonic_ns;

        if let Some(row) = self.rows[gi].as_ref() {
            if tick_matches_state_row(tick, row)
                && (recv_ns - row.seen_monotonic_ns) <= self.dedup_ttl_ns
            {
                if let Some(row) = self.rows[gi].as_mut() {
                    row.seen_monotonic_ns = recv_ns;
                }
                return DeltaEvent {
                    recv_monotonic_ns: recv_ns,
                    material_change: false,
                    ..Default::default()
                };
            }
        }

        let mut goal_delta_home = 0;
        let mut goal_delta_away = 0;
        if let Some(row) = self.rows[gi].as_ref() {
            goal_delta_home = tick.goals_home.unwrap_or(0) - row.goals_home.unwrap_or(0);
            goal_delta_away = tick.goals_away.unwrap_or(0) - row.goals_away.unwrap_or(0);
        }

        self.rows[gi] = Some(StateRow {
            seen_monotonic_ns: recv_ns,
            action: tick.action,
            goals_home: tick.goals_home,
            goals_away: tick.goals_away,
            inning_number: tick.inning_number,
            inning_half: tick.inning_half,
            match_completed: tick.match_completed,
        });

        DeltaEvent {
            recv_monotonic_ns: recv_ns,
            material_change: true,
            goal_delta_home,
            goal_delta_away,
        }
    }

    fn update_game_state(&mut self, gidx: GameIdx, tick: &Tick) -> (GameState, GameState) {
        let gi = gidx.0 as usize;
        let prev = self.game_states[gi];

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

        self.game_states[gi] = state;
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

    struct GameTargetBuilder<'a> {
        slots: &'a mut Vec<TargetSlot>,
        tokens: &'a mut Vec<TokenSlot>,
        token_id_to_idx: &'a mut HashMap<String, TokenIdx>,
        targets: GameTargets,
        has_totals: bool,
        has_nrfi: bool,
        has_final: bool,
    }

    impl<'a> GameTargetBuilder<'a> {
        fn alloc_token_idx(&mut self, token_id: &str) -> TokenIdx {
            if let Some(&idx) = self.token_id_to_idx.get(token_id) {
                return idx;
            }
            let idx = TokenIdx(self.tokens.len() as u16);
            self.tokens.push(TokenSlot { token_id: token_id.to_string() });
            self.token_id_to_idx.insert(token_id.to_string(), idx);
            idx
        }

        fn alloc(&mut self, token_id: &str, strategy_key: &str) -> TargetIdx {
            let token_idx = self.alloc_token_idx(token_id);
            let idx = TargetIdx(self.slots.len() as u16);
            self.slots.push(TargetSlot {
                token_idx,
                strategy_key: strategy_key.to_string(),
            });
            idx
        }

        fn over(&mut self, line: f64, token_id: &str) {
            self.has_totals = true;
            let lk = crate::eval::line_key(line);
            let tidx = self.alloc(token_id, &format!("_:TOTAL:OVER:{}", lk));
            self.targets.over_lines.push(OverLine {
                half_int: line.floor() as u16,
                target_idx: tidx,
            });
        }

        fn under(&mut self, line: f64, token_id: &str) {
            self.has_totals = true;
            let lk = crate::eval::line_key(line);
            let tidx = self.alloc(token_id, &format!("_:TOTAL:UNDER:{}", lk));
            self.targets.under_lines.push(OverLine {
                half_int: line.floor() as u16,
                target_idx: tidx,
            });
        }

        fn nrfi_yes(&mut self, token_id: &str) {
            self.has_nrfi = true;
            let tidx = self.alloc(token_id, "_:NRFI:YES");
            self.targets.nrfi_yes = Some(tidx);
        }

        fn nrfi_no(&mut self, token_id: &str) {
            self.has_nrfi = true;
            let tidx = self.alloc(token_id, "_:NRFI:NO");
            self.targets.nrfi_no = Some(tidx);
        }

        fn moneyline_home(&mut self, token_id: &str) {
            self.has_final = true;
            let tidx = self.alloc(token_id, "_:MONEYLINE:HOME");
            self.targets.moneyline_home = Some(tidx);
        }

        #[allow(dead_code)]
        fn moneyline_away(&mut self, token_id: &str) {
            self.has_final = true;
            let tidx = self.alloc(token_id, "_:MONEYLINE:AWAY");
            self.targets.moneyline_away = Some(tidx);
        }
    }

    fn add_game(engine: &mut NativeMlbEngine, game_id: &str, build: impl FnOnce(&mut GameTargetBuilder)) {
        let gidx = GameIdx(engine.game_ids.len() as u16);
        engine.game_id_to_idx.insert(game_id.to_string(), gidx);
        engine.game_ids.push(game_id.to_string());

        let mut builder = GameTargetBuilder {
            slots: &mut engine.target_slots,
            tokens: &mut engine.tokens,
            token_id_to_idx: &mut engine.token_id_to_idx,
            targets: GameTargets::default(),
            has_totals: false,
            has_nrfi: false,
            has_final: false,
        };
        build(&mut builder);

        builder.targets.over_lines.sort_by_key(|ol| ol.half_int);
        builder.targets.under_lines.sort_by_key(|ol| ol.half_int);

        engine.game_targets.push(builder.targets);
        engine.has_totals.push(builder.has_totals);
        engine.has_nrfi.push(builder.has_nrfi);
        engine.has_final.push(builder.has_final);
        engine.kickoff_ts.push(None);
        engine.token_ids_by_game.push(vec![]);
        engine.rows.push(None);
        engine.game_states.push(GameState::default());
        engine.totals_final_under_emitted.push(false);
        engine.nrfi_resolved_games.push(false);
        engine.nrfi_first_inning_observed.push(false);
        engine.final_resolved_games.push(false);
    }

    /// Test-time finalize: resize per-target runtime vecs and build the
    /// registry. Replaces the old `sync_target_vecs` helper. Tests call this
    /// once after all `add_game` calls.
    fn sync_target_vecs(engine: &mut NativeMlbEngine) {
        let n = engine.target_slots.len();
        engine.attempted.resize(n, false);
        engine.last_emit_ns.resize(n, 0);
        engine.last_signature.resize(n, None);
        engine.registry = Some(Arc::new(TargetRegistry {
            tokens: engine.tokens.clone(),
            targets: engine.target_slots.clone(),
        }));
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
    fn shared_token_dedupes_to_single_token_idx() {
        // Two targets pointing to the same token_id must share one TokenIdx.
        // This preserves the "one signed order per unique token" invariant
        // when the pool moves to TokenIdx-keyed indexing in commit 3.
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.over(2.5, "tok_shared");
            b.under(2.5, "tok_shared");
        });
        sync_target_vecs(&mut engine);
        assert_eq!(engine.tokens.len(), 1, "shared token should dedupe to one TokenIdx");
        assert_eq!(engine.target_slots.len(), 2, "two distinct targets");
        assert_eq!(engine.target_slots[0].token_idx, engine.target_slots[1].token_idx);
        let registry = engine.clone_registry().expect("registry should be set");
        assert_eq!(registry.tokens.len(), 1);
        assert_eq!(registry.targets.len(), 2);
    }

    #[test]
    fn frame_parse_produces_tick_results() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |_| {});
        sync_target_vecs(&mut engine);

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
        add_game(&mut engine, "g1", |_| {});
        add_game(&mut engine, "g2", |_| {});
        sync_target_vecs(&mut engine);

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
        add_game(&mut engine, "g1", |b| {
            b.over(0.5, "tok_over");
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let tick1 = Tick {
            universal_id: "g1".to_string(),
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
            universal_id: "g1".to_string(),
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
        add_game(&mut engine, "g1", |b| {
            b.under(8.5, "tok_under");
            b.moneyline_home("tok_ml_home");
        });
        sync_target_vecs(&mut engine);

        let tick1 = Tick {
            universal_id: "g1".to_string(),
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
            universal_id: "g1".to_string(),
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
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let tick = Tick {
            universal_id: "g1".to_string(),
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
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_first_inning_subscription_allows_evaluation() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let tick1 = Tick {
            universal_id: "g1".to_string(),
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
        assert!(engine.nrfi_first_inning_observed[0]);

        let tick2 = Tick {
            universal_id: "g1".to_string(),
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
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let tick = Tick {
            universal_id: "g1".to_string(),
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
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let tick = Tick {
            universal_id: "g1".to_string(),
            action: "update",
            recv_monotonic_ns: 1000,
            goals_home: Some(0),
            goals_away: Some(0),
            game_state: "live",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(out.intents.is_empty(), "no inning data should defer NRFI evaluation");
        assert!(!engine.nrfi_first_inning_observed[0]);
        assert!(!engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn totals_over_multi_line_crossing() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.over(1.5, "tok_over_1.5");
            b.over(2.5, "tok_over_2.5");
            b.over(3.5, "tok_over_3.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 0, 0, 1000));
        let out = engine.process_tick(tick_with_score("g1", 3, 0, 2000));

        assert_eq!(out.intents.len(), 2, "should emit intents for crossed lines 1.5 and 2.5");
    }

    #[test]
    fn totals_over_sequential_crossings() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.over(1.5, "tok_over_1.5");
            b.over(2.5, "tok_over_2.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 1, 0, 1000));
        let out1 = engine.process_tick(tick_with_score("g1", 2, 0, 2000));
        assert_eq!(out1.intents.len(), 1, "first crossing fires 1.5");

        let out2 = engine.process_tick(tick_with_score("g1", 2, 1, 3000));
        assert_eq!(out2.intents.len(), 1, "second crossing fires 2.5");
    }

    #[test]
    fn totals_over_no_crossing() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.over(5.5, "tok_over_5.5");
            b.over(6.5, "tok_over_6.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 0, 0, 1000));
        let out = engine.process_tick(tick_with_score("g1", 1, 1, 2000));

        assert!(out.intents.is_empty(), "total 2 is below all lines (5.5, 6.5)");
    }

    #[test]
    fn totals_over_one_shot_prevents_duplicate() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.over(1.5, "tok_over_1.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 0, 0, 1000));
        let out1 = engine.process_tick(tick_with_score("g1", 2, 0, 2000));
        assert_eq!(out1.intents.len(), 1, "first crossing should fire");

        let out2 = engine.process_tick(tick_with_score("g1", 3, 0, 3000));
        assert!(out2.intents.is_empty(), "one-shot should prevent duplicate");
    }

    #[test]
    fn totals_under_final_all_lines_above_total() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.under(5.5, "tok_under_5.5");
            b.under(6.5, "tok_under_6.5");
            b.under(7.5, "tok_under_7.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 3, 2, 1000));

        let mut final_tick = tick_with_score("g1", 3, 2, 2000);
        final_tick.match_completed = Some(true);
        final_tick.game_state = "FINAL";
        let out = engine.process_tick(final_tick);

        assert_eq!(out.intents.len(), 3, "all three under lines should fire");
    }

    #[test]
    fn totals_over_grand_slam_jump() {
        let mut engine = NativeMlbEngine::new(2.0, 0.0, 0.0);
        add_game(&mut engine, "g1", |b| {
            b.over(3.5, "tok_over_3.5");
            b.over(4.5, "tok_over_4.5");
            b.over(5.5, "tok_over_5.5");
            b.over(6.5, "tok_over_6.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 2, 1, 1000));
        let out = engine.process_tick(tick_with_score("g1", 6, 1, 2000));

        assert_eq!(out.intents.len(), 4, "all four lines should be crossed");
    }
}
