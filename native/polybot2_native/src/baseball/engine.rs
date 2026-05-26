use crate::baseball::types::*;
use crate::InlineStr;
use crate::*;
use rustc_hash::FxHashMap;

#[cfg(feature = "python-extension")]
#[pymethods]
impl NativeMlbEngine {
    #[new]
    pub fn new() -> Self {
        Self {
            game_id_to_idx: FxHashMap::default(),
            game_ids: Vec::new(),
            game_leagues: Vec::new(),
            game_targets: Vec::new(),
            target_slots: Vec::new(),
            tokens: Vec::new(),
            token_id_to_idx: FxHashMap::default(),
            strategy_keys: HashSet::new(),
            registry: None,
            kickoff_ts: Vec::new(),
            token_ids_by_game: Vec::new(),
            has_totals: Vec::new(),
            has_nrfi: Vec::new(),
            has_final: Vec::new(),
            rows: Vec::new(),
            bo_rows: Vec::new(),
            game_states: Vec::new(),
            totals_final_under_emitted: Vec::new(),
            nrfi_resolved_games: Vec::new(),
            nrfi_first_inning_observed: Vec::new(),
            final_resolved_games: Vec::new(),
        }
    }

    pub fn reset_runtime_state(&mut self) {
        self.rows.fill(None);
        self.bo_rows.fill(None);
        for gs in &mut self.game_states {
            *gs = GameState::default();
        }
        self.totals_final_under_emitted.fill(false);
        self.nrfi_resolved_games.fill(false);
        self.nrfi_first_inning_observed.fill(false);
        self.final_resolved_games.fill(false);
    }
}

#[cfg(not(feature = "python-extension"))]
impl NativeMlbEngine {
    pub fn new() -> Self {
        Self {
            game_id_to_idx: FxHashMap::default(),
            game_ids: Vec::new(),
            game_leagues: Vec::new(),
            game_targets: Vec::new(),
            target_slots: Vec::new(),
            tokens: Vec::new(),
            token_id_to_idx: FxHashMap::default(),
            strategy_keys: HashSet::new(),
            registry: None,
            kickoff_ts: Vec::new(),
            token_ids_by_game: Vec::new(),
            has_totals: Vec::new(),
            has_nrfi: Vec::new(),
            has_final: Vec::new(),
            rows: Vec::new(),
            bo_rows: Vec::new(),
            game_states: Vec::new(),
            totals_final_under_emitted: Vec::new(),
            nrfi_resolved_games: Vec::new(),
            nrfi_first_inning_observed: Vec::new(),
            final_resolved_games: Vec::new(),
        }
    }

    pub fn reset_runtime_state(&mut self) {
        self.rows.fill(None);
        self.bo_rows.fill(None);
        for gs in &mut self.game_states {
            *gs = GameState::default();
        }
        self.totals_final_under_emitted.fill(false);
        self.nrfi_resolved_games.fill(false);
        self.nrfi_first_inning_observed.fill(false);
        self.final_resolved_games.fill(false);
    }
}

impl NativeMlbEngine {
    /// Load a compiled plan from a JSON string. Same schema as the PyO3
    pub(crate) fn load_plan_from_json(&mut self, plan_json: &str) -> Result<(), String> {
        self.game_id_to_idx.clear();
        self.game_ids.clear();
        self.game_targets.clear();
        self.target_slots.clear();
        self.tokens.clear();
        self.token_id_to_idx.clear();
        self.strategy_keys.clear();
        self.registry = None;
        self.kickoff_ts.clear();
        self.token_ids_by_game.clear();
        self.has_totals.clear();
        self.game_leagues.clear();
        self.has_nrfi.clear();
        self.has_final.clear();

        let plan_value: serde_json::Value =
            serde_json::from_str(plan_json).map_err(|e| format!("load_plan_json_parse:{}", e))?;
        let games = plan_value
            .get("games")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "load_plan_missing_games".to_string())?;

        for game_val in games {
            let uid = game_val
                .get("provider_game_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if uid.is_empty() {
                continue;
            }
            if self.game_ids.len() >= u16::MAX as usize {
                return Err("load_plan_game_overflow".to_string());
            }
            let gidx = GameIdx(self.game_ids.len() as u16);
            self.game_id_to_idx.insert(uid.clone(), gidx);
            self.game_ids.push(uid);
            let league_str = game_val.get("canonical_league").and_then(|v| v.as_str()).unwrap_or("");
            self.game_leagues.push(Arc::from(league_str));

            // Insert alternate provider game IDs pointing to the same GameIdx.
            if let Some(alts) = game_val.get("alternate_provider_game_ids").and_then(|v| v.as_array()) {
                for alt in alts {
                    let alt_id = alt.get("game_id").and_then(|v| v.as_str()).unwrap_or("").trim();
                    if !alt_id.is_empty() && !self.game_id_to_idx.contains_key(alt_id) {
                        self.game_id_to_idx.insert(alt_id.to_string(), gidx);
                    }
                }
            }

            let kickoff = game_val.get("kickoff_ts_utc").and_then(|v| v.as_i64());
            self.kickoff_ts.push(kickoff);

            let markets = match game_val.get("markets").and_then(|v| v.as_array()) {
                Some(m) => m,
                None => {
                    self.game_targets.push(GameTargets::default());
                    self.has_totals.push(false);
                    self.has_nrfi.push(false);
                    self.has_final.push(false);
                    self.token_ids_by_game.push(Vec::new());
                    continue;
                }
            };

            let mut game_tgt = GameTargets::default();
            let mut game_has_totals = false;
            let mut game_has_nrfi = false;
            let mut game_has_final = false;
            let mut token_ids: HashSet<String> = HashSet::new();

            for market_val in markets {
                let sports_market_type = canonical_market_type(
                    market_val
                        .get("sports_market_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                );
                let line = market_val.get("line").and_then(|v| v.as_f64());
                let targets_arr = match market_val.get("targets").and_then(|v| v.as_array()) {
                    Some(t) => t,
                    None => continue,
                };

                for target_val in targets_arr {
                    let semantic = norm(
                        target_val
                            .get("outcome_semantic")
                            .and_then(|v| v.as_str())
                            .unwrap_or(""),
                    );
                    let token_id = target_val
                        .get("token_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if token_id.is_empty() {
                        continue;
                    }
                    token_ids.insert(token_id.clone());
                    let strategy_key = target_val
                        .get("strategy_key")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if strategy_key.is_empty() {
                        continue;
                    }
                    if self.tokens.len() >= u16::MAX as usize {
                        return Err("load_plan_token_overflow".to_string());
                    }
                    if self.target_slots.len() >= u16::MAX as usize {
                        return Err("load_plan_target_overflow".to_string());
                    }

                    let token_idx = match self.token_id_to_idx.get(&token_id) {
                        Some(&idx) => idx,
                        None => {
                            let idx = TokenIdx(self.tokens.len() as u16);
                            self.tokens.push(TokenSlot {
                                token_id: Arc::from(token_id.as_str()),
                            });
                            self.token_id_to_idx.insert(token_id.clone(), idx);
                            idx
                        }
                    };
                    let tidx = TargetIdx(self.target_slots.len() as u16);
                    self.strategy_keys.insert(strategy_key.clone());
                    self.target_slots.push(TargetSlot {
                        token_idx,
                        strategy_key: Arc::from(strategy_key.as_str()),
                    });

                    match sports_market_type.as_str() {
                        "totals" => {
                            game_has_totals = true;
                            if let Some(l) = line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled baseball semantic '{}' key={}",other,strategy_key);
                                    }
                                }
                            }
                        }
                        "nrfi" => {
                            game_has_nrfi = true;
                            match semantic.as_str() {
                                "yes" => game_tgt.nrfi_yes = Some(tidx),
                                "no" => game_tgt.nrfi_no = Some(tidx),
                                other => {
                                    eprintln!(
                                        "[polybot2] WARN: unhandled baseball nrfi semantic '{}'",
                                        other
                                    );
                                }
                            }
                        }
                        "moneyline" => {
                            game_has_final = true;
                            match semantic.as_str() {
                                "home" => game_tgt.moneyline_home = Some(tidx),
                                "away" => game_tgt.moneyline_away = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled baseball moneyline semantic '{}'", other);
                                }
                            }
                        }
                        "spread" => {
                            game_has_final = true;
                            if let Some(l) = line {
                                let (side, is_covers) = match semantic.as_str() {
                                    "home_covers" | "home" => (SpreadSide::Home, true),
                                    "home_not_covers" => (SpreadSide::Home, false),
                                    "away_covers" | "away" => (SpreadSide::Away, true),
                                    "away_not_covers" => (SpreadSide::Away, false),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled baseball spread semantic '{}' key={}", other, strategy_key);
                                        continue;
                                    }
                                };
                                if let Some(slot) = game_tgt
                                    .spreads
                                    .iter_mut()
                                    .find(|s| s.side == side && (s.line - l).abs() < 1e-9)
                                {
                                    if is_covers {
                                        slot.covers_idx = Some(tidx);
                                    } else {
                                        slot.not_covers_idx = Some(tidx);
                                    }
                                } else {
                                    let mut slot = SpreadSlot {
                                        side,
                                        line: l,
                                        covers_idx: None,
                                        not_covers_idx: None,
                                    };
                                    if is_covers {
                                        slot.covers_idx = Some(tidx);
                                    } else {
                                        slot.not_covers_idx = Some(tidx);
                                    }
                                    game_tgt.spreads.push(slot);
                                }
                            }
                        }
                        other => {
                            eprintln!("[polybot2] WARN: unhandled baseball market type '{}' key={}", other, strategy_key);
                        }
                    }
                }
            }

            game_tgt.over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.under_lines.sort_by_key(|ol| ol.half_int);
            self.game_targets.push(game_tgt);
            self.has_totals.push(game_has_totals);
            self.has_nrfi.push(game_has_nrfi);
            self.has_final.push(game_has_final);

            let mut token_list = token_ids.into_iter().collect::<Vec<_>>();
            token_list.sort();
            token_list.dedup();
            self.token_ids_by_game.push(token_list);
        }

        let num_games = self.game_ids.len();
        self.rows = vec![None; num_games];
        self.bo_rows = vec![None; num_games];
        self.game_states = vec![GameState::default(); num_games];
        self.totals_final_under_emitted = vec![false; num_games];
        self.nrfi_resolved_games = vec![false; num_games];
        self.nrfi_first_inning_observed = vec![false; num_games];
        self.final_resolved_games = vec![false; num_games];

        self.registry = Some(Arc::new(TargetRegistry {
            tokens: self.tokens.clone(),
            targets: self.target_slots.clone(),
        }));

        Ok(())
    }

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
        self.bo_rows[gi] = None;
        self.game_states[gi] = GameState::default();
        self.nrfi_first_inning_observed[gi] = false;
        // totals_final_under_emitted, nrfi_resolved_games, final_resolved_games
        // are intentionally preserved as tombstones — a repeated final frame must
        // not re-emit intents. One-shot gating is enforced by the presign pool
        // (depth=1, no refill), not by the engine.
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

    /// Resolve a BoltOdds game label to a GameIdx. Returns `None` if unknown.
    pub(crate) fn check_boltodds_game(&self, game_label: &str) -> Option<GameIdx> {
        self.game_id_to_idx.get(game_label).copied()
    }

    // ---------------------------------------------------------------
    // Zero-alloc live WS path
    // ---------------------------------------------------------------

    /// Pre-parse dedup + game index resolve in one lookup.
    /// Returns `None` if the frame is a duplicate (raw strings unchanged)
    /// or the fixture_id is unknown. Returns `Some(gidx)` otherwise.
    pub(crate) fn check_duplicate(
        &self,
        fixture_id: &str,
        home_str: &str,
        away_str: &str,
        free_text: &str,
    ) -> Option<GameIdx> {
        let &gidx = self.game_id_to_idx.get(fixture_id)?;
        let gi = gidx.0 as usize;
        if let Some(row) = self.rows[gi].as_ref() {
            if row.home_score_raw.as_str() == home_str
                && row.away_score_raw.as_str() == away_str
                && row.free_text_raw.as_str() == free_text
            {
                return None; // duplicate
            }
        }
        Some(gidx)
    }

    /// Process a tick from borrowed fields without constructing a `Tick` or
    /// allocating any strings. Returns `None` for unknown games.
    /// Dedup + game_id_to_idx resolve is handled by `check_duplicate` in the
    /// frame pipeline before calling this method.
    pub(crate) fn process_tick_live(
        &mut self,
        gidx: GameIdx,
        home_score_raw: &str,
        away_score_raw: &str,
        free_text_raw: &str,
        goals_home: Option<i64>,
        goals_away: Option<i64>,
        inning_number: Option<i64>,
        inning_half: &'static str,
        match_completed: Option<bool>,
        game_state: &'static str,
        _recv_monotonic_ns: i64,
    ) -> Option<LiveTickResult> {
        let gi = gidx.0 as usize;

        // Compute deltas from previous row.
        let mut goal_delta_home = 0i64;
        let mut goal_delta_away = 0i64;
        if let Some(row) = self.rows[gi].as_ref() {
            goal_delta_home = goals_home.unwrap_or(0) - row.goals_home.unwrap_or(0);
            goal_delta_away = goals_away.unwrap_or(0) - row.goals_away.unwrap_or(0);
        }

        // Update state row with raw strings + parsed scores.
        self.rows[gi] = Some(StateRow {
            home_score_raw: InlineStr::from_str(home_score_raw),
            away_score_raw: InlineStr::from_str(away_score_raw),
            free_text_raw: InlineStr::from_str(free_text_raw),
            goals_home,
            goals_away,
        });

        let delta = DeltaEvent {
            material_change: true,
            goal_delta_home,
            goal_delta_away,
        };

        // Update game state (mirrors update_game_state).
        let prev = self.game_states[gi];
        let home = goals_home.or(prev.home);
        let away = goals_away.or(prev.away);
        let inn = inning_number.or(prev.inning_number);
        let half = if inning_half.is_empty() {
            prev.inning_half
        } else {
            inning_half
        };
        let completed = if prev.match_completed.unwrap_or(false) {
            Some(true)
        } else if match_completed.is_some() {
            Some(match_completed.unwrap_or(false))
        } else {
            prev.match_completed
        };
        let gs = if !game_state.is_empty() {
            game_state
        } else if completed.unwrap_or(false) {
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
            inning_number: inn,
            inning_half: half,
            match_completed: completed,
            game_state: gs,
            outs: None,
            strikes: None,
            base1: None,
            base2: None,
            base3: None,
        };
        if home.is_some() && away.is_some() {
            state.home = home;
            state.away = away;
            state.prev_total = prev.total;
            state.total = Some(home.unwrap_or(0) + away.unwrap_or(0));
        }
        self.game_states[gi] = state;

        // Evaluate directly into stack-allocated SmallVec — no intermediate type.
        let mut intents = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_totals_into(gidx, &state, &mut intents);
        self.evaluate_nrfi_into(gidx, &state, &delta, &mut intents);
        self.evaluate_walkoff_into(gidx, &state, &mut intents);
        self.evaluate_final_into(gidx, &state, &mut intents);

        if state.match_completed.unwrap_or(false) && self.final_resolved_games[gi] {
            self.cleanup_completed_game_idx(gidx);
        }

        Some(LiveTickResult {
            game_idx: gidx,
            state,
            intents,
            material: true,
        })
    }

    // ---------------------------------------------------------------
    // BoltOdds live path (outs-based evaluators)
    // ---------------------------------------------------------------

    /// Process a BoltOdds baseball tick. Dedup is integer-based (outs,
    /// strikes, inning, top_of_inning, score). Evaluates: totals (shared),
    /// NRFI-from-outs (with early YES on run delta), walkoff, and
    /// game-end-from-outs. Does NOT run V1-specific evaluators (final,
    /// V1 NRFI via DeltaEvent).
    pub(crate) fn process_boltodds_tick_live(
        &mut self,
        gidx: GameIdx,
        outs: u8,
        strikes: u8,
        inning: i64,
        top_of_inning: bool,
        home_score: i64,
        away_score: i64,
        base1: bool,
        base2: bool,
        base3: bool,
        period_detail: &str,
        _recv_monotonic_ns: i64,
    ) -> Option<LiveTickResult> {
        let gi = gidx.0 as usize;

        // Integer-based dedup: ball-count-only changes are filtered out
        // (ball not in struct), but strike changes pass through (they
        // change the dedup row and thus allow the next outs change to
        // be detected even if score/inning are unchanged).
        let new_row = BoltOddsBaseballRow {
            outs,
            strikes,
            inning,
            top_of_inning,
            home_score,
            away_score,
        };
        if self.bo_rows[gi].as_ref() == Some(&new_row) {
            return None;
        }
        self.bo_rows[gi] = Some(new_row);

        // Build GameState from prev + BoltOdds data.
        let prev = self.game_states[gi];
        let total = home_score + away_score;
        let state = GameState {
            home: Some(home_score),
            away: Some(away_score),
            total: Some(total),
            prev_total: prev.total,
            inning_number: Some(inning),
            inning_half: if top_of_inning { "top" } else { "bottom" },
            // BoltOdds doesn't signal match completion — preserve V1's value.
            match_completed: prev.match_completed,
            game_state: prev.game_state,
            outs: Some(outs),
            strikes: Some(strikes),
            base1: Some(base1),
            base2: Some(base2),
            base3: Some(base3),
        };
        self.game_states[gi] = state;

        // Defensive guard: skip evaluators for break frames.
        // AT_MID_ = break between top and bottom of same inning.
        // AT_END_ = break between bottom of inning N and top of N+1.
        // Break frames currently always have outs=0/strikes=0, so
        // evaluators would be harmless, but explicitly skipping prevents
        // any future BoltOdds data quality regression from causing
        // mis-fires (e.g. walkoff evaluator doesn't check outs).
        let is_break = period_detail.starts_with("AT_MID_")
            || period_detail.starts_with("AT_END_");

        // Evaluate into stack-allocated SmallVec.
        let mut intents = smallvec::SmallVec::<[Intent; 32]>::new();
        if !is_break {
            self.evaluate_totals_into(gidx, &state, &mut intents);
            self.evaluate_nrfi_from_outs_into(gidx, &state, &mut intents);
            self.evaluate_walkoff_into(gidx, &state, &mut intents);
            self.evaluate_game_end_from_outs_into(gidx, &state, &mut intents);
        }

        Some(LiveTickResult {
            game_idx: gidx,
            state,
            intents,
            material: true,
        })
    }

    pub(crate) fn merge_plan(&mut self, plan_json: &str) -> Result<MergePlanResult, String> {
        let plan_value: serde_json::Value =
            serde_json::from_str(plan_json).map_err(|e| format!("merge_plan_json_parse:{}", e))?;
        let games = plan_value
            .get("games")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "merge_plan_missing_games".to_string())?;

        let mut new_token_count = 0usize;
        let mut new_target_count = 0usize;
        let mut dirty_games: HashSet<usize> = HashSet::new();

        for game_val in games {
            let uid = game_val
                .get("provider_game_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();
            if uid.is_empty() {
                continue;
            }
            let Some(&gidx) = self.game_id_to_idx.get(uid) else {
                continue;
            };
            // Add any new alternate IDs for this existing game.
            if let Some(alts) = game_val.get("alternate_provider_game_ids").and_then(|v| v.as_array()) {
                for alt in alts {
                    let alt_id = alt.get("game_id").and_then(|v| v.as_str()).unwrap_or("").trim();
                    if !alt_id.is_empty() && !self.game_id_to_idx.contains_key(alt_id) {
                        self.game_id_to_idx.insert(alt_id.to_string(), gidx);
                    }
                }
            }
            let gi = gidx.0 as usize;

            let markets = match game_val.get("markets").and_then(|v| v.as_array()) {
                Some(m) => m,
                None => continue,
            };

            for market_val in markets {
                let sports_market_type = canonical_market_type(
                    market_val
                        .get("sports_market_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                );
                let line = market_val.get("line").and_then(|v| v.as_f64());

                let targets_arr = match market_val.get("targets").and_then(|v| v.as_array()) {
                    Some(t) => t,
                    None => continue,
                };

                for target_val in targets_arr {
                    let strategy_key = target_val
                        .get("strategy_key")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if strategy_key.is_empty() {
                        continue;
                    }
                    if self.strategy_keys.contains(&strategy_key) {
                        continue;
                    }
                    let token_id = target_val
                        .get("token_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if token_id.is_empty() {
                        continue;
                    }
                    if self.target_slots.len() >= u16::MAX as usize {
                        return Err("merge_plan_target_overflow".to_string());
                    }
                    if self.tokens.len() >= u16::MAX as usize {
                        return Err("merge_plan_token_overflow".to_string());
                    }

                    let semantic = norm(
                        target_val
                            .get("outcome_semantic")
                            .and_then(|v| v.as_str())
                            .unwrap_or(""),
                    );

                    let token_idx = match self.token_id_to_idx.get(&token_id) {
                        Some(&idx) => idx,
                        None => {
                            let idx = TokenIdx(self.tokens.len() as u16);
                            self.tokens.push(TokenSlot {
                                token_id: Arc::from(token_id.as_str()),
                            });
                            self.token_id_to_idx.insert(token_id.clone(), idx);
                            new_token_count += 1;
                            idx
                        }
                    };

                    let tidx = TargetIdx(self.target_slots.len() as u16);
                    self.strategy_keys.insert(strategy_key.clone());
                    self.target_slots.push(TargetSlot {
                        token_idx,
                        strategy_key: Arc::from(strategy_key.as_str()),
                    });
                    new_target_count += 1;

                    let game_tgt = &mut self.game_targets[gi];
                    match sports_market_type.as_str() {
                        "totals" => {
                            if let Some(l) = line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => {
                                        game_tgt.over_lines.push(OverLine {
                                            half_int: half,
                                            target_idx: tidx,
                                        });
                                    }
                                    "under" => {
                                        game_tgt.under_lines.push(OverLine {
                                            half_int: half,
                                            target_idx: tidx,
                                        });
                                    }
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled baseball semantic '{}' key={}",other,strategy_key);
                                    }
                                }
                            }
                            self.has_totals[gi] = true;
                            dirty_games.insert(gi);
                        }
                        "nrfi" => {
                            match semantic.as_str() {
                                "yes" => game_tgt.nrfi_yes = Some(tidx),
                                "no" => game_tgt.nrfi_no = Some(tidx),
                                other => {
                                    eprintln!(
                                        "[polybot2] WARN: unhandled baseball nrfi semantic '{}'",
                                        other
                                    );
                                }
                            }
                            self.has_nrfi[gi] = true;
                        }
                        "moneyline" => {
                            match semantic.as_str() {
                                "home" => game_tgt.moneyline_home = Some(tidx),
                                "away" => game_tgt.moneyline_away = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled baseball moneyline semantic '{}'", other);
                                }
                            }
                            self.has_final[gi] = true;
                        }
                        "spread" => {
                            if let Some(l) = line {
                                let (side, is_covers) = match semantic.as_str() {
                                    "home_covers" | "home" => (SpreadSide::Home, true),
                                    "home_not_covers" => (SpreadSide::Home, false),
                                    "away_covers" | "away" => (SpreadSide::Away, true),
                                    "away_not_covers" => (SpreadSide::Away, false),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled baseball spread semantic '{}' key={}", other, strategy_key);
                                        continue;
                                    }
                                };
                                if let Some(slot) = game_tgt
                                    .spreads
                                    .iter_mut()
                                    .find(|s| s.side == side && (s.line - l).abs() < 1e-9)
                                {
                                    if is_covers {
                                        slot.covers_idx = Some(tidx);
                                    } else {
                                        slot.not_covers_idx = Some(tidx);
                                    }
                                } else {
                                    let mut slot = SpreadSlot {
                                        side,
                                        line: l,
                                        covers_idx: None,
                                        not_covers_idx: None,
                                    };
                                    if is_covers {
                                        slot.covers_idx = Some(tidx);
                                    } else {
                                        slot.not_covers_idx = Some(tidx);
                                    }
                                    game_tgt.spreads.push(slot);
                                }
                            }
                            self.has_final[gi] = true;
                        }
                        other => {
                            eprintln!("[polybot2] WARN: unhandled baseball market type '{}' key={}", other, strategy_key);
                        }
                    }

                    if !self.token_ids_by_game[gi].contains(&token_id) {
                        self.token_ids_by_game[gi].push(token_id);
                        dirty_games.insert(gi);
                    }
                }
            }
        }

        // Deferred sort/dedup — once per dirty game, not per inserted target.
        for gi in dirty_games {
            self.game_targets[gi]
                .over_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .under_lines
                .sort_by_key(|ol| ol.half_int);
            self.token_ids_by_game[gi].sort();
            self.token_ids_by_game[gi].dedup();
        }

        Ok(MergePlanResult {
            new_games: 0,
            new_tokens: new_token_count,
            new_targets: new_target_count,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::baseball::parse::parse_tick_from_kalstrop_update;
    use crate::kalstrop_types::KalstropFrame;

    impl NativeMlbEngine {
        fn process_tick(&mut self, tick: Tick) -> TickResult {
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
            let mut intents = Vec::new();
            intents.extend(self.evaluate_totals(gidx, &state));
            intents.extend(self.evaluate_nrfi(gidx, &state, &delta));
            intents.extend(self.evaluate_walkoff(gidx, &state));
            intents.extend(self.evaluate_final(gidx, &state));
            let gi = gidx.0 as usize;
            if state.match_completed.unwrap_or(false) && self.final_resolved_games[gi] {
                self.cleanup_completed_game_idx(gidx);
            }
            TickResult {
                game_id,
                state,
                intents,
                material: true,
            }
        }

        fn apply_delta(&mut self, gidx: GameIdx, tick: &Tick) -> DeltaEvent {
            let gi = gidx.0 as usize;
            let mut goal_delta_home = 0;
            let mut goal_delta_away = 0;
            if let Some(row) = self.rows[gi].as_ref() {
                goal_delta_home = tick.goals_home.unwrap_or(0) - row.goals_home.unwrap_or(0);
                goal_delta_away = tick.goals_away.unwrap_or(0) - row.goals_away.unwrap_or(0);
            }
            self.rows[gi] = Some(StateRow {
                home_score_raw: InlineStr::new(),
                away_score_raw: InlineStr::new(),
                free_text_raw: InlineStr::new(),
                goals_home: tick.goals_home,
                goals_away: tick.goals_away,
            });
            DeltaEvent {
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
                outs: None,
                strikes: None,
                base1: None,
                base2: None,
                base3: None,
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

    fn process_kalstrop_frame(
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

    struct GameTargetBuilder<'a> {
        slots: &'a mut Vec<TargetSlot>,
        tokens: &'a mut Vec<TokenSlot>,
        token_id_to_idx: &'a mut FxHashMap<String, TokenIdx>,
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
            self.tokens.push(TokenSlot {
                token_id: Arc::from(token_id),
            });
            self.token_id_to_idx.insert(token_id.to_string(), idx);
            idx
        }

        fn alloc(&mut self, token_id: &str, strategy_key: &str) -> TargetIdx {
            let token_idx = self.alloc_token_idx(token_id);
            let idx = TargetIdx(self.slots.len() as u16);
            self.slots.push(TargetSlot {
                token_idx,
                strategy_key: Arc::from(strategy_key),
            });
            idx
        }

        fn over(&mut self, line: f64, token_id: &str) {
            self.has_totals = true;
            let lk = crate::baseball::eval::line_key(line);
            let tidx = self.alloc(token_id, &format!("_:TOTAL:OVER:{}", lk));
            self.targets.over_lines.push(OverLine {
                half_int: line.floor() as u16,
                target_idx: tidx,
            });
        }

        fn under(&mut self, line: f64, token_id: &str) {
            self.has_totals = true;
            let lk = crate::baseball::eval::line_key(line);
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

        fn moneyline_away(&mut self, token_id: &str) {
            self.has_final = true;
            let tidx = self.alloc(token_id, "_:MONEYLINE:AWAY");
            self.targets.moneyline_away = Some(tidx);
        }

        fn spread_home(
            &mut self,
            line: f64,
            covers_token: &str,
            not_covers_token: &str,
        ) {
            self.has_final = true;
            let lk = crate::baseball::eval::line_key(line);
            let covers_idx = self.alloc(covers_token, &format!("_:SPREAD:HOME_COVERS:{}", lk));
            let not_covers_idx =
                self.alloc(not_covers_token, &format!("_:SPREAD:HOME_NOT_COVERS:{}", lk));
            self.targets.spreads.push(SpreadSlot {
                side: SpreadSide::Home,
                line,
                covers_idx: Some(covers_idx),
                not_covers_idx: Some(not_covers_idx),
            });
        }

        fn spread_away(
            &mut self,
            line: f64,
            covers_token: &str,
            not_covers_token: &str,
        ) {
            self.has_final = true;
            let lk = crate::baseball::eval::line_key(line);
            let covers_idx = self.alloc(covers_token, &format!("_:SPREAD:AWAY_COVERS:{}", lk));
            let not_covers_idx =
                self.alloc(not_covers_token, &format!("_:SPREAD:AWAY_NOT_COVERS:{}", lk));
            self.targets.spreads.push(SpreadSlot {
                side: SpreadSide::Away,
                line,
                covers_idx: Some(covers_idx),
                not_covers_idx: Some(not_covers_idx),
            });
        }
    }

    fn add_game(
        engine: &mut NativeMlbEngine,
        game_id: &str,
        build: impl FnOnce(&mut GameTargetBuilder),
    ) {
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
        engine.bo_rows.push(None);
        engine.game_states.push(GameState::default());
        engine.totals_final_under_emitted.push(false);
        engine.nrfi_resolved_games.push(false);
        engine.nrfi_first_inning_observed.push(false);
        engine.final_resolved_games.push(false);
    }

    /// Test-time finalize: build the registry. Tests call this once after
    /// all `add_game` calls.
    fn sync_target_vecs(engine: &mut NativeMlbEngine) {
        engine.registry = Some(Arc::new(TargetRegistry {
            tokens: engine.tokens.clone(),
            targets: engine.target_slots.clone(),
        }));
    }

    fn tick_with_score(game_id: &str, home: i64, away: i64, _ns: i64) -> Tick {
        Tick {
            universal_id: game_id.to_string(),

            goals_home: Some(home),
            goals_away: Some(away),
            inning_number: Some(3),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        }
    }

    #[test]
    fn shared_token_dedupes_to_single_token_idx() {
        // Two targets pointing to the same token_id must share one TokenIdx.
        // This preserves the "one signed order per unique token" invariant
        // when the pool moves to TokenIdx-keyed indexing in commit 3.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(2.5, "tok_shared");
            b.under(2.5, "tok_shared");
        });
        sync_target_vecs(&mut engine);
        assert_eq!(
            engine.tokens.len(),
            1,
            "shared token should dedupe to one TokenIdx"
        );
        assert_eq!(engine.target_slots.len(), 2, "two distinct targets");
        assert_eq!(
            engine.target_slots[0].token_idx,
            engine.target_slots[1].token_idx
        );
        let registry = engine.clone_registry().expect("registry should be set");
        assert_eq!(registry.tokens.len(), 1);
        assert_eq!(registry.targets.len(), 2);
    }

    #[test]
    fn frame_parse_produces_tick_results() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_| {});
        sync_target_vecs(&mut engine);

        let frame1 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"homeScore":"0","awayScore":"0","matchStatusDisplay":[{"freeText":"1st inning top"}]}}}}}"#;
        let results1 = process_kalstrop_frame(&mut engine, frame1, 1000);
        assert_eq!(results1.len(), 1);
        assert!(results1[0].material);
        assert_eq!(results1[0].game_id, "g1");

        let frame2 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"homeScore":"1","awayScore":"0","matchStatusDisplay":[{"freeText":"1st inning top"}]}}}}}"#;
        let results2 = process_kalstrop_frame(&mut engine, frame2, 2000);
        assert_eq!(results2.len(), 1);
        assert!(results2[0].material);
        assert_eq!(results2[0].state.home, Some(1));
    }

    #[test]
    fn frame_batch_parse_produces_multiple_results() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_| {});
        add_game(&mut engine, "g2", |_| {});
        sync_target_vecs(&mut engine);

        let setup1 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"homeScore":"0","awayScore":"0","matchStatusDisplay":[{"freeText":"1st inning top"}]}}}}}"#;
        let setup2 = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g2","matchSummary":{"homeScore":"0","awayScore":"0","matchStatusDisplay":[{"freeText":"1st inning top"}]}}}}}"#;
        process_kalstrop_frame(&mut engine, setup1, 1000);
        process_kalstrop_frame(&mut engine, setup2, 1000);

        let batch_frame = r#"[{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g1","matchSummary":{"homeScore":"1","awayScore":"0","matchStatusDisplay":[{"freeText":"2nd inning top"}]}}}}},{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"g2","matchSummary":{"homeScore":"0","awayScore":"2","matchStatusDisplay":[{"freeText":"3rd inning bottom"}]}}}}}]"#;
        let results = process_kalstrop_frame(&mut engine, batch_frame, 3000);
        assert_eq!(results.len(), 2);
        assert!(results[0].material);
        assert!(results[1].material);
    }

    #[test]
    fn multi_market_totals_and_nrfi_both_evaluated() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(0.5, "tok_over");
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let tick1 = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);

        let tick2 = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(1),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(
            out.intents.len(),
            2,
            "should have totals over + nrfi yes intents"
        );
    }

    #[test]
    fn evaluate_final_fires_alongside_totals_at_completion() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.under(8.5, "tok_under");
            b.moneyline_home("tok_ml_home");
        });
        sync_target_vecs(&mut engine);

        let tick1 = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(3),
            goals_away: Some(1),
            inning_number: Some(9),
            inning_half: "bottom",
            game_state: "LIVE",
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);

        let tick2 = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(3),
            goals_away: Some(1),
            inning_number: Some(9),
            inning_half: "bottom",
            match_completed: Some(true),
            game_state: "FINAL",
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        // 3 intents: walkoff moneyline_home (bottom 9th, home leading) +
        // evaluate_final moneyline_home + under. Presign pool deduplicates
        // the two moneyline_home intents at dispatch time.
        assert_eq!(out.intents.len(), 3, "walkoff ml + final ml + under");
    }

    #[test]
    fn nrfi_late_subscription_past_first_inning_blocked() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let tick = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(2),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(
            out.intents.is_empty(),
            "late subscription should not produce NRFI intent"
        );
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_first_inning_subscription_allows_evaluation() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let tick1 = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        };
        let _ = engine.process_tick(tick1);
        assert!(engine.nrfi_first_inning_observed[0]);

        let tick2 = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(1),
            goals_away: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        };
        let out = engine.process_tick(tick2);

        assert_eq!(out.intents.len(), 1, "NRFI yes intent should fire");
    }

    #[test]
    fn nrfi_completed_game_first_tick_blocked() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let tick = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(9),
            inning_half: "bottom",
            match_completed: Some(true),
            game_state: "FINAL",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(
            out.intents.is_empty(),
            "completed game on first observation should not produce NRFI"
        );
    }

    #[test]
    fn nrfi_no_inning_data_defers_evaluation() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let tick = Tick {
            universal_id: "g1".to_string(),

            goals_home: Some(0),
            goals_away: Some(0),
            game_state: "LIVE",
            ..Default::default()
        };
        let out = engine.process_tick(tick);

        assert!(
            out.intents.is_empty(),
            "no inning data should defer NRFI evaluation"
        );
        assert!(!engine.nrfi_first_inning_observed[0]);
        assert!(!engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn totals_over_multi_line_crossing() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(1.5, "tok_over_1.5");
            b.over(2.5, "tok_over_2.5");
            b.over(3.5, "tok_over_3.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 0, 0, 1000));
        let out = engine.process_tick(tick_with_score("g1", 3, 0, 2000));

        assert_eq!(
            out.intents.len(),
            2,
            "should emit intents for crossed lines 1.5 and 2.5"
        );
    }

    #[test]
    fn totals_over_sequential_crossings() {
        let mut engine = NativeMlbEngine::new();
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
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(5.5, "tok_over_5.5");
            b.over(6.5, "tok_over_6.5");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_score("g1", 0, 0, 1000));
        let out = engine.process_tick(tick_with_score("g1", 1, 1, 2000));

        assert!(
            out.intents.is_empty(),
            "total 2 is below all lines (5.5, 6.5)"
        );
    }

    #[test]
    fn totals_over_one_shot_prevents_duplicate() {
        let mut engine = NativeMlbEngine::new();
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
        let mut engine = NativeMlbEngine::new();
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
        let mut engine = NativeMlbEngine::new();
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

    // ---------------------------------------------------------------
    // Walkoff tests
    // ---------------------------------------------------------------

    fn tick_with_inning(
        game_id: &str,
        home: i64,
        away: i64,
        inning: i64,
        half: &'static str,
        _ns: i64,
    ) -> Tick {
        Tick {
            universal_id: game_id.to_string(),

            goals_home: Some(home),
            goals_away: Some(away),
            inning_number: Some(inning),
            inning_half: half,
            game_state: "LIVE",
            ..Default::default()
        }
    }

    fn tick_ended(game_id: &str, home: i64, away: i64, _ns: i64) -> Tick {
        Tick {
            universal_id: game_id.to_string(),

            goals_home: Some(home),
            goals_away: Some(away),
            inning_number: Some(9),
            inning_half: "bottom",
            match_completed: Some(true),
            game_state: "FINAL",
            ..Default::default()
        }
    }

    #[test]
    fn walkoff_fires_moneyline_home_bottom_9th_home_leads() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        // Tied 2-2 going into bottom of 9th
        let _ = engine.process_tick(tick_with_inning("g1", 2, 2, 9, "bottom", 1000));
        // Home scores, takes lead 3-2 in bottom 9th
        let out = engine.process_tick(tick_with_inning("g1", 3, 2, 9, "bottom", 2000));

        // Walkoff should fire moneyline_home
        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        assert!(
            out.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "walkoff should fire moneyline_home"
        );
    }

    #[test]
    fn walkoff_does_not_fire_top_of_9th() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        // Away takes lead in top of 9th — NOT a walkoff
        let _ = engine.process_tick(tick_with_inning("g1", 2, 2, 9, "top", 1000));
        let out = engine.process_tick(tick_with_inning("g1", 2, 3, 9, "top", 2000));

        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        assert!(
            !out.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "should NOT fire moneyline_home in top of 9th"
        );
    }

    #[test]
    fn walkoff_does_not_fire_before_9th() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        // Home leads in bottom of 7th — NOT a walkoff (game continues)
        let _ = engine.process_tick(tick_with_inning("g1", 2, 2, 7, "bottom", 1000));
        let out = engine.process_tick(tick_with_inning("g1", 3, 2, 7, "bottom", 2000));

        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        assert!(
            !out.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "should NOT fire moneyline_home before 9th inning"
        );
    }

    #[test]
    fn walkoff_fires_in_extra_innings() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        // Tied 4-4 going into bottom of 11th (extra innings)
        let _ = engine.process_tick(tick_with_inning("g1", 4, 4, 11, "bottom", 1000));
        // Home scores walkoff in 11th
        let out = engine.process_tick(tick_with_inning("g1", 5, 4, 11, "bottom", 2000));

        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        assert!(
            out.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "walkoff should fire in extra innings"
        );
    }

    #[test]
    fn walkoff_does_not_fire_when_tied() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        // Score stays tied in bottom 9th (no walkoff)
        let _ = engine.process_tick(tick_with_inning("g1", 2, 2, 9, "bottom", 1000));
        let out = engine.process_tick(tick_with_inning("g1", 2, 2, 9, "bottom", 2000));

        assert!(out.intents.is_empty(), "tied score should not fire walkoff");
    }

    #[test]
    fn walkoff_and_final_both_fire_on_ended_frame() {
        // When the game-end frame has bottom 9th + home leading, both
        // walkoff and evaluate_final emit moneyline_home. The presign
        // pool (not the evaluator) deduplicates at dispatch time.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        let _ = engine.process_tick(tick_with_inning("g1", 2, 2, 9, "bottom", 1000));
        let out = engine.process_tick(tick_ended("g1", 3, 2, 2000));

        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        assert!(
            out.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "moneyline_home should fire"
        );
    }

    #[test]
    fn walkoff_and_final_do_not_double_fire() {
        // Walkoff fires first, then game officially ends.
        // The presign pool handles dedup, but at the intent level we should
        // see both intents emitted (the pool prevents double-trading, not
        // the evaluator). This test verifies both fire independently.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        // Bottom 9th, home takes lead → walkoff fires
        let _ = engine.process_tick(tick_with_inning("g1", 2, 2, 9, "bottom", 1000));
        let out1 = engine.process_tick(tick_with_inning("g1", 3, 2, 9, "bottom", 2000));
        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        assert!(
            out1.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "walkoff should fire"
        );

        // Game officially ends — evaluate_final also fires moneyline_home.
        // At the intent level, this is a second emit for the same target.
        // The presign pool will reject the second one (pool slot already taken).
        let out2 = engine.process_tick(tick_ended("g1", 3, 2, 3000));
        assert!(
            out2.intents.iter().any(|i| i.target_idx == ml_home_idx),
            "evaluate_final should also emit moneyline_home (pool deduplicates)"
        );
    }

    // ---------------------------------------------------------------
    // BoltOdds evaluator tests: evaluate_nrfi_from_outs
    // ---------------------------------------------------------------

    #[test]
    fn nrfi_from_outs_out3_bottom1_total0_fires_no() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let nrfi_no_idx = engine.game_targets[0].nrfi_no.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        // Must first observe inning 1 to pass the gate.
        engine.nrfi_first_inning_observed[0] = true;
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, nrfi_no_idx);
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_from_outs_strikeout_prefire_does_not_fire() {
        // o=2,s=3 should NOT fire — BoltOdds data shows 68% false positive
        // rate for this signal (strikes flash to 3 then revert).
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        engine.nrfi_first_inning_observed[0] = true;
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(2),
            strikes: Some(3),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "o=2,s=3 should not fire — unreliable signal");
        assert!(!engine.nrfi_resolved_games[0], "should not resolve on false K signal");
    }

    #[test]
    fn nrfi_from_outs_out3_bottom1_runs_scored_fires_yes() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let nrfi_yes_idx = engine.game_targets[0].nrfi_yes.unwrap();
        let gidx = GameIdx(0);

        engine.nrfi_first_inning_observed[0] = true;
        let state = GameState {
            home: Some(1),
            away: Some(0),
            total: Some(1),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, nrfi_yes_idx);
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_from_outs_top1_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        engine.nrfi_first_inning_observed[0] = true;
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "top of 1st should not fire NRFI");
    }

    #[test]
    fn nrfi_from_outs_inning2_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        engine.nrfi_first_inning_observed[0] = true;
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(2),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "inning 2 should not fire NRFI");
    }

    #[test]
    fn nrfi_from_outs_out1_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        engine.nrfi_first_inning_observed[0] = true;
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(1),
            strikes: Some(2),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "out=1 should not fire NRFI");
    }

    #[test]
    fn nrfi_from_outs_already_resolved_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        engine.nrfi_first_inning_observed[0] = true;
        engine.nrfi_resolved_games[0] = true; // already resolved
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "already resolved should not fire");
    }

    #[test]
    fn nrfi_from_outs_none_outs_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        engine.nrfi_first_inning_observed[0] = true;
        // V1 tick: outs=None
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: None,
            strikes: None,
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "V1 tick with outs=None should not fire");
    }

    #[test]
    fn nrfi_from_outs_first_inning_gate_late_sub() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        // First tick shows inning 3 — should resolve and block forever.
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(3),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty());
        assert!(engine.nrfi_resolved_games[0], "late sub should resolve");
    }

    // ---------------------------------------------------------------
    // BoltOdds evaluator tests: evaluate_game_end_from_outs
    // ---------------------------------------------------------------

    #[test]
    fn game_end_from_outs_top9_home_ahead_fires() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
            b.spread_home(-1.5, "tok_sp_h_c", "tok_sp_h_nc");
            b.under(8.5, "tok_under_8.5");
        });
        sync_target_vecs(&mut engine);

        let ml_h_idx = engine.game_targets[0].moneyline_home.unwrap();
        let sp_h_covers_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let sp_h_not_covers_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let under_idx = engine.game_targets[0].under_lines[0].target_idx;
        let gidx = GameIdx(0);

        // Top of 9th, away batting, home leads 5-2. out=3 → game over.
        let state = GameState {
            home: Some(5),
            away: Some(2),
            total: Some(7),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);

        // Should fire: moneyline_home, spread home NOT covers (margin=3, line=-1.5, 3+(-1.5)=1.5>0 → covers),
        // and under 8.5 (total=7 < 8.5).
        assert!(intents.iter().any(|i| i.target_idx == ml_h_idx), "moneyline_home");
        assert!(
            intents.iter().any(|i| i.target_idx == sp_h_covers_idx),
            "spread home covers (margin=3, line=-1.5 → 1.5>0)"
        );
        assert!(intents.iter().any(|i| i.target_idx == under_idx), "under 8.5");
        assert!(
            !intents.iter().any(|i| i.target_idx == sp_h_not_covers_idx),
            "should NOT fire not_covers"
        );
        assert!(engine.final_resolved_games[0]);
        assert!(engine.totals_final_under_emitted[0]);
    }

    #[test]
    fn game_end_from_outs_bottom9_away_ahead_fires() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        let ml_a_idx = engine.game_targets[0].moneyline_away.unwrap();
        let gidx = GameIdx(0);

        // Bottom of 9th, home batting, away leads 4-1. out=3 → game over.
        let state = GameState {
            home: Some(1),
            away: Some(4),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.iter().any(|i| i.target_idx == ml_a_idx), "moneyline_away");
        assert!(engine.final_resolved_games[0]);
    }

    #[test]
    fn game_end_from_outs_top9_out3_home_ahead_fires() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        let ml_h_idx = engine.game_targets[0].moneyline_home.unwrap();
        let gidx = GameIdx(0);

        // Top 9th, out=3 → half-inning over, home leads → game over.
        let state = GameState {
            home: Some(6),
            away: Some(3),
            total: Some(9),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.iter().any(|i| i.target_idx == ml_h_idx), "moneyline_home");
        assert!(engine.final_resolved_games[0]);
    }

    #[test]
    fn game_end_from_outs_strikeout_prefire_does_not_fire() {
        // o=2,s=3 should NOT fire — BoltOdds data shows 68% false positive
        // rate for this signal (strikes flash to 3 then revert on dropped
        // 3rd strikes, scorer corrections, data wobbles).
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        // Top 9th, home leads, o=2 s=3 — looks like a strikeout but unreliable.
        let state = GameState {
            home: Some(6),
            away: Some(3),
            total: Some(9),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(2),
            strikes: Some(3),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.is_empty(), "o=2,s=3 should not fire — unreliable signal");
        assert!(!engine.final_resolved_games[0], "should not resolve on false K signal");
    }

    #[test]
    fn game_end_from_outs_tied_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        // Top 9th, tied 3-3, out=3 → game NOT over (goes to extras).
        let state = GameState {
            home: Some(3),
            away: Some(3),
            total: Some(6),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.is_empty(), "tied game should not fire");
        assert!(!engine.final_resolved_games[0]);
    }

    #[test]
    fn game_end_from_outs_inning8_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(5),
            away: Some(2),
            total: Some(7),
            inning_number: Some(8),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.is_empty(), "inning 8 should not fire game end");
    }

    #[test]
    fn game_end_from_outs_extras_bottom_away_ahead() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        let ml_a_idx = engine.game_targets[0].moneyline_away.unwrap();
        let gidx = GameIdx(0);

        // Bottom of 10th, away leads 5-4, out=3 → game over.
        let state = GameState {
            home: Some(4),
            away: Some(5),
            total: Some(9),
            inning_number: Some(10),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.iter().any(|i| i.target_idx == ml_a_idx), "extras away win");
        assert!(engine.final_resolved_games[0]);
    }

    #[test]
    fn game_end_from_outs_already_resolved_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        engine.final_resolved_games[0] = true; // already resolved
        let state = GameState {
            home: Some(5),
            away: Some(2),
            total: Some(7),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(intents.is_empty(), "already resolved should not fire");
    }

    #[test]
    fn game_end_from_outs_bottom9_home_ahead_no_fire() {
        // Bottom 9th, home leads → this is a WALKOFF scenario.
        // BoltOdds game-end evaluator should NOT fire here because
        // the "wrong half for leader" rule applies. V1 walkoff evaluator
        // handles this case faster via run delta.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(5),
            away: Some(3),
            total: Some(8),
            inning_number: Some(9),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        // This case: bottom 9th, home batting over, away still trails.
        // away > home? No (5>3), so home_wins check fails.
        // home > away? Yes but inning_half is "bottom", not "top". Doesn't match.
        // Result: no fire. Walkoff already handled by V1 path.
        assert!(intents.is_empty(), "bottom 9 home ahead is a walkoff, not game-end-from-outs");
    }

    #[test]
    fn game_end_from_outs_spread_away_evaluation() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_away(1.5, "tok_sp_a_c", "tok_sp_a_nc");
        });
        sync_target_vecs(&mut engine);

        let sp_a_covers_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let sp_a_not_covers_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        // Top 9th, home leads 3-2. margin_home=1. Away spread line=+1.5.
        // For Away side: margin = -margin_home = -1. margin+line = -1+1.5 = 0.5 > 0 → covers.
        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(
            intents.iter().any(|i| i.target_idx == sp_a_covers_idx),
            "away covers +1.5 (lost by 1)"
        );
        assert!(
            !intents.iter().any(|i| i.target_idx == sp_a_not_covers_idx),
            "away should NOT be not_covers"
        );
    }

    // ---------------------------------------------------------------
    // process_boltodds_tick_live tests
    // ---------------------------------------------------------------

    #[test]
    fn boltodds_tick_dedup_identical_returns_none() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_b| {});
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        let result1 = engine.process_boltodds_tick_live(gidx, 1, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000);
        assert!(result1.is_some(), "first tick should produce result");

        let result2 = engine.process_boltodds_tick_live(gidx, 1, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 2000);
        assert!(result2.is_none(), "identical tick should be deduped");
    }

    #[test]
    fn boltodds_tick_dedup_outs_change_passes() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_b| {});
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        let _ = engine.process_boltodds_tick_live(gidx, 1, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000);
        let result = engine.process_boltodds_tick_live(gidx, 2, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 2000);
        assert!(result.is_some(), "outs change should pass dedup");
    }

    #[test]
    fn boltodds_tick_score_change_fires_totals() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(0.5, "tok_over_0.5");
        });
        sync_target_vecs(&mut engine);

        let over_idx = engine.game_targets[0].over_lines[0].target_idx;
        let gidx = GameIdx(0);

        // First tick: 0-0
        let _ = engine.process_boltodds_tick_live(gidx, 0, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000);
        // Second tick: 1-0 → over 0.5 should fire
        let result = engine.process_boltodds_tick_live(gidx, 1, 0, 1, true, 1, 0, false, false, false, "AT_TOP_1ST_INNING", 2000);
        let r = result.expect("score change should produce result");
        assert!(
            r.intents.iter().any(|i| i.target_idx == over_idx),
            "over 0.5 should fire on score change"
        );
    }

    #[test]
    fn boltodds_tick_unknown_game_returns_none() {
        let engine = NativeMlbEngine::new();
        assert!(engine.check_boltodds_game("unknown_game").is_none());
    }

    #[test]
    fn boltodds_tick_sets_outs_strikes_on_state() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_b| {});
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        let result = engine.process_boltodds_tick_live(gidx, 2, 1, 3, false, 1, 0, false, false, false, "AT_BOT_3RD_INNING", 1000);
        let r = result.unwrap();
        assert_eq!(r.state.outs, Some(2));
        assert_eq!(r.state.strikes, Some(1));
        assert_eq!(r.state.inning_number, Some(3));
        assert_eq!(r.state.inning_half, "bottom");
        assert_eq!(r.state.home, Some(1));
        assert_eq!(r.state.away, Some(0));
    }

    #[test]
    fn v1_tick_clears_outs_and_strikes() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_b| {});
        sync_target_vecs(&mut engine);

        // First: BoltOdds tick sets outs/strikes.
        let gidx = GameIdx(0);
        let _ = engine.process_boltodds_tick_live(gidx, 2, 1, 3, true, 0, 0, false, false, false, "AT_TOP_3RD_INNING", 1000);
        assert_eq!(engine.game_states[0].outs, Some(2));
        assert_eq!(engine.game_states[0].strikes, Some(1));

        // Then: V1 tick should clear them.
        let tick = Tick {
            universal_id: "g1".to_string(),
            goals_home: Some(0),
            goals_away: Some(0),
            inning_number: Some(3),
            inning_half: "top",
            game_state: "LIVE",
            ..Default::default()
        };
        let _ = engine.process_tick(tick);
        assert_eq!(engine.game_states[0].outs, None, "V1 should clear outs");
        assert_eq!(engine.game_states[0].strikes, None, "V1 should clear strikes");
    }

    // ---------------------------------------------------------------
    // Cross-provider resolution flag sharing
    // ---------------------------------------------------------------

    #[test]
    fn boltodds_nrfi_blocks_v1_nrfi() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        // BoltOdds fires NRFI NO via outs.
        engine.nrfi_first_inning_observed[0] = true;
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let nrfi_intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert_eq!(nrfi_intents.len(), 1, "BoltOdds NRFI should fire");
        assert!(engine.nrfi_resolved_games[0]);

        // V1 tick arrives later — NRFI evaluator should be skipped.
        let delta = DeltaEvent::default();
        let v1_state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            inning_number: Some(2),
            inning_half: "top",
            ..Default::default()
        };
        let v1_intents = engine.evaluate_nrfi(gidx, &v1_state, &delta);
        assert!(v1_intents.is_empty(), "V1 NRFI should be skipped after BoltOdds resolved");
    }

    #[test]
    fn boltodds_game_end_blocks_v1_final() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        // BoltOdds fires game end.
        let state = GameState {
            home: Some(5),
            away: Some(2),
            total: Some(7),
            inning_number: Some(9),
            inning_half: "top",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let bo_intents = engine.evaluate_game_end_from_outs(gidx, &state);
        assert!(!bo_intents.is_empty(), "BoltOdds game end should fire");
        assert!(engine.final_resolved_games[0]);

        // V1 "Ended" frame arrives later — evaluate_final should be skipped.
        let v1_state = GameState {
            home: Some(5),
            away: Some(2),
            total: Some(7),
            match_completed: Some(true),
            ..Default::default()
        };
        let v1_intents = engine.evaluate_final(gidx, &v1_state);
        assert!(v1_intents.is_empty(), "V1 final should be skipped after BoltOdds resolved");
    }

    #[test]
    fn boltodds_tick_preserves_match_completed_from_v1() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_b| {});
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        // Manually set match_completed (as V1 would).
        engine.game_states[0].match_completed = Some(true);

        let result = engine.process_boltodds_tick_live(gidx, 0, 0, 9, true, 5, 2, false, false, false, "AT_TOP_9TH_INNING", 1000);
        let r = result.unwrap();
        assert_eq!(
            r.state.match_completed,
            Some(true),
            "BoltOdds should preserve V1's match_completed"
        );
    }

    #[test]
    fn boltodds_tick_preserves_game_state_from_v1() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |_b| {});
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        // Simulate V1 setting game_state.
        engine.game_states[0].game_state = "LIVE";

        let result = engine.process_boltodds_tick_live(gidx, 0, 0, 3, true, 0, 0, false, false, false, "AT_TOP_3RD_INNING", 1000);
        let r = result.unwrap();
        assert_eq!(r.state.game_state, "LIVE", "BoltOdds should preserve V1's game_state");
    }

    // ----- BoltOdds break frame guard tests -----

    #[test]
    fn boltodds_break_frame_mid_inning_skips_evaluators() {
        // AT_MID_ period (break between top and bottom of same inning).
        // With NRFI target and outs=0 in 1st inning bottom — would normally
        // fire NRFI NO via outs if evaluators ran. Guard should prevent it.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
            b.over(0.5, "tok_over_0.5");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);

        // Tick 1: active play, top 1st — establishes state.
        let r1 = engine.process_boltodds_tick_live(
            gidx, 0, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000,
        );
        assert!(r1.is_some());

        // Tick 2: break frame (AT_MID_1ST_INNING), bottom 1st, outs=0.
        // Score changed to 1-0 — would fire over 0.5 if evaluators ran.
        let r2 = engine.process_boltodds_tick_live(
            gidx, 0, 0, 1, false, 1, 0, false, false, false, "AT_MID_1ST_INNING", 2000,
        );
        let r2 = r2.unwrap();
        assert!(
            r2.intents.is_empty(),
            "Break frame should skip all evaluators, got {} intents",
            r2.intents.len()
        );
        // State should still be updated (for correct dedup on next frame).
        assert_eq!(r2.state.home, Some(1));
        assert_eq!(r2.state.inning_half, "bottom");
    }

    #[test]
    fn boltodds_break_frame_end_inning_skips_evaluators() {
        // AT_END_ period (break between bottom of N and top of N+1).
        // With totals target — score change during break should not fire.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(0.5, "tok_over_0.5");
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);

        // Tick 1: active play, establishes state.
        let _ = engine.process_boltodds_tick_live(
            gidx, 0, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000,
        );

        // Tick 2: end-of-inning break. Score went to 1-0.
        let r2 = engine.process_boltodds_tick_live(
            gidx, 0, 0, 1, false, 1, 0, false, false, false, "AT_END_1ST_INNING", 2000,
        );
        let r2 = r2.unwrap();
        assert!(
            r2.intents.is_empty(),
            "End-of-inning break should skip evaluators, got {} intents",
            r2.intents.len()
        );
    }

    #[test]
    fn boltodds_active_play_frame_still_evaluates() {
        // Non-break period (AT_BOT_) — evaluators should run normally.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.over(0.5, "tok_over_0.5");
        });
        sync_target_vecs(&mut engine);

        let over_idx = engine.game_targets[0].over_lines[0].target_idx;
        let gidx = GameIdx(0);

        // Tick 1: top 1st, 0-0.
        let _ = engine.process_boltodds_tick_live(
            gidx, 0, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000,
        );

        // Tick 2: active play in bottom 1st, score change → over 0.5 fires.
        let r2 = engine.process_boltodds_tick_live(
            gidx, 0, 0, 1, false, 1, 0, false, false, false, "AT_BOT_1ST_INNING", 2000,
        );
        let r2 = r2.unwrap();
        assert!(
            r2.intents.iter().any(|i| i.target_idx == over_idx),
            "Active play frame should fire evaluators"
        );
    }

    // ----- BoltOdds walkoff tests -----

    #[test]
    fn boltodds_walkoff_fires_moneyline_home() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
        });
        sync_target_vecs(&mut engine);

        let ml_home_idx = engine.game_targets[0].moneyline_home.unwrap();
        let gidx = GameIdx(0);

        // Bottom 9th, home leading → walkoff.
        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            outs: Some(1),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(intents.iter().any(|i| i.target_idx == ml_home_idx));
        // V1 path (base fields None) → Tier 1 only, no full resolution.
        assert!(!engine.final_resolved_games[0]);
    }

    #[test]
    fn boltodds_walkoff_not_leading_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);

        // Bottom 9th, away leading → no walkoff.
        let state = GameState {
            home: Some(2),
            away: Some(3),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            outs: Some(1),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(intents.is_empty());
    }

    #[test]
    fn boltodds_walkoff_blocked_by_final_resolved() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        engine.final_resolved_games[0] = true;

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            outs: Some(1),
            strikes: Some(0),
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(intents.is_empty(), "Walkoff should be blocked when final_resolved");
    }

    // ----- Walkoff spread tests (Tier 1: partial resolution) -----

    #[test]
    fn walkoff_fires_away_not_covers_negative_line() {
        // Away -1.5 spread: away lost → away_not_covers fires.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_away( -1.5, "tok_a_cov", "tok_a_nc");
        });
        sync_target_vecs(&mut engine);

        let a_nc_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            intents.iter().any(|i| i.target_idx == a_nc_idx),
            "away_not_covers should fire for negative away line"
        );
    }

    #[test]
    fn walkoff_fires_home_covers_when_margin_sufficient() {
        // Home -1.5 spread, margin=2 → home_covers fires.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_home( -1.5, "tok_h_cov", "tok_h_nc");
        });
        sync_target_vecs(&mut engine);

        let h_cov_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(4),
            away: Some(2),
            total: Some(6),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            intents.iter().any(|i| i.target_idx == h_cov_idx),
            "home_covers should fire when margin exceeds line"
        );
    }

    #[test]
    fn walkoff_does_not_fire_home_not_covers() {
        // Home -2.5 spread, margin=1. Margin could grow (home run with
        // runners), so home_not_covers is NOT resolvable yet.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_home( -2.5, "tok_h_cov", "tok_h_nc");
        });
        sync_target_vecs(&mut engine);

        let h_nc_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            !intents.iter().any(|i| i.target_idx == h_nc_idx),
            "home_not_covers should NOT fire — margin could grow"
        );
    }

    #[test]
    fn walkoff_does_not_fire_away_covers_positive_line() {
        // Away +1.5 spread, margin_home=1. Away_margin=-1, -1+1.5=0.5 > 0
        // → currently covers. But margin_home could grow (runners on base),
        // making away_margin more negative → could stop covering. NOT safe.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_away( 1.5, "tok_a_cov", "tok_a_nc");
        });
        sync_target_vecs(&mut engine);

        let a_cov_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let a_nc_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            !intents.iter().any(|i| i.target_idx == a_cov_idx),
            "away_covers should NOT fire -- margin could grow"
        );
        // away_margin=-1, -1+1.5=0.5 > 0 -- currently covers, not_covers doesn't fire.
        assert!(
            !intents.iter().any(|i| i.target_idx == a_nc_idx),
            "away_not_covers should NOT fire -- currently covers"
        );
    }

    #[test]
    fn walkoff_fires_away_not_covers_positive_line() {
        // Away +0.5 spread, margin_home=2. Away_margin=-2, -2+0.5=-1.5 <= 0
        // -> not covering. Away margin only gets worse -> safe to fire not_covers.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_away( 0.5, "tok_a_cov", "tok_a_nc");
        });
        sync_target_vecs(&mut engine);

        let a_nc_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(4),
            away: Some(2),
            total: Some(6),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            intents.iter().any(|i| i.target_idx == a_nc_idx),
            "away_not_covers should fire for positive line when not covering"
        );
    }

    // ----- Walkoff spread tests (Tier 2: empty bases, full resolution) -----

    #[test]
    fn walkoff_empty_bases_fires_all_spreads() {
        // All bases empty: score is final, full margin eval resolves both sides.
        // Score 3-2 (margin=1). Home -0.5: 1+(-0.5)=0.5>0 covers.
        // Away +1.5: away_margin=-1, -1+1.5=0.5>0 covers.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_home(-0.5, "tok_h_cov", "tok_h_nc");
            b.spread_away(1.5, "tok_a_cov", "tok_a_nc");
        });
        sync_target_vecs(&mut engine);

        let h_cov_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let a_cov_idx = engine.game_targets[0].spreads[1].covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            base1: Some(false),
            base2: Some(false),
            base3: Some(false),
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(intents.iter().any(|i| i.target_idx == h_cov_idx), "home_covers");
        assert!(intents.iter().any(|i| i.target_idx == a_cov_idx), "away_covers (full eval)");
        assert!(engine.final_resolved_games[0], "empty bases: full resolution");
    }

    #[test]
    fn walkoff_empty_bases_fires_unders() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.under(8.5, "tok_under_8_5");
            b.under(6.5, "tok_under_6_5");
        });
        sync_target_vecs(&mut engine);

        let under_8_5_idx = engine.game_targets[0].under_lines[0].target_idx;
        let under_6_5_idx = engine.game_targets[0].under_lines[1].target_idx;
        let gidx = GameIdx(0);

        // Score: 3-2 = total 5. Under 8.5 (half_int=8) fires, under 6.5 (half_int=6) fires.
        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            base1: Some(false),
            base2: Some(false),
            base3: Some(false),
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(intents.iter().any(|i| i.target_idx == under_8_5_idx), "under 8.5");
        assert!(intents.iter().any(|i| i.target_idx == under_6_5_idx), "under 6.5");
        assert!(engine.totals_final_under_emitted[0]);
    }

    #[test]
    fn walkoff_empty_bases_sets_final_resolved() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            base1: Some(false),
            base2: Some(false),
            base3: Some(false),
            ..Default::default()
        };
        let _ = engine.evaluate_walkoff(gidx, &state);
        assert!(engine.final_resolved_games[0], "empty bases sets final_resolved");

        // Subsequent evaluate_game_end_from_outs should be blocked.
        let state2 = GameState {
            outs: Some(3),
            ..state
        };
        let intents2 = engine.evaluate_game_end_from_outs(gidx, &state2);
        assert!(intents2.is_empty(), "game_end_from_outs blocked after walkoff resolution");
    }

    #[test]
    fn walkoff_occupied_bases_no_full_resolution() {
        // base2=true → NOT empty → only Tier 1 fires.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_home( -2.5, "tok_h_cov", "tok_h_nc");
            b.under(8.5, "tok_under_8_5");
        });
        sync_target_vecs(&mut engine);

        let h_nc_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let under_idx = engine.game_targets[0].under_lines[0].target_idx;
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            base1: Some(false),
            base2: Some(true),
            base3: Some(false),
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        // home -2.5, margin=1: margin + line = 1 + (-2.5) = -1.5 ≤ 0 → does not cover.
        // But margin could grow (runner on 2nd) → home_not_covers NOT fired.
        assert!(
            !intents.iter().any(|i| i.target_idx == h_nc_idx),
            "home_not_covers should NOT fire with occupied bases"
        );
        // Unders should NOT fire (bases not empty → not full resolution).
        assert!(
            !intents.iter().any(|i| i.target_idx == under_idx),
            "under should NOT fire with occupied bases"
        );
        assert!(!engine.final_resolved_games[0], "should NOT set final_resolved");
        assert!(!engine.totals_final_under_emitted[0], "should NOT set totals_final_under_emitted");
    }

    #[test]
    fn walkoff_v1_no_base_data_no_full_resolution() {
        // V1 path: base fields are None → only Tier 1.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.under(8.5, "tok_under_8_5");
        });
        sync_target_vecs(&mut engine);

        let under_idx = engine.game_targets[0].under_lines[0].target_idx;
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            // base1/base2/base3 default to None (V1 path).
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            !intents.iter().any(|i| i.target_idx == under_idx),
            "under should NOT fire on V1 path (no base data)"
        );
        assert!(!engine.final_resolved_games[0], "V1 path should NOT set final_resolved");
    }

    #[test]
    fn walkoff_partial_then_game_end_fires_remaining() {
        // Walkoff with occupied bases fires partial spreads (Tier 1).
        // Then evaluate_final (V1 "Ended") fires remaining spreads + unders.
        // Presign pool prevents double-fire (simulated by checking intent lists).
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_home(-0.5, "tok_h_cov", "tok_h_nc");
            b.spread_away(-1.5, "tok_a_cov", "tok_a_nc");
            b.under(8.5, "tok_under_8_5");
        });
        sync_target_vecs(&mut engine);

        let ml_h_idx = engine.game_targets[0].moneyline_home.unwrap();
        let h_cov_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let a_nc_idx = engine.game_targets[0].spreads[1].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        // Step 1: Walkoff with runner on base (Tier 1 only).
        let state1 = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            base1: Some(true),
            base2: Some(false),
            base3: Some(false),
            ..Default::default()
        };
        let intents1 = engine.evaluate_walkoff(gidx, &state1);
        assert!(intents1.iter().any(|i| i.target_idx == ml_h_idx), "moneyline_home");
        assert!(intents1.iter().any(|i| i.target_idx == h_cov_idx), "home -0.5 covers (margin=1 > 0.5)");
        assert!(intents1.iter().any(|i| i.target_idx == a_nc_idx), "away -1.5 not_covers (away lost)");
        assert!(!engine.final_resolved_games[0]);

        // Step 2: Game officially ends via V1 "Ended" signal.
        // evaluate_final fires ALL spreads + unders at actual final score.
        // Score may have increased due to base runner scoring.
        let state2 = GameState {
            home: Some(4),
            away: Some(2),
            total: Some(6),
            match_completed: Some(true),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents2 = engine.evaluate_final(gidx, &state2);
        // Presign pool (not tested here) prevents double-fire on targets
        // already popped by walkoff.
        assert!(!intents2.is_empty(), "final should fire remaining targets");
        assert!(engine.final_resolved_games[0], "final sets final_resolved");
    }

    #[test]
    fn boltodds_tick_walkoff_empty_bases_full_resolution() {
        // Integration: process_boltodds_tick_live with walkoff + empty bases.
        // Score 4-2 (margin=2), home -1.5 → covers (2+(-1.5)=0.5 > 0).
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_home( -1.5, "tok_h_cov", "tok_h_nc");
            b.under(8.5, "tok_under_8_5");
        });
        sync_target_vecs(&mut engine);

        let ml_h_idx = engine.game_targets[0].moneyline_home.unwrap();
        let h_cov_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let under_idx = engine.game_targets[0].under_lines[0].target_idx;
        let gidx = GameIdx(0);

        // Tick 1: establish prev state (bottom 9th, tied 2-2 — not a walkoff).
        let _ = engine.process_boltodds_tick_live(
            gidx, 0, 0, 9, false, 2, 2, false, false, false, "AT_BOT_9TH_INNING", 1000,
        );

        // Tick 2: walkoff! Home scores again (4-2), bases empty.
        let r = engine
            .process_boltodds_tick_live(
                gidx, 0, 0, 9, false, 4, 2, false, false, false, "AT_BOT_9TH_INNING", 2000,
            )
            .unwrap();
        assert!(r.intents.iter().any(|i| i.target_idx == ml_h_idx), "moneyline_home");
        // margin=2, line=-1.5: 2+(-1.5)=0.5 > 0 -> covers.
        assert!(r.intents.iter().any(|i| i.target_idx == h_cov_idx), "home -1.5 covers");
        // Empty bases -> Tier 2: under 8.5, total=6, half_int=8 >= 6 -> fires.
        assert!(r.intents.iter().any(|i| i.target_idx == under_idx), "under 8.5");
        assert!(engine.final_resolved_games[0], "empty bases sets final_resolved");
        assert!(engine.totals_final_under_emitted[0], "under emitted flag set");
    }

    // ----- NRFI early YES tests -----

    #[test]
    fn nrfi_from_outs_early_yes_on_run_delta() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let nrfi_yes_idx = engine.game_targets[0].nrfi_yes.unwrap();
        let gidx = GameIdx(0);
        engine.nrfi_first_inning_observed[0] = true;

        // Top of 1st, run scored (total went 0→1).
        let state = GameState {
            home: Some(0),
            away: Some(1),
            total: Some(1),
            prev_total: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, nrfi_yes_idx);
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_from_outs_early_yes_bottom_first() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let nrfi_yes_idx = engine.game_targets[0].nrfi_yes.unwrap();
        let gidx = GameIdx(0);
        engine.nrfi_first_inning_observed[0] = true;

        // Bottom of 1st, 2 runs scored (total went 0→2).
        let state = GameState {
            home: Some(2),
            away: Some(0),
            total: Some(2),
            prev_total: Some(0),
            inning_number: Some(1),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, nrfi_yes_idx);
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_from_outs_early_yes_cold_start_skips() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        engine.nrfi_first_inning_observed[0] = true;

        // Cold start: prev_total is None → early check skipped.
        let state = GameState {
            home: Some(1),
            away: Some(0),
            total: Some(1),
            prev_total: None,
            inning_number: Some(1),
            inning_half: "top",
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "Cold start should not fire early YES");
        assert!(!engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_from_outs_early_yes_no_delta_no_fire() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
        });
        sync_target_vecs(&mut engine);

        let gidx = GameIdx(0);
        engine.nrfi_first_inning_observed[0] = true;

        // No run scored: total unchanged at 0.
        let state = GameState {
            home: Some(0),
            away: Some(0),
            total: Some(0),
            prev_total: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            ..Default::default()
        };
        let intents = engine.evaluate_nrfi_from_outs(gidx, &state);
        assert!(intents.is_empty(), "No delta should not fire early YES");
        assert!(!engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn nrfi_from_outs_early_yes_blocks_later_outs_no() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let nrfi_yes_idx = engine.game_targets[0].nrfi_yes.unwrap();
        let gidx = GameIdx(0);
        engine.nrfi_first_inning_observed[0] = true;

        // First: early YES fires on run delta in top of 1st.
        let state1 = GameState {
            home: Some(0),
            away: Some(1),
            total: Some(1),
            prev_total: Some(0),
            inning_number: Some(1),
            inning_half: "top",
            ..Default::default()
        };
        let intents1 = engine.evaluate_nrfi_from_outs(gidx, &state1);
        assert_eq!(intents1.len(), 1);
        assert_eq!(intents1[0].target_idx, nrfi_yes_idx);
        assert!(engine.nrfi_resolved_games[0]);

        // Second: bottom of 1st ends with outs=3 — should be blocked.
        let state2 = GameState {
            home: Some(0),
            away: Some(1),
            total: Some(1),
            prev_total: Some(1),
            inning_number: Some(1),
            inning_half: "bottom",
            outs: Some(3),
            strikes: Some(0),
            ..Default::default()
        };
        let intents2 = engine.evaluate_nrfi_from_outs(gidx, &state2);
        assert!(intents2.is_empty(), "Already resolved — should not fire again");
    }

    #[test]
    fn boltodds_tick_nrfi_early_yes_integration() {
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.nrfi_yes("tok_nrfi_yes");
            b.nrfi_no("tok_nrfi_no");
        });
        sync_target_vecs(&mut engine);

        let nrfi_yes_idx = engine.game_targets[0].nrfi_yes.unwrap();
        let gidx = GameIdx(0);

        // Tick 1: top of 1st, no runs — establishes prev_total = 0.
        let r1 = engine.process_boltodds_tick_live(gidx, 0, 0, 1, true, 0, 0, false, false, false, "AT_TOP_1ST_INNING", 1000);
        let r1 = r1.unwrap();
        assert!(r1.intents.is_empty(), "Tick 1: no intents expected");

        // Tick 2: top of 1st, away scores (0→1) — early YES should fire.
        let r2 = engine.process_boltodds_tick_live(gidx, 0, 0, 1, true, 0, 1, false, false, false, "AT_TOP_1ST_INNING", 2000);
        let r2 = r2.unwrap();
        assert_eq!(r2.intents.len(), 1, "Tick 2: NRFI YES should fire on run delta");
        assert_eq!(r2.intents[0].target_idx, nrfi_yes_idx);
        assert!(engine.nrfi_resolved_games[0]);
    }

    #[test]
    fn walkoff_away_not_covers_boundary_line_equals_margin() {
        // Away +1.0 spread, margin_home=1. away_margin=-1,
        // (-1)+1.0 = 0.0 which is NOT > 0 → not_covers fires.
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.spread_away(1.0, "tok_a_cov", "tok_a_nc");
        });
        sync_target_vecs(&mut engine);

        let a_cov_idx = engine.game_targets[0].spreads[0].covers_idx.unwrap();
        let a_nc_idx = engine.game_targets[0].spreads[0].not_covers_idx.unwrap();
        let gidx = GameIdx(0);

        let state = GameState {
            home: Some(3),
            away: Some(2),
            total: Some(5),
            inning_number: Some(9),
            inning_half: "bottom",
            ..Default::default()
        };
        let intents = engine.evaluate_walkoff(gidx, &state);
        assert!(
            intents.iter().any(|i| i.target_idx == a_nc_idx),
            "away_not_covers should fire when margin+line == 0 (boundary)"
        );
        assert!(
            !intents.iter().any(|i| i.target_idx == a_cov_idx),
            "away_covers should NOT fire — margin could worsen"
        );
    }

    #[test]
    fn boltodds_match_completed_period_not_treated_as_break() {
        // MATCH_COMPLETED period does NOT start with AT_MID_ or AT_END_,
        // so evaluators should run (not be skipped by the break guard).
        let mut engine = NativeMlbEngine::new();
        add_game(&mut engine, "g1", |b| {
            b.moneyline_home("tok_ml_h");
            b.moneyline_away("tok_ml_a");
            b.under(8.5, "tok_under_8_5");
        });
        sync_target_vecs(&mut engine);

        let ml_a_idx = engine.game_targets[0].moneyline_away.unwrap();
        let under_idx = engine.game_targets[0].under_lines[0].target_idx;
        let gidx = GameIdx(0);

        // Tick 1: establish state in bottom 9th, away leading.
        let _ = engine.process_boltodds_tick_live(
            gidx, 2, 1, 9, false, 1, 3, false, false, false, "AT_BOT_9TH_INNING", 1000,
        );

        // Tick 2: MATCH_COMPLETED period with outs=0 — should NOT be
        // skipped as a break frame. Game-end evaluator won't fire (outs=0),
        // but walkoff check runs and detects away > home → no walkoff.
        // No intents expected from this specific frame (outs=0 blocks
        // game-end, away leads blocks walkoff), but the key assertion is
        // that the break guard did not suppress evaluation.
        let r2 = engine.process_boltodds_tick_live(
            gidx, 0, 0, 9, false, 1, 3, false, false, false, "MATCH_COMPLETED", 2000,
        );
        let r2 = r2.unwrap();
        // Evaluators ran but none fired (outs=0, away leads).
        assert!(r2.intents.is_empty());
        // State was updated — verify score is tracked.
        assert_eq!(r2.state.home, Some(1));
        assert_eq!(r2.state.away, Some(3));
        // final_resolved not set (outs=0, no game-end signal).
        assert!(!engine.final_resolved_games[0]);

        // Tick 3: now send outs=3 with MATCH_COMPLETED — game-end fires.
        let r3 = engine.process_boltodds_tick_live(
            gidx, 3, 0, 9, false, 1, 3, false, false, false, "MATCH_COMPLETED", 3000,
        );
        let r3 = r3.unwrap();
        assert!(
            r3.intents.iter().any(|i| i.target_idx == ml_a_idx),
            "MATCH_COMPLETED with outs=3 should fire moneyline_away"
        );
        assert!(
            r3.intents.iter().any(|i| i.target_idx == under_idx),
            "MATCH_COMPLETED with outs=3 should fire under"
        );
        assert!(engine.final_resolved_games[0]);
    }
}
