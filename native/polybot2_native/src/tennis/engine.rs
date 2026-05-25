//! Tennis engine: plan loading, game state management, zero-alloc live tick path.

use crate::tennis::types::*;
use crate::InlineStr;
use crate::*;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

impl NativeTennisEngine {
    pub(crate) fn new() -> Self {
        Self {
            game_id_to_idx: FxHashMap::default(),
            game_ids: Vec::new(),
            game_targets: Vec::new(),
            target_slots: Vec::new(),
            tokens: Vec::new(),
            token_id_to_idx: FxHashMap::default(),
            strategy_keys: HashSet::new(),
            registry: None,
            kickoff_ts: Vec::new(),
            token_ids_by_game: Vec::new(),
            has_match_totals: Vec::new(),
            has_first_set_totals: Vec::new(),
            has_set_totals: Vec::new(),
            has_moneyline: Vec::new(),
            has_first_set_winner: Vec::new(),
            has_set_handicap: Vec::new(),
            sets_to_win: Vec::new(),
            rows: Vec::new(),
            game_states: Vec::new(),
            match_total_under_emitted: Vec::new(),
            first_set_total_under_emitted: Vec::new(),
            set_total_under_emitted: Vec::new(),
            first_set_winner_resolved: Vec::new(),
            final_resolved_games: Vec::new(),
        }
    }

    pub(crate) fn reset_runtime_state(&mut self) {
        self.rows.fill(None);
        for gs in &mut self.game_states {
            *gs = TennisGameState::default();
        }
        self.match_total_under_emitted.fill(false);
        self.first_set_total_under_emitted.fill(false);
        self.set_total_under_emitted.fill(false);
        self.first_set_winner_resolved.fill(false);
        self.final_resolved_games.fill(false);
    }

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

    fn is_game_completed(&self, id: &str) -> bool {
        if let Some(&gidx) = self.game_id_to_idx.get(id) {
            let gi = gidx.0 as usize;
            self.game_states[gi].match_completed || self.final_resolved_games[gi]
        } else {
            false
        }
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
        let mut out: Vec<String> = tokens.into_iter().collect();
        out.sort();
        out
    }

    // ---------------------------------------------------------------
    // Plan loading from JSON
    // ---------------------------------------------------------------

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
        self.sets_to_win.clear();
        self.has_match_totals.clear();
        self.has_first_set_totals.clear();
        self.has_set_totals.clear();
        self.has_moneyline.clear();
        self.has_first_set_winner.clear();
        self.has_set_handicap.clear();

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
            let stw = game_val.get("sets_to_win").and_then(|v| v.as_i64()).unwrap_or(2);
            self.sets_to_win.push(stw);

            let markets = match game_val.get("markets").and_then(|v| v.as_array()) {
                Some(m) => m,
                None => {
                    self.game_targets.push(TennisGameTargets::default());
                    self.has_match_totals.push(false);
                    self.has_first_set_totals.push(false);
                    self.has_set_totals.push(false);
                    self.has_moneyline.push(false);
                    self.has_first_set_winner.push(false);
                    self.has_set_handicap.push(false);
                    self.token_ids_by_game.push(Vec::new());
                    continue;
                }
            };

            let mut game_tgt = TennisGameTargets::default();
            let mut game_has_match_totals = false;
            let mut game_has_first_set_totals = false;
            let mut game_has_set_totals = false;
            let mut game_has_moneyline = false;
            let mut game_has_first_set_winner = false;
            let mut game_has_set_handicap = false;
            let mut token_ids: HashSet<String> = HashSet::new();
            let game_id_ref = self.game_ids[gidx.0 as usize].as_str();

            for market_val in markets {
                let sports_market_type = canonical_tennis_market_type(
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
                    // Prefer target-level line, fall back to market-level.
                    let effective_line = target_val.get("line").and_then(|v| v.as_f64()).or(line);
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
                        "tennis_match_totals" => {
                            game_has_match_totals = true;
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.match_total_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.match_total_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_match_totals semantic '{}' for game {}", other, game_id_ref);
                                    }
                                }
                            }
                        }
                        "tennis_first_set_totals" => {
                            game_has_first_set_totals = true;
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.first_set_total_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.first_set_total_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_first_set_totals semantic '{}' for game {}", other, game_id_ref);
                                    }
                                }
                            }
                        }
                        "tennis_set_totals" => {
                            game_has_set_totals = true;
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.set_total_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.set_total_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_set_totals semantic '{}' for game {}", other, game_id_ref);
                                    }
                                }
                            }
                        }
                        "moneyline" => {
                            game_has_moneyline = true;
                            match semantic.as_str() {
                                "home" => game_tgt.moneyline_home = Some(tidx),
                                "away" => game_tgt.moneyline_away = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled moneyline semantic '{}' for game {}", other, game_id_ref);
                                }
                            }
                        }
                        "tennis_first_set_winner" => {
                            game_has_first_set_winner = true;
                            match semantic.as_str() {
                                "home" => game_tgt.first_set_winner_home = Some(tidx),
                                "away" => game_tgt.first_set_winner_away = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled tennis_first_set_winner semantic '{}' for game {}", other, game_id_ref);
                                }
                            }
                        }
                        "tennis_set_handicap" => {
                            game_has_set_handicap = true;
                            if let Some(l) = effective_line {
                                let (side, is_covers) = match semantic.as_str() {
                                    "home_covers" => (SpreadSide::Home, true),
                                    "home_not_covers" => (SpreadSide::Home, false),
                                    "away_covers" => (SpreadSide::Away, true),
                                    "away_not_covers" => (SpreadSide::Away, false),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_set_handicap semantic '{}' for game {}", other, game_id_ref);
                                        continue;
                                    }
                                };
                                if let Some(slot) = game_tgt
                                    .set_handicaps
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
                                    game_tgt.set_handicaps.push(slot);
                                }
                            }
                        }
                        other => {
                            eprintln!("[polybot2] WARN: unhandled tennis market type '{}' for game {}", other, game_id_ref);
                        }
                    }
                }
            }

            // Sort all over/under lines by half_int for correct crossing iteration.
            game_tgt.match_total_over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.match_total_under_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.first_set_total_over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.first_set_total_under_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.set_total_over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.set_total_under_lines.sort_by_key(|ol| ol.half_int);
            self.game_targets.push(game_tgt);
            self.has_match_totals.push(game_has_match_totals);
            self.has_first_set_totals.push(game_has_first_set_totals);
            self.has_set_totals.push(game_has_set_totals);
            self.has_moneyline.push(game_has_moneyline);
            self.has_first_set_winner.push(game_has_first_set_winner);
            self.has_set_handicap.push(game_has_set_handicap);

            let mut token_list = token_ids.into_iter().collect::<Vec<_>>();
            token_list.sort();
            token_list.dedup();
            self.token_ids_by_game.push(token_list);
        }

        let num_games = self.game_ids.len();
        self.rows = vec![None; num_games];
        self.game_states = vec![TennisGameState::default(); num_games];
        self.match_total_under_emitted = vec![false; num_games];
        self.first_set_total_under_emitted = vec![false; num_games];
        self.set_total_under_emitted = vec![false; num_games];
        self.first_set_winner_resolved = vec![false; num_games];
        self.final_resolved_games = vec![false; num_games];

        self.registry = Some(Arc::new(TargetRegistry {
            tokens: self.tokens.clone(),
            targets: self.target_slots.clone(),
        }));

        Ok(())
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
        sets_home: &str,
        sets_away: &str,
        games_home: &str,
        games_away: &str,
        free_text: &str,
    ) -> Option<GameIdx> {
        let &gidx = self.game_id_to_idx.get(fixture_id)?;
        let gi = gidx.0 as usize;
        if let Some(row) = self.rows[gi].as_ref() {
            if row.sets_home.as_str() == sets_home
                && row.sets_away.as_str() == sets_away
                && row.games_home.as_str() == games_home
                && row.games_away.as_str() == games_away
                && row.free_text_raw.as_str() == free_text
            {
                return None; // duplicate
            }
        }
        Some(gidx)
    }

    /// Process a tick from borrowed fields without constructing a `Tick` or
    /// allocating any strings. Returns `None` for unknown games.
    /// Dedup + game_id_to_idx resolve is handled by `check_duplicate`
    /// in the frame pipeline before calling this method.
    pub(crate) fn process_tick_live(
        &mut self,
        gidx: GameIdx,
        sets_home_raw: &str,
        sets_away_raw: &str,
        games_home_raw: &str,
        games_away_raw: &str,
        free_text_raw: &str,
        sets_home: i64,
        sets_away: i64,
        games_home: i64,
        games_away: i64,
        total_games: i64,
        first_set_games: Option<i64>,
        total_sets: i64,
        current_set: i64,
        match_completed: bool,
        first_set_completed: bool,
        game_state: &'static str,
        _recv_monotonic_ns: i64,
    ) -> Option<TennisLiveTickResult> {
        let gi = gidx.0 as usize;

        // Detect first observation before overwriting the dedup row.
        // On first tick, rows[gi] is None → prev fields must be None to prevent
        // spurious intent storm (cold-start protection, matching baseball/soccer).
        let is_first_observation = self.rows[gi].is_none();

        // Update dedup row.
        self.rows[gi] = Some(TennisStateRow {
            sets_home: InlineStr::from_str(sets_home_raw),
            sets_away: InlineStr::from_str(sets_away_raw),
            games_home: InlineStr::from_str(games_home_raw),
            games_away: InlineStr::from_str(games_away_raw),
            free_text_raw: InlineStr::from_str(free_text_raw),
        });

        // Update game state.
        let prev = self.game_states[gi];
        let gs = game_state;
        let resolved_first_set_games = first_set_games.unwrap_or(prev.first_set_games);
        let state = TennisGameState {
            sets_home,
            sets_away,
            games_home,
            games_away,
            total_games,
            prev_total_games: if is_first_observation { None } else { Some(prev.total_games) },
            first_set_games: resolved_first_set_games,
            prev_first_set_games: if is_first_observation { None } else { Some(prev.first_set_games) },
            total_sets,
            prev_total_sets: if is_first_observation { None } else { Some(prev.total_sets) },
            current_set,
            match_completed: match_completed || prev.match_completed,
            first_set_completed: first_set_completed || prev.first_set_completed,
            game_state: gs,
        };
        self.game_states[gi] = state;

        // Cold-start: mark already-occurred events so evaluators don't fire
        // on stale outcomes. Progressive evaluators are already guarded by
        // Option<i64> prev fields (None on first tick), but event-based
        // evaluators (first-set under/winner, match-end markets) need
        // explicit tombstones.
        if is_first_observation {
            if state.first_set_completed {
                self.first_set_winner_resolved[gi] = true;
                self.first_set_total_under_emitted[gi] = true;
            }
            if state.match_completed {
                self.match_total_under_emitted[gi] = true;
                self.set_total_under_emitted[gi] = true;
                self.final_resolved_games[gi] = true;
            }
        }

        // Evaluate directly into stack-allocated SmallVec — no intermediate type.
        let mut intents = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_match_totals_into(gidx, &state, &mut intents);
        self.evaluate_first_set_totals_into(gidx, &state, &mut intents);
        self.evaluate_set_totals_into(gidx, &state, &mut intents);
        self.evaluate_first_set_winner_into(gidx, &state, &mut intents);
        self.evaluate_moneyline_into(gidx, &state, &mut intents);
        self.evaluate_set_handicap_into(gidx, &state, &mut intents);

        if state.match_completed {
            self.final_resolved_games[gi] = true;
            self.cleanup_completed_game_idx(gidx);
        }

        Some(TennisLiveTickResult {
            game_idx: gidx,
            state,
            intents,
        })
    }

    fn cleanup_completed_game_idx(&mut self, gidx: GameIdx) {
        let gi = gidx.0 as usize;
        self.rows[gi] = None;
        self.game_states[gi] = TennisGameState::default();
        // Tombstones preserved: match_total_under_emitted, first_set_total_under_emitted,
        // set_total_under_emitted, first_set_winner_resolved, final_resolved_games
    }

    // ---------------------------------------------------------------
    // Evaluators
    // ---------------------------------------------------------------

    /// Match totals (total games across all sets): progressive over crossing,
    /// under at match end.
    fn evaluate_match_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &TennisGameState,
        intents: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_match_totals[gi] {
            return;
        }
        let tgt = &self.game_targets[gi];
        let now = state.total_games;

        // Over crossings: fire when half_int crosses from prev to now.
        // prev = None on first tick (cold start) → skip to establish baseline.
        if let Some(prev) = state.prev_total_games {
            if now > prev {
                let prev_u = prev.max(0) as u16;
                let now_u = now.max(0) as u16;
                for ol in &tgt.match_total_over_lines {
                    if ol.half_int >= prev_u && ol.half_int < now_u {
                        intents.push(Intent { target_idx: ol.target_idx });
                    }
                }
            }
        }

        // Under at match end.
        if state.match_completed && !self.match_total_under_emitted[gi] {
            self.match_total_under_emitted[gi] = true;
            let total_u = now.max(0) as u16;
            for ol in &tgt.match_total_under_lines {
                if ol.half_int >= total_u {
                    intents.push(Intent { target_idx: ol.target_idx });
                }
            }
        }
    }

    /// First-set totals (games in set 1): progressive over crossing,
    /// under at first set end.
    fn evaluate_first_set_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &TennisGameState,
        intents: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_first_set_totals[gi] {
            return;
        }
        let tgt = &self.game_targets[gi];
        let now = state.first_set_games;

        // Over crossings. prev = None on first tick → skip (cold-start safe).
        // No first_set_completed guard needed: first_set_games comes from
        // phases[0] which freezes after set 1 — subsequent ticks have now == prev.
        if let Some(prev) = state.prev_first_set_games {
            if now > prev {
                let prev_u = prev.max(0) as u16;
                let now_u = now.max(0) as u16;
                for ol in &tgt.first_set_total_over_lines {
                    if ol.half_int >= prev_u && ol.half_int < now_u {
                        intents.push(Intent { target_idx: ol.target_idx });
                    }
                }
            }
        }

        // Under at first set end.
        if state.first_set_completed && !self.first_set_total_under_emitted[gi] {
            self.first_set_total_under_emitted[gi] = true;
            let total_u = now.max(0) as u16;
            for ol in &tgt.first_set_total_under_lines {
                if ol.half_int >= total_u {
                    intents.push(Intent { target_idx: ol.target_idx });
                }
            }
        }
    }

    /// Set totals (total sets played): progressive over crossing,
    /// under at match end.
    fn evaluate_set_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &TennisGameState,
        intents: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_set_totals[gi] {
            return;
        }
        let tgt = &self.game_targets[gi];
        let now = state.total_sets;

        // Over: fire when outcome becomes guaranteed (not when set completes).
        // Over N.5 (half_int = N) is guaranteed when both players have enough
        // sets that the total cannot stay at N or below.
        // Condition: min(sets_home, sets_away) >= N + 1 - sets_to_win.
        // Example BO3: Over 2.5 guaranteed when min(h,a) >= 1 (i.e., 1-1).
        // Example BO5: Over 4.5 guaranteed when min(h,a) >= 2 (i.e., 2-2).
        // prev = None on first tick → skip (cold-start safe).
        if let Some(prev) = state.prev_total_sets {
            if now > prev {
            let stw = self.sets_to_win[gi];
            let min_sets = state.sets_home.min(state.sets_away);
            for ol in &tgt.set_total_over_lines {
                let n = ol.half_int as i64;
                let min_needed = (n + 1 - stw).max(0);
                if min_sets >= min_needed {
                    intents.push(Intent { target_idx: ol.target_idx });
                }
            }
            }
        }

        // Under at match end.
        if state.match_completed && !self.set_total_under_emitted[gi] {
            self.set_total_under_emitted[gi] = true;
            let total_u = now.max(0) as u16;
            for ol in &tgt.set_total_under_lines {
                if ol.half_int >= total_u {
                    intents.push(Intent { target_idx: ol.target_idx });
                }
            }
        }
    }

    /// First-set winner: fires once when first set completes.
    fn evaluate_first_set_winner_into(
        &mut self,
        gidx: GameIdx,
        state: &TennisGameState,
        intents: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_first_set_winner[gi] {
            return;
        }
        if !state.first_set_completed || self.first_set_winner_resolved[gi] {
            return;
        }
        self.first_set_winner_resolved[gi] = true;
        let tgt = &self.game_targets[gi];

        // Determine first-set winner by comparing sets won.
        // At the moment the first set completes, if home won set 1 then
        // sets_home > sets_away (e.g. 1-0), or we check the frozen first_set_games
        // to see who had more games. The simplest reliable signal: whoever has
        // more sets at the point first_set_completed becomes true won set 1.
        // Since this fires on the first set completion, the leading player in
        // sets_home vs sets_away won that set.
        if state.sets_home > state.sets_away {
            if let Some(tidx) = tgt.first_set_winner_home {
                intents.push(Intent { target_idx: tidx });
            }
        } else if state.sets_away > state.sets_home {
            if let Some(tidx) = tgt.first_set_winner_away {
                intents.push(Intent { target_idx: tidx });
            }
        }
    }

    /// Moneyline (match winner): fires when match is decided (sets_to_win reached)
    /// or at "Ended" — whichever comes first (~45-159ms edge in 13% of matches).
    fn evaluate_moneyline_into(
        &mut self,
        gidx: GameIdx,
        state: &TennisGameState,
        intents: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_moneyline[gi] {
            return;
        }
        let stw = self.sets_to_win[gi];
        let match_decided = state.match_completed
            || state.sets_home >= stw
            || state.sets_away >= stw;
        if !match_decided {
            return;
        }
        // Only fire once (final_resolved_games gate is shared with match totals;
        // use the moneyline flag presence + presign pool as the sole gate).
        let tgt = &self.game_targets[gi];
        if state.sets_home > state.sets_away {
            if let Some(tidx) = tgt.moneyline_home {
                intents.push(Intent { target_idx: tidx });
            }
        } else if state.sets_away > state.sets_home {
            if let Some(tidx) = tgt.moneyline_away {
                intents.push(Intent { target_idx: tidx });
            }
        }
    }

    /// Set handicap: covers fires at match end, not_covers fires early when
    /// the favored player can no longer achieve a sufficient margin.
    ///
    /// not_covers guaranteed when: max_margin + line <= 0, where
    /// max_margin = sets_to_win - opponent_sets (best case: favored wins match).
    /// covers can never fire early — favored could still lose the match.
    fn evaluate_set_handicap_into(
        &mut self,
        gidx: GameIdx,
        state: &TennisGameState,
        intents: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_set_handicap[gi] {
            return;
        }
        let tgt = &self.game_targets[gi];
        let stw = self.sets_to_win[gi];
        let margin = state.sets_home - state.sets_away;

        let match_decided = state.match_completed
            || state.sets_home >= stw
            || state.sets_away >= stw;

        for slot in &tgt.set_handicaps {
            let opponent_sets = match slot.side {
                SpreadSide::Home => state.sets_away,
                SpreadSide::Away => state.sets_home,
            };

            if match_decided {
                // Match end: fire covers or not_covers based on final margin.
                let adj_margin = match slot.side {
                    SpreadSide::Home => margin as f64,
                    SpreadSide::Away => -(margin as f64),
                };
                if adj_margin + slot.line > 0.0 {
                    if let Some(tidx) = slot.covers_idx {
                        intents.push(Intent { target_idx: tidx });
                    }
                } else {
                    if let Some(tidx) = slot.not_covers_idx {
                        intents.push(Intent { target_idx: tidx });
                    }
                }
            } else {
                // Mid-match: check if not_covers is already guaranteed.
                // Best case for favored: win match at sets_to_win, opponent stays.
                let max_margin = stw - opponent_sets;
                if (max_margin as f64) + slot.line <= 0.0 {
                    if let Some(tidx) = slot.not_covers_idx {
                        intents.push(Intent { target_idx: tidx });
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // merge_plan (hot-patch)
    // ---------------------------------------------------------------

    pub(crate) fn merge_plan(&mut self, plan_json: &str) -> Result<MergePlanResult, String> {
        let plan_value: serde_json::Value =
            serde_json::from_str(plan_json).map_err(|e| format!("merge_plan_json_parse:{}", e))?;
        let games = plan_value
            .get("games")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "merge_plan_missing_games".to_string())?;

        let mut new_game_count = 0usize;
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
            let gidx = match self.game_id_to_idx.get(uid) {
                Some(&idx) => {
                    // Existing game — add any new alternate IDs.
                    if let Some(alts) = game_val.get("alternate_provider_game_ids").and_then(|v| v.as_array()) {
                        for alt in alts {
                            let alt_id = alt.get("game_id").and_then(|v| v.as_str()).unwrap_or("").trim();
                            if !alt_id.is_empty() && !self.game_id_to_idx.contains_key(alt_id) {
                                self.game_id_to_idx.insert(alt_id.to_string(), idx);
                            }
                        }
                    }
                    idx
                }
                None => {
                    if self.game_ids.len() >= u16::MAX as usize {
                        eprintln!("[polybot2] WARN: merge_plan game overflow u16_max, skipping {}", uid);
                        continue;
                    }
                    let idx = GameIdx(self.game_ids.len() as u16);
                    let kickoff = game_val.get("kickoff_ts_utc").and_then(|v| v.as_i64());
                    self.game_id_to_idx.insert(uid.to_string(), idx);
                    self.game_ids.push(uid.to_string());
                    // Insert alternate provider game IDs for the new game.
                    if let Some(alts) = game_val.get("alternate_provider_game_ids").and_then(|v| v.as_array()) {
                        for alt in alts {
                            let alt_id = alt.get("game_id").and_then(|v| v.as_str()).unwrap_or("").trim();
                            if !alt_id.is_empty() && !self.game_id_to_idx.contains_key(alt_id) {
                                self.game_id_to_idx.insert(alt_id.to_string(), idx);
                            }
                        }
                    }
                    self.kickoff_ts.push(kickoff);
                    let stw = game_val.get("sets_to_win").and_then(|v| v.as_i64()).unwrap_or(2);
                    self.sets_to_win.push(stw);
                    self.game_targets.push(TennisGameTargets::default());
                    self.has_match_totals.push(false);
                    self.has_first_set_totals.push(false);
                    self.has_set_totals.push(false);
                    self.has_moneyline.push(false);
                    self.has_first_set_winner.push(false);
                    self.has_set_handicap.push(false);
                    self.token_ids_by_game.push(Vec::new());
                    self.rows.push(None);
                    self.game_states.push(TennisGameState::default());
                    self.match_total_under_emitted.push(false);
                    self.first_set_total_under_emitted.push(false);
                    self.set_total_under_emitted.push(false);
                    self.first_set_winner_resolved.push(false);
                    self.final_resolved_games.push(false);
                    new_game_count += 1;
                    idx
                }
            };
            let gi = gidx.0 as usize;

            let markets = match game_val.get("markets").and_then(|v| v.as_array()) {
                Some(m) => m,
                None => continue,
            };

            for market_val in markets {
                let sports_market_type = canonical_tennis_market_type(
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
                    let effective_line = target_val.get("line").and_then(|v| v.as_f64()).or(line);

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
                        "tennis_match_totals" => {
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.match_total_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.match_total_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_match_totals semantic '{}' for game {}", other, uid);
                                    }
                                }
                            }
                            self.has_match_totals[gi] = true;
                            dirty_games.insert(gi);
                        }
                        "tennis_first_set_totals" => {
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.first_set_total_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.first_set_total_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_first_set_totals semantic '{}' for game {}", other, uid);
                                    }
                                }
                            }
                            self.has_first_set_totals[gi] = true;
                            dirty_games.insert(gi);
                        }
                        "tennis_set_totals" => {
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.set_total_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.set_total_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_set_totals semantic '{}' for game {}", other, uid);
                                    }
                                }
                            }
                            self.has_set_totals[gi] = true;
                            dirty_games.insert(gi);
                        }
                        "moneyline" => {
                            match semantic.as_str() {
                                "home" => game_tgt.moneyline_home = Some(tidx),
                                "away" => game_tgt.moneyline_away = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled moneyline semantic '{}' for game {}", other, uid);
                                }
                            }
                            self.has_moneyline[gi] = true;
                        }
                        "tennis_first_set_winner" => {
                            match semantic.as_str() {
                                "home" => game_tgt.first_set_winner_home = Some(tidx),
                                "away" => game_tgt.first_set_winner_away = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled tennis_first_set_winner semantic '{}' for game {}", other, uid);
                                }
                            }
                            self.has_first_set_winner[gi] = true;
                        }
                        "tennis_set_handicap" => {
                            if let Some(l) = effective_line {
                                let (side, is_covers) = match semantic.as_str() {
                                    "home_covers" => (SpreadSide::Home, true),
                                    "home_not_covers" => (SpreadSide::Home, false),
                                    "away_covers" => (SpreadSide::Away, true),
                                    "away_not_covers" => (SpreadSide::Away, false),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled tennis_set_handicap semantic '{}' for game {}", other, uid);
                                        continue;
                                    }
                                };
                                if let Some(slot) = game_tgt
                                    .set_handicaps
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
                                    game_tgt.set_handicaps.push(slot);
                                }
                            }
                            self.has_set_handicap[gi] = true;
                        }
                        other => {
                            eprintln!("[polybot2] WARN: unhandled tennis market type '{}' for game {}", other, uid);
                        }
                    }

                    if !self.token_ids_by_game[gi].contains(&token_id) {
                        self.token_ids_by_game[gi].push(token_id);
                        dirty_games.insert(gi);
                    }
                }
            }
        }

        for gi in dirty_games {
            self.game_targets[gi]
                .match_total_over_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .match_total_under_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .first_set_total_over_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .first_set_total_under_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .set_total_over_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .set_total_under_lines
                .sort_by_key(|ol| ol.half_int);
            self.token_ids_by_game[gi].sort();
            self.token_ids_by_game[gi].dedup();
        }

        Ok(MergePlanResult {
            new_games: new_game_count,
            new_tokens: new_token_count,
            new_targets: new_target_count,
        })
    }
}

fn canonical_tennis_market_type(input: &str) -> String {
    let raw = norm(input).replace('-', "_").replace(' ', "_");
    match raw.as_str() {
        "moneyline" => "moneyline".to_string(),
        "tennis_match_totals" => "tennis_match_totals".to_string(),
        "tennis_first_set_totals" => "tennis_first_set_totals".to_string(),
        "tennis_set_totals" => "tennis_set_totals".to_string(),
        "tennis_first_set_winner" => "tennis_first_set_winner".to_string(),
        "tennis_set_handicap" => "tennis_set_handicap".to_string(),
        _ => raw,
    }
}

fn norm(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Intent;

    /// Helper: build a test plan JSON with specified markets for one game.
    fn plan_json_one_game(game_id: &str, markets_json: &str) -> String {
        format!(
            r#"{{"games":[{{"provider_game_id":"{}","kickoff_ts_utc":1700000000,"markets":[{}]}}]}}"#,
            game_id, markets_json
        )
    }

    /// Helper: build a single target JSON.
    fn target_json(token_id: &str, semantic: &str, strategy_key: &str) -> String {
        format!(
            r#"{{"outcome_index":0,"token_id":"{}","outcome_label":"{}","outcome_semantic":"{}","strategy_key":"{}"}}"#,
            token_id, semantic, semantic, strategy_key
        )
    }

    /// Helper: build a market JSON.
    fn market_json(market_type: &str, line: Option<f64>, targets: &[String]) -> String {
        let line_str = match line {
            Some(v) => format!("{}", v),
            None => "null".to_string(),
        };
        let targets_str = targets.join(",");
        format!(
            r#"{{"condition_id":"cond_1","market_id":"mid_1","event_id":"eid_1","sports_market_type":"{}","line":{},"question":"test","targets":[{}]}}"#,
            market_type, line_str, targets_str
        )
    }

    fn tick(
        engine: &mut NativeTennisEngine,
        game_id: &str,
        sets_home: i64,
        sets_away: i64,
        games_home: i64,
        games_away: i64,
        total_games: i64,
        first_set_games: Option<i64>,
        total_sets: i64,
        current_set: i64,
        match_completed: bool,
        first_set_completed: bool,
    ) -> Vec<Intent> {
        let gidx = match engine.game_id_to_idx.get(game_id) {
            Some(&g) => g,
            None => return vec![],
        };
        let result = engine.process_tick_live(
            gidx,
            "",
            "",
            "",
            "",
            "free_text",
            sets_home,
            sets_away,
            games_home,
            games_away,
            total_games,
            first_set_games,
            total_sets,
            current_set,
            match_completed,
            first_set_completed,
            "LIVE",
            1000,
        );
        match result {
            Some(r) => r.intents.to_vec(),
            None => vec![],
        }
    }

    // =================================================================
    // Match totals tests
    // =================================================================

    #[test]
    fn test_match_total_over_crossing() {
        let mut engine = NativeTennisEngine::new();
        let t1 = target_json("tok_over20", "over", "g1:MATCH_TOTAL:OVER:20.5");
        let m = market_json("tennis_match_totals", Some(20.5), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Tick 1: total_games=18, no fire
        let intents = tick(&mut engine, "game1", 1, 0, 5, 3, 18, None, 1, 2, false, true);
        assert!(intents.is_empty());

        // Tick 2: total_games=21, crosses 20.5 -> fires
        let intents = tick(&mut engine, "game1", 1, 0, 6, 5, 21, None, 1, 2, false, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_match_total_under_at_end() {
        let mut engine = NativeTennisEngine::new();
        let t_under = target_json("tok_under25", "under", "g1:MATCH_TOTAL:UNDER:25.5");
        let m = market_json("tennis_match_totals", Some(25.5), &[t_under]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // During game: total_games=22, no fire
        let intents = tick(&mut engine, "game1", 1, 1, 5, 4, 22, None, 2, 3, false, true);
        assert!(intents.is_empty());

        // Match ends with 24 total games -> under 25.5 fires
        let intents = tick(&mut engine, "game1", 2, 1, 6, 4, 24, None, 3, 3, true, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_match_total_under_no_fire() {
        let mut engine = NativeTennisEngine::new();
        let t_under = target_json("tok_under20", "under", "g1:MATCH_TOTAL:UNDER:20.5");
        let m = market_json("tennis_match_totals", Some(20.5), &[t_under]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Match ends with 24 total games -> under 20.5 does NOT fire (24 >= 20)
        let intents = tick(&mut engine, "game1", 2, 1, 6, 4, 24, None, 3, 3, true, true);
        assert!(intents.is_empty());
    }

    // =================================================================
    // First-set totals tests
    // =================================================================

    #[test]
    fn test_first_set_total_over() {
        let mut engine = NativeTennisEngine::new();
        let t1 = target_json("tok_fs_over9", "over", "g1:FS_TOTAL:OVER:9.5");
        let m = market_json("tennis_first_set_totals", Some(9.5), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // First set in progress: first_set_games=8, no fire
        let intents = tick(&mut engine, "game1", 0, 0, 4, 4, 8, Some(8), 0, 1, false, false);
        assert!(intents.is_empty());

        // First set: first_set_games=10, crosses 9.5 -> fires
        let intents = tick(&mut engine, "game1", 0, 0, 5, 5, 10, Some(10), 0, 1, false, false);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_first_set_total_under_at_set_end() {
        let mut engine = NativeTennisEngine::new();
        let t_under = target_json("tok_fs_under10", "under", "g1:FS_TOTAL:UNDER:10.5");
        let m = market_json("tennis_first_set_totals", Some(10.5), &[t_under]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Warm-up tick during set 1 (establishes baseline, cold-start skips)
        let intents = tick(&mut engine, "game1", 0, 0, 5, 4, 9, Some(9), 0, 1, false, false);
        assert!(intents.is_empty());

        // First set ends with 10 games -> under 10.5 fires
        let intents = tick(&mut engine, "game1", 1, 0, 0, 0, 10, Some(10), 1, 2, false, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    // =================================================================
    // Set totals tests
    // =================================================================

    #[test]
    fn test_set_total_over_guaranteed_certainty_bo3() {
        // BO3 (sets_to_win=2, the default): Over 2.5 fires at 1-1 (3rd set guaranteed).
        let mut engine = NativeTennisEngine::new();
        let t1 = target_json("tok_set_over2", "over", "g1:SET_TOTAL:OVER:2.5");
        let m = market_json("tennis_set_totals", Some(2.5), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // 1 set played (1-0): min(1,0)=0, min_needed=2+1-2=1. 0 < 1, no fire.
        let intents = tick(&mut engine, "game1", 1, 0, 3, 2, 15, None, 1, 2, false, true);
        assert!(intents.is_empty(), "Over 2.5 should not fire at 1-0");

        // 2 sets played (1-1): min(1,1)=1, min_needed=1. 1 >= 1, FIRES.
        let intents = tick(&mut engine, "game1", 1, 1, 0, 0, 20, None, 2, 3, false, true);
        assert_eq!(intents.len(), 1, "Over 2.5 should fire at 1-1 (3rd set guaranteed)");
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_set_total_over_guaranteed_certainty_bo5() {
        // BO5 (sets_to_win=3): Over 4.5 fires at 2-2 (5th set guaranteed).
        let mut engine = NativeTennisEngine::new();
        let t1 = target_json("tok_set_over4", "over", "g1:SET_TOTAL:OVER:4.5");
        let m = market_json("tennis_set_totals", Some(4.5), &[t1]);
        let plan_str = format!(
            r#"{{"games":[{{"provider_game_id":"game1","kickoff_ts_utc":1700000000,"sets_to_win":3,"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan_str).unwrap();

        // 3 sets played (2-1): min(2,1)=1, min_needed=4+1-3=2. 1 < 2, no fire.
        let intents = tick(&mut engine, "game1", 2, 1, 3, 2, 30, None, 3, 4, false, true);
        assert!(intents.is_empty(), "Over 4.5 should not fire at 2-1");

        // 4 sets played (2-2): min(2,2)=2, min_needed=2. 2 >= 2, FIRES.
        let intents = tick(&mut engine, "game1", 2, 2, 0, 0, 40, None, 4, 5, false, true);
        assert_eq!(intents.len(), 1, "Over 4.5 should fire at 2-2 (5th set guaranteed)");
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    // =================================================================
    // Moneyline tests
    // =================================================================

    #[test]
    fn test_moneyline_home_wins() {
        let mut engine = NativeTennisEngine::new();
        let t_home = target_json("tok_home", "home", "g1:ML:HOME");
        let m = market_json("moneyline", None, &[t_home]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Not completed -> no fire
        let intents = tick(&mut engine, "game1", 1, 0, 5, 3, 18, None, 1, 2, false, true);
        assert!(intents.is_empty());

        // Match ends with home winning 2-0
        let intents = tick(&mut engine, "game1", 2, 0, 6, 4, 24, None, 2, 2, true, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_moneyline_away_wins() {
        let mut engine = NativeTennisEngine::new();
        let t_away = target_json("tok_away", "away", "g1:ML:AWAY");
        let m = market_json("moneyline", None, &[t_away]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Match ends with away winning 1-2
        let intents = tick(&mut engine, "game1", 1, 2, 4, 6, 30, None, 3, 3, true, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_moneyline_fires_at_sets_to_win_before_ended() {
        // BO5: home reaches 3 sets with match_completed=false (no "Ended" yet).
        // Moneyline should fire immediately — match is decided.
        let mut engine = NativeTennisEngine::new();
        let t_home = target_json("tok_home", "home", "g1:ML:HOME");
        let m = market_json("moneyline", None, &[t_home]);
        let plan_str = format!(
            r#"{{"games":[{{"provider_game_id":"game1","kickoff_ts_utc":1700000000,"sets_to_win":3,"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan_str).unwrap();

        // Warm-up tick
        let intents = tick(&mut engine, "game1", 2, 1, 5, 3, 35, None, 3, 4, false, true);
        assert!(intents.is_empty());

        // Home wins 3rd set → sets=3-1, match_completed=false
        let intents = tick(&mut engine, "game1", 3, 1, 6, 4, 45, None, 4, 4, false, true);
        assert_eq!(intents.len(), 1, "Moneyline should fire at sets_to_win reached");
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_moneyline_does_not_fire_before_decided() {
        // BO5: score 2-1 — match not decided yet (need 3 sets).
        let mut engine = NativeTennisEngine::new();
        let t_home = target_json("tok_home", "home", "g1:ML:HOME");
        let m = market_json("moneyline", None, &[t_home]);
        let plan_str = format!(
            r#"{{"games":[{{"provider_game_id":"game1","kickoff_ts_utc":1700000000,"sets_to_win":3,"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan_str).unwrap();

        // Warm-up tick
        let intents = tick(&mut engine, "game1", 1, 0, 5, 3, 15, None, 1, 2, false, true);
        assert!(intents.is_empty());

        // Score 2-1, match_completed=false — not decided
        let intents = tick(&mut engine, "game1", 2, 1, 0, 0, 30, None, 3, 4, false, true);
        assert!(intents.is_empty(), "Moneyline should NOT fire at 2-1 in BO5");
    }

    // =================================================================
    // First-set winner tests
    // =================================================================

    #[test]
    fn test_first_set_winner_home() {
        let mut engine = NativeTennisEngine::new();
        let t_home = target_json("tok_fsw_home", "home", "g1:FSW:HOME");
        let m = market_json("tennis_first_set_winner", None, &[t_home]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // First set not completed -> no fire
        let intents = tick(&mut engine, "game1", 0, 0, 5, 4, 9, Some(9), 0, 1, false, false);
        assert!(intents.is_empty());

        // First set completed, home won (sets: 1-0)
        let intents = tick(&mut engine, "game1", 1, 0, 0, 0, 10, Some(10), 1, 2, false, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));

        // Second tick after first set -> no double fire
        let intents = tick(&mut engine, "game1", 1, 0, 1, 0, 11, Some(10), 1, 2, false, true);
        assert!(intents.is_empty());
    }

    // =================================================================
    // Set handicap tests
    // =================================================================

    #[test]
    fn test_set_handicap_home_covers() {
        let mut engine = NativeTennisEngine::new();
        let t_covers = target_json("tok_hc_home", "home_covers", "g1:SH:HOME_COVERS:-1.5");
        let t_not = target_json("tok_hc_home_not", "home_not_covers", "g1:SH:HOME_NOT_COVERS:-1.5");
        let m = market_json("tennis_set_handicap", Some(-1.5), &[t_covers, t_not]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Home wins 3-0 -> margin=3, 3 + (-1.5) = 1.5 > 0 -> covers
        let intents = tick(&mut engine, "game1", 3, 0, 6, 2, 30, None, 3, 3, true, true);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0)); // covers
    }

    #[test]
    fn test_set_handicap_single_slot_covers_and_not_covers() {
        // Tennis set handicap: single SpreadSlot with both covers_idx and
        // not_covers_idx. The slug determines the side (home or away).
        // When the favored player doesn't cover, not_covers fires.
        let mut engine = NativeTennisEngine::new();
        let t_covers = target_json("tok_hc_covers", "home_covers", "g1:SH:HOME_COVERS:-1.5");
        let t_not = target_json("tok_hc_not", "home_not_covers", "g1:SH:HOME_NOT_COVERS:-1.5");
        let m = market_json("tennis_set_handicap", Some(-1.5), &[t_covers, t_not]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Home wins 2-1 -> margin=1, 1 + (-1.5) = -0.5 <= 0 -> home doesn't cover
        // not_covers_idx fires (TargetIdx 1)
        let intents = tick(&mut engine, "game1", 2, 1, 6, 4, 32, None, 3, 3, true, true);
        assert_eq!(intents.len(), 1, "home_not_covers should fire");
        assert_eq!(intents[0].target_idx, TargetIdx(1));
    }

    #[test]
    fn test_set_handicap_covers_fires_when_margin_sufficient() {
        // Home wins 3-0 -> margin=3, 3 + (-2.5) = 0.5 > 0 -> covers
        let mut engine = NativeTennisEngine::new();
        let t_covers = target_json("tok_hc_covers", "home_covers", "g1:SH:HOME_COVERS:-2.5");
        let t_not = target_json("tok_hc_not", "home_not_covers", "g1:SH:HOME_NOT_COVERS:-2.5");
        let m = market_json("tennis_set_handicap", Some(-2.5), &[t_covers, t_not]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        let intents = tick(&mut engine, "game1", 3, 0, 6, 4, 30, None, 3, 3, true, true);
        assert_eq!(intents.len(), 1, "home_covers should fire");
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_set_handicap_not_covers_fires_early_bo5() {
        // BO5 line -2.5: not_covers guaranteed when opponent has >= 1 set.
        // max_margin = 3 - 1 = 2, 2 + (-2.5) = -0.5 <= 0 → guaranteed.
        let mut engine = NativeTennisEngine::new();
        let t_covers = target_json("tok_covers", "home_covers", "g1:SH:HOME_COVERS:-2.5");
        let t_not = target_json("tok_not", "home_not_covers", "g1:SH:HOME_NOT_COVERS:-2.5");
        let m = market_json("tennis_set_handicap", Some(-2.5), &[t_covers, t_not]);
        let plan_str = format!(
            r#"{{"games":[{{"provider_game_id":"game1","kickoff_ts_utc":1700000000,"sets_to_win":3,"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan_str).unwrap();

        // Warm-up tick (baseline)
        let intents = tick(&mut engine, "game1", 0, 0, 3, 2, 5, Some(5), 0, 1, false, false);
        assert!(intents.is_empty());

        // Opponent wins 1st set (score 0-1): not_covers fires immediately
        let intents = tick(&mut engine, "game1", 0, 1, 0, 0, 10, Some(10), 1, 2, false, true);
        assert_eq!(intents.len(), 1, "not_covers should fire when opponent has 1 set");
        assert_eq!(intents[0].target_idx, TargetIdx(1)); // not_covers
    }

    #[test]
    fn test_set_handicap_no_early_fire_when_favored_leads() {
        // BO5 line -2.5: favored leads 2-0. max_margin = 3-0 = 3, 3+(-2.5) = 0.5 > 0.
        // not_covers NOT guaranteed (could still win 3-0). covers not yet determined.
        let mut engine = NativeTennisEngine::new();
        let t_covers = target_json("tok_covers", "home_covers", "g1:SH:HOME_COVERS:-2.5");
        let t_not = target_json("tok_not", "home_not_covers", "g1:SH:HOME_NOT_COVERS:-2.5");
        let m = market_json("tennis_set_handicap", Some(-2.5), &[t_covers, t_not]);
        let plan_str = format!(
            r#"{{"games":[{{"provider_game_id":"game1","kickoff_ts_utc":1700000000,"sets_to_win":3,"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan_str).unwrap();

        // Warm-up tick
        let intents = tick(&mut engine, "game1", 0, 0, 3, 2, 5, Some(5), 0, 1, false, false);
        assert!(intents.is_empty());

        // Favored leads 2-0: nothing fires
        let intents = tick(&mut engine, "game1", 2, 0, 0, 0, 20, Some(10), 2, 3, false, true);
        assert!(intents.is_empty(), "Nothing should fire when favored leads 2-0");
    }

    #[test]
    fn test_set_handicap_not_covers_fires_early_bo3() {
        // BO3 line -1.5: not_covers guaranteed when opponent has >= 1 set.
        // max_margin = 2-1 = 1, 1 + (-1.5) = -0.5 <= 0 → guaranteed.
        // At score 1-1, not_covers fires.
        let mut engine = NativeTennisEngine::new();
        let t_covers = target_json("tok_covers", "home_covers", "g1:SH:HOME_COVERS:-1.5");
        let t_not = target_json("tok_not", "home_not_covers", "g1:SH:HOME_NOT_COVERS:-1.5");
        let m = market_json("tennis_set_handicap", Some(-1.5), &[t_covers, t_not]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Warm-up tick
        let intents = tick(&mut engine, "game1", 0, 0, 3, 2, 5, Some(5), 0, 1, false, false);
        assert!(intents.is_empty());

        // Score 1-1: not_covers fires
        let intents = tick(&mut engine, "game1", 1, 1, 0, 0, 20, Some(10), 2, 3, false, true);
        assert_eq!(intents.len(), 1, "not_covers should fire at 1-1 in BO3");
        assert_eq!(intents[0].target_idx, TargetIdx(1));
    }

    // =================================================================
    // merge_plan tests
    // =================================================================

    #[test]
    fn merge_plan_adds_new_game() {
        let mut engine = NativeTennisEngine::new();
        let initial = plan_json_one_game(
            "game_1",
            &format!(
                r#"{{"sports_market_type":"moneyline","line":null,"targets":[{},{}]}}"#,
                target_json("t1", "home", "g1:ML:HOME"),
                target_json("t2", "away", "g1:ML:AWAY"),
            ),
        );
        engine.load_plan_from_json(&initial).unwrap();
        assert_eq!(engine.game_ids.len(), 1);

        let patch = plan_json_one_game(
            "game_2",
            &format!(
                r#"{{"sports_market_type":"moneyline","line":null,"targets":[{},{}]}}"#,
                target_json("t3", "home", "g2:ML:HOME"),
                target_json("t4", "away", "g2:ML:AWAY"),
            ),
        );
        let result = engine.merge_plan(&patch).unwrap();
        assert_eq!(result.new_games, 1);
        assert_eq!(result.new_targets, 2);
        assert_eq!(result.new_tokens, 2);
        assert_eq!(engine.game_ids.len(), 2);
        assert!(engine.game_id_to_idx.contains_key("game_2"));
        assert!(engine.has_moneyline[1]);
    }

    #[test]
    fn merge_plan_dedup_strategy_key() {
        let mut engine = NativeTennisEngine::new();
        let initial = plan_json_one_game(
            "game_1",
            &format!(
                r#"{{"sports_market_type":"moneyline","line":null,"targets":[{}]}}"#,
                target_json("t1", "home", "g1:ML:HOME"),
            ),
        );
        engine.load_plan_from_json(&initial).unwrap();
        assert_eq!(engine.target_slots.len(), 1);

        // Merge with same strategy_key -> skipped
        let patch = plan_json_one_game(
            "game_1",
            &format!(
                r#"{{"sports_market_type":"moneyline","line":null,"targets":[{}]}}"#,
                target_json("t1", "home", "g1:ML:HOME"),
            ),
        );
        let result = engine.merge_plan(&patch).unwrap();
        assert_eq!(result.new_targets, 0);
        assert_eq!(engine.target_slots.len(), 1);
    }

    #[test]
    fn test_alternate_game_ids_resolve_to_same_gidx() {
        let mut engine = NativeTennisEngine::new();
        let t1 = target_json("tok_home", "home", "g1:ML:HOME");
        let m = market_json("moneyline", None, &[t1]);
        let plan = format!(
            r#"{{"games":[{{"provider_game_id":"primary_id","kickoff_ts_utc":1700000000,"alternate_provider_game_ids":[{{"provider":"boltodds","game_id":"boltodds_label"}},{{"provider":"kalstrop_v1","game_id":"v1_uuid"}}],"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan).unwrap();

        assert_eq!(engine.game_ids.len(), 1);
        assert_eq!(engine.game_ids[0], "primary_id");

        let primary = engine.game_id_to_idx.get("primary_id").copied();
        let alt_bo = engine.game_id_to_idx.get("boltodds_label").copied();
        let alt_v1 = engine.game_id_to_idx.get("v1_uuid").copied();
        assert_eq!(primary, Some(GameIdx(0)));
        assert_eq!(alt_bo, Some(GameIdx(0)));
        assert_eq!(alt_v1, Some(GameIdx(0)));
        assert_eq!(engine.game_id_to_idx.len(), 3);
    }

    #[test]
    fn test_is_game_completed_bool() {
        let mut engine = NativeTennisEngine::new();
        let t1 = target_json("tok_home", "home", "g1:ML:HOME");
        let m = market_json("moneyline", None, &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Initially not completed.
        assert!(!engine.is_game_completed("game1"));

        // Tick with match_completed=true.
        tick(&mut engine, "game1", 2, 0, 6, 4, 24, None, 2, 2, true, true);
        assert!(engine.is_game_completed("game1"));
    }

    // =================================================================
    // C1 regression: first-set total OVER must fire during set 1
    // =================================================================

    #[test]
    fn test_first_set_total_over_fires_during_set_1() {
        // Simulates the frame pipeline path: during set 1, first_set_games
        // is Some(live_value) and first_set_completed is false.
        let mut engine = NativeTennisEngine::new();
        let t_over = target_json("tok_fs_over5", "over", "g1:FS_TOTAL:OVER:5.5");
        let m = market_json("tennis_first_set_totals", Some(5.5), &[t_over]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Tick 1: first_set_games=4 (2-2), still in set 1, no fire
        let intents = tick(
            &mut engine, "game1",
            0, 0,       // sets: 0-0
            2, 2,       // games: 2-2
            4,          // total_games
            Some(4),    // first_set_games: live value from phases[0]
            0,          // total_sets: 0 (set 1 not done)
            1,          // current_set: 1
            false, false,
        );
        assert!(intents.is_empty(), "Over 5.5 should not fire at 4 games");

        // Tick 2: first_set_games=6 (3-3), crosses 5.5 -> fires
        let intents = tick(
            &mut engine, "game1",
            0, 0, 3, 3, 6,
            Some(6),    // first_set_games: live value
            0, 1,       // total_sets=0, current_set=1
            false, false,
        );
        assert_eq!(intents.len(), 1, "Over 5.5 should fire at 6 games");
        assert_eq!(intents[0].target_idx, TargetIdx(0));

        // Tick 3: first_set_games=8 (4-4), no re-fire (presign pool gate)
        let intents = tick(
            &mut engine, "game1",
            0, 0, 4, 4, 8,
            Some(8), 0, 1,
            false, false,
        );
        assert!(intents.is_empty(), "Over 5.5 should not re-fire");
    }

    // =================================================================
    // C2 regression: "Interrupted" must NOT trigger final evaluators
    // =================================================================

    #[test]
    fn test_interrupted_does_not_fire_finals() {
        let mut engine = NativeTennisEngine::new();
        let t_ml = target_json("tok_ml_home", "home", "g1:ML:HOME");
        let t_under = target_json("tok_mt_under30", "under", "g1:MT:UNDER:30.5");
        let m_ml = market_json("moneyline", None, &[t_ml]);
        let m_mt = market_json("tennis_match_totals", Some(30.5), &[t_under]);
        let plan = plan_json_one_game("game1", &format!("{},{}", m_ml, m_mt));
        engine.load_plan_from_json(&plan).unwrap();

        // Tick with match_completed=false (what frame pipeline produces for "Interrupted")
        let intents = tick(
            &mut engine, "game1",
            1, 0,       // sets: 1-0 (leading)
            3, 2,       // games: 3-2
            18,         // total_games
            Some(13),   // first_set_games
            1,          // total_sets
            2,          // current_set
            false,      // match_completed = false (Interrupted!)
            true,
        );
        assert!(intents.is_empty(), "No finals should fire on Interrupted");

        // Tick with match_completed=true (what frame pipeline produces for "Ended")
        let intents = tick(
            &mut engine, "game1",
            2, 0, 6, 4, 28,
            Some(13), 2, 2,
            true,       // match_completed = true (Ended)
            true,
        );
        // Should fire moneyline HOME + under 30.5
        assert_eq!(intents.len(), 2, "Moneyline + under should fire on Ended");
    }
}
