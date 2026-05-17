//! Soccer engine: plan loading, game state management, zero-alloc live tick path.

use crate::soccer::types::*;
use crate::InlineStr;
use crate::*;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

impl NativeSoccerEngine {
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
            has_totals: Vec::new(),
            has_moneyline: Vec::new(),
            has_btts: Vec::new(),
            has_corners: Vec::new(),
            has_halftime: Vec::new(),
            has_exact_score: Vec::new(),
            rows: Vec::new(),
            game_states: Vec::new(),
            totals_final_under_emitted: Vec::new(),
            final_resolved_games: Vec::new(),
            btts_resolved_games: Vec::new(),
            corners_final_under_emitted: Vec::new(),
            halftime_resolved: Vec::new(),
            exact_score_resolved: Vec::new(),
            boltodds_rows: Vec::new(),
        }
    }

    pub(crate) fn reset_runtime_state(&mut self) {
        self.rows.fill(None);
        for gs in &mut self.game_states {
            *gs = SoccerGameState::default();
        }
        self.totals_final_under_emitted.fill(false);
        self.final_resolved_games.fill(false);
        self.btts_resolved_games.fill(false);
        self.corners_final_under_emitted.fill(false);
        self.halftime_resolved.fill(false);
        self.exact_score_resolved.fill(false);
        self.boltodds_rows.fill(None);
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
            self.game_states[gidx.0 as usize]
                .match_completed
                .unwrap_or(false)
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
        self.has_totals.clear();
        self.has_moneyline.clear();
        self.has_btts.clear();
        self.has_corners.clear();
        self.has_halftime.clear();
        self.has_exact_score.clear();

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

            let markets = match game_val.get("markets").and_then(|v| v.as_array()) {
                Some(m) => m,
                None => {
                    self.game_targets.push(SoccerGameTargets::default());
                    self.has_totals.push(false);
                    self.has_moneyline.push(false);
                    self.has_btts.push(false);
                    self.has_corners.push(false);
                    self.has_halftime.push(false);
                    self.has_exact_score.push(false);
                    self.token_ids_by_game.push(Vec::new());
                    continue;
                }
            };

            let mut game_tgt = SoccerGameTargets::default();
            let mut game_has_totals = false;
            let mut game_has_moneyline = false;
            let mut game_has_btts = false;
            let mut game_has_corners = false;
            let mut game_has_halftime = false;
            let mut game_has_exact_score = false;
            let mut token_ids: HashSet<String> = HashSet::new();
            let game_id_ref = self.game_ids[gidx.0 as usize].as_str();

            for market_val in markets {
                let sports_market_type = canonical_soccer_market_type(
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
                    // Prefer target-level line (exact scores have it), fall back to market-level.
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
                        "totals" => {
                            game_has_totals = true;
                            if let Some(l) = effective_line {
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
                                        eprintln!("[polybot2] WARN: unhandled totals semantic '{}' for game {}", other, game_id_ref);
                                    }
                                }
                            }
                        }
                        "moneyline" => {
                            game_has_moneyline = true;
                            match semantic.as_str() {
                                "home_yes" | "home" => game_tgt.moneyline_home_yes = Some(tidx),
                                "home_no" => game_tgt.moneyline_home_no = Some(tidx),
                                "away_yes" | "away" => game_tgt.moneyline_away_yes = Some(tidx),
                                "away_no" => game_tgt.moneyline_away_no = Some(tidx),
                                "draw_yes" => game_tgt.moneyline_draw_yes = Some(tidx),
                                "draw_no" => game_tgt.moneyline_draw_no = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled moneyline semantic '{}' for game {}", other, game_id_ref);
                                }
                            }
                        }
                        "spread" => {
                            game_has_moneyline = true; // spreads use the same final gate
                            if let Some(l) = effective_line {
                                let (side, is_covers) = match semantic.as_str() {
                                    "home_covers" | "home" => (SpreadSide::Home, true),
                                    "home_not_covers" => (SpreadSide::Home, false),
                                    "away_covers" | "away" => (SpreadSide::Away, true),
                                    "away_not_covers" => (SpreadSide::Away, false),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled spread semantic '{}' for game {}", other, game_id_ref);
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
                        "btts" => {
                            game_has_btts = true;
                            match semantic.as_str() {
                                "yes" => game_tgt.btts_yes = Some(tidx),
                                "no" => game_tgt.btts_no = Some(tidx),
                                other => {
                                    eprintln!(
                                        "[polybot2] WARN: unhandled btts semantic '{}' for game {}",
                                        other, game_id_ref
                                    );
                                }
                            }
                        }
                        "total_corners" => {
                            game_has_corners = true;
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.corner_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.corner_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled corners semantic '{}' for game {}", other, game_id_ref);
                                    }
                                }
                            }
                        }
                        "soccer_halftime_result" => {
                            game_has_halftime = true;
                            match semantic.as_str() {
                                "home_yes" | "home" => game_tgt.halftime_home_yes = Some(tidx),
                                "home_no" => game_tgt.halftime_home_no = Some(tidx),
                                "away_yes" | "away" => game_tgt.halftime_away_yes = Some(tidx),
                                "away_no" => game_tgt.halftime_away_no = Some(tidx),
                                "draw_yes" => game_tgt.halftime_draw_yes = Some(tidx),
                                "draw_no" => game_tgt.halftime_draw_no = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled halftime semantic '{}' for game {}", other, game_id_ref);
                                }
                            }
                        }
                        "soccer_exact_score" => {
                            game_has_exact_score = true;
                            match semantic.as_str() {
                                "exact_yes" | "exact_no" => {
                                    if let Some(line_val) = effective_line {
                                        let home_pred = line_val.floor() as i64;
                                        let away_pred =
                                            ((line_val - line_val.floor()) * 10.0).round() as i64;
                                        let is_yes = semantic.as_str() == "exact_yes";
                                        // Find or create slot for this score
                                        if let Some(slot) =
                                            game_tgt.exact_scores.iter_mut().find(|s| {
                                                s.home_pred == home_pred && s.away_pred == away_pred
                                            })
                                        {
                                            if is_yes {
                                                slot.yes_idx = Some(tidx);
                                            } else {
                                                slot.no_idx = Some(tidx);
                                            }
                                        } else {
                                            let mut slot = ExactScoreSlot::default();
                                            slot.home_pred = home_pred;
                                            slot.away_pred = away_pred;
                                            if is_yes {
                                                slot.yes_idx = Some(tidx);
                                            } else {
                                                slot.no_idx = Some(tidx);
                                            }
                                            game_tgt.exact_scores.push(slot);
                                        }
                                    }
                                }
                                "any_other_yes" => game_tgt.any_other_score_yes = Some(tidx),
                                "any_other_no" => game_tgt.any_other_score_no = Some(tidx),
                                // Legacy: accept bare "yes"/"over" from old plans
                                "yes" | "over" => {
                                    if let Some(line_val) = effective_line {
                                        let home_pred = line_val.floor() as i64;
                                        let away_pred =
                                            ((line_val - line_val.floor()) * 10.0).round() as i64;
                                        if let Some(slot) =
                                            game_tgt.exact_scores.iter_mut().find(|s| {
                                                s.home_pred == home_pred && s.away_pred == away_pred
                                            })
                                        {
                                            slot.yes_idx = Some(tidx);
                                        } else {
                                            let mut slot = ExactScoreSlot::default();
                                            slot.home_pred = home_pred;
                                            slot.away_pred = away_pred;
                                            slot.yes_idx = Some(tidx);
                                            game_tgt.exact_scores.push(slot);
                                        }
                                    }
                                }
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled exact_score semantic '{}' for game {}", other, game_id_ref);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            game_tgt.over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.under_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.corner_over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.corner_under_lines.sort_by_key(|ol| ol.half_int);
            self.game_targets.push(game_tgt);
            self.has_totals.push(game_has_totals);
            self.has_moneyline.push(game_has_moneyline);
            self.has_btts.push(game_has_btts);
            self.has_corners.push(game_has_corners);
            self.has_halftime.push(game_has_halftime);
            self.has_exact_score.push(game_has_exact_score);

            let mut token_list = token_ids.into_iter().collect::<Vec<_>>();
            token_list.sort();
            token_list.dedup();
            self.token_ids_by_game.push(token_list);
        }

        let num_games = self.game_ids.len();
        self.rows = vec![None; num_games];
        self.game_states = vec![SoccerGameState::default(); num_games];
        self.totals_final_under_emitted = vec![false; num_games];
        self.final_resolved_games = vec![false; num_games];
        self.btts_resolved_games = vec![false; num_games];
        self.corners_final_under_emitted = vec![false; num_games];
        self.halftime_resolved = vec![false; num_games];
        self.exact_score_resolved = vec![false; num_games];
        self.boltodds_rows = vec![None; num_games];

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

    /// BoltOdds frame-level dedup + game index resolve in one lookup.
    /// Returns `None` if the frame is a duplicate or the game_id is unknown.
    /// Returns `Some(gidx)` otherwise, for use in subsequent indexed calls.
    pub(crate) fn check_boltodds_dedup(
        &self,
        game_id: &str,
        goals_a: i64,
        goals_b: i64,
        corners_a: i64,
        corners_b: i64,
        period: &str,
    ) -> Option<GameIdx> {
        let &gidx = self.game_id_to_idx.get(game_id)?;
        let gi = gidx.0 as usize;
        if let Some(row) = self.boltodds_rows.get(gi).and_then(|r| r.as_ref()) {
            if row.goals_a == goals_a
                && row.goals_b == goals_b
                && row.corners_a == corners_a
                && row.corners_b == corners_b
                && row.period.as_str() == period
            {
                return None; // duplicate
            }
        }
        Some(gidx)
    }

    /// Update the BoltOdds dedup state row by GameIdx (no hash lookup).
    pub(crate) fn update_boltodds_row_indexed(
        &mut self,
        gidx: GameIdx,
        goals_a: i64,
        goals_b: i64,
        corners_a: i64,
        corners_b: i64,
        period: &str,
    ) {
        let gi = gidx.0 as usize;
        if gi < self.boltodds_rows.len() {
            self.boltodds_rows[gi] = Some(BoltOddsSoccerStateRow {
                goals_a,
                goals_b,
                corners_a,
                corners_b,
                period: InlineStr::from_str(period),
            });
        }
    }

    /// Process a tick from borrowed fields without constructing a `Tick` or
    /// allocating any strings. Returns `None` for unknown games.
    /// Dedup + game_id_to_idx resolve is handled by `check_duplicate` /
    /// `check_boltodds_dedup` in the frame pipeline before calling this method.
    pub(crate) fn process_tick_live(
        &mut self,
        gidx: GameIdx,
        home_score_raw: &str,
        away_score_raw: &str,
        free_text_raw: &str,
        goals_home: Option<i64>,
        goals_away: Option<i64>,
        corners_home: Option<i64>,
        corners_away: Option<i64>,
        half: &'static str,
        match_completed: Option<bool>,
        game_state: &'static str,
        _recv_monotonic_ns: i64,
    ) -> Option<SoccerLiveTickResult> {
        let gi = gidx.0 as usize;

        // Update V1 dedup row only if we have V1 data (BoltOdds uses its own dedup).
        if !home_score_raw.is_empty() || !away_score_raw.is_empty() || !free_text_raw.is_empty() {
            self.rows[gi] = Some(SoccerStateRow {
                home_score_raw: InlineStr::from_str(home_score_raw),
                away_score_raw: InlineStr::from_str(away_score_raw),
                free_text_raw: InlineStr::from_str(free_text_raw),
            });
        }

        // Update game state.
        let prev = self.game_states[gi];
        let home = goals_home.or(prev.home);
        let away = goals_away.or(prev.away);
        let resolved_half = if half.is_empty() { prev.half } else { half };
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
        let mut state = SoccerGameState {
            home: prev.home,
            away: prev.away,
            total: prev.total,
            prev_total: prev.total,
            prev_home: prev.home,
            prev_away: prev.away,
            half: resolved_half,
            match_completed: completed,
            game_state: gs,
            total_corners: prev.total_corners,
            prev_total_corners: prev.total_corners,
        };
        if home.is_some() && away.is_some() {
            state.home = home;
            state.away = away;
            state.prev_total = prev.total;
            state.total = Some(home.unwrap_or(0) + away.unwrap_or(0));
        }
        // Update corner state
        if let (Some(ch), Some(ca)) = (corners_home, corners_away) {
            state.prev_total_corners = prev.total_corners;
            state.total_corners = Some(ch + ca);
        }
        self.game_states[gi] = state;

        // Evaluate directly into stack-allocated SmallVec — no intermediate type.
        let mut intents = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_totals_into(gidx, &state, &mut intents);
        self.evaluate_moneyline_into(gidx, &state, &mut intents);
        self.evaluate_btts_into(gidx, &state, &mut intents);
        self.evaluate_corners_into(gidx, &state, &mut intents);
        self.evaluate_halftime_into(gidx, &state, &mut intents);
        if state.home != prev.home || state.away != prev.away
            || state.match_completed.unwrap_or(false)
        {
            self.evaluate_exact_score_into(gidx, &state, &mut intents);
        }

        if state.match_completed.unwrap_or(false) && self.final_resolved_games[gi] {
            self.cleanup_completed_game_idx(gidx);
        }

        Some(SoccerLiveTickResult {
            game_idx: gidx,
            state,
            intents,
        })
    }

    fn cleanup_completed_game_idx(&mut self, gidx: GameIdx) {
        let gi = gidx.0 as usize;
        self.rows[gi] = None;
        self.boltodds_rows[gi] = None;
        self.game_states[gi] = SoccerGameState::default();
        // Tombstones preserved: totals_final_under_emitted, final_resolved_games,
        // btts_resolved_games, corners_final_under_emitted, halftime_resolved,
        // exact_score_resolved
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
                    self.game_targets.push(SoccerGameTargets::default());
                    self.has_totals.push(false);
                    self.has_moneyline.push(false);
                    self.has_btts.push(false);
                    self.has_corners.push(false);
                    self.has_halftime.push(false);
                    self.has_exact_score.push(false);
                    self.token_ids_by_game.push(Vec::new());
                    self.rows.push(None);
                    self.game_states.push(SoccerGameState::default());
                    self.totals_final_under_emitted.push(false);
                    self.final_resolved_games.push(false);
                    self.btts_resolved_games.push(false);
                    self.corners_final_under_emitted.push(false);
                    self.halftime_resolved.push(false);
                    self.exact_score_resolved.push(false);
                    self.boltodds_rows.push(None);
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
                let sports_market_type = canonical_soccer_market_type(
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
                        "totals" => {
                            if let Some(l) = effective_line {
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
                                        eprintln!("[polybot2] WARN: unhandled totals semantic '{}' for game {}", other, uid);
                                    }
                                }
                            }
                            self.has_totals[gi] = true;
                            dirty_games.insert(gi);
                        }
                        "moneyline" => {
                            match semantic.as_str() {
                                "home_yes" | "home" => game_tgt.moneyline_home_yes = Some(tidx),
                                "home_no" => game_tgt.moneyline_home_no = Some(tidx),
                                "away_yes" | "away" => game_tgt.moneyline_away_yes = Some(tidx),
                                "away_no" => game_tgt.moneyline_away_no = Some(tidx),
                                "draw_yes" => game_tgt.moneyline_draw_yes = Some(tidx),
                                "draw_no" => game_tgt.moneyline_draw_no = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled moneyline semantic '{}' for game {}", other, uid);
                                }
                            }
                            self.has_moneyline[gi] = true;
                        }
                        "spread" => {
                            if let Some(l) = effective_line {
                                let (side, is_covers) = match semantic.as_str() {
                                    "home_covers" | "home" => (SpreadSide::Home, true),
                                    "home_not_covers" => (SpreadSide::Home, false),
                                    "away_covers" | "away" => (SpreadSide::Away, true),
                                    "away_not_covers" => (SpreadSide::Away, false),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled spread semantic '{}' for game {}", other, uid);
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
                            self.has_moneyline[gi] = true;
                        }
                        "btts" => {
                            match semantic.as_str() {
                                "yes" => game_tgt.btts_yes = Some(tidx),
                                "no" => game_tgt.btts_no = Some(tidx),
                                other => {
                                    eprintln!(
                                        "[polybot2] WARN: unhandled btts semantic '{}' for game {}",
                                        other, uid
                                    );
                                }
                            }
                            self.has_btts[gi] = true;
                        }
                        "total_corners" => {
                            if let Some(l) = effective_line {
                                let half = l.floor() as u16;
                                match semantic.as_str() {
                                    "over" => game_tgt.corner_over_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    "under" => game_tgt.corner_under_lines.push(OverLine {
                                        half_int: half,
                                        target_idx: tidx,
                                    }),
                                    other => {
                                        eprintln!("[polybot2] WARN: unhandled corners semantic '{}' for game {}", other, uid);
                                    }
                                }
                            }
                            self.has_corners[gi] = true;
                            dirty_games.insert(gi);
                        }
                        "soccer_halftime_result" => {
                            match semantic.as_str() {
                                "home_yes" | "home" => game_tgt.halftime_home_yes = Some(tidx),
                                "home_no" => game_tgt.halftime_home_no = Some(tidx),
                                "away_yes" | "away" => game_tgt.halftime_away_yes = Some(tidx),
                                "away_no" => game_tgt.halftime_away_no = Some(tidx),
                                "draw_yes" => game_tgt.halftime_draw_yes = Some(tidx),
                                "draw_no" => game_tgt.halftime_draw_no = Some(tidx),
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled halftime semantic '{}' for game {}", other, uid);
                                }
                            }
                            self.has_halftime[gi] = true;
                        }
                        "soccer_exact_score" => {
                            match semantic.as_str() {
                                "exact_yes" | "exact_no" => {
                                    if let Some(line_val) = effective_line {
                                        let home_pred = line_val.floor() as i64;
                                        let away_pred =
                                            ((line_val - line_val.floor()) * 10.0).round() as i64;
                                        let is_yes = semantic.as_str() == "exact_yes";
                                        if let Some(slot) =
                                            game_tgt.exact_scores.iter_mut().find(|s| {
                                                s.home_pred == home_pred && s.away_pred == away_pred
                                            })
                                        {
                                            if is_yes {
                                                slot.yes_idx = Some(tidx);
                                            } else {
                                                slot.no_idx = Some(tidx);
                                            }
                                        } else {
                                            let mut slot = ExactScoreSlot::default();
                                            slot.home_pred = home_pred;
                                            slot.away_pred = away_pred;
                                            if is_yes {
                                                slot.yes_idx = Some(tidx);
                                            } else {
                                                slot.no_idx = Some(tidx);
                                            }
                                            game_tgt.exact_scores.push(slot);
                                        }
                                    }
                                }
                                "any_other_yes" => game_tgt.any_other_score_yes = Some(tidx),
                                "any_other_no" => game_tgt.any_other_score_no = Some(tidx),
                                "yes" | "over" => {
                                    if let Some(line_val) = effective_line {
                                        let home_pred = line_val.floor() as i64;
                                        let away_pred =
                                            ((line_val - line_val.floor()) * 10.0).round() as i64;
                                        if let Some(slot) =
                                            game_tgt.exact_scores.iter_mut().find(|s| {
                                                s.home_pred == home_pred && s.away_pred == away_pred
                                            })
                                        {
                                            slot.yes_idx = Some(tidx);
                                        } else {
                                            let mut slot = ExactScoreSlot::default();
                                            slot.home_pred = home_pred;
                                            slot.away_pred = away_pred;
                                            slot.yes_idx = Some(tidx);
                                            game_tgt.exact_scores.push(slot);
                                        }
                                    }
                                }
                                other => {
                                    eprintln!("[polybot2] WARN: unhandled exact_score semantic '{}' for game {}", other, uid);
                                }
                            }
                            self.has_exact_score[gi] = true;
                        }
                        _ => {}
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
                .over_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .under_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .corner_over_lines
                .sort_by_key(|ol| ol.half_int);
            self.game_targets[gi]
                .corner_under_lines
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

fn canonical_soccer_market_type(input: &str) -> String {
    let raw = norm(input).replace('-', "_").replace(' ', "_");
    match raw.as_str() {
        "total" | "totals" | "ou" | "o_u" => "totals".to_string(),
        "spread" | "spreads" => "spread".to_string(),
        "moneyline" | "game" | "match_result" => "moneyline".to_string(),
        "both_teams_to_score" | "btts" => "btts".to_string(),
        "total_corners" | "corners" => "total_corners".to_string(),
        "soccer_halftime_result" | "halftime_result" | "ht_result" => {
            "soccer_halftime_result".to_string()
        }
        "soccer_exact_score" | "exact_score" | "correct_score" => "soccer_exact_score".to_string(),
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
        engine: &mut NativeSoccerEngine,
        game_id: &str,
        goals_home: Option<i64>,
        goals_away: Option<i64>,
        corners_home: Option<i64>,
        corners_away: Option<i64>,
        half: &'static str,
        match_completed: Option<bool>,
    ) -> Vec<Intent> {
        let gidx = match engine.game_id_to_idx.get(game_id) {
            Some(&g) => g,
            None => return vec![],
        };
        let result = engine.process_tick_live(
            gidx,
            "",
            "",
            "free_text",
            goals_home,
            goals_away,
            corners_home,
            corners_away,
            half,
            match_completed,
            "LIVE",
            1000,
        );
        match result {
            Some(r) => r.intents.to_vec(),
            None => vec![],
        }
    }

    // =================================================================
    // Total Corners tests
    // =================================================================

    #[test]
    fn test_corner_over_crossing() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_over3", "over", "g1:CORNERS:OVER:3.5");
        let m = market_json("total_corners", Some(3.5), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Tick 1: corners 0+0=0, no fire
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            "1st half",
            Some(false),
        );
        assert!(intents.is_empty());

        // Tick 2: corners 2+2=4, crosses 3.5 -> fires
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            Some(2),
            Some(2),
            "1st half",
            Some(false),
        );
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_corner_multiple_crossings() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_over5", "over", "g1:CORNERS:OVER:5.5");
        let t2 = target_json("tok_over6", "over", "g1:CORNERS:OVER:6.5");
        let m = market_json("total_corners", Some(5.5), &[t1]);
        let m2 = market_json("total_corners", Some(6.5), &[t2]);
        let plan = plan_json_one_game("game1", &format!("{},{}", m, m2));
        engine.load_plan_from_json(&plan).unwrap();

        // Initial: corners 2+3=5
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            Some(2),
            Some(3),
            "1st half",
            Some(false),
        );
        assert!(intents.is_empty());

        // Jump: corners 4+3=7, crosses both 5.5 and 6.5
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            Some(4),
            Some(3),
            "2nd half",
            Some(false),
        );
        assert_eq!(intents.len(), 2);
    }

    #[test]
    fn test_corner_under_at_game_end() {
        let mut engine = NativeSoccerEngine::new();
        let t_under8 = target_json("tok_under8", "under", "g1:CORNERS:UNDER:8.5");
        let t_under9 = target_json("tok_under9", "under", "g1:CORNERS:UNDER:9.5");
        let t_under7 = target_json("tok_under7", "under", "g1:CORNERS:UNDER:7.5");
        let m1 = market_json("total_corners", Some(8.5), &[t_under8]);
        let m2 = market_json("total_corners", Some(9.5), &[t_under9]);
        let m3 = market_json("total_corners", Some(7.5), &[t_under7]);
        let plan = plan_json_one_game("game1", &format!("{},{},{}", m1, m2, m3));
        engine.load_plan_from_json(&plan).unwrap();

        // Corners at 4+4=8 during game
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(1),
            Some(4),
            Some(4),
            "2nd half",
            Some(false),
        );
        assert!(intents.is_empty());

        // Game ends with 8 total corners -> under 8.5 and under 9.5 fire, NOT under 7.5
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(1),
            Some(4),
            Some(4),
            "Ended",
            Some(true),
        );
        assert_eq!(intents.len(), 2);
        // Target indices: under 8.5 -> tidx 0, under 9.5 -> tidx 1, under 7.5 -> tidx 2
        let fired: Vec<u16> = intents.iter().map(|i| i.target_idx.0).collect();
        assert!(fired.contains(&0)); // under 8.5
        assert!(fired.contains(&1)); // under 9.5
        assert!(!fired.contains(&2)); // under 7.5 should NOT fire (8 >= 7)
    }

    #[test]
    fn test_corner_no_fire_on_no_change() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_over3", "over", "g1:CORNERS:OVER:3.5");
        let m = market_json("total_corners", Some(3.5), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Initial: corners 2+1=3
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            Some(2),
            Some(1),
            "1st half",
            Some(false),
        );
        assert!(intents.is_empty());

        // Same corners again: no change -> no fire
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            Some(2),
            Some(1),
            "1st half_",
            Some(false),
        );
        assert!(intents.is_empty());
    }

    // =================================================================
    // Halftime Result tests
    // =================================================================

    #[test]
    fn test_halftime_home_winning() {
        let mut engine = NativeSoccerEngine::new();
        let t_home = target_json("tok_ht_home", "home_yes", "g1:HT:HOME_YES");
        let t_away_no = target_json("tok_ht_away_no", "away_no", "g1:HT:AWAY_NO");
        let t_draw_no = target_json("tok_ht_draw_no", "draw_no", "g1:HT:DRAW_NO");
        let m = market_json(
            "soccer_halftime_result",
            None,
            &[t_home, t_away_no, t_draw_no],
        );
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Score 1-0 at halftime
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            None,
            None,
            "Halftime",
            Some(false),
        );
        assert_eq!(intents.len(), 3);
        let fired: Vec<u16> = intents.iter().map(|i| i.target_idx.0).collect();
        assert!(fired.contains(&0)); // home_yes
        assert!(fired.contains(&1)); // away_no
        assert!(fired.contains(&2)); // draw_no
    }

    #[test]
    fn test_halftime_away_winning() {
        let mut engine = NativeSoccerEngine::new();
        let t_away = target_json("tok_ht_away", "away_yes", "g1:HT:AWAY_YES");
        let t_home_no = target_json("tok_ht_home_no", "home_no", "g1:HT:HOME_NO");
        let t_draw_no = target_json("tok_ht_draw_no", "draw_no", "g1:HT:DRAW_NO");
        let m = market_json(
            "soccer_halftime_result",
            None,
            &[t_away, t_home_no, t_draw_no],
        );
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Score 0-2 at halftime
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(2),
            None,
            None,
            "Halftime",
            Some(false),
        );
        assert_eq!(intents.len(), 3);
        let fired: Vec<u16> = intents.iter().map(|i| i.target_idx.0).collect();
        assert!(fired.contains(&0)); // away_yes
        assert!(fired.contains(&1)); // home_no
        assert!(fired.contains(&2)); // draw_no
    }

    #[test]
    fn test_halftime_draw() {
        let mut engine = NativeSoccerEngine::new();
        let t_draw = target_json("tok_ht_draw", "draw_yes", "g1:HT:DRAW_YES");
        let t_home_no = target_json("tok_ht_home_no", "home_no", "g1:HT:HOME_NO");
        let t_away_no = target_json("tok_ht_away_no", "away_no", "g1:HT:AWAY_NO");
        let m = market_json(
            "soccer_halftime_result",
            None,
            &[t_draw, t_home_no, t_away_no],
        );
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Score 1-1 at halftime
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(1),
            None,
            None,
            "Halftime",
            Some(false),
        );
        assert_eq!(intents.len(), 3);
        let fired: Vec<u16> = intents.iter().map(|i| i.target_idx.0).collect();
        assert!(fired.contains(&0)); // draw_yes
        assert!(fired.contains(&1)); // home_no
        assert!(fired.contains(&2)); // away_no
    }

    #[test]
    fn test_halftime_fires_once() {
        let mut engine = NativeSoccerEngine::new();
        let t_draw = target_json("tok_ht_draw", "draw_yes", "g1:HT:DRAW_YES");
        let m = market_json("soccer_halftime_result", None, &[t_draw]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // First halftime tick -> fires
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            None,
            None,
            "Halftime",
            Some(false),
        );
        assert_eq!(intents.len(), 1);

        // Second halftime tick -> no fire (already resolved)
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            None,
            None,
            "Halftime",
            Some(false),
        );
        assert!(intents.is_empty());
    }

    #[test]
    fn test_halftime_not_fired_in_second_half() {
        let mut engine = NativeSoccerEngine::new();
        let t_home = target_json("tok_ht_home", "home_yes", "g1:HT:HOME_YES");
        let m = market_json("soccer_halftime_result", None, &[t_home]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // If we only see "2nd half" (missed halftime), don't fire
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            None,
            None,
            "2nd half",
            Some(false),
        );
        assert!(intents.is_empty());
    }

    // =================================================================
    // Exact Score tests
    // =================================================================

    #[test]
    fn test_exact_score_match() {
        let mut engine = NativeSoccerEngine::new();
        // line 1.3 encodes predicted home=1, away=3
        let t1 = target_json("tok_exact_1_3", "yes", "g1:EXACT:1:3");
        let m = market_json("soccer_exact_score", Some(1.3), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Game ends 1-3 -> fires
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(3),
            None,
            None,
            "Ended",
            Some(true),
        );
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_exact_score_no_match() {
        let mut engine = NativeSoccerEngine::new();
        // line 2.1 encodes predicted home=2, away=1
        let t1 = target_json("tok_exact_2_1", "yes", "g1:EXACT:2:1");
        let m = market_json("soccer_exact_score", Some(2.1), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Game ends 1-3 -> doesn't fire (predicted was 2-1)
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(3),
            None,
            None,
            "Ended",
            Some(true),
        );
        assert!(intents.is_empty());
    }

    #[test]
    fn test_exact_score_zero_zero() {
        let mut engine = NativeSoccerEngine::new();
        // line 0.0 encodes predicted home=0, away=0
        let t1 = target_json("tok_exact_0_0", "yes", "g1:EXACT:0:0");
        let m = market_json("soccer_exact_score", Some(0.0), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Game ends 0-0 -> fires
        let intents = tick(
            &mut engine,
            "game1",
            Some(0),
            Some(0),
            None,
            None,
            "Ended",
            Some(true),
        );
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0));
    }

    #[test]
    fn test_exact_score_fires_once() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_exact_1_0", "yes", "g1:EXACT:1:0");
        let m = market_json("soccer_exact_score", Some(1.0), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // First game-end tick -> fires
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            None,
            None,
            "Ended",
            Some(true),
        );
        assert_eq!(intents.len(), 1);

        // Repeated game-end tick -> no double fire
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            None,
            None,
            "Ended",
            Some(true),
        );
        assert!(intents.is_empty());
    }

    #[test]
    fn test_exact_score_multiple_targets() {
        let mut engine = NativeSoccerEngine::new();
        // Three exact score targets: 1-0, 2-1, 1-3
        let t1 = target_json("tok_exact_1_0", "yes", "g1:EXACT:1:0");
        let t2 = target_json("tok_exact_2_1", "yes", "g1:EXACT:2:1");
        let t3 = target_json("tok_exact_1_3", "yes", "g1:EXACT:1:3");
        let m1 = market_json("soccer_exact_score", Some(1.0), &[t1]);
        let m2 = market_json("soccer_exact_score", Some(2.1), &[t2]);
        let m3 = market_json("soccer_exact_score", Some(1.3), &[t3]);
        let plan = plan_json_one_game("game1", &format!("{},{},{}", m1, m2, m3));
        engine.load_plan_from_json(&plan).unwrap();

        // Game ends 2-1 -> only target for 2-1 fires
        let intents = tick(
            &mut engine,
            "game1",
            Some(2),
            Some(1),
            None,
            None,
            "Ended",
            Some(true),
        );
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(1)); // t2 is the second target
    }

    #[test]
    fn test_exact_score_not_fired_before_game_end() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_exact_1_0", "yes", "g1:EXACT:1:0");
        let m = market_json("soccer_exact_score", Some(1.0), &[t1]);
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        // Score is 1-0 but game not ended -> doesn't fire
        let intents = tick(
            &mut engine,
            "game1",
            Some(1),
            Some(0),
            None,
            None,
            "2nd half",
            Some(false),
        );
        assert!(intents.is_empty());
    }

    #[test]
    fn merge_plan_adds_new_game() {
        let mut engine = NativeSoccerEngine::new();
        let initial = plan_json_one_game(
            "game_1",
            &format!(
                r#"{{"sports_market_type":"btts","line":null,"targets":[{},{}]}}"#,
                target_json("t1", "yes", "g1:BTTS:YES"),
                target_json("t2", "no", "g1:BTTS:NO"),
            ),
        );
        engine.load_plan_from_json(&initial).unwrap();
        assert_eq!(engine.game_ids.len(), 1);

        let patch = plan_json_one_game(
            "game_2",
            &format!(
                r#"{{"sports_market_type":"btts","line":null,"targets":[{},{}]}}"#,
                target_json("t3", "yes", "g2:BTTS:YES"),
                target_json("t4", "no", "g2:BTTS:NO"),
            ),
        );
        let result = engine.merge_plan(&patch).unwrap();
        assert_eq!(result.new_games, 1);
        assert_eq!(result.new_targets, 2);
        assert_eq!(result.new_tokens, 2);
        assert_eq!(engine.game_ids.len(), 2);
        assert!(engine.game_id_to_idx.contains_key("game_2"));
        assert!(engine.has_btts[1]);
    }

    #[test]
    fn merge_plan_mix_existing_and_new_games() {
        let mut engine = NativeSoccerEngine::new();
        let initial = plan_json_one_game(
            "game_1",
            &format!(
                r#"{{"sports_market_type":"btts","line":null,"targets":[{},{}]}}"#,
                target_json("t1", "yes", "g1:BTTS:YES"),
                target_json("t2", "no", "g1:BTTS:NO"),
            ),
        );
        engine.load_plan_from_json(&initial).unwrap();

        let patch = format!(
            r#"{{"games":[{{"provider_game_id":"game_1","kickoff_ts_utc":null,"markets":[{{"sports_market_type":"totals","line":5.5,"targets":[{},{}]}}]}},{{"provider_game_id":"game_new","kickoff_ts_utc":1700000000,"markets":[{{"sports_market_type":"moneyline","line":null,"targets":[{},{},{}]}}]}}]}}"#,
            target_json("t5", "over", "g1:TOTAL:OVER:5.5"),
            target_json("t6", "under", "g1:TOTAL:UNDER:5.5"),
            target_json("t7", "home_yes", "gn:MONEYLINE:HOME_YES"),
            target_json("t8", "away_yes", "gn:MONEYLINE:AWAY_YES"),
            target_json("t9", "draw_yes", "gn:MONEYLINE:DRAW_YES"),
        );
        let result = engine.merge_plan(&patch).unwrap();
        assert_eq!(result.new_games, 1);
        assert_eq!(result.new_targets, 5);
        assert_eq!(engine.game_ids.len(), 2);
        assert!(engine.has_totals[0]);
        assert!(engine.has_moneyline[1]);
    }

    #[test]
    fn test_alternate_game_ids_resolve_to_same_gidx() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_over", "over", "g1:TOTAL:OVER:2.5");
        let m = market_json("totals", Some(2.5), &[t1]);
        // Plan with alternate provider game IDs
        let plan = format!(
            r#"{{"games":[{{"provider_game_id":"primary_id","kickoff_ts_utc":1700000000,"alternate_provider_game_ids":[{{"provider":"boltodds","game_id":"boltodds_label"}},{{"provider":"kalstrop_v1","game_id":"v1_uuid"}}],"markets":[{}]}}]}}"#,
            m
        );
        engine.load_plan_from_json(&plan).unwrap();

        // One game in game_ids
        assert_eq!(engine.game_ids.len(), 1);
        assert_eq!(engine.game_ids[0], "primary_id");

        // All three IDs resolve to the same GameIdx
        let primary = engine.game_id_to_idx.get("primary_id").copied();
        let alt_bo = engine.game_id_to_idx.get("boltodds_label").copied();
        let alt_v1 = engine.game_id_to_idx.get("v1_uuid").copied();
        assert_eq!(primary, Some(GameIdx(0)));
        assert_eq!(alt_bo, Some(GameIdx(0)));
        assert_eq!(alt_v1, Some(GameIdx(0)));

        // Total entries in the map: 3 (primary + 2 alternates)
        assert_eq!(engine.game_id_to_idx.len(), 3);
    }

    #[test]
    fn test_alternate_game_ids_no_alternates_field() {
        let mut engine = NativeSoccerEngine::new();
        let t1 = target_json("tok_over", "over", "g1:TOTAL:OVER:2.5");
        let m = market_json("totals", Some(2.5), &[t1]);
        // Plan without alternate_provider_game_ids — backward compat
        let plan = plan_json_one_game("game1", &m);
        engine.load_plan_from_json(&plan).unwrap();

        assert_eq!(engine.game_ids.len(), 1);
        assert_eq!(engine.game_id_to_idx.len(), 1);
    }
}
