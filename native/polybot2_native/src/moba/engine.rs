use crate::moba::types::*;
use crate::*;
use std::collections::HashSet;
use std::sync::Arc;

fn norm(s: &str) -> String {
    s.trim().to_lowercase()
}

fn canonical_moba_market_type(raw: &str) -> String {
    let s = raw.trim().to_lowercase().replace('-', "_");
    match s.as_str() {
        "moneyline" => "moneyline".to_string(),
        "child_moneyline" => "child_moneyline".to_string(),
        "totals" | "total" => "totals".to_string(),
        "map_handicap" => "map_handicap".to_string(),
        other => other.to_string(),
    }
}

impl NativeMobaEngine {
    pub(crate) fn new() -> Self {
        Self {
            game_id_to_idx: Default::default(),
            game_ids: Vec::new(),
            game_leagues: Vec::new(),
            game_targets: Vec::new(),
            target_slots: Vec::new(),
            tokens: Vec::new(),
            token_id_to_idx: Default::default(),
            strategy_keys: HashSet::new(),
            registry: None,
            kickoff_ts: Vec::new(),
            token_ids_by_game: Vec::new(),
            has_moneyline: Vec::new(),
            has_totals: Vec::new(),
            has_child_moneyline: Vec::new(),
            has_map_handicap: Vec::new(),
            rows: Vec::new(),
            game_states: Vec::new(),
            maps_to_win: Vec::new(),
            final_resolved_games: Vec::new(),
            totals_under_emitted: Vec::new(),
            map_handicap_early_emitted: Vec::new(),
            map_winner_resolved: Vec::new(),
        }
    }

    pub(crate) fn load_plan_from_json(&mut self, plan_json: &str) -> Result<(), String> {
        self.game_id_to_idx.clear();
        self.game_ids.clear();
        self.game_leagues.clear();
        self.game_targets.clear();
        self.target_slots.clear();
        self.tokens.clear();
        self.token_id_to_idx.clear();
        self.strategy_keys.clear();
        self.registry = None;
        self.kickoff_ts.clear();
        self.token_ids_by_game.clear();
        self.maps_to_win.clear();
        self.has_moneyline.clear();
        self.has_totals.clear();
        self.has_child_moneyline.clear();
        self.has_map_handicap.clear();

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
            let league_str = game_val
                .get("canonical_league")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            self.game_leagues.push(Arc::from(league_str));

            if let Some(alts) = game_val
                .get("alternate_provider_game_ids")
                .and_then(|v| v.as_array())
            {
                for alt in alts {
                    let alt_id = alt
                        .get("game_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim();
                    if !alt_id.is_empty() && !self.game_id_to_idx.contains_key(alt_id) {
                        self.game_id_to_idx.insert(alt_id.to_string(), gidx);
                    }
                }
            }

            let kickoff = game_val.get("kickoff_ts_utc").and_then(|v| v.as_i64());
            self.kickoff_ts.push(kickoff);
            let mtw = game_val
                .get("sets_to_win")
                .and_then(|v| v.as_i64())
                .unwrap_or(2);
            self.maps_to_win.push(mtw);

            let markets = match game_val.get("markets").and_then(|v| v.as_array()) {
                Some(m) => m,
                None => {
                    self.game_targets.push(MobaGameTargets::default());
                    self.has_moneyline.push(false);
                    self.has_totals.push(false);
                    self.has_child_moneyline.push(false);
                    self.has_map_handicap.push(false);
                    self.token_ids_by_game.push(Vec::new());
                    continue;
                }
            };

            let mut game_tgt = MobaGameTargets::default();
            let mut game_has_moneyline = false;
            let mut game_has_totals = false;
            let mut game_has_child_moneyline = false;
            let mut game_has_map_handicap = false;
            let mut token_ids: HashSet<String> = HashSet::new();
            let game_id_ref = self.game_ids[gidx.0 as usize].as_str();

            for market_val in markets {
                let sports_market_type = canonical_moba_market_type(
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
                    let effective_line =
                        target_val.get("line").and_then(|v| v.as_f64()).or(line);
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
                        "moneyline" => {
                            game_has_moneyline = true;
                            match semantic.as_str() {
                                "home" => game_tgt.moneyline_home = Some(tidx),
                                "away" => game_tgt.moneyline_away = Some(tidx),
                                other => {
                                    eprintln!(
                                        "[moba] WARN: unhandled moneyline semantic '{}' for game {}",
                                        other, game_id_ref
                                    );
                                }
                            }
                        }
                        "child_moneyline" => {
                            game_has_child_moneyline = true;
                            let map_num = parse_map_number_from_strategy_key(&strategy_key);
                            if map_num > 0 {
                                let idx = (map_num - 1) as usize;
                                while game_tgt.map_moneyline.len() <= idx {
                                    game_tgt.map_moneyline.push((None, None));
                                }
                                match semantic.as_str() {
                                    "home" => game_tgt.map_moneyline[idx].0 = Some(tidx),
                                    "away" => game_tgt.map_moneyline[idx].1 = Some(tidx),
                                    other => {
                                        eprintln!(
                                            "[moba] WARN: unhandled child_moneyline semantic '{}' for game {}",
                                            other, game_id_ref
                                        );
                                    }
                                }
                            } else {
                                eprintln!(
                                    "[moba] WARN: could not parse map number from strategy_key '{}' for game {}",
                                    strategy_key, game_id_ref
                                );
                            }
                        }
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
                                        eprintln!(
                                            "[moba] WARN: unhandled totals semantic '{}' for game {}",
                                            other, game_id_ref
                                        );
                                    }
                                }
                            }
                        }
                        "map_handicap" => {
                            game_has_map_handicap = true;
                            if let Some(l) = effective_line {
                                let side = match semantic.as_str() {
                                    "home_covers" | "home_not_covers" => SpreadSide::Home,
                                    "away_covers" | "away_not_covers" => SpreadSide::Away,
                                    other => {
                                        eprintln!(
                                            "[moba] WARN: unhandled map_handicap semantic '{}' for game {}",
                                            other, game_id_ref
                                        );
                                        continue;
                                    }
                                };
                                let slot = game_tgt
                                    .map_handicaps
                                    .iter_mut()
                                    .find(|s| s.side == side && (s.line - l).abs() < 1e-9);
                                if let Some(slot) = slot {
                                    match semantic.as_str() {
                                        "home_covers" | "away_covers" => {
                                            slot.covers_idx = Some(tidx)
                                        }
                                        _ => slot.not_covers_idx = Some(tidx),
                                    }
                                } else {
                                    let (covers, not_covers) = match semantic.as_str() {
                                        "home_covers" | "away_covers" => (Some(tidx), None),
                                        _ => (None, Some(tidx)),
                                    };
                                    game_tgt.map_handicaps.push(SpreadSlot {
                                        side,
                                        line: l,
                                        covers_idx: covers,
                                        not_covers_idx: not_covers,
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Sort over/under lines for efficient crossing detection
            game_tgt.over_lines.sort_by_key(|ol| ol.half_int);
            game_tgt.under_lines.sort_by_key(|ol| ol.half_int);

            self.game_targets.push(game_tgt);
            self.has_moneyline.push(game_has_moneyline);
            self.has_totals.push(game_has_totals);
            self.has_child_moneyline.push(game_has_child_moneyline);
            self.has_map_handicap.push(game_has_map_handicap);
            self.token_ids_by_game
                .push(token_ids.into_iter().collect());
        }

        // Initialize state vectors
        let n = self.game_ids.len();
        self.rows.resize(n, None);
        self.game_states.resize(n, MobaGameState::default());
        self.final_resolved_games.resize(n, false);
        self.totals_under_emitted.resize(n, false);
        self.map_handicap_early_emitted.resize(n, false);
        self.map_winner_resolved.resize(n, Vec::new());
        for gi in 0..n {
            let max_maps = (self.maps_to_win[gi] * 2 - 1).max(1) as usize;
            self.map_winner_resolved[gi].resize(max_maps, false);
        }

        // Build TargetRegistry
        let mut reg_targets: Vec<TargetSlot> = Vec::with_capacity(self.target_slots.len());
        for ts in &self.target_slots {
            reg_targets.push(ts.clone());
        }
        let mut reg_tokens: Vec<TokenSlot> = Vec::with_capacity(self.tokens.len());
        for ts in &self.tokens {
            reg_tokens.push(ts.clone());
        }
        self.registry = Some(Arc::new(TargetRegistry {
            targets: reg_targets,
            tokens: reg_tokens,
        }));

        Ok(())
    }

    pub(crate) fn reset_runtime_state(&mut self) {
        let n = self.game_ids.len();
        self.rows = vec![None; n];
        self.game_states = vec![MobaGameState::default(); n];
        self.final_resolved_games = vec![false; n];
        self.totals_under_emitted = vec![false; n];
        self.map_handicap_early_emitted = vec![false; n];
        for gi in 0..n {
            let max_maps = (self.maps_to_win[gi] * 2 - 1).max(1) as usize;
            self.map_winner_resolved[gi] = vec![false; max_maps];
        }
    }

    pub(crate) fn check_duplicate(
        &mut self,
        game_label: &str,
        maps_home_raw: &str,
        maps_away_raw: &str,
    ) -> Option<GameIdx> {
        let gidx = *self.game_id_to_idx.get(game_label)?;
        let gi = gidx.0 as usize;
        let new_row = MobaStateRow {
            maps_home_raw: InlineStr::from_str(maps_home_raw),
            maps_away_raw: InlineStr::from_str(maps_away_raw),
        };
        if let Some(existing) = &self.rows[gi] {
            if existing.maps_home_raw.as_str() == maps_home_raw
                && existing.maps_away_raw.as_str() == maps_away_raw
            {
                return None; // deduped
            }
        }
        self.rows[gi] = Some(new_row);
        Some(gidx)
    }

    pub(crate) fn process_tick_live(
        &mut self,
        gidx: GameIdx,
        maps_home: i64,
        maps_away: i64,
        match_completed: bool,
        game_state: &'static str,
        _recv_monotonic_ns: i64,
    ) -> Option<MobaLiveTickResult> {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return None;
        }

        let prev = &self.game_states[gi];
        let total_maps = maps_home + maps_away;

        // Detect first observation: default MobaGameState has maps_home = None.
        let is_first_observation = prev.maps_home.is_none();

        let state = MobaGameState {
            maps_home: Some(maps_home),
            maps_away: Some(maps_away),
            prev_maps_home: if is_first_observation { None } else { prev.maps_home },
            prev_maps_away: if is_first_observation { None } else { prev.maps_away },
            total_maps,
            prev_total_maps: if is_first_observation { None } else { Some(prev.total_maps) },
            match_completed,
            game_state,
        };
        self.game_states[gi] = state;

        // Cold-start protection: tombstone already-completed events.
        if is_first_observation {
            if match_completed {
                self.final_resolved_games[gi] = true;
                self.totals_under_emitted[gi] = true;
                return None;
            }
            let maps_done = (maps_home + maps_away) as usize;
            for mi in 0..maps_done.min(self.map_winner_resolved[gi].len()) {
                self.map_winner_resolved[gi][mi] = true;
            }
        }

        let mut intents = smallvec::SmallVec::<[Intent; 32]>::new();
        self.evaluate_child_moneyline_into(gidx, &state, &mut intents);
        self.evaluate_moneyline_into(gidx, &state, &mut intents);
        self.evaluate_totals_into(gidx, &state, &mut intents);
        self.evaluate_map_handicap_into(gidx, &state, &mut intents);

        // Mark match resolved AFTER all evaluators have run.
        let mtw = self.maps_to_win[gi];
        if match_completed || maps_home >= mtw || maps_away >= mtw {
            self.final_resolved_games[gi] = true;
        }

        Some(MobaLiveTickResult {
            game_idx: gidx,
            state,
            intents,
        })
    }

    pub(crate) fn is_game_completed(&self, game_label: &str) -> bool {
        if let Some(&gidx) = self.game_id_to_idx.get(game_label) {
            self.final_resolved_games[gidx.0 as usize]
        } else {
            false
        }
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

    pub(crate) fn merge_plan(&mut self, _plan_json: &str) -> Result<crate::MergePlanResult, String> {
        Err("moba_merge_plan_not_implemented".to_string())
    }

    pub(crate) fn all_token_ids(&self) -> Vec<String> {
        self.token_ids_by_game.iter().flatten().cloned().collect()
    }

    pub(crate) fn clone_registry(&self) -> Option<Arc<crate::TargetRegistry>> {
        self.registry.clone()
    }
}

/// Parse map number from strategy key: "...:CHILD_MONEYLINE:MAP1:HOME" → 1
fn parse_map_number_from_strategy_key(key: &str) -> i64 {
    if let Some(pos) = key.find(":MAP") {
        let rest = &key[pos + 4..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(n) = digits.parse::<i64>() {
            return n;
        }
    }
    0
}
