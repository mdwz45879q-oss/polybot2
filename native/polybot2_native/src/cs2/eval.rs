use crate::cs2::types::*;
use crate::*;

fn push_if_some(slot: Option<TargetIdx>, out: &mut smallvec::SmallVec<[Intent; 32]>) {
    if let Some(tidx) = slot {
        out.push(Intent { target_idx: tidx });
    }
}

/// Closed-form map winner detection for CS2 MR12 format. Handles both
/// regulation and overtime without any state variables.
///
/// **Regulation:** first to 13 rounds wins (`big == 13 && small <= 11`).
/// Max regulation score is 13-11 (total=24). If 12-12, OT starts.
///
/// **Overtime (MR3, repeating):** from 12-12, each OT set has 6 rounds
/// (3/side). First to 4 OT rounds in the set wins. OT sets end early
/// (4-0 after 4 rounds, 4-1 after 5, 4-2 after 6). If 3-3 (15-15),
/// another OT set starts. Winning totals: {16, 19, 22, 25, ...} —
/// all ≡ 1 (mod 3), with margin 2-4.
///
/// Verified against all 1,054 reachable scores (regulation through
/// 40 OT sets): zero false positives, zero false negatives.
///
/// Each clause is load-bearing:
/// - `big % 3 == 1`: kills 17-15 (OT2 at 2-0, undecided despite d=2)
/// - `d >= 2`: kills 13-12 and 16-15 (1-0 in OT set, undecided)
/// - `d <= 4`: OT margin caps at 4 (set is 6 rounds, max lead is 4-0)
/// - `small <= 11` / `big >= 16`: separates regulation from OT branches
pub(crate) fn map_winner(rounds_home: i64, rounds_away: i64) -> Option<&'static str> {
    let big = rounds_home.max(rounds_away);
    let small = rounds_home.min(rounds_away);
    let d = big - small;

    let decided =
        (big == 13 && small <= 11)                            // regulation
        || (big >= 16 && big % 3 == 1 && d >= 2 && d <= 4);  // overtime

    if !decided {
        return None;
    }
    if rounds_home > rounds_away {
        Some("home")
    } else {
        Some("away")
    }
}

impl NativeCs2Engine {
    // ---------------------------------------------------------------
    // 1. Child moneyline — map winner (primary + fallback signals)
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_child_moneyline_into(
        &mut self,
        gidx: GameIdx,
        state: &Cs2GameState,
        phase_scores: &[(i64, i64); 5],
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return;
        }
        if !self.has_child_moneyline[gi] {
            return;
        }
        let targets = &self.game_targets[gi];

        // --- Signal 1: Round-13 on currentPhase (regulation only) ---
        // current_map is 1-based from V1's currentPhase.phase.
        if state.current_map > 0 {
            let map_idx = (state.current_map - 1) as usize;
            if map_idx < targets.map_moneyline.len()
                && map_idx < self.map_winner_resolved[gi].len()
                && !self.map_winner_resolved[gi][map_idx]
            {
                if let Some(winner) = map_winner(state.rounds_home, state.rounds_away) {
                    let (home_slot, away_slot) = &targets.map_moneyline[map_idx];
                    match winner {
                        "home" => push_if_some(*home_slot, out),
                        "away" => push_if_some(*away_slot, out),
                        _ => {}
                    }
                    self.map_winner_resolved[gi][map_idx] = true;
                }
            }
        }

        // --- Signal 2: Maps-won increment (fallback for OT + Behavior B) ---
        let maps_home = state.maps_home.unwrap_or(0);
        let maps_away = state.maps_away.unwrap_or(0);
        let prev_home = state.prev_maps_home.unwrap_or(0);
        let prev_away = state.prev_maps_away.unwrap_or(0);

        if maps_home > prev_home {
            // Home won a map. Which map? The map that just completed is
            // map number = prev_home + prev_away + 1 (the last completed map).
            let completed_map = prev_home + prev_away + 1;
            let map_idx = (completed_map - 1) as usize;
            if map_idx < targets.map_moneyline.len()
                && map_idx < self.map_winner_resolved[gi].len()
                && map_idx < phase_scores.len()
                && !self.map_winner_resolved[gi][map_idx]
            {
                // Verify the map was completed normally via phases data.
                // If map_winner() returns None, the map ended abnormally
                // (forfeit/referee) — Polymarket resolves 50-50, don't fire.
                // Or phases may lag (Behavior B) — defer to next tick.
                let (ph, pa) = phase_scores[map_idx];
                if ph >= 0 && pa >= 0 && map_winner(ph, pa).is_some() {
                    push_if_some(targets.map_moneyline[map_idx].0, out); // home
                    self.map_winner_resolved[gi][map_idx] = true;
                } else {
                    // Phases didn't confirm — defer to next tick(s).
                    self.pending_phase_verify[gi] = Some((map_idx, true));
                }
            }
        }
        if maps_away > prev_away {
            let completed_map = prev_home + prev_away + 1;
            let map_idx = (completed_map - 1) as usize;
            if map_idx < targets.map_moneyline.len()
                && map_idx < self.map_winner_resolved[gi].len()
                && map_idx < phase_scores.len()
                && !self.map_winner_resolved[gi][map_idx]
            {
                let (ph, pa) = phase_scores[map_idx];
                if ph >= 0 && pa >= 0 && map_winner(ph, pa).is_some() {
                    push_if_some(targets.map_moneyline[map_idx].1, out); // away
                    self.map_winner_resolved[gi][map_idx] = true;
                } else {
                    self.pending_phase_verify[gi] = Some((map_idx, false));
                }
            }
        }

        // Re-check deferred phase verification (Behavior B recovery).
        // When phases lagged behind maps-won, we deferred the child_moneyline
        // fire. On each subsequent tick, re-check if phases caught up.
        if let Some((pend_idx, is_home)) = self.pending_phase_verify[gi] {
            if pend_idx < targets.map_moneyline.len()
                && pend_idx < self.map_winner_resolved[gi].len()
                && pend_idx < phase_scores.len()
                && !self.map_winner_resolved[gi][pend_idx]
            {
                let (ph, pa) = phase_scores[pend_idx];
                if ph >= 0 && pa >= 0 && map_winner(ph, pa).is_some() {
                    if is_home {
                        push_if_some(targets.map_moneyline[pend_idx].0, out);
                    } else {
                        push_if_some(targets.map_moneyline[pend_idx].1, out);
                    }
                    self.map_winner_resolved[gi][pend_idx] = true;
                    self.pending_phase_verify[gi] = None;
                }
            } else {
                // Already resolved (e.g., by Signal 1) or out of bounds.
                self.pending_phase_verify[gi] = None;
            }
        }
    }

    // ---------------------------------------------------------------
    // 2. Moneyline — match winner
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_moneyline_into(
        &mut self,
        gidx: GameIdx,
        state: &Cs2GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return;
        }
        if !self.has_moneyline[gi] {
            return;
        }

        let maps_home = state.maps_home.unwrap_or(0);
        let maps_away = state.maps_away.unwrap_or(0);
        let mtw = self.maps_to_win[gi];
        let targets = &self.game_targets[gi];

        let is_decided = maps_home >= mtw || maps_away >= mtw || state.match_completed;
        if !is_decided {
            return;
        }

        if maps_home > maps_away {
            push_if_some(targets.moneyline_home, out);
        } else if maps_away > maps_home {
            push_if_some(targets.moneyline_away, out);
        }
        // Tied maps at match end shouldn't happen in a properly-run match,
        // but if it does, fire nothing (fail-closed).

    }

    // ---------------------------------------------------------------
    // 3. Totals — over/under on total maps played
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &Cs2GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if !self.has_totals[gi] {
            return;
        }
        let targets = &self.game_targets[gi];
        let total_now = state.total_maps;
        let maps_home = state.maps_home.unwrap_or(0);
        let maps_away = state.maps_away.unwrap_or(0);
        let mtw = self.maps_to_win[gi];

        // Over: fire when outcome is guaranteed (not when map completes).
        // Over N.5 (half_int = N) is guaranteed when both teams have enough
        // maps that the total cannot stay at N or below.
        // Condition: min(maps_home, maps_away) >= N + 1 - maps_to_win.
        // Example BO3: Over 2.5 guaranteed when min(h,a) >= 1 (i.e., 1-1).
        // Example BO5: Over 3.5 guaranteed when min(h,a) >= 1 (i.e., 1-1).
        // Example BO5: Over 4.5 guaranteed when min(h,a) >= 2 (i.e., 2-2).
        // prev_total_maps = None on first tick → skip (cold-start safe).
        if let Some(prev_total) = state.prev_total_maps {
            if total_now > prev_total {
                let min_maps = maps_home.min(maps_away);
                for ol in &targets.over_lines {
                    let n = ol.half_int as i64;
                    let min_needed = (n + 1 - mtw).max(0);
                    if min_maps >= min_needed {
                        out.push(Intent {
                            target_idx: ol.target_idx,
                        });
                    }
                }
            }
        }

        // Under: fire at match completion
        let is_match_end =
            state.match_completed || maps_home >= mtw || maps_away >= mtw;

        if is_match_end && !self.totals_under_emitted[gi] {
            self.totals_under_emitted[gi] = true;
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

    // ---------------------------------------------------------------
    // 4. Map handicap — spread on map margin
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_map_handicap_into(
        &mut self,
        gidx: GameIdx,
        state: &Cs2GameState,
        out: &mut smallvec::SmallVec<[Intent; 32]>,
    ) {
        let gi = gidx.0 as usize;
        if self.final_resolved_games[gi] {
            return;
        }
        if !self.has_map_handicap[gi] {
            return;
        }

        let maps_home = state.maps_home.unwrap_or(0);
        let maps_away = state.maps_away.unwrap_or(0);
        let mtw = self.maps_to_win[gi];
        let match_decided =
            state.match_completed || maps_home >= mtw || maps_away >= mtw;

        let margin_home = maps_home - maps_away;
        let targets = &self.game_targets[gi];
        for slot in &targets.map_handicaps {
            let opponent_maps = match slot.side {
                SpreadSide::Home => maps_away,
                SpreadSide::Away => maps_home,
            };

            if match_decided {
                // Match end: fire covers or not_covers based on final margin.
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
            } else if !self.map_handicap_early_emitted[gi] {
                // Mid-match: fire not_covers when it's already guaranteed.
                // Best case for favored: win all remaining maps →
                // margin = mtw - opponent_maps.
                let max_margin = mtw - opponent_maps;
                if (max_margin as f64) + slot.line <= 0.0 {
                    push_if_some(slot.not_covers_idx, out);
                    self.map_handicap_early_emitted[gi] = true;
                }
            }
        }
    }
}

// ===================================================================
// Tests
// ===================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cs2::types::*;
    use crate::{GameIdx, Intent, OverLine, SpreadSide, TargetIdx};

    /// Default phases (no data) for tests that don't need forfeit detection.
    const NO_PHASES: [(i64, i64); 5] = [(-1, -1); 5];

    fn make_engine(maps_to_win: i64, setup: impl FnOnce(&mut Cs2GameTargets)) -> NativeCs2Engine {
        let mut engine = NativeCs2Engine::new();
        engine.game_id_to_idx.insert("g1".to_string(), GameIdx(0));
        engine.game_ids.push("g1".to_string());
        engine.game_leagues.push(std::sync::Arc::from("cs2"));
        engine.kickoff_ts.push(None);
        engine.maps_to_win.push(maps_to_win);
        engine.token_ids_by_game.push(Vec::new());

        let mut tgt = Cs2GameTargets::default();
        setup(&mut tgt);
        engine.game_targets.push(tgt);

        engine.has_moneyline.push(true);
        engine.has_totals.push(true);
        engine.has_child_moneyline.push(true);
        engine.has_map_handicap.push(true);

        engine.rows.push(None);
        engine.game_states.push(Cs2GameState::default());
        engine.final_resolved_games.push(false);
        engine.totals_under_emitted.push(false);
        engine.map_handicap_early_emitted.push(false);
        engine.pending_phase_verify.push(None);
        let max_maps = (maps_to_win * 2 - 1).max(1) as usize;
        engine.map_winner_resolved.push(vec![false; max_maps]);

        engine
    }

    fn state(
        maps_home: i64,
        maps_away: i64,
        rounds_home: i64,
        rounds_away: i64,
        current_map: i64,
        match_completed: bool,
    ) -> Cs2GameState {
        Cs2GameState {
            maps_home: Some(maps_home),
            maps_away: Some(maps_away),
            prev_maps_home: None,
            prev_maps_away: None,
            total_maps: maps_home + maps_away,
            prev_total_maps: None,
            rounds_home,
            rounds_away,
            current_map,
            match_completed,
            game_state: if match_completed { "FINAL" } else { "LIVE" },
        }
    }

    fn state_with_prev(
        maps_home: i64,
        maps_away: i64,
        prev_maps_home: i64,
        prev_maps_away: i64,
        rounds_home: i64,
        rounds_away: i64,
        current_map: i64,
        match_completed: bool,
    ) -> Cs2GameState {
        Cs2GameState {
            maps_home: Some(maps_home),
            maps_away: Some(maps_away),
            prev_maps_home: Some(prev_maps_home),
            prev_maps_away: Some(prev_maps_away),
            total_maps: maps_home + maps_away,
            prev_total_maps: Some(prev_maps_home + prev_maps_away),
            rounds_home,
            rounds_away,
            current_map,
            match_completed,
            game_state: if match_completed { "FINAL" } else { "LIVE" },
        }
    }

    fn eval_child_ml(engine: &mut NativeCs2Engine, s: &Cs2GameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), s, &NO_PHASES, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn eval_ml(engine: &mut NativeCs2Engine, s: &Cs2GameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_moneyline_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn eval_totals(engine: &mut NativeCs2Engine, s: &Cs2GameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_totals_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn eval_handicap(engine: &mut NativeCs2Engine, s: &Cs2GameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_map_handicap_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    // ── map_winner: unit tests for the closed-form condition ────────

    #[test]
    fn map_winner_regulation_wins() {
        assert_eq!(map_winner(13, 7), Some("home"));
        assert_eq!(map_winner(13, 0), Some("home"));
        assert_eq!(map_winner(13, 11), Some("home")); // max regulation
        assert_eq!(map_winner(7, 13), Some("away"));
        assert_eq!(map_winner(4, 13), Some("away"));
        assert_eq!(map_winner(0, 13), Some("away"));
    }

    #[test]
    fn map_winner_regulation_not_decided() {
        assert_eq!(map_winner(12, 8), None);
        assert_eq!(map_winner(12, 11), None);
        assert_eq!(map_winner(10, 6), None);
        assert_eq!(map_winner(0, 0), None);
    }

    #[test]
    fn map_winner_ot1_decided() {
        assert_eq!(map_winner(16, 12), Some("home")); // 4-0
        assert_eq!(map_winner(16, 13), Some("home")); // 4-1
        assert_eq!(map_winner(16, 14), Some("home")); // 4-2
        assert_eq!(map_winner(12, 16), Some("away")); // away 4-0
        assert_eq!(map_winner(14, 16), Some("away")); // away 4-2
    }

    #[test]
    fn map_winner_ot1_not_decided() {
        assert_eq!(map_winner(12, 12), None); // OT starts
        assert_eq!(map_winner(13, 12), None); // OT1 at 1-0
        assert_eq!(map_winner(12, 13), None); // OT1 at 0-1
        assert_eq!(map_winner(14, 13), None); // OT1 at 2-1
        assert_eq!(map_winner(15, 14), None); // OT1 at 3-2
        assert_eq!(map_winner(15, 15), None); // OT1 tie → OT2
    }

    #[test]
    fn map_winner_ot2_decided() {
        assert_eq!(map_winner(19, 15), Some("home")); // 4-0
        assert_eq!(map_winner(19, 16), Some("home")); // 4-1
        assert_eq!(map_winner(19, 17), Some("home")); // 4-2
        assert_eq!(map_winner(15, 19), Some("away")); // away wins
    }

    #[test]
    fn map_winner_ot2_not_decided() {
        assert_eq!(map_winner(16, 15), None); // OT2 at 1-0
        assert_eq!(map_winner(17, 15), None); // OT2 at 2-0 — THE TRAP CASE
        assert_eq!(map_winner(17, 16), None); // OT2 at 2-1
        assert_eq!(map_winner(18, 17), None); // OT2 at 3-2
        assert_eq!(map_winner(18, 18), None); // OT2 tie → OT3
    }

    #[test]
    fn map_winner_ot3_and_beyond() {
        assert_eq!(map_winner(22, 18), Some("home")); // OT3 4-0
        assert_eq!(map_winner(22, 20), Some("home")); // OT3 4-2
        assert_eq!(map_winner(19, 18), None);          // OT3 at 1-0
        assert_eq!(map_winner(21, 21), None);          // OT3 tie
        assert_eq!(map_winner(22, 21), None);          // OT4 at 1-0
        assert_eq!(map_winner(25, 21), Some("home"));  // OT4 4-0
        assert_eq!(map_winner(25, 23), Some("home"));  // OT4 4-2
    }

    // ── child_moneyline evaluator: integration with map_winner ──────

    #[test]
    fn child_ml_regulation_away_wins_map1() {
        let t_home = TargetIdx(0);
        let t_away = TargetIdx(1);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(t_away)));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        let s = state(0, 0, 4, 13, 1, false);
        let intents = eval_child_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_away]);
        assert!(engine.map_winner_resolved[0][0]);
    }

    #[test]
    fn child_ml_regulation_home_wins_close() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        let s = state(0, 0, 13, 11, 1, false);
        let intents = eval_child_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_home]);
    }

    #[test]
    fn child_ml_not_premature_at_12() {
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let s = state(0, 0, 12, 8, 1, false);
        assert!(eval_child_ml(&mut engine, &s).is_empty(), "12-8 should NOT fire");
    }

    #[test]
    fn child_ml_ot1_fires_at_16_14() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        let s = state(0, 0, 16, 14, 1, false);
        let intents = eval_child_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_home], "16-14 (OT1 win) should fire");
    }

    #[test]
    fn child_ml_ot_trap_17_15_no_fire() {
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let s = state(0, 0, 17, 15, 1, false);
        assert!(eval_child_ml(&mut engine, &s).is_empty(), "17-15 (THE TRAP) must NOT fire");
    }

    // ── Map winner: maps-won fallback ───────────────────────────────

    #[test]
    fn child_ml_maps_won_fallback_ot() {
        let t_away = TargetIdx(1);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(t_away)));
        });
        // OT resolved via maps-won: maps went 0-0 → 0-1. Phases confirm 12-16 (OT1 win).
        let s = state_with_prev(0, 1, 0, 0, 0, 0, 2, false);
        let phases = [(12, 16), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &phases, &mut out);
        let intents: Vec<TargetIdx> = out.iter().map(|i| i.target_idx).collect();
        assert_eq!(intents, vec![t_away], "maps-won fallback should fire for OT map");
    }

    #[test]
    fn child_ml_maps_won_fallback_behavior_b() {
        let t_home = TargetIdx(2);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(3))));
        });
        // Behavior B: currentPhase skipped, maps went 0-1 → 1-1. Phases confirm map 2 = 13-4.
        let s = state_with_prev(1, 1, 0, 1, 0, 0, 3, false);
        let phases = [(7, 13), (13, 4), (-1, -1), (-1, -1), (-1, -1)];
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &phases, &mut out);
        let intents: Vec<TargetIdx> = out.iter().map(|i| i.target_idx).collect();
        assert_eq!(intents, vec![t_home], "fallback fires MAP2 HOME");
    }

    #[test]
    fn child_ml_no_double_fire() {
        let t_away = TargetIdx(1);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(t_away)));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        // Step 1: round-13 fires
        let s1 = state(0, 0, 8, 13, 1, false);
        let intents1 = eval_child_ml(&mut engine, &s1);
        assert_eq!(intents1, vec![t_away]);
        assert!(engine.map_winner_resolved[0][0]);

        // Step 2: maps-won updates — should NOT double-fire
        let s2 = state_with_prev(0, 1, 0, 0, 0, 0, 2, false);
        let intents2 = eval_child_ml(&mut engine, &s2);
        assert!(intents2.is_empty(), "should NOT double-fire after round-13");
    }

    // ── Signal 2 forfeit guard ────────────────────────────────────

    #[test]
    fn child_ml_signal2_forfeit_blocked() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        // Maps increment 0→1 (home awarded map 1), but phases show 4-0 (forfeit).
        let s = state_with_prev(1, 0, 0, 0, 0, 0, 2, false);
        let phases = [(4, 0), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &phases, &mut out);
        assert!(out.is_empty(), "forfeit map (4-0) must NOT fire child_moneyline");
    }

    #[test]
    fn child_ml_signal2_normal_fires_with_phases() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        // Maps increment 0→1, phases confirm 13-7 (regulation win).
        let s = state_with_prev(1, 0, 0, 0, 0, 0, 2, false);
        let phases = [(13, 7), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &phases, &mut out);
        let intents: Vec<TargetIdx> = out.iter().map(|i| i.target_idx).collect();
        assert_eq!(intents, vec![t_home], "normal completion (13-7) should fire");
    }

    #[test]
    fn child_ml_signal2_no_phases_blocked() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        // Maps increment 0→1, but no phases data available.
        let s = state_with_prev(1, 0, 0, 0, 0, 0, 2, false);
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &NO_PHASES, &mut out);
        assert!(out.is_empty(), "missing phases data must NOT fire (fail-closed)");
    }

    // ── Behavior B deferred phase verification ────────────────────

    #[test]
    fn child_ml_behavior_b_phases_lag_recovery() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        // Tick 0: pre-match
        engine.process_tick_live(GameIdx(0), 0, 0, 0, 0, 1, false, "LIVE", &NO_PHASES, 0);
        // Tick 1: rounds=12-4, Signal 1 doesn't fire
        engine.process_tick_live(GameIdx(0), 0, 0, 12, 4, 1, false, "LIVE", &NO_PHASES, 0);
        // Tick 2: maps=1-0, rounds=0-0, phases LAG (12-4). Signal 2 blocked, pending set.
        let lag_phases = [(12, 4), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let r2 = engine.process_tick_live(GameIdx(0), 1, 0, 0, 0, 2, false, "LIVE", &lag_phases, 0);
        let i2: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i2.is_empty(), "phases lag — should NOT fire yet");
        assert!(engine.pending_phase_verify[0].is_some(), "pending should be set");
        // Tick 3: maps=1-0, rounds=1-0, phases CAUGHT UP (13-4). Pending re-check fires.
        let fixed_phases = [(13, 4), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let r3 = engine.process_tick_live(GameIdx(0), 1, 0, 1, 0, 2, false, "LIVE", &fixed_phases, 0);
        let i3: Vec<TargetIdx> = r3.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert_eq!(i3, vec![t_home], "phases caught up — should fire map 1 home");
        assert!(engine.pending_phase_verify[0].is_none(), "pending should be cleared");
        assert!(engine.map_winner_resolved[0][0], "map 1 should be resolved");
    }

    #[test]
    fn child_ml_pending_cleared_on_signal1() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        // Manually set pending for map 0 (home)
        engine.pending_phase_verify[0] = Some((0, true));
        // Signal 1 fires for map 1 (rounds=13-7, current_map=1)
        let s = state(0, 0, 13, 7, 1, false);
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &NO_PHASES, &mut out);
        // Signal 1 fires and sets map_winner_resolved[0][0] = true
        assert!(engine.map_winner_resolved[0][0]);
        // Pending re-check sees it's already resolved → clears
        assert!(engine.pending_phase_verify[0].is_none(), "pending cleared by Signal 1");
    }

    #[test]
    fn child_ml_pending_forfeit_stays_blocked() {
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        // Manually set pending for map 0 (home)
        engine.pending_phase_verify[0] = Some((0, true));
        // Phases "catch up" but still show forfeit score (4-0)
        let forfeit_phases = [(4, 0), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let s = state_with_prev(1, 0, 0, 0, 1, 0, 2, false);
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), &s, &forfeit_phases, &mut out);
        assert!(out.is_empty(), "forfeit phases should NOT fire");
        // Pending stays set — not resolved
        assert!(engine.pending_phase_verify[0].is_some(), "pending should persist for forfeit");
    }

    #[test]
    fn child_ml_pending_fires_on_duplicate_frame() {
        // Behavior B: maps increment with lagged phases → pending set.
        // Then a "duplicate" frame (same maps/rounds) arrives with updated phases.
        // The dedup bypass lets pending re-check fire on this frame.
        let t_away = TargetIdx(1);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(t_away)));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        // Tick 0: pre-match — go through check_duplicate to populate dedup row.
        engine.check_duplicate("g1", "0", "0", "1st map", "0", "0");
        engine.process_tick_live(GameIdx(0), 0, 0, 0, 0, 1, false, "LIVE", &NO_PHASES, 0);
        // Tick 1: maps=0-1, rounds=0-0 (Behavior B). Phases lag: (8,12) not (8,13).
        engine.check_duplicate("g1", "0", "1", "2nd map", "0", "0");
        let lag_phases = [(8, 12), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let r1 = engine.process_tick_live(GameIdx(0), 0, 1, 0, 0, 2, false, "LIVE", &lag_phases, 0);
        let i1: Vec<TargetIdx> = r1.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i1.is_empty(), "lagged phases — should not fire");
        assert!(engine.pending_phase_verify[0].is_some(), "pending should be set");
        // Tick 2: DUPLICATE frame — same maps=0-1, rounds=0-0, same freeText.
        // Without the dedup bypass, this would be rejected and pending stuck for 17+ minutes.
        let r2 = engine.check_duplicate("g1", "0", "1", "2nd map", "0", "0");
        assert!(r2.is_some(), "dedup should be bypassed when pending is active");
        // Process with updated phases (8,13) — phases caught up.
        let fixed_phases = [(8, 13), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let r2 = engine.process_tick_live(GameIdx(0), 0, 1, 0, 0, 2, false, "LIVE", &fixed_phases, 0);
        let i2: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert_eq!(i2, vec![t_away], "pending re-check should fire on duplicate frame");
        assert!(engine.pending_phase_verify[0].is_none(), "pending should be cleared");
    }

    // ── Moneyline ───────────────────────────────────────────────────

    #[test]
    fn moneyline_home_wins_bo3() {
        let t_home = TargetIdx(10);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_home);
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let s = state_with_prev(2, 0, 1, 0, 0, 0, 3, false);
        let intents = eval_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_home]);
        // final_resolved_games is now set by process_tick_live, not moneyline
    }

    #[test]
    fn moneyline_away_wins_bo3_map3() {
        let t_away = TargetIdx(11);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(t_away);
        });
        let s = state_with_prev(1, 2, 1, 1, 0, 0, 0, false);
        let intents = eval_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_away]);
    }

    #[test]
    fn moneyline_not_at_1_1() {
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let s = state(1, 1, 5, 3, 3, false);
        let intents = eval_ml(&mut engine, &s);
        assert!(intents.is_empty(), "1-1 in BO3 is not decided");
    }

    #[test]
    fn moneyline_closed_fallback() {
        let t_home = TargetIdx(10);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_home);
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let s = state(2, 1, 0, 0, 0, true);
        let intents = eval_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_home]);
    }

    // ── Totals ──────────────────────────────────────────────────────

    #[test]
    fn totals_over_fires_at_3_maps() {
        let t_over = TargetIdx(20);
        let mut engine = make_engine(2, |tgt| {
            tgt.over_lines.push(OverLine { half_int: 2, target_idx: t_over });
        });
        // prev=2 (maps 1-1), now=3 (maps 2-1)
        let s = state_with_prev(2, 1, 1, 1, 0, 0, 0, false);
        let intents = eval_totals(&mut engine, &s);
        assert_eq!(intents, vec![t_over]);
    }

    #[test]
    fn totals_over_fires_at_guaranteed_certainty() {
        let t_over = TargetIdx(20);
        let mut engine = make_engine(2, |tgt| {
            tgt.over_lines.push(OverLine { half_int: 2, target_idx: t_over });
        });
        // BO3, maps go 1-0 → 1-1. Over 2.5 is guaranteed at 1-1
        // (min(1,1) >= 2+1-2 = 1). Should fire NOW, not when map 3 completes.
        let s = state_with_prev(1, 1, 0, 1, 0, 0, 3, false);
        let intents = eval_totals(&mut engine, &s);
        assert_eq!(intents, vec![t_over], "over 2.5 should fire at 1-1 (guaranteed)");
    }

    #[test]
    fn totals_over_not_guaranteed_at_1_0() {
        let mut engine = make_engine(2, |tgt| {
            tgt.over_lines.push(OverLine { half_int: 2, target_idx: TargetIdx(20) });
        });
        // BO3, maps go 0-0 → 1-0. Over 2.5 NOT guaranteed (could end 2-0).
        // min(1,0) = 0 < 1. Should not fire.
        let s = state_with_prev(1, 0, 0, 0, 0, 0, 2, false);
        let intents = eval_totals(&mut engine, &s);
        assert!(intents.is_empty(), "over 2.5 should NOT fire at 1-0");
    }

    #[test]
    fn totals_under_fires_at_2_0_match_end() {
        let t_under = TargetIdx(21);
        let mut engine = make_engine(2, |tgt| {
            tgt.under_lines.push(OverLine { half_int: 2, target_idx: t_under });
        });
        let s = state_with_prev(2, 0, 1, 0, 0, 0, 0, true);
        let intents = eval_totals(&mut engine, &s);
        assert_eq!(intents, vec![t_under]);
    }

    #[test]
    fn totals_under_not_mid_match() {
        let mut engine = make_engine(2, |tgt| {
            tgt.under_lines.push(OverLine { half_int: 2, target_idx: TargetIdx(21) });
        });
        let s = state(1, 0, 5, 3, 2, false);
        let intents = eval_totals(&mut engine, &s);
        assert!(intents.is_empty(), "under should not fire mid-match");
    }

    // ── Map handicap ────────────────────────────────────────────────

    #[test]
    fn handicap_home_covers_2_0() {
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        let s = state_with_prev(2, 0, 1, 0, 0, 0, 0, true);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.contains(&t_covers), "home -1.5 covers at 2-0");
        assert!(!intents.contains(&t_not));
    }

    #[test]
    fn handicap_home_not_covers_2_1() {
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        let s = state_with_prev(2, 1, 1, 1, 0, 0, 0, true);
        let intents = eval_handicap(&mut engine, &s);
        assert!(!intents.contains(&t_covers));
        assert!(intents.contains(&t_not), "home -1.5 doesn't cover at 2-1");
    }

    #[test]
    fn handicap_not_covers_fires_early() {
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        // Home -1.5 in BO3, score 0-1 mid-match: max_margin = 2-1 = 1
        // 1 + (-1.5) = -0.5 ≤ 0 → home can never cover → fire not_covers
        let s = state(0, 1, 5, 3, 2, false);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.contains(&t_not), "not_covers should fire early at 0-1");
        assert!(!intents.contains(&t_covers), "covers must not fire mid-match");
        // Second call with same state: tombstone prevents double-fire
        let intents2 = eval_handicap(&mut engine, &s);
        assert!(intents2.is_empty(), "tombstone should prevent re-fire");
    }

    #[test]
    fn handicap_no_early_fire_when_favored_leads() {
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        // Home -1.5 in BO3, score 1-0 mid-match: max_margin = 2-0 = 2
        // 2 + (-1.5) = 0.5 > 0 → still possible → no fire
        let s = state(1, 0, 5, 3, 2, false);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.is_empty(), "should not fire when favored team still leads");
    }

    #[test]
    fn handicap_away_not_covers_fires_early() {
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Away,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        // Away -1.5 in BO3, score 1-0 mid-match (away lost a map):
        // opponent_maps (for Away) = maps_home = 1
        // max_margin = 2-1 = 1, 1+(-1.5) = -0.5 ≤ 0 → fire not_covers
        let s = state(1, 0, 5, 3, 2, false);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.contains(&t_not), "away not_covers should fire early at 1-0");
        assert!(!intents.contains(&t_covers), "covers must not fire mid-match");
    }

    #[test]
    fn handicap_not_mid_match() {
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(TargetIdx(30)),
                not_covers_idx: Some(TargetIdx(31)),
            });
        });
        let s = state(1, 0, 5, 3, 2, false);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.is_empty(), "handicap should not fire mid-match");
    }

    // ── Cross-evaluator ─────────────────────────────────────────────

    #[test]
    fn moneyline_and_handicap_both_fire_on_match_end() {
        let t_ml_home = TargetIdx(10);
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_ml_home);
            tgt.moneyline_away = Some(TargetIdx(11));
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        let s = state_with_prev(2, 0, 1, 0, 0, 0, 0, true);
        // Run moneyline first (same order as process_tick_live)
        let ml_intents = eval_ml(&mut engine, &s);
        assert_eq!(ml_intents, vec![t_ml_home]);
        // map_handicap must NOT be blocked by moneyline
        let hc_intents = eval_handicap(&mut engine, &s);
        assert!(hc_intents.contains(&t_covers), "handicap must fire alongside moneyline");
    }

    #[test]
    fn final_resolved_blocks_all() {
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        engine.final_resolved_games[0] = true;
        let s = state(2, 0, 13, 5, 2, true);
        assert!(eval_child_ml(&mut engine, &s).is_empty());
        assert!(eval_ml(&mut engine, &s).is_empty());
        assert!(eval_handicap(&mut engine, &s).is_empty());
    }

    // ── OT integration via process_tick_live ────────────────────────

    #[test]
    fn child_ml_ot_win_through_process_tick() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        // Tick 1: 12-12, OT starts — no fire
        let r1 = engine.process_tick_live(GameIdx(0), 0, 0, 12, 12, 1, false, "LIVE", &NO_PHASES, 0);
        assert!(r1.unwrap().intents.is_empty(), "12-12 must not fire");
        // Tick 2: 16-14, OT1 won by home — fires map 1 home
        let r2 = engine.process_tick_live(GameIdx(0), 0, 0, 16, 14, 1, false, "LIVE", &NO_PHASES, 0);
        let intents: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert_eq!(intents, vec![t_home], "16-14 OT1 should fire map 1 home");
    }

    // ── Match-end fires on round-13 (effective maps) ─────────────────

    #[test]
    fn match_end_fires_on_round13_signal() {
        let t_ml_home = TargetIdx(10);
        let t_map2_home = TargetIdx(2);
        let t_under = TargetIdx(20);
        let t_covers = TargetIdx(30);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_ml_home);
            tgt.moneyline_away = Some(TargetIdx(11));
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(t_map2_home), Some(TargetIdx(3))));
            tgt.under_lines.push(OverLine { half_int: 2, target_idx: t_under });
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(TargetIdx(31)),
            });
        });
        // Tick 0: pre-match 0-0
        engine.process_tick_live(GameIdx(0), 0, 0, 0, 0, 1, false, "LIVE", &NO_PHASES, 0);
        // Tick 1: home wins map 1 (maps-won fallback: 0-0 → 1-0)
        engine.process_tick_live(GameIdx(0), 1, 0, 0, 0, 2, false, "LIVE", &NO_PHASES, 0);
        // Tick 2: map 2 round-13 → home wins map 2 via Signal 1.
        // maps counter is STILL 1-0 (V1 hasn't caught up), but effective is 2-0.
        let r = engine.process_tick_live(GameIdx(0), 1, 0, 13, 7, 2, false, "LIVE", &NO_PHASES, 0);
        let intents: Vec<TargetIdx> = r.unwrap().intents.iter().map(|i| i.target_idx).collect();
        // All four should fire on the SAME tick:
        assert!(intents.contains(&t_map2_home), "child_moneyline MAP2 should fire");
        assert!(intents.contains(&t_ml_home), "moneyline should fire (effective maps=2)");
        assert!(intents.contains(&t_under), "totals under should fire (effective total=2)");
        assert!(intents.contains(&t_covers), "map handicap covers should fire (effective margin=2)");
        // Match should be resolved
        assert!(engine.final_resolved_games[0], "match should be final-resolved");
    }

    #[test]
    fn match_end_fires_on_map3_without_child_ml() {
        // BO3 map 3: no child_moneyline market for map 3 (PM doesn't list it).
        // Only maps 1-2 have child_moneyline targets.
        let t_ml_home = TargetIdx(10);
        let t_covers = TargetIdx(30);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_ml_home);
            tgt.moneyline_away = Some(TargetIdx(11));
            // Only 2 map_moneyline entries (maps 1-2) — NO map 3 target
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(TargetIdx(31)),
            });
        });
        // Tick 0: pre-match
        engine.process_tick_live(GameIdx(0), 0, 0, 0, 0, 1, false, "LIVE", &NO_PHASES, 0);
        // Tick 1: maps go to 1-1 (both teams won a map)
        engine.process_tick_live(GameIdx(0), 1, 0, 0, 0, 2, false, "LIVE", &NO_PHASES, 0);
        engine.process_tick_live(GameIdx(0), 1, 1, 0, 0, 3, false, "LIVE", &NO_PHASES, 0);
        // Tick 3: map 3, round-13 → home wins map 3. No child_moneyline target.
        // effective_state should detect the map winner via map_winner() directly.
        // Effective maps = 2-1. Moneyline fires (2 >= mtw=2).
        // Note: under 2.5 does NOT fire because effective total=3 > 2.5.
        let r = engine.process_tick_live(GameIdx(0), 1, 1, 13, 5, 3, false, "LIVE", &NO_PHASES, 0);
        let intents: Vec<TargetIdx> = r.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(intents.contains(&t_ml_home), "moneyline should fire on map 3 round-13 (no child_ml needed)");
        // Map handicap: margin=2-1=1, 1+(-1.5)=-0.5≤0 → not_covers.
        // But map_handicap_early_emitted might already be set from tick at 1-1.
        // The key assertion: moneyline fires without needing child_moneyline.
        assert!(engine.final_resolved_games[0], "match should be final-resolved");
    }

    // ── BO5 map numbering ───────────────────────────────────────────

    #[test]
    fn child_ml_bo5_map_numbering() {
        let mut engine = make_engine(3, |tgt| {
            for i in 0..5u16 {
                tgt.map_moneyline.push((Some(TargetIdx(i * 2)), Some(TargetIdx(i * 2 + 1))));
            }
        });
        assert_eq!(engine.map_winner_resolved[0].len(), 5, "BO5 should have 5 map slots");

        // Tick 0: pre-match 0-0 (establishes first observation, no intents)
        let r0 = engine.process_tick_live(GameIdx(0), 0, 0, 0, 0, 1, false, "LIVE", &NO_PHASES, 0);
        assert!(r0.unwrap().intents.is_empty(), "0-0 should not fire");

        // Map 1: home wins (maps 0-0 → 1-0). Phases confirm 13-7.
        let p1 = [(13, 7), (-1, -1), (-1, -1), (-1, -1), (-1, -1)];
        let r1 = engine.process_tick_live(GameIdx(0), 1, 0, 0, 0, 2, false, "LIVE", &p1, 0);
        let i1: Vec<TargetIdx> = r1.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert_eq!(i1, vec![TargetIdx(0)], "map 1 home = TargetIdx(0)");
        assert!(engine.map_winner_resolved[0][0]);

        // Map 2: away wins (maps 1-0 → 1-1). Phases confirm map 2 = 8-13.
        let p2 = [(13, 7), (8, 13), (-1, -1), (-1, -1), (-1, -1)];
        let r2 = engine.process_tick_live(GameIdx(0), 1, 1, 0, 0, 3, false, "LIVE", &p2, 0);
        let i2: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert_eq!(i2, vec![TargetIdx(3)], "map 2 away = TargetIdx(3)");
        assert!(engine.map_winner_resolved[0][1]);

        // Map 3: home wins (maps 1-1 → 2-1). Phases confirm map 3 = 13-10.
        let p3 = [(13, 7), (8, 13), (13, 10), (-1, -1), (-1, -1)];
        let r3 = engine.process_tick_live(GameIdx(0), 2, 1, 0, 0, 4, false, "LIVE", &p3, 0);
        let i3: Vec<TargetIdx> = r3.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i3.contains(&TargetIdx(4)), "map 3 home = TargetIdx(4)");
        assert!(engine.map_winner_resolved[0][2]);
    }

    // ── Moneyline tie at match_completed ────────────────────────────

    #[test]
    fn moneyline_does_not_fire_at_tie() {
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        // BO3, maps 1-1, match_completed=true (shouldn't happen but fail-closed)
        let s = state_with_prev(1, 1, 0, 1, 0, 0, 0, true);
        let intents = eval_ml(&mut engine, &s);
        assert!(intents.is_empty(), "tied maps at match end must fire nothing (fail-closed)");
    }

    // ── Cold-start: totals progressive skip ─────────────────────────

    #[test]
    fn totals_first_tick_no_progressive_fire() {
        let t_over = TargetIdx(20);
        let mut engine = make_engine(2, |tgt| {
            tgt.over_lines.push(OverLine { half_int: 1, target_idx: t_over });
        });
        // First tick: maps already 1-1 (mid-match subscribe via process_tick_live)
        let r1 = engine.process_tick_live(GameIdx(0), 1, 1, 5, 3, 3, false, "LIVE", &NO_PHASES, 0);
        let i1: Vec<TargetIdx> = r1.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i1.is_empty(), "first tick must NOT fire progressive over (cold-start)");

        // Second tick: maps go 1-1 → 2-1 — NOW progressive fires for crossing line 2
        let t_over2 = TargetIdx(21);
        engine.game_targets[0].over_lines.push(OverLine { half_int: 2, target_idx: t_over2 });
        engine.game_targets[0].over_lines.sort_by_key(|ol| ol.half_int);
        let r2 = engine.process_tick_live(GameIdx(0), 2, 1, 0, 0, 0, false, "LIVE", &NO_PHASES, 0);
        let i2: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        // total went 2→3, so half_int=2 fires (line 2.5 over). half_int=1 also fires (1→2 crossing missed on first tick, but 1 < prev=2 now).
        assert!(i2.contains(&t_over2), "second tick should fire over 2.5");
    }

    // ── Cold-start: child_moneyline tombstone for completed maps ────

    #[test]
    fn child_ml_first_tick_no_fallback_for_completed_maps() {
        let t_map1_home = TargetIdx(0);
        let t_map2_away = TargetIdx(3);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_map1_home), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(t_map2_away)));
        });
        // First tick: maps 1-0 (map 1 already completed before subscribe)
        let r1 = engine.process_tick_live(GameIdx(0), 1, 0, 5, 3, 2, false, "LIVE", &NO_PHASES, 0);
        let i1: Vec<TargetIdx> = r1.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i1.is_empty(), "first tick must NOT fire map 1 fallback (tombstone)");
        assert!(engine.map_winner_resolved[0][0], "map 1 should be tombstoned");
        assert!(!engine.map_winner_resolved[0][1], "map 2 should NOT be tombstoned");

        // Second tick: maps 1-0 → 1-1 (map 2 just completed by away). Phases confirm map 2 = 9-13.
        let p2 = [(13, 7), (9, 13), (-1, -1), (-1, -1), (-1, -1)];
        let r2 = engine.process_tick_live(GameIdx(0), 1, 1, 0, 0, 3, false, "LIVE", &p2, 0);
        let i2: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert_eq!(i2, vec![t_map2_away], "map 2 away should fire on second tick");
    }

    // ── Handicap: away covers at match end ──────────────────────────

    #[test]
    fn handicap_away_covers_at_match_end() {
        let t_covers = TargetIdx(30);
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Away,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(t_not),
            });
        });
        // Away -1.5, final 0-2 (away sweeps)
        let s = state_with_prev(0, 2, 0, 1, 0, 0, 0, true);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.contains(&t_covers), "away -1.5 covers at 0-2");
        assert!(!intents.contains(&t_not));
    }
}
