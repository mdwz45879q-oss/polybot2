use crate::moba::types::*;
use crate::*;

fn push_if_some(slot: Option<TargetIdx>, out: &mut smallvec::SmallVec<[Intent; 32]>) {
    if let Some(tidx) = slot {
        out.push(Intent { target_idx: tidx });
    }
}

impl NativeMobaEngine {
    // ---------------------------------------------------------------
    // 1. Child moneyline — map winner (maps-won increment only)
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_child_moneyline_into(
        &mut self,
        gidx: GameIdx,
        state: &MobaGameState,
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

        // Maps-won increment detection (only signal — no round-level data in MOBA).
        let maps_home = state.maps_home.unwrap_or(0);
        let maps_away = state.maps_away.unwrap_or(0);
        let prev_home = state.prev_maps_home.unwrap_or(0);
        let prev_away = state.prev_maps_away.unwrap_or(0);

        let home_inc = maps_home > prev_home;
        let away_inc = maps_away > prev_away;

        // Fail-closed: if BOTH sides incremented on the same tick (e.g., after
        // WS reconnection: 0-0 → 1-1), we cannot determine which team won
        // which map. Skip child_moneyline entirely — miss two bets rather
        // than risk one wrong bet. Moneyline/totals/handicap still fire correctly.
        if home_inc && away_inc {
            eprintln!(
                "[moba] child_moneyline: both sides incremented ({}-{} → {}-{}), skipping (ambiguous map attribution)",
                prev_home, prev_away, maps_home, maps_away
            );
            // Tombstone the maps so they don't fire on subsequent ticks either.
            let map1 = (prev_home + prev_away) as usize;
            let map2 = map1 + 1;
            if map1 < self.map_winner_resolved[gi].len() {
                self.map_winner_resolved[gi][map1] = true;
            }
            if map2 < self.map_winner_resolved[gi].len() {
                self.map_winner_resolved[gi][map2] = true;
            }
            return;
        }

        if home_inc {
            let completed_map = prev_home + prev_away + 1;
            let map_idx = (completed_map - 1) as usize;
            if map_idx < targets.map_moneyline.len()
                && map_idx < self.map_winner_resolved[gi].len()
                && !self.map_winner_resolved[gi][map_idx]
            {
                push_if_some(targets.map_moneyline[map_idx].0, out); // home
                self.map_winner_resolved[gi][map_idx] = true;
            }
        }
        if away_inc {
            let completed_map = prev_home + prev_away + 1;
            let map_idx = (completed_map - 1) as usize;
            if map_idx < targets.map_moneyline.len()
                && map_idx < self.map_winner_resolved[gi].len()
                && !self.map_winner_resolved[gi][map_idx]
            {
                push_if_some(targets.map_moneyline[map_idx].1, out); // away
                self.map_winner_resolved[gi][map_idx] = true;
            }
        }
    }

    // ---------------------------------------------------------------
    // 2. Moneyline — match winner
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_moneyline_into(
        &mut self,
        gidx: GameIdx,
        state: &MobaGameState,
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
        // Tied maps at match end: fire nothing (fail-closed).
    }

    // ---------------------------------------------------------------
    // 3. Totals — over/under on total maps played
    // ---------------------------------------------------------------

    pub(crate) fn evaluate_totals_into(
        &mut self,
        gidx: GameIdx,
        state: &MobaGameState,
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
        // Condition: min(maps_home, maps_away) >= N + 1 - maps_to_win.
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
        state: &MobaGameState,
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
    use crate::{GameIdx, Intent, OverLine, SpreadSide, TargetIdx};

    fn make_engine(maps_to_win: i64, setup: impl FnOnce(&mut MobaGameTargets)) -> NativeMobaEngine {
        let mut engine = NativeMobaEngine::new();
        engine.game_ids.push("g1".to_string());
        engine.game_leagues.push(std::sync::Arc::from("lol"));
        engine.kickoff_ts.push(None);
        engine.maps_to_win.push(maps_to_win);
        engine.token_ids_by_game.push(Vec::new());

        let mut tgt = MobaGameTargets::default();
        setup(&mut tgt);
        engine.game_targets.push(tgt);

        engine.has_moneyline.push(true);
        engine.has_totals.push(true);
        engine.has_child_moneyline.push(true);
        engine.has_map_handicap.push(true);

        engine.rows.push(None);
        engine.game_states.push(MobaGameState::default());
        engine.final_resolved_games.push(false);
        engine.totals_under_emitted.push(false);
        engine.map_handicap_early_emitted.push(false);
        let max_maps = (maps_to_win * 2 - 1).max(1) as usize;
        engine.map_winner_resolved.push(vec![false; max_maps]);

        engine
    }

    fn eval_child_ml(engine: &mut NativeMobaEngine, s: &MobaGameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_child_moneyline_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn eval_ml(engine: &mut NativeMobaEngine, s: &MobaGameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_moneyline_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn eval_totals(engine: &mut NativeMobaEngine, s: &MobaGameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_totals_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn eval_handicap(engine: &mut NativeMobaEngine, s: &MobaGameState) -> Vec<TargetIdx> {
        let mut out = smallvec::SmallVec::<[Intent; 32]>::new();
        engine.evaluate_map_handicap_into(GameIdx(0), s, &mut out);
        out.iter().map(|i| i.target_idx).collect()
    }

    fn state_with_prev(
        maps_home: i64, maps_away: i64,
        prev_maps_home: i64, prev_maps_away: i64,
        match_completed: bool,
    ) -> MobaGameState {
        MobaGameState {
            maps_home: Some(maps_home),
            maps_away: Some(maps_away),
            prev_maps_home: Some(prev_maps_home),
            prev_maps_away: Some(prev_maps_away),
            total_maps: maps_home + maps_away,
            prev_total_maps: Some(prev_maps_home + prev_maps_away),
            match_completed,
            game_state: if match_completed { "FINAL" } else { "LIVE" },
        }
    }

    // ── Child moneyline ────────────────────────────────────────────

    #[test]
    fn child_ml_maps_won_fires() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        let s = state_with_prev(1, 0, 0, 0, false);
        let intents = eval_child_ml(&mut engine, &s);
        assert_eq!(intents, vec![t_home], "map 1 home should fire");
        assert!(engine.map_winner_resolved[0][0]);
    }

    #[test]
    fn child_ml_no_double_fire() {
        let t_home = TargetIdx(0);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(t_home), Some(TargetIdx(1))));
        });
        // First tick: fires
        let s1 = state_with_prev(1, 0, 0, 0, false);
        assert_eq!(eval_child_ml(&mut engine, &s1), vec![t_home]);
        // Second tick: same maps, no fire
        let s2 = state_with_prev(1, 0, 1, 0, false);
        assert!(eval_child_ml(&mut engine, &s2).is_empty());
    }

    #[test]
    fn child_ml_double_increment_skips_both() {
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        // Both sides increment on same tick: 0-0 → 1-1 (ambiguous attribution)
        let s = state_with_prev(1, 1, 0, 0, false);
        let intents = eval_child_ml(&mut engine, &s);
        assert!(intents.is_empty(), "should skip both maps (fail-closed)");
        // Both maps tombstoned
        assert!(engine.map_winner_resolved[0][0], "map 1 should be tombstoned");
        assert!(engine.map_winner_resolved[0][1], "map 2 should be tombstoned");
        // Subsequent tick with normal increment for map 3 still works
        // (if this were BO5 with 5 map slots)
    }

    // ── Moneyline ──────────────────────────────────────────────────

    #[test]
    fn moneyline_home_wins_bo3() {
        let t_home = TargetIdx(10);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_home);
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let s = state_with_prev(2, 0, 1, 0, false);
        assert_eq!(eval_ml(&mut engine, &s), vec![t_home]);
    }

    #[test]
    fn moneyline_not_at_1_1() {
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let s = state_with_prev(1, 1, 1, 0, false);
        assert!(eval_ml(&mut engine, &s).is_empty());
    }

    // ── Totals ─────────────────────────────────────────────────────

    #[test]
    fn totals_over_guaranteed_certainty() {
        let t_over = TargetIdx(20);
        let mut engine = make_engine(2, |tgt| {
            tgt.over_lines.push(OverLine { half_int: 2, target_idx: t_over });
        });
        // BO3, maps 1-0 → 1-1: over 2.5 guaranteed (min=1 >= 1)
        let s = state_with_prev(1, 1, 1, 0, false);
        assert_eq!(eval_totals(&mut engine, &s), vec![t_over]);
    }

    #[test]
    fn totals_over_not_guaranteed_at_1_0() {
        let mut engine = make_engine(2, |tgt| {
            tgt.over_lines.push(OverLine { half_int: 2, target_idx: TargetIdx(20) });
        });
        let s = state_with_prev(1, 0, 0, 0, false);
        assert!(eval_totals(&mut engine, &s).is_empty());
    }

    #[test]
    fn totals_under_at_match_end() {
        let t_under = TargetIdx(21);
        let mut engine = make_engine(2, |tgt| {
            tgt.under_lines.push(OverLine { half_int: 2, target_idx: t_under });
        });
        let s = state_with_prev(2, 0, 1, 0, false);
        assert_eq!(eval_totals(&mut engine, &s), vec![t_under]);
    }

    // ── Map handicap ───────────────────────────────────────────────

    #[test]
    fn handicap_covers_at_match_end() {
        let t_covers = TargetIdx(30);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(TargetIdx(31)),
            });
        });
        let s = state_with_prev(2, 0, 1, 0, false);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.contains(&t_covers));
    }

    #[test]
    fn handicap_not_covers_fires_early() {
        let t_not = TargetIdx(31);
        let mut engine = make_engine(2, |tgt| {
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(TargetIdx(30)),
                not_covers_idx: Some(t_not),
            });
        });
        // Home -1.5 in BO3, maps 0-1: max_margin=2-1=1, 1+(-1.5)=-0.5≤0
        let s = state_with_prev(0, 1, 0, 0, false);
        let intents = eval_handicap(&mut engine, &s);
        assert!(intents.contains(&t_not));
        // Tombstone prevents re-fire
        let intents2 = eval_handicap(&mut engine, &s);
        assert!(intents2.is_empty());
    }

    // ── Cross-evaluator ────────────────────────────────────────────

    #[test]
    fn moneyline_and_handicap_both_fire() {
        let t_ml = TargetIdx(10);
        let t_covers = TargetIdx(30);
        let mut engine = make_engine(2, |tgt| {
            tgt.moneyline_home = Some(t_ml);
            tgt.moneyline_away = Some(TargetIdx(11));
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(TargetIdx(31)),
            });
        });
        let s = state_with_prev(2, 0, 1, 0, false);
        let ml = eval_ml(&mut engine, &s);
        let hc = eval_handicap(&mut engine, &s);
        assert_eq!(ml, vec![t_ml]);
        assert!(hc.contains(&t_covers));
    }

    // ── Cold-start ─────────────────────────────────────────────────

    #[test]
    fn cold_start_tombstones_completed_maps() {
        let mut engine = make_engine(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        // First tick at maps 1-0 (mid-match subscribe)
        let r1 = engine.process_tick_live(GameIdx(0), 1, 0, false, "LIVE", 0);
        let i1: Vec<TargetIdx> = r1.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i1.is_empty(), "first tick must NOT fire map 1 (tombstoned)");
        assert!(engine.map_winner_resolved[0][0], "map 1 should be tombstoned");
        // Second tick: maps 1-0 → 1-1 (map 2 won by away)
        let r2 = engine.process_tick_live(GameIdx(0), 1, 1, false, "LIVE", 0);
        let i2: Vec<TargetIdx> = r2.unwrap().intents.iter().map(|i| i.target_idx).collect();
        assert!(i2.contains(&TargetIdx(3)), "map 2 away should fire");
    }
}
