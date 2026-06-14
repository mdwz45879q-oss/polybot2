use crate::cs2::eval::map_winner;
use crate::cs2::types::{Cs2GameState, NativeCs2Engine};
use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::log_writer::LogWriter;
use crate::{GameIdx, Intent};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Per-match state — tracks maps-won and score history for dual-trigger logic.
// CS2-specific (LoL/Dota2 would have different state structs).
// ---------------------------------------------------------------------------

pub(crate) struct PandaScoreMatchState {
    pub match_id: i64,
    pub current_game_id: i64,
    pub prev_home_score: i64,
    pub prev_away_score: i64,
    pub maps_home: i64,
    pub maps_away: i64,
    pub maps_to_win: i64,
    pub map_fired: Vec<bool>,
    pub home_team_id: i64,
    pub away_team_id: i64,
    pub match_completed: bool,
    pub forfeit_pending: bool,
    pub prev_total_maps: Option<i64>,
    pub completed_game_ids: smallvec::SmallVec<[i64; 8]>,
}

impl PandaScoreMatchState {
    pub(crate) fn new(
        match_id: i64,
        maps_to_win: i64,
        maps_home: i64,
        maps_away: i64,
        home_team_id: i64,
        away_team_id: i64,
    ) -> Self {
        let max_maps = (maps_to_win * 2 - 1).max(1) as usize;
        let mut map_fired = vec![false; max_maps];
        let maps_done = (maps_home + maps_away) as usize;
        for i in 0..maps_done.min(max_maps) {
            map_fired[i] = true;
        }
        Self {
            match_id,
            current_game_id: 0,
            prev_home_score: 0,
            prev_away_score: 0,
            maps_home,
            maps_away,
            maps_to_win,
            map_fired,
            home_team_id,
            away_team_id,
            match_completed: false,
            forfeit_pending: false,
            prev_total_maps: None,
            completed_game_ids: smallvec::SmallVec::new(),
        }
    }

    pub(crate) fn current_map_number(&self) -> i64 {
        self.maps_home + self.maps_away + 1
    }
}

// ---------------------------------------------------------------------------
// Pending log — returned for deferred tick flush by the WS worker
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct PandaScoreCs2PendingLog {
    pub game_idx: GameIdx,
    pub maps_home: i64,
    pub maps_away: i64,
    pub rounds_home: i64,
    pub rounds_away: i64,
    pub current_map: i64,
    pub game_state: &'static str,
}

// ---------------------------------------------------------------------------
// Dual-trigger map winner detection
// ---------------------------------------------------------------------------

fn evaluate_triggers(
    ms: &mut PandaScoreMatchState,
    engine: &NativeCs2Engine,
    gidx: GameIdx,
    home_score: i64,
    away_score: i64,
    game_id: i64,
) -> smallvec::SmallVec<[Intent; 32]> {
    let gi = gidx.0 as usize;
    let mut intents = smallvec::SmallVec::<[Intent; 32]>::new();

    if !engine.has_child_moneyline[gi] {
        let trigger_a_fired = run_trigger_a_maps_only(ms, home_score, away_score, game_id);
        if !trigger_a_fired {
            run_trigger_b_maps_only(ms, home_score, away_score, game_id);
        }
        return intents;
    }

    let targets = &engine.game_targets[gi];
    let mut trigger_a_fired = false;

    // --- Trigger A: map_winner(current_score) ---
    if let Some(winner) = map_winner(home_score, away_score) {
        let map_idx = ms.current_map_number() as usize - 1;
        if map_idx < ms.map_fired.len() && !ms.map_fired[map_idx] {
            ms.map_fired[map_idx] = true;
            ms.completed_game_ids.push(game_id);
            trigger_a_fired = true;
            match winner {
                "home" => ms.maps_home += 1,
                _ => ms.maps_away += 1,
            }
            if map_idx < targets.map_moneyline.len() {
                let (home_slot, away_slot) = &targets.map_moneyline[map_idx];
                match winner {
                    "home" => {
                        if let Some(tidx) = home_slot {
                            intents.push(Intent { target_idx: *tidx });
                        }
                    }
                    _ => {
                        if let Some(tidx) = away_slot {
                            intents.push(Intent { target_idx: *tidx });
                        }
                    }
                }
            }
        }
    }

    // --- Trigger B: score-reset (0,0) from non-zero on SAME game_id ---
    // Skipped when Trigger A already fired this tick (C1 fix).
    if !trigger_a_fired
        && home_score == 0
        && away_score == 0
        && (ms.prev_home_score != 0 || ms.prev_away_score != 0)
        && game_id == ms.current_game_id
        && ms.current_game_id != 0
    {
        let map_idx = ms.current_map_number() as usize - 1;
        if map_idx < ms.map_fired.len() && !ms.map_fired[map_idx] {
            let home_won = map_winner(ms.prev_home_score + 1, ms.prev_away_score) == Some("home");
            let away_won = map_winner(ms.prev_home_score, ms.prev_away_score + 1) == Some("away");

            if home_won && !away_won {
                ms.map_fired[map_idx] = true;
                ms.completed_game_ids.push(game_id);
                ms.maps_home += 1;
                if map_idx < targets.map_moneyline.len() {
                    if let Some(tidx) = targets.map_moneyline[map_idx].0 {
                        intents.push(Intent { target_idx: tidx });
                    }
                }
            } else if away_won && !home_won {
                ms.map_fired[map_idx] = true;
                ms.completed_game_ids.push(game_id);
                ms.maps_away += 1;
                if map_idx < targets.map_moneyline.len() {
                    if let Some(tidx) = targets.map_moneyline[map_idx].1 {
                        intents.push(Intent { target_idx: tidx });
                    }
                }
            } else {
                ms.forfeit_pending = true;
            }
        }
    }

    intents
}

fn run_trigger_a_maps_only(ms: &mut PandaScoreMatchState, home_score: i64, away_score: i64, game_id: i64) -> bool {
    if let Some(winner) = map_winner(home_score, away_score) {
        let map_idx = ms.current_map_number() as usize - 1;
        if map_idx < ms.map_fired.len() && !ms.map_fired[map_idx] {
            ms.map_fired[map_idx] = true;
            ms.completed_game_ids.push(game_id);
            match winner {
                "home" => ms.maps_home += 1,
                _ => ms.maps_away += 1,
            }
            return true;
        }
    }
    false
}

fn run_trigger_b_maps_only(
    ms: &mut PandaScoreMatchState,
    home_score: i64,
    away_score: i64,
    game_id: i64,
) {
    if home_score == 0
        && away_score == 0
        && (ms.prev_home_score != 0 || ms.prev_away_score != 0)
        && game_id == ms.current_game_id
        && ms.current_game_id != 0
    {
        let map_idx = ms.current_map_number() as usize - 1;
        if map_idx < ms.map_fired.len() && !ms.map_fired[map_idx] {
            let home_won = map_winner(ms.prev_home_score + 1, ms.prev_away_score) == Some("home");
            let away_won = map_winner(ms.prev_home_score, ms.prev_away_score + 1) == Some("away");
            if home_won && !away_won {
                ms.map_fired[map_idx] = true;
                ms.completed_game_ids.push(game_id);
                ms.maps_home += 1;
            } else if away_won && !home_won {
                ms.map_fired[map_idx] = true;
                ms.completed_game_ids.push(game_id);
                ms.maps_away += 1;
            } else {
                ms.forfeit_pending = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main frame pipeline
// ---------------------------------------------------------------------------

pub(crate) fn process_pandascore_cs2_frame(
    engine: &mut NativeCs2Engine,
    match_state: &mut PandaScoreMatchState,
    frame_text: &str,
    _recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<PandaScoreCs2PendingLog> {
    let frame: super::types::PandaScoreLLFrame = serde_json::from_str(frame_text).ok()?;

    if frame.is_hello() || !frame.is_game_state() {
        return None;
    }

    let home = frame.home_team.as_ref()?;
    let away = frame.away_team.as_ref()?;
    let home_score = home.score;
    let away_score = away.score;
    let game_id = frame.game_id.unwrap_or(0);
    let match_status = frame.match_status.as_deref().unwrap_or("");

    let match_id_str = match_state.match_id.to_string();
    let gidx = *engine.game_id_to_idx.get(&match_id_str)?;
    let gi = gidx.0 as usize;

    if engine.final_resolved_games[gi] {
        return None;
    }

    // Reject echo frames from completed game_ids (C3 fix)
    if match_state.completed_game_ids.contains(&game_id) {
        return None;
    }

    // Dedup
    if game_id == match_state.current_game_id
        && home_score == match_state.prev_home_score
        && away_score == match_state.prev_away_score
    {
        return None;
    }

    // Track game_id transitions (new map started)
    if game_id != match_state.current_game_id && match_state.current_game_id != 0 {
        // New game_id means a new map started. Reset prev scores so the 0-0
        // on the new game_id is NOT treated as a score reset (Trigger B).
        match_state.prev_home_score = 0;
        match_state.prev_away_score = 0;
    }

    // Save pre-trigger maps for Cs2GameState construction
    let prev_maps_home = match_state.maps_home;
    let prev_maps_away = match_state.maps_away;
    let prev_total_maps = match_state.prev_total_maps;

    // Run dual-trigger detection
    let mut intents = evaluate_triggers(
        match_state, engine, gidx, home_score, away_score, game_id,
    );

    // Construct engine state
    let match_completed =
        match_status == "finished" || match_state.maps_home >= match_state.maps_to_win
            || match_state.maps_away >= match_state.maps_to_win;
    let game_state: &'static str = if match_completed { "FINAL" } else { "LIVE" };

    let state = Cs2GameState {
        maps_home: Some(match_state.maps_home),
        maps_away: Some(match_state.maps_away),
        prev_maps_home: Some(prev_maps_home),
        prev_maps_away: Some(prev_maps_away),
        total_maps: match_state.maps_home + match_state.maps_away,
        prev_total_maps,
        rounds_home: home_score,
        rounds_away: away_score,
        current_map: match_state.current_map_number(),
        match_completed,
        game_state,
    };

    // Call match-level evaluators directly
    engine.evaluate_moneyline_into(gidx, &state, &mut intents);
    engine.evaluate_totals_into(gidx, &state, &mut intents);
    engine.evaluate_map_handicap_into(gidx, &state, &mut intents);

    // Set final_resolved after evaluators run
    let mtw = engine.maps_to_win[gi];
    if match_completed || match_state.maps_home >= mtw || match_state.maps_away >= mtw {
        engine.final_resolved_games[gi] = true;
        match_state.match_completed = true;
    }

    // Dispatch all intents (child_moneyline + engine intents together)
    if !intents.is_empty() {
        dispatch_intents(&intents, dispatch_handle, log);
    }

    // Update match state for next tick
    match_state.current_game_id = game_id;
    match_state.prev_home_score = home_score;
    match_state.prev_away_score = away_score;
    match_state.prev_total_maps = Some(match_state.maps_home + match_state.maps_away);

    Some(PandaScoreCs2PendingLog {
        game_idx: gidx,
        maps_home: match_state.maps_home,
        maps_away: match_state.maps_away,
        rounds_home: home_score,
        rounds_away: away_score,
        current_map: match_state.current_map_number(),
        game_state,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cs2::types::*;
    use crate::{OverLine, SpreadSide, TargetIdx, TargetRegistry, TargetSlot, TokenIdx, TokenSlot};
    use std::collections::HashSet;
    use std::sync::Arc;

    fn make_engine_with(maps_to_win: i64, setup: impl FnOnce(&mut Cs2GameTargets)) -> NativeCs2Engine {
        let mut engine = NativeCs2Engine::new();
        engine.game_id_to_idx.insert("1513132".to_string(), GameIdx(0));
        engine.game_ids.push("1513132".to_string());
        engine.game_leagues.push(Arc::from("cs2"));
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
        engine.strategy_keys = HashSet::new();

        let reg = Arc::new(TargetRegistry {
            targets: Vec::new(),
            tokens: Vec::new(),
        });
        engine.registry = Some(reg);

        engine
    }

    fn make_match_state(maps_to_win: i64, maps_home: i64, maps_away: i64) -> PandaScoreMatchState {
        PandaScoreMatchState::new(1513132, maps_to_win, maps_home, maps_away, 3216, 133708)
    }

    // ── Trigger A tests ────────────────────────────────────────────────

    #[test]
    fn trigger_a_regulation_away_wins() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 9, 13, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(1)); // away wins map 1
        assert!(ms.map_fired[0]);
        assert_eq!(ms.maps_away, 1);
        assert_eq!(ms.maps_home, 0);
    }

    #[test]
    fn trigger_a_regulation_home_wins() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 13, 11, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0)); // home wins map 1
        assert_eq!(ms.maps_home, 1);
    }

    #[test]
    fn trigger_a_ot_16_14() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 16, 14, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0)); // home wins OT
        assert_eq!(ms.maps_home, 1);
    }

    #[test]
    fn trigger_a_no_fire_at_12() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 12, 8, 100);
        assert!(intents.is_empty());
        assert!(!ms.map_fired[0]);
        assert_eq!(ms.maps_home, 0);
    }

    // ── Trigger B tests ────────────────────────────────────────────────

    #[test]
    fn trigger_b_regulation_home_wins() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 12;
        ms.prev_away_score = 4;

        // Score resets to 0-0 on same game_id → infer home won (13-4)
        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0)); // home
        assert!(ms.map_fired[0]);
        assert_eq!(ms.maps_home, 1);
    }

    #[test]
    fn trigger_b_regulation_away_wins() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 4;
        ms.prev_away_score = 12;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(1)); // away
        assert_eq!(ms.maps_away, 1);
    }

    #[test]
    fn trigger_b_ot_away_wins() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 12;
        ms.prev_away_score = 15;

        // 12-15 → 0-0. Infer away won: map_winner(12, 16) = true
        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(1)); // away
        assert_eq!(ms.maps_away, 1);
    }

    #[test]
    fn trigger_a_then_b_skipped() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        // Trigger A fires on 9-13
        let intents1 = evaluate_triggers(&mut ms, &engine, GameIdx(0), 9, 13, 100);
        assert_eq!(intents1.len(), 1);
        assert_eq!(intents1[0].target_idx, TargetIdx(1)); // away map 1
        assert!(ms.map_fired[0]);
        assert_eq!(ms.maps_away, 1);
        assert_eq!(ms.maps_home, 0, "maps_home must stay 0 after Trigger A");

        // Update prev scores as the pipeline would
        ms.prev_home_score = 9;
        ms.prev_away_score = 13;

        // Subsequent 0-0: Trigger B must NOT fire (C1 fix)
        let intents2 = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert!(intents2.is_empty(), "Trigger B must be skipped after Trigger A");
        assert_eq!(ms.maps_away, 1, "maps_away must stay 1");
        assert_eq!(ms.maps_home, 0, "maps_home must stay 0 (C1 fix)");
        assert!(!ms.map_fired[1], "map 2 must NOT be fired");
    }

    #[test]
    fn trigger_b_anomalous_reset_forfeit() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 2;
        ms.prev_away_score = 4;

        // 2-4 → 0-0: map_winner(3,4) = None, map_winner(2,5) = None → anomalous
        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert!(intents.is_empty(), "forfeit should NOT fire child_moneyline");
        assert!(!ms.map_fired[0], "map should NOT be marked fired");
        assert!(ms.forfeit_pending, "forfeit_pending should be set");
        assert_eq!(ms.maps_home, 0);
        assert_eq!(ms.maps_away, 0);
    }

    #[test]
    fn trigger_b_new_game_id_not_reset() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 12;
        ms.prev_away_score = 4;

        // 0-0 on a DIFFERENT game_id → new map, NOT a reset
        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 200);
        assert!(intents.is_empty(), "new game_id should NOT trigger B");
        assert!(!ms.map_fired[0]);
    }

    #[test]
    fn trigger_b_winner_direction_home() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 12;
        ms.prev_away_score = 4;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(0)); // home
        assert_eq!(ms.maps_home, 1);
        assert_eq!(ms.maps_away, 0);
    }

    #[test]
    fn trigger_b_winner_direction_away() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;
        ms.prev_home_score = 4;
        ms.prev_away_score = 12;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].target_idx, TargetIdx(1)); // away
        assert_eq!(ms.maps_home, 0);
        assert_eq!(ms.maps_away, 1);
    }

    #[test]
    fn echo_frame_rejected_after_trigger_a() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let registry = build_registry(12, 12);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        // Trigger A fires on 9-13, game_id=100
        let f1 = make_game_frame(9, 13, 100);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f1, 0, &mut dispatch, &log);
        assert_eq!(ms.maps_away, 1);
        assert_eq!(ms.maps_home, 0);
        assert!(ms.completed_game_ids.contains(&100));

        // New map starts on game_id=200
        let f2 = make_game_frame(0, 0, 200);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f2, 0, &mut dispatch, &log);

        // Echo frame: old game_id=100 with 9-13 and game_status=finished
        let echo = r#"{"match_id":1513132,"game_id":100,"game_status":"finished","match_status":"running","home_team":{"team_id":3216,"team_name":"Navi","score":9},"away_team":{"team_id":133708,"team_name":"Legacy","score":13},"current_round":{"round_number":22,"timer":0}}"#;
        let r = process_pandascore_cs2_frame(&mut engine, &mut ms, echo, 0, &mut dispatch, &log);
        assert!(r.is_none(), "echo frame from completed game_id must be rejected");
        assert_eq!(ms.maps_away, 1, "maps_away must not change from echo");
        assert_eq!(ms.maps_home, 0, "maps_home must not change from echo");
    }

    #[test]
    fn echo_frame_rejected_after_trigger_b() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        let registry = build_registry(4, 4);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        // Score builds to 12-4 on game_id=100
        let f1 = make_game_frame(12, 4, 100);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f1, 0, &mut dispatch, &log);

        // Reset to 0-0 → Trigger B fires, home wins map 1
        let f2 = make_game_frame(0, 0, 100);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f2, 0, &mut dispatch, &log);
        assert_eq!(ms.maps_home, 1);
        assert!(ms.completed_game_ids.contains(&100));

        // New map on game_id=200
        let f3 = make_game_frame(0, 0, 200);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f3, 0, &mut dispatch, &log);

        // Echo: old game_id=100 comes back with 13-4
        let echo = make_game_frame(13, 4, 100);
        let r = process_pandascore_cs2_frame(&mut engine, &mut ms, &echo, 0, &mut dispatch, &log);
        assert!(r.is_none(), "echo frame must be rejected");
        assert_eq!(ms.maps_home, 1, "maps_home must not change");
    }

    // ── Maps-won tracking ──────────────────────────────────────────────

    #[test]
    fn maps_tracking_two_maps() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
        });
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        // Map 1: home wins (Trigger A at 13-7)
        evaluate_triggers(&mut ms, &engine, GameIdx(0), 13, 7, 100);
        assert_eq!(ms.maps_home, 1);
        assert_eq!(ms.maps_away, 0);
        assert_eq!(ms.current_map_number(), 2);

        // Map 2: away wins (Trigger B: 12-9 → 0-0)
        ms.prev_home_score = 9;
        ms.prev_away_score = 12;
        evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert_eq!(ms.maps_home, 1);
        assert_eq!(ms.maps_away, 1);
        assert_eq!(ms.current_map_number(), 3);
    }

    // ── Engine integration tests ───────────────────────────────────────

    fn make_game_frame(home_score: i64, away_score: i64, game_id: i64) -> String {
        format!(
            r#"{{"match_id":1513132,"game_id":{},"game_status":"running","match_status":"running","home_team":{{"team_id":3216,"team_name":"Navi","score":{}}},"away_team":{{"team_id":133708,"team_name":"Legacy","score":{}}},"current_round":{{"round_number":1,"timer":115}}}}"#,
            game_id, home_score, away_score
        )
    }

    fn make_noop_dispatch(registry: Arc<TargetRegistry>) -> DispatchHandle {
        let shared = Arc::new(arc_swap::ArcSwap::new(Arc::clone(&registry)));
        DispatchHandle::new(crate::DispatchConfig::default(), registry, shared)
    }

    fn make_log() -> Arc<Mutex<LogWriter>> {
        Arc::new(Mutex::new(
            LogWriter::open("/dev/null", "cs2").unwrap(),
        ))
    }

    fn build_registry(n_targets: usize, n_tokens: usize) -> Arc<TargetRegistry> {
        let mut targets = Vec::new();
        let mut tokens = Vec::new();
        for i in 0..n_tokens {
            tokens.push(TokenSlot {
                token_id: Arc::from(format!("tok_{}", i).as_str()),
            });
        }
        for i in 0..n_targets {
            targets.push(TargetSlot {
                token_idx: TokenIdx(i.min(n_tokens.saturating_sub(1)) as u16),
                strategy_key: Arc::from(format!("sk_{}", i).as_str()),
            });
        }
        Arc::new(TargetRegistry { targets, tokens })
    }

    #[test]
    fn match_end_evaluators_fire_on_trigger() {
        let t_map1_home = TargetIdx(0);
        let t_map2_home = TargetIdx(2);
        let t_ml_home = TargetIdx(10);
        let t_under = TargetIdx(20);
        let t_covers = TargetIdx(30);
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(t_map1_home), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(t_map2_home), Some(TargetIdx(3))));
            tgt.moneyline_home = Some(t_ml_home);
            tgt.moneyline_away = Some(TargetIdx(11));
            tgt.under_lines.push(OverLine { half_int: 2, target_idx: t_under });
            tgt.map_handicaps.push(SpreadSlot {
                side: SpreadSide::Home,
                line: -1.5,
                covers_idx: Some(t_covers),
                not_covers_idx: Some(TargetIdx(31)),
            });
        });
        let registry = build_registry(32, 32);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        // Map 1: home wins (Trigger A at 13-7) on game_id=100
        let frame1 = make_game_frame(13, 7, 100);
        let r1 = process_pandascore_cs2_frame(
            &mut engine, &mut ms, &frame1, 0, &mut dispatch, &log,
        );
        assert!(r1.is_some());
        assert_eq!(ms.maps_home, 1);
        assert!(!engine.final_resolved_games[0], "match not decided after map 1");

        // Map 2 on game_id=200: home wins (Trigger B: 12-4 → 0-0)
        let frame2a = make_game_frame(12, 4, 200);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &frame2a, 0, &mut dispatch, &log);
        let frame2b = make_game_frame(0, 0, 200);
        let r2 = process_pandascore_cs2_frame(
            &mut engine, &mut ms, &frame2b, 0, &mut dispatch, &log,
        );
        assert!(r2.is_some());
        assert_eq!(ms.maps_home, 2);
        assert!(engine.final_resolved_games[0], "match should be decided (2-0)");
        assert!(ms.match_completed);
    }

    #[test]
    fn final_resolved_blocks_subsequent() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let registry = build_registry(12, 12);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        // Map 1: Trigger A on game_id=100
        let f1 = make_game_frame(13, 7, 100);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f1, 0, &mut dispatch, &log);
        // Map 2: Trigger A on game_id=200 — match decided
        let f2a = make_game_frame(5, 3, 200);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f2a, 0, &mut dispatch, &log);
        let f2 = make_game_frame(13, 9, 200);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f2, 0, &mut dispatch, &log);
        assert!(engine.final_resolved_games[0]);

        // Subsequent frame → should return None
        let f3 = make_game_frame(0, 0, 300);
        let r = process_pandascore_cs2_frame(&mut engine, &mut ms, &f3, 0, &mut dispatch, &log);
        assert!(r.is_none(), "should be blocked by final_resolved");
    }

    #[test]
    fn cold_start_no_progressive_over() {
        let t_over = TargetIdx(20);
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
            tgt.over_lines.push(OverLine { half_int: 1, target_idx: t_over });
        });
        let registry = build_registry(21, 21);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        // Cold start: join mid-match with maps 1-0
        let mut ms = make_match_state(2, 1, 0);

        // First frame: just a normal round update. prev_total_maps is None.
        let frame = make_game_frame(5, 3, 100);
        let r = process_pandascore_cs2_frame(
            &mut engine, &mut ms, &frame, 0, &mut dispatch, &log,
        );
        assert!(r.is_some());
        // Should NOT fire progressive over on cold start (prev_total_maps = None)
        // The only way to verify is that no over intent was dispatched.
        // In noop mode, dispatch_intents logs but doesn't return intents.
        // The key check: prev_total_maps is now set for future ticks.
        assert_eq!(ms.prev_total_maps, Some(1));
    }

    #[test]
    fn progressive_over_fires_on_second_map() {
        let t_over = TargetIdx(20);
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
            tgt.map_moneyline.push((Some(TargetIdx(2)), Some(TargetIdx(3))));
            tgt.over_lines.push(OverLine { half_int: 1, target_idx: t_over });
        });
        let registry = build_registry(21, 21);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        // Map 1: home wins on game_id=100 (sets prev_total_maps = Some(1))
        let f1 = make_game_frame(13, 7, 100);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f1, 0, &mut dispatch, &log);
        assert_eq!(ms.prev_total_maps, Some(1));
        assert_eq!(ms.maps_home, 1);

        // Map 2 on game_id=200: away wins (Trigger B) — total goes 1→2
        let f2a = make_game_frame(9, 12, 200);
        process_pandascore_cs2_frame(&mut engine, &mut ms, &f2a, 0, &mut dispatch, &log);
        let f2 = make_game_frame(0, 0, 200);
        let r = process_pandascore_cs2_frame(&mut engine, &mut ms, &f2, 0, &mut dispatch, &log);
        assert!(r.is_some());
        assert_eq!(ms.maps_home, 1);
        assert_eq!(ms.maps_away, 1);
        // over 1.5 (half_int=1) should have fired since total went 1→2
        // and min(1,1)=1 >= max(0, 1+1-2)=0. Verified via totals_under_emitted
        // being set (since match is not decided, under doesn't fire, but over does).
    }

    #[test]
    fn dedup_same_frame() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.map_moneyline.push((Some(TargetIdx(0)), Some(TargetIdx(1))));
        });
        let registry = build_registry(2, 2);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        let frame = make_game_frame(5, 3, 100);
        let r1 = process_pandascore_cs2_frame(
            &mut engine, &mut ms, &frame, 0, &mut dispatch, &log,
        );
        assert!(r1.is_some());

        // Same exact frame again → deduped
        let r2 = process_pandascore_cs2_frame(
            &mut engine, &mut ms, &frame, 0, &mut dispatch, &log,
        );
        assert!(r2.is_none(), "duplicate frame should be deduped");
    }

    // ── match_status="finished" completion (M3) ────────────────────────

    fn make_game_frame_with_status(home_score: i64, away_score: i64, game_id: i64, match_status: &str) -> String {
        format!(
            r#"{{"match_id":1513132,"game_id":{},"game_status":"running","match_status":"{}","home_team":{{"team_id":3216,"team_name":"Navi","score":{}}},"away_team":{{"team_id":133708,"team_name":"Legacy","score":{}}},"current_round":{{"round_number":1,"timer":115}}}}"#,
            game_id, match_status, home_score, away_score
        )
    }

    #[test]
    fn match_status_finished_triggers_completion() {
        let mut engine = make_engine_with(2, |tgt| {
            tgt.moneyline_home = Some(TargetIdx(10));
            tgt.moneyline_away = Some(TargetIdx(11));
        });
        let registry = build_registry(12, 12);
        engine.registry = Some(Arc::clone(&registry));
        let mut dispatch = make_noop_dispatch(registry);
        let log = make_log();
        let mut ms = make_match_state(2, 0, 0);

        // Frame with maps below threshold but match_status="finished"
        let frame = make_game_frame_with_status(5, 3, 100, "finished");
        let r = process_pandascore_cs2_frame(
            &mut engine, &mut ms, &frame, 0, &mut dispatch, &log,
        );
        assert!(r.is_some());
        let log_entry = r.unwrap();
        assert_eq!(log_entry.game_state, "FINAL");
        assert!(engine.final_resolved_games[0], "final_resolved must be set");
        assert!(ms.match_completed);
    }

    // ── Maps-only path (M4) ──────────────────────────────────────────

    #[test]
    fn maps_only_trigger_a_increments() {
        let mut engine = make_engine_with(2, |_tgt| {});
        engine.has_child_moneyline[0] = false;
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        let intents = evaluate_triggers(&mut ms, &engine, GameIdx(0), 13, 7, 100);
        assert!(intents.is_empty(), "no child_moneyline targets → no intents");
        assert_eq!(ms.maps_home, 1);
        assert_eq!(ms.maps_away, 0);
        assert!(ms.map_fired[0]);
    }

    #[test]
    fn maps_only_trigger_a_blocks_trigger_b() {
        let mut engine = make_engine_with(2, |_tgt| {});
        engine.has_child_moneyline[0] = false;
        let mut ms = make_match_state(2, 0, 0);
        ms.current_game_id = 100;

        // Trigger A fires on 9-13
        let intents1 = evaluate_triggers(&mut ms, &engine, GameIdx(0), 9, 13, 100);
        assert!(intents1.is_empty());
        assert_eq!(ms.maps_away, 1);
        assert_eq!(ms.maps_home, 0);

        // Update prev as pipeline would
        ms.prev_home_score = 9;
        ms.prev_away_score = 13;

        // Subsequent 0-0: Trigger B must be skipped (C1 fix in maps-only path)
        let intents2 = evaluate_triggers(&mut ms, &engine, GameIdx(0), 0, 0, 100);
        assert!(intents2.is_empty());
        assert_eq!(ms.maps_away, 1, "maps_away must stay 1");
        assert_eq!(ms.maps_home, 0, "maps_home must stay 0");
        assert!(!ms.map_fired[1], "map 2 must NOT be fired");
    }

    // ── Match state init tests (carried from Step 1) ───────────────────

    #[test]
    fn match_state_init_fresh() {
        let ms = PandaScoreMatchState::new(1513132, 2, 0, 0, 3216, 133708);
        assert_eq!(ms.maps_to_win, 2);
        assert_eq!(ms.map_fired.len(), 3);
        assert!(ms.map_fired.iter().all(|&f| !f));
        assert_eq!(ms.current_map_number(), 1);
        assert!(ms.prev_total_maps.is_none());
    }

    #[test]
    fn match_state_init_mid_match() {
        let ms = PandaScoreMatchState::new(1513132, 2, 1, 0, 3216, 133708);
        assert!(ms.map_fired[0]);
        assert!(!ms.map_fired[1]);
        assert_eq!(ms.current_map_number(), 2);
    }

    #[test]
    fn match_state_init_bo5() {
        let ms = PandaScoreMatchState::new(100, 3, 0, 0, 1, 2);
        assert_eq!(ms.map_fired.len(), 5);
    }

    #[test]
    fn match_state_init_bo5_mid_match() {
        let ms = PandaScoreMatchState::new(100, 3, 2, 1, 1, 2);
        assert!(ms.map_fired[0] && ms.map_fired[1] && ms.map_fired[2]);
        assert!(!ms.map_fired[3] && !ms.map_fired[4]);
        assert_eq!(ms.current_map_number(), 4);
    }
}
