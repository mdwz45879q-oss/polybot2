//! Tennis V1 frame pipeline: zero-alloc live WS path via byte extractor.

use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::fast_extract::{self, fast_parse_score};
use crate::log_writer::LogWriter;
use crate::tennis::types::NativeTennisEngine;
use std::sync::{Arc, Mutex};

pub(crate) fn process_decoded_frame_sync(
    engine: &mut NativeTennisEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    let extract = match fast_extract::fast_extract_tennis_v1(frame_text) {
        Some(e) => e,
        None => return,
    };

    let gidx = match engine.check_duplicate(
        extract.fixture_id,
        extract.sets_home,
        extract.sets_away,
        extract.games_home,
        extract.games_away,
        extract.free_text,
    ) {
        Some(g) => g,
        None => return,
    };

    // Parse scores — sets must parse; games may be empty at match end (currentPhase: null).
    let sets_home = match fast_parse_score(extract.sets_home) {
        Some(v) => v,
        None => return,
    };
    let sets_away = match fast_parse_score(extract.sets_away) {
        Some(v) => v,
        None => return,
    };
    let games_home = fast_parse_score(extract.games_home).unwrap_or(0);
    let games_away = fast_parse_score(extract.games_away).unwrap_or(0);

    // Compute derived values
    let total_sets = sets_home + sets_away;
    let current_set = extract.current_phase.unwrap_or(0);
    let first_set_completed = total_sets >= 1;
    // Tennis-specific: only "Ended" is a valid completion signal.
    // "Interrupted" is a temporary suspension (rain/darkness) — NOT match end.
    // "Finished" is not observed in Kalstrop V1 tennis frames.
    let match_completed = extract.free_text.trim().eq_ignore_ascii_case("Ended");
    let game_state: &'static str = if match_completed { "FINAL" } else { "LIVE" };
    // During set 1 (total_sets == 0), phases[0] IS the live first-set game count.
    // After set 1 completes (total_sets >= 1), phases[0] is the frozen final value.
    // Both cases: pass Some(extract.first_set_games).
    let first_set_games = if first_set_completed || total_sets == 0 {
        Some(extract.first_set_games)
    } else {
        None
    };
    let total_games = extract.total_games;

    // Process tick
    let result = engine.process_tick_live(
        gidx,
        extract.sets_home,
        extract.sets_away,
        extract.games_home,
        extract.games_away,
        extract.free_text,
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
        game_state,
        recv_monotonic_ns,
    );

    if let Some(tick_result) = result {
        if !tick_result.intents.is_empty() {
            dispatch_intents(&tick_result.intents, dispatch_handle, log);
        }

        // Log tick
        let game_id = engine
            .game_ids
            .get(tick_result.game_idx.0 as usize)
            .map(|s| s.as_str())
            .unwrap_or("_");
        let lg = engine.game_leagues.get(tick_result.game_idx.0 as usize).map(|s| s.as_ref()).unwrap_or("");
        if let Ok(mut g) = log.lock() {
            g.log_tick(
                game_id,
                &crate::log_writer::TickPayload::Tennis {
                    lg,
                    sets_home: tick_result.state.sets_home,
                    sets_away: tick_result.state.sets_away,
                    games_home: tick_result.state.games_home,
                    games_away: tick_result.state.games_away,
                    total_games: tick_result.state.total_games,
                    half: extract.free_text,
                    gs: game_state,
                },
            );
        }
    }
}
