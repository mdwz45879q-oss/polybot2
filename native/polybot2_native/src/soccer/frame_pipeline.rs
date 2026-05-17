//! Soccer V1 frame pipeline: zero-alloc live WS path via byte extractor.

use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::fast_extract;
use crate::log_writer::LogWriter;
use crate::parse_common::is_completed_free_text;
use crate::soccer::parse::parse_half;
use crate::soccer::types::*;
use crate::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
struct PendingTickLog {
    game_idx: GameIdx,
    state: SoccerGameState,
}

pub(crate) fn process_decoded_frame_sync(
    engine: &mut NativeSoccerEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    let Some(extract) = fast_extract::fast_extract_v1(frame_text) else {
        return;
    };
    let pending = process_extracted_fields(
        engine,
        &extract,
        recv_monotonic_ns,
        dispatch_handle,
        log,
    );
    if let Some(tl) = pending {
        flush_tick_logs(engine, &[tl], log);
    }
}

fn flush_tick_logs(
    engine: &NativeSoccerEngine,
    pending: &[PendingTickLog],
    log: &Arc<Mutex<LogWriter>>,
) {
    if pending.is_empty() {
        return;
    }
    if let Ok(mut g) = log.lock() {
        for tl in pending {
            let game_id = engine
                .game_ids
                .get(tl.game_idx.0 as usize)
                .map(|s| s.as_str())
                .unwrap_or("_");
            g.log_tick(
                game_id,
                tl.state.home,
                tl.state.away,
                None, // no inning number for soccer
                tl.state.half,
                tl.state.game_state,
                tl.state.total_corners,
            );
        }
    }
}

fn process_extracted_fields(
    engine: &mut NativeSoccerEngine,
    extract: &fast_extract::V1Extract<'_>,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<PendingTickLog> {
    // Pre-parse dedup + game index resolve in one FxHashMap lookup.
    let gidx = engine.check_duplicate(
        extract.fixture_id,
        extract.home_score,
        extract.away_score,
        extract.free_text,
    )?;

    // Parse: half, scores, completion status, corners.
    let half = parse_half(extract.free_text);
    let goals_home = fast_extract::fast_parse_score(extract.home_score);
    let goals_away = fast_extract::fast_parse_score(extract.away_score);
    let corners_home = extract.corners_home;
    let corners_away = extract.corners_away;
    let is_completed = if extract.free_text.is_empty() {
        false
    } else {
        is_completed_free_text(extract.free_text)
    };
    let match_completed = if extract.free_text.is_empty() {
        None
    } else {
        Some(is_completed)
    };
    let game_state: &'static str = if extract.free_text.is_empty() {
        "UNKNOWN"
    } else if is_completed {
        "FINAL"
    } else {
        "LIVE"
    };

    let result = engine.process_tick_live(
        gidx,
        extract.home_score,
        extract.away_score,
        extract.free_text,
        goals_home,
        goals_away,
        corners_home,
        corners_away,
        half,
        match_completed,
        game_state,
        recv_monotonic_ns,
    )?;

    dispatch_intents(&result.intents, dispatch_handle, log);

    Some(PendingTickLog {
        game_idx: result.game_idx,
        state: result.state,
    })
}
