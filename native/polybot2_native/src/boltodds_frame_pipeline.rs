//! BoltOdds soccer frame processing pipeline.
//! Parses BoltOdds WS frames via the byte-level extractor, deduplicates,
//! maps period strings, and dispatches through the soccer engine.

use crate::boltodds_types::fast_extract_boltodds;
use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::log_writer::LogWriter;
use crate::soccer::types::{NativeSoccerEngine, SoccerGameState};
use crate::GameIdx;
use std::sync::{Arc, Mutex};

/// Deferred tick log entry, flushed after the WS drain loop.
#[derive(Clone, Copy)]
pub(crate) struct BoltOddsPendingLog {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: SoccerGameState,
    pub(crate) half: &'static str,
    pub(crate) game_state: &'static str,
}

pub(crate) fn process_boltodds_frame_sync(
    engine: &mut NativeSoccerEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<BoltOddsPendingLog> {
    let extract = fast_extract_boltodds(frame_text)?;

    // Dedup + game index resolve in one FxHashMap lookup.
    let gidx = engine.check_boltodds_dedup(
        extract.game_label,
        extract.goals_a,
        extract.goals_b,
        extract.corners_a,
        extract.corners_b,
        extract.match_period_detail,
    )?;

    // Update BoltOdds dedup state by index (no hash lookup).
    engine.update_boltodds_row_indexed(
        gidx,
        extract.goals_a,
        extract.goals_b,
        extract.corners_a,
        extract.corners_b,
        extract.match_period_detail,
    );

    // Map BoltOdds period string to the engine's half notation.
    let half: &'static str = match extract.match_period_detail {
        "IN_FIRST_HALF" => "1st half",
        "AT_HALF_TIME" => "Halftime",
        "IN_SECOND_HALF" => "2nd half",
        "AT_FULL_TIME" | "MATCH_COMPLETED" => "Ended",
        _ => "",
    };
    let match_completed = matches!(
        extract.match_period_detail,
        "AT_FULL_TIME" | "MATCH_COMPLETED"
    );
    let game_state: &'static str = if match_completed {
        "FINAL"
    } else if half.is_empty() {
        "UNKNOWN"
    } else {
        "LIVE"
    };

    // Drive the soccer engine's tick evaluation using GameIdx (no hash lookup).
    let result = engine.process_tick_live(
        gidx,
        "", // no raw home score string (BoltOdds provides integers directly)
        "", // no raw away score string
        "", // BoltOdds uses integer-based dedup, not V1 string dedup
        Some(extract.goals_a),
        Some(extract.goals_b),
        Some(extract.corners_a),
        Some(extract.corners_b),
        half,
        Some(match_completed),
        game_state,
        recv_monotonic_ns,
    )?;

    dispatch_intents(&result.intents, dispatch_handle, log);

    // Return pending log (flushed by ws_boltodds after drain loop).
    Some(BoltOddsPendingLog {
        game_idx: result.game_idx,
        state: result.state,
        half,
        game_state,
    })
}
