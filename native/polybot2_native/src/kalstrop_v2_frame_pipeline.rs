//! Kalstrop V2 soccer frame processing pipeline.
//! Processes genius_update frames via the byte-level extractor, deduplicates
//! using the shared BoltOdds dedup (with corners=0), maps V2 phase strings,
//! and dispatches through the soccer engine.

use crate::kalstrop_v2_types::{fast_extract_v2, map_v2_phase};
use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::log_writer::LogWriter;
use crate::soccer::types::{NativeSoccerEngine, SoccerGameState};
use crate::GameIdx;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
pub(crate) struct V2PendingLog {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: SoccerGameState,
    pub(crate) half: &'static str,
    pub(crate) game_state: &'static str,
}

pub(crate) fn process_v2_frame_sync(
    engine: &mut NativeSoccerEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<V2PendingLog> {
    let extract = fast_extract_v2(frame_text)?;

    let gidx = engine.check_boltodds_dedup(
        extract.fixture_id,
        extract.home_score,
        extract.away_score,
        0, // V2 has no corner data
        0,
        extract.current_phase,
    )?;

    engine.update_boltodds_row_indexed(
        gidx,
        extract.home_score,
        extract.away_score,
        0,
        0,
        extract.current_phase,
    );

    let (half, match_completed) = map_v2_phase(extract.current_phase);
    let game_state: &'static str = if match_completed {
        "FINAL"
    } else if half.is_empty() {
        "UNKNOWN"
    } else {
        "LIVE"
    };

    let result = engine.process_tick_live(
        gidx,
        "",
        "",
        "",
        Some(extract.home_score),
        Some(extract.away_score),
        None, // no corners_home
        None, // no corners_away
        half,
        Some(match_completed),
        game_state,
        recv_monotonic_ns,
    )?;

    dispatch_intents(&result.intents, dispatch_handle, log);

    Some(V2PendingLog {
        game_idx: result.game_idx,
        state: result.state,
        half,
        game_state,
    })
}
