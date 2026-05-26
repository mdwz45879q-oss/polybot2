//! BoltOdds baseball frame pipeline: extract → engine tick → dispatch.
//! Thin glue layer — all logic lives in the extractor, engine, and
//! dispatch modules. Called per-frame from the WS worker.

use crate::baseball::types::{GameState, NativeMlbEngine};
use crate::boltodds_baseball_types::fast_extract_boltodds_baseball;
use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::log_writer::LogWriter;
use crate::{GameIdx, InlineStr};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct BoltOddsBaseballPendingLog {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: GameState,
    pub(crate) period_raw: InlineStr<32>,
}

/// Process a single BoltOdds baseball frame. Returns a pending log entry
/// for the caller to flush after the drain loop, or `None` if the frame
/// was rejected (not baseball, unknown game, or duplicate).
pub(crate) fn process_boltodds_baseball_frame_sync(
    engine: &mut NativeMlbEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<BoltOddsBaseballPendingLog> {
    let extract = fast_extract_boltodds_baseball(frame_text)?;
    let gidx = engine.check_boltodds_game(extract.game_label)?;
    let result = engine.process_boltodds_tick_live(
        gidx,
        extract.outs,
        extract.strikes,
        extract.inning,
        extract.top_of_inning,
        extract.home_score,
        extract.away_score,
        extract.base1,
        extract.base2,
        extract.base3,
        extract.period_detail,
        recv_monotonic_ns,
    )?;

    if !result.intents.is_empty() {
        dispatch_intents(&result.intents, dispatch_handle, log);
    }

    Some(BoltOddsBaseballPendingLog {
        game_idx: result.game_idx,
        state: result.state,
        period_raw: InlineStr::from_str(extract.period_detail),
    })
}
