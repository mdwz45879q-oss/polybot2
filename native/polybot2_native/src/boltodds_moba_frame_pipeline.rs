//! BoltOdds MOBA (LoL/Dota2) frame pipeline: extract → engine tick → dispatch.
//! Thin glue layer — all logic lives in the extractor, engine, and
//! dispatch modules. Called per-frame from the BoltOdds WS worker.

use crate::boltodds_moba_types::{
    fast_extract_boltodds_moba, serde_extract_boltodds_moba, BoltOddsMobaExtract,
};
use crate::dispatch::{dispatch_intents, DispatchHandle};
use crate::log_writer::LogWriter;
use crate::moba::types::{MobaGameState, NativeMobaEngine};
use crate::GameIdx;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct BoltOddsMobaPendingLog {
    pub(crate) game_idx: GameIdx,
    pub(crate) state: MobaGameState,
}

/// Process a single BoltOdds MOBA frame. Returns a pending log entry
/// for the caller to flush after the drain loop, or `None` if the frame
/// was rejected (not new_play, unknown game, or duplicate).
pub(crate) fn process_boltodds_moba_frame_sync(
    engine: &mut NativeMobaEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<BoltOddsMobaPendingLog> {
    // Try fast byte-level extraction; fall back to serde on failure.
    if let Some(extract) = fast_extract_boltodds_moba(frame_text) {
        return process_extract(engine, &extract, recv_monotonic_ns, dispatch_handle, log);
    }
    // Serde fallback — resilient to field reordering and structural changes.
    if let Some(owned) = serde_extract_boltodds_moba(frame_text) {
        let extract = BoltOddsMobaExtract {
            game_label: &owned.game_label,
            maps_home: owned.maps_home,
            maps_away: owned.maps_away,
        };
        eprintln!(
            "[mux-bo-moba] serde fallback used (fast extract failed): {}",
            &frame_text[..frame_text.len().min(150)]
        );
        return process_extract(engine, &extract, recv_monotonic_ns, dispatch_handle, log);
    }
    // Neither path could extract — not a new_play or missing required fields.
    // Only log for frames that look like they SHOULD be data (contain "new_play").
    if frame_text.contains("new_play") {
        eprintln!(
            "[mux-bo-moba] extract failed (both paths): {}",
            &frame_text[..frame_text.len().min(200)]
        );
    }
    None
}

fn process_extract(
    engine: &mut NativeMobaEngine,
    extract: &BoltOddsMobaExtract<'_>,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<BoltOddsMobaPendingLog> {
    let maps_home_str = itoa::Buffer::new().format(extract.maps_home).to_string();
    let maps_away_str = itoa::Buffer::new().format(extract.maps_away).to_string();
    let gidx = match engine.check_duplicate(extract.game_label, &maps_home_str, &maps_away_str) {
        Some(g) => g,
        None => {
            // Silent None means either: unknown game label, or duplicate tick.
            // Log unknown labels (first occurrence only — duplicates are expected).
            if engine.game_id_to_idx.get(extract.game_label).is_none() {
                eprintln!("[mux-bo-moba] unknown game label: '{}'", extract.game_label);
            }
            return None;
        }
    };

    let result = engine.process_tick_live(
        gidx,
        extract.maps_home,
        extract.maps_away,
        false, // match_completed — MOBA detects via maps >= mtw
        "LIVE",
        recv_monotonic_ns,
    )?;

    if !result.intents.is_empty() {
        dispatch_intents(&result.intents, dispatch_handle, log);
    }

    Some(BoltOddsMobaPendingLog {
        game_idx: result.game_idx,
        state: result.state,
    })
}
