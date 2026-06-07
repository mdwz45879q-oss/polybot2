//! BoltOdds baseball frame pipeline: extract → engine tick → dispatch.
//! Thin glue layer — all logic lives in the extractor, engine, and
//! dispatch modules. Called per-frame from the WS worker.

use crate::baseball::types::{GameState, NativeMlbEngine};
use crate::boltodds_baseball_types::{
    fast_extract_boltodds_baseball, serde_extract_boltodds_baseball, BoltOddsBaseballExtract,
};
use crate::dispatch::{DispatchHandle, SubmitBatch};
use crate::log_writer::{gid_from_sk, LogWriter};
use crate::{DispatchMode, GameIdx, InlineStr};
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
    clock_origin: Instant,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<BoltOddsBaseballPendingLog> {
    // Try fast byte-level extraction; fall back to serde on failure.
    if let Some(extract) = fast_extract_boltodds_baseball(frame_text) {
        let recv_monotonic_ns = clock_origin.elapsed().as_nanos() as i64;
        return process_extract(engine, &extract, recv_monotonic_ns, dispatch_handle, log);
    }
    // Serde fallback — resilient to field reordering and structural changes.
    if let Some(owned) = serde_extract_boltodds_baseball(frame_text) {
        let extract = BoltOddsBaseballExtract {
            game_label: &owned.game_label,
            outs: owned.outs,
            strikes: owned.strikes,
            inning: owned.inning,
            top_of_inning: owned.top_of_inning,
            home_score: owned.home_score,
            away_score: owned.away_score,
            period_detail: &owned.period_detail,
            base1: owned.base1,
            base2: owned.base2,
            base3: owned.base3,
        };
        let recv_monotonic_ns = clock_origin.elapsed().as_nanos() as i64;
        return process_extract(engine, &extract, recv_monotonic_ns, dispatch_handle, log);
    }
    // Neither path could extract — not a match_update or missing required fields.
    None
}

fn process_extract(
    engine: &mut NativeMlbEngine,
    extract: &BoltOddsBaseballExtract<'_>,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) -> Option<BoltOddsBaseballPendingLog> {
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
        if matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
            for intent in &result.intents {
                let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
                if let Ok(mut g) = log.lock() {
                    g.log_order_ok(gid_from_sk(sk), sk, tok, "noop", "");
                }
            }
        } else {
            let mut batch = SubmitBatch::new();
            for intent in &result.intents {
                match dispatch_handle.pop_for_target(intent.target_idx) {
                    Ok(orders) => {
                        for signed in orders {
                            batch.push((intent.target_idx, signed));
                        }
                    }
                    Err(err) => {
                        let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
                        if let Ok(mut g) = log.lock() {
                            g.log_order_err(gid_from_sk(sk), sk, tok, &err, "");
                        }
                    }
                }
            }
            if !batch.is_empty() {
                dispatch_handle.send_batch(batch, log);
            }
        }
    }

    Some(BoltOddsBaseballPendingLog {
        game_idx: result.game_idx,
        state: result.state,
        period_raw: InlineStr::from_str(extract.period_detail),
    })
}
