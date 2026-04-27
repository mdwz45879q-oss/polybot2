use super::*;
use crate::dispatch::{DispatchHandle, SubmitBatch};
use crate::engine::process_kalstrop_frame;
use crate::log_writer::LogWriter;
use std::sync::{Arc, Mutex};

/// Process a decoded WS frame: parse → evaluate → pop presigned orders for all
/// intents in the frame → send one `SubmitWork::Batch` covering the frame →
/// log ticks. Frame-level batching guarantees that multiple intents from the
/// same provider frame reach the CLOB as one `POST /orders` call (modulo
/// `MAX_BATCH_SIZE=15` chunking on the submitter side).
pub(crate) fn process_decoded_frame_sync(
    engine: &mut NativeMlbEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    let results = process_kalstrop_frame(engine, frame_text, recv_monotonic_ns);

    if matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
        // Noop mode: log a synthetic `noop` outcome for every intent inline.
        // No presign pop, no channel send. Tick logging follows.
        for r in &results {
            if !r.material {
                continue;
            }
            for intent in &r.intents {
                let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
                if let Ok(mut g) = log.lock() {
                    g.log_order_ok(sk, tok, "noop");
                }
            }
        }
    } else {
        // Http mode: collect a single Batch per frame, send it once.
        let mut batch: SubmitBatch = SubmitBatch::new();
        for r in &results {
            if !r.material {
                continue;
            }
            for intent in &r.intents {
                match dispatch_handle.pop_for_target(intent.target_idx) {
                    Ok(signed) => batch.push((intent.target_idx, signed)),
                    Err(err) => {
                        let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
                        if let Ok(mut g) = log.lock() {
                            g.log_order_err(sk, tok, &err);
                        }
                    }
                }
            }
        }
        if !batch.is_empty() {
            dispatch_handle.send_batch(batch, log);
        }
    }

    // Tick logging after dispatch handoff. One lock acquisition for the whole
    // result set.
    if !results.is_empty() {
        if let Ok(mut g) = log.lock() {
            for r in &results {
                if r.material {
                    g.log_tick(
                        &r.game_id,
                        r.state.home,
                        r.state.away,
                        r.state.inning_number,
                        r.state.inning_half,
                        r.state.game_state,
                    );
                }
            }
        }
    }
}
