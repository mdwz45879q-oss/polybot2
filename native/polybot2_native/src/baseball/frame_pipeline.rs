use crate::*;
use crate::baseball::types::*;
use crate::dispatch::{DispatchHandle, SubmitBatch};
use crate::fast_extract;
use crate::kalstrop_types::KalstropFrame;
use crate::log_writer::LogWriter;
use crate::baseball::parse::{is_completed_free_text, parse_period};
use std::sync::{Arc, Mutex};

/// Compact tick-log record collected during frame processing and flushed
/// after dispatch. All fields are Copy — no heap allocation.
#[derive(Clone, Copy)]
struct PendingTickLog {
    game_idx: GameIdx,
    state: GameState,
}

/// Process a decoded WS frame through the zero-allocation live path:
/// fast_extract (single frames) or serde parse (batch frames) → extract
/// borrowed fields → process_tick_live (no Tick/TickResult/Vec) → pop
/// presigned orders → send one Batch → log ticks (after dispatch).
pub(crate) fn process_decoded_frame_sync(
    engine: &mut NativeMlbEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    let first_byte = frame_text.as_bytes().first().copied().unwrap_or(0);
    if first_byte == b'[' {
        if let Ok(frames) = serde_json::from_str::<Vec<KalstropFrame<'_>>>(frame_text) {
            let mut batch: SubmitBatch = SubmitBatch::new();
            let mut pending_logs = smallvec::SmallVec::<[PendingTickLog; 4]>::new();
            for frame in &frames {
                if frame.msg_type != "next" { continue; }
                let update = frame.payload.as_ref()
                    .and_then(|p| p.data.as_ref())
                    .and_then(|d| d.update.as_ref());
                if let Some(u) = update {
                    let summary = u.match_summary.as_ref();
                    let home_str = summary.and_then(|s| s.home_score).unwrap_or("");
                    let away_str = summary.and_then(|s| s.away_score).unwrap_or("");
                    let free_text = summary.and_then(|s| s.first_free_text).unwrap_or("");
                    if let Some(tl) = process_extracted_fields(
                        engine, u.fixture_id, home_str, away_str, free_text,
                        recv_monotonic_ns, dispatch_handle, log, &mut batch,
                    ) {
                        pending_logs.push(tl);
                    }
                }
            }
            if !batch.is_empty() && !matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
                dispatch_handle.send_batch(batch, log);
            }
            flush_tick_logs(engine, &pending_logs, log);
        }
    } else if let Some(extract) = fast_extract::fast_extract_v1(frame_text) {
        let mut batch: SubmitBatch = SubmitBatch::new();
        let pending = process_extracted_fields(
            engine, extract.fixture_id, extract.home_score, extract.away_score, extract.free_text,
            recv_monotonic_ns, dispatch_handle, log, &mut batch,
        );
        if !batch.is_empty() && !matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
            dispatch_handle.send_batch(batch, log);
        }
        if let Some(tl) = pending {
            flush_tick_logs(engine, &[tl], log);
        }
    }
}

/// Flush collected tick-log records. Called after send_batch so dispatch
/// is never blocked by logging.
fn flush_tick_logs(
    engine: &NativeMlbEngine,
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
                tl.state.inning_number,
                tl.state.inning_half,
                tl.state.game_state,
                None, // no corners for baseball
            );
        }
    }
}

/// Process pre-extracted fields through the live path. Handles dedup,
/// parsing, engine evaluation, and dispatch. Returns a pending tick-log
/// record if the frame was material, for the caller to flush after
/// send_batch.
fn process_extracted_fields(
    engine: &mut NativeMlbEngine,
    fixture_id: &str,
    home_str: &str,
    away_str: &str,
    free_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
    batch: &mut SubmitBatch,
) -> Option<PendingTickLog> {
    // Pre-parse dedup + game index resolve in one lookup.
    let Some(gidx) = engine.check_duplicate(fixture_id, home_str, away_str, free_text) else {
        return None;
    };

    // THEN parse: period, scores, completion status.
    let (inning_number, inning_half) = parse_period(free_text);
    let goals_home: Option<i64> = if home_str.is_empty() { None } else { home_str.parse().ok() };
    let goals_away: Option<i64> = if away_str.is_empty() { None } else { away_str.parse().ok() };
    let is_completed = if free_text.is_empty() { false } else { is_completed_free_text(free_text) };
    let match_completed = if free_text.is_empty() { None } else { Some(is_completed) };
    let game_state: &'static str = if free_text.is_empty() { "UNKNOWN" } else if is_completed { "FINAL" } else { "LIVE" };

    // Process through engine path (no dedup inside — already handled above).
    let result = engine.process_tick_live(
        gidx,
        home_str,
        away_str,
        free_text,
        goals_home,
        goals_away,
        inning_number,
        inning_half,
        match_completed,
        game_state,
        recv_monotonic_ns,
    )?;

    if !result.material {
        return None;
    }

    // Dispatch intents.
    if matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
        for intent in &result.intents {
            let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
            if let Ok(mut g) = log.lock() {
                g.log_order_ok(sk, tok, "noop");
            }
        }
    } else {
        for intent in &result.intents {
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

    // Return tick-log data for the caller to flush after send_batch.
    Some(PendingTickLog {
        game_idx: result.game_idx,
        state: result.state,
    })
}
