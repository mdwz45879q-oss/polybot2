//! Soccer frame pipeline: zero-alloc live WS path.

use crate::*;
use crate::soccer::types::*;
use crate::soccer::parse::{parse_half, is_completed_free_text};
use crate::dispatch::{DispatchHandle, SubmitBatch};
use crate::kalstrop_types::KalstropFrame;
use crate::log_writer::LogWriter;
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
    let first_byte = frame_text.as_bytes().first().copied().unwrap_or(0);
    if first_byte == b'[' {
        if let Ok(frames) = serde_json::from_str::<Vec<KalstropFrame<'_>>>(frame_text) {
            let mut batch: SubmitBatch = SubmitBatch::new();
            let mut pending_logs = smallvec::SmallVec::<[PendingTickLog; 4]>::new();
            for frame in &frames {
                if let Some(tl) = process_single_frame_live(
                    engine, frame, recv_monotonic_ns, dispatch_handle, log, &mut batch,
                ) {
                    pending_logs.push(tl);
                }
            }
            if !batch.is_empty() && !matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
                dispatch_handle.send_batch(batch, log);
            }
            flush_tick_logs(engine, &pending_logs, log);
        }
    } else if let Ok(frame) = serde_json::from_str::<KalstropFrame<'_>>(frame_text) {
        let mut batch: SubmitBatch = SubmitBatch::new();
        let pending = process_single_frame_live(
            engine, &frame, recv_monotonic_ns, dispatch_handle, log, &mut batch,
        );
        if !batch.is_empty() && !matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
            dispatch_handle.send_batch(batch, log);
        }
        if let Some(tl) = pending {
            flush_tick_logs(engine, &[tl], log);
        }
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

fn process_single_frame_live(
    engine: &mut NativeSoccerEngine,
    frame: &KalstropFrame<'_>,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
    batch: &mut SubmitBatch,
) -> Option<PendingTickLog> {
    if frame.msg_type != "next" {
        return None;
    }
    let update = frame
        .payload
        .as_ref()
        .and_then(|p| p.data.as_ref())
        .and_then(|d| d.update.as_ref())?;

    // Extract raw strings from borrowed KalstropUpdate.
    let fixture_id = update.fixture_id;
    let summary = update.match_summary.as_ref();
    let home_str = summary.and_then(|s| s.home_score).unwrap_or("");
    let away_str = summary.and_then(|s| s.away_score).unwrap_or("");
    let free_text = summary
        .and_then(|s| s.first_free_text)
        .unwrap_or("");

    // Pre-parse dedup + game index resolve in one FxHashMap lookup.
    let gidx = engine.check_duplicate(fixture_id, home_str, away_str, free_text)?;

    // THEN parse: half, scores, completion status.
    let half = parse_half(free_text);
    let goals_home = crate::fast_extract::fast_parse_score(home_str);
    let goals_away = crate::fast_extract::fast_parse_score(away_str);
    let is_completed = if free_text.is_empty() { false } else { is_completed_free_text(free_text) };
    let match_completed = if free_text.is_empty() { None } else { Some(is_completed) };
    let game_state: &'static str = if free_text.is_empty() { "UNKNOWN" } else if is_completed { "FINAL" } else { "LIVE" };

    let result = engine.process_tick_live(
        gidx,
        home_str,
        away_str,
        free_text,
        goals_home,
        goals_away,
        None, // corners_home (V1 doesn't provide corner data)
        None, // corners_away
        half,
        match_completed,
        game_state,
        recv_monotonic_ns,
    )?;

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

    Some(PendingTickLog {
        game_idx: result.game_idx,
        state: result.state,
    })
}
