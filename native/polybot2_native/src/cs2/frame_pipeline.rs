use crate::cs2::types::*;
use crate::dispatch::{DispatchHandle, SubmitBatch};
use crate::fast_extract;
use crate::log_writer::{LogWriter, TickPayload, gid_from_sk};
use crate::*;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct PendingTickLog {
    game_idx: GameIdx,
    state: Cs2GameState,
    period_raw: InlineStr<32>,
}

pub(crate) fn process_decoded_frame_sync(
    engine: &mut NativeCs2Engine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    let first_byte = frame_text.as_bytes().first().copied().unwrap_or(0);
    if first_byte == b'[' {
        if let Ok(frames) = serde_json::from_str::<Vec<crate::kalstrop_types::KalstropFrame<'_>>>(frame_text) {
            let mut batch: SubmitBatch = SubmitBatch::new();
            let mut pending_logs = smallvec::SmallVec::<[PendingTickLog; 4]>::new();
            for frame in &frames {
                if frame.msg_type != "next" {
                    continue;
                }
                let update = frame
                    .payload
                    .as_ref()
                    .and_then(|p| p.data.as_ref())
                    .and_then(|d| d.update.as_ref());
                if let Some(u) = update {
                    let summary = u.match_summary.as_ref();
                    let home_str = summary.and_then(|s| s.home_score).unwrap_or("");
                    let away_str = summary.and_then(|s| s.away_score).unwrap_or("");
                    let free_text = summary.and_then(|s| s.first_free_text).unwrap_or("");
                    if let Some(tl) = process_extracted_fields(
                        engine, u.fixture_id, home_str, away_str, free_text,
                        "", "", None, None, frame_text.as_bytes(), // rounds/phase/phases not available from serde
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
    } else if let Some(extract) = fast_extract::fast_extract_cs2_v1(frame_text) {
        let mut batch: SubmitBatch = SubmitBatch::new();
        let pending = process_extracted_fields(
            engine,
            extract.fixture_id,
            extract.maps_home,
            extract.maps_away,
            extract.free_text,
            extract.rounds_home,
            extract.rounds_away,
            extract.current_phase,
            extract.phases_offset,
            frame_text.as_bytes(),
            recv_monotonic_ns,
            dispatch_handle,
            log,
            &mut batch,
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
    engine: &NativeCs2Engine,
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
            let lg = engine
                .game_leagues
                .get(tl.game_idx.0 as usize)
                .map(|s| s.as_ref())
                .unwrap_or("");
            g.log_tick(
                game_id,
                &TickPayload::Cs2 {
                    lg,
                    maps_home: tl.state.maps_home.unwrap_or(0),
                    maps_away: tl.state.maps_away.unwrap_or(0),
                    rounds_home: tl.state.rounds_home,
                    rounds_away: tl.state.rounds_away,
                    current_map: tl.state.current_map,
                    gs: tl.state.game_state,
                    src: "kalstrop_v1",
                },
            );
        }
    }
}

fn process_extracted_fields(
    engine: &mut NativeCs2Engine,
    fixture_id: &str,
    maps_home_str: &str,
    maps_away_str: &str,
    free_text: &str,
    rounds_home_str: &str,
    rounds_away_str: &str,
    current_phase: Option<i64>,
    phases_offset: Option<usize>,
    frame_bytes: &[u8],
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
    batch: &mut SubmitBatch,
) -> Option<PendingTickLog> {
    let Some(gidx) = engine.check_duplicate(
        fixture_id, maps_home_str, maps_away_str, free_text, rounds_home_str, rounds_away_str,
    ) else {
        return None;
    };

    let maps_home = fast_extract::fast_parse_score(maps_home_str).unwrap_or(0);
    let maps_away = fast_extract::fast_parse_score(maps_away_str).unwrap_or(0);
    let rounds_home = fast_extract::fast_parse_score(rounds_home_str).unwrap_or(0);
    let rounds_away = fast_extract::fast_parse_score(rounds_away_str).unwrap_or(0);
    let current_map = current_phase.unwrap_or(0);
    let match_completed = if free_text.is_empty() {
        false
    } else {
        free_text.trim().eq_ignore_ascii_case("Closed")
    };
    let game_state: &'static str = if free_text.is_empty() {
        "UNKNOWN"
    } else if match_completed {
        "FINAL"
    } else {
        "LIVE"
    };

    // Deferred phases scan: only scan when Signal 2 or pending verification
    // needs the data (~1% of ticks). Saves ~200-400ns on the other ~99%.
    let no_phases = [(-1i64, -1i64); 5];
    let phase_scores = if engine.needs_phase_scores(gidx, maps_home, maps_away) {
        if let Some(offset) = phases_offset {
            fast_extract::scan_phases_cs2(frame_bytes, offset)
        } else {
            no_phases
        }
    } else {
        no_phases
    };

    let result = engine.process_tick_live(
        gidx,
        maps_home,
        maps_away,
        rounds_home,
        rounds_away,
        current_map,
        match_completed,
        game_state,
        &phase_scores,
        recv_monotonic_ns,
    )?;

    // Dispatch intents
    if matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
        for intent in &result.intents {
            let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
            if let Ok(mut g) = log.lock() {
                g.log_order_ok(gid_from_sk(sk), sk, tok, "noop", "");
            }
        }
    } else {
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
    }

    Some(PendingTickLog {
        game_idx: result.game_idx,
        state: result.state,
        period_raw: InlineStr::from_str(free_text),
    })
}
