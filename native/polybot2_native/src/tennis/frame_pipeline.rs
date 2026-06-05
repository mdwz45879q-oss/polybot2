//! Tennis V1 frame pipeline: zero-alloc live WS path via byte extractor.

use crate::dispatch::{dispatch_intents, DispatchHandle, SubmitBatch};
use crate::fast_extract::{self, fast_parse_score};
use crate::log_writer::{gid_from_sk, LogWriter};
use crate::tennis::types::NativeTennisEngine;
use crate::DispatchMode;
use std::sync::{Arc, Mutex};

/// Returns the winning side ("home"/"away") if freeText is a retirement signal.
/// Exact match against 4 normalized strings — no fuzzy matching, no regex.
/// A mistake here is catastrophic (wrong winner → wrong bets), so the match
/// set is deliberately closed and exhaustive.
fn detect_retirement(free_text: &str) -> Option<&'static str> {
    let mut buf = [0u8; 64];
    let trimmed = free_text.trim().as_bytes();
    if trimmed.len() > buf.len() {
        return None;
    }
    // lowercase into stack buffer (no allocation)
    for (i, &b) in trimmed.iter().enumerate() {
        buf[i] = b.to_ascii_lowercase();
    }
    let norm = std::str::from_utf8(&buf[..trimmed.len()]).ok()?;
    match norm {
        "player 1 retired, player 2 won" | "player 2 won, player 1 retired" => Some("away"),
        "player 2 retired, player 1 won" | "player 1 won, player 2 retired" => Some("home"),
        _ => None,
    }
}

pub(crate) fn process_decoded_frame_sync(
    engine: &mut NativeTennisEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    let extract = match fast_extract::fast_extract_tennis_v1(frame_text) {
        Some(e) => e,
        None => return,
    };

    let gidx = match engine.check_duplicate(
        extract.fixture_id,
        extract.sets_home,
        extract.sets_away,
        extract.games_home,
        extract.games_away,
        extract.free_text,
    ) {
        Some(g) => g,
        None => return,
    };

    // Parse scores — sets must parse; games may be empty at match end (currentPhase: null).
    let sets_home = match fast_parse_score(extract.sets_home) {
        Some(v) => v,
        None => return,
    };
    let sets_away = match fast_parse_score(extract.sets_away) {
        Some(v) => v,
        None => return,
    };
    let games_home = fast_parse_score(extract.games_home).unwrap_or(0);
    let games_away = fast_parse_score(extract.games_away).unwrap_or(0);

    // Compute derived values
    let total_sets = sets_home + sets_away;
    let current_set = extract.current_phase.unwrap_or(0);
    let first_set_completed = total_sets >= 1;
    // Retirement detection: exact match against 4 normalized strings.
    // Must be checked BEFORE match_completed — retirement is a separate path.
    let retirement_winner = detect_retirement(extract.free_text);

    // Tennis-specific: only "Ended" is a valid completion signal.
    // "Interrupted" is a temporary suspension (rain/darkness) — NOT match end.
    // Retirement is NOT normal completion — it takes a separate path in the engine.
    let match_completed = if retirement_winner.is_some() {
        false
    } else {
        extract.free_text.trim().eq_ignore_ascii_case("Ended")
    };
    let game_state: &'static str = if match_completed || retirement_winner.is_some() {
        "FINAL"
    } else {
        "LIVE"
    };
    // During set 1 (total_sets == 0), phases[0] IS the live first-set game count.
    // After set 1 completes (total_sets >= 1), phases[0] is the frozen final value.
    // Both cases: pass Some(extract.first_set_games).
    let first_set_games = if first_set_completed || total_sets == 0 {
        Some(extract.first_set_games)
    } else {
        None
    };
    let total_games = extract.total_games;

    // Process tick
    let result = engine.process_tick_live(
        gidx,
        extract.sets_home,
        extract.sets_away,
        extract.games_home,
        extract.games_away,
        extract.free_text,
        sets_home,
        sets_away,
        games_home,
        games_away,
        total_games,
        first_set_games,
        total_sets,
        current_set,
        match_completed,
        first_set_completed,
        retirement_winner,
        game_state,
        recv_monotonic_ns,
    );

    if let Some(tick_result) = result {
        if !tick_result.intents.is_empty() {
            if tick_result.normal_intent_count < tick_result.intents.len() {
                // Retirement detected: split intents between pools.
                // First `normal_intent_count` intents are moneyline → normal pool.
                // Remaining intents are 50/50 → retirement pool.
                if matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
                    for intent in &tick_result.intents {
                        let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
                        if let Ok(mut g) = log.lock() {
                            g.log_order_ok(gid_from_sk(sk), sk, tok, "noop", "");
                        }
                    }
                } else {
                    let mut batch = SubmitBatch::new();
                    for (i, intent) in tick_result.intents.iter().enumerate() {
                        if i < tick_result.normal_intent_count {
                            // Moneyline → normal pool
                            match dispatch_handle.pop_for_target(intent.target_idx) {
                                Ok(orders) => {
                                    for o in orders {
                                        batch.push((intent.target_idx, o));
                                    }
                                }
                                Err(err) => {
                                    let (sk, tok) = dispatch_handle.resolve_strings(intent.target_idx);
                                    if let Ok(mut g) = log.lock() {
                                        g.log_order_err(gid_from_sk(sk), sk, tok, &err, "");
                                    }
                                }
                            }
                        } else {
                            // 50/50 → retirement pool
                            match dispatch_handle.pop_for_target_retirement(intent.target_idx) {
                                Ok(orders) => {
                                    for o in orders {
                                        batch.push((intent.target_idx, o));
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
                    if !batch.is_empty() {
                        dispatch_handle.send_batch(batch, log);
                    }
                }
            } else {
                // Normal dispatch — all intents use normal pool.
                dispatch_intents(&tick_result.intents, dispatch_handle, log);
            }
        }

        // Log tick
        let game_id = engine
            .game_ids
            .get(tick_result.game_idx.0 as usize)
            .map(|s| s.as_str())
            .unwrap_or("_");
        let lg = engine.game_leagues.get(tick_result.game_idx.0 as usize).map(|s| s.as_ref()).unwrap_or("");
        if let Ok(mut g) = log.lock() {
            g.log_tick(
                game_id,
                &crate::log_writer::TickPayload::Tennis {
                    lg,
                    sets_home: tick_result.state.sets_home,
                    sets_away: tick_result.state.sets_away,
                    games_home: tick_result.state.games_home,
                    games_away: tick_result.state.games_away,
                    total_games: tick_result.state.total_games,
                    half: extract.free_text,
                    gs: game_state,
                },
            );
        }
    }
}
