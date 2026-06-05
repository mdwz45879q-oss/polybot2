//! Integration tests: real captured frames → byte extractor → engine → intents.
//!
//! These tests verify the full extraction-to-evaluation pipeline using real
//! JSON frames captured from live provider WebSocket feeds. Each test loads
//! a minimal plan, feeds raw frame text through the same extractor + parser +
//! dedup + engine sequence that the production frame pipeline uses, and
//! asserts that the correct intents fire.
//!
//! This closes audit findings H3/H4: all prior engine tests called
//! `process_tick_live` directly with manually constructed integers, so
//! extractor bugs (wrong field, off-by-one, broken nesting) were invisible.

#![cfg(test)]

use crate::baseball::parse::parse_period;
use crate::baseball::types::NativeMlbEngine;
use crate::fast_extract::{self, fast_parse_score};
use crate::soccer::parse::parse_half;
use crate::soccer::types::NativeSoccerEngine;
use crate::tennis::types::NativeTennisEngine;
use crate::Intent;

// ---------------------------------------------------------------------------
// Plan builder helpers (local copies — can't import from engine #[cfg(test)])
// ---------------------------------------------------------------------------

fn target_json(token_id: &str, semantic: &str, strategy_key: &str) -> String {
    format!(
        r#"{{"outcome_index":0,"token_id":"{}","outcome_label":"lbl","outcome_semantic":"{}","strategy_key":"{}"}}"#,
        token_id, semantic, strategy_key
    )
}

fn market_json(market_type: &str, line: Option<f64>, targets: &[String]) -> String {
    let targets_str = targets.join(",");
    let line_str = match line {
        Some(l) => format!("{}", l),
        None => "null".to_string(),
    };
    format!(
        r#"{{"condition_id":"cond_1","market_id":"mid_1","event_id":"eid_1","sports_market_type":"{}","line":{},"question":"test","targets":[{}]}}"#,
        market_type, line_str, targets_str
    )
}

fn plan_json_one_game(game_id: &str, markets_json: &str) -> String {
    format!(
        r#"{{"games":[{{"provider_game_id":"{}","canonical_league":"test","kickoff_ts_utc":1700000000,"markets":[{}]}}]}}"#,
        game_id, markets_json
    )
}

fn plan_json_one_game_tennis(game_id: &str, markets_json: &str, sets_to_win: i64) -> String {
    format!(
        r#"{{"games":[{{"provider_game_id":"{}","canonical_league":"test","kickoff_ts_utc":1700000000,"sets_to_win":{},"markets":[{}]}}]}}"#,
        game_id, sets_to_win, markets_json
    )
}

// ---------------------------------------------------------------------------
// Pipeline helpers: extract → parse → dedup → engine, returning intents
// ---------------------------------------------------------------------------

fn baseball_v1_tick(
    engine: &mut NativeMlbEngine,
    frame_text: &str,
    ns: i64,
) -> Option<Vec<Intent>> {
    let extract = fast_extract::fast_extract_v1(frame_text)?;
    let gidx = engine.check_duplicate(
        extract.fixture_id,
        extract.home_score,
        extract.away_score,
        extract.free_text,
    )?;
    let (inning_number, inning_half) = parse_period(extract.free_text);
    let goals_home = fast_parse_score(extract.home_score);
    let goals_away = fast_parse_score(extract.away_score);
    let is_completed = if extract.free_text.is_empty() {
        false
    } else {
        extract.free_text.trim().eq_ignore_ascii_case("Ended")
    };
    let match_completed = if extract.free_text.is_empty() {
        None
    } else {
        Some(is_completed)
    };
    let game_state: &'static str = if extract.free_text.is_empty() {
        "UNKNOWN"
    } else if is_completed {
        "FINAL"
    } else {
        "LIVE"
    };
    let result = engine.process_tick_live(
        gidx, extract.home_score, extract.away_score, extract.free_text,
        goals_home, goals_away, inning_number, inning_half,
        match_completed, game_state, ns,
    )?;
    Some(result.intents.to_vec())
}

fn soccer_v1_tick(
    engine: &mut NativeSoccerEngine,
    frame_text: &str,
    ns: i64,
) -> Option<Vec<Intent>> {
    let extract = fast_extract::fast_extract_v1(frame_text)?;
    let gidx = engine.check_duplicate(
        extract.fixture_id,
        extract.home_score,
        extract.away_score,
        extract.free_text,
    )?;
    let half = parse_half(extract.free_text);
    let goals_home = fast_parse_score(extract.home_score);
    let goals_away = fast_parse_score(extract.away_score);
    let corners_home = extract.corners_home;
    let corners_away = extract.corners_away;
    let is_completed = if extract.free_text.is_empty() {
        false
    } else {
        extract.free_text.trim().eq_ignore_ascii_case("Ended")
    };
    let match_completed = if extract.free_text.is_empty() {
        None
    } else {
        Some(is_completed)
    };
    let game_state: &'static str = if extract.free_text.is_empty() {
        "UNKNOWN"
    } else if is_completed {
        "FINAL"
    } else {
        "LIVE"
    };
    let result = engine.process_tick_live(
        gidx, extract.home_score, extract.away_score, extract.free_text,
        goals_home, goals_away, corners_home, corners_away,
        half, match_completed, game_state, ns,
    )?;
    Some(result.intents.to_vec())
}

fn soccer_boltodds_tick(
    engine: &mut NativeSoccerEngine,
    frame_text: &str,
    ns: i64,
) -> Option<Vec<Intent>> {
    let extract = crate::boltodds_types::fast_extract_boltodds(frame_text)?;
    let gidx = engine.check_boltodds_dedup(
        extract.game_label,
        extract.goals_a,
        extract.goals_b,
        extract.corners_a,
        extract.corners_b,
        extract.match_period_detail,
    )?;
    engine.update_boltodds_row_indexed(
        gidx,
        extract.goals_a,
        extract.goals_b,
        extract.corners_a,
        extract.corners_b,
        extract.match_period_detail,
    );
    let half: &'static str = match extract.match_period_detail {
        "IN_FIRST_HALF" => "1st half",
        "AT_HALF_TIME" => "Halftime",
        "IN_SECOND_HALF" => "2nd half",
        "AT_FULL_TIME" => "Ended",
        _ => "",
    };
    let match_completed = matches!(
        extract.match_period_detail,
        "AT_FULL_TIME"
    );
    let game_state: &'static str = if match_completed {
        "FINAL"
    } else if half.is_empty() {
        "UNKNOWN"
    } else {
        "LIVE"
    };
    let result = engine.process_tick_live(
        gidx, "", "", "",
        Some(extract.goals_a), Some(extract.goals_b),
        Some(extract.corners_a), Some(extract.corners_b),
        half, Some(match_completed), game_state, ns,
    )?;
    Some(result.intents.to_vec())
}

fn soccer_v2_tick(
    engine: &mut NativeSoccerEngine,
    frame_text: &str,
    ns: i64,
) -> Option<Vec<Intent>> {
    let extract = crate::kalstrop_v2_types::fast_extract_v2(frame_text)?;
    let gidx = engine.check_boltodds_dedup(
        extract.fixture_id,
        extract.home_score,
        extract.away_score,
        0, 0,
        extract.current_phase,
    )?;
    engine.update_boltodds_row_indexed(
        gidx,
        extract.home_score,
        extract.away_score,
        0, 0,
        extract.current_phase,
    );
    let (half, match_completed) = crate::kalstrop_v2_types::map_v2_phase(extract.current_phase);
    let game_state: &'static str = if match_completed {
        "FINAL"
    } else if half.is_empty() {
        "UNKNOWN"
    } else {
        "LIVE"
    };
    let result = engine.process_tick_live(
        gidx, "", "", "",
        Some(extract.home_score), Some(extract.away_score),
        None, None,
        half, Some(match_completed), game_state, ns,
    )?;
    Some(result.intents.to_vec())
}

fn tennis_v1_tick(
    engine: &mut NativeTennisEngine,
    frame_text: &str,
    ns: i64,
) -> Option<Vec<Intent>> {
    let extract = fast_extract::fast_extract_tennis_v1(frame_text)?;
    let gidx = engine.check_duplicate(
        extract.fixture_id,
        extract.sets_home,
        extract.sets_away,
        extract.games_home,
        extract.games_away,
        extract.free_text,
    )?;
    let sets_home = fast_parse_score(extract.sets_home)?;
    let sets_away = fast_parse_score(extract.sets_away)?;
    let games_home = fast_parse_score(extract.games_home).unwrap_or(0);
    let games_away = fast_parse_score(extract.games_away).unwrap_or(0);
    let total_sets = sets_home + sets_away;
    let current_set = extract.current_phase.unwrap_or(0);
    let first_set_completed = total_sets >= 1;
    let match_completed = extract.free_text.trim().eq_ignore_ascii_case("Ended");
    let game_state: &'static str = if match_completed { "FINAL" } else { "LIVE" };
    let first_set_games = if first_set_completed || total_sets == 0 {
        Some(extract.first_set_games)
    } else {
        None
    };
    let total_games = extract.total_games;
    let result = engine.process_tick_live(
        gidx,
        extract.sets_home, extract.sets_away,
        extract.games_home, extract.games_away,
        extract.free_text,
        sets_home, sets_away, games_home, games_away,
        total_games, first_set_games, total_sets, current_set,
        match_completed, first_set_completed, None, game_state, ns,
    )?;
    Some(result.intents.to_vec())
}

// ---------------------------------------------------------------------------
// Real captured frame constants
// ---------------------------------------------------------------------------

// Baseball V1 — fixture 9cf4bbb8-05ca-43d6-9f99-0fef042ddc5c
const BASEBALL_V1_MID_GAME: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"ba10e953-37aa-4f0f-94d4-6f0cce14130d","fixtureId":"9cf4bbb8-05ca-43d6-9f99-0fef042ddc5c","streamExists":true,"matchSummary":{"matchStatusDisplay":[{"freeText":"3rd inning top"}],"homeScore":"0","awayScore":"1","possession":null,"currentPhase":null,"phases":[{"phase":1,"homeScore":"0","awayScore":"0"},{"phase":2,"homeScore":"0","awayScore":"0"},{"phase":3,"homeScore":"0","awayScore":"1"}],"statistics":null,"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":null,"clockRunning":null,"providerMessageTimestamp":"2026-05-21T17:49:56.320Z"}}}}}"#;

const BASEBALL_V1_ENDED: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"ba10e953-37aa-4f0f-94d4-6f0cce14130d","fixtureId":"9cf4bbb8-05ca-43d6-9f99-0fef042ddc5c","streamExists":true,"matchSummary":{"matchStatusDisplay":[{"freeText":"Ended"}],"homeScore":"1","awayScore":"3","possession":null,"currentPhase":null,"phases":[{"phase":1,"homeScore":"0","awayScore":"0"},{"phase":2,"homeScore":"0","awayScore":"0"},{"phase":3,"homeScore":"0","awayScore":"2"},{"phase":4,"homeScore":"0","awayScore":"0"},{"phase":5,"homeScore":"0","awayScore":"0"},{"phase":6,"homeScore":"0","awayScore":"0"},{"phase":7,"homeScore":"0","awayScore":"0"},{"phase":8,"homeScore":"1","awayScore":"1"},{"phase":9,"homeScore":"0","awayScore":"0"}],"statistics":null,"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":null,"clockRunning":null,"providerMessageTimestamp":"2026-05-21T19:51:16.052Z"}}}}}"#;

// Baseball V1 baseline — same fixture, 0-0, "1st inning top"
const BASEBALL_V1_BASELINE: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"ba10e953-37aa-4f0f-94d4-6f0cce14130d","fixtureId":"9cf4bbb8-05ca-43d6-9f99-0fef042ddc5c","streamExists":true,"matchSummary":{"matchStatusDisplay":[{"freeText":"1st inning top"}],"homeScore":"0","awayScore":"0","possession":null,"currentPhase":null,"phases":[{"phase":1,"homeScore":"0","awayScore":"0"}],"statistics":null,"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":null,"clockRunning":null,"providerMessageTimestamp":"2026-05-21T17:00:00.000Z"}}}}}"#;

// Soccer V1 — goal frame: fixture d900a26f-1c01-4eea-a1ef-9507ae11af5a
const SOCCER_V1_GOAL: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"c79e807f-3ef7-4509-bd21-5d14820d8c15","fixtureId":"d900a26f-1c01-4eea-a1ef-9507ae11af5a","streamExists":false,"matchSummary":{"matchStatusDisplay":[{"freeText":"1st half"}],"homeScore":"0","awayScore":"1","possession":null,"currentPhase":{"phase":1,"homeScore":"0","awayScore":"1"},"phases":[{"phase":1,"homeScore":"0","awayScore":"1"}],"statistics":{"corners":{"home":0,"away":2},"redCards":{"home":0,"away":0},"yellowCards":{"home":0,"away":1},"yellowRedCards":{"home":0,"away":0}},"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":1671000,"clockRunning":true,"providerMessageTimestamp":"2026-05-14T18:28:53.638Z"}}}}}"#;

// Soccer V1 baseline — same fixture as goal frame, 0-0
const SOCCER_V1_BASELINE_GOAL: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"c79e807f-3ef7-4509-bd21-5d14820d8c15","fixtureId":"d900a26f-1c01-4eea-a1ef-9507ae11af5a","streamExists":false,"matchSummary":{"matchStatusDisplay":[{"freeText":"1st half"}],"homeScore":"0","awayScore":"0","possession":null,"currentPhase":{"phase":1,"homeScore":"0","awayScore":"0"},"phases":[{"phase":1,"homeScore":"0","awayScore":"0"}],"statistics":{"corners":{"home":0,"away":0},"redCards":{"home":0,"away":0},"yellowCards":{"home":0,"away":0},"yellowRedCards":{"home":0,"away":0}},"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":600000,"clockRunning":true,"providerMessageTimestamp":"2026-05-14T18:10:00.000Z"}}}}}"#;

// Soccer V1 — ended frame: fixture a0b0f6a2-7836-4139-baa1-7f6d1d8c37d9 (different game)
const SOCCER_V1_ENDED: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"a2c946d8-8446-4b60-a1a8-5a6e9721006a","fixtureId":"a0b0f6a2-7836-4139-baa1-7f6d1d8c37d9","streamExists":false,"matchSummary":{"matchStatusDisplay":[{"freeText":"Ended"}],"homeScore":"1","awayScore":"1","possession":null,"currentPhase":null,"phases":[{"phase":1,"homeScore":"1","awayScore":"1"},{"phase":2,"homeScore":"0","awayScore":"0"}],"statistics":{"corners":{"home":5,"away":1},"redCards":{"home":0,"away":0},"yellowCards":{"home":1,"away":1},"yellowRedCards":{"home":0,"away":0}},"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":null,"clockRunning":null,"providerMessageTimestamp":"2026-05-14T19:00:33.686Z"}}}}}"#;

// Soccer V1 baseline for ended-frame fixture
const SOCCER_V1_BASELINE_ENDED: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"a2c946d8-8446-4b60-a1a8-5a6e9721006a","fixtureId":"a0b0f6a2-7836-4139-baa1-7f6d1d8c37d9","streamExists":false,"matchSummary":{"matchStatusDisplay":[{"freeText":"1st half"}],"homeScore":"0","awayScore":"0","possession":null,"currentPhase":{"phase":1,"homeScore":"0","awayScore":"0"},"phases":[{"phase":1,"homeScore":"0","awayScore":"0"}],"statistics":{"corners":{"home":0,"away":0},"redCards":{"home":0,"away":0},"yellowCards":{"home":0,"away":0},"yellowRedCards":{"home":0,"away":0}},"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":600000,"clockRunning":true,"providerMessageTimestamp":"2026-05-14T17:10:00.000Z"}}}}}"#;

// BoltOdds — game label "Valencia vs Rayo Vallecano, 2026-05-14, 01"
const BOLTODDS_GOAL: &str = r#"{"action":"match_update","game":"Valencia vs Rayo Vallecano, 2026-05-14, 01","universal_id":"432278261fd9","home":"Valencia","away":"Rayo Vallecano","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"clockRunningNow":true,"matchPeriod":["FootballMatchPeriod","IN_FIRST_HALF"],"elapsedTimeSeconds":1212,"goalsA":0,"goalsB":1,"cornersA":0,"cornersB":1,"yellowCardsA":1,"yellowCardsB":0,"redCardsA":0,"redCardsB":0,"firstHalfGoalsA":0,"firstHalfGoalsB":1,"secondHalfGoalsA":0,"secondHalfGoalsB":0,"varReferralInProgress":false,"matchTime":"20:12","clockStatus":"SET_PERIOD_START","clockRunning":true}}"#;

const BOLTODDS_COMPLETED: &str = r#"{"action":"match_update","game":"Valencia vs Rayo Vallecano, 2026-05-14, 01","universal_id":"432278261fd9","home":"Valencia","away":"Rayo Vallecano","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":true,"clockRunningNow":false,"matchPeriod":["FootballMatchPeriod","AT_FULL_TIME"],"elapsedTimeSeconds":5400,"goalsA":1,"goalsB":1,"cornersA":5,"cornersB":1,"yellowCardsA":1,"yellowCardsB":1,"redCardsA":0,"redCardsB":0,"firstHalfGoalsA":1,"firstHalfGoalsB":1,"secondHalfGoalsA":0,"secondHalfGoalsB":0,"varReferralInProgress":false,"matchTime":"90:00","addedPeriodTime":"0:34","clockStatus":"SET_PERIOD_START","clockRunning":false}}"#;

const BOLTODDS_BASELINE: &str = r#"{"action":"match_update","game":"Valencia vs Rayo Vallecano, 2026-05-14, 01","universal_id":"432278261fd9","home":"Valencia","away":"Rayo Vallecano","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"clockRunningNow":true,"matchPeriod":["FootballMatchPeriod","IN_FIRST_HALF"],"elapsedTimeSeconds":60,"goalsA":0,"goalsB":0,"cornersA":0,"cornersB":0,"yellowCardsA":0,"yellowCardsB":0,"redCardsA":0,"redCardsB":0,"firstHalfGoalsA":0,"firstHalfGoalsB":0,"secondHalfGoalsA":0,"secondHalfGoalsB":0,"varReferralInProgress":false,"matchTime":"1:00","clockStatus":"SET_PERIOD_START","clockRunning":true}}"#;

// V2 — fixture 12562654
const V2_GOAL: &str = r#"{"data":{"betGeniusFixtureId":"12562654","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"FirstHalf","penaltiesPhase":false,"phase":"20' ","awayScore":1,"homeScore":0}}}"#;

const V2_FULLTIME: &str = r#"{"data":{"betGeniusFixtureId":"12562654","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"FullTimeNormalTime","penaltiesPhase":false,"phase":null,"awayScore":1,"homeScore":1}}}"#;

const V2_BASELINE: &str = r#"{"data":{"betGeniusFixtureId":"12562654","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"FirstHalf","penaltiesPhase":false,"phase":"1' ","awayScore":0,"homeScore":0}}}"#;

// Tennis V1 — fixture 85282be8-635b-4c12-90b0-f8eea1237b4e (BO5)
// Mid-game: sets 2-2, 5th set 3-1. total_games = (3+6)+(6+4)+(6+2)+(6+7)+(3+1) = 44
const TENNIS_V1_MID_GAME: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"d4b78a66-2a87-46f3-8b09-38bb1626d62b","fixtureId":"85282be8-635b-4c12-90b0-f8eea1237b4e","streamExists":true,"matchSummary":{"matchStatusDisplay":[{"freeText":"5th set"}],"homeScore":"2","awayScore":"2","possession":"AWAY","currentPhase":{"phase":5,"homeScore":"3","awayScore":"1","homeGameScore":"0","awayGameScore":"0"},"phases":[{"phase":1,"homeScore":"3","awayScore":"6"},{"phase":2,"homeScore":"6","awayScore":"4"},{"phase":3,"homeScore":"6","awayScore":"2"},{"phase":4,"homeScore":"6","awayScore":"7"},{"phase":5,"homeScore":"3","awayScore":"1","homeGameScore":"0","awayGameScore":"0"}],"statistics":null,"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":null,"clockRunning":null,"providerMessageTimestamp":"2026-05-24T15:49:49.507Z"}}}}}"#;

// Ended: sets 3-2 (home wins). total_games = (3+6)+(6+4)+(6+2)+(6+7)+(6+4) = 50
const TENNIS_V1_ENDED: &str = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"id":"d4b78a66-2a87-46f3-8b09-38bb1626d62b","fixtureId":"85282be8-635b-4c12-90b0-f8eea1237b4e","streamExists":true,"matchSummary":{"matchStatusDisplay":[{"freeText":"Ended"}],"homeScore":"3","awayScore":"2","possession":null,"currentPhase":null,"phases":[{"phase":1,"homeScore":"3","awayScore":"6"},{"phase":2,"homeScore":"6","awayScore":"4"},{"phase":3,"homeScore":"6","awayScore":"2"},{"phase":4,"homeScore":"6","awayScore":"7"},{"phase":5,"homeScore":"6","awayScore":"4"}],"statistics":null,"timeRemaining":null,"stoppageTime":null,"stoppageTimeAnnounced":null,"timeElapsed":null,"clockRunning":null,"providerMessageTimestamp":"2026-05-24T16:15:22.753Z"}}}}}"#;

// ===========================================================================
// Test cases
// ===========================================================================

// ── Baseball V1 ─────────────────────────────────────────────────────────

#[test]
fn integration_baseball_v1_over_crossing() {
    let mut engine = NativeMlbEngine::new();
    let t = target_json("tok_over", "over", "g1:TOTAL:OVER:0.5");
    let m = market_json("totals", Some(0.5), &[t]);
    let plan = plan_json_one_game("9cf4bbb8-05ca-43d6-9f99-0fef042ddc5c", &m);
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: 0-0
    let _ = baseball_v1_tick(&mut engine, BASEBALL_V1_BASELINE, 1000);
    // Real frame: 0-1 → crosses 0.5
    let intents = baseball_v1_tick(&mut engine, BASEBALL_V1_MID_GAME, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 1, "over 0.5 should fire on 0-1");
}

#[test]
fn integration_baseball_v1_completion() {
    let mut engine = NativeMlbEngine::new();
    let t_under = target_json("tok_under", "under", "g1:TOTAL:UNDER:8.5");
    let t_ml = target_json("tok_ml_away", "away", "g1:MONEYLINE:AWAY");
    let m_totals = market_json("totals", Some(8.5), &[t_under]);
    let m_ml = market_json("moneyline", None, &[t_ml]);
    let plan = plan_json_one_game(
        "9cf4bbb8-05ca-43d6-9f99-0fef042ddc5c",
        &format!("{},{}", m_totals, m_ml),
    );
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: mid-game (0-1)
    let _ = baseball_v1_tick(&mut engine, BASEBALL_V1_MID_GAME, 1000);
    // Ended: 1-3 (total=4 < 8.5 → under fires; away wins → moneyline fires)
    let intents = baseball_v1_tick(&mut engine, BASEBALL_V1_ENDED, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 2, "under 8.5 + moneyline_away should fire");
}

// ── Soccer V1 ───────────────────────────────────────────────────────────

#[test]
fn integration_soccer_v1_over_crossing() {
    let mut engine = NativeSoccerEngine::new();
    let t = target_json("tok_over", "over", "g1:TOTAL:OVER:0.5");
    let m = market_json("totals", Some(0.5), &[t]);
    let plan = plan_json_one_game("d900a26f-1c01-4eea-a1ef-9507ae11af5a", &m);
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: 0-0
    let _ = soccer_v1_tick(&mut engine, SOCCER_V1_BASELINE_GOAL, 1000);
    // Real goal frame: 0-1 → crosses 0.5
    let intents = soccer_v1_tick(&mut engine, SOCCER_V1_GOAL, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 1, "over 0.5 should fire on 0-1 goal");
}

#[test]
fn integration_soccer_v1_completion() {
    let mut engine = NativeSoccerEngine::new();
    let t_under = target_json("tok_under", "under", "g1:TOTAL:UNDER:2.5");
    let t_draw = target_json("tok_draw", "draw_yes", "g1:MONEYLINE:DRAW_YES");
    let t_corner = target_json("tok_corner", "under", "g1:CORNERS:UNDER:8.5");
    let m_totals = market_json("totals", Some(2.5), &[t_under]);
    let m_ml = market_json("moneyline", None, &[t_draw]);
    let m_corners = market_json("total_corners", Some(8.5), &[t_corner]);
    let plan = plan_json_one_game(
        "a0b0f6a2-7836-4139-baa1-7f6d1d8c37d9",
        &format!("{},{},{}", m_totals, m_ml, m_corners),
    );
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: 0-0
    let _ = soccer_v1_tick(&mut engine, SOCCER_V1_BASELINE_ENDED, 1000);
    // Ended: 1-1 (total=2 < 2.5, draw, corners 5+1=6 < 8.5)
    let intents = soccer_v1_tick(&mut engine, SOCCER_V1_ENDED, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 3, "under 2.5 + draw_yes + corner under 8.5 should fire");
}

// ── Soccer BoltOdds ─────────────────────────────────────────────────────

#[test]
fn integration_soccer_boltodds_over_crossing() {
    let mut engine = NativeSoccerEngine::new();
    let t = target_json("tok_over", "over", "g1:TOTAL:OVER:0.5");
    let m = market_json("totals", Some(0.5), &[t]);
    let plan = plan_json_one_game("Valencia vs Rayo Vallecano, 2026-05-14, 01", &m);
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: 0-0
    let _ = soccer_boltodds_tick(&mut engine, BOLTODDS_BASELINE, 1000);
    // Real goal: 0-1 → crosses 0.5
    let intents = soccer_boltodds_tick(&mut engine, BOLTODDS_GOAL, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 1, "over 0.5 should fire on BoltOdds 0-1");
}

#[test]
fn integration_soccer_boltodds_completion() {
    let mut engine = NativeSoccerEngine::new();
    let t_under = target_json("tok_under", "under", "g1:TOTAL:UNDER:2.5");
    let t_draw = target_json("tok_draw", "draw_yes", "g1:MONEYLINE:DRAW_YES");
    let t_corner = target_json("tok_corner", "under", "g1:CORNERS:UNDER:8.5");
    let m_totals = market_json("totals", Some(2.5), &[t_under]);
    let m_ml = market_json("moneyline", None, &[t_draw]);
    let m_corners = market_json("total_corners", Some(8.5), &[t_corner]);
    let plan = plan_json_one_game(
        "Valencia vs Rayo Vallecano, 2026-05-14, 01",
        &format!("{},{},{}", m_totals, m_ml, m_corners),
    );
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: goal frame (0-1)
    let _ = soccer_boltodds_tick(&mut engine, BOLTODDS_GOAL, 1000);
    // AT_FULL_TIME: 1-1 (total=2, draw, corners 5+1=6)
    let intents = soccer_boltodds_tick(&mut engine, BOLTODDS_COMPLETED, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 3, "under 2.5 + draw_yes + corner under 8.5 should fire");
}

// ── Soccer V2 ───────────────────────────────────────────────────────────

#[test]
fn integration_soccer_v2_over_crossing() {
    let mut engine = NativeSoccerEngine::new();
    let t = target_json("tok_over", "over", "g1:TOTAL:OVER:0.5");
    let m = market_json("totals", Some(0.5), &[t]);
    let plan = plan_json_one_game("12562654", &m);
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: 0-0
    let _ = soccer_v2_tick(&mut engine, V2_BASELINE, 1000);
    // Real goal: 0-1 → crosses 0.5
    let intents = soccer_v2_tick(&mut engine, V2_GOAL, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 1, "over 0.5 should fire on V2 0-1");
}

#[test]
fn integration_soccer_v2_completion() {
    let mut engine = NativeSoccerEngine::new();
    let t_under = target_json("tok_under", "under", "g1:TOTAL:UNDER:2.5");
    let t_draw = target_json("tok_draw", "draw_yes", "g1:MONEYLINE:DRAW_YES");
    let m_totals = market_json("totals", Some(2.5), &[t_under]);
    let m_ml = market_json("moneyline", None, &[t_draw]);
    let plan = plan_json_one_game(
        "12562654",
        &format!("{},{}", m_totals, m_ml),
    );
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: goal frame (0-1)
    let _ = soccer_v2_tick(&mut engine, V2_GOAL, 1000);
    // FullTimeNormalTime: 1-1 (total=2, draw)
    let intents = soccer_v2_tick(&mut engine, V2_FULLTIME, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 2, "under 2.5 + draw_yes should fire");
}

// ── Tennis V1 ───────────────────────────────────────────────────────────

#[test]
fn integration_tennis_v1_moneyline() {
    let mut engine = NativeTennisEngine::new();
    let t = target_json("tok_ml_home", "home", "g1:MONEYLINE:HOME");
    let m = market_json("moneyline", None, &[t]);
    let plan = plan_json_one_game_tennis(
        "85282be8-635b-4c12-90b0-f8eea1237b4e", &m, 3,
    );
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: mid-game (sets 2-2, establishes prev)
    let _ = tennis_v1_tick(&mut engine, TENNIS_V1_MID_GAME, 1000);
    // Ended frame: sets 3-2 (home wins → moneyline fires)
    let intents = tennis_v1_tick(&mut engine, TENNIS_V1_ENDED, 2000)
        .expect("should produce intents");
    assert!(
        intents.iter().any(|i| i.target_idx.0 == 0),
        "moneyline_home should fire (sets went from 2-2 to 3-2)"
    );
}

#[test]
fn integration_tennis_v1_completion() {
    let mut engine = NativeTennisEngine::new();
    let t_ml = target_json("tok_ml_home", "home", "g1:MONEYLINE:HOME");
    let t_under = target_json("tok_under", "under", "g1:SET_TOTAL:UNDER:5.5");
    let m_ml = market_json("moneyline", None, &[t_ml]);
    let m_totals = market_json("tennis_set_totals", Some(5.5), &[t_under]);
    let plan = plan_json_one_game_tennis(
        "85282be8-635b-4c12-90b0-f8eea1237b4e",
        &format!("{},{}", m_ml, m_totals),
        3,
    );
    engine.load_plan_from_json(&plan).unwrap();

    // Baseline: mid-game (sets 2-2, establishes prev)
    let _ = tennis_v1_tick(&mut engine, TENNIS_V1_MID_GAME, 1000);
    // Ended: sets 3-2 (home wins → moneyline; total_sets=5 < 5.5 → under fires)
    let intents = tennis_v1_tick(&mut engine, TENNIS_V1_ENDED, 2000)
        .expect("should produce intents");
    assert_eq!(intents.len(), 2, "moneyline_home + set total under 5.5 should fire");
}
