//! Byte-level field extractor for BoltOdds baseball match_update frames.
//! Scans raw JSON for the fields needed by the baseball engine and returns
//! borrowed slices. Returns None for non-match_update frames or missing fields.

pub(crate) struct BoltOddsBaseballExtract<'a> {
    pub game_label: &'a str,
    pub outs: u8,
    pub strikes: u8,
    pub inning: i64,
    pub top_of_inning: bool,
    pub home_score: i64,
    pub away_score: i64,
    pub period_detail: &'a str,
    pub base1: bool,
    pub base2: bool,
    pub base3: bool,
}

use memchr::memmem::Finder;
use std::sync::LazyLock;

static FINDER_MATCH_UPDATE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"match_update\""));
static FINDER_GAME: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"game\""));
static FINDER_OUT: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"out\""));
static FINDER_INNING: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"inning\""));
static FINDER_STRIKE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"strike\""));
static FINDER_BASE1: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"base1\""));
static FINDER_BASE2: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"base2\""));
static FINDER_BASE3: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"base3\""));
static FINDER_TOP_OF_INNING: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"topOfInning\""));
static FINDER_MATCH_PERIOD: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"matchPeriod\""));
static FINDER_TOTAL_RUNS_A: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"totalRunsForTeamA\""));
static FINDER_TOTAL_RUNS_B: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"totalRunsForTeamB\""));

fn find_with(finder: &Finder, haystack: &[u8], from: usize) -> Option<usize> {
    if from >= haystack.len() {
        return None;
    }
    finder.find(&haystack[from..]).map(|pos| pos + from)
}

fn extract_string_value(bytes: &[u8], start: usize) -> Option<(&[u8], usize)> {
    let mut pos = start;
    while pos < bytes.len() {
        if bytes[pos] == b'\\' {
            pos += 2;
            continue;
        }
        if bytes[pos] == b'"' {
            return Some((&bytes[start..pos], pos + 1));
        }
        pos += 1;
    }
    None
}

fn find_key_string_start(
    finder: &Finder,
    key_len: usize,
    bytes: &[u8],
    from: usize,
) -> Option<usize> {
    let mut pos = from;
    while pos + key_len < bytes.len() {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                if p < bytes.len() && bytes[p] == b'"' {
                    return Some(p + 1);
                }
            }
            pos = idx + 1;
        } else {
            break;
        }
    }
    None
}

fn find_key_integer(
    finder: &Finder,
    key_len: usize,
    bytes: &[u8],
    from: usize,
) -> Option<(i64, usize)> {
    let mut pos = from;
    while pos + key_len < bytes.len() {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                let negative = p < bytes.len() && bytes[p] == b'-';
                if negative {
                    p += 1;
                }
                let start = p;
                let mut acc: i64 = 0;
                while p < bytes.len() && bytes[p].is_ascii_digit() {
                    acc = acc.wrapping_mul(10).wrapping_add((bytes[p] - b'0') as i64);
                    p += 1;
                }
                if p > start {
                    return Some((if negative { -acc } else { acc }, p));
                }
            }
            pos = idx + 1;
        } else {
            break;
        }
    }
    None
}

fn find_key_bool(
    finder: &Finder,
    key_len: usize,
    bytes: &[u8],
    from: usize,
) -> Option<(bool, usize)> {
    let mut pos = from;
    while pos + key_len < bytes.len() {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                if p < bytes.len() {
                    if bytes[p] == b't' {
                        return Some((true, p + 4));
                    } else if bytes[p] == b'f' {
                        return Some((false, p + 5));
                    }
                }
            }
            pos = idx + 1;
        } else {
            break;
        }
    }
    None
}

fn find_key_array_second_string<'a>(
    finder: &Finder,
    key_len: usize,
    bytes: &'a [u8],
    from: usize,
) -> Option<(&'a [u8], usize)> {
    let mut pos = from;
    while pos + key_len < bytes.len() {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                if p < bytes.len() && bytes[p] == b'[' {
                    p += 1;
                    while p < bytes.len() && bytes[p] != b'"' {
                        p += 1;
                    }
                    if p >= bytes.len() {
                        pos = idx + 1;
                        continue;
                    }
                    p += 1;
                    if let Some((_, end)) = extract_string_value(bytes, p) {
                        p = end;
                    } else {
                        pos = idx + 1;
                        continue;
                    }
                    while p < bytes.len() && bytes[p] != b'"' && bytes[p] != b']' {
                        p += 1;
                    }
                    if p < bytes.len() && bytes[p] == b'"' {
                        p += 1;
                        return extract_string_value(bytes, p);
                    }
                }
            }
            pos = idx + 1;
        } else {
            break;
        }
    }
    None
}

pub(crate) fn fast_extract_boltodds_baseball(json: &str) -> Option<BoltOddsBaseballExtract<'_>> {
    let bytes = json.as_bytes();

    if find_with(&FINDER_MATCH_UPDATE, bytes, 0).is_none() {
        return None;
    }

    let mut pos = 0usize;

    let game_start = find_key_string_start(&FINDER_GAME, 6, bytes, pos)?;
    let (game_bytes, end) = extract_string_value(bytes, game_start)?;
    let game_label = std::str::from_utf8(game_bytes).ok()?;
    if game_label.is_empty() {
        return None;
    }
    pos = end;

    let (outs_i64, end) = find_key_integer(&FINDER_OUT, 5, bytes, pos)?;
    pos = end;

    let (inning, end) = find_key_integer(&FINDER_INNING, 8, bytes, pos)?;
    pos = end;

    let (strikes_i64, end) = find_key_integer(&FINDER_STRIKE, 8, bytes, pos)?;
    pos = end;

    // base1/base2/base3 appear between ball and topOfInning in the JSON.
    let (base1, end) = find_key_bool(&FINDER_BASE1, 7, bytes, pos)?;
    pos = end;
    let (base2, end) = find_key_bool(&FINDER_BASE2, 7, bytes, pos)?;
    pos = end;
    let (base3, end) = find_key_bool(&FINDER_BASE3, 7, bytes, pos)?;
    pos = end;

    let (top_of_inning, end) = find_key_bool(&FINDER_TOP_OF_INNING, 13, bytes, pos)?;
    pos = end;

    let (period_bytes, end) = find_key_array_second_string(&FINDER_MATCH_PERIOD, 13, bytes, pos)?;
    let period_detail = std::str::from_utf8(period_bytes).ok()?;
    pos = end;

    let (home_score, end) = find_key_integer(&FINDER_TOTAL_RUNS_A, 19, bytes, pos)?;
    pos = end;

    let (away_score, _) = find_key_integer(&FINDER_TOTAL_RUNS_B, 19, bytes, pos)?;

    Some(BoltOddsBaseballExtract {
        game_label,
        outs: outs_i64 as u8,
        strikes: strikes_i64 as u8,
        inning,
        top_of_inning,
        home_score,
        away_score,
        period_detail,
        base1,
        base2,
        base3,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_of_first_inning() {
        let frame = r#"{"action":"match_update","game":"Arizona Diamondbacks vs Colorado Rockies, 2026-05-21, 09","universal_id":"523c996cb2a3","home":"Arizona Diamondbacks","away":"Colorado Rockies","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"out":0,"inning":1,"runs":{"A":0,"B":0},"strike":0,"ball":0,"base1":false,"base2":false,"base3":false,"topOfInning":true,"extraInningsRuns":{"A":0,"B":0},"inningScores":{"1":{"A":0,"B":0}},"matchPeriod":["BaseballMatchPeriod","AT_TOP_1ST_INNING"],"matchNumberOfInnings":0,"clockStatus":"SET_PERIOD_END","totalRunsForTeamA":0,"totalRunsForTeamB":0,"clockRunning":false}}"#;
        let r = fast_extract_boltodds_baseball(frame).unwrap();
        assert_eq!(
            r.game_label,
            "Arizona Diamondbacks vs Colorado Rockies, 2026-05-21, 09"
        );
        assert_eq!(r.outs, 0);
        assert_eq!(r.strikes, 0);
        assert_eq!(r.inning, 1);
        assert!(r.top_of_inning);
        assert_eq!(r.home_score, 0);
        assert_eq!(r.away_score, 0);
        assert_eq!(r.period_detail, "AT_TOP_1ST_INNING");
        assert!(!r.base1);
        assert!(!r.base2);
        assert!(!r.base3);
    }

    #[test]
    fn test_out_3_top_of_first() {
        let frame = r#"{"action":"match_update","game":"Arizona Diamondbacks vs Colorado Rockies, 2026-05-21, 09","universal_id":"523c996cb2a3","home":"Arizona Diamondbacks","away":"Colorado Rockies","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"out":3,"inning":1,"runs":{"A":0,"B":0},"strike":3,"ball":3,"base1":false,"base2":false,"base3":false,"topOfInning":true,"extraInningsRuns":{"A":0,"B":0},"inningScores":{"1":{"A":0,"B":0}},"matchPeriod":["BaseballMatchPeriod","AT_TOP_1ST_INNING"],"matchNumberOfInnings":0,"clockStatus":"SET_PERIOD_END","totalRunsForTeamA":0,"totalRunsForTeamB":0,"clockRunning":false}}"#;
        let r = fast_extract_boltodds_baseball(frame).unwrap();
        assert_eq!(r.outs, 3);
        assert_eq!(r.strikes, 3);
        assert_eq!(r.inning, 1);
        assert!(r.top_of_inning);
        assert_eq!(r.home_score, 0);
        assert_eq!(r.away_score, 0);
    }

    #[test]
    fn test_out_3_bottom_of_first() {
        let frame = r#"{"action":"match_update","game":"Arizona Diamondbacks vs Colorado Rockies, 2026-05-21, 09","universal_id":"523c996cb2a3","home":"Arizona Diamondbacks","away":"Colorado Rockies","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"out":3,"inning":1,"runs":{"A":0,"B":0},"strike":1,"ball":0,"base1":false,"base2":false,"base3":false,"topOfInning":false,"extraInningsRuns":{"A":0,"B":0},"inningScores":{"1":{"A":0,"B":0}},"matchPeriod":["BaseballMatchPeriod","AT_BOT_1ST_INNING"],"matchNumberOfInnings":0,"clockStatus":"SET_PERIOD_END","totalRunsForTeamA":0,"totalRunsForTeamB":0,"clockRunning":false}}"#;
        let r = fast_extract_boltodds_baseball(frame).unwrap();
        assert_eq!(r.outs, 3);
        assert_eq!(r.inning, 1);
        assert!(!r.top_of_inning);
        assert_eq!(r.period_detail, "AT_BOT_1ST_INNING");
    }

    #[test]
    fn test_mid_game_with_score() {
        let frame = r#"{"action":"match_update","game":"Detroit Tigers vs Cleveland Guardians, 2026-05-21, 01","universal_id":"908c94984949","home":"Detroit Tigers","away":"Cleveland Guardians","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"out":2,"inning":3,"runs":{"A":0,"B":1},"strike":0,"ball":1,"base1":false,"base2":true,"base3":false,"topOfInning":true,"extraInningsRuns":{"A":0,"B":0},"inningScores":{"1":{"A":0,"B":0},"2":{"A":0,"B":0},"3":{"A":0,"B":1}},"matchPeriod":["BaseballMatchPeriod","AT_TOP_3RD_INNING"],"matchNumberOfInnings":0,"clockStatus":"SET_PERIOD_END","totalRunsForTeamA":0,"totalRunsForTeamB":1,"clockRunning":false}}"#;
        let r = fast_extract_boltodds_baseball(frame).unwrap();
        assert_eq!(
            r.game_label,
            "Detroit Tigers vs Cleveland Guardians, 2026-05-21, 01"
        );
        assert_eq!(r.outs, 2);
        assert_eq!(r.strikes, 0);
        assert_eq!(r.inning, 3);
        assert!(r.top_of_inning);
        assert_eq!(r.home_score, 0);
        assert_eq!(r.away_score, 1);
        assert_eq!(r.period_detail, "AT_TOP_3RD_INNING");
        assert!(!r.base1);
        assert!(r.base2);
        assert!(!r.base3);
    }

    #[test]
    fn test_match_completed() {
        let frame = r#"{"action":"match_update","game":"Detroit Tigers vs Cleveland Guardians, 2026-05-21, 01","universal_id":"908c94984949","home":"Detroit Tigers","away":"Cleveland Guardians","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":true,"out":0,"inning":9,"runs":{"A":1,"B":3},"strike":0,"ball":0,"base1":false,"base2":false,"base3":false,"topOfInning":false,"extraInningsRuns":{"A":0,"B":0},"inningScores":{"1":{"A":0,"B":0},"2":{"A":0,"B":0},"3":{"A":0,"B":2},"4":{"A":0,"B":0},"5":{"A":0,"B":0},"6":{"A":0,"B":0},"7":{"A":0,"B":0},"8":{"A":1,"B":1},"9":{"A":0,"B":0}},"matchPeriod":["BaseballMatchPeriod","MATCH_COMPLETED"],"matchNumberOfInnings":0,"clockStatus":"SET_PERIOD_END","totalRunsForTeamA":1,"totalRunsForTeamB":3,"clockRunning":false}}"#;
        let r = fast_extract_boltodds_baseball(frame).unwrap();
        assert_eq!(r.outs, 0);
        assert_eq!(r.inning, 9);
        assert!(!r.top_of_inning);
        assert_eq!(r.home_score, 1);
        assert_eq!(r.away_score, 3);
        assert_eq!(r.period_detail, "MATCH_COMPLETED");
    }

    #[test]
    fn test_rejection_of_non_match_update() {
        let frame = r#"{"action":"socket_connected","plan":"Pro","feed":"livescores"}"#;
        assert!(fast_extract_boltodds_baseball(frame).is_none());
    }

    #[test]
    fn test_rejection_of_ping() {
        let frame = r#"{"action":"ping"}"#;
        assert!(fast_extract_boltodds_baseball(frame).is_none());
    }

    #[test]
    fn test_missing_total_runs_b() {
        let frame = r#"{"action":"match_update","game":"Test Game, 2026-05-21, 01","universal_id":"abc","home":"A","away":"B","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"out":1,"inning":2,"runs":{"A":0,"B":0},"strike":1,"ball":0,"base1":false,"base2":false,"base3":false,"topOfInning":true,"extraInningsRuns":{"A":0,"B":0},"inningScores":{"1":{"A":0,"B":0}},"matchPeriod":["BaseballMatchPeriod","AT_TOP_2ND_INNING"],"matchNumberOfInnings":0,"clockStatus":"SET_PERIOD_END","totalRunsForTeamA":0,"clockRunning":false}}"#;
        assert!(fast_extract_boltodds_baseball(frame).is_none());
    }
}
