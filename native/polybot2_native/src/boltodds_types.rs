//! Byte-level field extractor for BoltOdds match_update frames.
//! Scans raw JSON for the fields needed by the soccer engine and returns
//! borrowed slices. Returns None for non-match_update frames or missing fields.

pub(crate) struct BoltOddsExtract<'a> {
    pub game_label: &'a str,
    pub goals_a: i64,
    pub goals_b: i64,
    pub corners_a: i64,
    pub corners_b: i64,
    pub match_period_detail: &'a str,
}

use memchr::memmem::Finder;
use std::sync::LazyLock;

static FINDER_MATCH_UPDATE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"match_update\""));
static FINDER_GAME: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"game\""));
static FINDER_MATCH_PERIOD: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"matchPeriod\""));
static FINDER_GOALS_A: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"goalsA\""));
static FINDER_GOALS_B: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"goalsB\""));
static FINDER_CORNERS_A: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"cornersA\""));
static FINDER_CORNERS_B: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"cornersB\""));

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

pub(crate) fn fast_extract_boltodds(json: &str) -> Option<BoltOddsExtract<'_>> {
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

    let (period_bytes, end) = find_key_array_second_string(&FINDER_MATCH_PERIOD, 13, bytes, pos)?;
    let match_period_detail = std::str::from_utf8(period_bytes).ok()?;
    pos = end;

    let (goals_a, end) = find_key_integer(&FINDER_GOALS_A, 8, bytes, pos)?;
    pos = end;

    let (goals_b, end) = find_key_integer(&FINDER_GOALS_B, 8, bytes, pos)?;
    pos = end;

    let (corners_a, end) = find_key_integer(&FINDER_CORNERS_A, 10, bytes, pos)?;
    pos = end;

    let (corners_b, _) = find_key_integer(&FINDER_CORNERS_B, 10, bytes, pos)?;

    Some(BoltOddsExtract {
        game_label,
        goals_a,
        goals_b,
        corners_a,
        corners_b,
        match_period_detail,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_real_match_update_frame() {
        let frame = r#"{"action":"match_update","game":"Chelsea vs Nottingham Forest, 2026-05-04, 10","universal_id":"a07b00129a1b","home":"Chelsea","away":"Nottingham Forest","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"clockRunningNow":true,"matchPeriod":["FootballMatchPeriod","IN_FIRST_HALF"],"elapsedTimeSeconds":30,"goalsA":0,"goalsB":0,"cornersA":0,"cornersB":0,"yellowCardsA":0,"yellowCardsB":0,"redCardsA":0,"redCardsB":0,"firstHalfGoalsA":0,"firstHalfGoalsB":0,"secondHalfGoalsA":0,"secondHalfGoalsB":0,"varReferralInProgress":false,"clockRunning":false}}"#;
        let result = fast_extract_boltodds(frame).unwrap();
        assert_eq!(
            result.game_label,
            "Chelsea vs Nottingham Forest, 2026-05-04, 10"
        );
        assert_eq!(result.goals_a, 0);
        assert_eq!(result.goals_b, 0);
        assert_eq!(result.corners_a, 0);
        assert_eq!(result.corners_b, 0);
        assert_eq!(result.match_period_detail, "IN_FIRST_HALF");
    }

    #[test]
    fn test_match_update_with_goals() {
        let frame = r#"{"action":"match_update","game":"Arsenal vs Liverpool, 2026-05-04, 12","universal_id":"b08c11230b2c","home":"Arsenal","away":"Liverpool","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"clockRunningNow":true,"matchPeriod":["FootballMatchPeriod","IN_SECOND_HALF"],"elapsedTimeSeconds":3600,"goalsA":2,"goalsB":1,"cornersA":5,"cornersB":3,"yellowCardsA":1,"yellowCardsB":2,"redCardsA":0,"redCardsB":0,"firstHalfGoalsA":1,"firstHalfGoalsB":0,"secondHalfGoalsA":1,"secondHalfGoalsB":1,"varReferralInProgress":false,"clockRunning":true}}"#;
        let result = fast_extract_boltodds(frame).unwrap();
        assert_eq!(result.game_label, "Arsenal vs Liverpool, 2026-05-04, 12");
        assert_eq!(result.goals_a, 2);
        assert_eq!(result.goals_b, 1);
        assert_eq!(result.corners_a, 5);
        assert_eq!(result.corners_b, 3);
        assert_eq!(result.match_period_detail, "IN_SECOND_HALF");
    }

    #[test]
    fn test_match_completed_frame() {
        let frame = r#"{"action":"match_update","game":"Chelsea vs Wolves, 2026-05-04, 15","universal_id":"c09d22341c3d","home":"Chelsea","away":"Wolves","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":true,"clockRunningNow":false,"matchPeriod":["FootballMatchPeriod","MATCH_COMPLETED"],"elapsedTimeSeconds":5400,"goalsA":3,"goalsB":0,"cornersA":7,"cornersB":2,"yellowCardsA":1,"yellowCardsB":3,"redCardsA":0,"redCardsB":1,"firstHalfGoalsA":2,"firstHalfGoalsB":0,"secondHalfGoalsA":1,"secondHalfGoalsB":0,"varReferralInProgress":false,"clockRunning":false}}"#;
        let result = fast_extract_boltodds(frame).unwrap();
        assert_eq!(result.game_label, "Chelsea vs Wolves, 2026-05-04, 15");
        assert_eq!(result.goals_a, 3);
        assert_eq!(result.goals_b, 0);
        assert_eq!(result.corners_a, 7);
        assert_eq!(result.corners_b, 2);
        assert_eq!(result.match_period_detail, "MATCH_COMPLETED");
    }

    #[test]
    fn test_rejection_of_non_match_update() {
        let frame = r#"{"action":"socket_connected","plan":"Pro","feed":"livescores"}"#;
        assert!(fast_extract_boltodds(frame).is_none());
    }

    #[test]
    fn test_rejection_of_ping() {
        let frame = r#"{"action":"ping"}"#;
        assert!(fast_extract_boltodds(frame).is_none());
    }

    #[test]
    fn test_missing_game_label() {
        // No "game" key — should fail to extract.
        let frame = r#"{"action":"match_update","universal_id":"abc123","state":{"goalsA":0,"goalsB":0,"cornersA":0,"cornersB":0,"matchPeriod":["FootballMatchPeriod","IN_FIRST_HALF"]}}"#;
        assert!(fast_extract_boltodds(frame).is_none());
    }

    #[test]
    fn test_missing_goals_field() {
        // Missing goalsB — has game label but incomplete state
        let frame = r#"{"action":"match_update","game":"Test Match","state":{"goalsA":1,"cornersA":0,"cornersB":0,"matchPeriod":["FootballMatchPeriod","IN_FIRST_HALF"]}}"#;
        assert!(fast_extract_boltodds(frame).is_none());
    }

    #[test]
    fn test_halftime_period() {
        let frame = r#"{"action":"match_update","game":"Test","universal_id":"ht_test_01","home":"A","away":"B","designation":{"A":"home","B":"away"},"state":{"preMatch":false,"matchCompleted":false,"clockRunningNow":false,"matchPeriod":["FootballMatchPeriod","AT_HALF_TIME"],"elapsedTimeSeconds":2700,"goalsA":1,"goalsB":1,"cornersA":3,"cornersB":4,"yellowCardsA":0,"yellowCardsB":0,"redCardsA":0,"redCardsB":0,"firstHalfGoalsA":1,"firstHalfGoalsB":1,"secondHalfGoalsA":0,"secondHalfGoalsB":0,"varReferralInProgress":false,"clockRunning":false}}"#;
        let result = fast_extract_boltodds(frame).unwrap();
        assert_eq!(result.game_label, "Test");
        assert_eq!(result.goals_a, 1);
        assert_eq!(result.goals_b, 1);
        assert_eq!(result.match_period_detail, "AT_HALF_TIME");
    }
}
