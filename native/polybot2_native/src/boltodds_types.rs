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

/// Find a byte pattern in a slice starting from `from`.
/// Uses the `memchr` crate's SIMD-accelerated memmem searcher.
fn memchr_find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from >= haystack.len() {
        return None;
    }
    memchr::memmem::find(&haystack[from..], needle).map(|pos| pos + from)
}

/// Extract a JSON string value starting at `start` (the position after the opening `"`).
/// Returns the string content slice and the position after the closing `"`.
fn extract_string_value(bytes: &[u8], start: usize) -> Option<(&[u8], usize)> {
    let mut pos = start;
    while pos < bytes.len() {
        if bytes[pos] == b'\\' {
            pos += 2; // skip escaped character
            continue;
        }
        if bytes[pos] == b'"' {
            return Some((&bytes[start..pos], pos + 1));
        }
        pos += 1;
    }
    None
}

/// Scan for a JSON key like `"keyName"` followed by `:` and optional whitespace,
/// then a `"` opening a string value. Returns the byte offset after the opening `"`.
fn find_key_string_start(bytes: &[u8], key_pattern: &[u8], from: usize) -> Option<usize> {
    let mut pos = from;
    while pos + key_pattern.len() < bytes.len() {
        if let Some(idx) = memchr_find(bytes, key_pattern, pos) {
            let mut p = idx + key_pattern.len();
            // Skip whitespace
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            // Expect `:`
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                // Skip whitespace
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                // Expect opening `"`
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

/// Scan for a JSON key like `"keyName"` followed by `:` and optional whitespace,
/// then digits (an integer value). Returns the parsed i64 and position after the digits.
fn find_key_integer(bytes: &[u8], key_pattern: &[u8], from: usize) -> Option<(i64, usize)> {
    let mut pos = from;
    while pos + key_pattern.len() < bytes.len() {
        if let Some(idx) = memchr_find(bytes, key_pattern, pos) {
            let mut p = idx + key_pattern.len();
            // Skip whitespace
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            // Expect `:`
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                // Skip whitespace
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                // Parse integer (optional leading minus, then digits)
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

/// Extract the second string from a JSON array value for a key.
/// Expects: `"key": ["first", "SECOND"]` and returns `"SECOND"`.
fn find_key_array_second_string<'a>(bytes: &'a [u8], key_pattern: &[u8], from: usize) -> Option<(&'a [u8], usize)> {
    let mut pos = from;
    while pos + key_pattern.len() < bytes.len() {
        if let Some(idx) = memchr_find(bytes, key_pattern, pos) {
            let mut p = idx + key_pattern.len();
            // Skip whitespace
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            // Expect `:`
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                // Skip whitespace
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                    p += 1;
                }
                // Expect `[`
                if p < bytes.len() && bytes[p] == b'[' {
                    p += 1;
                    // Find first string: skip to `"`
                    while p < bytes.len() && bytes[p] != b'"' {
                        p += 1;
                    }
                    if p >= bytes.len() {
                        pos = idx + 1;
                        continue;
                    }
                    // Skip past first string value
                    p += 1; // past opening quote
                    if let Some((_, end)) = extract_string_value(bytes, p) {
                        p = end;
                    } else {
                        pos = idx + 1;
                        continue;
                    }
                    // Find second string: skip to `"`
                    while p < bytes.len() && bytes[p] != b'"' && bytes[p] != b']' {
                        p += 1;
                    }
                    if p < bytes.len() && bytes[p] == b'"' {
                        p += 1; // past opening quote
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

    // Quick-reject: "match_update" is unique to relevant BoltOdds frames.
    // Single scan replaces the previous double scan ("match_update" + "action").
    if memchr_find(bytes, b"\"match_update\"", 0).is_none() {
        return None;
    }

    // Carry-forward position: extract fields in frame layout order so each
    // scan starts where the previous field ended, not from byte 0.
    // Frame field order: action(~2) → game(~27) → matchPeriod(~269)
    //                   → goalsA(~358) → goalsB(~370) → cornersA(~381) → cornersB(~395)
    let mut pos = 0usize;

    // game label
    let game_start = find_key_string_start(bytes, b"\"game\"", pos)?;
    let (game_bytes, end) = extract_string_value(bytes, game_start)?;
    let game_label = std::str::from_utf8(game_bytes).ok()?;
    if game_label.is_empty() {
        return None;
    }
    pos = end;

    // matchPeriod (from after game)
    let (period_bytes, end) = find_key_array_second_string(bytes, b"\"matchPeriod\"", pos)?;
    let match_period_detail = std::str::from_utf8(period_bytes).ok()?;
    pos = end;

    // goalsA (from after matchPeriod)
    let (goals_a, end) = find_key_integer(bytes, b"\"goalsA\"", pos)?;
    pos = end;

    // goalsB (from after goalsA)
    let (goals_b, end) = find_key_integer(bytes, b"\"goalsB\"", pos)?;
    pos = end;

    // cornersA (from after goalsB)
    let (corners_a, end) = find_key_integer(bytes, b"\"cornersA\"", pos)?;
    pos = end;

    // cornersB (from after cornersA)
    let (corners_b, _) = find_key_integer(bytes, b"\"cornersB\"", pos)?;

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
        assert_eq!(result.game_label, "Chelsea vs Nottingham Forest, 2026-05-04, 10");
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
