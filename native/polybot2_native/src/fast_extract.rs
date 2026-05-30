//! Fast byte-level field extractor for Kalstrop V1 frames.
//! Scans raw JSON for the 4 fields needed by the engine and returns
//! borrowed slices. Returns None for non-"next" frames or missing fields.

pub(crate) struct V1Extract<'a> {
    pub fixture_id: &'a str,
    pub home_score: &'a str,
    pub away_score: &'a str,
    pub free_text: &'a str,
    pub corners_home: Option<i64>,
    pub corners_away: Option<i64>,
}

#[inline(always)]
pub(crate) fn fast_parse_score(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    match b.len() {
        0 => None,
        1 => {
            let d = b[0].wrapping_sub(b'0');
            if d <= 9 {
                Some(d as i64)
            } else {
                None
            }
        }
        2 => {
            let d0 = b[0].wrapping_sub(b'0');
            let d1 = b[1].wrapping_sub(b'0');
            if d0 <= 9 && d1 <= 9 {
                Some((d0 * 10 + d1) as i64)
            } else {
                None
            }
        }
        3 => {
            let d0 = b[0].wrapping_sub(b'0');
            let d1 = b[1].wrapping_sub(b'0');
            let d2 = b[2].wrapping_sub(b'0');
            if d0 <= 9 && d1 <= 9 && d2 <= 9 {
                Some(d0 as i64 * 100 + d1 as i64 * 10 + d2 as i64)
            } else {
                None
            }
        }
        _ => s.parse().ok(),
    }
}

use memchr::memmem::Finder;
use std::sync::LazyLock;

static FINDER_TYPE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"type\""));
static FINDER_FIXTURE_ID: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"fixtureId\""));
static FINDER_FREE_TEXT: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"freeText\""));
static FINDER_HOME_SCORE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"homeScore\""));
static FINDER_AWAY_SCORE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"awayScore\""));
static FINDER_CORNERS: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"corners\""));
static FINDER_HOME: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"home\""));
static FINDER_AWAY: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"away\""));
static FINDER_CURRENT_PHASE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"currentPhase\""));
static FINDER_PHASES: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"phases\""));
static FINDER_PHASE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"phase\""));

fn find_with(finder: &Finder, haystack: &[u8], from: usize) -> Option<usize> {
    if from >= haystack.len() {
        return None;
    }
    finder.find(&haystack[from..]).map(|pos| pos + from)
}

fn find_key_value_start(
    finder: &Finder,
    key_len: usize,
    bytes: &[u8],
    from: usize,
) -> Option<usize> {
    let mut pos = from;
    let len = bytes.len();
    while pos + key_len < len {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < len
                && (bytes[p] == b' ' || bytes[p] == b'\t' || bytes[p] == b'\n' || bytes[p] == b'\r')
            {
                p += 1;
            }
            if p < len && bytes[p] == b':' {
                p += 1;
                while p < len
                    && (bytes[p] == b' '
                        || bytes[p] == b'\t'
                        || bytes[p] == b'\n'
                        || bytes[p] == b'\r')
                {
                    p += 1;
                }
                if p < len && bytes[p] == b'"' {
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

fn find_key_integer(finder: &Finder, key_len: usize, bytes: &[u8], from: usize) -> Option<(i64, usize)> {
    let mut pos = from;
    while pos + key_len < bytes.len() {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') { p += 1; }
            if p < bytes.len() && bytes[p] == b':' {
                p += 1;
                while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') { p += 1; }
                let negative = p < bytes.len() && bytes[p] == b'-';
                if negative { p += 1; }
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

/// Extract a JSON string value starting at `start` (the position after the opening `"`).
/// Returns the string content and the position after the closing `"`.
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

pub(crate) fn fast_extract_v1(json: &str) -> Option<V1Extract<'_>> {
    let bytes = json.as_bytes();

    // Quick reject: check for "type":"next".
    // In live V1 frames this appears in the first ~30 bytes, but we search the
    // full buffer for robustness (test data may have reordered keys).
    let is_next = find_with(&FINDER_TYPE, bytes, 0)
        .and_then(|pos| {
            let mut p = pos + 6; // skip past `"type"`
            while p < bytes.len() && bytes[p] != b'"' {
                p += 1;
            }
            if p < bytes.len() {
                extract_string_value(bytes, p + 1)
            } else {
                None
            }
        })
        .map(|(val, _)| val == b"next")
        .unwrap_or(false);

    if !is_next {
        return None;
    }

    // Scan for the 4 needed fields. They appear in this order in V1 frames:
    // fixtureId (~byte 130), freeText (~byte 247), homeScore (~byte 276), awayScore (~byte 294)
    // Carry forward position so each search starts where the last one ended.

    let mut fixture_id: Option<&str> = None;
    let mut free_text: Option<&str> = None;
    let mut home_score: Option<&str> = None;
    let mut away_score: Option<&str> = None;
    let mut pos = 0usize;

    if let Some(start) = find_key_value_start(&FINDER_FIXTURE_ID, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            fixture_id = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    if let Some(start) = find_key_value_start(&FINDER_FREE_TEXT, 10, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            free_text = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    if let Some(start) = find_key_value_start(&FINDER_HOME_SCORE, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            home_score = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    if let Some(start) = find_key_value_start(&FINDER_AWAY_SCORE, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            away_score = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    let mut corners_home: Option<i64> = None;
    let mut corners_away: Option<i64> = None;
    if let Some(corners_pos) = find_with(&FINDER_CORNERS, bytes, pos) {
        if let Some((h, end)) = find_key_integer(&FINDER_HOME, 6, bytes, corners_pos) {
            corners_home = Some(h);
            if let Some((a, _)) = find_key_integer(&FINDER_AWAY, 6, bytes, end) {
                corners_away = Some(a);
            }
        }
    }

    Some(V1Extract {
        fixture_id: fixture_id?,
        home_score: home_score.unwrap_or(""),
        away_score: away_score.unwrap_or(""),
        free_text: free_text.unwrap_or(""),
        corners_home,
        corners_away,
    })
}

pub(crate) struct TennisV1Extract<'a> {
    pub fixture_id: &'a str,
    pub sets_home: &'a str,        // top-level homeScore (sets won)
    pub sets_away: &'a str,        // top-level awayScore
    pub games_home: &'a str,       // currentPhase.homeScore (empty if null)
    pub games_away: &'a str,       // currentPhase.awayScore (empty if null)
    pub free_text: &'a str,        // matchStatusDisplay[0].freeText
    pub current_phase: Option<i64>, // currentPhase.phase (None if null)
    pub total_games: i64,          // sum of all games from phases array
    pub first_set_games: i64,      // games in phases[0] only
}

/// Scan the `"phases"` array starting at `start` (the position of `[`).
/// For each phase entry, extracts homeScore and awayScore as integers and
/// sums them. Returns (total_games_across_all_phases, first_set_games).
fn scan_phases_games(bytes: &[u8], start: usize) -> (i64, i64) {
    let len = bytes.len();
    let mut pos = start;
    // Find the opening '['.
    while pos < len && bytes[pos] != b'[' {
        pos += 1;
    }
    if pos >= len {
        return (0, 0);
    }
    pos += 1; // skip '['

    let mut total: i64 = 0;
    let mut first_set: i64 = 0;
    let mut phase_count: usize = 0;

    // Walk the array. Each phase object has "homeScore":"N" and "awayScore":"N".
    // We look for balanced braces to track phase boundaries.
    while pos < len {
        // Skip whitespace/commas
        while pos < len && matches!(bytes[pos], b' ' | b'\t' | b'\n' | b'\r' | b',') {
            pos += 1;
        }
        if pos >= len || bytes[pos] == b']' {
            break;
        }
        if bytes[pos] != b'{' {
            pos += 1;
            continue;
        }
        // Found a phase object — find its end by brace counting
        let obj_start = pos;
        let mut depth = 1u32;
        pos += 1;
        while pos < len && depth > 0 {
            match bytes[pos] {
                b'{' => depth += 1,
                b'}' => depth -= 1,
                b'"' => {
                    // skip string contents
                    pos += 1;
                    while pos < len {
                        if bytes[pos] == b'\\' {
                            pos += 2;
                            continue;
                        }
                        if bytes[pos] == b'"' {
                            break;
                        }
                        pos += 1;
                    }
                }
                _ => {}
            }
            pos += 1;
        }
        let obj_end = pos;
        let obj_slice = &bytes[obj_start..obj_end.min(len)];

        // Extract homeScore and awayScore from this phase object.
        let mut phase_games: i64 = 0;
        if let Some(hs) = find_key_value_start(&FINDER_HOME_SCORE, 11, obj_slice, 0) {
            if let Some((val, _)) = extract_string_value(obj_slice, hs) {
                if let Ok(s) = std::str::from_utf8(val) {
                    if let Some(v) = fast_parse_score(s) {
                        phase_games += v;
                    }
                }
            }
        }
        if let Some(as_start) = find_key_value_start(&FINDER_AWAY_SCORE, 11, obj_slice, 0) {
            if let Some((val, _)) = extract_string_value(obj_slice, as_start) {
                if let Ok(s) = std::str::from_utf8(val) {
                    if let Some(v) = fast_parse_score(s) {
                        phase_games += v;
                    }
                }
            }
        }
        total += phase_games;
        if phase_count == 0 {
            first_set = phase_games;
        }
        phase_count += 1;
    }

    (total, first_set)
}

pub(crate) fn fast_extract_tennis_v1(json: &str) -> Option<TennisV1Extract<'_>> {
    let bytes = json.as_bytes();

    // Quick reject: check for "type":"next".
    let is_next = find_with(&FINDER_TYPE, bytes, 0)
        .and_then(|pos| {
            let mut p = pos + 6; // skip past `"type"`
            while p < bytes.len() && bytes[p] != b'"' {
                p += 1;
            }
            if p < bytes.len() {
                extract_string_value(bytes, p + 1)
            } else {
                None
            }
        })
        .map(|(val, _)| val == b"next")
        .unwrap_or(false);

    if !is_next {
        return None;
    }

    // fixture_id
    let mut fixture_id: Option<&str> = None;
    let mut pos = 0usize;

    if let Some(start) = find_key_value_start(&FINDER_FIXTURE_ID, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            fixture_id = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    // freeText
    let mut free_text: &str = "";
    if let Some(start) = find_key_value_start(&FINDER_FREE_TEXT, 10, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            free_text = std::str::from_utf8(val).ok()?;
            pos = end;
        }
    }

    // sets_home / sets_away: FIRST occurrence of homeScore/awayScore (top-level, before currentPhase)
    let mut sets_home: &str = "";
    let mut sets_away: &str = "";
    if let Some(start) = find_key_value_start(&FINDER_HOME_SCORE, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            sets_home = std::str::from_utf8(val).ok()?;
            pos = end;
        }
    }
    if let Some(start) = find_key_value_start(&FINDER_AWAY_SCORE, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            sets_away = std::str::from_utf8(val).ok()?;
            pos = end;
        }
    }

    // currentPhase — may be null (match ended)
    let mut games_home: &str = "";
    let mut games_away: &str = "";
    let mut current_phase: Option<i64> = None;

    if let Some(cp_pos) = find_with(&FINDER_CURRENT_PHASE, bytes, pos) {
        // Find ':' after "currentPhase"
        let key_end = cp_pos + 14; // len of "currentPhase"
        let mut p = key_end;
        while p < bytes.len() && bytes[p] != b':' {
            p += 1;
        }
        if p < bytes.len() {
            p += 1; // skip ':'
            // Skip whitespace
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            if p < bytes.len() && bytes[p] != b'n' {
                // Not null — extract games_home, games_away, phase from within currentPhase object
                let cp_content_start = p;
                if let Some(start) = find_key_value_start(&FINDER_HOME_SCORE, 11, bytes, cp_content_start) {
                    if let Some((val, _)) = extract_string_value(bytes, start) {
                        games_home = std::str::from_utf8(val).ok()?;
                    }
                }
                if let Some(start) = find_key_value_start(&FINDER_AWAY_SCORE, 11, bytes, cp_content_start) {
                    if let Some((val, _)) = extract_string_value(bytes, start) {
                        games_away = std::str::from_utf8(val).ok()?;
                    }
                }
                if let Some((phase_val, _)) = find_key_integer(&FINDER_PHASE, 7, bytes, cp_content_start) {
                    current_phase = Some(phase_val);
                }
            }
            // else: null — games_home/away stay "", current_phase stays None
        }
    }

    // phases array — scan for total games and first set games
    let mut total_games: i64 = 0;
    let mut first_set_games: i64 = 0;
    if let Some(phases_pos) = find_with(&FINDER_PHASES, bytes, 0) {
        let after_key = phases_pos + 8; // len of "phases"
        (total_games, first_set_games) = scan_phases_games(bytes, after_key);
    }

    Some(TennisV1Extract {
        fixture_id: fixture_id?,
        sets_home,
        sets_away,
        games_home,
        games_away,
        free_text,
        current_phase,
        total_games,
        first_set_games,
    })
}

// ---------------------------------------------------------------------------
// CS2: extract maps, rounds, current phase from V1 frames
// ---------------------------------------------------------------------------

pub(crate) struct Cs2V1Extract<'a> {
    pub fixture_id: &'a str,
    pub maps_home: &'a str,         // top-level homeScore (maps won)
    pub maps_away: &'a str,         // top-level awayScore
    pub rounds_home: &'a str,       // currentPhase.homeScore (rounds in current map, empty if null)
    pub rounds_away: &'a str,       // currentPhase.awayScore
    pub free_text: &'a str,         // matchStatusDisplay[0].freeText ("1st map", "Closed")
    pub current_phase: Option<i64>, // currentPhase.phase (map number, None if null)
}

/// Dedicated CS2 V1 frame extractor. Same frame structure as tennis
/// (`sportsMatchStateUpdatedV2`) but without phases array scanning.
pub(crate) fn fast_extract_cs2_v1(json: &str) -> Option<Cs2V1Extract<'_>> {
    let bytes = json.as_bytes();

    // Quick reject: check for "type":"next".
    let is_next = find_with(&FINDER_TYPE, bytes, 0)
        .and_then(|pos| {
            let mut p = pos + 6;
            while p < bytes.len() && bytes[p] != b'"' {
                p += 1;
            }
            if p < bytes.len() {
                extract_string_value(bytes, p + 1)
            } else {
                None
            }
        })
        .map(|(val, _)| val == b"next")
        .unwrap_or(false);

    if !is_next {
        return None;
    }

    let mut fixture_id: Option<&str> = None;
    let mut pos = 0usize;

    if let Some(start) = find_key_value_start(&FINDER_FIXTURE_ID, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            fixture_id = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    let mut free_text: &str = "";
    if let Some(start) = find_key_value_start(&FINDER_FREE_TEXT, 10, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            free_text = std::str::from_utf8(val).ok()?;
            pos = end;
        }
    }

    let mut maps_home: &str = "";
    let mut maps_away: &str = "";
    if let Some(start) = find_key_value_start(&FINDER_HOME_SCORE, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            maps_home = std::str::from_utf8(val).ok()?;
            pos = end;
        }
    }
    if let Some(start) = find_key_value_start(&FINDER_AWAY_SCORE, 11, bytes, pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            maps_away = std::str::from_utf8(val).ok()?;
            pos = end;
        }
    }

    // currentPhase — may be null (match ended)
    let mut rounds_home: &str = "";
    let mut rounds_away: &str = "";
    let mut current_phase: Option<i64> = None;

    if let Some(cp_pos) = find_with(&FINDER_CURRENT_PHASE, bytes, pos) {
        let key_end = cp_pos + 14;
        let mut p = key_end;
        while p < bytes.len() && bytes[p] != b':' {
            p += 1;
        }
        if p < bytes.len() {
            p += 1;
            while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
                p += 1;
            }
            if p < bytes.len() && bytes[p] != b'n' {
                let cp_content_start = p;
                if let Some(start) = find_key_value_start(&FINDER_HOME_SCORE, 11, bytes, cp_content_start) {
                    if let Some((val, _)) = extract_string_value(bytes, start) {
                        rounds_home = std::str::from_utf8(val).ok()?;
                    }
                }
                if let Some(start) = find_key_value_start(&FINDER_AWAY_SCORE, 11, bytes, cp_content_start) {
                    if let Some((val, _)) = extract_string_value(bytes, start) {
                        rounds_away = std::str::from_utf8(val).ok()?;
                    }
                }
                if let Some((phase_val, _)) = find_key_integer(&FINDER_PHASE, 7, bytes, cp_content_start) {
                    current_phase = Some(phase_val);
                }
            }
        }
    }

    Some(Cs2V1Extract {
        fixture_id: fixture_id?,
        maps_home,
        maps_away,
        rounds_home,
        rounds_away,
        free_text,
        current_phase,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_next_frame() {
        let frame = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"abc-123","matchSummary":{"matchStatusDisplay":[{"freeText":"4th inning top"}],"homeScore":"3","awayScore":"1"}}}}}"#;
        let result = fast_extract_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "abc-123");
        assert_eq!(result.home_score, "3");
        assert_eq!(result.away_score, "1");
        assert_eq!(result.free_text, "4th inning top");
        assert_eq!(result.corners_home, None);
        assert_eq!(result.corners_away, None);
    }

    #[test]
    fn test_connection_ack_rejected() {
        let frame = r#"{"type":"connection_ack"}"#;
        assert!(fast_extract_v1(frame).is_none());
    }

    #[test]
    fn test_missing_fixture_id() {
        let frame = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"matchSummary":{"homeScore":"0","awayScore":"0"}}}}}"#;
        assert!(fast_extract_v1(frame).is_none());
    }

    #[test]
    fn test_fast_parse_score() {
        assert_eq!(fast_parse_score(""), None);
        assert_eq!(fast_parse_score("0"), Some(0));
        assert_eq!(fast_parse_score("5"), Some(5));
        assert_eq!(fast_parse_score("9"), Some(9));
        assert_eq!(fast_parse_score("10"), Some(10));
        assert_eq!(fast_parse_score("42"), Some(42));
        assert_eq!(fast_parse_score("99"), Some(99));
        assert_eq!(fast_parse_score("100"), Some(100));
        assert_eq!(fast_parse_score("abc"), None);
        assert_eq!(fast_parse_score("1a"), None);
        assert_eq!(fast_parse_score("a1"), None);
        assert_eq!(fast_parse_score("1234"), Some(1234)); // fallback path
    }

    #[test]
    fn test_soccer_v1_frame_with_corners() {
        let frame = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"87c39b80-625c-4d79-8e29-fcea7d9ee421","matchSummary":{"matchStatusDisplay":[{"freeText":"2nd half"}],"homeScore":"3","awayScore":"2","statistics":{"corners":{"home":5,"away":3},"redCards":{"home":0,"away":0}}}}}}}"#;
        let result = fast_extract_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "87c39b80-625c-4d79-8e29-fcea7d9ee421");
        assert_eq!(result.home_score, "3");
        assert_eq!(result.away_score, "2");
        assert_eq!(result.free_text, "2nd half");
        assert_eq!(result.corners_home, Some(5));
        assert_eq!(result.corners_away, Some(3));
    }

    // =================================================================
    // Tennis V1 extraction tests
    // =================================================================

    #[test]
    fn test_tennis_v1_set1_in_progress() {
        // Real frame from captured fixture: 1st set, home 2-1 games, sets 0-0
        let frame = r#"{"id":"ks-tennis","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"05d39a19-ddad-4afb-9438-5a73ce03bc6d","matchSummary":{"matchStatusDisplay":[{"freeText":"1st set"}],"homeScore":"0","awayScore":"0","currentPhase":{"phase":1,"homeScore":"2","awayScore":"1","homeGameScore":"0","awayGameScore":"15"},"phases":[{"phase":1,"homeScore":"2","awayScore":"1","homeGameScore":"0","awayGameScore":"15"}]}}}}}"#;
        let result = fast_extract_tennis_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "05d39a19-ddad-4afb-9438-5a73ce03bc6d");
        assert_eq!(result.sets_home, "0");
        assert_eq!(result.sets_away, "0");
        assert_eq!(result.games_home, "2");
        assert_eq!(result.games_away, "1");
        assert_eq!(result.free_text, "1st set");
        assert_eq!(result.current_phase, Some(1));
        assert_eq!(result.total_games, 3);  // 2 + 1
        assert_eq!(result.first_set_games, 3);
    }

    #[test]
    fn test_tennis_v1_set2_frame() {
        // Real frame: 2nd set start, home won set 1 (7-6), sets 1-0
        let frame = r#"{"id":"ks-tennis","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"05d39a19-ddad-4afb-9438-5a73ce03bc6d","matchSummary":{"matchStatusDisplay":[{"freeText":"2nd set"}],"homeScore":"1","awayScore":"0","currentPhase":{"phase":2,"homeScore":"0","awayScore":"0","homeGameScore":"0","awayGameScore":"0"},"phases":[{"phase":1,"homeScore":"7","awayScore":"6"},{"phase":2,"homeScore":"0","awayScore":"0","homeGameScore":"0","awayGameScore":"0"}]}}}}}"#;
        let result = fast_extract_tennis_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "05d39a19-ddad-4afb-9438-5a73ce03bc6d");
        assert_eq!(result.sets_home, "1");
        assert_eq!(result.sets_away, "0");
        assert_eq!(result.games_home, "0");
        assert_eq!(result.games_away, "0");
        assert_eq!(result.free_text, "2nd set");
        assert_eq!(result.current_phase, Some(2));
        assert_eq!(result.total_games, 13); // 7+6 + 0+0
        assert_eq!(result.first_set_games, 13); // 7+6
    }

    #[test]
    fn test_tennis_v1_ended_frame() {
        // Real frame: match ended, home wins 2-1 in sets, currentPhase is null
        let frame = r#"{"id":"ks-tennis","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"05d39a19-ddad-4afb-9438-5a73ce03bc6d","matchSummary":{"matchStatusDisplay":[{"freeText":"Ended"}],"homeScore":"2","awayScore":"1","currentPhase":null,"phases":[{"phase":1,"homeScore":"7","awayScore":"6"},{"phase":2,"homeScore":"3","awayScore":"6"},{"phase":3,"homeScore":"6","awayScore":"2"}]}}}}}"#;
        let result = fast_extract_tennis_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "05d39a19-ddad-4afb-9438-5a73ce03bc6d");
        assert_eq!(result.sets_home, "2");
        assert_eq!(result.sets_away, "1");
        assert_eq!(result.games_home, "");
        assert_eq!(result.games_away, "");
        assert_eq!(result.free_text, "Ended");
        assert_eq!(result.current_phase, None);
        assert_eq!(result.total_games, 30); // (7+6) + (3+6) + (6+2)
        assert_eq!(result.first_set_games, 13); // 7+6
    }

    #[test]
    fn test_tennis_v1_tiebreak_frame() {
        // Real frame: 1st set tiebreak, games 6-6
        let frame = r#"{"id":"ks-tennis","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"05d39a19-ddad-4afb-9438-5a73ce03bc6d","matchSummary":{"matchStatusDisplay":[{"freeText":"1st set"}],"homeScore":"0","awayScore":"0","currentPhase":{"phase":1,"homeScore":"6","awayScore":"6","homeGameScore":"0","awayGameScore":"0"},"phases":[{"phase":1,"homeScore":"6","awayScore":"6","homeGameScore":"0","awayGameScore":"0"}]}}}}}"#;
        let result = fast_extract_tennis_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "05d39a19-ddad-4afb-9438-5a73ce03bc6d");
        assert_eq!(result.sets_home, "0");
        assert_eq!(result.sets_away, "0");
        assert_eq!(result.games_home, "6");
        assert_eq!(result.games_away, "6");
        assert_eq!(result.free_text, "1st set");
        assert_eq!(result.current_phase, Some(1));
        assert_eq!(result.total_games, 12); // 6+6
        assert_eq!(result.first_set_games, 12);
    }

    #[test]
    fn test_tennis_v1_non_next_rejected() {
        let frame = r#"{"id":"ks-tennis","type":"connection_ack","payload":{}}"#;
        assert!(fast_extract_tennis_v1(frame).is_none());
    }

    #[test]
    fn test_tennis_v1_empty_phases() {
        // Edge case: phases array is empty
        let frame = r#"{"id":"ks-tennis","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"abc-123","matchSummary":{"matchStatusDisplay":[{"freeText":"1st set"}],"homeScore":"0","awayScore":"0","currentPhase":{"phase":1,"homeScore":"0","awayScore":"0"},"phases":[]}}}}}"#;
        let result = fast_extract_tennis_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "abc-123");
        assert_eq!(result.total_games, 0);
        assert_eq!(result.first_set_games, 0);
    }
}
