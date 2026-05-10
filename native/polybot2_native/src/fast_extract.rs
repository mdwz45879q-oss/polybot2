//! Fast byte-level field extractor for Kalstrop V1 frames.
//! Scans raw JSON for the 4 fields needed by the engine and returns
//! borrowed slices. Returns None for non-"next" frames or missing fields.

pub(crate) struct V1Extract<'a> {
    pub fixture_id: &'a str,
    pub home_score: &'a str,
    pub away_score: &'a str,
    pub free_text: &'a str,
}

#[inline(always)]
pub(crate) fn fast_parse_score(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    match b.len() {
        0 => None,
        1 => {
            let d = b[0].wrapping_sub(b'0');
            if d <= 9 { Some(d as i64) } else { None }
        }
        2 => {
            let d0 = b[0].wrapping_sub(b'0');
            let d1 = b[1].wrapping_sub(b'0');
            if d0 <= 9 && d1 <= 9 { Some((d0 * 10 + d1) as i64) } else { None }
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
static FINDER_FIXTURE_ID: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"fixtureId\""));
static FINDER_FREE_TEXT: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"freeText\""));
static FINDER_HOME_SCORE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"homeScore\""));
static FINDER_AWAY_SCORE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"awayScore\""));

fn find_with(finder: &Finder, haystack: &[u8], from: usize) -> Option<usize> {
    if from >= haystack.len() { return None; }
    finder.find(&haystack[from..]).map(|pos| pos + from)
}

fn find_key_value_start(finder: &Finder, key_len: usize, bytes: &[u8], from: usize) -> Option<usize> {
    let mut pos = from;
    let len = bytes.len();
    while pos + key_len < len {
        if let Some(idx) = find_with(finder, bytes, pos) {
            let mut p = idx + key_len;
            while p < len && (bytes[p] == b' ' || bytes[p] == b'\t' || bytes[p] == b'\n' || bytes[p] == b'\r') {
                p += 1;
            }
            if p < len && bytes[p] == b':' {
                p += 1;
                while p < len && (bytes[p] == b' ' || bytes[p] == b'\t' || bytes[p] == b'\n' || bytes[p] == b'\r') {
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
            while p < bytes.len() && bytes[p] != b'"' { p += 1; }
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
        if let Some((val, _end)) = extract_string_value(bytes, start) {
            away_score = Some(std::str::from_utf8(val).ok()?);
        }
    }

    Some(V1Extract {
        fixture_id: fixture_id?,
        home_score: home_score.unwrap_or(""),
        away_score: away_score.unwrap_or(""),
        free_text: free_text.unwrap_or(""),
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
}
