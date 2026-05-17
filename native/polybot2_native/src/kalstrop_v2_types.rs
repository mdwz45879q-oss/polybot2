//! Byte-level field extractor for Kalstrop V2 genius_update frames.
//! Extracts betGeniusFixtureId, homeScore, awayScore, currentPhase
//! without full JSON parsing. Returns None for non-update frames.

use memchr::memmem::Finder;
use std::sync::LazyLock;

static FINDER_FIXTURE_ID: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"betGeniusFixtureId\""));
static FINDER_HOME_SCORE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"homeScore\""));
static FINDER_AWAY_SCORE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"awayScore\""));
static FINDER_CURRENT_PHASE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"currentPhase\""));

pub(crate) struct V2Extract<'a> {
    pub fixture_id: &'a str,
    pub home_score: i64,
    pub away_score: i64,
    pub current_phase: &'a str,
}

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

fn find_key_string_start(finder: &Finder, key_len: usize, bytes: &[u8], from: usize) -> Option<usize> {
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

fn find_key_integer(finder: &Finder, key_len: usize, bytes: &[u8], from: usize) -> Option<(i64, usize)> {
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

pub(crate) fn fast_extract_v2(frame: &str) -> Option<V2Extract<'_>> {
    let bytes = frame.as_bytes();

    // When called from the frame pipeline, the genius_update wrapper is already
    // stripped by classify_frame — only check for the fixture_id key instead.
    if find_with(&FINDER_FIXTURE_ID, bytes, 0).is_none() {
        return None;
    }

    let mut pos = 0usize;

    // betGeniusFixtureId (string) — appears first in data.data
    let fid_start = find_key_string_start(&FINDER_FIXTURE_ID, 20, bytes, pos)?;
    let (fid_bytes, end) = extract_string_value(bytes, fid_start)?;
    let fixture_id = std::str::from_utf8(fid_bytes).ok()?;
    if fixture_id.is_empty() {
        return None;
    }
    pos = end;

    // currentPhase (string) — appears before scores in scoreboardInfo
    let phase_start = find_key_string_start(&FINDER_CURRENT_PHASE, 14, bytes, pos)?;
    let (phase_bytes, end) = extract_string_value(bytes, phase_start)?;
    let current_phase = std::str::from_utf8(phase_bytes).ok()?;
    pos = end;

    // awayScore (integer) — appears before homeScore in scoreboardInfo
    let (away_score, end) = find_key_integer(&FINDER_AWAY_SCORE, 11, bytes, pos)?;
    pos = end;

    // homeScore (integer)
    let (home_score, _) = find_key_integer(&FINDER_HOME_SCORE, 11, bytes, pos)?;

    Some(V2Extract {
        fixture_id,
        home_score,
        away_score,
        current_phase,
    })
}

pub(crate) fn map_v2_phase(phase: &str) -> (&'static str, bool) {
    match phase {
        "FirstHalf" => ("1st half", false),
        "HalfTime" => ("Halftime", false),
        "SecondHalf" => ("2nd half", false),
        "FullTimeNormalTime" | "PostMatch" => ("Ended", true),
        _ => ("", false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_stripped_payload() {
        // This is how process_v2_frame_sync calls it — payload after classify_frame strips the event wrapper
        let payload = r#"{"data":{"betGeniusFixtureId":"12483313","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"FirstHalf","awayScore":0,"homeScore":0},"matchInfo":{},"court":{}},"sport":"Football"}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.fixture_id, "12483313");
        assert_eq!(result.home_score, 0);
        assert_eq!(result.away_score, 0);
        assert_eq!(result.current_phase, "FirstHalf");
    }

    #[test]
    fn test_extract_full_frame() {
        // Also works with the full 42[...] wrapper (e.g. if called on raw frame)
        let frame = r#"42["genius_update",{"data":{"betGeniusFixtureId":"12483313","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"FirstHalf","awayScore":0,"homeScore":0},"matchInfo":{},"court":{}},"sport":"Football"}]"#;
        let result = fast_extract_v2(frame).unwrap();
        assert_eq!(result.fixture_id, "12483313");
    }

    #[test]
    fn test_extract_score_change() {
        let payload = r#"{"data":{"betGeniusFixtureId":"99999","scoreboardInfo":{"currentPhase":"SecondHalf","awayScore":1,"homeScore":2}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.fixture_id, "99999");
        assert_eq!(result.home_score, 2);
        assert_eq!(result.away_score, 1);
        assert_eq!(result.current_phase, "SecondHalf");
    }

    #[test]
    fn test_extract_halftime() {
        let payload = r#"{"data":{"betGeniusFixtureId":"123","scoreboardInfo":{"currentPhase":"HalfTime","awayScore":0,"homeScore":1}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.current_phase, "HalfTime");
    }

    #[test]
    fn test_extract_fulltime() {
        let payload = r#"{"data":{"betGeniusFixtureId":"123","scoreboardInfo":{"currentPhase":"PostMatch","awayScore":2,"homeScore":3}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.current_phase, "PostMatch");
        assert_eq!(result.home_score, 3);
        assert_eq!(result.away_score, 2);
    }

    #[test]
    fn test_reject_non_update() {
        assert!(fast_extract_v2("2").is_none());
        assert!(fast_extract_v2(r#"{"sid":"abc"}"#).is_none());
        assert!(fast_extract_v2(r#"{"status":"success"}"#).is_none());
    }

    #[test]
    fn test_phase_mapping() {
        assert_eq!(map_v2_phase("PreMatch"), ("", false));
        assert_eq!(map_v2_phase("FirstHalf"), ("1st half", false));
        assert_eq!(map_v2_phase("HalfTime"), ("Halftime", false));
        assert_eq!(map_v2_phase("SecondHalf"), ("2nd half", false));
        assert_eq!(map_v2_phase("FullTimeNormalTime"), ("Ended", true));
        assert_eq!(map_v2_phase("PostMatch"), ("Ended", true));
        assert_eq!(map_v2_phase("Unknown"), ("", false));
    }
}
