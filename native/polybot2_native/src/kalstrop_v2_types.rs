//! Byte-level field extractor for Kalstrop V2 genius_update frames.
//! Extracts betGeniusFixtureId, homeScore, awayScore, currentPhase,
//! and the first matchAction's type/subType (for VAR detection).
//! Returns None for non-update frames.

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
static FINDER_MATCH_ACTIONS: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"matchActions\""));
static FINDER_TYPE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"type\""));
static FINDER_SUBTYPE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"subType\""));

pub(crate) struct V2Extract<'a> {
    pub fixture_id: &'a str,
    pub home_score: i64,
    pub away_score: i64,
    pub current_phase: &'a str,
    /// First matchAction type (e.g., "Var", "VarEnded", "Goal", "DangerStateChanged").
    /// Empty if no matchActions present.
    pub var_action_type: &'a str,
    /// First matchAction subType (e.g., "Goal", "GoalAwarded", "GoalNotAwarded").
    /// Empty if no matchActions present.
    pub var_action_subtype: &'a str,
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
    let (home_score, end) = find_key_integer(&FINDER_HOME_SCORE, 11, bytes, pos)?;
    pos = end;

    // VAR match action extraction (best-effort, not required for score processing).
    // Find "matchActions" array, then extract "type" and "subType" from the first entry.
    // The first entry is the most recent action.
    let (var_action_type, var_action_subtype) =
        extract_first_match_action(bytes, pos);

    Some(V2Extract {
        fixture_id,
        home_score,
        away_score,
        current_phase,
        var_action_type,
        var_action_subtype,
    })
}

/// Extract type and subType from the first entry in court.matchActions[].
/// Returns ("", "") if matchActions is absent, empty, or unparseable.
fn extract_first_match_action(bytes: &[u8], from: usize) -> (&str, &str) {
    let ma_pos = match find_with(&FINDER_MATCH_ACTIONS, bytes, from) {
        Some(p) => p,
        None => return ("", ""),
    };

    // Search within ~500 bytes after "matchActions" for the first entry's fields.
    // The first entry starts right after the opening '[' and '{'.
    let search_end = (ma_pos + 500).min(bytes.len());
    let search_region_start = ma_pos + 15; // skip past `"matchActions"`

    // Find "type" (the first occurrence after matchActions is the first entry's type)
    let action_type = find_key_string_start(&FINDER_TYPE, 6, bytes, search_region_start)
        .and_then(|start| extract_string_value(bytes, start))
        .and_then(|(val, _)| {
            if val.len() < 40 {
                std::str::from_utf8(val).ok()
            } else {
                None
            }
        })
        .unwrap_or("");

    // Only extract subType if we found a meaningful type (avoid scanning deeply)
    let var_subtype = if !action_type.is_empty() && search_region_start < search_end {
        find_key_string_start(&FINDER_SUBTYPE, 9, bytes, search_region_start)
            .and_then(|start| {
                if start < search_end {
                    extract_string_value(bytes, start)
                } else {
                    None
                }
            })
            .and_then(|(val, _)| {
                if val.len() < 40 {
                    std::str::from_utf8(val).ok()
                } else {
                    None
                }
            })
            .unwrap_or("")
    } else {
        ""
    };

    (action_type, var_subtype)
}

pub(crate) fn map_v2_phase(phase: &str) -> (&'static str, bool) {
    match phase {
        "FirstHalf" => ("1st half", false),
        "HalfTime" => ("Halftime", false),
        "SecondHalf" => ("2nd half", false),
        "FullTimeNormalTime" => ("Ended", true),
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
        assert_eq!(result.var_action_type, "");
        assert_eq!(result.var_action_subtype, "");
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
    fn test_extract_var_review() {
        let payload = r#"{"data":{"betGeniusFixtureId":"13755875","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"SecondHalf","awayScore":1,"homeScore":1},"matchInfo":{},"court":{"matchActions":[{"sportId":"10","id":"516","type":"Var","subType":"Goal","teamId":"10143","isConfirmed":true,"phase":"Regular","phaseQualifier":"Second Period","timestamp":"2026-05-04T20:27:07.698","actionDetails":{"varReason":"HomeGoal"},"gameTime":"01:08:16","competitorContributions":[]}]}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.fixture_id, "13755875");
        assert_eq!(result.home_score, 1);
        assert_eq!(result.away_score, 1);
        assert_eq!(result.var_action_type, "Var");
        assert_eq!(result.var_action_subtype, "Goal");
    }

    #[test]
    fn test_extract_var_ended_goal_awarded() {
        let payload = r#"{"data":{"betGeniusFixtureId":"13755875","scoreboardInfo":{"matchStatus":"InPlay","currentPhase":"SecondHalf","awayScore":1,"homeScore":1},"matchInfo":{},"court":{"matchActions":[{"sportId":"10","id":"517","type":"VarEnded","subType":"GoalAwarded","teamId":"10143","isConfirmed":true,"phase":"Regular","phaseQualifier":"Second Period","timestamp":"2026-05-04T20:28:14.753","actionDetails":{"varOutcome":"HomeGoalAwarded"},"gameTime":"01:09:23","competitorContributions":[]}]}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.var_action_type, "VarEnded");
        assert_eq!(result.var_action_subtype, "GoalAwarded");
    }

    #[test]
    fn test_extract_no_match_actions() {
        let payload = r#"{"data":{"betGeniusFixtureId":"123","scoreboardInfo":{"currentPhase":"FirstHalf","awayScore":0,"homeScore":0},"matchInfo":{},"court":{}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.var_action_type, "");
        assert_eq!(result.var_action_subtype, "");
    }

    #[test]
    fn test_extract_danger_state_action() {
        let payload = r#"{"data":{"betGeniusFixtureId":"123","scoreboardInfo":{"currentPhase":"SecondHalf","awayScore":0,"homeScore":0},"matchInfo":{},"court":{"matchActions":[{"sportId":"10","id":"510","type":"DangerStateChanged","subType":"DangerousAttack","teamId":"10143","isConfirmed":true}]}}}"#;
        let result = fast_extract_v2(payload).unwrap();
        assert_eq!(result.var_action_type, "DangerStateChanged");
        assert_eq!(result.var_action_subtype, "DangerousAttack");
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
        assert_eq!(map_v2_phase("PostMatch"), ("", false));
        assert_eq!(map_v2_phase("Unknown"), ("", false));
    }
}
