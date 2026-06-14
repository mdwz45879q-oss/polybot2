use memchr::memmem::Finder;
use std::sync::LazyLock;

static FINDER_TYPE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"type\""));
static FINDER_GAME_ID: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"game_id\""));
static FINDER_MATCH_STATUS: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"match_status\""));
static FINDER_HOME_TEAM: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"home_team\""));
static FINDER_AWAY_TEAM: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"away_team\""));
static FINDER_SCORE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"score\""));

pub(crate) struct PandaScoreCs2Extract {
    pub game_id: i64,
    pub home_score: i64,
    pub away_score: i64,
    pub match_status_finished: bool,
}

fn find_in(finder: &Finder, bytes: &[u8], from: usize) -> Option<usize> {
    if from >= bytes.len() {
        return None;
    }
    finder.find(&bytes[from..]).map(|p| p + from)
}

fn skip_ws(bytes: &[u8], mut p: usize) -> usize {
    while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
        p += 1;
    }
    p
}

fn extract_integer_after_colon(bytes: &[u8], key_end: usize) -> Option<(i64, usize)> {
    let mut p = skip_ws(bytes, key_end);
    if p >= bytes.len() || bytes[p] != b':' {
        return None;
    }
    p = skip_ws(bytes, p + 1);
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
        Some((if negative { -acc } else { acc }, p))
    } else {
        None
    }
}

fn find_key_integer(finder: &Finder, key_len: usize, bytes: &[u8], from: usize) -> Option<(i64, usize)> {
    let idx = find_in(finder, bytes, from)?;
    extract_integer_after_colon(bytes, idx + key_len)
}

/// Extract `"score":N` within the next JSON object starting at `from`.
/// Scans forward from `from` looking for `"score"`, but stops at the
/// object's closing `}` to avoid reading into sibling objects.
fn extract_nested_score(bytes: &[u8], from: usize) -> Option<(i64, usize)> {
    // Find the opening `{` of the team object
    let mut p = from;
    while p < bytes.len() && bytes[p] != b'{' {
        p += 1;
    }
    if p >= bytes.len() {
        return None;
    }
    let obj_start = p;

    // Find "score" within this object. Track brace depth to avoid
    // crossing into nested objects (current_round has its own objects).
    let score_pos = find_in(&FINDER_SCORE, bytes, obj_start)?;

    // Verify "score" is within the team object (before the matching `}`)
    // by checking brace depth. For home_team/away_team, "score" is at
    // depth 1 — right inside the team object, not nested deeper.
    let mut depth = 0i32;
    for i in obj_start..score_pos {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => depth -= 1,
            _ => {}
        }
        if depth <= 0 {
            return None; // "score" is outside the team object
        }
    }

    extract_integer_after_colon(bytes, score_pos + b"\"score\"".len())
}

/// Check if the value after a key position matches a specific string.
fn value_matches_str(bytes: &[u8], key_end: usize, target: &[u8]) -> bool {
    let mut p = skip_ws(bytes, key_end);
    if p >= bytes.len() || bytes[p] != b':' {
        return false;
    }
    p = skip_ws(bytes, p + 1);
    if p >= bytes.len() || bytes[p] != b'"' {
        return false;
    }
    p += 1; // skip opening quote
    let end = p + target.len();
    if end >= bytes.len() {
        return false;
    }
    &bytes[p..end] == target && bytes[end] == b'"'
}

/// Find a top-level team key and extract its nested "score" field.
/// Retries on the next occurrence if the first match is inside
/// `current_round` (which has its own `home_team`/`away_team` without `score`).
fn find_team_score(
    finder: &Finder,
    key_len: usize,
    bytes: &[u8],
    from: usize,
) -> Option<(i64, usize)> {
    let mut pos = from;
    loop {
        let team_pos = find_in(finder, bytes, pos)?;
        if let Some(result) = extract_nested_score(bytes, team_pos + key_len) {
            return Some(result);
        }
        pos = team_pos + 1;
    }
}

pub(crate) fn fast_extract_pandascore_cs2(bytes: &[u8]) -> Option<PandaScoreCs2Extract> {
    // Reject hello frames early
    if let Some(type_pos) = find_in(&FINDER_TYPE, bytes, 0) {
        if value_matches_str(bytes, type_pos + b"\"type\"".len(), b"hello") {
            return None;
        }
    }

    // home_team presence → game state frame. If absent → status change, skip.
    // Retry if first match is inside current_round (no "score" field there).
    let (home_score, home_end) =
        find_team_score(&FINDER_HOME_TEAM, b"\"home_team\"".len(), bytes, 0)?;

    // away_team.score — search after home_team's position
    let (away_score, _) =
        find_team_score(&FINDER_AWAY_TEAM, b"\"away_team\"".len(), bytes, home_end)?;

    // Extract game_id (top-level, search from start)
    let (game_id, _) = find_key_integer(&FINDER_GAME_ID, b"\"game_id\"".len(), bytes, 0)?;

    // Check match_status == "finished"
    let match_status_finished = if let Some(ms_pos) = find_in(&FINDER_MATCH_STATUS, bytes, 0) {
        value_matches_str(bytes, ms_pos + b"\"match_status\"".len(), b"finished")
    } else {
        false
    };

    Some(PandaScoreCs2Extract {
        game_id,
        home_score,
        away_score,
        match_status_finished,
    })
}

pub(crate) fn serde_fallback_pandascore_cs2(text: &str) -> Option<PandaScoreCs2Extract> {
    let frame: super::types::PandaScoreLLFrame = serde_json::from_str(text).ok()?;
    if frame.is_hello() || !frame.is_game_state() {
        return None;
    }
    let home = frame.home_team.as_ref()?;
    let away = frame.away_team.as_ref()?;
    Some(PandaScoreCs2Extract {
        game_id: frame.game_id.unwrap_or(0),
        home_score: home.score,
        away_score: away.score,
        match_status_finished: frame.match_status.as_deref() == Some("finished"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_game_state_frame() {
        let json = r#"{"match_id":1513132,"game_id":219829,"game_status":"running","match_status":"running","home_team":{"team_id":3216,"team_name":"Natus Vincere","score":12},"away_team":{"team_id":133708,"team_name":"Legacy","score":4},"current_round":{"round_number":17,"timer":82}}"#;
        let ext = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        assert_eq!(ext.game_id, 219829);
        assert_eq!(ext.home_score, 12);
        assert_eq!(ext.away_score, 4);
        assert!(!ext.match_status_finished);
    }

    #[test]
    fn extract_hello_returns_none() {
        let json = r#"{"type":"hello","payload":{"match_id":"1513141","match_status":"not_started","videogame":"csgo"}}"#;
        assert!(fast_extract_pandascore_cs2(json.as_bytes()).is_none());
    }

    #[test]
    fn extract_status_change_returns_none() {
        let json = r#"{"updated_at":"2026-06-12T15:21:36.628Z","match_id":1513132,"game_id":219829,"game_status":"finished","match_status":"running"}"#;
        assert!(fast_extract_pandascore_cs2(json.as_bytes()).is_none());
    }

    #[test]
    fn extract_ot_scores() {
        let json = r#"{"match_id":100,"game_id":200,"match_status":"running","home_team":{"team_id":1,"team_name":"A","score":16},"away_team":{"team_id":2,"team_name":"B","score":14},"current_round":{"round_number":31}}"#;
        let ext = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        assert_eq!(ext.home_score, 16);
        assert_eq!(ext.away_score, 14);
    }

    #[test]
    fn extract_zero_scores() {
        let json = r#"{"match_id":100,"game_id":300,"game_status":"not_started","match_status":"running","home_team":{"team_id":1,"team_name":"A","score":0},"away_team":{"team_id":2,"team_name":"B","score":0},"current_round":{"round_number":1,"timer":115}}"#;
        let ext = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        assert_eq!(ext.game_id, 300);
        assert_eq!(ext.home_score, 0);
        assert_eq!(ext.away_score, 0);
    }

    #[test]
    fn serde_fallback_works() {
        let json = r#"{"match_id":1513132,"game_id":219829,"match_status":"running","home_team":{"team_id":3216,"team_name":"Navi","score":9},"away_team":{"team_id":133708,"team_name":"Legacy","score":13},"current_round":{"round_number":22}}"#;
        let fast = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        let serde = serde_fallback_pandascore_cs2(json).unwrap();
        assert_eq!(fast.game_id, serde.game_id);
        assert_eq!(fast.home_score, serde.home_score);
        assert_eq!(fast.away_score, serde.away_score);
        assert_eq!(fast.match_status_finished, serde.match_status_finished);
    }

    #[test]
    fn match_status_finished_detected() {
        let json = r#"{"match_id":100,"game_id":200,"match_status":"finished","home_team":{"team_id":1,"team_name":"A","score":5},"away_team":{"team_id":2,"team_name":"B","score":3}}"#;
        let ext = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        assert!(ext.match_status_finished);
    }

    #[test]
    fn unknown_fields_ignored() {
        let json = r#"{"future_field":"test","match_id":100,"game_id":200,"match_status":"running","home_team":{"team_id":1,"team_name":"X","score":7,"extra":true},"away_team":{"team_id":2,"team_name":"Y","score":8},"map_id":3}"#;
        let ext = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        assert_eq!(ext.home_score, 7);
        assert_eq!(ext.away_score, 8);
        assert_eq!(ext.game_id, 200);
    }

    #[test]
    fn real_frame_with_nested_current_round() {
        // Ensures "score" inside current_round's nested team objects
        // doesn't confuse the home_team/away_team score extraction
        let json = r#"{"match_id":1513132,"updated_at":"2026-06-12T15:16:58.675308Z","current_round":{"timer":3,"home_team":{"team_id":3216,"players":[{"alive":false,"name":"Aleksib"}],"side":"terrorists"},"away_team":{"team_id":133708,"players":[{"alive":true,"name":"arT"}],"side":"counter_terrorists"},"round_number":15,"bomb":{"status":"planted"}},"game_id":219829,"game_status":"running","match_status":"running","map_id":2,"home_team":{"team_id":3216,"team_name":"Natus Vincere","score":11},"away_team":{"team_id":133708,"team_name":"Legacy","score":3}}"#;
        let ext = fast_extract_pandascore_cs2(json.as_bytes()).unwrap();
        assert_eq!(ext.game_id, 219829);
        assert_eq!(ext.home_score, 11);
        assert_eq!(ext.away_score, 3);
    }
}
