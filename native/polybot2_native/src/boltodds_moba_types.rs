//! BoltOdds MOBA (LoL/Dota2) frame extractor.
//! Extracts game label (`event` field) and maps-won scores from
//! `new_play` frames. Includes serde fallback for resilience.

use memchr::memmem::Finder;
use std::sync::LazyLock;

pub(crate) struct BoltOddsMobaExtract<'a> {
    pub game_label: &'a str,
    pub maps_home: i64,
    pub maps_away: i64,
}

pub(crate) struct BoltOddsMobaExtractOwned {
    pub game_label: String,
    pub maps_home: i64,
    pub maps_away: i64,
}

// ─── Byte-level finders ────────────────────────────────────────────

static FINDER_NEW_PLAY: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"new_play\""));
static FINDER_EVENT: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"event\""));
// The "score" field appears inside both "home" and "away" blocks.
// We locate each block first, then find "score" within it.
static FINDER_TEAMS: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"teams\""));
static FINDER_HOME: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"home\""));
static FINDER_AWAY: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"away\""));
static FINDER_SCORE: LazyLock<Finder<'static>> =
    LazyLock::new(|| Finder::new(b"\"score\""));

fn find_with(finder: &Finder, bytes: &[u8], from: usize) -> Option<usize> {
    finder.find(&bytes[from..]).map(|i| i + from)
}

fn extract_string_value(bytes: &[u8], start: usize) -> Option<(&[u8], usize)> {
    let mut p = start;
    while p < bytes.len() && bytes[p] != b'"' {
        p += 1;
    }
    if p >= bytes.len() {
        return None;
    }
    let val_start = p + 1;
    p = val_start;
    while p < bytes.len() && bytes[p] != b'"' {
        if bytes[p] == b'\\' {
            p += 1;
        }
        p += 1;
    }
    if p >= bytes.len() {
        return None;
    }
    Some((&bytes[val_start..p], p + 1))
}

fn find_integer_after_colon(bytes: &[u8], key_pos: usize, key_len: usize) -> Option<(i64, usize)> {
    let mut p = key_pos + key_len;
    while p < bytes.len() && matches!(bytes[p], b' ' | b'\t' | b'\n' | b'\r') {
        p += 1;
    }
    if p >= bytes.len() || bytes[p] != b':' {
        return None;
    }
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
        Some((if negative { -acc } else { acc }, p))
    } else {
        None
    }
}

/// Fast byte-level extractor for BoltOdds MOBA frames.
/// Extracts `event` (game label), `teams.home.score`, `teams.away.score`.
pub(crate) fn fast_extract_boltodds_moba(json: &str) -> Option<BoltOddsMobaExtract<'_>> {
    let bytes = json.as_bytes();

    // Quick reject: must contain "new_play"
    find_with(&FINDER_NEW_PLAY, bytes, 0)?;

    // Extract "event" field (game label)
    let event_pos = find_with(&FINDER_EVENT, bytes, 0)?;
    let (event_bytes, pos) = extract_string_value(bytes, event_pos + 7)?;
    let game_label = std::str::from_utf8(event_bytes).ok()?;
    if game_label.is_empty() {
        return None;
    }

    // Find "teams" block, then "home" → "score" and "away" → "score"
    let teams_pos = find_with(&FINDER_TEAMS, bytes, pos)?;
    let home_pos = find_with(&FINDER_HOME, bytes, teams_pos)?;
    let score_home_pos = find_with(&FINDER_SCORE, bytes, home_pos)?;
    let (maps_home, _) = find_integer_after_colon(bytes, score_home_pos, 7)?;

    let away_pos = find_with(&FINDER_AWAY, bytes, home_pos)?;
    let score_away_pos = find_with(&FINDER_SCORE, bytes, away_pos)?;
    let (maps_away, _) = find_integer_after_colon(bytes, score_away_pos, 7)?;

    Some(BoltOddsMobaExtract {
        game_label,
        maps_home,
        maps_away,
    })
}

// ─── Serde fallback ────────────────────────────────────────────────

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct BoMobaFrame {
    action: String,
    event: Option<String>,
    play_info: Option<BoMobaPlayInfo>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct BoMobaPlayInfo {
    teams: Option<BoMobaTeams>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct BoMobaTeams {
    home: Option<BoMobaTeamScore>,
    away: Option<BoMobaTeamScore>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct BoMobaTeamScore {
    score: Option<i64>,
}

/// Serde-based extraction fallback. Only called when the byte-level
/// extractor fails. Resilient to field reordering and structural changes.
pub(crate) fn serde_extract_boltodds_moba(json: &str) -> Option<BoltOddsMobaExtractOwned> {
    let frame: BoMobaFrame = serde_json::from_str(json).ok()?;
    if frame.action != "new_play" {
        return None;
    }
    let game_label = frame.event?;
    if game_label.is_empty() {
        return None;
    }
    let pi = frame.play_info?;
    let teams = pi.teams?;
    let maps_home = teams.home?.score?;
    let maps_away = teams.away?.score?;
    Some(BoltOddsMobaExtractOwned {
        game_label,
        maps_home,
        maps_away,
    })
}

// ─── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const LOL_FRAME: &str = r#"{"stream":8,"action":"new_play","event":"Solary vs Karmine Corp Blue, 2026-05-27, 12","universal_id":"c35f2e29f5e2","home":"Solary","away":"Karmine Corp Blue","league":"League of Legends","state":"new_play","play_info":{"currentMap":2,"currentGoldLead":9460,"timer":{"isActive":true,"time":2153000,"__typename":"Timer"},"teams":{"home":{"score":2,"__typename":"LOLOverviewTeam"},"away":{"score":0,"__typename":"LOLOverviewTeam"},"__typename":"LOLOverviewTeams"},"maps":[],"bestOf":5,"__typename":"LOLOverview"}}"#;

    const DOTA_FRAME: &str = r#"{"stream":8,"action":"new_play","event":"Team Liquid vs PARIVISION, 2026-05-29, 07","universal_id":"3c8a6f74af93","home":"Team Liquid","away":"PARIVISION","league":"Dota","state":"new_play","play_info":{"currentMap":1,"currentGoldLead":-2639,"timer":{"isActive":true,"time":1159000,"__typename":"Timer"},"teams":{"home":{"score":0,"__typename":"Dota2OverviewTeam"},"away":{"score":0,"__typename":"Dota2OverviewTeam"},"__typename":"Dota2OverviewTeams"},"bestOf":1,"__typename":"Dota2Overview"}}"#;

    #[test]
    fn test_fast_extract_lol() {
        let r = fast_extract_boltodds_moba(LOL_FRAME).unwrap();
        assert_eq!(r.game_label, "Solary vs Karmine Corp Blue, 2026-05-27, 12");
        assert_eq!(r.maps_home, 2);
        assert_eq!(r.maps_away, 0);
    }

    #[test]
    fn test_fast_extract_dota() {
        let r = fast_extract_boltodds_moba(DOTA_FRAME).unwrap();
        assert_eq!(r.game_label, "Team Liquid vs PARIVISION, 2026-05-29, 07");
        assert_eq!(r.maps_home, 0);
        assert_eq!(r.maps_away, 0);
    }

    #[test]
    fn test_serde_extract_lol() {
        let r = serde_extract_boltodds_moba(LOL_FRAME).unwrap();
        assert_eq!(r.game_label, "Solary vs Karmine Corp Blue, 2026-05-27, 12");
        assert_eq!(r.maps_home, 2);
        assert_eq!(r.maps_away, 0);
    }

    #[test]
    fn test_serde_extract_dota() {
        let r = serde_extract_boltodds_moba(DOTA_FRAME).unwrap();
        assert_eq!(r.game_label, "Team Liquid vs PARIVISION, 2026-05-29, 07");
        assert_eq!(r.maps_home, 0);
        assert_eq!(r.maps_away, 0);
    }

    #[test]
    fn test_rejects_connected_ack() {
        let frame = r#"{"stream":8,"action":"connected","event":"Solary vs Karmine Corp Blue, 2026-05-27, 12","universal_id":"c35f2e29f5e2"}"#;
        assert!(fast_extract_boltodds_moba(frame).is_none());
        assert!(serde_extract_boltodds_moba(frame).is_none());
    }
}
