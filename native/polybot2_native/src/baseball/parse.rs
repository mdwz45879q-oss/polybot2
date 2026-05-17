#[cfg(test)]
pub(crate) fn parse_tick_from_kalstrop_update(
    update: &crate::kalstrop_types::KalstropUpdate<'_>,
    _recv_monotonic_ns: i64,
) -> crate::baseball::types::Tick {
    let summary = update.match_summary.as_ref();
    let free_text = summary.and_then(|s| s.first_free_text).unwrap_or("");
    let (inning_number, inning_half) = parse_period(free_text);

    crate::baseball::types::Tick {
        universal_id: update.fixture_id.to_owned(),
        goals_home: summary
            .and_then(|s| s.home_score)
            .and_then(|s| s.parse().ok()),
        goals_away: summary
            .and_then(|s| s.away_score)
            .and_then(|s| s.parse().ok()),
        inning_number,
        inning_half,
        match_completed: if free_text.is_empty() {
            None
        } else {
            Some(is_completed_free_text(free_text))
        },
        game_state: normalize_game_state_from_free_text(free_text),
    }
}

// ---------------------------------------------------------------------------
// Period / inning parsing
// ---------------------------------------------------------------------------

/// Parse inning_number and inning_half from Kalstrop freeText.
///
/// Known patterns:
///   "4th inning top"         → (Some(4), "top")
///   "4th inning bottom"      → (Some(4), "bottom")
///   "Break top 3 bottom 3"   → (Some(3), "break")
///   "Not started"            → (None, "")
///   "Ended"                  → (None, "")
pub(crate) fn parse_period(text: &str) -> (Option<i64>, &'static str) {
    let s = text.trim();
    if s.is_empty() {
        return (None, "");
    }

    // "Break top 3 bottom 3", "Break top EI bottom 9", "Break top EI bottom EI"
    // Check breaks first — extra-inning breaks use "EI" (not "extra").
    if s.len() >= 5 && s.as_bytes()[..5].eq_ignore_ascii_case(b"break") {
        if contains_ascii_ci(s, " EI") {
            // Extra-inning break: sentinel inning 10 so walkoff (>= 9) works.
            return (Some(10), "break");
        }
        let number = extract_first_number_fast(s.as_bytes());
        return (number, "break");
    }

    // "Extra inning top", "Extra inning bottom"
    // The freeText is identical regardless of which extra inning (10th, 11th, etc.).
    // Use inning_number = 10 as a sentinel so walkoff (inning >= 9) fires correctly.
    if contains_ascii_ci(s, "extra") && contains_ascii_ci(s, "inning") {
        let half = if contains_ascii_ci(s, "top") {
            "top"
        } else if contains_ascii_ci(s, "bottom") {
            "bottom"
        } else {
            ""
        };
        return (Some(10), half);
    }

    // "4th inning top" / "4th inning bottom"
    if contains_ascii_ci(s, "inning") {
        let half = if contains_ascii_ci(s, "top") {
            "top"
        } else if contains_ascii_ci(s, "bottom") {
            "bottom"
        } else {
            ""
        };
        let number = extract_first_number_fast(s.as_bytes());
        return (number, half);
    }

    (None, "")
}

fn extract_first_number_fast(bytes: &[u8]) -> Option<i64> {
    let mut i = 0;
    while i < bytes.len() && !bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    let mut n: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        n = n * 10 + (bytes[i] - b'0') as i64;
        i += 1;
    }
    Some(n)
}

fn contains_ascii_ci(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    for start in 0..=(h.len() - n.len()) {
        if h[start..start + n.len()].eq_ignore_ascii_case(n) {
            return true;
        }
    }
    false
}

pub(crate) use crate::parse_common::is_completed_free_text;

#[cfg(test)]
pub(crate) fn normalize_game_state_from_free_text(free_text: &str) -> &'static str {
    let s = free_text.trim();
    if s.is_empty() {
        return "UNKNOWN";
    }
    if is_completed_free_text(s) {
        return "FINAL";
    }
    "LIVE"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_period_inning_top() {
        let (num, half) = parse_period("4th inning top");
        assert_eq!(num, Some(4));
        assert_eq!(half, "top");
    }

    #[test]
    fn parse_period_inning_bottom() {
        let (num, half) = parse_period("1st inning bottom");
        assert_eq!(num, Some(1));
        assert_eq!(half, "bottom");
    }

    #[test]
    fn parse_period_break() {
        let (num, half) = parse_period("Break top 3 bottom 3");
        assert_eq!(num, Some(3));
        assert_eq!(half, "break");
    }

    #[test]
    fn parse_period_not_started() {
        let (num, half) = parse_period("Not started");
        assert_eq!(num, None);
        assert_eq!(half, "");
    }

    #[test]
    fn parse_period_ended() {
        let (num, half) = parse_period("Ended");
        assert_eq!(num, None);
        assert_eq!(half, "");
    }

    #[test]
    fn parse_period_extra_inning_top() {
        let (num, half) = parse_period("Extra inning top");
        assert_eq!(num, Some(10));
        assert_eq!(half, "top");
    }

    #[test]
    fn parse_period_extra_inning_bottom() {
        let (num, half) = parse_period("Extra inning bottom");
        assert_eq!(num, Some(10));
        assert_eq!(half, "bottom");
    }

    #[test]
    fn parse_period_break_into_extras() {
        // Break between bottom 9th and top of first extra inning
        let (num, half) = parse_period("Break top EI bottom 9");
        assert_eq!(num, Some(10));
        assert_eq!(half, "break");
    }

    #[test]
    fn parse_period_break_within_extras() {
        // Break between top and bottom of an extra inning
        let (num, half) = parse_period("Break top EI bottom EI");
        assert_eq!(num, Some(10));
        assert_eq!(half, "break");
    }

    #[test]
    fn parse_period_empty() {
        let (num, half) = parse_period("");
        assert_eq!(num, None);
        assert_eq!(half, "");
    }

    #[test]
    fn parse_period_double_digit_inning() {
        let (num, half) = parse_period("10th inning top");
        assert_eq!(num, Some(10));
        assert_eq!(half, "top");
    }
}
