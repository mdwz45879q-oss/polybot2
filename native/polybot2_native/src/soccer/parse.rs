//! Soccer-specific freeText parsing.

/// Parse the match half from Kalstrop freeText.
///
/// Known patterns:
///   "1st half"    → "1st"
///   "Halftime"    → "Halftime"
///   "2nd half"    → "2nd"
///   "Not started" → ""
///   "Ended"       → ""
///   ""            → ""
///
/// Extra time / penalties: TODO — returns "" for now.
pub(crate) fn parse_half(text: &str) -> &'static str {
    let s = text.trim();
    if s.is_empty() {
        return "";
    }
    let lower = s.to_ascii_lowercase();
    if lower == "1st half" {
        return "1st";
    }
    if lower == "halftime" || lower == "half time" || lower == "ht" {
        return "Halftime";
    }
    if lower == "2nd half" {
        return "2nd";
    }
    // Extra time / penalties: TODO
    ""
}

/// Detect game completion from freeText. Same completion terms as baseball
/// — Kalstrop V1 uses the same vocabulary across sports.
pub(crate) fn is_completed_free_text(free_text: &str) -> bool {
    let s = free_text.trim();
    s.eq_ignore_ascii_case("Ended")
        || s.eq_ignore_ascii_case("Final")
        || s.eq_ignore_ascii_case("Game Over")
        || s.eq_ignore_ascii_case("Finished")
        || s.eq_ignore_ascii_case("FT")
}

/// Derive game state from freeText.
#[allow(dead_code)]
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
    fn parse_half_first() {
        assert_eq!(parse_half("1st half"), "1st");
    }

    #[test]
    fn parse_half_halftime() {
        assert_eq!(parse_half("Halftime"), "Halftime");
    }

    #[test]
    fn parse_half_second() {
        assert_eq!(parse_half("2nd half"), "2nd");
    }

    #[test]
    fn parse_half_not_started() {
        assert_eq!(parse_half("Not started"), "");
    }

    #[test]
    fn parse_half_ended() {
        assert_eq!(parse_half("Ended"), "");
    }

    #[test]
    fn parse_half_empty() {
        assert_eq!(parse_half(""), "");
    }

    #[test]
    fn completion_ended() {
        assert!(is_completed_free_text("Ended"));
    }

    #[test]
    fn completion_ft() {
        assert!(is_completed_free_text("FT"));
    }

    #[test]
    fn not_completed_live() {
        assert!(!is_completed_free_text("1st half"));
    }
}
