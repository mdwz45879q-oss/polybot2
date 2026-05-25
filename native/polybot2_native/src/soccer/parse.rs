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
    if s.eq_ignore_ascii_case("1st half") {
        return "1st";
    }
    if s.eq_ignore_ascii_case("halftime")
        || s.eq_ignore_ascii_case("half time")
        || s.eq_ignore_ascii_case("ht")
    {
        return "Halftime";
    }
    if s.eq_ignore_ascii_case("2nd half") {
        return "2nd";
    }
    ""
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
}
