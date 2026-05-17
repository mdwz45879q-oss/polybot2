pub(crate) fn is_completed_free_text(free_text: &str) -> bool {
    let s = free_text.trim();
    s.eq_ignore_ascii_case("Ended")
        || s.eq_ignore_ascii_case("Final")
        || s.eq_ignore_ascii_case("Game Over")
        || s.eq_ignore_ascii_case("Finished")
        || s.eq_ignore_ascii_case("FT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_variants() {
        assert!(is_completed_free_text("Ended"));
        assert!(is_completed_free_text("Final"));
        assert!(is_completed_free_text("Game Over"));
        assert!(is_completed_free_text("Finished"));
        assert!(is_completed_free_text("FT"));
        assert!(is_completed_free_text(" Ended "));
    }

    #[test]
    fn non_completed() {
        assert!(!is_completed_free_text("1st half"));
        assert!(!is_completed_free_text("Break top 2 bottom 1"));
        assert!(!is_completed_free_text(""));
    }
}
