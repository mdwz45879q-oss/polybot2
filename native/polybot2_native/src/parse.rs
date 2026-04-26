use super::*;
use crate::kalstrop_types::KalstropUpdate;

// ---------------------------------------------------------------------------
// Zero-copy Kalstrop parse (live WS path)
// ---------------------------------------------------------------------------

pub(crate) fn parse_tick_from_kalstrop_update(
    update: &KalstropUpdate<'_>,
    recv_monotonic_ns: i64,
) -> Tick {
    let summary = update.match_summary.as_ref();
    let event_state = summary.and_then(|s| s.event_state).unwrap_or("");
    let free_text = summary
        .and_then(|s| s.first_free_text)
        .unwrap_or("");
    let period_text = if free_text.is_empty() {
        event_state
    } else {
        free_text
    };
    let (inning_number, inning_half) = parse_period(period_text);

    Tick {
        universal_id: update.fixture_id.to_owned(),
        action: "sportsMatchStateUpdatedV2",
        recv_monotonic_ns,
        goals_home: summary
            .and_then(|s| s.home_score)
            .and_then(|s| s.parse().ok()),
        goals_away: summary
            .and_then(|s| s.away_score)
            .and_then(|s| s.parse().ok()),
        inning_number,
        inning_half,
        match_completed: if event_state.is_empty() {
            None
        } else {
            Some(is_completed_state(event_state))
        },
        game_state: normalize_game_state(event_state),
    }
}

// ---------------------------------------------------------------------------
// PyO3 parse paths (Python callers — not the live WS hot path)
// ---------------------------------------------------------------------------

pub(crate) fn parse_tick_any(event: &Bound<'_, PyAny>, recv_monotonic_ns: i64) -> Tick {
    if let Ok(as_dict) = event.downcast::<PyDict>() {
        return parse_tick_from_dict(as_dict, recv_monotonic_ns);
    }
    parse_tick_from_event(event, recv_monotonic_ns)
}

pub(crate) fn parse_tick_from_dict(event: &Bound<'_, PyDict>, recv_monotonic_ns: i64) -> Tick {
    let inning_half_str = get_str(event, "inning_half");
    Tick {
        universal_id: get_str(event, "universal_id"),
        action: "sportsMatchStateUpdatedV2",
        recv_monotonic_ns: if recv_monotonic_ns > 0 {
            recv_monotonic_ns
        } else {
            get_i64_opt(event, "recv_monotonic_ns").unwrap_or(0)
        },
        goals_home: get_i64_opt(event, "goals_home"),
        goals_away: get_i64_opt(event, "goals_away"),
        inning_number: get_i64_opt(event, "inning_number"),
        inning_half: map_inning_half(&inning_half_str),
        match_completed: get_bool_opt(event, "match_completed"),
        game_state: normalize_game_state(&get_str(event, "game_state")),
    }
}

fn parse_tick_from_event(event: &Bound<'_, PyAny>, recv_monotonic_ns: i64) -> Tick {
    let match_completed = get_attr_bool_opt(event, "match_completed");
    Tick {
        universal_id: get_attr_str(event, "universal_id"),
        action: "sportsMatchStateUpdatedV2",
        recv_monotonic_ns,
        goals_home: get_attr_i64_opt(event, "home_score"),
        goals_away: get_attr_i64_opt(event, "away_score"),
        inning_number: None,
        inning_half: "",
        match_completed,
        game_state: normalize_game_state(&get_attr_str(event, "game_state")),
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
    let s = text.trim().to_lowercase();
    if s.is_empty() {
        return (None, "");
    }

    // TODO: Handle extra innings. Kalstrop's exact freeText format for extras
    // is unknown. Guard: if "extra" appears anywhere, return None to avoid
    // misidentifying it as inning 1.
    if s.contains("extra") {
        return (None, "");
    }

    // "4th inning top" / "4th inning bottom"
    if s.contains("inning") {
        let half = if s.contains("top") {
            "top"
        } else if s.contains("bottom") {
            "bottom"
        } else {
            ""
        };
        let number = extract_first_number(&s);
        return (number, half);
    }

    // "Break top 3 bottom 3"
    if s.starts_with("break") {
        let number = extract_first_number(&s);
        return (number, "break");
    }

    (None, "")
}

fn extract_first_number(s: &str) -> Option<i64> {
    let digits: String = s
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn map_inning_half(s: &str) -> &'static str {
    match s.trim().to_lowercase().as_str() {
        "top" => "top",
        "bottom" => "bottom",
        "break" => "break",
        _ => "",
    }
}

fn is_completed_state(event_state: &str) -> bool {
    matches!(
        event_state.trim().to_uppercase().as_str(),
        "FINISHED" | "ENDED" | "MATCH_COMPLETED" | "CLOSED" | "FINAL" | "FT" | "COMPLETE" | "COMPLETED"
    )
}

pub(crate) fn normalize_game_state(event_state: &str) -> &'static str {
    let s = event_state.trim().to_lowercase();
    if s.is_empty() {
        return "UNKNOWN";
    }
    if matches!(
        s.as_str(),
        "closed"
            | "resolved"
            | "ended"
            | "finished"
            | "final"
            | "complete"
            | "completed"
            | "cancelled"
            | "canceled"
            | "ft"
    ) {
        return "FINAL";
    }
    if matches!(
        s.as_str(),
        "live"
            | "inplay"
            | "in_play"
            | "ongoing"
            | "in_progress"
            | "started"
            | "halftime"
            | "overtime"
    ) {
        return "LIVE";
    }
    if matches!(
        s.as_str(),
        "scheduled"
            | "upcoming"
            | "not_started"
            | "not started"
            | "pending"
            | "pre"
            | "pregame"
            | "pre_game"
            | "pre-match"
            | "pre_match"
            | "prematch"
    ) {
        return "NOT STARTED";
    }
    "UNKNOWN"
}

// ---------------------------------------------------------------------------
// freeText extraction (PyDict path only)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// PyO3 extraction helpers
// ---------------------------------------------------------------------------

pub(crate) fn get_str(obj: &Bound<'_, PyDict>, key: &str) -> String {
    if let Ok(Some(value)) = obj.get_item(key) {
        if let Ok(text) = value.extract::<String>() {
            return text;
        }
    }
    String::new()
}

pub(crate) fn get_i64_opt(obj: &Bound<'_, PyDict>, key: &str) -> Option<i64> {
    if let Ok(Some(value)) = obj.get_item(key) {
        if value.is_none() {
            return None;
        }
        if let Ok(v) = value.extract::<i64>() {
            return Some(v);
        }
        if let Ok(v) = value.extract::<f64>() {
            return Some(v as i64);
        }
        if let Ok(v) = value.extract::<String>() {
            let text = v.trim();
            if !text.is_empty() {
                if let Ok(parsed) = text.parse::<i64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

pub(crate) fn get_bool_opt(obj: &Bound<'_, PyDict>, key: &str) -> Option<bool> {
    if let Ok(Some(value)) = obj.get_item(key) {
        if value.is_none() {
            return None;
        }
        if let Ok(v) = value.extract::<bool>() {
            return Some(v);
        }
        if let Ok(v) = value.extract::<i64>() {
            return Some(v != 0);
        }
        if let Ok(v) = value.extract::<String>() {
            let t = v.trim().to_lowercase();
            if ["1", "true", "yes", "y", "on"].contains(&t.as_str()) {
                return Some(true);
            }
            if ["0", "false", "no", "n", "off"].contains(&t.as_str()) {
                return Some(false);
            }
        }
    }
    None
}

pub(crate) fn get_f64_opt(obj: &Bound<'_, PyDict>, key: &str) -> Option<f64> {
    if let Ok(Some(value)) = obj.get_item(key) {
        if value.is_none() {
            return None;
        }
        if let Ok(v) = value.extract::<f64>() {
            return Some(v);
        }
        if let Ok(v) = value.extract::<i64>() {
            return Some(v as f64);
        }
        if let Ok(v) = value.extract::<String>() {
            let text = v.trim();
            if !text.is_empty() {
                if let Ok(parsed) = text.parse::<f64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn scalar_to_i64(value: &Bound<'_, PyAny>) -> Option<i64> {
    if value.is_none() {
        return None;
    }
    if let Ok(v) = value.extract::<i64>() {
        return Some(v);
    }
    if let Ok(v) = value.extract::<f64>() {
        return Some(v as i64);
    }
    if let Ok(v) = value.extract::<String>() {
        let text = v.trim();
        if !text.is_empty() {
            if let Ok(parsed) = text.parse::<i64>() {
                return Some(parsed);
            }
        }
    }
    None
}

fn scalar_to_bool(value: &Bound<'_, PyAny>) -> Option<bool> {
    if value.is_none() {
        return None;
    }
    if let Ok(v) = value.extract::<bool>() {
        return Some(v);
    }
    if let Ok(v) = value.extract::<i64>() {
        return Some(v != 0);
    }
    if let Ok(v) = value.extract::<String>() {
        let t = v.trim().to_lowercase();
        if ["1", "true", "yes", "y", "on"].contains(&t.as_str()) {
            return Some(true);
        }
        if ["0", "false", "no", "n", "off"].contains(&t.as_str()) {
            return Some(false);
        }
    }
    None
}

fn get_attr_i64_opt(obj: &Bound<'_, PyAny>, key: &str) -> Option<i64> {
    if let Ok(value) = obj.getattr(key) {
        return scalar_to_i64(&value);
    }
    None
}

fn get_attr_bool_opt(obj: &Bound<'_, PyAny>, key: &str) -> Option<bool> {
    if let Ok(value) = obj.getattr(key) {
        return scalar_to_bool(&value);
    }
    None
}

fn get_attr_str(obj: &Bound<'_, PyAny>, key: &str) -> String {
    if let Ok(value) = obj.getattr(key) {
        if let Ok(text) = value.extract::<String>() {
            return text;
        }
    }
    String::new()
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
    fn parse_period_extra_innings_guard() {
        let (num, half) = parse_period("1st Extra inning top");
        assert_eq!(
            num, None,
            "extra innings must not be misidentified as inning 1"
        );
        assert_eq!(half, "");
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
