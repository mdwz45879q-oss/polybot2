use super::*;

pub(crate) fn parse_tick_any(event: &Bound<'_, PyAny>, recv_monotonic_ns: i64) -> Tick {
    if let Ok(as_dict) = event.downcast::<PyDict>() {
        return parse_tick_from_dict(as_dict, recv_monotonic_ns);
    }
    parse_tick_from_event(event, recv_monotonic_ns)
}

pub(crate) fn parse_tick_from_dict(event: &Bound<'_, PyDict>, recv_monotonic_ns: i64) -> Tick {
    Tick {
        universal_id: get_str(event, "universal_id"),
        action: get_str(event, "action"),
        recv_monotonic_ns: if recv_monotonic_ns > 0 {
            recv_monotonic_ns
        } else {
            get_i64_opt(event, "recv_monotonic_ns").unwrap_or(0)
        },
        goals_home: get_i64_opt(event, "goals_home"),
        goals_away: get_i64_opt(event, "goals_away"),
        inning_number: get_i64_opt(event, "inning_number"),
        inning_half: get_str(event, "inning_half"),
        outs: get_i64_opt(event, "outs"),
        balls: get_i64_opt(event, "balls"),
        strikes: get_i64_opt(event, "strikes"),
        runner_on_first: get_bool_opt(event, "runner_on_first"),
        runner_on_second: get_bool_opt(event, "runner_on_second"),
        runner_on_third: get_bool_opt(event, "runner_on_third"),
        match_completed: get_bool_opt(event, "match_completed"),
        period: get_str(event, "period"),
        game_state: get_str(event, "game_state"),
    }
}

fn parse_tick_from_event(event: &Bound<'_, PyAny>, recv_monotonic_ns: i64) -> Tick {
    let mut baseball: Option<Bound<'_, PyDict>> = None;
    if let Ok(raw_payload) = event.getattr("raw_payload") {
        if let Ok(raw_dict) = raw_payload.downcast::<PyDict>() {
            if let Ok(Some(baseball_obj)) = raw_dict.get_item("_hotpath_baseball") {
                if let Ok(baseball_dict) = baseball_obj.downcast::<PyDict>() {
                    baseball = Some(baseball_dict.clone());
                }
            }
        }
    }

    let inning_number = baseball
        .as_ref()
        .and_then(|b| get_i64_opt(b, "inning_number"));
    let inning_half = baseball
        .as_ref()
        .map(|b| get_str(b, "inning_half"))
        .unwrap_or_else(String::new);
    let outs = baseball.as_ref().and_then(|b| get_i64_opt(b, "outs"));
    let balls = baseball.as_ref().and_then(|b| get_i64_opt(b, "balls"));
    let strikes = baseball.as_ref().and_then(|b| get_i64_opt(b, "strikes"));
    let runner_on_first = baseball
        .as_ref()
        .and_then(|b| get_bool_opt(b, "runner_on_first"));
    let runner_on_second = baseball
        .as_ref()
        .and_then(|b| get_bool_opt(b, "runner_on_second"));
    let runner_on_third = baseball
        .as_ref()
        .and_then(|b| get_bool_opt(b, "runner_on_third"));
    let match_completed = baseball
        .as_ref()
        .and_then(|b| get_bool_opt(b, "match_completed"))
        .or_else(|| get_attr_bool_opt(event, "match_completed"));

    Tick {
        universal_id: get_attr_str(event, "universal_id"),
        action: get_attr_str(event, "action"),
        recv_monotonic_ns,
        goals_home: get_attr_i64_opt(event, "home_score"),
        goals_away: get_attr_i64_opt(event, "away_score"),
        inning_number,
        inning_half,
        outs,
        balls,
        strikes,
        runner_on_first,
        runner_on_second,
        runner_on_third,
        match_completed,
        period: get_attr_str(event, "period"),
        game_state: get_attr_str(event, "game_state"),
    }
}

pub(crate) fn parse_tick_from_kalstrop_row(
    row: &Bound<'_, PyDict>,
    recv_monotonic_ns: i64,
    _source_recv_monotonic_ns: i64,
) -> Tick {
    let uid = ["fixtureId", "fixture_id", "id", "universal_id", "uid"]
        .iter()
        .find_map(|k| {
            let v = get_str(row, k);
            if v.trim().is_empty() {
                None
            } else {
                Some(v)
            }
        })
        .unwrap_or_default();

    let match_summary = get_dict(row, "matchSummary");
    let event_state = match_summary
        .as_ref()
        .map(|s| get_str(s, "eventState"))
        .unwrap_or_default();
    let period_text = match_summary
        .as_ref()
        .map(|s| extract_period_text(s, &event_state))
        .unwrap_or_else(|| event_state.clone());
    let match_completed = if event_state.trim().is_empty() {
        None
    } else {
        Some(is_completed_state(&event_state))
    };

    let (
        inning_number,
        inning_half,
        outs,
        balls,
        strikes,
        runner_on_first,
        runner_on_second,
        runner_on_third,
        resolved_match_completed,
    ) = if let Some(summary) = match_summary.as_ref() {
        parse_kalstrop_baseball(row, summary, &period_text, match_completed)
    } else {
        (
            None,
            String::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            match_completed,
        )
    };

    let goals_home = match_summary
        .as_ref()
        .and_then(|s| get_i64_opt(s, "homeScore"));
    let goals_away = match_summary
        .as_ref()
        .and_then(|s| get_i64_opt(s, "awayScore"));

    Tick {
        universal_id: uid,
        action: "sportsMatchStateUpdatedV2".to_string(),
        recv_monotonic_ns,
        goals_home,
        goals_away,
        inning_number,
        inning_half,
        outs,
        balls,
        strikes,
        runner_on_first,
        runner_on_second,
        runner_on_third,
        match_completed: resolved_match_completed,
        period: period_text,
        game_state: normalize_game_state(event_state.as_str()),
    }
}

pub(crate) fn parse_tick_from_kalstrop_row_value(
    row: &Value,
    recv_monotonic_ns: i64,
    _source_recv_monotonic_ns: i64,
) -> Tick {
    let mut uid = String::new();
    for key in ["fixtureId", "fixture_id", "id", "universal_id", "uid"] {
        let candidate = get_value_str(row, key);
        if !candidate.trim().is_empty() {
            uid = candidate;
            break;
        }
    }

    let match_summary = row.get("matchSummary").unwrap_or(&Value::Null);
    let event_state = get_value_str(match_summary, "eventState");
    let period_text = if match_summary.is_object() {
        extract_period_text_value(match_summary, &event_state)
    } else {
        event_state.clone()
    };
    let match_completed = if event_state.trim().is_empty() {
        None
    } else {
        Some(is_completed_state(&event_state))
    };
    let (
        inning_number,
        inning_half,
        outs,
        balls,
        strikes,
        runner_on_first,
        runner_on_second,
        runner_on_third,
        resolved_match_completed,
    ) = if match_summary.is_object() {
        parse_kalstrop_baseball_value(row, match_summary, &period_text, match_completed)
    } else {
        (
            None,
            String::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            match_completed,
        )
    };

    Tick {
        universal_id: uid,
        action: "sportsMatchStateUpdatedV2".to_string(),
        recv_monotonic_ns,
        goals_home: get_value_i64_opt(match_summary, "homeScore"),
        goals_away: get_value_i64_opt(match_summary, "awayScore"),
        inning_number,
        inning_half,
        outs,
        balls,
        strikes,
        runner_on_first,
        runner_on_second,
        runner_on_third,
        match_completed: resolved_match_completed,
        period: period_text,
        game_state: normalize_game_state(event_state.as_str()),
    }
}

pub(crate) fn iter_payload_items_value<'a>(parsed: &'a Value) -> Vec<&'a Value> {
    if let Some(items) = parsed.as_array() {
        return items.iter().collect();
    }
    vec![parsed]
}

fn parse_inning_text(text: &str) -> (Option<i64>, String) {
    let src = text.trim().to_lowercase();
    if src.is_empty() {
        return (None, String::new());
    }
    let mut digits = String::new();
    let mut seen_digit = false;
    for ch in src.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            seen_digit = true;
        } else if seen_digit {
            break;
        }
    }
    let inning_number = if digits.is_empty() {
        None
    } else {
        digits.parse::<i64>().ok()
    };
    let inning_half = if src.contains("top") {
        "top".to_string()
    } else if src.contains("bottom") || src.contains("bot") {
        "bottom".to_string()
    } else if src.contains("end") {
        "end".to_string()
    } else {
        String::new()
    };
    (inning_number, inning_half)
}

fn is_completed_state(event_state: &str) -> bool {
    matches!(
        event_state.trim().to_uppercase().as_str(),
        "FINISHED" | "ENDED" | "MATCH_COMPLETED" | "CLOSED" | "FINAL" | "FT"
    )
}

pub(crate) fn normalize_game_state(event_state: &str) -> String {
    let s = event_state.trim().to_lowercase();
    if s.is_empty() {
        return "UNKNOWN".to_string();
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
        return "FINAL".to_string();
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
        return "LIVE".to_string();
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
        return "NOT STARTED".to_string();
    }
    "UNKNOWN".to_string()
}

fn extract_period_text(match_summary: &Bound<'_, PyDict>, event_state: &str) -> String {
    if let Some(display) = get_list(match_summary, "matchStatusDisplay") {
        for item in display.iter() {
            if let Ok(row) = item.downcast::<PyDict>() {
                let free_text = get_str(&row, "freeText");
                if !free_text.trim().is_empty() {
                    return free_text;
                }
            }
        }
    }
    event_state.to_string()
}

fn extract_period_text_value(match_summary: &Value, event_state: &str) -> String {
    if let Some(display) = match_summary
        .get("matchStatusDisplay")
        .and_then(|x| x.as_array())
    {
        for item in display.iter() {
            if let Some(free_text) = item.get("freeText").and_then(|x| x.as_str()) {
                if !free_text.trim().is_empty() {
                    return free_text.to_string();
                }
            }
        }
    }
    event_state.to_string()
}

fn parse_kalstrop_baseball(
    row: &Bound<'_, PyDict>,
    match_summary: &Bound<'_, PyDict>,
    period_text: &str,
    match_completed: Option<bool>,
) -> (
    Option<i64>,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
) {
    if let Some(baseball) = get_dict(row, "_hotpath_baseball") {
        let inning_number = get_i64_opt(&baseball, "inning_number");
        let inning_half = get_str(&baseball, "inning_half");
        let outs = get_i64_opt(&baseball, "outs");
        let balls = get_i64_opt(&baseball, "balls");
        let strikes = get_i64_opt(&baseball, "strikes");
        let runner_on_first = get_bool_opt(&baseball, "runner_on_first");
        let runner_on_second = get_bool_opt(&baseball, "runner_on_second");
        let runner_on_third = get_bool_opt(&baseball, "runner_on_third");
        let baseball_completed = get_bool_opt(&baseball, "match_completed").or(match_completed);
        return (
            inning_number,
            inning_half,
            outs,
            balls,
            strikes,
            runner_on_first,
            runner_on_second,
            runner_on_third,
            baseball_completed,
        );
    }

    let mut inning_number: Option<i64> = None;
    let mut inning_half = String::new();
    let mut texts: Vec<String> = vec![period_text.to_string()];
    if let Some(display) = get_list(match_summary, "matchStatusDisplay") {
        for item in display.iter() {
            if let Ok(row) = item.downcast::<PyDict>() {
                texts.push(get_str(&row, "freeText"));
            }
        }
    }
    if let Some(phases) = get_list(match_summary, "phases") {
        for item in phases.iter() {
            if let Ok(row) = item.downcast::<PyDict>() {
                texts.push(get_str(&row, "phaseText"));
            }
        }
    }
    for text in texts.iter() {
        let (num, half) = parse_inning_text(text);
        if inning_number.is_none() && num.is_some() {
            inning_number = num;
        }
        if inning_half.is_empty() && !half.is_empty() {
            inning_half = half;
        }
        if inning_number.is_some() && !inning_half.is_empty() {
            break;
        }
    }
    (
        inning_number,
        inning_half,
        None,
        None,
        None,
        None,
        None,
        None,
        match_completed,
    )
}

fn parse_kalstrop_baseball_value(
    row: &Value,
    match_summary: &Value,
    period_text: &str,
    match_completed: Option<bool>,
) -> (
    Option<i64>,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
) {
    if let Some(baseball) = row.get("_hotpath_baseball") {
        if baseball.is_object() {
            let inning_number = get_value_i64_opt(baseball, "inning_number");
            let inning_half = get_value_str(baseball, "inning_half");
            let outs = get_value_i64_opt(baseball, "outs");
            let balls = get_value_i64_opt(baseball, "balls");
            let strikes = get_value_i64_opt(baseball, "strikes");
            let runner_on_first = get_value_bool_opt(baseball, "runner_on_first");
            let runner_on_second = get_value_bool_opt(baseball, "runner_on_second");
            let runner_on_third = get_value_bool_opt(baseball, "runner_on_third");
            let baseball_completed =
                get_value_bool_opt(baseball, "match_completed").or(match_completed);
            return (
                inning_number,
                inning_half,
                outs,
                balls,
                strikes,
                runner_on_first,
                runner_on_second,
                runner_on_third,
                baseball_completed,
            );
        }
    }

    let mut inning_number: Option<i64> = None;
    let mut inning_half = String::new();
    let mut texts: Vec<String> = vec![period_text.to_string()];
    if let Some(display) = match_summary
        .get("matchStatusDisplay")
        .and_then(|x| x.as_array())
    {
        for item in display.iter() {
            if let Some(text) = item.get("freeText").and_then(|x| x.as_str()) {
                texts.push(text.to_string());
            }
        }
    }
    if let Some(phases) = match_summary.get("phases").and_then(|x| x.as_array()) {
        for item in phases.iter() {
            if let Some(text) = item.get("phaseText").and_then(|x| x.as_str()) {
                texts.push(text.to_string());
            }
        }
    }
    for text in texts.iter() {
        let (num, half) = parse_inning_text(text);
        if inning_number.is_none() && num.is_some() {
            inning_number = num;
        }
        if inning_half.is_empty() && !half.is_empty() {
            inning_half = half;
        }
        if inning_number.is_some() && !inning_half.is_empty() {
            break;
        }
    }
    (
        inning_number,
        inning_half,
        None,
        None,
        None,
        None,
        None,
        None,
        match_completed,
    )
}

// --- PyO3 extraction helpers ---

pub(crate) fn get_str(obj: &Bound<'_, PyDict>, key: &str) -> String {
    if let Ok(Some(value)) = obj.get_item(key) {
        if let Ok(text) = value.extract::<String>() {
            return text;
        }
    }
    String::new()
}

fn get_dict<'py>(obj: &Bound<'py, PyDict>, key: &str) -> Option<Bound<'py, PyDict>> {
    if let Ok(Some(value)) = obj.get_item(key) {
        if let Ok(dict) = value.downcast::<PyDict>() {
            return Some(dict.clone());
        }
    }
    None
}

fn get_list<'py>(obj: &Bound<'py, PyDict>, key: &str) -> Option<Bound<'py, PyList>> {
    if let Ok(Some(value)) = obj.get_item(key) {
        if let Ok(list) = value.downcast::<PyList>() {
            return Some(list.clone());
        }
    }
    None
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

// --- serde_json Value helpers ---

pub(crate) fn get_value_str(obj: &Value, key: &str) -> String {
    obj.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

pub(crate) fn get_value_i64_opt(obj: &Value, key: &str) -> Option<i64> {
    let val = obj.get(key)?;
    if val.is_null() {
        return None;
    }
    if let Some(v) = val.as_i64() {
        return Some(v);
    }
    if let Some(v) = val.as_u64() {
        return i64::try_from(v).ok();
    }
    if let Some(v) = val.as_f64() {
        return Some(v as i64);
    }
    if let Some(v) = val.as_str() {
        let text = v.trim();
        if text.is_empty() {
            return None;
        }
        if let Ok(parsed) = text.parse::<i64>() {
            return Some(parsed);
        }
    }
    None
}

fn get_value_bool_opt(obj: &Value, key: &str) -> Option<bool> {
    let val = obj.get(key)?;
    if val.is_null() {
        return None;
    }
    if let Some(v) = val.as_bool() {
        return Some(v);
    }
    if let Some(v) = val.as_i64() {
        return Some(v != 0);
    }
    if let Some(v) = val.as_str() {
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
