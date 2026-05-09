//! Fast byte-level field extractor for Kalstrop V1 frames.
//! Scans raw JSON for the 4 fields needed by the engine and returns
//! borrowed slices. Returns None for non-"next" frames or missing fields.

pub(crate) struct V1Extract<'a> {
    pub fixture_id: &'a str,
    pub home_score: &'a str,
    pub away_score: &'a str,
    pub free_text: &'a str,
}

/// Scan for a JSON key pattern like `"keyName":"` and return the byte offset
/// of the character after the opening quote of the value.
/// Handles optional whitespace after `:`.
fn find_key_value_start(bytes: &[u8], key_pattern: &[u8], from: usize) -> Option<usize> {
    let mut pos = from;
    let len = bytes.len();
    while pos + key_pattern.len() < len {
        if let Some(idx) = memchr_find(bytes, key_pattern, pos) {
            // After the key pattern (which ends with `"`), expect `:` then optional whitespace then `"`
            let mut p = idx + key_pattern.len();
            // Skip whitespace
            while p < len && (bytes[p] == b' ' || bytes[p] == b'\t' || bytes[p] == b'\n' || bytes[p] == b'\r') {
                p += 1;
            }
            // Expect `:`
            if p < len && bytes[p] == b':' {
                p += 1;
                // Skip whitespace
                while p < len && (bytes[p] == b' ' || bytes[p] == b'\t' || bytes[p] == b'\n' || bytes[p] == b'\r') {
                    p += 1;
                }
                // Expect opening `"`
                if p < len && bytes[p] == b'"' {
                    return Some(p + 1); // position after the opening quote
                }
            }
            pos = idx + 1; // try next occurrence
        } else {
            break;
        }
    }
    None
}

/// Find a byte pattern in a slice starting from `from`.
fn memchr_find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from >= haystack.len() {
        return None;
    }
    memchr::memmem::find(&haystack[from..], needle).map(|pos| pos + from)
}

/// Extract a JSON string value starting at `start` (the position after the opening `"`).
/// Returns the string content and the position after the closing `"`.
fn extract_string_value(bytes: &[u8], start: usize) -> Option<(&[u8], usize)> {
    let mut pos = start;
    while pos < bytes.len() {
        if bytes[pos] == b'\\' {
            pos += 2; // skip escaped character
            continue;
        }
        if bytes[pos] == b'"' {
            return Some((&bytes[start..pos], pos + 1));
        }
        pos += 1;
    }
    None
}

pub(crate) fn fast_extract_v1(json: &str) -> Option<V1Extract<'_>> {
    let bytes = json.as_bytes();

    // Quick reject: check for "type":"next".
    // In live V1 frames this appears in the first ~30 bytes, but we search the
    // full buffer for robustness (test data may have reordered keys).
    let is_next = memchr_find(bytes, b"\"type\"", 0)
        .and_then(|pos| {
            // Find the value after "type":
            let mut p = pos + 6; // skip "type"
            while p < bytes.len() && bytes[p] != b'"' { p += 1; }
            if p < bytes.len() {
                let start = p + 1;
                extract_string_value(bytes, start)
            } else {
                None
            }
        })
        .map(|(val, _)| val == b"next")
        .unwrap_or(false);

    if !is_next {
        return None;
    }

    // Scan for the 4 needed fields. They appear in this order in V1 frames:
    // fixtureId (~byte 130), freeText (~byte 247), homeScore (~byte 276), awayScore (~byte 294)
    // Carry forward position so each search starts where the last one ended.

    let mut fixture_id: Option<&str> = None;
    let mut free_text: Option<&str> = None;
    let mut home_score: Option<&str> = None;
    let mut away_score: Option<&str> = None;
    let mut pos = 0usize;

    // fixtureId
    if let Some(start) = find_key_value_start(bytes, b"\"fixtureId\"", pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            fixture_id = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    // freeText (inside matchStatusDisplay array — first occurrence)
    if let Some(start) = find_key_value_start(bytes, b"\"freeText\"", pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            free_text = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    // homeScore
    if let Some(start) = find_key_value_start(bytes, b"\"homeScore\"", pos) {
        if let Some((val, end)) = extract_string_value(bytes, start) {
            home_score = Some(std::str::from_utf8(val).ok()?);
            pos = end;
        }
    }

    // awayScore
    if let Some(start) = find_key_value_start(bytes, b"\"awayScore\"", pos) {
        if let Some((val, _end)) = extract_string_value(bytes, start) {
            away_score = Some(std::str::from_utf8(val).ok()?);
        }
    }

    Some(V1Extract {
        fixture_id: fixture_id?,
        home_score: home_score.unwrap_or(""),
        away_score: away_score.unwrap_or(""),
        free_text: free_text.unwrap_or(""),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_next_frame() {
        let frame = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"abc-123","matchSummary":{"matchStatusDisplay":[{"freeText":"4th inning top"}],"homeScore":"3","awayScore":"1"}}}}}"#;
        let result = fast_extract_v1(frame).unwrap();
        assert_eq!(result.fixture_id, "abc-123");
        assert_eq!(result.home_score, "3");
        assert_eq!(result.away_score, "1");
        assert_eq!(result.free_text, "4th inning top");
    }

    #[test]
    fn test_connection_ack_rejected() {
        let frame = r#"{"type":"connection_ack"}"#;
        assert!(fast_extract_v1(frame).is_none());
    }

    #[test]
    fn test_missing_fixture_id() {
        let frame = r#"{"type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"matchSummary":{"homeScore":"0","awayScore":"0"}}}}}"#;
        assert!(fast_extract_v1(frame).is_none());
    }
}
