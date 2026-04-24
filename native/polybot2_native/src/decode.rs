use serde_json::Value;

pub(crate) fn parse_json_bytes(bytes: &[u8]) -> Result<Value, String> {
    let mut simd_buf = bytes.to_vec();
    if let Ok(v) = simd_json::serde::from_slice::<Value>(&mut simd_buf) {
        return Ok(v);
    }
    serde_json::from_slice::<Value>(bytes).map_err(|e| format!("json_decode_error: {}", e))
}

pub(crate) fn parse_json_text(text: &str) -> Result<Value, String> {
    parse_json_bytes(text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_bytes_valid() {
        let out = parse_json_bytes(br#"{"a":1,"b":"x"}"#).expect("json should parse");
        assert_eq!(out.get("a").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(out.get("b").and_then(|v| v.as_str()), Some("x"));
    }

    #[test]
    fn parse_json_bytes_invalid() {
        let out = parse_json_bytes(br#"{"a":1"#);
        assert!(out.is_err());
    }
}
