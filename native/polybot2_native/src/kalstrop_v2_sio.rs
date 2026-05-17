//! Minimal Socket.IO client for Kalstrop V2 (Engine.IO v4 + Socket.IO).
//! Implements just enough of the protocol to connect, subscribe to fixtures,
//! receive genius_update events, and handle ping/pong keepalive.
//! No external Socket.IO crate — uses raw tokio-tungstenite.

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

pub(crate) type WsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

pub(crate) struct SioConnection {
    pub ws: WsStream,
    #[allow(dead_code)]
    pub ping_interval: Duration,
}

#[derive(Debug)]
pub(crate) enum SioFrame<'a> {
    GeniusUpdate(&'a str),
    Subscribed(&'a str),
    Ping,
    ConnectAck,
    Other,
}

pub(crate) async fn connect(
    base_url: &str,
    sio_path: &str,
    client_id: &str,
    shared_secret_raw: &str,
) -> Result<SioConnection, String> {
    let mut ws_url = format!(
        "{}{}/?EIO=4&transport=websocket",
        base_url.trim_end_matches('/'),
        sio_path,
    )
    .replace("https://", "wss://")
    .replace("http://", "ws://");

    if !client_id.is_empty() && !shared_secret_raw.is_empty() {
        let ts = crate::dispatch::now_unix_s().to_string();
        let sig = crate::ws::kalstrop_signature(client_id, shared_secret_raw, &ts);
        ws_url = format!(
            "{}&product=genius-stats&X-Client-ID={}&X-Timestamp={}&Authorization=Bearer+{}",
            ws_url,
            urlencoding::encode(client_id),
            urlencoding::encode(&ts),
            urlencoding::encode(&sig),
        );
    }

    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        &ws_url, None, true, None,
    )
        .await
        .map_err(|e| format!("v2_sio_connect:{}", e))?;

    // Read Engine.IO OPEN packet: 0{"sid":"...","pingInterval":25000,...}
    let open_msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "v2_sio_open_timeout".to_string())?
        .ok_or_else(|| "v2_sio_open_closed".to_string())?
        .map_err(|e| format!("v2_sio_open_read:{}", e))?;

    let open_text = match &open_msg {
        Message::Text(t) => t.as_str(),
        _ => return Err("v2_sio_open_not_text".to_string()),
    };
    if !open_text.starts_with('0') {
        return Err(format!("v2_sio_open_bad_prefix:{}", &open_text[..open_text.len().min(60)]));
    }
    let ping_interval = parse_ping_interval(&open_text[1..]);

    // Send Socket.IO CONNECT
    ws.send(Message::Text("40".to_string().into()))
        .await
        .map_err(|e| format!("v2_sio_connect_send:{}", e))?;

    // Read Socket.IO CONNECT ACK: 40{"sid":"..."}
    let ack_msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "v2_sio_connect_ack_timeout".to_string())?
        .ok_or_else(|| "v2_sio_connect_ack_closed".to_string())?
        .map_err(|e| format!("v2_sio_connect_ack_read:{}", e))?;

    if let Message::Text(t) = &ack_msg {
        if !t.starts_with("40") {
            return Err(format!("v2_sio_connect_ack_bad:{}", &t[..t.len().min(60)]));
        }
    }

    Ok(SioConnection { ws, ping_interval })
}

pub(crate) async fn subscribe(
    conn: &mut SioConnection,
    fixture_id: &str,
    competition_id: &str,
    sport_id: &str,
) -> Result<(), String> {
    let params = serde_json::json!({
        "fixtureId": fixture_id,
        "activeContent": "court",
        "sport": "Football",
        "sportId": sport_id,
        "competitionId": competition_id,
    });
    let payload = format!("42[\"genius_subscribe\",{}]", params);
    conn.ws
        .send(Message::Text(payload.into()))
        .await
        .map_err(|e| format!("v2_sio_subscribe_send:{}", e))
}

#[allow(dead_code)]
pub(crate) async fn unsubscribe(
    conn: &mut SioConnection,
    fixture_id: &str,
) -> Result<(), String> {
    let params = serde_json::json!({
        "fixtureId": fixture_id,
        "activeContent": "court",
    });
    let payload = format!("42[\"genius_unsubscribe\",{}]", params);
    conn.ws
        .send(Message::Text(payload.into()))
        .await
        .map_err(|e| format!("v2_sio_unsubscribe_send:{}", e))
}

pub(crate) async fn send_pong(conn: &mut SioConnection) -> Result<(), String> {
    conn.ws
        .send(Message::Text("3".to_string().into()))
        .await
        .map_err(|e| format!("v2_sio_pong_send:{}", e))
}

pub(crate) fn classify_frame(text: &str) -> SioFrame<'_> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return SioFrame::Other;
    }

    // Engine.IO PING
    if bytes.len() == 1 && bytes[0] == b'2' {
        return SioFrame::Ping;
    }

    // Socket.IO CONNECT ACK: 40{...}
    if bytes.len() >= 2 && bytes[0] == b'4' && bytes[1] == b'0' {
        return SioFrame::ConnectAck;
    }

    // Socket.IO EVENT: 42["event_name", payload]
    if bytes.len() >= 4 && bytes[0] == b'4' && bytes[1] == b'2' && bytes[2] == b'[' && bytes[3] == b'"' {
        let (event_name, payload) = match parse_sio_event(&text[2..]) {
            Some(v) => v,
            None => return SioFrame::Other,
        };
        return match event_name {
            "genius_update" => SioFrame::GeniusUpdate(payload),
            "subscribed" => SioFrame::Subscribed(payload),
            _ => SioFrame::Other,
        };
    }

    SioFrame::Other
}

fn parse_sio_event(arr_text: &str) -> Option<(&str, &str)> {
    // arr_text starts with: ["event_name",{...payload...}]
    let bytes = arr_text.as_bytes();
    if bytes.len() < 5 || bytes[0] != b'[' || bytes[1] != b'"' {
        return None;
    }
    // Find end of event name
    let name_end = memchr::memchr(b'"', &bytes[2..])? + 2;
    let event_name = &arr_text[2..name_end];

    // Skip ","  to find payload start
    let mut p = name_end + 1;
    while p < bytes.len() && bytes[p] != b',' {
        p += 1;
    }
    if p >= bytes.len() {
        return None;
    }
    p += 1; // skip comma

    // Payload is everything from here to the last ]
    if bytes[bytes.len() - 1] != b']' {
        return None;
    }
    let payload = &arr_text[p..bytes.len() - 1];
    Some((event_name, payload))
}

fn parse_ping_interval(open_json: &str) -> Duration {
    if let Ok(v) = serde_json::from_str::<Value>(open_json) {
        if let Some(ms) = v.get("pingInterval").and_then(|v| v.as_u64()) {
            return Duration::from_millis(ms);
        }
    }
    Duration::from_secs(25)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_ping() {
        assert!(matches!(classify_frame("2"), SioFrame::Ping));
    }

    #[test]
    fn test_classify_connect_ack() {
        assert!(matches!(
            classify_frame(r#"40{"sid":"abc123"}"#),
            SioFrame::ConnectAck
        ));
    }

    #[test]
    fn test_classify_genius_update() {
        let frame = r#"42["genius_update",{"data":{"betGeniusFixtureId":"123","scoreboardInfo":{"homeScore":1,"awayScore":0,"currentPhase":"FirstHalf"}}}]"#;
        match classify_frame(frame) {
            SioFrame::GeniusUpdate(payload) => {
                assert!(payload.contains("betGeniusFixtureId"));
                assert!(payload.contains("homeScore"));
            }
            other => panic!("expected GeniusUpdate, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_subscribed() {
        let frame = r#"42["subscribed",{"status":"success","room":"event_123_court"}]"#;
        match classify_frame(frame) {
            SioFrame::Subscribed(payload) => {
                assert!(payload.contains("success"));
            }
            other => panic!("expected Subscribed, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_other() {
        assert!(matches!(classify_frame(""), SioFrame::Other));
        assert!(matches!(classify_frame("3"), SioFrame::Other));
        assert!(matches!(classify_frame("41"), SioFrame::Other));
    }

    #[test]
    fn test_parse_sio_event_extracts_name_and_payload() {
        let arr = r#"["genius_update",{"key":"value"}]"#;
        let (name, payload) = parse_sio_event(arr).unwrap();
        assert_eq!(name, "genius_update");
        assert_eq!(payload, r#"{"key":"value"}"#);
    }

    #[test]
    fn test_parse_ping_interval() {
        let interval = parse_ping_interval(r#"{"sid":"abc","pingInterval":25000,"pingTimeout":20000}"#);
        assert_eq!(interval, Duration::from_millis(25000));
    }

    #[test]
    fn test_parse_ping_interval_default() {
        let interval = parse_ping_interval("{}");
        assert_eq!(interval, Duration::from_secs(25));
    }
}
