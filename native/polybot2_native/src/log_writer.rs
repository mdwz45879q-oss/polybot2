use std::fs::File;
use std::io::{BufWriter, Write};

pub(crate) struct LogWriter {
    writer: BufWriter<File>,
}

fn opt_i64(v: Option<i64>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "null".to_string(),
    }
}

fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

impl LogWriter {
    pub fn open(path: &str) -> Result<Self, String> {
        let file = File::create(path).map_err(|e| format!("log_open_failed:{}", e))?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write_line(&mut self, line: &str) {
        let _ = writeln!(self.writer, "{}", line);
    }

    pub fn flush(&mut self) {
        let _ = self.writer.flush();
    }

    pub fn log_tick(
        &mut self,
        gid: &str,
        home: Option<i64>,
        away: Option<i64>,
        inn: Option<i64>,
        half: &str,
        gs: &str,
    ) {
        let ts = now_unix_ms();
        self.write_line(&format!(
            r#"{{"ts":{},"ev":"tick","gid":"{}","h":{},"a":{},"inn":{},"half":"{}","gs":"{}"}}"#,
            ts,
            json_escape(gid),
            opt_i64(home),
            opt_i64(away),
            opt_i64(inn),
            json_escape(half),
            json_escape(gs)
        ));
    }

    pub fn log_order_ok(&mut self, sk: &str, tok: &str, eid: &str) {
        let ts = now_unix_ms();
        self.write_line(&format!(
            r#"{{"ts":{},"ev":"order","sk":"{}","tok":"{}","ok":true,"eid":"{}"}}"#,
            ts,
            json_escape(sk),
            json_escape(tok),
            json_escape(eid)
        ));
    }

    pub fn log_order_err(&mut self, sk: &str, tok: &str, err: &str) {
        let ts = now_unix_ms();
        self.write_line(&format!(
            r#"{{"ts":{},"ev":"order","sk":"{}","tok":"{}","ok":false,"err":"{}"}}"#,
            ts,
            json_escape(sk),
            json_escape(tok),
            json_escape(err)
        ));
    }

    pub fn log_startup(&mut self, run_id: i64, games: usize, tokens: usize, mode: &str) {
        let ts = now_unix_ms();
        self.write_line(&format!(
            r#"{{"ts":{},"ev":"startup","run_id":{},"games":{},"tokens":{},"mode":"{}"}}"#,
            ts, run_id, games, tokens, json_escape(mode)
        ));
    }

    pub fn log_ws_connect(&mut self, subs: &[String]) {
        let ts = now_unix_ms();
        let subs_json: Vec<String> = subs
            .iter()
            .map(|s| format!("\"{}\"", json_escape(s)))
            .collect();
        self.write_line(&format!(
            r#"{{"ts":{},"ev":"ws_connect","subs":[{}]}}"#,
            ts,
            subs_json.join(",")
        ));
    }

    pub fn log_ws_disconnect(&mut self, reason: &str, reconnects: i64) {
        let ts = now_unix_ms();
        self.write_line(&format!(
            r#"{{"ts":{},"ev":"ws_disconnect","reason":"{}","reconnects":{}}}"#,
            ts,
            json_escape(reason),
            reconnects
        ));
        self.flush();
    }
}
