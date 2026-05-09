use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::{BufWriter, Write};

pub(crate) struct LogWriter {
    writer: BufWriter<File>,
    buf: String,
}

fn write_opt_i64(buf: &mut String, v: Option<i64>) {
    match v {
        Some(n) => { let _ = write!(buf, "{}", n); }
        None => buf.push_str("null"),
    }
}

fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

fn write_json_escape(buf: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if c.is_control() => {}
            c => buf.push(c),
        }
    }
}

impl LogWriter {
    pub fn open(path: &str) -> Result<Self, String> {
        let file = File::create(path).map_err(|e| format!("log_open_failed:{}", e))?;
        Ok(Self {
            writer: BufWriter::new(file),
            buf: String::with_capacity(256),
        })
    }

    fn flush_buf(&mut self) {
        let _ = writeln!(self.writer, "{}", self.buf);
        self.buf.clear();
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
        self.buf.clear();
        let _ = write!(self.buf, r#"{{"ts":{},"ev":"tick","gid":""#, now_unix_ms());
        write_json_escape(&mut self.buf, gid);
        self.buf.push_str(r#"","h":"#);
        write_opt_i64(&mut self.buf, home);
        self.buf.push_str(r#","a":"#);
        write_opt_i64(&mut self.buf, away);
        self.buf.push_str(r#","inn":"#);
        write_opt_i64(&mut self.buf, inn);
        self.buf.push_str(r#","half":""#);
        write_json_escape(&mut self.buf, half);
        self.buf.push_str(r#"","gs":""#);
        write_json_escape(&mut self.buf, gs);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_order_ok(&mut self, sk: &str, tok: &str, eid: &str) {
        self.buf.clear();
        let _ = write!(self.buf, r#"{{"ts":{},"ev":"order","sk":""#, now_unix_ms());
        write_json_escape(&mut self.buf, sk);
        self.buf.push_str(r#"","tok":""#);
        write_json_escape(&mut self.buf, tok);
        self.buf.push_str(r#"","ok":true,"eid":""#);
        write_json_escape(&mut self.buf, eid);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_order_err(&mut self, sk: &str, tok: &str, err: &str) {
        self.buf.clear();
        let _ = write!(self.buf, r#"{{"ts":{},"ev":"order","sk":""#, now_unix_ms());
        write_json_escape(&mut self.buf, sk);
        self.buf.push_str(r#"","tok":""#);
        write_json_escape(&mut self.buf, tok);
        self.buf.push_str(r#"","ok":false,"err":""#);
        write_json_escape(&mut self.buf, err);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_patch(&mut self, new_tokens: usize, new_targets: usize) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"patch","new_tokens":{},"new_targets":{}}}"#,
            now_unix_ms(), new_tokens, new_targets
        );
        self.flush_buf();
    }

    pub fn log_startup(&mut self, run_id: i64, games: usize, tokens: usize, mode: &str) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"startup","run_id":{},"games":{},"tokens":{},"mode":""#,
            now_unix_ms(), run_id, games, tokens
        );
        write_json_escape(&mut self.buf, mode);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_ws_connect(&mut self, subs: &[String]) {
        self.buf.clear();
        let _ = write!(self.buf, r#"{{"ts":{},"ev":"ws_connect","subs":["#, now_unix_ms());
        for (i, s) in subs.iter().enumerate() {
            if i > 0 {
                self.buf.push(',');
            }
            self.buf.push('"');
            write_json_escape(&mut self.buf, s);
            self.buf.push('"');
        }
        self.buf.push_str("]}");
        self.flush_buf();
    }

    pub fn log_ws_disconnect(&mut self, reason: &str, reconnects: i64) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"ws_disconnect","reason":""#,
            now_unix_ms()
        );
        write_json_escape(&mut self.buf, reason);
        let _ = write!(self.buf, r#"","reconnects":{}}}"#, reconnects);
        self.flush_buf();
        self.flush();
    }
}
