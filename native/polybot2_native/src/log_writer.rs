use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::{BufWriter, Write};

pub(crate) enum TickPayload<'a> {
    Baseball {
        lg: &'a str,
        runs_home: i64,
        runs_away: i64,
        inn: Option<i64>,
        inn_half: &'a str,
        gs: &'a str,
        src: &'static str,
        outs: Option<u8>,
        strikes: Option<u8>,
        base1: Option<bool>,
        base2: Option<bool>,
        base3: Option<bool>,
        period_raw: &'a str,
    },
    Soccer {
        lg: &'a str,
        goals_home: i64,
        goals_away: i64,
        half: &'a str,
        corners_home: Option<i64>,
        corners_away: Option<i64>,
        gs: &'a str,
        src: &'static str,
        var_action_type: &'a str,
        var_action_subtype: &'a str,
    },
    Tennis {
        lg: &'a str,
        sets_home: i64,
        sets_away: i64,
        games_home: i64,
        games_away: i64,
        total_games: i64,
        half: &'a str,
        gs: &'a str,
    },
    Cs2 {
        lg: &'a str,
        maps_home: i64,
        maps_away: i64,
        rounds_home: i64,
        rounds_away: i64,
        current_map: i64,
        gs: &'a str,
        src: &'static str,
    },
    Moba {
        lg: &'a str,
        maps_home: i64,
        maps_away: i64,
        gs: &'a str,
        src: &'static str,
    },
}

pub(crate) struct LogWriter {
    writer: BufWriter<File>,
    buf: String,
    sport: &'static str,
}

fn write_opt_i64(buf: &mut String, v: Option<i64>) {
    match v {
        Some(n) => {
            let _ = write!(buf, "{}", n);
        }
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

/// Extract the game ID prefix from a strategy key (everything before first ':').
pub(crate) fn gid_from_sk(sk: &str) -> &str {
    sk.split_once(':').map_or(sk, |(g, _)| g)
}

impl LogWriter {
    pub fn open(path: &str, sport: &'static str) -> Result<Self, String> {
        let file = File::create(path).map_err(|e| format!("log_open_failed:{}", e))?;
        Ok(Self {
            writer: BufWriter::new(file),
            buf: String::with_capacity(256),
            sport,
        })
    }

    fn flush_buf(&mut self) {
        let _ = writeln!(self.writer, "{}", self.buf);
        self.buf.clear();
    }

    pub fn flush(&mut self) {
        let _ = self.writer.flush();
    }

    pub fn log_tick(&mut self, gid: &str, payload: &TickPayload<'_>) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"tick","sport":"{}","lg":""#,
            now_unix_ms(),
            self.sport,
        );
        match payload {
            TickPayload::Baseball { lg, .. } => write_json_escape(&mut self.buf, lg),
            TickPayload::Soccer { lg, .. } => write_json_escape(&mut self.buf, lg),
            TickPayload::Tennis { lg, .. } => write_json_escape(&mut self.buf, lg),
            TickPayload::Cs2 { lg, .. } => write_json_escape(&mut self.buf, lg),
            TickPayload::Moba { lg, .. } => write_json_escape(&mut self.buf, lg),
        }
        self.buf.push_str(r#"","gid":""#);
        write_json_escape(&mut self.buf, gid);
        self.buf.push('"');

        match payload {
            TickPayload::Baseball {
                runs_home,
                runs_away,
                inn,
                inn_half,
                gs,
                src,
                outs,
                strikes,
                base1,
                base2,
                base3,
                period_raw,
                ..
            } => {
                self.buf.push_str(r#","runs_home":"#);
                let _ = write!(self.buf, "{}", runs_home);
                self.buf.push_str(r#","runs_away":"#);
                let _ = write!(self.buf, "{}", runs_away);
                self.buf.push_str(r#","inn":"#);
                write_opt_i64(&mut self.buf, *inn);
                self.buf.push_str(r#","inn_half":""#);
                write_json_escape(&mut self.buf, inn_half);
                self.buf.push('"');
                self.buf.push_str(r#","gs":""#);
                write_json_escape(&mut self.buf, gs);
                self.buf.push('"');
                if !src.is_empty() {
                    self.buf.push_str(r#","src":""#);
                    self.buf.push_str(src);
                    self.buf.push('"');
                }
                if let Some(o) = outs {
                    self.buf.push_str(r#","outs":"#);
                    self.buf.push_str(itoa::Buffer::new().format(*o));
                }
                if let Some(s) = strikes {
                    self.buf.push_str(r#","strikes":"#);
                    self.buf.push_str(itoa::Buffer::new().format(*s));
                }
                if let Some(b) = base1 {
                    self.buf.push_str(if *b { r#","base1":true"# } else { r#","base1":false"# });
                }
                if let Some(b) = base2 {
                    self.buf.push_str(if *b { r#","base2":true"# } else { r#","base2":false"# });
                }
                if let Some(b) = base3 {
                    self.buf.push_str(if *b { r#","base3":true"# } else { r#","base3":false"# });
                }
                if !period_raw.is_empty() {
                    self.buf.push_str(r#","period_raw":""#);
                    write_json_escape(&mut self.buf, period_raw);
                    self.buf.push('"');
                }
            }
            TickPayload::Soccer {
                goals_home,
                goals_away,
                half,
                corners_home,
                corners_away,
                gs,
                src,
                var_action_type,
                var_action_subtype,
                ..
            } => {
                self.buf.push_str(r#","goals_home":"#);
                let _ = write!(self.buf, "{}", goals_home);
                self.buf.push_str(r#","goals_away":"#);
                let _ = write!(self.buf, "{}", goals_away);
                self.buf.push_str(r#","half":""#);
                write_json_escape(&mut self.buf, half);
                self.buf.push('"');
                if corners_home.is_some() || corners_away.is_some() {
                    self.buf.push_str(r#","corners_home":"#);
                    write_opt_i64(&mut self.buf, *corners_home);
                    self.buf.push_str(r#","corners_away":"#);
                    write_opt_i64(&mut self.buf, *corners_away);
                }
                self.buf.push_str(r#","gs":""#);
                write_json_escape(&mut self.buf, gs);
                self.buf.push('"');
                if !src.is_empty() {
                    self.buf.push_str(r#","src":""#);
                    self.buf.push_str(src);
                    self.buf.push('"');
                }
                if !var_action_type.is_empty() {
                    self.buf.push_str(r#","var_type":""#);
                    write_json_escape(&mut self.buf, var_action_type);
                    self.buf.push_str(r#"","var_sub":""#);
                    write_json_escape(&mut self.buf, var_action_subtype);
                    self.buf.push('"');
                }
            }
            TickPayload::Tennis {
                sets_home,
                sets_away,
                games_home,
                games_away,
                total_games,
                half,
                gs,
                ..
            } => {
                self.buf.push_str(r#","sets_home":"#);
                let _ = write!(self.buf, "{}", sets_home);
                self.buf.push_str(r#","sets_away":"#);
                let _ = write!(self.buf, "{}", sets_away);
                self.buf.push_str(r#","games_home":"#);
                let _ = write!(self.buf, "{}", games_home);
                self.buf.push_str(r#","games_away":"#);
                let _ = write!(self.buf, "{}", games_away);
                self.buf.push_str(r#","total_games":"#);
                let _ = write!(self.buf, "{}", total_games);
                self.buf.push_str(r#","half":""#);
                write_json_escape(&mut self.buf, half);
                self.buf.push('"');
                self.buf.push_str(r#","gs":""#);
                write_json_escape(&mut self.buf, gs);
                self.buf.push('"');
            }
            TickPayload::Cs2 {
                maps_home,
                maps_away,
                rounds_home,
                rounds_away,
                current_map,
                gs,
                src,
                ..
            } => {
                self.buf.push_str(r#","maps_home":"#);
                let _ = write!(self.buf, "{}", maps_home);
                self.buf.push_str(r#","maps_away":"#);
                let _ = write!(self.buf, "{}", maps_away);
                self.buf.push_str(r#","rounds_home":"#);
                let _ = write!(self.buf, "{}", rounds_home);
                self.buf.push_str(r#","rounds_away":"#);
                let _ = write!(self.buf, "{}", rounds_away);
                self.buf.push_str(r#","current_map":"#);
                let _ = write!(self.buf, "{}", current_map);
                self.buf.push_str(r#","gs":""#);
                write_json_escape(&mut self.buf, gs);
                self.buf.push('"');
                self.buf.push_str(r#","src":""#);
                write_json_escape(&mut self.buf, src);
                self.buf.push('"');
            }
            TickPayload::Moba {
                maps_home,
                maps_away,
                gs,
                src,
                ..
            } => {
                self.buf.push_str(r#","maps_home":"#);
                let _ = write!(self.buf, "{}", maps_home);
                self.buf.push_str(r#","maps_away":"#);
                let _ = write!(self.buf, "{}", maps_away);
                self.buf.push_str(r#","gs":""#);
                write_json_escape(&mut self.buf, gs);
                self.buf.push('"');
                self.buf.push_str(r#","src":""#);
                write_json_escape(&mut self.buf, src);
                self.buf.push('"');
            }
        }
        self.buf.push('}');
        self.flush_buf();
    }

    pub fn log_order_ok(&mut self, gid: &str, sk: &str, tok: &str, eid: &str, tif: &str) {
        self.buf.clear();
        let _ = write!(self.buf, r#"{{"ts":{},"ev":"order","gid":""#, now_unix_ms());
        write_json_escape(&mut self.buf, gid);
        self.buf.push_str(r#"","sk":""#);
        write_json_escape(&mut self.buf, sk);
        self.buf.push_str(r#"","tok":""#);
        write_json_escape(&mut self.buf, tok);
        self.buf.push_str(r#"","ok":true,"eid":""#);
        write_json_escape(&mut self.buf, eid);
        self.buf.push_str(r#"","tif":""#);
        self.buf.push_str(tif);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_order_err(&mut self, gid: &str, sk: &str, tok: &str, err: &str, tif: &str) {
        self.buf.clear();
        let _ = write!(self.buf, r#"{{"ts":{},"ev":"order","gid":""#, now_unix_ms());
        write_json_escape(&mut self.buf, gid);
        self.buf.push_str(r#"","sk":""#);
        write_json_escape(&mut self.buf, sk);
        self.buf.push_str(r#"","tok":""#);
        write_json_escape(&mut self.buf, tok);
        self.buf.push_str(r#"","ok":false,"err":""#);
        write_json_escape(&mut self.buf, err);
        self.buf.push_str(r#"","tif":""#);
        self.buf.push_str(tif);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_patch(&mut self, new_tokens: usize, new_targets: usize) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"patch","new_tokens":{},"new_targets":{}}}"#,
            now_unix_ms(),
            new_tokens,
            new_targets
        );
        self.flush_buf();
    }

    pub fn log_startup(
        &mut self,
        run_id: i64,
        games: usize,
        tokens: usize,
        mode: &str,
        leagues: &[&str],
    ) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"startup","v":2,"sport":"{}","leagues":["#,
            now_unix_ms(),
            self.sport,
        );
        for (i, lg) in leagues.iter().enumerate() {
            if i > 0 {
                self.buf.push(',');
            }
            self.buf.push('"');
            write_json_escape(&mut self.buf, lg);
            self.buf.push('"');
        }
        let _ = write!(
            self.buf,
            r#"],"run_id":{},"games":{},"tokens":{},"mode":""#,
            run_id, games, tokens,
        );
        write_json_escape(&mut self.buf, mode);
        self.buf.push_str(r#""}"#);
        self.flush_buf();
    }

    pub fn log_ws_connect(&mut self, src: &str, subs: &[String]) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"ws_connect","src":""#,
            now_unix_ms()
        );
        write_json_escape(&mut self.buf, src);
        self.buf.push_str(r#"","subs":["#);
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

    pub fn log_ws_disconnect(&mut self, src: &str, reason: &str, reconnects: i64) {
        self.buf.clear();
        let _ = write!(
            self.buf,
            r#"{{"ts":{},"ev":"ws_disconnect","src":""#,
            now_unix_ms()
        );
        write_json_escape(&mut self.buf, src);
        self.buf.push_str(r#"","reason":""#);
        write_json_escape(&mut self.buf, reason);
        let _ = write!(self.buf, r#"","reconnects":{}}}"#, reconnects);
        self.flush_buf();
        self.flush();
    }
}
