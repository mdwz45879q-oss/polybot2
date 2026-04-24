use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

#[cfg(unix)]
use std::os::unix::net::UnixDatagram;

pub(crate) const TELEMETRY_SCHEMA_VERSION: i64 = 1;
pub(crate) const TELEMETRY_EVENT_MAX_BYTES: usize = 16 * 1024;
pub(crate) const TELEMETRY_SOCKET_PATH: &str = "/tmp/polybot2_hotpath_telemetry.sock";
pub(crate) const TELEMETRY_QUEUE_CAPACITY: usize = 4096;

const HIGH_VALUE_EVENT_TYPES: [&str; 22] = [
    "score_changed",
    "game_state_changed",
    "order_submit_called",
    "order_submit_ok",
    "order_submit_failed",
    "order_cancel_called",
    "order_replace_called",
    "order_acknowledged",
    "order_resting",
    "order_partially_filled",
    "order_filled",
    "order_canceled",
    "order_rejected",
    "order_failed",
    "ws_connected",
    "ws_reconnected",
    "ws_disconnected",
    "exec_connected",
    "exec_error",
    "provider_decode_error",
    "runtime_heartbeat",
    "subscriptions_changed",
];

#[derive(Default)]
pub(crate) struct TelemetryStats {
    emitted: AtomicU64,
    dropped: AtomicU64,
}

impl TelemetryStats {
    fn inc_emitted(&self) {
        self.emitted.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn emitted(&self) -> u64 {
        self.emitted.load(Ordering::Relaxed)
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

enum TelemetryMessage {
    Event(Vec<u8>),
    Stop,
}

#[derive(Clone)]
pub(crate) struct TelemetryEmitter {
    sender: SyncSender<TelemetryMessage>,
    stats: Arc<TelemetryStats>,
    seq: Arc<AtomicU64>,
    provider: String,
    league: String,
}

impl TelemetryEmitter {
    #[inline]
    fn is_high_value_event(event_type: &str) -> bool {
        let event = event_type.trim();
        HIGH_VALUE_EVENT_TYPES.iter().any(|x| *x == event)
    }

    pub(crate) fn emit(
        &self,
        event_type: &str,
        game_id: &str,
        chain_id: &str,
        strategy_key: &str,
        order_client_id: &str,
        order_exchange_id: &str,
        reason_code: &str,
        payload: Value,
    ) {
        if !Self::is_high_value_event(event_type) {
            return;
        }

        let seq = self.seq.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let envelope = json!({
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "seq": seq as i64,
            "ts_unix_ns": crate::dispatch::now_unix_ns(),
            "event_type": event_type,
            "provider": self.provider,
            "league": self.league,
            "game_id": game_id,
            "chain_id": chain_id,
            "strategy_key": strategy_key,
            "order_ref": {
                "client_order_id": order_client_id,
                "exchange_order_id": order_exchange_id,
            },
            "reason_code": reason_code,
            "payload": payload,
        });
        let bytes = match serde_json::to_vec(&envelope) {
            Ok(v) => v,
            Err(_) => {
                self.stats.inc_dropped();
                return;
            }
        };
        if bytes.len() > TELEMETRY_EVENT_MAX_BYTES {
            self.stats.inc_dropped();
            return;
        }
        match self.sender.try_send(TelemetryMessage::Event(bytes)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                if self.stats.dropped() < 3 {
                    eprintln!("[polybot2_native] telemetry channel full");
                }
                self.stats.inc_dropped();
            }
            Err(TrySendError::Disconnected(_)) => {
                if self.stats.dropped() < 3 {
                    eprintln!("[polybot2_native] telemetry channel disconnected");
                }
                self.stats.inc_dropped();
            }
        }
    }

    #[inline]
    pub(crate) fn emit_empty(
        &self,
        event_type: &str,
        game_id: &str,
        chain_id: &str,
        strategy_key: &str,
        order_client_id: &str,
        order_exchange_id: &str,
        reason_code: &str,
    ) {
        self.emit(
            event_type,
            game_id,
            chain_id,
            strategy_key,
            order_client_id,
            order_exchange_id,
            reason_code,
            Value::Null,
        );
    }
}

pub(crate) struct TelemetryWorkerHandle {
    control: SyncSender<TelemetryMessage>,
    join: Option<JoinHandle<()>>,
    stats: Arc<TelemetryStats>,
}

impl TelemetryWorkerHandle {
    pub(crate) fn shutdown(&mut self) {
        // Use blocking send to guarantee worker termination even if queue is full.
        let _ = self.control.send(TelemetryMessage::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    pub(crate) fn emitted(&self) -> u64 {
        self.stats.emitted()
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.stats.dropped()
    }
}

fn run_worker(rx: Receiver<TelemetryMessage>, socket_path: String, stats: Arc<TelemetryStats>) {
    #[cfg(unix)]
    let socket = match UnixDatagram::unbound() {
        Ok(s) => {
            let _ = s.set_nonblocking(true);
            Some(s)
        }
        Err(_) => None,
    };
    #[cfg(not(unix))]
    let socket: Option<()> = None;

    let mut waited_for_socket = false;

    loop {
        match rx.recv() {
            Ok(TelemetryMessage::Stop) => break,
            Ok(TelemetryMessage::Event(bytes)) => {
                #[cfg(unix)]
                {
                    if !waited_for_socket {
                        for _ in 0..20 {
                            if std::path::Path::new(socket_path.as_str()).exists() {
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(50));
                        }
                        waited_for_socket = true;
                    }
                    if let Some(sock) = socket.as_ref() {
                        match sock.send_to(bytes.as_slice(), socket_path.as_str()) {
                            Ok(_) => stats.inc_emitted(),
                            Err(e) => {
                                if stats.dropped() < 5 {
                                    eprintln!(
                                        "[polybot2] telemetry drop: {} len={} path={} exists={}",
                                        e,
                                        bytes.len(),
                                        socket_path,
                                        std::path::Path::new(socket_path.as_str()).exists(),
                                    );
                                }
                                stats.inc_dropped();
                            }
                        }
                    } else {
                        eprintln!("[polybot2] telemetry drop: no socket created");
                        stats.inc_dropped();
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = bytes;
                    let _ = socket_path;
                    stats.inc_dropped();
                }
            }
            Err(_) => break,
        }
    }
}

pub(crate) fn build_telemetry(
    provider: &str,
    league: &str,
) -> (Option<TelemetryEmitter>, Option<TelemetryWorkerHandle>) {
    let cap = TELEMETRY_QUEUE_CAPACITY.max(16);
    let (tx, rx) = sync_channel::<TelemetryMessage>(cap);
    let stats = Arc::new(TelemetryStats::default());
    let stats_worker = Arc::clone(&stats);
    let socket_owned = TELEMETRY_SOCKET_PATH.to_string();
    let join = thread::spawn(move || run_worker(rx, socket_owned, stats_worker));

    let emitter = TelemetryEmitter {
        sender: tx.clone(),
        stats: Arc::clone(&stats),
        seq: Arc::new(AtomicU64::new(0)),
        provider: provider.to_string(),
        league: league.to_string(),
    };
    let worker = TelemetryWorkerHandle {
        control: tx,
        join: Some(join),
        stats,
    };
    (Some(emitter), Some(worker))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_high_value_events_are_emitted() {
        assert!(TelemetryEmitter::is_high_value_event("score_changed"));
        assert!(TelemetryEmitter::is_high_value_event("exec_error"));
        assert!(!TelemetryEmitter::is_high_value_event("random_noise"));
    }

    #[test]
    fn emitter_drops_when_queue_is_full_without_blocking() {
        let (tx, _rx) = sync_channel::<TelemetryMessage>(1);
        let stats = Arc::new(TelemetryStats::default());
        let emitter = TelemetryEmitter {
            sender: tx,
            stats: Arc::clone(&stats),
            seq: Arc::new(AtomicU64::new(0)),
            provider: "kalstrop".to_string(),
            league: "mlb".to_string(),
        };

        emitter.emit(
            "score_changed",
            "g1",
            "g1:1",
            "",
            "",
            "",
            "",
            json!({"new_home_score": 1, "new_away_score": 0}),
        );
        emitter.emit(
            "order_submit_called",
            "g1",
            "g1:1",
            "s1",
            "",
            "",
            "test_reason",
            json!({"decision": "trade"}),
        );

        assert_eq!(stats.emitted(), 0);
        assert_eq!(stats.dropped(), 1);
    }
}
