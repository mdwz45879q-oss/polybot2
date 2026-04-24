"""Simple live monitor for hotpath telemetry (strictly off hot path)."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
import json
import os
import select
import shutil
import socket
import sqlite3
import sys
import threading
import time
from typing import Any, TextIO


DEFAULT_TELEMETRY_SOCKET_PATH = "/tmp/polybot2_hotpath_telemetry.sock"
DEFAULT_REFRESH_SECONDS = 5.0
MIN_REFRESH_SECONDS = 5.0
DEFAULT_MAX_GAMES = 40
DEFAULT_TAIL_LOG_LINES = 40


_STATE_LIVE = {"LIVE", "INPLAY", "IN_PLAY", "IN PROGRESS", "IN_PROGRESS", "STARTED", "ONGOING"}
_STATE_FINAL = {"FINAL", "ENDED", "FINISHED", "CLOSED", "RESOLVED", "COMPLETE", "COMPLETED", "FT"}
_STATE_UPCOMING = {
    "NOT STARTED",
    "NOT_STARTED",
    "UPCOMING",
    "SCHEDULED",
    "PENDING",
    "PREMATCH",
    "PRE-MATCH",
    "PRE_MATCH",
    "PREGAME",
    "PRE_GAME",
}

_DEFAULT_LOG_EVENT_TYPES = {
    "score_changed",
    "game_state_changed",
    "order_submit_called",
    "order_submit_ok",
    "order_acknowledged",
    "order_resting",
    "order_partially_filled",
    "order_filled",
    "order_canceled",
    "order_rejected",
    "order_failed",
    "order_submit_failed",
    "ws_connected",
    "ws_reconnected",
    "ws_disconnected",
    "exec_connected",
    "exec_error",
    "provider_decode_error",
    "subscriptions_changed",
}


@dataclass(slots=True)
class ObserveEvent:
    ts_unix_ns: int
    level: str
    event_type: str
    game_id: str
    chain_id: str
    strategy_key: str
    reason_code: str
    order_client_id: str
    order_exchange_id: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ObserveEvent":
        order_ref = payload.get("order_ref") if isinstance(payload.get("order_ref"), dict) else {}
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        return cls(
            ts_unix_ns=int(payload.get("ts_unix_ns") or 0),
            level=str(payload.get("level") or "info"),
            event_type=str(payload.get("event_type") or ""),
            game_id=str(payload.get("game_id") or ""),
            chain_id=str(payload.get("chain_id") or ""),
            strategy_key=str(payload.get("strategy_key") or ""),
            reason_code=str(payload.get("reason_code") or ""),
            order_client_id=str(order_ref.get("client_order_id") or ""),
            order_exchange_id=str(order_ref.get("exchange_order_id") or ""),
            payload=inner,
        )


@dataclass(frozen=True, slots=True)
class MonitorConfig:
    socket_path: str = DEFAULT_TELEMETRY_SOCKET_PATH
    refresh_seconds: float = DEFAULT_REFRESH_SECONDS
    max_games: int = DEFAULT_MAX_GAMES
    no_color: bool = False
    max_log_lines: int = 400
    tail_log_lines: int = DEFAULT_TAIL_LOG_LINES


def effective_refresh_seconds(value: float | int | str | None) -> float:
    try:
        parsed = float(value if value is not None else DEFAULT_REFRESH_SECONDS)
    except Exception:
        parsed = DEFAULT_REFRESH_SECONDS
    return float(max(MIN_REFRESH_SECONDS, parsed))


def _decode_datagram(raw: bytes) -> ObserveEvent | None:
    try:
        data = json.loads(raw.decode("utf-8", errors="ignore"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return ObserveEvent.from_payload(data)


def _fmt_time(ts_ns: int) -> str:
    if ts_ns <= 0:
        return "--:--:--.---"
    dt = datetime.fromtimestamp(float(ts_ns) / 1_000_000_000.0)
    return dt.strftime("%H:%M:%S.%f")[:-3]


def _now_wall_ns() -> int:
    return int(time.time() * 1_000_000_000)


def _state_bucket(state: str) -> str:
    s = str(state or "").strip().upper()
    if s in _STATE_LIVE:
        return "live"
    if s in _STATE_FINAL:
        return "final"
    if s in _STATE_UPCOMING:
        return "upcoming"
    return "unknown"


def _state_icon_and_label(state: str) -> tuple[str, str]:
    bucket = _state_bucket(state)
    if bucket == "live":
        return "🔴", "LIVE"
    if bucket == "final":
        return "🏁", "FINAL"
    if bucket == "upcoming":
        return "⚪", "UPCOMING"
    return "❔", "UNKNOWN"


def _coerce_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _short_id(gid: str) -> str:
    g = str(gid or "").strip()
    if len(g) <= 12:
        return g or "UNKNOWN"
    return g[:8]


def _team_short(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    upper = raw.upper()
    if upper.isalpha() and 2 <= len(upper) <= 4:
        return upper
    words = [w for w in "".join((c if c.isalpha() or c.isspace() else " ") for c in raw).split() if w]
    if not words:
        letters = "".join(ch for ch in raw.upper() if ch.isalpha())
        return letters[:3] if letters else ""
    if len(words) == 1:
        return words[0][:3].upper()
    first = words[0]
    second = words[1]
    if len(first) <= 2:
        return (first + second)[:3].upper()
    return first[:3].upper()


def build_matchup_label(home_team: str, away_team: str) -> str:
    home = _team_short(home_team)
    away = _team_short(away_team)
    if home and away:
        return f"{home}-{away}"
    if home:
        return home
    if away:
        return away
    return ""


def _ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _format_inning(half: str, number: str) -> str:
    h = str(half or "").strip().lower()
    try:
        n = int(number)
    except (ValueError, TypeError):
        return ""
    label = {"top": "Top", "bottom": "Bot", "end": "End"}.get(h, h.capitalize() if h else "")
    if not label:
        return _ordinal(n)
    return f"{label} {_ordinal(n)}"


def _extract_line_from_reason(reason: str) -> str:
    parts = str(reason or "").split(":")
    if len(parts) >= 2:
        candidate = parts[-1].strip()
        try:
            float(candidate)
            return candidate
        except ValueError:
            pass
    return ""


def _is_meaningful_game_state_change(event: ObserveEvent) -> bool:
    old_state = str(event.payload.get("old_game_state") or "")
    new_state = str(event.payload.get("new_game_state") or "")
    return _state_bucket(old_state) != _state_bucket(new_state)


def _should_log_event(event: ObserveEvent) -> bool:
    et = str(event.event_type or "").strip()
    if et not in _DEFAULT_LOG_EVENT_TYPES:
        return False
    if et == "game_state_changed":
        return _is_meaningful_game_state_change(event)
    return True


class _GameNameResolver:
    def __init__(self, *, preferred: dict[str, str] | None = None) -> None:
        self._preferred = {str(k): str(v) for k, v in (preferred or {}).items() if str(k).strip() and str(v).strip()}
        self._cache: dict[str, str] = {}
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        db_path = str(os.getenv("POLYBOT2_DB_PATH", "") or "").strip()
        if not db_path or not os.path.exists(db_path):
            return
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT provider_game_id, home_raw, away_raw
                FROM provider_games
                WHERE TRIM(COALESCE(provider_game_id, '')) <> ''
                """
            ).fetchall()
            conn.close()
        except Exception:
            return
        for row in rows:
            gid = str(row["provider_game_id"] or "").strip()
            if not gid:
                continue
            matchup = build_matchup_label(str(row["home_raw"] or ""), str(row["away_raw"] or ""))
            if matchup:
                self._cache[gid] = matchup

    def resolve(self, game_id: str) -> str:
        gid = str(game_id or "").strip()
        if not gid:
            return "UNKNOWN"
        preferred = self._preferred.get(gid)
        if preferred:
            return preferred
        self._load()
        return self._cache.get(gid, _short_id(gid))


@dataclass(slots=True)
class GameRow:
    game_id: str
    matchup: str
    state: str = "UNKNOWN"
    home_score: int | None = None
    away_score: int | None = None
    period: str = ""
    last_update_ns: int = 0
    last_order_ns: int = 0
    orders_submitted: int = 0
    orders_filled: int = 0
    orders_partial: int = 0
    orders_resting: int = 0
    orders_canceled: int = 0
    orders_rejected: int = 0
    orders_failed: int = 0


class ObserveStore:
    def __init__(self, *, max_log_lines: int = 400, matchup_by_game_id: dict[str, str] | None = None) -> None:
        self.max_log_lines = int(max(50, max_log_lines))
        self.logs: deque[str] = deque(maxlen=self.max_log_lines)
        self.games: dict[str, GameRow] = {}
        self.event_counts: Counter[str] = Counter()
        self.ws_state = "DOWN"
        self.exec_state = "DOWN"
        self.decode_errors = 0
        self.error_events = 0
        self._resolver = _GameNameResolver(preferred=matchup_by_game_id)

    def _get_game(self, game_id: str) -> GameRow:
        gid = str(game_id or "").strip()
        row = self.games.get(gid)
        if row is None:
            row = GameRow(game_id=gid, matchup=self._resolver.resolve(gid))
            self.games[gid] = row
        return row

    def ingest(self, event: ObserveEvent, *, with_color: bool = True) -> str | None:
        self.event_counts[event.event_type] += 1
        et = event.event_type
        lvl = str(event.level or "").strip().lower()

        if et == "runtime_heartbeat":
            self.ws_state = "CONNECTED"
            pl = event.payload if isinstance(event.payload, dict) else {}
            teams_data = pl.get("teams") if isinstance(pl.get("teams"), dict) else {}
            for gid, pair in teams_data.items():
                if isinstance(pair, list) and len(pair) >= 2:
                    label = build_matchup_label(str(pair[0] or ""), str(pair[1] or ""))
                    if label:
                        self._resolver._preferred[str(gid)] = label
            games_data = pl.get("games") if isinstance(pl.get("games"), dict) else {}
            for gid, gs in games_data.items():
                if not isinstance(gs, dict) or not str(gid or "").strip():
                    continue
                game = self._get_game(str(gid))
                hs = _coerce_int(gs.get("h"))
                aw = _coerce_int(gs.get("a"))
                if hs is not None:
                    game.home_score = hs
                if aw is not None:
                    game.away_score = aw
                state = str(gs.get("s") or "").strip()
                if state:
                    game.state = state
                inn = str(gs.get("inn") or "").strip()
                half = str(gs.get("half") or "").strip()
                if inn:
                    game.period = _format_inning(half, inn)
                game.last_update_ns = max(game.last_update_ns, int(event.ts_unix_ns or 0))
            dm = str(pl.get("dm") or "").strip()
            if dm:
                self.exec_state = f"CONNECTED ({dm})"
            return None

        if et in {"ws_connected", "ws_reconnected"}:
            self.ws_state = "CONNECTED"
        elif et == "ws_disconnected":
            self.ws_state = "DOWN"

        if et == "exec_connected":
            self.exec_state = "CONNECTED"
        elif et in {"exec_error", "order_submit_failed", "order_failed"}:
            self.exec_state = "DEGRADED"

        if et in {"provider_decode_error"}:
            self.decode_errors += 1
        if lvl in {"error"} or et.endswith("_failed") or et.endswith("_error"):
            self.error_events += 1

        if event.game_id:
            game = self._get_game(event.game_id)
            game.last_update_ns = max(game.last_update_ns, int(event.ts_unix_ns or 0))
            if et == "game_state_changed":
                game.state = str(event.payload.get("new_game_state") or game.state or "UNKNOWN")
            elif et == "score_changed":
                hs = _coerce_int(event.payload.get("new_home_score"))
                aw = _coerce_int(event.payload.get("new_away_score"))
                if hs is not None:
                    game.home_score = hs
                if aw is not None:
                    game.away_score = aw
                period = str(event.payload.get("period") or "").strip()
                if period:
                    game.period = period
                gstate = str(event.payload.get("game_state") or "").strip()
                if gstate:
                    game.state = gstate
            elif et in {
                "order_submit_called",
                "order_submit_ok",
                "order_acknowledged",
                "order_resting",
                "order_partially_filled",
                "order_filled",
                "order_canceled",
                "order_rejected",
                "order_failed",
                "order_submit_failed",
            }:
                game.last_order_ns = max(game.last_order_ns, int(event.ts_unix_ns or 0))
                if et == "order_submit_called":
                    game.orders_submitted += 1
                elif et == "order_filled":
                    game.orders_filled += 1
                elif et == "order_partially_filled":
                    game.orders_partial += 1
                elif et == "order_resting":
                    game.orders_resting += 1
                elif et == "order_canceled":
                    game.orders_canceled += 1
                elif et == "order_rejected":
                    game.orders_rejected += 1
                elif et in {"order_failed", "order_submit_failed"}:
                    game.orders_failed += 1

        if not _should_log_event(event):
            return None
        line = self.format_log_line(event, with_color=with_color)
        if not line:
            return None
        self.logs.append(line)
        return line

    def format_log_line(self, event: ObserveEvent, *, with_color: bool = True) -> str:
        et = str(event.event_type or "")
        game = self._resolver.resolve(event.game_id) if event.game_id else "SYSTEM"
        ts = _fmt_time(int(event.ts_unix_ns or 0))

        tag = "ℹ️ EVENT"
        msg = str(event.reason_code or "").strip()

        if et == "score_changed":
            tag = "🎯 SCORE"
            hs = event.payload.get("new_home_score")
            aw = event.payload.get("new_away_score")
            period = str(event.payload.get("period") or "").strip()
            score_txt = f"{hs}-{aw}" if hs is not None and aw is not None else "score updated"
            msg = score_txt
            if period:
                msg = f"{msg} | {period}"
        elif et == "game_state_changed":
            tag = "📍 STATE"
            old = str(event.payload.get("old_game_state") or "UNKNOWN")
            new = str(event.payload.get("new_game_state") or "UNKNOWN")
            msg = f"{old} -> {new}"
        elif et == "order_submit_called":
            tag = "📝 BET"
            mkt = str(event.payload.get("market_type") or "").lower()
            sem = str(event.payload.get("outcome_semantic") or "").upper()
            price = event.payload.get("limit_price")
            notional = event.payload.get("amount_usdc")
            tif = str(event.payload.get("time_in_force") or "")
            line_val = _extract_line_from_reason(str(event.reason_code or ""))
            if mkt == "totals" and line_val:
                label = f"O/U {line_val} {sem}"
            elif mkt == "spread" and line_val:
                label = f"SPREAD {line_val} {sem}"
            elif mkt:
                label = f"{mkt.upper()} {sem}"
            else:
                label = str(event.payload.get("side") or "").upper()
            msg = f"{label} @ {price} x {notional}"
            if tif:
                msg = f"{msg} | {tif}"
        elif et in {"order_submit_ok", "order_acknowledged"}:
            tag = "📦 ACK"
            status = str(event.payload.get("status") or "ok")
            req = event.payload.get("requested_amount_usdc")
            fill = event.payload.get("filled_amount_usdc")
            msg = f"status={status} | filled={fill}/{req}"
        elif et == "order_filled":
            tag = "✅ FILL"
            req = event.payload.get("requested_amount_usdc")
            fill = event.payload.get("filled_amount_usdc")
            msg = f"filled={fill}/{req}"
        elif et == "order_partially_filled":
            tag = "🟨 PARTIAL"
            req = event.payload.get("requested_amount_usdc")
            fill = event.payload.get("filled_amount_usdc")
            msg = f"filled={fill}/{req}"
        elif et == "order_resting":
            tag = "⏳ RESTING"
            msg = str(event.payload.get("status") or "resting")
        elif et == "order_canceled":
            tag = "🚫 CANCEL"
            msg = str(event.reason_code or event.payload.get("status") or "canceled")
        elif et == "order_rejected":
            tag = "❌ REJECT"
            msg = str(event.reason_code or event.payload.get("error_code") or "rejected")
        elif et in {"order_failed", "order_submit_failed", "exec_error", "provider_decode_error"}:
            tag = "⚠️ ERROR"
            msg = str(event.reason_code or "error")
        elif et in {"ws_connected", "ws_reconnected"}:
            tag = "🔌 WS"
            subs = event.payload.get("subscriptions") if isinstance(event.payload, dict) else []
            count = len(subs) if isinstance(subs, list) else 0
            msg = f"{et} (subscriptions={count})"
        elif et == "ws_disconnected":
            tag = "🔌 WS"
            msg = et
        elif et in {"exec_connected", "exec_error"}:
            tag = "🔌 EXEC"
            msg = et
        elif et == "subscriptions_changed":
            tag = "📡 SUBS"
            count = event.payload.get("active_count") if isinstance(event.payload, dict) else 0
            msg = f"active={count}"

        tag_txt = tag
        if with_color:
            if tag in {"✅ FILL", "📦 ACK", "🔌 WS", "🔌 EXEC"} and "disconnected" not in msg:
                tag_txt = f"\033[32m{tag}\033[0m"
            elif tag in {"⚠️ ERROR", "❌ REJECT", "🚫 CANCEL"}:
                tag_txt = f"\033[31m{tag}\033[0m"
            elif tag in {"⏳ RESTING", "🟨 PARTIAL"}:
                tag_txt = f"\033[33m{tag}\033[0m"
            elif tag in {"🎯 SCORE", "📍 STATE", "📝 BET"}:
                tag_txt = f"\033[36m{tag}\033[0m"

        return f"{ts} | {tag_txt:<12} | {game:<10} | {msg}".rstrip()

    def _header_line(self) -> str:
        games = list(self.games.values())
        live = sum(1 for g in games if _state_bucket(g.state) == "live")
        final = sum(1 for g in games if _state_bucket(g.state) == "final")
        upcoming = sum(1 for g in games if _state_bucket(g.state) == "upcoming")
        unknown = sum(1 for g in games if _state_bucket(g.state) == "unknown")
        return (
            f"WS:{self.ws_state}  EXEC:{self.exec_state}  tracked={len(games)} "
            f"live={live} final={final} upcoming={upcoming} unknown={unknown} "
            f"submits={int(self.event_counts.get('order_submit_called', 0))} "
            f"filled={int(self.event_counts.get('order_filled', 0))} "
            f"partial={int(self.event_counts.get('order_partially_filled', 0))} "
            f"resting={int(self.event_counts.get('order_resting', 0))} "
            f"canceled={int(self.event_counts.get('order_canceled', 0))} "
            f"rejected={int(self.event_counts.get('order_rejected', 0))} "
            f"failed={int(self.event_counts.get('order_failed', 0) + self.event_counts.get('order_submit_failed', 0))} "
            f"decode_err={self.decode_errors}"
        )

    def render_scoreboard(self, *, max_games: int = DEFAULT_MAX_GAMES, with_color: bool = True) -> str:
        width = max(80, int((shutil.get_terminal_size(fallback=(120, 40))).columns))
        sep = "=" * width
        now_txt = datetime.now().strftime("%H:%M:%S")
        lines: list[str] = [sep, f"🎯 SCOREBOARD | {now_txt}", self._header_line(), sep]

        def game_sort_key(g: GameRow) -> tuple[int, int]:
            bucket = _state_bucket(g.state)
            rank = {"live": 0, "upcoming": 1, "unknown": 2, "final": 3}.get(bucket, 4)
            return (rank, -int(g.last_update_ns or 0))

        now_ns = _now_wall_ns()
        rows = sorted(self.games.values(), key=game_sort_key)
        for g in rows[: int(max(1, max_games))]:
            icon, state_label = _state_icon_and_label(g.state)
            score = "-"
            if g.home_score is not None and g.away_score is not None:
                score = f"{int(g.home_score)} - {int(g.away_score)}"
            period = str(g.period or "").strip()
            counts = (
                f"orders:{g.orders_submitted} "
                f"F:{g.orders_filled} P:{g.orders_partial} R:{g.orders_resting} "
                f"C:{g.orders_canceled} X:{g.orders_rejected} E:{g.orders_failed}"
            )
            row = (
                f"{icon} {g.matchup:<10} {score:<9} {state_label:<8} "
                f"{period[:18]:<18} | {counts}"
            )
            if with_color:
                if state_label == "LIVE":
                    row = row.replace("LIVE", "\033[31mLIVE\033[0m", 1)
                elif state_label == "FINAL":
                    row = row.replace("FINAL", "\033[90mFINAL\033[0m", 1)
                elif state_label == "UPCOMING":
                    row = row.replace("UPCOMING", "\033[37mUPCOMING\033[0m", 1)
                else:
                    row = row.replace("UNKNOWN", "\033[33mUNKNOWN\033[0m", 1)
            lines.append(row)

        lines.append(sep)
        lines.append("EXECUTION LOG")
        lines.append(sep)
        return "\n".join(lines)

    def recent_log_lines(self, *, n: int = DEFAULT_TAIL_LOG_LINES) -> list[str]:
        keep = max(1, int(n))
        return list(self.logs)[-keep:]


class HotpathInlineMonitor:
    def __init__(
        self,
        *,
        logger: Any,
        config: MonitorConfig,
        matchup_by_game_id: dict[str, str] | None = None,
        output: TextIO | None = None,
    ) -> None:
        self._logger = logger
        self._config = config
        self._refresh_seconds = effective_refresh_seconds(config.refresh_seconds)
        self._store = ObserveStore(max_log_lines=int(config.max_log_lines), matchup_by_game_id=matchup_by_game_id)
        self._output = output if output is not None else sys.stdout
        self._with_color = (not bool(config.no_color)) and bool(getattr(self._output, "isatty", lambda: False)())
        self._can_clear = bool(getattr(self._output, "isatty", lambda: False)())
        self._sock: socket.socket | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = False

    @property
    def socket_path(self) -> str:
        return str(self._config.socket_path)

    def is_running(self) -> bool:
        return bool(self._running and self._thread is not None and self._thread.is_alive())

    def start(self) -> None:
        self._bind_socket()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_thread, name="polybot2-hotpath-monitor", daemon=True)
        self._running = True
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._teardown_socket()
        self._running = False

    def run_foreground(self) -> int:
        self._bind_socket()
        self._running = True
        try:
            self._event_loop()
            return 0
        except KeyboardInterrupt:
            return 0
        finally:
            self._teardown_socket()
            self._running = False

    def _bind_socket(self) -> None:
        path = str(self._config.socket_path or "").strip() or DEFAULT_TELEMETRY_SOCKET_PATH
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if os.path.exists(path):
            os.unlink(path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.bind(path)
        try:
            os.chmod(path, 0o666)
        except Exception:
            pass
        self._sock = sock

    def _teardown_socket(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        path = str(self._config.socket_path or "").strip() or DEFAULT_TELEMETRY_SOCKET_PATH
        if os.path.exists(path):
            try:
                os.unlink(path)
            except Exception:
                pass

    def _run_thread(self) -> None:
        try:
            self._event_loop()
        except Exception as exc:
            try:
                self._logger.warning("hotpath monitor disabled after runtime error: %s: %s", type(exc).__name__, exc)
            except Exception:
                pass
        finally:
            self._running = False

    def _print_refresh(self) -> None:
        text = self._store.render_scoreboard(max_games=int(self._config.max_games), with_color=self._with_color)
        if self._can_clear:
            self._output.write("\033[2J\033[H")
        self._output.write(text)
        lines = self._store.recent_log_lines(n=int(self._config.tail_log_lines))
        if lines:
            self._output.write("\n" + "\n".join(lines))
        self._output.write("\n")
        self._output.flush()

    def _event_loop(self) -> None:
        if self._sock is None:
            raise RuntimeError("monitor socket is not initialized")

        self._logger.info(
            "hotpath monitor listening on %s (refresh=%.1fs)",
            str(self._config.socket_path),
            float(self._refresh_seconds),
        )
        next_refresh = time.monotonic() + float(self._refresh_seconds)

        while not self._stop.is_set():
            now = time.monotonic()
            timeout_s = max(0.0, min(0.25, next_refresh - now))
            ready, _, _ = select.select([self._sock], [], [], timeout_s)
            if ready:
                while True:
                    try:
                        payload = self._sock.recv(65535)
                    except BlockingIOError:
                        break
                    except Exception:
                        break
                    event = _decode_datagram(payload)
                    if event is None:
                        continue
                    line = self._store.ingest(event, with_color=self._with_color)
                    if line:
                        self._output.write(line + "\n")
                        self._output.flush()
            now = time.monotonic()
            if now >= next_refresh:
                self._print_refresh()
                next_refresh = now + float(self._refresh_seconds)


def run_hotpath_observer(args: Any, *, logger: Any) -> int:
    socket_path = str(getattr(args, "socket_path", "") or "").strip() or DEFAULT_TELEMETRY_SOCKET_PATH
    raw_refresh = getattr(args, "refresh_seconds", DEFAULT_REFRESH_SECONDS)
    refresh_seconds = effective_refresh_seconds(raw_refresh)
    try:
        requested_refresh = float(raw_refresh)
    except Exception:
        requested_refresh = float(refresh_seconds)
    if float(refresh_seconds) > float(requested_refresh):
        logger.warning("observe refresh_seconds clamped to %.1f (minimum %.1f)", refresh_seconds, MIN_REFRESH_SECONDS)
    max_games = int(getattr(args, "max_games", DEFAULT_MAX_GAMES) or DEFAULT_MAX_GAMES)
    no_color = bool(getattr(args, "no_color", False))

    monitor = HotpathInlineMonitor(
        logger=logger,
        config=MonitorConfig(
            socket_path=socket_path,
            refresh_seconds=refresh_seconds,
            max_games=max_games,
            no_color=no_color,
        ),
    )
    return int(monitor.run_foreground())


__all__ = [
    "DEFAULT_TELEMETRY_SOCKET_PATH",
    "DEFAULT_REFRESH_SECONDS",
    "MIN_REFRESH_SECONDS",
    "MonitorConfig",
    "ObserveEvent",
    "ObserveStore",
    "HotpathInlineMonitor",
    "build_matchup_label",
    "effective_refresh_seconds",
    "run_hotpath_observer",
    "_decode_datagram",
]
