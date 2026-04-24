from __future__ import annotations

import io
import logging
import os
import time

import polybot2.hotpath.observe as observe_mod
from polybot2.hotpath.observe import (
    HotpathInlineMonitor,
    MonitorConfig,
    ObserveEvent,
    ObserveStore,
    _decode_datagram,
    effective_refresh_seconds,
)


def _event(
    *,
    ts: int,
    level: str,
    event_type: str,
    game_id: str = "",
    chain_id: str = "",
    reason: str = "",
    payload: dict | None = None,
) -> ObserveEvent:
    return ObserveEvent(
        ts_unix_ns=ts,
        level=level,
        event_type=event_type,
        game_id=game_id,
        chain_id=chain_id,
        strategy_key="",
        reason_code=reason,
        order_client_id="",
        order_exchange_id="",
        payload=payload or {},
    )


def test_observe_store_uses_preferred_matchup_labels_and_live_first_sort() -> None:
    store = ObserveStore(max_log_lines=200, matchup_by_game_id={"g_live": "PHI-ATL", "g_final": "CLE-BAL"})

    store.ingest(
        _event(
            ts=1000,
            level="info",
            event_type="game_state_changed",
            game_id="g_live",
            payload={"old_game_state": "UPCOMING", "new_game_state": "LIVE"},
        ),
        with_color=False,
    )
    store.ingest(
        _event(
            ts=1100,
            level="info",
            event_type="score_changed",
            game_id="g_live",
            payload={"new_home_score": 2, "new_away_score": 1, "period": "Top 3rd", "game_state": "LIVE"},
        ),
        with_color=False,
    )
    store.ingest(
        _event(
            ts=1200,
            level="info",
            event_type="game_state_changed",
            game_id="g_final",
            payload={"old_game_state": "LIVE", "new_game_state": "FINAL"},
        ),
        with_color=False,
    )

    board = store.render_scoreboard(max_games=10, with_color=False)
    assert "PHI-ATL" in board
    assert "CLE-BAL" in board
    assert board.index("PHI-ATL") < board.index("CLE-BAL")


def test_observe_store_filters_to_high_signal_and_meaningful_state_changes() -> None:
    store = ObserveStore(max_log_lines=200, matchup_by_game_id={"g1": "PHI-ATL"})

    # Non-meaningful state transition (live->live) should not emit log line.
    line_1 = store.ingest(
        _event(
            ts=1000,
            level="info",
            event_type="game_state_changed",
            game_id="g1",
            payload={"old_game_state": "LIVE", "new_game_state": "IN_PLAY"},
        ),
        with_color=False,
    )
    assert line_1 is None

    # High-signal state transition should emit.
    line_2 = store.ingest(
        _event(
            ts=1100,
            level="info",
            event_type="game_state_changed",
            game_id="g1",
            payload={"old_game_state": "UPCOMING", "new_game_state": "LIVE"},
        ),
        with_color=False,
    )
    assert line_2 is not None and "📍 STATE" in line_2

    # Trigger-only row is suppressed in default high-signal mode.
    line_3 = store.ingest(
        _event(ts=1200, level="warn", event_type="trigger_fired", game_id="g1", reason="edge"),
        with_color=False,
    )
    assert line_3 is None

    # Score + order rows remain.
    line_4 = store.ingest(
        _event(
            ts=1300,
            level="info",
            event_type="score_changed",
            game_id="g1",
            payload={"new_home_score": 3, "new_away_score": 1, "period": "Bot 5th", "game_state": "LIVE"},
        ),
        with_color=False,
    )
    assert line_4 is not None and "🎯 SCORE" in line_4

    line_5 = store.ingest(
        _event(
            ts=1400,
            level="info",
            event_type="order_submit_called",
            game_id="g1",
            payload={"side": "buy_yes", "limit_price": 0.41, "notional_usdc": 10.0, "time_in_force": "FAK"},
        ),
        with_color=False,
    )
    assert line_5 is not None and "📝 BET" in line_5


def test_effective_refresh_seconds_clamps_to_minimum() -> None:
    assert float(effective_refresh_seconds(1.0)) == 5.0
    assert float(effective_refresh_seconds("3")) == 5.0
    assert float(effective_refresh_seconds(5.0)) == 5.0
    assert float(effective_refresh_seconds(10.0)) == 10.0
    assert float(effective_refresh_seconds(90.0)) == 90.0


def test_decode_datagram_tolerates_malformed_payloads() -> None:
    assert _decode_datagram(b"") is None
    assert _decode_datagram(b"not-json") is None
    assert _decode_datagram(b"[]") is None

    parsed = _decode_datagram(
        b'{"ts_unix_ns":123,"level":"info","event_type":"score_changed","game_id":"g1","payload":{"new_home_score":1,"new_away_score":0}}'
    )
    assert parsed is not None
    assert parsed.event_type == "score_changed"
    assert parsed.game_id == "g1"
    assert int(parsed.payload.get("new_home_score") or 0) == 1


def test_inline_monitor_rebinds_stale_socket_path(monkeypatch) -> None:
    socket_path = f"/tmp/polybot2_test_monitor_{int(time.time() * 1_000_000)}.sock"
    with open(socket_path, "w", encoding="utf-8") as f:
        f.write("stale")

    class _FakeSock:
        def __init__(self):
            self.bound = ""
            self.closed = False

        def setblocking(self, _flag):
            return None

        def bind(self, path):
            self.bound = str(path)
            return None

        def close(self):
            self.closed = True
            return None

    fake_sock = _FakeSock()
    monkeypatch.setattr(observe_mod.socket, "socket", lambda *_a, **_k: fake_sock)

    monitor = HotpathInlineMonitor(
        logger=logging.getLogger("polybot2.test.observe"),
        config=MonitorConfig(socket_path=str(socket_path), refresh_seconds=45.0, max_games=5, no_color=True),
        output=io.StringIO(),
    )
    monitor._bind_socket()  # type: ignore[attr-defined]
    assert fake_sock.bound == str(socket_path)
    monitor._teardown_socket()  # type: ignore[attr-defined]

    assert not os.path.exists(socket_path)
    assert bool(fake_sock.closed) is True


def test_observe_store_ingests_heartbeat_and_populates_games() -> None:
    store = ObserveStore(max_log_lines=50)
    heartbeat = ObserveEvent(
        ts_unix_ns=1_000_000_000,
        level="info",
        event_type="runtime_heartbeat",
        game_id="",
        chain_id="",
        strategy_key="",
        reason_code="",
        order_client_id="",
        order_exchange_id="",
        payload={
            "games": {
                "g1": {"h": 3, "a": 1, "s": "LIVE", "inn": 5, "half": "top", "mc": False},
                "g2": {"s": "NOT STARTED"},
            },
            "teams": {
                "g1": ["Philadelphia Phillies", "Chicago Cubs"],
                "g2": ["Atlanta Braves", "New York Mets"],
            },
            "dm": "noop",
        },
    )
    result = store.ingest(heartbeat, with_color=False)
    assert result is None
    assert len(store.games) == 2
    assert store.games["g1"].home_score == 3
    assert store.games["g1"].away_score == 1
    assert store.games["g1"].state == "LIVE"
    assert store.games["g1"].period == "Top 5th"
    assert store.games["g2"].state == "NOT STARTED"
    assert store.ws_state == "CONNECTED"
    assert "noop" in store.exec_state
    assert store.games["g1"].matchup == "PHI-CHI"
    assert store.games["g2"].matchup == "ATL-NEW"


def test_heartbeat_does_not_appear_in_log() -> None:
    store = ObserveStore(max_log_lines=50)
    heartbeat = ObserveEvent(
        ts_unix_ns=1_000_000_000,
        level="info",
        event_type="runtime_heartbeat",
        game_id="",
        chain_id="",
        strategy_key="",
        reason_code="",
        order_client_id="",
        order_exchange_id="",
        payload={"games": {"g1": {"h": 1, "a": 0, "s": "LIVE"}}, "teams": {}, "dm": "noop"},
    )
    result = store.ingest(heartbeat, with_color=False)
    assert result is None
    assert len(store.logs) == 0
