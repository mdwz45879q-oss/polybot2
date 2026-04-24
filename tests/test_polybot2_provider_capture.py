from __future__ import annotations

from collections import deque
import json
import logging
from pathlib import Path
import time

from polybot2._cli.actions import run_provider_capture
from polybot2._cli.parser import build_parser
from polybot2.sports import (
    BoltOddsProvider,
    BoltOddsProviderConfig,
    JsonlRawFrameRecorder,
    JsonlUpdateRecorder,
    OddsUpdateEvent,
    PlayByPlayUpdateEvent,
    ProviderGameRecord,
    ScoreUpdateEvent,
    StreamEnvelope,
)


class _FakeWS:
    def __init__(self, frames: list[object]):
        self._frames = deque(frames)
        self.sent: list[str] = []

    def settimeout(self, timeout: float) -> None:
        del timeout

    def recv(self):
        if self._frames:
            return self._frames.popleft()
        raise TimeoutError("timed out")

    def send(self, payload: str) -> None:
        self.sent.append(payload)

    def close(self) -> None:
        return None


class _FakeWSFactory:
    def __init__(self, *, odds_frames: list[object], scores_frames: list[object], playbyplay_frames: list[object]):
        self.odds_frames = odds_frames
        self.scores_frames = scores_frames
        self.playbyplay_frames = playbyplay_frames

    def __call__(self, *, ws_url: str, timeout_seconds: float):
        del timeout_seconds
        if "playbyplay" in ws_url:
            return _FakeWS(list(self.playbyplay_frames))
        if "livescores" in ws_url:
            return _FakeWS(list(self.scores_frames))
        return _FakeWS(list(self.odds_frames))


class _FailConnectWSFactory:
    def __call__(self, *, ws_url: str, timeout_seconds: float):
        del ws_url, timeout_seconds
        raise RuntimeError("Handshake status 502 Bad Gateway")


def test_playbyplay_parser_emits_expected_event_fields() -> None:
    ack = b'{"action":"socket_connected","feed":"playbyplay"}'
    pbp_update = {
        "action": "play_update",
        "stream_id": "sid-1",
        "timestamp": "2026-04-18T19:10:00Z",
        "game": "Team A vs Team B, 2026-04-18, 07",
        "league": "MLB",
        "universal_id": "u-123",
        "state": {"inning": 5},
        "play_info": {"description": "Single"},
        "score": {"home": 2, "away": 3},
    }
    ws_factory = _FakeWSFactory(
        odds_frames=[],
        scores_frames=[],
        playbyplay_frames=[ack, json.dumps(pbp_update)],
    )
    provider = BoltOddsProvider(config=BoltOddsProviderConfig(api_key="k-test"), ws_factory=ws_factory)
    provider._http_get_json = lambda endpoint: {  # type: ignore[method-assign]
        "Team A vs Team B, 2026-04-18, 07": {"universal_id": "u-123", "sport": "MLB"}
    }
    provider.load_game_catalog()
    provider.subscribe_playbyplay(["u-123"])
    envs = provider.stream_playbyplay(read_timeout_seconds=0.05)
    assert len(envs) == 1
    event = envs[0].event
    assert isinstance(event, PlayByPlayUpdateEvent)
    assert event.universal_id == "u-123"
    assert event.action == "play_update"
    assert event.stream_id == "sid-1"
    assert event.league == "MLB"
    assert event.state.get("inning") == 5
    assert event.play_info.get("description") == "Single"
    assert event.score == {"home": 2, "away": 3}
    provider.close()


def test_playbyplay_parser_preserves_string_state_and_numeric_stream_id_fallback() -> None:
    ack = b'{"action":"socket_connected","feed":"playbyplay"}'
    pbp_update = {
        "stream": 2,
        "action": "new_play",
        "event": "Team A vs Team B, 2026-04-18, 07",
        "universal_id": "u-123",
        "league": "MLB",
        "state": "new_play",
        "play_info": {"title": "Top 7TH"},
        "score": {"home": 2, "away": 3},
    }
    ws_factory = _FakeWSFactory(
        odds_frames=[],
        scores_frames=[],
        playbyplay_frames=[ack, json.dumps(pbp_update)],
    )
    provider = BoltOddsProvider(config=BoltOddsProviderConfig(api_key="k-test"), ws_factory=ws_factory)
    provider._http_get_json = lambda endpoint: {  # type: ignore[method-assign]
        "Team A vs Team B, 2026-04-18, 07": {"universal_id": "u-123", "sport": "MLB"}
    }
    provider.load_game_catalog()
    provider.subscribe_playbyplay(["u-123"])
    envs = provider.stream_playbyplay(read_timeout_seconds=0.05)
    assert len(envs) == 1
    event = envs[0].event
    assert isinstance(event, PlayByPlayUpdateEvent)
    assert event.stream_id == "2"
    assert event.state == "new_play"
    provider.close()


def test_raw_frame_recorder_captures_handshake_ping_and_updates(tmp_path: Path) -> None:
    ack = b'{"action":"socket_connected","feed":"livescores"}'
    ping = b'{"action":"ping"}'
    score_update = {
        "action": "match_update",
        "timestamp": "2026-04-18T19:12:00Z",
        "game": "Team A vs Team B, 2026-04-18, 07",
        "universal_id": "u-123",
        "state": {"goalsA": 1, "goalsB": 0, "matchCompleted": False},
    }
    ws_factory = _FakeWSFactory(
        odds_frames=[],
        scores_frames=[ack, ping, json.dumps(score_update)],
        playbyplay_frames=[],
    )
    raw_rec = JsonlRawFrameRecorder(tmp_path / "raw", default_universal_id="u-123")
    provider = BoltOddsProvider(
        config=BoltOddsProviderConfig(api_key="k-test"),
        ws_factory=ws_factory,
        raw_frame_recorder=raw_rec,
    )
    provider._http_get_json = lambda endpoint: {  # type: ignore[method-assign]
        "Team A vs Team B, 2026-04-18, 07": {"universal_id": "u-123", "sport": "MLB"}
    }
    provider.load_game_catalog()
    provider.subscribe_scores(["u-123"])
    provider.stream_scores(read_timeout_seconds=0.05)  # consumes ping
    provider.stream_scores(read_timeout_seconds=0.05)  # consumes update
    provider.close()

    file_path = tmp_path / "raw" / "provider=boltodds" / "stream=scores" / "game=u-123.jsonl"
    assert file_path.exists()
    lines = [ln for ln in file_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) >= 3  # socket_connected + ping + update
    joined = "\n".join(lines)
    assert "socket_connected" in joined
    assert "\"action\":\"ping\"" in joined
    assert "match_update" in joined


def test_stream_scores_connect_failure_is_recoverable() -> None:
    provider = BoltOddsProvider(
        config=BoltOddsProviderConfig(api_key="k-test"),
        ws_factory=_FailConnectWSFactory(),
    )
    provider._http_get_json = lambda endpoint: {  # type: ignore[method-assign]
        "Team A vs Team B, 2026-04-18, 07": {"universal_id": "u-123", "sport": "MLB"}
    }
    provider.load_game_catalog()
    provider.subscribe_scores(["u-123"])
    envs = provider.stream_scores(read_timeout_seconds=0.05)
    assert envs == []
    metrics = provider.get_stream_metrics().get("scores") or {}
    assert int(metrics.get("errors") or 0) >= 1
    assert "connect:" in str(metrics.get("last_error") or "")
    provider.close()


class _CaptureProvider:
    def __init__(self, config, recorder=None, raw_frame_recorder=None, http_client=None, ws_factory=None):
        del config, http_client, ws_factory
        self._recorder = recorder
        self._raw = raw_frame_recorder
        self._scores_calls = 0
        self._odds_calls = 0
        self._pbp_calls = 0
        self._record = ProviderGameRecord(
            provider="boltodds",
            provider_game_id="u-123",
            game_label="Team A vs Team B, 2026-04-18, 07",
            league_key="mlb",
            parse_status="ok",
        )

    def load_game_catalog(self):
        return [self._record]

    def get_provider_record(self, provider_game_id: str):
        return self._record if str(provider_game_id) == "u-123" else None

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def subscribe_scores(self, universal_ids) -> None:
        del universal_ids
        return None

    def subscribe_playbyplay(self, universal_ids) -> None:
        del universal_ids
        return None

    def subscribe_odds(self, universal_ids) -> None:
        del universal_ids
        return None

    def stream_scores(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        self._scores_calls += 1
        match_completed = self._scores_calls >= 2
        event = ScoreUpdateEvent(
            provider="boltodds",
            universal_id="u-123",
            action="match_update",
            match_completed=match_completed,
            raw_payload={"action": "match_update", "matchCompleted": match_completed},
        )
        env = StreamEnvelope(
            provider="boltodds",
            stream="scores",
            universal_id="u-123",
            payload_kind="match_update",
            received_ts=int(time.time()),
            dedup_key=f"score_{self._scores_calls}",
            event=event,
        )
        if self._recorder is not None:
            self._recorder.record(env)
        if self._raw is not None:
            self._raw.record_raw(
                provider="boltodds",
                stream="scores",
                received_ts=int(time.time()),
                universal_id="u-123",
                raw_frame={"action": "match_update", "n": self._scores_calls},
                parsed_frame={"action": "match_update"},
            )
        return [env]

    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        self._pbp_calls += 1
        event = PlayByPlayUpdateEvent(
            provider="boltodds",
            universal_id="u-123",
            action="play_update",
            raw_payload={"action": "play_update", "n": self._pbp_calls},
        )
        env = StreamEnvelope(
            provider="boltodds",
            stream="playbyplay",
            universal_id="u-123",
            payload_kind="play_update",
            received_ts=int(time.time()),
            dedup_key=f"pbp_{self._pbp_calls}",
            event=event,
        )
        if self._recorder is not None:
            self._recorder.record(env)
        if self._raw is not None:
            self._raw.record_raw(
                provider="boltodds",
                stream="playbyplay",
                received_ts=int(time.time()),
                universal_id="u-123",
                raw_frame={"action": "play_update", "n": self._pbp_calls},
                parsed_frame={"action": "play_update"},
            )
        return [env]

    def stream_odds(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        self._odds_calls += 1
        event = OddsUpdateEvent(
            provider="boltodds",
            universal_id="u-123",
            action="odds_update",
            raw_payload={"action": "odds_update", "n": self._odds_calls},
        )
        env = StreamEnvelope(
            provider="boltodds",
            stream="odds",
            universal_id="u-123",
            payload_kind="odds_update",
            received_ts=int(time.time()),
            dedup_key=f"odds_{self._odds_calls}",
            event=event,
        )
        if self._recorder is not None:
            self._recorder.record(env)
        if self._raw is not None:
            self._raw.record_raw(
                provider="boltodds",
                stream="odds",
                received_ts=int(time.time()),
                universal_id="u-123",
                raw_frame={"action": "odds_update", "n": self._odds_calls},
                parsed_frame={"action": "odds_update"},
            )
        return [env]

    def get_stream_metrics(self):
        return {
            "odds": {"connect_successes": 1},
            "scores": {"connect_successes": 1},
            "playbyplay": {"connect_successes": 1},
        }


class _NeverCompleteCaptureProvider(_CaptureProvider):
    def stream_scores(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        return []

    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        return []

    def stream_odds(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        return []


class _RaisingCaptureProvider(_CaptureProvider):
    def stream_scores(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        raise RuntimeError("Handshake status 502 Bad Gateway")

    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        raise RuntimeError("Handshake status 502 Bad Gateway")

    def stream_odds(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        raise RuntimeError("Handshake status 502 Bad Gateway")

    def get_stream_metrics(self):
        return {
            "odds": {"connect_successes": 0},
            "scores": {"connect_successes": 0},
            "playbyplay": {"connect_successes": 0},
        }


class _MultiCaptureProvider:
    def __init__(self, config, recorder=None, raw_frame_recorder=None, http_client=None, ws_factory=None):
        del config, http_client, ws_factory
        self._recorder = recorder
        self._raw = raw_frame_recorder
        self._scores_calls = 0
        self._subscribed_scores: list[str] = []
        self._records = {
            "u-1": ProviderGameRecord(
                provider="boltodds",
                provider_game_id="u-1",
                game_label="Team A vs Team B, 2026-04-19",
                league_key="mlb",
                start_ts_utc=None,
                when_raw="2026-04-19T16:00:00Z",
                parse_status="ok",
            ),
            "u-2": ProviderGameRecord(
                provider="boltodds",
                provider_game_id="u-2",
                game_label="Team C vs Team D, 2026-04-19",
                league_key="mlb",
                start_ts_utc=None,
                when_raw="2026-04-19T17:00:00Z",
                parse_status="ok",
            ),
            "u-x": ProviderGameRecord(
                provider="boltodds",
                provider_game_id="u-x",
                game_label="Other League Game, 2026-04-19",
                league_key="epl",
                start_ts_utc=None,
                when_raw="2026-04-19T18:00:00Z",
                parse_status="ok",
            ),
            "u-y": ProviderGameRecord(
                provider="boltodds",
                provider_game_id="u-y",
                game_label="Old Date Game, 2026-04-18",
                league_key="mlb",
                start_ts_utc=None,
                when_raw="2026-04-18T18:00:00Z",
                parse_status="ok",
            ),
        }

    def load_game_catalog(self):
        return list(self._records.values())

    def get_provider_record(self, provider_game_id: str):
        return self._records.get(str(provider_game_id))

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def subscribe_scores(self, universal_ids) -> None:
        self._subscribed_scores = [str(x) for x in universal_ids]

    def subscribe_playbyplay(self, universal_ids) -> None:
        del universal_ids
        return None

    def subscribe_odds(self, universal_ids) -> None:
        del universal_ids
        return None

    def stream_scores(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        self._scores_calls += 1
        envs: list[StreamEnvelope] = []
        complete_after = {"u-1": 1, "u-2": 3}
        for uid in self._subscribed_scores:
            match_completed = self._scores_calls >= int(complete_after.get(uid, 99))
            event = ScoreUpdateEvent(
                provider="boltodds",
                universal_id=uid,
                action="match_update",
                match_completed=match_completed,
                raw_payload={"action": "match_update", "matchCompleted": match_completed, "uid": uid},
            )
            env = StreamEnvelope(
                provider="boltodds",
                stream="scores",
                universal_id=uid,
                payload_kind="match_update",
                received_ts=int(time.time()),
                dedup_key=f"{uid}_{self._scores_calls}",
                event=event,
            )
            if self._recorder is not None:
                self._recorder.record(env)
            if self._raw is not None:
                self._raw.record_raw(
                    provider="boltodds",
                    stream="scores",
                    received_ts=int(time.time()),
                    universal_id=uid,
                    raw_frame={"action": "match_update", "uid": uid, "n": self._scores_calls},
                    parsed_frame={"action": "match_update", "uid": uid},
                )
            envs.append(env)
        return envs

    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        return []

    def stream_odds(self, *, read_timeout_seconds: float = 1.0):
        del read_timeout_seconds
        return []

    def get_stream_metrics(self):
        return {
            "odds": {"connect_successes": 1},
            "scores": {"connect_successes": 1},
            "playbyplay": {"connect_successes": 1},
        }


def test_provider_capture_writes_manifest_and_dual_artifacts(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _CaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--universal-id",
            "u-123",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--tail-seconds",
            "0",
            "--max-duration-seconds",
            "5",
            "--read-timeout-seconds",
            "0.01",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 0

    manifests = list((tmp_path / "caps").rglob("manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["stop_reason"] == "game_completed_tail"
    assert int(manifest["counts"]["parsed"]["scores"]) >= 1
    assert int(manifest["counts"]["parsed"]["playbyplay"]) >= 1
    assert int(manifest["counts"]["raw"]["scores"]) >= 1
    assert int(manifest["counts"]["raw"]["playbyplay"]) >= 1

    parsed_score_files = list((Path(manifest["parsed_dir"]) / "provider=boltodds" / "stream=scores").rglob("*.jsonl"))
    parsed_pbp_files = list((Path(manifest["parsed_dir"]) / "provider=boltodds" / "stream=playbyplay").rglob("*.jsonl"))
    raw_score_files = list((Path(manifest["raw_dir"]) / "provider=boltodds" / "stream=scores").rglob("*.jsonl"))
    raw_pbp_files = list((Path(manifest["raw_dir"]) / "provider=boltodds" / "stream=playbyplay").rglob("*.jsonl"))
    assert parsed_score_files and parsed_pbp_files and raw_score_files and raw_pbp_files


def test_provider_capture_max_duration_fallback(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _NeverCompleteCaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--universal-id",
            "u-123",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--tail-seconds",
            "0",
            "--max-duration-seconds",
            "0.2",
            "--read-timeout-seconds",
            "0.01",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 0
    manifests = list((tmp_path / "caps").rglob("manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["stop_reason"] == "max_duration"


def test_provider_capture_returns_nonzero_for_unknown_game(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _CaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--universal-id",
            "unknown",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 1


def test_provider_capture_connection_not_established_fails_fast(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _RaisingCaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--universal-id",
            "u-123",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--max-duration-seconds",
            "5",
            "--read-timeout-seconds",
            "0.01",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 1
    manifests = list((tmp_path / "caps").rglob("manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["stop_reason"] == "connection_not_established"


def test_provider_capture_kalstrop_scores_and_odds(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _CaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "kalstrop",
            "--universal-id",
            "u-123",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--tail-seconds",
            "0",
            "--max-duration-seconds",
            "5",
            "--read-timeout-seconds",
            "0.01",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 0

    manifests = list((tmp_path / "caps").rglob("manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert int(manifest["counts"]["parsed"]["scores"]) >= 1
    assert int(manifest["counts"]["parsed"]["odds"]) >= 1
    assert int(manifest["counts"]["parsed"].get("playbyplay", 0)) == 0


def test_provider_capture_multi_game_shared_session_waits_for_all(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _MultiCaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--universal-id",
            "u-1",
            "--universal-id",
            "u-2",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--tail-seconds",
            "0",
            "--max-duration-seconds",
            "5",
            "--read-timeout-seconds",
            "0.01",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 0
    manifest = json.loads(next((tmp_path / "caps").rglob("manifest.json")).read_text(encoding="utf-8"))
    assert manifest["stop_reason"] == "all_games_completed_tail"
    assert sorted(manifest["resolved_ids"]) == ["u-1", "u-2"]
    per_game = {row["universal_id"]: row for row in manifest["per_game"]}
    assert per_game["u-1"]["stop_reason"] == "game_completed_tail"
    assert per_game["u-2"]["stop_reason"] == "game_completed_tail"


def test_provider_capture_ids_file_python_and_missing_ids_warn_skip(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    ids_file = tmp_path / "capture_ids.py"
    ids_file.write_text("UNIVERSAL_IDS = ['u-1', 'missing-id', 'u-2']\n", encoding="utf-8")

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _MultiCaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--ids-file",
            str(ids_file),
            "--ids-var",
            "UNIVERSAL_IDS",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--tail-seconds",
            "0",
            "--max-duration-seconds",
            "5",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 0
    manifest = json.loads(next((tmp_path / "caps").rglob("manifest.json")).read_text(encoding="utf-8"))
    assert sorted(manifest["resolved_ids"]) == ["u-1", "u-2"]
    assert manifest["missing_ids"] == ["missing-id"]


def test_provider_capture_today_selects_by_league_and_date(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    monkeypatch.setenv("BOLTODDS_API_KEY", "k-test")
    monkeypatch.setattr(data_actions,
        "build_sports_provider",
        lambda **kwargs: _MultiCaptureProvider(
            config=None,
            recorder=kwargs.get("recorder"),
            raw_frame_recorder=kwargs.get("raw_frame_recorder"),
        ),
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--today",
            "--date-et",
            "2026-04-19",
            "--league",
            "mlb",
            "--out",
            str(tmp_path / "caps"),
            "--tail-seconds",
            "0",
            "--max-duration-seconds",
            "5",
        ]
    )
    code = run_provider_capture(args, logger=logging.getLogger("polybot2.test.provider_capture"))
    assert code == 0
    manifest = json.loads(next((tmp_path / "caps").rglob("manifest.json")).read_text(encoding="utf-8"))
    assert manifest["selection_mode"] == "today"
    assert sorted(manifest["resolved_ids"]) == ["u-1", "u-2"]
