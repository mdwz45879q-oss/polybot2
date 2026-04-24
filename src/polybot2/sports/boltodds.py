"""BoltOdds provider implementation for odds/scores/play-by-play streaming."""

from __future__ import annotations

import json
from datetime import datetime, timezone
import re
import threading
import time
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

import httpx

try:  # pragma: no cover - optional dependency
    import orjson as _orjson
except Exception:  # pragma: no cover
    _orjson = None

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import (
    OddsOutcome,
    OddsUpdateEvent,
    PlayByPlayUpdateEvent,
    ProviderGameRecord,
    ScoreUpdateEvent,
    SportsProviderConfig,
    StreamEnvelope,
)
from polybot2.sports.recorder import NullRawFrameRecorder, NullRecorder, RawFrameRecorder, UpdateRecorder
from polybot2.linking.normalize import normalize_league_key, sport_key_for_league


class BoltOddsProviderConfig(SportsProviderConfig):
    """Runtime settings for BoltOdds integration."""

    def __init__(
        self,
        *,
        api_key: str,
        http_base: str = "https://spro.agency/api",
        odds_ws_url: str = "wss://spro.agency/api",
        scores_ws_url: str = "wss://spro.agency/api/livescores",
        playbyplay_ws_url: str = "wss://spro.agency/api/playbyplay",
        request_timeout_seconds: float = 20.0,
        reconnect_sleep_seconds: float = 0.2,
        queue_maxsize: int = 50_000,
    ):
        super().__init__(
            provider_name="boltodds",
            request_timeout_seconds=float(request_timeout_seconds),
            reconnect_sleep_seconds=float(reconnect_sleep_seconds),
            queue_maxsize=int(queue_maxsize),
        )
        self.api_key = str(api_key or "").strip()
        self.http_base = str(http_base or "").rstrip("/")
        self.odds_ws_url = str(odds_ws_url or "").strip()
        self.scores_ws_url = str(scores_ws_url or "").strip()
        self.playbyplay_ws_url = str(playbyplay_ws_url or "").strip()
        if not self.api_key:
            raise ValueError("api_key must be non-empty")
        if not self.http_base:
            raise ValueError("http_base must be non-empty")
        if not self.odds_ws_url:
            raise ValueError("odds_ws_url must be non-empty")
        if not self.scores_ws_url:
            raise ValueError("scores_ws_url must be non-empty")
        if not self.playbyplay_ws_url:
            raise ValueError("playbyplay_ws_url must be non-empty")


class BoltOddsProvider(SportsDataProviderBase):
    _et_tz = ZoneInfo("America/New_York")

    def __init__(
        self,
        *,
        config: BoltOddsProviderConfig,
        recorder: UpdateRecorder | None = None,
        raw_frame_recorder: RawFrameRecorder | None = None,
        http_client: httpx.Client | None = None,
        ws_factory: Any | None = None,
    ):
        super().__init__(
            config=SportsProviderConfig(
                provider_name="boltodds",
                request_timeout_seconds=float(config.request_timeout_seconds),
                reconnect_sleep_seconds=float(config.reconnect_sleep_seconds),
                queue_maxsize=int(config.queue_maxsize),
            ),
            recorder=recorder or NullRecorder(),
            http_client=http_client,
        )
        self._cfg = config
        self._raw_frame_recorder = raw_frame_recorder or NullRawFrameRecorder()
        self._ws_factory = ws_factory

        self._catalog_lock = threading.RLock()
        self._catalog_by_uid: dict[str, ProviderGameRecord] = {}
        self._uid_to_games: dict[str, set[str]] = {}
        self._game_to_uid: dict[str, str] = {}
        self._game_to_uid_normalized: dict[str, str] = {}

        self._subscribed_odds_uids: set[str] = set()
        self._subscribed_scores_uids: set[str] = set()
        self._subscribed_playbyplay_uids: set[str] = set()

        self._odds_ws: Any | None = None
        self._scores_ws: Any | None = None
        self._playbyplay_ws: Any | None = None
        self._metrics: dict[str, dict[str, Any]] = {
            "odds": {
                "connect_attempts": 0,
                "connect_successes": 0,
                "reconnects": 0,
                "recv_calls": 0,
                "events_emitted": 0,
                "timeouts": 0,
                "errors": 0,
                "last_error": "",
            },
            "scores": {
                "connect_attempts": 0,
                "connect_successes": 0,
                "reconnects": 0,
                "recv_calls": 0,
                "events_emitted": 0,
                "timeouts": 0,
                "errors": 0,
                "last_error": "",
            },
            "playbyplay": {
                "connect_attempts": 0,
                "connect_successes": 0,
                "reconnects": 0,
                "recv_calls": 0,
                "events_emitted": 0,
                "timeouts": 0,
                "errors": 0,
                "last_error": "",
            },
        }
        self._last_stream_timing: dict[str, dict[str, int]] = {
            "odds": {},
            "scores": {},
            "playbyplay": {},
        }

    def _mark_metric(self, stream: str, key: str, delta: int = 1) -> None:
        if stream not in self._metrics:
            return
        current = self._metrics[stream].get(key, 0)
        try:
            self._metrics[stream][key] = int(current) + int(delta)
        except Exception:
            self._metrics[stream][key] = current

    def _set_metric(self, stream: str, key: str, value: Any) -> None:
        if stream not in self._metrics:
            return
        self._metrics[stream][key] = value

    def get_stream_metrics(self) -> dict[str, dict[str, Any]]:
        return {
            "odds": dict(self._metrics.get("odds", {})),
            "scores": dict(self._metrics.get("scores", {})),
            "playbyplay": dict(self._metrics.get("playbyplay", {})),
        }

    def pop_last_stream_timing(self, *, stream: str) -> dict[str, int]:
        key = str(stream or "").strip().lower()
        if key not in self._last_stream_timing:
            return {}
        timing = self._last_stream_timing.get(key) or {}
        self._last_stream_timing[key] = {}
        return dict(timing)

    @property
    def config(self) -> BoltOddsProviderConfig:
        return self._cfg

    @property
    def odds_ws_uri(self) -> str:
        return f"{self._cfg.odds_ws_url}?key={self._cfg.api_key}"

    @property
    def scores_ws_uri(self) -> str:
        return f"{self._cfg.scores_ws_url}?key={self._cfg.api_key}"

    @property
    def playbyplay_ws_uri(self) -> str:
        return f"{self._cfg.playbyplay_ws_url}?key={self._cfg.api_key}"

    def _create_ws(self, *, ws_url: str, timeout_seconds: float) -> Any:
        if self._ws_factory is not None:
            return self._ws_factory(ws_url=ws_url, timeout_seconds=float(timeout_seconds))
        # Prefer websockets sync transport to match notebook-proven connectivity.
        try:
            from websockets.sync.client import connect as ws_connect  # type: ignore

            class _WebsocketsSyncAdapter:
                def __init__(self, conn: Any, timeout: float):
                    self._conn = conn
                    self._timeout = float(timeout)

                def settimeout(self, timeout: float) -> None:
                    self._timeout = float(timeout)

                def recv(self) -> Any:
                    return self._conn.recv(timeout=max(0.01, float(self._timeout)))

                def send(self, payload: str) -> None:
                    self._conn.send(payload)

                def close(self) -> None:
                    self._conn.close()

            conn = ws_connect(
                ws_url,
                open_timeout=max(0.1, float(timeout_seconds)),
                close_timeout=5.0,
                ping_interval=20.0,
                ping_timeout=20.0,
                max_size=None,
            )
            return _WebsocketsSyncAdapter(conn=conn, timeout=float(timeout_seconds))
        except Exception:
            pass
        try:
            import websocket  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "A websocket client is required for sports streaming. Install `websockets` or `websocket-client`."
            ) from exc
        return websocket.create_connection(
            ws_url,
            timeout=max(0.1, float(timeout_seconds)),
            enable_multithread=True,
        )

    def _http_get_json(self, endpoint: str) -> Any:
        url = f"{self._cfg.http_base}/{endpoint}?key={self._cfg.api_key}"
        response = self._client.get(url)
        response.raise_for_status()
        return response.json()

    def _http_post_json(self, endpoint: str, payload: dict[str, Any]) -> Any:
        url = f"{self._cfg.http_base}/{endpoint}?key={self._cfg.api_key}"
        response = self._client.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalize_game_label(value: str) -> str:
        text = " ".join(str(value or "").strip().lower().split())
        return text

    @staticmethod
    def _extract_universal_id(row: dict[str, Any]) -> str:
        for key in ("universal_id", "universalId", "universalID", "uid"):
            value = row.get(key)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _extract_game_label(row: dict[str, Any]) -> str:
        for key in ("game", "event", "name"):
            value = row.get(key)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _extract_orig_teams(row: dict[str, Any]) -> str:
        for key in ("orig_teams", "origTeams", "orig_teams_label"):
            text = str(row.get(key) or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _parse_start_ts(value: str) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
        for fmt in ("%Y-%m-%d, %I:%M %p", "%Y-%m-%d %I:%M %p", "%Y-%m-%d, %H:%M", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=BoltOddsProvider._et_tz)
                return int(dt.astimezone(timezone.utc).timestamp())
            except Exception:
                continue
        return None

    @staticmethod
    def _parse_teams_from_label(label: str) -> tuple[str, str]:
        text = str(label or "").strip()
        if not text:
            return ("", "")
        # Strip known suffix patterns like ", 2026-04-16, 01".
        text = re.sub(r",\s*\d{4}-\d{2}-\d{2}(?:,.*)?$", "", text).strip()
        for sep in (" vs ", " vs. ", " v ", " v. ", " @ "):
            if sep not in text:
                continue
            left, right = text.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if not left or not right:
                return ("", "")
            if sep == " @ ":
                return (right, left)
            return (left, right)
        return ("", "")

    @staticmethod
    def _normalize_sport_and_league(*, sport_raw: str, league_raw: str) -> tuple[str, str]:
        league_key = normalize_league_key(league_raw) if str(league_raw or "").strip() else ""
        # BoltOdds often sends league-like labels in sport.
        league_from_sport = normalize_league_key(sport_raw) if str(sport_raw or "").strip() else ""
        if not league_key and league_from_sport and sport_key_for_league(league_from_sport):
            league_key = league_from_sport
        sport_key = sport_key_for_league(league_key)
        if not sport_key:
            raw = str(sport_raw or "").strip().lower()
            direct_map = {
                "soccer": "soccer",
                "football": "soccer",
                "basketball": "basketball",
                "nba": "basketball",
                "mlb": "baseball",
                "baseball": "baseball",
                "nfl": "american_football",
                "american football": "american_football",
                "nhl": "ice_hockey",
                "hockey": "ice_hockey",
                "tennis": "tennis",
                "ufc": "mma",
                "mma": "mma",
                "boxing": "boxing",
                "dota": "esports",
                "cs2": "esports",
                "valorant": "esports",
                "league of legends": "esports",
            }
            sport_key = direct_map.get(raw, raw.replace(" ", "_"))
        return (str(sport_key or ""), str(league_key or ""))

    def _build_provider_record_from_row(self, row: dict[str, Any]) -> ProviderGameRecord | None:
        uid = str(self._extract_universal_id(row))
        if not uid:
            return None
        game_label = self._extract_game_label(row)
        orig_teams = self._extract_orig_teams(row)
        sport_raw = str(row.get("sport") or "").strip()
        league_raw = str(row.get("league") or row.get("competition") or "").strip()
        when_raw = str(row.get("when") or row.get("start_time") or row.get("start") or "").strip()
        explicit_home = str(row.get("home") or row.get("home_team") or row.get("homeTeam") or "").strip()
        explicit_away = str(row.get("away") or row.get("away_team") or row.get("awayTeam") or "").strip()
        home_raw, away_raw = explicit_home, explicit_away
        parse_reason_parts: list[str] = []

        if not home_raw or not away_raw:
            home_o, away_o = self._parse_teams_from_label(orig_teams)
            if home_o and away_o:
                home_raw, away_raw = home_o, away_o
            else:
                home_g, away_g = self._parse_teams_from_label(game_label)
                if home_g and away_g:
                    home_raw, away_raw = home_g, away_g
        if not home_raw or not away_raw:
            parse_reason_parts.append("missing_teams")

        start_ts_utc = self._parse_start_ts(when_raw)
        if start_ts_utc is None:
            parse_reason_parts.append("invalid_start_time")

        sport_key, league_key = self._normalize_sport_and_league(sport_raw=sport_raw, league_raw=league_raw)
        if not sport_key:
            parse_reason_parts.append("missing_sport_key")

        alias_values: set[str] = set()
        if game_label:
            alias_values.add(game_label)
        if orig_teams:
            alias_values.add(orig_teams)
        row_aliases = row.get("aliases")
        if isinstance(row_aliases, (list, tuple)):
            for alias in row_aliases:
                text = str(alias or "").strip()
                if text:
                    alias_values.add(text)
        parse_status = "ok" if not parse_reason_parts else "partial"
        return ProviderGameRecord(
            provider="boltodds",
            provider_game_id=uid,
            game_label=game_label,
            orig_teams=orig_teams,
            sport_raw=sport_raw,
            league_raw=league_raw,
            when_raw=when_raw,
            home_team_raw=home_raw,
            away_team_raw=away_raw,
            sport_key=sport_key,
            league_key=league_key,
            start_ts_utc=start_ts_utc,
            parse_status=parse_status,
            parse_reason="|".join(parse_reason_parts),
            aliases=tuple(sorted(alias_values)),
            raw_payload=dict(row),
        )

    @staticmethod
    def _candidate_score(record: ProviderGameRecord) -> int:
        score = 0
        if record.home_team_raw and record.away_team_raw:
            score += 20
        elif record.home_team_raw or record.away_team_raw:
            score += 8
        if record.start_ts_utc is not None:
            score += 10
        if record.league_key:
            score += 6
        if record.sport_key:
            score += 4
        if record.orig_teams:
            score += 2
        if record.game_label:
            score += 1
        if record.parse_status == "ok":
            score += 1
        return score

    def _rows_from_games_payload(self, payload: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            for game, meta in payload.items():
                row = {"game": game}
                if isinstance(meta, dict):
                    row.update(meta)
                rows.append(row)
            return rows
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    rows.append(dict(item))
                else:
                    rows.append({"game": str(item)})
            return rows
        return rows

    def load_game_catalog(self) -> list[ProviderGameRecord]:
        payload = self._http_get_json("get_games")
        rows = self._rows_from_games_payload(payload)
        grouped: dict[str, list[ProviderGameRecord]] = {}
        for row in rows:
            record = self._build_provider_record_from_row(row)
            if record is None:
                continue
            grouped.setdefault(str(record.provider_game_id), []).append(record)

        with self._catalog_lock:
            self._catalog_by_uid.clear()
            self._uid_to_games.clear()
            self._game_to_uid.clear()
            self._game_to_uid_normalized.clear()

            for uid, candidates in grouped.items():
                candidates_sorted = sorted(
                    candidates,
                    key=lambda rec: (self._candidate_score(rec), len(rec.aliases)),
                    reverse=True,
                )
                selected = candidates_sorted[0]
                all_aliases: set[str] = set()
                game_labels: set[str] = set()
                for rec in candidates:
                    all_aliases.update({str(a).strip() for a in rec.aliases if str(a).strip()})
                    if rec.game_label:
                        game_labels.add(str(rec.game_label))
                merged = ProviderGameRecord(
                    provider=selected.provider,
                    provider_game_id=selected.provider_game_id,
                    game_label=selected.game_label,
                    orig_teams=selected.orig_teams,
                    sport_raw=selected.sport_raw,
                    league_raw=selected.league_raw,
                    when_raw=selected.when_raw,
                    home_team_raw=selected.home_team_raw,
                    away_team_raw=selected.away_team_raw,
                    sport_key=selected.sport_key,
                    league_key=selected.league_key,
                    start_ts_utc=selected.start_ts_utc,
                    parse_status=selected.parse_status,
                    parse_reason=selected.parse_reason,
                    aliases=tuple(sorted(all_aliases)),
                    raw_payload=dict(selected.raw_payload),
                )
                self._catalog_by_uid[uid] = merged
                self._uid_to_games[uid] = game_labels
                for alias in all_aliases:
                    self._game_to_uid[alias] = uid
                    norm = self._normalize_game_label(alias)
                    if norm:
                        self._game_to_uid_normalized[norm] = uid
                for label in game_labels:
                    norm = self._normalize_game_label(label)
                    if norm:
                        self._game_to_uid_normalized[norm] = uid

            out = [self._catalog_by_uid[k] for k in sorted(self._catalog_by_uid)]
        return out

    def fetch_info(self) -> Any:
        return self._http_get_json("get_info")

    def fetch_games_raw(self) -> Any:
        return self._http_get_json("get_games")

    def game_labels_for_universal_id(self, universal_id: str) -> list[str]:
        uid = str(universal_id or "").strip()
        if not uid:
            return []
        with self._catalog_lock:
            return sorted(self._uid_to_games.get(uid, set()))

    def _get_provider_record(self, universal_id: str) -> ProviderGameRecord | None:
        with self._catalog_lock:
            return self._catalog_by_uid.get(str(universal_id))

    def resolve_universal_ids(
        self,
        *,
        game_labels: Sequence[str] | None = None,
        universal_ids: Sequence[str] | None = None,
    ) -> list[str]:
        with self._catalog_lock:
            has_catalog = bool(self._catalog_by_uid)
        if not has_catalog:
            self.load_game_catalog()

        resolved: set[str] = set()
        for uid in (universal_ids or ()): 
            text = str(uid or "").strip()
            if not text:
                continue
            if self._get_provider_record(text) is not None:
                resolved.add(text)

        with self._catalog_lock:
            for label in (game_labels or ()): 
                game = str(label or "").strip()
                if not game:
                    continue
                uid = self._game_to_uid.get(game)
                if not uid:
                    uid = self._game_to_uid_normalized.get(self._normalize_game_label(game))
                if uid:
                    resolved.add(uid)

        return sorted(resolved)

    def _send_subscribe(self, *, ws: Any, stream: str, universal_ids: Sequence[str]) -> None:
        game_labels: list[str] = []
        with self._catalog_lock:
            for uid in universal_ids:
                for game in sorted(self._uid_to_games.get(str(uid), set())):
                    if game not in game_labels:
                        game_labels.append(game)

        if not game_labels:
            return

        payload = {
            "action": "subscribe",
            "filters": {"games": game_labels},
        }
        ws.send(json.dumps(payload, separators=(",", ":"), default=str))

    def _ensure_odds_ws(self) -> Any:
        if self._odds_ws is not None:
            return self._odds_ws
        self._mark_metric("odds", "connect_attempts", 1)
        ws = self._create_ws(ws_url=self.odds_ws_uri, timeout_seconds=8.0)
        self._odds_ws = ws
        self._mark_metric("odds", "connect_successes", 1)
        if int(self._metrics["odds"]["connect_successes"]) > 1:
            self._mark_metric("odds", "reconnects", 1)
        try:
            ws.settimeout(0.5)
            ack = ws.recv()  # socket_connected ack
            parsed_ack, parse_error = self._try_decode_ws_frame(ack)
            self._record_raw_frame(stream="odds", raw_frame=ack, parsed_frame=parsed_ack, parse_error=parse_error)
        except Exception:
            pass
        if self._subscribed_odds_uids:
            self._send_subscribe(ws=ws, stream="odds", universal_ids=sorted(self._subscribed_odds_uids))
        return ws

    def _ensure_scores_ws(self) -> Any:
        if self._scores_ws is not None:
            return self._scores_ws
        self._mark_metric("scores", "connect_attempts", 1)
        ws = self._create_ws(ws_url=self.scores_ws_uri, timeout_seconds=8.0)
        self._scores_ws = ws
        self._mark_metric("scores", "connect_successes", 1)
        if int(self._metrics["scores"]["connect_successes"]) > 1:
            self._mark_metric("scores", "reconnects", 1)
        try:
            ws.settimeout(0.5)
            ack = ws.recv()  # socket_connected ack
            parsed_ack, parse_error = self._try_decode_ws_frame(ack)
            self._record_raw_frame(stream="scores", raw_frame=ack, parsed_frame=parsed_ack, parse_error=parse_error)
        except Exception:
            pass
        if self._subscribed_scores_uids:
            self._send_subscribe(ws=ws, stream="scores", universal_ids=sorted(self._subscribed_scores_uids))
        return ws

    def _ensure_playbyplay_ws(self) -> Any:
        if self._playbyplay_ws is not None:
            return self._playbyplay_ws
        self._mark_metric("playbyplay", "connect_attempts", 1)
        ws = self._create_ws(ws_url=self.playbyplay_ws_uri, timeout_seconds=8.0)
        self._playbyplay_ws = ws
        self._mark_metric("playbyplay", "connect_successes", 1)
        if int(self._metrics["playbyplay"]["connect_successes"]) > 1:
            self._mark_metric("playbyplay", "reconnects", 1)
        try:
            ws.settimeout(0.5)
            ack = ws.recv()  # socket_connected ack
            parsed_ack, parse_error = self._try_decode_ws_frame(ack)
            self._record_raw_frame(stream="playbyplay", raw_frame=ack, parsed_frame=parsed_ack, parse_error=parse_error)
        except Exception:
            pass
        if self._subscribed_playbyplay_uids:
            self._send_subscribe(ws=ws, stream="playbyplay", universal_ids=sorted(self._subscribed_playbyplay_uids))
        return ws

    def _close_odds_ws(self) -> None:
        ws = self._odds_ws
        self._odds_ws = None
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            pass

    def _close_scores_ws(self) -> None:
        ws = self._scores_ws
        self._scores_ws = None
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            pass

    def _close_playbyplay_ws(self) -> None:
        ws = self._playbyplay_ws
        self._playbyplay_ws = None
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            pass

    @staticmethod
    def _is_timeout_exc(exc: Exception) -> bool:
        name = type(exc).__name__.lower()
        text = str(exc).lower()
        if "timeout" in name or "timed out" in text or "timeout" in text:
            return True
        return isinstance(exc, TimeoutError)

    def _on_close(self) -> None:
        self._close_odds_ws()
        self._close_scores_ws()
        self._close_playbyplay_ws()
        try:
            self._raw_frame_recorder.close()
        except Exception:
            pass

    def subscribe_scores(self, universal_ids: Sequence[str]) -> None:
        resolved = self.resolve_universal_ids(universal_ids=universal_ids)
        prev = set(self._subscribed_scores_uids)
        self._subscribed_scores_uids = set(resolved)
        if self._scores_ws is not None:
            if not resolved:
                self._close_scores_ws()
                return
            if prev and (prev - set(resolved)):
                self._close_scores_ws()
                return
            self._send_subscribe(ws=self._scores_ws, stream="scores", universal_ids=resolved)

    def subscribe_odds(self, universal_ids: Sequence[str]) -> None:
        resolved = self.resolve_universal_ids(universal_ids=universal_ids)
        prev = set(self._subscribed_odds_uids)
        self._subscribed_odds_uids = set(resolved)
        if self._odds_ws is not None:
            if not resolved:
                self._close_odds_ws()
                return
            if prev and (prev - set(resolved)):
                self._close_odds_ws()
                return
            self._send_subscribe(ws=self._odds_ws, stream="odds", universal_ids=resolved)

    def subscribe_playbyplay(self, universal_ids: Sequence[str]) -> None:
        resolved = self.resolve_universal_ids(universal_ids=universal_ids)
        self._subscribed_playbyplay_uids = set(resolved)
        if self._playbyplay_ws is not None and resolved:
            self._send_subscribe(ws=self._playbyplay_ws, stream="playbyplay", universal_ids=resolved)

    @staticmethod
    def _decode_ws_frame(raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            if _orjson is not None:
                return _orjson.loads(raw)
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            if _orjson is not None:
                return _orjson.loads(raw.encode("utf-8"))
            return json.loads(raw)
        return raw

    @staticmethod
    def _try_decode_ws_frame(raw: Any) -> tuple[Any | None, str]:
        try:
            return (BoltOddsProvider._decode_ws_frame(raw), "")
        except Exception as exc:
            return (None, f"{type(exc).__name__}: {exc}")

    @staticmethod
    def _iter_payload_items(parsed: Any) -> list[dict[str, Any]]:
        items = parsed if isinstance(parsed, list) else [parsed]
        out: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                out.append(item)
        return out

    @staticmethod
    def _iter_odds_payloads(item: dict[str, Any]):
        data = item.get("data")
        if isinstance(data, list):
            for row in data:
                if isinstance(row, dict):
                    merged = dict(item)
                    merged["data"] = row
                    yield merged
            return
        yield item

    @staticmethod
    def _state_dict(msg: dict[str, Any]) -> dict[str, Any]:
        state = msg.get("state")
        if isinstance(state, dict):
            return state
        for k in ("live_state", "liveScore", "score_state"):
            maybe = msg.get(k)
            if isinstance(maybe, dict):
                return maybe
        return {}

    @staticmethod
    def _first_not_none(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _ab_to_home_away(value_a: Any, value_b: Any, designation: dict[str, Any]) -> tuple[Any, Any]:
        side_a = str((designation or {}).get("A", "home")).lower()
        side_b = str((designation or {}).get("B", "away")).lower()
        home_val, away_val = value_a, value_b
        if side_a == "away" and side_b == "home":
            home_val, away_val = value_b, value_a
        return home_val, away_val

    def _uid_from_payload(self, payload: dict[str, Any]) -> str:
        uid = self._extract_universal_id(payload)
        if uid:
            return uid
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        uid = self._extract_universal_id(data)
        if uid:
            return uid
        game = str(data.get("game") or payload.get("game") or payload.get("event") or "").strip()
        if not game:
            return ""
        with self._catalog_lock:
            uid = self._game_to_uid.get(game)
            if not uid:
                uid = self._game_to_uid_normalized.get(self._normalize_game_label(game))
            return str(uid or "")

    @staticmethod
    def _game_from_payload(payload: dict[str, Any]) -> str:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        return str(data.get("game") or payload.get("game") or payload.get("event") or "").strip()

    def _record_raw_frame(self, *, stream: str, raw_frame: Any, parsed_frame: Any = None, parse_error: str = "") -> None:
        universal_id = ""
        game_label = ""
        source = parsed_frame if parsed_frame is not None else raw_frame
        if isinstance(source, dict):
            universal_id = self._uid_from_payload(source)
            game_label = self._game_from_payload(source)
        elif isinstance(source, list) and source:
            first = source[0]
            if isinstance(first, dict):
                universal_id = self._uid_from_payload(first)
                game_label = self._game_from_payload(first)
        try:
            self._raw_frame_recorder.record_raw(
                provider="boltodds",
                stream=str(stream),
                received_ts=int(time.time()),
                universal_id=str(universal_id or ""),
                game_label=str(game_label or ""),
                raw_frame=raw_frame,
                parsed_frame=parsed_frame,
                parse_error=str(parse_error or ""),
            )
        except Exception:
            pass

    def _close_stream_ws(self, stream: str) -> None:
        if stream == "odds":
            self._close_odds_ws()
            return
        if stream == "scores":
            self._close_scores_ws()
            return
        self._close_playbyplay_ws()

    def _try_ensure_stream_ws(self, *, stream: str, ensure: Callable[[], Any]) -> Any | None:
        try:
            return ensure()
        except Exception as exc:
            self._mark_metric(stream, "errors", 1)
            self._set_metric(stream, "last_error", f"connect:{type(exc).__name__}: {exc}")
            self._close_stream_ws(stream)
            time.sleep(float(self._config.reconnect_sleep_seconds))
            return None

    def _emit_event(self, *, stream: str, payload_kind: str, uid: str, provider_timestamp: str, raw: dict[str, Any], event: Any) -> StreamEnvelope:
        received_ts = int(time.time())
        dedup_key = self.build_dedup_key(
            stream=stream,
            universal_id=uid,
            payload_kind=payload_kind,
            provider_timestamp=provider_timestamp,
            raw_payload=raw,
        )
        env = StreamEnvelope(
            provider="boltodds",
            stream=str(stream),
            universal_id=str(uid),
            payload_kind=str(payload_kind),
            received_ts=int(received_ts),
            dedup_key=str(dedup_key),
            event=event,
        )
        self._enqueue(env)
        return env

    def _recv_parsed(self, *, stream: str, ws: Any, read_timeout_seconds: float) -> tuple[Any, int, dict[str, int]] | None:
        try:
            ws.settimeout(max(0.01, float(read_timeout_seconds)))
        except Exception:
            pass
        self._mark_metric(stream, "recv_calls", 1)
        recv_started_ns = time.perf_counter_ns()
        try:
            raw = ws.recv()
            source_recv_monotonic_ns = int(time.perf_counter_ns())
        except Exception as exc:
            recv_wait_ns = max(0, int(time.perf_counter_ns() - recv_started_ns))
            self._last_stream_timing[str(stream)] = {
                "recv_wait_ns": int(recv_wait_ns),
                "decode_ns": 0,
                "provider_map_ns": 0,
                "events_emitted": 0,
            }
            if self._is_timeout_exc(exc):
                self._mark_metric(stream, "timeouts", 1)
                return None
            self._mark_metric(stream, "errors", 1)
            self._set_metric(stream, "last_error", f"{type(exc).__name__}: {exc}")
            self._close_stream_ws(stream)
            time.sleep(float(self._config.reconnect_sleep_seconds))
            return None
        recv_wait_ns = max(0, int(source_recv_monotonic_ns - recv_started_ns))
        decode_started_ns = time.perf_counter_ns()
        try:
            parsed = self._decode_ws_frame(raw)
        except Exception as exc:
            decode_ns = max(0, int(time.perf_counter_ns() - decode_started_ns))
            self._last_stream_timing[str(stream)] = {
                "recv_wait_ns": int(recv_wait_ns),
                "decode_ns": int(decode_ns),
                "provider_map_ns": 0,
                "events_emitted": 0,
            }
            self._mark_metric(stream, "errors", 1)
            self._set_metric(stream, "last_error", f"decode:{type(exc).__name__}: {exc}")
            self._record_raw_frame(stream=stream, raw_frame=raw, parsed_frame=None, parse_error=f"{type(exc).__name__}: {exc}")
            return None
        decode_ns = max(0, int(time.perf_counter_ns() - decode_started_ns))
        self._record_raw_frame(stream=stream, raw_frame=raw, parsed_frame=parsed, parse_error="")
        return parsed, int(source_recv_monotonic_ns), {
            "recv_wait_ns": int(recv_wait_ns),
            "decode_ns": int(decode_ns),
            "provider_map_ns": 0,
            "events_emitted": 0,
        }

    def stream_odds_fast(self, *, read_timeout_seconds: float = 1.0) -> list[OddsUpdateEvent]:
        ws = self._try_ensure_stream_ws(stream="odds", ensure=self._ensure_odds_ws)
        if ws is None:
            return []
        parsed_msg = self._recv_parsed(stream="odds", ws=ws, read_timeout_seconds=read_timeout_seconds)
        if parsed_msg is None:
            return []
        parsed, source_recv_monotonic_ns, base_timing = parsed_msg
        map_started_ns = time.perf_counter_ns()
        out: list[OddsUpdateEvent] = []
        for item in self._iter_payload_items(parsed):
            action = str(item.get("action") or "")
            if action == "ping":
                continue
            for payload in self._iter_odds_payloads(item):
                uid = self._uid_from_payload(payload)
                if not uid:
                    continue
                data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
                info = data.get("info") if isinstance(data.get("info"), dict) else {}
                outcomes_obj = data.get("outcomes") if isinstance(data.get("outcomes"), dict) else {}
                outcomes: list[OddsOutcome] = []
                for outcome_key, outcome_payload in outcomes_obj.items():
                    op = outcome_payload if isinstance(outcome_payload, dict) else {}
                    outcomes.append(
                        OddsOutcome(
                            outcome_key=str(outcome_key),
                            odds=op.get("odds"),
                            outcome_name=str(op.get("outcome_name") or ""),
                            outcome_target=str(op.get("outcome_target") or ""),
                            outcome_line=str(op.get("outcome_line") or ""),
                            outcome_over_under=str(op.get("outcome_over_under") or ""),
                            outcome_link=str(op.get("link") or ""),
                        )
                    )
                out.append(
                    OddsUpdateEvent(
                        provider="boltodds",
                        universal_id=str(uid),
                        action=action,
                        provider_timestamp=str(payload.get("timestamp") or data.get("timestamp") or ""),
                        game=str(data.get("game") or payload.get("game") or ""),
                        sport=str(data.get("sport") or ""),
                        league=str(data.get("league") or ""),
                        sportsbook=str(data.get("sportsbook") or ""),
                        home_team=str(data.get("home_team") or payload.get("home") or ""),
                        away_team=str(data.get("away_team") or payload.get("away") or ""),
                        game_when=str(info.get("when") or data.get("when") or ""),
                        book_event_id=str(info.get("id") or ""),
                        book_event_link=str(info.get("link") or ""),
                        outcomes=tuple(outcomes),
                        raw_payload=payload,
                        source_recv_monotonic_ns=int(source_recv_monotonic_ns),
                    )
                )
        if out:
            self._mark_metric("odds", "events_emitted", len(out))
        self._last_stream_timing["odds"] = {
            "recv_wait_ns": int(base_timing.get("recv_wait_ns") or 0),
            "decode_ns": int(base_timing.get("decode_ns") or 0),
            "provider_map_ns": int(max(0, time.perf_counter_ns() - map_started_ns)),
            "events_emitted": int(len(out)),
        }
        return out

    def stream_odds(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        emitted: list[StreamEnvelope] = []
        for event in self.stream_odds_fast(read_timeout_seconds=read_timeout_seconds):
            emitted.append(
                self._emit_event(
                    stream="odds",
                    payload_kind=str(event.action or "odds_update"),
                    uid=str(event.universal_id),
                    provider_timestamp=str(event.provider_timestamp),
                    raw=dict(event.raw_payload or {}),
                    event=event,
                )
            )
        return emitted

    def stream_scores_fast(self, *, read_timeout_seconds: float = 1.0) -> list[ScoreUpdateEvent]:
        ws = self._try_ensure_stream_ws(stream="scores", ensure=self._ensure_scores_ws)
        if ws is None:
            return []
        parsed_msg = self._recv_parsed(stream="scores", ws=ws, read_timeout_seconds=read_timeout_seconds)
        if parsed_msg is None:
            return []
        parsed, source_recv_monotonic_ns, base_timing = parsed_msg
        map_started_ns = time.perf_counter_ns()
        out: list[ScoreUpdateEvent] = []
        for payload in self._iter_payload_items(parsed):
            action = str(payload.get("action") or "")
            if action == "ping":
                continue
            uid = self._uid_from_payload(payload)
            if not uid:
                continue

            state = self._state_dict(payload)
            designation = payload.get("designation") if isinstance(payload.get("designation"), dict) else {}

            score_a = self._first_not_none(
                state.get("goalsA"),
                state.get("totalRunsForTeamA"),
                state.get("scoreA"),
                (state.get("runs") or {}).get("A") if isinstance(state.get("runs"), dict) else None,
            )
            score_b = self._first_not_none(
                state.get("goalsB"),
                state.get("totalRunsForTeamB"),
                state.get("scoreB"),
                (state.get("runs") or {}).get("B") if isinstance(state.get("runs"), dict) else None,
            )
            home_score, away_score = self._ab_to_home_away(score_a, score_b, designation)

            home_corners, away_corners = self._ab_to_home_away(state.get("cornersA"), state.get("cornersB"), designation)
            home_yellow, away_yellow = self._ab_to_home_away(state.get("yellowCardsA"), state.get("yellowCardsB"), designation)
            home_red, away_red = self._ab_to_home_away(state.get("redCardsA"), state.get("redCardsB"), designation)
            home_1h, away_1h = self._ab_to_home_away(state.get("firstHalfGoalsA"), state.get("firstHalfGoalsB"), designation)
            home_2h, away_2h = self._ab_to_home_away(state.get("secondHalfGoalsA"), state.get("secondHalfGoalsB"), designation)

            raw_period = self._first_not_none(state.get("period"), state.get("matchPeriod"))
            period = raw_period[1] if isinstance(raw_period, list) and len(raw_period) >= 2 else raw_period
            out.append(
                ScoreUpdateEvent(
                    provider="boltodds",
                    universal_id=str(uid),
                    action=str(action),
                    provider_timestamp=str(payload.get("timestamp") or ""),
                    game=str(payload.get("game") or payload.get("event") or ""),
                    home_team=str(payload.get("home") or ""),
                    away_team=str(payload.get("away") or ""),
                    period=str(period or ""),
                    elapsed_time_seconds=(None if state.get("elapsedTimeSeconds") is None else int(state.get("elapsedTimeSeconds"))),
                    pre_match=(None if state.get("preMatch") is None else bool(state.get("preMatch"))),
                    match_completed=(None if state.get("matchCompleted") is None else bool(state.get("matchCompleted"))),
                    clock_running_now=(None if state.get("clockRunningNow") is None else bool(state.get("clockRunningNow"))),
                    clock_running=(None if state.get("clockRunning") is None else bool(state.get("clockRunning"))),
                    home_score=home_score,
                    away_score=away_score,
                    home_corners=home_corners,
                    away_corners=away_corners,
                    home_yellow_cards=home_yellow,
                    away_yellow_cards=away_yellow,
                    home_red_cards=home_red,
                    away_red_cards=away_red,
                    home_first_half_goals=home_1h,
                    away_first_half_goals=away_1h,
                    home_second_half_goals=home_2h,
                    away_second_half_goals=away_2h,
                    var_referral_in_progress=(
                        None
                        if state.get("varReferralInProgress") is None
                        else bool(state.get("varReferralInProgress"))
                    ),
                    raw_payload=payload,
                    source_recv_monotonic_ns=int(source_recv_monotonic_ns),
                )
            )
        if out:
            self._mark_metric("scores", "events_emitted", len(out))
        self._last_stream_timing["scores"] = {
            "recv_wait_ns": int(base_timing.get("recv_wait_ns") or 0),
            "decode_ns": int(base_timing.get("decode_ns") or 0),
            "provider_map_ns": int(max(0, time.perf_counter_ns() - map_started_ns)),
            "events_emitted": int(len(out)),
        }
        return out

    def stream_scores(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        emitted: list[StreamEnvelope] = []
        for event in self.stream_scores_fast(read_timeout_seconds=read_timeout_seconds):
            emitted.append(
                self._emit_event(
                    stream="scores",
                    payload_kind=str(event.action or "score_update"),
                    uid=str(event.universal_id),
                    provider_timestamp=str(event.provider_timestamp),
                    raw=dict(event.raw_payload or {}),
                    event=event,
                )
            )
        return emitted

    def stream_playbyplay_fast(self, *, read_timeout_seconds: float = 1.0) -> list[PlayByPlayUpdateEvent]:
        ws = self._try_ensure_stream_ws(stream="playbyplay", ensure=self._ensure_playbyplay_ws)
        if ws is None:
            return []
        parsed_msg = self._recv_parsed(stream="playbyplay", ws=ws, read_timeout_seconds=read_timeout_seconds)
        if parsed_msg is None:
            return []
        parsed, source_recv_monotonic_ns, base_timing = parsed_msg
        map_started_ns = time.perf_counter_ns()
        out: list[PlayByPlayUpdateEvent] = []
        for payload in self._iter_payload_items(parsed):
            action = str(payload.get("action") or "")
            uid = self._uid_from_payload(payload)
            if not uid:
                continue
            play_info = payload.get("play_info")
            if not isinstance(play_info, dict):
                play_info = payload.get("playInfo") if isinstance(payload.get("playInfo"), dict) else {}
            score_value = payload.get("score")
            score = score_value if isinstance(score_value, dict) else None
            state_value = payload.get("state")
            if isinstance(state_value, dict):
                state = state_value
            elif isinstance(state_value, str):
                state = state_value
            else:
                state = {}
            stream_id_value = self._first_not_none(payload.get("stream_id"), payload.get("streamId"), payload.get("stream"))
            stream_id = "" if stream_id_value is None else str(stream_id_value)
            out.append(
                PlayByPlayUpdateEvent(
                    provider="boltodds",
                    universal_id=str(uid),
                    action=action,
                    stream_id=stream_id,
                    provider_timestamp=str(payload.get("timestamp") or ""),
                    game=self._game_from_payload(payload),
                    league=str(payload.get("league") or ""),
                    state=state,
                    play_info=play_info,
                    score=score,
                    raw_payload=payload,
                    source_recv_monotonic_ns=int(source_recv_monotonic_ns),
                )
            )
        if out:
            self._mark_metric("playbyplay", "events_emitted", len(out))
        self._last_stream_timing["playbyplay"] = {
            "recv_wait_ns": int(base_timing.get("recv_wait_ns") or 0),
            "decode_ns": int(base_timing.get("decode_ns") or 0),
            "provider_map_ns": int(max(0, time.perf_counter_ns() - map_started_ns)),
            "events_emitted": int(len(out)),
        }
        return out

    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        emitted: list[StreamEnvelope] = []
        for event in self.stream_playbyplay_fast(read_timeout_seconds=read_timeout_seconds):
            emitted.append(
                self._emit_event(
                    stream="playbyplay",
                    payload_kind=str(event.action or "playbyplay_update"),
                    uid=str(event.universal_id),
                    provider_timestamp=str(event.provider_timestamp),
                    raw=dict(event.raw_payload or {}),
                    event=event,
                )
            )
        return emitted

    def fetch_match_info(self, universal_ids: Sequence[str]) -> Any:
        resolved = self.resolve_universal_ids(universal_ids=universal_ids)
        game_labels: list[str] = []
        with self._catalog_lock:
            for uid in resolved:
                for game in sorted(self._uid_to_games.get(uid, set())):
                    if game not in game_labels:
                        game_labels.append(game)
        if not game_labels:
            return []
        return self._http_post_json("match_info", {"games": game_labels})


__all__ = ["BoltOddsProvider", "BoltOddsProviderConfig"]
