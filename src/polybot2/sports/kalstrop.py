"""Kalstrop provider implementation for catalog + scores/odds streaming."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
import re
import threading
import time
from typing import Any, Callable, Sequence
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx

try:  # pragma: no cover - optional dependency
    import orjson as _orjson
except Exception:  # pragma: no cover
    _orjson = None

from polybot2.linking.normalize import normalize_league_key, sport_key_for_league
from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import (
    OddsOutcome,
    OddsUpdateEvent,
    ProviderGameRecord,
    ScoreUpdateEvent,
    SportsProviderConfig,
    StreamEnvelope,
)
from polybot2.sports.recorder import NullRawFrameRecorder, NullRecorder, RawFrameRecorder, UpdateRecorder


class KalstropProviderConfig(SportsProviderConfig):
    """Runtime settings for Kalstrop integration."""

    def __init__(
        self,
        *,
        client_id: str,
        shared_secret_raw: str,
        http_base: str = "https://sportsapi.kalstropservice.com/odds_v1/v1",
        ws_url: str = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws",
        request_timeout_seconds: float = 20.0,
        reconnect_sleep_seconds: float = 0.2,
        queue_maxsize: int = 50_000,
        catalog_sport_codes: Sequence[str] = ("baseball", "soccer"),
        catalog_types: Sequence[str] = ("live", "popular"),
        catalog_first: int = 6,
        catalog_fixture_first: int = 3,
        catalog_max_outer_pages: int = 20,
        catalog_max_inner_pages: int = 20,
    ):
        super().__init__(
            provider_name="kalstrop",
            request_timeout_seconds=float(request_timeout_seconds),
            reconnect_sleep_seconds=float(reconnect_sleep_seconds),
            queue_maxsize=int(queue_maxsize),
        )
        self.client_id = str(client_id or "").strip()
        self.shared_secret_raw = str(shared_secret_raw or "").strip()
        self.http_base = str(http_base or "").rstrip("/")
        self.ws_url = str(ws_url or "").strip()
        self.catalog_sport_codes = tuple(
            sorted(
                {
                    str(x or "").strip().lower().replace("-", "_")
                    for x in (catalog_sport_codes or ())
                    if str(x or "").strip()
                }
            )
        )
        self.catalog_types = tuple(
            sorted(
                {
                    str(x or "").strip().lower()
                    for x in (catalog_types or ())
                    if str(x or "").strip().lower() in {"live", "upcoming"}
                }
            )
        )
        self.catalog_first = int(catalog_first)
        self.catalog_fixture_first = int(catalog_fixture_first)
        self.catalog_max_outer_pages = int(catalog_max_outer_pages)
        self.catalog_max_inner_pages = int(catalog_max_inner_pages)

        if not self.client_id:
            raise ValueError("client_id must be non-empty")
        if not self.shared_secret_raw:
            raise ValueError("shared_secret_raw must be non-empty")
        if not self.http_base:
            raise ValueError("http_base must be non-empty")
        if not self.ws_url:
            raise ValueError("ws_url must be non-empty")
        if not self.catalog_sport_codes:
            raise ValueError("catalog_sport_codes must be non-empty")
        if not self.catalog_types:
            raise ValueError("catalog_types must be non-empty")
        if self.catalog_first <= 0:
            raise ValueError("catalog_first must be > 0")
        if self.catalog_fixture_first <= 0:
            raise ValueError("catalog_fixture_first must be > 0")
        if self.catalog_max_outer_pages <= 0:
            raise ValueError("catalog_max_outer_pages must be > 0")
        if self.catalog_max_inner_pages <= 0:
            raise ValueError("catalog_max_inner_pages must be > 0")


class KalstropProvider(SportsDataProviderBase):
    _et_tz = ZoneInfo("America/New_York")
    _inning_re = re.compile(r"(?i)(\d+)(?:st|nd|rd|th)?\s*inning")

    def __init__(
        self,
        *,
        config: KalstropProviderConfig,
        recorder: UpdateRecorder | None = None,
        raw_frame_recorder: RawFrameRecorder | None = None,
        http_client: httpx.Client | None = None,
        ws_factory: Any | None = None,
    ):
        super().__init__(
            config=SportsProviderConfig(
                provider_name="kalstrop",
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
        self._scores_sub_id = "kal_scores_sub"
        self._odds_sub_id = "kal_odds_sub"
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
                "last_error": "unsupported_v1",
            },
        }
        self._last_stream_timing: dict[str, dict[str, int]] = {
            "odds": {},
            "scores": {},
            "playbyplay": {},
        }

    @property
    def config(self) -> KalstropProviderConfig:
        return self._cfg

    @staticmethod
    def build_signature(*, client_id: str, shared_secret_raw: str, timestamp: str) -> str:
        hashed_secret = hashlib.sha256(str(shared_secret_raw).encode("utf-8")).hexdigest()
        payload = f"{client_id}:{timestamp}".encode("utf-8")
        return hmac.new(hashed_secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()

    def _auth_headers(self, *, timestamp: str | None = None) -> dict[str, str]:
        ts = str(timestamp or int(time.time()))
        signature = self.build_signature(
            client_id=self._cfg.client_id,
            shared_secret_raw=self._cfg.shared_secret_raw,
            timestamp=ts,
        )
        return {
            "X-Client-ID": self._cfg.client_id,
            "X-Timestamp": ts,
            "Authorization": f"Bearer {signature}",
        }

    def _auth_ws_query(self, *, timestamp: str | None = None) -> str:
        ts = str(timestamp or int(time.time()))
        signature = self.build_signature(
            client_id=self._cfg.client_id,
            shared_secret_raw=self._cfg.shared_secret_raw,
            timestamp=ts,
        )
        return urlencode(
            {
                "X-Client-ID": self._cfg.client_id,
                "X-Timestamp": ts,
                "Authorization": f"Bearer {signature}",
            }
        )

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

    def _create_ws(self, *, ws_url: str, timeout_seconds: float) -> Any:
        if self._ws_factory is not None:
            return self._ws_factory(ws_url=ws_url, timeout_seconds=float(timeout_seconds))
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

    def _ws_uri(self) -> str:
        query = self._auth_ws_query()
        return f"{self._cfg.ws_url}?{query}"

    def _http_get_json(self, endpoint: str, *, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._cfg.http_base}/{str(endpoint).lstrip('/')}"
        headers = self._auth_headers()
        query_params = dict(params or {})
        has_cursor = bool(str(query_params.get("cursor") or "").strip())
        # Kalstrop requests can intermittently return 400 TOO_MANY_REQUEST.
        # Retry with backoff before surfacing an error.
        for attempt in range(6):
            response = self._client.get(url, params=query_params, headers=headers)
            try:
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError:
                status = int(response.status_code)
                body_lc = str(response.text or "").lower()
                is_rate_limited = status == 429 or (status == 400 and "too_many_request" in body_lc)
                if is_rate_limited and attempt < 5:
                    time.sleep(min(1.0, 0.1 * (2**attempt)))
                    continue
                if has_cursor and status == 400 and attempt < 2:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise

    @staticmethod
    def _normalize_game_label(value: str) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _extract_universal_id(row: dict[str, Any]) -> str:
        for key in ("fixtureId", "fixture_id", "id", "universal_id", "uid"):
            value = row.get(key)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _extract_game_label(row: dict[str, Any]) -> str:
        for key in ("name", "game", "event", "shortName"):
            value = row.get(key)
            text = str(value or "").strip()
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
            return None

    @staticmethod
    def _parse_teams_from_label(label: str) -> tuple[str, str]:
        text = str(label or "").strip()
        if not text:
            return ("", "")
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
    def _clean_league_name(*, league_name: str, league_slug: str) -> str:
        name = " ".join(str(league_name or "").strip().lower().split())
        if "," in name:
            name = name.split(",", 1)[0].strip()
        if name:
            return name
        slug = str(league_slug or "").strip().lower()
        slug = re.sub(r"^\d+-", "", slug)
        return " ".join(slug.replace("-", " ").split())

    @staticmethod
    def _normalize_sport_and_league(*, sport_raw: str, league_raw: str, league_slug: str) -> tuple[str, str]:
        sport_guess = " ".join(str(sport_raw or "").strip().lower().replace("-", " ").split())
        league_guess = KalstropProvider._clean_league_name(league_name=league_raw, league_slug=league_slug)
        league_key = normalize_league_key(league_guess) if league_guess else ""
        if not league_key:
            league_key = normalize_league_key(str(league_slug or "").replace("-", " "))
        sport_key = sport_key_for_league(league_key)
        if not sport_key:
            direct_map = {
                "soccer": "soccer",
                "football": "soccer",
                "basketball": "basketball",
                "baseball": "baseball",
                "ice hockey": "hockey",
                "hockey": "hockey",
                "american football": "american_football",
                "tennis": "tennis",
                "mma": "mma",
                "boxing": "boxing",
            }
            sport_key = direct_map.get(sport_guess, sport_guess.replace(" ", "_"))
        return (str(sport_key or ""), str(league_key or ""))

    def _build_provider_record_from_row(self, row: dict[str, Any]) -> ProviderGameRecord | None:
        uid = str(self._extract_universal_id(row))
        if not uid:
            return None
        game_label = self._extract_game_label(row)
        orig_teams = str(row.get("shortName") or game_label or "").strip()
        category = row.get("category") if isinstance(row.get("category"), dict) else {}
        competition = row.get("competition") if isinstance(row.get("competition"), dict) else {}
        sport_raw = str(category.get("sports") or row.get("sport") or "").strip()
        league_name = str(competition.get("name") or row.get("competition_name") or "").strip()
        league_slug = str(competition.get("slug") or row.get("competition_slug") or "").strip()
        league_raw = self._clean_league_name(league_name=league_name, league_slug=league_slug)
        when_raw = str(row.get("startTime") or row.get("start_time") or "").strip()
        parse_reason_parts: list[str] = []

        competitors = row.get("competitors")
        home_raw = ""
        away_raw = ""
        if isinstance(competitors, list):
            for comp in competitors:
                if not isinstance(comp, dict):
                    continue
                team_name = str(comp.get("displayName") or comp.get("name") or "").strip()
                if not team_name:
                    continue
                is_home = comp.get("isHome")
                if is_home is True:
                    home_raw = team_name
                elif is_home is False:
                    away_raw = team_name
        if not home_raw or not away_raw:
            parsed_home, parsed_away = self._parse_teams_from_label(game_label or orig_teams)
            if parsed_home and parsed_away:
                if not home_raw:
                    home_raw = parsed_home
                if not away_raw:
                    away_raw = parsed_away
        if not home_raw or not away_raw:
            parse_reason_parts.append("missing_competitors_home_away")

        start_ts_utc = self._parse_start_ts(when_raw)
        if start_ts_utc is None:
            parse_reason_parts.append("missing_or_invalid_start_time")

        sport_key, league_key = self._normalize_sport_and_league(
            sport_raw=sport_raw,
            league_raw=league_raw,
            league_slug=league_slug,
        )
        if not sport_key:
            parse_reason_parts.append("missing_sport_key")
        if not league_key:
            parse_reason_parts.append("missing_league_key")

        alias_values: set[str] = set()
        if game_label:
            alias_values.add(game_label)
        if orig_teams:
            alias_values.add(orig_teams)
        fixture_slug = str(row.get("slug") or "").strip()
        if fixture_slug:
            alias_values.add(fixture_slug)

        parse_status = "ok" if not parse_reason_parts else "partial"
        return ProviderGameRecord(
            provider="kalstrop",
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

    def _fetch_sports_page(
        self,
        *,
        sport_code: str,
        feed_type: str,
        cursor: str = "",
    ) -> dict[str, Any]:
        endpoint = f"sports/{sport_code}/{feed_type}"
        first_value = int(self._cfg.catalog_first)
        fixture_first_value = int(self._cfg.catalog_fixture_first)
        normalized_feed = str(feed_type or "").strip().lower()

        # `/sports/{sport}/upcoming` can reject `fixtureFirst` for some providers/sports.
        # Keep a fallback matrix so subscription resolution does not fail on this parameter.
        variants: list[dict[str, Any]] = []
        if normalized_feed == "upcoming":
            variants.append({"first": first_value})
            variants.append({"first": first_value, "fixtureFirst": fixture_first_value})
        else:
            variants.append({"first": first_value, "fixtureFirst": fixture_first_value})
            variants.append({"first": first_value})
        if cursor:
            for params in variants:
                params["cursor"] = str(cursor)
            variants.append({"cursor": str(cursor), "first": first_value})
            variants.append({"cursor": str(cursor)})

        deduped: list[dict[str, Any]] = []
        seen_keys: set[tuple[tuple[str, str], ...]] = set()
        for params in variants:
            key = tuple(sorted((str(k), str(v)) for k, v in params.items()))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(params)

        last_http_error: httpx.HTTPStatusError | None = None
        for idx, params in enumerate(deduped):
            try:
                payload = self._http_get_json(endpoint, params=params)
                return payload if isinstance(payload, dict) else {}
            except httpx.HTTPStatusError as exc:
                last_http_error = exc
                status = int(exc.response.status_code) if exc.response is not None else 0
                should_retry_alt = status == 400 and idx < (len(deduped) - 1)
                if should_retry_alt:
                    continue
                break

        if cursor and last_http_error is not None:
            status = int(last_http_error.response.status_code) if last_http_error.response is not None else 0
            if status == 400:
                # Cursor pagination can be unstable; fail-soft for this page only.
                return {}

        if last_http_error is not None:
            raise last_http_error
        return {}

    def _fetch_competition_fixtures_page(self, *, competition_slug: str, cursor: str) -> tuple[list[dict[str, Any]], str]:
        params: dict[str, Any] = {"first": int(self._cfg.catalog_fixture_first)}
        if cursor:
            params["cursor"] = str(cursor)
        try:
            payload = self._http_get_json(f"competition/{competition_slug}/fixtures", params=params)
        except httpx.HTTPStatusError as exc:
            status = int(exc.response.status_code) if exc.response is not None else 0
            if cursor and status == 400:
                return ([], "")
            raise
        if isinstance(payload, dict):
            fixtures_obj = payload.get("fixtures") if isinstance(payload.get("fixtures"), dict) else payload
            nodes = fixtures_obj.get("nodes") if isinstance(fixtures_obj.get("nodes"), list) else []
            next_cursor = str(fixtures_obj.get("nextCursor") or "")
            out = [dict(x) for x in nodes if isinstance(x, dict)]
            return (out, next_cursor)
        return ([], "")

    def load_game_catalog(self) -> list[ProviderGameRecord]:
        grouped: dict[str, list[ProviderGameRecord]] = {}

        def _append_fixture_row(
            *,
            fixture: dict[str, Any],
            sport_code: str,
            comp_name: str = "",
            comp_slug: str = "",
            comp_category: dict[str, Any] | None = None,
        ) -> None:
            fixture_row = dict(fixture)
            if comp_name or comp_slug:
                fixture_row["competition"] = {
                    "name": comp_name,
                    "slug": comp_slug,
                }
            elif not isinstance(fixture_row.get("competition"), dict):
                fixture_row["competition"] = {"name": "", "slug": ""}
            if isinstance(comp_category, dict):
                fixture_row["category"] = dict(comp_category)
            elif not isinstance(fixture_row.get("category"), dict):
                competition_obj = fixture_row.get("competition") if isinstance(fixture_row.get("competition"), dict) else {}
                category_obj = (
                    competition_obj.get("category")
                    if isinstance(competition_obj.get("category"), dict)
                    else {}
                )
                fixture_row["category"] = dict(category_obj) if isinstance(category_obj, dict) else {}
            fixture_row["sport"] = sport_code
            record = self._build_provider_record_from_row(fixture_row)
            if record is not None:
                grouped.setdefault(str(record.provider_game_id), []).append(record)

        for sport_code in self._cfg.catalog_sport_codes:
            for feed_type in self._cfg.catalog_types:
                cursor = ""
                outer_page = 0
                while outer_page < self._cfg.catalog_max_outer_pages:
                    outer_page += 1
                    try:
                        payload = self._fetch_sports_page(
                            sport_code=sport_code,
                            feed_type=feed_type,
                            cursor=cursor,
                        )
                    except httpx.HTTPStatusError as exc:
                        status = int(exc.response.status_code) if exc.response is not None else 0
                        detail = (
                            str(exc.response.text or "").strip().lower()
                            if exc.response is not None
                            else ""
                        )
                        unsupported = (
                            status == 404
                            or (
                                status == 400
                                and (
                                    "sport code not found" in detail
                                    or "unsupported" in detail
                                    or "invalid sport" in detail
                                    or "unknown argument" in detail
                                )
                            )
                        )
                        # Unsupported sport/feed branches should not abort full catalog sync.
                        if not cursor and unsupported:
                            break
                        raise
                    sc = payload.get("sportsCompetitions") if isinstance(payload.get("sportsCompetitions"), dict) else None
                    sf = payload.get("sportsFixtures") if isinstance(payload.get("sportsFixtures"), dict) else None

                    if isinstance(sc, dict):
                        comps = sc.get("nodes") if isinstance(sc.get("nodes"), list) else []
                        for comp in comps:
                            if not isinstance(comp, dict):
                                continue
                            comp_slug = str(comp.get("slug") or "").strip()
                            category = comp.get("category") if isinstance(comp.get("category"), dict) else {}
                            fixtures_obj = comp.get("fixtures") if isinstance(comp.get("fixtures"), dict) else {}
                            fixtures = fixtures_obj.get("nodes") if isinstance(fixtures_obj.get("nodes"), list) else []
                            inner_next = str(fixtures_obj.get("nextCursor") or "")
                            for fixture in fixtures:
                                if not isinstance(fixture, dict):
                                    continue
                                _append_fixture_row(
                                    fixture=fixture,
                                    sport_code=sport_code,
                                    comp_name=str(comp.get("name") or ""),
                                    comp_slug=comp_slug,
                                    comp_category=category,
                                )

                            inner_page = 0
                            while comp_slug and inner_next and inner_page < self._cfg.catalog_max_inner_pages:
                                inner_page += 1
                                extra, inner_next = self._fetch_competition_fixtures_page(
                                    competition_slug=comp_slug,
                                    cursor=inner_next,
                                )
                                for fixture in extra:
                                    _append_fixture_row(
                                        fixture=fixture,
                                        sport_code=sport_code,
                                        comp_name=str(comp.get("name") or ""),
                                        comp_slug=comp_slug,
                                        comp_category=category,
                                    )
                        cursor = str(sc.get("nextCursor") or "")
                    elif isinstance(sf, dict):
                        fixtures = sf.get("nodes") if isinstance(sf.get("nodes"), list) else []
                        for fixture in fixtures:
                            if not isinstance(fixture, dict):
                                continue
                            _append_fixture_row(fixture=fixture, sport_code=sport_code)
                        cursor = str(sf.get("nextCursor") or "")
                    else:
                        cursor = ""
                    if not cursor:
                        break

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

    def _get_provider_record(self, provider_game_id: str) -> ProviderGameRecord | None:
        with self._catalog_lock:
            return self._catalog_by_uid.get(str(provider_game_id))

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
            return (KalstropProvider._decode_ws_frame(raw), "")
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

    def _uid_from_payload(self, payload: dict[str, Any]) -> str:
        uid = self._extract_universal_id(payload)
        if uid:
            return uid
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        uid = self._extract_universal_id(data)
        if uid:
            return uid
        game = str(data.get("name") or payload.get("name") or payload.get("game") or "").strip()
        if not game:
            return ""
        with self._catalog_lock:
            uid = self._game_to_uid.get(game)
            if not uid:
                uid = self._game_to_uid_normalized.get(self._normalize_game_label(game))
            return str(uid or "")

    def _game_from_payload(self, payload: dict[str, Any]) -> str:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        return str(data.get("name") or payload.get("name") or payload.get("game") or "").strip()

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
                provider="kalstrop",
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

    def _send_subscribe(self, *, ws: Any, stream: str, sub_id: str, operation_name: str, query: str, variables: dict[str, Any]) -> None:
        payload = {
            "id": str(sub_id),
            "type": "subscribe",
            "payload": {
                "operationName": str(operation_name),
                "query": str(query),
                "variables": dict(variables),
            },
        }
        ws.send(json.dumps(payload, separators=(",", ":"), default=str))

    def _send_complete(self, *, ws: Any, sub_id: str) -> None:
        payload = {"id": str(sub_id), "type": "complete"}
        ws.send(json.dumps(payload, separators=(",", ":"), default=str))

    def _send_init(self, *, ws: Any) -> None:
        payload = {"type": "connection_init", "payload": {}}
        ws.send(json.dumps(payload, separators=(",", ":"), default=str))

    def _send_pong(self, *, ws: Any) -> None:
        try:
            ws.send(json.dumps({"type": "pong"}, separators=(",", ":"), default=str))
        except Exception:
            pass

    def _ensure_scores_ws(self) -> Any:
        if self._scores_ws is not None:
            return self._scores_ws
        self._mark_metric("scores", "connect_attempts", 1)
        ws = self._create_ws(ws_url=self._ws_uri(), timeout_seconds=8.0)
        self._scores_ws = ws
        self._mark_metric("scores", "connect_successes", 1)
        if int(self._metrics["scores"]["connect_successes"]) > 1:
            self._mark_metric("scores", "reconnects", 1)
        try:
            self._send_init(ws=ws)
            ws.settimeout(0.5)
            ack = ws.recv()
            parsed_ack, parse_error = self._try_decode_ws_frame(ack)
            self._record_raw_frame(stream="scores", raw_frame=ack, parsed_frame=parsed_ack, parse_error=parse_error)
        except Exception:
            pass
        if self._subscribed_scores_uids:
            self._send_subscribe(
                ws=ws,
                stream="scores",
                sub_id=self._scores_sub_id,
                operation_name="sportsMatchStateUpdatedV2",
                query=(
                    "subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!) "
                    "{ sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }"
                ),
                variables={"fixtureIds": sorted(self._subscribed_scores_uids)},
            )
        return ws

    def _ensure_odds_ws(self) -> Any:
        if self._odds_ws is not None:
            return self._odds_ws
        self._mark_metric("odds", "connect_attempts", 1)
        ws = self._create_ws(ws_url=self._ws_uri(), timeout_seconds=8.0)
        self._odds_ws = ws
        self._mark_metric("odds", "connect_successes", 1)
        if int(self._metrics["odds"]["connect_successes"]) > 1:
            self._mark_metric("odds", "reconnects", 1)
        try:
            self._send_init(ws=ws)
            ws.settimeout(0.5)
            ack = ws.recv()
            parsed_ack, parse_error = self._try_decode_ws_frame(ack)
            self._record_raw_frame(stream="odds", raw_frame=ack, parsed_frame=parsed_ack, parse_error=parse_error)
        except Exception:
            pass
        if self._subscribed_odds_uids:
            self._send_subscribe(
                ws=ws,
                stream="odds",
                sub_id=self._odds_sub_id,
                operation_name="sportsMatchOddsUpdated",
                query=(
                    "subscription sportsMatchOddsUpdated($fixtureIds: [String!]) "
                    "{ sportsMatchOddsUpdated(fixtureIds: $fixtureIds) }"
                ),
                variables={"fixtureIds": sorted(self._subscribed_odds_uids)},
            )
        return ws

    def _close_scores_ws(self) -> None:
        ws = self._scores_ws
        self._scores_ws = None
        if ws is None:
            return
        try:
            self._send_complete(ws=ws, sub_id=self._scores_sub_id)
        except Exception:
            pass
        try:
            ws.close()
        except Exception:
            pass

    def _close_odds_ws(self) -> None:
        ws = self._odds_ws
        self._odds_ws = None
        if ws is None:
            return
        try:
            self._send_complete(ws=ws, sub_id=self._odds_sub_id)
        except Exception:
            pass
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
        self._close_scores_ws()
        self._close_odds_ws()
        try:
            self._raw_frame_recorder.close()
        except Exception:
            pass

    def subscribe_scores(self, universal_ids: Sequence[str]) -> None:
        resolved = self.resolve_universal_ids(universal_ids=universal_ids)
        self._subscribed_scores_uids = set(resolved)
        if self._scores_ws is not None:
            try:
                self._send_complete(ws=self._scores_ws, sub_id=self._scores_sub_id)
            except Exception:
                pass
            if resolved:
                self._send_subscribe(
                    ws=self._scores_ws,
                    stream="scores",
                    sub_id=self._scores_sub_id,
                    operation_name="sportsMatchStateUpdatedV2",
                    query=(
                        "subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!) "
                        "{ sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }"
                    ),
                    variables={"fixtureIds": sorted(resolved)},
                )

    def recv_raw_score_frame(self, timeout: float = 1.0) -> str | None:
        """Read one raw text frame from the scores WebSocket.

        Returns the raw JSON string exactly as Kalstrop sent it, or None on
        timeout. No parsing, no dedup, no envelope — used by the capture
        command to record raw provider data to disk.
        """
        ws = self._try_ensure_stream_ws(stream="scores", ensure=self._ensure_scores_ws)
        if ws is None:
            return None
        try:
            ws.settimeout(max(0.01, float(timeout)))
        except Exception:
            pass
        try:
            data = ws.recv()
            if isinstance(data, bytes):
                data = data.decode("utf-8", errors="replace")
            return data
        except Exception as exc:
            if self._is_timeout_exc(exc):
                return None
            self._close_scores_ws()
            return None

    def subscribe_odds(self, universal_ids: Sequence[str]) -> None:
        resolved = self.resolve_universal_ids(universal_ids=universal_ids)
        self._subscribed_odds_uids = set(resolved)
        if self._odds_ws is not None:
            try:
                self._send_complete(ws=self._odds_ws, sub_id=self._odds_sub_id)
            except Exception:
                pass
            if resolved:
                self._send_subscribe(
                    ws=self._odds_ws,
                    stream="odds",
                    sub_id=self._odds_sub_id,
                    operation_name="sportsMatchOddsUpdated",
                    query=(
                        "subscription sportsMatchOddsUpdated($fixtureIds: [String!]) "
                        "{ sportsMatchOddsUpdated(fixtureIds: $fixtureIds) }"
                    ),
                    variables={"fixtureIds": sorted(resolved)},
                )

    def subscribe_playbyplay(self, universal_ids: Sequence[str]) -> None:
        # v1 decision: Kalstrop play-by-play is intentionally unsupported.
        self._subscribed_playbyplay_uids = set(self.resolve_universal_ids(universal_ids=universal_ids))
        self._set_metric("playbyplay", "last_error", "unsupported_v1")

    def _close_stream_ws(self, stream: str) -> None:
        if stream == "scores":
            self._close_scores_ws()
            return
        if stream == "odds":
            self._close_odds_ws()
            return

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
            provider="kalstrop",
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

    @staticmethod
    def _to_int_or_none(value: Any) -> int | None:
        try:
            if value is None:
                return None
            return int(float(str(value)))
        except Exception:
            return None

    @staticmethod
    def _score_completed_from_free_text(free_text: str) -> bool:
        """Detect game completion from matchStatusDisplay freeText.
        Kalstrop sends 'Ended' when a game finishes."""
        s = str(free_text or "").strip().lower()
        return s in {"ended", "final", "game over", "finished", "ft"}

    @staticmethod
    def _extract_free_text(match_summary: dict[str, Any]) -> str:
        display = match_summary.get("matchStatusDisplay")
        if isinstance(display, list):
            for item in display:
                if isinstance(item, dict):
                    text = str(item.get("freeText") or "").strip()
                    if text:
                        return text
        return ""

    @classmethod
    def _parse_inning_text(cls, text: str) -> tuple[int | None, str]:
        src = str(text or "").strip().lower()
        if not src:
            return (None, "")
        inning_num: int | None = None
        m = cls._inning_re.search(src)
        if m:
            inning_num = cls._to_int_or_none(m.group(1))
        half = ""
        if "top" in src:
            half = "top"
        elif "bottom" in src or "bot" in src:
            half = "bottom"
        elif "end" in src:
            half = "end"
        return (inning_num, half)

    @classmethod
    def _hotpath_baseball_fields(
        cls,
        *,
        match_summary: dict[str, Any],
        period_text: str,
        match_completed: bool | None,
    ) -> dict[str, Any]:
        inning_number: int | None = None
        inning_half = ""
        texts: list[str] = [str(period_text or "")]
        display = match_summary.get("matchStatusDisplay")
        if isinstance(display, list):
            for item in display:
                if isinstance(item, dict):
                    texts.append(str(item.get("freeText") or ""))
        phases = match_summary.get("phases")
        if isinstance(phases, list):
            for item in phases:
                if isinstance(item, dict):
                    texts.append(str(item.get("phaseText") or ""))
        for text in texts:
            num, half = cls._parse_inning_text(text)
            if inning_number is None and num is not None:
                inning_number = num
            if not inning_half and half:
                inning_half = half
            if inning_number is not None and inning_half:
                break
        return {
            "inning_number": inning_number,
            "inning_half": str(inning_half or ""),
            "outs": None,
            "balls": None,
            "strikes": None,
            "runner_on_first": None,
            "runner_on_second": None,
            "runner_on_third": None,
            "match_completed": match_completed,
        }

    def _extract_odds_outcomes(self, payload: dict[str, Any]) -> tuple[OddsOutcome, ...]:
        outcomes: list[OddsOutcome] = []

        def _emit_from_items(items: list[Any], *, context_prefix: str = "") -> None:
            for idx, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                outcome_key = str(
                    item.get("id")
                    or item.get("key")
                    or item.get("outcomeId")
                    or item.get("selectionId")
                    or item.get("name")
                    or item.get("label")
                    or f"{context_prefix}{idx}"
                )
                price = item.get("odds")
                if price is None:
                    price = item.get("price")
                if price is None:
                    price = item.get("decimalOdds")
                if price is None:
                    price = item.get("americanOdds")
                outcomes.append(
                    OddsOutcome(
                        outcome_key=outcome_key,
                        odds=price,
                        outcome_name=str(item.get("name") or item.get("label") or item.get("selectionName") or ""),
                        outcome_target=str(item.get("line") or item.get("points") or item.get("target") or ""),
                        outcome_line=str(item.get("line") or item.get("points") or ""),
                        outcome_over_under=str(item.get("overUnder") or item.get("side") or ""),
                        outcome_link="",
                    )
                )

        direct = payload.get("odds")
        if isinstance(direct, list):
            _emit_from_items(direct)

        outcomes_obj = payload.get("outcomes")
        if isinstance(outcomes_obj, list):
            _emit_from_items(outcomes_obj)
        elif isinstance(outcomes_obj, dict):
            _emit_from_items(list(outcomes_obj.values()))

        markets = payload.get("markets")
        if isinstance(markets, list):
            for m_idx, market in enumerate(markets):
                if not isinstance(market, dict):
                    continue
                market_odds = market.get("odds")
                if isinstance(market_odds, list):
                    _emit_from_items(market_odds, context_prefix=f"m{m_idx}_")

        return tuple(outcomes)

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
        for msg in self._iter_payload_items(parsed):
            mtype = str(msg.get("type") or "").strip().lower()
            if mtype == "ping":
                self._send_pong(ws=ws)
                continue
            if mtype != "next":
                continue
            payload = msg.get("payload") if isinstance(msg.get("payload"), dict) else {}
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            row = data.get("sportsMatchStateUpdatedV2")
            if not isinstance(row, dict):
                continue
            uid = str(row.get("fixtureId") or row.get("fixture_id") or "").strip()
            if not uid:
                uid = self._uid_from_payload(row)
            if not uid:
                continue
            rec = self._get_provider_record(uid)
            match_summary = row.get("matchSummary") if isinstance(row.get("matchSummary"), dict) else {}
            elapsed_ms = self._to_int_or_none(match_summary.get("timeElapsed"))
            period_text = self._extract_free_text(match_summary)
            match_completed = self._score_completed_from_free_text(period_text)
            row_payload = dict(row)
            row_payload["_hotpath_baseball"] = self._hotpath_baseball_fields(
                match_summary=match_summary,
                period_text=period_text,
                match_completed=match_completed,
            )
            out.append(
                ScoreUpdateEvent(
                    provider="kalstrop",
                    universal_id=uid,
                    action="sportsMatchStateUpdatedV2",
                    provider_timestamp=str(match_summary.get("updatedAt") or row.get("updatedAt") or ""),
                    game=str((rec.game_label if rec else "") or row.get("name") or ""),
                    home_team=str((rec.home_team_raw if rec else "") or ""),
                    away_team=str((rec.away_team_raw if rec else "") or ""),
                    period=period_text,
                    elapsed_time_seconds=(None if elapsed_ms is None else int(elapsed_ms / 1000)),
                    pre_match=None,  # Prematch games don't produce WS frames
                    match_completed=match_completed,
                    clock_running=(None if match_summary.get("clockRunning") is None else bool(match_summary.get("clockRunning"))),
                    home_score=self._to_int_or_none(match_summary.get("homeScore")),
                    away_score=self._to_int_or_none(match_summary.get("awayScore")),
                    raw_payload=row_payload,
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

    def stream_scores_frame_fast(
        self, *, read_timeout_seconds: float = 1.0
    ) -> tuple[Any, int] | None:
        """Return decoded score stream frame for native hotpath batch processing."""
        ws = self._try_ensure_stream_ws(stream="scores", ensure=self._ensure_scores_ws)
        if ws is None:
            return None
        parsed_msg = self._recv_parsed(stream="scores", ws=ws, read_timeout_seconds=read_timeout_seconds)
        if parsed_msg is None:
            return None
        parsed, source_recv_monotonic_ns, base_timing = parsed_msg
        self._last_stream_timing["scores"] = {
            "recv_wait_ns": int(base_timing.get("recv_wait_ns") or 0),
            "decode_ns": int(base_timing.get("decode_ns") or 0),
            "provider_map_ns": 0,
            "events_emitted": 0,
        }
        return parsed, int(source_recv_monotonic_ns)

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
        for msg in self._iter_payload_items(parsed):
            mtype = str(msg.get("type") or "").strip().lower()
            if mtype == "ping":
                self._send_pong(ws=ws)
                continue
            if mtype != "next":
                continue
            payload = msg.get("payload") if isinstance(msg.get("payload"), dict) else {}
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            row = data.get("sportsMatchOddsUpdated")
            if row is None:
                row = data.get("sportsMatchDefaultMarketsOddsUpdated")
            rows: list[dict[str, Any]] = []
            if isinstance(row, dict):
                rows = [row]
            elif isinstance(row, list):
                rows = [x for x in row if isinstance(x, dict)]
            for node in rows:
                uid = str(node.get("fixtureId") or node.get("fixture_id") or "").strip()
                if not uid:
                    uid = self._uid_from_payload(node)
                if not uid:
                    continue
                rec = self._get_provider_record(uid)
                outcomes = self._extract_odds_outcomes(node)
                out.append(
                    OddsUpdateEvent(
                        provider="kalstrop",
                        universal_id=uid,
                        action="sportsMatchOddsUpdated",
                        provider_timestamp=str(node.get("updatedAt") or ""),
                        game=str((rec.game_label if rec else "") or node.get("name") or ""),
                        sport=str((rec.sport_raw if rec else "") or ""),
                        league=str((rec.league_raw if rec else "") or ""),
                        home_team=str((rec.home_team_raw if rec else "") or ""),
                        away_team=str((rec.away_team_raw if rec else "") or ""),
                        book_event_id=uid,
                        outcomes=outcomes,
                        raw_payload=dict(node),
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

    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        del read_timeout_seconds
        self._mark_metric("playbyplay", "recv_calls", 1)
        self._set_metric("playbyplay", "last_error", "unsupported_v1")
        return []


__all__ = ["KalstropProvider", "KalstropProviderConfig"]
