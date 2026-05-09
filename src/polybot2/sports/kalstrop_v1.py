"""Kalstrop V1 provider implementation for catalog sync."""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
import re
import threading
import time
from typing import Any, Sequence
import httpx

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import (
    ProviderGameRecord,
    SportsProviderConfig,
)


class KalstropV1ProviderConfig(SportsProviderConfig):
    """Runtime settings for Kalstrop integration."""

    def __init__(
        self,
        *,
        client_id: str,
        shared_secret_raw: str,
        http_base: str = "https://sportsapi.kalstropservice.com/odds_v1/v1",
        request_timeout_seconds: float = 20.0,
        catalog_sport_codes: Sequence[str] = ("baseball", "soccer"),
        catalog_types: Sequence[str] = ("live", "upcoming", "popular"),
        catalog_first: int = 10,
        catalog_fixture_first: int = 50,
        catalog_max_outer_pages: int = 20,
        catalog_max_inner_pages: int = 20,
    ):
        super().__init__(
            provider_name="kalstrop_v1",
            request_timeout_seconds=float(request_timeout_seconds),
        )
        self.client_id = str(client_id or "").strip()
        self.shared_secret_raw = str(shared_secret_raw or "").strip()
        self.http_base = str(http_base or "").rstrip("/")
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
                    if str(x or "").strip().lower() in {"live", "upcoming", "popular"}
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


class KalstropV1Provider(SportsDataProviderBase):

    def __init__(
        self,
        *,
        config: KalstropV1ProviderConfig,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(
            config=SportsProviderConfig(
                provider_name="kalstrop_v1",
                request_timeout_seconds=float(config.request_timeout_seconds),
            ),
            http_client=http_client,
        )
        self._cfg = config

        self._catalog_lock = threading.RLock()
        self._catalog_by_uid: dict[str, ProviderGameRecord] = {}
        self._game_to_uid: dict[str, str] = {}
        self._game_to_uid_normalized: dict[str, str] = {}

    @property
    def config(self) -> KalstropV1ProviderConfig:
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

        category_name = str(category.get("name") or "").strip()
        category_country_code = str(category.get("countryCode") or "").strip()

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
            provider="kalstrop_v1",
            provider_game_id=uid,
            game_label=game_label,
            orig_teams=orig_teams,
            sport_raw=sport_raw,
            league_raw=league_raw,
            when_raw=when_raw,
            home_team_raw=home_raw,
            away_team_raw=away_raw,
            category_name=category_name,
            category_country_code=category_country_code,
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
        if record.orig_teams:
            score += 2
        if record.game_label:
            score += 1
        if record.parse_status == "ok":
            score += 1
        return score

    _FEED_FIRST_LIMITS = {"live": 10, "upcoming": 30, "popular": 10}

    def _fetch_sports_page(
        self,
        *,
        sport_code: str,
        feed_type: str,
        cursor: str = "",
    ) -> dict[str, Any]:
        endpoint = f"sports/{sport_code}/{feed_type}"
        normalized_feed = str(feed_type or "").strip().lower()
        first_value = self._FEED_FIRST_LIMITS.get(normalized_feed, int(self._cfg.catalog_first))
        fixture_first_value = int(self._cfg.catalog_fixture_first)

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
                    category_name=selected.category_name,
                    category_country_code=selected.category_country_code,
                    start_ts_utc=selected.start_ts_utc,
                    parse_status=selected.parse_status,
                    parse_reason=selected.parse_reason,
                    aliases=tuple(sorted(all_aliases)),
                    raw_payload=dict(selected.raw_payload),
                )
                self._catalog_by_uid[uid] = merged
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

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass


__all__ = [
    "KalstropV1Provider",
    "KalstropV1ProviderConfig",
]
