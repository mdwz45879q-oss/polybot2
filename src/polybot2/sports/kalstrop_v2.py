"""Kalstrop V2 (BetGenius) provider — catalog discovery via REST.

V2 covers top-tier soccer leagues (EPL, La Liga, Bundesliga, UCL, etc.).
Live streaming (Socket.IO) is not yet implemented.

Catalog discovery follows the V2 API hierarchy:
  Step 1: GET /sports → sport slugs
  Step 2: GET /sports/{sport}/competitions → categories + tournaments
  Step 3: GET /sports/{sport}/competitions/{category}/{tournament}/fixtures → fixtures
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

import httpx

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import ProviderGameRecord, SportsProviderConfig


class KalstropV2SyncError(Exception):
    """Raised when V2 catalog discovery fails at the root level."""


class KalstropV2ProviderConfig:

    def __init__(
        self,
        *,
        api_base: str = "https://stats.kalstropservice.com/api/v2",
        catalog_sport_slugs: Sequence[str] = ("football",),
        request_timeout_seconds: float = 15.0,
    ):
        self.api_base = str(api_base).rstrip("/")
        self.catalog_sport_slugs = tuple(catalog_sport_slugs)
        self.request_timeout_seconds = float(request_timeout_seconds)


class KalstropV2Provider(SportsDataProviderBase):
    """Kalstrop V2 provider — catalog via REST, no live streaming yet."""

    def __init__(
        self,
        config: KalstropV2ProviderConfig,
    ):
        super().__init__(
            config=SportsProviderConfig(
                provider_name="kalstrop_v2",
                request_timeout_seconds=float(config.request_timeout_seconds),
            ),
        )
        self._cfg = config
        self._catalog_by_uid: dict[str, ProviderGameRecord] = {}

    def load_game_catalog(self) -> list[ProviderGameRecord]:
        records: list[ProviderGameRecord] = []
        for sport_slug in self._cfg.catalog_sport_slugs:
            tournaments = self._discover_tournaments(sport_slug)
            for category_name, category_slug, tournament_name, tournament_slug in tournaments:
                fixtures = self._fetch_fixtures(sport_slug, category_slug, tournament_slug)
                for fixture in fixtures:
                    rec = self._parse_fixture(
                        fixture, sport_slug, category_name, category_slug, tournament_name, tournament_slug,
                    )
                    if rec is not None:
                        records.append(rec)
        self._catalog_by_uid = {r.provider_game_id: r for r in records}
        return records

    def _get_provider_record(self, provider_game_id: str) -> ProviderGameRecord | None:
        return self._catalog_by_uid.get(str(provider_game_id))

    def resolve_universal_ids(
        self,
        *,
        game_labels: Sequence[str] | None = None,
        universal_ids: Sequence[str] | None = None,
    ) -> list[str]:
        if not self._catalog_by_uid:
            self.load_game_catalog()
        resolved: set[str] = set()
        for uid in (universal_ids or ()):
            text = str(uid or "").strip()
            if text and text in self._catalog_by_uid:
                resolved.add(text)
        return sorted(resolved)

    def _discover_tournaments(
        self, sport_slug: str,
    ) -> list[tuple[str, str, str, str]]:
        url = f"{self._cfg.api_base}/sports/{sport_slug}/competitions"
        resp = self._client.get(url)
        if resp.status_code != 200:
            raise KalstropV2SyncError(
                f"competitions fetch failed: HTTP {resp.status_code} for {url}"
            )
        data = resp.json()
        if not isinstance(data, dict):
            raise KalstropV2SyncError(
                f"competitions fetch returned non-dict for {url}"
            )

        result: list[tuple[str, str, str, str]] = []
        for comp in data.get("competitions", []):
            if not isinstance(comp, dict):
                continue
            category = comp.get("category", {})
            cat_name = str(category.get("name") or "").strip()
            cat_slug = str(category.get("slug") or "").strip()
            if not cat_slug:
                continue
            for tourn in comp.get("tournaments", []):
                if not isinstance(tourn, dict):
                    continue
                try:
                    match_count = int(tourn.get("match_count") or 0)
                except (ValueError, TypeError):
                    match_count = 0
                if match_count <= 0:
                    continue
                t_name = str(tourn.get("name") or "").strip()
                t_slug = str(tourn.get("slug") or "").strip()
                if not t_slug:
                    continue
                result.append((cat_name, cat_slug, t_name, t_slug))
        return result

    def _fetch_fixtures(
        self, sport_slug: str, category_slug: str, tournament_slug: str,
    ) -> list[dict[str, Any]]:
        url = (
            f"{self._cfg.api_base}/sports/{sport_slug}"
            f"/competitions/{category_slug}/{tournament_slug}/fixtures"
        )
        import time as _time
        for attempt in range(3):
            try:
                resp = self._client.get(url)
            except Exception as exc:
                if attempt < 2:
                    _time.sleep(0.5 * (attempt + 1))
                    continue
                import logging
                logging.getLogger(__name__).warning(
                    "V2 fixture fetch failed for %s/%s: %s", category_slug, tournament_slug, exc,
                )
                return []
            if resp.status_code == 200:
                data = resp.json()
                return data.get("fixtures", []) if isinstance(data, dict) else []
            if resp.status_code >= 500 and attempt < 2:
                _time.sleep(0.5 * (attempt + 1))
                continue
            import logging
            logging.getLogger(__name__).warning(
                "V2 fixture fetch HTTP %d for %s/%s", resp.status_code, category_slug, tournament_slug,
            )
            return []
        return []

    def _parse_fixture(
        self,
        fixture: dict[str, Any],
        sport_slug: str,
        category_name: str,
        category_slug: str,
        tournament_name: str,
        tournament_slug: str,
    ) -> ProviderGameRecord | None:
        event_id = str(fixture.get("event_id") or "").strip()
        if not event_id:
            return None
        competitors = fixture.get("competitors", {})
        if not isinstance(competitors, dict):
            return None
        home = competitors.get("home", {})
        away = competitors.get("away", {})
        home_name = str(home.get("name") or "").strip()
        away_name = str(away.get("name") or "").strip()
        if not home_name or not away_name:
            return None

        match_status = str(fixture.get("match_status") or "").strip().upper()
        if match_status in ("ENDED", "FINISHED", "COMPLETED"):
            return None

        game_label = str(fixture.get("name") or "").strip()
        fixture_slug = str(fixture.get("slug") or "").strip()
        start_time_raw = str(fixture.get("start_time") or "").strip()
        start_ts_utc = self._parse_iso_ts(start_time_raw)

        parse_parts: list[str] = []
        if not start_ts_utc:
            parse_parts.append("missing_start_time")

        alias_values: set[str] = set()
        if game_label:
            alias_values.add(game_label)
        if fixture_slug:
            alias_values.add(fixture_slug)
        home_slug = str(home.get("slug") or "").strip()
        away_slug = str(away.get("slug") or "").strip()
        if home_slug and away_slug:
            alias_values.add(f"{home_slug}-vs-{away_slug}")

        return ProviderGameRecord(
            provider="kalstrop_v2",
            provider_game_id=event_id,
            game_label=game_label or f"{home_name} vs {away_name}",
            orig_teams=f"{home_name} vs {away_name}",
            sport_raw=sport_slug,
            league_raw=tournament_name,
            category_name=category_name,
            category_country_code="",
            when_raw=start_time_raw,
            home_team_raw=home_name,
            away_team_raw=away_name,
            start_ts_utc=start_ts_utc,
            parse_status="ok" if not parse_parts else "partial",
            parse_reason="|".join(parse_parts),
            aliases=tuple(sorted(alias_values)),
            raw_payload=dict(fixture) | {
                "_category_slug": category_slug,
                "_tournament_slug": tournament_slug,
            },
        )

    @staticmethod
    def _parse_iso_ts(raw: str) -> int | None:
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None


__all__ = ["KalstropV2Provider", "KalstropV2ProviderConfig", "KalstropV2SyncError"]
