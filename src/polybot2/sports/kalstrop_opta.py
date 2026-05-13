"""Kalstrop Opta provider — catalog discovery via REST.

Opta covers football (EPL, La Liga, Bundesliga, UCL, Serie A, Ligue 1, etc.)
and baseball (MLB, NPB, KBO). Same HMAC auth as Genius (V2).

Catalog discovery follows the Opta API hierarchy:
  Step 1: GET /sports → sport slugs
  Step 2: GET /sports/{sport}/competitions → categories + tournaments (numeric IDs)
  Step 3: GET /sports/{sport}/competitions/{category_id}/{tournament_id}/fixtures → fixtures
"""

from __future__ import annotations

import logging
import time as _time
from datetime import datetime, timezone
from typing import Any, Sequence

import httpx

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import ProviderGameRecord, SportsProviderConfig


class KalstropOptaSyncError(Exception):
    """Raised when Opta catalog discovery fails at the root level."""


class KalstropOptaProviderConfig:

    def __init__(
        self,
        *,
        api_base: str = "https://stats.kalstropservice.com/api/v2/opta",
        catalog_sport_slugs: Sequence[str] = ("football", "baseball"),
        request_timeout_seconds: float = 15.0,
        client_id: str = "",
        shared_secret_raw: str = "",
    ):
        self.api_base = str(api_base).rstrip("/")
        self.catalog_sport_slugs = tuple(catalog_sport_slugs)
        self.request_timeout_seconds = float(request_timeout_seconds)
        self.client_id = str(client_id).strip()
        self.shared_secret_raw = str(shared_secret_raw).strip()


class KalstropOptaProvider(SportsDataProviderBase):
    """Kalstrop Opta provider — catalog via REST with HMAC auth."""

    def __init__(
        self,
        config: KalstropOptaProviderConfig,
    ):
        super().__init__(
            config=SportsProviderConfig(
                provider_name="kalstrop_opta",
                request_timeout_seconds=float(config.request_timeout_seconds),
            ),
        )
        self._cfg = config
        self._catalog_by_uid: dict[str, ProviderGameRecord] = {}

    def _auth_headers(self) -> dict[str, str]:
        from polybot2.sports.kalstrop_auth import kalstrop_auth_headers
        if not self._cfg.client_id or not self._cfg.shared_secret_raw:
            return {}
        return kalstrop_auth_headers(self._cfg.client_id, self._cfg.shared_secret_raw)

    def load_game_catalog(self) -> list[ProviderGameRecord]:
        records: list[ProviderGameRecord] = []
        for sport_slug in self._cfg.catalog_sport_slugs:
            tournaments = self._discover_tournaments(sport_slug)
            for cat_name, cat_id, tourn_name, tourn_id in tournaments:
                fixtures = self._fetch_fixtures(sport_slug, cat_id, tourn_id)
                for fixture in fixtures:
                    rec = self._parse_fixture(
                        fixture, sport_slug, cat_name, cat_id, tourn_name, tourn_id,
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
        """Returns [(cat_name, cat_id, tourn_name, tourn_id), ...]."""
        url = f"{self._cfg.api_base}/sports/{sport_slug}/competitions"
        resp = self._client.get(url, headers=self._auth_headers())
        if resp.status_code != 200:
            raise KalstropOptaSyncError(
                f"competitions fetch failed: HTTP {resp.status_code} for {url}"
            )
        data = resp.json()
        if not isinstance(data, dict):
            raise KalstropOptaSyncError(
                f"competitions fetch returned non-dict for {url}"
            )

        result: list[tuple[str, str, str, str]] = []
        for comp in data.get("competitions", []):
            if not isinstance(comp, dict):
                continue
            category = comp.get("category", {})
            cat_name = str(category.get("name") or "").strip()
            cat_id = str(category.get("id") or "").strip()
            if not cat_id:
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
                t_id = str(tourn.get("id") or "").strip()
                if not t_id:
                    continue
                result.append((cat_name, cat_id, t_name, t_id))
        return result

    def _fetch_fixtures(
        self, sport_slug: str, category_id: str, tournament_id: str,
    ) -> list[dict[str, Any]]:
        url = (
            f"{self._cfg.api_base}/sports/{sport_slug}"
            f"/competitions/{category_id}/{tournament_id}/fixtures"
        )
        for attempt in range(3):
            try:
                resp = self._client.get(url, headers=self._auth_headers())
            except Exception as exc:
                if attempt < 2:
                    _time.sleep(0.5 * (attempt + 1))
                    continue
                logging.getLogger(__name__).warning(
                    "Opta fixture fetch failed for %s/%s: %s", category_id, tournament_id, exc,
                )
                return []
            if resp.status_code == 200:
                data = resp.json()
                return data.get("fixtures", []) if isinstance(data, dict) else []
            if resp.status_code >= 500 and attempt < 2:
                _time.sleep(0.5 * (attempt + 1))
                continue
            logging.getLogger(__name__).warning(
                "Opta fixture fetch HTTP %d for %s/%s", resp.status_code, category_id, tournament_id,
            )
            return []
        return []

    def _parse_fixture(
        self,
        fixture: dict[str, Any],
        sport_slug: str,
        cat_name: str,
        cat_id: str,
        tourn_name: str,
        tourn_id: str,
    ) -> ProviderGameRecord | None:
        event_id = str(fixture.get("event_id") or "").strip()
        if not event_id:
            return None
        competitors = fixture.get("competitors") or {}
        home_obj = competitors.get("home") or {}
        away_obj = competitors.get("away") or {}
        home_name = str(home_obj.get("name") or "").strip() if isinstance(home_obj, dict) else ""
        away_name = str(away_obj.get("name") or "").strip() if isinstance(away_obj, dict) else ""

        # Baseball fixtures have null competitors — parse from game label
        if not home_name or not away_name:
            game_name = str(fixture.get("name") or "").strip()
            if " at " in game_name:
                away_name, home_name = game_name.split(" at ", 1)
            elif " vs " in game_name.lower():
                parts = game_name.split(" vs ", 1) if " vs " in game_name else game_name.split(" Vs ", 1)
                home_name, away_name = parts[0].strip(), parts[1].strip()
            if not home_name or not away_name:
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
        home_slug = str(home_obj.get("slug") or "").strip() if isinstance(home_obj, dict) else ""
        away_slug = str(away_obj.get("slug") or "").strip() if isinstance(away_obj, dict) else ""
        if home_slug and away_slug:
            alias_values.add(f"{home_slug}-vs-{away_slug}")

        return ProviderGameRecord(
            provider="kalstrop_opta",
            provider_game_id=event_id,
            game_label=game_label or f"{home_name} vs {away_name}",
            orig_teams=f"{home_name} vs {away_name}",
            sport_raw=sport_slug,
            league_raw=f"{tourn_name}|{tourn_id}",
            category_name=f"{cat_name}|{cat_id}",
            category_country_code="",
            when_raw=start_time_raw,
            home_team_raw=home_name,
            away_team_raw=away_name,
            start_ts_utc=start_ts_utc,
            parse_status="ok" if not parse_parts else "partial",
            parse_reason="|".join(parse_parts),
            aliases=tuple(sorted(alias_values)),
            raw_payload=dict(fixture) | {
                "_category_id": cat_id,
                "_tournament_id": tourn_id,
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


__all__ = ["KalstropOptaProvider", "KalstropOptaProviderConfig", "KalstropOptaSyncError"]
