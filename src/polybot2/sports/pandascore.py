"""PandaScore esports data provider — catalog adapter.

Pulls upcoming + running matches for CS2, LoL, and Dota 2 via the
PandaScore REST API. Catalog-only (no streaming — WebSocket requires
a paid real-time data plan).

API docs: https://developers.pandascore.co/docs/getting-started
Auth: token as query param (?token=...).
Pagination: page + per_page (max 100).
CS2 endpoints use /csgo/ prefix (legacy naming).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Sequence

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import ProviderGameRecord, SportsProviderConfig

log = logging.getLogger(__name__)

# API path prefix → default sport_raw (overridden by videogame.name in response)
_VIDEOGAME_PATHS = {
    "csgo": "Counter-Strike",
    "lol": "LoL",
    "dota2": "Dota 2",
}


class PandaScoreProviderConfig(SportsProviderConfig):
    def __init__(
        self,
        *,
        api_token: str,
        http_base: str = "https://api.pandascore.co",
        request_timeout_seconds: float = 20.0,
        full_catalog: bool = False,
    ):
        super().__init__(
            provider_name="pandascore",
            request_timeout_seconds=float(request_timeout_seconds),
        )
        self.api_token = str(api_token or "").strip()
        self.http_base = str(http_base or "").rstrip("/")
        self.full_catalog = bool(full_catalog)
        if not self.api_token:
            raise ValueError("api_token must be non-empty")


def _parse_iso_to_epoch(ts: str | None) -> int | None:
    """Parse ISO 8601 timestamp to Unix epoch seconds."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


class PandaScoreProvider(SportsDataProviderBase):
    def __init__(self, config: PandaScoreProviderConfig):
        super().__init__(config=config)
        self._cfg = config
        self._catalog: list[ProviderGameRecord] = []
        self._by_id: dict[str, ProviderGameRecord] = {}

    def _api_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Make an authenticated GET request to the PandaScore API."""
        url = f"{self._cfg.http_base}{path}"
        p = dict(params or {})
        p["token"] = self._cfg.api_token
        resp = self._client.get(url, params=p, timeout=self._cfg.request_timeout_seconds)
        resp.raise_for_status()
        return resp.json()

    def _fetch_matches_paginated(self, path: str, max_pages: int = 20) -> list[dict[str, Any]]:
        """Fetch all pages of a paginated endpoint."""
        all_matches: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            data = self._api_get(path, params={"per_page": 100, "page": page})
            if not isinstance(data, list) or not data:
                break
            all_matches.extend(data)
            if len(data) < 100:
                break
            time.sleep(0.2)  # Gentle rate limiting
        return all_matches

    def _match_to_record(self, m: dict[str, Any]) -> ProviderGameRecord | None:
        """Convert a PandaScore match object to a ProviderGameRecord."""
        match_id = m.get("id")
        if not match_id:
            return None

        opponents = m.get("opponents") or []
        if len(opponents) < 2:
            return None  # TBD matches — skip

        home_team = opponents[0].get("opponent", {})
        away_team = opponents[1].get("opponent", {})
        home_name = str(home_team.get("name") or "").strip()
        away_name = str(away_team.get("name") or "").strip()
        if not home_name or not away_name:
            return None

        videogame = m.get("videogame") or {}
        league = m.get("league") or {}
        serie = m.get("serie") or {}
        tournament = m.get("tournament") or {}
        live = m.get("live") or {}
        videogame_title = m.get("videogame_title") or {}

        scheduled_at = m.get("scheduled_at") or ""

        extra = {
            "number_of_games": m.get("number_of_games"),
            "live_supported": tournament.get("live_supported"),
            "live_opens_at": live.get("opens_at"),
            "tournament_tier": tournament.get("tier"),
            "tournament_name": tournament.get("name"),
            "rescheduled": m.get("rescheduled"),
            "videogame_title": videogame_title.get("name"),
            "match_slug": m.get("slug"),
            "team_acronyms": {
                "home": home_team.get("acronym", ""),
                "away": away_team.get("acronym", ""),
            },
        }
        # Low-latency endpoint (injected by _load_low_latency_catalog)
        if m.get("_ll_endpoint_url"):
            extra["ll_endpoint_url"] = m["_ll_endpoint_url"]
        if m.get("_ll_opens_at"):
            extra["ll_opens_at"] = m["_ll_opens_at"]

        return ProviderGameRecord(
            provider="pandascore",
            provider_game_id=str(match_id),
            game_label=f"{home_name} vs {away_name}",
            orig_teams=f"{home_name} vs {away_name}",
            sport_raw=videogame.get("name", ""),
            league_raw=league.get("name", ""),
            category_name=serie.get("full_name", ""),
            category_country_code=tournament.get("country") or tournament.get("region") or "",
            when_raw=scheduled_at,
            start_ts_utc=_parse_iso_to_epoch(scheduled_at),
            home_team_raw=home_name,
            away_team_raw=away_name,
            parse_status="ok",
            extra_json=json.dumps(extra, separators=(",", ":")),
        )

    def _load_low_latency_catalog(self) -> list[ProviderGameRecord]:
        """Load matches from /low_latency_feeds — only games with LL WebSocket support."""
        records: list[ProviderGameRecord] = []
        seen_ids: set[str] = set()

        try:
            data = self._api_get("/low_latency_feeds")
        except Exception as exc:
            log.warning("PandaScore /low_latency_feeds failed: %s", exc)
            return records

        if not isinstance(data, list):
            return records

        for entry in data:
            m = entry.get("match", {})
            endpoint = entry.get("endpoint", {})
            # Inject the LL endpoint URL into the match for extra_json
            m["_ll_endpoint_url"] = endpoint.get("url", "")
            m["_ll_opens_at"] = endpoint.get("opens_at", "")
            rec = self._match_to_record(m)
            if rec and rec.provider_game_id not in seen_ids:
                seen_ids.add(rec.provider_game_id)
                records.append(rec)

        return records

    def _load_full_catalog(self) -> list[ProviderGameRecord]:
        """Load upcoming + running matches for all supported esports (general catalog)."""
        records: list[ProviderGameRecord] = []
        seen_ids: set[str] = set()

        for slug in _VIDEOGAME_PATHS:
            for status in ("upcoming", "running"):
                path = f"/{slug}/matches/{status}"
                try:
                    matches = self._fetch_matches_paginated(path)
                except Exception as exc:
                    log.warning("PandaScore %s %s failed: %s", slug, status, exc)
                    continue

                for m in matches:
                    rec = self._match_to_record(m)
                    if rec and rec.provider_game_id not in seen_ids:
                        seen_ids.add(rec.provider_game_id)
                        records.append(rec)

        return records

    def load_game_catalog(self) -> list[ProviderGameRecord]:
        """Load matches. Default: low-latency only. With full_catalog: all esports."""
        if self._cfg.full_catalog:
            records = self._load_full_catalog()
            source = "full catalog"
        else:
            records = self._load_low_latency_catalog()
            source = "low_latency_feeds"

        with self._lock:
            self._catalog = records
            self._by_id = {r.provider_game_id: r for r in records}

        by_sport: dict[str, int] = {}
        for r in records:
            by_sport[r.sport_raw] = by_sport.get(r.sport_raw, 0) + 1
        sport_summary = ", ".join(f"{k}={v}" for k, v in sorted(by_sport.items()))

        log.info("PandaScore catalog (%s): %d matches (%s)", source, len(records), sport_summary)
        return records

    def _get_provider_record(self, provider_game_id: str) -> ProviderGameRecord | None:
        with self._lock:
            return self._by_id.get(str(provider_game_id))

    def resolve_universal_ids(
        self,
        *,
        game_labels: Sequence[str] | None = None,
        universal_ids: Sequence[str] | None = None,
    ) -> list[str]:
        # PandaScore uses numeric match IDs, not game labels.
        # Resolution is by ID lookup only.
        with self._lock:
            if universal_ids:
                return [uid for uid in universal_ids if uid in self._by_id]
        return []


__all__ = ["PandaScoreProvider", "PandaScoreProviderConfig"]
