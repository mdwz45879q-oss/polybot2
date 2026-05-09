"""BoltOdds provider implementation for game catalog and resolution."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import threading
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import httpx

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.contracts import (
    ProviderGameRecord,
    SportsProviderConfig,
)


class BoltOddsProviderConfig(SportsProviderConfig):
    """Runtime settings for BoltOdds integration."""

    def __init__(
        self,
        *,
        api_key: str,
        http_base: str = "https://spro.agency/api",
        request_timeout_seconds: float = 20.0,
    ):
        super().__init__(
            provider_name="boltodds",
            request_timeout_seconds=float(request_timeout_seconds),
        )
        self.api_key = str(api_key or "").strip()
        self.http_base = str(http_base or "").rstrip("/")
        if not self.api_key:
            raise ValueError("api_key must be non-empty")
        if not self.http_base:
            raise ValueError("http_base must be non-empty")


class BoltOddsProvider(SportsDataProviderBase):
    _et_tz = ZoneInfo("America/New_York")

    def __init__(
        self,
        *,
        config: BoltOddsProviderConfig,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(
            config=SportsProviderConfig(
                provider_name="boltodds",
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
    def config(self) -> BoltOddsProviderConfig:
        return self._cfg

    def _http_get_json(self, endpoint: str) -> Any:
        url = f"{self._cfg.http_base}/{endpoint}"
        response = self._client.get(url, params={"key": self._cfg.api_key})
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

    def _build_provider_record_from_row(self, row: dict[str, Any]) -> ProviderGameRecord | None:
        game_label = self._extract_game_label(row)
        if not game_label:
            return None
        uid = str(self._extract_universal_id(row))
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
            provider_game_id=game_label,
            game_label=game_label,
            orig_teams=orig_teams,
            sport_raw=sport_raw,
            league_raw=league_raw,
            when_raw=when_raw,
            home_team_raw=home_raw,
            away_team_raw=away_raw,
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
                for rec in candidates:
                    all_aliases.update({str(a).strip() for a in rec.aliases if str(a).strip()})
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

            out = [self._catalog_by_uid[k] for k in sorted(self._catalog_by_uid)]
        return out

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

__all__ = ["BoltOddsProvider", "BoltOddsProviderConfig"]
