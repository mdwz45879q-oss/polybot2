"""Provider game catalog sync for polybot2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from typing import Any

from zoneinfo import ZoneInfo

from polybot2.data.storage.database import Database
from polybot2.linking.mapping_loader import load_mapping
from polybot2.sports.boltodds import BoltOddsProvider, BoltOddsProviderConfig
from polybot2.sports.factory import resolve_kalstrop_credentials_from_env
from polybot2.sports.kalstrop import KalstropProvider, KalstropProviderConfig

_ET = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")


@dataclass(frozen=True)
class ProviderSyncResult:
    provider: str
    n_rows: int
    status: str
    reason: str = ""


def _parse_boltodds_when_et(when_raw: str) -> tuple[int | None, str]:
    text = str(when_raw or "").strip()
    if not text:
        return (None, "")
    formats = (
        "%Y-%m-%d, %I:%M %p",
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d, %H:%M",
        "%Y-%m-%d %H:%M",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=_ET)
            return (int(dt.astimezone(_UTC).timestamp()), dt.date().isoformat())
        except Exception:
            continue
    return (None, "")


def _parse_when_to_utc_and_date_et(when_raw: str) -> tuple[int | None, str]:
    ts, date_et = _parse_boltodds_when_et(when_raw)
    if ts is not None:
        return (ts, date_et)
    text = str(when_raw or "").strip()
    if not text:
        return (None, "")
    try:
        if text.endswith("Z"):
            ts_val = int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_UTC)
            ts_val = int(dt.timestamp())
        return (ts_val, datetime.fromtimestamp(ts_val, tz=_ET).date().isoformat())
    except Exception:
        return (None, "")


def _derive_start_ts_and_game_date_et(*, provider_start_ts_utc: int | None, when_raw: str) -> tuple[int | None, str]:
    """Return canonical kickoff timestamp/date for provider row persistence.

    We intentionally prefer the provider adapter's parsed `start_ts_utc` because:
    - Kalstrop `when_raw_et` storage field contains provider-raw UTC text.
    - BoltOdds raw time text can be ET-formatted.
    Using canonical parsed kickoff avoids coupling runtime logic to raw text semantics.
    """
    if provider_start_ts_utc is not None:
        ts = int(provider_start_ts_utc)
        return (ts, datetime.fromtimestamp(ts, tz=_ET).date().isoformat())
    return _parse_when_to_utc_and_date_et(when_raw)


def _resolve_kalstrop_catalog_sport_codes(*, loaded_mapping: Any) -> tuple[str, ...]:
    env_value = str(os.getenv("KALSTROP_CATALOG_SPORT_CODES", "") or "").strip()
    if env_value:
        parsed = {
            str(part or "").strip().lower().replace("-", "_")
            for part in str(env_value).split(",")
            if str(part or "").strip()
        }
        if parsed:
            return tuple(sorted(parsed))

    sport_codes: set[str] = set()
    leagues = getattr(loaded_mapping, "leagues", None)
    if isinstance(leagues, dict):
        for meta in leagues.values():
            if not isinstance(meta, dict):
                continue
            sport_family = str(meta.get("sport_family") or "").strip().lower().replace("-", "_")
            if sport_family:
                sport_codes.add(sport_family)
    if not sport_codes:
        sport_codes = {"baseball", "soccer", "basketball", "hockey", "american_football", "tennis"}
    return tuple(sorted(sport_codes))


def sync_provider_games(
    *,
    db: Database,
    provider: str,
    payload_ref: str = "",
) -> ProviderSyncResult:
    p = str(provider or "").strip().lower()
    now_ts = int(datetime.now(tz=_UTC).timestamp())
    if p not in {"boltodds", "kalstrop"}:
        return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="unsupported_provider")

    client: Any
    if p == "boltodds":
        api_key = str(os.getenv("BOLTODDS_API_KEY") or "").strip()
        if not api_key:
            return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="missing_BOLTODDS_API_KEY")
        client = BoltOddsProvider(config=BoltOddsProviderConfig(api_key=api_key))
    else:
        client_id, shared_secret_raw, _source = resolve_kalstrop_credentials_from_env()
        if not client_id or not shared_secret_raw:
            return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="missing_kalstrop_credentials")
        http_base = str(os.getenv("KALSTROP_BASE_URL") or "https://sportsapi.kalstropservice.com/odds_v1/v1").strip()
        ws_url = str(os.getenv("KALSTROP_WS_URL") or "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws").strip()
        loaded_mapping = load_mapping()
        sport_codes = _resolve_kalstrop_catalog_sport_codes(loaded_mapping=loaded_mapping)
        client = KalstropProvider(
            config=KalstropProviderConfig(
                client_id=client_id,
                shared_secret_raw=shared_secret_raw,
                http_base=http_base,
                ws_url=ws_url,
                catalog_sport_codes=sport_codes,
                catalog_types=("live", "upcoming"),
                catalog_first=6,
                catalog_fixture_first=3,
            )
        )
    try:
        records = client.load_game_catalog()
    finally:
        client.close()

    rows: list[tuple[Any, ...]] = []
    for rec in records:
        start_ts_utc, game_date_et = _derive_start_ts_and_game_date_et(
            provider_start_ts_utc=(None if rec.start_ts_utc is None else int(rec.start_ts_utc)),
            when_raw=str(rec.when_raw or ""),
        )
        payload_json = json.dumps(rec.raw_payload or {}, separators=(",", ":"), sort_keys=True, default=str)
        payload_sha = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        rows.append(
            (
                p,
                str(rec.provider_game_id),
                str(rec.game_label or ""),
                str(rec.orig_teams or ""),
                str(rec.sport_raw or ""),
                str(rec.league_raw or ""),
                str(rec.when_raw or ""),
                start_ts_utc,
                str(game_date_et or ""),
                str(rec.home_team_raw or ""),
                str(rec.away_team_raw or ""),
                str(rec.parse_status or ""),
                str(rec.parse_reason or ""),
                payload_sha,
                str(payload_ref or ""),
                int(len(payload_json.encode("utf-8"))),
                now_ts,
            )
        )

    db.linking.replace_provider_games_snapshot(provider=p, rows=rows)
    return ProviderSyncResult(provider=p, n_rows=len(rows), status="ok")
