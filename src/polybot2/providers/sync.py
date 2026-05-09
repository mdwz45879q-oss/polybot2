"""Provider game catalog sync for polybot2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from typing import Any

from zoneinfo import ZoneInfo

from polybot2.data.storage.database import Database
from polybot2.sports.boltodds import BoltOddsProvider, BoltOddsProviderConfig
from polybot2.sports.factory import resolve_kalstrop_credentials_from_env
from polybot2.sports.kalstrop_v1 import KalstropV1Provider, KalstropV1ProviderConfig

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
    - Kalstrop `when_raw` storage field contains provider-raw UTC text.
    - BoltOdds raw time text can be ET-formatted.
    Using canonical parsed kickoff avoids coupling runtime logic to raw text semantics.
    """
    if provider_start_ts_utc is not None:
        ts = int(provider_start_ts_utc)
        return (ts, datetime.fromtimestamp(ts, tz=_ET).date().isoformat())
    return _parse_when_to_utc_and_date_et(when_raw)


def _resolve_kalstrop_catalog_sport_codes() -> tuple[str, ...]:
    env_value = str(os.getenv("KALSTROP_CATALOG_SPORT_CODES", "") or "").strip()
    if env_value:
        parsed = {
            str(part or "").strip().lower().replace("-", "_")
            for part in str(env_value).split(",")
            if str(part or "").strip()
        }
        if parsed:
            return tuple(sorted(parsed))
    return ("baseball", "soccer")



def sync_provider_games(
    *,
    db: Database,
    provider: str,
) -> ProviderSyncResult:
    p = str(provider or "").strip().lower()
    now_ts = int(datetime.now(tz=_UTC).timestamp())
    # Normalize legacy "kalstrop" to "kalstrop_v1"
    if p == "kalstrop":
        p = "kalstrop_v1"
    if p not in {"boltodds", "kalstrop_v1", "kalstrop_v2"}:
        return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="unsupported_provider")

    client: Any
    if p == "boltodds":
        api_key = str(os.getenv("BOLTODDS_API_KEY") or "").strip()
        if not api_key:
            return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="missing_BOLTODDS_API_KEY")
        client = BoltOddsProvider(config=BoltOddsProviderConfig(api_key=api_key))
    elif p == "kalstrop_v1":
        client_id, shared_secret_raw, _source = resolve_kalstrop_credentials_from_env()
        if not client_id or not shared_secret_raw:
            return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="missing_kalstrop_credentials")
        http_base = str(os.getenv("KALSTROP_BASE_URL") or "https://sportsapi.kalstropservice.com/odds_v1/v1").strip()
        sport_codes = _resolve_kalstrop_catalog_sport_codes()
        client = KalstropV1Provider(
            config=KalstropV1ProviderConfig(
                client_id=client_id,
                shared_secret_raw=shared_secret_raw,
                http_base=http_base,
                catalog_sport_codes=sport_codes,
                catalog_types=("live", "upcoming", "popular"),
                catalog_first=10,
                catalog_fixture_first=10,
            )
        )
    else:
        from polybot2.sports.kalstrop_v2 import KalstropV2Provider, KalstropV2ProviderConfig
        client = KalstropV2Provider(config=KalstropV2ProviderConfig())
    try:
        records = client.load_game_catalog()
    except Exception as exc:
        try:
            client.close()
        except Exception:
            pass
        return ProviderSyncResult(provider=p, n_rows=0, status="error", reason=f"catalog_fetch_failed:{exc}")
    finally:
        try:
            client.close()
        except Exception:
            pass

    if not records:
        return ProviderSyncResult(provider=p, n_rows=0, status="error", reason="empty_catalog")

    rows: list[tuple[Any, ...]] = []
    for rec in records:
        start_ts_utc, game_date_et = _derive_start_ts_and_game_date_et(
            provider_start_ts_utc=(None if rec.start_ts_utc is None else int(rec.start_ts_utc)),
            when_raw=str(rec.when_raw or ""),
        )
        rows.append(
            (
                p,
                str(rec.provider_game_id),
                str(rec.game_label or ""),
                str(rec.orig_teams or ""),
                str(rec.sport_raw or ""),
                str(rec.league_raw or ""),
                str(rec.category_name or ""),
                str(rec.category_country_code or ""),
                str(rec.when_raw or ""),
                start_ts_utc,
                str(game_date_et or ""),
                str(rec.home_team_raw or ""),
                str(rec.away_team_raw or ""),
                str(rec.parse_status or ""),
                str(rec.parse_reason or ""),
                now_ts,
            )
        )

    db.linking.replace_provider_games_snapshot(provider=p, rows=rows)
    return ProviderSyncResult(provider=p, n_rows=len(rows), status="ok")
