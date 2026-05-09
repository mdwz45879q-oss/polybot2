"""Contracts for sports data providers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ProviderGameRecord:
    """Provider-native catalog record normalized for linking."""

    provider: str
    provider_game_id: str
    game_label: str = ""
    orig_teams: str = ""
    sport_raw: str = ""
    league_raw: str = ""
    when_raw: str = ""
    home_team_raw: str = ""
    away_team_raw: str = ""
    category_name: str = ""
    category_country_code: str = ""
    start_ts_utc: int | None = None
    parse_status: str = "ok"
    parse_reason: str = ""
    aliases: tuple[str, ...] = ()
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.provider or "").strip():
            raise ValueError("provider must be non-empty")
        if not str(self.provider_game_id or "").strip():
            raise ValueError("provider_game_id must be non-empty")


@dataclass(frozen=True)
class SportsProviderConfig:
    provider_name: str
    request_timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        if not str(self.provider_name or "").strip():
            raise ValueError("provider_name must be non-empty")
        if float(self.request_timeout_seconds) <= 0.0:
            raise ValueError("request_timeout_seconds must be > 0")


__all__ = [
    "ProviderGameRecord",
    "SportsProviderConfig",
]
