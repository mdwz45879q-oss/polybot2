"""Contracts for provider-agnostic sports streaming."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


StreamType = Literal["odds", "scores", "playbyplay"]


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
    sport_key: str = ""
    league_key: str = ""
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
class OddsOutcome:
    outcome_key: str
    odds: float | int | None = None
    outcome_name: str = ""
    outcome_target: str = ""
    outcome_line: str = ""
    outcome_over_under: str = ""
    outcome_link: str = ""


@dataclass(frozen=True)
class OddsUpdateEvent:
    provider: str
    universal_id: str
    action: str
    provider_timestamp: str = ""
    game: str = ""
    sport: str = ""
    league: str = ""
    sportsbook: str = ""
    home_team: str = ""
    away_team: str = ""
    game_when: str = ""
    book_event_id: str = ""
    book_event_link: str = ""
    outcomes: tuple[OddsOutcome, ...] = ()
    raw_payload: dict[str, Any] = field(default_factory=dict)
    source_recv_monotonic_ns: int | None = None


@dataclass(frozen=True)
class ScoreUpdateEvent:
    provider: str
    universal_id: str
    action: str
    provider_timestamp: str = ""
    game: str = ""
    home_team: str = ""
    away_team: str = ""
    period: str = ""
    elapsed_time_seconds: int | None = None
    pre_match: bool | None = None
    match_completed: bool | None = None
    clock_running_now: bool | None = None
    clock_running: bool | None = None
    home_score: int | float | None = None
    away_score: int | float | None = None
    home_corners: int | float | None = None
    away_corners: int | float | None = None
    home_yellow_cards: int | float | None = None
    away_yellow_cards: int | float | None = None
    home_red_cards: int | float | None = None
    away_red_cards: int | float | None = None
    home_first_half_goals: int | float | None = None
    away_first_half_goals: int | float | None = None
    home_second_half_goals: int | float | None = None
    away_second_half_goals: int | float | None = None
    var_referral_in_progress: bool | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    source_recv_monotonic_ns: int | None = None


@dataclass(frozen=True)
class PlayByPlayUpdateEvent:
    provider: str
    universal_id: str
    action: str
    stream_id: str = ""
    provider_timestamp: str = ""
    game: str = ""
    league: str = ""
    state: Any = field(default_factory=dict)
    play_info: dict[str, Any] = field(default_factory=dict)
    score: dict[str, Any] | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    source_recv_monotonic_ns: int | None = None


@dataclass(frozen=True)
class StreamEnvelope:
    provider: str
    stream: StreamType
    universal_id: str
    payload_kind: str
    received_ts: int
    dedup_key: str
    event: OddsUpdateEvent | ScoreUpdateEvent | PlayByPlayUpdateEvent


@dataclass(frozen=True)
class SportsProviderConfig:
    provider_name: str
    request_timeout_seconds: float = 20.0
    reconnect_sleep_seconds: float = 0.2
    queue_maxsize: int = 50_000

    def __post_init__(self) -> None:
        if not str(self.provider_name or "").strip():
            raise ValueError("provider_name must be non-empty")
        if float(self.request_timeout_seconds) <= 0.0:
            raise ValueError("request_timeout_seconds must be > 0")
        if float(self.reconnect_sleep_seconds) <= 0.0:
            raise ValueError("reconnect_sleep_seconds must be > 0")
        if int(self.queue_maxsize) <= 0:
            raise ValueError("queue_maxsize must be > 0")


__all__ = [
    "ProviderGameRecord",
    "OddsOutcome",
    "OddsUpdateEvent",
    "PlayByPlayUpdateEvent",
    "ScoreUpdateEvent",
    "SportsProviderConfig",
    "StreamEnvelope",
    "StreamType",
]
