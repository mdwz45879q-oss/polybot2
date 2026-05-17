"""Contracts for dedicated low-latency hot path."""

from __future__ import annotations

from dataclasses import dataclass, field


# Outcome semantics produced by the compiler. Includes two-way (home, away,
# over, under, yes, no) and three-way (home_yes, home_no, away_yes, away_no,
# draw_yes, draw_no) values.
OutcomeSemantic = str


@dataclass(frozen=True, slots=True)
class HotPathConfig:
    reconnect_base_sleep_seconds: float = 0.05
    native_engine_required: bool = False

    def __post_init__(self) -> None:
        if float(self.reconnect_base_sleep_seconds) <= 0.0:
            raise ValueError("reconnect_base_sleep_seconds must be > 0")


@dataclass(frozen=True, slots=True)
class CompiledTarget:
    condition_id: str
    outcome_index: int
    token_id: str
    sports_market_type: str
    line: float | None
    outcome_label: str
    outcome_semantic: OutcomeSemantic
    strategy_key: str


@dataclass(frozen=True, slots=True)
class CompiledMarket:
    condition_id: str
    market_id: str
    event_id: str
    sports_market_type: str
    line: float | None
    question: str
    targets: tuple[CompiledTarget, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class CompiledGamePlan:
    provider_game_id: str
    canonical_league: str
    canonical_home_team: str
    canonical_away_team: str
    kickoff_ts_utc: int | None
    markets: tuple[CompiledMarket, ...] = field(default_factory=tuple)
    # Alternate provider game IDs for the same canonical game.
    # Each entry is (provider_name, provider_game_id).
    alternate_provider_game_ids: tuple[tuple[str, str], ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class CompiledPlan:
    provider: str
    league: str
    run_id: int
    plan_hash: str
    compiled_at: int
    games: tuple[CompiledGamePlan, ...] = field(default_factory=tuple)


__all__ = [
    "CompiledGamePlan",
    "CompiledMarket",
    "CompiledPlan",
    "CompiledTarget",
    "HotPathConfig",
    "OutcomeSemantic",
]
