"""Contracts for dedicated low-latency hot path."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any


# Outcome semantics produced by the compiler. Includes two-way (home, away,
# over, under, yes, no) and three-way (home_yes, home_no, away_yes, away_no,
# draw_yes, draw_no) values.
OutcomeSemantic = str


@dataclass(frozen=True, slots=True)
class HotPathConfig:
    run_scores: bool = True
    run_odds: bool = False
    read_timeout_seconds: float = 0.05
    reconnect_base_sleep_seconds: float = 0.05
    profiling_enabled: bool = False
    native_engine_enabled: bool = False
    native_engine_required: bool = False

    @classmethod
    def from_env(cls, overrides: dict[str, object] | None = None) -> "HotPathConfig":
        def _get(name: str) -> str | None:
            return os.getenv(f"POLY_HOTPATH_{name}")

        vals: dict[str, object] = {}
        if (v := _get("RUN_SCORES")) is not None:
            vals["run_scores"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("RUN_ODDS")) is not None:
            vals["run_odds"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("READ_TIMEOUT_SECONDS")) is not None:
            vals["read_timeout_seconds"] = float(v)
        if (v := _get("RECONNECT_BASE_SLEEP_SECONDS")) is not None:
            vals["reconnect_base_sleep_seconds"] = float(v)
        if (v := _get("PROFILING_ENABLED")) is not None:
            vals["profiling_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("NATIVE_ENGINE_ENABLED")) is not None:
            vals["native_engine_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("NATIVE_ENGINE_REQUIRED")) is not None:
            vals["native_engine_required"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if overrides:
            vals.update(overrides)
        return cls(**vals)

    def __post_init__(self) -> None:
        if not bool(self.run_scores) and not bool(self.run_odds):
            raise ValueError("at least one stream must be enabled")
        if float(self.read_timeout_seconds) <= 0.0:
            raise ValueError("read_timeout_seconds must be > 0")
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
