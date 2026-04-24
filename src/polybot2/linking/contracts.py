"""Deterministic linking contracts for polybot2."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


BindingStatus = Literal["exact", "ambiguous", "unresolved"]


@dataclass(frozen=True, slots=True)
class BindingTarget:
    provider: str
    provider_game_id: str
    condition_id: str
    outcome_index: int
    token_id: str
    market_slug: str
    sports_market_type: str
    binding_status: str
    reason_code: str
    is_tradeable: bool

    @property
    def market_id(self) -> str:
        return str(self.condition_id)

    @property
    def instrument_id(self) -> str:
        return str(self.token_id)


@dataclass(frozen=True, slots=True)
class GameBindingView:
    provider: str
    provider_game_id: str
    event_slug_prefix: str
    binding_status: str
    reason_code: str
    is_tradeable: bool
    targets: tuple[BindingTarget, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class LinkBuildResult:
    provider: str
    run_id: int
    mapping_version: str
    mapping_hash: str
    n_games_seen: int
    n_games_linked: int
    n_games_tradeable: int
    n_targets: int
    n_targets_tradeable: int
    gate_result: str
    report: dict[str, Any]
