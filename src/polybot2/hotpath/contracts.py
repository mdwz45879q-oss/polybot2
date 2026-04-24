"""Contracts for dedicated low-latency hot path."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import TYPE_CHECKING, Any, Literal, Protocol

if TYPE_CHECKING:
    from polybot2.execution.contracts import OrderState
    from polybot2.linking.contracts import GameBindingView
    from polybot2.sports.contracts import StreamEnvelope


DecisionType = Literal["no_action", "action"]
StreamKind = Literal["scores", "odds"]
OutcomeSemantic = Literal["over", "under", "yes", "no", "home", "away", "unknown"]


@dataclass(frozen=True, slots=True)
class HotPathConfig:
    run_scores: bool = True
    run_odds: bool = False
    read_timeout_seconds: float = 0.05
    decision_cooldown_seconds: float = 0.5
    decision_debounce_seconds: float = 0.1
    dedup_ttl_seconds: float = 2.0
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
        if (v := _get("DECISION_COOLDOWN_SECONDS")) is not None:
            vals["decision_cooldown_seconds"] = float(v)
        if (v := _get("DECISION_DEBOUNCE_SECONDS")) is not None:
            vals["decision_debounce_seconds"] = float(v)
        if (v := _get("DEDUP_TTL_SECONDS")) is not None:
            vals["dedup_ttl_seconds"] = float(v)
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
        if float(self.decision_cooldown_seconds) < 0.0:
            raise ValueError("decision_cooldown_seconds must be >= 0")
        if float(self.decision_debounce_seconds) < 0.0:
            raise ValueError("decision_debounce_seconds must be >= 0")
        if float(self.dedup_ttl_seconds) <= 0.0:
            raise ValueError("dedup_ttl_seconds must be > 0")
        if float(self.reconnect_base_sleep_seconds) <= 0.0:
            raise ValueError("reconnect_base_sleep_seconds must be > 0")


@dataclass(frozen=True, slots=True)
class MatchUpdateTick:
    stream: StreamKind
    universal_id: str
    action: str
    recv_monotonic_ns: int
    source_recv_monotonic_ns: int | None = None
    provider_timestamp: str = ""
    elapsed_time_seconds: int | None = None
    goals_home: int | None = None
    goals_away: int | None = None
    yellow_home: int | None = None
    yellow_away: int | None = None
    red_home: int | None = None
    red_away: int | None = None
    inning_number: int | None = None
    inning_half: str = ""
    outs: int | None = None
    balls: int | None = None
    strikes: int | None = None
    runner_on_first: bool | None = None
    runner_on_second: bool | None = None
    runner_on_third: bool | None = None
    match_completed: bool | None = None
    period: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MatchDeltaEvent:
    universal_id: str
    action: str
    recv_monotonic_ns: int
    stream: StreamKind
    material_change: bool
    goal_delta_home: int = 0
    goal_delta_away: int = 0
    yellow_delta_home: int = 0
    yellow_delta_away: int = 0
    red_delta_home: int = 0
    red_delta_away: int = 0
    outs_delta: int = 0
    balls_delta: int = 0
    strikes_delta: int = 0
    elapsed_delta_seconds: int = 0
    inning_changed: bool = False
    base_state_changed: bool = False
    match_completed_changed: bool = False
    period_changed: bool = False
    raw_tick: MatchUpdateTick | None = None


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


@dataclass(frozen=True, slots=True)
class CompiledPlan:
    provider: str
    league: str
    run_id: int
    plan_hash: str
    compiled_at: int
    games: tuple[CompiledGamePlan, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class OrderIntent:
    strategy_key: str
    token_id: str
    side: str
    amount_usdc: float
    limit_price: float
    time_in_force: str = "FAK"
    expire_ts: int | None = None
    condition_id: str = ""
    client_order_id: str = ""
    source_universal_id: str = ""
    reason: str = ""


@dataclass(frozen=True, slots=True)
class TriggerDecision:
    decision: DecisionType
    strategy_key: str = ""
    reason: str = ""
    intents: tuple[OrderIntent, ...] = field(default_factory=tuple)
    intent: OrderIntent | None = None

    def __post_init__(self) -> None:
        intents = tuple(self.intents or ())
        if self.intent is not None and not intents:
            intents = (self.intent,)
        object.__setattr__(self, "intents", intents)
        if self.intent is None and intents:
            object.__setattr__(self, "intent", intents[0])


@dataclass(frozen=True, slots=True)
class OrderTaskState:
    strategy_key: str
    client_order_id: str
    exchange_order_id: str
    status: str
    updated_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class GameContext:
    universal_id: str
    now_monotonic_ns: int
    binding_view: "GameBindingView | None" = None
    compiled_game: CompiledGamePlan | None = None
    state: dict[str, Any] | None = None


class UpdateAdapter(Protocol):
    def from_envelope(self, env: "StreamEnvelope", *, recv_monotonic_ns: int) -> MatchUpdateTick | None:
        raise NotImplementedError

    def from_fast_event(self, *, stream: StreamKind, event: Any, recv_monotonic_ns: int) -> MatchUpdateTick | None:
        raise NotImplementedError


class StateReducer(Protocol):
    def on_tick(self, tick: MatchUpdateTick, delta_event: MatchDeltaEvent) -> dict[str, Any] | None:
        raise NotImplementedError


class TriggerEngine(Protocol):
    def evaluate(self, delta_event: MatchDeltaEvent, game_context: GameContext) -> TriggerDecision:
        raise NotImplementedError


class IntentRouter(Protocol):
    def route(self, *, intent: OrderIntent, event_ns: int) -> "OrderState | None":
        raise NotImplementedError


__all__ = [
    "CompiledGamePlan",
    "CompiledMarket",
    "CompiledPlan",
    "CompiledTarget",
    "DecisionType",
    "GameContext",
    "HotPathConfig",
    "IntentRouter",
    "MatchDeltaEvent",
    "MatchUpdateTick",
    "OrderIntent",
    "OrderTaskState",
    "OutcomeSemantic",
    "StateReducer",
    "StreamKind",
    "TriggerDecision",
    "TriggerEngine",
    "UpdateAdapter",
]
