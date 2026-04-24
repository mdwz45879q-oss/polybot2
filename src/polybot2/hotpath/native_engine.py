"""Native MLB hotpath engine bridge (Rust extension wrapper)."""

from __future__ import annotations

import json
from typing import Any

from polybot2.hotpath.contracts import CompiledPlan, HotPathConfig
from polybot2.hotpath.mlb.triggers import MlbOrderPolicy


class NativeEngineUnavailable(RuntimeError):
    """Raised when the required native hotpath module cannot be imported."""


def _import_native_module(*, required: bool):
    try:
        import polybot2_native  # type: ignore

        return polybot2_native
    except Exception as exc:
        if not required:
            return None
        raise NativeEngineUnavailable(
            "polybot2 native hotpath module is required but unavailable. "
            "Build/install /Users/reda/polymarket_bot/polybot2/native/polybot2_native with maturin."
        ) from exc


def serialize_compiled_plan(plan: CompiledPlan | None) -> dict[str, Any]:
    games: list[dict[str, Any]] = []
    if plan is not None:
        for game in tuple(plan.games):
            uid = str(game.provider_game_id or "").strip()
            if not uid:
                continue
            markets: list[dict[str, Any]] = []
            for market in tuple(game.markets):
                targets: list[dict[str, Any]] = []
                for target in tuple(market.targets):
                    targets.append(
                        {
                            "token_id": str(target.token_id or ""),
                            "condition_id": str(target.condition_id or ""),
                            "strategy_key": str(target.strategy_key or ""),
                            "outcome_semantic": str(target.outcome_semantic or ""),
                        }
                    )
                markets.append(
                    {
                        "sports_market_type": str(market.sports_market_type or ""),
                        "line": (None if market.line is None else float(market.line)),
                        "targets": targets,
                    }
                )
            games.append(
                {
                    "provider_game_id": uid,
                    "kickoff_ts_utc": (
                        None if game.kickoff_ts_utc is None else int(game.kickoff_ts_utc)
                    ),
                    "canonical_home_team": str(game.canonical_home_team or ""),
                    "canonical_away_team": str(game.canonical_away_team or ""),
                    "markets": markets,
                }
            )
    return {
        "provider": ("" if plan is None else str(plan.provider or "")),
        "league": ("" if plan is None else str(plan.league or "")),
        "run_id": (0 if plan is None else int(plan.run_id)),
        "games": games,
    }


class NativeMlbEngineBridge:
    """Thin Python bridge over the Rust NativeMlbEngine."""

    def __init__(
        self,
        *,
        config: HotPathConfig,
        order_policy: MlbOrderPolicy,
        required: bool,
    ) -> None:
        mod = _import_native_module(required=required)
        if mod is None:
            raise NativeEngineUnavailable("native module unavailable")
        self._module = mod
        self._engine = mod.NativeMlbEngine(
            dedup_ttl_seconds=float(config.dedup_ttl_seconds),
            decision_cooldown_seconds=float(config.decision_cooldown_seconds),
            decision_debounce_seconds=float(config.decision_debounce_seconds),
            amount_usdc=float(order_policy.amount_usdc),
            size_shares=float(order_policy.size_shares),
            limit_price=float(order_policy.limit_price),
            time_in_force=str(order_policy.time_in_force),
        )

    def load_plan(self, plan: CompiledPlan | None) -> None:
        payload = serialize_compiled_plan(plan)
        self._engine.load_plan(payload)

    def reset_runtime_state(self) -> None:
        self._engine.reset_runtime_state()

    def process_score_event(self, event: Any, *, recv_monotonic_ns: int) -> dict[str, Any]:
        out = self._engine.process_score_event(event, int(recv_monotonic_ns))
        return dict(out) if isinstance(out, dict) else {}

    def process_score_frame(
        self,
        frame: Any,
        *,
        recv_monotonic_ns: int,
        source_recv_monotonic_ns: int,
    ) -> dict[str, Any]:
        out = self._engine.process_score_frame(
            frame,
            int(recv_monotonic_ns),
            int(source_recv_monotonic_ns),
        )
        return dict(out) if isinstance(out, dict) else {}


class NativeHotPathRuntimeBridge:
    """Lifecycle bridge over the Rust NativeHotPathRuntime."""

    def __init__(self, *, required: bool) -> None:
        mod = _import_native_module(required=required)
        if mod is None:
            raise NativeEngineUnavailable("native module unavailable")
        self._module = mod
        runtime_cls = getattr(mod, "NativeHotPathRuntime", None)
        if runtime_cls is None:
            raise NativeEngineUnavailable(
                "polybot2_native is present but missing NativeHotPathRuntime. "
                "Rebuild/install the native module with maturin."
            )
        self._runtime = runtime_cls()

    def start(
        self,
        *,
        config_json: str,
        compiled_plan_json: str,
        exec_config_json: str,
    ) -> None:
        self._runtime.start(
            str(config_json),
            str(compiled_plan_json),
            str(exec_config_json),
        )

    def stop(self) -> None:
        self._runtime.stop()

    def set_subscriptions(self, subscriptions: list[str]) -> None:
        self._runtime.set_subscriptions(list(subscriptions))

    def health_snapshot(self) -> dict[str, Any]:
        out = self._runtime.health_snapshot()
        return dict(out) if isinstance(out, dict) else {}

    def prewarm_presign(self, template_orders: list[dict[str, Any]]) -> int:
        out = self._runtime.prewarm_presign(
            json.dumps(
                list(template_orders),
                separators=(",", ":"),
                sort_keys=True,
                default=str,
            )
        )
        try:
            return int(out)
        except Exception:
            return 0


__all__ = [
    "NativeEngineUnavailable",
    "NativeHotPathRuntimeBridge",
    "NativeMlbEngineBridge",
    "serialize_compiled_plan",
]
