"""Dedicated low-latency hot-path runtime."""

from polybot2.hotpath.compiler import (
    HotPathPlanError,
    ScopedLaunchCheck,
    compile_hotpath_plan,
    evaluate_hotpath_scope,
)
from polybot2.hotpath.contracts import (
    CompiledGamePlan,
    CompiledMarket,
    CompiledPlan,
    CompiledTarget,
    HotPathConfig,
    MatchDeltaEvent,
    OrderIntent,
)
from polybot2.hotpath.order_policy import OrderPolicy
from polybot2.hotpath.native_service import NativeHotPathService
from polybot2.hotpath.incremental import (
    IncrementalRefreshResult,
    discover_new_markets,
    discover_new_markets_sync,
)

__all__ = [
    "CompiledGamePlan",
    "CompiledMarket",
    "CompiledPlan",
    "CompiledTarget",
    "HotPathConfig",
    "HotPathPlanError",
    "IncrementalRefreshResult",
    "OrderPolicy",
    "MatchDeltaEvent",
    "NativeHotPathService",
    "OrderIntent",
    "ScopedLaunchCheck",
    "compile_hotpath_plan",
    "discover_new_markets",
    "discover_new_markets_sync",
    "evaluate_hotpath_scope",
]
