"""Native-backed hotpath service adapter over Rust runtime lifecycle APIs."""

from __future__ import annotations

from collections import deque
import json
import os
import threading
from typing import Any, Callable

from polybot2.execution.contracts import OrderRequest
from polybot2.execution.service import FastExecutionService
from polybot2.hotpath.contracts import CompiledPlan, HotPathConfig
from polybot2.hotpath.order_policy import OrderPolicy
from polybot2.hotpath.native_engine import (
    NativeEngineUnavailable,
    NativeHotPathRuntimeBridge,
    serialize_compiled_plan,
)
from polybot2.linking.snapshot import BindingResolver
from polybot2.sports.base import SportsDataProviderBase

SidecarSink = Callable[[dict[str, Any]], None]


class NativeHotPathService:
    """Mandatory native runtime path for live run and runtime benchmark."""

    def __init__(
        self,
        *,
        provider: SportsDataProviderBase,
        execution: FastExecutionService,
        execution_mode: str = "live",
        config: HotPathConfig | None = None,
        binding_resolver: BindingResolver | None = None,
        compiled_plan: CompiledPlan | None = None,
    ):
        self._provider = provider
        self._execution = execution
        self._config = config or HotPathConfig()

        self._running = False
        self._lock = threading.RLock()
        self._plan_lock = threading.RLock()
        self._last_errors: deque[str] = deque(maxlen=50)
        self._sidecar_sinks: list[SidecarSink] = []

        self._subscriptions: list[str] = []
        self._subscription_resolution = {
            "requested_count": 0,
            "resolved_count": 0,
            "missing_count": 0,
        }

        self._compiled_plan: CompiledPlan | None = None

        self._native_order_policy = OrderPolicy()
        self._runtime_bridge: NativeHotPathRuntimeBridge | None = None
        self._pending_presign_templates: list[dict[str, Any]] = []
        self._subscribe_lead_minutes: int = 90
        self._subscription_refresh_seconds: int = 120
        self._execution_mode = (
            "paper"
            if str(execution_mode or "").strip().lower() == "paper"
            else "live"
        )
        self._log_dir: str | None = os.environ.get("POLYBOT2_LOG_DIR")

        self.set_compiled_plan(compiled_plan)

    def register_sidecar_sink(self, sink: SidecarSink) -> None:
        self._sidecar_sinks.append(sink)

    def set_compiled_plan(self, plan: CompiledPlan | None) -> None:
        with self._plan_lock:
            self._compiled_plan = plan

    def set_order_policy(self, policy: OrderPolicy) -> None:
        self._native_order_policy = policy

    def _append_error(self, text: str) -> None:
        self._last_errors.append(str(text))

    def _resolve_subscriptions(self, universal_ids: list[str]) -> list[str]:
        unique = sorted(
            {str(uid or "").strip() for uid in universal_ids if str(uid or "").strip()}
        )
        resolved = list(unique)
        if hasattr(self._provider, "resolve_universal_ids"):
            try:
                resolved = sorted(
                    {
                        str(uid or "").strip()
                        for uid in self._provider.resolve_universal_ids(
                            universal_ids=unique
                        )
                        if str(uid or "").strip()
                    }
                )
            except Exception as exc:
                self._append_error(
                    f"subscription_resolution:{type(exc).__name__}:{exc}"
                )
                resolved = list(unique)
        missing_count = max(0, int(len(unique) - len(resolved)))
        self._subscription_resolution = {
            "requested_count": int(len(unique)),
            "resolved_count": int(len(resolved)),
            "missing_count": int(missing_count),
        }
        return resolved

    def set_subscriptions(self, universal_ids: list[str]) -> None:
        cleaned = sorted({str(uid or "").strip() for uid in universal_ids if str(uid or "").strip()})
        with self._lock:
            self._subscriptions = cleaned
        bridge = self._runtime_bridge
        if bridge is not None:
            try:
                bridge.set_subscriptions(cleaned)
            except Exception as exc:
                self._append_error(f"runtime_set_subscriptions:{type(exc).__name__}:{exc}")

    def set_runtime_timing_policy(
        self,
        *,
        subscribe_lead_minutes: int = 90,
        subscription_refresh_seconds: int = 120,
    ) -> None:
        self._subscribe_lead_minutes = max(0, int(subscribe_lead_minutes))
        self._subscription_refresh_seconds = max(1, int(subscription_refresh_seconds))

    @staticmethod
    def _serialize_template_order(order: OrderRequest) -> dict[str, Any]:
        out: dict[str, Any] = {
            "token_id": str(order.token_id or ""),
            "side": str(order.side or ""),
            "amount_usdc": float(order.amount_usdc),
            "limit_price": float(order.limit_price),
            "time_in_force": str(order.time_in_force or ""),
        }
        if hasattr(order, "size_shares") and order.size_shares is not None:
            out["size_shares"] = float(order.size_shares)
        return out

    def prewarm_presign(self, template_orders: list[OrderRequest]) -> int:
        templates: list[dict[str, Any]] = []
        for req in tuple(template_orders or ()):
            try:
                templates.append(self._serialize_template_order(req))
            except Exception:
                continue
        self._pending_presign_templates = templates
        bridge = self._runtime_bridge
        if bridge is not None:
            try:
                bridge.prewarm_presign(list(self._pending_presign_templates))
            except Exception as exc:
                self._append_error(f"runtime_prewarm_presign:{type(exc).__name__}:{exc}")
        return int(len(self._pending_presign_templates))

    def _runtime_config_payload(self) -> dict[str, Any]:
        provider_cfg = getattr(self._provider, "config", None)
        plan = self._compiled_plan
        provider_name = str(getattr(self._provider, "provider_name", "") or "")
        payload = {
            "subscribe_lead_minutes": int(self._subscribe_lead_minutes),
            "subscription_refresh_seconds": int(self._subscription_refresh_seconds),
            "amount_usdc": float(self._native_order_policy.amount_usdc),
            "size_shares": float(self._native_order_policy.size_shares),
            "limit_price": float(self._native_order_policy.limit_price),
            "time_in_force": str(self._native_order_policy.time_in_force),
            "live_enabled": True,
            "reconnect_sleep_seconds": float(self._config.reconnect_base_sleep_seconds),
            "kalstrop_ws_url": str(getattr(provider_cfg, "ws_url", "") or "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"),
            "kalstrop_client_id": str(getattr(provider_cfg, "client_id", "") or ""),
            "kalstrop_shared_secret_raw": str(
                getattr(provider_cfg, "shared_secret_raw", "") or ""
            ),
            "log_dir": str(self._log_dir) if self._log_dir else ".",
            "run_id": int(plan.run_id) if plan is not None else 0,
            "provider": provider_name,
            "boltodds_api_key": os.getenv("BOLTODDS_API_KEY", ""),
            "boltodds_ws_url": os.getenv("BOLTODDS_WS_URL", "wss://spro.agency/api/livescores"),
        }
        return payload

    def _execution_config_payload(self) -> dict[str, Any]:
        exec_cfg = self._execution.config if hasattr(self._execution, "config") else None
        return {
            "dispatch_mode": ("noop" if self._execution_mode == "paper" else "http"),
            "clob_host": str(getattr(exec_cfg, "clob_host", "") or ""),
            "api_key": str(getattr(exec_cfg, "api_key", "") or ""),
            "api_secret": str(getattr(exec_cfg, "api_secret", "") or ""),
            "api_passphrase": str(getattr(exec_cfg, "api_passphrase", "") or ""),
            "funder": str(getattr(exec_cfg, "funder", "") or ""),
            "signature_type": int(getattr(exec_cfg, "signature_type", 0) or 0),
            "chain_id": int(getattr(exec_cfg, "chain_id", 137) or 137),
            "presign_enabled": bool(getattr(exec_cfg, "presign_enabled", False)),
            "presign_private_key": str(
                getattr(exec_cfg, "presign_private_key", "") or ""
            ),
            "presign_pool_target_per_key": int(
                getattr(exec_cfg, "presign_pool_target_per_key", 1) or 1
            ),
            "presign_startup_warm_timeout_seconds": float(
                getattr(exec_cfg, "presign_startup_warm_timeout_seconds", 5.0) or 5.0
            ),
        }

    def start(self) -> None:
        with self._lock:
            if self._running:
                return

        plan = self._compiled_plan
        if plan is None:
            raise RuntimeError("native hotpath requires compiled plan")

        try:
            bridge = NativeHotPathRuntimeBridge(
                required=bool(self._config.native_engine_required)
            )
            bridge.set_subscriptions(list(self._subscriptions))
            if self._pending_presign_templates:
                bridge.prewarm_presign(list(self._pending_presign_templates))
            bridge.start(
                config_json=json.dumps(
                    self._runtime_config_payload(),
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                ),
                compiled_plan_json=json.dumps(
                    serialize_compiled_plan(plan),
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                ),
                exec_config_json=json.dumps(
                    self._execution_config_payload(),
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                ),
            )
            bridge.set_subscriptions(list(self._subscriptions))
        except NativeEngineUnavailable:
            raise
        except Exception as exc:
            self._append_error(f"runtime_start:{type(exc).__name__}:{exc}")
            raise

        with self._lock:
            self._runtime_bridge = bridge
            self._running = True

    def stop(self) -> None:
        with self._lock:
            bridge = self._runtime_bridge
            self._runtime_bridge = None
            self._running = False
        if bridge is not None:
            try:
                bridge.stop()
            except Exception as exc:
                self._append_error(f"runtime_stop:{type(exc).__name__}:{exc}")

    def health(self) -> dict[str, Any]:
        with self._lock:
            running = bool(self._running)
            subscriptions = list(self._subscriptions)
            subscription_resolution = dict(self._subscription_resolution)
            bridge = self._runtime_bridge
        errors = list(self._last_errors)

        runtime_health: dict[str, Any] = {}
        if bridge is not None:
            try:
                runtime_health = dict(bridge.health_snapshot() or {})
            except Exception as exc:
                self._append_error(f"runtime_health:{type(exc).__name__}:{exc}")

        snap = {
            "running": bool(runtime_health.get("running", running)),
            "execution_mode": str(self._execution_mode),
            "subscriptions": list(runtime_health.get("subscriptions", subscriptions)),
            "subscription_resolution": subscription_resolution,
            "reconnects": int(runtime_health.get("reconnects", 0) or 0),
            "last_error": str(runtime_health.get("last_error", "") or ""),
            "errors": errors,
        }
        return snap

    def apply_incremental_refresh(
        self,
        result: Any,
        order_policy: OrderPolicy | None = None,
    ) -> int:
        if result.new_plan is None or not result.new_targets:
            return 0
        with self._lock:
            bridge = self._runtime_bridge
        if bridge is None:
            self._append_error("apply_incremental_refresh:no_runtime_bridge")
            return 0
        policy = order_policy or self._native_order_policy
        templates: list[dict[str, Any]] = []
        seen_tokens: set[str] = set()
        for target in result.new_targets:
            token_id = str(target.token_id or "").strip()
            if not token_id or token_id in seen_tokens:
                continue
            seen_tokens.add(token_id)
            p = policy.for_market_type(str(target.sports_market_type or ""))
            templates.append({
                "token_id": token_id,
                "side": "buy_yes",
                "amount_usdc": float(p.amount_usdc),
                "size_shares": float(p.size_shares),
                "limit_price": float(p.limit_price),
                "time_in_force": str(p.time_in_force),
            })
            if p.has_secondary:
                templates.append({
                    "token_id": token_id,
                    "side": "buy_yes",
                    "amount_usdc": float(p.secondary_amount_usdc),
                    "size_shares": float(p.secondary_size_shares),
                    "limit_price": float(p.secondary_limit_price),
                    "time_in_force": str(p.secondary_time_in_force),
                })
        plan_json = json.dumps(
            serialize_compiled_plan(result.new_plan),
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
        templates_json = json.dumps(
            templates,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
        try:
            count = bridge.patch_plan(
                compiled_plan_json=plan_json,
                templates_json=templates_json,
            )
            with self._lock:
                self._compiled_plan = result.new_plan
            if result.new_plan:
                new_game_ids = {
                    g.provider_game_id
                    for g in result.new_plan.games
                    if g.provider_game_id
                }
                current_subs = set(self._subscriptions)
                added = new_game_ids - current_subs
                if added:
                    self._subscriptions = sorted(current_subs | added)
                    bridge.set_subscriptions(list(self._subscriptions))
            return count
        except Exception as exc:
            self._append_error(
                f"apply_incremental_refresh:{type(exc).__name__}:{exc}"
            )
            return 0

    def reset_latency_samples(self) -> None:
        # Rust runtime owns latency buckets; this method remains for compatibility.
        return None

__all__ = ["NativeHotPathService"]
