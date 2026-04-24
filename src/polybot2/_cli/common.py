"""Shared CLI helpers."""

from __future__ import annotations

import io
import logging
import os
from typing import Any

from polybot2.data import DataRuntimeConfig
from polybot2.execution import OrderRequest
from polybot2.hotpath import MlbOrderPolicy
from polybot2.linking import load_live_trading_policy
from polybot2.linking.normalize import sport_key_for_league

try:  # optional dependency in local env
    from rich import box
    from rich.console import Console
    from rich.table import Table

    _RICH_AVAILABLE = True
except Exception:  # pragma: no cover - fallback if rich is absent
    _RICH_AVAILABLE = False

def _runtime_from_args(args: Any) -> DataRuntimeConfig:
    overrides: dict[str, Any] = {}
    if str(getattr(args, "db", "")).strip():
        overrides["db_path"] = str(args.db).strip()
    return DataRuntimeConfig.from_env(overrides)


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


_VALID_PROVIDERS = {"boltodds", "kalstrop"}


def _resolve_provider_name(
    *,
    args: Any,
    logger: logging.Logger,
    context: str,
    live_policy: Any | None = None,
) -> str | None:
    explicit = str(getattr(args, "provider", "")).strip().lower()
    if explicit:
        if explicit not in _VALID_PROVIDERS:
            logger.error("%s supports only provider=boltodds|kalstrop", context)
            return None
        return explicit

    policy = live_policy or load_live_trading_policy()
    default_provider = str(getattr(policy, "default_provider", "") or "").strip().lower() or "kalstrop"
    if default_provider not in _VALID_PROVIDERS:
        logger.error(
            "invalid DEFAULT_PROVIDER=%s (must be boltodds|kalstrop)",
            str(getattr(policy, "default_provider", "") or ""),
        )
        return None
    logger.info("%s provider defaulted to %s", context, default_provider)
    return default_provider


def _parse_int_list(text: str, *, default: list[int]) -> list[int]:
    src = str(text or "").strip()
    if not src:
        return list(default)
    vals: list[int] = []
    for part in src.replace(";", ",").split(","):
        p = str(part).strip()
        if not p:
            continue
        vals.append(int(p))
    return [v for v in vals if v > 0] or list(default)


def _hotpath_order_policy_for_league(*, live_policy: Any, league_key: str) -> tuple[MlbOrderPolicy, bool, bool]:
    cfg = dict((getattr(live_policy, "hotpath_execution_by_league", {}) or {}).get(str(league_key), {}) or {})
    return (
        MlbOrderPolicy(
            amount_usdc=float(cfg.get("amount_usdc", 5.0)),
            limit_price=float(cfg.get("limit_price", 0.52)),
            time_in_force=str(cfg.get("time_in_force", "FAK") or "FAK"),
        ),
        bool(cfg.get("require_presign", True)),
        bool(cfg.get("presign_fallback_on_miss", False)),
    )


def _hotpath_runtime_policy_for_league(*, live_policy: Any, league_key: str) -> dict[str, int]:
    cfg = dict((getattr(live_policy, "hotpath_runtime_by_league", {}) or {}).get(str(league_key), {}) or {})
    refresh_seconds = int(cfg.get("subscription_refresh_seconds", cfg.get("reload_interval_seconds", 120)))
    return {
        "plan_horizon_hours": int(cfg.get("plan_horizon_hours", 24)),
        "subscribe_lead_minutes": int(cfg.get("subscribe_lead_minutes", 90)),
        "unsubscribe_grace_minutes": int(cfg.get("unsubscribe_grace_minutes", 15)),
        "provider_catalog_max_age_seconds": int(cfg.get("provider_catalog_max_age_seconds", 600)),
        # Backward compatible: older policy uses reload_interval_seconds.
        "subscription_refresh_seconds": refresh_seconds,
    }


def _apply_env_uid_filter(*, uids: list[str], env_uids: list[str]) -> list[str]:
    if not env_uids:
        return sorted(set(str(x) for x in uids if str(x).strip()))
    return sorted(set(str(x) for x in uids if str(x).strip()).intersection(set(env_uids)))


def _build_hotpath_template_orders(*, compiled_plan: Any, order_policy: MlbOrderPolicy) -> list[OrderRequest]:
    out: list[OrderRequest] = []
    seen: set[tuple[str, str, float, float, str, str]] = set()
    for game in tuple(compiled_plan.games):
        for market in tuple(game.markets):
            for target in tuple(market.targets):
                token_id = str(target.token_id or "").strip()
                if not token_id:
                    continue
                key = (
                    token_id,
                    "buy_yes",
                    float(order_policy.amount_usdc),
                    float(order_policy.limit_price),
                    str(order_policy.time_in_force),
                    str(target.condition_id or ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    OrderRequest(
                        token_id=token_id,
                        side="buy_yes",
                        notional_usdc=float(order_policy.amount_usdc),
                        limit_price=float(order_policy.limit_price),
                        time_in_force=str(order_policy.time_in_force),
                        client_order_id=f"hp_template_{len(out) + 1}",
                        condition_id=str(target.condition_id or ""),
                    )
                )
    return out


def _scope_provider_catalog_to_league(*, provider: Any, provider_name: str, league_key: str) -> None:
    if str(provider_name or "").strip().lower() != "kalstrop":
        return
    cfg = getattr(provider, "config", None)
    if cfg is None or not hasattr(cfg, "catalog_sport_codes"):
        return
    sport_code = str(sport_key_for_league(str(league_key or "")) or "").strip().lower()
    if not sport_code:
        return
    try:
        current = tuple(
            sorted(
                {
                    str(x or "").strip().lower().replace("-", "_")
                    for x in tuple(getattr(cfg, "catalog_sport_codes", ()) or ())
                    if str(x or "").strip()
                }
            )
        )
    except Exception:
        current = tuple()
    desired = (sport_code,)
    if current == desired:
        return
    try:
        setattr(cfg, "catalog_sport_codes", desired)
    except Exception:
        return


def _env_bool(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw not in {"0", "false", "no", "off"}


def _render_table(*, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "(none)"
    if _RICH_AVAILABLE:
        table = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan")
        for _, title in columns:
            table.add_column(str(title), overflow="fold")
        for row in rows:
            table.add_row(*[str(row.get(key, "") if row.get(key, "") is not None else "") for key, _ in columns])
        buf = io.StringIO()
        Console(file=buf, force_terminal=True, color_system="standard", width=160).print(table)
        return buf.getvalue().rstrip()
    widths: list[int] = []
    for key, title in columns:
        max_val = max((len(str(r.get(key, "") if r.get(key, "") is not None else "")) for r in rows), default=0)
        widths.append(max(len(title), max_val))
    header = " | ".join(title.ljust(widths[i]) for i, (_, title) in enumerate(columns))
    sep = "-+-".join("-" * widths[i] for i in range(len(columns)))
    body = []
    for row in rows:
        body.append(
            " | ".join(
                str(row.get(key, "") if row.get(key, "") is not None else "").ljust(widths[i])
                for i, (key, _) in enumerate(columns)
            )
        )
    return "\n".join([header, sep, *body])


def _color_enabled() -> bool:
    return os.getenv("NO_COLOR", "").strip() == ""


def _color(text: str, code: str) -> str:
    if not _color_enabled():
        return text
    return f"\033[{code}m{text}\033[0m"


__all__ = [
    "_runtime_from_args",
    "_int_or_none",
    "_resolve_provider_name",
    "_parse_int_list",
    "_hotpath_order_policy_for_league",
    "_hotpath_runtime_policy_for_league",
    "_apply_env_uid_filter",
    "_build_hotpath_template_orders",
    "_scope_provider_catalog_to_league",
    "_env_bool",
    "_render_table",
    "_color_enabled",
    "_color",
]
