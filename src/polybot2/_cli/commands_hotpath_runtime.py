"""Hotpath runtime/replay command handlers."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
import signal
import time
from typing import Any

from polybot2._cli.common import _apply_env_uid_filter
from polybot2._cli.common import _build_hotpath_template_orders
from polybot2._cli.common import _hotpath_order_policy_for_league as _common_hotpath_order_policy_for_league
from polybot2._cli.common import _hotpath_runtime_policy_for_league as _common_hotpath_runtime_policy_for_league
from polybot2._cli.common import _int_or_none
from polybot2._cli.common import _runtime_from_args
from polybot2._cli.common import _scope_provider_catalog_to_league
from polybot2.execution import FastExecutionConfig
from polybot2.execution import FastExecutionService as _FastExecutionService
from polybot2.hotpath import HotPathConfig
from polybot2.hotpath import HotPathPlanError
from polybot2.hotpath import NativeHotPathService as _NativeHotPathService
from polybot2.hotpath import compile_hotpath_plan
from polybot2.linking import BindingResolver
from polybot2.linking import load_live_trading_policy as _load_live_trading_policy
from polybot2.data import open_database
from polybot2.sports import build_sports_provider as _build_sports_provider

# Patchable dependency hooks for tests.
FastExecutionService = _FastExecutionService
NativeHotPathService = _NativeHotPathService
build_sports_provider = _build_sports_provider
load_live_trading_policy = _load_live_trading_policy
_hotpath_order_policy_for_league = _common_hotpath_order_policy_for_league
_hotpath_runtime_policy_for_league = _common_hotpath_runtime_policy_for_league

def run_hotpath_observe(args: Any, *, logger: logging.Logger) -> int:
    try:
        from polybot2.hotpath.live_observer import LiveObserver, find_latest_log

        log_file = str(getattr(args, "log_file", "") or "").strip()
        if not log_file:
            log_dir = str(getattr(args, "log_dir", "") or "").strip()
            if not log_dir:
                log_dir = os.environ.get("POLYBOT2_LOG_DIR", ".")
            run_id = getattr(args, "run_id", None)
            log_file = find_latest_log(log_dir, run_id=run_id) or ""
        if not log_file or not os.path.isfile(log_file):
            logger.error("no hotpath log file found (use --log-file or set POLYBOT2_LOG_DIR)")
            return 1
        logger.info("observing log file: %s", log_file)

        # Try to load compiled plan for team name resolution
        compiled_plan = None
        try:
            link_run_id = getattr(args, "link_run_id", None)
            league_key = str(getattr(args, "league", "mlb") or "mlb").strip().lower()
            if link_run_id is not None:
                from polybot2.hotpath.compiler import compile_hotpath_plan
                from polybot2.linking import load_mapping as _load_mapping_obs
                runtime = _runtime_from_args(args)
                mapping_obs = _load_mapping_obs()
                obs_league_cfg = mapping_obs.leagues.get(league_key, {})
                obs_provider = str(obs_league_cfg.get("provider", "kalstrop_v1")).strip().lower()
                with open_database(runtime) as db:
                    compiled_plan = compile_hotpath_plan(
                        db=db,
                        provider=obs_provider,
                        league=league_key,
                        run_id=int(link_run_id),
                    )
                logger.info("loaded plan: %d games", len(compiled_plan.games))
        except Exception as exc:
            logger.debug("could not load compiled plan: %s", exc)

        observer = LiveObserver(log_path=log_file, compiled_plan=compiled_plan)
        observer.run()
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        logger.error("hotpath observe failed: %s: %s", type(exc).__name__, exc)
        return 1


def _load_dotenv(logger: logging.Logger) -> None:
    """Load .env file if present. Does not override existing env vars."""
    from pathlib import Path
    env_file = Path(".env")
    if not env_file.exists():
        return
    loaded = 0
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
            loaded += 1
    if loaded:
        logger.info("loaded %d env vars from .env", loaded)


def run_hotpath_live(args: Any, *, logger: logging.Logger) -> int:
    """Run the hotpath with incremental market refresh.

    Prerequisites (run before this command):
      polybot2 market sync
      polybot2 provider sync
      polybot2 link build --league-scope live
      polybot2 link review --run-id <N>

    Startup: compile plan from the approved link run → presign → start.

    Refresh loop (no restart): targeted Gamma API fetch for known
    event IDs → diff → hot-patch new targets into the running engine.
    """
    from polybot2.hotpath.incremental import discover_new_markets_sync
    from polybot2.linking import load_mapping as _load_mapping

    _load_dotenv(logger)

    live_policy = load_live_trading_policy()
    league_key = str(getattr(args, "league", "") or "").strip().lower()
    if not league_key:
        logger.error("--league is required")
        return 1
    mapping = _load_mapping()
    league_cfg = mapping.leagues.get(league_key, {})
    provider_name = str(league_cfg.get("provider", "")).strip().lower()
    if not provider_name:
        logger.error("league %s has no provider configured in mappings.py", league_key)
        return 1
    run_id = _int_or_none(getattr(args, "link_run_id", None))
    if run_id is None:
        with open_database(_runtime_from_args(args)) as _db:
            latest = _db.linking.load_latest_link_run_for_league(league=league_key)
        if latest is None:
            logger.error("no link run found for league=%s; run 'link build' first or pass --link-run-id", league_key)
            return 1
        run_id = int(latest["run_id"])
        logger.info("using latest link run: run_id=%d league=%s", run_id, league_key)
    execution_mode = str(getattr(args, "execution_mode", "live") or "live").strip().lower()
    runtime = _runtime_from_args(args)
    runtime_policy = _hotpath_runtime_policy_for_league(live_policy=live_policy, league_key=league_key)
    cli_refresh = getattr(args, "refresh_interval", None)
    config_refresh = int(runtime_policy.get("refresh_interval_seconds", 300))
    refresh_interval = int(cli_refresh) if cli_refresh is not None else config_refresh
    order_policy, require_presign, _ = _hotpath_order_policy_for_league(
        live_policy=live_policy, league_key=league_key,
    )

    stop_requested = False

    def _handle_signal(_sig: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    prev_int = signal.signal(signal.SIGINT, _handle_signal)
    prev_term = signal.signal(signal.SIGTERM, _handle_signal)
    iteration = 0

    hotpath: _NativeHotPathService | None = None
    try:
        # --- Compile plan and start hotpath ---
        with open_database(runtime) as db:
            compiled_plan = compile_hotpath_plan(
                db=db,
                provider=provider_name,
                league=league_key,
                run_id=run_id,
                live_policy=live_policy,
                now_ts_utc=int(time.time()),
                plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
            )
            resolver = BindingResolver(db=db)
            resolver.reload()

        try:
            prov = build_sports_provider(provider_name=provider_name, logger=logger)
        except ValueError as exc:
            logger.error("provider init failed: %s", exc)
            return 1
        _scope_provider_catalog_to_league(
            provider=prov, provider_name=provider_name, league_key=league_key,
        )

        exec_cfg_overrides: dict[str, Any] = {}
        if require_presign:
            exec_cfg_overrides["presign_enabled"] = True
        exec_cfg = FastExecutionConfig.from_env(exec_cfg_overrides)
        exec_service = FastExecutionService(config=exec_cfg)

        hp_cfg = HotPathConfig(
            run_scores=True, run_odds=False, read_timeout_seconds=0.05,
            native_engine_enabled=True, native_engine_required=True,
        )
        hotpath = NativeHotPathService(
            provider=prov, execution=exec_service,
            execution_mode=execution_mode, config=hp_cfg,
            binding_resolver=resolver,
        )
        hotpath.set_compiled_plan(compiled_plan)
        hotpath.set_order_policy(order_policy)

        plan_uids = [
            str(g.provider_game_id) for g in tuple(compiled_plan.games)
            if str(g.provider_game_id or "").strip()
        ]
        env_uids = [
            x.strip()
            for x in str(os.getenv("POLYBOT2_SUBSCRIBE_UNIVERSAL_IDS") or "").split(",")
            if x.strip()
        ]
        wanted = _apply_env_uid_filter(uids=sorted(set(plan_uids)), env_uids=env_uids)

        if hasattr(hotpath, "set_runtime_timing_policy"):
            hotpath.set_runtime_timing_policy(
                subscribe_lead_minutes=int(runtime_policy.get("subscribe_lead_minutes", 90)),
                subscription_refresh_seconds=int(runtime_policy.get("subscription_refresh_seconds", 120)),
            )
        hotpath.set_subscriptions(wanted)

        template_orders = _build_hotpath_template_orders(
            compiled_plan=compiled_plan, order_policy=order_policy,
        )
        if template_orders and hasattr(hotpath, "prewarm_presign"):
            hotpath.prewarm_presign(template_orders)

        n_targets = sum(len(m.targets) for g in compiled_plan.games for m in g.markets)
        logger.info(
            "hotpath starting: run_id=%d games=%d targets=%d subs=%d refresh=%ds",
            run_id, len(compiled_plan.games), n_targets, len(wanted), refresh_interval,
        )
        hotpath.start()

        # --- Incremental refresh loop ---
        while not stop_requested:
            for _ in range(refresh_interval):
                if stop_requested:
                    break
                time.sleep(1.0)
            if stop_requested:
                break

            iteration += 1
            try:
                with open_database(runtime) as db:
                    result = discover_new_markets_sync(
                        current_plan=hotpath._compiled_plan,
                        db=db,
                        live_policy=live_policy,
                        plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                    )
            except Exception as exc:
                logger.warning("incremental refresh failed (continuing): %s: %s", type(exc).__name__, exc)
                continue

            if result.new_targets:
                count = hotpath.apply_incremental_refresh(result, order_policy)
                logger.info(
                    "hot-patch applied: cycle=%d new_markets=%d new_targets=%d presigned=%d",
                    iteration, result.markets_discovered, len(result.new_targets), count,
                )
            else:
                logger.info("incremental refresh cycle=%d: no new markets (events_fetched=%d)", iteration, result.events_fetched)

    except HotPathPlanError as exc:
        logger.error("plan compile failed: code=%s message=%s", exc.code, exc)
        return 1
    except Exception as exc:
        logger.error("startup failed: %s: %s", type(exc).__name__, exc)
        return 1
    finally:
        if hotpath is not None:
            try:
                hotpath.stop()
            except Exception:
                pass
        signal.signal(signal.SIGINT, prev_int)
        signal.signal(signal.SIGTERM, prev_term)

    logger.info("orchestrator stopped after %d refresh cycles", iteration)
    return 0


__all__ = [
    "run_hotpath_live",
    "run_hotpath_observe",
    "FastExecutionService",
    "NativeHotPathService",
    "build_sports_provider",
    "load_live_trading_policy",
    "_hotpath_order_policy_for_league",
    "_hotpath_runtime_policy_for_league",
]
