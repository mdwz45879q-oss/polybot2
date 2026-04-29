"""Hotpath runtime/replay command handlers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
import signal
import time
from typing import Any

from polybot2._cli.commands_data_provider import _dedup_ids
from polybot2._cli.common import _apply_env_uid_filter
from polybot2._cli.common import _build_hotpath_template_orders
from polybot2._cli.common import _hotpath_order_policy_for_league as _common_hotpath_order_policy_for_league
from polybot2._cli.common import _hotpath_runtime_policy_for_league as _common_hotpath_runtime_policy_for_league
from polybot2._cli.common import _int_or_none
from polybot2._cli.common import _render_table
from polybot2._cli.common import _resolve_provider_name
from polybot2._cli.common import _runtime_from_args
from polybot2._cli.common import _scope_provider_catalog_to_league
from polybot2.execution import FastExecutionConfig
from polybot2.execution import FastExecutionService as _FastExecutionService
from polybot2.hotpath import HotPathConfig
from polybot2.hotpath import HotPathPlanError
from polybot2.hotpath import MlbOrderPolicy
from polybot2.hotpath import NativeHotPathService as _NativeHotPathService
from polybot2.hotpath import ReplayConfig
from polybot2.hotpath import compile_hotpath_plan
from polybot2.hotpath import evaluate_hotpath_scope
from polybot2.hotpath import run_hotpath_replay as run_hotpath_replay_api
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

def run_hotpath(args: Any, *, logger: logging.Logger) -> int:
    live_policy = load_live_trading_policy()
    provider_name = _resolve_provider_name(args=args, logger=logger, context="hotpath run", live_policy=live_policy)
    if provider_name is None:
        return 1
    league_key = " ".join(str(getattr(args, "league", "")).strip().lower().split())
    if not league_key:
        logger.error("--league is required")
        return 1
    execution_mode = str(getattr(args, "execution_mode", "live") or "live").strip().lower()
    if execution_mode not in {"live", "paper"}:
        logger.error("--execution-mode must be one of {'live','paper'}")
        return 1

    runtime = _runtime_from_args(args)
    link_run_id = _int_or_none(getattr(args, "link_run_id", None))
    approve_link_run = _int_or_none(getattr(args, "approve_link_run", None))
    if link_run_id is None:
        link_run_id = approve_link_run
        if approve_link_run is not None:
            logger.warning("--approve-link-run is deprecated; use --link-run-id")
    if link_run_id is None:
        logger.error("--link-run-id is required")
        return 1
    force_launch = bool(getattr(args, "force_launch", False))
    selected_run_id: int | None = None
    blockers: list[str] = []
    preflight_msg = ""
    blocker_csv = ""
    runtime_policy = _hotpath_runtime_policy_for_league(live_policy=live_policy, league_key=league_key)
    with open_database(runtime) as db:
        resolver = BindingResolver(db=db)
        resolver.reload()
        scope = evaluate_hotpath_scope(
            db=db,
            provider=provider_name,
            league=league_key,
            run_id=int(link_run_id),
            live_policy=live_policy,
            now_ts_utc=int(time.time()),
        )
        selected_run_id = int(scope.run_id)
        blockers = list(scope.blockers)
        progress = {
            "provider": scope.provider,
            "league": scope.league,
            "run_id": int(scope.run_id),
            "total_in_scope": int(scope.in_scope_games),
            "n_approved": int(scope.approved_games),
            "n_rejected": int(scope.rejected_games),
            "n_skipped": int(scope.skipped_games),
            "n_pending": int(scope.pending_games),
            "all_approved": bool(
                scope.in_scope_games > 0
                and scope.approved_games == scope.in_scope_games
                and scope.rejected_games == 0
                and scope.skipped_games == 0
                and scope.pending_games == 0
            ),
            "approved_game_ids": list(scope.approved_game_ids),
            "tradeable_targets": int(scope.tradeable_targets),
        }
        gate_result = "pass" if not blockers else "fail"
        n_unresolved_games = int(scope.pending_games + scope.rejected_games + scope.skipped_games)
        blocked = bool(blockers) and not force_launch
        blocker_csv = ",".join(blockers)
        preflight_msg = (
            f"provider={provider_name} league={league_key} run_id={selected_run_id} in_scope_count={scope.in_scope_games} "
            f"gate={gate_result} unresolved_games={n_unresolved_games} "
            f"approved={progress.get('n_approved')} rejected={progress.get('n_rejected')} skipped={progress.get('n_skipped')} "
            f"pending={progress.get('n_pending')} total={progress.get('total_in_scope')} blockers={blocker_csv or 'none'}"
        )
        db.linking.insert_launch_audit(
            run_id=selected_run_id,
            provider=provider_name,
            approved_run_id=selected_run_id,
            gate_result=gate_result,
            unresolved_games=n_unresolved_games,
            decision_progress=progress,
            force_launch=force_launch,
            blocked=blocked,
            message=preflight_msg,
            created_at=int(time.time()),
            commit=True,
        )
        compiled_plan = None
        if not blocked:
            try:
                compiled_plan = compile_hotpath_plan(
                    db=db,
                    provider=provider_name,
                    league=league_key,
                    run_id=int(selected_run_id),
                    live_policy=live_policy,
                    require_all_approved=not force_launch,
                    include_all_scope_games=force_launch,
                    now_ts_utc=int(time.time()),
                    plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                )
            except HotPathPlanError as exc:
                logger.error("hotpath plan compile failed: code=%s message=%s", exc.code, exc)
                return 1

        logger.info("link review preflight v2: %s", preflight_msg)
        if bool(blockers) and not force_launch:
            approve_hint = str(selected_run_id) if selected_run_id is not None else "<link_run_id>"
            logger.error(
                "hotpath launch blocked by link review v2: %s; resolve decisions and re-run with --link-run-id %s, or bypass with --force-launch",
                blocker_csv,
                approve_hint,
            )
            return 1
        if force_launch and blockers:
            logger.warning("hotpath launch proceeding due to --force-launch override; blockers=%s", blocker_csv)
        if compiled_plan is None:
            logger.error("compiled hotpath plan is unavailable")
            return 1
        if league_key != "mlb":
            logger.error("no trigger plugin implemented for league=%s", league_key)
            return 1

        try:
            provider = build_sports_provider(provider_name=provider_name, logger=logger)
        except ValueError as exc:
            reason = str(exc)
            if reason == "missing_BOLTODDS_API_KEY":
                logger.error("BOLTODDS_API_KEY is required for hotpath run")
            elif reason == "missing_kalstrop_credentials":
                logger.error(
                    "Kalstrop credentials are required for hotpath run (KALSTROP_* or legacy CLIENT_ID/SHARED_SECRET_RAW)"
                )
            else:
                logger.error("hotpath provider initialization failed: %s", reason)
            return 1
        _scope_provider_catalog_to_league(
            provider=provider,
            provider_name=provider_name,
            league_key=league_key,
        )
        order_policy, require_presign, _policy_presign_fallback_on_miss = _hotpath_order_policy_for_league(
            live_policy=live_policy,
            league_key=league_key,
        )
        exec_cfg_overrides: dict[str, Any] = {}
        if require_presign:
            exec_cfg_overrides["presign_enabled"] = True
        exec_cfg = FastExecutionConfig.from_env(exec_cfg_overrides)
        if execution_mode == "live":
            if require_presign and not bool(exec_cfg.presign_enabled):
                logger.error("hotpath requires presign-enabled execution for league=%s", league_key)
                return 1
            if require_presign and not str(exec_cfg.presign_private_key or "").strip():
                logger.error("hotpath requires POLY_EXEC_PRESIGN_PRIVATE_KEY when presign is required")
                return 1
        else:
            logger.warning(
                "hotpath run execution_mode=paper enabled; live order dispatch is disabled (noop dispatch)"
            )
        exec_service = FastExecutionService(config=exec_cfg)

        hp_cfg = HotPathConfig(
            run_scores=True,
            run_odds=False,
            read_timeout_seconds=float(getattr(args, "read_timeout_seconds", 0.05) or 0.05),
            profiling_enabled=bool(getattr(args, "profile_latency", False)),
            native_engine_enabled=True,
            native_engine_required=True,
        )
        try:
            hotpath = NativeHotPathService(
                provider=provider,
                execution=exec_service,
                execution_mode=execution_mode,
                config=hp_cfg,
                binding_resolver=resolver,
            )
        except TypeError:
            # Backward-compat for patched test doubles that do not accept execution_mode.
            hotpath = NativeHotPathService(
                provider=provider,
                execution=exec_service,
                config=hp_cfg,
                binding_resolver=resolver,
            )
        if hasattr(hotpath, "set_compiled_plan"):
            hotpath.set_compiled_plan(compiled_plan)
        if hasattr(hotpath, "set_order_policy"):
            hotpath.set_order_policy(order_policy)

        plan_uids = [str(g.provider_game_id) for g in tuple(compiled_plan.games) if str(g.provider_game_id or "").strip()]
        env_uids = [
            x.strip()
            for x in str(os.getenv("POLYBOT2_SUBSCRIBE_UNIVERSAL_IDS") or "").split(",")
            if x.strip()
        ]
        wanted = _apply_env_uid_filter(uids=sorted(set(plan_uids)), env_uids=env_uids)
        if env_uids:
            logger.info(
                "hotpath subscriptions filtered by POLYBOT2_SUBSCRIBE_UNIVERSAL_IDS: compiled=%d env=%d selected=%d",
                len(plan_uids),
                len(env_uids),
                len(wanted),
            )
        if hasattr(hotpath, "set_runtime_timing_policy"):
            hotpath.set_runtime_timing_policy(
                subscribe_lead_minutes=int(runtime_policy.get("subscribe_lead_minutes", 90)),
                subscription_refresh_seconds=int(runtime_policy.get("subscription_refresh_seconds", 120)),
            )
        hotpath.set_subscriptions(wanted)
        current_snapshot = hotpath.health() if hasattr(hotpath, "health") else {}
        current_subscriptions = list(current_snapshot.get("subscriptions", wanted)) if isinstance(current_snapshot, dict) else list(wanted)
        if wanted:
            missing_startup = sorted(set(wanted) - set(current_subscriptions))
            if missing_startup:
                logger.warning(
                    "hotpath subscription resolution shrink at startup: provider=%s requested=%d resolved=%d missing=%d",
                    provider_name,
                    len(wanted),
                    len(current_subscriptions),
                    len(missing_startup),
                )
            if not current_subscriptions:
                logger.error(
                    "hotpath startup blocked: in-window candidates exist but provider resolved none (provider=%s league=%s run_id=%s timed_candidates=%d selected=%d)",
                    provider_name,
                    league_key,
                    selected_run_id,
                    len(plan_uids),
                    len(wanted),
                )
                return 1
        if current_subscriptions:
            logger.info("hotpath subscriptions=%d", len(current_subscriptions))
        else:
            logger.info(
                "hotpath subscriptions=0 (provider=%s league=%s run_id=%s); waiting for games to enter subscription window",
                provider_name,
                league_key,
                int(selected_run_id),
            )

        template_orders = _build_hotpath_template_orders(compiled_plan=compiled_plan, order_policy=order_policy)
        if template_orders and hasattr(hotpath, "prewarm_presign"):
            hotpath.prewarm_presign(template_orders)
            logger.info("hotpath presign templates prewarmed=%d", len(template_orders))

        matchup_by_game_id: dict[str, str] = {}
        if compiled_plan is not None:
            for game in tuple(compiled_plan.games):
                gid = str(game.provider_game_id or "").strip()
                if gid:
                    from polybot2.hotpath.observe import build_matchup_label as _build_matchup
                    matchup_by_game_id[gid] = _build_matchup(
                        str(game.canonical_home_team or ""),
                        str(game.canonical_away_team or ""),
                    )

        monitor = None
        if bool(getattr(args, "with_observe", False)):
            from polybot2.hotpath.observe import HotpathInlineMonitor, MonitorConfig
            monitor = HotpathInlineMonitor(
                logger=logger,
                config=MonitorConfig(refresh_seconds=5.0, max_games=40),
                matchup_by_game_id=matchup_by_game_id,
            )

        stop = False

        def _handle(_sig: int, _frame: Any) -> None:
            nonlocal stop
            stop = True

        prev_int = signal.signal(signal.SIGINT, _handle)
        prev_term = signal.signal(signal.SIGTERM, _handle)
        started_at = datetime.now(timezone.utc).isoformat()
        try:
            if monitor is not None:
                monitor.start()
                import os as _os
                _sock_path = "/tmp/polybot2_hotpath_telemetry.sock"
                logger.info("socket after monitor.start(): exists=%s", _os.path.exists(_sock_path))
            hotpath.start()
            logger.info("hotpath started at %s (Ctrl+C to stop)", started_at)
            while not stop:
                time.sleep(1.0)
        except Exception as exc:
            logger.error("hotpath run failed: %s: %s", type(exc).__name__, exc)
            return 1
        finally:
            if monitor is not None:
                try:
                    monitor.stop()
                except Exception:
                    pass
            try:
                hotpath.stop()
            except Exception:
                pass
            signal.signal(signal.SIGINT, prev_int)
            signal.signal(signal.SIGTERM, prev_term)
    logger.info("hotpath stopped")
    return 0


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
                db_path = str(getattr(args, "db", "") or "").strip()
                if not db_path:
                    db_path = os.environ.get("POLYBOT2_DB_PATH", "")
                if True:
                    from polybot2.hotpath.compiler import compile_hotpath_plan
                    runtime = _runtime_from_args(args)
                    with open_database(runtime) as db:
                        compiled_plan = compile_hotpath_plan(
                            db=db,
                            provider="kalstrop",
                            league=league_key,
                            run_id=int(link_run_id),
                            require_all_approved=False,
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


def run_hotpath_replay(args: Any, *, logger: logging.Logger) -> int:
    provider_name = _resolve_provider_name(args=args, logger=logger, context="hotpath replay")
    if provider_name is None:
        return 1
    league_key = " ".join(str(getattr(args, "league", "")).strip().lower().split())
    if not league_key:
        logger.error("--league is required")
        return 1
    run_id = _int_or_none(getattr(args, "link_run_id", None))
    if run_id is None:
        logger.error("--link-run-id is required")
        return 1
    capture_manifest = str(getattr(args, "capture_manifest", "") or "").strip()
    if not capture_manifest:
        logger.error("--capture-manifest is required")
        return 1
    mode = str(getattr(args, "mode", "as_fast") or "as_fast").strip().lower()
    speed_multiplier = float(getattr(args, "speed_multiplier", 1.0) or 1.0)
    out_dir = str(getattr(args, "out", "") or "").strip()
    fmt = str(getattr(args, "format", "table") or "table").strip().lower()
    selected_ids = tuple(_dedup_ids([str(x).strip() for x in (getattr(args, "universal_ids", None) or []) if str(x).strip()]))

    cfg = ReplayConfig(
        provider=provider_name,
        league=league_key,
        run_id=int(run_id),
        capture_manifest=capture_manifest,
        out_dir=out_dir,
        universal_ids=selected_ids,
        mode=mode,
        speed_multiplier=float(speed_multiplier),
        decision_cooldown_seconds=float(HotPathConfig().decision_cooldown_seconds),
        decision_debounce_seconds=float(HotPathConfig().decision_debounce_seconds),
    )
    runtime = _runtime_from_args(args)
    with open_database(runtime) as db:
        try:
            summary = run_hotpath_replay_api(db=db, config=cfg)
        except Exception as exc:
            logger.error("hotpath replay failed: %s: %s", type(exc).__name__, exc)
            return 1

    payload = summary.to_dict()
    if fmt == "json":
        logger.info("%s", json.dumps(payload, sort_keys=True, default=str))
        return 0

    headline = [
        {
            "provider": summary.provider,
            "league": summary.league,
            "run_id": int(summary.run_id),
            "events_total": int(summary.n_events_total),
            "events_material": int(summary.n_events_material),
            "intents_attempted": int(summary.n_intents_attempted),
            "correct": int(summary.n_correct),
            "incorrect": int(summary.n_incorrect),
            "unknown": int(summary.n_unknown),
            "drops_cooldown": int(summary.n_drops_cooldown),
            "drops_debounce": int(summary.n_drops_debounce),
            "drops_one_shot": int(summary.n_drops_one_shot),
            "timeline_path": str(summary.timeline_path),
            "summary_path": str(summary.summary_path),
        }
    ]
    logger.info(
        "Hotpath Replay Summary\n%s",
        _render_table(
            rows=headline,
            columns=[
                ("provider", "provider"),
                ("league", "league"),
                ("run_id", "run_id"),
                ("events_total", "events_total"),
                ("events_material", "events_material"),
                ("intents_attempted", "intents_attempted"),
                ("correct", "correct"),
                ("incorrect", "incorrect"),
                ("unknown", "unknown"),
                ("drops_cooldown", "drops_cooldown"),
                ("drops_debounce", "drops_debounce"),
                ("drops_one_shot", "drops_one_shot"),
                ("timeline_path", "timeline_path"),
                ("summary_path", "summary_path"),
            ],
        ),
    )
    per_game_rows = []
    for uid, stats in sorted((summary.per_game or {}).items(), key=lambda x: x[0]):
        per_game_rows.append(
            {
                "universal_id": str(uid),
                "attempted": int(stats.get("attempted") or 0),
                "correct": int(stats.get("correct") or 0),
                "incorrect": int(stats.get("incorrect") or 0),
                "unknown": int(stats.get("unknown") or 0),
                "drops_cooldown": int(stats.get("drops_cooldown") or 0),
                "drops_debounce": int(stats.get("drops_debounce") or 0),
                "drops_one_shot": int(stats.get("drops_one_shot") or 0),
            }
        )
    if per_game_rows:
        logger.info(
            "Replay Per-Game\n%s",
            _render_table(
                rows=per_game_rows,
                columns=[
                    ("universal_id", "universal_id"),
                    ("attempted", "attempted"),
                    ("correct", "correct"),
                    ("incorrect", "incorrect"),
                    ("unknown", "unknown"),
                    ("drops_cooldown", "drops_cooldown"),
                    ("drops_debounce", "drops_debounce"),
                    ("drops_one_shot", "drops_one_shot"),
                ],
            ),
        )
    return 0


def _auto_approve_pending_games(*, db: Any, run_id: int, provider: str, league: str, logger: logging.Logger) -> int:
    """Approve all pending games in the link run scope. Returns count approved."""
    scope = evaluate_hotpath_scope(
        db=db, provider=provider, league=league, run_id=run_id,
        now_ts_utc=int(time.time()),
    )
    approved = 0
    for row in scope.scope_rows:
        decision = str(getattr(row, "decision", "") or "").strip().lower()
        if decision in {"approve", "reject", "skip"}:
            continue
        gid = str(getattr(row, "provider_game_id", "") or "").strip()
        if not gid:
            continue
        db.linking.insert_review_decision(
            run_id=run_id,
            provider=provider,
            provider_game_id=gid,
            decision="approve",
            note="auto-approved by hotpath live orchestrator",
            actor="orchestrator",
            decided_at=int(time.time()),
            commit=False,
        )
        approved += 1
    if approved > 0:
        db.commit()
    if approved:
        logger.info("auto-approved %d pending games", approved)
    return approved


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


def _get_latest_link_run_id(db: Any, provider: str, league: str) -> int | None:
    """Return the most recent run_id for the given provider.
    The league_scope column stores 'live' or 'all', not the league name."""
    row = db.execute(
        "SELECT MAX(run_id) AS rid FROM link_runs WHERE provider = ?",
        (provider,),
    ).fetchone()
    if row is None:
        return None
    rid = row["rid"] if isinstance(row, dict) else row[0]
    return int(rid) if rid is not None else None


def run_hotpath_live(args: Any, *, logger: logging.Logger) -> int:
    """Run the hotpath with periodic stop/restart for plan refresh.

    On each cycle: sync markets → sync provider → rebuild linking →
    auto-approve → compile plan (excluding fired strategy keys) →
    start hotpath → wait refresh_interval → stop → repeat.
    """
    import subprocess

    _load_dotenv(logger)

    live_policy = load_live_trading_policy()
    provider_name = str(getattr(args, "provider", "kalstrop") or "kalstrop").strip().lower()
    league_key = str(getattr(args, "league", "") or "").strip().lower()
    if not league_key:
        logger.error("--league is required")
        return 1
    current_run_id: int | None = _int_or_none(getattr(args, "link_run_id", None))
    execution_mode = str(getattr(args, "execution_mode", "live") or "live").strip().lower()
    refresh_interval = int(getattr(args, "refresh_interval", 300) or 300)

    runtime = _runtime_from_args(args)
    runtime_policy = _hotpath_runtime_policy_for_league(live_policy=live_policy, league_key=league_key)
    order_policy, require_presign, _ = _hotpath_order_policy_for_league(
        live_policy=live_policy, league_key=league_key,
    )

    log_dir = os.environ.get("POLYBOT2_LOG_DIR", ".")
    db_path = str(getattr(args, "db", "") or "").strip()
    db_args = ["--db", db_path] if db_path else []

    # Recover fired strategy keys from prior run logs across all run IDs.
    from polybot2.hotpath.log_reader import read_fired_strategy_keys as _read_one_log
    fired_strategy_keys: set[str] = set()
    from pathlib import Path
    log_dir_path = Path(log_dir)
    if log_dir_path.is_dir():
        for log_file in log_dir_path.glob("hotpath_*.jsonl"):
            fired_strategy_keys |= _read_one_log(log_file)
    if fired_strategy_keys:
        logger.info("recovered %d fired strategy keys from prior logs", len(fired_strategy_keys))

    stop_requested = False

    def _handle_signal(_sig: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    prev_int = signal.signal(signal.SIGINT, _handle_signal)
    prev_term = signal.signal(signal.SIGTERM, _handle_signal)
    iteration = 0

    def _compile_and_start(iteration: int) -> _NativeHotPathService | None:
        """Compile plan from current DB state and start a new hotpath.
        Returns the running service, or None on failure."""
        nonlocal current_run_id
        try:
            with open_database(runtime) as db:
                latest_run_id = _get_latest_link_run_id(db, provider_name, league_key)
                if latest_run_id is None:
                    logger.error("no link runs found for provider=%s league=%s", provider_name, league_key)
                    return None
                if latest_run_id != current_run_id:
                    if current_run_id is not None:
                        logger.info("run_id updated: %d → %d", current_run_id, latest_run_id)
                    else:
                        logger.info("run_id resolved: %d", latest_run_id)
                    current_run_id = latest_run_id

                _auto_approve_pending_games(
                    db=db, run_id=current_run_id, provider=provider_name,
                    league=league_key, logger=logger,
                )

                compiled_plan = compile_hotpath_plan(
                    db=db,
                    provider=provider_name,
                    league=league_key,
                    run_id=int(current_run_id),
                    live_policy=live_policy,
                    require_all_approved=True,
                    now_ts_utc=int(time.time()),
                    plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                    exclude_strategy_keys=fired_strategy_keys if fired_strategy_keys else None,
                )

                resolver = BindingResolver(db=db)
                resolver.reload()

                try:
                    prov = build_sports_provider(provider_name=provider_name, logger=logger)
                except ValueError as exc:
                    logger.error("provider init failed: %s", exc)
                    return None

                _scope_provider_catalog_to_league(
                    provider=prov, provider_name=provider_name, league_key=league_key,
                )

                exec_cfg_overrides: dict[str, Any] = {}
                if require_presign:
                    exec_cfg_overrides["presign_enabled"] = True
                exec_cfg = FastExecutionConfig.from_env(exec_cfg_overrides)
                exec_service = FastExecutionService(config=exec_cfg)

                hp_cfg = HotPathConfig(
                    run_scores=True,
                    run_odds=False,
                    read_timeout_seconds=0.05,
                    native_engine_enabled=True,
                    native_engine_required=True,
                )
                hotpath = NativeHotPathService(
                    provider=prov,
                    execution=exec_service,
                    execution_mode=execution_mode,
                    config=hp_cfg,
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

                n_targets = sum(
                    len(m.targets) for g in compiled_plan.games for m in g.markets
                )
                logger.info(
                    "hotpath starting: cycle=%d games=%d targets=%d excluded=%d subs=%d",
                    iteration, len(compiled_plan.games), n_targets,
                    len(fired_strategy_keys), len(wanted),
                )

                hotpath.start()
                return hotpath

        except HotPathPlanError as exc:
            logger.error("plan compile failed: code=%s message=%s", exc.code, exc)
            return None
        except Exception as exc:
            logger.error("compile/start failed: %s: %s", type(exc).__name__, exc)
            return None

    def _sync_data_and_linking() -> None:
        """Run data sync, provider sync, and link build. These only touch
        the SQLite database and can run while the hotpath is live."""
        try:
            cmd = ["polybot2", "data", "sync", "--markets", "--open-only"] + db_args
            logger.info("running: %s", " ".join(cmd))
            subprocess.run(cmd, check=True, timeout=120)
        except Exception as exc:
            logger.warning("data sync failed (continuing): %s", exc)

        if stop_requested:
            return

        try:
            cmd = ["polybot2", "provider", "sync"] + db_args
            logger.info("running: %s", " ".join(cmd))
            subprocess.run(cmd, check=True, timeout=120)
        except Exception as exc:
            logger.warning("provider sync failed (continuing): %s", exc)

        if stop_requested:
            return

        try:
            cmd = ["polybot2", "link", "build", "--provider", provider_name, "--league-scope", "live"] + db_args
            logger.info("running: %s", " ".join(cmd))
            subprocess.run(cmd, check=True, timeout=60)
        except Exception as exc:
            logger.warning("link build failed (continuing): %s", exc)

    try:
        # --- Initial sync before first cycle ---
        logger.info("=== initial sync ===")
        _sync_data_and_linking()

        hotpath: _NativeHotPathService | None = None

        while not stop_requested:
            iteration += 1
            logger.info("=== refresh cycle %d ===", iteration)

            # --- Stop previous hotpath (brief: only compile + warmup is blind) ---
            if hotpath is not None:
                try:
                    hotpath.stop()
                except Exception:
                    pass
                hotpath = None

            if stop_requested:
                break

            # --- Compile plan and start hotpath ---
            hotpath = _compile_and_start(iteration)
            if hotpath is None:
                # Compile/start failed — wait before retrying
                for _ in range(min(refresh_interval, 30)):
                    if stop_requested:
                        break
                    time.sleep(1.0)
                continue

            # --- Run hotpath while preparing next cycle's data in background ---
            cycle_start = time.time()
            sync_done = False
            while not stop_requested and (time.time() - cycle_start) < refresh_interval:
                remaining = refresh_interval - (time.time() - cycle_start)
                # Start data sync/linking 30s before the refresh interval ends,
                # so the DB is fresh when we stop + recompile. The hotpath keeps
                # processing frames during the sync.
                if not sync_done and remaining <= 30:
                    logger.info("=== background sync (hotpath still running) ===")
                    _sync_data_and_linking()
                    sync_done = True
                time.sleep(1.0)

            # --- Update fired strategy keys ---
            new_total: set[str] = set()
            for log_file in log_dir_path.glob("hotpath_*.jsonl"):
                new_total |= _read_one_log(log_file)
            new_count = len(new_total - fired_strategy_keys)
            fired_strategy_keys = new_total
            if new_count:
                logger.info("fired strategy keys: %d total (+%d new)", len(fired_strategy_keys), new_count)

    finally:
        # Stop the last running hotpath
        if hotpath is not None:
            try:
                hotpath.stop()
            except Exception:
                pass
        signal.signal(signal.SIGINT, prev_int)
        signal.signal(signal.SIGTERM, prev_term)

    logger.info("orchestrator stopped after %d cycles", iteration)
    return 0


__all__ = [
    "run_hotpath",
    "run_hotpath_live",
    "run_hotpath_observe",
    "run_hotpath_replay",
    "FastExecutionService",
    "NativeHotPathService",
    "build_sports_provider",
    "load_live_trading_policy",
    "_hotpath_order_policy_for_league",
    "_hotpath_runtime_policy_for_league",
    "signal",
    "time",
]
