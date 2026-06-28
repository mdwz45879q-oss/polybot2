"""Hotpath runtime/replay command handlers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import logging
import os
import signal
import time
from typing import Any

from polybot2._cli.common import _apply_env_uid_filter
from polybot2._cli.common import _build_hotpath_template_orders
from polybot2._cli.common import _build_pandascore_match_inits
from polybot2._cli.common import _build_retirement_template_orders
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


def _primary_provider_for_league(league_cfg: dict) -> str:
    """Extract the primary provider name from a league config dict."""
    raw_p = league_cfg.get("provider", "")
    if isinstance(raw_p, list):
        return str(raw_p[0]).strip().lower() if raw_p else ""
    return str(raw_p).strip().lower()


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
            _league_arg = getattr(args, "league", ["mlb"]) or ["mlb"]
            league_key = (str(_league_arg[0]) if isinstance(_league_arg, list) else str(_league_arg)).strip().lower()
            if link_run_id is not None:
                from polybot2.hotpath.compiler import compile_hotpath_plan
                from polybot2.linking import load_mapping as _load_mapping_obs
                runtime = _runtime_from_args(args)
                mapping_obs = _load_mapping_obs()
                obs_league_cfg = mapping_obs.leagues.get(league_key, {})
                _obs_raw_p = obs_league_cfg.get("provider", "kalstrop_v1")
                obs_provider = (
                    str(_obs_raw_p[0]).strip().lower() if isinstance(_obs_raw_p, list) and _obs_raw_p
                    else str(_obs_raw_p).strip().lower()
                )
                _obs_sport = str(obs_league_cfg.get("sport_family", "baseball")).strip().lower()
                _obs_stw = int(obs_league_cfg.get("sets_to_win", 2))
                with open_database(runtime) as db:
                    compiled_plan = compile_hotpath_plan(
                        db=db,
                        provider=obs_provider,
                        league=league_key,
                        run_id=int(link_run_id),
                        sport=_obs_sport,
                        sets_to_win=_obs_stw,
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


from polybot2._cli.common import _load_dotenv

_READINESS_DISPLAY_ORDER = (
    "in_plan", "promoted", "pending_stream", "pending_v2",
    "catalog_missing", "compile_excluded", "excluded",
)


@dataclass
class ReadinessResult:
    promoted: list[tuple[str, str, str, Any]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    new_subscriptions: list[tuple[str, str]] = field(default_factory=list)


def _format_readiness_summary(result: ReadinessResult, *, prefix: str = "[ready]") -> str:
    total = sum(result.counts.values())
    parts = [
        f"{result.counts[k]} {k}"
        for k in _READINESS_DISPLAY_ORDER
        if result.counts.get(k, 0) > 0
    ]
    return f"{prefix} {total} evaluated: {', '.join(parts)}"


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
    from pathlib import Path
    from polybot2.hotpath.incremental import discover_new_markets_sync, IncrementalRefreshResult
    from polybot2.hotpath.v2_resolver import (
        build_pending_games, try_resolve_games, compile_for_resolved_game,
        resolution_time_delta_seconds,
    )
    from polybot2.linking import load_mapping as _load_mapping

    _load_dotenv(logger)

    live_policy = load_live_trading_policy()

    # --- Resolve league list from --league or --sport ---
    raw_leagues = getattr(args, "league", None)  # list or None (nargs="+")
    sport_arg = getattr(args, "sport", None)
    mapping = _load_mapping()

    if sport_arg and raw_leagues:
        logger.error("cannot specify both --sport and --league")
        return 1
    if not sport_arg and not raw_leagues:
        logger.error("one of --sport or --league is required")
        return 1

    if sport_arg:
        league_keys = sorted(
            lk for lk, cfg in mapping.leagues.items()
            if cfg.get("sport_family") == sport_arg
            and lk in live_policy.live_betting_leagues
        )
        gender_arg = str(getattr(args, "gender", "") or "").strip().lower()
        if gender_arg:
            league_keys = [
                lk for lk in league_keys
                if mapping.leagues.get(lk, {}).get("polymarket_league_code") == gender_arg
            ]
        if not league_keys:
            logger.error("no live leagues for sport=%s gender=%s", sport_arg, gender_arg or "all")
            return 1
        logger.info("sport=%s gender=%s resolved to leagues: %s", sport_arg, gender_arg or "all", ", ".join(league_keys))
    else:
        league_keys = [str(l).strip().lower() for l in raw_leagues]

    if len(league_keys) > 1:
        families = {mapping.leagues.get(lk, {}).get("sport_family", "") for lk in league_keys}
        if len(families) > 1:
            logger.error("cannot mix sports in one process: %s", ", ".join(sorted(families)))
            return 1

    # Use first league as "primary" for backward compat with single-league code paths.
    league_key = league_keys[0]
    league_cfg = mapping.leagues.get(league_key, {})
    sport_family = str(league_cfg.get("sport_family", "baseball")).strip().lower()
    league_sets_to_win = int(league_cfg.get("sets_to_win", 2))
    raw_provider = league_cfg.get("provider", "")
    if isinstance(raw_provider, list):
        provider_names = [str(p).strip().lower() for p in raw_provider if str(p).strip()]
        provider_name = provider_names[0] if provider_names else ""
        is_multiplexed = len(provider_names) > 1
    else:
        provider_name = str(raw_provider).strip().lower()
        provider_names = [provider_name] if provider_name else []
        is_multiplexed = False

    # For multi-league mode, collect all providers across all leagues.
    if len(league_keys) > 1:
        _all_providers: set[str] = set()
        for _lk in league_keys:
            _cfg = mapping.leagues.get(_lk, {})
            _raw_p = _cfg.get("provider", "")
            if isinstance(_raw_p, list):
                _all_providers.update(str(p).strip().lower() for p in _raw_p if str(p).strip())
            elif str(_raw_p).strip():
                _all_providers.add(str(_raw_p).strip().lower())
        provider_names = sorted(_all_providers)
        is_multiplexed = len(provider_names) > 1

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
    runtime_policy = _hotpath_runtime_policy_for_league(live_policy=live_policy, league_key=league_key, sport_family=sport_family)
    cli_refresh = getattr(args, "refresh_interval", None)
    config_refresh = int(runtime_policy.get("refresh_interval_seconds", 300))
    refresh_interval = int(cli_refresh) if cli_refresh is not None else config_refresh
    order_policies: dict[str, Any] = {}
    require_presign = False
    for _lk in league_keys:
        _sf = str(mapping.leagues.get(_lk, {}).get("sport_family", "")).strip().lower()
        _p, _rp, _ = _hotpath_order_policy_for_league(live_policy=live_policy, league_key=_lk, sport_family=_sf)
        order_policies[_lk] = _p
        if _rp:
            require_presign = True

    stop_requested = False

    def _handle_signal(_sig: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    prev_int = signal.signal(signal.SIGINT, _handle_signal)
    prev_term = signal.signal(signal.SIGTERM, _handle_signal)
    iteration = 0

    rust_started = False
    hotpath: _NativeHotPathService | None = None
    try:
        # --- Build provider + execution + hotpath service (shared by V2 and non-V2) ---
        with open_database(runtime) as db:
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

        hp_cfg = HotPathConfig(native_engine_required=True)
        hotpath = NativeHotPathService(
            provider=prov, execution=exec_service,
            execution_mode=execution_mode, config=hp_cfg,
        )
        hotpath.set_order_policies(order_policies)
        _cli_ws_core = getattr(args, "ws_core", None)
        _cli_sub_core = getattr(args, "submitter_core", None)
        hotpath._ws_core_idx = _cli_ws_core if _cli_ws_core is not None else runtime_policy.get("ws_core_idx")
        hotpath._submitter_core_idx = _cli_sub_core if _cli_sub_core is not None else runtime_policy.get("submitter_core_idx")

        # Build multiplexed provider configs when the league uses multiple providers.
        if is_multiplexed:
            from polybot2.sports.factory import resolve_kalstrop_credentials_from_env
            _mux_client_id, _mux_secret, _ = resolve_kalstrop_credentials_from_env()
            _providers_cfg: list[dict[str, Any]] = []
            for _pn in provider_names:
                if _pn == "kalstrop_v2":
                    _providers_cfg.append({
                        "provider": "kalstrop_v2",
                        "base_url": "https://stats.kalstropservice.com",
                        "sio_path": "/socket.io",
                        "client_id": _mux_client_id,
                        "shared_secret_raw": _mux_secret,
                    })
                elif _pn in ("kalstrop_v1", "kalstrop"):
                    _providers_cfg.append({
                        "provider": "kalstrop_v1",
                        "ws_url": os.getenv("KALSTROP_WS_URL", "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"),
                        "client_id": _mux_client_id,
                        "shared_secret_raw": _mux_secret,
                    })
                elif _pn == "boltodds":
                    _providers_cfg.append({
                        "provider": "boltodds",
                        "api_key": os.getenv("BOLTODDS_API_KEY", ""),
                        "boltodds_ws_url": os.getenv("BOLTODDS_WS_URL", "wss://spro.agency/api/livescores"),
                    })
            hotpath._providers = _providers_cfg
            logger.info("multiplexed providers: %s", ", ".join(provider_names))

        # ── Readiness-based orchestrator (see docs/design/orchestrator_v2.md) ──
        #
        # All linked+approved games are evaluated for provider-specific readiness:
        #   V1 (kalstrop_v1): stream_exists == true
        #   V2 (kalstrop_v2): fixture ID resolved
        #   BoltOdds / other:  immediately ready
        # Ready games start the hotpath (first batch) or get hot-patched (subsequent).

        from polybot2.hotpath.compiler import compile_multi_league_plan
        from polybot2.providers.sync import load_provider_catalog

        # V2 credentials (needed for fixture resolution)
        v2_client_id = ""
        v2_shared_secret_raw = ""
        _has_v2 = any(
            "kalstrop_v2" in (
                [str(p).strip().lower() for p in mapping.leagues.get(lk, {}).get("provider", [])]
                if isinstance(mapping.leagues.get(lk, {}).get("provider"), list)
                else [str(mapping.leagues.get(lk, {}).get("provider", "")).strip().lower()]
            )
            for lk in league_keys
        )
        if _has_v2:
            from polybot2.sports.factory import resolve_kalstrop_credentials_from_env
            v2_client_id, v2_shared_secret_raw, _ = resolve_kalstrop_credentials_from_env()
        _v2_sport_slug = "football" if sport_family == "soccer" else sport_family

        # State tracking
        in_plan_keys: set[tuple[str, str]] = set()  # (provider, slug_prefix)
        in_plan_provider_game_ids: set[tuple[str, str]] = set()  # (provider, provider_game_id)
        pending_v2_ids: set[str] = set()  # resolved/finished V2 prematch IDs
        _cumulative_provider_subs: dict[str, list[str]] = {}
        _seen_catalog_missing: set[tuple[str, str]] = set()  # (provider, gid) — log once
        _seen_compile_excluded: set[tuple[str, str]] = set()  # (provider, gid) — log once
        _seen_alt_subscribed: set[tuple[str, str]] = set()  # (provider, gid) — log once
        cycle_interval = float(refresh_interval)

        _stw_by_league = {
            lk: int(mapping.leagues.get(lk, {}).get("sets_to_win", 2))
            for lk in league_keys
        }

        def _providers_for_league(lk: str) -> list[str]:
            raw = mapping.leagues.get(lk, {}).get("provider", "")
            if isinstance(raw, list):
                return [str(p).strip().lower() for p in raw if str(p).strip()]
            return [str(raw).strip().lower()] if str(raw).strip() else []

        def _get_slug_prefix(db: Any, prov: str, gid: str) -> str:
            row = db.execute(
                "SELECT event_slug_prefix FROM link_run_provider_games "
                "WHERE run_id = ? AND provider = ? AND provider_game_id = ?",
                (run_id, prov, gid),
            ).fetchone()
            return str(row["event_slug_prefix"] or "") if row else ""

        def _merge_into_compiled_plan(game_plan: Any) -> None:
            existing = hotpath._compiled_plan
            if existing:
                merged = replace(existing, games=tuple(existing.games) + tuple(game_plan.games))
                hotpath.set_compiled_plan(merged)
            else:
                hotpath.set_compiled_plan(game_plan)

        def _cold_start(plan: Any) -> None:
            nonlocal rust_started
            hotpath.set_compiled_plan(plan)
            if hasattr(hotpath, "set_runtime_timing_policy"):
                hotpath.set_runtime_timing_policy(
                    subscribe_lead_minutes=int(runtime_policy.get("subscribe_lead_minutes", 90)),
                    subscription_refresh_seconds=int(runtime_policy.get("subscription_refresh_seconds", 120)),
                )
            hotpath.set_subscriptions(_cumulative_provider_subs)
            templates = _build_hotpath_template_orders(compiled_plan=plan, order_policies=order_policies)
            if templates and hasattr(hotpath, "prewarm_presign"):
                hotpath.prewarm_presign(templates)
            ret_templates = _build_retirement_template_orders(compiled_plan=plan, order_policies=order_policies)
            if ret_templates and hasattr(hotpath, "prewarm_presign_retirement"):
                hotpath.prewarm_presign_retirement(ret_templates)
            n_tgt = sum(len(m.targets) for g in plan.games for m in g.markets)
            logger.info("[startup] plan: %d games, %d targets", len(plan.games), n_tgt)
            hotpath.start()
            rust_started = True
            logger.info("[startup] hotpath started")

        def _hot_patch_game(game_plan: Any) -> int:
            new_targets = tuple(t for g in game_plan.games for m in g.markets for t in m.targets)
            refresh_result = IncrementalRefreshResult(
                new_plan=game_plan,
                new_targets=new_targets,
                new_condition_ids=frozenset(m.condition_id for g in game_plan.games for m in g.markets),
                events_fetched=0,
                markets_discovered=len(new_targets),
                targets_inserted=len(new_targets),
            )
            count = hotpath.apply_incremental_refresh(refresh_result, order_policies)
            _merge_into_compiled_plan(game_plan)
            return count

        # ── Readiness evaluation ──

        def _run_readiness_layer(db: Any) -> ReadinessResult:
            """Evaluate all linked games and categorize each into exactly one bucket."""
            counts: dict[str, int] = {k: 0 for k in _READINESS_DISPLAY_ORDER}
            newly_ready: list[tuple[str, str, str, Any]] = []
            new_subscriptions: list[tuple[str, str]] = []
            _promoted_this_pass: set[tuple[str, str]] = set()
            now_ts = int(time.time())

            # ── Batch V2 resolution (one call per league, not per game) ──
            v2_resolved: dict[str, Any] = {}  # prematch_event_id → V2ResolvedGame
            v2_finished: set[str] = set()
            for lk in league_keys:
                provs = _providers_for_league(lk)
                if "kalstrop_v2" not in provs:
                    continue
                try:
                    all_pending = build_pending_games(
                        db=db, league=lk, run_id=run_id,
                        provider="kalstrop_v2", already_resolved=pending_v2_ids,
                    )
                    due = [g for g in all_pending if g.start_ts_utc is None or g.start_ts_utc <= now_ts]
                    if due:
                        resolution = try_resolve_games(
                            due, client_id=v2_client_id, shared_secret_raw=v2_shared_secret_raw,
                            sport_slug=_v2_sport_slug,
                        )
                        for fg in resolution.finished:
                            pending_v2_ids.add(fg.prematch_event_id)
                            v2_finished.add(fg.prematch_event_id)
                            logger.info("[ready]   - %s | %s | kalstrop_v2 | finished", lk, fg.prematch_event_id)
                        for r in resolution.resolved:
                            v2_resolved[r.pending.prematch_event_id] = r
                            pending_v2_ids.add(r.pending.prematch_event_id)
                except Exception as exc:
                    logger.warning("[ready] V2 batch resolution failed for %s: %s", lk, exc)

            # Cache compiled plans per (provider, league) to avoid recompiling for each game
            compiled_cache: dict[tuple[str, str], Any] = {}

            # Collect all linked+approved games per (provider, league) across all leagues
            for lk in league_keys:
                provs = _providers_for_league(lk)
                for prov in provs:
                    # Get all linked games for this (run_id, provider, league)
                    rows = db.execute(
                        """
                        SELECT pg.provider_game_id, pg.canonical_home_team, pg.canonical_away_team,
                               pg.start_ts_utc, gr.resolution_state, pg.event_slug_prefix,
                               COALESCE(ld.decision, '') AS decision
                        FROM link_run_provider_games pg
                        LEFT JOIN link_run_game_reviews gr
                          ON gr.run_id = pg.run_id AND gr.provider = pg.provider
                         AND gr.provider_game_id = pg.provider_game_id
                        LEFT JOIN (
                            SELECT d.*
                            FROM link_review_decisions d
                            INNER JOIN (
                                SELECT run_id, provider, provider_game_id, MAX(decision_id) AS max_id
                                FROM link_review_decisions
                                WHERE run_id = ? AND provider = ?
                                GROUP BY run_id, provider, provider_game_id
                            ) x ON x.max_id = d.decision_id
                        ) ld
                          ON ld.run_id = pg.run_id AND ld.provider = pg.provider
                         AND ld.provider_game_id = pg.provider_game_id
                        WHERE pg.run_id = ? AND pg.provider = ?
                          AND pg.parse_status = 'ok' AND pg.canonical_league = ?
                        """,
                        (run_id, prov, run_id, prov, lk),
                    ).fetchall()

                    for r in rows:
                        gid = str(r["provider_game_id"] or "").strip()
                        if not gid:
                            continue
                        decision = str(r["decision"] or "").strip().lower()
                        res_state = str(r["resolution_state"] or "").strip().upper()
                        slug = str(r["event_slug_prefix"] or "").strip()
                        home = str(r["canonical_home_team"] or "")
                        away = str(r["canonical_away_team"] or "")

                        # Excluded: rejected, pending-kickoff-review without approval, unresolvable
                        if decision == "reject":
                            counts["excluded"] += 1
                            continue
                        if res_state == "PENDING_KICKOFF_REVIEW" and decision != "approve":
                            counts["excluded"] += 1
                            continue
                        if not res_state or res_state in ("TEAM_SET_NOT_FOUND", "NO_EVENT_CANDIDATES", "AMBIGUOUS_EVENT_MATCH"):
                            counts["excluded"] += 1
                            continue

                        # Already in plan (primary dedup by slug)?
                        key = (prov, slug)
                        if key in in_plan_keys:
                            counts["in_plan"] += 1
                            continue

                        # Covered by another provider's entry via alternate IDs?
                        # Still run the readiness check so counts reflect true status
                        # (e.g., V1 game shows pending_stream even if BoltOdds is in plan).
                        _covered_by_alt = (prov, gid) in in_plan_provider_game_ids

                        # Provider-specific readiness check
                        if prov == "kalstrop_v2":
                            if gid in pending_v2_ids and gid not in v2_resolved:
                                counts["excluded"] += 1
                                continue
                            resolved = v2_resolved.get(gid)
                            if resolved is None:
                                counts["pending_v2"] += 1
                                continue
                            if _covered_by_alt or (prov, gid) in _promoted_this_pass:
                                new_subscriptions.append((prov, gid))
                                counts["in_plan"] += 1
                                if (prov, gid) not in _seen_alt_subscribed:
                                    _seen_alt_subscribed.add((prov, gid))
                                    logger.info("[ready]   ~ %s | %s vs %s | %s | subscribed", lk, home, away, prov)
                                continue
                            delta_s = resolution_time_delta_seconds(resolved)
                            logger.info(
                                "V2 resolved: %s -> fixture_id=%s (%s vs %s, status=%s, delta=%ds)",
                                resolved.pending.prematch_event_id, resolved.fixture_id,
                                resolved.resolved_home, resolved.resolved_away,
                                resolved.match_status, delta_s,
                            )
                            game_plan = compile_for_resolved_game(
                                resolved=resolved, db=db, run_id=run_id,
                                provider=prov, league=lk,
                                live_policy=live_policy,
                                plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                                sport=sport_family,
                                sets_to_win=_stw_by_league.get(lk, 2),
                            )
                            if game_plan is None:
                                counts["compile_excluded"] += 1
                                if (prov, gid) not in _seen_compile_excluded:
                                    _seen_compile_excluded.add((prov, gid))
                                    logger.info("[ready]   - %s | %s vs %s | %s | compile_excluded", lk, home, away, prov)
                                continue
                            for g in game_plan.games:
                                _promoted_this_pass.add((prov, str(g.provider_game_id)))
                                for ap, aid in g.alternate_provider_game_ids:
                                    _promoted_this_pass.add((str(ap), str(aid)))
                            newly_ready.append((prov, lk, slug, game_plan))
                            new_subscriptions.append((prov, gid))

                        elif prov in ("kalstrop_v1", "kalstrop"):
                            # V1: check stream_exists
                            se_row = db.execute(
                                "SELECT stream_exists FROM provider_games WHERE provider = ? AND provider_game_id = ?",
                                (prov, gid),
                            ).fetchone()
                            if se_row is None:
                                counts["catalog_missing"] += 1
                                if (prov, gid) not in _seen_catalog_missing:
                                    _seen_catalog_missing.add((prov, gid))
                                    logger.info("[ready]   - %s | %s vs %s | %s | catalog_missing", lk, home, away, prov)
                                continue
                            se = se_row["stream_exists"]
                            if se is None or int(se) != 1:
                                counts["pending_stream"] += 1
                                continue
                            if _covered_by_alt or (prov, gid) in _promoted_this_pass:
                                new_subscriptions.append((prov, gid))
                                counts["in_plan"] += 1
                                if (prov, gid) not in _seen_alt_subscribed:
                                    _seen_alt_subscribed.add((prov, gid))
                                    logger.info("[ready]   ~ %s | %s vs %s | %s | subscribed", lk, home, away, prov)
                                continue
                            _cache_key = (prov, lk)
                            if _cache_key not in compiled_cache:
                                compiled_cache[_cache_key] = compile_hotpath_plan(
                                    db=db, provider=prov, league=lk, run_id=run_id,
                                    sport=sport_family, sets_to_win=_stw_by_league.get(lk, 2),
                                    live_policy=live_policy,
                                    now_ts_utc=now_ts,
                                    plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                                    include_inactive=True,
                                )
                            matching = [g for g in compiled_cache[_cache_key].games if g.provider_game_id == gid]
                            if not matching:
                                counts["compile_excluded"] += 1
                                if (prov, gid) not in _seen_compile_excluded:
                                    _seen_compile_excluded.add((prov, gid))
                                    logger.info("[ready]   - %s | %s vs %s | %s | compile_excluded", lk, home, away, prov)
                                continue
                            game_plan = replace(compiled_cache[_cache_key], games=tuple(matching))
                            for g in matching:
                                _promoted_this_pass.add((prov, str(g.provider_game_id)))
                                for ap, aid in g.alternate_provider_game_ids:
                                    _promoted_this_pass.add((str(ap), str(aid)))
                            newly_ready.append((prov, lk, slug, game_plan))
                            new_subscriptions.append((prov, gid))

                        else:
                            # BoltOdds / other: immediately ready
                            if _covered_by_alt or (prov, gid) in _promoted_this_pass:
                                new_subscriptions.append((prov, gid))
                                counts["in_plan"] += 1
                                if (prov, gid) not in _seen_alt_subscribed:
                                    _seen_alt_subscribed.add((prov, gid))
                                    logger.info("[ready]   ~ %s | %s vs %s | %s | subscribed", lk, home, away, prov)
                                continue
                            _cache_key = (prov, lk)
                            if _cache_key not in compiled_cache:
                                compiled_cache[_cache_key] = compile_hotpath_plan(
                                    db=db, provider=prov, league=lk, run_id=run_id,
                                    sport=sport_family, sets_to_win=_stw_by_league.get(lk, 2),
                                    live_policy=live_policy,
                                    now_ts_utc=now_ts,
                                    plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                                    include_inactive=True,
                                )
                            matching = [g for g in compiled_cache[_cache_key].games if g.provider_game_id == gid]
                            if not matching:
                                counts["compile_excluded"] += 1
                                if (prov, gid) not in _seen_compile_excluded:
                                    _seen_compile_excluded.add((prov, gid))
                                    logger.info("[ready]   - %s | %s vs %s | %s | compile_excluded", lk, home, away, prov)
                                continue
                            _primary_prov = _primary_provider_for_league(mapping.leagues.get(lk, {}))
                            if prov != _primary_prov:
                                _swapped = []
                                for g in matching:
                                    _primary_alt = None
                                    for ap, aid in g.alternate_provider_game_ids:
                                        if str(ap) == _primary_prov:
                                            _primary_alt = str(aid)
                                            break
                                    if _primary_alt:
                                        _new_alts = tuple(
                                            (ap, aid) for ap, aid in g.alternate_provider_game_ids
                                            if str(ap) != _primary_prov
                                        ) + ((prov, str(g.provider_game_id)),)
                                        _swapped.append(replace(g, provider_game_id=_primary_alt, alternate_provider_game_ids=_new_alts))
                                    else:
                                        _swapped.append(g)
                                matching = _swapped
                            game_plan = replace(compiled_cache[_cache_key], games=tuple(matching))
                            for g in matching:
                                _promoted_this_pass.add((_primary_prov, str(g.provider_game_id)))
                                for ap, aid in g.alternate_provider_game_ids:
                                    _promoted_this_pass.add((str(ap), str(aid)))
                            newly_ready.append((prov, lk, slug, game_plan))
                            new_subscriptions.append((prov, gid))

            counts["promoted"] = len(newly_ready)
            return ReadinessResult(promoted=newly_ready, counts=counts, new_subscriptions=new_subscriptions)

        # ── Initial readiness evaluation at startup ──

        logger.info("[startup] run_id=%d leagues=%s providers=%s",
                    run_id, ",".join(league_keys), ",".join(provider_names))

        # Refresh V1 catalog for fresh stream_exists values
        _has_v1 = any("kalstrop_v1" in _providers_for_league(lk) for lk in league_keys)
        if _has_v1:
            try:
                with open_database(runtime) as db:
                    v1_rows = load_provider_catalog(provider="kalstrop_v1")
                    db.linking.upsert_provider_games(v1_rows)
            except Exception as exc:
                logger.warning("V1 catalog refresh failed at startup: %s", exc)

        with open_database(runtime) as db:
            initial_result = _run_readiness_layer(db)

        logger.info(_format_readiness_summary(initial_result, prefix="[startup]"))

        if initial_result.promoted:
            all_games = []
            for prov, lk, slug, gp in initial_result.promoted:
                all_games.extend(gp.games)
                in_plan_keys.add((prov, slug))
                _id_prov = _primary_provider_for_league(mapping.leagues.get(lk, {}))
                for g in gp.games:
                    in_plan_provider_game_ids.add((_id_prov, str(g.provider_game_id)))
                    for ap, aid in g.alternate_provider_game_ids:
                        in_plan_provider_game_ids.add((str(ap), str(aid)))
                    logger.info(
                        "[startup]   + %s | %s vs %s | %s",
                        lk, g.canonical_home_team, g.canonical_away_team, prov,
                    )
            merged = replace(initial_result.promoted[0][3], games=tuple(all_games))
            for sub_prov, sub_gid in initial_result.new_subscriptions:
                if sub_gid not in _cumulative_provider_subs.get(sub_prov, []):
                    _cumulative_provider_subs.setdefault(sub_prov, []).append(sub_gid)
            _cold_start(merged)
        else:
            logger.info("[startup] waiting for first game to become ready...")

        # ── Main loop ──

        last_cycle = 0.0
        _no_discovery = bool(getattr(args, "no_discovery", False))
        _no_market_refresh = bool(getattr(args, "no_market_refresh", False))

        def _run_linking_layer() -> None:
            """Layer 1: Refresh catalogs, PM events, and run incremental linking."""
            # Step 1: Refresh provider catalogs (additive upsert — preserves existing rows)
            from polybot2.providers.sync import sync_provider_games as _sync_prov
            prov_counts: list[str] = []
            with open_database(runtime) as db:
                for prov_name in provider_names:
                    try:
                        res = _sync_prov(db=db, provider=prov_name, additive=True)
                        prov_counts.append(f"{prov_name}={res.n_rows}")
                    except Exception as exc:
                        logger.warning("[linking] catalog refresh failed for %s: %s", prov_name, exc)
                        prov_counts.append(f"{prov_name}=err")
            logger.info("[linking] provider sync: %s", " ".join(prov_counts))

            # Step 2: Refresh PM events by league tags
            pm_league_codes = {
                str(mapping.leagues.get(lk, {}).get("polymarket_league_code", "")).strip().lower()
                for lk in league_keys
            } - {""}
            if pm_league_codes:
                try:
                    import asyncio as _aio
                    import concurrent.futures as _cf
                    from polybot2.hotpath.incremental import _fetch_events_by_tags
                    with _cf.ThreadPoolExecutor(max_workers=1) as pool:
                        events_data = pool.submit(
                            _aio.run,
                            _fetch_events_by_tags(gamma_api="https://gamma-api.polymarket.com", tags=pm_league_codes),
                        ).result()
                    if events_data:
                        with open_database(runtime) as db:
                            db.markets.upsert_from_gamma_events(
                                events_data=events_data, updated_ts=int(time.time()), commit=True,
                            )
                    logger.info("[linking] market sync: %d events", len(events_data) if events_data else 0)
                except Exception as exc:
                    logger.warning("[linking] PM event refresh failed: %s", exc)

            # Step 3: Incremental link
            with open_database(runtime) as db:
                # Get all provider_game_ids already in this run
                existing_rows = db.execute(
                    "SELECT provider_game_id FROM link_run_provider_games WHERE run_id = ?",
                    (run_id,),
                ).fetchall()
                existing_game_ids = {str(r["provider_game_id"]) for r in existing_rows}

                from polybot2.linking.service import LinkService
                link_service = LinkService(db=db)
                n_new_total = 0
                for lk in league_keys:
                    for prov_name in _providers_for_league(lk):
                        try:
                            result = link_service.build_links_incremental(
                                run_id=run_id,
                                league_provider_pairs=[(lk, prov_name)],
                                mapping=mapping,
                                live_policy=live_policy,
                                existing_game_ids=existing_game_ids,
                                horizon_hours=float(runtime_policy.get("plan_horizon_hours", 24)),
                            )
                            if result.n_games_linked > 0:
                                n_new_total += result.n_games_linked
                                new_ids = result.report.get("new_game_ids", []) if isinstance(result.report, dict) else []
                                for gid in new_ids:
                                    existing_game_ids.add(gid)
                        except Exception as exc:
                            logger.warning("[linking] incremental link failed for %s/%s: %s", lk, prov_name, exc)
                if n_new_total > 0:
                    logger.info("[linking] %d new links", n_new_total)
                else:
                    logger.info("[linking] no new games")

        while not stop_requested:
            time.sleep(1.0)
            if stop_requested:
                break
            now = time.time()
            if (now - last_cycle) < cycle_interval:
                continue
            last_cycle = now
            iteration += 1
            logger.info("--- cycle %d at %s ---", iteration, datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

            # Layer 1: Linking (if enabled)
            if not _no_discovery:
                try:
                    _run_linking_layer()
                except (NameError, TypeError, AttributeError, KeyError):
                    raise
                except Exception as exc:
                    logger.warning("[linking] failed: %s: %s", type(exc).__name__, exc)

            # Layer 2: Readiness (always)
            try:
                # Refresh V1 catalog for fresh stream_exists values
                if _has_v1 and _no_discovery:
                    try:
                        with open_database(runtime) as db:
                            v1_rows = load_provider_catalog(provider="kalstrop_v1")
                            db.linking.upsert_provider_games(v1_rows)
                    except Exception as exc:
                        logger.warning("V1 catalog refresh failed: %s", exc)

                with open_database(runtime) as db:
                    result = _run_readiness_layer(db)

                n_promoted = 0
                if result.promoted:
                    # Batch cold-start: merge all ready games if hotpath not yet started
                    if not rust_started:
                        all_cold_games = []
                        for _, _, _, gp in result.promoted:
                            all_cold_games.extend(gp.games)
                        merged = replace(result.promoted[0][3], games=tuple(all_cold_games))
                        try:
                            for sub_prov, sub_gid in result.new_subscriptions:
                                if sub_gid not in _cumulative_provider_subs.get(sub_prov, []):
                                    _cumulative_provider_subs.setdefault(sub_prov, []).append(sub_gid)
                            _cold_start(merged)
                        except Exception as exc:
                            logger.error("[ready] cold-start failed: %s: %s", type(exc).__name__, exc)
                            result = ReadinessResult(promoted=[], counts=result.counts)
                            result.counts["promoted"] = 0

                    for prov, lk, slug, game_plan in result.promoted:
                        try:
                            if rust_started and (prov, slug) not in in_plan_keys:
                                n_tgt = sum(len(m.targets) for g in game_plan.games for m in g.markets)
                                _hot_patch_game(game_plan)
                                for g in game_plan.games:
                                    reason = "fixture_id resolved" if prov == "kalstrop_v2" else (
                                        "stream_exists=true" if prov in ("kalstrop_v1", "kalstrop") else "ready"
                                    )
                                    logger.info(
                                        "[ready]   + %s | %s vs %s | %s | %s | %d targets",
                                        lk, g.canonical_home_team, g.canonical_away_team, prov, reason, n_tgt,
                                    )
                            in_plan_keys.add((prov, slug))
                            _id_prov = _primary_provider_for_league(mapping.leagues.get(lk, {}))
                            for g in game_plan.games:
                                in_plan_provider_game_ids.add((_id_prov, str(g.provider_game_id)))
                                for ap, aid in g.alternate_provider_game_ids:
                                    in_plan_provider_game_ids.add((str(ap), str(aid)))
                            n_promoted += 1
                        except Exception as exc:
                            result.counts["promoted"] = max(0, result.counts.get("promoted", 0) - 1)
                            logger.warning(
                                "[ready] patch failed for %s/%s: %s: %s",
                                prov, slug, type(exc).__name__, exc,
                            )
                if result.new_subscriptions and rust_started:
                    _subs_changed = False
                    for sub_prov, sub_gid in result.new_subscriptions:
                        if sub_gid not in _cumulative_provider_subs.get(sub_prov, []):
                            _cumulative_provider_subs.setdefault(sub_prov, []).append(sub_gid)
                            _subs_changed = True
                    if _subs_changed:
                        hotpath.set_subscriptions(_cumulative_provider_subs)
                logger.info(_format_readiness_summary(result))
            except (NameError, TypeError, AttributeError, KeyError):
                raise
            except Exception as exc:
                logger.warning("[ready] readiness check failed: %s: %s", type(exc).__name__, exc)

            # Layer 3: Market refresh (if enabled and Rust running)
            if not _no_market_refresh and rust_started:
                try:
                    with open_database(runtime) as db:
                        result = discover_new_markets_sync(
                            current_plan=hotpath._compiled_plan,
                            db=db,
                            live_policy=live_policy,
                            plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
                        )
                except (NameError, TypeError, AttributeError, KeyError):
                    raise
                except Exception as exc:
                    logger.warning("[markets] refresh failed: %s: %s", type(exc).__name__, exc)
                    continue

                if result.new_targets:
                    count = hotpath.apply_incremental_refresh(result, order_policies)
                    logger.info(
                        "[markets] +%d targets across %d markets (cycle=%d)",
                        len(result.new_targets), result.markets_discovered, iteration,
                    )
                else:
                    logger.info("[markets] no new markets (events_fetched=%d, cycle=%d)", result.events_fetched, iteration)

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


def run_hotpath_compile(args: Any, *, logger: logging.Logger) -> int:
    """Compile hotpath plan and print a human-readable summary (dry run).

    Same compilation as 'hotpath live' but without starting the runtime,
    presigning orders, or connecting to any WS feed. Use this to verify
    that the compiler produced the correct outcome semantics and strategy
    keys before trading.
    """
    from collections import Counter
    from polybot2.linking import load_mapping as _load_mapping

    _load_dotenv(logger)
    live_policy = load_live_trading_policy()
    league_key = str(getattr(args, "league", "") or "").strip().lower()
    if not league_key:
        logger.error("--league is required")
        return 1
    mapping = _load_mapping()
    league_cfg = mapping.leagues.get(league_key, {})
    _raw_p = league_cfg.get("provider", "")
    provider_name = (
        str(_raw_p[0]).strip().lower() if isinstance(_raw_p, list) and _raw_p
        else str(_raw_p).strip().lower()
    )
    if not provider_name:
        logger.error("league %s has no provider configured", league_key)
        return 1
    run_id = _int_or_none(getattr(args, "link_run_id", None))
    if run_id is None:
        with open_database(_runtime_from_args(args)) as _db:
            latest = _db.linking.load_latest_link_run_for_league(league=league_key)
        if latest is None:
            logger.error("no link run found for league=%s", league_key)
            return 1
        run_id = int(latest["run_id"])
        logger.info("using latest link run: run_id=%d league=%s", run_id, league_key)

    runtime = _runtime_from_args(args)
    sport_family = str(league_cfg.get("sport_family", "baseball")).strip().lower()
    runtime_policy = _hotpath_runtime_policy_for_league(
        live_policy=live_policy, league_key=league_key, sport_family=sport_family,
    )
    league_sets_to_win = int(league_cfg.get("sets_to_win", 2))

    try:
        with open_database(runtime) as db:
            compiled_plan = compile_hotpath_plan(
                db=db,
                provider=provider_name,
                league=league_key,
                run_id=run_id,
                sport=sport_family,
                sets_to_win=league_sets_to_win,
                live_policy=live_policy,
                now_ts_utc=int(time.time()),
                plan_horizon_hours=int(runtime_policy.get("plan_horizon_hours", 24)),
            )
    except HotPathPlanError as exc:
        logger.error("compilation failed: %s: %s", exc.code, str(exc))
        return 1

    # --- Print compiled plan summary ---
    team_map = mapping.team_map.get(league_key, {})
    n_targets = 0
    n_unknown = 0
    n_generic_keys = 0

    for game in compiled_plan.games:
        home = game.canonical_home_team
        away = game.canonical_away_team
        home_code = str(team_map.get(home, {}).get("polymarket_code", "")).upper() or home[:3].upper()
        away_code = str(team_map.get(away, {}).get("polymarket_code", "")).upper() or away[:3].upper()

        print(f"\n{'═' * 70}")
        print(f"  {home_code}-{away_code}  ({home} vs {away})")
        print(f"  Home: {home}    Away: {away}")
        if game.kickoff_ts_utc:
            print(f"  Kickoff: {datetime.fromtimestamp(game.kickoff_ts_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'═' * 70}")

        # Group targets by market type
        by_type: dict[str, list[tuple[Any, Any]]] = {}
        for market in game.markets:
            for target in market.targets:
                by_type.setdefault(market.sports_market_type, []).append((market, target))

        for mtype, items in sorted(by_type.items()):
            print(f"  {mtype.upper()} ({len(items)} targets)")
            for _market, target in items:
                sem = str(target.outcome_semantic)
                tok = str(target.token_id)
                tok_short = tok[:8] + "..." + tok[-4:] if len(tok) > 16 else tok
                line_str = f" {target.line}" if target.line is not None else ""

                # Resolve team name for home/away semantics
                sem_upper = sem.upper()
                team_note = ""
                if "HOME" in sem_upper:
                    team_note = f" ({home})"
                elif "AWAY" in sem_upper:
                    team_note = f" ({away})"

                # Flag problems
                flags = ""
                if sem == "unknown":
                    flags = " ⚠️  UNKNOWN SEMANTIC"
                    n_unknown += 1
                sk_parts = str(target.strategy_key).split(":")
                if len(sk_parts) >= 3 and sk_parts[2].startswith("0x"):
                    flags = " ⚠️  GENERIC KEY"
                    n_generic_keys += 1

                print(f"    {sem_upper}{line_str}{team_note:<30} → {tok_short}  {flags}")
                n_targets += 1

    # Summary
    print(f"\n{'─' * 70}")
    print(f"  Summary: {len(compiled_plan.games)} games, {n_targets} targets, run_id={run_id}")
    if n_unknown > 0:
        print(f"  ⚠️  {n_unknown} unknown semantics")
    if n_generic_keys > 0:
        print(f"  ⚠️  {n_generic_keys} generic strategy keys")
    if n_unknown == 0 and n_generic_keys == 0:
        print(f"  ✓ All targets have resolved semantics and self-describing keys")

    # Check for duplicate semantics within a game+market_type
    for game in compiled_plan.games:
        by_type_sem: dict[str, list[str]] = {}
        for market in game.markets:
            for target in market.targets:
                by_type_sem.setdefault(market.sports_market_type, []).append(
                    str(target.outcome_semantic),
                )
        for mtype, sems in by_type_sem.items():
            counts = Counter(sems)
            for sem, count in counts.items():
                # Multi-instance semantics are expected for: totals (multiple lines),
                # spreads (multiple lines), exact scores (multiple scorelines).
                multi_instance = {
                    "over", "under",                  # totals / corners at different lines
                    "home", "away",                   # spreads at different lines
                    "exact_yes", "exact_no",          # exact scores at different scorelines
                }
                if count > 1 and sem not in multi_instance:
                    gid_short = game.provider_game_id[:30]
                    print(f"  ⚠️  DUPLICATE SEMANTIC: {gid_short} {mtype} has {count}× {sem}")
    print()
    return 0


def run_hotpath_launch(args: Any, *, logger: logging.Logger) -> int:
    """Sync + link + launch hotpath in one step.

    Runs: market sync → provider sync → link build → (confirm) → hotpath live.
    """
    _load_dotenv(logger)
    runtime = _runtime_from_args(args)

    # Resolve leagues (same logic as run_hotpath_live).
    from polybot2.linking import load_mapping as _load_mapping
    mapping = _load_mapping()
    live_policy = load_live_trading_policy()

    sport_arg = str(getattr(args, "sport", "") or "").strip().lower()
    raw_leagues = getattr(args, "league", None) or []
    if sport_arg and raw_leagues:
        logger.error("cannot specify both --sport and --league")
        return 1
    if not sport_arg and not raw_leagues:
        logger.error("one of --sport or --league is required")
        return 1

    if sport_arg:
        league_keys = sorted(
            lk for lk, cfg in mapping.leagues.items()
            if cfg.get("sport_family") == sport_arg
            and lk in live_policy.live_betting_leagues
        )
        gender_arg = str(getattr(args, "gender", "") or "").strip().lower()
        if gender_arg:
            league_keys = [
                lk for lk in league_keys
                if mapping.leagues.get(lk, {}).get("polymarket_league_code") == gender_arg
            ]
        if not league_keys:
            logger.error("no live leagues for sport=%s gender=%s", sport_arg, gender_arg or "all")
            return 1
        logger.info("sport=%s gender=%s resolved to %d leagues", sport_arg, gender_arg or "all", len(league_keys))
    else:
        league_keys = [str(l).strip().lower() for l in raw_leagues]

    # Determine providers needed (all providers, not just primary).
    providers = sorted({
        str(p).strip().lower()
        for lk in league_keys
        for p in (
            mapping.leagues.get(lk, {}).get("provider", [])
            if isinstance(mapping.leagues.get(lk, {}).get("provider"), list)
            else [mapping.leagues.get(lk, {}).get("provider", "")]
        )
        if str(p).strip()
    })

    # Step 1: Market sync.
    logger.info("step 1/4: market sync")
    try:
        from polybot2.data import MarketSync, MarketSyncConfig
        import asyncio
        import concurrent.futures
        with open_database(runtime) as db:
            sync = MarketSync(db=db, config=MarketSyncConfig(gamma_api=runtime.gamma_api, open_only=True, fast_mode=True))
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                count = pool.submit(asyncio.run, sync.run()).result()
            logger.info("  market sync complete: %d markets", int(count))
    except Exception as exc:
        logger.error("market sync failed: %s: %s", type(exc).__name__, exc)
        return 1

    # Step 2: Provider sync (only relevant providers).
    logger.info("step 2/4: provider sync (%s)", ", ".join(providers))
    try:
        from polybot2.providers import sync_provider_games
        with open_database(runtime) as db:
            for p in providers:
                res = sync_provider_games(db=db, provider=p)
                if res.status != "ok":
                    logger.error("  %s: failed (%s)", p, res.reason)
                    return 1
                logger.info("  %s: %d games", p, int(res.n_rows))
    except Exception as exc:
        logger.error("provider sync failed: %s: %s", type(exc).__name__, exc)
        return 1

    # Step 3: Link build.
    logger.info("step 3/4: link build")
    try:
        from polybot2.linking import LinkService
        pairs: list[tuple[str, str]] = []
        for lk in league_keys:
            league_cfg = mapping.leagues.get(lk, {})
            raw_p = league_cfg.get("provider", "")
            prov_list = list(raw_p) if isinstance(raw_p, list) else [raw_p] if raw_p else []
            for p in prov_list:
                p = str(p).strip()
                if p:
                    pairs.append((lk, p))
        with open_database(runtime) as db:
            svc = LinkService(db=db)
            result = svc.build_links_multi(
                league_provider_pairs=pairs,
                mapping=mapping,
                live_policy=live_policy,
                league_scope="live",
            )
        logger.info(
            "  linked %d games, %d targets (run_id=%d)",
            int(result.n_games_linked), int(result.n_targets), int(result.run_id),
        )
        if result.n_games_linked == 0:
            logger.warning("  no games linked — nothing to trade")
    except Exception as exc:
        logger.error("link build failed: %s: %s", type(exc).__name__, exc)
        return 1

    # Step 4: Confirm + start.
    if not getattr(args, "yes", False):
        try:
            response = input(
                f"\nStart hotpath with {result.n_games_linked} games, "
                f"{result.n_targets} targets? [Y/n] "
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            logger.info("aborted")
            return 0
        if response and response != "y":
            logger.info("aborted")
            return 0

    logger.info("step 4/4: starting hotpath")
    return run_hotpath_live(args, logger=logger)


__all__ = [
    "run_hotpath_compile",
    "run_hotpath_launch",
    "run_hotpath_live",
    "run_hotpath_observe",
    "FastExecutionService",
    "NativeHotPathService",
    "build_sports_provider",
    "load_live_trading_policy",
    "_hotpath_order_policy_for_league",
    "_hotpath_runtime_policy_for_league",
]
