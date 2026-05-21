"""Guardian CLI command handlers."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from typing import Any

from polybot2.guardian.clob_client import ClobClient
from polybot2.guardian.polymarket_ws import PolymarketUserWS
from polybot2.guardian.state import TrackerState
from polybot2.guardian.tracker import OrderStateTracker
from polybot2.hotpath.live_observer import find_latest_log


def _render_tracker_state(state: TrackerState) -> str:
    """Render the current tracker state as a human-readable string."""
    lines: list[str] = []

    if not state.games:
        return "No games tracked yet.\n"

    for gid in sorted(state.games.keys()):
        game = state.games[gid]
        lines.append(f"\nGame: {gid}")
        lines.append(f"Score: {game.current_home}-{game.current_away} ({game.current_half}) [{game.current_game_state}]")

        if game.score_timeline:
            lines.append(f"Score changes: {len(game.score_timeline)}")

        if game.orders:
            lines.append(f"{'Score Change':<18} {'Strategy Key':<40} {'TIF':<5} {'Fill':>8} {'Price':>7} {'Status':<15}")
            lines.append("-" * 100)
            for order in game.orders:
                sc = order.triggered_by
                if sc and sc.prev_home is not None and sc.prev_away is not None:
                    score_str = f"{sc.prev_home}-{sc.prev_away} -> {sc.home}-{sc.away}"
                else:
                    score_str = "—"

                # Truncate strategy key to show market type + side
                sk_parts = order.strategy_key.split(":", 1)
                sk_short = sk_parts[1] if len(sk_parts) > 1 else order.strategy_key

                if not order.ok:
                    status = f"FAILED"
                    fill_str = "—"
                    price_str = "—"
                elif order.exchange_id == "noop":
                    status = "NOOP"
                    fill_str = "—"
                    price_str = "—"
                elif order.clob_queried:
                    status = order.order_status or "UNKNOWN"
                    fill_str = f"${order.fill_amount:.2f}" if order.fill_amount is not None else "?"
                    price_str = f"{order.fill_price:.3f}" if order.fill_price is not None else "?"
                else:
                    status = "PENDING_QUERY"
                    fill_str = "?"
                    price_str = "?"

                lines.append(f"{score_str:<18} {sk_short:<40} {order.time_in_force:<5} {fill_str:>8} {price_str:>7} {status:<15}")
        else:
            lines.append("  No orders.")

    # Summary
    total_orders = sum(len(g.orders) for g in state.games.values())
    ok_orders = sum(1 for g in state.games.values() for o in g.orders if o.ok)
    filled = sum(1 for g in state.games.values() for o in g.orders if o.clob_queried and o.fill_amount and o.fill_amount > 0)
    pending_fak = len(state.pending_fak_queries)
    pending_gtc = len(state.pending_gtc_ids)

    lines.append(f"\n--- Summary: {len(state.games)} games, {total_orders} orders ({ok_orders} accepted, {filled} filled), FAK pending: {pending_fak}, GTC polling: {pending_gtc} ---")

    return "\n".join(lines) + "\n"


def run_guardian_track(args: Any, *, logger: logging.Logger) -> int:
    """Run the order state tracker."""
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

    snapshot_mode = bool(getattr(args, "snapshot", False))
    logger.info("guardian tracking: %s (mode=%s)", log_file, "snapshot" if snapshot_mode else "watch")

    # Load order policy config for TIF determination
    try:
        from polybot2.linking import load_live_trading_policy
        live_policy = load_live_trading_policy()
        # Use the first available league's execution policy as default
        exec_by_league = getattr(live_policy, "hotpath_execution_by_league", {}) or {}
        order_policy_cfg = next(iter(exec_by_league.values()), {}) if exec_by_league else {}
    except Exception:
        order_policy_cfg = {}

    # Load compiled plan for token_id → condition_id mapping
    from polybot2.guardian.tracker import build_token_to_condition_map
    token_to_condition: dict[str, str] = {}
    compiled_plan = None
    try:
        link_run_id = getattr(args, "link_run_id", None)
        league_key = str(getattr(args, "league", "") or "").strip().lower()
        if link_run_id is not None and league_key:
            from polybot2._cli.common import _runtime_from_args
            from polybot2.hotpath.compiler import compile_hotpath_plan
            from polybot2.linking import load_mapping as _load_mapping_guardian
            runtime = _runtime_from_args(args)
            mapping_g = _load_mapping_guardian()
            _g_cfg = mapping_g.leagues.get(league_key, {})
            _g_raw_p = _g_cfg.get("provider", "kalstrop_v1")
            g_provider = (
                str(_g_raw_p[0]).strip().lower() if isinstance(_g_raw_p, list) and _g_raw_p
                else str(_g_raw_p).strip().lower()
            )
            from polybot2.data import open_database
            with open_database(runtime) as db:
                compiled_plan = compile_hotpath_plan(
                    db=db, provider=g_provider, league=league_key,
                    run_id=int(link_run_id),
                )
            token_to_condition = build_token_to_condition_map(compiled_plan)
            logger.info("loaded plan: %d games, %d token→condition mappings", len(compiled_plan.games), len(token_to_condition))
    except Exception as exc:
        logger.debug("could not load compiled plan for condition_id mapping: %s", exc)

    # Build CLOB client (for REST fallback fill queries)
    clob: ClobClient | None = None
    ws: PolymarketUserWS | None = None
    if not snapshot_mode:
        try:
            clob = ClobClient.from_env()
            if not clob._api_key:
                logger.warning("CLOB credentials not set — fill queries disabled")
                clob = None
        except Exception as exc:
            logger.warning("CLOB client init failed: %s — fill queries disabled", exc)
            clob = None
        # Build Polymarket user WS client
        try:
            ws = PolymarketUserWS.from_env()
            if not ws._api_key:
                logger.warning("WS credentials not set — WS fill tracking disabled")
                ws = None
            else:
                logger.info("Polymarket user WS enabled")
        except Exception as exc:
            logger.warning("WS client init failed: %s — WS disabled", exc)
            ws = None

    tracker = OrderStateTracker(
        log_path=log_file,
        clob=clob,
        ws=ws,
        order_policy_config=order_policy_cfg,
        token_to_condition=token_to_condition,
    )

    if snapshot_mode:
        state = tracker.run_snapshot()
        print(_render_tracker_state(state))
        return 0

    # Watch mode: tail log and continuously update
    stop = False

    def _on_signal(_sig: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    prev_int = signal.signal(signal.SIGINT, _on_signal)
    prev_term = signal.signal(signal.SIGTERM, _on_signal)

    last_render = 0.0

    def _on_update(state: TrackerState) -> None:
        nonlocal last_render
        now = time.time()
        if now - last_render >= 1.0:
            os.system("clear" if os.name != "nt" else "cls")
            print(_render_tracker_state(state))
            last_render = now

    tracker.set_on_update(_on_update)

    try:
        asyncio.run(tracker.run_watch())
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGINT, prev_int)
        signal.signal(signal.SIGTERM, prev_term)
        if clob:
            asyncio.run(clob.close())

    # Final render
    print(_render_tracker_state(tracker.state))
    return 0
