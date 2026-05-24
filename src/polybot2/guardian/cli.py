"""Guardian CLI command handlers.

Supports ``--snapshot`` mode only.  The live watch loop is now handled by
the hotpath orchestrator via :class:`GuardianManager`.
"""

from __future__ import annotations

import logging
import os
from typing import Any

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
                    status = "FAILED"
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


def run_guardian_watch(args: Any, *, logger: logging.Logger) -> int:
    """Run a guardian snapshot (one-shot read of log, print state, exit).

    Live watch mode is handled by the hotpath orchestrator
    (``polybot2 hotpath live``).
    """
    snapshot_mode = bool(getattr(args, "snapshot", False))
    if not snapshot_mode:
        logger.error(
            "guardian watch mode is now integrated into the orchestrator. "
            "Use 'polybot2 hotpath live --sport soccer' for live monitoring, "
            "or 'polybot2 guardian watch --snapshot' for a one-shot state dump."
        )
        return 1

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

    logger.info("guardian snapshot: %s", log_file)

    # Load compiled plan for token→condition + game-ID mappings
    from polybot2.guardian.tracker import build_game_id_map, build_token_to_condition_map

    token_to_condition: dict[str, str] = {}
    game_id_map: dict[str, str] = {}
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
                str(_g_raw_p[0]).strip().lower()
                if isinstance(_g_raw_p, list) and _g_raw_p
                else str(_g_raw_p).strip().lower()
            )
            from polybot2.data import open_database

            _g_sport = str(_g_cfg.get("sport_family", "baseball")).strip().lower()
            _g_stw = int(_g_cfg.get("sets_to_win", 2))
            with open_database(runtime) as db:
                compiled_plan = compile_hotpath_plan(
                    db=db,
                    provider=g_provider,
                    league=league_key,
                    run_id=int(link_run_id),
                    sport=_g_sport,
                    sets_to_win=_g_stw,
                )
            token_to_condition = build_token_to_condition_map(compiled_plan)
            game_id_map = build_game_id_map(compiled_plan)
            logger.info(
                "loaded plan: %d games, %d token→condition, %d game-id mappings",
                len(compiled_plan.games),
                len(token_to_condition),
                len(game_id_map),
            )
    except Exception as exc:
        logger.debug("could not load compiled plan: %s", exc)

    tracker = OrderStateTracker(
        log_path=log_file,
        token_to_condition=token_to_condition,
        game_id_map=game_id_map,
    )
    state = tracker.run_snapshot()
    print(_render_tracker_state(state))
    return 0
