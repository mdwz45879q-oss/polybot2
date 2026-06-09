"""GuardianManager: launches the guardian from the hotpath orchestrator.

Runs the OrderStateTracker in a daemon thread with its own asyncio event
loop.  The orchestrator calls ``update_plan`` after incremental refresh or
V2 resolution so the guardian always has current game-ID and
token-to-condition mappings.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Any

from polybot2.guardian.clob_client import ClobClient
from polybot2.guardian.executor import OverturnExecutor
from polybot2.guardian.guardian_logger import GuardianLogger
from polybot2.guardian.market_ws import PolymarketMarketWS
from polybot2.guardian.overturn import OverturnDetector
from polybot2.guardian.polymarket_ws import PolymarketUserWS
from polybot2.guardian.state import OverturnAlert
from polybot2.guardian.tracker import (
    OrderStateTracker,
    build_game_id_map,
    build_token_to_condition_map,
    build_token_to_sk_map,
)

logger = logging.getLogger("polybot2.guardian")

DEFAULT_BID_THRESHOLD = 0.80
DEFAULT_CONFIRMATION_WINDOW_S = 10.0


def _build_session_header(compiled_plan: Any, dry_run: bool) -> dict[str, Any]:
    """Build the session_start log entry from the compiled plan."""
    games: dict[str, dict[str, Any]] = {}
    tokens: dict[str, dict[str, Any]] = {}

    plan_league = str(getattr(compiled_plan, "league", "") or "")
    plan_sport = str(getattr(compiled_plan, "sport", "") or "")
    plan_run_id = int(getattr(compiled_plan, "run_id", 0) or 0)

    for game in getattr(compiled_plan, "games", ()):
        gid = str(getattr(game, "provider_game_id", "") or "")
        if not gid:
            continue
        games[gid] = {
            "home": str(getattr(game, "canonical_home_team", "") or ""),
            "away": str(getattr(game, "canonical_away_team", "") or ""),
            "league": str(getattr(game, "canonical_league", "") or ""),
            "kickoff_utc": getattr(game, "kickoff_ts_utc", None),
        }
        for market in getattr(game, "markets", ()):
            for target in getattr(market, "targets", ()):
                tok = str(getattr(target, "token_id", "") or "")
                if not tok:
                    continue
                tokens[tok] = {
                    "gid": gid,
                    "sk": str(getattr(target, "strategy_key", "") or ""),
                    "market": str(getattr(target, "sports_market_type", "") or ""),
                    "semantic": str(getattr(target, "outcome_semantic", "") or ""),
                    "line": getattr(target, "line", None),
                    "label": str(getattr(target, "outcome_label", "") or ""),
                }

    return {
        "league": plan_league,
        "sport": plan_sport,
        "run_id": plan_run_id,
        "dry_run": dry_run,
        "games": games,
        "tokens": tokens,
    }


class GuardianManager:
    """Manages the guardian lifecycle from the hotpath orchestrator."""

    def __init__(
        self,
        *,
        log_path: str,
        compiled_plan: Any,
        order_policy_config: dict[str, Any] | None = None,
        dry_run: bool = True,
    ):
        self._log_path = log_path
        self._dry_run = dry_run
        self._thread: threading.Thread | None = None

        # Build mappings from compiled plan
        token_to_condition = build_token_to_condition_map(compiled_plan)
        game_id_map = build_game_id_map(compiled_plan)
        logger.info(
            "guardian mappings: %d token→condition, %d game-id entries",
            len(token_to_condition), len(game_id_map),
        )

        # Build session header for self-contained log
        session_header = _build_session_header(compiled_plan, dry_run)

        # Build CLOB client
        clob: ClobClient | None = None
        try:
            clob = ClobClient.from_env()
            if not clob._api_key:
                clob = None
        except Exception:
            clob = None

        if not clob:
            raise RuntimeError("guardian requires POLY_EXEC_* credentials (CLOB client failed to initialize)")

        # Build Polymarket user WS
        ws: PolymarketUserWS | None = None
        try:
            ws = PolymarketUserWS.from_env()
            if not ws._api_key:
                ws = None
        except Exception:
            ws = None

        if not ws:
            raise RuntimeError("guardian requires POLY_EXEC_* credentials (user WS failed to initialize)")

        # Build guardian logger
        log_dir = os.environ.get("POLYBOT2_LOG_DIR", ".")
        glog = GuardianLogger(log_dir=log_dir)

        # Build market WS + overturn detector + executor
        market_ws = PolymarketMarketWS()
        executor = OverturnExecutor(clob, dry_run=dry_run, guardian_logger=glog)

        async def _on_overturn(alert: OverturnAlert) -> None:
            if executor and detector:
                await executor.execute(alert, detector._best_bids)
            else:
                logger.warning(
                    "OVERTURN CONFIRMED: %s — %d affected orders (executor disabled)",
                    alert.game_id, len(alert.affected_orders),
                )

        detector = OverturnDetector(
            confirmation_window_s=DEFAULT_CONFIRMATION_WINDOW_S,
            bid_threshold=DEFAULT_BID_THRESHOLD,
            on_overturn_triggered=_on_overturn,
            guardian_logger=glog,
        )

        # All plan tokens are goal-sensitive in soccer — subscribe at startup
        all_startup_tokens = list(token_to_condition.keys())

        # Build tracker with all dependencies
        self._tracker = OrderStateTracker(
            log_path=log_path,
            clob=clob,
            ws=ws,
            market_ws=market_ws,
            detector=detector,
            order_policy_config=order_policy_config or {},
            token_to_condition=token_to_condition,
            game_id_map=game_id_map,
            startup_token_ids=all_startup_tokens,
            guardian_logger=glog,
            session_header=session_header,
        )
        self._glog = glog
        self._clob = clob

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name="guardian",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        try:
            asyncio.run(self._tracker.run_watch())
        except Exception:
            logger.exception("guardian thread crashed")

    def update_plan(self, compiled_plan: Any) -> None:
        new_tok = build_token_to_condition_map(compiled_plan)
        new_gid = build_game_id_map(compiled_plan)
        new_sk = build_token_to_sk_map(compiled_plan)
        self._tracker.update_mappings(new_tok, new_gid, token_to_sk=new_sk)
        logger.debug(
            "guardian mappings updated: +%d token→condition, +%d game-id entries",
            len(new_tok), len(new_gid),
        )

    def add_game_id_alias(self, alias_id: str, canonical_id: str) -> None:
        """Register an alternate game ID mapping (e.g., pre-resolution → fixture ID).

        Used after V2 resolution so the guardian connects order events
        (which use strategy key prefix = prematch ID) with tick events
        (which use the resolved fixture ID).
        """
        self._tracker._game_id_map[alias_id] = canonical_id
        logger.debug("guardian game-id alias: %s → %s", alias_id, canonical_id)

    def stop(self) -> None:
        self._tracker.request_stop()
        if self._glog:
            self._glog.close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning("guardian thread did not stop within 5s")
