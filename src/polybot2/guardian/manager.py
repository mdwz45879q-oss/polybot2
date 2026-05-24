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
from polybot2.guardian.market_ws import PolymarketMarketWS
from polybot2.guardian.overturn import OverturnDetector
from polybot2.guardian.polymarket_ws import PolymarketUserWS
from polybot2.guardian.state import OverturnAlert
from polybot2.guardian.tracker import (
    OrderStateTracker,
    build_game_id_map,
    build_token_to_condition_map,
)

logger = logging.getLogger("polybot2.guardian")

# Default overturn detection thresholds
DEFAULT_BID_THRESHOLD = 0.80
DEFAULT_CONFIRMATION_WINDOW_S = 10.0


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

        # Build CLOB client (for REST fallback fill queries)
        clob: ClobClient | None = None
        try:
            clob = ClobClient.from_env()
            if not clob._api_key:
                clob = None
        except Exception:
            clob = None

        if not clob:
            raise RuntimeError("guardian requires POLY_EXEC_* credentials (CLOB client failed to initialize)")

        # Build Polymarket user WS (for real-time fill notifications)
        ws: PolymarketUserWS | None = None
        try:
            ws = PolymarketUserWS.from_env()
            if not ws._api_key:
                ws = None
        except Exception:
            ws = None

        if not ws:
            raise RuntimeError("guardian requires POLY_EXEC_* credentials (user WS failed to initialize)")

        # Build overturn detector + market WS
        market_ws = PolymarketMarketWS()

        # Build executor (dry-run by default)
        log_dir = os.environ.get("POLYBOT2_LOG_DIR", ".")
        executor: OverturnExecutor | None = None
        if clob:
            executor = OverturnExecutor(clob, dry_run=dry_run, log_dir=log_dir)

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
        )

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
        )
        self._executor = executor
        self._clob = clob

    def start(self) -> None:
        """Start the guardian in a daemon thread."""
        self._thread = threading.Thread(
            target=self._run,
            name="guardian",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        """Entry point for the guardian thread."""
        try:
            asyncio.run(self._tracker.run_watch())
        except Exception:
            logger.exception("guardian thread crashed")

    def update_plan(self, compiled_plan: Any) -> None:
        """Update mappings after incremental refresh or V2 resolution."""
        new_tok = build_token_to_condition_map(compiled_plan)
        new_gid = build_game_id_map(compiled_plan)
        self._tracker.update_mappings(new_tok, new_gid)
        logger.debug(
            "guardian mappings updated: +%d token→condition, +%d game-id entries",
            len(new_tok), len(new_gid),
        )

    def stop(self) -> None:
        """Stop the guardian and wait for the thread to exit."""
        self._tracker.request_stop()
        if self._executor:
            self._executor.close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning("guardian thread did not stop within 5s")
