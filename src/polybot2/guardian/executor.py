"""Overturn response executor: cancel resting GTCs, sell filled positions.

Executes automatically when both overturn signals are confirmed.
All actions are logged to a JSONL file for post-session review.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from polybot2.guardian.clob_client import ClobClient
from polybot2.guardian.state import OverturnAlert, TrackedOrder

logger = logging.getLogger("polybot2.guardian")

# Don't sell if best bid is below this (sanity check — avoids selling at near-zero)
MIN_SELL_BID = 0.01


class OverturnExecutor:
    """Executes sell/cancel actions when an overturn is confirmed."""

    def __init__(
        self,
        clob: ClobClient,
        *,
        dry_run: bool = True,
        log_dir: str = ".",
    ):
        self.clob = clob
        self.dry_run = dry_run
        self.actions: list[dict[str, Any]] = []
        guardian_log_dir = os.path.join(log_dir, "guardian_logs")
        os.makedirs(guardian_log_dir, exist_ok=True)
        self._log_path = os.path.join(
            guardian_log_dir,
            f"guardian_actions_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl",
        )
        self._log_file = open(self._log_path, "a")
        logger.info("guardian action log: %s", self._log_path)

    async def execute(
        self,
        alert: OverturnAlert,
        best_bids: dict[str, float],
    ) -> None:
        """Execute sell/cancel for a confirmed overturn alert.

        1. Cancel all resting GTC orders triggered by the overturned goal.
        2. Submit sell GTC orders for filled positions at current best bid.
        """
        game_id = alert.game_id
        orig = alert.original_score_event
        logger.warning(
            "🚨 EXECUTING overturn response for %s (score %d-%d → %d-%d): %d affected orders",
            game_id, orig.home, orig.away, alert.reversed_home, alert.reversed_away,
            len(alert.affected_orders),
        )

        # Phase 1: Cancel resting GTC orders
        for order in alert.affected_orders:
            if order.time_in_force == "GTC" and order.order_status in ("OPEN", "PLACEMENT", ""):
                if order.exchange_id and order.exchange_id != "noop":
                    await self._cancel_order(game_id, order)

        # Phase 2: Sell filled positions at best bid
        for order in alert.affected_orders:
            if order.fill_amount and order.fill_amount > 0:
                bid = best_bids.get(order.token_id, 0.0)
                if bid > MIN_SELL_BID:
                    await self._submit_sell(game_id, order, bid)
                else:
                    logger.warning(
                        "skipping sell for %s — best bid %.4f too low (min: %.4f)",
                        order.strategy_key, bid, MIN_SELL_BID,
                    )
                    self._log_action({
                        "action": "sell_skipped",
                        "game_id": game_id,
                        "strategy_key": order.strategy_key,
                        "token_id": order.token_id,
                        "reason": f"bid_too_low:{bid:.4f}",
                    })

    async def _cancel_order(self, game_id: str, order: TrackedOrder) -> None:
        """Cancel a resting GTC order."""
        if self.dry_run:
            logger.warning("[DRY RUN] would cancel GTC: eid=%s sk=%s", order.exchange_id, order.strategy_key)
            self._log_action({"action": "cancel", "dry_run": True, "game_id": game_id, "strategy_key": order.strategy_key, "eid": order.exchange_id, "token_id": order.token_id})
            return
        logger.info("cancelling GTC: eid=%s sk=%s", order.exchange_id, order.strategy_key)
        ok = await self.clob.cancel_order_by_id(order.exchange_id)
        self._log_action({"action": "cancel", "game_id": game_id, "strategy_key": order.strategy_key, "eid": order.exchange_id, "token_id": order.token_id, "ok": ok})
        if ok:
            order.order_status = "CANCELLED"
            logger.info("✓ cancelled: %s", order.strategy_key)
        else:
            logger.warning("✗ cancel failed: %s", order.strategy_key)

    async def _submit_sell(self, game_id: str, order: TrackedOrder, price: float) -> None:
        """Submit a GTC sell order for the filled amount at the given price."""
        if self.dry_run:
            logger.warning("[DRY RUN] would sell: sk=%s size=%.4f price=%.4f", order.strategy_key, order.fill_amount, price)
            self._log_action({"action": "sell", "dry_run": True, "game_id": game_id, "strategy_key": order.strategy_key, "token_id": order.token_id, "size": order.fill_amount, "price": price})
            return
        logger.info("selling: sk=%s size=%.4f price=%.4f", order.strategy_key, order.fill_amount, price)
        result = await self.clob.submit_sell_order(
            token_id=order.token_id,
            size=order.fill_amount,
            price=price,
        )
        ok = result is not None
        sell_eid = ""
        if isinstance(result, dict):
            sell_eid = str(result.get("orderID", "") or result.get("order_id", "") or "")
        self._log_action({"action": "sell", "game_id": game_id, "strategy_key": order.strategy_key, "token_id": order.token_id, "original_eid": order.exchange_id, "size": order.fill_amount, "price": price, "ok": ok, "sell_eid": sell_eid})
        if ok:
            logger.info("✓ sell submitted: %s → eid=%s", order.strategy_key, sell_eid)
        else:
            logger.warning("✗ sell failed: %s", order.strategy_key)

    def _log_action(self, data: dict[str, Any]) -> None:
        """Log an action to the JSONL file and in-memory list."""
        data["ts"] = int(time.time() * 1000)
        self.actions.append(data)
        try:
            self._log_file.write(json.dumps(data, default=str) + "\n")
            self._log_file.flush()
        except Exception:
            pass

    def close(self) -> None:
        try:
            self._log_file.close()
        except Exception:
            pass
