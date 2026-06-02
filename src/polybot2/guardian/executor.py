"""Overturn response executor: cancel resting GTCs, sell filled positions.

Executes automatically when both overturn signals are confirmed.
All actions are logged via GuardianLogger.
"""

from __future__ import annotations

import logging
from typing import Any

from polybot2.guardian.clob_client import ClobClient
from polybot2.guardian.guardian_logger import GuardianLogger
from polybot2.guardian.state import OverturnAlert, TrackedOrder

logger = logging.getLogger("polybot2.guardian")

MIN_SELL_BID = 0.01


class OverturnExecutor:
    """Executes sell/cancel actions when an overturn is confirmed."""

    def __init__(
        self,
        clob: ClobClient,
        *,
        dry_run: bool = True,
        guardian_logger: GuardianLogger,
    ):
        self.clob = clob
        self.dry_run = dry_run
        self.glog = guardian_logger

    async def execute(
        self,
        alert: OverturnAlert,
        best_bids: dict[str, float],
    ) -> None:
        game_id = alert.game_id
        orig = alert.original_score_event
        logger.warning(
            "🚨 EXECUTING overturn response for %s (score %d-%d → %d-%d): %d affected orders",
            game_id, orig.home, orig.away, alert.reversed_home, alert.reversed_away,
            len(alert.affected_orders),
        )

        for order in alert.affected_orders:
            if order.time_in_force == "GTC" and order.order_status in ("OPEN", "PLACEMENT", ""):
                if order.exchange_id and order.exchange_id != "noop":
                    await self._cancel_order(game_id, order)

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
                    self.glog.log_sell_skipped(
                        game_id, order.strategy_key, order.token_id,
                        f"bid_too_low:{bid:.4f}",
                    )

    async def _cancel_order(self, game_id: str, order: TrackedOrder) -> None:
        if self.dry_run:
            logger.warning("[DRY RUN] would cancel GTC: eid=%s sk=%s", order.exchange_id, order.strategy_key)
            self.glog.log_order_cancelled(game_id, order.strategy_key, order.exchange_id, True, True)
            return
        logger.info("cancelling GTC: eid=%s sk=%s", order.exchange_id, order.strategy_key)
        ok = await self.clob.cancel_order_by_id(order.exchange_id)
        self.glog.log_order_cancelled(game_id, order.strategy_key, order.exchange_id, ok, False)
        if ok:
            order.order_status = "CANCELLED"
            logger.info("✓ cancelled: %s", order.strategy_key)
        else:
            logger.warning("✗ cancel failed: %s", order.strategy_key)

    async def _submit_sell(self, game_id: str, order: TrackedOrder, price: float) -> None:
        if self.dry_run:
            logger.warning("[DRY RUN] would sell: sk=%s size=%.4f price=%.4f", order.strategy_key, order.fill_amount, price)
            self.glog.log_position_sold(
                game_id, order.strategy_key, order.token_id,
                order.fill_amount, price, order.fill_price or 0.0, "", True, True,
            )
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
        self.glog.log_position_sold(
            game_id, order.strategy_key, order.token_id,
            order.fill_amount, price, order.fill_price or 0.0, sell_eid, ok, False,
        )
        if ok:
            logger.info("✓ sell submitted: %s → eid=%s", order.strategy_key, sell_eid)
        else:
            logger.warning("✗ sell failed: %s", order.strategy_key)
