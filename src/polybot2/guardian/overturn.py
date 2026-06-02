"""Overturn detection engine with dual-signal confirmation.

Detects VAR goal overturns using two mandatory signals:
1. Score reversal from provider (held for >N seconds, not wobble)
2. Market activity resumption on Polymarket (best bid drops below threshold)

Both signals must be confirmed before triggering sell/cancel actions.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from polybot2.guardian.state import (
    GameState,
    OverturnAlert,
    ScoreEvent,
    TrackedOrder,
)

logger = logging.getLogger("polybot2.guardian")

DEFAULT_CONFIRMATION_WINDOW_S = 10.0
DEFAULT_BID_THRESHOLD = 0.80


class OverturnDetector:
    """Detects goal overturns using dual-signal confirmation."""

    def __init__(
        self,
        *,
        confirmation_window_s: float = DEFAULT_CONFIRMATION_WINDOW_S,
        bid_threshold: float = DEFAULT_BID_THRESHOLD,
        on_overturn_triggered: Callable[[OverturnAlert], Awaitable[None] | None] | None = None,
        guardian_logger: Any = None,
    ):
        self._confirmation_window_s = confirmation_window_s
        self._bid_threshold = bid_threshold
        self._on_overturn_triggered = on_overturn_triggered
        self.glog = guardian_logger

        # Active alerts keyed by game_id
        self.alerts: dict[str, OverturnAlert] = {}

        # Best bid per token_id (updated from market WS)
        self._best_bids: dict[str, float] = {}
        self._best_asks: dict[str, float] = {}

        # Map token_id → game_id for quick alert lookup from market events
        self._token_to_game: dict[str, str] = {}

    def on_score_change(
        self,
        game: GameState,
        prev_home: int,
        prev_away: int,
        new_home: int,
        new_away: int,
        ts: int,
    ) -> OverturnAlert | None:
        """Called on every score change. Detects reversals and manages alerts.

        Returns an OverturnAlert if a new alert was armed, or None.
        """
        game_id = game.game_id

        # Check for score DECREASE (reversal)
        is_reversal = new_home < prev_home or new_away < prev_away

        if is_reversal:
            # Find the original score event that produced the now-reversed goal
            original_event = self._find_original_goal_event(
                game, prev_home, prev_away, new_home, new_away,
            )
            if original_event is None:
                return None

            # Find all orders triggered by that goal
            affected = self._find_affected_orders(game, original_event)
            if not affected:
                logger.info(
                    "score reversal in %s (%d-%d → %d-%d) but no orders were triggered by the original goal",
                    game_id, prev_home, prev_away, new_home, new_away,
                )
                return None

            affected_tokens = {o.token_id for o in affected if o.token_id}

            alert = OverturnAlert(
                game_id=game_id,
                original_score_event=original_event,
                reversal_ts=ts,
                reversed_home=new_home,
                reversed_away=new_away,
                affected_orders=list(affected),
                affected_token_ids=affected_tokens,
            )
            if game_id in self.alerts:
                self._cleanup_alert(game_id)
            self.alerts[game_id] = alert

            # Register tokens for market monitoring
            for tid in affected_tokens:
                self._token_to_game[tid] = game_id

            logger.warning(
                "⚠️  OVERTURN ALERT ARMED: %s score %d-%d → %d-%d (reversal of %d-%d → %d-%d), %d affected orders",
                game_id,
                prev_home, prev_away, new_home, new_away,
                original_event.prev_home, original_event.prev_away,
                original_event.home, original_event.away,
                len(affected),
            )
            if self.glog:
                self.glog.log_overturn_armed(
                    game_id,
                    (original_event.home, original_event.away),
                    (new_home, new_away),
                    list(affected_tokens),
                    [{"sk": o.strategy_key, "eid": o.exchange_id, "tif": o.time_in_force, "fill_amount": o.fill_amount} for o in affected],
                )
            return alert

        # Check if score went back UP — disarm alert (wobble resolved)
        if game_id in self.alerts:
            alert = self.alerts[game_id]
            if not alert.acted:
                # Score increased again — the reversal was wobble
                is_restored = (
                    new_home >= alert.original_score_event.home
                    and new_away >= alert.original_score_event.away
                )
                if is_restored:
                    logger.info(
                        "overturn alert DISARMED for %s — score restored to %d-%d (wobble)",
                        game_id, new_home, new_away,
                    )
                    self._cleanup_alert(game_id)

        return None

    def on_best_bid_ask(self, data: dict[str, Any]) -> None:
        """Called on market WS best_bid_ask events.

        Checks if any armed alert's token has a bid below threshold (Signal 2).
        """
        # The event may have asset_id or token_id
        token_id = str(data.get("asset_id", "") or data.get("token_id", ""))
        if not token_id:
            return

        bid = data.get("best_bid")
        ask = data.get("best_ask")
        if bid is not None:
            try:
                bid_val = float(bid)
                self._best_bids[token_id] = bid_val
            except (TypeError, ValueError):
                return
        else:
            return
        if ask is not None:
            try:
                self._best_asks[token_id] = float(ask)
            except (TypeError, ValueError):
                pass

        logger.debug("market bid: token=%s… bid=%.4f", token_id[:16], bid_val)

        # Check if this token belongs to an armed alert
        game_id = self._token_to_game.get(token_id)
        if not game_id or game_id not in self.alerts:
            return

        alert = self.alerts[game_id]
        if alert.acted:
            return

        bid_val = self._best_bids.get(token_id, 1.0)
        if token_id in alert.affected_token_ids:
            logger.warning(
                "📊 bid update for %s: token %s… bid=%.4f (threshold: %.4f, signal1=%s, signal2=%s)",
                game_id, token_id[:16], bid_val, self._bid_threshold,
                alert.signal1_confirmed, alert.signal2_confirmed,
            )
        if bid_val < self._bid_threshold and token_id in alert.affected_token_ids:
            if not alert.signal2_confirmed:
                alert.signal2_confirmed = True
                logger.warning(
                    "⚠️  Signal 2 CONFIRMED for %s — token %s bid dropped to %.3f (threshold: %.3f)",
                    game_id, token_id[:20] + "...", bid_val, self._bid_threshold,
                )
                self._check_and_trigger(alert)

    def on_var_action(self, game: Any, var_type: str, var_subtype: str, ts: int) -> None:
        """Called when a V2 VAR match action is detected in the log tick.

        Provides early warning (VAR review started) and definitive confirmation
        (GoalAwarded/GoalNotAwarded) without relying on score reversal timing.
        """
        game_id = game.game_id
        if var_type == "Var" and var_subtype == "Goal":
            logger.warning(
                "🔍 VAR review started for %s (goal under review)", game_id,
            )
        elif var_type == "VarEnded":
            if "NotAwarded" in var_subtype:
                logger.warning(
                    "🚨 VAR: GOAL OVERTURNED for %s (%s)", game_id, var_subtype,
                )
                # TODO: this is a definitive overturn signal — can trigger
                # sell/buy immediately without waiting for score reversal.
            elif "Awarded" in var_subtype:
                logger.info(
                    "✅ VAR: goal confirmed for %s (%s)", game_id, var_subtype,
                )

    def check_confirmations(self) -> list[OverturnAlert]:
        """Check if any armed alerts have passed the confirmation window (Signal 1).

        Call this periodically (e.g., every second). Returns list of newly triggered alerts.
        """
        now_ms = int(time.time() * 1000)
        triggered: list[OverturnAlert] = []

        for game_id, alert in list(self.alerts.items()):
            if alert.acted:
                continue

            # Check Signal 1: score held reversed for >N seconds
            if not alert.signal1_confirmed:
                elapsed_s = (now_ms - alert.reversal_ts) / 1000.0
                if elapsed_s >= self._confirmation_window_s:
                    alert.signal1_confirmed = True
                    logger.warning(
                        "⚠️  Signal 1 CONFIRMED for %s — score reversal held for %.1fs",
                        game_id, elapsed_s,
                    )
                    result = self._check_and_trigger(alert)
                    if result:
                        triggered.append(alert)

        return triggered

    def _check_and_trigger(self, alert: OverturnAlert) -> bool:
        """If both signals confirmed, trigger the overturn response."""
        if alert.signal1_confirmed and alert.signal2_confirmed and not alert.acted:
            logger.warning(
                "🚨 OVERTURN CONFIRMED for %s — TRIGGERING sell/cancel for %d orders",
                alert.game_id, len(alert.affected_orders),
            )
            if self.glog:
                prices: dict[str, dict[str, float]] = {}
                for tok in alert.affected_token_ids:
                    p: dict[str, float] = {}
                    if tok in self._best_bids:
                        p["bid"] = round(self._best_bids[tok], 4)
                    if tok in self._best_asks:
                        p["ask"] = round(self._best_asks[tok], 4)
                    if p:
                        prices[tok] = p
                self.glog.log_overturn_confirmed(
                    alert.game_id, alert.signal1_confirmed, alert.signal2_confirmed, prices,
                )
            if self._on_overturn_triggered:
                result = self._on_overturn_triggered(alert)
                if asyncio.iscoroutine(result):
                    task = asyncio.ensure_future(result)
                    task.add_done_callback(
                        lambda t, a=alert: self._on_execution_done(t, a)
                    )
                else:
                    # Synchronous callback completed — mark acted
                    alert.acted = True
            else:
                alert.acted = True
            return True
        return False

    def _on_execution_done(self, task: asyncio.Task, alert: OverturnAlert) -> None:
        """Callback after async overturn execution completes or fails."""
        exc = task.exception()
        if exc is None:
            alert.acted = True
            logger.info("overturn execution completed for %s", alert.game_id)
        else:
            # Execution failed — do NOT mark acted, allow retry on next check
            logger.error(
                "🚨 OVERTURN EXECUTION FAILED for %s: %s — will retry on next confirmation check",
                alert.game_id, exc,
            )
            # Reset both signals so check_confirmations re-evaluates
            # (signals are still true from the data, so it will re-trigger immediately)
            alert.signal1_confirmed = True
            alert.signal2_confirmed = True

    def _find_original_goal_event(
        self,
        game: GameState,
        prev_home: int,
        prev_away: int,
        new_home: int,
        new_away: int,
    ) -> ScoreEvent | None:
        """Find the ScoreEvent that produced the goal that was just reversed.

        E.g., if score went 2-0 → 1-0 (home goal overturned), find the event
        where score went from X-0 → 2-0 (the original home goal).
        """
        # The reversed goal is the difference between prev and new scores
        # Home goal reversed: prev_home > new_home → look for event where home increased to prev_home
        # Away goal reversed: prev_away > new_away → look for event where away increased to prev_away

        for event in reversed(game.score_timeline):
            if event.prev_home is None or event.prev_away is None:
                continue
            # Match: this event's result equals the pre-reversal score,
            # and the change direction matches what was reversed
            if event.home == prev_home and event.away == prev_away:
                # This is the goal that produced the score we just lost
                if prev_home > new_home and event.home > event.prev_home:
                    return event  # home goal that was overturned
                if prev_away > new_away and event.away > event.prev_away:
                    return event  # away goal that was overturned
        return None

    def _find_affected_orders(
        self,
        game: GameState,
        original_event: ScoreEvent,
    ) -> list[TrackedOrder]:
        """Find orders triggered by the original goal event."""
        affected: list[TrackedOrder] = []
        for order in game.orders:
            if not order.ok or order.exchange_id == "noop":
                continue
            if order.triggered_by is None:
                continue
            # Match by the exact score event that triggered the order
            if (
                order.triggered_by.ts == original_event.ts
                and order.triggered_by.home == original_event.home
                and order.triggered_by.away == original_event.away
            ):
                affected.append(order)
        return affected

    def _cleanup_alert(self, game_id: str) -> None:
        """Remove an alert and its token mappings."""
        alert = self.alerts.pop(game_id, None)
        if alert:
            for tid in alert.affected_token_ids:
                self._token_to_game.pop(tid, None)

    def get_active_alerts(self) -> list[OverturnAlert]:
        """Return all active (non-acted) alerts."""
        return [a for a in self.alerts.values() if not a.acted]

    def get_token_ids_to_monitor(self) -> set[str]:
        """Return all token IDs that need market WS monitoring."""
        ids: set[str] = set()
        for alert in self.alerts.values():
            if not alert.acted:
                ids.update(alert.affected_token_ids)
        return ids
