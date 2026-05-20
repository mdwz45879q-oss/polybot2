"""Order state tracker: tails hotpath log, queries CLOB for fill state.

Associates orders with the score changes that triggered them,
queries Polymarket CLOB for actual fill amounts, and maintains
an in-memory model of our current inventory per game.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from polybot2.guardian.clob_client import ClobClient
from polybot2.guardian.log_tailer import tail_log, read_log_snapshot
from polybot2.guardian.state import (
    GameState,
    ScoreEvent,
    TrackedOrder,
    TrackerState,
)

logger = logging.getLogger("polybot2.guardian")

# How often to re-query GTC orders that are still OPEN (seconds)
GTC_POLL_INTERVAL_S = 10.0


class OrderStateTracker:
    """Tracks order fill state by tailing the hotpath log and querying the CLOB."""

    def __init__(
        self,
        log_path: str,
        clob: ClobClient | None = None,
        *,
        order_policy_config: dict[str, Any] | None = None,
    ):
        self.log_path = log_path
        self.clob = clob
        self.state = TrackerState()
        self._prev_scores: dict[str, tuple[int, int]] = {}
        self._last_gtc_poll: float = 0.0
        self._order_policy = order_policy_config or {}
        self._on_update_callback: Any = None

    def set_on_update(self, callback: Any) -> None:
        """Set a callback invoked after each event is processed."""
        self._on_update_callback = callback

    def _extract_game_id_from_sk(self, strategy_key: str) -> str:
        """Extract game_id from strategy key like 'gid:MARKET_TYPE:SIDE:LINE'."""
        parts = strategy_key.split(":")
        return parts[0] if parts else ""

    def _extract_market_type_from_sk(self, strategy_key: str) -> str:
        """Extract market type from strategy key like 'gid:TOTAL:OVER:1.5'."""
        parts = strategy_key.split(":")
        return parts[1] if len(parts) > 1 else ""

    def _determine_tif(self, strategy_key: str) -> str:
        """Determine FAK or GTC from order policy config.

        Uses the strategy key to look up the market type, then checks
        the league's HOTPATH_EXECUTION_POLICY for time_in_force.
        Falls back to "FAK" if unknown.
        """
        market_type = self._extract_market_type_from_sk(strategy_key).lower()
        # Check market overrides first
        overrides = self._order_policy.get("market_overrides", {})
        if market_type in overrides:
            return str(overrides[market_type].get("time_in_force", "")).upper() or "FAK"
        # Base policy
        return str(self._order_policy.get("time_in_force", "FAK")).upper()

    def _on_tick(self, ev: dict[str, Any]) -> None:
        """Process a tick event from the log."""
        gid = str(ev.get("gid", ""))
        if not gid:
            return
        home = ev.get("h")
        away = ev.get("a")
        half = str(ev.get("half", ""))
        gs = str(ev.get("gs", ""))
        ts = int(ev.get("ts", 0))

        if home is None or away is None:
            return

        home = int(home)
        away = int(away)

        game = self.state.get_or_create_game(gid)
        game.current_home = home
        game.current_away = away
        game.current_half = half
        game.current_game_state = gs

        # Detect score change
        prev = self._prev_scores.get(gid)
        if prev is None or prev != (home, away):
            prev_home, prev_away = prev if prev else (0, 0)
            score_event = ScoreEvent(
                ts=ts,
                home=home,
                away=away,
                half=half,
                game_state=gs,
                prev_home=prev_home,
                prev_away=prev_away,
            )
            game.score_timeline.append(score_event)
            self._prev_scores[gid] = (home, away)

    def _on_order(self, ev: dict[str, Any]) -> None:
        """Process an order event from the log."""
        sk = str(ev.get("sk", ""))
        tok = str(ev.get("tok", ""))
        ok = bool(ev.get("ok", False))
        eid = str(ev.get("eid", ""))
        err = str(ev.get("err", ""))
        ts = int(ev.get("ts", 0))

        if not sk:
            return

        gid = self._extract_game_id_from_sk(sk)
        tif = self._determine_tif(sk)

        # Find the score change that triggered this order
        # (orders and ticks from the same frame share the same ts)
        game = self.state.get_or_create_game(gid)
        triggered_by: ScoreEvent | None = None
        for score_ev in reversed(game.score_timeline):
            if score_ev.ts <= ts:
                triggered_by = score_ev
                break

        order = TrackedOrder(
            ts=ts,
            strategy_key=sk,
            token_id=tok,
            exchange_id=eid,
            time_in_force=tif,
            ok=ok,
            error=err,
            triggered_by=triggered_by,
        )
        game.orders.append(order)

        # Queue for CLOB fill query if accepted
        if ok and eid and eid != "noop":
            self.state.orders_by_eid[eid] = order
            if tif == "FAK":
                self.state.pending_fak_queries.append(eid)
            else:
                self.state.pending_gtc_ids.add(eid)

    async def _query_pending_fak_orders(self) -> None:
        """Query CLOB for FAK orders that were accepted. One-shot (FAK resolves immediately)."""
        if not self.clob or not self.state.pending_fak_queries:
            return

        batch = list(self.state.pending_fak_queries)
        self.state.pending_fak_queries.clear()

        for eid in batch:
            order = self.state.orders_by_eid.get(eid)
            if not order or order.clob_queried:
                continue
            resp = await self.clob.get_order(eid)
            if resp:
                _apply_clob_response(order, resp)
            order.clob_queried = True

    async def _poll_gtc_orders(self) -> None:
        """Re-query CLOB for GTC orders that are still OPEN."""
        if not self.clob or not self.state.pending_gtc_ids:
            return

        now = time.time()
        if now - self._last_gtc_poll < GTC_POLL_INTERVAL_S:
            return
        self._last_gtc_poll = now

        resolved: list[str] = []
        for eid in list(self.state.pending_gtc_ids):
            order = self.state.orders_by_eid.get(eid)
            if not order:
                resolved.append(eid)
                continue
            resp = await self.clob.get_order(eid)
            if resp:
                _apply_clob_response(order, resp)
                order.clob_queried = True
                status = order.order_status.upper()
                if status in ("MATCHED", "CANCELLED", "EXPIRED"):
                    resolved.append(eid)

        for eid in resolved:
            self.state.pending_gtc_ids.discard(eid)

    def process_event(self, ev: dict[str, Any]) -> None:
        """Process a single log event (synchronous, no CLOB queries)."""
        ev_type = ev.get("ev", "")
        if ev_type == "tick":
            self._on_tick(ev)
        elif ev_type == "order":
            self._on_order(ev)

    async def run_watch(self) -> None:
        """Tail the log file and continuously update state. Blocks forever."""
        for event in tail_log(self.log_path):
            self.process_event(event)

            # Query CLOB for pending orders
            if self.state.pending_fak_queries:
                await self._query_pending_fak_orders()
            await self._poll_gtc_orders()

            if self._on_update_callback:
                self._on_update_callback(self.state)

    def run_snapshot(self) -> TrackerState:
        """Read all events from the log (non-blocking) and return current state."""
        for event in read_log_snapshot(self.log_path):
            self.process_event(event)
        return self.state


def _apply_clob_response(order: TrackedOrder, resp: dict[str, Any]) -> None:
    """Update a TrackedOrder with data from a CLOB GET /order response."""
    order.order_status = str(resp.get("status", ""))
    # Parse fill data — field names may vary by CLOB API version
    size_matched = resp.get("size_matched") or resp.get("sizeMatched") or resp.get("filled_size")
    if size_matched is not None:
        try:
            order.fill_amount = float(size_matched)
        except (TypeError, ValueError):
            pass
    price = resp.get("price") or resp.get("avg_fill_price") or resp.get("avgFillPrice")
    if price is not None:
        try:
            order.fill_price = float(price)
        except (TypeError, ValueError):
            pass
